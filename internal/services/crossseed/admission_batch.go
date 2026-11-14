package crossseed

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"time"

	"github.com/rs/zerolog/log"
)

func (p admissionPending) remainingTargets() []int {
	remaining := make([]int, 0, len(p.ResolvedTargets))
	for _, id := range p.ResolvedTargets {
		if !slices.Contains(p.CompletedInstanceIDs, id) {
			remaining = append(remaining, id)
		}
	}
	return remaining
}

func (p admissionPending) needsLegacyAttempt() bool {
	return p.SeasonPack != nil || (p.NeedsSeasonPack && !p.AnySuccess && len(p.remainingTargets()) == 0)
}

func (p admissionPending) due(id int) bool {
	return !time.Now().Before(p.RetryAfter[id]) &&
		((id == 0 && p.needsLegacyAttempt()) || (id != 0 && slices.Contains(p.remainingTargets(), id)))
}

func (s *Service) admissionJobs(ctx context.Context) (map[int][]string, error) {
	directory, err := admissionQueueDir()
	if err != nil {
		return nil, err
	}
	instances, err := s.instanceStore.List(ctx)
	if err != nil {
		return nil, err
	}
	active := make(map[int]bool)
	for _, instance := range instances {
		if instance.IsActive {
			active[instance.ID] = true
		}
	}
	admissionQueueMu.Lock()
	defer admissionQueueMu.Unlock()
	files, err := admissionFiles(directory)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	jobs := make(map[int][]string)
	unreadable := 0
	for _, path := range files {
		item, readErr := prepareAdmission(path, active)
		if os.IsNotExist(readErr) {
			continue
		}
		if readErr != nil {
			unreadable++
			continue
		}
		ids := item.remainingTargets()
		if item.needsLegacyAttempt() {
			ids = []int{0}
		}
		for _, id := range ids {
			if item.due(id) {
				jobs[id] = append(jobs[id], path)
			}
		}
	}
	if unreadable > 0 {
		log.Debug().Int("entries", unreadable).Msg("Unprocessed deferred cross-seeds retained")
	}
	return jobs, nil
}

func admissionFiles(directory string) ([]string, error) {
	files, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	type waiting struct {
		path string
		at   time.Time
	}
	ordered := make([]waiting, 0, len(files))
	for _, file := range files {
		if file.IsDir() || filepath.Ext(file.Name()) != ".gob" {
			continue
		}
		info, statErr := file.Info()
		if statErr == nil {
			ordered = append(ordered, waiting{filepath.Join(directory, file.Name()), info.ModTime()})
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].at.Before(ordered[j].at) })
	paths := make([]string, 0, len(ordered))
	for _, file := range ordered {
		paths = append(paths, file.path)
	}
	return paths, nil
}

func prepareAdmission(path string, active map[int]bool) (admissionPending, error) {
	item, queuedAt, err := readAdmission(path)
	if err != nil || item.CrossSeed == nil {
		return item, err
	}
	changed := false
	if !item.TargetsResolved {
		item.ResolvedTargets = normalizeInstanceIDs(item.CrossSeed.TargetInstanceIDs)
		if len(item.ResolvedTargets) == 0 {
			for id := range active {
				item.ResolvedTargets = append(item.ResolvedTargets, id)
			}
		}
		slices.Sort(item.ResolvedTargets)
		item.TargetsResolved, changed = true, true
	}
	for _, id := range item.remainingTargets() {
		if !active[id] {
			item.CompletedInstanceIDs = append(item.CompletedInstanceIDs, id)
			changed = true
		}
	}
	if len(item.remainingTargets()) == 0 && !item.needsLegacyAttempt() {
		if err := os.Remove(path); err != nil {
			return item, err
		}
		return item, os.ErrNotExist
	}
	if changed {
		return item, saveAdmission(path, item, queuedAt)
	}
	return item, nil
}

func readAdmissionBatch(paths []string, id int) []string {
	admissionQueueMu.Lock()
	defer admissionQueueMu.Unlock()
	batch := make([]string, 0, 4)
	for _, path := range paths {
		item, _, err := readAdmission(path)
		if err != nil || !item.due(id) {
			continue
		}
		batch = append(batch, path)
		if len(batch) == 4 || id == 0 {
			break
		}
	}
	return batch
}

func admissionBatchTargets(item admissionPending, id int) []int {
	if id != 0 {
		return []int{id}
	}
	if item.CrossSeed != nil {
		return item.CrossSeed.TargetInstanceIDs
	}
	return item.SeasonPack.InstanceIDs
}

func (s *Service) admissionTargetWorker(ctx context.Context, id int, mailbox <-chan []string, slots chan struct{}) {
	for {
		var paths []string
		select {
		case <-ctx.Done():
			return
		case paths = <-mailbox:
		}
		select {
		case <-ctx.Done():
			return
		case slots <- struct{}{}:
		}
		processed := s.processAdmissionBatch(ctx, id, paths)
		<-slots
		// Release the storage lease between batches so WIP and resume can run.
		delay := time.Second
		if processed == 0 {
			delay = 15 * time.Second
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (s *Service) processAdmissionBatch(ctx context.Context, id int, paths []string) int {
	batch := readAdmissionBatch(paths, id)
	if len(batch) == 0 {
		return 0
	}
	admissionQueueMu.Lock()
	first, _, err := readAdmission(batch[0])
	admissionQueueMu.Unlock()
	if err != nil {
		return 0
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	ctx = context.WithValue(ctx, admissionWorkerKey{}, true)
	ctx, release, ready, err := s.acquireAdmission(ctx, admissionBatchTargets(first, id))
	if err != nil || !ready {
		log.Debug().Err(err).Int("instanceID", id).Msg("Deferred cross-seed target is not admitted")
		return 0
	}
	defer release()
	processed, failed := 0, 0
	started := time.Now()
	for _, path := range batch {
		if ctx.Err() != nil || (processed > 0 && time.Since(started) >= 15*time.Second) {
			break
		}
		admissionQueueMu.Lock()
		item, _, err := readAdmission(path)
		admissionQueueMu.Unlock()
		if err != nil || !item.due(id) {
			continue
		}
		settled, response, applyErr := s.applyAdmission(ctx, item, id)
		processed++
		if !settled {
			failed++
		}
		if applyErr != nil {
			log.Debug().Err(applyErr).Int("instanceID", id).Msg("Deferred cross-seed target will retry")
		}
		if err := finishAdmission(path, id, settled, response); err != nil && !os.IsNotExist(err) {
			log.Debug().Err(err).Int("instanceID", id).Msg("Could not checkpoint deferred cross-seed target")
			break
		}
	}
	log.Debug().Int("instanceID", id).Int("processed", processed).Int("retrying", failed).
		Msg("Deferred cross-seed admission batch finished")
	return processed
}

func (s *Service) applyAdmission(ctx context.Context, item admissionPending, id int) (bool, *CrossSeedResponse, error) {
	if item.CrossSeed != nil {
		req := *item.CrossSeed
		if id != 0 {
			req.TargetInstanceIDs = []int{id}
			ctx = context.WithValue(ctx, admissionTargetAttemptKey{}, true)
			ctx = context.WithValue(ctx, admissionAddedReceiversKey{}, item.AddedInstanceIDs)
		}
		response, err := s.invokeCrossSeed(ctx, &req)
		if err != nil || response == nil {
			return false, nil, err
		}
		return !crossSeedNeedsRetry(response), response, nil
	}
	if item.SeasonPack != nil {
		req := *item.SeasonPack
		req.autonomous = item.SeasonPackAutonomous
		response, err := s.invokeSeasonPackApply(ctx, &req)
		return err == nil && response != nil && response.Reason != "queued", nil, err
	}
	return false, nil, errors.New("unknown deferred cross-seed request")
}

func finishAdmission(path string, id int, settled bool, response *CrossSeedResponse) error {
	admissionQueueMu.Lock()
	defer admissionQueueMu.Unlock()
	item, queuedAt, err := readAdmission(path)
	if err != nil {
		return err
	}
	if response != nil {
		item.AnySuccess = item.AnySuccess || response.Success
		item.NeedsSeasonPack = item.NeedsSeasonPack || response.admissionNeedsSeasonPack
		for _, result := range response.Results {
			receiver := result.CrossSeedInstanceID
			if receiver == 0 {
				receiver = result.InstanceID
			}
			if result.Success && receiver != 0 && !slices.Contains(item.AddedInstanceIDs, receiver) {
				item.AddedInstanceIDs = append(item.AddedInstanceIDs, receiver)
			}
		}
	}
	if settled {
		if id == 0 {
			return os.Remove(path)
		}
		if !slices.Contains(item.CompletedInstanceIDs, id) {
			item.CompletedInstanceIDs = append(item.CompletedInstanceIDs, id)
		}
		if len(item.remainingTargets()) == 0 && !item.needsLegacyAttempt() {
			return os.Remove(path)
		}
	} else {
		if item.RetryAfter == nil {
			item.RetryAfter = make(map[int]time.Time)
		}
		item.RetryAfter[id] = time.Now().Add(30 * time.Second)
	}
	return saveAdmission(path, item, queuedAt)
}
