package crossseed

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

type admissionPending struct {
	CrossSeed            *CrossSeedRequest
	SeasonPack           *SeasonPackApplyRequest
	SeasonPackAutonomous bool
	TargetsResolved      bool
	ResolvedTargets      []int
	CompletedInstanceIDs []int
	AddedInstanceIDs     []int
	RetryAfter           map[int]time.Time
	AnySuccess           bool
	NeedsSeasonPack      bool
}

var admissionQueueMu sync.Mutex

type admissionTargetAttemptKey struct{}
type admissionAddedReceiversKey struct{}

func admissionQueueDir() (string, error) {
	if configured := strings.TrimSpace(os.Getenv("QUI_CROSS_SEED_GATE_QUEUE_DIR")); configured != "" {
		return configured, nil
	}
	configDir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(configDir, "qui", "cross-seed-gate-queue"), nil
}

func (s *Service) queueAdmission(item admissionPending) error {
	admissionQueueMu.Lock()
	defer admissionQueueMu.Unlock()
	var data bytes.Buffer
	if err := gob.NewEncoder(&data).Encode(item); err != nil {
		return fmt.Errorf("encode deferred cross-seed: %w", err)
	}
	directory, err := admissionQueueDir()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	digest := sha256.Sum256(data.Bytes())
	var finalPath string
	for {
		finalPath = filepath.Join(directory, hex.EncodeToString(digest[:])+".gob")
		if err := admissionCancellationError(finalPath); err != nil {
			if !os.IsNotExist(err) {
				return err
			}
			// A later announcement gets a new identity; stale workers keep the old fence.
			digest = sha256.Sum256(digest[:])
			continue
		}
		break
	}
	if _, err := os.Stat(finalPath); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	return writeAdmissionBytes(finalPath, data.Bytes(), time.Time{})
}

func writeAdmissionBytes(finalPath string, data []byte, queuedAt time.Time) error {
	if err := admissionCancellationError(finalPath); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(finalPath), ".pending-*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if !queuedAt.IsZero() {
		if err := os.Chtimes(temp.Name(), queuedAt, queuedAt); err != nil {
			return err
		}
	}
	return publishAdmission(temp.Name(), finalPath)
}

func readAdmission(path string) (admissionPending, time.Time, error) {
	var item admissionPending
	if err := admissionCancellationError(path); err != nil {
		if os.IsNotExist(err) {
			_ = os.Remove(path)
		}
		return item, time.Time{}, err
	}
	file, err := os.Open(path)
	if err != nil {
		return item, time.Time{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return item, time.Time{}, err
	}
	err = gob.NewDecoder(file).Decode(&item)
	if cancelErr := admissionCancellationError(path); cancelErr != nil {
		return item, info.ModTime(), cancelErr
	}
	return item, info.ModTime(), err
}

func saveAdmission(path string, item admissionPending, queuedAt time.Time) error {
	var data bytes.Buffer
	if err := gob.NewEncoder(&data).Encode(item); err != nil {
		return err
	}
	return writeAdmissionBytes(path, data.Bytes(), queuedAt)
}

func (s *Service) admissionQueueWorker(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	workers := map[int]chan []string{}
	slots := make(chan struct{}, 3)
	var running sync.WaitGroup
	defer func() { cancel(); running.Wait() }()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		jobs, err := s.admissionJobs(ctx)
		if err != nil {
			log.Debug().Err(err).Msg("Cannot refresh deferred cross-seed targets")
		}
		for id, paths := range jobs {
			mailbox := workers[id]
			if mailbox == nil {
				mailbox = make(chan []string, 1)
				workers[id] = mailbox
				running.Go(func() { s.admissionTargetWorker(ctx, id, mailbox, slots) })
			}
			// Keep one fresh snapshot per target rather than an accumulating work list.
			select {
			case <-mailbox:
			default:
			}
			select {
			case mailbox <- paths:
			default:
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func crossSeedNeedsRetry(response *CrossSeedResponse) bool {
	if len(response.Results) == 0 {
		return true
	}
	for _, result := range response.Results {
		switch result.Status {
		case "queued", "error", "hardlink_error", "reflink_error", crossSeedUnavailableStatus:
			return true
		default:
		}
	}
	return false
}
