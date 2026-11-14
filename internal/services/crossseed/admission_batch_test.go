package crossseed

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	qbt "github.com/autobrr/go-qbittorrent"
	"github.com/stretchr/testify/require"

	"github.com/autobrr/qui/internal/models"
	"github.com/autobrr/qui/pkg/stringutils"
)

func admissionQueueService(t *testing.T) *Service {
	t.Helper()
	t.Setenv("QUI_CROSS_SEED_GATE_QUEUE_DIR", t.TempDir())
	t.Setenv("QUI_CROSS_SEED_GATE_COMMAND", "")
	return &Service{instanceStore: admissionTestInstances{items: []*models.Instance{
		{ID: 1, Name: "node-a", IsActive: true}, {ID: 2, Name: "node-b", IsActive: true},
	}}}
}

func admissionFixture(t *testing.T, s *Service, data string, ids ...int) {
	t.Helper()
	require.NoError(t, s.queueAdmission(admissionPending{CrossSeed: &CrossSeedRequest{
		TorrentData: data, TargetInstanceIDs: ids,
		SearchDecision: searchDecisionProvenance{
			Class: searchCandidateClassStrict, SourceInstanceID: 1, SourceHash: "source-hash",
			SearchCandidateName: "Example.Series.S01.WEB-GROUP", Relaxed: differencesOf(differenceSource),
		},
	}}))
}

func admissionResult(status string) *CrossSeedResponse {
	return &CrossSeedResponse{Results: []InstanceCrossSeedResult{{Status: status}}}
}

func TestAdmissionProgressSurvivesReloadAndRetainsProvenance(t *testing.T) {
	s := admissionQueueService(t)
	admissionFixture(t, s, "payload", 1, 2)
	jobs, err := s.admissionJobs(t.Context())
	require.NoError(t, err)
	path := jobs[1][0]
	queuedAt := time.Date(2026, 11, 10, 8, 0, 0, 0, time.UTC)
	require.NoError(t, os.Chtimes(path, queuedAt, queuedAt))
	s.crossSeedInvoker = func(ctx context.Context, req *CrossSeedRequest) (*CrossSeedResponse, error) {
		require.Equal(t, []int{1}, req.TargetInstanceIDs)
		require.NotNil(t, ctx.Value(admissionTargetAttemptKey{}))
		require.Equal(t, "source-hash", req.SearchDecision.SourceHash)
		require.True(t, req.SearchDecision.Relaxed.has(differenceSource))
		return admissionResult("added"), nil
	}
	require.Equal(t, 1, s.processAdmissionBatch(t.Context(), 1, jobs[1]))
	item, at, err := readAdmission(path)
	require.NoError(t, err)
	require.Equal(t, queuedAt, at.UTC())
	require.Equal(t, []int{2}, item.remainingTargets())
	require.Equal(t, []int{1, 2}, item.CrossSeed.TargetInstanceIDs)

	// A fresh Service sees the durable target progress, not an in-memory cursor.
	restarted := &Service{instanceStore: s.instanceStore}
	jobs, err = restarted.admissionJobs(t.Context())
	require.NoError(t, err)
	require.Empty(t, jobs[1])
	require.Len(t, jobs[2], 1)
	restarted.crossSeedInvoker = func(_ context.Context, req *CrossSeedRequest) (*CrossSeedResponse, error) {
		require.Equal(t, []int{2}, req.TargetInstanceIDs)
		return admissionResult("no_match"), nil
	}
	require.Equal(t, 1, restarted.processAdmissionBatch(t.Context(), 2, jobs[2]))
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
}

func TestAdmissionFailedTargetDoesNotBlockOtherTargetsAndBacksOff(t *testing.T) {
	s := admissionQueueService(t)
	admissionFixture(t, s, "payload", 1, 2)
	synctest.Test(t, func(t *testing.T) {
		jobs, err := s.admissionJobs(t.Context())
		require.NoError(t, err)
		s.crossSeedInvoker = func(_ context.Context, req *CrossSeedRequest) (*CrossSeedResponse, error) {
			if req.TargetInstanceIDs[0] == 1 {
				return nil, errors.New("temporary node outage")
			}
			return admissionResult("exists"), nil
		}
		require.Equal(t, 1, s.processAdmissionBatch(t.Context(), 1, jobs[1]))
		require.Equal(t, 1, s.processAdmissionBatch(t.Context(), 2, jobs[2]))
		item, _, err := readAdmission(jobs[1][0])
		require.NoError(t, err)
		require.Equal(t, []int{1}, item.remainingTargets())
		jobs, err = s.admissionJobs(t.Context())
		require.NoError(t, err)
		require.Empty(t, jobs)
		time.Sleep(31 * time.Second)
		jobs, err = s.admissionJobs(t.Context())
		require.NoError(t, err)
		require.Len(t, jobs[1], 1)
		require.Empty(t, jobs[2])
	})
}

func TestAdmissionConcurrentTargetCheckpointsDoNotLoseProgress(t *testing.T) {
	s := admissionQueueService(t)
	admissionFixture(t, s, "payload", 1, 2)
	jobs, err := s.admissionJobs(t.Context())
	require.NoError(t, err)
	var running sync.WaitGroup
	results := make(chan error, 2)
	for _, id := range []int{1, 2} {
		running.Go(func() { results <- finishAdmission(jobs[id][0], id, true, nil) })
	}
	running.Wait()
	require.NoError(t, <-results)
	require.NoError(t, <-results)
	_, err = os.Stat(jobs[1][0])
	require.True(t, os.IsNotExist(err))
}

func TestAdmissionRemembersSuccessfulReceiverAcrossSourceTargets(t *testing.T) {
	s := admissionQueueService(t)
	admissionFixture(t, s, "payload", 1, 2)
	jobs, err := s.admissionJobs(t.Context())
	require.NoError(t, err)
	response := &CrossSeedResponse{Success: true, Results: []InstanceCrossSeedResult{
		{InstanceID: 1, CrossSeedInstanceID: 99, Success: true, Status: "added"},
	}}
	require.NoError(t, finishAdmission(jobs[1][0], 1, true, response))
	s.crossSeedInvoker = func(ctx context.Context, req *CrossSeedRequest) (*CrossSeedResponse, error) {
		require.Equal(t, []int{2}, req.TargetInstanceIDs)
		require.Equal(t, []int{99}, ctx.Value(admissionAddedReceiversKey{}))
		return admissionResult("exists"), nil
	}
	require.Equal(t, 1, s.processAdmissionBatch(t.Context(), 2, jobs[2]))
	_, err = os.Stat(jobs[1][0])
	require.True(t, os.IsNotExist(err))
}

func TestAdmissionBatchReusesLeaseAndStopsAfterFourRequests(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fixture uses a POSIX shell")
	}
	s := admissionQueueService(t)
	directory := t.TempDir()
	command := filepath.Join(directory, "gate")
	require.NoError(t, os.WriteFile(command, []byte("#!/bin/sh\nprintf 'lease\\n' >> \"$0.count\"\nprintf 'READY\\n'\ncat >/dev/null\n"), 0o600))
	require.NoError(t, os.Chmod(command, 0o700))
	t.Setenv("QUI_CROSS_SEED_GATE_COMMAND", command)
	t.Setenv("QUI_CROSS_SEED_GATE_INSTANCES", "node-a:node-a,node-b:node-b")
	for _, data := range []string{"a", "b", "c", "d", "e", "f"} {
		admissionFixture(t, s, data, 1)
	}
	s.crossSeedInvoker = func(ctx context.Context, req *CrossSeedRequest) (*CrossSeedResponse, error) {
		_, release, ready, err := s.acquireAdmission(ctx, req.TargetInstanceIDs)
		require.NoError(t, err)
		require.True(t, ready)
		release()
		return admissionResult("no_match"), nil
	}
	jobs, err := s.admissionJobs(t.Context())
	require.NoError(t, err)
	require.Equal(t, 4, s.processAdmissionBatch(t.Context(), 1, jobs[1]))
	count, err := os.ReadFile(command + ".count")
	require.NoError(t, err)
	require.Equal(t, "lease\n", string(count))
	jobs, err = s.admissionJobs(t.Context())
	require.NoError(t, err)
	require.Len(t, jobs[1], 2)
	require.Equal(t, 2, s.processAdmissionBatch(t.Context(), 1, jobs[1]))
	count, err = os.ReadFile(command + ".count")
	require.NoError(t, err)
	require.Equal(t, "lease\nlease\n", string(count))
}

func TestAdmissionSeasonPackStillSelectsAcrossOriginalTargets(t *testing.T) {
	s := admissionQueueService(t)
	require.NoError(t, s.queueAdmission(admissionPending{
		SeasonPack:           &SeasonPackApplyRequest{TorrentName: "Example Series", InstanceIDs: []int{1, 2}},
		SeasonPackAutonomous: true,
	}))
	s.seasonPackApplier = func(_ context.Context, req *SeasonPackApplyRequest) (*SeasonPackApplyResponse, error) {
		require.Equal(t, []int{1, 2}, req.InstanceIDs)
		require.True(t, req.autonomous)
		return &SeasonPackApplyResponse{Applied: true}, nil
	}
	jobs, err := s.admissionJobs(t.Context())
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.Equal(t, 1, s.processAdmissionBatch(t.Context(), 0, jobs[0]))
	jobs, err = s.admissionJobs(t.Context())
	require.NoError(t, err)
	require.Empty(t, jobs)
}

func TestAdmissionSeasonPackFallbackWaitsForAllTargets(t *testing.T) {
	for _, directSuccess := range []bool{false, true} {
		t.Run(map[bool]string{false: "assemble-once", true: "direct-match-skips-assembly"}[directSuccess], func(t *testing.T) {
			s := admissionQueueService(t)
			admissionFixture(t, s, "payload", 1, 2)
			s.crossSeedInvoker = func(ctx context.Context, req *CrossSeedRequest) (*CrossSeedResponse, error) {
				if ctx.Value(admissionTargetAttemptKey{}) == nil {
					require.Equal(t, []int{1, 2}, req.TargetInstanceIDs)
					return admissionResult("no_match"), nil
				}
				response := admissionResult("no_match")
				response.admissionNeedsSeasonPack = true
				response.Success = directSuccess && slices.Contains(req.TargetInstanceIDs, 2)
				return response, nil
			}
			jobs, err := s.admissionJobs(t.Context())
			require.NoError(t, err)
			require.Equal(t, 1, s.processAdmissionBatch(t.Context(), 1, jobs[1]))
			jobs, err = s.admissionJobs(t.Context())
			require.NoError(t, err)
			require.Empty(t, jobs[0])
			require.Equal(t, 1, s.processAdmissionBatch(t.Context(), 2, jobs[2]))
			jobs, err = s.admissionJobs(t.Context())
			require.NoError(t, err)
			if directSuccess {
				require.Empty(t, jobs)
			} else {
				require.Len(t, jobs[0], 1)
				require.Equal(t, 1, s.processAdmissionBatch(t.Context(), 0, jobs[0]))
			}
		})
	}
}

func TestAdmissionAllTargetsResolveOnceAndDisabledTargetsFinish(t *testing.T) {
	s := admissionQueueService(t)
	admissionFixture(t, s, "all-targets")
	jobs, err := s.admissionJobs(t.Context())
	require.NoError(t, err)
	item, _, err := readAdmission(jobs[1][0])
	require.NoError(t, err)
	require.Equal(t, []int{1, 2}, item.ResolvedTargets)
	s.instanceStore = admissionTestInstances{items: []*models.Instance{
		{ID: 1, Name: "node-a", IsActive: true}, {ID: 3, Name: "new-node", IsActive: true},
	}}
	jobs, err = s.admissionJobs(t.Context())
	require.NoError(t, err)
	require.Empty(t, jobs[2])
	require.Empty(t, jobs[3])
	item, _, err = readAdmission(jobs[1][0])
	require.NoError(t, err)
	require.Equal(t, []int{1}, item.remainingTargets())
}

func TestAdmissionWorkersFinishHealthyTargetWhileAnotherIsBlocked(t *testing.T) {
	s := admissionQueueService(t)
	admissionFixture(t, s, "payload", 1, 2)
	jobs, err := s.admissionJobs(t.Context())
	require.NoError(t, err)
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		release := make(chan struct{})
		s.crossSeedInvoker = func(ctx context.Context, req *CrossSeedRequest) (*CrossSeedResponse, error) {
			if req.TargetInstanceIDs[0] == 1 {
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return admissionResult("no_match"), nil
		}
		done := make(chan struct{})
		go func() { s.admissionQueueWorker(ctx); close(done) }()
		synctest.Wait()
		item, _, err := readAdmission(jobs[1][0])
		require.NoError(t, err)
		require.Equal(t, []int{1}, item.remainingTargets())
		close(release)
		synctest.Wait()
		_, err = os.Stat(jobs[1][0])
		require.True(t, os.IsNotExist(err))
		cancel()
		<-done
	})
}

func TestAdmissionBatchReleasesAfterShortProcessingWindow(t *testing.T) {
	s := admissionQueueService(t)
	for _, data := range []string{"a", "b", "c", "d"} {
		admissionFixture(t, s, data, 1)
	}
	synctest.Test(t, func(t *testing.T) {
		jobs, err := s.admissionJobs(t.Context())
		require.NoError(t, err)
		s.crossSeedInvoker = func(context.Context, *CrossSeedRequest) (*CrossSeedResponse, error) {
			time.Sleep(8 * time.Second)
			return admissionResult("no_match"), nil
		}
		require.Equal(t, 2, s.processAdmissionBatch(t.Context(), 1, jobs[1]))
		jobs, err = s.admissionJobs(t.Context())
		require.NoError(t, err)
		require.Len(t, jobs[1], 2)
	})
}

func TestAdmissionWorkersBoundConcurrencyAndEventuallyFinishEveryTarget(t *testing.T) {
	s := admissionQueueService(t)
	instances := make([]*models.Instance, 0, 5)
	for id := 1; id <= 5; id++ {
		instances = append(instances, &models.Instance{ID: id, IsActive: true})
	}
	s.instanceStore = admissionTestInstances{items: instances}
	admissionFixture(t, s, "payload", 1, 2, 3, 4, 5)
	jobs, err := s.admissionJobs(t.Context())
	require.NoError(t, err)
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		release, started := make(chan struct{}), make(chan int, 5)
		s.crossSeedInvoker = func(ctx context.Context, req *CrossSeedRequest) (*CrossSeedResponse, error) {
			started <- req.TargetInstanceIDs[0]
			select {
			case <-release:
				return admissionResult("no_match"), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		done := make(chan struct{})
		go func() { s.admissionQueueWorker(ctx); close(done) }()
		synctest.Wait()
		require.Len(t, started, 3)
		close(release)
		synctest.Wait()
		_, err = os.Stat(jobs[1][0])
		require.True(t, os.IsNotExist(err))
		require.Len(t, started, 5)
		cancel()
		<-done
	})
}

type admissionUnavailableSync struct{ qbittorrentSync }

func (admissionUnavailableSync) GetTorrents(context.Context, int, qbt.TorrentFilterOptions) ([]qbt.Torrent, error) {
	return nil, errors.New("temporary qBittorrent outage")
}

func TestAdmissionTargetOutageIsNotSettledAsNoMatch(t *testing.T) {
	s := admissionQueueService(t)
	s.releaseCache, s.stringNormalizer = NewReleaseCache(), stringutils.NewDefaultNormalizer()
	s.syncManager = admissionUnavailableSync{}
	ctx := context.WithValue(t.Context(), admissionTargetAttemptKey{}, true)
	response, err := s.FindCandidates(ctx, &FindCandidatesRequest{
		TorrentName: "Example.Film.2024.1080p.WEB-GROUP", TargetInstanceIDs: []int{1},
	})
	require.ErrorContains(t, err, "temporary qBittorrent outage")
	require.Nil(t, response)
}
