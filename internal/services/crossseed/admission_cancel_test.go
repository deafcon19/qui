package crossseed

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func archiveAdmission(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(cancelledAdmissionPath(path)), 0o700))
	require.NoError(t, os.Rename(path, cancelledAdmissionPath(path)))
}

func TestAdmissionCancellationFencesAStaleCheckpoint(t *testing.T) {
	s := admissionQueueService(t)
	admissionFixture(t, s, "payload", 1, 2)
	jobs, err := s.admissionJobs(t.Context())
	require.NoError(t, err)
	path := jobs[1][0]
	item, queuedAt, err := readAdmission(path)
	require.NoError(t, err)
	archiveAdmission(t, path)
	before, err := os.ReadFile(cancelledAdmissionPath(path))
	require.NoError(t, err)
	require.True(t, os.IsNotExist(saveAdmission(path, item, queuedAt)))
	require.True(t, os.IsNotExist(finishAdmission(path, 1, true, admissionResult("added"))))
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
	after, err := os.ReadFile(cancelledAdmissionPath(path))
	require.NoError(t, err)
	require.Equal(t, before, after)
	jobs, err = s.admissionJobs(t.Context())
	require.NoError(t, err)
	require.Empty(t, jobs)
}

func TestAdmissionCancellationWinsAConcurrentPublish(t *testing.T) {
	s := admissionQueueService(t)
	admissionFixture(t, s, "payload", 1)
	jobs, err := s.admissionJobs(t.Context())
	require.NoError(t, err)
	path := jobs[1][0]
	// The writer's initial check passed before the user archived this request.
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	temp := filepath.Join(filepath.Dir(path), ".pending-stale-checkpoint")
	require.NoError(t, os.WriteFile(temp, data, 0o600))
	archiveAdmission(t, path)
	require.True(t, os.IsNotExist(publishAdmission(temp, path)))
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
	archived, err := os.ReadFile(cancelledAdmissionPath(path))
	require.NoError(t, err)
	require.Equal(t, data, archived)
}

func TestAdmissionCancellationSkipsSelectedWorkAndAllowsAFutureRequest(t *testing.T) {
	s := admissionQueueService(t)
	admissionFixture(t, s, "payload", 1)
	jobs, err := s.admissionJobs(t.Context())
	require.NoError(t, err)
	old := jobs[1][0]
	archiveAdmission(t, old)
	s.crossSeedInvoker = func(context.Context, *CrossSeedRequest) (*CrossSeedResponse, error) {
		t.Fatal("cancelled work reached matching")
		return nil, nil
	}
	require.Zero(t, s.processAdmissionBatch(t.Context(), 1, jobs[1]))
	admissionFixture(t, s, "payload", 1)
	admissionFixture(t, s, "payload", 1)
	jobs, err = s.admissionJobs(t.Context())
	require.NoError(t, err)
	require.Len(t, jobs[1], 1)
	require.NotEqual(t, old, jobs[1][0])
	s.crossSeedInvoker = func(context.Context, *CrossSeedRequest) (*CrossSeedResponse, error) {
		return admissionResult("no_match"), nil
	}
	require.Equal(t, 1, s.processAdmissionBatch(t.Context(), 1, jobs[1]))
	_, err = os.Stat(cancelledAdmissionPath(old))
	require.NoError(t, err)
}
