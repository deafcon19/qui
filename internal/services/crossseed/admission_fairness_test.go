package crossseed

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	qbt "github.com/autobrr/go-qbittorrent"
	"github.com/stretchr/testify/require"

	"github.com/autobrr/qui/pkg/stringutils"
)

type admissionEmptySync struct{ qbittorrentSync }

func (admissionEmptySync) GetTorrents(context.Context, int, qbt.TorrentFilterOptions) ([]qbt.Torrent, error) {
	return nil, nil
}

func admissionFairnessService(t *testing.T) (*Service, string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("fixture uses a POSIX shell")
	}
	s := admissionQueueService(t)
	s.releaseCache, s.stringNormalizer = NewReleaseCache(), stringutils.NewDefaultNormalizer()
	s.syncManager = admissionEmptySync{}
	command := filepath.Join(t.TempDir(), "gate")
	require.NoError(t, os.WriteFile(command, []byte("#!/bin/sh\nprintf '%s\\n' \"$1\" >> \"$0.calls\"\nif [ ! -f \"$0.allow\" ]; then printf 'WAIT\\n'; exit 0; fi\nprintf 'READY\\n'\ncat >/dev/null\n"), 0o600))
	require.NoError(t, os.Chmod(command, 0o700))
	t.Setenv("QUI_CROSS_SEED_GATE_COMMAND", command)
	t.Setenv("QUI_CROSS_SEED_GATE_INSTANCES", "node-a:node-a,node-b:node-b")
	return s, command
}

func admissionFreshRequest(t *testing.T, id int) *CrossSeedRequest {
	t.Helper()
	name := fmt.Sprintf("Example.Queue.Film.%d.2026.1080p.WEB-GROUP", id)
	return &CrossSeedRequest{
		TorrentData:       base64.StdEncoding.EncodeToString(createNamedFileTestTorrent(t, name, name+".mkv", 1)),
		TargetInstanceIDs: []int{1, 2}, StartPaused: new(true), SkipAutoResume: true, SkipRecheck: true,
		SearchDecision: searchDecisionProvenance{
			Class: searchCandidateClassStrict, SourceInstanceID: 1, SourceHash: "source-hash",
			SearchCandidateName: name, SourceTitles: []string{"Example Queue Film"},
		},
	}
}

func TestAdmissionFreshRequestsPersistBeforeAcquiringAnyGate(t *testing.T) {
	for _, seasonPack := range []bool{false, true} {
		t.Run(fmt.Sprintf("season-pack=%v", seasonPack), func(t *testing.T) {
			s, command := admissionFairnessService(t)
			req := admissionFreshRequest(t, 1)
			if seasonPack {
				pack := &SeasonPackApplyRequest{TorrentName: "Example.Series.S01.WEB-GROUP", TorrentData: req.TorrentData, InstanceIDs: []int{1, 2}, autonomous: true}
				result, err := s.ApplySeasonPackWebhook(t.Context(), pack)
				require.NoError(t, err)
				require.Equal(t, "queued", result.Reason)
				jobs, err := s.admissionJobs(t.Context())
				require.NoError(t, err)
				item, _, err := readAdmission(jobs[0][0])
				require.NoError(t, err)
				expected := *pack
				expected.autonomous = false // Gob carries this separately as SeasonPackAutonomous.
				require.Equal(t, &expected, item.SeasonPack)
				require.True(t, item.SeasonPackAutonomous)
			} else {
				result, err := s.CrossSeed(t.Context(), req)
				require.NoError(t, err)
				require.Equal(t, "queued", result.Results[0].Status)
				jobs, err := s.admissionJobs(t.Context())
				require.NoError(t, err)
				item, _, err := readAdmission(jobs[1][0])
				require.NoError(t, err)
				require.Equal(t, req, item.CrossSeed)
			}
			_, err := os.Stat(command + ".calls")
			require.True(t, os.IsNotExist(err), "a fresh arrival acquired gates ahead of queued work")
		})
	}
}

func TestAdmissionOlderRequestProgressesWhileFreshAllTargetRequestsArrive(t *testing.T) {
	s, command := admissionFairnessService(t)
	original := admissionFreshRequest(t, 0)
	response, err := s.CrossSeed(t.Context(), original)
	require.NoError(t, err)
	require.Equal(t, "queued", response.Results[0].Status)
	jobs, err := s.admissionJobs(t.Context())
	require.NoError(t, err)
	oldest := jobs[1][0]
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, os.Chtimes(oldest, at, at))
	require.NoError(t, os.WriteFile(command+".allow", nil, 0o600))
	// New callers always append durable work; only the worker acquires a target lease.
	for id := 1; id <= 12; id++ {
		response, err = s.CrossSeed(t.Context(), admissionFreshRequest(t, id))
		require.NoError(t, err)
		require.Equal(t, "queued", response.Results[0].Status)
		if id%4 != 0 {
			continue
		}
		jobs, err = s.admissionJobs(t.Context())
		require.NoError(t, err)
		if id == 4 {
			require.Equal(t, oldest, jobs[1][0])
		}
		require.Equal(t, 4, s.processAdmissionBatch(t.Context(), 1, jobs[1]))
		item, queuedAt, err := readAdmission(oldest)
		require.NoError(t, err)
		require.Equal(t, []int{2}, item.remainingTargets())
		require.Equal(t, at, queuedAt.UTC())
		require.Equal(t, original, item.CrossSeed)
	}
	jobs, err = s.admissionJobs(t.Context())
	require.NoError(t, err)
	require.Equal(t, oldest, jobs[2][0])
	require.Equal(t, 4, s.processAdmissionBatch(t.Context(), 2, jobs[2]))
	_, err = os.Stat(oldest)
	require.True(t, os.IsNotExist(err), "older request did not finish before newer arrivals")
	calls, err := os.ReadFile(command + ".calls")
	require.NoError(t, err)
	require.Equal(t, []string{"node-a", "node-a", "node-a", "node-b"}, strings.Fields(string(calls)))
}
