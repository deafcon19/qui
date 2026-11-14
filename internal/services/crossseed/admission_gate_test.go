package crossseed

import (
	"bytes"
	"context"
	"encoding/gob"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/autobrr/qui/internal/models"
)

type admissionTestInstances struct {
	items []*models.Instance
}

func (s admissionTestInstances) List(context.Context) ([]*models.Instance, error) {
	return s.items, nil
}

func (s admissionTestInstances) Get(_ context.Context, id int) (*models.Instance, error) {
	for _, item := range s.items {
		if item.ID == id {
			return item, nil
		}
	}
	return nil, os.ErrNotExist
}

func TestAdmissionGateMapsInstanceAndHoldsUntilRelease(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fixture uses a POSIX shell")
	}
	command := filepath.Join(t.TempDir(), "gate")
	require.NoError(t, os.WriteFile(command, []byte("#!/bin/sh\n[ \"$1\" = srce6 ] || exit 2\nprintf 'READY\\n'\ncat >/dev/null\n"), 0o600))
	require.NoError(t, os.Chmod(command, 0o700))
	t.Setenv("QUI_CROSS_SEED_GATE_COMMAND", command)
	t.Setenv("QUI_CROSS_SEED_GATE_INSTANCES", "srce6.tarner.online:srce6")
	s := &Service{instanceStore: admissionTestInstances{items: []*models.Instance{{ID: 25, Name: "srce6.tarner.online"}}}}
	ctx, release, ready, err := s.acquireAdmission(context.WithValue(t.Context(), admissionWorkerKey{}, true), []int{25})
	require.NoError(t, err)
	require.True(t, ready)
	require.NotNil(t, ctx.Value(admissionHeldKey{}))
	release()
}

func TestAdmissionGateDefersAndQueuePersists(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fixture uses a POSIX shell")
	}
	command := filepath.Join(t.TempDir(), "gate")
	require.NoError(t, os.WriteFile(command, []byte("#!/bin/sh\nprintf 'WAIT\\n'\n"), 0o600))
	require.NoError(t, os.Chmod(command, 0o700))
	t.Setenv("QUI_CROSS_SEED_GATE_COMMAND", command)
	t.Setenv("QUI_CROSS_SEED_GATE_INSTANCES", "srce6.tarner.online:srce6")
	queueDir := filepath.Join(t.TempDir(), "queue")
	t.Setenv("QUI_CROSS_SEED_GATE_QUEUE_DIR", queueDir)
	s := &Service{instanceStore: admissionTestInstances{items: []*models.Instance{{ID: 25, Name: "srce6.tarner.online"}}}}
	_, release, ready, err := s.acquireAdmission(context.WithValue(t.Context(), admissionWorkerKey{}, true), []int{25})
	require.NoError(t, err)
	require.False(t, ready)
	release()
	item := admissionPending{CrossSeed: &CrossSeedRequest{
		TorrentData: "payload", TargetInstanceIDs: []int{25},
		SearchDecision: searchDecisionProvenance{
			Class: searchCandidateClassStrict, SourceInstanceID: 25,
			SourceHash: "sourcehash", SearchCandidateName: "Example.Series.S01.WEB-GROUP",
			Relaxed: differencesOf(differenceSource), SourceTitles: []string{"Example Series"},
		},
	}}
	require.NoError(t, s.queueAdmission(item))
	require.NoError(t, s.queueAdmission(item))
	entries, err := os.ReadDir(queueDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	data, err := os.ReadFile(filepath.Join(queueDir, entries[0].Name()))
	require.NoError(t, err)
	require.NotEmpty(t, data)
	var decoded admissionPending
	require.NoError(t, gob.NewDecoder(bytes.NewReader(data)).Decode(&decoded))
	require.Equal(t, item.CrossSeed, decoded.CrossSeed)
}

func TestAdmissionGateCoversRoutedReceiverAndRejectsScopeExpansion(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fixture uses a POSIX shell")
	}
	r := newRoutedRig(t, false)
	command := filepath.Join(t.TempDir(), "gate")
	require.NoError(t, os.WriteFile(command, []byte("#!/bin/sh\nprintf '%s\\n' \"$1\" >> \"$0.count\"\nprintf 'READY\\n'\ncat >/dev/null\n"), 0o600))
	require.NoError(t, os.Chmod(command, 0o700))
	t.Setenv("QUI_CROSS_SEED_GATE_COMMAND", command)
	t.Setenv("QUI_CROSS_SEED_GATE_INSTANCES", "lib:node-a,seeds:node-b")
	ctx, release, ready, err := r.svc.acquireAdmission(context.WithValue(t.Context(), admissionWorkerKey{}, true), []int{r.source.ID})
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, admissionScope{"node-a", "node-b"}, ctx.Value(admissionHeldKey{}))
	release()
	count, err := os.ReadFile(command + ".count")
	require.NoError(t, err)
	require.Equal(t, []string{"node-a", "node-b"}, strings.Fields(string(count)))
	ctx = context.WithValue(t.Context(), admissionHeldKey{}, admissionScope{"node-a"})
	_, _, ready, err = r.svc.acquireAdmission(ctx, []int{r.source.ID})
	require.ErrorContains(t, err, "route changed")
	require.False(t, ready)
}
