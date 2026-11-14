// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package crossseed

import (
	"context"
	"path/filepath"
	"testing"

	qbt "github.com/autobrr/go-qbittorrent"
	"github.com/stretchr/testify/require"

	"github.com/autobrr/qui/internal/models"
	"github.com/autobrr/qui/pkg/stringutils"
)

func TestProcessCrossSeedCandidateDoesNotInheritIncompleteDirectory(t *testing.T) {
	t.Parallel()

	for _, paused := range []bool{true, false} {
		name := "running"
		if paused {
			name = "paused"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			const matchedHash, newHash = "matchedhash", "newhash"
			const releaseName = "Example.Series.S01.1080p.WEB-DL-GROUP"
			files := qbt.TorrentFiles{{Name: releaseName + "/episode.mkv", Size: 1024}}
			savePath := filepath.Join(t.TempDir(), "existing")
			sync := &rootlessSavePathSyncManager{
				files: map[string]qbt.TorrentFiles{matchedHash: files, newHash: files},
				props: map[string]*qbt.TorrentProperties{matchedHash: {SavePath: savePath}},
			}
			service := &Service{
				syncManager: sync,
				instanceStore: &rootlessSavePathInstanceStore{instances: map[int]*models.Instance{
					1: {ID: 1},
				}},
				releaseCache:     NewReleaseCache(),
				stringNormalizer: stringutils.NewDefaultNormalizer(),
				automationSettingsLoader: func(context.Context) (*models.CrossSeedAutomationSettings, error) {
					return models.DefaultCrossSeedAutomationSettings(), nil
				},
			}
			service.adder = NewAdder(sync, nil, nil, service.ResumeBudgetBytes, service.RunExternalProgram)
			service.aligner = NewAligner(sync)
			candidate := CrossSeedCandidate{InstanceID: 1, InstanceName: "Test", Torrents: []qbt.Torrent{{
				Hash: matchedHash, Name: releaseName, Progress: 1, Size: 1024,
				ContentPath: filepath.Join(savePath, releaseName),
			}}}
			result := service.processCrossSeedCandidate(context.Background(), candidate, []byte("torrent"),
				newHash, "", releaseName, &CrossSeedRequest{StartPaused: &paused},
				service.releaseCache.Parse(releaseName), files, nil, nil)
			require.True(t, result.Success, result.Message)
			require.Equal(t, "false", sync.addedOptions["autoTMM"])
			require.Equal(t, savePath, sync.addedOptions["savepath"])
			// qBittorrent 5.3 defaults an omitted per-torrent override to the
			// global incomplete-directory policy, even with an explicit savepath.
			require.Equal(t, "false", sync.addedOptions["useDownloadPath"])
		})
	}
}
