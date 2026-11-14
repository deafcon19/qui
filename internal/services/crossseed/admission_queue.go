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
	"time"

	"github.com/rs/zerolog/log"
)

type admissionPending struct {
	CrossSeed            *CrossSeedRequest
	SeasonPack           *SeasonPackApplyRequest
	SeasonPackAutonomous bool
}

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
	finalPath := filepath.Join(directory, hex.EncodeToString(digest[:])+".gob")
	if _, err := os.Stat(finalPath); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	temp, err := os.CreateTemp(directory, ".pending-*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	if _, err := temp.Write(data.Bytes()); err != nil {
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
	return os.Rename(temp.Name(), finalPath)
}

func (s *Service) admissionQueueWorker(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		s.drainAdmissionQueue(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) drainAdmissionQueue(ctx context.Context) {
	directory, err := admissionQueueDir()
	if err != nil {
		log.Error().Err(err).Msg("Cannot locate deferred cross-seed queue")
		return
	}
	entries, err := os.ReadDir(directory)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		log.Error().Err(err).Msg("Cannot read deferred cross-seed queue")
		return
	}
	for _, entry := range entries {
		if ctx.Err() != nil {
			return
		}
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".gob" {
			continue
		}
		path := filepath.Join(directory, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			log.Error().Err(err).Str("file", entry.Name()).Msg("Cannot read deferred cross-seed")
			continue
		}
		var item admissionPending
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&item); err != nil {
			log.Error().Err(err).Str("file", entry.Name()).Msg("Cannot decode deferred cross-seed")
			continue
		}
		attemptCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		settled := false
		if item.CrossSeed != nil {
			response, applyErr := s.CrossSeed(attemptCtx, item.CrossSeed)
			settled = applyErr == nil && response != nil && !crossSeedNeedsRetry(response)
			if applyErr != nil {
				log.Warn().Err(applyErr).Str("file", entry.Name()).Msg("Deferred cross-seed retry failed")
			}
		} else if item.SeasonPack != nil {
			item.SeasonPack.autonomous = item.SeasonPackAutonomous
			response, applyErr := s.ApplySeasonPackWebhook(attemptCtx, item.SeasonPack)
			settled = applyErr == nil && response != nil && response.Reason != "queued"
			if applyErr != nil {
				log.Warn().Err(applyErr).Str("file", entry.Name()).Msg("Deferred season-pack retry failed")
			}
		}
		cancel()
		if settled {
			if err := os.Remove(path); err != nil {
				log.Error().Err(err).Str("file", entry.Name()).Msg("Cannot remove settled cross-seed request")
			}
		}
	}
}

func crossSeedNeedsRetry(response *CrossSeedResponse) bool {
	for _, result := range response.Results {
		switch result.Status {
		case "queued", "error", "hardlink_error", "reflink_error":
			return true
		default:
		}
	}
	return false
}
