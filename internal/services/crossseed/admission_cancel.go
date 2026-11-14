package crossseed

import (
	"os"
	"path/filepath"
)

func cancelledAdmissionPath(path string) string {
	return filepath.Join(filepath.Dir(path), ".cancelled", filepath.Base(path))
}

// Archived requests fence checkpoints that were read before cancellation.
func admissionCancellationError(path string) error {
	_, err := os.Lstat(cancelledAdmissionPath(path))
	if os.IsNotExist(err) {
		return nil
	}
	if err == nil {
		return os.ErrNotExist
	}
	return err
}

func publishAdmission(tempPath, finalPath string) error {
	if err := os.Rename(tempPath, finalPath); err != nil {
		return err
	}
	if err := admissionCancellationError(finalPath); err != nil {
		if os.IsNotExist(err) {
			// Cancellation can win between a writer's initial check and rename.
			_ = os.Remove(finalPath)
		}
		return err
	}
	return nil
}
