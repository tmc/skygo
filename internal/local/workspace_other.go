//go:build !linux && !darwin

package local

import (
	"errors"
	"os"

	"github.com/tmc/skygo/internal/artifact"
)

func workspaceFileID(os.FileInfo) (artifact.FileID, error) {
	return artifact.FileID{}, errors.New("workspace root identity unavailable")
}
func openWorkspaceDirectory(*os.Root) (*os.File, error) {
	return nil, errors.New("workspace root identity unavailable")
}

func workspaceCleanupEntryID(os.FileInfo) (artifact.FileID, error) {
	return artifact.FileID{}, errors.New("workspace entry identity unavailable")
}
