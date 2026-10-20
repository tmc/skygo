//go:build !linux && !darwin

package server

import (
	"os"

	"github.com/tmc/skygo/internal/artifact"
)

func artifactInitializerSupported() bool { return false }
func artifactInitializerIdentity(os.FileInfo) (artifact.FileID, error) {
	return artifact.FileID{}, os.ErrInvalid
}
