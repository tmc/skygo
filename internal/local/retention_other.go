//go:build !linux && !darwin

package local

import (
	"errors"
	"os"
)

func outputFilesystem(string) (uint64, uint64, error) {
	return 0, 0, errors.New("local output measurement unavailable")
}
func allocatedOutput(os.FileInfo) (uint64, error) {
	return 0, errors.New("local output measurement unavailable")
}

func openRetainedOutput(string) (*os.File, error) {
	return nil, errors.New("local output read unavailable")
}
