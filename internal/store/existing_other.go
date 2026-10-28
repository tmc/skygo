//go:build !linux && !darwin

package store

import (
	"errors"
	"os"
	"time"
)

func openExistingFile(string, bool, time.Duration) (*os.File, os.FileInfo, error) {
	return nil, nil, errors.New("existing installation unsupported on this platform")
}
