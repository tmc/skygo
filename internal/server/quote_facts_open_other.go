//go:build !linux && !darwin

package server

import (
	"errors"
	"os"
)

func openTargetFacts(path string) (*os.File, error) {
	return nil, errors.New("target facts files are unsupported on this platform")
}
