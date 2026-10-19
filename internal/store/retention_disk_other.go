//go:build !linux && !darwin

package store

import "errors"

func filesystemFree(string) (uint64, error) {
	return 0, errors.New("filesystem measurement unavailable")
}
func outputVolume(string, map[string]uint64) (uint64, uint64, map[string]uint64, error) {
	return 0, 0, nil, errors.New("output volume measurement unavailable")
}
