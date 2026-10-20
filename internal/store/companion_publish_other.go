//go:build !linux && !darwin

package store

import (
	"errors"
	"os"
)

func companionRename(parent *os.File, old, new string) error {
	return errors.New("companion publication unsupported on this platform")
}

func companionMkdir(parent *os.File, name string, mode os.FileMode) error {
	return errors.New("companion directory creation unsupported on this platform")
}

func companionDirectoryID(parent *os.File, name string) (ArtifactFile, error) {
	return ArtifactFile{}, errors.New("companion directory identity unsupported on this platform")
}

func companionStat(info os.FileInfo) (uid uint32, links uint64, id ArtifactFile, err error) {
	return 0, 0, ArtifactFile{}, errors.New("companion file identity unsupported on this platform")
}

func companionOpen(parent *os.File, name string, flags int, mode os.FileMode) (*os.File, error) {
	return nil, errors.New("companion file open unsupported on this platform")
}
