//go:build linux || darwin

package store

import (
	"errors"
	"golang.org/x/sys/unix"
	"math"
	"os"
	"syscall"
)

func filesystemFree(dir string) (uint64, error) {
	var s unix.Statfs_t
	if e := unix.Statfs(dir, &s); e != nil {
		return 0, e
	}
	if s.Bsize <= 0 || uint64(s.Bavail) > math.MaxUint64/uint64(s.Bsize) {
		return 0, errors.New("invalid filesystem free space")
	}
	return uint64(s.Bavail) * uint64(s.Bsize), nil
}
func outputVolume(dir string, files map[string]uint64) (uint64, uint64, map[string]uint64, error) {
	var s unix.Statfs_t
	if e := unix.Statfs(dir, &s); e != nil {
		return 0, 0, nil, e
	}
	if s.Bsize <= 0 || s.Bsize > 65536 {
		return 0, 0, nil, errors.New("unsupported output allocation unit")
	}
	a := uint64(4096)
	for a < uint64(s.Bsize) {
		a *= 2
	}
	i, e := os.Stat(dir)
	if e != nil {
		return 0, 0, nil, e
	}
	st, ok := i.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, nil, errors.New("output volume identity unavailable")
	}
	return a, uint64(st.Dev), files, nil
}
