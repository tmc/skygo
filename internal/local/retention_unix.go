//go:build linux || darwin

package local

import (
	"errors"
	"golang.org/x/sys/unix"
	"math"
	"os"
	"syscall"
)

func outputFilesystem(directory string) (uint64, uint64, error) {
	var s unix.Statfs_t
	if err := unix.Statfs(directory, &s); err != nil {
		return 0, 0, err
	}
	if s.Bsize <= 0 || s.Bsize > 65536 {
		return 0, 0, errors.New("unsupported output allocation unit")
	}
	unit := uint64(4096)
	for unit < uint64(s.Bsize) {
		unit *= 2
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return 0, 0, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, errors.New("output volume identity unavailable")
	}
	return unit, uint64(st.Dev), nil
}
func allocatedOutput(info os.FileInfo) (uint64, error) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Blocks < 0 || uint64(st.Blocks) > math.MaxUint64/512 {
		return 0, errors.New("output allocation unavailable")
	}
	return uint64(st.Blocks) * 512, nil
}

// Nonblocking plus no-follow prevents an unexpected FIFO/symlink from acquiring
// a read owner before the caller validates the exact original regular inode.
func openRetainedOutput(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	if f == nil {
		unix.Close(fd)
		return nil, errors.New("open output handle unavailable")
	}
	return f, nil
}
