//go:build linux || darwin

package store

import (
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// openExistingFile opens an existing nonempty regular database without
// creating it or following a leaf symbolic link, and takes bbolt's own flock
// on that descriptor: shared, or exclusive for a writer. Contention and
// refusal are settled here, while this function still owns the descriptor;
// bbolt's later flock of the same open file description succeeds at once.
// The descriptor is checked again once the lock is held, so the file handed
// to bbolt is the one checked under the lock. On failure the descriptor is
// closed once.
func openExistingFile(path string, writer bool, wait time.Duration) (*os.File, os.FileInfo, error) {
	flag, how := unix.O_RDONLY, unix.LOCK_SH
	if writer {
		flag, how = unix.O_RDWR, unix.LOCK_EX
	}
	fd, err := unix.Open(path, flag|unix.O_NONBLOCK|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("open existing job database: %w", err)
	}
	f := os.NewFile(uintptr(fd), path)
	info, err := existingRegular(f)
	if err == nil {
		err = lockExistingFile(f, how, wait)
	}
	if err == nil && writer && existingAcquiredHook != nil {
		existingAcquiredHook()
	}
	// The file may have been truncated while lockExistingFile waited, and
	// bbolt initializes a zero-byte file before anything validates it.
	if err == nil {
		info, err = existingRegular(f)
	}
	if err != nil {
		if closeErr := f.Close(); closeErr != nil {
			return nil, nil, &closeError{file: f, err: errors.Join(err, closeErr)}
		}
		return nil, nil, err
	}
	return f, info, nil
}

// existingRegular stats the open descriptor f and refuses anything but a
// regular file at least as long as bbolt's meta pages. As for physical
// inspection, a shorter database is not one bbolt wrote; a zero-byte file is
// one bbolt would initialize.
func existingRegular(f *os.File) (os.FileInfo, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() < 2048 {
		return nil, fmt.Errorf("%w: job database is not an existing regular database", ErrNotCurrent)
	}
	return info, nil
}

func lockExistingFile(f *os.File, how int, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	for {
		err := unix.Flock(int(f.Fd()), how|unix.LOCK_NB)
		if err == nil {
			return nil
		}
		if err != unix.EWOULDBLOCK || !time.Now().Before(deadline) {
			return fmt.Errorf("lock job database: %w", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
