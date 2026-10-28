//go:build darwin

package remotehelper

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func renameNoReplace(fd int, from, to string) error {
	return unix.RenameatxNp(fd, from, fd, to, unix.RENAME_EXCL)
}

func lockShared(f *os.File) error { return unix.Flock(int(f.Fd()), unix.LOCK_SH) }

func unlockFile(f *os.File) error { return unix.Flock(int(f.Fd()), unix.LOCK_UN) }

// tryLockExclusive reports whether it took f's lock exclusively without
// waiting.
func tryLockExclusive(f *os.File) (bool, error) {
	err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) {
		return false, nil
	}
	return err == nil, err
}
