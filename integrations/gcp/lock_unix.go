//go:build unix

package gce

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"syscall"
)

const openNoFollow = syscall.O_NOFOLLOW | syscall.O_NONBLOCK

var errBusy = errors.New("state directory is locked by another machine")

// lockFile takes an exclusive advisory lock on f without waiting. The lock
// belongs to f's open file description and ends when f is closed.
func lockFile(f *os.File) error {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return errBusy
	}
	if err != nil {
		return fmt.Errorf("lock state: %w", err)
	}
	return nil
}

// checkPrivate reports whether fi, describing name, is a directory (if dir)
// or a single-link regular file owned by the effective user with owner
// read/write access and no group or other access.
func checkPrivate(name string, fi fs.FileInfo, dir bool) error {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("%s: no owner information", name)
	}
	perm := fi.Mode().Perm()
	switch {
	case dir && !fi.IsDir():
		return fmt.Errorf("%s is not a directory", name)
	case !dir && !fi.Mode().IsRegular():
		return fmt.Errorf("%s is not a regular file", name)
	case int(st.Uid) != os.Geteuid():
		return fmt.Errorf("%s is owned by another user", name)
	case perm&0o077 != 0:
		return fmt.Errorf("%s has mode %v; group and other must have no access", name, perm)
	case dir && perm&0o700 != 0o700, !dir && perm&0o600 != 0o600:
		return fmt.Errorf("%s has mode %v; the owner needs full access", name, perm)
	case !dir && st.Nlink != 1:
		return fmt.Errorf("%s has %d links, want 1", name, st.Nlink)
	}
	return nil
}

// syncDir flushes the entries of directory d to stable storage.
func syncDir(d *os.File) error {
	return syscall.Fsync(int(d.Fd()))
}
