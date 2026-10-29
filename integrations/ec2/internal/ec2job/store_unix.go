//go:build darwin || linux

package ec2job

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"syscall"
)

const (
	openNoFollow  = syscall.O_NOFOLLOW
	openDirectory = syscall.O_DIRECTORY
	openNonblock  = syscall.O_NONBLOCK
)

// lockFile takes an exclusive advisory lock on f without waiting. The lock
// belongs to f's open file description and is released when f is closed,
// including when the process exits.
func lockFile(f *os.File) error {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return ErrBusy
	}
	if err != nil {
		return fmt.Errorf("lock job state: %w", err)
	}
	return nil
}

// checkPrivate reports whether fi, the description of name, is a directory
// (if dir is set) or a regular file that only the current user can use: it
// is owned by the effective user, grants no group or other permission and
// has owner read and write (and, for a directory, search) permission. A
// regular file must have exactly one link.
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

// syncDir flushes directory d's entries to stable storage.
func syncDir(d *os.File) error {
	return syscall.Fsync(int(d.Fd()))
}
