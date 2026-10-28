//go:build linux || darwin

package kube

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

// Every open is nonblocking and never follows a final symlink, so that a FIFO
// or device substituted for a state entry fails its type check instead of
// blocking the caller in open.
const (
	receiptFileFlags = unix.O_NOFOLLOW | unix.O_CLOEXEC | unix.O_NONBLOCK
	receiptDirFlags  = unix.O_RDONLY | unix.O_DIRECTORY | receiptFileFlags
)

// checkPrivate accepts a directory with mode 0700, or a singly linked regular
// file with mode 0600, owned by the effective user.
func checkPrivate(info os.FileInfo, dir bool) error {
	s, ok := info.Sys().(*syscall.Stat_t)
	if !ok || s.Uid != uint32(os.Geteuid()) {
		return errors.New("kubernetes execution state has the wrong owner")
	}
	if dir && info.Mode() != os.ModeDir|0o700 || !dir && (info.Mode() != 0o600 || s.Nlink != 1) {
		return errors.New("unsafe kubernetes execution state permissions")
	}
	return nil
}

// privateID checks info as checkPrivate does and returns its identity.
func privateID(info os.FileInfo, dir bool) (fileID, error) {
	if err := checkPrivate(info, dir); err != nil {
		return fileID{}, err
	}
	s := info.Sys().(*syscall.Stat_t)
	return fileID{dev: uint64(s.Dev), ino: s.Ino}, nil
}

func checkPrivateDir(name string) error {
	_, err := dirID(name)
	return err
}

// dirID returns the identity of the private directory name, which must not be
// a symlink.
func dirID(name string) (fileID, error) {
	info, err := os.Lstat(name)
	if err != nil {
		return fileID{}, err
	}
	return privateID(info, true)
}

// openScope opens the scope directory through the root directory without
// following symlinks and checks that both are still the private directories
// recorded when the store was opened. Operations on the returned directory
// are anchored to it rather than to its path.
func openScope(root string, rootID fileID, scope string, scopeID fileID) (*os.File, error) {
	rfd, err := unix.Open(root, receiptDirFlags, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: root, Err: err}
	}
	rf := os.NewFile(uintptr(rfd), root)
	defer rf.Close()
	if err := sameDir(rf, rootID); err != nil {
		return nil, err
	}
	name := filepath.Join(root, scope)
	sfd, err := unix.Openat(rfd, scope, receiptDirFlags, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	sf := os.NewFile(uintptr(sfd), name)
	if err := sameDir(sf, scopeID); err != nil {
		sf.Close()
		return nil, err
	}
	return sf, nil
}

func sameDir(f *os.File, want fileID) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	id, err := privateID(info, true)
	if err != nil {
		return err
	}
	if id != want {
		return errors.New("kubernetes execution state directory was replaced")
	}
	return nil
}

// openAt opens the entry name of dir.
func openAt(dir *os.File, name string, flag int) (*os.File, error) {
	fd, err := unix.Openat(int(dir.Fd()), name, flag|receiptFileFlags, 0o600)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: filepath.Join(dir.Name(), name), Err: err}
	}
	return os.NewFile(uintptr(fd), filepath.Join(dir.Name(), name)), nil
}

func removeAt(dir *os.File, name string) error {
	if err := unix.Unlinkat(int(dir.Fd()), name, 0); err != nil {
		return &os.PathError{Op: "remove", Path: filepath.Join(dir.Name(), name), Err: err}
	}
	return nil
}

func renameAt(dir *os.File, from, to string) error {
	fd := int(dir.Fd())
	if err := unix.Renameat(fd, from, fd, to); err != nil {
		return &os.LinkError{Op: "rename", Old: filepath.Join(dir.Name(), from), New: filepath.Join(dir.Name(), to), Err: err}
	}
	return nil
}

// syncAt flushes the directory entries of dir.
func syncAt(dir *os.File) error {
	return unix.Fsync(int(dir.Fd()))
}

// readDirAt lists dir from a fresh descriptor of the same directory.
func readDirAt(dir *os.File) ([]fs.DirEntry, error) {
	fd, err := unix.Openat(int(dir.Fd()), ".", receiptDirFlags, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: dir.Name(), Err: err}
	}
	f := os.NewFile(uintptr(fd), dir.Name())
	entries, err := f.ReadDir(-1)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return entries, err
}

// flock locks f, shared or exclusive. Without wait it reports false instead
// of waiting for a conflicting holder.
func flock(f *os.File, exclusive, wait bool) (bool, error) {
	how := unix.LOCK_SH
	if exclusive {
		how = unix.LOCK_EX
	}
	if !wait {
		how |= unix.LOCK_NB
	}
	for {
		err := unix.Flock(int(f.Fd()), how)
		switch {
		case err == nil:
			return true, nil
		case err == unix.EINTR:
			continue
		case !wait && err == unix.EWOULDBLOCK:
			return false, nil
		}
		return false, err
	}
}

// makeScope creates the scope directory in root if it is absent and then,
// whether it created the entry or found it, syncs root, so that no store is
// accepted before the scope's entry in root was confirmed durable by this
// call. Both use one descriptor of root checked against rootID; beforeSync
// runs between them. It returns the identity of the private scope directory.
func makeScope(root string, rootID fileID, scope string, beforeSync func() error) (id fileID, err error) {
	rfd, err := unix.Open(root, receiptDirFlags, 0)
	if err != nil {
		return fileID{}, &os.PathError{Op: "open", Path: root, Err: err}
	}
	rf := os.NewFile(uintptr(rfd), root)
	defer func() {
		if cerr := rf.Close(); err == nil && cerr != nil {
			id, err = fileID{}, cerr
		}
	}()
	if err := sameDir(rf, rootID); err != nil {
		return fileID{}, err
	}
	name := filepath.Join(root, scope)
	if err := unix.Mkdirat(rfd, scope, 0o700); err != nil && err != unix.EEXIST {
		return fileID{}, &os.PathError{Op: "mkdir", Path: name, Err: err}
	}
	if err := beforeSync(); err != nil {
		return fileID{}, err
	}
	if err := unix.Fsync(rfd); err != nil {
		return fileID{}, &os.PathError{Op: "sync", Path: root, Err: err}
	}
	sfd, err := unix.Openat(rfd, scope, receiptDirFlags, 0)
	if err != nil {
		return fileID{}, &os.PathError{Op: "open", Path: name, Err: err}
	}
	sf := os.NewFile(uintptr(sfd), name)
	info, err := sf.Stat()
	if cerr := sf.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fileID{}, err
	}
	return privateID(info, true)
}
