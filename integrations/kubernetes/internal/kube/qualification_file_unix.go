//go:build linux || darwin

package kube

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// Only the final component is protected by O_NOFOLLOW. Ancestors and namespace
// mutation by the trusted operator remain the private configuration premise.
func openQualificationFile(name string) (*os.File, error) {
	fd, err := unix.Open(name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

func qualificationFileIdentity(f *os.File) (qualificationIdentity, error) {
	info, err := f.Stat()
	if err != nil {
		return qualificationIdentity{}, fmt.Errorf("stat kubernetes qualification file: %w", err)
	}
	s, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || s.Nlink != 1 || s.Uid != uint32(os.Geteuid()) || info.Mode() != 0o400 && info.Mode() != 0o600 || info.Size() <= 0 || info.Size() > maxQualificationRecord {
		return qualificationIdentity{}, errors.New("unsafe kubernetes qualification file")
	}
	return qualificationIdentity{device: uint64(s.Dev), inode: uint64(s.Ino), size: info.Size(), modified: info.ModTime(), mode: info.Mode(), uid: s.Uid}, nil
}
