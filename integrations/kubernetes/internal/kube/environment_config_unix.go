//go:build linux || darwin

package kube

import (
	"errors"
	"os"
	"syscall"
)

// environmentConfigFileIdentity checks that f is a private profile file of
// the server's user and returns its identity.
func environmentConfigFileIdentity(f *os.File) (qualificationIdentity, error) {
	info, err := f.Stat()
	if err != nil {
		return qualificationIdentity{}, errors.New("stat environment profiles")
	}
	s, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || s.Nlink != 1 || s.Uid != uint32(os.Geteuid()) ||
		info.Mode() != 0o400 && info.Mode() != 0o600 || info.Size() <= 0 || info.Size() > maxEnvironmentConfigBytes {
		return qualificationIdentity{}, errors.New("unsafe environment profiles file")
	}
	return qualificationIdentity{device: uint64(s.Dev), inode: uint64(s.Ino), size: info.Size(), modified: info.ModTime(), mode: info.Mode(), uid: s.Uid}, nil
}
