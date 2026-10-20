//go:build linux || darwin

package server

import (
	"os"
	"syscall"

	"github.com/tmc/skygo/internal/artifact"
)

func artifactInitializerSupported() bool { return true }

func artifactInitializerIdentity(info os.FileInfo) (artifact.FileID, error) {
	physical, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() || info.Mode().Perm()&0077 != 0 || info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 || physical.Uid != uint32(os.Geteuid()) {
		return artifact.FileID{}, artifact.ErrFileConflict
	}
	id := artifact.FileID{Device: uint64(physical.Dev), Inode: uint64(physical.Ino)}
	if id.Device == 0 || id.Inode == 0 {
		return artifact.FileID{}, artifact.ErrFileConflict
	}
	return id, nil
}
