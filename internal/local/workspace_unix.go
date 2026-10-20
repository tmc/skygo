//go:build linux || darwin

package local

import (
	"errors"
	"os"
	"syscall"

	"github.com/tmc/skygo/internal/artifact"
	"golang.org/x/sys/unix"
)

func workspaceFileID(info os.FileInfo) (artifact.FileID, error) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Dev == 0 || st.Ino == 0 || st.Uid != uint32(os.Geteuid()) || !info.IsDir() || info.Mode().Perm()&0077 != 0 || info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
		return artifact.FileID{}, errors.New("workspace root identity unavailable")
	}
	return artifact.FileID{Device: uint64(st.Dev), Inode: uint64(st.Ino)}, nil
}
func openWorkspaceDirectory(root *os.Root) (*os.File, error) {
	return root.OpenFile(".", os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_DIRECTORY, 0)
}

// Cleanup never opens a non-directory entry. Symlinks are unlinked themselves;
// arbitrary modes are not treated as authority to follow them or read bytes.
func workspaceCleanupEntryID(info os.FileInfo) (artifact.FileID, error) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Dev == 0 || st.Ino == 0 || st.Uid != uint32(os.Geteuid()) {
		return artifact.FileID{}, errors.New("workspace entry identity unavailable")
	}
	return artifact.FileID{Device: uint64(st.Dev), Inode: uint64(st.Ino)}, nil
}
