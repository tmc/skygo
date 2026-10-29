//go:build darwin || linux || freebsd || openbsd || netbsd || dragonfly

package cli

import (
	"errors"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

const workspaceNoFollow = syscall.O_NOFOLLOW

func workspacePlatform() error { return nil }

func workspacePermissions(info os.FileInfo, private, directory bool) error {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("workspace file ownership unavailable")
	}
	uid := uint32(os.Getuid())
	if private {
		want := os.FileMode(0600)
		if directory {
			want = 0700
		}
		if st.Uid != uid || info.Mode().Perm() != want || info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
			return errors.New("workspace state must be private and owned by current user")
		}
	} else if (st.Uid != uid && st.Uid != 0) || info.Mode().Perm()&0022 != 0 {
		return errors.New("workspace state ancestor is insecure")
	}
	if !directory && st.Nlink != 1 {
		return errors.New("workspace state file has multiple links")
	}
	return nil
}

func lockWorkspace(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return errors.New("workspace state is locked")
	}
	return nil
}

func renameWorkspaceFile(directory *os.File, old, new string) error {
	return unix.Renameat(int(directory.Fd()), old, int(directory.Fd()), new)
}

func workspaceCacheOwner(info os.FileInfo) error {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != uint32(os.Getuid()) {
		return errors.New("workspace cache must be owned by current user")
	}
	return nil
}
