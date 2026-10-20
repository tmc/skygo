//go:build darwin

package store

import (
	"errors"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func companionRename(parent *os.File, old, new string) error {
	return unix.RenameatxNp(int(parent.Fd()), old, int(parent.Fd()), new, unix.RENAME_EXCL)
}

func companionMkdir(parent *os.File, name string, mode os.FileMode) error {
	return unix.Mkdirat(int(parent.Fd()), name, uint32(mode.Perm()))
}

func companionDirectoryID(parent *os.File, name string) (ArtifactFile, error) {
	var st unix.Stat_t
	if err := unix.Fstatat(int(parent.Fd()), name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return ArtifactFile{}, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFDIR || st.Uid != uint32(os.Geteuid()) || st.Mode&07777 != 0700 {
		return ArtifactFile{}, errors.New("companion directory is not private and owned")
	}
	return ArtifactFile{Device: uint64(st.Dev), Inode: uint64(st.Ino)}, nil
}

func companionStat(info os.FileInfo) (uid uint32, links uint64, id ArtifactFile, err error) {
	if info == nil {
		return 0, 0, ArtifactFile{}, errors.New("companion file identity unavailable")
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, ArtifactFile{}, errors.New("companion file identity unavailable")
	}
	return st.Uid, uint64(st.Nlink), ArtifactFile{Device: uint64(st.Dev), Inode: uint64(st.Ino)}, nil
}

func companionOpen(parent *os.File, name string, flags int, mode os.FileMode) (*os.File, error) {
	dirfd := unix.AT_FDCWD
	if parent != nil {
		dirfd = int(parent.Fd())
	}
	fd, err := unix.Openat(dirfd, name, flags|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, uint32(mode.Perm()))
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), name)
	if f == nil {
		return nil, errors.Join(errors.New("companion file unavailable"), unix.Close(fd))
	}
	return f, nil
}
