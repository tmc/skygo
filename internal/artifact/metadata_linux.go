//go:build linux

package artifact

import (
	"errors"
	"os"
	"syscall"
)

func fileMetadata(info os.FileInfo) (metadata, error) {
	s, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return metadata{}, errors.New("artifact inode metadata unavailable")
	}
	return metadata{inode: inode{uint64(s.Dev), uint64(s.Ino)}, uid: s.Uid, links: uint64(s.Nlink), changeSec: int64(s.Ctim.Sec), changeNsec: int64(s.Ctim.Nsec)}, nil
}
func nonblockingRead() (int, error) { return os.O_RDONLY | syscall.O_NONBLOCK, nil }
