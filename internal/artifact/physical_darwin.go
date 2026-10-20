//go:build darwin

package artifact

import "golang.org/x/sys/unix"

func physicalStat(s *unix.Stat_t) (physicalObservation, error) {
	if s.Blocks < 0 || s.Size < 0 {
		return physicalObservation{}, ErrFileConflict
	}
	return physicalObservation{id: FileID{Device: uint64(s.Dev), Inode: uint64(s.Ino)}, blocks: uint64(s.Blocks), size: s.Size, mode: uint32(s.Mode), uid: s.Uid, links: uint64(s.Nlink), modifiedSec: int64(s.Mtim.Sec), modifiedNsec: int64(s.Mtim.Nsec), changedSec: int64(s.Ctim.Sec), changedNsec: int64(s.Ctim.Nsec)}, nil
}
