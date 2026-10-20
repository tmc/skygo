//go:build linux || darwin

package artifact

import (
	"errors"
	"math"
	"os"

	"golang.org/x/sys/unix"
)

func physicalDirectory(o physicalObservation) bool { return o.mode&unix.S_IFMT == unix.S_IFDIR }

func physicalEntryType(o physicalObservation) PhysicalEntryType {
	switch o.mode & unix.S_IFMT {
	case unix.S_IFREG:
		return PhysicalEntryRegular
	case unix.S_IFDIR:
		return PhysicalEntryDirectory
	default:
		return PhysicalEntryOther
	}
}

func openPhysicalDirectory(parent *os.File, name string) (*os.File, error) {
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}
func openPhysicalMarker(parent *os.File, name string) (*os.File, error) {
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}
func physicalAt(parent *os.File, name string) (physicalObservation, error) {
	var stat unix.Stat_t
	if err := unix.Fstatat(int(parent.Fd()), name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return physicalObservation{}, err
	}
	return physicalStat(&stat)
}
func physicalDescriptor(file *os.File) (physicalObservation, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return physicalObservation{}, err
	}
	return physicalStat(&stat)
}
func physicalFree(file *os.File) (uint64, error) {
	var stat unix.Statfs_t
	if err := unix.Fstatfs(int(file.Fd()), &stat); err != nil {
		return 0, err
	}
	if stat.Bsize < 512 || stat.Bsize > 65536 {
		return 0, errors.New("unsupported artifact allocation unit")
	}
	unit := uint64(stat.Bsize)
	if unit&(unit-1) != 0 || uint64(stat.Bavail) > math.MaxUint64/unit {
		return 0, errors.New("artifact physical free-space overflow")
	}
	return uint64(stat.Bavail) * unit, nil
}
