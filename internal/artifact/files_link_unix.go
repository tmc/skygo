//go:build linux || darwin

package artifact

import (
	"errors"
	"golang.org/x/sys/unix"
	"os"
)

func linkFile(dir int, part, object string) error {
	return unix.Linkat(dir, part, dir, object, 0)
}

func filesystemUnit(directory *os.File) (uint64, error) {
	var stat unix.Statfs_t
	if err := unix.Fstatfs(int(directory.Fd()), &stat); err != nil {
		return 0, err
	}
	if stat.Bsize < 512 || stat.Bsize > 65536 {
		return 0, errors.New("unsupported artifact allocation unit")
	}
	unit := uint64(stat.Bsize)
	if unit&(unit-1) != 0 {
		return 0, errors.New("unsupported artifact allocation unit")
	}
	return unit, nil
}
