//go:build linux || darwin

package server

import (
	"errors"
	"os"
	"syscall"
)

// Nonblocking open prevents a substituted FIFO from blocking before f.Stat.
// No-follow rejects a substituted symlink. Regular-file reads can still wait
// for the filesystem; this helper does not impose an I/O deadline.
func openTargetFacts(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, errors.New("read target facts")
	}
	return f, nil
}
