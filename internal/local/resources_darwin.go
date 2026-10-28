package local

import (
	"errors"

	"golang.org/x/sys/unix"
)

// hostMemory reports hw.memsize, the installed physical memory.
func hostMemory() (int64, error) {
	n, err := unix.SysctlUint64("hw.memsize")
	if err != nil {
		return 0, err
	}
	if n == 0 || n > 1<<62 {
		return 0, errors.New("invalid hw.memsize")
	}
	return int64(n), nil
}
