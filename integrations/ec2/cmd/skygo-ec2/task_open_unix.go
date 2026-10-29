//go:build linux || darwin

package main

import (
	"os"
	"syscall"
)

// openTaskFile does not wait for a FIFO's writer. The caller checks the
// actual open file is regular before reading it, and owns its one Close.
func openTaskFile(name string) (*os.File, error) {
	return os.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK, 0)
}
