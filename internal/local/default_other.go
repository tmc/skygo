//go:build !linux && !darwin

package local

import (
	"errors"
	"os/exec"
)

func waitDefaultTerminal(*exec.Cmd) error                { return errors.New("unsupported operating system") }
func openDefaultHandle(*exec.Cmd) (defaultHandle, error) { return defaultHandle{fd: -1}, nil }
func (*defaultHandle) signal() error                     { return errors.New("unsupported operating system") }
func (*defaultHandle) close() error                      { return nil }
