//go:build !linux && !darwin

package local

import (
	"errors"
	"os/exec"
)

func supported() bool          { return false }
func configure(cmd *exec.Cmd)  {}
func kill(cmd *exec.Cmd) error { return errors.New("unsupported operating system") }

func defaultGroupAbsent(*exec.Cmd) error { return errors.New("unsupported operating system") }
