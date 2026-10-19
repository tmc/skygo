//go:build linux || darwin

package local

import (
	"errors"
	"os/exec"
	"syscall"
)

func supported() bool         { return true }
func configure(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} }
func kill(cmd *exec.Cmd) error {
	err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}

// An existence probe after reaping cannot authorize a signal to that group.
func defaultGroupAbsent(cmd *exec.Cmd) error {
	err := syscall.Kill(-cmd.Process.Pid, 0)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.New("process group still exists")
}
