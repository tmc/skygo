//go:build linux

package local

import (
	"errors"
	"os/exec"

	"golang.org/x/sys/unix"
)

func waitDefaultTerminal(cmd *exec.Cmd) error { return waitUnreaped(cmd) }
func openDefaultHandle(cmd *exec.Cmd) (defaultHandle, error) {
	fd, err := unix.PidfdOpen(cmd.Process.Pid, 0)
	if err != nil {
		return defaultHandle{fd: -1}, err
	}
	return defaultHandle{fd: fd}, nil
}
func (h *defaultHandle) signal() error {
	if h.fd < 0 {
		return errors.New("original process handle is unavailable")
	}
	err := unix.PidfdSendSignal(h.fd, unix.SIGKILL, nil, 0)
	if errors.Is(err, unix.ESRCH) {
		return nil
	}
	return err
}
func (h *defaultHandle) close() error {
	if h.fd < 0 {
		return nil
	}
	fd := h.fd
	h.fd = -1
	return unix.Close(fd)
}
