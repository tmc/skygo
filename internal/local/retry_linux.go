//go:build linux

package local

import (
	"errors"
	"os"
	"os/exec"
	"syscall"

	"golang.org/x/sys/unix"
)

func retrySupported() bool {
	fd, err := unix.PidfdOpen(os.Getpid(), 0)
	if err != nil {
		return false
	}
	defer unix.Close(fd)
	return unix.PidfdSendSignal(fd, 0, nil, 0) == nil
}

func waitUnreaped(cmd *exec.Cmd) error {
	for {
		var info unix.Siginfo
		err := unix.Waitid(unix.P_PID, cmd.Process.Pid, &info, unix.WEXITED|unix.WNOWAIT, nil)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		// Linux CLD_EXITED, CLD_KILLED and CLD_DUMPED. No stop/continue event,
		// WNOHANG result or opaque zeroed siginfo may authorize reaping cleanup.
		if info.Code < 1 || info.Code > 3 {
			return errors.New("wait returned a nonterminal event")
		}
		return nil
	}
}

func groupAbsent(cmd *exec.Cmd) error {
	err := unix.Kill(-cmd.Process.Pid, 0)
	if errors.Is(err, unix.ESRCH) {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.New("process group still exists")
}

// CLONE_PIDFD supplies this descriptor at the original fork. Go duplicates it
// for os.Process, so releasing ours never closes or aliases the reaper's handle.
// Missing fork handles refuse Start and use bounded handle-only cleanup;
// no numeric-PID signal fallback is permitted.
func configureRetry(cmd *exec.Cmd) {
	fd := new(int)
	*fd = -1
	cmd.SysProcAttr.PidFD = fd
}

func abortRetry(cmd *exec.Cmd) error {
	if !retryHandleReady(cmd) {
		return errors.New("original process handle is unavailable")
	}
	fd := *cmd.SysProcAttr.PidFD
	err := unix.PidfdSendSignal(fd, syscall.SIGKILL, nil, 0)
	if errors.Is(err, unix.ESRCH) {
		return nil
	}
	return err
}

func releaseRetry(cmd *exec.Cmd) {
	fd := cmd.SysProcAttr.PidFD
	if fd != nil && *fd >= 0 {
		unix.Close(*fd)
		*fd = -1
	}
}

func retryHandleReady(cmd *exec.Cmd) bool {
	return cmd.SysProcAttr != nil && cmd.SysProcAttr.PidFD != nil && *cmd.SysProcAttr.PidFD >= 0
}

// This is called only immediately after Start and before any waiter/reaper.
// Exclusive ownership of the unreaped original child still reserves its PID.
func recoverRetryHandle(cmd *exec.Cmd) error {
	fd, err := unix.PidfdOpen(cmd.Process.Pid, 0)
	if err != nil {
		return err
	}
	cmd.SysProcAttr.PidFD = &fd
	return nil
}
