//go:build !linux

package local

import (
	"errors"
	"os/exec"
)

func retrySupported() bool { return false }
func waitUnreaped(*exec.Cmd) error {
	return errors.New("nonreaping wait is not qualified on this platform")
}
func groupAbsent(*exec.Cmd) error {
	return errors.New("process group absence is not qualified on this platform")
}

func configureRetry(*exec.Cmd) {}
func abortRetry(*exec.Cmd) error {
	return errors.New("original process handles are not qualified on this platform")
}
func releaseRetry(*exec.Cmd) {}

func retryHandleReady(*exec.Cmd) bool { return false }
func recoverRetryHandle(*exec.Cmd) error {
	return errors.New("original process handles are not qualified on this platform")
}
