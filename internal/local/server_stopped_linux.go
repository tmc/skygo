//go:build linux

package local

import (
	"os"

	"golang.org/x/sys/unix"
)

// ObserveServerStopped reports qualified PID absence in this active PID
// namespace. Both identities must be strict, nonzero and in the same trusted
// host, boot and namespace; current must equal this process's immutable admission
// identity. Self identity is requalified without replacing that admission value.
//
// The caller must separately qualify the kernel's pidfd lookup semantics and
// syscall visibility, including absence of mediation that synthesizes ESRCH.
// A true result does not prove descriptor nontransfer or cessation of inherited
// copies or independent HTTP/I/O use. Those are separate deployment premises.
//
// Only pidfd_open ESRCH returns true. An acquired pidfd is closed once and always
// returns false, including for reused PIDs and unreaped processes. Other failures
// remain unavailable. Original self-capture and query Close errors are returned
// intact and must remain retained; neither permits retry or settlement.
func ObserveServerStopped(current, original ServerLifetime) (bool, error) {
	if !stoppedLifetimePair(current, original) {
		return false, ErrServerLifetimeUnavailable
	}
	actual, err := CaptureServerLifetime()
	if err != nil {
		return false, err
	}
	if actual != current {
		return false, ErrServerLifetimeUnavailable
	}
	fd, err := unix.PidfdOpen(int(original.PID), 0)
	if err == unix.ESRCH {
		return true, nil
	}
	if err != nil {
		return false, lifetimeUnavailable(err)
	}
	return closeStoppedQuery(os.NewFile(uintptr(fd), "server pidfd"))
}
