//go:build darwin

package local

import (
	"errors"
	"fmt"
	"os/exec"
	"runtime"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// The public Darwin LP64 siginfo_t layout agrees with Go's amd64 and arm64
// runtime definitions. Scalar storage cannot introduce pointers during copyout.
type defaultSiginfo struct {
	signo, errno, code, pid int32
	uid                     uint32
	status                  int32
	addr, value             uint64
	band                    int64
	reserved                [7]uint64
}

var (
	_ [104 - unsafe.Sizeof(defaultSiginfo{})]byte
	_ [unsafe.Sizeof(defaultSiginfo{}) - 104]byte
	_ [8 - unsafe.Alignof(defaultSiginfo{})]byte
	_ [unsafe.Alignof(defaultSiginfo{}) - 8]byte
	_ [0 - unsafe.Offsetof(defaultSiginfo{}.signo)]byte
	_ [unsafe.Offsetof(defaultSiginfo{}.signo) - 0]byte
	_ [4 - unsafe.Offsetof(defaultSiginfo{}.errno)]byte
	_ [unsafe.Offsetof(defaultSiginfo{}.errno) - 4]byte
	_ [8 - unsafe.Offsetof(defaultSiginfo{}.code)]byte
	_ [unsafe.Offsetof(defaultSiginfo{}.code) - 8]byte
	_ [12 - unsafe.Offsetof(defaultSiginfo{}.pid)]byte
	_ [unsafe.Offsetof(defaultSiginfo{}.pid) - 12]byte
)

func defaultTerminalRecord(info defaultSiginfo, pid int) (bool, error) {
	if info.signo != int32(unix.SIGCHLD) || info.errno != 0 || info.pid != int32(pid) {
		return false, fmt.Errorf("unexpected waitid record: signo=%d errno=%d code=%d pid=%d", info.signo, info.errno, info.code, info.pid)
	}
	switch info.code {
	case 1, 2, 3:
		return true, nil // CLD_EXITED, CLD_KILLED, CLD_DUMPED
	case 4, 5, 6:
		return false, nil // CLD_TRAPPED, CLD_STOPPED, CLD_CONTINUED
	default:
		return false, fmt.Errorf("unsupported waitid child code %d", info.code)
	}
}
func waitDefaultTerminal(cmd *exec.Cmd) error {
	pid := cmd.Process.Pid
	if pid <= 0 || uint64(pid) > 1<<31-1 {
		return errors.New("invalid owned child pid")
	}
	for {
		var info defaultSiginfo
		_, _, errno := unix.Syscall6(unix.SYS_WAITID, 1, uintptr(pid), uintptr(unsafe.Pointer(&info)), unix.WEXITED|unix.WNOWAIT, 0, 0)
		runtime.KeepAlive(&info)
		if errno == unix.EINTR {
			continue
		}
		if errno != 0 {
			return errno
		}
		terminal, err := defaultTerminalRecord(info, pid)
		if err != nil || terminal {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
}
func openDefaultHandle(*exec.Cmd) (defaultHandle, error) { return defaultHandle{fd: -1}, nil }
func (*defaultHandle) signal() error {
	return errors.New("original process handles are unavailable on Darwin")
}
func (*defaultHandle) close() error { return nil }
