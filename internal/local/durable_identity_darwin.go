//go:build darwin

package local

import (
	"errors"
	"fmt"
	"unsafe"

	"golang.org/x/sys/unix"
)

func bootIdentity() (string, error) {
	tv, err := unix.SysctlTimeval("kern.boottime")
	if err != nil {
		return "", fmt.Errorf("read boot time: %w", err)
	}
	if tv.Sec <= 0 {
		return "", errors.New("malformed boot time")
	}
	return fmt.Sprintf("boottime:%d.%06d", tv.Sec, tv.Usec), nil
}

// processStart returns p_starttime in microseconds since the epoch.
// Zombies have exited and are reported as gone.
func processStart(pid int) (uint64, error) {
	buf, err := unix.SysctlRaw("kern.proc.pid", pid)
	if err != nil {
		return 0, err
	}
	if len(buf) == 0 {
		return 0, errProcessGone
	}
	if len(buf) != unix.SizeofKinfoProc {
		return 0, errors.New("malformed process info")
	}
	p := (*unix.KinfoProc)(unsafe.Pointer(&buf[0]))
	if int(p.Proc.P_pid) != pid {
		return 0, errors.New("process info names another pid")
	}
	const zombie = 5 // SZOMB
	if p.Proc.P_stat == zombie {
		return 0, errProcessGone
	}
	start := uint64(p.Proc.P_starttime.Sec)*1e6 + uint64(p.Proc.P_starttime.Usec)
	if p.Proc.P_starttime.Sec <= 0 || start == 0 {
		return 0, errors.New("malformed process start time")
	}
	return start, nil
}
