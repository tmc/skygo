package cli

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// sendRetryCustody runs in the workload. It opens a pidfd for its own process
// and passes that descriptor, with the fixture nonce, to the test that owns
// dir, on the socket whose address that test wrote there. It waits for the
// test's acknowledgement before returning.
func sendRetryCustody(dir string) (err error) {
	nonce, err := os.ReadFile(retryCustodyPath(dir, retryCustodyNonce))
	if err != nil {
		return err
	}
	addr, err := os.ReadFile(retryCustodyPath(dir, retryCustodyAddr))
	if err != nil {
		return err
	}
	fd, err := unix.PidfdOpen(os.Getpid(), 0)
	if err != nil {
		return fmt.Errorf("open own pidfd: %w", err)
	}
	defer func() {
		if cerr := unix.Close(fd); cerr != nil && err == nil {
			err = fmt.Errorf("close own pidfd: %w", cerr)
		}
	}()
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: string(addr), Net: "unix"})
	if err != nil {
		return err
	}
	defer func() {
		if cerr := conn.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	rights := unix.UnixRights(fd)
	n, oobn, err := conn.WriteMsgUnix(nonce, rights, nil)
	if err != nil {
		return err
	}
	if n != len(nonce) || oobn != len(rights) {
		return errors.New("short custody handoff")
	}
	ack := make([]byte, len(retryCustodyAck)+1)
	n, err = conn.Read(ack)
	if err != nil {
		return fmt.Errorf("custody acknowledgement: %w", err)
	}
	if string(ack[:n]) != retryCustodyAck {
		return errors.New("invalid custody acknowledgement")
	}
	return nil
}

// receiveRetryCustody accepts exactly one pidfd from the connected peer. The
// descriptor must arrive with the fixture nonce, from this user, and name the
// same process as the kernel's peer credential. Received descriptors that are
// not returned are closed here.
func receiveRetryCustody(conn *net.UnixConn, nonce []byte) (fd, pid int, err error) {
	var cred *unix.Ucred
	raw, err := conn.SyscallConn()
	if err != nil {
		return -1, 0, err
	}
	if cerr := raw.Control(func(s uintptr) { cred, err = unix.GetsockoptUcred(int(s), unix.SOL_SOCKET, unix.SO_PEERCRED) }); cerr != nil {
		return -1, 0, cerr
	}
	if err != nil {
		return -1, 0, fmt.Errorf("peer credential: %w", err)
	}
	buf := make([]byte, len(nonce)+1)
	oob := make([]byte, unix.CmsgSpace(4*4))
	n, oobn, flags, _, err := conn.ReadMsgUnix(buf, oob)
	if err != nil {
		return -1, 0, err
	}
	var fds []int
	msgs, perr := unix.ParseSocketControlMessage(oob[:oobn])
	for i := range msgs {
		got, rerr := unix.ParseUnixRights(&msgs[i])
		perr = errors.Join(perr, rerr)
		fds = append(fds, got...)
	}
	fd = -1
	defer func() {
		for _, extra := range fds {
			if extra != fd {
				err = errors.Join(err, unix.Close(extra))
			}
		}
		if err != nil && fd >= 0 {
			err = errors.Join(err, unix.Close(fd))
			fd = -1
		}
	}()
	switch {
	case perr != nil:
		return -1, 0, perr
	case flags&unix.MSG_CTRUNC != 0:
		return -1, 0, errors.New("truncated custody descriptors")
	case len(fds) != 1:
		return -1, 0, fmt.Errorf("received %d custody descriptors; want 1", len(fds))
	case !bytes.Equal(buf[:n], nonce):
		return -1, 0, errors.New("custody nonce mismatch")
	case int(cred.Uid) != os.Getuid():
		return -1, 0, errors.New("custody peer belongs to another user")
	}
	fd = fds[0]
	pid, err = pidfdProcess(fd)
	if err != nil {
		return fd, 0, err
	}
	if pid != int(cred.Pid) {
		return fd, 0, fmt.Errorf("custody pidfd names process %d; peer is %d", pid, cred.Pid)
	}
	return fd, pid, nil
}

// pidfdProcess reports the process a received descriptor refers to, from the
// kernel's own fdinfo. It fails unless the descriptor is a live pidfd.
func pidfdProcess(fd int) (int, error) {
	data, err := os.ReadFile("/proc/self/fdinfo/" + strconv.Itoa(fd))
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if v, ok := strings.CutPrefix(line, "Pid:"); ok {
			pid, err := strconv.Atoi(strings.TrimSpace(v))
			if err != nil || pid <= 0 {
				return 0, fmt.Errorf("custody descriptor is not a live pidfd: Pid %q", strings.TrimSpace(v))
			}
			return pid, nil
		}
	}
	return 0, errors.New("custody descriptor is not a pidfd")
}

// awaitRetryExit waits until the kernel reports the pidfd's process has
// exited. Reaching the deadline is a failure, never evidence of exit.
func awaitRetryExit(fd int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		ms := int(time.Until(deadline) / time.Millisecond)
		if ms <= 0 {
			return fmt.Errorf("workload exit not observed within %v", timeout)
		}
		fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		n, err := unix.Poll(fds, ms)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return fmt.Errorf("poll pidfd: %w", err)
		}
		if n == 1 {
			if fds[0].Revents&(unix.POLLERR|unix.POLLNVAL) != 0 {
				return fmt.Errorf("poll pidfd: revents %#x", fds[0].Revents)
			}
			if fds[0].Revents&unix.POLLIN != 0 {
				return nil
			}
		}
	}
}

func closeRetryPidfd(fd int) error { return unix.Close(fd) }
