//go:build !linux

package cli

import (
	"errors"
	"net"
	"time"
)

var errRetryCustody = errors.New("retry workload custody requires Linux pidfds")

func sendRetryCustody(dir string) error { return errRetryCustody }

func receiveRetryCustody(conn *net.UnixConn, nonce []byte) (fd, pid int, err error) {
	return -1, 0, errRetryCustody
}

func awaitRetryExit(fd int, timeout time.Duration) error { return errRetryCustody }

func closeRetryPidfd(fd int) error { return errRetryCustody }
