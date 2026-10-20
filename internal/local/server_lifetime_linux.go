//go:build linux

package local

import (
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// CaptureServerLifetime captures only this server's identity. The server calls
// it once before artifact admission, outside Store, backend and service locks.
// No partial value is returned. Ordinary failures wrap ErrServerLifetimeUnavailable;
// uncertain Close returns ServerLifetimeCloseError and must remain retained.
// Trusted host/procfs paths and visibility assumptions are documented in the
// source packet; even a successful capture does not authorize stopped recovery.
func CaptureServerLifetime() (ServerLifetime, error) {
	var value ServerLifetime
	host, err := lifetimeRead("/etc/machine-id", 128, false)
	if err != nil {
		return value, err
	}
	value.Host, err = lifetimeID(host, false)
	if err != nil {
		return ServerLifetime{}, lifetimeUnavailable(err)
	}
	boot, err := lifetimeRead("/proc/sys/kernel/random/boot_id", 128, true)
	if err != nil {
		return ServerLifetime{}, err
	}
	value.Boot, err = lifetimeID(boot, true)
	if err != nil {
		return ServerLifetime{}, lifetimeUnavailable(err)
	}
	value.NamespaceDevice, value.NamespaceInode, err = lifetimeNamespace()
	if err != nil {
		return ServerLifetime{}, err
	}
	pid := os.Getpid()
	if pid <= 0 || uint64(pid) > 2147483647 {
		return ServerLifetime{}, lifetimeUnavailable(os.ErrInvalid)
	}
	value.PID = uint32(pid)
	stat, err := lifetimeRead("/proc/self/stat", 4096, true)
	if err != nil {
		return ServerLifetime{}, err
	}
	value.StartTicks, err = lifetimeStart(stat, value.PID)
	if err != nil {
		return ServerLifetime{}, lifetimeUnavailable(err)
	}
	return value, nil
}

func lifetimeRead(path string, limit int64, proc bool) ([]byte, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NONBLOCK|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, lifetimeUnavailable(err)
	}
	file := os.NewFile(uintptr(fd), path)
	var stat unix.Stat_t
	err = unix.Fstat(fd, &stat)
	if err == nil && stat.Mode&unix.S_IFMT != unix.S_IFREG {
		err = os.ErrInvalid
	}
	if err == nil && proc {
		var fs unix.Statfs_t
		err = unix.Fstatfs(fd, &fs)
		if err == nil && fs.Type != unix.PROC_SUPER_MAGIC {
			err = os.ErrInvalid
		}
	}
	var data []byte
	if err == nil {
		data, err = io.ReadAll(io.LimitReader(file, limit+1))
		if err == nil && int64(len(data)) > limit {
			err = os.ErrInvalid
		}
	}
	if closeErr := file.Close(); closeErr != nil {
		return nil, &ServerLifetimeCloseError{file: file, closeErr: closeErr, captureErr: err}
	}
	if err != nil {
		return nil, lifetimeUnavailable(fmt.Errorf("read %s: %w", path, err))
	}
	return data, nil
}

func lifetimeNamespace() (uint64, uint64, error) {
	// Following this ONE fixed procfs namespace magic link is intentional.
	// O_NONBLOCK avoids an attacker-substituted FIFO blocking before validation.
	const path = "/proc/self/ns/pid"
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err != nil {
		return 0, 0, lifetimeUnavailable(err)
	}
	file := os.NewFile(uintptr(fd), path)
	var stat unix.Stat_t
	var fs unix.Statfs_t
	err = unix.Fstat(fd, &stat)
	if err == nil {
		err = unix.Fstatfs(fd, &fs)
	}
	if err == nil && (fs.Type != unix.NSFS_MAGIC || stat.Ino == 0) {
		err = os.ErrInvalid
	}
	if err == nil {
		var kind int
		kind, err = unix.IoctlRetInt(fd, unix.NS_GET_NSTYPE)
		if err == nil && kind != unix.CLONE_NEWPID {
			err = os.ErrInvalid
		}
	}
	if closeErr := file.Close(); closeErr != nil {
		return 0, 0, &ServerLifetimeCloseError{file: file, closeErr: closeErr, captureErr: err}
	}
	if err != nil {
		return 0, 0, lifetimeUnavailable(err)
	}
	return uint64(stat.Dev), stat.Ino, nil
}
