//go:build linux

package remotehelper

import "golang.org/x/sys/unix"

func publish(fd int) error { return unix.Renameat2(fd, ".stage", fd, "work", unix.RENAME_NOREPLACE) }
