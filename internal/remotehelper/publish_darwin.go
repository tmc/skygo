//go:build darwin

package remotehelper

import "golang.org/x/sys/unix"

func publish(fd int) error { return unix.RenameatxNp(fd, ".stage", fd, "work", unix.RENAME_EXCL) }
