//go:build linux || darwin

package local

import (
	"io/fs"
	"os"
	"os/exec"
	"syscall"
)

const noFollow = syscall.O_NOFOLLOW

func ownedByUser(info fs.FileInfo) bool {
	st, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(st.Uid) == os.Geteuid()
}

// detach starts the supervisor in its own session so terminal and
// process-group signals aimed at the API server do not reach it.
func detach(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true} }

func closeOnExec(f *os.File) { syscall.CloseOnExec(int(f.Fd())) }
