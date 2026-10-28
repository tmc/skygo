//go:build !linux && !darwin

package local

import (
	"io/fs"
	"os"
	"os/exec"
)

const noFollow = 0

func ownedByUser(fs.FileInfo) bool { return false }
func detach(*exec.Cmd)             {}
func closeOnExec(*os.File)         {}
