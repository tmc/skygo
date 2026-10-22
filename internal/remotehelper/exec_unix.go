//go:build linux || darwin

package remotehelper

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

func execute(directory string, args, env []string) error {
	if err := os.Chdir(directory); err != nil {
		return err
	}
	file := args[0]
	if !strings.ContainsRune(file, '/') {
		search := ""
		for _, entry := range env {
			if strings.HasPrefix(entry, "PATH=") {
				search = strings.TrimPrefix(entry, "PATH=")
			}
		}
		file = ""
		for _, dir := range filepath.SplitList(search) {
			candidate := filepath.Join(dir, args[0])
			info, err := os.Stat(candidate)
			if err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0111 != 0 {
				if !filepath.IsAbs(candidate) {
					return errors.New("relative path executable refused")
				}
				file = candidate
				break
			}
		}
		if file == "" {
			return errors.New("program is unavailable")
		}
	}
	return syscall.Exec(file, args, env)
}
