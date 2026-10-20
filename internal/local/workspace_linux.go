//go:build linux

package local

import (
	"fmt"
	"os"
)

func workspaceCWD(directory *os.File) (string, error) {
	if directory == nil {
		return "", os.ErrInvalid
	}
	if _, err := directory.Stat(); err != nil {
		return "", err
	}
	path := fmt.Sprintf("/proc/self/fd/%d", directory.Fd())
	opened, err := directory.Stat()
	if err != nil {
		return "", err
	}
	routed, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !os.SameFile(opened, routed) {
		return "", os.ErrInvalid
	}
	return path, nil
}
func workspaceWriterPlatform() bool { return true }
