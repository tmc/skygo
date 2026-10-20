//go:build !linux

package local

import (
	"errors"
	"os"
)

func workspaceCWD(*os.File) (string, error) {
	return "", errors.New("workspace process ownership is not qualified on this platform")
}
func workspaceWriterPlatform() bool { return false }
