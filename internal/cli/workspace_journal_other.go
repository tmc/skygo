//go:build !darwin && !linux && !freebsd && !openbsd && !netbsd && !dragonfly

package cli

import (
	"errors"
	"os"
)

const workspaceNoFollow = 0

func workspacePlatform() error {
	return errors.New("workspace journals are unsupported on this platform")
}
func workspacePermissions(os.FileInfo, bool, bool) error { return workspacePlatform() }
func lockWorkspace(*os.File) error                       { return workspacePlatform() }

func renameWorkspaceFile(*os.File, string, string) error { return workspacePlatform() }

func workspaceCacheOwner(os.FileInfo) error { return workspacePlatform() }
