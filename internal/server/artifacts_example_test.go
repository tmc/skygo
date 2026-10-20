//go:build darwin || linux

package server_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/server"
)

func ExampleInstallArtifacts() {
	// The qualification caller supplies a project-scoped TMPDIR.
	dir, err := os.MkdirTemp("", "skygo-artifact-install-")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer os.RemoveAll(dir)
	name := filepath.Join(dir, "artifacts")
	if err := os.Mkdir(name, 0700); err != nil {
		fmt.Println(err)
		return
	}
	root, err := os.OpenRoot(name)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer root.Close()
	info, err := root.Stat(".")
	if err != nil {
		fmt.Println(err)
		return
	}
	physical, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		fmt.Println("root identity unavailable")
		return
	}
	identity := artifact.FileID{Device: uint64(physical.Dev), Inode: uint64(physical.Ino)}
	if _, err := artifact.Initialize(root, identity, strings.Repeat("a", 32)); err != nil {
		fmt.Println(err)
		return
	}
	files, err := artifact.Open(root, identity)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer files.Close()
	// No server or other Files writer is running during this installation.
	fmt.Println(server.InstallArtifacts(filepath.Join(dir, "state"), files))
	// Output: <nil>
}
