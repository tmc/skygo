//go:build linux || darwin

package server_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tmc/skygo/internal/server"
)

func ExampleInitializeArtifacts() {
	base, err := os.MkdirTemp("", "skygo-stopped-artifacts-")
	if err != nil {
		fmt.Println(err)
		return
	}
	root := filepath.Join(base, "artifacts")
	if err := os.Mkdir(root, 0700); err != nil {
		fmt.Println(err)
		return
	}
	// This example owns a new empty root; no server or prior writer exists.
	if err := server.InitializeArtifacts(filepath.Join(base, "state"), root); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("<nil>")
	os.RemoveAll(base)
	// Output: <nil>
}

func ExampleArtifactInitializationError() {
	// Empty parameters open no Store/root and retain no resources.
	err := server.InitializeArtifacts("", "")
	var failure *server.ArtifactInitializationError
	fmt.Println(errors.As(err, &failure))
	// Output: true
}
