//go:build linux || darwin

package store_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tmc/skygo/internal/server"
	"github.com/tmc/skygo/internal/store"
)

func ExampleStore_ArtifactRequiredObjects() {
	home, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261020-required-objects-example")
	if err := os.MkdirAll(parent, 0700); err != nil {
		panic(err)
	}
	base, err := os.MkdirTemp(parent, "case-")
	if err != nil {
		panic(err)
	}
	dataDir := filepath.Join(base, "state")
	artifactDir := filepath.Join(base, "artifacts")
	if err := os.Mkdir(artifactDir, 0700); err != nil {
		panic(err)
	}
	// This stopped initializer probes and binds the actual private root. It
	// starts no workload and supplies no synthetic filesystem identity.
	if err := server.InitializeArtifacts(dataDir, artifactDir); err != nil {
		panic(err)
	}
	s, err := store.Open(dataDir)
	if err != nil {
		panic(err)
	}
	objects, queryErr := s.ArtifactRequiredObjects()
	if err := errors.Join(queryErr, s.Close()); err != nil {
		// Retain this private case on uncertain original settlement.
		panic(err)
	}
	if err := os.RemoveAll(base); err != nil {
		panic(err)
	}
	fmt.Println("objects", len(objects))
	// Output: objects 0
}
