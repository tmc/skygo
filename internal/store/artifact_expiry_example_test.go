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

func ExampleArtifactUploadExpiry() {
	var observed store.ArtifactUploadExpiry
	// Zero File is unknown, not evidence that an original file is absent.
	fmt.Println(observed.File == (store.ArtifactFile{}))
	// Output: true
}

func expiryExampleStore() (*store.Store, func(error)) {
	home, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261020-expiry-example")
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
	// The stopped initializer probes the actual private root. No file, clock
	// or native deletion receipt is fabricated to make this example runnable.
	if err := server.InitializeArtifacts(dataDir, artifactDir); err != nil {
		panic(err)
	}
	s, err := store.Open(dataDir)
	if err != nil {
		panic(err)
	}
	return s, func(operationErr error) {
		if err := errors.Join(operationErr, s.Close()); err != nil {
			// Preserve the original case after an uncertain operation or Close.
			panic(err)
		}
		if err := os.RemoveAll(base); err != nil {
			panic(err)
		}
	}
}

func ExampleStore_NextArtifactUploadExpiry() {
	s, finish := expiryExampleStore()
	_, found, err := s.NextArtifactUploadExpiry("")
	finish(err)
	fmt.Println("expired uploads", found)
	// Output: expired uploads false
}

func ExampleStore_ExpireArtifactUpload() {
	s, finish := expiryExampleStore()
	// No matching original observation exists in this initialized empty root.
	// A zero DTO cannot mark an unknown file or create cleanup authority.
	work, err := s.ExpireArtifactUpload(store.ArtifactUploadExpiry{})
	if !errors.Is(err, store.ErrArtifactConflict) {
		finish(fmt.Errorf("expected expiry refusal, got %v", err))
		return
	}
	finish(nil)
	fmt.Println("refused", work == (store.ArtifactDeleteWork{}))
	// Output: refused true
}
