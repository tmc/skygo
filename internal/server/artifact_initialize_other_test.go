//go:build !linux && !darwin

package server

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestInitializerUnsupportedBeforeMutation(t *testing.T) {
	base := t.TempDir()
	data, root := filepath.Join(base, "state"), filepath.Join(base, "artifacts")
	if err := InitializeArtifacts(data, root); err == nil {
		t.Fatal("unsupported initialization accepted")
	}
	for _, name := range []string{data, root} {
		if _, err := os.Lstat(name); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("unsupported opt-in mutated path", err)
		}
	}
}
