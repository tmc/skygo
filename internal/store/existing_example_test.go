//go:build linux || darwin

package store_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tmc/skygo/internal/store"
)

func ExampleInspectExisting() {
	dir, err := os.MkdirTemp("", "skygo-existing-example-")
	if err != nil {
		panic(err)
	}
	s, err := store.Open(dir)
	if err != nil {
		panic(err)
	}
	owner := s.Owner()
	if err := s.ActivateScoped(); err != nil {
		panic(err)
	}
	if err := s.Close(); err != nil {
		panic(err)
	}

	installed, err := store.InspectExisting(dir)
	if err != nil {
		panic(err)
	}
	s, err = store.OpenExisting(dir, installed)
	if err != nil {
		panic(err)
	}
	if err := s.Close(); err != nil {
		panic(err)
	}
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}
	fmt.Println(installed.Owner() == owner, s.Owner() == owner, s.Scoped())
	// Output: true true true
}

func ExampleInspectExisting_notCurrent() {
	dir, err := os.MkdirTemp("", "skygo-existing-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	// An empty directory is never initialized.
	_, err = store.InspectExisting(dir)
	_, statErr := os.Stat(filepath.Join(dir, "jobs.db"))
	fmt.Println(err != nil, errors.Is(statErr, os.ErrNotExist))
	// Output: true true
}

// existingScoped initializes a scoped installation in a new temporary
// directory with Open and ActivateScoped, closes it, and returns the
// directory and its owner. Any failure panics without removing the
// directory; callers remove it only after every Store they open has closed.
func existingScoped() (string, string) {
	dir, err := os.MkdirTemp("", "skygo-existing-example-")
	if err != nil {
		panic(err)
	}
	s, err := store.Open(dir)
	if err != nil {
		panic(err)
	}
	owner := s.Owner()
	if err := s.ActivateScoped(); err != nil {
		panic(err)
	}
	if err := s.Close(); err != nil {
		panic(err)
	}
	return dir, owner
}

func ExampleOpenExisting() {
	dir, owner := existingScoped()
	installed, err := store.InspectExisting(dir)
	if err != nil {
		panic(err)
	}
	s, err := store.OpenExisting(dir, installed)
	if err != nil {
		panic(err)
	}
	fmt.Println(s.Owner() == owner, s.Scoped())
	if err := s.Close(); err != nil {
		panic(err)
	}
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}
	// Output: true true
}

func ExampleExistingInstallation() {
	dir, owner := existingScoped()
	var zero store.ExistingInstallation
	_, err := store.OpenExisting(dir, zero) // The zero value is invalid.
	fmt.Println(err != nil)
	if err == nil {
		panic("zero ExistingInstallation accepted")
	}
	installed, err := store.InspectExisting(dir)
	if err != nil {
		panic(err)
	}
	fmt.Println(installed.Owner() == owner)
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}
	// Output:
	// true
	// true
}

func ExampleExistingInstallation_Owner() {
	dir, owner := existingScoped()
	installed, err := store.InspectExisting(dir)
	if err != nil {
		panic(err)
	}
	fmt.Println(installed.Owner() != "", installed.Owner() == owner)
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}
	// Output: true true
}

func ExampleExistingInstallation_ArtifactInstallation() {
	dir, _ := existingScoped()
	installed, err := store.InspectExisting(dir)
	if err != nil {
		panic(err)
	}
	// No artifact root was installed, so none is persisted.
	fmt.Println(installed.ArtifactInstallation().Enabled)
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}
	// Output: false
}
