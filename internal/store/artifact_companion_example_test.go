//go:build linux || darwin

package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/tmc/skygo/internal/artifact"
)

// companionExampleBase keeps test and example artifacts in a persistent,
// task-scoped directory. Failed operations retain their case directory.
func companionExampleBase() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261020-companion-tests")
	if err := os.MkdirAll(parent, 0700); err != nil {
		return "", err
	}
	return os.MkdirTemp(parent, "case-")
}

// companionExampleFrame retains actual constructor, Store and public operation
// owners on failure. There is no retry handle or production owner registry.
type companionExampleFrame struct {
	base                           string
	root                           *os.Root
	store                          *Store
	rootAttempted, storeAttempted  bool
	constructorErr, closeErr, held error
	keeping                        bool
}

func (f *companionExampleFrame) hold(err error) {
	f.held = errors.Join(f.held, err)
	if !f.keeping {
		f.keeping = true
		wait := make(chan struct{})
		go func() { <-wait; runtime.KeepAlive(f) }()
	}
}

func (f *companionExampleFrame) fail(err error) {
	f.hold(err)
	panic(err)
}

func (f *companionExampleFrame) closeStore() error {
	if f.store == nil || f.storeAttempted {
		return f.closeErr
	}
	f.storeAttempted = true
	if err := f.store.Close(); err != nil {
		f.closeErr = errors.Join(f.closeErr, err)
		f.hold(err)
		return err
	}
	f.store = nil
	return nil
}

// exampleCompanionInstallation starts no background owners. A constructor's
// actual ErrFileCloseUncertain retains this frame, cause and borrowed Root;
// deferred cleanup cannot close that Root beneath the unavailable Files owner.
func exampleCompanionInstallation(f *companionExampleFrame) (dataDir, artifactDir string, err error) {
	dataDir, artifactDir = filepath.Join(f.base, "state"), filepath.Join(f.base, "objects")
	defer func() {
		err = errors.Join(err, f.closeStore())
		if f.constructorErr == nil && f.root != nil && !f.rootAttempted {
			f.rootAttempted = true
			rootErr := f.root.Close()
			err = errors.Join(err, rootErr)
			if rootErr == nil {
				f.root = nil
			}
		}
		if err != nil {
			f.hold(err)
		}
	}()
	if err = os.Mkdir(artifactDir, 0700); err != nil {
		return
	}
	f.root, err = os.OpenRoot(artifactDir)
	if err != nil {
		return
	}
	info, err := f.root.Stat(".")
	if err != nil {
		return
	}
	_, _, id, err := companionStat(info)
	if err != nil {
		return
	}
	facts, err := artifact.Initialize(f.root, artifact.FileID{Device: id.Device, Inode: id.Inode}, "0123456789abcdef0123456789abcdef")
	if errors.Is(err, artifact.ErrFileCloseUncertain) {
		f.constructorErr = err
	}
	if err != nil {
		return
	}
	f.store, err = Open(dataDir)
	if err != nil {
		return
	}
	_, err = f.store.InstallArtifacts(ArtifactInstallation{Enabled: true, ID: facts.ID, Unit: facts.Unit, Volume: facts.Volume})
	return
}

func ExampleBackupCompanion() {
	base, err := companionExampleBase()
	if err != nil {
		panic(err)
	}
	// On an uncertain Close or publication error retain all paths for operator
	// review. Remove the temporary installation only after complete success.
	f := &companionExampleFrame{base: base}
	dataDir, artifactDir, err := exampleCompanionInstallation(f)
	if err != nil {
		f.fail(err)
	}
	bundle := filepath.Join(base, "backup")
	if err := BackupCompanion(context.Background(), dataDir, artifactDir, bundle); err != nil {
		f.fail(err)
	}
	if err := os.RemoveAll(base); err != nil {
		f.fail(err)
	}
	fmt.Println("stopped installation backed up")
	// Output: stopped installation backed up
}

func ExampleRestoreCompanion() {
	base, err := companionExampleBase()
	if err != nil {
		panic(err)
	}
	f := &companionExampleFrame{base: base}
	dataDir, artifactDir, err := exampleCompanionInstallation(f)
	if err != nil {
		f.fail(err)
	}
	bundle := filepath.Join(base, "backup")
	if err := BackupCompanion(context.Background(), dataDir, artifactDir, bundle); err != nil {
		f.fail(err)
	}
	// The original installation remains stopped. Restore uses a new directory
	// and creates its own artifacts root; it does not relocate original files.
	restored := filepath.Join(base, "restored")
	if err := RestoreCompanion(context.Background(), bundle, restored); err != nil {
		f.fail(err)
	}
	s, err := Open(restored)
	if err != nil {
		f.fail(err)
	}
	f.store, f.storeAttempted = s, false
	required, queryErr := s.ArtifactRequiredObjects()
	if err := errors.Join(queryErr, f.closeStore()); err != nil {
		f.fail(err)
	}
	if err := os.RemoveAll(base); err != nil {
		f.fail(err)
	}
	fmt.Println("restored required objects:", len(required))
	// Output: restored required objects: 0
}
