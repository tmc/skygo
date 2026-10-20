package server

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/store"
)

// ArtifactInitializationError reports a stopped initialization failure without
// exposing operator paths or root identities in its text. It may retain the
// original resources and error; callers must retain it, stop and not retry or
// close its resources through another path. Unwrap permits trusted diagnostics.
// A retained Store pointer is not proof of a lock after Store.Close was attempted.
type ArtifactInitializationError struct {
	stage string
	cause error
	frame *artifactInitialization
}

func (e *ArtifactInitializationError) Error() string {
	return "artifact initialization: " + e.stage
}

// Unwrap returns the original error, including typed descriptor uncertainty.
func (e *ArtifactInitializationError) Unwrap() error { return e.cause }

type artifactInitialization struct {
	store *store.Store
	root  *os.Root
	files *artifact.Files
	held  *ArtifactInitializationError
}

// InitializeArtifacts explicitly initializes an existing empty private artifact
// directory and installs its facts while holding one original Store lock.
// The caller must stop every server and prior writer and own the directory's
// initialization authority. This human premise does not prove arbitrary writers
// absent. Existing markers/configurations are refused; nothing is adopted or
// deleted. Success is persisted installation readback, not server readiness.
//
// Unsupported platforms refuse before mutation. Store.Open failure stops before
// opening the artifact root; dependency-hidden Close/lock uncertainty is not
// recoverable through this adapter. Other unsettled originals are retained in
// ArtifactInitializationError; no deferred close runs underneath them.
func InitializeArtifacts(dataDir, artifactDir string) error {
	if !artifactInitializerSupported() {
		return initializationFailure("unsupported platform", os.ErrInvalid)
	}
	if dataDir == "" || artifactDir == "" {
		return initializationFailure("data and artifact directories required", os.ErrInvalid)
	}
	s, err := store.Open(dataDir)
	if err != nil {
		// Store.Open may fail after dependency-owned cleanup; it returns no
		// original Store on failure. Do not reopen or touch the artifact root.
		return initializationFailure("open stopped state", err)
	}
	frame := &artifactInitialization{store: s}
	installed, err := s.ArtifactInstallation()
	if err != nil {
		return frame.finish("read installation", err)
	}
	if installed.Enabled {
		return frame.finish("already installed", store.ErrArtifactConflict)
	}
	name, err := filepath.Abs(artifactDir)
	if err != nil {
		return frame.finish("resolve artifact directory", err)
	}
	before, err := os.Lstat(name)
	if err != nil {
		return frame.finish("inspect artifact directory", err)
	}
	identity, err := artifactInitializerIdentity(before)
	if err != nil {
		return frame.finish("private artifact directory required", err)
	}
	frame.root, err = os.OpenRoot(name)
	if err != nil {
		return frame.finish("open artifact directory", err)
	}
	opened, err := frame.root.Lstat(".")
	if err != nil {
		return frame.finish("read original artifact directory", err)
	}
	actual, err := artifactInitializerIdentity(opened)
	if err != nil || actual != identity || !os.SameFile(before, opened) {
		return frame.finish("artifact directory changed", errors.Join(artifact.ErrFileConflict, err))
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return frame.finish("create root identity", err)
	}
	facts, err := artifact.Initialize(frame.root, identity, hex.EncodeToString(random[:]))
	if err != nil {
		return frame.finish("initialize root marker", err)
	}
	frame.files, err = artifact.Open(frame.root, identity)
	if err != nil {
		return frame.finish("open initialized files", err)
	}
	readback, err := frame.files.RootFacts()
	if err != nil {
		return frame.finish("read root marker", err)
	}
	if readback != facts {
		return frame.finish("root readback differs", artifact.ErrFileConflict)
	}
	want := store.ArtifactInstallation{Enabled: true, ID: facts.ID, Unit: facts.Unit, Volume: facts.Volume}
	installed, err = s.InstallArtifacts(want)
	if err != nil {
		return frame.finish("install root metadata", err)
	}
	if installed != want {
		return frame.finish("installation differs", store.ErrArtifactConflict)
	}
	installed, err = s.ArtifactInstallation()
	if err != nil {
		return frame.finish("read installed metadata", err)
	}
	if installed != want {
		return frame.finish("installation readback differs", store.ErrArtifactConflict)
	}
	return frame.finish("close installation", nil)
}

func initializationFailure(stage string, cause error) error {
	return &ArtifactInitializationError{stage: stage, cause: cause}
}

// finish is called once by the original synchronous adapter. A held boundary is
// sticky even if an internal caller mistakenly calls it twice. Nil resources
// mean only previously positive closes, never inferred cleanup or deletion.
func (f *artifactInitialization) finish(stage string, cause error) error {
	if f.held != nil {
		return f.held
	}
	hold := func(at string, err error) error {
		f.held = &ArtifactInitializationError{stage: at, cause: errors.Join(cause, err), frame: f}
		return f.held
	}
	if errors.Is(cause, artifact.ErrFileCloseUncertain) {
		return hold(stage, nil)
	}
	if f.files != nil {
		if err := f.files.Close(); err != nil {
			return hold("files remain unsettled", err)
		}
		f.files = nil
	}
	if f.root != nil {
		if err := f.root.Close(); err != nil {
			return hold("root close uncertain", err)
		}
		f.root = nil
	}
	if f.store != nil {
		if err := f.store.Close(); err != nil {
			return hold("state close failed", err)
		}
		f.store = nil
	}
	if cause != nil {
		return initializationFailure(stage, cause)
	}
	return nil
}
