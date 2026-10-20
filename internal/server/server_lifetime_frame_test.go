//go:build linux || darwin

package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/store"
)

func TestServerLifetimeHeldFrameStopsDownstreamClosure(t *testing.T) {
	f := lifetimeFixture(t)
	s, err := store.Open(f.data)
	if err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	f.opened = s
	root, err := os.OpenRoot(f.directory)
	if err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	frame := &serverRun{store: s, root: root}
	f.frame = frame
	original := errors.New("controlled policy refusal before any Close")
	failure := frame.hold("controlled boundary", original)
	var held *RunError
	if !errors.As(failure, &held) || held.frame != frame || held.cause != original || !errors.Is(failure, original) {
		f.hold(t, failure)
		t.Fatal("original cause/frame changed", failure)
	}
	if err := frame.closeResources(errors.New("later cause")); err != failure {
		f.hold(t, failure)
		t.Fatal("held finalizer replaced original error", err)
	}
	if err := frame.closeStartup(nil); err != failure {
		f.hold(t, failure)
		t.Fatal("held startup finalizer retried", err)
	}
	if frame.store != s || frame.root != root {
		f.hold(t, failure)
		t.Fatal("held finalizer cleared downstream identity")
	}
	if _, err := root.Lstat("."); err != nil {
		f.hold(t, failure)
		t.Fatal("held finalizer closed downstream root", err)
	}
	if _, err := s.ArtifactInstallation(); err != nil {
		f.hold(t, failure)
		t.Fatal("held finalizer closed downstream Store", err)
	}
	if strings.Contains(failure.Error(), f.directory) || strings.Contains(failure.Error(), original.Error()) {
		f.hold(t, failure)
		t.Fatal("RunError exposed private cause/path")
	}
	// This is a test-only logical HOLD, not a simulated kernel Close failure.
	// Keep its real original Store/root/frame and namespace until process exit;
	// no reset, alternate closer, reopen, or later RemoveAll is authorized here.
	f.expectedHeld = original
	f.hold(t, failure)
}

func TestServerLifetimeArtifactStartupRefusesWithoutAdoption(t *testing.T) {
	for _, mode := range []string{"uninitialized", "different installed root"} {
		t.Run(mode, func(t *testing.T) {
			f := lifetimeFixture(t)
			original := filepath.Join(f.directory, "original")
			supplied := filepath.Join(f.directory, "supplied")
			for _, name := range []string{original, supplied} {
				if err := os.Mkdir(name, 0700); err != nil {
					t.Fatal(err)
				}
			}
			if err := InitializeArtifacts(f.data, original); err != nil {
				f.hold(t, err)
				t.Fatal(err)
			}
			var suppliedBefore []byte
			if mode == "different installed root" {
				otherData := filepath.Join(f.directory, "other-state")
				if err := InitializeArtifacts(otherData, supplied); err != nil {
					f.hold(t, err)
					t.Fatal(err)
				}
				var err error
				suppliedBefore, err = os.ReadFile(filepath.Join(supplied, ".skygo-artifact-root"))
				if err != nil {
					t.Fatal(err)
				}
			}
			originalBefore, err := os.ReadFile(filepath.Join(original, ".skygo-artifact-root"))
			if err != nil {
				t.Fatal(err)
			}
			frame := &serverRun{}
			f.frame = frame
			cfg := lifetimeRemoteConfig(&lifetimeRemote{})
			cfg.DataDir, cfg.Listen, cfg.ArtifactDir = f.data, "127.0.0.1:0", supplied
			startErr := frame.start(context.Background(), cfg)
			if startErr == nil {
				t.Fatal("startup adopted supplied artifact root")
			}
			if frame.listener != nil || frame.runDone != nil || frame.serveDone != nil {
				f.hold(t, startErr)
				t.Fatal("invalid root launched original serving functions")
			}
			if mode == "different installed root" && !errors.Is(startErr, artifact.ErrFileConflict) {
				t.Error("mismatched root did not refuse exact identity", startErr)
			}
			result := frame.closeStartup(startErr)
			var held *RunError
			if errors.As(result, &held) {
				f.hold(t, result)
				t.Fatal("ordinary root refusal failed positive startup finalization")
			}
			if !errors.Is(result, startErr) || frame.store != nil || frame.root != nil || frame.files != nil {
				f.hold(t, result)
				t.Fatal("startup refusal lost cause or retained unfinished resources")
			}
			f.frame = nil
			originalAfter, err := os.ReadFile(filepath.Join(original, ".skygo-artifact-root"))
			if err != nil || string(originalAfter) != string(originalBefore) {
				t.Fatal("installed original marker changed", err)
			}
			if mode == "uninitialized" {
				entries, err := os.ReadDir(supplied)
				if err != nil || len(entries) != 0 {
					t.Fatal("uninitialized root was mutated", entries, err)
				}
				if _, err := os.Lstat(filepath.Join(supplied, ".skygo-artifact-root")); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("Run initialized a marker", err)
				}
			} else {
				after, err := os.ReadFile(filepath.Join(supplied, ".skygo-artifact-root"))
				if err != nil || string(after) != string(suppliedBefore) {
					t.Fatal("foreign installed marker changed", err)
				}
			}
			// These reopens follow actual positive startup closure, never a deadline.
			s, err := store.Open(f.data)
			if err != nil {
				f.hold(t, err)
				t.Fatal(err)
			}
			f.opened = s
			installed, readErr := s.ArtifactInstallation()
			if err := s.Close(); err != nil {
				f.hold(t, err)
				t.Fatal(err)
			}
			f.opened = nil
			if readErr != nil || !installed.Enabled {
				t.Fatal("startup refusal changed stopped installation", installed, readErr)
			}
		})
	}
}

func ExampleRunError() {
	original := errors.New("original ownership remains held")
	frame := &serverRun{}
	err := frame.hold("preparation", original)
	var failure *RunError
	fmt.Println(errors.As(err, &failure), errors.Is(err, original))
	fmt.Println(err)
	// Output:
	// true true
	// server lifetime: preparation
}
