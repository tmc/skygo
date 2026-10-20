//go:build linux || darwin

package control_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
)

// The frame owns its namespace explicitly: t.TempDir's separate RemoveAll would
// run even when an original owner or close remains held. No production registry
// or resource lifetime is changed by this test-only frame.
type coordinatorArtifactResources struct {
	directory         string
	root, otherRoot   *os.Root
	files, otherFiles *artifact.Files
	store             *store.Store
	control           *control.Control
	stop              context.CancelFunc
	done              <-chan error
	held, wantHeld    error
	keeping, retired  bool
}

func coordinatorArtifactDirectory(t *testing.T) string {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261019-upload-growth-tests")
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	name, err := os.MkdirTemp(parent, "control-")
	if err != nil {
		t.Fatal(err)
	}
	return name
}

func (f *coordinatorArtifactResources) retain(t *testing.T, err error) bool {
	t.Helper()
	if f.held == nil {
		f.held = err
	}
	if f.wantHeld != nil && errors.Is(f.held, f.wantHeld) {
		t.Logf("expected test-boundary HOLD %s: %v", f.directory, f.held)
	} else {
		t.Errorf("HOLD original coordinator fixture %s: %v", f.directory, f.held)
	}
	if !f.keeping {
		f.keeping = true
		original := f.held
		// One keeper per failed frame, until process exit. A cleanup closure alone
		// would become unreachable and permit finalizers to touch original owners.
		hold := make(chan struct{})
		go func() { <-hold; runtime.KeepAlive(f); runtime.KeepAlive(original) }()
	}
	return false
}

func (f *coordinatorArtifactResources) retire(t *testing.T) bool {
	t.Helper()
	if f.held != nil {
		return f.retain(t, f.held)
	}
	if f.retired {
		return true
	}
	if f.control != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		drainErr := f.control.DrainArtifacts(ctx)
		var runErr, joinErr error
		if f.stop != nil {
			f.stop()
			select {
			case runErr = <-f.done:
				if errors.Is(runErr, context.Canceled) {
					runErr = nil
				}
			case <-ctx.Done():
				runErr = fmt.Errorf("original Run return remains held: %w", ctx.Err())
			}
			joinErr = f.control.Join(ctx)
		}
		if err := errors.Join(drainErr, runErr, joinErr); err != nil {
			return f.retain(t, err)
		}
	}
	for _, files := range []*artifact.Files{f.files, f.otherFiles} {
		if files != nil {
			if err := files.Close(); err != nil {
				return f.retain(t, err)
			}
		}
	}
	for _, root := range []*os.Root{f.root, f.otherRoot} {
		if root != nil {
			if err := root.Close(); err != nil {
				return f.retain(t, err)
			}
		}
	}
	if f.store != nil {
		if err := f.store.Close(); err != nil {
			return f.retain(t, err)
		}
	}
	if err := os.RemoveAll(f.directory); err != nil {
		return f.retain(t, err)
	}
	f.retired = true
	return true
}

// This is a controlled pre-retirement refusal, NOT a kernel/Run failure. The
// same actual retirement gate must leave its original root and namespace intact.
func TestCoordinatorFixtureRetainsOriginalNamespaceOnHeldBoundary(t *testing.T) {
	f := &coordinatorArtifactResources{directory: coordinatorArtifactDirectory(t)}
	var err error
	f.root, err = os.OpenRoot(f.directory)
	if err != nil {
		t.Fatal(err)
	}
	refusal := errors.New("controlled no-I/O settlement refusal")
	f.held, f.wantHeld = refusal, refusal
	if f.retire(t) {
		t.Fatal("held original fixture retired")
	}
	if _, err := f.root.Stat("."); err != nil {
		t.Fatalf("held root closed: %v", err)
	}
	if _, err := os.Stat(f.directory); err != nil {
		t.Fatalf("held namespace removed: %v", err)
	}
	if f.held != refusal {
		t.Fatal("original refusal changed")
	}
	// Only this explicit no-I/O sentinel is reset. No real wait/unknown Close is
	// retried, no native owner existed, and positive ordinary cleanup now applies.
	f.held = nil
	if !f.retire(t) {
		t.Fatal("controlled boundary did not permit ordinary cleanup")
	}
}
