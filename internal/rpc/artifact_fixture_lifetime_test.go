//go:build darwin || linux

package rpc_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// The default parent is task-scoped and persistent. An explicit override must
// name an existing private directory. Only the concrete fixture owns its unique
// child and removal; testing.TempDir never removes a held owner's namespace.
func artifactFixtureDirectory(t *testing.T) string {
	t.Helper()
	base := os.Getenv("SKYGO_ARTIFACT_TEST_ROOT")
	defaultRoot := base == ""
	if defaultRoot {
		home, err := os.UserHomeDir()
		if err != nil {
			t.Fatal(err)
		}
		base = filepath.Join(home, "tmp", "skypilot", "20261019-upload-growth-tests")
	}
	base = filepath.Clean(base)
	if !filepath.IsAbs(base) || base == "/" || base == "/tmp" || strings.HasPrefix(base, "/tmp/") || base == "/private/tmp" || strings.HasPrefix(base, "/private/tmp/") {
		t.Fatal("artifact test root must name a private project-scoped persistent directory")
	}
	if defaultRoot {
		if err := os.MkdirAll(base, 0700); err != nil {
			t.Fatal(err)
		}
	}
	info, err := os.Lstat(base)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		t.Fatal("artifact test root must be a real directory with mode 0700")
	}
	dir, err := os.MkdirTemp(base, "rpc-artifact-")
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

// settle joins the actual enclosing HTTP handlers before inspecting original
// Control owners. A dependent upload fixture borrows exactly these resources;
// its original error/frame stays linked even after its helper returns.
func (f *artifactRPCFixture) settle(t *testing.T) bool {
	t.Helper()
	if f.held != nil {
		return false
	}
	if f.dependency != nil && !f.dependency.settle(t) {
		f.held = fmt.Errorf("temporary upload dependency: %w", f.dependency.held)
		return false
	}
	if !f.httpJoined {
		if f.server != nil {
			f.server.Close()
		}
		f.httpJoined = true
	}
	return f.stopSampler(t)
}

// Retention has one bounded goroutine per failed concrete fixture, never a
// replacement operation/Close or a success callback. The private channel has no
// sender and is never closed; the frame and its original error/dependency/path
// remain reachable until test-process exit. No global fixture registry exists.
func (f *artifactRPCFixture) retain(t *testing.T) {
	t.Helper()
	f.retainOnce.Do(func() {
		if f.wantHeld != nil && errors.Is(f.held, f.wantHeld) {
			t.Logf("expected original artifact fixture remains held at %s: %v", f.directory, f.held)
		} else {
			t.Errorf("retaining original artifact fixture at %s: %v", f.directory, f.held)
		}
		processLifetime := make(chan struct{})
		go func(original *artifactRPCFixture) {
			<-processLifetime
			runtime.KeepAlive(original)
		}(f)
	})
}

// retireJoined contains the same guarded decision used by ordinary cleanup.
// Returning an error never reports or replaces it; cleanup retains that exact
// concrete frame. The controlled sentinel oracle can inspect this decision.
func (f *artifactRPCFixture) retireJoined(t *testing.T) error {
	t.Helper()
	if f.retired {
		return nil
	}
	if !f.settle(t) {
		return f.held
	}
	if f.retire != nil {
		if err := f.retire(); err != nil {
			f.held = err
			return err
		}
	}
	if f.remove != nil {
		if err := f.remove(f.directory); err != nil {
			f.held = err
			return err
		}
	}
	f.retired = true
	return nil
}

func (f *artifactRPCFixture) cleanup(t *testing.T) {
	t.Helper()
	if err := f.retireJoined(t); err != nil {
		f.retain(t)
	}
}

// UNRUN source oracle. This injects ONLY a fixture settlement refusal; it does
// not simulate a kernel operation, failing Files Close or unknown worker death.
// The retained dependency is the actual temporary production upload fixture.
// Spies make any attempted explicit retirement/removal visible immediately.
func TestArtifactFixtureDependentRefusalPreservesFrameAndNamespace(t *testing.T) {
	f := artifactRPC(t, nil)
	child := f.uploadService(t)
	failure := errors.New("injected fixture settlement boundary remains held")
	child.held = failure
	if child.runCancel != nil || child.stopSampler(t) {
		t.Fatal("unrun nil-cancel shortcut cleared held dependency")
	}
	retireCalls, removeCalls := 0, 0
	realRetire, realRemove := f.retire, f.remove
	f.retire = func() error { retireCalls++; return realRetire() }
	f.remove = func(path string) error { removeCalls++; return realRemove(path) }
	// Check the gate without invoking cleanup's intentional test-error report or
	// process-lifetime keeper: no actual sampler/I/O was started in this oracle.
	if err := f.retireJoined(t); !errors.Is(err, failure) {
		t.Fatalf("dependent refusal became positive settlement: %v", err)
	}
	if !errors.Is(f.held, failure) || f.dependency != child || child.held != failure || child.control == nil || child.server == nil {
		t.Fatal("lost original dependency/error frame")
	}
	saved := f.held
	if err := f.retireJoined(t); err != saved || f.held != saved || child.held != failure {
		t.Fatal("repeated cleanup replaced or cleared held error")
	}
	if retireCalls != 0 || removeCalls != 0 || f.retired {
		t.Fatal("dependent refusal retired borrowed resources")
	}
	if _, err := os.Stat(f.directory); err != nil {
		t.Fatalf("held namespace disappeared: %v", err)
	}
	if _, err := f.root.Stat("."); err != nil {
		t.Fatalf("held root was closed: %v", err)
	}
	if _, err := f.store.ArtifactReservations(); err != nil {
		t.Fatalf("held Store was closed: %v", err)
	}
	// The injected boundary owns no uncertain syscall. Release ONLY that known
	// test sentinel, then positive actual server/Control settlement permits this
	// oracle's cleanup. Real held owner errors are never reset by fixture code.
	child.held = nil
	f.held = nil
	f.cleanup(t)
	if !f.retired || retireCalls != 1 || removeCalls != 1 {
		t.Fatalf("positive cleanup counts retire=%d remove=%d", retireCalls, removeCalls)
	}
	if _, err := os.Stat(f.directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("positively settled namespace not removed: %v", err)
	}
}
