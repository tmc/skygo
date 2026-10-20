//go:build linux

package server

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

func TestServerLifetimeOwnedLocalWorkspaceUsesOriginalConcreteRoute(t *testing.T) {
	f := lifetimeFixture(t)
	name := filepath.Join(f.directory, "artifacts")
	if err := os.Mkdir(name, 0700); err != nil {
		t.Fatal(err)
	}
	if err := InitializeArtifacts(f.data, name); err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	frame := &serverRun{}
	f.frame = frame
	cfg := Config{DataDir: f.data, Listen: "127.0.0.1:0", ArtifactDir: name}
	if err := frame.start(context.Background(), cfg); err != nil {
		result := frame.closeStartup(err)
		if frame.held != nil {
			f.hold(t, result)
		}
		t.Fatal("owned local startup", result)
	}
	if frame.localBackend == nil || frame.control == nil || frame.files == nil || frame.root == nil || !frame.ownedFiles {
		t.Fatal("owned original resource route missing")
	}
	binding, err := backend.LocalBinding(frame.store.Owner())
	if err != nil {
		t.Fatal(err)
	}
	if !frame.control.WorkspaceTargetEnabled("local", binding) {
		t.Fatal("owned route did not install exact LocalWorkspace binding")
	}
	actor, err := frame.engine.Authenticate("")
	if err != nil {
		t.Fatal(err)
	}
	targets, err := frame.control.Targets(actor)
	if err != nil || len(targets) != 1 || targets[0].Backend != frame.localBackend || targets[0].Binding != binding {
		t.Fatal("target is not original concrete Local pointer", targets, err)
	}
	// No workload is launched. This witnesses actual configuration and the fresh
	// original producer/physical closure path, not native cancellation correctness.
	if err := frame.shutdown(nil); err != nil {
		f.hold(t, err)
		t.Fatal("owned original shutdown", err)
	}
	if frame.store != nil || frame.localBackend != nil || frame.files != nil || frame.root != nil {
		f.hold(t, errors.New("positive shutdown retained ownership"))
		t.Fatal("positive close sequence incomplete")
	}
	f.frame = nil
	s, err := store.Open(f.data)
	if err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	f.opened = s
	if err := s.Close(); err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	f.opened = nil
}
