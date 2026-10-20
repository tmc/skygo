//go:build linux

package control

import (
	"context"
	"errors"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

// No workspace process is started. All four original request owners remain
// occupied while production schedule must leave the attempt unchanged.
func TestWorkspaceFullSharedBudgetBeforeBeginAndCancelFirst(t *testing.T) {
	path := t.TempDir()
	if err := os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := root.Stat(".")
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("missing original root identity")
	}
	identity := artifact.FileID{Device: uint64(stat.Dev), Inode: uint64(stat.Ino)}
	facts, err := artifact.Initialize(root, identity, strings.Repeat("a", 32))
	if err != nil {
		t.Fatal(err)
	}
	files, err := artifact.Open(root, identity)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	if _, err := store.InstallArtifacts(directory, store.ArtifactInstallation{Enabled: true, ID: facts.ID, Unit: facts.Unit, Volume: facts.Volume}); err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	b, err := local.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	binding, err := backend.LocalBinding(s.Owner())
	if err != nil {
		t.Fatal(err)
	}
	target := backend.Target{Name: "local", Binding: binding, Backend: b, OutputStorage: backend.RemoteOutput}
	c, err := NewWithConfig(s, []backend.Target{target}, testEngine, Config{Artifacts: files, LocalBackend: b, LocalWorkspace: &LocalWorkspaceConfig{Target: target.Name, Binding: binding, Backend: b, Roots: root}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.DrainArtifacts(context.Background()); err != nil {
			t.Errorf("HOLD original artifact owners: %v", err)
			return
		}
		if err := b.Close(); err != nil {
			t.Errorf("HOLD original Local owner: %v", err)
			return
		}
		if err := files.Close(); err != nil {
			t.Errorf("HOLD original Files owner: %v", err)
			return
		}
		if err := s.Close(); err != nil {
			t.Errorf("HOLD original Store owner: %v", err)
			return
		}
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	})
	if !c.WorkspaceTargetEnabled(target.Name, binding) {
		t.Fatal("HOLD Linux original process ownership prerequisites unavailable")
	}
	input := workspaceUploadedInput(t, c)
	task := backend.Task{Argv: []string{"never-start"}, Workdir: &input}
	selected, _, err := c.Plan(testActor(), task)
	if err != nil || selected != target.Name {
		t.Fatalf("configured Plan=%q %v", selected, err)
	}
	quote, err := c.QuoteTask(testActor(), task, "USD", backend.TargetHour, nil)
	if err != nil || len(quote.Candidates) != 1 || !quote.Candidates[0].Eligible {
		t.Fatalf("configured Quote=%+v %v", quote, err)
	}
	j, err := c.SubmitWorkspace(testActor(), "full-four", task)
	if err != nil {
		t.Fatal(err)
	}
	var calls []*ArtifactCall
	for i := 0; i < 4; i++ {
		call, err := c.TryArtifactCall()
		if err != nil {
			t.Fatal(err)
		}
		calls = append(calls, call)
		t.Cleanup(call.Done)
	}
	state := &loopState{workers: []targetWorker{{target: target, input: make(chan operation, 1)}}, results: make(chan operationResult, 1), due: make(map[string]time.Time)}
	if err := c.schedule(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	held, err := s.Get(j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if held.Status.State != backend.Queued || len(held.Attempts) != 0 || state.workers[0].pending != nil || len(state.workers[0].input) != 0 {
		t.Fatalf("full budget began execution: %+v", held)
	}
	if _, err := c.Cancel(testActor(), j.ID); err != nil {
		t.Fatal(err)
	}
	if err := c.schedule(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	canceled, err := s.Get(j.ID)
	if err != nil || canceled.Status.State != backend.Canceled || state.workers[0].pending != nil {
		t.Fatalf("cancel blocked by full budget: %+v %v", canceled, err)
	}
	if _, err := c.tryArtifactIO(); !errors.Is(err, ErrArtifactBusy) {
		t.Fatalf("cancel invented an I/O release: %v", err)
	}
	for _, call := range calls {
		call.Done()
	}
}
