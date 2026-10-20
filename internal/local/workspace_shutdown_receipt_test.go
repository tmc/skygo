//go:build linux

package local

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
)

// receiptFixture owns this private test namespace. Its concrete originals stay
// strongly reachable on any uncertain acquisition/Close; no TempDir cleanup or
// directory name substitutes for the actual owner/error frame.
type receiptFixture struct {
	workspace              *workspaceFixture
	path                   string
	inputRoot              *os.Root
	file                   *os.File
	others                 []*Backend
	acquisition, held      error
	expectedHeld, retained bool
}

var receiptFixtureHolds struct {
	sync.Mutex
	frames []*receiptFixture
}

func (f *receiptFixture) close() error {
	if f.held != nil {
		return f.held
	}
	hold := func(err error) error { f.held = err; return err }
	if errors.Is(f.acquisition, artifact.ErrFileCloseUncertain) {
		return hold(f.acquisition)
	}
	// Backend.Close joins actual original sequencers; nonnil never permits
	// dependent borrowed Files/Root Close or namespace teardown.
	for _, b := range f.others {
		if err := b.Close(); err != nil {
			return hold(err)
		}
	}
	if f.workspace.backend != nil {
		if err := f.workspace.backend.Close(); err != nil {
			return hold(err)
		}
	}
	if f.file != nil {
		if err := f.file.Close(); err != nil {
			return hold(err)
		}
		f.file = nil
	}
	if f.inputRoot != nil {
		if err := f.inputRoot.Close(); err != nil {
			return hold(err)
		}
		f.inputRoot = nil
	}
	if f.workspace.files != nil {
		if err := f.workspace.files.Close(); err != nil {
			return hold(err)
		}
		f.workspace.files = nil
	}
	if f.workspace.root != nil {
		if err := f.workspace.root.Close(); err != nil {
			return hold(err)
		}
		f.workspace.root = nil
	}
	if f.path != "" {
		if err := os.RemoveAll(f.path); err != nil {
			return hold(err)
		}
	} // test namespace, not Store/GC credit
	return nil
}

func newReceiptFixture(t *testing.T) *receiptFixture {
	t.Helper()
	f := &receiptFixture{workspace: &workspaceFixture{slots: make(chan struct{}, 4)}}
	t.Cleanup(func() {
		if err := f.close(); err != nil {
			receiptFixtureHolds.Lock()
			if !f.retained {
				receiptFixtureHolds.frames = append(receiptFixtureHolds.frames, f)
				f.retained = true
			}
			receiptFixtureHolds.Unlock()
			if f.expectedHeld {
				t.Logf("held original receipt fixture %p path=%s error=%v", f, f.path, err)
			} else {
				t.Errorf("held original receipt fixture %p path=%s error=%v", f, f.path, err)
			}
		}
	})
	fail := func(err error) { f.acquisition = err; t.Fatal(err) }
	home, err := os.UserHomeDir()
	if err != nil {
		fail(err)
	}
	base := filepath.Join(home, "tmp", "skypilot", "20261019-workspace-shutdown-receipt", "tests")
	if err := os.MkdirAll(base, 0700); err != nil {
		fail(err)
	}
	f.path, err = os.MkdirTemp(base, "receipt-")
	if err != nil {
		fail(err)
	}
	f.workspace.backend, err = New(filepath.Join(f.path, "logs"))
	if err != nil {
		fail(err)
	}
	if !f.workspace.backend.SupportsRetry() {
		t.Skip("actual Linux pidfd/nonreaping prerequisites unavailable")
	}
	for _, name := range []string{"artifacts", "input"} {
		if err := os.Mkdir(filepath.Join(f.path, name), 0700); err != nil {
			fail(err)
		}
	}
	f.workspace.root, err = os.OpenRoot(filepath.Join(f.path, "artifacts"))
	if err != nil {
		fail(err)
	}
	info, err := f.workspace.root.Lstat(".")
	if err != nil {
		fail(err)
	}
	id, err := workspaceFileID(info)
	if err != nil {
		fail(err)
	}
	rootID := strings.Repeat("2", 32)
	if _, err := artifact.Initialize(f.workspace.root, id, rootID); err != nil {
		fail(err)
	}
	f.workspace.files, err = artifact.Open(f.workspace.root, id)
	if err != nil {
		fail(err)
	}
	f.inputRoot, err = os.OpenRoot(filepath.Join(f.path, "input"))
	if err != nil {
		fail(err)
	}
	f.file, err = f.inputRoot.Create("input")
	if err != nil {
		fail(err)
	}
	if _, err := io.WriteString(f.file, "immutable input"); err != nil {
		fail(err)
	}
	if err := f.file.Close(); err != nil {
		f.held = err
		fail(err)
	}
	f.file = nil
	var encoded bytes.Buffer
	if err := artifact.Collect(f.inputRoot, []string{"input"}, &encoded); err != nil {
		fail(err)
	}
	if err := f.inputRoot.Close(); err != nil {
		f.held = err
		fail(err)
	}
	f.inputRoot = nil
	digest := sha256.Sum256(encoded.Bytes())
	ref := artifact.Ref{ID: strings.Repeat("3", 32), SHA256: hex.EncodeToString(digest[:]), Size: int64(encoded.Len())}
	fileID, err := f.workspace.files.CreatePart(ref)
	if err != nil {
		fail(err)
	}
	if _, err := f.workspace.files.WritePart(ref, fileID, 0, 0, encoded.Bytes()); err != nil {
		fail(err)
	}
	if _, err := f.workspace.files.SealPart(ref, fileID, func() error { return nil }); err != nil {
		fail(err)
	}
	w := f.workspace
	config := WorkspaceConfig{Files: w.files, Roots: w.root, InstallationID: strings.Repeat("1", 32), RootID: rootID, CheckGrowth: func() error { return nil }, PhysicalChanged: func() {}, TryCollectionIO: func() (func(), bool) {
		select {
		case w.slots <- struct{}{}:
			return onceWorkspaceRelease(func() { <-w.slots; w.collectionReleases.Add(1) }), true
		default:
			return nil, false
		}
	}}
	if err := w.backend.ConfigureWorkspace(config); err != nil {
		fail(err)
	}
	task := backend.Task{Argv: workspaceProgram("run"), Env: map[string]string{"SKYGO_WORKSPACE_PROGRAM": "1"}, Workdir: &ref, Setup: workspaceProgram("setup"), Outputs: []string{"output"}, TimeoutSeconds: 5}
	w.binding = WorkspaceBinding{Owner: "principal-test", JobID: strings.Repeat("5", 32), AttemptID: strings.Repeat("6", 32), Binding: w.backend.workspaces.binding, RootID: rootID, Task: task, Input: ref, InputFile: fileID, ResultID: strings.Repeat("7", 32)}
	return f
}

func (f *receiptFixture) entries(t *testing.T, w *workspaceExecution, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		var err error
		f.file, err = w.root.Create(fmt.Sprintf("entry-%03d", i))
		if err != nil {
			t.Fatal(err)
		}
		if err := f.file.Close(); err != nil {
			f.held = err
			t.Fatal(err)
		}
		f.file = nil
	}
}

func TestWorkspaceShutdownReceiptDone(t *testing.T) {
	f := newReceiptFixture(t)
	w := f.workspace
	b := w.backend
	ref := w.start(t)
	o := awaitWorkspace(t, w, ref)
	b.mu.Lock()
	original := b.jobs[ref.ID]
	bound := b.workspaces.bindings[ref.ID]
	before := original.workspaceClosed
	b.mu.Unlock()
	if !o.Ready || o.Writer == nil || before {
		t.Fatal("terminal/Ready status manufactured receipt")
	}
	f.entries(t, bound, 101)
	subject := workspaceCleanupSubject(t, w, ref, o)
	origin, err := fixturePrepareWorkspaceCleanup(b, ref, subject)
	if err != nil {
		t.Fatal(err)
	}
	intent := workspaceCleanupIntent(origin)
	b.mu.Lock()
	preparedOnly := original.workspaceClosed
	b.mu.Unlock()
	if preparedOnly {
		t.Fatal("Prepare/Commit manufactured cleanup-handle closure")
	}
	progress, err := b.RemoveWorkspaceRoot(context.Background(), intent)
	if err != nil || progress.Done || progress.Visited != 100 {
		t.Fatalf("partial original quantum=%+v %v", progress, err)
	}
	b.mu.Lock()
	partial := original.workspaceClosed
	b.mu.Unlock()
	if partial {
		t.Fatal("partial original quantum published closure")
	}
	finishWorkspaceCleanup(t, b, intent)
	b.mu.Lock()
	closed, detached, deleted := original.workspaceClosed, original.workspace == nil, b.workspaces.bindings[ref.ID] == nil
	b.mu.Unlock()
	if !closed || !detached || !deleted {
		t.Fatalf("Done receipt=%v detached=%v deleted=%v", closed, detached, deleted)
	}
	if _, err := w.root.Lstat(w.binding.ResultID + ".tar"); err != nil {
		t.Fatal("root Done changed independent result", err)
	}
	if err := f.close(); err != nil {
		t.Fatal("detached positive Done failed Close audit", err)
	}
}

func TestWorkspaceShutdownReceiptHandoff(t *testing.T) {
	f := newReceiptFixture(t)
	w := f.workspace
	b := w.backend
	ref, intent := prepareWorkspaceCleanup(t, w)
	b.mu.Lock()
	original := b.jobs[ref.ID]
	c := b.workspaces.bindings[ref.ID].cleanup
	b.mu.Unlock()
	if err := b.HandoffWorkspaceCleanup(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	done, handed := c.done, c.handed
	c.mu.Unlock()
	b.mu.Lock()
	closed, detached, binding := original.workspaceClosed, original.workspace == nil, b.workspaces.bindings[ref.ID] != nil
	b.mu.Unlock()
	if !closed || !detached || !binding || done || !handed {
		t.Fatal("Handoff lost receipt or fabricated root Done")
	}
	if _, err := w.root.Lstat("workspace-" + ref.ID); err != nil {
		t.Fatal("handoff removed root", err)
	}
	if _, err := w.root.Lstat(w.binding.ResultID + ".tar"); err != nil {
		t.Fatal("handoff removed independent result", err)
	}
	if err := f.close(); err != nil {
		t.Fatal("detached positive Handoff failed Close audit", err)
	}
}

func TestWorkspaceShutdownReceiptNamespaceRefusalHandoff(t *testing.T) {
	f := newReceiptFixture(t)
	w := f.workspace
	b := w.backend
	ref := w.start(t)
	o := awaitWorkspace(t, w, ref)
	b.mu.Lock()
	original := b.jobs[ref.ID]
	bound := b.workspaces.bindings[ref.ID]
	b.mu.Unlock()
	name := ""
	for range 65 {
		if name != "" {
			name += "/"
		}
		name += "deep"
		if err := bound.root.Mkdir(name, 0700); err != nil {
			t.Fatal(err)
		}
	}
	origin, err := fixturePrepareWorkspaceCleanup(b, ref, workspaceCleanupSubject(t, w, ref, o))
	if err != nil {
		t.Fatal(err)
	}
	intent := workspaceCleanupIntent(origin)
	progress, err := b.RemoveWorkspaceRoot(context.Background(), intent)
	if err == nil || !strings.Contains(err.Error(), "depth") || progress.Done {
		t.Fatalf("depth refusal=%+v %v", progress, err)
	}
	if err := b.HandoffWorkspaceCleanup(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	closed := original.workspaceClosed
	b.mu.Unlock()
	c := bound.cleanup
	c.mu.Lock()
	stillBlocked, done := c.blocked != nil, c.done
	c.mu.Unlock()
	if !closed || !stillBlocked || done {
		t.Fatal("namespace refusal prevented positive handle handoff or gained Done")
	}
}

func TestWorkspaceShutdownReceiptCloseUncertainty(t *testing.T) {
	f := newReceiptFixture(t)
	w := f.workspace
	b := w.backend
	ref, intent := prepareWorkspaceCleanup(t, w)
	b.mu.Lock()
	original := b.jobs[ref.ID]
	bound := b.workspaces.bindings[ref.ID]
	c := bound.cleanup
	b.mu.Unlock()
	c.mu.Lock()
	directory := c.directory
	c.mu.Unlock()
	diagnostic := errors.New("policy error after positive original directory Close")
	var calls atomic.Int32
	b.workspaceCleanupOps.closeFile = func(file *os.File) error {
		if file != directory {
			return file.Close()
		}
		calls.Add(1)
		if err := file.Close(); err != nil {
			return err
		}
		return diagnostic // post-positive policy error, not native Close fault/live FD proof
	}
	err := b.HandoffWorkspaceCleanup(context.Background(), intent)
	f.expectedHeld = true
	if !errors.Is(err, artifact.ErrFileCloseUncertain) || !errors.Is(err, diagnostic) {
		t.Fatal("original Close cause lost", err)
	}
	c.mu.Lock()
	sameOwner, sameError := c.directory == directory, c.err == err
	c.mu.Unlock()
	b.mu.Lock()
	closed, associated := original.workspaceClosed, original.workspace == bound
	b.mu.Unlock()
	if closed || !associated || !sameOwner || !sameError {
		t.Fatal("uncertain Close detached original owner or published closure")
	}
	if again := b.HandoffWorkspaceCleanup(context.Background(), intent); again != err || calls.Load() != 1 {
		t.Fatal("uncertain original Close was retried or error replaced")
	}
}

func TestWorkspaceShutdownReceiptPostmarkCreatesNoJob(t *testing.T) {
	f := newReceiptFixture(t)
	w := f.workspace
	b := w.backend
	ref, intent := prepareWorkspaceCleanup(t, w)
	if err := b.HandoffWorkspaceCleanup(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	other, err := New(filepath.Join(f.path, "restarted-logs"))
	if err != nil {
		t.Fatal(err)
	}
	f.others = append(f.others, other) // same borrowed root/Files retained until this owner closes
	if err := other.ConfigureWorkspace(b.workspaces.config); err != nil {
		t.Fatal(err)
	}
	finishWorkspaceCleanup(t, other, intent)
	other.mu.Lock()
	jobs, bindings, frames := len(other.jobs), len(other.workspaces.bindings), len(other.workspaces.cleanupFrames)
	other.mu.Unlock()
	if jobs != 0 || bindings != 0 || frames != 0 {
		t.Fatal("postmark root cleanup manufactured original execution receipt")
	}
	b.mu.Lock()
	closed := b.jobs[ref.ID].workspaceClosed
	b.mu.Unlock()
	if !closed {
		t.Fatal("independent postmark owner erased earlier original Handoff receipt")
	}
}

func TestWorkspaceShutdownReceiptDoesNotHideLaterFrame(t *testing.T) {
	f := newReceiptFixture(t)
	w := f.workspace
	b := w.backend
	ref, intent := prepareWorkspaceCleanup(t, w)
	finishWorkspaceCleanup(t, b, intent)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.RemoveWorkspaceRoot(ctx, intent); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	b.mu.Lock()
	closed := b.jobs[ref.ID].workspaceClosed
	later := b.workspaces.cleanupFrames[ref.ID]
	b.mu.Unlock()
	if !closed || later == nil {
		t.Fatal("earlier receipt hid a new independent postmark cleanup frame")
	}
	later.mu.Lock()
	pending := !later.done && !later.handed
	later.mu.Unlock()
	if !pending {
		t.Fatal("receipt credited later postmark work")
	}
	finishWorkspaceCleanup(t, b, intent) // actual separate original frame must settle too
}

func TestWorkspaceShutdownReceiptMissingDetachedRefusesClose(t *testing.T) {
	f := newReceiptFixture(t)
	b := f.workspace.backend
	done := make(chan struct{})
	close(done)
	// Deliberately invalid detached DTO row: no actual positive producer or
	// receipt is synthesized. This is a refusal oracle, not native evidence.
	b.mu.Lock()
	b.jobs[strings.Repeat("e", 32)] = &execution{done: done}
	b.mu.Unlock()
	f.expectedHeld = true
	first := f.close()
	if first == nil || !strings.Contains(first.Error(), "closure receipt") || f.close() != first {
		t.Fatal("missing detached original receipt accepted or held error replaced")
	}
	// This deliberately ownerless invalid row permits repeating only the actual
	// metadata audit: no original raw File/native/root Close can be retried.
	if again := b.Close(); again == nil || !strings.Contains(again.Error(), "closure receipt") {
		t.Fatal("repeated detached-row Close lost refusal", again)
	}
}
