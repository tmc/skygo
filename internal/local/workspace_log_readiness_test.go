//go:build linux

package local

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
)

// readinessJoinWait bounds the cleanup join of the original sequencer. Expiry
// is a HOLD of every owner and the namespace, never a substitute for done.
const readinessJoinWait = 10 * time.Second

// readinessFixture owns a private test namespace under the project tmp root.
// No TempDir or old fixture cleanup is registered beneath it. Every original
// owner stays strongly reachable until positive retirement; the first
// uncertain result is cached and returned again without retry.
type readinessFixture struct {
	workspace *workspaceFixture
	path      string
	inputRoot *os.Root
	input     *os.File
	// open is the SAME growth gate the configured CheckGrowth consults.
	open     atomic.Bool
	checks   atomic.Int32
	joinWait time.Duration
	// acquisition is the setup error; an uncertain one retains everything.
	acquisition, held            error
	backendClosed, retired       bool
	expectHeld, retainedGlobally bool
}

// readinessHolds keeps held frames strongly reachable for the process life.
var readinessHolds struct {
	sync.Mutex
	frames []*readinessFixture
}

func registerReadinessCleanup(t *testing.T, f *readinessFixture) {
	t.Cleanup(func() {
		err := f.close()
		if err == nil {
			return
		}
		f.retain()
		if f.expectHeld {
			t.Logf("held readiness fixture %p path=%s error=%v", f, f.path, err)
		} else {
			t.Errorf("held readiness fixture %p path=%s error=%v", f, f.path, err)
		}
	})
}

func (f *readinessFixture) retain() {
	readinessHolds.Lock()
	defer readinessHolds.Unlock()
	if !f.retainedGlobally {
		readinessHolds.frames = append(readinessHolds.frames, f)
		f.retainedGlobally = true
	}
}

// readinessRetirable is called with b.mu held after the original done. It
// mirrors the trusted completion owner contract: done alone, Held, an unknown
// cleanup frame or any uncertain native/log/root/collection Close refuses.
func readinessRetirable(b *Backend, e *execution) error {
	w := e.workspace
	if w == nil || w.parent != e || b.workspaces == nil || b.workspaces.bindings[w.binding.AttemptID] != w {
		return errors.New("original workspace frame is not provable")
	}
	o := w.observation
	if !o.Ready || o.Held || o.Writer == nil || o.Outcome == WorkspaceUncertain {
		return errors.New("original workspace writer closure is not positive")
	}
	wr := o.Writer
	if e.status.Instance == "" || wr.InstallationID != b.workspaces.config.InstallationID || wr.RootID != w.binding.RootID || wr.JobID != w.binding.JobID ||
		wr.Execution.ID != w.binding.AttemptID || wr.Execution.Binding != w.binding.Binding || wr.Execution.Instance != e.status.Instance {
		return errors.New("original workspace writer identity mismatch")
	}
	if w.phase != nil || w.failedPhase != nil || e.cmd != nil || e.owned != nil || e.retry != nil ||
		w.release != nil || w.collectionRelease != nil || w.rootErr != nil || w.collectionErr != nil || w.candidate != nil ||
		!w.logCloseAttempted || w.logCloseErr != nil || w.taskDeadlineOpen || w.physical.Uncertain {
		return errors.New("original workspace owners remain held")
	}
	if w.cleanup != nil || w.cleanupBusy || w.cleanupOwned || len(b.workspaces.cleanupFrames) != 0 {
		return errors.New("original workspace cleanup frame is unknown")
	}
	if out := e.output; out != nil {
		out.mu.Lock()
		closed := out.closed && out.err == nil && out.markerCloseErr == nil
		out.mu.Unlock()
		if !closed {
			return errors.New("original workspace log owner remains held")
		}
	}
	never := !w.rootAttempted && !w.created && w.root == nil && w.directory == nil
	owned := w.rootAttempted && w.created && w.root != nil && w.directory != nil && w.rootInfo != nil
	if !never && !owned {
		return errors.New("original workspace root identity is unknown")
	}
	return nil
}

// close opens the same gate, wakes the same sequencer and joins it within
// joinWait. Only an admitted original frame has its quiescent root handles
// closed, then Backend.Close must return nil before shared Files/Root and the
// namespace are touched. A pointer is cleared only after its positive Close.
func (f *readinessFixture) close() error {
	if f.retired {
		return nil
	}
	if f.held != nil {
		return f.held
	}
	hold := func(err error) error { f.held = err; return err }
	if errors.Is(f.acquisition, artifact.ErrFileCloseUncertain) {
		return hold(f.acquisition)
	}
	if b := f.workspace.backend; b != nil && !f.backendClosed {
		f.open.Store(true)
		b.mu.Lock()
		e := b.jobs[f.workspace.binding.AttemptID]
		if e != nil && e.workspace != nil {
			wakeWorkspace(e.workspace)
		}
		b.mu.Unlock()
		if e != nil {
			timer := time.NewTimer(f.joinWait)
			select {
			case <-e.done:
				timer.Stop()
			case <-timer.C:
				return hold(errors.New("original workspace sequencer not joined"))
			}
			b.mu.Lock()
			err := readinessRetirable(b, e)
			var w *workspaceExecution
			var directory *os.File
			var root *os.Root
			if err == nil {
				w, directory, root = e.workspace, e.workspace.directory, e.workspace.root
			}
			b.mu.Unlock()
			if err != nil {
				return hold(err)
			}
			if directory != nil {
				if err := directory.Close(); err != nil {
					return hold(err)
				}
				b.mu.Lock()
				w.directory = nil
				b.mu.Unlock()
			}
			if root != nil {
				if err := root.Close(); err != nil {
					return hold(err)
				}
				b.mu.Lock()
				w.root = nil
				b.mu.Unlock()
			}
		}
		if err := b.Close(); err != nil {
			return hold(err)
		}
		f.backendClosed = true
	}
	if f.input != nil {
		if err := f.input.Close(); err != nil {
			return hold(err)
		}
		f.input = nil
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
	} // test namespace only, not Store/GC credit
	f.retired = true
	return nil
}

func readinessNamespace(t *testing.T, f *readinessFixture, prefix string) {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		f.acquisition = err
		t.Fatal(err)
	}
	base := filepath.Join(home, "tmp", "skypilot", "20261028-workspace-log-readiness", "tests")
	if err := os.MkdirAll(base, 0700); err != nil {
		f.acquisition = err
		t.Fatal(err)
	}
	f.path, err = os.MkdirTemp(base, prefix)
	if err != nil {
		f.acquisition = err
		t.Fatal(err)
	}
}

func newReadinessFixture(t *testing.T) *readinessFixture {
	t.Helper()
	f := &readinessFixture{workspace: &workspaceFixture{slots: make(chan struct{}, 4)}, joinWait: readinessJoinWait}
	f.open.Store(true)
	registerReadinessCleanup(t, f)
	fail := func(err error) { f.acquisition = err; t.Fatal(err) }
	readinessNamespace(t, f, "readiness-")
	w := f.workspace
	var err error
	w.backend, err = New(filepath.Join(f.path, "logs"))
	if err != nil {
		fail(err)
	}
	if !w.backend.SupportsRetry() {
		t.Skip("actual Linux pidfd/nonreaping prerequisites unavailable")
	}
	for _, name := range []string{"artifacts", "input"} {
		if err := os.Mkdir(filepath.Join(f.path, name), 0700); err != nil {
			fail(err)
		}
		if err := os.Chmod(filepath.Join(f.path, name), 0700); err != nil {
			fail(err)
		}
	}
	w.root, err = os.OpenRoot(filepath.Join(f.path, "artifacts"))
	if err != nil {
		fail(err)
	}
	info, err := w.root.Lstat(".")
	if err != nil {
		fail(err)
	}
	id, err := workspaceFileID(info)
	if err != nil {
		fail(err)
	}
	rootID := strings.Repeat("2", 32)
	// An uncertain Initialize/Open may return no pointer; the acquisition
	// error then retains the borrowed root and namespace.
	if _, err := artifact.Initialize(w.root, id, rootID); err != nil {
		fail(err)
	}
	w.files, err = artifact.Open(w.root, id)
	if err != nil {
		fail(err)
	}
	f.inputRoot, err = os.OpenRoot(filepath.Join(f.path, "input"))
	if err != nil {
		fail(err)
	}
	f.input, err = f.inputRoot.Create("input")
	if err != nil {
		fail(err)
	}
	if _, err := io.WriteString(f.input, "immutable input"); err != nil {
		fail(err)
	}
	if err := f.input.Close(); err != nil {
		f.held = err
		fail(err)
	}
	f.input = nil
	var encoded bytes.Buffer
	if err := artifact.Collect(f.inputRoot, []string{"input"}, &encoded); err != nil {
		fail(err)
	}
	digest := sha256.Sum256(encoded.Bytes())
	ref := artifact.Ref{ID: strings.Repeat("3", 32), SHA256: hex.EncodeToString(digest[:]), Size: int64(encoded.Len())}
	fileID, err := w.files.CreatePart(ref)
	if err != nil {
		fail(err)
	}
	if _, err := w.files.WritePart(ref, fileID, 0, 0, encoded.Bytes()); err != nil {
		fail(err)
	}
	if _, err := w.files.SealPart(ref, fileID, func() error { return nil }); err != nil {
		fail(err)
	} // test-owned input admission
	closed := errors.New("test growth gate closed")
	config := WorkspaceConfig{Files: w.files, Roots: w.root, InstallationID: strings.Repeat("1", 32), RootID: rootID, CheckGrowth: func() error {
		f.checks.Add(1)
		if !f.open.Load() {
			return closed
		}
		return nil
	}, PhysicalChanged: func() {}, TryCollectionIO: func() (func(), bool) {
		select {
		case w.slots <- struct{}{}:
			return onceWorkspaceRelease(func() { <-w.slots; w.collectionReleases.Add(1) }), true
		default:
			return nil, false
		}
	}} // test-owned capacity; not a production cache
	if err := w.backend.ConfigureWorkspace(config); err != nil {
		fail(err)
	}
	task := backend.Task{Argv: workspaceProgram("run"), Env: map[string]string{"SKYGO_WORKSPACE_PROGRAM": "1"}, Workdir: &ref, Setup: workspaceProgram("setup"), Outputs: []string{"output"}, TimeoutSeconds: 5}
	w.binding = WorkspaceBinding{Owner: "principal-test", JobID: strings.Repeat("5", 32), AttemptID: strings.Repeat("6", 32), Binding: w.backend.workspaces.binding, RootID: rootID, Task: task, Input: ref, InputFile: fileID, ResultID: strings.Repeat("7", 32)}
	return f
}

func awaitWorkspaceState(t *testing.T, f *workspaceFixture, ref backend.Reference, ready func(backend.Status, WorkspaceObservation) bool) (backend.Status, WorkspaceObservation) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for {
		status, err := f.backend.Observe(context.Background(), ref)
		if err != nil {
			t.Fatal(err)
		}
		o, err := f.backend.WorkspaceObservation(ref)
		if err != nil {
			t.Fatal(err)
		}
		if ready(status, o) {
			return status, o
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("owned workspace state not reached: status=%+v phase=%q", status, o.Phase)
		}
		time.Sleep(time.Millisecond)
	}
}

// The real CheckGrowth seam holds the same sequencer before output creation.
// Starting with the stable Instance is reported until the actual log exists.
func TestWorkspaceLogReadinessStartingUntilOutputAttached(t *testing.T) {
	rf := newReadinessFixture(t)
	f := rf.workspace
	var starts atomic.Int32
	ops := f.backend.defaultOps
	f.backend.defaultOps.start = func(cmd *exec.Cmd) error { starts.Add(1); return ops.start(cmd) }
	rf.open.Store(false)
	ref := f.start(t)
	log := filepath.Join(f.backend.directory, ref.ID+".log")
	held := func(n int32) {
		t.Helper()
		status, o := awaitWorkspaceState(t, f, ref, func(_ backend.Status, o WorkspaceObservation) bool {
			return rf.checks.Load() >= n && o.Phase == "waiting-materialization"
		})
		if status.State != backend.Starting || status.Instance != ref.Instance {
			t.Fatalf("held workspace advertised %+v", status)
		}
		if o.Ready || o.Held || o.Writer != nil || o.Candidate != nil || o.Status.State != backend.Starting || o.Status.Instance != ref.Instance {
			t.Fatalf("held observation=%+v", o)
		}
		if _, err := os.Lstat(log); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("output exists before admission: %v", err)
		}
		// Control maps exactly this to empty incomplete STARTING offset 0.
		if _, err := f.backend.Logs(context.Background(), ref, 0, 1024); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("pre-output log read=%v", err)
		}
		if starts.Load() != 0 {
			t.Fatal("native writer started before output")
		}
	}
	held(1)
	if f.stageReleases.Load() != 1 || len(f.slots) != 0 {
		t.Fatalf("unused owner not yielded: stage=%d slots=%d", f.stageReleases.Load(), len(f.slots))
	}
	if err := f.backend.Start(context.Background(), ref.ID, ref.Task); err == nil {
		t.Fatal("second Start accepted")
	}
	// A refused resume attempt keeps the same original Starting frame.
	if err := f.backend.ResumeWorkspacePublication(ref); err != nil {
		t.Fatal(err)
	}
	held(2)
	rf.open.Store(true)
	if err := f.backend.ResumeWorkspacePublication(ref); err != nil {
		t.Fatal(err)
	}
	status, _ := awaitWorkspaceState(t, f, ref, func(s backend.Status, _ WorkspaceObservation) bool { return s.State != backend.Starting })
	if status.State != backend.Running || status.Instance != ref.Instance {
		t.Fatalf("output publication advertised %+v", status)
	}
	if info, err := os.Lstat(log); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("Running without actual output: %v", err)
	}
	o := awaitWorkspace(t, f, ref)
	if !o.Ready || o.Outcome != WorkspaceTaskExit || o.Status.State != backend.Succeeded || o.Writer == nil || o.Status.Instance != ref.Instance {
		t.Fatalf("completion=%+v", o)
	}
	if starts.Load() != 2 {
		t.Fatalf("setup/task starts=%d", starts.Load())
	}
	data, err := os.ReadFile(log)
	if err != nil || strings.Count(string(data), "setup output") != 1 || strings.Count(string(data), "task output") != 1 {
		t.Fatalf("shared log=%q %v", data, err)
	}
	// Terminal stays behind trusted commit; the parent phase never regresses.
	if status, err := f.backend.Observe(context.Background(), ref); err != nil || status.State != backend.Running || status.Instance != ref.Instance {
		t.Fatalf("uncommitted Observe=%+v %v", status, err)
	}
	if err := f.backend.Start(context.Background(), ref.ID, ref.Task); err == nil || starts.Load() != 2 {
		t.Fatal("setup/task restarted")
	}
}

// A refused exclusive output open never promotes the parent. The foreign file
// is not adopted, and the failed outcome stays behind trusted commit.
func TestWorkspaceLogReadinessFailedOutputOpenStaysStarting(t *testing.T) {
	rf := newReadinessFixture(t)
	f := rf.workspace
	var starts atomic.Int32
	ops := f.backend.defaultOps
	f.backend.defaultOps.start = func(cmd *exec.Cmd) error { starts.Add(1); return ops.start(cmd) }
	log := filepath.Join(f.backend.directory, f.binding.AttemptID+".log")
	if err := os.WriteFile(log, []byte("foreign"), 0600); err != nil {
		t.Fatal(err)
	}
	ref := f.start(t)
	o := awaitWorkspace(t, f, ref)
	if !o.Ready || o.Outcome != WorkspaceMaterializationFailure || o.Status.State != backend.Failed || o.Candidate != nil {
		t.Fatalf("failed open=%+v", o)
	}
	status, err := f.backend.Observe(context.Background(), ref)
	if err != nil || status.State != backend.Starting || status.Instance != ref.Instance {
		t.Fatalf("failed output promoted parent: %+v %v", status, err)
	}
	f.backend.mu.Lock()
	output := f.backend.jobs[ref.ID].output
	f.backend.mu.Unlock()
	if output != nil || starts.Load() != 0 {
		t.Fatal("foreign output adopted or task started")
	}
	if data, err := os.ReadFile(log); err != nil || string(data) != "foreign" {
		t.Fatalf("foreign file changed: %q %v", data, err)
	}
}

// newModeledReadinessFixture builds a MODELED original frame: a hand-built
// Backend/execution with real private namespace, shared Root and workspace
// root/directory handles. It exercises only the cleanup admission and
// retention logic; it is not native fault or production close evidence.
func newModeledReadinessFixture(t *testing.T, joined bool, mutate func(*workspaceExecution)) *readinessFixture {
	t.Helper()
	f := &readinessFixture{workspace: &workspaceFixture{slots: make(chan struct{}, 4)}, joinWait: 10 * time.Millisecond}
	f.open.Store(true)
	registerReadinessCleanup(t, f)
	fail := func(err error) { f.acquisition = err; t.Fatal(err) }
	readinessNamespace(t, f, "model-")
	var err error
	if f.workspace.root, err = os.OpenRoot(f.path); err != nil {
		fail(err)
	}
	if err := f.workspace.root.Mkdir("workspace", 0700); err != nil {
		fail(err)
	}
	binding := WorkspaceBinding{JobID: strings.Repeat("5", 32), AttemptID: strings.Repeat("6", 32), Binding: "modeled-binding", RootID: strings.Repeat("2", 32)}
	e := &execution{status: backend.Status{State: backend.Running, Instance: strings.Repeat("a", 32)}, done: make(chan struct{})}
	w := &workspaceExecution{binding: binding, parent: e, wake: make(chan struct{}), rootAttempted: true, created: true, logCloseAttempted: true}
	e.workspace = w
	b := &Backend{jobs: map[string]*execution{binding.AttemptID: e}, workspaces: &workspaceState{config: WorkspaceConfig{InstallationID: strings.Repeat("1", 32)}, bindings: map[string]*workspaceExecution{binding.AttemptID: w}}}
	f.workspace.backend, f.workspace.binding = b, binding
	if w.root, err = f.workspace.root.OpenRoot("workspace"); err != nil {
		fail(err)
	}
	if w.directory, err = openWorkspaceDirectory(w.root); err != nil {
		fail(err)
	}
	if w.rootInfo, err = w.root.Lstat("."); err != nil {
		fail(err)
	}
	w.observation = WorkspaceObservation{Phase: "complete", Ready: true, Outcome: WorkspaceTaskExit, Status: backend.Status{State: backend.Succeeded, Instance: e.status.Instance},
		Writer: &WorkspaceWriterClosure{InstallationID: strings.Repeat("1", 32), RootID: binding.RootID, JobID: binding.JobID, Execution: backend.Reference{ID: binding.AttemptID, Binding: binding.Binding, Instance: e.status.Instance}}}
	if mutate != nil {
		mutate(w)
	}
	if joined {
		close(e.done)
	}
	return f
}

// MODELED: done+Held, done Ready with an uncertain original Close, and an
// unjoined sequencer each refuse retirement. The same error is returned without
// retry; namespace, shared Root and original root/directory stay owned and
// reachable for the process life. Three held frames per run, no more.
func TestWorkspaceLogReadinessCleanupRetainsModeledHold(t *testing.T) {
	uncertain := errors.Join(artifact.ErrFileCloseUncertain, errors.New("MODELED uncertain original Close"))
	for _, tt := range []struct {
		name   string
		joined bool
		mutate func(*workspaceExecution)
	}{
		{"MODELED done Held uncertain log Close", true, func(w *workspaceExecution) {
			w.observation.Ready, w.observation.Held, w.observation.Writer = false, true, nil
			w.observation.Outcome = WorkspaceUncertain
			w.observation.Status.State = backend.Unknown
			w.logCloseErr = uncertain
			w.physical.Uncertain = true
		}},
		{"MODELED done Ready writer uncertain root Close", true, func(w *workspaceExecution) {
			w.rootErr = uncertain
		}},
		{"MODELED unjoined original", false, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newModeledReadinessFixture(t, tt.joined, tt.mutate)
			f.expectHeld = true
			b := f.workspace.backend
			w := b.workspaces.bindings[f.workspace.binding.AttemptID]
			shared, root, directory := f.workspace.root, w.root, w.directory
			err := f.close()
			if err == nil {
				t.Fatal("held original frame retired")
			}
			if again := f.close(); again != err {
				t.Fatalf("held cleanup retried: first=%v again=%v", err, again)
			}
			f.retain()
			if f.retired || f.backendClosed || b.closed || f.workspace.root != shared || w.root != root || w.directory != directory || b.jobs[f.workspace.binding.AttemptID] != w.parent {
				t.Fatal("held original owner released")
			}
			if _, err := shared.Lstat("workspace"); err != nil {
				t.Fatalf("held shared Root closed: %v", err)
			}
			if _, err := root.Lstat("."); err != nil {
				t.Fatalf("held original root closed: %v", err)
			}
			if _, err := directory.Stat(); err != nil {
				t.Fatalf("held original directory closed: %v", err)
			}
			if info, err := os.Lstat(f.path); err != nil || !info.IsDir() {
				t.Fatalf("held namespace removed: %v", err)
			}
		})
	}
}

// MODELED fresh independent twin: an exact joined Ready writer frame has its
// real original handles positively closed before Backend.Close, shared Root
// and namespace removal. No held frame is reset or reused.
func TestWorkspaceLogReadinessCleanupRetiresModeledClosedTwin(t *testing.T) {
	f := newModeledReadinessFixture(t, true, nil)
	b := f.workspace.backend
	w := b.workspaces.bindings[f.workspace.binding.AttemptID]
	if err := f.close(); err != nil {
		t.Fatal(err)
	}
	if !f.retired || !f.backendClosed || !b.closed || w.root != nil || w.directory != nil || f.workspace.root != nil {
		t.Fatal("positive original frame not retired")
	}
	if _, err := os.Lstat(f.path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("retired namespace remains: %v", err)
	}
	if err := f.close(); err != nil {
		t.Fatal(err)
	}
}
