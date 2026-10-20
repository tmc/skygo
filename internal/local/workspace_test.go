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
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
)

// This real owned Go child runs only in its fresh private attempt cwd. It never
// signals external processes or discovers/reconstructs another process identity.
func TestWorkspaceProgram(t *testing.T) {
	if os.Getenv("SKYGO_WORKSPACE_PROGRAM") != "1" {
		return
	}
	mode := os.Args[len(os.Args)-1]
	switch mode {
	case "setup", "setup-fail":
		if _, err := os.Stat("setup-once"); !errors.Is(err, os.ErrNotExist) {
			os.Exit(10)
		}
		if err := os.WriteFile("setup-once", []byte("once"), 0600); err != nil {
			os.Exit(11)
		}
		fmt.Fprintln(os.Stdout, "setup output")
		if mode == "setup-fail" {
			os.Exit(7)
		}
	case "run", "run-fail":
		if data, err := os.ReadFile("input"); err != nil || string(data) != "immutable input" {
			os.Exit(12)
		}
		if data, err := os.ReadFile("setup-once"); err != nil || string(data) != "once" {
			os.Exit(13)
		}
		if err := os.Mkdir("output", 0700); err != nil {
			os.Exit(14)
		}
		if err := os.WriteFile("output/result", []byte("verified result"), 0600); err != nil {
			os.Exit(15)
		}
		fmt.Fprintln(os.Stdout, "task output")
		if mode == "run-fail" {
			os.Exit(9)
		}
	case "phase-gate":
		if err := os.WriteFile("setup-once", []byte("once"), 0600); err != nil {
			os.Exit(21)
		}
		deadline := time.Now().Add(8 * time.Second)
		for {
			if _, err := os.Stat("release-phase"); err == nil {
				break
			}
			if !time.Now().Before(deadline) {
				os.Exit(22)
			}
			time.Sleep(time.Millisecond)
		}
	case "hold-pipe":
		time.Sleep(8 * time.Second)
	case "signal":
		self, err := os.FindProcess(os.Getpid())
		if err != nil {
			os.Exit(18)
		}
		if err := self.Kill(); err != nil {
			os.Exit(19)
		}
		os.Exit(20)
	case "wait":
		if err := os.WriteFile("started", []byte("owned"), 0600); err != nil {
			os.Exit(16)
		}
		time.Sleep(8 * time.Second)
	default:
		os.Exit(17)
	}
	os.Exit(0)
}

func workspaceProgram(mode string) []string {
	exe, err := os.Executable()
	if err != nil {
		panic(err)
	}
	return []string{exe, "-test.run=^TestWorkspaceProgram$", "--", mode}
}

type workspaceFixture struct {
	backend            *Backend
	files              *artifact.Files
	root               *os.Root
	binding            WorkspaceBinding
	slots              chan struct{}
	stageReleases      atomic.Int32
	collectionReleases atomic.Int32
	expectHeld         bool
}

func newWorkspaceFixture(t *testing.T) *workspaceFixture {
	return newWorkspaceProducerFixture(t, ServerLifetime{})
}

func newWorkspaceProducerFixture(t *testing.T, producer ServerLifetime) *workspaceFixture {
	t.Helper()
	b, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if !b.SupportsRetry() {
		t.Skip("actual Linux pidfd/nonreaping prerequisites unavailable")
	}
	// TempDir does not promise private mode; the workspace root requires it.
	rootDirectory := t.TempDir()
	if err := os.Chmod(rootDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(rootDirectory)
	if err != nil {
		t.Fatal(err)
	}
	info, err := root.Lstat(".")
	if err != nil {
		t.Fatal(err)
	}
	id, err := workspaceFileID(info)
	if err != nil {
		t.Fatal(err)
	}
	rootID := strings.Repeat("2", 32)
	if _, err := artifact.Initialize(root, id, rootID); err != nil {
		t.Fatal(err)
	}
	files, err := artifact.Open(root, id)
	if err != nil {
		t.Fatal(err)
	}
	inputRoot, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer inputRoot.Close()
	input, err := inputRoot.Create("input")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(input, "immutable input"); err != nil {
		t.Fatal(err)
	}
	if err := input.Close(); err != nil {
		t.Fatal(err)
	}
	var encoded bytes.Buffer
	if err := artifact.Collect(inputRoot, []string{"input"}, &encoded); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encoded.Bytes())
	ref := artifact.Ref{ID: strings.Repeat("3", 32), SHA256: hex.EncodeToString(digest[:]), Size: int64(encoded.Len())}
	fileID, err := files.CreatePart(ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := files.WritePart(ref, fileID, 0, 0, encoded.Bytes()); err != nil {
		t.Fatal(err)
	}
	if _, err := files.SealPart(ref, fileID, func() error { return nil }); err != nil {
		t.Fatal(err)
	} // test-owned input admission
	f := &workspaceFixture{backend: b, files: files, root: root, slots: make(chan struct{}, 4)}
	config := WorkspaceConfig{Files: files, Roots: root, InstallationID: strings.Repeat("1", 32), RootID: rootID, ServerLifetime: producer, CheckGrowth: func() error { return nil }, PhysicalChanged: func() {}, TryCollectionIO: func() (func(), bool) {
		select {
		case f.slots <- struct{}{}:
			return onceWorkspaceRelease(func() { <-f.slots; f.collectionReleases.Add(1) }), true
		default:
			return nil, false
		}
	}} // test-owned capacity; not a production cache
	if err := b.ConfigureWorkspace(config); err != nil {
		t.Fatal(err)
	}
	task := backend.Task{Argv: workspaceProgram("run"), Env: map[string]string{"SKYGO_WORKSPACE_PROGRAM": "1"}, Workdir: &ref, Setup: workspaceProgram("setup"), Outputs: []string{"output"}, TimeoutSeconds: 5}
	f.binding = WorkspaceBinding{Owner: "principal-test", JobID: strings.Repeat("5", 32), AttemptID: strings.Repeat("6", 32), Binding: b.workspaces.binding, RootID: rootID, Task: task, Input: ref, InputFile: fileID, ResultID: strings.Repeat("7", 32)}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			expected := "workspace root cleanup remains pending"
			if f.expectHeld {
				expected += "\nworkspace ownership remains held"
			}
			if err.Error() != expected {
				t.Error(err)
			}
		}
		// Test owns all rows and actual sequencers are joined before these original
		// readonly root descriptors close; this is not production deletion credit.
		b.mu.Lock()
		var owned []*workspaceExecution
		for _, w := range b.workspaces.bindings {
			owned = append(owned, w)
		}
		b.mu.Unlock()
		for _, w := range owned {
			if w.parent != nil {
				select {
				case <-w.parent.done:
				default:
					t.Error("active original sequence retained during failed test cleanup")
					continue
				}
			}
			if errors.Is(w.rootErr, artifact.ErrFileCloseUncertain) || errors.Is(w.collectionErr, artifact.ErrFileCloseUncertain) {
				t.Error("uncertain original filesystem owner retained")
				continue
			}
			if w.directory != nil {
				if err := w.directory.Close(); err != nil {
					t.Error(err)
				}
			}
			if w.root != nil {
				if err := w.root.Close(); err != nil {
					t.Error(err)
				}
			}
			// Prepare moved these SAME original handles into the cleanup frame.
			// Fixture teardown closes only positively quiescent originals; it
			// supplies no namespace removal, Store credit or unknown Close retry.
			if c := w.cleanup; c != nil {
				b.mu.Lock()
				busy := c.busy
				b.mu.Unlock()
				if busy {
					t.Error("active original cleanup owner retained")
					continue
				}
				c.mu.Lock()
				if c.err == nil {
					files := make(map[*os.File]bool)
					roots := make(map[*os.Root]bool)
					for _, frame := range c.stack {
						if frame.dir != nil {
							files[frame.dir] = true
						}
						if frame.root != nil {
							roots[frame.root] = true
						}
					}
					if c.directory != nil {
						files[c.directory] = true
					}
					if c.parentDir != nil {
						files[c.parentDir] = true
					}
					if c.root != nil {
						roots[c.root] = true
					}
					if c.parent != nil {
						roots[c.parent] = true
					}
					for file := range files {
						if err := file.Close(); err != nil {
							t.Error(err)
						}
					}
					for root := range roots {
						if err := root.Close(); err != nil {
							t.Error(err)
						}
					}
				}
				c.mu.Unlock()
			}
		}
		if err := files.Close(); err != nil {
			t.Error(err)
			return
		}
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	})
	return f
}

func (f *workspaceFixture) start(t *testing.T) backend.Reference {
	t.Helper()
	f.slots <- struct{}{}
	if err := f.backend.BindWorkspace(f.binding, func() { <-f.slots; f.stageReleases.Add(1) }); err != nil {
		<-f.slots
		t.Fatal(err)
	}
	if err := f.backend.Start(context.Background(), f.binding.AttemptID, f.binding.Task); err != nil {
		t.Fatal(err)
	}
	s, err := f.backend.Observe(context.Background(), backend.Reference{ID: f.binding.AttemptID})
	if err != nil || s.Instance == "" {
		t.Fatalf("installed original Instance=%+v %v", s, err)
	}
	return backend.Reference{ID: f.binding.AttemptID, Binding: f.binding.Binding, Instance: s.Instance, Task: backend.CloneTask(f.binding.Task)}
}
func awaitWorkspace(t *testing.T, f *workspaceFixture, ref backend.Reference) WorkspaceObservation {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for {
		observation, err := f.backend.WorkspaceObservation(ref)
		if err != nil {
			t.Fatal(err)
		}
		if observation.Ready || observation.Held {
			return observation
		}
		if !time.Now().Before(deadline) {
			t.Fatal("owned workspace did not finish")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWorkspaceSequenceAndStableCandidate(t *testing.T) {
	f := newWorkspaceFixture(t)
	ops := f.backend.defaultOps
	var starts atomic.Int32
	f.backend.defaultOps.start = func(cmd *exec.Cmd) error {
		starts.Add(1)
		if len(f.slots) != 0 {
			return errors.New("staging permit retained during execution")
		}
		return ops.start(cmd)
	}
	ref := f.start(t)
	first := awaitWorkspace(t, f, ref)
	if first.Held || first.Outcome != WorkspaceTaskExit || first.Status.State != backend.Succeeded || first.Writer == nil || first.Candidate == nil || first.Status.Termination != backend.UnknownTermination {
		t.Fatalf("completion=%+v", first)
	}
	if starts.Load() != 2 || f.stageReleases.Load() != 1 || f.collectionReleases.Load() != 1 || len(f.slots) != 0 {
		t.Fatalf("ownership starts=%d stage=%d collect=%d slots=%d", starts.Load(), f.stageReleases.Load(), f.collectionReleases.Load(), len(f.slots))
	}
	second, err := f.backend.WorkspaceObservation(ref)
	if err != nil || *first.Candidate != *second.Candidate {
		t.Fatal("stable owned candidate replaced")
	}
	log, err := os.ReadFile(filepath.Join(f.backend.directory, ref.ID+".log"))
	if err != nil || strings.Count(string(log), "setup output") != 1 || strings.Count(string(log), "task output") != 1 {
		t.Fatalf("shared log=%q %v", log, err)
	}
	generic, err := f.backend.Observe(context.Background(), ref)
	if err != nil || generic.State != backend.Running {
		t.Fatal("terminal published before atomic completion")
	}
	if err := f.backend.Start(context.Background(), ref.ID, ref.Task); err == nil || starts.Load() != 2 {
		t.Fatal("setup/task restarted")
	}
	reader, err := f.files.OpenObject(first.Candidate.Manifest.Reference, first.Candidate.File)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, first.Candidate.Manifest.Reference.Size)
	if _, err := reader.ReadAt(data, 0); err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	// Extraction requires an owned private destination, independent of umask.
	destination := t.TempDir()
	if err := os.Chmod(destination, 0700); err != nil {
		t.Fatal(err)
	}
	dst, err := os.OpenRoot(destination)
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	if err := artifact.Extract(dst, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	result, err := dst.Open("output/result")
	if err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(result)
	result.Close()
	if err != nil || string(content) != "verified result" {
		t.Fatal("result bytes differ")
	}
	if _, err := fixturePrepareWorkspaceCleanup(f.backend, ref, workspaceCleanupSubject(t, f, ref, first)); err != nil {
		t.Fatal(err)
	}
	generic, err = f.backend.Observe(context.Background(), ref)
	if err != nil || generic.State != backend.Succeeded {
		t.Fatal("completion acknowledgement not projected")
	}
}

func TestWorkspaceSetupFailureNeverRunsTask(t *testing.T) {
	f := newWorkspaceFixture(t)
	f.binding.Task.Setup = workspaceProgram("setup-fail")
	var starts atomic.Int32
	ops := f.backend.defaultOps
	f.backend.defaultOps.start = func(cmd *exec.Cmd) error { starts.Add(1); return ops.start(cmd) }
	o := awaitWorkspace(t, f, f.start(t))
	if !o.Ready || o.Outcome != WorkspaceSetupFailure || o.Status.State != backend.Failed || o.Candidate != nil || o.Writer == nil || o.Status.Termination != backend.UnknownTermination || starts.Load() != 1 {
		t.Fatalf("setup failure=%+v starts=%d", o, starts.Load())
	}
}

func TestWorkspaceCancelDuringStarting(t *testing.T) {
	f := newWorkspaceFixture(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(resume) }) }
	t.Cleanup(release)
	ops := f.backend.defaultOps
	var starts atomic.Int32
	f.backend.defaultOps.start = func(cmd *exec.Cmd) error { starts.Add(1); close(entered); <-resume; return ops.start(cmd) }
	ref := f.start(t) // Start must return before the long staging/start owner finishes.
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("actual Start barrier not reached")
	}
	if err := f.backend.Cancel(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	release()
	o := awaitWorkspace(t, f, ref)
	if !o.Ready || o.Outcome != WorkspaceCanceled || o.Status.State != backend.Canceled || o.Candidate != nil || starts.Load() != 1 {
		t.Fatalf("pending cancellation=%+v starts=%d", o, starts.Load())
	}
}

func TestWorkspaceBindingRejectsCallerMutation(t *testing.T) {
	f := newWorkspaceFixture(t)
	f.slots <- struct{}{}
	if err := f.backend.BindWorkspace(f.binding, func() { <-f.slots; f.stageReleases.Add(1) }); err != nil {
		t.Fatal(err)
	}
	f.binding.Task.Argv[0] = "untrusted replacement"
	if err := f.backend.Start(context.Background(), f.binding.AttemptID, f.binding.Task); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("mutated accepted task=%v", err)
	}
	// No I/O started. The test explicitly ends its transferred original permit;
	// reservation/root adoption or production manual settlement is not implied.
	f.backend.workspaces.bindings[f.binding.AttemptID].release()
	if _, err := os.Stat(filepath.Join(f.backend.directory, f.binding.AttemptID+".log")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("changed task launched output")
	}
}

func TestWorkspaceTimeoutCoversSetup(t *testing.T) {
	f := newWorkspaceFixture(t)
	f.binding.Task.Setup = workspaceProgram("wait")
	f.binding.Task.TimeoutSeconds = 1
	ops := f.backend.defaultOps
	var starts atomic.Int32
	f.backend.defaultOps.start = func(cmd *exec.Cmd) error { starts.Add(1); return ops.start(cmd) }
	o := awaitWorkspace(t, f, f.start(t))
	if !o.Ready || o.Outcome != WorkspaceTimeout || o.Status.State != backend.Failed || o.Writer == nil || o.Candidate != nil || o.Status.Termination != backend.UnknownTermination || starts.Load() != 1 {
		t.Fatalf("timeout=%+v starts=%d", o, starts.Load())
	}
}

func TestWorkspaceFailedPinKeepsOriginalOwner(t *testing.T) {
	f := newWorkspaceFixture(t)
	f.expectHeld = true
	f.binding.Task.Setup = workspaceProgram("wait")
	injected := errors.New("actual leader pin failed")
	var groupSignals atomic.Int32
	f.backend.defaultOps.wait = func(cmd *exec.Cmd) error { return injected }
	ops := f.backend.defaultOps
	f.backend.defaultOps.kill = func(cmd *exec.Cmd) error { groupSignals.Add(1); return ops.kill(cmd) }
	ref := f.start(t)
	o := awaitWorkspace(t, f, ref)
	if !o.Held || o.Writer != nil || o.Candidate != nil || groupSignals.Load() != 0 {
		t.Fatalf("unproved pin gained authority: %+v signals=%d", o, groupSignals.Load())
	}
	f.backend.mu.Lock()
	w := f.backend.jobs[ref.ID].workspace
	phase := w.failedPhase
	f.backend.mu.Unlock()
	if phase == nil || phase.cmd == nil || phase.cmd.ProcessState == nil || phase.cmd.Args != nil || phase.cmd.Env != nil {
		t.Fatal("original uncertain owner lost or payload retained")
	}
	select {
	case <-phase.owned.reaped:
	default:
		t.Fatal("test did not join actual original leader")
	}
}

func TestWorkspaceCollectionBusyDoesNotRerun(t *testing.T) {
	f := newWorkspaceFixture(t)
	ops := f.backend.defaultOps
	var once sync.Once
	var starts atomic.Int32
	f.backend.defaultOps.start = func(cmd *exec.Cmd) error {
		starts.Add(1)
		once.Do(func() {
			for i := 0; i < 4; i++ {
				f.slots <- struct{}{}
			}
		}) // four separately owned test calls
		return ops.start(cmd)
	}
	defer func() {
		for len(f.slots) != 0 {
			<-f.slots
		}
	}()
	ref := f.start(t)
	o := awaitWorkspacePhase(t, f, ref, "waiting-collection")
	if o.Ready || o.Held || o.Writer != nil || o.Candidate != nil || starts.Load() != 2 {
		t.Fatalf("collection busy=%+v starts=%d", o, starts.Load())
	}
	if err := f.backend.Start(context.Background(), ref.ID, ref.Task); err == nil || starts.Load() != 2 {
		t.Fatal("collection pressure replayed task")
	}
	<-f.slots
	if err := f.backend.ResumeWorkspacePublication(ref); err != nil {
		t.Fatal(err)
	}
	o = awaitWorkspace(t, f, ref)
	if !o.Ready || o.Held || o.Candidate == nil || o.Writer == nil || o.Status.State != backend.Succeeded || starts.Load() != 2 {
		t.Fatalf("resumed collection=%+v starts=%d", o, starts.Load())
	}
}

func TestWorkspaceActualPermitClassification(t *testing.T) {
	// This tests the finite caller handoff, not OS descriptor-close behavior.
	// Actual original-file fault/identity oracles live in artifact's source tests.
	for _, uncertain := range []bool{false, true} {
		t.Run(fmt.Sprint(uncertain), func(t *testing.T) {
			slots := make(chan struct{}, 4)
			slots <- struct{}{}
			var releases atomic.Int32
			release := onceWorkspaceRelease(func() { <-slots; releases.Add(1) })
			err := errors.New("ordinary policy/sync error after positive original close")
			if uncertain {
				err = errors.Join(err, artifact.ErrFileCloseUncertain)
			}
			held := settleWorkspacePermit(release, err)
			if uncertain {
				if held == nil || releases.Load() != 0 || len(slots) != 1 {
					t.Fatal("uncertain original slot released or replaced")
				}
				// Explicit test-owner disposal only after asserting HOLD; no production
				// close/reconciliation/capacity proof is supplied by invoking this.
				held()
			} else if held != nil || releases.Load() != 1 || len(slots) != 0 {
				t.Fatal("positive close failed to end original I/O ownership")
			}
			release()
			if releases.Load() != 1 {
				t.Fatal("original permit released twice")
			}
		})
	}
}

func TestWorkspaceSignalFailureIsNotNumericExit(t *testing.T) {
	f := newWorkspaceFixture(t)
	f.binding.Task.Argv = workspaceProgram("signal")
	f.binding.Task.Retry = &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{9}}
	o := awaitWorkspace(t, f, f.start(t))
	if !o.Ready || o.Held || o.Outcome != WorkspaceExecutionFailure || o.Status.State != backend.Failed || o.Status.ExitCode != nil || o.Writer == nil || o.Candidate != nil || o.Status.Termination != backend.UnknownTermination || !strings.Contains(o.Status.Reason, "signal") {
		t.Fatalf("signal classification=%+v", o)
	}
}

func TestWorkspacePreflightFailureClosesActualIO(t *testing.T) {
	f := newWorkspaceFixture(t)
	t.Setenv("PATH", t.TempDir()) // private empty lookup root; this test is not parallel
	f.binding.Task.Argv = []string{"skygo-no-such-workspace-program"}
	ops := f.backend.defaultOps
	var input, writer *os.File
	f.backend.defaultOps.start = func(cmd *exec.Cmd) error {
		if cmd.Err != nil {
			input = cmd.Stdin.(*os.File)
			writer = cmd.Stdout.(*os.File)
		}
		return ops.start(cmd)
	}
	o := awaitWorkspace(t, f, f.start(t))
	if !o.Ready || o.Outcome != WorkspaceExecutionFailure || o.Status.State != backend.Failed || o.Status.ExitCode != nil || o.Writer == nil || o.Candidate != nil || o.Status.Termination != backend.UnknownTermination {
		t.Fatalf("preflight failure=%+v", o)
	}
	if input == nil || writer == nil {
		t.Fatal("actual preflight boundary was not observed")
	}
	for _, file := range []*os.File{input, writer} {
		if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatal("preflight original stdio did not close")
		}
	}
}

func TestWorkspacePostForkStartFailureRemainsHeld(t *testing.T) {
	f := newWorkspaceFixture(t)
	f.expectHeld = true
	// Slash avoids exec.Command's LookPath preflight. Actual Cmd.Start runs the
	// original fork/exec failure cleanup, whose non-EINTR Wait4 error is opaque.
	f.binding.Task.Argv = []string{"./no-such-private-workspace-program"}
	o := awaitWorkspace(t, f, f.start(t))
	if !o.Held || o.Ready || o.Writer != nil || o.Candidate != nil || o.Status.Termination != backend.UnknownTermination {
		t.Fatalf("opaque Start failure gained closure authority: %+v", o)
	}
}

func TestWorkspaceCanceledStartupContextEndsUnusedPermit(t *testing.T) {
	f := newWorkspaceFixture(t)
	f.slots <- struct{}{}
	if err := f.backend.BindWorkspace(f.binding, func() { <-f.slots; f.stageReleases.Add(1) }); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.backend.Start(ctx, f.binding.AttemptID, f.binding.Task); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled startup=%v", err)
	}
	if f.stageReleases.Load() != 1 || len(f.slots) != 0 {
		t.Fatal("never-started materializer retained original permit")
	}
	if err := f.backend.Start(context.Background(), f.binding.AttemptID, f.binding.Task); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatal("expired startup replayed original binding")
	}
	if _, err := os.Stat(filepath.Join(f.backend.directory, f.binding.AttemptID+".log")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("context-failed Start created filesystem owner")
	}
}

func TestWorkspaceOwnedGroupStopsInheritedPipeWriter(t *testing.T) {
	f := newWorkspaceFixture(t)
	f.binding.Task.Setup = workspaceProgram("phase-gate")
	ops := f.backend.defaultOps
	var starts atomic.Int32
	var holder *exec.Cmd
	joined := make(chan struct{})
	holderStarted := false
	hookFinished := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-hookFinished:
		case <-time.After(3 * time.Second):
			t.Error("original startup hook retained")
			return
		}
		if !holderStarted {
			return
		}
		select {
		case <-joined:
		default:
			holder.Process.Kill()
		}
		select {
		case <-joined:
		case <-time.After(3 * time.Second):
			t.Error("original owned pipe holder did not join")
		}
	})
	f.backend.defaultOps.start = func(cmd *exec.Cmd) error {
		if starts.Add(1) != 1 {
			return ops.start(cmd)
		}
		defer close(hookFinished)
		if err := ops.start(cmd); err != nil {
			return err
		}
		ready := false
		defer func() {
			if !ready {
				cmd.Process.Kill()
				cmd.Wait()
				if holderStarted {
					holder.Process.Kill()
					select {
					case <-joined:
					case <-time.After(3 * time.Second):
						t.Error("failed startup original holder retained")
					}
				}
			}
		}()
		args := workspaceProgram("hold-pipe")
		holder = exec.Command(args[0], args[1:]...)
		holder.Env = append(os.Environ(), "SKYGO_WORKSPACE_PROGRAM=1")
		holder.Dir = cmd.Dir
		holder.Stdout = cmd.Stdout
		holder.Stderr = cmd.Stderr
		// The leader is alive at its explicit gate. This exact new child joins
		// only that original owned group, never a discovered/recovered identity.
		holder.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pgid: cmd.Process.Pid}
		if err := holder.Start(); err != nil {
			return err
		}
		holderStarted = true
		go func() { holder.Wait(); close(joined) }()
		w := f.backend.workspaces.bindings[f.binding.AttemptID]
		release, err := w.root.OpenFile("release-phase", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		err = release.Close()
		ready = err == nil
		return err
	}
	ref := f.start(t)
	o := awaitWorkspace(t, f, ref)
	select {
	case <-joined:
	case <-time.After(3 * time.Second):
		t.Fatal("actual group cleanup did not join inherited pipe holder")
	}
	if !o.Ready || o.Held || o.Writer == nil || o.Candidate == nil || starts.Load() != 2 || holder.ProcessState == nil {
		t.Fatalf("owned group handoff=%+v starts=%d", o, starts.Load())
	}
}

func TestWorkspaceCancelDuringFinalOutputClose(t *testing.T) {
	f := newWorkspaceFixture(t)
	ops := f.backend.defaultOps
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(resume) }) }
	t.Cleanup(release)
	f.backend.defaultOps.closeOutput = func(out *output) error { close(entered); <-resume; return ops.closeOutput(out) }
	ref := f.start(t)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("actual common log close barrier not reached")
	}
	if err := f.backend.Cancel(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	release()
	o := awaitWorkspace(t, f, ref)
	if !o.Ready || o.Outcome != WorkspaceCanceled || o.Status.State != backend.Canceled || o.Candidate != nil || o.Writer == nil || o.Status.Termination != backend.UnknownTermination {
		t.Fatalf("accepted final-close cancellation lost: %+v", o)
	}
	if _, err := f.root.Lstat(f.binding.ResultID + ".tar"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("cancellation admitted before collection created a candidate")
	}
}

func TestWorkspaceTaskRetryRequiresOrdinaryJoinedWait(t *testing.T) {
	for _, waitFailure := range []bool{false, true} {
		t.Run(fmt.Sprint(waitFailure), func(t *testing.T) {
			f := newWorkspaceFixture(t)
			f.binding.Task.Argv = workspaceProgram("run-fail")
			f.binding.Task.Retry = &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{9}}
			ops := f.backend.defaultOps
			if waitFailure {
				f.backend.defaultOps.reap = func(cmd *exec.Cmd) error {
					err := ops.reap(cmd)
					if cmd.Args[len(cmd.Args)-1] == "run-fail" {
						return errors.New("extra actual Wait diagnostic")
					}
					return err
				}
			}
			o := awaitWorkspace(t, f, f.start(t))
			if !o.Ready || o.Outcome != WorkspaceTaskExit || o.Status.State != backend.Failed || o.Status.ExitCode == nil || *o.Status.ExitCode != 9 || o.Candidate != nil || o.Writer == nil {
				t.Fatalf("task exit=%+v", o)
			}
			want := backend.StoppedWithExitCode
			if waitFailure {
				want = backend.UnknownTermination
			}
			if o.Status.Termination != want {
				t.Fatalf("retry proof=%v want=%v", o.Status.Termination, want)
			}
		})
	}
}

func TestWorkspaceFinalOutputCloseRetainsOriginalError(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			f := newWorkspaceFixture(t)
			f.expectHeld = fail
			ops := f.backend.defaultOps
			entered, resume := make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(resume) }) }
			t.Cleanup(release)
			var original *os.File
			var actualErr error
			var closes atomic.Int32
			diagnostic := errors.New("prior output diagnostic")
			f.backend.defaultOps.closeOutput = func(out *output) error {
				closes.Add(1)
				original = out.file
				close(entered)
				<-resume
				if fail {
					// Deliberately preclose this exact original File, then obtain the
					// actual Close error. This is a deterministic already-closed-file
					// fault, not a spontaneous kernel Close-failure reproduction.
					if err := original.Close(); err != nil {
						t.Error(err)
					}
					out.err = diagnostic
				}
				actualErr = ops.closeOutput(out)
				return actualErr
			}
			ref := f.start(t)
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("original output Close not reached")
			}
			f.backend.mu.Lock()
			e := f.backend.jobs[ref.ID]
			w := e.workspace
			root, directory := w.root, w.directory
			// Internal sentinel checks the real finishPayload guard. It is not a
			// Store reservation or a fabricated physical closure receipt.
			var retained *retainedOutput
			if fail {
				retained = &retainedOutput{}
				e.retained = retained
			}
			f.backend.mu.Unlock()
			release()
			first := awaitWorkspace(t, f, ref)
			<-e.done
			stage, collection := f.stageReleases.Load(), f.collectionReleases.Load()
			if fail {
				if actualErr == nil || !errors.Is(actualErr, os.ErrClosed) {
					t.Fatalf("actual original Close=%v", actualErr)
				}
				if !first.Held || first.Ready || first.Writer != nil || first.Candidate != nil || first.Status.Termination != backend.UnknownTermination {
					t.Fatalf("unproved output closure=%+v", first)
				}
				owner, ok := f.backend.workspaces.config.TryCollectionIO()
				if !ok {
					t.Fatal("no negative-prepare test owner")
				}
				if _, _, err := f.backend.PrepareWorkspaceCompletion(ref); err == nil {
					t.Fatal("uncertain original log prepared a physical origin")
				}
				owner()
				collection = f.collectionReleases.Load()
			} else if actualErr != nil || first.Held || !first.Ready || first.Writer == nil {
				t.Fatalf("positive actual Close=%+v %v", first, actualErr)
			}
			for range 3 {
				o, err := f.backend.WorkspaceObservation(ref)
				if err != nil || o.Held != first.Held || o.Ready != first.Ready || o.Status.Reason != first.Status.Reason {
					t.Fatalf("unstable observation=%+v %v", o, err)
				}
			}
			if err := f.backend.Cancel(context.Background(), ref); err != nil {
				t.Fatal(err)
			}
			f.backend.mu.Lock()
			if w.root != root || w.directory != directory {
				t.Error("original root ownership replaced")
			}
			if fail {
				f.backend.finishPayload(e)
				if retained.released || retained.closed {
					t.Error("Held output published payload release or closure")
				}
			}
			e.output.mu.Lock()
			if e.output.file != original || e.output.closed == fail {
				t.Error("original output identity or closure fact lost")
			}
			if fail {
				var got, want *os.PathError
				if !errors.Is(e.output.err, actualErr) || !errors.Is(e.output.err, diagnostic) || !errors.As(e.output.err, &got) || !errors.As(actualErr, &want) || got != want {
					t.Errorf("original joined Close error lost: %v", e.output.err)
				}
			} else if e.output.err != nil {
				t.Error(e.output.err)
			}
			e.output.mu.Unlock()
			f.backend.mu.Unlock()
			if closes.Load() != 1 || f.stageReleases.Load() != stage || f.collectionReleases.Load() != collection {
				t.Error("observation/cancel retried Close or settled an unrelated permit")
			}
			// Fixture Close must also refuse pending roots without retrying raw Close.
			if err := f.backend.Close(); err == nil {
				t.Error("pending root ownership was dropped")
			}
			if closes.Load() != 1 {
				t.Error("Close retried original output descriptor")
			}
		})
	}
}

func TestWorkspaceSupportUsesActualConfiguration(t *testing.T) {
	var absent *Backend
	if absent.SupportsWorkspace() {
		t.Fatal("nil backend advertised workspace ownership")
	}
	unconfigured := &Backend{retryAvailable: true}
	if unconfigured.SupportsWorkspace() {
		t.Fatal("process prerequisites alone advertised an installation")
	}
	f := newWorkspaceFixture(t) // actual marker, opened root, Files and ConfigureWorkspace
	if !f.backend.SupportsWorkspace() {
		t.Fatal("actual configured Linux backend unavailable")
	}
	if err := f.backend.Check(f.binding.Task); err != nil {
		t.Fatal(err)
	}
	f.backend.mu.Lock()
	f.backend.retryAvailable = false
	f.backend.mu.Unlock()
	if f.backend.SupportsWorkspace() {
		t.Fatal("disabled process prerequisites advertised workspace ownership")
	}
	if err := f.backend.Check(f.binding.Task); err == nil {
		t.Fatal("Check diverged from unavailable workspace support")
	}
	f.backend.mu.Lock()
	f.backend.retryAvailable = true
	f.backend.mu.Unlock()
	if !f.backend.SupportsWorkspace() {
		t.Fatal("restored prerequisites not observed")
	}
	if err := f.backend.Close(); err != nil {
		t.Fatal(err)
	} // no Bind, roots or sequence installed
	if f.backend.SupportsWorkspace() {
		t.Fatal("closed backend advertised workspace ownership")
	}
	if err := f.backend.Check(f.binding.Task); err == nil {
		t.Fatal("Check diverged from closed service")
	}
}
