//go:build linux

package local

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
)

type shutdownWorkspaceFixture struct {
	*shutdownFixture
	binding WorkspaceBinding
	slots   chan struct{}
	allow   atomic.Bool
}

func newShutdownWorkspaceFixture(t *testing.T) *shutdownWorkspaceFixture {
	t.Helper()
	f := &shutdownWorkspaceFixture{shutdownFixture: newShutdownFixture(t), slots: make(chan struct{}, 4)}
	b := f.backend
	requireRetry(t, b)
	name := filepath.Join(f.directory, "artifacts")
	if err := os.Mkdir(name, 0700); err != nil {
		f.held = err
		t.Fatal(err)
	}
	var err error
	f.root, err = os.OpenRoot(name)
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	info, err := f.root.Lstat(".")
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	id, err := workspaceFileID(info)
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	rootID := strings.Repeat("2", 32)
	if _, err := artifact.Initialize(f.root, id, rootID); err != nil {
		f.held = err
		t.Fatal(err)
	}
	f.files, err = artifact.Open(f.root, id)
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	var encoded bytes.Buffer
	tw := tar.NewWriter(&encoded)
	data := []byte("immutable input")
	if err := tw.WriteHeader(&tar.Header{Name: "input", Mode: 0600, Size: int64(len(data)), Format: tar.FormatUSTAR}); err != nil {
		f.held = err
		t.Fatal(err)
	}
	if _, err := tw.Write(data); err != nil {
		f.held = err
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		f.held = err
		t.Fatal(err)
	}
	digest := sha256.Sum256(encoded.Bytes())
	ref := artifact.Ref{ID: strings.Repeat("3", 32), SHA256: hex.EncodeToString(digest[:]), Size: int64(encoded.Len())}
	file, err := f.files.CreatePart(ref)
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	if _, err := f.files.WritePart(ref, file, 0, 0, encoded.Bytes()); err != nil {
		f.held = err
		t.Fatal(err)
	}
	// Test-owned namespace/capacity, not a production cache or Store grant.
	if _, err := f.files.SealPart(ref, file, func() error { return nil }); err != nil {
		f.held = err
		t.Fatal(err)
	}
	config := WorkspaceConfig{Files: f.files, Roots: f.root, InstallationID: strings.Repeat("1", 32), RootID: rootID,
		CheckGrowth: func() error {
			if !f.allow.Load() {
				return artifact.ErrFilesBusy
			}
			return nil
		}, PhysicalChanged: func() {},
		TryCollectionIO: func() (func(), bool) {
			select {
			case f.slots <- struct{}{}:
				return onceWorkspaceRelease(func() { <-f.slots }), true
			default:
				return nil, false
			}
		}}
	if err := b.ConfigureWorkspace(config); err != nil {
		f.held = err
		t.Fatal(err)
	}
	task := defaultTask(0)
	task.Workdir = &ref
	task.TimeoutSeconds = 5
	f.binding = WorkspaceBinding{Owner: "query-test", JobID: strings.Repeat("5", 32), AttemptID: strings.Repeat("6", 32), Binding: b.workspaces.binding, RootID: rootID, Task: task, Input: ref, InputFile: file}
	return f
}

func (f *shutdownWorkspaceFixture) bind(t *testing.T, release func()) {
	t.Helper()
	f.slots <- struct{}{}
	if release == nil {
		release = func() { <-f.slots }
	}
	if err := f.backend.BindWorkspace(f.binding, release); err != nil {
		<-f.slots
		f.held = err
		t.Fatal(err)
	}
}

func TestCheckShutdownOriginalNoCallCallback(t *testing.T) {
	f := newShutdownWorkspaceFixture(t)
	b := f.backend
	entered, resume, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	f.bind(t, func() { close(entered); <-resume; <-f.slots })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := make(chan error, 1)
	go func() { result <- b.Start(ctx, f.binding.AttemptID, f.binding.Task); close(returned) }()
	t.Cleanup(func() { unblock(); <-returned }) // exact original return, never a timeout receipt
	awaitDefault(t, entered, "original no-call callback entry")
	if err := b.CheckShutdown(); err == nil {
		t.Fatal("detached callback mistaken for return")
	}
	unblock()
	awaitDefault(t, returned, "original no-call Start return")
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := b.CheckShutdown(); err != nil {
		t.Fatal("actual no-call return not recognized", err)
	}
}

func TestCheckShutdownWorkspaceRequiresPreparedMarkedRetirement(t *testing.T) {
	for _, mode := range []string{"handoff", "done", "blocked-handoff"} {
		t.Run(mode, func(t *testing.T) {
			remove := mode == "done"
			f := newShutdownWorkspaceFixture(t)
			b := f.backend
			f.bind(t, nil)
			if err := b.Start(context.Background(), f.binding.AttemptID, f.binding.Task); err != nil {
				f.held = err
				t.Fatal(err)
			}
			e := shutdownExecutionFor(t, b, f.binding.AttemptID)
			if err := b.CheckShutdown(); err == nil {
				t.Fatal("live sequencer accepted from its mirrors")
			}
			f.allow.Store(true)
			ref := backend.Reference{ID: f.binding.AttemptID, Binding: f.binding.Binding, Task: backend.CloneTask(f.binding.Task)}
			status, err := b.Observe(context.Background(), ref)
			if err != nil {
				t.Fatal(err)
			}
			ref.Instance = status.Instance
			if err := b.ResumeWorkspacePublication(ref); err != nil {
				t.Fatal(err)
			}
			awaitDefault(t, e.done, "original workspace sequencer")
			if err := b.CheckShutdown(); err == nil {
				t.Fatal("Ready parent mistaken for cleanup closure")
			}
			if mode == "blocked-handoff" {
				b.mu.Lock()
				original := b.workspaces.bindings[ref.ID]
				root := original.root
				b.mu.Unlock()
				name := ""
				for range 65 {
					if name != "" {
						name += "/"
					}
					name += "deep"
					if err := root.Mkdir(name, 0700); err != nil {
						f.held = err
						t.Fatal(err)
					}
				}
			}
			release, ok := b.workspaces.config.TryCollectionIO()
			if !ok {
				t.Fatal("test preparation owner unavailable")
			}
			o, p, err := b.PrepareWorkspaceCompletion(ref)
			if !errors.Is(err, artifact.ErrFileCloseUncertain) {
				release()
			}
			if err != nil {
				f.held = err
				t.Fatal(err)
			}
			if err := b.CheckShutdown(); err == nil {
				t.Fatal("prepared unmarked original handles accepted")
			}
			raw, err := backend.TaskBytes(ref.Task)
			if err != nil {
				t.Fatal(err)
			}
			// These opaque CAS values exercise Local only; no Store transaction or
			// Control callback/integration completion is claimed by this fixture.
			subject := WorkspaceCleanupIdentity{Owner: f.binding.Owner, JobID: f.binding.JobID, AttemptID: ref.ID, Binding: ref.Binding, Instance: ref.Instance, RootID: f.binding.RootID, TaskDigest: sha256.Sum256(raw), CompletionDigest: sha256.Sum256([]byte("fixture completion")), WriterReceipt: sha256.Sum256([]byte("fixture writer")), InputRef: f.binding.Input, ResultState: "none"}
			intent := workspaceCleanupIntent(fixtureWorkspaceOrigin(subject, p))
			if !o.Ready || o.Writer == nil {
				t.Fatal("actual original writer not prepared")
			}
			if err := b.CommitWorkspace(ref, intent); err != nil {
				t.Fatal(err)
			}
			if err := b.CheckShutdown(); err == nil {
				t.Fatal("marked but unhanded handles accepted")
			}
			if mode == "blocked-handoff" {
				release, ok := b.workspaces.config.TryCollectionIO()
				if !ok {
					t.Fatal("test cleanup owner unavailable")
				}
				progress, err := b.RemoveWorkspaceRoot(context.Background(), intent)
				if !errors.Is(err, artifact.ErrFileCloseUncertain) {
					release()
				}
				if err == nil || !strings.Contains(err.Error(), "depth") || progress.Done {
					f.held = err
					t.Fatal("actual namespace refusal missing", progress, err)
				}
			}
			retire := func() {
				for quantum := 0; quantum < 8; quantum++ {
					release, ok := b.workspaces.config.TryCollectionIO()
					if !ok {
						t.Fatal("test retirement owner unavailable")
					}
					var progress WorkspaceRootProgress
					var err error
					if remove {
						progress, err = b.RemoveWorkspaceRoot(context.Background(), intent)
					} else {
						err = b.HandoffWorkspaceCleanup(context.Background(), intent)
					}
					if !errors.Is(err, artifact.ErrFileCloseUncertain) {
						release()
					}
					if err != nil {
						f.held = err
						t.Fatal(err)
					}
					if !remove || progress.Done {
						return
					}
				}
				t.Fatal("bounded test root did not finish actual retirement")
			}
			retire()
			b.mu.Lock()
			receipt, detached := e.workspaceClosed, e.workspace == nil
			b.mu.Unlock()
			if !receipt || !detached {
				t.Fatal("actual original cleanup did not publish receipt before detach")
			}
			if err := b.CheckShutdown(); err != nil {
				t.Fatal("positive original cleanup receipt refused", err)
			}
			if remove {
				// A later independent postmark frame is a separate obligation even
				// though this ORIGINAL historical job's receipt remains true.
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if _, err := b.RemoveWorkspaceRoot(ctx, intent); !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				b.mu.Lock()
				later := b.workspaces.cleanupFrames[ref.ID]
				oldReceipt := e.workspaceClosed
				b.mu.Unlock()
				if later == nil || !oldReceipt {
					t.Fatal("separate postmark frame or original receipt lost")
				}
				if err := b.CheckShutdown(); err == nil {
					t.Fatal("earlier positive receipt concealed later original frame")
				}
				retire() // same new postmark frame positively closes its original handles
				if err := b.CheckShutdown(); err != nil {
					t.Fatal("positive later postmark Done refused", err)
				}
			} else {
				if _, err := f.root.Lstat("workspace-" + ref.ID); err != nil {
					t.Fatal("handle handoff changed namespace", err)
				}
				if mode == "blocked-handoff" {
					b.mu.Lock()
					c := b.workspaces.bindings[ref.ID].cleanup
					b.mu.Unlock()
					c.mu.Lock()
					blocked, done := c.blocked != nil, c.done
					c.mu.Unlock()
					if !blocked || done {
						t.Fatal("query erased namespace refusal or manufactured Done")
					}
				}
			}
		})
	}
}

func TestCheckShutdownCleanupMirrorCannotReplaceOwners(t *testing.T) {
	done := make(chan struct{})
	close(done)
	failure := errors.New("modeled original cleanup Close uncertainty")
	c := &workspaceCleanup{err: failure, closeHeld: true}
	e := &execution{done: done}
	w := &workspaceExecution{parent: e, cleanup: c, committed: true, logCloseAttempted: true, observation: WorkspaceObservation{Ready: true, Writer: &WorkspaceWriterClosure{}}}
	e.workspace = w
	b := &Backend{jobs: map[string]*execution{"modeled": e}, workspaces: &workspaceState{bindings: map[string]*workspaceExecution{"modeled": w}}}
	if err := b.CheckShutdown(); !errors.Is(err, failure) {
		t.Fatal("empty physical mirror concealed original Close", err)
	}
	// A competing cleanup mutex never creates b.mu -> c.mu lock inversion.
	c.mu.Lock()
	if err := b.CheckShutdown(); err == nil {
		c.mu.Unlock()
		t.Fatal("busy original cleanup accepted")
	}
	if !b.mu.TryLock() {
		c.mu.Unlock()
		t.Fatal("query retained backend mutex while waiting for cleanup")
	}
	b.mu.Unlock()
	c.mu.Unlock()
}
