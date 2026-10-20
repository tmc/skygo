//go:build darwin || linux

package rpc

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
)

// The bodies below are explicit simulated transport owners, not physical FD or
// kernel deadline evidence. Control slots, Store and Files are the real objects.
type artifactOwnerBody struct {
	closes   atomic.Int32
	closeErr error
	readErr  error
	entered  chan struct{}
	release  <-chan struct{}
}

func (b *artifactOwnerBody) Read([]byte) (int, error) { return 0, b.readErr }
func (b *artifactOwnerBody) Close() error {
	b.closes.Add(1)
	if b.entered != nil {
		close(b.entered)
	}
	if b.release != nil {
		<-b.release
	}
	return b.closeErr
}

type artifactOwnerWriter struct {
	*httptest.ResponseRecorder
	flushErr error
	flushes  int
}

func (w *artifactOwnerWriter) FlushError() error { w.flushes++; return w.flushErr }

type artifactOwnerBackend struct{}

func (*artifactOwnerBackend) Check(backend.Task) error { return nil }
func (*artifactOwnerBackend) Start(context.Context, string, backend.Task) error {
	return errors.New("unexpected workload")
}
func (*artifactOwnerBackend) Observe(context.Context, backend.Reference) (backend.Status, error) {
	return backend.Status{}, errors.New("unexpected observe")
}
func (*artifactOwnerBackend) Cancel(context.Context, backend.Reference) error {
	return errors.New("unexpected cancel")
}
func (*artifactOwnerBackend) Logs(context.Context, backend.Reference, int64, int) (backend.LogChunk, error) {
	return backend.LogChunk{}, errors.New("unexpected logs")
}

func artifactOwnerControl(t *testing.T) (*control.Control, *HTTPHandler, auth.Actor) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(dir, "files")
	if err := os.Mkdir(name, 0700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	})
	info, err := root.Stat(".")
	if err != nil {
		t.Fatal(err)
	}
	physical, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("actual root identity unavailable")
	}
	identity := artifact.FileID{Device: uint64(physical.Dev), Inode: uint64(physical.Ino)}
	facts, err := artifact.Initialize(root, identity, strings.Repeat("a", 32))
	if err != nil {
		t.Fatal(err)
	}
	files, err := artifact.Open(root, identity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := files.Close(); err != nil {
			t.Error(err)
		}
	})
	state := filepath.Join(dir, "state")
	if _, err := store.InstallArtifacts(state, store.ArtifactInstallation{Enabled: true, ID: facts.ID, Unit: facts.Unit, Volume: facts.Volume}); err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(state)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	engine := auth.NewLegacy("")
	actor, err := engine.Authenticate("")
	if err != nil {
		t.Fatal(err)
	}
	c, err := control.NewWithConfig(s, []backend.Target{{Name: "test", Binding: "test-binding", Backend: new(artifactOwnerBackend)}}, engine, control.Config{Artifacts: files})
	if err != nil {
		t.Fatal(err)
	}
	return c, NewHandler(c, engine), actor
}

// Only the upload-exclusion oracle needs a real physical sample. Its one
// persistent frame owns Run and every dependency. An intentional failed body
// Close retains that frame/path; neither a test timeout nor worker Join is a
// production HTTP/descriptor drain receipt.
type artifactOwnerUploadFixture struct {
	directory string
	root      *os.Root
	files     *artifact.Files
	store     *store.Store
	control   *control.Control
	handler   *HTTPHandler
	actor     auth.Actor
	cancel    context.CancelFunc
	returned  chan error
	held      error
	wantHeld  error
}

func artifactOwnerSampledControl(t *testing.T) *artifactOwnerUploadFixture {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(home, "tmp", "skypilot", "20261019-artifact-owner-test-repair")
	if err := os.MkdirAll(base, 0700); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(base)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		t.Fatal("artifact owner test parent must be a real private directory")
	}
	dir, err := os.MkdirTemp(base, "upload-")
	if err != nil {
		t.Fatal(err)
	}
	f := &artifactOwnerUploadFixture{directory: dir}
	t.Cleanup(func() { f.cleanup(t) })
	name := filepath.Join(dir, "files")
	if err := os.Mkdir(name, 0700); err != nil {
		f.held = err
		t.Fatal(err)
	}
	f.root, err = os.OpenRoot(name)
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	info, err = f.root.Stat(".")
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	physical, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		f.held = errors.New("actual root identity unavailable")
		t.Fatal(f.held)
	}
	identity := artifact.FileID{Device: uint64(physical.Dev), Inode: uint64(physical.Ino)}
	facts, err := artifact.Initialize(f.root, identity, strings.Repeat("a", 32))
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	f.files, err = artifact.Open(f.root, identity)
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	state := filepath.Join(dir, "state")
	if _, err := store.InstallArtifacts(state, store.ArtifactInstallation{Enabled: true, ID: facts.ID, Unit: facts.Unit, Volume: facts.Volume}); err != nil {
		f.held = err
		t.Fatal(err)
	}
	f.store, err = store.Open(state)
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	engine := auth.NewLegacy("")
	f.actor, err = engine.Authenticate("")
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	f.control, err = control.NewWithConfig(f.store, []backend.Target{{Name: "test", Binding: "test-binding", Backend: new(artifactOwnerBackend)}}, engine, control.Config{Artifacts: f.files})
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	f.handler = NewHandler(f.control, engine)
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel, f.returned = cancel, make(chan error, 1)
	go func() { f.returned <- f.control.Run(ctx) }()
	return f
}

func (f *artifactOwnerUploadFixture) cleanup(t *testing.T) {
	t.Helper()
	unexpected := f.held != nil
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if f.handler != nil {
		done := f.handler.stopArtifactHTTP()
		f.handler.mu.Lock()
		for owner := range f.handler.owners {
			if owner.closeErr != nil {
				if f.wantHeld == nil || owner.closeErr != f.wantHeld {
					unexpected = true
				}
				f.held = errors.Join(f.held, owner.closeErr)
			}
		}
		f.handler.mu.Unlock()
		if f.held == nil {
			select {
			case <-done:
			case <-ctx.Done():
				f.held = ctx.Err()
			}
		}
	}
	if f.control != nil {
		if f.held == nil {
			f.held = f.control.DrainArtifacts(ctx)
		} else {
			f.control.StopArtifactAdmission()
		}
	}
	var runErr, joinErr error
	if f.cancel != nil {
		f.cancel()
		select {
		case err := <-f.returned:
			if !errors.Is(err, context.Canceled) {
				runErr = err
			}
		case <-ctx.Done():
			runErr = ctx.Err()
		}
		joinErr = f.control.Join(ctx)
		f.held = errors.Join(f.held, runErr, joinErr)
	}
	if f.held == nil && f.files != nil {
		f.held = f.files.Close()
	}
	if f.held == nil && f.root != nil {
		f.held = f.root.Close()
	}
	if f.held == nil && f.store != nil {
		f.held = f.store.Close()
	}
	if f.held == nil {
		f.held = os.RemoveAll(f.directory)
	}
	if f.held != nil {
		if !unexpected && f.wantHeld != nil && errors.Is(f.held, f.wantHeld) && runErr == nil && joinErr == nil {
			t.Logf("expected original body owner remains held at %s: %v", f.directory, f.held)
		} else {
			t.Errorf("retaining original artifact owner fixture at %s: %v", f.directory, f.held)
		}
		// Same concrete frame and dependencies stay reachable until test-process
		// exit. No retry, extra production owner or global fixture registry.
		lifetime := make(chan struct{})
		go func(original *artifactOwnerUploadFixture) { <-lifetime; runtime.KeepAlive(original) }(f)
	}
}

func TestArtifactOwnerCloseErrorRetainsFourAndDrain(t *testing.T) {
	c, h, _ := artifactOwnerControl(t)
	original := errors.New("original simulated body close uncertainty")
	var held []*artifactResponse
	for i := 0; i < 4; i++ {
		body := &artifactOwnerBody{closeErr: original}
		writer := &artifactOwnerWriter{ResponseRecorder: httptest.NewRecorder()}
		owner, err := h.admit(c, body, writer)
		if err != nil {
			t.Fatal(err)
		}
		// Cover both orders: Connect closes first, or enclosing finish does.
		if i%2 == 0 {
			if err := owner.body.Close(); err != original {
				t.Fatal(err)
			}
		}
		owner.finish()
		owner.finish()
		if owner.closeErr != original || owner.body.original != body || owner.writer != writer || owner.call == nil || body.closes.Load() != 1 || writer.flushes != 0 {
			t.Fatal("lost or retried original failed owner")
		}
		held = append(held, owner)
	}
	if owner, err := h.admit(c, &artifactOwnerBody{}, httptest.NewRecorder()); !errors.Is(err, control.ErrArtifactBusy) {
		if owner != nil {
			owner.finish()
		}
		t.Fatalf("fifth admitted: %v", err)
	}
	if call, err := c.TryArtifactCall(); !errors.Is(err, control.ErrArtifactBusy) {
		if call != nil {
			call.Done()
		}
		t.Fatalf("uncertainty recycled Control slot: %v", err)
	}
	done := h.stopArtifactHTTP()
	select {
	case <-done:
		t.Fatal("uncertain original body falsely drained")
	default:
	}
	if len(h.owners) != 4 {
		t.Fatal("lost bounded original owner map")
	}
	for _, owner := range held {
		if _, ok := h.owners[owner]; !ok {
			t.Fatal("original owner dropped")
		}
	}
	if owner, err := h.admit(c, &artifactOwnerBody{}, httptest.NewRecorder()); !errors.Is(err, control.ErrUnavailable) {
		if owner != nil {
			owner.finish()
		}
		t.Fatal("admitted after stop")
	}
	// No cleanup calls Done or retries an uncertain original body. These bodies
	// have no FD; fixture Store/Files cleanup is not a production drain receipt.
}

func TestArtifactOwnerPositiveCloseRecoversReadAndWriteErrors(t *testing.T) {
	for _, closeBeforeFinish := range []bool{false, true} {
		t.Run(map[bool]string{false: "outer_first", true: "connect_first"}[closeBeforeFinish], func(t *testing.T) {
			c, h, _ := artifactOwnerControl(t)
			body := &artifactOwnerBody{readErr: io.ErrUnexpectedEOF}
			writer := &artifactOwnerWriter{ResponseRecorder: httptest.NewRecorder(), flushErr: io.ErrClosedPipe}
			owner, err := h.admit(c, body, writer)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := owner.body.Read(make([]byte, 1)); err != io.ErrUnexpectedEOF {
				t.Fatal(err)
			}
			if closeBeforeFinish {
				if err := owner.body.Close(); err != nil {
					t.Fatal(err)
				}
			}
			owner.finish()
			owner.finish()
			if owner.closeErr != nil || owner.flushErr != io.ErrClosedPipe || body.closes.Load() != 1 || writer.flushes != 1 || len(h.owners) != 0 {
				t.Fatal("positive body closure was not distinguished from ordinary transport error")
			}
			h.WaitArtifactHTTP()
			h.WaitArtifactHTTP()
			var calls []*control.ArtifactCall
			for i := 0; i < 4; i++ {
				call, err := c.TryArtifactCall()
				if err != nil {
					t.Fatal(err)
				}
				calls = append(calls, call)
			}
			for _, call := range calls {
				call.Done()
			}
		})
	}
}

func TestArtifactOwnerBlockedCloseJoinsOriginal(t *testing.T) {
	c, h, _ := artifactOwnerControl(t)
	entered, release, finished, drained := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	body := &artifactOwnerBody{entered: entered, release: release}
	owner, err := h.admit(c, body, httptest.NewRecorder())
	if err != nil {
		t.Fatal(err)
	}
	go func() { defer close(finished); owner.finish() }()
	go func() { defer close(drained); h.WaitArtifactHTTP() }()
	defer func() { unblock(); <-finished; <-drained }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("original close not entered")
	}
	select {
	case <-drained:
		t.Fatal("blocked original body falsely drained")
	default:
	}
	unblock()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("original finish did not return")
	}
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("positive original body did not drain")
	}
	owner.finish()
	if body.closes.Load() != 1 {
		t.Fatal("original Close retried")
	}
}

func TestArtifactOwnerCloseErrorPreservesSameUploadExclusion(t *testing.T) {
	f := artifactOwnerSampledControl(t)
	c, h, actor := f.control, f.handler, f.actor
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var owner *artifactResponse
	var upload store.ArtifactUpload
	var err error
	original := errors.New("original simulated uncertainty")
	for attempt := 0; ; attempt++ {
		body := &artifactOwnerBody{}
		owner, err = h.admit(c, body, httptest.NewRecorder())
		if err != nil {
			t.Fatal(err)
		}
		upload, err = c.BeginArtifactUpload(ctx, actor, owner.call, strings.Repeat("b", 64), 1024)
		if err == nil {
			// Install the simulated failure on THIS original body before its
			// first actual Close, only after real cache admission/part creation.
			body.closeErr = original
			f.wantHeld = original
			break
		}
		owner.finish() // prior positively closed refusal is fully retired
		if !control.IsArtifactBeginNotStarted(err) || attempt == 20 {
			t.Fatal(err)
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			t.Fatal(ctx.Err())
		}
	}
	owner.finish()
	second, err := c.TryArtifactCall()
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.WriteArtifactChunk(context.Background(), actor, second, upload.Reference.ID, 0, []byte("x"))
	second.Done()
	if !errors.Is(err, control.ErrArtifactBusy) {
		t.Fatalf("same original upload response exclusion released: %v", err)
	}
	current, err := c.GetArtifactUpload(context.Background(), actor, upload.Reference.ID)
	if err != nil || current.Acknowledged != 0 {
		t.Fatalf("held upload changed: %v %v", current, err)
	}
}

func TestArtifactOwnerZeroStopNeverReopens(t *testing.T) {
	c, h, _ := artifactOwnerControl(t)
	h.WaitArtifactHTTP()
	h.WaitArtifactHTTP()
	if owner, err := h.admit(c, nil, httptest.NewRecorder()); !errors.Is(err, control.ErrUnavailable) {
		if owner != nil {
			owner.finish()
		}
		t.Fatal("admitted after initially empty stop")
	}
}

// Deliberately stage the previously reachable completion/admission boundary:
// actual positive body/response completion and one real Control permit released,
// while its original response is still mapped. No source-layout assertion, test
// hook, fake call or scheduler sleep is used. This is simulated transport I/O,
// with the actual Control slots and production handler admission/finish methods.
func TestArtifactOwnerFreePermitCannotAdmitFifthMappedResponse(t *testing.T) {
	c, h, _ := artifactOwnerControl(t)
	var originals []*artifactResponse
	for i := 0; i < 4; i++ {
		owner, err := h.admit(c, &artifactOwnerBody{}, httptest.NewRecorder())
		if err != nil {
			t.Fatal(err)
		}
		originals = append(originals, owner)
	}
	defer func() {
		for owner := range h.owners {
			owner.finish()
		}
	}()
	old := originals[0]
	if err := old.body.Close(); err != nil {
		t.Fatal(err)
	}
	if err := http.NewResponseController(old.writer).Flush(); err != nil {
		t.Fatal(err)
	}
	old.call.Done()
	// Positively demonstrate the Control permit really is free, not merely
	// assume it from a counter. Retain the mapped original response throughout.
	free, err := c.TryArtifactCall()
	if err != nil {
		t.Fatalf("real Control permit not free: %v", err)
	}
	free.Done()
	if owner, err := h.admit(c, &artifactOwnerBody{}, httptest.NewRecorder()); !errors.Is(err, control.ErrArtifactBusy) {
		if owner != nil {
			owner.finish()
		}
		t.Fatalf("free Control permit admitted a fifth original response: %v", err)
	}
	if len(h.owners) != 4 {
		t.Fatal("boundary changed original response set")
	}
	for _, owner := range originals {
		if _, ok := h.owners[owner]; !ok {
			t.Fatal("original response dropped")
		}
	}
	// Actual positive finish retires this exact original. Done is idempotent;
	// permit release/map retirement are locked together on this normal path.
	old.finish()
	if _, ok := h.owners[old]; ok {
		t.Fatal("positive original did not retire")
	}
	replacement, err := h.admit(c, &artifactOwnerBody{}, httptest.NewRecorder())
	if err != nil {
		t.Fatal(err)
	}
	if len(h.owners) != 4 {
		replacement.finish()
		t.Fatal("replacement cardinality differs")
	}
	replacement.finish()
	for _, owner := range originals[1:] {
		owner.finish()
	}
	h.WaitArtifactHTTP()
}

// This stress regression checks actual concurrent admission/finish invariants;
// it does not claim to deterministically pause inside Done/map retirement.
func TestArtifactOwnerConcurrentAdmissionAndPositiveFinish(t *testing.T) {
	c, h, _ := artifactOwnerControl(t)
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := 0; i < 16; i++ {
				owner, err := h.admit(c, &artifactOwnerBody{}, httptest.NewRecorder())
				if errors.Is(err, control.ErrArtifactBusy) {
					continue
				}
				if err != nil {
					t.Error(err)
					return
				}
				h.mu.Lock()
				mapped := len(h.owners)
				h.mu.Unlock()
				if mapped > 4 {
					t.Error("more than four mapped originals")
				}
				owner.finish()
			}
		}()
	}
	workers.Wait()
	h.mu.Lock()
	remaining := len(h.owners)
	h.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("positive completions left%d originals", remaining)
	}
	h.WaitArtifactHTTP()
	var calls []*control.ArtifactCall
	for i := 0; i < 4; i++ {
		call, err := c.TryArtifactCall()
		if err != nil {
			t.Fatal(err)
		}
		calls = append(calls, call)
	}
	for _, call := range calls {
		call.Done()
	}
}
