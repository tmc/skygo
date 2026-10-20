//go:build linux || darwin

package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

// Source-only draft. These tests exercise actual server/Control/HTTP owners,
// using channel-controlled remote callbacks; no native FD failure is simulated.
// Public Run has no listener publication, so the address reservation below has
// a small release/rebind race. Actual startup readiness or return detects it.
type lifetimeServerFixture struct {
	directory, data, url string
	cancel               context.CancelFunc
	done                 chan struct{}
	result               error
	opened               *store.Store
	frame                *serverRun
	expectedHeld         error
	borrowedFiles        *artifact.Files
	borrowedRoot         *os.Root
	client               *http.Client
	external             []<-chan struct{}
	releases             []func()
	held                 error
	keep                 bool
}

func lifetimeFixture(t *testing.T) *lifetimeServerFixture {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261019-server-lifetime-tests")
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	directory, err := os.MkdirTemp(parent, "server-")
	if err != nil {
		t.Fatal(err)
	}
	f := &lifetimeServerFixture{directory: directory, data: filepath.Join(directory, "state"), client: &http.Client{Timeout: time.Second}}
	t.Cleanup(func() { f.retire(t) })
	return f
}

func (f *lifetimeServerFixture) hold(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		err = errors.New("missing original finalization receipt")
	}
	if f.held == nil {
		f.held = err
	}
	if f.expectedHeld != nil && errors.Is(f.held, f.expectedHeld) {
		t.Logf("expected policy HOLD original server frame and namespace %s: %v", f.directory, f.held)
	} else {
		t.Errorf("HOLD original server frame and namespace %s: %v", f.directory, f.held)
	}
	if !f.keep {
		f.keep = true
		original := f.held
		hold := make(chan struct{})
		go func() { <-hold; runtime.KeepAlive(f); runtime.KeepAlive(original) }()
	}
}

func (f *lifetimeServerFixture) retire(t *testing.T) {
	t.Helper()
	if f.held != nil {
		f.hold(t, f.held)
		return
	}
	for _, release := range f.releases {
		release()
	}
	if f.cancel != nil {
		f.cancel()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for _, done := range f.external {
		select {
		case <-done:
		case <-ctx.Done():
			f.hold(t, fmt.Errorf("original client call: %w", ctx.Err()))
			return
		}
	}
	if f.done != nil {
		select {
		case <-f.done:
		case <-ctx.Done():
			f.hold(t, fmt.Errorf("original server Run: %w", ctx.Err()))
			return
		}
		if f.result != nil {
			f.hold(t, f.result)
			return
		}
	}
	if f.frame != nil {
		var err error
		if f.frame.runDone != nil {
			err = f.frame.shutdown(nil)
		} else {
			err = f.frame.closeStartup(nil)
		}
		if err != nil {
			f.hold(t, err)
			return
		}
		f.frame = nil
	}
	if f.opened != nil {
		f.hold(t, fmt.Errorf("original fixture Store lacks positive Close"))
		return
	}
	if f.held != nil {
		f.hold(t, f.held)
		return
	}
	// Nil Run is the server's positive original finalization receipt; an error
	// never grants this fixture permission to close or remove its dependencies.
	if f.borrowedFiles != nil {
		if err := f.borrowedFiles.Close(); err != nil {
			f.hold(t, err)
			return
		}
		f.borrowedFiles = nil
	}
	if f.borrowedRoot != nil {
		if err := f.borrowedRoot.Close(); err != nil {
			f.hold(t, err)
			return
		}
		f.borrowedRoot = nil
	}
	f.client.CloseIdleConnections()
	if err := os.RemoveAll(f.directory); err != nil {
		f.hold(t, err)
	}
}

func (f *lifetimeServerFixture) start(t *testing.T, cfg Config) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	cfg.DataDir, cfg.Listen = f.data, address
	f.url = "http://" + address
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	f.done = make(chan struct{})
	go func() { f.result = Run(ctx, cfg); close(f.done) }()
	deadline := time.NewTimer(8 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		response, err := f.client.Get(f.url + "/ready")
		if err == nil {
			status := response.StatusCode
			if err := response.Body.Close(); err != nil {
				f.hold(t, err)
				t.Fatal(err)
			}
			if status == http.StatusOK {
				return
			}
		}
		select {
		case <-f.done:
			t.Fatal("original server startup", f.result)
		case <-deadline.C:
			t.Fatal("server did not become ready")
		case <-tick.C:
		}
	}
}

func (f *lifetimeServerFixture) admissionStopped(t *testing.T) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		response, err := f.client.Get(f.url + "/ready")
		if err != nil {
			return
		} // Connection refusal also means this observer cannot enter.
		status := response.StatusCode
		if err := response.Body.Close(); err != nil {
			f.hold(t, err)
			t.Fatal(err)
		}
		if status == http.StatusServiceUnavailable {
			return
		}
		select {
		case <-f.done:
			t.Fatal("server returned before blocked original owner", f.result)
		case <-deadline.C:
			t.Fatal("server admission remained ready")
		case <-tick.C:
		}
	}
}

func lifetimeReceive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(8 * time.Second):
		t.Fatal("original callback did not reach its boundary")
		var zero T
		return zero
	}
}

func (f *lifetimeServerFixture) stopped(t *testing.T) {
	t.Helper()
	f.cancel()
	lifetimeReceive(t, f.done)
	if f.result != nil {
		t.Fatal("original server finalization", f.result)
	}
}

func (f *lifetimeServerFixture) readJob(t *testing.T, id string) backend.Job {
	t.Helper()
	// Called only after actual nil Run. Reopening is a singleton-lock observation,
	// not proof of arbitrary hidden dependency FDs or kernel network quiescence.
	s, err := store.Open(f.data)
	if err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	f.opened = s
	job, readErr := s.Get(id)
	if err := s.Close(); err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	f.opened = nil
	if readErr != nil {
		t.Fatal(readErr)
	}
	return job
}

type lifetimeRemote struct {
	checkEntered, checkRelease     chan struct{}
	observeEntered, observeRelease chan struct{}
	checkOnce, observeOnce         sync.Once
	starts, cancels, observes      atomic.Int32
}

func (b *lifetimeRemote) Check(backend.Task) error {
	if b.checkEntered != nil {
		b.checkOnce.Do(func() { close(b.checkEntered) })
		<-b.checkRelease
	}
	return nil
}
func (b *lifetimeRemote) Start(context.Context, string, backend.Task) error {
	b.starts.Add(1)
	return nil
}
func (b *lifetimeRemote) Cancel(context.Context, backend.Reference) error {
	b.cancels.Add(1)
	return nil
}
func (b *lifetimeRemote) Observe(context.Context, backend.Reference) (backend.Status, error) {
	b.observes.Add(1)
	if b.observeEntered != nil {
		b.observeOnce.Do(func() { close(b.observeEntered) })
		<-b.observeRelease
	}
	return backend.Status{State: backend.Running, Instance: "remote-original"}, nil
}
func (*lifetimeRemote) Logs(context.Context, backend.Reference, int64, int) (backend.LogChunk, error) {
	return backend.LogChunk{}, nil
}
func lifetimeRemoteConfig(b *lifetimeRemote) Config {
	return Config{DisableLocal: true, Targets: func(string) ([]backend.Target, error) {
		return []backend.Target{{Name: "remote", Binding: "remote-fixture", Backend: b}}, nil
	}}
}

func TestServerLifetimeJoinsAdmittedHTTPSubmitWithoutArtifacts(t *testing.T) {
	f := lifetimeFixture(t)
	b := &lifetimeRemote{checkEntered: make(chan struct{}), checkRelease: make(chan struct{})}
	release := sync.OnceFunc(func() { close(b.checkRelease) })
	f.releases = append(f.releases, release)
	f.start(t, lifetimeRemoteConfig(b))
	api := skyv1connect.NewControlServiceClient(&http.Client{}, f.url)
	type reply struct {
		id  string
		err error
	}
	result := make(chan reply, 1)
	done := make(chan struct{})
	f.external = append(f.external, done)
	go func() {
		defer close(done)
		response, err := api.SubmitJob(context.Background(), connect.NewRequest(&v1.SubmitJobRequest{RequestId: "before-stop", Task: &v1.Task{Argv: []string{"work"}}}))
		got := reply{err: err}
		if response != nil {
			got.id = response.Msg.Job.Id
		}
		result <- got
	}()
	lifetimeReceive(t, b.checkEntered)
	f.cancel()
	f.admissionStopped(t)
	select {
	case <-f.done:
		t.Fatal("Run returned under original HTTP submit", f.result)
	case <-time.After(50 * time.Millisecond):
	}
	// This deadline diagnoses a blocked original; it supplies no join credit.
	release()
	accepted := lifetimeReceive(t, result)
	if accepted.err != nil || accepted.id == "" {
		t.Fatal("accepted original request lost its normal response", accepted)
	}
	f.stopped(t)
	got := f.readJob(t, accepted.id)
	if got.ID != accepted.id || b.cancels.Load() != 0 {
		t.Fatal("original acceptance lost or remote canceled", got, b.cancels.Load())
	}
	// /ready becoming unavailable does not publish Run's exact freeze boundary.
	// Queued-versus-already-dispatched Start is checked by the Control tests,
	// where Begin precedes original Run publication deterministically.
}

func TestServerLifetimeIntegratesOriginalRemoteCallback(t *testing.T) {
	f := lifetimeFixture(t)
	s, err := store.Open(f.data)
	if err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	f.opened = s
	engine := auth.NewLegacy("")
	actor, err := engine.Authenticate("")
	if err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	job, submitErr := s.Submit(actor, "running", backend.Task{Argv: []string{"work"}}, "remote", "remote-fixture")
	if submitErr == nil {
		_, submitErr = s.Update(job.ID, func(j *backend.Job) error { j.Status.State = backend.Running; return nil })
	}
	if err := s.Close(); err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	f.opened = nil
	if submitErr != nil {
		t.Fatal(submitErr)
	}
	b := &lifetimeRemote{observeEntered: make(chan struct{}), observeRelease: make(chan struct{})}
	release := sync.OnceFunc(func() { close(b.observeRelease) })
	f.releases = append(f.releases, release)
	f.start(t, lifetimeRemoteConfig(b))
	lifetimeReceive(t, b.observeEntered)
	f.cancel()
	f.admissionStopped(t)
	select {
	case <-f.done:
		t.Fatal("Run returned under original backend callback", f.result)
	case <-time.After(50 * time.Millisecond):
	}
	release()
	f.stopped(t)
	got := f.readJob(t, job.ID)
	if got.Status.State != backend.Running || got.Status.Instance != "remote-original" {
		t.Fatal("original return was not integrated before Store finalization", got)
	}
	if b.starts.Load() != 0 || b.cancels.Load() != 0 || b.observes.Load() != 1 {
		t.Fatal("shutdown replaced/canceled remote work", b.starts.Load(), b.cancels.Load(), b.observes.Load())
	}
}

func TestServerLifetimeOpensInitializedArtifactDirectory(t *testing.T) {
	f := lifetimeFixture(t)
	root := filepath.Join(f.directory, "artifacts")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := InitializeArtifacts(f.data, root); err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	marker := filepath.Join(root, ".skygo-artifact-root")
	before, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	cfg := lifetimeRemoteConfig(&lifetimeRemote{})
	cfg.ArtifactDir = root
	f.start(t, cfg)
	f.stopped(t)
	after, err := os.ReadFile(marker)
	if err != nil || string(after) != string(before) {
		t.Fatal("Run changed initialized marker", err)
	}
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
		t.Fatal("runtime lost stopped installation", installed, readErr)
	}
}

func TestServerLifetimeLeavesBorrowedArtifactResourcesWithCaller(t *testing.T) {
	f := lifetimeFixture(t)
	name := filepath.Join(f.directory, "artifacts")
	if err := os.Mkdir(name, 0700); err != nil {
		t.Fatal(err)
	}
	if err := InitializeArtifacts(f.data, name); err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	root, err := os.OpenRoot(name)
	if err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	f.borrowedRoot = root
	info, err := root.Lstat(".")
	if err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	id, err := artifactInitializerIdentity(info)
	if err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	files, err := artifact.Open(root, id)
	if err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	f.borrowedFiles = files
	before, err := files.RootFacts()
	if err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	conflictData := filepath.Join(f.directory, "unopened-conflict-state")
	if err := Run(context.Background(), Config{DataDir: conflictData, Listen: "127.0.0.1:0", Artifacts: files, ArtifactDir: name}); err == nil {
		t.Fatal("conflicting root ownership accepted")
	}
	if _, err := os.Lstat(conflictData); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("conflicting configuration opened Store", err)
	}
	cfg := lifetimeRemoteConfig(&lifetimeRemote{})
	cfg.Artifacts = files
	f.start(t, cfg)
	f.stopped(t)
	after, err := files.RootFacts()
	if err != nil || after != before {
		t.Fatal("server closed or changed caller-owned Files", after, err)
	}
	if _, err := root.Lstat("."); err != nil {
		t.Fatal("server closed caller-owned root", err)
	}
	// The fixture, acting as the original caller, closes these resources only
	// after positive nil Run; this is never inferred from HTTP handler return.
}
