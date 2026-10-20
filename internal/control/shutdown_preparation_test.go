package control

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

// These are coordinator protocol tests. Channel-controlled backends witness
// actual callback entry/return; they do not stand in for Local native closure.
// Source only: execution and formatting remain separate qualification gates.
type shutdownPreparationFixture struct {
	directory   string
	store       *store.Store
	locals      []*local.Backend
	control     *Control
	stop        context.CancelFunc
	returned    chan struct{}
	runErr      error
	external    []<-chan struct{}
	releases    []func()
	held        error
	keeping     bool
	storeClosed bool
}

func newShutdownPreparationFixture(t *testing.T) *shutdownPreparationFixture {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261019-shutdown-preparation-tests")
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	directory, err := os.MkdirTemp(parent, "control-")
	if err != nil {
		t.Fatal(err)
	}
	f := &shutdownPreparationFixture{directory: directory}
	t.Cleanup(func() { f.retire(t) })
	f.store, err = store.Open(directory)
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	return f
}

func (f *shutdownPreparationFixture) retain(t *testing.T, err error) {
	t.Helper()
	if f.keeping {
		return
	}
	if f.held == nil {
		f.held = err
	}
	f.keeping = true
	t.Errorf("HOLD original shutdown fixture %s: %v", f.directory, err)
	original := err
	hold := make(chan struct{})
	go func() { <-hold; runtime.KeepAlive(f); runtime.KeepAlive(original) }()
}

func (f *shutdownPreparationFixture) retire(t *testing.T) {
	t.Helper()
	if f.held != nil {
		f.retain(t, f.held)
		return
	}
	for _, release := range f.releases {
		release()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, done := range f.external {
		select {
		case <-done:
		case <-ctx.Done():
			f.retain(t, fmt.Errorf("original external call: %w", ctx.Err()))
			return
		}
	}
	if f.stop != nil {
		f.stop()
		select {
		case <-f.returned:
		case <-ctx.Done():
			f.retain(t, fmt.Errorf("original Run: %w", ctx.Err()))
			return
		}
		if err := f.control.Join(ctx); err != nil {
			f.retain(t, err)
			return
		}
	}
	for _, b := range f.locals {
		if err := b.Close(); err != nil {
			f.retain(t, err)
			return
		}
	}
	// This frame owns no artifact Files/Roots. Local test instances below never
	// launch a process; Close remains a separate actual finalization receipt.
	if f.store != nil && !f.storeClosed {
		if err := f.store.Close(); err != nil {
			f.retain(t, err)
			return
		}
		f.storeClosed = true
	}
	if err := os.RemoveAll(f.directory); err != nil {
		f.retain(t, err)
	}
}

func (f *shutdownPreparationFixture) start(c *Control) {
	f.control = c
	ctx, cancel := context.WithCancel(context.Background())
	f.stop = cancel
	f.returned = make(chan struct{})
	go func() { f.runErr = c.Run(ctx); close(f.returned) }()
}

func preparationWait(t *testing.T, c *Control) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.WaitShutdownPrepared(ctx); err != nil {
		t.Fatal("original Run preparation", err)
	}
}

func preparationHeld(t *testing.T, c *Control) {
	t.Helper()
	// The wait begins live, so an erroneous early prepared receipt cannot hide
	// behind a context that was already canceled. Expiry supplies no join credit.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := c.WaitShutdownPrepared(ctx); !errors.Is(err, ErrUnavailable) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("unprepared wait did not retain ownership until its diagnostic deadline", err)
	}
}

type preparationCheckBackend struct {
	entered, release chan struct{}
	checks           atomic.Int32
}

func (b *preparationCheckBackend) Check(backend.Task) error {
	b.checks.Add(1)
	close(b.entered)
	<-b.release
	return nil
}
func (*preparationCheckBackend) Start(context.Context, string, backend.Task) error { return nil }
func (*preparationCheckBackend) Cancel(context.Context, backend.Reference) error   { return nil }
func (*preparationCheckBackend) Observe(context.Context, backend.Reference) (backend.Status, error) {
	return backend.Status{State: backend.Running}, nil
}
func (*preparationCheckBackend) Logs(context.Context, backend.Reference, int64, int) (backend.LogChunk, error) {
	return backend.LogChunk{}, nil
}

func TestShutdownPreparationWaitsForAdmittedSubmit(t *testing.T) {
	f := newShutdownPreparationFixture(t)
	b := &preparationCheckBackend{entered: make(chan struct{}), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(b.release) })
	f.releases = append(f.releases, release)
	c, err := New(f.store, []backend.Target{{Name: "remote", Binding: "original", Backend: b}}, testEngine)
	if err != nil {
		t.Fatal(err)
	}
	type reply struct {
		job backend.Job
		err error
	}
	result := make(chan reply, 1)
	done := make(chan struct{})
	f.external = append(f.external, done)
	go func() {
		defer close(done)
		job, err := c.Submit(testActor(), "admitted", backend.Task{Argv: []string{"work"}})
		result <- reply{job, err}
	}()
	waitValue(t, b.entered)
	if err := c.BeginShutdown(); err != nil {
		t.Fatal(err)
	}
	f.start(c)
	preparationHeld(t, c)
	if _, err := c.Submit(testActor(), "admitted", backend.Task{Argv: []string{"work"}}); !errors.Is(err, ErrUnavailable) {
		t.Fatal("new replay entered after Begin", err)
	}
	release()
	got := waitValue(t, result)
	if got.err != nil || got.job.ID == "" {
		t.Fatal("admitted call lost normal acceptance", got)
	}
	preparationWait(t, c)
	stored, err := f.store.Get(got.job.ID)
	if err != nil || stored.Status.State != backend.Queued {
		t.Fatal("accepted submission was started after freeze", stored, err)
	}
	if b.checks.Load() != 1 {
		t.Fatal("closed gate reached capability check", b.checks.Load())
	}
}

func TestShutdownPreparationGatePrecedesAuthValidationAndReplay(t *testing.T) {
	f := newShutdownPreparationFixture(t)
	c, err := New(f.store, []backend.Target{{Name: "remote", Binding: "original", Backend: &routeBackend{}}}, testEngine)
	if err != nil {
		t.Fatal(err)
	}
	task := backend.Task{Argv: []string{"work"}}
	first, err := c.Submit(testActor(), "replay", task)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.BeginShutdown(); err != nil {
		t.Fatal(err)
	}
	if err := c.BeginShutdown(); err != nil {
		t.Fatal("repeat Begin", err)
	}
	for _, tc := range []struct {
		name string
		call func() (backend.Job, error)
	}{
		{"plain", func() (backend.Job, error) { return c.Submit(auth.Actor{}, "!", backend.Task{}) }},
		{"retained", func() (backend.Job, error) {
			return c.SubmitRetained(auth.Actor{}, store.RequestDomain{}, "!", backend.Task{})
		}},
		{"workspace", func() (backend.Job, error) { return c.SubmitWorkspace(auth.Actor{}, "!", backend.Task{}) }},
		{"workspace domain", func() (backend.Job, error) {
			return c.SubmitWorkspaceInDomain(auth.Actor{}, store.RequestDomain{}, "!", backend.Task{})
		}},
		{"durable replay", func() (backend.Job, error) { return c.Submit(testActor(), "replay", task) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.call(); !errors.Is(err, ErrUnavailable) {
				t.Fatal("gate did not refuse first", err)
			}
		})
	}
	if call, err := c.TryArtifactCall(); call != nil || !errors.Is(err, ErrUnavailable) {
		t.Fatal("new artifact registration passed Begin", call, err)
	}
	stored, err := f.store.Get(first.ID)
	if err != nil || stored.Status.State != backend.Queued {
		t.Fatal("gate modified durable replay", stored, err)
	}
	// A diagnostic deadline has no authority to prepare the service.
	preparationHeld(t, c)
	f.start(c)
	preparationWait(t, c)
}

type preparationObserveBackend struct {
	entered, release          chan struct{}
	once                      sync.Once
	observes, starts, cancels atomic.Int32
}

func (*preparationObserveBackend) Check(backend.Task) error { return nil }
func (b *preparationObserveBackend) Start(context.Context, string, backend.Task) error {
	b.starts.Add(1)
	return nil
}
func (b *preparationObserveBackend) Cancel(context.Context, backend.Reference) error {
	b.cancels.Add(1)
	return nil
}
func (b *preparationObserveBackend) Observe(context.Context, backend.Reference) (backend.Status, error) {
	b.observes.Add(1)
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return backend.Status{State: backend.Running, Instance: "original-remote-instance"}, nil
}
func (*preparationObserveBackend) Logs(context.Context, backend.Reference, int64, int) (backend.LogChunk, error) {
	return backend.LogChunk{}, nil
}

func TestShutdownPreparationIntegratesOriginalRemoteResult(t *testing.T) {
	f := newShutdownPreparationFixture(t)
	b := &preparationObserveBackend{entered: make(chan struct{}), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(b.release) })
	f.releases = append(f.releases, release)
	c, err := New(f.store, []backend.Target{{Name: "remote", Binding: "original", Backend: b}}, testEngine)
	if err != nil {
		t.Fatal(err)
	}
	running, err := c.Submit(testActor(), "running", backend.Task{Argv: []string{"work"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Update(running.ID, func(j *backend.Job) error { j.Status.State = backend.Running; return nil }); err != nil {
		t.Fatal(err)
	}
	queued, err := c.Submit(testActor(), "queued", backend.Task{Argv: []string{"work"}})
	if err != nil {
		t.Fatal(err)
	}
	f.start(c)
	waitValue(t, b.entered)
	if err := c.BeginShutdown(); err != nil {
		t.Fatal(err)
	}
	preparationHeld(t, c)
	release()
	preparationWait(t, c)
	got, err := f.store.Get(running.ID)
	if err != nil || got.Status.State != backend.Running || got.Status.Instance != "original-remote-instance" {
		t.Fatal("preparation bypassed original integration", got, err)
	}
	next, err := f.store.Get(queued.ID)
	if err != nil || next.Status.State != backend.Queued || b.starts.Load() != 0 || b.cancels.Load() != 0 {
		t.Fatal("freeze started queued work or canceled remote", next, err, b.starts.Load(), b.cancels.Load())
	}
	// Prepared establishes Run's stable scheduling frontier; later Run cancellation
	// and Join remain necessary and are performed by the original fixture.
	c.runMu.Lock()
	frame := c.run
	c.runMu.Unlock()
	if frame == nil || frame.state.workers[0].pending != nil || frame.state.workers[0].received != nil || frame.state.unmatched != nil {
		t.Fatal("prepared retained an unintegrated original target pair")
	}
	if b.observes.Load() != 1 {
		t.Fatal("preparing replaced the original remote callback", b.observes.Load())
	}
}

func TestShutdownPreparationRunFailureAndJoinAreNotPreparation(t *testing.T) {
	f := newShutdownPreparationFixture(t)
	c, err := New(f.store, []backend.Target{{Name: "remote", Binding: "original", Backend: &routeBackend{}}}, testEngine)
	if err != nil {
		t.Fatal(err)
	}
	f.start(c)
	f.stop()
	waitValue(t, f.returned)
	if !errors.Is(f.runErr, context.Canceled) {
		t.Fatal("original Run return", f.runErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Join(ctx); err != nil {
		t.Fatal("original worker join", err)
	}
	if err := c.BeginShutdown(); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitShutdownPrepared(ctx); !errors.Is(err, ErrUnavailable) {
		t.Fatal("failed Run or Join manufactured prepared", err)
	}
}

func TestShutdownPreparationZeroControlRefuses(t *testing.T) {
	var c Control
	if err := c.BeginShutdown(); !errors.Is(err, ErrUnavailable) {
		t.Fatal("zero Begin", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.WaitShutdownPrepared(ctx); !errors.Is(err, ErrUnavailable) {
		t.Fatal("zero Wait", err)
	}
}

// Actual registration races the external gate while blocked on the existing
// service mutex. No call/map/ioActive state is fabricated to release a permit.
func TestShutdownPreparationArtifactRegistrationPublishesBeforeGateExit(t *testing.T) {
	c := artifactCallControl()
	s := c.artifacts
	s.mu.Lock()
	type registered struct {
		call *ArtifactCall
		err  error
	}
	result := make(chan registered, 1)
	go func() { call, err := c.TryArtifactCall(); result <- registered{call, err} }()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		c.shutdown.mu.Lock()
		count := c.shutdown.external
		c.shutdown.mu.Unlock()
		if count == 1 {
			break
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			s.mu.Unlock()
			t.Fatal("original registration did not enter counted gate")
		}
	}
	if err := c.BeginShutdown(); err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}
	c.shutdown.mu.Lock()
	count := c.shutdown.external
	c.shutdown.mu.Unlock()
	if count != 1 {
		s.mu.Unlock()
		t.Fatal("Begin discarded an admitted registration")
	}
	s.mu.Unlock()
	got := waitValue(t, result)
	if got.err != nil || got.call == nil {
		t.Fatal("admitted registration lost its original call", got.err)
	}
	defer got.call.Done()
	c.shutdown.mu.Lock()
	count = c.shutdown.external
	c.shutdown.mu.Unlock()
	s.mu.Lock()
	_, published := s.calls[got.call]
	s.mu.Unlock()
	if count != 0 || !published {
		t.Fatal("external count retired before original service publication")
	}
	if call, err := c.TryArtifactCall(); call != nil || !errors.Is(err, ErrUnavailable) {
		t.Fatal("post-Begin call entered registration", call, err)
	}
}

type preparationStartBackend struct {
	entered, release chan struct{}
	starts, cancels  atomic.Int32
}

func (*preparationStartBackend) Check(backend.Task) error { return nil }
func (b *preparationStartBackend) Start(context.Context, string, backend.Task) error {
	b.starts.Add(1)
	close(b.entered)
	<-b.release
	return nil
}
func (*preparationStartBackend) Observe(context.Context, backend.Reference) (backend.Status, error) {
	return backend.Status{State: backend.Running, Instance: "original-committed-start"}, nil
}
func (b *preparationStartBackend) Cancel(context.Context, backend.Reference) error {
	b.cancels.Add(1)
	return nil
}
func (*preparationStartBackend) Logs(context.Context, backend.Reference, int64, int) (backend.LogChunk, error) {
	return backend.LogChunk{}, nil
}

func TestShutdownPreparationRetainsAlreadyCommittedOriginalStart(t *testing.T) {
	f := newShutdownPreparationFixture(t)
	b := &preparationStartBackend{entered: make(chan struct{}), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(b.release) })
	f.releases = append(f.releases, release)
	c, err := New(f.store, []backend.Target{{Name: "remote", Binding: "original", Backend: b}}, testEngine)
	if err != nil {
		t.Fatal(err)
	}
	first, err := c.Submit(testActor(), "committed-start", backend.Task{Argv: []string{"work"}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.Submit(testActor(), "frozen-next", backend.Task{Argv: []string{"work"}})
	if err != nil {
		t.Fatal(err)
	}
	f.start(c)
	waitValue(t, b.entered)
	committed, err := f.store.Get(first.ID)
	if err != nil || committed.Status.State != backend.Starting {
		t.Fatal("actual native callback lacked durable BeginAttempt", committed, err)
	}
	if err := c.BeginShutdown(); err != nil {
		t.Fatal(err)
	}
	preparationHeld(t, c)
	release()
	preparationWait(t, c)
	queued, err := f.store.Get(second.ID)
	if err != nil || queued.Status.State != backend.Queued || b.starts.Load() != 1 || b.cancels.Load() != 0 {
		t.Fatal("freeze dropped/replaced committed start or launched next remote", queued, err)
	}
}

// Actual already-positively-closed Store is a metadata error tripwire, not a DB
// crash/fault or resource-closure experiment. No target operation is dispatched.
func TestShutdownPreparationRetainsFirstRunMetadataFailure(t *testing.T) {
	f := newShutdownPreparationFixture(t)
	c, err := New(f.store, []backend.Target{{Name: "remote", Binding: "original", Backend: &routeBackend{}}}, testEngine)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.Close(); err != nil {
		f.held = err
		t.Fatal(err)
	}
	f.storeClosed = true // original Close actually returned nil; never raw-retry it
	f.start(c)
	waitValue(t, f.returned)
	if f.runErr == nil {
		t.Fatal("closed Store did not fail original reconciliation")
	}
	if err := c.BeginShutdown(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.WaitShutdownPrepared(ctx); !errors.Is(err, ErrUnavailable) || !errors.Is(err, f.runErr) {
		t.Fatal("preparation lost exact original first failure", err)
	}
	c.shutdown.mu.Lock()
	first, prepared := c.shutdown.failure, c.shutdown.preparedClosed
	c.shutdown.mu.Unlock()
	if first != f.runErr || prepared {
		t.Fatal("Run error or function return manufactured preparation")
	}
}
