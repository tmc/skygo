//go:build linux || darwin

package server

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

// testIntegration records the server's calls in order. Run blocks until its
// ctx ends or release is closed, then returns runErr; with ignoreCancel it
// waits for release only. Close returns closeErr. With closeWait, Close
// first waits for its ctx to end and returns ctx's error; if overrun is
// set, it then closes expired and keeps running until overrun is closed,
// and returns closeErr.
type testIntegration struct {
	store        *store.Store
	targets      []backend.Target
	release      chan struct{}
	ignoreCancel bool
	runErr       error
	closeErr     error
	closeWait    bool
	expired      chan struct{}
	overrun      chan struct{}

	mu       sync.Mutex
	events   []string
	readable error // the Store read at Close
	deadline bool  // Close's ctx had a deadline
}

var errNoDeadline = errors.New("integration Close context has no deadline")

func (in *testIntegration) record(event string) {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.events = append(in.events, event)
}

func (in *testIntegration) recorded() []string {
	in.mu.Lock()
	defer in.mu.Unlock()
	return slices.Clone(in.events)
}

func (in *testIntegration) Targets() []backend.Target {
	in.record("targets")
	return in.targets
}

func (in *testIntegration) Run(ctx context.Context) error {
	in.record("run")
	done := ctx.Done()
	if in.ignoreCancel {
		done = nil
	}
	select {
	case <-done:
		in.record("run canceled")
	case <-in.release:
		in.record("run released")
	}
	return in.runErr
}

func (in *testIntegration) Close(ctx context.Context) error {
	_, _, err := in.store.RetentionPolicy()
	_, deadline := ctx.Deadline()
	in.mu.Lock()
	in.events = append(in.events, "close")
	in.readable, in.deadline = err, deadline
	in.mu.Unlock()
	if !in.closeWait {
		return in.closeErr
	}
	if !deadline {
		return errNoDeadline
	}
	<-ctx.Done()
	if in.overrun == nil {
		return ctx.Err()
	}
	close(in.expired)
	<-in.overrun
	return in.closeErr
}

func (in *testIntegration) install(s *store.Store, e *auth.Engine) (Integration, error) {
	if s == nil || e == nil {
		return nil, errors.New("install without Store or Engine")
	}
	in.store = s
	in.record("install")
	return in, nil
}

func newTestIntegration() *testIntegration {
	return &testIntegration{
		targets: []backend.Target{{Name: "remote", Binding: "integration-fixture", Backend: &lifetimeRemote{}}},
		release: make(chan struct{}),
	}
}

// integrationReceive waits for ch, allowing for the server's ten-second
// join and Close deadlines. It bounds the test, not the server.
func integrationReceive(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(30 * time.Second):
		t.Fatalf("%s: no return within the test's 30s bound", what)
	}
}

// heldIntegration fails unless err is a RunError at stage whose frame
// retains in as its integration, with its Store and authentication still
// serving reads.
func heldIntegration(t *testing.T, err error, stage string, in *testIntegration) *serverRun {
	t.Helper()
	var held *RunError
	if !errors.As(err, &held) || held.stage != stage {
		t.Fatalf("Run = %v, want a RunError at %q", err, stage)
	}
	frame := held.frame
	if frame.integration != Integration(in) {
		t.Fatalf("held integration %v, want the same original", frame.integration)
	}
	if _, _, err := frame.store.RetentionPolicy(); err != nil {
		t.Fatalf("Store read in the held frame = %v, want the Store still open", err)
	}
	actor, err := frame.engine.Authenticate("")
	if err == nil {
		_, err = frame.engine.Check(actor, auth.Read)
	}
	if err != nil {
		t.Fatalf("authentication in the held frame = %v, want it still serving", err)
	}
	return frame
}

// TestIntegrationLifetime checks that the server publishes the integration's
// targets, runs it, and closes it once after its Run returns and while the
// Store is still open.
func TestIntegrationLifetime(t *testing.T) {
	f := lifetimeFixture(t)
	in := newTestIntegration()
	f.start(t, Config{DisableLocal: true, Install: in.install})

	api := skyv1connect.NewControlServiceClient(&http.Client{}, f.url)
	response, err := api.ListTargets(context.Background(), connect.NewRequest(&v1.ListTargetsRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Msg.Targets) != 1 || response.Msg.Targets[0].Name != "remote" {
		t.Fatalf("targets = %v, want the integration's remote target", response.Msg.Targets)
	}
	f.stopped(t)

	want := []string{"install", "targets", "run", "run canceled", "close"}
	if got := in.recorded(); !slices.Equal(got, want) {
		t.Fatalf("integration calls = %q, want %q", got, want)
	}
	if in.readable != nil {
		t.Fatalf("Store read at integration Close = %v, want the Store still open", in.readable)
	}
	if !in.deadline {
		t.Fatal("integration Close context has no deadline")
	}
	// A nil Run is the original's release: the Store's singleton lock is
	// free again.
	s, err := store.Open(f.data)
	if err != nil {
		t.Fatalf("store.Open after a clean stop = %v, want the Store released", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestIntegrationInstallFailure checks that a startup failure closes an
// installed integration once, without running it, and releases the Store.
func TestIntegrationInstallFailure(t *testing.T) {
	sentinel := errors.New("install failed")
	for _, tt := range []struct {
		name    string
		publish bool // Install returns its original with the error
		fail    error
		want    []string
	}{
		{"original with error", true, sentinel, []string{"install", "close"}},
		{"nil with error", false, sentinel, []string{"install"}},
		{"later startup failure", true, nil, []string{"install", "targets", "close"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			in := newTestIntegration()
			cfg := Config{DataDir: dir, Listen: "127.0.0.1:0", DisableLocal: true}
			cfg.Install = func(s *store.Store, e *auth.Engine) (Integration, error) {
				original, err := in.install(s, e)
				if err != nil {
					return nil, err
				}
				if tt.fail == nil {
					return original, nil
				}
				if !tt.publish {
					return nil, tt.fail
				}
				return original, tt.fail
			}
			if tt.fail == nil {
				// Two targets of one name fail Control's construction after
				// Install has published its original.
				in.targets = append(in.targets, in.targets[0])
			}
			err := Run(context.Background(), cfg)
			if err == nil || tt.fail != nil && !errors.Is(err, tt.fail) {
				t.Fatalf("Run = %v, want %v", err, tt.fail)
			}
			var held *RunError
			if errors.As(err, &held) {
				t.Fatalf("Run = %v, want an ordinary startup error", err)
			}
			if got := in.recorded(); !slices.Equal(got, tt.want) {
				t.Fatalf("integration calls = %q, want %q", got, tt.want)
			}
			if in.readable != nil {
				t.Fatalf("Store read at integration Close = %v, want the Store still open", in.readable)
			}
			s, err := store.Open(dir)
			if err != nil {
				t.Fatalf("store.Open after failed startup = %v, want the Store released", err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestIntegrationCloseFailureHolds checks that a failed integration Close
// returns a RunError and leaves the Store open.
func TestIntegrationCloseFailureHolds(t *testing.T) {
	f := lifetimeFixture(t)
	in := newTestIntegration()
	in.closeErr = errors.New("integration closure failed")
	f.start(t, Config{DisableLocal: true, Install: in.install})
	f.cancel()
	lifetimeReceive(t, f.done)
	var held *RunError
	if !errors.As(f.result, &held) || !errors.Is(f.result, in.closeErr) {
		t.Fatalf("Run = %v, want a RunError wrapping %v", f.result, in.closeErr)
	}
	if _, _, err := in.store.RetentionPolicy(); err != nil {
		t.Fatalf("Store read after held integration Close = %v, want the Store still open", err)
	}
	if got, want := in.recorded(), []string{"install", "targets", "run", "run canceled", "close"}; !slices.Equal(got, want) {
		t.Fatalf("integration calls = %q, want %q", got, want)
	}
	// The held frame keeps its Store, namespace and integration until exit.
	f.expectedHeld = in.closeErr
	f.hold(t, f.result)
}

// TestIntegrationEarlyReturn checks that an integration Run returning before
// cancellation shuts the server down with its error and still closes it.
func TestIntegrationEarlyReturn(t *testing.T) {
	f := lifetimeFixture(t)
	in := newTestIntegration()
	in.runErr = errors.New("integration stopped")
	f.start(t, Config{DisableLocal: true, Install: in.install})
	close(in.release)
	lifetimeReceive(t, f.done)
	var held *RunError
	if !errors.Is(f.result, in.runErr) || errors.As(f.result, &held) {
		t.Fatalf("Run = %v, want %v without a RunError", f.result, in.runErr)
	}
	if got, want := in.recorded(), []string{"install", "targets", "run", "run released", "close"}; !slices.Equal(got, want) {
		t.Fatalf("integration calls = %q, want %q", got, want)
	}
	// Run returned its cause after closing every original; reopening the
	// Store observes that the singleton lock was released.
	s, err := store.Open(f.data)
	if err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	f.result = nil
}

// TestIntegrationHeldRunNotClosed checks that an integration Run that
// ignores cancellation is retained: the server returns a RunError holding
// the same original, never calls Close, and keeps the Store and
// authentication open. Releasing the Run joins it through the original's
// own channel, and the frame stays held.
func TestIntegrationHeldRunNotClosed(t *testing.T) {
	f := lifetimeFixture(t)
	in := newTestIntegration()
	in.ignoreCancel = true
	f.start(t, Config{DisableLocal: true, Install: in.install})
	f.cancel()
	integrationReceive(t, f.done, "server Run with a held integration Run")
	frame := heldIntegration(t, f.result, "original integration run remains held", in)
	if !errors.Is(f.result, context.DeadlineExceeded) {
		t.Fatalf("Run = %v, want the join's deadline", f.result)
	}
	select {
	case <-frame.integrationDone:
		t.Fatal("held integration Run reported joined")
	default:
	}
	if got, want := in.recorded(), []string{"install", "targets", "run"}; !slices.Equal(got, want) {
		t.Fatalf("integration calls = %q, want %q", got, want)
	}

	close(in.release)
	lifetimeReceive(t, frame.integrationDone)
	if got, want := in.recorded(), []string{"install", "targets", "run", "run released"}; !slices.Equal(got, want) {
		t.Fatalf("integration calls after release = %q, want %q", got, want)
	}
	if frame.integration != Integration(in) {
		t.Fatal("released Run cleared the held original")
	}
	f.expectedHeld = context.DeadlineExceeded
	f.hold(t, f.result)
}

// TestIntegrationCloseContextHolds checks a Close that honors its ctx: it
// returns the deadline's error, and the server holds the same original with
// its Store and authentication open.
func TestIntegrationCloseContextHolds(t *testing.T) {
	f := lifetimeFixture(t)
	in := newTestIntegration()
	in.closeWait = true
	f.start(t, Config{DisableLocal: true, Install: in.install})
	f.cancel()
	integrationReceive(t, f.done, "server Run with a cooperative integration Close")
	heldIntegration(t, f.result, "integration closure remains held", in)
	if !errors.Is(f.result, context.DeadlineExceeded) {
		t.Fatalf("Run = %v, want the Close deadline's error", f.result)
	}
	if got, want := in.recorded(), []string{"install", "targets", "run", "run canceled", "close"}; !slices.Equal(got, want) {
		t.Fatalf("integration calls = %q, want %q", got, want)
	}
	f.expectedHeld = context.DeadlineExceeded
	f.hold(t, f.result)
}

// TestIntegrationCloseAwaitedPastDeadline checks that the server waits for
// a Close that outlives its ctx, and treats its later nil result as a
// positive close that releases the Store.
func TestIntegrationCloseAwaitedPastDeadline(t *testing.T) {
	f := lifetimeFixture(t)
	in := newTestIntegration()
	in.closeWait = true
	in.expired, in.overrun = make(chan struct{}), make(chan struct{})
	f.start(t, Config{DisableLocal: true, Install: in.install})
	f.cancel()
	integrationReceive(t, in.expired, "integration Close deadline")
	// The window only makes a server that stops waiting likely to be seen;
	// a server that waits passes it however long it is.
	select {
	case <-f.done:
		t.Fatalf("server Run = %v while its integration Close was still running", f.result)
	case <-time.After(2 * time.Second):
	}
	if _, _, err := in.store.RetentionPolicy(); err != nil {
		t.Fatalf("Store read during the overrunning Close = %v, want the Store still open", err)
	}
	close(in.overrun)
	integrationReceive(t, f.done, "server Run after its integration Close returned")
	if f.result != nil {
		t.Fatalf("Run = %v, want nil after a nil Close", f.result)
	}
	s, err := store.Open(f.data)
	if err != nil {
		f.hold(t, err)
		t.Fatalf("store.Open after the Close returned = %v, want the Store released", err)
	}
	if err := s.Close(); err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
}
