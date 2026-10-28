//go:build linux || darwin

package server

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/store"
)

// realAuth is scoped state read by the actual file Engine selected by
// Config.AuthFile. It holds an admin and an operator credential.
type realAuth struct {
	path         string
	admin, alice string
}

var realAuthModes = []string{"file"}

// newRealAuth activates scoped state in f's data directory, as auth init
// does, with its credentials in a private 0700 parent.
func newRealAuth(t *testing.T, f *lifetimeServerFixture) *realAuth {
	t.Helper()
	// Auth refuses a group- or other-writable credential parent.
	private := filepath.Join(f.directory, "auth")
	if err := os.Mkdir(private, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(private, 0700); err != nil {
		t.Fatal(err)
	}
	a := &realAuth{path: filepath.Join(private, "auth.json")}
	output := filepath.Join(private, "admin.token")
	s, err := store.Open(f.data)
	if err != nil {
		t.Fatal(err)
	}
	owner := s.Owner()
	_, initErr := auth.Initialize(a.path, output, owner)
	if initErr == nil {
		initErr = s.ActivateScoped()
	}
	if err := s.Close(); err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	if initErr != nil {
		t.Fatal(initErr)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	a.admin = strings.TrimSuffix(string(data), "\n")
	if err := auth.AddPrincipal(a.path, "alice", auth.Operator); err != nil {
		t.Fatal(err)
	}
	operator := filepath.Join(private, "alice.token")
	if _, err := auth.Issue(a.path, "alice", operator, 10*time.Minute); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(operator)
	if err != nil {
		t.Fatal(err)
	}
	a.alice = strings.TrimSuffix(string(data), "\n")
	return a
}

func (a *realAuth) config(in *testIntegration) Config {
	return Config{DisableLocal: true, Install: in.install, AuthFile: a.path}
}

func (a *realAuth) tokens() []string {
	if a.alice == "" {
		return []string{a.admin}
	}
	return []string{a.admin, a.alice}
}

// serves checks, through the running server, that the real Engine admits
// each valid credential and refuses an invalid one. It waits up to five
// seconds for the file reader's first read.
func (a *realAuth) serves(t *testing.T, f *lifetimeServerFixture) {
	t.Helper()
	api := skyv1connect.NewControlServiceClient(f.client, f.url)
	read := func(token string) error {
		r := connect.NewRequest(&v1.GetServerInfoRequest{})
		r.Header().Set("Authorization", "Bearer "+token)
		_, err := api.GetServerInfo(context.Background(), r)
		return err
	}
	deadline := time.Now().Add(5 * time.Second)
	for _, token := range a.tokens() {
		for {
			err := read(token)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("valid credential refused by the real Engine: %v", err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	if err := read("not-a-credential"); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("invalid credential = %v, want Unauthenticated", err)
	}
}

// engineServes fails unless engine has a fresh credential read and admits
// a's credentials now. A canceled loop stops reading, and its snapshot is
// stale two seconds after its last read; an Actor minted earlier proves
// nothing about the loop.
func (a *realAuth) engineServes(t *testing.T, engine *auth.Engine) {
	t.Helper()
	if !engine.Ready() {
		t.Fatal("held frame's Engine has no fresh credential read: its loop stopped")
	}
	for _, token := range a.tokens() {
		actor, err := engine.Authenticate("Bearer " + token)
		if err == nil {
			_, err = engine.Check(actor, auth.Read)
		}
		if err != nil {
			t.Fatalf("held frame's Engine refused a valid credential: %v", err)
		}
	}
}

// released fails unless the Store's singleton lock is free again.
func released(t *testing.T, f *lifetimeServerFixture) {
	t.Helper()
	s, err := store.Open(f.data)
	if err != nil {
		f.hold(t, err)
		t.Fatalf("store.Open after a nil server return = %v, want the Store released", err)
	}
	if err := s.Close(); err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
}

func closes(in *testIntegration) int {
	n := 0
	for _, e := range in.recorded() {
		if e == "close" {
			n++
		}
	}
	return n
}

// TestIntegrationRealAuthStop checks, with the actual file Engine, that a
// clean stop closes the integration once, joins the
// Engine's loop and releases the Store.
func TestIntegrationRealAuthStop(t *testing.T) {
	for _, name := range realAuthModes {
		t.Run(name, func(t *testing.T) {
			f := lifetimeFixture(t)
			a := newRealAuth(t, f)
			in := newTestIntegration()
			f.start(t, a.config(in))
			a.serves(t, f)
			f.stopped(t)
			if got, want := in.recorded(), []string{"install", "targets", "run", "run canceled", "close"}; !slices.Equal(got, want) {
				t.Fatalf("integration calls = %q, want %q", got, want)
			}
			released(t, f)
		})
	}
}

// TestIntegrationRealAuthLateClose checks that a nil Close returned after
// its own deadline still lets the actual Engine's loop join, in its own
// ten-second phase, and the server release the Store.
//
// The server joined Control and the integration's Run on one ten-second
// context before calling Close. This Close waits out its own ten-second
// context, which began later, so the shared one has expired by the time
// Close returns. On I1, which waited for authentication on it too,
// shutdown held unless the loop had already returned at the wait's first
// non-blocking check; whether it had depends on scheduling. Its failure
// there is therefore a diagnostic, not a deterministic witness.
func TestIntegrationRealAuthLateClose(t *testing.T) {
	for _, name := range realAuthModes {
		t.Run(name, func(t *testing.T) {
			f := lifetimeFixture(t)
			a := newRealAuth(t, f)
			in := newTestIntegration()
			in.closeWait = true
			in.expired, in.overrun = make(chan struct{}), make(chan struct{})
			f.start(t, a.config(in))
			a.serves(t, f)
			f.cancel()
			integrationReceive(t, in.expired, "integration Close deadline")
			close(in.overrun)
			integrationReceive(t, f.done, "server Run after its late integration Close")
			if f.result != nil {
				t.Fatalf("Run = %v, want nil: authentication has its own join after a late nil Close", f.result)
			}
			if n := closes(in); n != 1 {
				t.Fatalf("%d integration Close calls, want 1", n)
			}
			released(t, f)
		})
	}
}

// TestIntegrationRealAuthHeldClose checks that a held integration Close
// leaves the actual Engine running: authentication is canceled only after
// a positive Close, so the held frame's Engine keeps reading and admitting
// its credentials, and the Store stays open.
func TestIntegrationRealAuthHeldClose(t *testing.T) {
	for _, name := range realAuthModes {
		t.Run(name, func(t *testing.T) {
			f := lifetimeFixture(t)
			a := newRealAuth(t, f)
			in := newTestIntegration()
			in.closeWait = true
			f.start(t, a.config(in))
			a.serves(t, f)
			f.cancel()
			integrationReceive(t, f.done, "server Run with a cooperative integration Close")
			var held *RunError
			if !errors.As(f.result, &held) || held.stage != "integration closure remains held" || !errors.Is(f.result, context.DeadlineExceeded) {
				t.Fatalf("Run = %v, want a RunError at %q with the Close deadline's error", f.result, "integration closure remains held")
			}
			// Retain the same held original before any further assertion
			// can stop the test.
			f.expectedHeld = context.DeadlineExceeded
			f.hold(t, f.result)
			frame := held.frame
			if frame.integration != Integration(in) {
				t.Fatalf("held integration %v, want the same original", frame.integration)
			}
			if _, _, err := frame.store.RetentionPolicy(); err != nil {
				t.Fatalf("Store read in the held frame = %v, want the Store still open", err)
			}
			// Close waited out its ten-second deadline, far beyond the
			// two-second freshness envelope, so only a running loop reads.
			a.engineServes(t, frame.engine)
			if n := closes(in); n != 1 {
				t.Fatalf("%d integration Close calls, want 1", n)
			}
		})
	}
}

// modeledAuthFrame starts a real private frame, with the legacy Engine,
// which has no loop and closes authDone at start, and gives it an
// explicitly MODELED original authDone. The frame's own cancelAuth is
// wrapped only to record its call. This models the frame's authentication
// owner; it says nothing about a blocked real Engine, syscall or process.
func modeledAuthFrame(t *testing.T, f *lifetimeServerFixture, in *testIntegration) (frame *serverRun, done, canceled chan struct{}) {
	t.Helper()
	frame = &serverRun{}
	if err := frame.start(context.Background(), Config{DataDir: f.data, Listen: "127.0.0.1:0", DisableLocal: true, Install: in.install}); err != nil {
		if cerr := frame.closeStartup(err); cerr != nil {
			f.hold(t, cerr)
		}
		t.Fatal(err)
	}
	f.frame = frame // shut down by the fixture if the test stops early
	select {
	case <-frame.authDone:
	default:
		t.Fatal("legacy authentication has a loop")
	}
	done, canceled = make(chan struct{}), make(chan struct{})
	frame.authDone = done
	cancel, record := frame.cancelAuth, sync.OnceFunc(func() { close(canceled) })
	frame.cancelAuth = func() { cancel(); record() }
	return frame, done, canceled
}

// TestIntegrationModeledAuthJoin drives the frame's own shutdown against a
// MODELED original authDone. Closed, shutdown releases everything after
// one Close. Open, shutdown spends the fresh auth phase and holds: the
// integration is already positively closed, once, while the same frame,
// channel, Engine and still-open Store are retained.
func TestIntegrationModeledAuthJoin(t *testing.T) {
	t.Run("closed", func(t *testing.T) {
		f := lifetimeFixture(t)
		in := newTestIntegration()
		frame, done, canceled := modeledAuthFrame(t, f, in)
		close(done)
		err := frame.shutdown(nil)
		f.frame = nil
		if err != nil {
			f.hold(t, err)
			t.Fatalf("shutdown = %v, want nil once the original authDone is closed", err)
		}
		select {
		case <-canceled:
		default:
			t.Fatal("shutdown did not cancel the frame's authentication")
		}
		if got, want := in.recorded(), []string{"install", "targets", "run", "run canceled", "close"}; !slices.Equal(got, want) {
			t.Fatalf("integration calls = %q, want %q", got, want)
		}
		released(t, f)
	})
	t.Run("held", func(t *testing.T) {
		f := lifetimeFixture(t)
		in := newTestIntegration()
		frame, done, canceled := modeledAuthFrame(t, f, in)
		engine, s := frame.engine, frame.store
		err := frame.shutdown(nil)
		var held *RunError
		if !errors.As(err, &held) || held.stage != "original authentication remains held" || !errors.Is(err, context.DeadlineExceeded) {
			if err != nil {
				f.hold(t, err)
			}
			f.frame = nil // shutdown ran; it is never run again
			t.Fatalf("shutdown = %v, want a RunError at %q with the auth join's deadline", err, "original authentication remains held")
		}
		// Retain the same held original before any further assertion can
		// stop the test, then release the fixture's frame reference.
		f.expectedHeld = context.DeadlineExceeded
		f.hold(t, err)
		f.frame = nil
		if held.frame != frame || frame.authDone != done || frame.engine != engine || frame.store != s {
			t.Fatal("held frame, authDone, Engine or Store is not the original")
		}
		select {
		case <-canceled:
		default:
			t.Fatal("authentication was not canceled before its join")
		}
		select {
		case <-done:
			t.Fatal("the modeled original authDone was closed by shutdown")
		default:
		}
		if frame.integration != nil || closes(in) != 1 {
			t.Fatalf("integration %v after %d Close calls, want it closed once before authentication", frame.integration, closes(in))
		}
		if _, _, err := s.RetentionPolicy(); err != nil {
			t.Fatalf("Store read while authentication is held = %v, want the Store still open", err)
		}
		// Only the original notification ends; the frame stays held.
		close(done)
	})
}
