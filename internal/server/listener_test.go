//go:build linux || darwin

package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
)

// This policy listener controls actual net/http tracking/deferred Close paths.
// It owns no native socket and is not a kernel Close-fault or FD proof.
type policyListener struct {
	entered, stopped    chan struct{}
	enterOnce, stopOnce sync.Once
	calls               atomic.Int32
	acceptErr, closeErr error
}

func (l *policyListener) Accept() (net.Conn, error) {
	l.enterOnce.Do(func() { close(l.entered) })
	<-l.stopped
	return nil, l.acceptErr
}

func (l *policyListener) Close() error {
	n := l.calls.Add(1)
	l.stopOnce.Do(func() { close(l.stopped) })
	if n == 1 {
		return l.closeErr
	}
	return nil
}

func (*policyListener) Addr() net.Addr { return &net.TCPAddr{} }

func TestServerListenerRetainsCloseErrorAfterHTTPUntracks(t *testing.T) {
	original := errors.New("original listener close failed")
	raw := &policyListener{entered: make(chan struct{}), stopped: make(chan struct{}), acceptErr: net.ErrClosed, closeErr: original}
	owner := &serverListener{Listener: raw}
	if returned, err := owner.closeResult(); returned || err != nil {
		t.Fatal("unattempted Close became a positive receipt", returned, err)
	}
	h := &http.Server{Handler: http.NotFoundHandler()}
	served := make(chan error, 1)
	go func() { served <- h.Serve(owner) }()
	lifetimeReceive(t, raw.entered)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.Shutdown(ctx); !errors.Is(err, original) {
		t.Fatal("original Shutdown error lost", err)
	}
	if err := lifetimeReceive(t, served); !errors.Is(err, http.ErrServerClosed) {
		t.Fatal("original Serve return", err)
	}
	// The original Serve has untracked net/http's onceCloseListener. This nil
	// says nothing about its prior failed underlying Close.
	if err := h.Close(); err != nil {
		t.Fatal("later HTTP Close", err)
	}
	returned, err := owner.closeResult()
	if !returned || err != original || raw.calls.Load() != 1 {
		t.Fatal("original Close outcome replaced or retried", returned, err, raw.calls.Load())
	}
	if err := owner.Close(); err != original || raw.calls.Load() != 1 {
		t.Fatal("same owner retried original listener", err, raw.calls.Load())
	}
}

func TestServerUnexpectedServeCloseErrorHoldsDependencies(t *testing.T) {
	f := lifetimeFixture(t)
	s, err := store.Open(f.data)
	if err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	f.opened = s
	engine := auth.NewLegacy("")
	c, err := control.New(s, []backend.Target{{Name: "remote", Binding: "remote-fixture", Backend: &lifetimeRemote{}}}, engine)
	if err != nil {
		f.hold(t, err)
		t.Fatal(err)
	}
	acceptErr := errors.New("original accept failed")
	closeErr := errors.New("original deferred listener close failed")
	raw := &policyListener{entered: make(chan struct{}), stopped: make(chan struct{}), acceptErr: acceptErr, closeErr: closeErr}
	owner := &serverListener{Listener: raw}
	runCtx, cancelRun := context.WithCancel(context.Background())
	_, cancelAuth := context.WithCancel(context.Background())
	frame := &serverRun{store: s, control: c, engine: engine, listener: owner, cancelRun: cancelRun, cancelAuth: cancelAuth, runDone: make(chan struct{}), serveDone: make(chan struct{}), authDone: make(chan struct{})}
	f.frame = frame
	frame.outer = newArtifactHTTPDrain(http.NotFoundHandler())
	frame.http = &http.Server{Handler: frame.outer}
	close(frame.authDone) // Actual legacy mode has no auth function to join.
	go func() { defer close(frame.runDone); frame.runErr = c.Run(runCtx) }()
	go func() { defer close(frame.serveDone); frame.serveErr = frame.http.Serve(owner) }()
	lifetimeReceive(t, raw.entered)
	raw.stopOnce.Do(func() { close(raw.stopped) }) // Unexpected original Accept return.
	lifetimeReceive(t, frame.serveDone)
	if !errors.Is(frame.serveErr, acceptErr) {
		f.hold(t, frame.serveErr)
		t.Fatal("unexpected Serve cause lost")
	}
	failure := frame.shutdown(frame.serveErr)
	var held *RunError
	if !errors.As(failure, &held) || held.frame != frame || !errors.Is(failure, closeErr) || !errors.Is(failure, acceptErr) {
		f.hold(t, failure)
		t.Fatal("original listener/Serve causes or frame lost", failure)
	}
	if frame.listener != owner || owner.Listener != raw || raw.calls.Load() != 1 || frame.store != s {
		f.hold(t, failure)
		t.Fatal("original owner or dependency changed")
	}
	if _, err := s.ArtifactInstallation(); err != nil {
		f.hold(t, failure)
		t.Fatal("listener failure closed Store", err)
	}
	if runCtx.Err() != nil {
		f.hold(t, failure)
		t.Fatal("listener failure canceled Run before positive joins")
	}
	// Expected logical policy HOLD: keep the actual original Run/Store/frame
	// until process exit. No alternate Close, cancel or namespace removal.
	f.expectedHeld = closeErr
	f.hold(t, failure)
}

func TestServerListenerPositiveOriginalClosure(t *testing.T) {
	raw := &policyListener{entered: make(chan struct{}), stopped: make(chan struct{}), acceptErr: net.ErrClosed}
	owner := &serverListener{Listener: raw}
	h := &http.Server{Handler: http.NotFoundHandler()}
	served := make(chan error, 1)
	go func() { served <- h.Serve(owner) }()
	lifetimeReceive(t, raw.entered)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.Shutdown(ctx); err != nil {
		t.Fatal("normal Shutdown", err)
	}
	if err := lifetimeReceive(t, served); !errors.Is(err, http.ErrServerClosed) {
		t.Fatal("normal original Serve", err)
	}
	returned, err := owner.closeResult()
	if !returned || err != nil || raw.calls.Load() != 1 {
		t.Fatal("normal original closure missing", returned, err, raw.calls.Load())
	}
	if err := owner.Close(); err != nil || raw.calls.Load() != 1 {
		t.Fatal("normal owner retried", err, raw.calls.Load())
	}
}
