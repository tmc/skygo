package bootstrap

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

// A countingClient wraps an actual *ssh.Client and counts the Close and
// Wait calls SSHRunner makes. A nonzero closeErr is a MODELED Close
// failure returned after the actual Close. A non-nil waited is closed
// once the first actual Wait has returned.
type countingClient struct {
	*ssh.Client
	closeErr error
	waited   chan struct{}
	waitOnce sync.Once

	mu            sync.Mutex
	closes, waits int
	waitErr       error // the actual Wait's result
}

func (c *countingClient) Close() error {
	c.mu.Lock()
	c.closes++
	c.mu.Unlock()
	err := c.Client.Close()
	if c.closeErr != nil {
		return c.closeErr
	}
	return err
}

func (c *countingClient) Wait() error {
	c.mu.Lock()
	c.waits++
	c.mu.Unlock()
	err := c.Client.Wait()
	c.mu.Lock()
	c.waitErr = err
	c.mu.Unlock()
	if c.waited != nil {
		c.waitOnce.Do(func() { close(c.waited) })
	}
	return err
}

func (c *countingClient) counts() (closes, waits int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closes, c.waits
}

// TestRunnerSuccessKeepsClient is the success twin: a command that exits
// leaves the client open, and the session's normal end is not an error.
func TestRunnerSuccessKeepsClient(t *testing.T) {
	s := newExecServer(t)
	c := &countingClient{Client: s.client}
	stdout, stderr, err := SSHRunner{Client: c}.Run(context.Background(), "echo ok", nil)
	if err != nil || string(stdout) != "ok\n" || len(stderr) != 0 {
		t.Fatalf("Run = %q, %q, %v", stdout, stderr, err)
	}
	if closes, waits := c.counts(); closes != 0 || waits != 0 {
		t.Errorf("closes %d, waits %d; want none", closes, waits)
	}
	// The client is still usable.
	if stdout, _, err := (SSHRunner{Client: c}).Run(context.Background(), "echo again", nil); err != nil || string(stdout) != "again\n" {
		t.Fatalf("second Run = %q, %v", stdout, err)
	}
}

// TestRunnerCancelClosesOnce checks that canceling a pending session open
// closes the actual client once, waits for that Close and the client's
// Wait before Run returns, and reports the Close failure.
func TestRunnerCancelClosesOnce(t *testing.T) {
	s := newExecServer(t)
	hold := make(chan struct{})
	s.mu.Lock()
	s.holdSessions = hold
	s.mu.Unlock()
	t.Cleanup(func() { close(hold) })
	closeErr := errors.New("modeled close failure")
	c := &countingClient{Client: s.client, closeErr: closeErr}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, err := SSHRunner{Client: c}.Run(ctx, "true", nil)
		done <- err
	}()
	select {
	case <-s.sessionArrived:
	case <-time.After(3 * time.Second):
		t.Fatal("session open did not arrive")
	}
	cancel()
	err := <-done
	if !errors.Is(err, context.Canceled) || !errors.Is(err, closeErr) {
		t.Fatalf("Run = %v; want canceled with the Close failure", err)
	}
	if closes, waits := c.counts(); closes != 1 || waits != 1 {
		t.Errorf("closes %d, waits %d; want 1 and 1 before Run returned", closes, waits)
	}
	if len(s.records()) != 0 {
		t.Errorf("commands ran: %v", s.records())
	}
}

// A gatedReader is a MODEL stdin. Its first Read closes started, which
// x/crypto does only after the server accepted the command, then waits
// for release and reports EOF.
type gatedReader struct {
	started, release chan struct{}
	once             sync.Once
}

func (r *gatedReader) Read([]byte) (int, error) {
	r.once.Do(func() { close(r.started) })
	<-r.release
	return 0, io.EOF
}

// TestRunnerCancelStartedCommand checks that canceling a command the
// server has accepted closes the actual client once, does not return
// before the command's own Run returns, which here waits for its stdin,
// and keeps that Run's result and Client's Wait result alongside the
// cancellation and the Close failure. The Close failure and the gated
// stdin are MODELED; the client, session and server are actual.
func TestRunnerCancelStartedCommand(t *testing.T) {
	s := newExecServer(t)
	closeErr := errors.New("modeled close failure")
	c := &countingClient{Client: s.client, closeErr: closeErr}
	stdin := &gatedReader{started: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(stdin.release) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		// The command never exits 0, so its Run cannot succeed.
		_, _, err := SSHRunner{Client: c}.Run(ctx, "cat >/dev/null; exit 3", stdin)
		done <- err
	}()
	select {
	case <-stdin.started:
	case <-time.After(3 * time.Second):
		t.Fatal("command was not started")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("Run = %v before the command's Run returned", err)
	default:
	}
	release.Do(func() { close(stdin.release) })
	err := <-done
	if !errors.Is(err, context.Canceled) || !errors.Is(err, closeErr) {
		t.Fatalf("Run = %v; want canceled with the Close failure", err)
	}
	var ce *canceledError
	if !errors.As(err, &ce) {
		t.Fatalf("Run = %#v, want a *canceledError", err)
	}
	c.mu.Lock()
	closes, waits, waitErr := c.closes, c.waits, c.waitErr
	c.mu.Unlock()
	if closes != 1 || waits != 1 {
		t.Errorf("closes %d, waits %d; want 1 and 1", closes, waits)
	}
	// Wait's result is the connection's shutdown cause: kept, whatever
	// it is, and not a failure.
	if ce.wait != waitErr {
		t.Errorf("canceledError.wait = %v, want Wait's result %v", ce.wait, waitErr)
	}
	// The command's channel closed under it, so its own Run reports that.
	if ce.run == nil {
		t.Error("canceledError.run = nil, want the cut-short command's result")
	}
}

// TestRunnerCancelAfterSuccess checks the race in which the command's
// session completes successfully and ctx is canceled before Run stops its
// cancellation callback. The client, session, server and command are
// actual; the scheduling frontier is MODELED by holding runThen after
// runSession returns until the callback's actual Close and Wait return.
// Run then reports the cancellation, not a nil inner error, and keeps the
// command's output and Wait's result.
func TestRunnerCancelAfterSuccess(t *testing.T) {
	s := newExecServer(t)
	c := &countingClient{Client: s.client, waited: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan error, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	type result struct {
		stdout []byte
		err    error
	}
	done := make(chan result, 1)
	go func() {
		stdout, _, err := SSHRunner{Client: c}.runThen(ctx, "echo ok", nil, func(err error) {
			entered <- err
			<-release
		})
		done <- result{stdout, err}
	}()
	select {
	case err := <-entered:
		if err != nil {
			t.Fatalf("runSession = %v, want success", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runSession did not return")
	}
	cancel()
	select {
	case <-c.waited:
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation callback did not Close and Wait")
	}
	releaseOnce.Do(func() { close(release) })
	r := <-done
	if string(r.stdout) != "ok\n" {
		t.Errorf("stdout = %q, want %q", r.stdout, "ok\n")
	}
	var msg string
	func() {
		defer func() {
			if p := recover(); p != nil {
				t.Errorf("Run error's Error panicked: %v", p)
			}
		}()
		if r.err != nil {
			msg = r.err.Error()
		}
	}()
	if r.err == nil || msg == "" || !errors.Is(r.err, context.Canceled) {
		t.Fatalf("Run = %q; want the cancellation", msg)
	}
	var ce *canceledError
	if !errors.As(r.err, &ce) {
		t.Fatalf("Run = %#v, want a *canceledError", r.err)
	}
	c.mu.Lock()
	closes, waits, waitErr := c.closes, c.waits, c.waitErr
	c.mu.Unlock()
	if closes != 1 || waits != 1 {
		t.Errorf("closes %d, waits %d; want 1 and 1", closes, waits)
	}
	if ce.wait != waitErr {
		t.Errorf("canceledError.wait = %v, want Wait's result %v", ce.wait, waitErr)
	}
	// The command completed before the cancellation, so none was cut short.
	if ce.run != nil {
		t.Errorf("canceledError.run = %v, want nil", ce.run)
	}
	if recs := s.records(); len(recs) != 1 || recs[0].command != "echo ok" {
		t.Errorf("commands ran: %v, want one echo ok", recs)
	}
}

// A modelRunner is a MODEL Runner returning fixed results.
type modelRunner struct {
	stderr []byte
	err    error
}

func (m modelRunner) Run(context.Context, string, io.Reader) ([]byte, []byte, error) {
	return nil, m.stderr, m.err
}

type typedCause struct{}

func (*typedCause) Error() string { return "modeled typed cause" }

// TestRunKeepsRunnerError checks that a failed command reported by its
// diagnostic keeps the Runner's original error, as it does without one.
func TestRunKeepsRunnerError(t *testing.T) {
	closeErr := errors.New("modeled close failure")
	typed := &typedCause{}
	runErr := errors.Join(context.Canceled, closeErr, typed)
	for _, tt := range []struct {
		name, stderr, want string
	}{
		{"diagnostic", "progress\nbootstrap: agent \x1b[1mstopped\x7f\n", "bootstrap: status: bootstrap: agent [1mstopped"},
		{"no diagnostic", "", "bootstrap: status: " + runErr.Error()},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := run(context.Background(), modelRunner{[]byte(tt.stderr), runErr}, nil, "status")
			if err == nil || err.Error() != tt.want {
				t.Fatalf("run = %q, want %q", err, tt.want)
			}
			var got *typedCause
			if !errors.Is(err, context.Canceled) || !errors.Is(err, closeErr) || !errors.As(err, &got) || got != typed {
				t.Errorf("run = %v; want every original cause", err)
			}
		})
	}
}

// A modelClient is a MODEL SSH client. NewSession stays pending until
// Close, and Wait returns io.EOF, a shutdown cause, once Close has run.
type modelClient struct {
	opening  chan struct{} // closed when NewSession starts
	closed   chan struct{} // closed by the first Close
	closeErr error

	mu            sync.Mutex
	closes, waits int
	opens         int
}

var errModelOpen = errors.New("modeled: connection closed during session open")

func (m *modelClient) NewSession() (*ssh.Session, error) {
	m.mu.Lock()
	m.opens++
	m.mu.Unlock()
	close(m.opening)
	<-m.closed
	return nil, errModelOpen
}

func (m *modelClient) Close() error {
	m.mu.Lock()
	m.closes++
	first := m.closes == 1
	m.mu.Unlock()
	if first {
		close(m.closed)
	}
	return m.closeErr
}

func (m *modelClient) Wait() error {
	m.mu.Lock()
	m.waits++
	m.mu.Unlock()
	<-m.closed
	return io.EOF
}

func TestRunnerModeledCancel(t *testing.T) {
	closeErr := errors.New("modeled close failure")
	tests := []struct {
		name     string
		closeErr error
		before   bool // ctx canceled before Run
	}{
		{"pending open", nil, false},
		{"pending open, failed close", closeErr, false},
		{"canceled before run", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &modelClient{opening: make(chan struct{}), closed: make(chan struct{}), closeErr: tt.closeErr}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.before {
				cancel()
			}
			done := make(chan error, 1)
			go func() {
				_, _, err := SSHRunner{Client: m}.Run(ctx, "true", nil)
				done <- err
			}()
			if !tt.before {
				<-m.opening
				cancel()
			}
			err := <-done
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run = %v, want canceled", err)
			}
			if tt.closeErr != nil && !errors.Is(err, tt.closeErr) {
				t.Errorf("Run = %v, want the Close failure", err)
			}
			if !tt.before && !errors.Is(err, errModelOpen) {
				t.Errorf("Run = %v, want the open's own error", err)
			}
			m.mu.Lock()
			defer m.mu.Unlock()
			// Wait returned io.EOF; Run does not treat that as a failure.
			if m.closes != 1 || m.waits != 1 || errors.Is(err, io.EOF) {
				t.Errorf("closes %d, waits %d, err %v; want one Close and one Wait", m.closes, m.waits, err)
			}
			// It keeps that EOF as the receipt of Wait's return. No
			// command started, so there is no Run result.
			var ce *canceledError
			if !errors.As(err, &ce) || ce.wait != io.EOF || ce.run != nil {
				t.Errorf("Run = %#v; want a *canceledError keeping Wait's io.EOF", err)
			}
			if tt.before && m.opens != 0 {
				t.Errorf("opened %d sessions after cancellation", m.opens)
			}
		})
	}
}
