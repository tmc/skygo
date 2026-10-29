package agentssh

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// A countingConn wraps Dial's actual TCP connection. It counts the Close
// calls that reach the connection; Close waits for gate, if set, closes the
// connection and returns closeErr in place of its result, if set. A
// nonzero deadlineErr is returned by SetDeadline instead of setting it,
// and a nonzero resetErr only by SetDeadline(time.Time{}). closeErr,
// deadlineErr and resetErr are MODELED failures.
type countingConn struct {
	net.Conn
	closeErr    error
	deadlineErr error
	resetErr    error
	gate        chan struct{}
	entered     chan struct{} // closed when the first Close starts

	mu     sync.Mutex
	closes int
}

func (c *countingConn) Close() error {
	c.mu.Lock()
	c.closes++
	first := c.closes == 1
	c.mu.Unlock()
	if first {
		close(c.entered)
	}
	if c.gate != nil {
		<-c.gate
	}
	err := c.Conn.Close()
	if c.closeErr != nil {
		return c.closeErr
	}
	return err
}

func (c *countingConn) SetDeadline(t time.Time) error {
	if c.deadlineErr != nil {
		return c.deadlineErr
	}
	if c.resetErr != nil && t.IsZero() {
		return c.resetErr
	}
	return c.Conn.SetDeadline(t)
}

// A waitConn wraps the actual ssh.Conn a real handshake returned. Its
// Wait closes entered, waits for gate, if set, and then calls the
// original Wait and records its result. Every other method is the
// original's.
type waitConn struct {
	ssh.Conn
	gate    chan struct{}
	entered chan struct{}

	mu    sync.Mutex
	waits int
	err   error // the original Wait's result
}

func newWaitConn(gate chan struct{}) *waitConn {
	return &waitConn{gate: gate, entered: make(chan struct{})}
}

func (c *waitConn) Wait() error {
	c.mu.Lock()
	c.waits++
	first := c.waits == 1
	c.mu.Unlock()
	if first {
		close(c.entered)
	}
	if c.gate != nil {
		<-c.gate
	}
	err := c.Conn.Wait()
	c.mu.Lock()
	c.err = err
	c.mu.Unlock()
	return err
}

func (c *waitConn) result() (waits int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.waits, c.err
}

// wrapHandshake runs the actual ssh.NewClientConn and, if it succeeds,
// calls then and returns the connection wrapped in wc.
func wrapHandshake(wc *waitConn, then func()) func(net.Conn, string, *ssh.ClientConfig) (ssh.Conn, <-chan ssh.NewChannel, <-chan *ssh.Request, error) {
	return func(c net.Conn, addr string, config *ssh.ClientConfig) (ssh.Conn, <-chan ssh.NewChannel, <-chan *ssh.Request, error) {
		conn, chans, reqs, err := ssh.NewClientConn(c, addr, config)
		if err != nil {
			return conn, chans, reqs, err
		}
		if then != nil {
			then()
		}
		wc.Conn = conn
		return wc, chans, reqs, nil
	}
}

func (c *countingConn) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closes
}

// countingDial dials address, or to if it is set, and stores the
// connection, edited by edit, in *conn.
func countingDial(conn **countingConn, to string, edit func(*countingConn)) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		if to != "" {
			address = to
		}
		nc, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		c := &countingConn{Conn: nc, entered: make(chan struct{})}
		if edit != nil {
			edit(c)
		}
		*conn = c
		return c, nil
	}
}

// silentServer accepts one TCP connection and never sends a byte: a MODEL
// of an SSH server that stalls before its version line. accepted is
// closed when the connection arrives.
func silentServer(t *testing.T) (address string, accepted <-chan struct{}) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	acc := make(chan struct{})
	done := make(chan struct{})
	var conn net.Conn
	go func() {
		defer close(done)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		conn = c
		close(acc)
	}()
	t.Cleanup(func() {
		ln.Close()
		<-done
		if conn != nil {
			conn.Close()
		}
	})
	return ln.Addr().String(), acc
}

func dialError(t *testing.T, err error) *DialError {
	t.Helper()
	var e *DialError
	if !errors.As(err, &e) {
		t.Fatalf("Dial = %v, want a *DialError", err)
	}
	if e.raw == nil {
		t.Fatal("DialError does not retain its connection")
	}
	return e
}

// TestDialOwnedClose is the success twin: the client's Closes share the
// connection's one Close, and Wait returns after it.
func TestDialOwnedClose(t *testing.T) {
	f := newFixture(t)
	var cc *countingConn
	client, err := dialer{countingDial(&cc, "", nil), ssh.NewClientConn}.dialSSH(context.Background(), f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.waitHandshake()
	if f.handshakeCount() != 1 {
		t.Errorf("handshakes = %d, want 1", f.handshakeCount())
	}
	first := client.Close()
	if first != nil {
		t.Fatalf("Close = %v", first)
	}
	if err := client.Close(); err != first {
		t.Errorf("second Close = %v, want the first result %v", err, first)
	}
	// Wait returns the connection's shutdown cause, which need not be nil;
	// its return is the join.
	client.Wait()
	if n := cc.count(); n != 1 {
		t.Errorf("connection closed %d times, want 1", n)
	}
}

// TestDialCanceledInHandshake checks that a cancellation during the
// handshake closes the connection once, through Dial's callback and
// x/crypto's own Close together, and that Dial joins the callback. It
// cancels only once Dial has entered its handshake, immediately before
// the actual ssh.NewClientConn, so the connection and the callback have
// been published.
func TestDialCanceledInHandshake(t *testing.T) {
	f := newFixture(t)
	address, accepted := silentServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var cc *countingConn
	entered := make(chan struct{})
	d := dialer{
		dial: countingDial(&cc, address, nil),
		handshake: func(c net.Conn, addr string, config *ssh.ClientConfig) (ssh.Conn, <-chan ssh.NewChannel, <-chan *ssh.Request, error) {
			close(entered)
			return ssh.NewClientConn(c, addr, config)
		},
	}
	done := make(chan error, 1)
	go func() {
		client, err := d.dialSSH(ctx, f.cfg)
		if client != nil {
			client.Close()
		}
		done <- err
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("Dial = %v before its handshake", err)
	}
	<-accepted
	cancel()
	err := <-done
	if !errors.Is(err, context.Canceled) || !strings.HasPrefix(err.Error(), "agentssh: ssh handshake: ") {
		t.Fatalf("Dial = %v, want a canceled handshake", err)
	}
	e := dialError(t, err)
	if e.handshake == nil || e.close != nil || e.wait != nil {
		t.Errorf("DialError = %+v; want the handshake's own error and a clean Close", e)
	}
	if n := cc.count(); n != 1 {
		t.Errorf("connection closed %d times, want 1", n)
	}
}

// TestDialLateHandshake checks that a handshake that succeeds after ctx
// ended settles that same connection: Dial waits for its callback's
// Close, which is MODELED to block and fail, reports that result, and
// joins the connection's Wait.
func TestDialLateHandshake(t *testing.T) {
	f := newFixture(t)
	closeErr := errors.New("modeled close failure")
	gate := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var cc *countingConn
	returned := make(chan struct{})
	d := dialer{
		dial: countingDial(&cc, "", func(c *countingConn) { c.gate, c.closeErr = gate, closeErr }),
		handshake: func(c net.Conn, addr string, config *ssh.ClientConfig) (ssh.Conn, <-chan ssh.NewChannel, <-chan *ssh.Request, error) {
			conn, chans, reqs, err := ssh.NewClientConn(c, addr, config)
			if err == nil {
				f.waitHandshake()
			}
			cancel()
			<-cc.entered // the callback is closing the connection
			close(returned)
			return conn, chans, reqs, err
		},
	}
	done := make(chan error, 1)
	go func() {
		client, err := d.dialSSH(ctx, f.cfg)
		if client != nil {
			client.Close()
		}
		done <- err
	}()
	<-returned
	// Dial cannot return before the callback's Close does: its result is
	// set only once the gate opens.
	close(gate)
	err := <-done
	if f.handshakeCount() != 1 {
		t.Errorf("handshakes = %d, want 1", f.handshakeCount())
	}
	e := dialError(t, err)
	if e.handshake != nil || e.close != closeErr || e.CloseErr() != closeErr {
		t.Errorf("DialError = %+v; want a successful handshake and the callback's Close result", e)
	}
	if !errors.Is(err, context.Canceled) || !errors.Is(err, closeErr) || !strings.HasSuffix(err.Error(), " (close: modeled close failure)") {
		t.Errorf("Dial = %v", err)
	}
	if n := cc.count(); n != 1 {
		t.Errorf("connection closed %d times, want 1", n)
	}
}

// TestDialLateWaitJoined checks that Dial, after a handshake that succeeded
// late, does not return before the same connection's Wait returns, and
// that it keeps that Wait's result. The connection and its Wait are the
// actual ones; only the gate in front of Wait is MODELED.
func TestDialLateWaitJoined(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var cc *countingConn
	wc := newWaitConn(make(chan struct{}))
	d := dialer{
		dial: countingDial(&cc, "", nil),
		handshake: wrapHandshake(wc, func() {
			cancel()
			<-cc.entered // the callback is closing the connection
		}),
	}
	done := make(chan error, 1)
	go func() {
		client, err := d.dialSSH(ctx, f.cfg)
		if client != nil {
			client.Close()
		}
		done <- err
	}()
	select {
	case <-wc.entered:
	case err := <-done:
		t.Fatalf("Dial = %v without waiting for the connection", err)
	}
	select {
	case err := <-done:
		t.Fatalf("Dial = %v while the connection's Wait was held", err)
	default:
	}
	close(wc.gate)
	err := <-done
	e := dialError(t, err)
	waits, werr := wc.result()
	if waits != 1 || e.wait != werr {
		t.Errorf("Wait called %d times, DialError.wait = %v; want 1 and the original result %v", waits, e.wait, werr)
	}
	if e.handshake != nil || e.close != nil || !errors.Is(err, context.Canceled) {
		t.Errorf("Dial = %v; want a canceled late handshake and a clean Close", err)
	}
	if n := cc.count(); n != 1 {
		t.Errorf("connection closed %d times, want 1", n)
	}
}

// TestDialDeadlineResetRefused checks that an authenticated connection
// whose deadline cannot be cleared is closed once and its Wait joined,
// and that no client is returned. The reset failure and the second
// case's Close failure are MODELED; the handshake is actual.
func TestDialDeadlineResetRefused(t *testing.T) {
	resetErr := errors.New("modeled deadline reset failure")
	for _, closeErr := range []error{nil, errors.New("modeled close failure")} {
		f := newFixture(t)
		var cc *countingConn
		wc := newWaitConn(nil)
		d := dialer{
			dial:      countingDial(&cc, "", func(c *countingConn) { c.resetErr, c.closeErr = resetErr, closeErr }),
			handshake: wrapHandshake(wc, f.waitHandshake),
		}
		client, err := d.dialSSH(context.Background(), f.cfg)
		if client != nil {
			client.Close()
			client.Wait()
			t.Fatal("Dial returned a client whose deadline was not cleared")
		}
		e := dialError(t, err)
		if !errors.Is(err, resetErr) || e.CloseErr() != closeErr || closeErr != nil && !errors.Is(err, closeErr) {
			t.Errorf("Dial = %v, CloseErr = %v; want the reset failure and Close result %v", err, e.CloseErr(), closeErr)
		}
		if waits, werr := wc.result(); waits != 1 || e.wait != werr {
			t.Errorf("Wait called %d times, DialError.wait = %v; want 1 and the original result %v", waits, e.wait, werr)
		}
		if n := cc.count(); n != 1 {
			t.Errorf("connection closed %d times, want 1", n)
		}
		if f.handshakeCount() != 1 {
			t.Errorf("handshakes = %d, want 1", f.handshakeCount())
		}
	}
}

func TestOwnershipUnsettled(t *testing.T) {
	closeErr := errors.New("modeled close failure")
	settledDial := &DialError{op: "dial", err: errors.New("refused")}
	unsettledDial := &DialError{op: "dial", err: errors.New("refused"), close: closeErr}
	held := &heldError{}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"plain", closeErr, false},
		{"settled dial", settledDial, false},
		{"wrapped settled dial", fmt.Errorf("open: %w", settledDial), false},
		{"settled request", &requestError{op: "dial", err: errors.New("refused")}, false},
		{"unsettled dial", unsettledDial, true},
		{"wrapped unsettled dial", fmt.Errorf("open: %w", unsettledDial), true},
		{"joined", errors.Join(settledDial, fmt.Errorf("open: %w", unsettledDial)), true},
		{"request close", &requestError{op: "read", err: errors.New("reset"), close: closeErr}, true},
		{"held", fmt.Errorf("read: %w", held), true},
		{"request held", &requestError{op: "dial", err: errors.New("refused"), close: held}, true},
		{"file close", &closeError{owner: io.NopCloser(nil), err: closeErr}, true},
	}
	for _, tt := range tests {
		if got := OwnershipUnsettled(tt.err); got != tt.want {
			t.Errorf("%s: OwnershipUnsettled = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestValidators(t *testing.T) {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	canonical := ssh.FingerprintSHA256(newSigner(t).PublicKey())
	// The last of 43 digits carries 2 unused bits; the next digit sets one.
	last := strings.IndexByte(alphabet, canonical[len(canonical)-1])
	noncanonical := canonical[:len(canonical)-1] + alphabet[last+1:last+2]
	for _, tt := range []struct {
		s    string
		want bool
	}{
		{canonical, true},
		{"SHA256:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU", true},
		{"SHA256:abc", false},
		{canonical + "=", false},
		{noncanonical, false},
		{"sha256:" + strings.TrimPrefix(canonical, "SHA256:"), false},
		{"SHA256:", false},
		{"", false},
	} {
		if got := ValidFingerprint(tt.s); got != tt.want {
			t.Errorf("ValidFingerprint(%q) = %v, want %v", tt.s, got, tt.want)
		}
	}
	for _, tt := range []struct {
		s    string
		want bool
	}{
		{"dgx.example:22", true},
		{"192.0.2.1:2222", true},
		{"[2001:db8::1]:22", true},
		{"[2001:DB8::1]:22", false},
		{"DGX:22", false},
		{"dgx:022", false},
		{"dgx:0", false},
		{"[fe80::1%en0]:22", false},
	} {
		if got := ValidAddress(tt.s); got != tt.want {
			t.Errorf("ValidAddress(%q) = %v, want %v", tt.s, got, tt.want)
		}
	}
}

// TestDialHandshakeRefusedClosedOnce checks that a refused host key leaves
// the connection closed once, with x/crypto's Close and Dial's sharing one
// result, and that errors.As still finds the refusal. The failing twin's
// Close is MODELED to fail; the error then keeps the connection.
func TestDialHandshakeRefusedClosedOnce(t *testing.T) {
	for _, closeErr := range []error{nil, errors.New("modeled close failure")} {
		f := newFixture(t)
		c := f.cfg
		c.knownHostsFile = f.write("known_hosts.other", knownhosts.Line([]string{c.sshAddress}, newSigner(t).PublicKey())+"\n", 0o644)
		var cc *countingConn
		client, err := dialer{countingDial(&cc, "", func(w *countingConn) { w.closeErr = closeErr }), ssh.NewClientConn}.dialSSH(context.Background(), c)
		if client != nil {
			client.Close()
			t.Fatal("Dial succeeded with a mismatched host key")
		}
		var hk *hostKeyError
		if !errors.As(err, &hk) || !strings.HasPrefix(err.Error(), "agentssh: ssh handshake: ") {
			t.Fatalf("Dial = %v, want a *hostKeyError", err)
		}
		e := dialError(t, err)
		if e.CloseErr() != closeErr {
			t.Errorf("CloseErr = %v, want %v", e.CloseErr(), closeErr)
		}
		if hasClose := strings.Contains(err.Error(), "(close: "); hasClose != (closeErr != nil) {
			t.Errorf("Dial = %q; close reported %v, want %v", err, hasClose, closeErr != nil)
		}
		if closeErr != nil && !errors.Is(err, closeErr) {
			t.Errorf("Dial = %v, want it to wrap the Close failure", err)
		}
		if n := cc.count(); n != 1 {
			t.Errorf("connection closed %d times, want 1", n)
		}
	}
}

// TestDialDeadlineRefused checks that a connection whose deadline cannot
// be set is closed once and reported, before any handshake.
func TestDialDeadlineRefused(t *testing.T) {
	f := newFixture(t)
	deadlineErr := errors.New("modeled deadline failure")
	var cc *countingConn
	d := dialer{countingDial(&cc, "", func(c *countingConn) { c.deadlineErr = deadlineErr }), ssh.NewClientConn}
	client, err := d.dialSSH(context.Background(), f.cfg)
	if client != nil {
		client.Close()
		t.Fatal("Dial succeeded")
	}
	if !errors.Is(err, deadlineErr) || err.Error() != "agentssh: dial: modeled deadline failure" {
		t.Fatalf("Dial = %v", err)
	}
	if e := dialError(t, err); e.CloseErr() != nil {
		t.Errorf("CloseErr = %v", e.CloseErr())
	}
	if n := cc.count(); n != 1 {
		t.Errorf("connection closed %d times, want 1", n)
	}
	if f.handshakeCount() != 0 {
		t.Errorf("handshakes = %d, want 0", f.handshakeCount())
	}
}
