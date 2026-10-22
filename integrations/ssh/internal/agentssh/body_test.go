package agentssh

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/ssh/knownhosts"
)

var errInjected = errors.New("injected close failure")

// A closeFault is a TCP connection whose Close fails with err and, if
// block is set, waits for block to close first. It counts Close calls.
type closeFault struct {
	net.Conn
	err   error
	block chan struct{}

	mu     sync.Mutex
	closes int
}

func (c *closeFault) Close() error {
	c.mu.Lock()
	c.closes++
	c.mu.Unlock()
	if c.block != nil {
		<-c.block
	}
	c.Conn.Close()
	return c.err
}

func (c *closeFault) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closes
}

// faultDial makes tr dial through a closeFault, which it returns.
func faultDial(tr *transport, err error, block chan struct{}) *closeFault {
	fault := &closeFault{err: err, block: block}
	tr.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		fault.Conn = c
		return fault, nil
	}
	return fault
}

func get(t *testing.T, ctx context.Context, tr *transport, f *fixture, path string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.cfg.endpoint()+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestBodyCancel(t *testing.T) {
	f := newFixture(t)
	_, tr := f.client(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resp := get(t, ctx, tr, f, "/raw/partial")
	buf := make([]byte, 5)
	if _, err := io.ReadFull(resp.Body, buf); err != nil || string(buf) != "hello" {
		t.Fatalf("read %q, %v", buf, err)
	}
	cancel()
	_, err := resp.Body.Read(buf)
	var re *requestError
	if !errors.Is(err, context.Canceled) || !errors.As(err, &re) || re.op != "read" || re.err == nil {
		t.Fatalf("read after cancel: %v, want the read error and the cancellation", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !closed(re.frame.done) {
		t.Error("teardown not joined after Close")
	}
	f.release("partial")
}

func TestBodyEOFIsNotCleanup(t *testing.T) {
	f := newFixture(t)
	_, tr := f.client(nil)
	resp := get(t, context.Background(), tr, f, "/raw/echo/")
	body, err := io.ReadAll(resp.Body)
	if err != nil || string(body) != "ok" {
		t.Fatalf("read %q, %v", body, err)
	}
	frame := resp.Body.(*responseBody).frame
	frame.mu.Lock()
	raw := frame.raw
	frame.mu.Unlock()
	raw.mu.Lock()
	rawClosing := raw.closing
	raw.mu.Unlock()
	if closed(frame.done) || rawClosing {
		t.Fatal("EOF released the request's connection")
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if !closed(frame.done) {
		t.Fatal("Close returned before teardown finished")
	}
	frame.mu.Lock()
	waited := frame.waitErr != nil
	frame.mu.Unlock()
	if !waited {
		t.Error("SSH connection Wait not joined")
	}
}

func TestCloseSticky(t *testing.T) {
	f := newFixture(t)
	_, tr := f.client(nil)
	fault := faultDial(tr, errInjected, nil)
	resp := get(t, context.Background(), tr, f, "/raw/echo/")
	if _, err := io.ReadAll(resp.Body); err != nil {
		t.Fatal(err)
	}
	first := resp.Body.Close()
	second := resp.Body.Close()
	if !errors.Is(first, errInjected) || second != first {
		t.Fatalf("Close = %v then %v, want the first close error both times", first, second)
	}
	if n := fault.count(); n != 1 {
		t.Errorf("connection closed %d times, want 1", n)
	}
}

// TestCloseStickyAfterHandshake checks that x/crypto's own Close after a
// refused handshake is the first Close and is reported once.
func TestCloseStickyAfterHandshake(t *testing.T) {
	f := newFixture(t)
	_, tr := f.client(func(c *config) {
		c.knownHostsFile = f.write("known_hosts.other", knownhosts.Line([]string{c.sshAddress}, newSigner(t).PublicKey())+"\n", 0o644)
	})
	fault := faultDial(tr, errInjected, nil)
	req, _ := http.NewRequest(http.MethodGet, f.cfg.endpoint()+"/raw/echo/", nil)
	_, err := tr.RoundTrip(req)
	var re *requestError
	if !errors.As(err, &re) || re.op != "ssh handshake" || !errors.Is(err, errInjected) {
		t.Fatalf("err = %v, want handshake failure carrying the close error", err)
	}
	if n := fault.count(); n != 1 {
		t.Errorf("connection closed %d times, want 1", n)
	}
}

func TestCloseHeld(t *testing.T) {
	f := newFixture(t)
	_, tr := f.client(nil)
	tr.closeBudget = 20 * time.Millisecond
	block := make(chan struct{})
	fault := faultDial(tr, nil, block)
	resp := get(t, context.Background(), tr, f, "/raw/echo/")
	if _, err := io.ReadAll(resp.Body); err != nil {
		t.Fatal(err)
	}
	var held *heldError
	if err := resp.Body.Close(); !errors.As(err, &held) {
		close(block)
		t.Fatalf("Close = %v, want held", err)
	}
	frame := held.frame
	if closed(frame.done) {
		t.Fatal("teardown finished while the connection Close was blocked")
	}
	if err := resp.Body.Close(); !errors.As(err, &held) {
		t.Errorf("second Close = %v, want still held", err)
	}
	close(block)
	if err := frame.wait(); err != nil {
		t.Fatalf("joined teardown: %v", err)
	}
	if frame.waitErr == nil {
		t.Error("teardown did not join the SSH connection after the blocked Close")
	}
	if err := resp.Body.Close(); err != nil {
		t.Errorf("Close after join = %v, want the first close result", err)
	}
	if n := fault.count(); n != 1 {
		t.Errorf("connection closed %d times, want 1", n)
	}
}
