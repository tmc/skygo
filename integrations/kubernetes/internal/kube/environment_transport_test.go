package kube

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/httpstream"
	spdyserver "k8s.io/apimachinery/pkg/util/httpstream/spdy"
	"k8s.io/client-go/rest"
)

// These tests run the frame's real upgrade (its envDialer, client-go's HTTP
// wrappers and NewClientConnectionWithPings) against apimachinery's real SPDY
// server upgrader in this process. Refusal, proxy and cancellation tests use a
// hijacked or raw loopback peer that itself observes the client end the
// original connection before the test returns; fixture cleanup's server-side
// closes are teardown, never that proof. Failures that a real server cannot
// be made to produce on demand are injected by faultConn and faultStream
// around the real connection and streams; every such error is errSynthetic. Nothing here contacts a Kubernetes cluster.

// errSynthetic marks an injected failure.
var errSynthetic = errors.New("synthetic test fault")

// testWait bounds every wait in these tests.
const testWait = 10 * time.Second

// retainedFixtures keeps the fixtures of frames that did not settle. Their
// servers are not closed, so the original calls keep their peers.
var (
	retainedMu       sync.Mutex
	retainedFixtures []*fakeAPI
)

// fakeAPI is an in-process API server that upgrades every request with
// apimachinery's SPDY server implementation, selecting protocol.
type fakeAPI struct {
	t        *testing.T
	server   *httptest.Server
	protocol string
	streams  chan httpstream.Stream

	mu       sync.Mutex
	override func(w http.ResponseWriter, r *http.Request) bool
	requests []*http.Request
	conns    []httpstream.Connection
	frames   []*envFrame
	hijacked []io.Closer
}

// newFakeAPI starts a TLS fakeAPI. Its cleanup is registered before the
// server starts: it closes the server only if every kept frame settled, and
// otherwise reports and retains the fixture.
func newFakeAPI(t *testing.T, protocol string) *fakeAPI {
	t.Helper()
	return newFakeAPIWith(t, protocol, nil)
}

// newFakeAPIWith is newFakeAPI with edit, if set, applied to the server's
// TLS configuration before it starts.
func newFakeAPIWith(t *testing.T, protocol string, edit func(*tls.Config)) *fakeAPI {
	t.Helper()
	a := &fakeAPI{t: t, protocol: protocol, streams: make(chan httpstream.Stream, 16)}
	t.Cleanup(a.cleanup)
	a.server = httptest.NewUnstartedServer(a)
	if edit != nil {
		a.server.TLS = &tls.Config{}
		edit(a.server.TLS)
	}
	a.server.StartTLS()
	return a
}

func (a *fakeAPI) cleanup() {
	a.mu.Lock()
	frames := append([]*envFrame(nil), a.frames...)
	a.mu.Unlock()
	timeout := time.After(testWait)
	for _, f := range frames {
		select {
		case <-f.done:
		case <-timeout:
			a.t.Errorf("frame did not settle; fixture retained: %v", f.state().Err)
			retainedMu.Lock()
			retainedFixtures = append(retainedFixtures, a)
			retainedMu.Unlock()
			return
		}
	}
	if a.server == nil {
		return
	}
	a.mu.Lock()
	conns, hijacked := a.conns, a.hijacked
	a.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
	for _, c := range hijacked {
		c.Close()
	}
	// Server-side handlers still writing to a socket that a round tripper
	// keeps open end here; the frames above have all settled.
	a.server.CloseClientConnections()
	a.server.Close()
}

// keep registers f; cleanup requires it to settle.
func (a *fakeAPI) keep(f *envFrame) {
	a.mu.Lock()
	a.frames = append(a.frames, f)
	a.mu.Unlock()
}

func (a *fakeAPI) config() *rest.Config {
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: a.server.Certificate().Raw})
	return &rest.Config{Host: a.server.URL, BearerToken: "control-plane-token",
		TLSClientConfig: rest.TLSClientConfig{CAData: ca}}
}

func (a *fakeAPI) transport(t *testing.T) *envTransport {
	t.Helper()
	tr, err := newEnvTransport(a.config(), "workloads")
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

// transportWith is transport with edit applied to a's configuration first.
func (a *fakeAPI) transportWith(t *testing.T, edit func(*rest.Config)) *envTransport {
	t.Helper()
	config := a.config()
	edit(config)
	tr, err := newEnvTransport(config, "workloads")
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func (a *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	a.requests = append(a.requests, r.Clone(context.Background()))
	h := a.override
	a.mu.Unlock()
	if h != nil && h(w, r) {
		return
	}
	if _, err := httpstream.Handshake(r, w, []string{a.protocol}); err != nil {
		return
	}
	conn := spdyserver.NewResponseUpgrader().UpgradeResponse(w, r, func(s httpstream.Stream, _ <-chan struct{}) error {
		a.streams <- s
		return nil
	})
	if conn == nil {
		return
	}
	a.mu.Lock()
	a.conns = append(a.conns, conn)
	a.mu.Unlock()
	<-conn.CloseChan()
}

func (a *fakeAPI) requestList() []*http.Request {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]*http.Request(nil), a.requests...)
}

// takeStreams returns the next n streams the server accepted.
func (a *fakeAPI) takeStreams(t *testing.T, n int) []httpstream.Stream {
	t.Helper()
	var out []httpstream.Stream
	timeout := time.After(testWait)
	for len(out) < n {
		select {
		case s := <-a.streams:
			out = append(out, s)
		case <-timeout:
			t.Fatalf("got %d server streams, want %d", len(out), n)
		}
	}
	return out
}

// waitSettled waits for f to settle and returns its state.
func waitSettled(t *testing.T, f *envFrame) envState {
	t.Helper()
	select {
	case <-f.done:
	case <-time.After(testWait):
		t.Fatalf("frame did not settle: %v", f.state().Err)
	}
	f.mu.Lock()
	running := f.running
	f.mu.Unlock()
	if running != 0 {
		t.Fatalf("settled frame has %d running owners", running)
	}
	return f.state()
}

// faultConn wraps a real connection, counts the frame's calls and injects
// synthetic failures.
type faultConn struct {
	httpstream.Connection

	closeErr   error         // returned after the real Close
	closeGate  chan struct{} // if set, Close waits for it first
	failCreate int           // if positive, that CreateStream call fails without a real call
	createGate chan struct{} // if set, CreateStream waits for it first
	stream     func(httpstream.Stream) httpstream.Stream

	mu      sync.Mutex
	closes  int
	creates int
}

func (c *faultConn) Close() error {
	c.mu.Lock()
	c.closes++
	c.mu.Unlock()
	if c.closeGate != nil {
		<-c.closeGate
	}
	err := c.Connection.Close()
	if c.closeErr != nil {
		return c.closeErr
	}
	return err
}

func (c *faultConn) CreateStream(h http.Header) (httpstream.Stream, error) {
	c.mu.Lock()
	c.creates++
	n := c.creates
	c.mu.Unlock()
	if c.createGate != nil {
		<-c.createGate
	}
	if n == c.failCreate {
		return nil, errSynthetic
	}
	s, err := c.Connection.CreateStream(h)
	if err == nil && c.stream != nil {
		s = c.stream(s)
	}
	return s, err
}

func (c *faultConn) counts() (closes, creates int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closes, c.creates
}

// faultStream wraps a real stream, counts FIN and Reset calls and can fail
// writes.
type faultStream struct {
	httpstream.Stream
	writeErr error

	mu     sync.Mutex
	fins   int
	resets int
}

func (s *faultStream) Write(p []byte) (int, error) {
	if s.writeErr != nil {
		return 0, s.writeErr
	}
	return s.Stream.Write(p)
}

func (s *faultStream) Close() error {
	s.mu.Lock()
	s.fins++
	s.mu.Unlock()
	return s.Stream.Close()
}

func (s *faultStream) Reset() error {
	s.mu.Lock()
	s.resets++
	s.mu.Unlock()
	return s.Stream.Reset()
}

func (s *faultStream) counts() (fins, resets int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fins, s.resets
}

// wrapWith installs a hook that wraps each connection in c.
func wrapWith(c *faultConn) func(*envFrame, httpstream.Connection) httpstream.Connection {
	return func(_ *envFrame, conn httpstream.Connection) httpstream.Connection {
		c.Connection = conn
		return c
	}
}

func TestNewEnvTransportRefusesIgnoredTransport(t *testing.T) {
	base := &rest.Config{Host: "https://127.0.0.1:6443"}
	tests := []struct {
		name string
		edit func(*rest.Config)
		ns   string
	}{
		{"transport", func(c *rest.Config) { c.Transport = http.DefaultTransport }, "workloads"},
		{"dial", func(c *rest.Config) { c.Dial = (&noDial{}).dial }, "workloads"},
		{"namespace", func(*rest.Config) {}, "Bad_NS"},
		{"server", func(c *rest.Config) { c.Host = "ftp://127.0.0.1" }, "workloads"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := rest.CopyConfig(base)
			tt.edit(c)
			if _, err := newEnvTransport(c, tt.ns); err == nil {
				t.Fatal("newEnvTransport succeeded")
			}
		})
	}
	if _, err := newEnvTransport(base, "workloads"); err != nil {
		t.Fatal(err)
	}
}

type noDial struct{}

func (noDial) dial(context.Context, string, string) (net.Conn, error) { return nil, errSynthetic }

// eventually waits until cond holds.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(testWait)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestEnvPodURL(t *testing.T) {
	tr, err := newEnvTransport(&rest.Config{Host: "https://cluster.example/prefix/"}, "workloads")
	if err != nil {
		t.Fatal(err)
	}
	u, err := tr.podURL("env-0", "portforward", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := u.String(), "https://cluster.example/prefix/api/v1/namespaces/workloads/pods/env-0/portforward"; got != want {
		t.Fatalf("url = %s, want %s", got, want)
	}
	for _, pod := range []string{"", "../other", "a/b", "UPPER", strings.Repeat("a", 254)} {
		if _, err := tr.podURL(pod, "exec", nil); err == nil {
			t.Errorf("podURL(%q) succeeded", pod)
		}
	}
}

// openFrame dials a frame for protocol and returns it after the dial
// returned, with the dial's error.
func openFrame(t *testing.T, a *fakeAPI, tr *envTransport, protocol string) (*envFrame, error) {
	t.Helper()
	f := newEnvFrame()
	a.keep(f)
	u, err := tr.podURL("env-0", "exec", nil)
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	f.spawn(func() error {
		_, err := f.dial(tr, u, protocol)
		errc <- err
		if err != nil {
			return err
		}
		if c := f.closeConn(); c != nil {
			<-c
		}
		return nil
	})
	select {
	case err := <-errc:
		return f, err
	case <-time.After(testWait):
		t.Fatal("dial did not return")
	}
	return nil, nil
}

func TestEnvDialPreservesAuthAndProtocol(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	tr := a.transport(t)
	f, err := openFrame(t, a, tr, execProtocol)
	if err != nil {
		t.Fatal(err)
	}
	if s := waitSettled(t, f); s.Err != nil {
		t.Fatalf("settled with %v", s.Err)
	}
	reqs := a.requestList()
	if len(reqs) != 1 {
		t.Fatalf("server saw %d requests, want 1", len(reqs))
	}
	r := reqs[0]
	if r.Method != http.MethodPost {
		t.Errorf("method = %s", r.Method)
	}
	if got := r.Header.Get("Authorization"); got != "Bearer control-plane-token" {
		t.Errorf("Authorization = %q", got)
	}
	if got := r.Header.Values(httpstream.HeaderProtocolVersion); len(got) != 1 || got[0] != execProtocol {
		t.Errorf("protocols = %q", got)
	}
	if r.TLS == nil {
		t.Error("upgrade did not use TLS")
	}
	if r.Header.Get("Upgrade") != "SPDY/3.1" || !strings.EqualFold(r.Header.Get("Connection"), "Upgrade") {
		t.Errorf("upgrade headers = %v", r.Header)
	}
}

func TestEnvDialRefusesUntrustedServer(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	c := a.config()
	c.CAData = nil
	tr, err := newEnvTransport(c, "workloads")
	if err != nil {
		t.Fatal(err)
	}
	f, err := openFrame(t, a, tr, execProtocol)
	if err == nil {
		t.Fatal("dial to an untrusted server succeeded")
	}
	waitSettled(t, f)
	if n := len(a.requestList()); n != 0 {
		t.Fatalf("server saw %d requests", n)
	}
}

// A connection that NewConnection returns after cancellation is closed once,
// by the frame, before anyone can use it.
func TestEnvCancelBeforePublish(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	tr := a.transport(t)
	fc := &faultConn{}
	returned := make(chan struct{})
	canceled := make(chan struct{})
	tr.wrap = func(f *envFrame, conn httpstream.Connection) httpstream.Connection {
		fc.Connection = conn
		close(returned)
		<-canceled
		return fc
	}
	f := newEnvFrame()
	a.keep(f)
	u, _ := tr.podURL("env-0", "exec", nil)
	errc := make(chan error, 1)
	f.spawn(func() error {
		_, err := f.dial(tr, u, execProtocol)
		errc <- err
		return err
	})
	select {
	case <-returned:
	case <-time.After(testWait):
		t.Fatal("upgrade did not return")
	}
	f.cancel()
	close(canceled)
	if err := <-errc; !errors.Is(err, errEnvCanceled) {
		t.Fatalf("dial error = %v", err)
	}
	s := waitSettled(t, f)
	if !errors.Is(s.Err, errEnvCanceled) {
		t.Fatalf("state = %v", s.Err)
	}
	if closes, _ := fc.counts(); closes != 1 {
		t.Fatalf("late connection closed %d times, want 1", closes)
	}
}

func TestEnvFirstCloseErrorSticky(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	tr := a.transport(t)
	fc := &faultConn{closeErr: errSynthetic}
	tr.wrap = wrapWith(fc)
	f, err := openFrame(t, a, tr, execProtocol)
	if err != nil {
		t.Fatal(err)
	}
	s := waitSettled(t, f)
	if !errors.Is(s.Err, errSynthetic) {
		t.Fatalf("state = %v, want the synthetic Close error", s.Err)
	}
	// A later request neither calls Close again nor clears the error.
	if c := f.closeConn(); c == nil {
		t.Fatal("closeConn returned no channel")
	} else {
		<-c
	}
	f.cancel()
	if closes, _ := fc.counts(); closes != 1 {
		t.Fatalf("Close called %d times, want 1", closes)
	}
	if !errors.Is(f.state().Err, errSynthetic) {
		t.Fatal("Close error lost")
	}
}

func TestEnvBlockedClose(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	tr := a.transport(t)
	gate := make(chan struct{})
	fc := &faultConn{closeGate: gate}
	tr.wrap = wrapWith(fc)
	f, err := openFrame(t, a, tr, execProtocol)
	if err != nil {
		t.Fatal(err)
	}
	// The fixture keeps the frame if a check below fails with Close blocked.
	eventually(t, "the blocked Close", func() bool { c, _ := fc.counts(); return c == 1 })
	if s := f.state(); s.Settled {
		t.Fatal("frame settled while Close is blocked")
	}
	f.cancel()
	f.closeConn()
	select {
	case <-f.done:
		t.Fatal("frame settled while Close is blocked")
	case <-time.After(20 * time.Millisecond):
	}
	if closes, _ := fc.counts(); closes != 1 {
		t.Fatalf("Close called %d times while blocked, want 1", closes)
	}
	close(gate)
	if s := waitSettled(t, f); !errors.Is(s.Err, errEnvCanceled) {
		t.Fatalf("state = %v", s.Err)
	}
	if closes, _ := fc.counts(); closes != 1 {
		t.Fatalf("Close called %d times, want 1", closes)
	}
}

// Canceling a settled frame changes nothing.
func TestEnvCancelAfterSettle(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	tr := a.transport(t)
	f, err := openFrame(t, a, tr, execProtocol)
	if err != nil {
		t.Fatal(err)
	}
	if s := waitSettled(t, f); s.Err != nil {
		t.Fatal(s.Err)
	}
	f.cancel()
	if s := f.state(); s.Err != nil || !s.Settled {
		t.Fatalf("after cancel: %+v", s)
	}
}

// countConn wraps a frame's original TCP connection, counting its Close
// calls and the bytes read and written through it.
type countConn struct {
	net.Conn
	mu     sync.Mutex
	closes int
	read   int
	wrote  int
}

func (c *countConn) wrap(conn net.Conn) net.Conn {
	c.Conn = conn
	return c
}

func (c *countConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.mu.Lock()
	c.read += n
	c.mu.Unlock()
	return n, err
}

func (c *countConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.mu.Lock()
	c.wrote += n
	c.mu.Unlock()
	return n, err
}

func (c *countConn) written() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wrote
}

func (c *countConn) Close() error {
	c.mu.Lock()
	c.closes++
	c.mu.Unlock()
	return c.Conn.Close()
}

func (c *countConn) counts() (closes, read int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closes, c.read
}

// hijackPeer makes a answer every request on the hijacked connection itself:
// it writes response, then fill bytes from another goroutine, and reads until
// the connection ends. The returned channel receives that read's error, the
// peer's observation that the client ended the connection.
func (a *fakeAPI) hijackPeer(response string, fill int) <-chan error {
	ended := make(chan error, 1)
	a.override = func(w http.ResponseWriter, r *http.Request) bool {
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			ended <- err
			return true
		}
		a.mu.Lock()
		a.hijacked = append(a.hijacked, conn)
		a.mu.Unlock()
		io.WriteString(conn, response)
		if fill > 0 {
			go conn.Write(make([]byte, fill))
		}
		buf := make([]byte, 4096)
		for {
			if _, err := conn.Read(buf); err != nil {
				ended <- err
				return true
			}
		}
	}
	return ended
}

// waitPeer waits for the peer to observe the client end the connection. The
// test calls it before returning, so before any fixture cleanup.
func waitPeer(t *testing.T, ended <-chan error) {
	t.Helper()
	select {
	case err := <-ended:
		t.Logf("peer observed client end: %v", err)
	case <-time.After(testWait):
		t.Fatal("peer did not observe the client end the original connection")
	}
}

// startFrame dials a frame for protocol in an owned goroutine without waiting.
func startFrame(t *testing.T, a *fakeAPI, tr *envTransport, protocol string) (*envFrame, <-chan error) {
	t.Helper()
	f := newEnvFrame()
	a.keep(f)
	u, err := tr.podURL("env-0", "exec", nil)
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	f.spawn(func() error {
		_, err := f.dial(tr, u, protocol)
		errc <- err
		return err
	})
	return f, errc
}

// checkReleased checks that a settled refused frame closed its original
// connection exactly once and its body once, and that later requests reuse
// those results.
func checkReleased(t *testing.T, f *envFrame, cc *countConn, body bool) {
	t.Helper()
	f.mu.Lock()
	raw, bodies, conn := f.raw, len(f.bodies), f.conn
	bodyDone := bodies == 1 && f.bodies[0].op.done
	rawDone := raw != nil && raw.op.done
	f.mu.Unlock()
	if conn != nil {
		t.Fatal("refused upgrade published a SPDY connection")
	}
	if !rawDone {
		t.Fatal("original connection Close not recorded")
	}
	if want := map[bool]int{true: 1}[body]; bodies != want {
		t.Fatalf("frame recorded %d response bodies, want %d", bodies, want)
	}
	if body && !bodyDone {
		t.Fatal("response body not closed")
	}
	first := raw.Close()
	f.cancel()
	f.closeRaw()
	if again := raw.Close(); again != first {
		t.Fatalf("cached Close result changed: %v then %v", first, again)
	}
	if closes, _ := cc.counts(); closes != 1 {
		t.Fatalf("original connection closed %d times, want 1", closes)
	}
	if s := f.state(); s.Err != nil && strings.Contains(s.Err.Error(), "not closed") {
		t.Fatalf("state = %v", s.Err)
	}
}

// A refused upgrade ends with the original connection closed once by the
// frame, observed by the peer, and the body closed once afterwards.
func TestEnvRefusalClosesOriginal(t *testing.T) {
	tests := []struct {
		name     string
		response string
		fill     int
		want     error
	}{
		{"short403", "HTTP/1.1 403 Forbidden\r\nContent-Type: text/plain\r\nContent-Length: 9\r\n\r\nforbidden", 0, errEnvRefused},
		{"redirect", "HTTP/1.1 302 Found\r\nLocation: /elsewhere\r\nContent-Length: 0\r\n\r\n", 0, errEnvRedirect},
		{"overlimit", "HTTP/1.1 403 Forbidden\r\nContent-Length: 4194304\r\n\r\n", 16 * maxUpgradeErrorBody, errEnvRefused},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newFakeAPI(t, execProtocol)
			ended := a.hijackPeer(tt.response, tt.fill)
			tr := a.transport(t)
			cc := &countConn{}
			tr.wrapRaw = cc.wrap
			f, err := openFrame(t, a, tr, execProtocol)
			if !errors.Is(err, tt.want) || !errors.Is(err, errEnvRefused) {
				t.Fatalf("dial error = %v, want %v", err, tt.want)
			}
			waitPeer(t, ended)
			s := waitSettled(t, f)
			if !errors.Is(s.Err, errEnvRefused) {
				t.Fatalf("state = %v", s.Err)
			}
			func() {
				defer func() {
					if value := recover(); value != nil {
						t.Fatalf("replayed Close panicked: %v", value)
					}
				}()
				checkReleased(t, f, cc, true)
			}()
			// TLS records and read-ahead are counted too.
			if _, read := cc.counts(); read > 8*maxUpgradeErrorBody {
				t.Fatalf("read %d bytes of the refusal", read)
			}
			if n := len(a.requestList()); n != 1 {
				t.Fatalf("server saw %d requests, want 1", n)
			}
		})
	}
}

// A refusal whose body stalls below the limit is ended by cancellation.
func TestEnvStalledRefusalCancel(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	response := "HTTP/1.1 403 Forbidden\r\nContent-Length: 100\r\n\r\npartial"
	ended := a.hijackPeer(response, 0)
	tr := a.transport(t)
	cc := &countConn{}
	tr.wrapRaw = cc.wrap
	f, errc := startFrame(t, a, tr, execProtocol)
	eventually(t, "the partial refusal", func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.bodies) == 1
	})
	if f.state().Settled {
		t.Fatal("settled while the refusal body stalls")
	}
	f.cancel()
	waitPeer(t, ended)
	if err := <-errc; !errors.Is(err, errEnvRefused) {
		t.Fatalf("dial error = %v", err)
	}
	s := waitSettled(t, f)
	if !errors.Is(s.Err, errEnvCanceled) {
		t.Fatalf("state = %v", s.Err)
	}
	checkReleased(t, f, cc, true)
}

// An unanswered upgrade is ended by cancellation: the frame closes its
// original connection, which ends the response read.
func TestEnvUnansweredUpgradeAborted(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	ended := a.hijackPeer("", 0)
	tr := a.transport(t)
	cc := &countConn{}
	tr.wrapRaw = cc.wrap
	f, errc := startFrame(t, a, tr, execProtocol)
	eventually(t, "the request", func() bool { return len(a.requestList()) == 1 })
	f.cancel()
	waitPeer(t, ended)
	if err := <-errc; err == nil {
		t.Fatal("unanswered upgrade succeeded")
	}
	s := waitSettled(t, f)
	if !errors.Is(s.Err, errEnvCanceled) {
		t.Fatalf("state = %v", s.Err)
	}
	checkReleased(t, f, cc, false)
}

// An original connection dialed after cancellation is closed once when it
// is kept, and no response is read from it. The server is plain HTTP, so no
// TLS handshake observes the canceled context in its place.
func TestEnvCancelBeforeRawKept(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	addr, ended := tcpPeer(t, func(c net.Conn) error { return readToEnd(c) })
	tr := a.transportWith(t, func(c *rest.Config) { c.Host = "http://" + addr })
	cc := &countConn{}
	dialed := make(chan struct{})
	canceled := make(chan struct{})
	tr.wrapRaw = func(conn net.Conn) net.Conn {
		close(dialed)
		<-canceled
		return cc.wrap(conn)
	}
	f, errc := startFrame(t, a, tr, execProtocol)
	select {
	case <-dialed:
	case <-time.After(testWait):
		t.Fatal("dial did not return")
	}
	f.cancel()
	close(canceled)
	waitPeer(t, ended)
	if err := <-errc; err == nil {
		t.Fatal("canceled upgrade succeeded")
	}
	waitSettled(t, f)
	checkReleased(t, f, cc, false)
}

// A different selected protocol releases the original connection without
// making a SPDY connection.
func TestEnvDialWrongProtocol(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	ended := a.hijackPeer("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: SPDY/3.1\r\n"+
		"X-Stream-Protocol-Version: v5.channel.k8s.io\r\n\r\n", 0)
	tr := a.transport(t)
	cc := &countConn{}
	tr.wrapRaw = cc.wrap
	f, err := openFrame(t, a, tr, execProtocol)
	if !errors.Is(err, errEnvProtocol) {
		t.Fatalf("dial error = %v, want errEnvProtocol", err)
	}
	waitPeer(t, ended)
	if s := waitSettled(t, f); !errors.Is(s.Err, errEnvProtocol) {
		t.Fatalf("state = %v", s.Err)
	}
	checkReleased(t, f, cc, true)
}

// A peer that ends the connection without a response fails the upgrade, and
// the frame still closes its original connection once.
func TestEnvUpgradeEOFReleasesOriginal(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	a.override = func(w http.ResponseWriter, r *http.Request) bool {
		if conn, _, err := http.NewResponseController(w).Hijack(); err == nil {
			conn.Close()
		}
		return true
	}
	tr := a.transport(t)
	cc := &countConn{}
	tr.wrapRaw = cc.wrap
	f, err := openFrame(t, a, tr, execProtocol)
	if err == nil {
		t.Fatal("upgrade without a response succeeded")
	}
	waitSettled(t, f)
	checkReleased(t, f, cc, false)
}

// The request is sent at most once per frame.
func TestEnvUpgradeNoReplay(t *testing.T) {
	f := newEnvFrame()
	proxyCalls := 0
	d := &envDialer{
		frame: f,
		proxy: func(*http.Request) (*url.URL, error) {
			proxyCalls++
			return nil, errSynthetic
		},
	}
	u := &envUpgrade{frame: f, dialer: d}
	f.mu.Lock()
	f.dialed = true
	f.mu.Unlock()
	req, _ := http.NewRequest(http.MethodPost, "https://127.0.0.1/", nil)
	if _, err := u.RoundTrip(req); !errors.Is(err, errEnvReplay) {
		t.Errorf("second RoundTrip = %v", err)
	}
	if proxyCalls != 0 {
		t.Fatalf("proxy called %d times for replay", proxyCalls)
	}
}

// Bytes the peer sent after the upgrade response, buffered while it was
// parsed, are read first from the original connection.
func TestEnvRawBuffered(t *testing.T) {
	client, peer := net.Pipe()
	defer client.Close()
	go func() {
		io.WriteString(peer, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: SPDY/3.1\r\n\r\nframe")
		peer.Close()
	}()
	wire := &envBuffered{Conn: client, br: bufio.NewReader(client)}
	resp, err := http.ReadResponse(wire.br, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !upgraded(resp) {
		t.Fatalf("response not upgraded: %v", resp.Header)
	}
	got, err := io.ReadAll(wire)
	if string(got) != "frame" {
		t.Fatalf("read %q, %v; want the buffered frame", got, err)
	}
}

// blockConn is an original connection whose Close blocks until release is
// closed and then fails.
type blockConn struct {
	net.Conn
	closing chan struct{}
	release chan struct{}
}

func (c *blockConn) Close() error {
	close(c.closing)
	<-c.release
	c.Conn.Close()
	return errSynthetic
}

// Repeated close requests start one Close of the original connection, and its
// failure is recorded once.
func TestEnvCloseRawOnce(t *testing.T) {
	client, peer := net.Pipe()
	defer peer.Close()
	f := newEnvFrame()
	f.enter()
	c := &blockConn{Conn: client, closing: make(chan struct{}), release: make(chan struct{})}
	f.keepRaw(&envRaw{Conn: c, frame: f, done: make(chan struct{})})
	first := f.closeRaw()
	<-c.closing
	if again := f.closeRaw(); again != first {
		t.Fatal("second close request returned a different result")
	}
	if s := f.state(); s.Settled || !errors.Is(s.Err, errEnvRawClosing) {
		t.Fatalf("state during a blocked Close = %+v", s)
	}
	close(c.release)
	<-first
	f.leave(nil)
	s := waitSettled(t, f)
	n := 0
	for _, err := range f.errs {
		if errors.Is(err, errSynthetic) {
			n++
		}
	}
	if n != 1 || !errors.Is(s.Err, errSynthetic) {
		t.Fatalf("recorded the Close failure %d times: %v", n, s.Err)
	}
}

// Once the SPDY connection owns the original connection, the frame never
// closes it directly. Handed is a transfer of the Close, not a receipt: a
// frame that settled without that Close returning reports it, until the
// owner's single Close is recorded.
func TestEnvCloseRawHanded(t *testing.T) {
	client, peer := net.Pipe()
	defer peer.Close()
	defer client.Close()
	f := newEnvFrame()
	f.enter()
	cc := &countConn{Conn: client}
	raw := &envRaw{Conn: cc, frame: f, done: make(chan struct{})}
	f.keepRaw(raw)
	f.mu.Lock()
	f.handed = true
	f.mu.Unlock()
	if c := f.closeRaw(); c != nil {
		<-c
		t.Fatal("frame closed a handed original connection")
	}
	f.abort(true)
	f.leave(nil)
	s := waitSettled(t, f)
	if closes, _ := cc.counts(); closes != 0 {
		t.Fatalf("handed original connection closed %d times", closes)
	}
	// No SPDY connection was published here, so nothing waited for the
	// handed Close: the settled frame must not attest it.
	if s.Err == nil || !strings.Contains(s.Err.Error(), "original connection not closed") {
		t.Fatalf("settled without the handed Close: state = %v", s.Err)
	}
	// The owner's Close is the original's single Close and is recorded.
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-raw.done:
	default:
		t.Fatal("original connection Close returned without its receipt")
	}
	if closes, _ := cc.counts(); closes != 1 {
		t.Fatalf("original connection closed %d times, want 1", closes)
	}
	if s := f.state(); s.Err == nil || strings.Contains(s.Err.Error(), "not closed") || !errors.Is(s.Err, errEnvCanceled) {
		t.Fatalf("after the handed Close: state = %v", s.Err)
	}
}

// tcpPeer accepts one connection on a loopback listener and runs serve on
// it; serve owns the connection until it returns. The returned channel
// receives serve's result: the peer's observation of how the client ended
// the connection. Tests wait on it before returning.
func tcpPeer(t *testing.T, serve func(net.Conn) error) (string, <-chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	ended := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		ln.Close()
		if err != nil {
			ended <- err
			return
		}
		defer conn.Close()
		ended <- serve(conn)
	}()
	return ln.Addr().String(), ended
}

// readToEnd reads r until a read fails and returns that failure.
func readToEnd(r io.Reader) error {
	buf := make([]byte, 32<<10)
	for {
		if _, err := r.Read(buf); err != nil {
			return err
		}
	}
}

// proxyAt returns a Proxy function selecting rawURL for every request.
func proxyAt(t *testing.T, rawURL string) func(*http.Request) (*url.URL, error) {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	return func(*http.Request) (*url.URL, error) { return u, nil }
}

// apiAddr returns the host:port of a's server.
func (a *fakeAPI) apiAddr() string {
	return strings.TrimPrefix(a.server.URL, "https://")
}

// A proxy that reads CONNECT and never answers is ended by cancellation:
// the frame closes the original connection it kept before writing CONNECT.
func TestEnvProxyConnectStalled(t *testing.T) {
	for _, scheme := range []string{"http", "https"} {
		t.Run(scheme, func(t *testing.T) {
			a := newFakeAPI(t, execProtocol)
			connects := make(chan *http.Request, 1)
			addr, ended := tcpPeer(t, func(c net.Conn) error {
				if scheme == "https" {
					tc := tls.Server(c, a.server.TLS.Clone())
					defer tc.Close()
					c = tc
				}
				br := bufio.NewReader(c)
				req, err := http.ReadRequest(br)
				if err != nil {
					return err
				}
				connects <- req
				return readToEnd(br)
			})
			tr := a.transportWith(t, func(c *rest.Config) {
				c.Proxy = proxyAt(t, scheme+"://user:secret@"+addr)
			})
			cc := &countConn{}
			tr.wrapRaw = cc.wrap
			f, errc := startFrame(t, a, tr, execProtocol)
			select {
			case req := <-connects:
				want := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:secret"))
				if req.Method != http.MethodConnect || req.Host != a.apiAddr() || req.Header.Get("Proxy-Authorization") != want {
					t.Errorf("proxy request = %s %s %v", req.Method, req.Host, req.Header)
				}
			case <-time.After(testWait):
				t.Fatal("proxy saw no CONNECT")
			}
			if f.state().Settled {
				t.Fatal("settled while CONNECT is unanswered")
			}
			f.cancel()
			waitPeer(t, ended)
			if err := <-errc; err == nil {
				t.Fatal("canceled CONNECT succeeded")
			}
			if s := waitSettled(t, f); !errors.Is(s.Err, errEnvCanceled) {
				t.Fatalf("state = %v", s.Err)
			}
			checkReleased(t, f, cc, false)
			if n := len(a.requestList()); n != 0 {
				t.Fatalf("API server saw %d requests", n)
			}
		})
	}
}

// A proxy that refuses CONNECT, or answers it with garbage, and keeps its
// connection open: the frame closes the original connection once, then the
// refusal body.
func TestEnvProxyConnectRefused(t *testing.T) {
	tests := []struct {
		name     string
		response string
		body     bool
	}{
		{"framed403", "HTTP/1.1 403 Forbidden\r\nContent-Length: 9\r\n\r\nforbidden", true},
		{"unframed403", "HTTP/1.1 403 Forbidden\r\n\r\nforbidden", true},
		{"large403", fmt.Sprintf("HTTP/1.1 403 Forbidden\r\nContent-Length: %d\r\n\r\n%s", 2*maxUpgradeErrorBody, strings.Repeat("x", 2*maxUpgradeErrorBody)), true},
		{"malformed", "NOT HTTP\r\n\r\n", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newFakeAPI(t, execProtocol)
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { ln.Close() })
			addr := ln.Addr().String()
			ended := make(chan error, 1)
			go func() {
				c, err := ln.Accept()
				ln.Close()
				if err != nil {
					ended <- err
					return
				}
				err = func() error {
					br := bufio.NewReader(c)
					if _, err := http.ReadRequest(br); err != nil {
						return err
					}
					if _, err := io.WriteString(c, tt.response); err != nil {
						return err
					}
					return readToEnd(br)
				}()
				// The peer's write and single Close have returned before
				// it reports the client's end, including an early reset.
				ended <- errors.Join(err, c.Close())
			}()
			tr := a.transportWith(t, func(c *rest.Config) { c.Proxy = proxyAt(t, "http://"+addr) })
			cc := &countConn{}
			tr.wrapRaw = cc.wrap
			f, errc := startFrame(t, a, tr, execProtocol)
			waitPeer(t, ended)
			err = <-errc
			if err == nil || tt.body != errors.Is(err, errEnvProxyRefused) {
				t.Fatalf("dial error = %v", err)
			}
			waitSettled(t, f)
			checkReleased(t, f, cc, tt.body)
			if _, read := cc.counts(); read > maxUpgradeErrorBody {
				t.Fatalf("read %d bytes of proxy refusal, want at most %d", read, maxUpgradeErrorBody)
			}
			if n := len(a.requestList()); n != 0 {
				t.Fatalf("API server saw %d requests", n)
			}
		})
	}
}

// A request write that blocks because the server stops reading is ended by
// cancellation through the original connection.
func TestEnvBlockedRequestWrite(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	handshaken := make(chan struct{})
	canceled := make(chan struct{})
	addr, ended := tcpPeer(t, func(c net.Conn) error {
		tc := tls.Server(c, a.server.TLS.Clone())
		if err := tc.Handshake(); err != nil {
			return err
		}
		close(handshaken)
		select {
		case <-canceled:
		case <-time.After(testWait):
		}
		return readToEnd(tc)
	})
	tr := a.transportWith(t, func(c *rest.Config) {
		c.Host = "https://" + addr
		// Larger than any loopback socket buffers: the write blocks.
		c.BearerToken = strings.Repeat("t", 32<<20)
	})
	cc := &countConn{}
	tr.wrapRaw = cc.wrap
	f, errc := startFrame(t, a, tr, execProtocol)
	select {
	case <-handshaken:
	case <-time.After(testWait):
		t.Fatal("no TLS handshake")
	}
	eventually(t, "a partial request write", func() bool { return cc.written() >= 64<<10 })
	select {
	case err := <-errc:
		t.Fatalf("request write returned: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	f.mu.Lock()
	written := f.wire != nil
	f.mu.Unlock()
	if written {
		t.Fatal("the request write completed; it did not block")
	}
	f.cancel()
	close(canceled)
	waitPeer(t, ended)
	if err := <-errc; err == nil {
		t.Fatal("canceled write succeeded")
	}
	if s := waitSettled(t, f); !errors.Is(s.Err, errEnvCanceled) {
		t.Fatalf("state = %v", s.Err)
	}
	checkReleased(t, f, cc, false)
}

// A CONNECT tunnel carries the upgrade with the configured TLS verification
// and authentication; an untrusted server is refused in the handshake.
func TestEnvProxyConnectTLS(t *testing.T) {
	for _, trusted := range []bool{true, false} {
		t.Run(map[bool]string{true: "trusted", false: "untrusted"}[trusted], func(t *testing.T) {
			a := newFakeAPI(t, execProtocol)
			connects := make(chan *http.Request, 1)
			addr, ended := tcpPeer(t, func(c net.Conn) error {
				br := bufio.NewReader(c)
				req, err := http.ReadRequest(br)
				if err != nil {
					return err
				}
				connects <- req
				up, err := net.Dial("tcp", req.Host)
				if err != nil {
					return err
				}
				defer up.Close()
				if _, err := io.WriteString(c, "HTTP/1.1 200 Connection established\r\n\r\n"); err != nil {
					return err
				}
				back := make(chan struct{})
				go func() {
					io.Copy(c, up)
					close(back)
				}()
				_, err = io.Copy(up, br)
				up.Close()
				<-back
				return fmt.Errorf("client ended the tunnel: %v", err)
			})
			tr := a.transportWith(t, func(c *rest.Config) {
				c.Proxy = proxyAt(t, "http://user:secret@"+addr)
				if !trusted {
					c.TLSClientConfig.CAData = nil
				}
			})
			cc := &countConn{}
			tr.wrapRaw = cc.wrap
			f, err := openFrame(t, a, tr, execProtocol)
			select {
			case req := <-connects:
				want := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:secret"))
				if req.Host != a.apiAddr() || req.Header.Get("Proxy-Authorization") != want {
					t.Errorf("proxy request = %s %v", req.Host, req.Header)
				}
			case <-time.After(testWait):
				t.Fatal("proxy saw no CONNECT")
			}
			waitPeer(t, ended)
			s := waitSettled(t, f)
			if !trusted {
				if err == nil || !strings.Contains(err.Error(), "TLS handshake") {
					t.Fatalf("untrusted dial error = %v", err)
				}
				checkReleased(t, f, cc, false)
				if n := len(a.requestList()); n != 0 {
					t.Fatalf("API server saw %d requests", n)
				}
				return
			}
			if err != nil || s.Err != nil {
				t.Fatalf("dial = %v, state = %v", err, s.Err)
			}
			reqs := a.requestList()
			if len(reqs) != 1 || reqs[0].TLS == nil || reqs[0].Header.Get("Authorization") != "Bearer control-plane-token" {
				t.Fatalf("API requests = %d", len(reqs))
			}
			eventually(t, "the original Close", func() bool {
				f.mu.Lock()
				defer f.mu.Unlock()
				return f.raw.op.done
			})
			if closes, _ := cc.counts(); closes != 1 {
				t.Fatalf("original connection closed %d times", closes)
			}
		})
	}
}

// A SOCKS5 proxy that never answers the greeting is ended by cancellation:
// the forward dialer kept the original connection before the greeting.
func TestEnvSocks5Stalled(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	greeted := make(chan struct{})
	addr, ended := tcpPeer(t, func(c net.Conn) error {
		greeting := make([]byte, 3)
		if _, err := io.ReadFull(c, greeting); err != nil {
			return err
		}
		close(greeted)
		return readToEnd(c)
	})
	tr := a.transportWith(t, func(c *rest.Config) { c.Proxy = proxyAt(t, "socks5://"+addr) })
	cc := &countConn{}
	tr.wrapRaw = cc.wrap
	f, errc := startFrame(t, a, tr, execProtocol)
	select {
	case <-greeted:
	case <-time.After(testWait):
		t.Fatal("proxy saw no SOCKS5 greeting")
	}
	f.cancel()
	waitPeer(t, ended)
	if err := <-errc; err == nil {
		t.Fatal("canceled SOCKS5 dial succeeded")
	}
	if s := waitSettled(t, f); !errors.Is(s.Err, errEnvCanceled) {
		t.Fatalf("state = %v", s.Err)
	}
	checkReleased(t, f, cc, false)
}

// A configured proxy with an unsupported scheme is refused before any
// connection is made, never bypassed.
func TestEnvProxySchemeRefused(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	tr := a.transportWith(t, func(c *rest.Config) { c.Proxy = proxyAt(t, "ftp://127.0.0.1:1") })
	f, err := openFrame(t, a, tr, execProtocol)
	if !errors.Is(err, errEnvProxyScheme) {
		t.Fatalf("dial error = %v", err)
	}
	waitSettled(t, f)
	f.mu.Lock()
	raw := f.raw
	f.mu.Unlock()
	if raw != nil {
		t.Fatal("a connection was made")
	}
	if n := len(a.requestList()); n != 0 {
		t.Fatalf("API server saw %d requests", n)
	}
}

// testPKI is a throwaway certificate authority for TLS identity tests.
type testPKI struct {
	ca    *x509.Certificate
	key   *ecdsa.PrivateKey
	caPEM []byte
	pool  *x509.CertPool
}

func newTestPKI(t *testing.T) *testPKI {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return &testPKI{ca: ca, key: key, caPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pool: pool}
}

// issue returns a leaf certificate for cn signed by p, as a tls.Certificate
// and as PEM certificate and key.
func (p *testPKI) issue(t *testing.T, serial int64, cn string, dns []string, ips []net.IP, usage x509.ExtKeyUsage) (tls.Certificate, []byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
		DNSNames:     dns,
		IPAddresses:  ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.ca, &key.PublicKey, p.key)
	if err != nil {
		t.Fatal(err)
	}
	kder, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kder})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return cert, certPEM, keyPEM
}

// The proxy hop's TLS configuration is a clone without client identity,
// with verification on and the proxy's name; the API hop's keeps the
// transport's identity and name.
func TestEnvProxyTLSConfig(t *testing.T) {
	pool := x509.NewCertPool()
	identity := func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &tls.Certificate{}, nil }
	base := &tls.Config{
		Certificates:         []tls.Certificate{{}},
		GetClientCertificate: identity,
		InsecureSkipVerify:   true,
		ServerName:           "api.example",
		RootCAs:              pool,
	}
	d := &envDialer{tls: base}
	p := d.proxyTLS("proxy.example")
	if p == base || p.Certificates != nil || p.GetClientCertificate != nil || p.InsecureSkipVerify ||
		p.ServerName != "proxy.example" || p.RootCAs != pool {
		t.Fatalf("proxy hop config = %+v", p)
	}
	if len(base.Certificates) != 1 || base.GetClientCertificate == nil || !base.InsecureSkipVerify || base.ServerName != "api.example" {
		t.Fatal("proxy hop config changed the transport's")
	}
	if g := d.targetTLS("127.0.0.1"); g.GetClientCertificate == nil || len(g.Certificates) != 1 || g.ServerName != "api.example" {
		t.Fatalf("API hop config = %+v", g)
	}
	if g := (&envDialer{}).proxyTLS("proxy.example"); g.ServerName != "proxy.example" || g.InsecureSkipVerify {
		t.Fatalf("proxy hop config without transport TLS = %+v", g)
	}
}

// An https proxy hop presents no cluster client certificate and receives no
// cluster credential, while the API hop through it keeps the client
// certificate, its name and the bearer token. A proxy that requires a client
// certificate fails the handshake rather than receive the cluster's.
func TestEnvHTTPSProxyClusterIdentity(t *testing.T) {
	pki := newTestPKI(t)
	proxyCert, _, _ := pki.issue(t, 2, "proxy", []string{"localhost"}, []net.IP{net.IPv4(127, 0, 0, 1)}, x509.ExtKeyUsageServerAuth)
	_, clientPEM, clientKey := pki.issue(t, 3, "cluster-client", nil, nil, x509.ExtKeyUsageClientAuth)
	for _, require := range []bool{false, true} {
		t.Run(map[bool]string{false: "caonly", true: "requireclientcert"}[require], func(t *testing.T) {
			a := newFakeAPIWith(t, execProtocol, func(c *tls.Config) {
				c.ClientAuth = tls.RequireAndVerifyClientCert
				c.ClientCAs = pki.pool
			})
			type hello struct {
				serverName string
				peerCerts  int
			}
			hellos := make(chan hello, 1)
			connects := make(chan *http.Request, 1)
			addr, ended := tcpPeer(t, func(c net.Conn) error {
				config := &tls.Config{Certificates: []tls.Certificate{proxyCert}, ClientAuth: tls.RequestClientCert}
				if require {
					config.ClientAuth = tls.RequireAnyClientCert
				}
				tc := tls.Server(c, config)
				if err := tc.Handshake(); err != nil {
					return fmt.Errorf("proxy handshake: %w", err)
				}
				st := tc.ConnectionState()
				hellos <- hello{st.ServerName, len(st.PeerCertificates)}
				if len(st.PeerCertificates) != 0 {
					return fmt.Errorf("proxy was presented a client certificate %q", st.PeerCertificates[0].Subject.CommonName)
				}
				br := bufio.NewReader(tc)
				req, err := http.ReadRequest(br)
				if err != nil {
					return err
				}
				connects <- req
				up, err := net.Dial("tcp", req.Host)
				if err != nil {
					return err
				}
				defer up.Close()
				if _, err := io.WriteString(tc, "HTTP/1.1 200 Connection established\r\n\r\n"); err != nil {
					return err
				}
				back := make(chan struct{})
				go func() {
					io.Copy(tc, up)
					close(back)
				}()
				_, err = io.Copy(up, br)
				up.Close()
				<-back
				return fmt.Errorf("client ended the tunnel: %v", err)
			})
			_, port, _ := net.SplitHostPort(addr)
			tr := a.transportWith(t, func(c *rest.Config) {
				c.Proxy = proxyAt(t, "https://user:secret@localhost:"+port)
				c.TLSClientConfig.CAData = append(c.TLSClientConfig.CAData, pki.caPEM...)
				c.TLSClientConfig.CertData = clientPEM
				c.TLSClientConfig.KeyData = clientKey
				c.TLSClientConfig.ServerName = "example.com"
			})
			cc := &countConn{}
			tr.wrapRaw = cc.wrap
			f, err := openFrame(t, a, tr, execProtocol)
			if require {
				if err == nil {
					t.Fatal("dial through a proxy requiring a client certificate succeeded")
				}
				select {
				case perr := <-ended:
					if perr == nil || !strings.HasPrefix(perr.Error(), "proxy handshake:") {
						t.Fatalf("proxy ended with %v, want a failed handshake", perr)
					}
					t.Logf("proxy observed: %v", perr)
				case <-time.After(testWait):
					t.Fatal("proxy did not end")
				}
				waitSettled(t, f)
				checkReleased(t, f, cc, false)
				if n := len(a.requestList()); n != 0 {
					t.Fatalf("API server saw %d requests", n)
				}
				return
			}
			if err != nil {
				t.Fatalf("dial through CA-only https proxy: %v", err)
			}
			select {
			case h := <-hellos:
				if h.serverName != "localhost" || h.peerCerts != 0 {
					t.Errorf("proxy handshake: server name %q, %d client certificates", h.serverName, h.peerCerts)
				}
			case <-time.After(testWait):
				t.Fatal("no proxy handshake")
			}
			select {
			case req := <-connects:
				want := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:secret"))
				if req.Header.Get("Proxy-Authorization") != want || req.Header.Get("Authorization") != "" {
					t.Errorf("CONNECT headers = %v", req.Header)
				}
			case <-time.After(testWait):
				t.Fatal("proxy saw no CONNECT")
			}
			waitPeer(t, ended)
			if s := waitSettled(t, f); s.Err != nil {
				t.Fatalf("state = %v", s.Err)
			}
			reqs := a.requestList()
			if len(reqs) != 1 || reqs[0].TLS == nil || reqs[0].Header.Get("Authorization") != "Bearer control-plane-token" {
				t.Fatalf("API requests = %d", len(reqs))
			}
			if st := reqs[0].TLS; st.ServerName != "example.com" || len(st.PeerCertificates) != 1 || st.PeerCertificates[0].Subject.CommonName != "cluster-client" {
				t.Fatalf("API hop: server name %q, %d client certificates", st.ServerName, len(st.PeerCertificates))
			}
			eventually(t, "the original Close", func() bool {
				f.mu.Lock()
				defer f.mu.Unlock()
				return f.raw.op.done
			})
			if closes, _ := cc.counts(); closes != 1 {
				t.Fatalf("original connection closed %d times", closes)
			}
		})
	}
}
