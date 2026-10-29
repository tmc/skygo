package agentsession

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/integrations/ssh/internal/agentbuild"
	"github.com/tmc/skygo/integrations/ssh/internal/bootstrap"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// A modelCloser is a MODEL of the SSH agent connection. It counts its
// Close calls; Close waits for gate, if set, and returns err.
type modelCloser struct {
	err     error
	gate    chan struct{}
	entered chan struct{} // closed when the first Close starts

	mu     sync.Mutex
	closes int
}

func newModelCloser(err error) *modelCloser {
	return &modelCloser{err: err, entered: make(chan struct{})}
}

func (c *modelCloser) Close() error {
	c.mu.Lock()
	c.closes++
	if c.closes == 1 {
		close(c.entered)
	}
	c.mu.Unlock()
	if c.gate != nil {
		<-c.gate
	}
	return c.err
}

func (c *modelCloser) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closes
}

func testSigner(t *testing.T) ssh.Signer {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	s, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// failingOptions returns options whose SSH agent is the MODEL closer and
// whose source is not a skygo checkout, so Open fails after it acquired
// the SSH agent connection and before it builds or connects anything.
func failingOptions(t *testing.T, c *modelCloser, log io.Writer) Options {
	dir := t.TempDir()
	signer := testSigner(t)
	return Options{
		Name:           "dgx",
		SSHAddress:     "127.0.0.1:22",
		User:           "skygo",
		KnownHostsFile: filepath.Join(dir, "known_hosts"),
		AgentSocket:    filepath.Join(dir, "agent.sock"),
		SignerKey:      ssh.FingerprintSHA256(signer.PublicKey()),
		Source:         dir,
		CacheDir:       filepath.Join(dir, "cache"),
		IdentityDir:    filepath.Join(dir, "agents"),
		Target:         "dgx",
		Port:           DefaultPort,
		Log:            log,
		signer: func(socket, fingerprint string) (ssh.Signer, io.Closer, error) {
			return signer, c, nil
		},
	}
}

// TestOpenFailureSettled checks that a failed Open closes the SSH agent
// connection it acquired exactly once and, because that Close succeeded,
// returns the ordinary error.
func TestOpenFailureSettled(t *testing.T) {
	c := newModelCloser(nil)
	var log bytes.Buffer
	o := failingOptions(t, c, &log)
	s, err := Open(context.Background(), o)
	if s != nil || err == nil || !strings.Contains(err.Error(), "checkout") {
		t.Fatalf("Open = %v, %v; want a checkout refusal", s, err)
	}
	if errors.As(err, new(*UnsettledError)) {
		t.Fatalf("Open = %v; want an ordinary error after a clean Close", err)
	}
	if n := c.count(); n != 1 {
		t.Errorf("ssh agent connection closed %d times, want 1", n)
	}
	if log.Len() != 0 {
		t.Errorf("Open logged %q before building", log)
	}
	for _, name := range []string{o.CacheDir, o.IdentityDir} {
		if _, err := os.Lstat(name); !os.IsNotExist(err) {
			t.Errorf("%s: %v; want it absent", name, err)
		}
	}
}

// TestOpenFailureUnsettled checks that a failed Close of the SSH agent
// connection turns the failure into an *UnsettledError that keeps the
// original connection and its Close result, and is never retried.
func TestOpenFailureUnsettled(t *testing.T) {
	closeErr := errors.New("modeled close failure")
	c := newModelCloser(closeErr)
	s, err := Open(context.Background(), failingOptions(t, c, nil))
	var u *UnsettledError
	if s != nil || !errors.As(err, &u) {
		t.Fatalf("Open = %v, %v; want an *UnsettledError", s, err)
	}
	if u.SignerClose != closeErr || u.signer != io.Closer(c) || u.SSHClose != nil || u.client != nil {
		t.Errorf("UnsettledError = %+v", u)
	}
	if !errors.Is(err, closeErr) || !strings.Contains(err.Error(), "checkout") || !strings.Contains(err.Error(), "close ssh agent connection: modeled close failure") {
		t.Errorf("Open = %v", err)
	}
	if n := c.count(); n != 1 {
		t.Errorf("ssh agent connection closed %d times, want 1", n)
	}
}

// TestOpenSignerRefused checks that a refused SSH agent leaves nothing to
// settle, and that its error is reported as before.
func TestOpenSignerRefused(t *testing.T) {
	o := failingOptions(t, newModelCloser(nil), nil)
	o.signer = nil
	s, err := Open(context.Background(), o)
	if s != nil || err == nil || !strings.HasPrefix(err.Error(), "skygo-ssh: ssh agent: ") || errors.As(err, new(*UnsettledError)) {
		t.Fatalf("Open = %v, %v; want an ordinary ssh agent error", s, err)
	}
	o.AgentSocket = ""
	if _, err := Open(context.Background(), o); err == nil || err.Error() != "skygo-ssh: SSH_AUTH_SOCK is not set; start an SSH agent holding the key or pass --key-file" {
		t.Fatalf("Open without a socket = %v", err)
	}
}

// TestSessionCloseSticky checks that concurrent and repeated Closes run the
// connection's Close once, wait for it, and all return its result.
func TestSessionCloseSticky(t *testing.T) {
	closeErr := errors.New("modeled close failure")
	c := newModelCloser(closeErr)
	c.gate = make(chan struct{})
	s := &Session{signer: c}
	results := make(chan error, 4)
	go func() { results <- s.Close() }()
	<-c.entered
	for range 3 {
		go func() { results <- s.Close() }()
	}
	// No Close can return before the gate opens: a caller that did not
	// wait for the first Close would return nil, not closeErr.
	close(c.gate)
	for range 4 {
		if err := <-results; err != closeErr {
			t.Errorf("Close = %v, want the first result %v", err, closeErr)
		}
	}
	if err := s.Close(); err != closeErr {
		t.Errorf("later Close = %v", err)
	}
	if n := c.count(); n != 1 {
		t.Errorf("connection closed %d times, want 1", n)
	}
	if s.signer != io.Closer(c) {
		t.Error("session dropped its connection after a failed Close")
	}
}

func TestValidTarget(t *testing.T) {
	for _, tt := range []struct {
		s    string
		want bool
	}{
		{"dgx", true},
		{"dgx-1.lab", true},
		{"", false},
		{"DGX", false},
		{"-dgx", false},
		{"a..b", false},
		{"a;b", false},
		{strings.Repeat("a", 64), false},
	} {
		if got := ValidTarget(tt.s); got != tt.want {
			t.Errorf("ValidTarget(%q) = %v, want %v", tt.s, got, tt.want)
		}
	}
}

// Readiness requests. The transports and response bodies below are
// MODELS: no connection stands behind them, and a modeled Close failure is
// returned by a body that holds nothing. They test what WaitReady does
// with each result, not that any connection was closed.

// A modelBody is a MODEL response body. It serves data; Close counts its
// calls, closes entered on the first, waits for gate, if set, and returns
// err.
type modelBody struct {
	r       *bytes.Reader
	err     error
	gate    chan struct{}
	entered chan struct{}

	mu     sync.Mutex
	closes int
}

func newModelBody(data []byte, err error, gate chan struct{}) *modelBody {
	return &modelBody{r: bytes.NewReader(data), err: err, gate: gate, entered: make(chan struct{})}
}

func (b *modelBody) Read(p []byte) (int, error) { return b.r.Read(p) }

func (b *modelBody) Close() error {
	b.mu.Lock()
	b.closes++
	if b.closes == 1 {
		close(b.entered)
	}
	b.mu.Unlock()
	if b.gate != nil {
		<-b.gate
	}
	return b.err
}

func (b *modelBody) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closes
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// A modelTransport is a MODEL agent transport. It reads and closes each
// request's body and answers with the next of its replies.
type modelTransport struct {
	replies []roundTripFunc

	mu    sync.Mutex
	trips int
}

func (m *modelTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		io.Copy(io.Discard, req.Body)
		req.Body.Close()
	}
	m.mu.Lock()
	i := m.trips
	m.trips++
	m.mu.Unlock()
	if i >= len(m.replies) {
		return nil, errors.New("modeled: no more replies")
	}
	return m.replies[i](req)
}

func (m *modelTransport) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.trips
}

var errRefused = errors.New("modeled: connection refused")

func refused(*http.Request) (*http.Response, error) { return nil, errRefused }

// infoReply answers GetServerInfo with body, which holds infoBody.
func infoReply(body io.ReadCloser) roundTripFunc {
	return reply(http.StatusOK, "application/proto", body)
}

// unauthenticatedReply answers with a Connect error, whose body holds
// errorBody. Connect discards the body's Close result after an error.
func unauthenticatedReply(body io.ReadCloser) roundTripFunc {
	return reply(http.StatusUnauthorized, "application/json", body)
}

var errorBody = []byte(`{"code":"unauthenticated","message":"modeled"}`)

func reply(code int, contentType string, body io.ReadCloser) roundTripFunc {
	return func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			Status:        fmt.Sprintf("%d %s", code, http.StatusText(code)),
			StatusCode:    code,
			Proto:         "HTTP/1.1",
			ProtoMajor:    1,
			ProtoMinor:    1,
			Header:        http.Header{"Content-Type": {contentType}},
			Body:          body,
			ContentLength: -1,
			Request:       req,
		}, nil
	}
}

// infoBody returns the body of a GetServerInfo response naming target as
// a local target, as the actual Connect handler of the MODEL agent writes
// it.
func infoBody(t *testing.T, target string) []byte {
	t.Helper()
	_, h := skyv1connect.NewControlServiceHandler(&modelAgent{target: target, token: "token"})
	req := httptest.NewRequest(http.MethodPost, skyv1connect.ControlServiceGetServerInfoProcedure, http.NoBody)
	req.Header.Set("Content-Type", "application/proto")
	req.Header.Set("Authorization", "Bearer token")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK || w.Header().Get("Content-Type") != "application/proto" {
		t.Fatalf("GetServerInfo = %d %q: %s", w.Code, w.Header().Get("Content-Type"), w.Body)
	}
	return w.Body.Bytes()
}

// A statusModel counts the status calls WaitReady makes and reports agent.
type statusModel struct {
	agent string

	mu    sync.Mutex
	calls int
}

func (s *statusModel) status(context.Context) (bootstrap.State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return bootstrap.State{Agent: s.agent}, nil
}

func (s *statusModel) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func waitReady(next http.RoundTripper, st *statusModel) error {
	return WaitReady(context.Background(), &http.Client{Transport: next}, "https://"+AgentName, "token", "dgx", false, st.status)
}

// TestWaitReadyBodyCloseHold checks that a response whose body's Close
// fails does not settle the request, whether the agent answered as itself
// or with an error, after which Connect discards the Close result:
// WaitReady returns a *ProbeError keeping the same body and its Close
// result, closes it once, and sends nothing more.
func TestWaitReadyBodyCloseHold(t *testing.T) {
	closeErr := errors.New("modeled response body close failure")
	for _, tt := range []struct {
		name  string
		data  []byte
		reply func(io.ReadCloser) roundTripFunc
	}{
		{"answer", infoBody(t, "dgx"), infoReply},
		{"connect error", errorBody, unauthenticatedReply},
	} {
		t.Run(tt.name, func(t *testing.T) {
			body := newModelBody(tt.data, closeErr, nil)
			next := &modelTransport{replies: []roundTripFunc{tt.reply(body)}}
			st := &statusModel{agent: "running"}
			err := waitReady(next, st)
			var pe *ProbeError
			if !errors.As(err, &pe) || !errors.Is(err, closeErr) || pe.Close != closeErr {
				t.Fatalf("WaitReady = %v; want a *ProbeError keeping the Close failure", err)
			}
			if n := body.count(); n != 1 {
				t.Errorf("body closed %d times, want 1", n)
			}
			if next.count() != 1 || st.count() != 0 {
				t.Errorf("requests %d, status calls %d; want 1 and 0", next.count(), st.count())
			}
			if len(pe.probe.bodies) != 1 || pe.probe.bodies[0].body != io.ReadCloser(body) {
				t.Fatal("ProbeError does not keep the original body")
			}
			// Closing through the recorder again returns the first result
			// and does not close the body again.
			if err := pe.probe.bodies[0].Close(); err != closeErr || body.count() != 1 {
				t.Errorf("second Close = %v after %d body Closes; want %v after 1", err, body.count(), closeErr)
			}
		})
	}
}

// TestWaitReadyHeldClose checks that a response body Close that does not
// return holds WaitReady: it sends no further request, calls no status and
// does not return until that same Close returns. The success twin then
// proceeds as before; the failure twin returns a *ProbeError.
func TestWaitReadyHeldClose(t *testing.T) {
	for _, closeErr := range []error{nil, errors.New("modeled response body close failure")} {
		gate := make(chan struct{})
		body := newModelBody(infoBody(t, "dgx"), closeErr, gate)
		next := &modelTransport{replies: []roundTripFunc{infoReply(body)}}
		st := &statusModel{agent: "running"}
		done := make(chan error, 1)
		go func() { done <- waitReady(next, st) }()
		<-body.entered
		select {
		case err := <-done:
			t.Fatalf("WaitReady = %v while the body's Close was held", err)
		default:
		}
		if next.count() != 1 || st.count() != 0 {
			t.Errorf("while held: requests %d, status calls %d; want 1 and 0", next.count(), st.count())
		}
		close(gate)
		err := <-done
		if closeErr == nil && err != nil {
			t.Errorf("WaitReady = %v after a clean Close", err)
		}
		if closeErr != nil && (!errors.As(err, new(*ProbeError)) || !errors.Is(err, closeErr)) {
			t.Errorf("WaitReady = %v; want a *ProbeError keeping %v", err, closeErr)
		}
		if body.count() != 1 || next.count() != 1 || st.count() != 0 {
			t.Errorf("body Closes %d, requests %d, status calls %d; want 1, 1 and 0", body.count(), next.count(), st.count())
		}
	}
}

// TestWaitReadyRetriesSettled is the healthy twin: a refused request that
// returned and holds no body is settled, so WaitReady checks the status and
// tries again, as before, until the agent answers.
func TestWaitReadyRetriesSettled(t *testing.T) {
	body := newModelBody(infoBody(t, "dgx"), nil, nil)
	next := &modelTransport{replies: []roundTripFunc{refused, infoReply(body)}}
	st := &statusModel{agent: "running"}
	if err := waitReady(next, st); err != nil {
		t.Fatalf("WaitReady = %v", err)
	}
	if next.count() != 2 || st.count() != 1 || body.count() != 1 {
		t.Errorf("requests %d, status calls %d, body Closes %d; want 2, 1 and 1", next.count(), st.count(), body.count())
	}

	stopped := &statusModel{agent: "stopped"}
	err := waitReady(&modelTransport{replies: []roundTripFunc{refused}}, stopped)
	if err == nil || !strings.Contains(err.Error(), "agent stopped before it was ready") || !errors.Is(err, errRefused) || errors.As(err, new(*ProbeError)) {
		t.Fatalf("WaitReady with a stopped agent = %v", err)
	}
}

// TestProbeUnsettled checks that a probe whose body Close or RoundTrip is
// still running elsewhere is unsettled without waiting for it, and is
// settled once that same call has returned cleanly; and that a body nobody
// closed is closed once by settle.
func TestProbeUnsettled(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "https://"+AgentName+"/", nil)
	if err != nil {
		t.Fatal(err)
	}

	gate := make(chan struct{})
	body := newModelBody(nil, nil, gate)
	p := newProbe(&http.Client{Transport: &modelTransport{replies: []roundTripFunc{infoReply(body)}}})
	resp, err := p.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- resp.Body.Close() }()
	<-body.entered
	if err := p.settle(nil); !errors.As(err, new(*ProbeError)) {
		t.Errorf("settle with a Close running = %v, want a *ProbeError", err)
	}
	close(gate)
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if err := p.settle(nil); err != nil || body.count() != 1 {
		t.Errorf("settle after the Close returned = %v after %d Closes; want nil after 1", err, body.count())
	}

	hold := make(chan struct{})
	entered := make(chan struct{})
	p = newProbe(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		close(entered)
		<-hold
		return nil, errRefused
	})})
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		p.RoundTrip(req)
	}()
	<-entered
	if err := p.settle(errRefused); !errors.As(err, new(*ProbeError)) || !errors.Is(err, errRefused) {
		t.Errorf("settle with a RoundTrip running = %v, want a *ProbeError", err)
	}
	close(hold)
	<-returned
	if err := p.settle(errRefused); err != nil {
		t.Errorf("settle after the RoundTrip returned = %v, want nil", err)
	}

	unclosed := newModelBody(nil, nil, nil)
	p = newProbe(&http.Client{Transport: &modelTransport{replies: []roundTripFunc{infoReply(unclosed)}}})
	if _, err := p.RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	if err := p.settle(nil); err != nil || unclosed.count() != 1 {
		t.Errorf("settle of an unclosed body = %v after %d Closes; want nil after 1", err, unclosed.count())
	}
}

// A secretCause is a MODEL transport failure whose text holds a secret.
type secretCause struct{ token string }

func (c *secretCause) Error() string { return "modeled transport failure: " + c.token }

// TestProbeErrorKeepsRoundTripErrors checks that a *ProbeError reaches the
// errors its request's RoundTrips returned, which Connect may replace in
// CheckReady's result, as of the settle that made it; and that its text
// holds none of its causes. The transport errors and the response body
// are MODELS; CheckReady and the Connect client that replaces an error
// matching io.EOF with io.ErrUnexpectedEOF are actual. The body is
// returned by a second RoundTrip of the same probe, since one Connect
// call does not both fail in its transport and hold a body.
func TestProbeErrorKeepsRoundTripErrors(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "https://"+AgentName+"/", nil)
	if err != nil {
		t.Fatal(err)
	}
	cause := &secretCause{"secret-token"}
	original := errors.Join(io.EOF, cause)
	eofReply := func(*http.Request) (*http.Response, error) { return nil, original }
	closeErr := errors.New("modeled response body close failure: secret-close")

	for _, unsettled := range []bool{true, false} {
		replies := []roundTripFunc{eofReply}
		body := newModelBody(nil, closeErr, nil)
		if unsettled {
			replies = append(replies, infoReply(body), refused)
		}
		hc := &http.Client{Transport: &modelTransport{replies: replies}}
		p := newProbe(hc)
		checkErr := CheckReady(context.Background(), skyv1connect.NewControlServiceClient(p.client(hc), "https://"+AgentName), "token", "dgx", false)
		if !errors.Is(checkErr, io.ErrUnexpectedEOF) || errors.Is(checkErr, io.EOF) || errors.As(checkErr, new(*secretCause)) {
			t.Fatalf("CheckReady = %v; want Connect's io.ErrUnexpectedEOF in place of the original", checkErr)
		}
		if !unsettled {
			// The healthy twin: a returned transport error and no body
			// settle, so WaitReady would check the status and retry.
			if err := p.settle(checkErr); err != nil {
				t.Errorf("settle of a returned transport error = %v, want nil", err)
			}
			continue
		}
		if _, err := p.RoundTrip(req); err != nil {
			t.Fatal(err)
		}
		perr := p.settle(checkErr)
		var pe *ProbeError
		if !errors.As(perr, &pe) || pe.Err != checkErr || pe.Close != closeErr || body.count() != 1 {
			t.Fatalf("settle = %#v after %d body Closes; want a *ProbeError keeping CheckReady's result and the Close failure after 1", perr, body.count())
		}
		var got *secretCause
		if !errors.Is(perr, io.ErrUnexpectedEOF) || !errors.Is(perr, io.EOF) || !errors.Is(perr, closeErr) || !errors.As(perr, &got) || got != cause {
			t.Errorf("settle = %v; want the normalized result, the original transport error and the Close failure", perr)
		}
		// A RoundTrip error returned after settle is not part of its result.
		if _, err := p.RoundTrip(req); err != errRefused {
			t.Fatalf("third RoundTrip = %v, want %v", err, errRefused)
		}
		if errors.Is(perr, errRefused) {
			t.Errorf("settle = %v; it reaches a RoundTrip error returned after it", perr)
		}
		if msg := perr.Error(); strings.Contains(msg, "secret") || msg != "skygo-ssh: readiness request did not settle (close response body failed)" {
			t.Errorf("Error = %q; want fixed text without its causes", msg)
		}
		if body.count() != 1 {
			t.Errorf("body closed %d times, want 1", body.count())
		}
	}
}

// Open against a host. The SSH server, its host key check and client key,
// the SSH channels, the agent's TLS certificate check and the typed
// GetServerInfo calls below are actual. The agent build, the bootstrap
// script and the agent process are MODELS: modelBuild builds nothing, and
// the host interprets the script's operands instead of running it. This
// is not a test of the agent binary, the script or a native host.

// modelBinary is the MODEL agent binary. Open hashes and transfers it;
// nothing runs it.
var modelBinary = []byte("#!/bin/sh\n# agentsession test: MODEL agent binary, never run\nexit 1\n")

const (
	modelCommit = "0123456789abcdef0123456789abcdef01234567"
	modelPID    = 4242 // reported by the MODEL script; no such process is used
)

// modelBuild is a MODEL of buildAgent: it writes modelBinary into cache.
func modelBuild(_ context.Context, _, cache string, _ bool, _ io.Writer) (agentbuild.Build, error) {
	if err := os.MkdirAll(cache, 0o700); err != nil {
		return agentbuild.Build{}, err
	}
	name := filepath.Join(cache, "skygo-ssh")
	if err := os.WriteFile(name, modelBinary, 0o600); err != nil {
		return agentbuild.Build{}, err
	}
	return agentbuild.Build{Commit: modelCommit, Digest: modelDigest(), Path: name}, nil
}

func modelDigest() string {
	sum := sha256.Sum256(modelBinary)
	return hex.EncodeToString(sum[:])
}

// A modelHost is an in-process SSH server. Its exec requests are a MODEL
// of the bootstrap script, keeping the agent directory in memory. Its
// direct-tcpip channels to the agent's address reach an actual TLS Connect
// server, the MODEL agent, which serves the certificate, key and token
// Open transferred. close joins every goroutine and connection it started.
type modelHost struct {
	t       *testing.T
	addr    string
	hostKey ssh.Signer
	port    int    // the agent's remote loopback port
	target  string // the target the MODEL agent reports
	serve   bool   // whether start serves the MODEL agent

	ln     net.Listener
	config *ssh.ServerConfig
	wg     sync.WaitGroup

	mu      sync.Mutex
	closing bool
	conns   []net.Conn
	dials   int      // SSH connections accepted
	modes   []string // script modes run
	files   map[string][]byte
	started bool
	server  *httptest.Server
	agent   *modelAgent
}

func newModelHost(t *testing.T, client ssh.PublicKey, target string, serve bool) *modelHost {
	t.Helper()
	h := &modelHost{t: t, hostKey: testSigner(t), port: DefaultPort, target: target, serve: serve, files: map[string][]byte{}}
	h.config = &ssh.ServerConfig{
		PublicKeyCallback: func(_ ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			if bytes.Equal(key.Marshal(), client.Marshal()) {
				return nil, nil
			}
			return nil, errors.New("model: unknown client key")
		},
	}
	h.config.AddHostKey(h.hostKey)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h.ln, h.addr = ln, ln.Addr().String()
	h.wg.Add(1)
	go h.accept()
	t.Cleanup(h.close)
	return h
}

// close stops accepting, closes every connection the host holds, joins
// every goroutine it started and then stops the MODEL agent.
func (h *modelHost) close() {
	h.ln.Close()
	h.mu.Lock()
	h.closing = true
	conns, server := h.conns, h.server
	h.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
	h.wg.Wait()
	if server != nil {
		server.Close()
	}
}

// track records c for close, or closes it if close has started.
func (h *modelHost) track(c net.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closing {
		c.Close()
		return
	}
	h.conns = append(h.conns, c)
}

func (h *modelHost) accept() {
	defer h.wg.Done()
	for {
		c, err := h.ln.Accept()
		if err != nil {
			return
		}
		h.track(c)
		h.mu.Lock()
		h.dials++
		h.mu.Unlock()
		h.wg.Add(1)
		go h.serveConn(c)
	}
}

func (h *modelHost) serveConn(c net.Conn) {
	defer h.wg.Done()
	defer c.Close()
	conn, chans, reqs, err := ssh.NewServerConn(c, h.config)
	if err != nil {
		return
	}
	defer conn.Close()
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		ssh.DiscardRequests(reqs)
	}()
	for nc := range chans {
		switch nc.ChannelType() {
		case "session":
			ch, requests, err := nc.Accept()
			if err != nil {
				return
			}
			h.wg.Add(1)
			go h.session(ch, requests)
		case "direct-tcpip":
			h.wg.Add(1)
			go h.forward(nc)
		default:
			nc.Reject(ssh.UnknownChannelType, "refused")
		}
	}
}

func (h *modelHost) session(ch ssh.Channel, requests <-chan *ssh.Request) {
	defer h.wg.Done()
	defer ch.Close()
	for req := range requests {
		if req.Type != "exec" {
			h.t.Errorf("unexpected session request %q", req.Type)
			req.Reply(false, nil)
			continue
		}
		var payload struct{ Command string }
		if err := ssh.Unmarshal(req.Payload, &payload); err != nil {
			req.Reply(false, nil)
			return
		}
		req.Reply(true, nil)
		stdin, _ := io.ReadAll(ch)
		stdout, stderr, code := h.script(payload.Command, stdin)
		ch.Write(stdout)
		ch.Stderr().Write(stderr)
		ch.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{code}))
		return
	}
}

// script is the MODEL of the bootstrap script. It accepts only the
// script's own command line and answers each mode in the script's output
// form.
func (h *modelHost) script(command string, stdin []byte) (stdout, stderr []byte, code uint32) {
	i := strings.LastIndex(command, " skygo-bootstrap ")
	if !strings.HasPrefix(command, "sh -c '") || i < 0 {
		return nil, []byte("model: not the bootstrap script\n"), 2
	}
	var ops []string
	for _, w := range strings.Fields(command[i+len(" skygo-bootstrap "):]) {
		ops = append(ops, strings.Trim(w, "'"))
	}
	if len(ops) < 3 || ops[1] != modelCommit || ops[2] != modelDigest() {
		return nil, []byte("model: unexpected agent directory\n"), 2
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.modes = append(h.modes, ops[0])
	switch ops[0] {
	case "status":
		var b strings.Builder
		fmt.Fprintf(&b, "arch %s\n", bootstrap.Arch)
		for _, name := range bootstrap.Files {
			sum := "missing"
			if data, ok := h.files[name]; ok {
				s := sha256.Sum256(data)
				sum = hex.EncodeToString(s[:])
			}
			fmt.Fprintf(&b, "file %s %s\n", name, sum)
		}
		switch {
		case !h.started:
			b.WriteString("agent none\nconfig none\n")
		case h.server != nil:
			fmt.Fprintf(&b, "agent running %d\nconfig legacy same\n", modelPID)
		default:
			fmt.Fprintf(&b, "agent stopped %d\nconfig legacy same\n", modelPID)
		}
		return []byte(b.String()), nil, 0
	case "put":
		sum := sha256.Sum256(stdin)
		if len(ops) != 5 || hex.EncodeToString(sum[:]) != ops[4] {
			return nil, []byte("model: put refused\n"), 1
		}
		h.files[ops[3]] = stdin
		return nil, nil, 0
	case "start":
		h.started = true
		if h.serve {
			if err := h.startAgent(); err != nil {
				return nil, []byte("model: " + err.Error() + "\n"), 1
			}
		}
		return fmt.Appendf(nil, "agent started %d\n", modelPID), nil, 0
	case "log":
		return []byte("model agent log\n"), nil, 0
	}
	return nil, []byte("model: invalid mode\n"), 2
}

// startAgent serves the MODEL agent with the transferred certificate, key
// and token. h.mu is held.
func (h *modelHost) startAgent() error {
	pair, err := tls.X509KeyPair(h.files["cert.pem"], h.files["key.pem"])
	if err != nil {
		return err
	}
	h.agent = &modelAgent{target: h.target, token: strings.TrimSuffix(string(h.files["token"]), "\n")}
	mux := http.NewServeMux()
	mux.Handle(skyv1connect.NewControlServiceHandler(h.agent))
	h.server = httptest.NewUnstartedServer(mux)
	h.server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}}
	h.server.StartTLS()
	return nil
}

// forward connects a direct-tcpip channel to the agent's address with the
// MODEL agent, and joins both copies.
func (h *modelHost) forward(nc ssh.NewChannel) {
	defer h.wg.Done()
	var to struct {
		Host     string
		Port     uint32
		OrigHost string
		OrigPort uint32
	}
	if err := ssh.Unmarshal(nc.ExtraData(), &to); err != nil || to.Host != "127.0.0.1" || int(to.Port) != h.port {
		nc.Reject(ssh.Prohibited, "model: not the agent's address")
		return
	}
	h.mu.Lock()
	server := h.server
	h.mu.Unlock()
	if server == nil {
		nc.Reject(ssh.ConnectionFailed, "model: agent not listening")
		return
	}
	c, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		nc.Reject(ssh.ConnectionFailed, "model: agent unreachable")
		return
	}
	h.track(c)
	ch, reqs, err := nc.Accept()
	if err != nil {
		c.Close()
		return
	}
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		ssh.DiscardRequests(reqs)
	}()
	var copies sync.WaitGroup
	copies.Add(2)
	go func() {
		defer copies.Done()
		io.Copy(c, ch)
		c.(*net.TCPConn).CloseWrite()
	}()
	go func() {
		defer copies.Done()
		io.Copy(ch, c)
		ch.CloseWrite()
	}()
	copies.Wait()
	ch.Close()
	c.Close()
}

func (h *modelHost) record() (dials int, modes []string, infos int) {
	h.mu.Lock()
	dials, modes, agent := h.dials, slices.Clone(h.modes), h.agent
	h.mu.Unlock()
	if agent != nil {
		infos = agent.count()
	}
	return dials, modes, infos
}

// options returns options for an Open of h with the client key in
// keyFile and modelBuild.
func (h *modelHost) options(t *testing.T, dir, keyFile string, log io.Writer) Options {
	t.Helper()
	known := filepath.Join(dir, "known_hosts")
	if err := os.WriteFile(known, []byte(knownhosts.Line([]string{h.addr}, h.hostKey.PublicKey())+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return Options{
		Name:           "model",
		SSHAddress:     h.addr,
		User:           "skygo",
		KnownHostsFile: known,
		KeyFile:        keyFile,
		Source:         dir,
		CacheDir:       filepath.Join(dir, "cache"),
		IdentityDir:    filepath.Join(dir, "agents"),
		Target:         "dgx",
		Port:           h.port,
		Log:            log,
		build:          modelBuild,
	}
}

// A modelAgent is the MODEL agent's control service. It answers an
// authorized GetServerInfo naming target.
type modelAgent struct {
	skyv1connect.UnimplementedControlServiceHandler
	target, token string

	mu    sync.Mutex
	calls int
}

func (a *modelAgent) GetServerInfo(_ context.Context, req *connect.Request[v1.GetServerInfoRequest]) (*connect.Response[v1.GetServerInfoResponse], error) {
	a.mu.Lock()
	a.calls++
	a.mu.Unlock()
	if req.Header().Get("Authorization") != "Bearer "+a.token {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("model: wrong token"))
	}
	return connect.NewResponse(&v1.GetServerInfoResponse{Targets: []*v1.Target{{Name: a.target, Kind: "local"}}}), nil
}

func (a *modelAgent) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

// writeClientKey writes a new unencrypted client key to dir.
func writeClientKey(t *testing.T, dir string) (string, ssh.Signer) {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(dir, "id_ed25519")
	if err := os.WriteFile(name, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	return name, signer
}

var installModes = []string{"status", "put", "put", "put", "put", "start"}

// TestOpenModelHost checks a successful Open through the actual SSH
// runner, SSH and TLS transports and readiness check: it installs the
// four files, starts the MODEL agent once, checks it with the agent's own
// token over its own certificate, closes the bootstrap connection and
// returns a session that reaches the agent.
func TestOpenModelHost(t *testing.T) {
	dir := t.TempDir()
	keyFile, signer := writeClientKey(t, dir)
	h := newModelHost(t, signer.PublicKey(), "dgx", true)
	var log bytes.Buffer
	o := h.options(t, dir, keyFile, &log)
	s, err := Open(context.Background(), o)
	if err != nil {
		t.Fatalf("Open = %v\nlog:\n%s", err, log.String())
	}
	digest := modelDigest()
	want := Binding{
		Name:           "model",
		SSHAddress:     h.addr,
		User:           "skygo",
		KnownHostsFile: o.KnownHostsFile,
		KeyFile:        keyFile,
		AgentAddress:   fmt.Sprintf("127.0.0.1:%d", DefaultPort),
		Target:         "dgx",
		Commit:         modelCommit,
		Digest:         digest,
		AgentDir:       "~/skygo-test/" + modelCommit + "/" + digest,
		PID:            modelPID,
	}
	if b := s.Binding(); b != want {
		t.Errorf("Binding = %+v\nwant %+v", b, want)
	}
	dials, modes, infos := h.record()
	if dials != 2 || !slices.Equal(modes, installModes) || infos != 1 {
		t.Errorf("dials %d, modes %q, GetServerInfo calls %d; want 2, %q and 1", dials, modes, infos, installModes)
	}
	h.mu.Lock()
	binary, token := h.files["skygo-ssh"], string(h.files["token"])
	h.mu.Unlock()
	if !bytes.Equal(binary, modelBinary) || token != s.Bearer()+"\n" {
		t.Errorf("transferred binary %q and token %q; want the MODEL binary and the session's token", binary, token)
	}
	if s.TokenFile() != filepath.Join(o.IdentityDir, modelCommit, digest, "token") {
		t.Errorf("TokenFile = %s", s.TokenFile())
	}
	if !strings.Contains(log.String(), fmt.Sprintf("skygo-ssh: agent ready on model: target dgx, pid %d, ", modelPID)) {
		t.Errorf("log = %q", log.String())
	}

	// The session reaches the agent through its own client.
	cc := skyv1connect.NewControlServiceClient(s.HTTPClient(), s.Endpoint())
	if err := CheckReady(context.Background(), cc, s.Bearer(), "dgx", false); err != nil {
		t.Fatalf("CheckReady through the session = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if dials, _, infos := h.record(); dials != 3 || infos != 2 {
		t.Errorf("after a session request: dials %d, GetServerInfo calls %d; want 3 and 2", dials, infos)
	}
}

// TestOpenModelHostFailures checks Opens of the same host that fail before
// connecting, while installing, or because the agent is not the expected
// one or stopped. Each has settled, so its error is ordinary.
func TestOpenModelHostFailures(t *testing.T) {
	errBuild := errors.New("modeled build failure")
	tests := []struct {
		name   string
		target string // the target the MODEL agent reports
		serve  bool
		edit   func(*Options)
		err    string // in the error
		is     error
		dials  int
		modes  []string
	}{
		{
			name: "build", target: "dgx", serve: true,
			edit: func(o *Options) {
				o.build = func(context.Context, string, string, bool, io.Writer) (agentbuild.Build, error) {
					return agentbuild.Build{}, errBuild
				}
			},
			is: errBuild,
		},
		{
			name: "invalid address", target: "dgx", serve: true,
			edit: func(o *Options) { o.SSHAddress = "127.0.0.1:0" },
			err:  "ssh address",
		},
		{
			name: "wrong agent", target: "other", serve: true,
			is:    ErrWrongAgent,
			dials: 2, modes: append(slices.Clone(installModes), "log"),
		},
		{
			name: "agent stopped", target: "dgx", serve: false,
			err:   "agent stopped before it was ready",
			dials: 2, modes: append(slices.Clone(installModes), "status", "log"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			keyFile, signer := writeClientKey(t, dir)
			h := newModelHost(t, signer.PublicKey(), tt.target, tt.serve)
			o := h.options(t, dir, keyFile, nil)
			if tt.edit != nil {
				tt.edit(&o)
			}
			s, err := Open(context.Background(), o)
			if s != nil || err == nil {
				t.Fatalf("Open = %v, %v; want a failure", s, err)
			}
			if tt.is != nil && !errors.Is(err, tt.is) || tt.err != "" && !strings.Contains(err.Error(), tt.err) {
				t.Errorf("Open = %v; want %v %q", err, tt.is, tt.err)
			}
			if errors.As(err, new(*UnsettledError)) {
				t.Errorf("Open = %v; want an ordinary error", err)
			}
			if dials, modes, _ := h.record(); dials != tt.dials || !slices.Equal(modes, tt.modes) {
				t.Errorf("dials %d, modes %q; want %d and %q", dials, modes, tt.dials, tt.modes)
			}
		})
	}
}

// TestOpenModelHostUnsettled checks that a failed Open whose SSH agent
// connection's Close fails, here MODELED, returns an *UnsettledError that
// keeps the cause, closes that connection once and names it.
func TestOpenModelHostUnsettled(t *testing.T) {
	dir := t.TempDir()
	signer := testSigner(t)
	h := newModelHost(t, signer.PublicKey(), "other", true)
	closeErr := errors.New("modeled ssh agent close failure")
	c := newModelCloser(closeErr)
	o := h.options(t, dir, "", nil)
	o.AgentSocket = filepath.Join(dir, "agent.sock")
	o.SignerKey = ssh.FingerprintSHA256(signer.PublicKey())
	o.signer = func(string, string) (ssh.Signer, io.Closer, error) { return signer, c, nil }
	s, err := Open(context.Background(), o)
	var u *UnsettledError
	if s != nil || !errors.As(err, &u) {
		t.Fatalf("Open = %v, %v; want an *UnsettledError", s, err)
	}
	if !errors.Is(err, ErrWrongAgent) || u.SignerClose != closeErr || u.SSHClose != nil || u.signer != io.Closer(c) {
		t.Errorf("UnsettledError = %+v", u)
	}
	if got := u.Summary(); got != "ssh agent connection" {
		t.Errorf("Summary = %q", got)
	}
	if n := c.count(); n != 1 {
		t.Errorf("ssh agent connection closed %d times, want 1", n)
	}
	if dials, modes, _ := h.record(); dials != 2 || !slices.Equal(modes, append(slices.Clone(installModes), "log")) {
		t.Errorf("dials %d, modes %q", dials, modes)
	}
}
