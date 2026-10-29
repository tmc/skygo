package sshagent

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/integrations/ssh/internal/agentsession"
)

// The control clients, Connect and the interceptors, recorder and response
// body wrappers under test below are actual. The agent transport, its
// response bodies, the SSH agent connection, a cookie jar used to pause a
// request after the session admitted it, and the agent answering are
// MODELS: no SSH connection stands behind them, and a modeled Close
// failure or block is returned by a body that holds nothing. These tests
// show what a Session does with each result, not that any connection was
// closed, and are no evidence about a native SSH host. A modelConn stands
// in for Connect's streaming connection only where named.
//
// Every goroutine a test starts, and every gate holding one, is owned by a
// testOwner, which releases and joins them however the test ends. Being
// over models does not excuse a goroutine from that join.

const testEndpoint = "https://" + agentsession.AgentName

// newTestSession returns a session whose control client reaches next
// through the session transport, as Open's does, and whose SSH agent
// connection is signer, or none if signer is nil.
func newTestSession(signer io.Closer, next http.RoundTripper) *Session {
	s, _ := newJarSession(signer, next, nil)
	return s
}

// newJarSession is newTestSession with a client consulting jar, if not
// nil, after the session admitted a request and before its transport
// runs. It returns the client it gave the session, too.
func newJarSession(signer io.Closer, next http.RoundTripper, jar http.CookieJar) (*Session, *http.Client) {
	l := newLedger(signer)
	hc := &http.Client{Transport: agentsession.Transport{Next: next}}
	if jar != nil {
		hc.Jar = jar
	}
	return &Session{calls: l, control: newControl(l, hc, testEndpoint, "session-token")}, hc
}

// logsBidi returns a bidirectional stream client of the StreamJobLogs
// procedure wired as newControl wires the session's control client, with
// its ledger and a recorder over hc's transport. The control service has
// no bidirectional method; this is Connect's generic client, used to reach
// the CloseRequest that starts Connect's request in a goroutine.
func logsBidi(s *Session, hc *http.Client) *connect.Client[v1.StreamJobLogsRequest, v1.StreamJobLogsResponse] {
	c := *hc
	c.Transport = &recorder{l: s.calls, next: hc.Transport}
	return connect.NewClient[v1.StreamJobLogsRequest, v1.StreamJobLogsResponse](&c,
		testEndpoint+skyv1connect.ControlServiceStreamJobLogsProcedure,
		connect.WithInterceptors(s.calls, bearer("session-token")))
}

// liveCalls returns how many calls s still holds.
func liveCalls(s *Session) int {
	s.calls.mu.Lock()
	defer s.calls.mu.Unlock()
	return len(s.calls.live)
}

func infoRequest() *connect.Request[v1.GetServerInfoRequest] {
	return connect.NewRequest(&v1.GetServerInfoRequest{})
}

func logsRequest() *connect.Request[v1.StreamJobLogsRequest] {
	return connect.NewRequest(&v1.StreamJobLogsRequest{})
}

// A secretError is a MODELED error whose text holds a secret, as a remote
// message or credential might.
type secretError struct{ what, secret string }

func (e *secretError) Error() string { return e.what + ": remote said " + e.secret }

const secret = "s3cret-0f9e"

// A modelSigner is a MODEL SSH agent connection. Close counts its calls
// and returns err.
type modelSigner struct {
	err error

	mu     sync.Mutex
	closes int
}

func (m *modelSigner) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closes++
	return m.err
}

func (m *modelSigner) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closes
}

// A modelBody is a MODEL response body serving data. Once data is read
// it returns readErr, if set, in place of io.EOF. Its first Read closes
// readIn and waits for readGate, if set. Its first Close closes entered;
// every Close waits for gate, if set, and returns closeErr.
type modelBody struct {
	r        *bytes.Reader
	readErr  error
	readGate chan struct{}
	readIn   chan struct{}
	closeErr error
	gate     chan struct{}
	entered  chan struct{}

	mu            sync.Mutex
	reads, closes int
}

func newModelBody(data []byte) *modelBody {
	return &modelBody{r: bytes.NewReader(data), readIn: make(chan struct{}), entered: make(chan struct{})}
}

func (b *modelBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	b.reads++
	first := b.reads == 1
	b.mu.Unlock()
	if first {
		close(b.readIn)
		if b.readGate != nil {
			<-b.readGate
		}
	}
	n, err := b.r.Read(p)
	if err == io.EOF && b.readErr != nil {
		err = b.readErr
	}
	return n, err
}

func (b *modelBody) Close() error {
	b.mu.Lock()
	b.closes++
	first := b.closes == 1
	b.mu.Unlock()
	if first {
		close(b.entered)
	}
	if b.gate != nil {
		<-b.gate
	}
	return b.closeErr
}

func (b *modelBody) readCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.reads
}

func (b *modelBody) closeCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closes
}

type replyFunc func(*http.Request) (*http.Response, error)

// A modelTransport is a MODEL agent transport. It reads and closes each
// request's body, counts the request as sent to the agent, and answers
// with reply.
type modelTransport struct {
	reply func(i int, req *http.Request) (*http.Response, error)

	mu  sync.Mutex
	got []*http.Request
}

var errNoReply = errors.New("modeled: no more replies")

// sequence returns a transport answering its i'th request with replies[i].
func sequence(replies ...replyFunc) *modelTransport {
	return &modelTransport{reply: func(i int, req *http.Request) (*http.Response, error) {
		if i >= len(replies) {
			return nil, errNoReply
		}
		return replies[i](req)
	}}
}

func (m *modelTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		io.Copy(io.Discard, req.Body)
		req.Body.Close()
	}
	m.mu.Lock()
	i := len(m.got)
	m.got = append(m.got, req)
	m.mu.Unlock()
	if m.reply == nil {
		return nil, errNoReply
	}
	return m.reply(i, req)
}

func (m *modelTransport) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.got)
}

// A modelAgent is a MODEL agent served by the actual generated handler.
type modelAgent struct {
	skyv1connect.UnimplementedControlServiceHandler
}

func (modelAgent) GetServerInfo(context.Context, *connect.Request[v1.GetServerInfoRequest]) (*connect.Response[v1.GetServerInfoResponse], error) {
	return connect.NewResponse(&v1.GetServerInfoResponse{Targets: []*v1.Target{{Name: "dgx", Kind: "local"}}}), nil
}

func (modelAgent) StreamJobLogs(_ context.Context, _ *connect.Request[v1.StreamJobLogsRequest], st *connect.ServerStream[v1.StreamJobLogsResponse]) error {
	return st.Send(&v1.StreamJobLogsResponse{Data: []byte("log line\n")})
}

// A served response is what the actual generated handler of the MODEL
// agent wrote for one request.
type served struct {
	code   int
	header http.Header
	body   []byte
}

func serve(t *testing.T, procedure, contentType string, body []byte) served {
	t.Helper()
	_, h := skyv1connect.NewControlServiceHandler(modelAgent{})
	req := httptest.NewRequest(http.MethodPost, procedure, bytes.NewReader(body))
	req.Header.Set("Content-Type", contentType)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("%s = %d: %s", procedure, w.Code, w.Body)
	}
	return served{w.Code, w.Header().Clone(), w.Body.Bytes()}
}

// infoServed is a GetServerInfo response naming target dgx.
func infoServed(t *testing.T) served {
	return serve(t, skyv1connect.ControlServiceGetServerInfoProcedure, "application/proto", nil)
}

// logsServed is a StreamJobLogs response with one message and its end.
func logsServed(t *testing.T) served {
	// An empty enveloped request message.
	return serve(t, skyv1connect.ControlServiceStreamJobLogsProcedure, "application/connect+proto", make([]byte, 5))
}

// firstMessage returns the first enveloped message of a streaming
// response body, without what follows it, its end of stream included.
func firstMessage(t *testing.T, body []byte) []byte {
	t.Helper()
	if len(body) < 5 {
		t.Fatalf("streaming body of %d bytes", len(body))
	}
	n := 5 + int(binary.BigEndian.Uint32(body[1:5]))
	if n > len(body) {
		t.Fatalf("envelope of %d bytes in a body of %d", n, len(body))
	}
	return body[:n]
}

// reply answers with sv's status and header and with body, over HTTP/1.1.
func (sv served) reply(body io.ReadCloser) replyFunc { return sv.replyProto(1, body) }

// replyProto answers over HTTP of the given major version; Connect's
// bidirectional streams need 2.
func (sv served) replyProto(major int, body io.ReadCloser) replyFunc {
	return func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			Status:        fmt.Sprintf("%d %s", sv.code, http.StatusText(sv.code)),
			StatusCode:    sv.code,
			Proto:         fmt.Sprintf("HTTP/%d.0", major),
			ProtoMajor:    major,
			Header:        sv.header.Clone(),
			Body:          body,
			ContentLength: -1,
			Request:       req,
		}, nil
	}
}

// A pauseJar is a MODEL cookie jar. http.Client consults it once the
// session has admitted a request and before the session's transport
// runs. The first Cookies call for path closes entered and waits for
// release, pausing that request there.
type pauseJar struct {
	path    string
	entered chan struct{}
	release *gate
	once    sync.Once
}

// newPauseJar returns a jar whose release gate o owns.
func newPauseJar(o *testOwner, procedure string) *pauseJar {
	return &pauseJar{path: procedure, entered: make(chan struct{}), release: o.gate()}
}

func (j *pauseJar) Cookies(u *url.URL) []*http.Cookie {
	if u.Path == j.path {
		j.once.Do(func() {
			close(j.entered)
			<-j.release.c
		})
	}
	return nil
}

func (j *pauseJar) SetCookies(*url.URL, []*http.Cookie) {}

// A modelConn is a MODEL streaming connection standing in for Connect's,
// for what actual Connect does not produce on demand: a CloseRequest
// error, a header read held open, and a count of each method's calls.
// ResponseHeader closes headerIn, if set, and waits for headerGate, if
// set, before returning header.
type modelConn struct {
	spec            connect.Spec
	closeRequestErr error
	header          http.Header
	headerIn        chan struct{}
	headerGate      chan struct{}

	mu    sync.Mutex
	calls map[string]int
}

func (m *modelConn) record(method string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.calls == nil {
		m.calls = make(map[string]int)
	}
	m.calls[method]++
	return m.calls[method]
}

// count returns how many times method was called.
func (m *modelConn) count(method string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[method]
}

func (m *modelConn) Spec() connect.Spec { return m.spec }

func (m *modelConn) Peer() connect.Peer { return connect.Peer{} }

func (m *modelConn) RequestHeader() http.Header { return make(http.Header) }

func (m *modelConn) Send(any) error {
	m.record("Send")
	return nil
}

func (m *modelConn) CloseRequest() error {
	m.record("CloseRequest")
	return m.closeRequestErr
}

func (m *modelConn) Receive(any) error {
	m.record("Receive")
	return io.EOF
}

func (m *modelConn) ResponseHeader() http.Header {
	if m.record("ResponseHeader") == 1 && m.headerIn != nil {
		close(m.headerIn)
	}
	if m.headerGate != nil {
		<-m.headerGate
	}
	if m.header == nil {
		return make(http.Header)
	}
	return m.header
}

func (m *modelConn) ResponseTrailer() http.Header {
	m.record("ResponseTrailer")
	return make(http.Header)
}

func (m *modelConn) CloseResponse() error {
	m.record("CloseResponse")
	return nil
}

// modelStream returns conn as the session's ledger l admits it, through
// the actual stream interceptor.
func modelStream(t *testing.T, l *ledger, conn *modelConn) *stream {
	t.Helper()
	sc := l.WrapStreamingClient(func(context.Context, connect.Spec) connect.StreamingClientConn {
		return conn
	})(context.Background(), conn.spec)
	st, ok := sc.(*stream)
	if !ok {
		t.Fatalf("WrapStreamingClient = %T, want an admitted *stream", sc)
	}
	return st
}

// A testOwner owns the gates and goroutines a test starts over its
// MODELS. newOwner registers its cleanup before the test starts anything:
// the cleanup releases every gate once and then waits, against a single
// bounded guard, until the function of every goroutine returned. One that
// did not is reported and kept in stranded, with all it references, for
// the life of the test process. Tests using an owner register no other
// cleanup, so nothing they depend on is retired before that join.
//
// A goroutine's function must not call t.FailNow; it reports with t.Error
// and leaves results in variables the test reads after joining it.
type testOwner struct {
	mu    sync.Mutex
	gates []*gate
	runs  []*run
}

// A gate holds what waits on c until released, once.
type gate struct {
	c    chan struct{}
	once sync.Once
}

func (g *gate) release() { g.once.Do(func() { close(g.c) }) }

// A run is a goroutine an owner started. done is closed when f returned.
type run struct {
	name string
	f    func()
	done chan struct{}
}

// ownerGuard bounds every wait of a test on its own goroutines and models.
const ownerGuard = 10 * time.Second

// stranded keeps, for the life of the test process, each run that did not
// return within its owner's guard.
var stranded struct {
	mu   sync.Mutex
	runs []*run
}

func newOwner(t *testing.T) *testOwner {
	o := new(testOwner)
	t.Cleanup(func() {
		left := o.retire(ownerGuard)
		strand(left)
		for _, r := range left {
			t.Errorf("goroutine %q did not return within %v; kept for the life of the process", r.name, ownerGuard)
		}
	})
	return o
}

// strand keeps runs in stranded for the life of the test process.
func strand(runs []*run) {
	stranded.mu.Lock()
	defer stranded.mu.Unlock()
	stranded.runs = append(stranded.runs, runs...)
}

func (o *testOwner) gate() *gate {
	g := &gate{c: make(chan struct{})}
	o.mu.Lock()
	o.gates = append(o.gates, g)
	o.mu.Unlock()
	return g
}

// goRun starts f in a goroutine o owns.
func (o *testOwner) goRun(name string, f func()) *run {
	r := &run{name: name, f: f, done: make(chan struct{})}
	o.mu.Lock()
	o.runs = append(o.runs, r)
	o.mu.Unlock()
	go func() {
		defer close(r.done)
		r.f()
	}()
	return r
}

// retire releases every gate of o and returns each run of o that did not
// return within guard.
func (o *testOwner) retire(guard time.Duration) []*run {
	o.mu.Lock()
	gates, runs := slices.Clone(o.gates), slices.Clone(o.runs)
	o.mu.Unlock()
	for _, g := range gates {
		g.release()
	}
	return joinRuns(runs, guard)
}

// joinRuns waits, against one timer of guard, until each run returned,
// and returns those that did not.
func joinRuns(runs []*run, guard time.Duration) []*run {
	timer := time.NewTimer(guard)
	defer timer.Stop()
	for i, r := range runs {
		select {
		case <-r.done:
		case <-timer.C:
			var left []*run
			for _, r := range runs[i:] {
				select {
				case <-r.done:
				default:
					left = append(left, r)
				}
			}
			return left
		}
	}
	return nil
}

// await waits for c to be closed, failing t after ownerGuard.
func await(t *testing.T, c <-chan struct{}, what string) {
	t.Helper()
	timer := time.NewTimer(ownerGuard)
	defer timer.Stop()
	select {
	case <-c:
	case <-timer.C:
		t.Fatalf("waited %v for %s", ownerGuard, what)
	}
}

// join waits until r's function returned, failing t after ownerGuard.
func join(t *testing.T, r *run) {
	t.Helper()
	await(t, r.done, r.name+" to return")
}
