package kube

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/environmentcontrol"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	spdyserver "k8s.io/apimachinery/pkg/util/httpstream/spdy"
)

// These tests stop an exec at each phase of its connection: at the handoff
// of the original connection to SPDY, during stream creation, and with its
// workers running. The connection and its streams are real; orderConn only
// records the frame's calls on them, and may hold the original Close. The
// stream-creation cases hold the peer's SYN_REPLY with replyGate, so a
// stream request is known to be in flight when the frame is stopped.
//
// A peer's spdystream stops reading at GOAWAY, and apimachinery v0.32.3's
// CreateStream waits up to 30 seconds for a reply that losing the connection
// does not cut short. So the frame must never start the connection's Close
// while a CreateStream is in flight, nor request a stream after it. The
// original connection's recorded Close, which spdystream makes only once no
// stream of the connection is open, is checked as well as the frame's own
// settling: a settled frame must already have it.
//
// The HeldOriginalClose tests hold that original Close itself, with rawGate,
// before the underlying call: before the upgrade, at the handoff, during
// exec and port-forward stream setup, with exec workers running, and under
// the runtime's and the integration's join. The frame must stay unsettled,
// even after the SPDY connection's Close returned, until it is released.
// Every gate opens at cleanup before the fakeAPI cleanup, which keeps the
// fixture of any frame that did not settle. The port and integration tests
// also own their original outer call, the client exchange or Controller.Exec:
// cleanup joins it before retiring any fixture it may use, and retains them
// all if it cannot.

// releaser returns an idempotent close of c, for tests and their cleanup.
func releaser(c chan struct{}) func() {
	var once sync.Once
	return func() { once.Do(func() { close(c) }) }
}

// replyGate holds the in-process server's reply to stream requests.
type replyGate struct {
	requested chan string // the stream type of each held request the server read
	open      func()      // lets every reply, held or later, go
}

// gateReplies makes a answer the upgrade with apimachinery's SPDY server, as
// ServeHTTP does, except that each SYN_REPLY waits for the gate to open. The
// gate opens at cleanup, before a's own cleanup.
func gateReplies(t *testing.T, a *fakeAPI) *replyGate {
	t.Helper()
	return gateReply(t, a, "")
}

// gateReply is gateReplies holding only the replies to requests for streams
// of type typ, or every reply if typ is empty.
func gateReply(t *testing.T, a *fakeAPI, typ string) *replyGate {
	t.Helper()
	release := make(chan struct{})
	g := &replyGate{requested: make(chan string, 16), open: releaser(release)}
	t.Cleanup(g.open)
	a.mu.Lock()
	defer a.mu.Unlock()
	a.override = func(w http.ResponseWriter, r *http.Request) bool {
		if _, err := httpstream.Handshake(r, w, []string{a.protocol}); err != nil {
			return true
		}
		conn := spdyserver.NewResponseUpgrader().UpgradeResponse(w, r, func(s httpstream.Stream, _ <-chan struct{}) error {
			if st := s.Headers().Get(corev1.StreamType); typ == "" || st == typ {
				g.requested <- st
				<-release
			}
			a.streams <- s
			return nil
		})
		if conn == nil {
			return true
		}
		a.mu.Lock()
		a.conns = append(a.conns, conn)
		a.mu.Unlock()
		<-conn.CloseChan()
		return true
	}
	return g
}

// orderConn wraps a frame's real SPDY connection and records, in order, the
// start and return of each CreateStream (by stream type) and of Close. If
// closeGate is set, Close waits for it before the real Close.
type orderConn struct {
	httpstream.Connection
	closeGate chan struct{}

	mu     sync.Mutex
	events []string
}

func (c *orderConn) wrap(_ *envFrame, conn httpstream.Connection) httpstream.Connection {
	c.Connection = conn
	return c
}

func (c *orderConn) note(e string) {
	c.mu.Lock()
	c.events = append(c.events, e)
	c.mu.Unlock()
}

func (c *orderConn) CreateStream(h http.Header) (httpstream.Stream, error) {
	typ := h.Get(corev1.StreamType)
	c.note("create " + typ)
	s, err := c.Connection.CreateStream(h)
	c.note("created " + typ)
	return s, err
}

func (c *orderConn) Close() error {
	c.note("close")
	if c.closeGate != nil {
		<-c.closeGate
	}
	err := c.Connection.Close()
	c.note("closed")
	return err
}

func (c *orderConn) log() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.events)
}

// count returns the number of events with prefix.
func (c *orderConn) count(prefix string) int {
	n := 0
	for _, e := range c.log() {
		if strings.HasPrefix(e, prefix) {
			n++
		}
	}
	return n
}

// checkOrder fails unless Close started once, with no CreateStream in
// flight, returned, and no CreateStream started after it.
func checkOrder(t *testing.T, events []string) {
	t.Helper()
	inFlight, closes := 0, 0
	for _, e := range events {
		switch {
		case strings.HasPrefix(e, "create "):
			if closes != 0 {
				t.Fatalf("stream requested after the connection Close: %q", events)
			}
			inFlight++
		case strings.HasPrefix(e, "created "):
			inFlight--
		case e == "close":
			if inFlight != 0 {
				t.Fatalf("connection Close started with %d stream requests in flight: %q", inFlight, events)
			}
			closes++
		}
	}
	if closes != 1 || events[len(events)-1] != "closed" {
		t.Fatalf("connection Close not made once and returned: %q", events)
	}
}

// closeStarted reports whether f started its connection's Close.
func closeStarted(f *envFrame) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connErr.started
}

// originalRaw returns f's original network connection.
func originalRaw(t *testing.T, f *envFrame) *envRaw {
	t.Helper()
	f.mu.Lock()
	raw, handed := f.raw, f.handed
	f.mu.Unlock()
	if raw == nil || !handed {
		t.Fatalf("original connection kept %v, handed %v", raw != nil, handed)
	}
	return raw
}

// TestExecStopDuringStreamSetup stops an exec whose error stream request is
// in flight. Its Close waits for that request, no other stream is requested,
// and the frame and its original connection close once the peer replies; a
// held Close holds the frame until it is released.
func TestExecStopDuringStreamSetup(t *testing.T) {
	for _, held := range []bool{false, true} {
		t.Run(map[bool]string{false: "close", true: "heldclose"}[held], func(t *testing.T) {
			a := newFakeAPI(t, execProtocol)
			g := gateReplies(t, a)
			tr := a.transport(t)
			oc := &orderConn{}
			release := func() {}
			if held {
				oc.closeGate = make(chan struct{})
				release = releaser(oc.closeGate)
				t.Cleanup(release)
			}
			tr.wrap = oc.wrap
			x := startTestExec(t, a, tr, execRequest{Pod: "env-0", Container: "main", Argv: []string{"sleep", "1000"}})
			if typ := receive(t, "the error stream request", g.requested); typ != corev1.StreamTypeError {
				t.Fatalf("first stream request %q", typ)
			}

			x.cancel()
			// abort starts any Close it makes before it returns.
			if closeStarted(x.envFrame) {
				t.Fatal("connection Close started while a stream request is in flight")
			}
			if x.result().Settled {
				t.Fatal("settled while a stream request is in flight")
			}
			raw := originalRaw(t, x.envFrame)

			g.open()
			if held {
				eventually(t, "the held Close", func() bool { return oc.count("close") == 1 })
				if x.result().Settled {
					t.Fatal("settled while the connection Close is held")
				}
				select {
				case <-raw.done:
					t.Fatal("original connection closed before the connection Close ran")
				default:
				}
				release()
			}
			r := waitExec(t, x)
			if r.Reported || !errors.Is(r.Err, errEnvCanceled) {
				t.Fatalf("result = %+v", r)
			}
			events := oc.log()
			checkOrder(t, events)
			if n := oc.count("create "); n != 1 {
				t.Fatalf("%d stream requests, want the 1 in flight: %q", n, events)
			}
			checkRawJoined(t, x.envFrame, raw, nil)
			if n := len(a.requestList()); n != 1 {
				t.Fatalf("server saw %d exec requests", n)
			}
		})
	}
}

// TestExecStopRunning stops an exec with all streams created and its workers
// waiting on a held command. Close resets every stream, and the original
// connection closes; a held Close holds the frame until it is released.
func TestExecStopRunning(t *testing.T) {
	for _, held := range []bool{false, true} {
		t.Run(map[bool]string{false: "close", true: "heldclose"}[held], func(t *testing.T) {
			a := newFakeAPI(t, execProtocol)
			hold := make(chan struct{})
			defer close(hold)
			a.serveExec(execScript{status: successStatus, hold: hold})
			tr := a.transport(t)
			oc := &orderConn{}
			release := func() {}
			if held {
				oc.closeGate = make(chan struct{})
				release = releaser(oc.closeGate)
				t.Cleanup(release)
			}
			tr.wrap = oc.wrap
			x := startTestExec(t, a, tr, execRequest{Pod: "env-0", Container: "main", Argv: []string{"sleep", "1000"}})
			eventually(t, "the exec streams", func() bool { return oc.count("created ") == 3 })
			raw := originalRaw(t, x.envFrame)

			x.cancel()
			if held {
				eventually(t, "the held Close", func() bool { return oc.count("close") == 1 })
				if x.result().Settled {
					t.Fatal("settled while the connection Close is held")
				}
				select {
				case <-raw.done:
					t.Fatal("original connection closed before the connection Close ran")
				default:
				}
				release()
			}
			r := waitExec(t, x)
			if r.Reported || !errors.Is(r.Err, errEnvCanceled) {
				t.Fatalf("result = %+v", r)
			}
			checkOrder(t, oc.log())
			if n := oc.count("create "); n != 3 {
				t.Fatalf("%d stream requests, want 3", n)
			}
			checkRawJoined(t, x.envFrame, raw, nil)
		})
	}
}

// TestExecStopAtHandoff stops an exec after the SPDY connection took the
// original connection and before the frame published it. The frame closes
// that connection once when it publishes it, requests no stream, and the
// original connection closes.
func TestExecStopAtHandoff(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	tr := a.transport(t)
	oc := &orderConn{}
	made, canceled := make(chan struct{}), make(chan struct{})
	release := releaser(canceled)
	t.Cleanup(release)
	tr.wrap = func(f *envFrame, conn httpstream.Connection) httpstream.Connection {
		close(made)
		<-canceled
		return oc.wrap(f, conn)
	}
	x := startTestExec(t, a, tr, execRequest{Pod: "env-0", Container: "main", Argv: []string{"true"}})
	receive(t, "the SPDY connection", made)
	raw := originalRaw(t, x.envFrame)
	x.mu.Lock()
	published := x.conn != nil
	x.mu.Unlock()
	if published {
		t.Fatal("connection published before the wrap returned")
	}
	x.cancel()
	release()
	r := waitExec(t, x)
	if r.Reported || !errors.Is(r.Err, errEnvCanceled) {
		t.Fatalf("result = %+v", r)
	}
	if events := oc.log(); !slices.Equal(events, []string{"close", "closed"}) {
		t.Fatalf("connection calls %q, want only its Close", events)
	}
	checkRawJoined(t, x.envFrame, raw, nil)
}

// TestEnvironmentRuntimeStopExecStreamSetup is TestEnvironmentRuntimeStopExec
// with the peer's reply to the first stream request held: join reports the
// exec frame while the request is in flight and returns nil only after the
// reply lets the frame settle. The frame's Close waits for the request, no
// other stream is requested, and the original connection closes.
func TestEnvironmentRuntimeStopExecStreamSetup(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	g := gateReplies(t, a)
	e := newEnvRT(t, a.config())
	oc := &orderConn{}
	e.rt.port.wrap = oc.wrap
	e.created()
	e.run("node-1")
	x, err := e.rt.Exec(e.ref, environmentcontrol.ExecRequest{Argv: []string{"/bin/sleep", "1000"}})
	if err != nil {
		t.Fatal(err)
	}
	frame := x.(*environmentExec).x.envFrame
	a.keep(frame)
	if typ := receive(t, "the error stream request", g.requested); typ != corev1.StreamTypeError {
		t.Fatalf("first stream request %q", typ)
	}

	if err := shortJoin(e.rt); err == nil || !strings.Contains(err.Error(), "1 kubernetes environment frames") {
		t.Fatalf("join with a stream request in flight = %v", err)
	}
	if closeStarted(frame) {
		t.Fatal("connection Close started while a stream request is in flight")
	}
	if _, err := e.rt.Exec(e.ref, environmentcontrol.ExecRequest{Argv: []string{"/bin/true"}}); !errors.Is(err, errEnvironmentStopped) {
		t.Fatalf("Exec after stop = %v", err)
	}
	raw := originalRaw(t, frame)

	g.open()
	longJoin(t, e.rt)
	select {
	case <-x.Done():
	default:
		t.Fatal("join returned before the exec frame settled")
	}
	if res := x.Result(); !res.Settled || res.Exited || !errors.Is(res.Err, errEnvCanceled) {
		t.Fatalf("stopped exec = %+v", res)
	}
	events := oc.log()
	checkOrder(t, events)
	if n := oc.count("create "); n != 1 {
		t.Fatalf("%d stream requests, want the 1 in flight: %q", n, events)
	}
	checkRawJoined(t, frame, raw, nil)
	if len(a.requestList()) != 1 {
		t.Fatal("an exec after stop reached the server")
	}
	if len(e.calls("update", "pods")) != 0 || len(e.calls("delete", "pods")) != 0 {
		t.Fatal("stop wrote to the Pod")
	}
	e.borrowedIntact()
}

// rawGate wraps a frame's original TCP connection. Its Close counts the call
// and then waits for gate before the real Close, so the frame's single
// recorded Close of the original is held before the underlying call.
type rawGate struct {
	net.Conn
	gate   <-chan struct{}
	called chan struct{} // closed by the first Close

	mu     sync.Mutex
	closes int
}

func newRawGate(gate <-chan struct{}) *rawGate {
	return &rawGate{gate: gate, called: make(chan struct{})}
}

// holdRaw returns a rawGate and its release, which also runs at cleanup.
// Call it after newFakeAPI, so that the release runs before that fixture's
// cleanup, which requires every kept frame to settle.
func holdRaw(t *testing.T) (*rawGate, func()) {
	t.Helper()
	gate := make(chan struct{})
	release := releaser(gate)
	t.Cleanup(release)
	return newRawGate(gate), release
}

func (g *rawGate) wrap(c net.Conn) net.Conn {
	g.Conn = c
	return g
}

func (g *rawGate) Close() error {
	g.mu.Lock()
	g.closes++
	if g.closes == 1 {
		close(g.called)
	}
	g.mu.Unlock()
	<-g.gate
	return g.Conn.Close()
}

func (g *rawGate) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.closes
}

// keptRaw returns f's original network connection and whether the SPDY
// connection owns it.
func keptRaw(t *testing.T, f *envFrame) (*envRaw, bool) {
	t.Helper()
	f.mu.Lock()
	raw, handed := f.raw, f.handed
	f.mu.Unlock()
	if raw == nil {
		t.Fatal("no original connection kept")
	}
	return raw, handed
}

// connClosed reports whether f's connection Close returned.
func connClosed(f *envFrame) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connErr.done
}

// checkRawHeld fails unless f is held by the original connection's Close
// that g holds: that Close was called once and has not returned, raw.done is
// open, and f is unsettled, with an owner running and errEnvRawClosing
// reported. The short window only detects a premature settle; nothing waits
// on it to pass.
func checkRawHeld(t *testing.T, f *envFrame, raw *envRaw, g *rawGate) {
	t.Helper()
	receive(t, "the original connection Close", g.called)
	select {
	case <-f.done:
		t.Fatal("frame settled while its original connection Close is held")
	case <-raw.done:
		t.Fatal("original connection Close returned while held")
	case <-time.After(20 * time.Millisecond):
	}
	f.mu.Lock()
	running, started, done := f.running, raw.op.started, raw.op.done
	f.mu.Unlock()
	if running == 0 || !started || done {
		t.Fatalf("held frame has %d owners; original Close started %v, returned %v", running, started, done)
	}
	if s := f.state(); s.Settled || !errors.Is(s.Err, errEnvRawClosing) {
		t.Fatalf("held state = %+v", s)
	}
	if n := g.count(); n != 1 {
		t.Fatalf("original connection Close called %d times while held, want 1", n)
	}
}

// checkRawJoined waits for f to settle and fails unless the original
// connection's single Close had already returned by then, with its result
// recorded. If g is set, its Close was called exactly once.
func checkRawJoined(t *testing.T, f *envFrame, raw *envRaw, g *rawGate) {
	t.Helper()
	s := waitSettled(t, f)
	select {
	case <-raw.done:
	default:
		t.Fatal("frame settled before its original connection Close returned")
	}
	f.mu.Lock()
	started, done := raw.op.started, raw.op.done
	f.mu.Unlock()
	if !started || !done {
		t.Fatalf("original Close started %v, returned %v", started, done)
	}
	if s.Err != nil && strings.Contains(s.Err.Error(), "not closed") {
		t.Fatalf("settled state = %v", s.Err)
	}
	if g != nil {
		if n := g.count(); n != 1 {
			t.Fatalf("original connection Close called %d times, want 1", n)
		}
	}
}

// TestExecStopHeldOriginalClose stops an exec at each phase of its
// connection with the original TCP connection's Close held before the
// underlying call. Before the upgrade the frame makes that Close; after the
// handoff the SPDY connection does, once its Close returned. Either way the
// frame stays unsettled, with the original Close called once and raw.done
// open, until the Close is released; then the frame settles only with
// raw.done closed.
func TestExecStopHeldOriginalClose(t *testing.T) {
	sleep := execRequest{Pod: "env-0", Container: "main", Argv: []string{"sleep", "1000"}}

	t.Run("upgrade", func(t *testing.T) {
		a := newFakeAPI(t, execProtocol)
		ended := a.hijackPeer("", 0) // never answers the upgrade
		rg, release := holdRaw(t)
		tr := a.transport(t)
		tr.wrapRaw = rg.wrap
		x := startTestExec(t, a, tr, sleep)
		eventually(t, "the exec request", func() bool { return len(a.requestList()) == 1 })
		raw, handed := keptRaw(t, x.envFrame)
		if handed {
			t.Fatal("original connection handed before any response")
		}

		x.cancel()
		checkRawHeld(t, x.envFrame, raw, rg)
		select {
		case err := <-ended:
			t.Fatalf("peer observed the client end while its Close is held: %v", err)
		default:
		}

		release()
		r := waitExec(t, x)
		if r.Reported || !errors.Is(r.Err, errEnvCanceled) {
			t.Fatalf("result = %+v", r)
		}
		checkRawJoined(t, x.envFrame, raw, rg)
		waitPeer(t, ended)
		x.mu.Lock()
		published := x.conn != nil
		x.mu.Unlock()
		if published {
			t.Fatal("connection published after cancellation")
		}
	})

	t.Run("handoff", func(t *testing.T) {
		a := newFakeAPI(t, execProtocol)
		rg, release := holdRaw(t)
		tr := a.transport(t)
		oc := &orderConn{}
		made, canceled := make(chan struct{}), make(chan struct{})
		resume := releaser(canceled)
		t.Cleanup(resume)
		tr.wrap = func(f *envFrame, conn httpstream.Connection) httpstream.Connection {
			close(made)
			<-canceled
			return oc.wrap(f, conn)
		}
		tr.wrapRaw = rg.wrap
		x := startTestExec(t, a, tr, execRequest{Pod: "env-0", Container: "main", Argv: []string{"true"}})
		receive(t, "the SPDY connection", made)
		raw := originalRaw(t, x.envFrame)

		x.cancel()
		resume()
		eventually(t, "the connection Close to return", func() bool { return connClosed(x.envFrame) })
		checkRawHeld(t, x.envFrame, raw, rg)

		release()
		r := waitExec(t, x)
		if r.Reported || !errors.Is(r.Err, errEnvCanceled) {
			t.Fatalf("result = %+v", r)
		}
		if events := oc.log(); !slices.Equal(events, []string{"close", "closed"}) {
			t.Fatalf("connection calls %q, want only its Close", events)
		}
		checkRawJoined(t, x.envFrame, raw, rg)
	})

	t.Run("setup", func(t *testing.T) {
		a := newFakeAPI(t, execProtocol)
		g := gateReplies(t, a)
		rg, release := holdRaw(t)
		tr := a.transport(t)
		oc := &orderConn{}
		tr.wrap, tr.wrapRaw = oc.wrap, rg.wrap
		x := startTestExec(t, a, tr, sleep)
		if typ := receive(t, "the error stream request", g.requested); typ != corev1.StreamTypeError {
			t.Fatalf("first stream request %q", typ)
		}
		raw := originalRaw(t, x.envFrame)

		x.cancel()
		if closeStarted(x.envFrame) {
			t.Fatal("connection Close started while a stream request is in flight")
		}
		g.open()
		eventually(t, "the connection Close to return", func() bool { return connClosed(x.envFrame) })
		checkRawHeld(t, x.envFrame, raw, rg)

		release()
		r := waitExec(t, x)
		if r.Reported || !errors.Is(r.Err, errEnvCanceled) {
			t.Fatalf("result = %+v", r)
		}
		events := oc.log()
		checkOrder(t, events)
		if n := oc.count("create "); n != 1 {
			t.Fatalf("%d stream requests, want the 1 in flight: %q", n, events)
		}
		checkRawJoined(t, x.envFrame, raw, rg)
	})

	t.Run("running", func(t *testing.T) {
		a := newFakeAPI(t, execProtocol)
		hold := make(chan struct{})
		defer close(hold)
		a.serveExec(execScript{status: successStatus, hold: hold})
		rg, release := holdRaw(t)
		tr := a.transport(t)
		oc := &orderConn{}
		tr.wrap, tr.wrapRaw = oc.wrap, rg.wrap
		x := startTestExec(t, a, tr, sleep)
		eventually(t, "the exec streams", func() bool { return oc.count("created ") == 3 })
		raw := originalRaw(t, x.envFrame)

		x.cancel()
		eventually(t, "the connection Close to return", func() bool { return connClosed(x.envFrame) })
		checkRawHeld(t, x.envFrame, raw, rg)

		release()
		r := waitExec(t, x)
		if r.Reported || !errors.Is(r.Err, errEnvCanceled) {
			t.Fatalf("result = %+v", r)
		}
		checkOrder(t, oc.log())
		if n := oc.count("create "); n != 3 {
			t.Fatalf("%d stream requests, want 3", n)
		}
		checkRawJoined(t, x.envFrame, raw, rg)
	})
}

// retainedCalls keeps, until the test process exits, each original call
// that cleanup could not join, with every fixture it may still use. Nothing
// in it is closed, and it records no completion.
var (
	retainedCallsMu sync.Mutex
	retainedCalls   [][]any
)

func retainCall(refs ...any) {
	retainedCallsMu.Lock()
	retainedCalls = append(retainedCalls, refs)
	retainedCallsMu.Unlock()
}

// keepServer takes a's server from a, so that a's cleanup closes neither it
// nor its connections, and returns it for retention.
func keepServer(a *fakeAPI) *httptest.Server {
	srv := a.server
	a.server = nil
	return srv
}

// portCall is a test's original client exchange with a gateway; it owns the
// gateway's server. err is stored before done closes, so the test and its
// cleanup each read it after done without competing receives.
type portCall struct {
	gw     *gateway
	cancel context.CancelFunc // the client leaves
	done   chan struct{}
	err    error
}

// startPortCall serves tr's gateway for a as newGateway does, but leaves the
// server's Close to the call's cleanup, and starts one client exchange with
// it. That cleanup, registered before the exchange starts, cancels the
// exchange and runs release, which must open every gate the test holds, and
// then joins the exchange: only once it returned is the server closed and a's
// cleanup left to require the frames. An exchange that does not return
// within testWait is reported, and it, the server and a are retained,
// unclosed, for the life of the test process.
func startPortCall(t *testing.T, a *fakeAPI, tr *envTransport, release func()) *portCall {
	t.Helper()
	gw := &gateway{frames: make(chan *portFrame, 4)}
	gw.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w = deadlineWriter{w, http.NewResponseController(w), &gw.reads, &gw.failFlush}
		p := tr.servePort(w, r, portTarget{Pod: "env-0", Port: testPort})
		a.keep(p.envFrame)
		gw.frames <- p
	}))
	ctx, cancel := context.WithCancel(context.Background())
	c := &portCall{gw: gw, cancel: cancel, done: make(chan struct{})}
	t.Cleanup(func() {
		c.cancel()
		release()
		select {
		case <-c.done:
			gw.server.Close()
		case <-time.After(testWait):
			t.Errorf("client exchange did not return; gateway and fixture retained")
			retainCall(c, tr, a, keepServer(a))
		}
	})
	go func() {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, gw.server.URL+"/p", nil)
		if err == nil {
			var resp *http.Response
			if resp, err = testClient().Do(req); err == nil {
				resp.Body.Close()
			}
		}
		c.err = err
		close(c.done)
	}()
	return c
}

// TestPortStopHeldOriginalClose stops a gateway exchange, by the client
// leaving, while the reply to its error or data stream request is held, and
// with its original TCP connection's Close held. The connection's Close waits
// for the request, no stream is requested after it, and the frame stays
// unsettled after that Close returned until the original Close is released.
func TestPortStopHeldOriginalClose(t *testing.T) {
	for _, typ := range []string{corev1.StreamTypeError, corev1.StreamTypeData} {
		t.Run(typ, func(t *testing.T) {
			a := newFakeAPI(t, portProtocol)
			tr := a.transport(t)
			oc := &orderConn{}
			frames := make(chan *envFrame, 1)
			tr.wrap = func(f *envFrame, conn httpstream.Connection) httpstream.Connection {
				frames <- f
				return oc.wrap(f, conn)
			}
			gate := make(chan struct{})
			release := releaser(gate)
			rg := newRawGate(gate)
			tr.wrapRaw = rg.wrap
			g := gateReply(t, a, typ)
			// The call's cleanup opens both gates, cancels and joins the
			// client before the gateway's Close and a's cleanup.
			c := startPortCall(t, a, tr, func() { g.open(); release() })

			f := receive(t, "the port-forward connection", frames)
			a.keep(f)
			if got := receive(t, "the held stream request", g.requested); got != typ {
				t.Fatalf("held stream request %q, want %q", got, typ)
			}
			raw := originalRaw(t, f)

			// The client leaves, and the gateway aborts the frame.
			c.cancel()
			eventually(t, "the client's departure", f.isClosing)
			if closeStarted(f) {
				t.Fatal("connection Close started while a stream request is in flight")
			}
			if f.state().Settled {
				t.Fatal("settled while a stream request is in flight")
			}
			g.open()
			eventually(t, "the connection Close to return", func() bool { return connClosed(f) })
			checkRawHeld(t, f, raw, rg)

			release()
			checkRawJoined(t, f, raw, rg)
			if p := c.gw.frame(t); p.envFrame != f {
				t.Fatal("handler returned another frame")
			}
			receive(t, "the client", c.done)
			events := oc.log()
			checkOrder(t, events)
			want := map[string]int{corev1.StreamTypeError: 1, corev1.StreamTypeData: 2}[typ]
			if n := oc.count("create "); n != want {
				t.Fatalf("%d stream requests, want %d: %q", n, want, events)
			}
			if n := len(a.requestList()); n != 1 {
				t.Fatalf("server saw %d port-forward requests", n)
			}
		})
	}
}

// TestEnvironmentRuntimeStopExecHeldOriginalClose stops the runtime with an
// exec running, its command held by the server and its original TCP
// connection's Close held. join reports the exec frame even after the SPDY
// connection's Close returned, and returns nil only once the original Close
// is released and the frame settled with it.
func TestEnvironmentRuntimeStopExecHeldOriginalClose(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	rg, release := holdRaw(t)
	e := newEnvRT(t, a.config())
	oc := &orderConn{}
	e.rt.port.wrap, e.rt.port.wrapRaw = oc.wrap, rg.wrap
	e.created()
	e.run("node-1")
	hold := make(chan struct{})
	defer close(hold)
	a.serveExec(execScript{status: successStatus, hold: hold})
	x, err := e.rt.Exec(e.ref, environmentcontrol.ExecRequest{Argv: []string{"/bin/sleep", "1000"}})
	if err != nil {
		t.Fatal(err)
	}
	frame := x.(*environmentExec).x.envFrame
	a.keep(frame)
	eventually(t, "the exec streams", func() bool { return oc.count("created ") == 3 })
	raw := originalRaw(t, frame)

	if err := shortJoin(e.rt); err == nil || !strings.Contains(err.Error(), "1 kubernetes environment frames") {
		t.Fatalf("join with the exec running = %v", err)
	}
	eventually(t, "the connection Close to return", func() bool { return connClosed(frame) })
	checkRawHeld(t, frame, raw, rg)
	if err := shortJoin(e.rt); err == nil || !strings.Contains(err.Error(), "1 kubernetes environment frames") {
		t.Fatalf("join with the original Close held = %v", err)
	}
	select {
	case <-x.Done():
		t.Fatal("exec done while its original connection Close is held")
	default:
	}
	if _, err := e.rt.Exec(e.ref, environmentcontrol.ExecRequest{Argv: []string{"/bin/true"}}); !errors.Is(err, errEnvironmentStopped) {
		t.Fatalf("Exec after stop = %v", err)
	}

	release()
	longJoin(t, e.rt)
	select {
	case <-x.Done():
	default:
		t.Fatal("join returned before the exec frame settled")
	}
	if res := x.Result(); !res.Settled || res.Exited || !errors.Is(res.Err, errEnvCanceled) {
		t.Fatalf("stopped exec = %+v", res)
	}
	checkRawJoined(t, frame, raw, rg)
	checkOrder(t, oc.log())
	if n := oc.count("create "); n != 3 {
		t.Fatalf("%d stream requests, want 3", n)
	}
	if len(a.requestList()) != 1 {
		t.Fatal("an exec after stop reached the server")
	}
	if len(e.calls("update", "pods")) != 0 || len(e.calls("delete", "pods")) != 0 {
		t.Fatal("stop wrote to the Pod")
	}
	e.borrowedIntact()
}

// TestEnvironmentIntegrationCloseHeldRawExec is
// TestEnvironmentIntegrationCloseHeldExec with the harness's gate holding the
// original TCP connection's Close in place of the SPDY connection's. The SPDY
// Close returns, and still the integration's Close does not return nil until
// the original connection's recorded Close has returned.
func TestEnvironmentIntegrationCloseHeldRawExec(t *testing.T) {
	h := newCloseHarness(t, execProtocol)
	// Both edits precede the exec, the transport's only use.
	h.fc.closeGate = nil
	rg := newRawGate(h.gate)
	tr := h.in.rt.port
	frames := make(chan *envFrame, 1)
	wrap := tr.wrap
	tr.wrap = func(f *envFrame, conn httpstream.Connection) httpstream.Connection {
		frames <- f
		return wrap(f, conn)
	}
	tr.wrapRaw = rg.wrap
	h.api.serveExec(execScript{status: successStatus, hold: h.hold})
	// The original Exec call. res and err are stored before done closes, so
	// the test and the cleanup each read them after done without competing
	// receives.
	out := &struct {
		done chan struct{}
		res  environmentcontrol.ExecResult
		err  error
	}{done: make(chan struct{})}
	// Registered before the call starts, so it runs before the harness's
	// cleanups and the owner's retirement. It releases the modeled gates and
	// makes the owner's one attempt to close the integration, which returns a
	// held error unchanged and never retries it; then it joins the call. The
	// session-idle fence of the Store is not this call's return. A call that
	// does not return latches h.o.held, unless a real held error already is,
	// so the owner retires neither the Store nor the auth Engine; the call,
	// the harness and h.api, unclosed, are retained for the life of the test
	// process.
	t.Cleanup(func() {
		h.release()
		h.o.closeIntegration()
		select {
		case <-out.done:
			return
		case <-time.After(testWait):
		}
		if h.o.held == nil {
			h.o.held = errors.New("original Controller.Exec did not return")
		}
		t.Errorf("original Controller.Exec did not return; fixture retained: %v", h.o.held)
		retainCall(out, h, h.api, keepServer(h.api))
	})
	go func() {
		res, err := h.ctl.Exec(context.Background(), h.actor, h.id, environmentcontrol.ExecRequest{Argv: []string{"/bin/sleep", "1000"}})
		out.res, out.err = res, err
		close(out.done)
	}()
	f := receive(t, "the exec connection", frames)
	h.api.keep(f)
	eventually(t, "exec streams", func() bool { _, n := h.fc.counts(); return n == 3 })
	raw := originalRaw(t, f)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	err := h.in.Close(ctx)
	cancel()
	if err == nil {
		h.o.held = errors.New("integration Close = nil while the original TCP connection Close is held")
		t.Fatal(h.o.held)
	}
	eventually(t, "the SPDY connection Close to return", func() bool { return connClosed(f) })
	checkRawHeld(t, f, raw, rg)
	if n := h.in.rt.activeFrames(); n != 1 {
		t.Fatalf("%d runtime frames while held, want the original 1", n)
	}

	// closeHeld's own first Close fails while held too; it then releases
	// h.gate, the original Close, and requires a nil Close.
	h.closeHeld(t)
	checkRawJoined(t, f, raw, rg)

	receive(t, "Exec", out.done)
	if out.err != nil {
		t.Fatalf("Exec = %v; a started command is not an error", out.err)
	}
	if out.res.Exited {
		t.Fatalf("canceled exec reported exit %d", out.res.Code)
	}
	v, err := h.ctl.Get(h.actor, h.id)
	if err != nil {
		t.Fatal(err)
	}
	if v.Sessions != 0 || v.Unknown != 1 {
		t.Fatalf("sessions = %d open, %d unknown; want 0, 1", v.Sessions, v.Unknown)
	}
}

// libConn stands in for a SPDY connection whose Close returns before the
// library closes the original connection, as spdystream's does. Only Close
// is called.
type libConn struct {
	httpstream.Connection

	mu     sync.Mutex
	closes int
}

func (c *libConn) Close() error {
	c.mu.Lock()
	c.closes++
	c.mu.Unlock()
	return nil
}

// TestEnvConnCloseAwaitsOriginal checks the frame's close owner directly:
// after the connection's Close returned, the owner holds the frame until the
// original connection's single Close, which the library makes, returns.
func TestEnvConnCloseAwaitsOriginal(t *testing.T) {
	client, peer := net.Pipe()
	f := newEnvFrame()
	f.enter() // stands for the dial
	cc := &countConn{Conn: client}
	raw := &envRaw{Conn: cc, frame: f, done: make(chan struct{})}
	f.keepRaw(raw)
	f.mu.Lock()
	f.handed = true
	f.mu.Unlock()
	lc := &libConn{}
	f.publish(lc)
	c := f.closeConn()
	f.leave(nil)
	// The one owner of this test's resources, registered before any check
	// can fail. It makes the library's part, the original's Close, through
	// its once record: after the test's own Close it returns the cached
	// result and makes no second underlying Close. Then it joins the
	// original close owner and the frame, and only then closes the peer. A
	// frame that does not join keeps the peer open.
	t.Cleanup(func() {
		raw.Close()
		select {
		case <-c:
		case <-time.After(testWait):
			t.Errorf("connection Close owner did not return; peer kept: %v", f.state().Err)
			return
		}
		select {
		case <-f.done:
		case <-time.After(testWait):
			t.Errorf("frame did not settle; peer kept: %v", f.state().Err)
			return
		}
		peer.Close()
	})

	eventually(t, "the connection Close to return", func() bool { return connClosed(f) })
	select {
	case <-c:
		t.Fatal("connection Close owner returned before the original connection Close")
	case <-f.done:
		t.Fatal("frame settled before the original connection Close")
	case <-time.After(20 * time.Millisecond):
	}
	f.mu.Lock()
	running := f.running
	f.mu.Unlock()
	if running != 1 {
		t.Fatalf("%d owners, want the connection Close owner", running)
	}
	if s := f.state(); s.Settled || !errors.Is(s.Err, errEnvRawHeld) {
		t.Fatalf("held state = %+v", s)
	}
	if closes, _ := cc.counts(); closes != 0 {
		t.Fatalf("frame closed the handed original connection %d times", closes)
	}

	// The library's shutdown makes the original's single Close.
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	receive(t, "the connection Close owner", c)
	if s := waitSettled(t, f); s.Err != nil {
		t.Fatalf("settled with %v", s.Err)
	}
	lc.mu.Lock()
	lcCloses := lc.closes
	lc.mu.Unlock()
	if closes, _ := cc.counts(); closes != 1 || lcCloses != 1 {
		t.Fatalf("original closed %d times, connection %d times; want 1, 1", closes, lcCloses)
	}
}
