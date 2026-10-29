package kube

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"golang.org/x/net/http2"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

// AUTHORED_NOT_RUN. The writer tests use local servers: an HTTP server that
// answers 307 and 308, and a raw HTTP/2 peer, written with x/net/http2's
// Framer on a listener the test owns, that reads a whole request body and
// then resets the stream with REFUSED_STREAM. Both are MODELED peers, not an
// API server. Each has a control that sends the same request through the
// same target client with a rewindable body, to show the test can observe
// the replay it forbids. Every request is bounded by a test-owned context,
// and every server handler is joined before the test ends.
//
// This is the HTTP writer's own no-replay test. It is not the signed schema
// 12 predecessor writer's test and qualifies nothing about it.

func writerObject(k serviceKind) any {
	meta := metav1.ObjectMeta{Name: "skygo-svc-x", Namespace: "workloads"}
	if k == deploymentObject {
		return &appsv1.Deployment{ObjectMeta: meta}
	}
	return &corev1.Service{ObjectMeta: meta}
}

func writerPath(k serviceKind) string {
	if k == deploymentObject {
		return "/apis/apps/v1/namespaces/workloads/deployments"
	}
	return "/api/v1/namespaces/workloads/services"
}

// hits counts requests and records their bodies and credentials. ended
// receives once per handler that returned.
type hits struct {
	mu     sync.Mutex
	bodies [][]byte
	auth   []string
	ended  chan struct{}
}

func newHits() *hits { return &hits{ended: make(chan struct{}, 16)} }

func (h *hits) record(r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.bodies = append(h.bodies, body)
	h.auth = append(h.auth, r.Header.Get("Authorization"))
}

func (h *hits) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.bodies)
}

// ownServer registers the bounded join of s: its Close waits for every
// handler; one that does not return within testWait is reported and the
// server retained, unclosed.
func ownServer(t *testing.T, s *httptest.Server) {
	t.Cleanup(func() {
		closed := make(chan struct{})
		go func() { s.Close(); close(closed) }()
		select {
		case <-closed:
		case <-time.After(testWait):
			t.Errorf("server %s handlers did not return; retained", s.URL)
			retainCall(s)
		}
	})
}

func TestServiceWriterRefusesRedirect(t *testing.T) {
	for _, k := range []serviceKind{deploymentObject, serviceObjectKind} {
		for _, code := range []int{http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
			t.Run(fmt.Sprintf("%s/%d", kindName(k), code), func(t *testing.T) {
				origin, elsewhere := newHits(), newHits()
				other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer func() { elsewhere.ended <- struct{}{} }()
					elsewhere.record(r)
					w.WriteHeader(http.StatusCreated)
				}))
				ownServer(t, other)
				api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer func() { origin.ended <- struct{}{} }()
					origin.record(r)
					http.Redirect(w, r, other.URL+r.URL.Path, code)
				}))
				ownServer(t, api)
				config := &rest.Config{Host: api.URL, BearerToken: "control-plane-token"}

				writer, err := newServiceHTTP(config, "workloads")
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), testWait)
				defer cancel()
				a := writer.create(ctx, k, writerObject(k))
				if a.Status != code || !a.Complete || a.Err != nil {
					t.Fatalf("answer = %d complete %v, %v", a.Status, a.Complete, a.Err)
				}
				receive(t, "the origin handler", origin.ended)
				if origin.count() != 1 || elsewhere.count() != 0 {
					t.Fatalf("origin saw %d, redirect target saw %d; want 1 and 0", origin.count(), elsewhere.count())
				}
				if origin.auth[0] != "Bearer control-plane-token" {
					t.Fatalf("origin credentials %q", origin.auth[0])
				}

				// Control: the target's own client follows the same redirect
				// and replays a rewindable body.
				shared, err := rest.HTTPClientFor(rest.CopyConfig(config))
				if err != nil {
					t.Fatal(err)
				}
				req, _ := http.NewRequestWithContext(ctx, http.MethodPost, api.URL+writerPath(k), bytes.NewReader([]byte(`{}`)))
				resp, err := shared.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				resp.Body.Close()
				receive(t, "the origin handler", origin.ended)
				receive(t, "the redirect target handler", elsewhere.ended)
				if elsewhere.count() != 1 || string(elsewhere.bodies[0]) != `{}` {
					t.Fatalf("control did not follow the redirect: %d", elsewhere.count())
				}
			})
		}
	}
}

// refusingH2 is a raw HTTP/2 peer on a TLS listener the test owns. It
// counts each request's HEADERS, reads each request body to its end and
// then resets the stream with REFUSED_STREAM, never answering. Each accepted
// connection is owned by its one handler, which closes it exactly once.
type refusingH2 struct {
	ln      net.Listener
	caPEM   []byte
	streams atomic.Int32
	bodies  chan []byte
	closes  atomic.Int32 // connection Closes, by handlers or a refused accept

	mu     sync.Mutex
	conns  []net.Conn // every accepted connection, for deadlines at stop and the Close count
	closed bool

	// served counts the accept loop and each handler; done closes once
	// all have returned.
	served sync.WaitGroup
	done   chan struct{}
}

// countedConn counts its Close calls.
type countedConn struct {
	net.Conn
	n *atomic.Int32
}

func (c countedConn) Close() error {
	c.n.Add(1)
	return c.Conn.Close()
}

func newRefusingH2(t *testing.T) *refusingH2 {
	t.Helper()
	pki := newTestPKI(t)
	cert, _, _ := pki.issue(t, 2, "127.0.0.1", nil, []net.IP{net.ParseIP("127.0.0.1")}, x509.ExtKeyUsageServerAuth)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{http2.NextProtoTLS},
	})
	if err != nil {
		t.Fatal(err)
	}
	p := &refusingH2{ln: ln, caPEM: pki.caPEM, bodies: make(chan []byte, 16), done: make(chan struct{})}
	// Registered before the accept loop starts.
	t.Cleanup(func() { p.stop(t) })
	p.served.Add(1)
	go p.accept()
	go func() { p.served.Wait(); close(p.done) }()
	return p
}

func (p *refusingH2) accept() {
	defer p.served.Done()
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		c = countedConn{c, &p.closes}
		p.mu.Lock()
		p.conns = append(p.conns, c)
		if p.closed {
			p.mu.Unlock()
			c.Close() // its one Close: it is never handed to a handler
			return
		}
		// The accept loop's own count keeps the group above zero here.
		p.served.Add(1)
		p.mu.Unlock()
		go func() {
			defer p.served.Done()
			defer c.Close()
			p.serve(c)
		}()
	}
}

// stop closes the listener once, ends each handler's blocked read or write
// with a deadline, and joins the accept loop and every handler. Nothing but
// a connection's own handler closes it. A peer that does not return within
// testWait is reported and retained.
func (p *refusingH2) stop(t *testing.T) {
	p.mu.Lock()
	p.closed = true
	conns := slices.Clone(p.conns)
	p.mu.Unlock()
	lerr := p.ln.Close()
	for _, c := range conns {
		c.SetDeadline(time.Now())
	}
	select {
	case <-p.done:
	case <-time.After(testWait):
		t.Errorf("raw HTTP/2 peer did not return; retained")
		retainCall(p)
		return
	}
	if lerr != nil {
		t.Errorf("close listener: %v", lerr)
	}
	p.mu.Lock()
	accepted := len(p.conns)
	p.mu.Unlock()
	if n := int(p.closes.Load()); n != accepted {
		t.Errorf("%d connection Closes for %d accepted connections, want one each", n, accepted)
	}
}

func (p *refusingH2) serve(c net.Conn) {
	preface := make([]byte, len(http2.ClientPreface))
	if _, err := io.ReadFull(c, preface); err != nil || string(preface) != http2.ClientPreface {
		return
	}
	fr := http2.NewFramer(c, c)
	if fr.WriteSettings() != nil {
		return
	}
	bodies := map[uint32][]byte{}
	for {
		f, err := fr.ReadFrame()
		if err != nil {
			return
		}
		switch f := f.(type) {
		case *http2.SettingsFrame:
			if !f.IsAck() {
				fr.WriteSettingsAck()
			}
		case *http2.PingFrame:
			if !f.IsAck() {
				fr.WritePing(true, f.Data)
			}
		case *http2.HeadersFrame:
			p.streams.Add(1)
			if f.StreamEnded() {
				fr.WriteRSTStream(f.StreamID, http2.ErrCodeRefusedStream)
			}
		case *http2.DataFrame:
			id := f.StreamID
			bodies[id] = append(bodies[id], f.Data()...)
			if f.StreamEnded() {
				select {
				case p.bodies <- bodies[id]:
				default:
				}
				delete(bodies, id)
				fr.WriteRSTStream(id, http2.ErrCodeRefusedStream)
			}
		}
	}
}

func (p *refusingH2) config() *rest.Config {
	return &rest.Config{Host: "https://" + p.ln.Addr().String(), BearerToken: "control-plane-token",
		TLSClientConfig: rest.TLSClientConfig{CAData: p.caPEM}}
}

// TestServiceWriterNoReplayAfterRefusedStream: a peer reset after the body
// was consumed is not retried, because the writer's body cannot be rewound.
func TestServiceWriterNoReplayAfterRefusedStream(t *testing.T) {
	p := newRefusingH2(t)
	writer, err := newServiceHTTP(p.config(), "workloads")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	d := writerObject(deploymentObject).(*appsv1.Deployment)
	a := writer.create(ctx, deploymentObject, d)
	if a.Status != 0 || a.Err == nil {
		t.Fatalf("answer = %d, %v; want no answer", a.Status, a.Err)
	}
	body := receive(t, "the peer's read of the body", p.bodies)
	var got appsv1.Deployment
	if err := json.Unmarshal(body, &got); err != nil || got.Name != d.Name {
		t.Fatalf("peer read %q", body)
	}
	// The x/net transport retries the first time without delay; wait past
	// its first backoff.
	time.Sleep(2 * time.Second)
	if n := p.streams.Load(); n != 1 {
		t.Fatalf("peer saw %d requests, want 1", n)
	}

	// Control: the target's own client, with a rewindable body, replays it.
	shared, err := rest.HTTPClientFor(p.config())
	if err != nil {
		t.Fatal(err)
	}
	// Its later retries back off; the first replay is immediate.
	short, stop := context.WithTimeout(ctx, 3*time.Second)
	defer stop()
	req, _ := http.NewRequestWithContext(short, http.MethodPost, "https://"+p.ln.Addr().String()+writerPath(deploymentObject), bytes.NewReader([]byte(`{}`)))
	if resp, err := shared.Do(req); err == nil {
		resp.Body.Close()
	}
	if n := p.streams.Load(); n < 3 {
		t.Fatalf("control: peer saw %d requests in all, want a replay", n)
	}
}

// TestServiceForwardRuntimeGatewayCredentials is a component test: the
// service runtime's Forward with the real port gateway, the fake API
// server's SPDY port-forward (MODELED kubelet) and an application server,
// behind a plain handler rather than the production route, Controller and
// auth. TestServiceForwardComposedCredentials is the composed test.
func TestServiceForwardRuntimeGatewayCredentials(t *testing.T) {
	app := newApp(t, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") })
	a := newFakeAPI(t, portProtocol)
	tr := a.transport(t)
	seen := a.servePortForward(app.Listener.Addr(), "")

	client := fake.NewSimpleClientset()
	rt := newTestServiceRuntime(t, client, newModelWriter(client))
	native := mustApply(t, rt)
	addReady(t, client, nil)
	rt.serve = func(w http.ResponseWriter, r *http.Request, target portTarget) *portFrame {
		p := tr.servePort(w, r, target)
		a.keep(p.envFrame)
		return p
	}

	var reads atomic.Int32
	var failFlush atomic.Bool
	forwarded := make(chan error, 1)
	route := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w = deadlineWriter{w, http.NewResponseController(w), &reads, &failFlush}
		forwarded <- rt.Forward(r.Context(), testRef(native), w, r)
	}))
	ownServer(t, route)

	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, route.URL+"/v1/predict", nil)
	for k, v := range map[string]string{
		"Authorization":            "Bearer client-token",
		"Proxy-Authorization":      "Basic cHJveHk=",
		"Cookie":                   "session=1",
		"Impersonate-User":         "admin",
		"Impersonate-Group":        "system:masters",
		"Impersonate-Extra-Scopes": "all",
		"X-Skygo-Session":          "proof",
		"X-Skygo-Login":            "login",
		"Skygo-Audit":              "audit",
		"X-Custom":                 "keep",
	} {
		req.Header.Set(k, v)
	}
	resp, err := testClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "ok" {
		t.Fatalf("response %d %q", resp.StatusCode, body)
	}
	if err := receive(t, "the forward", forwarded); err != nil {
		t.Fatal(err)
	}
	if pair := receive(t, "the port-forward peer", seen); pair.err != nil {
		t.Fatal(pair.err)
	}

	ar := app.requestList()
	if len(ar) != 1 {
		t.Fatalf("app saw %d requests", len(ar))
	}
	for k := range req.Header {
		if k != "X-Custom" && ar[0].Header.Get(k) != "" {
			t.Errorf("workload saw %s", k)
		}
	}
	if ar[0].Header.Get("X-Custom") != "keep" {
		t.Errorf("workload headers %v", ar[0].Header)
	}
	reqs := a.requestList()
	if len(reqs) != 1 {
		t.Fatalf("api saw %d requests", len(reqs))
	}
	api := reqs[0]
	if api.URL.Path != "/api/v1/namespaces/workloads/pods/skygo-svc-0123456789abcdef0123-abc123-x/portforward" ||
		api.Header.Get("Authorization") != "Bearer control-plane-token" {
		t.Errorf("api request = %s %v", api.URL, api.Header)
	}
	for k := range req.Header {
		if k != "Authorization" && api.Header.Get(k) != "" {
			t.Errorf("api server saw client %s", k)
		}
	}
}

// TestServiceWriterBoundsOriginalWait: the actual writer, with private
// 25ms/10ms caller budgets, sends one create to a local server (MODELED API)
// whose handler ignores the request's context and holds its answer. The
// caller returns at its budget, under a live context, with the answer
// pending and the same exchange: the original request and one-shot body,
// not resent. Once released, that same exchange's Do and read return with
// the 201 and its request and response Closes return: nothing is
// restarted. The twin's unheld answer settles before the caller returns.
func TestServiceWriterBoundsOriginalWait(t *testing.T) {
	for _, held := range []bool{true, false} {
		t.Run(fmt.Sprintf("held %v", held), func(t *testing.T) {
			h := newHits()
			gate := make(chan struct{})
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer func() { h.ended <- struct{}{} }()
				h.record(r)
				if held {
					// The MODELED server ignores r.Context(). The caller's
					// context stays live: this shows a bounded wait, not a Do
					// that ignores cancellation (see the writerTap tests).
					<-gate
				}
				w.WriteHeader(http.StatusCreated)
				w.Write([]byte(`{}`))
			}))
			ownServer(t, api)
			var once sync.Once
			open := func() { once.Do(func() { close(gate) }) }
			t.Cleanup(open) // before the server's join, and before any Fatal below
			writer, err := newServiceHTTP(&rest.Config{Host: api.URL, BearerToken: "control-plane-token"}, "workloads")
			if err != nil {
				t.Fatal(err)
			}
			if held {
				writer.answerWait, writer.settleWait = 25*time.Millisecond, 10*time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), testWait)
			defer cancel()
			got := make(chan serviceAnswer, 1)
			go func() { got <- writer.create(ctx, deploymentObject, writerObject(deploymentObject)) }()
			var a serviceAnswer
			select {
			case a = <-got:
			case <-time.After(testWait): // independent watchdog
				t.Fatal("caller wait not bounded by its budget")
			}
			if ctx.Err() != nil {
				t.Fatal("caller returned only at its context's end")
			}
			if a.x == nil || a.x.req == nil || a.x.req.Body != io.ReadCloser(a.x.body) {
				t.Fatalf("answer without its original exchange: %+v", a)
			}
			if !held {
				if a.Err != nil || a.Status != http.StatusCreated || !a.Complete || !a.facts.settled() {
					t.Fatalf("unheld answer %d %v, facts %+v", a.Status, a.Err, a.facts)
				}
				return
			}
			if a.Status != 0 || !errors.Is(a.Err, errServiceAnswerPending) || a.facts.answered {
				t.Fatalf("held answer %d %v, facts %+v; want pending", a.Status, a.Err, a.facts)
			}
			open()
			x := a.x
			for _, ch := range []<-chan struct{}{x.done, x.requestClosed()} {
				select {
				case <-ch:
				case <-time.After(testWait):
					t.Fatal("released original did not return")
				}
			}
			select {
			case <-x.closeDone():
			case <-time.After(testWait):
				t.Fatal("original response Close did not return")
			}
			f := x.facts()
			if !f.settled() || x.failed(f) || x.cause(f) != nil {
				t.Fatalf("released exchange facts %+v: %v", f, x.cause(f))
			}
			if late := x.answer(f); late.Status != http.StatusCreated || !late.Complete || string(late.Body) != `{}` || late.x != x {
				t.Fatalf("late answer %d %q", late.Status, late.Body)
			}
			if n := h.count(); n != 1 {
				t.Fatalf("%d requests, want the one original", n)
			}
		})
	}
}

// modeledServiceBody is a MODELED response body: its bytes come from Reader and
// its Close calls close.
type modeledServiceBody struct {
	io.Reader
	close func() error
}

func (b modeledServiceBody) Close() error { return b.close() }

// TestReadAnswerKeepsCloseApart: the bytes read and the original Close are
// separate facts. A complete body keeps Complete and its bytes whatever its
// Close returns; a read error and a Close error are both kept. The bodies
// and their faults are MODELED.
func TestReadAnswerKeepsCloseApart(t *testing.T) {
	body := []byte(`{"kind":"Status"}`)
	for _, tt := range []struct {
		name     string
		r        io.Reader
		closeErr error
		complete bool
		readErr  error
	}{
		{"complete nil close", bytes.NewReader(body), nil, true, nil},
		{"complete failed close", bytes.NewReader(body), errModeledClose, true, nil},
		{"read error failed close", io.MultiReader(bytes.NewReader(body), iotest.ErrReader(errModeledRead)), errModeledClose, false, errModeledRead},
		{"read error nil close", io.MultiReader(bytes.NewReader(body), iotest.ErrReader(errModeledRead)), nil, false, errModeledRead},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var closes atomic.Int32
			a := readAnswer(&http.Response{StatusCode: http.StatusCreated, Body: modeledServiceBody{tt.r, func() error {
				closes.Add(1)
				return tt.closeErr
			}}})
			ctx, cancel := context.WithTimeout(context.Background(), testWait)
			defer cancel()
			a.Close.wait(ctx)
			if a.Status != http.StatusCreated || a.Complete != tt.complete || !bytes.Equal(a.Body, body) {
				t.Fatalf("answer %d complete %v body %q", a.Status, a.Complete, a.Body)
			}
			if tt.readErr == nil && a.Err != nil || tt.readErr != nil && !errors.Is(a.Err, tt.readErr) {
				t.Fatalf("read error = %v, want %v", a.Err, tt.readErr)
			}
			if cerr := a.closeErr(); tt.closeErr == nil && cerr != nil || tt.closeErr != nil && (!errors.Is(cerr, errServiceCloseFailed) || !errors.Is(cerr, tt.closeErr)) {
				t.Fatalf("close cause = %v, want %v", cerr, tt.closeErr)
			}
			if n := closes.Load(); n != 1 {
				t.Fatalf("%d Closes, want the one original", n)
			}
		})
	}
}

// A gate holds one original until it is opened; opening is idempotent.
type serviceGate struct {
	once sync.Once
	c    chan struct{}
}

func newServiceGate() *serviceGate { return &serviceGate{c: make(chan struct{})} }

func (g *serviceGate) open() {
	if g != nil {
		g.once.Do(func() { close(g.c) })
	}
}

// An event is closed, once, when an original reaches a point.
type event struct {
	once sync.Once
	c    chan struct{}
}

func newEvent() *event { return &event{c: make(chan struct{})} }

func (e *event) fire() { e.once.Do(func() { close(e.c) }) }

// within waits up to testWait for e.
func within(t *testing.T, what string, e *event) {
	t.Helper()
	select {
	case <-e.c:
	case <-time.After(testWait):
		t.Fatalf("%s did not happen", what)
	}
}

// A writerTap wraps a service writer's private do hook. It saves the hook
// the constructor set, before the writer's first write, and calls it
// exactly once per request, with that same request: the writer's own
// client, credentials, TLS and endpoint send it. The tap records each
// original and, for a request a tapFault selects, applies that fault's
// MODELED holds and errors around the actual call: after the actual Do
// returned, at the end of the actual response body read, before the
// underlying request body's first Close, and after the underlying
// response body's first Close. Every hold ignores the caller's context, as
// a stuck original would; that is the model, not net/http behavior. It
// replaces no client, transport or writer, and makes up no response.
type writerTap struct {
	actual func(*http.Request) (*http.Response, error)

	mu     sync.Mutex
	calls  []*tapCall
	faults []*tapFault
}

// A tapFault selects the first request whose method, resource and service
// object name match, and holds at each gate set. readErr replaces the
// read's io.EOF and closeErr the first Close's result (MODELED). call is
// the request it selected; matched fires then.
type tapFault struct {
	method, resource, name string

	doReturn, read, reqClose, respClose *serviceGate
	readErr, closeErr                   error

	call    *tapCall
	matched *event
}

// A tapCall is one original request the tap delegated: the request and its
// original body, and, once doReturned fires, the actual Do's response, its
// original body and error. Events fire at each hold, at returned when the
// hook returns, and at reqClosed and respClosed when the first Close of a
// body the tap wrapped returned. The counts are of Close calls on each
// wrapper and of the underlying originals' Closes, each at most once.
type tapCall struct {
	req  *http.Request
	body io.ReadCloser

	resp     *http.Response
	respBody io.ReadCloser
	err      error

	doReturned, atDo, atRead, atReqClose, atRespClose, returned, reqClosed, respClosed *event

	reqCloses, underReqCloses, respCloses, underRespCloses atomic.Int32
	wrappedReq, wrappedResp                                atomic.Bool

	mu      sync.Mutex
	read    []byte // bytes the response body read returned
	readErr error  // actual terminal Read result, before any modeled substitution
}

func (c *tapCall) readBytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.read)
}

// actualReadError is the original body's terminal Read result. atRead
// fires after this value is recorded; a terminal error is not necessarily EOF.
func (c *tapCall) actualReadError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readErr
}

// tapWriter saves w's do hook, which must be the one the constructor set,
// and installs a writerTap around it. It must run before w's first write.
// Its cleanup, registered before any Fatal of the test, opens every gate
// of every fault and joins each original the tap holds; one that does not
// return within testWait is reported and retained.
func tapWriter(t *testing.T, w *serviceHTTP) *writerTap {
	t.Helper()
	if w.do == nil {
		t.Fatal("writer has no do hook")
	}
	p := &writerTap{actual: w.do}
	w.do = p.do
	t.Cleanup(func() {
		p.openAll()
		if err := p.join(testWait); err != nil {
			t.Errorf("tapped originals did not return; retained: %v", err)
			retainCall(p)
		}
	})
	return p
}

// fault adds a fault selecting the next request of method on resource
// ("deployments" or "services") naming service id's object.
func (p *writerTap) fault(method, resource, id string) *tapFault {
	return p.faultNamed(method, resource, serviceObjectName(id))
}

// faultNamed adds a fault selecting the next request of method on
// resource naming the object name.
func (p *writerTap) faultNamed(method, resource, name string) *tapFault {
	f := &tapFault{method: method, resource: resource, name: name, matched: newEvent()}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.faults = append(p.faults, f)
	return f
}

// openAll opens every gate of every fault.
func (p *writerTap) openAll() {
	p.mu.Lock()
	faults := slices.Clone(p.faults)
	p.mu.Unlock()
	for _, f := range faults {
		for _, g := range []*serviceGate{f.doReturn, f.read, f.reqClose, f.respClose} {
			g.open()
		}
	}
}

// joined reports, without waiting, whether every original the tap
// delegated has returned: each hook, and each first Close of a body it
// wrapped.
func (p *writerTap) joined() error {
	p.mu.Lock()
	calls := slices.Clone(p.calls)
	p.mu.Unlock()
	for _, c := range calls {
		for _, e := range c.joins() {
			if !isClosed(e.c) {
				return fmt.Errorf("%s %s not returned", c.req.Method, c.req.URL.Path)
			}
		}
	}
	return nil
}

// join waits up to d for every original the tap delegated.
func (p *writerTap) join(d time.Duration) error {
	p.mu.Lock()
	calls := slices.Clone(p.calls)
	p.mu.Unlock()
	timeout := time.After(d)
	for _, c := range calls {
		for _, e := range c.joins() {
			select {
			case <-e.c:
			case <-timeout:
				return fmt.Errorf("%s %s not returned", c.req.Method, c.req.URL.Path)
			}
		}
	}
	return nil
}

func (c *tapCall) joins() []*event {
	es := []*event{c.returned}
	if c.wrappedReq.Load() {
		es = append(es, c.reqClosed)
	}
	if c.wrappedResp.Load() {
		es = append(es, c.respClosed)
	}
	return es
}

// count returns the requests of method on resource naming id's object.
func (p *writerTap) count(method, resource, id string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, c := range p.calls {
		if matchRequestBody(c.req, c.body, method, resource, serviceObjectName(id)) {
			n++
		}
	}
	return n
}

// total returns the number of requests the tap delegated.
func (p *writerTap) total() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.calls)
}

// called waits for f to select its request and returns that call.
func called(t *testing.T, f *tapFault) *tapCall {
	t.Helper()
	within(t, "the selected request", f.matched)
	return f.call // set before matched fired
}

// matchRequest reports whether req is a method request on resource naming
// the object name: in its path for a delete, in its one-shot body, read
// in place without consuming it, for a create.
func matchRequest(req *http.Request, method, resource, name string) bool {
	return matchRequestBody(req, req.Body, method, resource, name)
}

// matchRequestBody lets counters inspect the original body saved before a
// fault wraps req.Body. The original bytes remain unchanged and unconsumed.
func matchRequestBody(req *http.Request, body io.ReadCloser, method, resource, name string) bool {
	if req.Method != method {
		return false
	}
	if method == http.MethodDelete {
		return strings.HasSuffix(req.URL.Path, "/"+resource+"/"+name)
	}
	if !strings.HasSuffix(req.URL.Path, "/"+resource) {
		return false
	}
	b, ok := body.(*oneShotBody)
	if !ok {
		return false
	}
	raw := make([]byte, b.r.Size())
	n, _ := b.r.ReadAt(raw, 0)
	return bytes.Contains(raw[:n], []byte(`"name":"`+name+`"`))
}

func (p *writerTap) do(req *http.Request) (*http.Response, error) {
	c := &tapCall{req: req, body: req.Body,
		doReturned: newEvent(), atDo: newEvent(), atRead: newEvent(), atReqClose: newEvent(),
		atRespClose: newEvent(), returned: newEvent(), reqClosed: newEvent(), respClosed: newEvent()}
	defer c.returned.fire()
	p.mu.Lock()
	var f *tapFault
	for _, g := range p.faults {
		if g.call == nil && matchRequest(req, g.method, g.resource, g.name) {
			f, g.call = g, c
			break
		}
	}
	p.calls = append(p.calls, c)
	p.mu.Unlock()
	if f != nil {
		f.matched.fire()
		if f.reqClose != nil {
			c.wrappedReq.Store(true)
			req.Body = &tapRequestBody{c: c, under: req.Body, hold: f.reqClose}
		}
	}
	resp, err := p.actual(req) // exactly once
	c.resp, c.err = resp, err
	if resp != nil {
		c.respBody = resp.Body
	}
	c.doReturned.fire()
	if f == nil {
		return resp, err
	}
	if f.doReturn != nil {
		c.atDo.fire()
		<-f.doReturn.c
	}
	if resp != nil && resp.Body != nil && (f.read != nil || f.readErr != nil || f.respClose != nil || f.closeErr != nil) {
		c.wrappedResp.Store(true)
		resp.Body = &tapResponseBody{c: c, under: resp.Body, f: f}
	}
	return resp, err
}

// tapRequestBody is a request's original body with a MODELED hold of its
// first Close made before the underlying body's own Close: while held, the
// underlying one-shot body is genuinely unclosed. Reads are the original's.
type tapRequestBody struct {
	c     *tapCall
	under io.ReadCloser
	hold  *serviceGate
	once  sync.Once
	err   error
}

func (b *tapRequestBody) Read(p []byte) (int, error) { return b.under.Read(p) }

func (b *tapRequestBody) Close() error {
	b.c.reqCloses.Add(1)
	b.once.Do(func() {
		b.c.atReqClose.fire()
		<-b.hold.c
		b.c.underReqCloses.Add(1)
		b.err = b.under.Close()
		b.c.reqClosed.fire()
	})
	return b.err
}

// tapResponseBody is a response's original body. Reads are the
// original's; at the read's end it holds, if f.read is set, and replaces
// io.EOF with f.readErr, if set. Its first Close calls the original's
// Close once, then holds, if f.respClose is set, and returns f.closeErr, if
// set, in place of the original's result. All of that is MODELED.
type tapResponseBody struct {
	c     *tapCall
	under io.ReadCloser
	f     *tapFault
	once  sync.Once
	err   error
}

func (b *tapResponseBody) Read(p []byte) (int, error) {
	n, err := b.under.Read(p)
	b.c.mu.Lock()
	b.c.read = append(b.c.read, p[:n]...)
	if err != nil {
		b.c.readErr = err
	}
	b.c.mu.Unlock()
	if err != nil {
		b.c.atRead.fire()
		if b.f.read != nil {
			<-b.f.read.c
		}
		if b.f.readErr != nil && err == io.EOF {
			err = b.f.readErr
		}
	}
	return n, err
}

func (b *tapResponseBody) Close() error {
	b.c.respCloses.Add(1)
	b.once.Do(func() {
		b.c.underRespCloses.Add(1)
		b.err = b.under.Close()
		b.c.atRespClose.fire()
		if b.f.respClose != nil {
			<-b.f.respClose.c
		}
		if b.f.closeErr != nil {
			b.err = b.f.closeErr
		}
		b.c.respClosed.fire()
	})
	return b.err
}

// tlsAPI starts a TLS server for h, HTTP/2 if h2, joined at cleanup, and
// returns it with a configuration naming it, trusting only its CA.
func tlsAPI(t *testing.T, h http.Handler, h2 bool) (*httptest.Server, *rest.Config) {
	t.Helper()
	s := httptest.NewUnstartedServer(h)
	s.EnableHTTP2 = h2
	s.StartTLS()
	ownServer(t, s)
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw})
	return s, &rest.Config{Host: s.URL, BearerToken: "control-plane-token", TLSClientConfig: rest.TLSClientConfig{CAData: ca}}
}

// TestServiceWriterDoHookIsClientDo (U03): the constructor's do hook is
// the writer's own client copy's Do. Unwrapped, one write reaches the TLS
// endpoint the configuration names, trusted through its CA alone, once,
// with its bearer, and a redirect is returned, not followed, as only that
// copy's CheckRedirect makes it. The control is a substitute hook,
// http.DefaultClient's Do: the same write fails, so the oracle detects a
// nil or substituted path.
func TestServiceWriterDoHookIsClientDo(t *testing.T) {
	h := newHits()
	api, config := tlsAPI(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { h.ended <- struct{}{} }()
		h.record(r)
		http.Redirect(w, r, "/elsewhere", http.StatusTemporaryRedirect)
	}), false)
	_ = api
	writer, err := newServiceHTTP(config, "workloads")
	if err != nil {
		t.Fatal(err)
	}
	if writer.do == nil || writer.client == nil || writer.client.CheckRedirect == nil {
		t.Fatal("constructor left the do hook or its client copy unset")
	}
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	a := writer.create(ctx, deploymentObject, writerObject(deploymentObject))
	if a.Status != http.StatusTemporaryRedirect || !a.Complete || a.Err != nil {
		t.Fatalf("answer = %d complete %v, %v; want the redirect itself", a.Status, a.Complete, a.Err)
	}
	receive(t, "the handler", h.ended)
	if h.count() != 1 || h.auth[0] != "Bearer control-plane-token" {
		t.Fatalf("api saw %d requests, credentials %q", h.count(), h.auth)
	}

	sub := *writer
	sub.do = http.DefaultClient.Do
	if a := sub.create(ctx, deploymentObject, writerObject(deploymentObject)); a.Status != 0 || a.Err == nil {
		t.Fatalf("substitute hook answered %d, %v; the oracle cannot tell it apart", a.Status, a.Err)
	}
	if h.count() != 1 {
		t.Fatalf("substitute hook reached the api: %d requests", h.count())
	}
}

// TestServiceWriterRequestClosePendingH2 (V04, the HTTP/2 twin): over an
// HTTP/2 TLS server, net/http's HTTP/2 transport closes the request body
// apart from the response. The tap holds the request body's first Close
// before the underlying one-shot body's Close (MODELED), and the answer
// arrives complete, 201 with its bytes, while that original stays
// genuinely pending: the one-shot body is not closed. The caller returns at
// its 10ms settle budget, with the answer and the request Close pending,
// and the same exchange; once released, the underlying Close is made once
// and the exchange settles. The unheld twin settles before the caller
// returns. Response and request-body progress are observed explicitly;
// no ordering of HTTP/1.1 response delivery and request Close is assumed.
func TestServiceWriterRequestClosePendingH2(t *testing.T) {
	for _, held := range []bool{true, false} {
		t.Run(fmt.Sprintf("held %v", held), func(t *testing.T) {
			h := newHits()
			_, config := tlsAPI(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer func() { h.ended <- struct{}{} }()
				h.record(r)
				w.WriteHeader(http.StatusCreated)
				w.Write([]byte(`{}`))
			}), true)
			writer, err := newServiceHTTP(config, "workloads")
			if err != nil {
				t.Fatal(err)
			}
			p := tapWriter(t, writer)
			writer.settleWait = 10 * time.Millisecond
			obj := writerObject(deploymentObject)
			var f *tapFault
			if held {
				f = p.faultNamed(http.MethodPost, "deployments", "skygo-svc-x")
				f.reqClose = newServiceGate()
			}
			ctx, cancel := context.WithTimeout(context.Background(), testWait)
			defer cancel()
			got := make(chan serviceAnswer, 1)
			go func() { got <- writer.create(ctx, deploymentObject, obj) }()
			var a serviceAnswer
			select {
			case a = <-got:
			case <-time.After(testWait):
				t.Fatal("caller wait not bounded by its budget")
			}
			if a.Status != http.StatusCreated || !a.Complete || string(a.Body) != `{}` || a.x == nil {
				t.Fatalf("answer %d %q complete %v", a.Status, a.Body, a.Complete)
			}
			if a.x.resp == nil || a.x.resp.ProtoMajor != 2 {
				t.Fatal("answer not over HTTP/2")
			}
			x := a.x
			if !held {
				if !a.facts.settled() || x.cause(a.facts) != nil {
					t.Fatalf("unheld facts %+v: %v", a.facts, x.cause(a.facts))
				}
				return
			}
			c := f.call
			within(t, "the request body's first Close", c.atReqClose)
			if !a.facts.answered || a.facts.requestClosed || !errors.Is(x.pending(a.facts), errServiceRequestClosePending) {
				t.Fatalf("facts %+v, %v; want answered with the request Close pending", a.facts, x.pending(a.facts))
			}
			if isClosed(x.body.closed) || c.underReqCloses.Load() != 0 || io.ReadCloser(x.body) != c.body || x.req != c.req {
				t.Fatal("request body closed, or not the original, while its Close is held")
			}
			f.reqClose.open()
			select {
			case <-x.requestClosed():
			case <-time.After(testWait):
				t.Fatal("released request Close did not return")
			}
			if err := p.join(testWait); err != nil {
				t.Fatal(err)
			}
			select {
			case <-x.closeDone():
			case <-time.After(testWait):
				t.Fatal("response Close did not return")
			}
			if fs := x.facts(); !fs.settled() || x.cause(fs) != nil || c.underReqCloses.Load() != 1 || h.count() != 1 {
				t.Fatalf("released: facts %+v, %v; %d underlying Closes, %d requests", fs, x.cause(fs), c.underReqCloses.Load(), h.count())
			}
		})
	}
}

// TestServiceTapActualReadError distinguishes EOF from a real truncated
// HTTP response. The configured TLS client reads the same server's body;
// the model is only the server's deliberately incorrect Content-Length.
func TestServiceTapActualReadError(t *testing.T) {
	for _, truncated := range []bool{false, true} {
		t.Run(fmt.Sprintf("truncated %v", truncated), func(t *testing.T) {
			_, config := tlsAPI(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if truncated {
					w.Header().Set("Content-Length", "10")
				}
				w.WriteHeader(http.StatusCreated)
				w.Write([]byte(`{}`))
			}), false)
			writer, err := newServiceHTTP(config, "workloads")
			if err != nil {
				t.Fatal(err)
			}
			p := tapWriter(t, writer)
			f := p.faultNamed(http.MethodPost, "deployments", "skygo-svc-x")
			f.read = newServiceGate()
			ctx, cancel := context.WithTimeout(context.Background(), testWait)
			defer cancel()
			done := make(chan serviceAnswer, 1)
			go func() { done <- writer.create(ctx, deploymentObject, writerObject(deploymentObject)) }()
			c := called(t, f)
			within(t, "the actual terminal Read", c.atRead)
			want := error(io.EOF)
			if truncated {
				want = io.ErrUnexpectedEOF
			}
			if got := c.actualReadError(); got != want {
				t.Fatalf("actual terminal Read = %v, want %v", got, want)
			}
			if truncated && c.actualReadError() == io.EOF {
				t.Fatal("a truncated response satisfied the EOF-before-cancel oracle")
			}
			f.read.open()
			var a serviceAnswer
			select {
			case a = <-done:
			case <-time.After(testWait):
				t.Fatal("writer did not return after its actual Read joined")
			}
			if a.Status != http.StatusCreated || string(a.Body) != `{}` || a.Complete == truncated || (truncated && !errors.Is(a.Err, io.ErrUnexpectedEOF)) {
				t.Fatalf("answer = %d %q, complete %v, %v", a.Status, a.Body, a.Complete, a.Err)
			}
			if err := p.join(testWait); err != nil {
				t.Fatal(err)
			}
		})
	}
}
