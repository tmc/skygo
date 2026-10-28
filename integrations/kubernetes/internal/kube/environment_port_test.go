package kube

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/client-go/rest"
)

const testPort = 8080

type pairSeen struct {
	headers []http.Header
	err     error
	// The relay's record, complete once both copies returned: each copy
	// in the order it returned, then the Close errors of the app
	// connection, the data stream and the error stream.
	copies    []copyEnd
	connClose error
	dataClose error
	errClose  error
}

// copyEnd is one relay copy's result: "up" copies the data stream to the
// app, "down" the app to the data stream.
type copyEnd struct {
	dir string
	err error
}

// servePortForward acts as the kubelet for the next port-forward pair: it
// takes the error and data streams, connects to app and copies the data
// stream in both directions. The first copy to return ends the relay: the
// app connection is closed, once, the data stream is half-closed after the
// down copy returns, and both copies are joined before the error stream is
// closed and the record is sent. If remoteErr is set it instead reports a
// forwarding failure: it writes remoteErr on the error stream, closes it, and
// leaves the data stream open until the client ends it.
func (a *fakeAPI) servePortForward(app net.Addr, remoteErr string) <-chan pairSeen {
	c := make(chan pairSeen, 1)
	go func() {
		var seen pairSeen
		defer func() { c <- seen }()
		var errStream, data httpstream.Stream
		timeout := time.After(testWait)
		for len(seen.headers) < 2 {
			select {
			case s := <-a.streams:
				seen.headers = append(seen.headers, s.Headers().Clone())
				if s.Headers().Get(corev1.StreamType) == corev1.StreamTypeError {
					errStream = s
				} else {
					data = s
				}
			case <-timeout:
				seen.err = errors.New("streams did not arrive")
				return
			}
		}
		if errStream == nil || data == nil {
			seen.err = errors.New("pair is not one error and one data stream")
			return
		}
		if remoteErr != "" {
			errStream.Write([]byte(remoteErr))
			errStream.Close()
			io.Copy(io.Discard, data)
			return
		}
		conn, err := net.Dial("tcp", app.String())
		if err != nil {
			seen.err = err
			return
		}
		up := make(chan error, 1)
		down := make(chan error, 1)
		go func() {
			_, err := io.Copy(conn, data)
			up <- err
		}()
		go func() {
			_, err := io.Copy(data, conn)
			down <- err
		}()
		upDone := false
		select {
		case err := <-up:
			seen.copies = append(seen.copies, copyEnd{"up", err})
			upDone = true
		case err := <-down:
			seen.copies = append(seen.copies, copyEnd{"down", err})
		}
		// Closing the app connection ends the app's wait for the rest of a
		// request the gateway cut, and the down copy's wait for a response
		// that will not come.
		seen.connClose = conn.Close()
		if upDone {
			seen.copies = append(seen.copies, copyEnd{"down", <-down})
		}
		// Nothing more goes to the gateway; the half-close ends a response
		// delimited by the stream's end. The up copy returns once the
		// gateway ends the data stream.
		seen.dataClose = data.Close()
		if !upDone {
			seen.copies = append(seen.copies, copyEnd{"up", <-up})
		}
		seen.errClose = errStream.Close()
	}()
	return c
}

// gateway serves tr's fixed-port gateway for target and keeps every frame in
// a. Its cleanup runs before a's.
type gateway struct {
	server    *httptest.Server
	frames    chan *portFrame
	reads     atomic.Int32 // SetReadDeadline calls made by the gateway
	failFlush atomic.Bool  // if set, flushes fail with errSynthetic unmade
}

// deadlineWriter passes a real net/http ResponseWriter's deadlines and
// flushes through, counting SetReadDeadline calls.
type deadlineWriter struct {
	http.ResponseWriter
	rc        *http.ResponseController
	reads     *atomic.Int32
	failFlush *atomic.Bool
}

func (d deadlineWriter) SetReadDeadline(t time.Time) error {
	d.reads.Add(1)
	return d.rc.SetReadDeadline(t)
}

func (d deadlineWriter) SetWriteDeadline(t time.Time) error { return d.rc.SetWriteDeadline(t) }

func (d deadlineWriter) FlushError() error {
	if d.failFlush.Load() {
		return errSynthetic
	}
	return d.rc.Flush()
}

func newGateway(t *testing.T, a *fakeAPI, tr *envTransport) *gateway {
	g := &gateway{frames: make(chan *portFrame, 4)}
	g.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w = deadlineWriter{w, http.NewResponseController(w), &g.reads, &g.failFlush}
		p := tr.servePort(w, r, portTarget{Pod: "env-0", Port: testPort})
		a.keep(p.envFrame)
		g.frames <- p
	}))
	t.Cleanup(g.server.Close)
	return g
}

func (g *gateway) frame(t *testing.T) *portFrame {
	t.Helper()
	select {
	case p := <-g.frames:
		return p
	case <-time.After(testWait):
		t.Fatal("handler did not return")
	}
	return nil
}

// testClient bounds every exchange by testWait.
func testClient() *http.Client {
	return &http.Client{
		Timeout:       testWait,
		Transport:     &http.Transport{DisableKeepAlives: true},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// appServer starts an application server and records its requests.
type appServer struct {
	*httptest.Server
	mu       sync.Mutex
	requests []*http.Request
}

func newApp(t *testing.T, h http.HandlerFunc) *appServer {
	app := &appServer{}
	app.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		app.mu.Lock()
		app.requests = append(app.requests, r.Clone(context.Background()))
		app.mu.Unlock()
		h(w, r)
	}))
	t.Cleanup(app.Close)
	return app
}

func (app *appServer) requestList() []*http.Request {
	app.mu.Lock()
	defer app.mu.Unlock()
	return slices.Clone(app.requests)
}

func TestPortExchange(t *testing.T) {
	app := newApp(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-App", "yes")
		io.WriteString(w, "hello")
	})
	a := newFakeAPI(t, portProtocol)
	tr := a.transport(t)
	g := newGateway(t, a, tr)
	seen := a.servePortForward(app.Listener.Addr(), "")

	req, _ := http.NewRequest(http.MethodGet, g.server.URL+"/p/a?x=1", nil)
	req.Header.Set("Authorization", "Bearer client-supplied")
	req.Header.Set("Proxy-Authorization", "Basic cHJveHk=")
	req.Header.Set("Connection", "X-Hop")
	req.Header.Set("X-Hop", "drop")
	req.Header.Set("X-Custom", "keep")
	resp, err := testClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "hello" || resp.Header.Get("X-App") != "yes" {
		t.Fatalf("response %d %q %v", resp.StatusCode, body, resp.Header)
	}
	p := g.frame(t)
	s := waitSettled(t, p.envFrame)
	if s.Err != nil {
		t.Fatalf("frame = %v", s.Err)
	}
	if got := p.result().Status; got != http.StatusOK {
		t.Fatalf("status = %d", got)
	}

	pair := <-seen
	if pair.err != nil {
		t.Fatal(pair.err)
	}
	for i, typ := range []string{corev1.StreamTypeError, corev1.StreamTypeData} {
		h := pair.headers[i]
		if h.Get(corev1.StreamType) != typ || h.Get(corev1.PortHeader) != "8080" || h.Get(corev1.PortForwardRequestIDHeader) != portRequestID {
			t.Errorf("stream %d headers = %v", i, h)
		}
	}

	ar := app.requestList()
	if len(ar) != 1 {
		t.Fatalf("app saw %d requests", len(ar))
	}
	r := ar[0]
	if r.Header.Get("Authorization") != "" || r.Header.Get("Proxy-Authorization") != "" || r.Header.Get("X-Hop") != "" {
		t.Errorf("app saw stripped headers: %v", r.Header)
	}
	if r.Header.Get("X-Custom") != "keep" || r.Host != "localhost:8080" || r.URL.Path != "/p/a" || r.URL.RawQuery != "x=1" {
		t.Errorf("app request = %s %s %v", r.Host, r.URL, r.Header)
	}

	api := a.requestList()[0]
	if api.URL.Path != "/api/v1/namespaces/workloads/pods/env-0/portforward" ||
		api.Header.Get(httpstream.HeaderProtocolVersion) != portProtocol ||
		api.Header.Get("Authorization") != "Bearer control-plane-token" {
		t.Errorf("api request = %s %v", api.URL, api.Header)
	}
}

func TestPortRequestBody(t *testing.T) {
	app := newApp(t, func(w http.ResponseWriter, r *http.Request) {
		io.Copy(w, r.Body)
	})
	a := newFakeAPI(t, portProtocol)
	tr := a.transport(t)
	g := newGateway(t, a, tr)
	a.servePortForward(app.Listener.Addr(), "")
	resp, err := testClient().Post(g.server.URL+"/echo", "text/plain", strings.NewReader("payload"))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "payload" {
		t.Fatalf("body = %q", body)
	}
	if s := waitSettled(t, g.frame(t).envFrame); s.Err != nil {
		t.Fatal(s.Err)
	}
}

// A chunked request body one byte over the limit fails the exchange, and
// the Pod receives at most the limit, as a cut body.
func TestPortRequestBodyLimit(t *testing.T) {
	type appResult struct {
		n   int64
		err error
	}
	done := make(chan appResult, 1)
	app := newApp(t, func(w http.ResponseWriter, r *http.Request) {
		n, err := io.Copy(io.Discard, r.Body)
		done <- appResult{n, err}
	})
	a := newFakeAPI(t, portProtocol)
	tr := a.transport(t)
	g := newGateway(t, a, tr)
	seen := a.servePortForward(app.Listener.Addr(), "")
	body := io.MultiReader(strings.NewReader(strings.Repeat("b", maxPortRequestBody+1)))
	req, _ := http.NewRequest(http.MethodPost, g.server.URL+"/upload", body)
	resp, err := testClient().Do(req)
	if err == nil {
		resp.Body.Close()
	}
	s := waitSettled(t, g.frame(t).envFrame)
	if !errors.Is(s.Err, errPortLimit) {
		t.Fatalf("frame = %v", s.Err)
	}
	var got appResult
	select {
	case got = <-done:
	case <-time.After(testWait):
		// End the handler's read, so that the app's cleanup returns.
		app.CloseClientConnections()
		t.Fatal("app handler did not finish reading the request body")
	}
	if got.err == nil || got.n > maxPortRequestBody {
		t.Fatalf("app read %d body bytes with error %v; want a cut body of at most %d", got.n, got.err, maxPortRequestBody)
	}
	var pair pairSeen
	select {
	case pair = <-seen:
	case <-time.After(testWait):
		t.Fatal("port-forward relay did not finish")
	}
	if pair.err != nil {
		t.Fatal(pair.err)
	}
	// The gateway cut the data stream; only the app connection's Close
	// ends the down copy.
	if len(pair.copies) != 2 || pair.copies[0].dir != "up" || pair.copies[1].dir != "down" {
		t.Fatalf("relay copies = %v; want up, then down", pair.copies)
	}
	if pair.connClose != nil {
		t.Fatalf("app connection Close = %v", pair.connClose)
	}
}

// A chunked request body of exactly the limit is relayed whole.
func TestPortRequestBodyExactLimit(t *testing.T) {
	app := newApp(t, func(w http.ResponseWriter, r *http.Request) {
		n, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		io.WriteString(w, strconv.FormatInt(n, 10))
	})
	a := newFakeAPI(t, portProtocol)
	tr := a.transport(t)
	g := newGateway(t, a, tr)
	a.servePortForward(app.Listener.Addr(), "")
	body := io.MultiReader(strings.NewReader(strings.Repeat("b", maxPortRequestBody)))
	req, _ := http.NewRequest(http.MethodPost, g.server.URL+"/upload", body)
	resp, err := testClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(got) != strconv.Itoa(maxPortRequestBody) {
		t.Fatalf("response %d %q", resp.StatusCode, got)
	}
	if s := waitSettled(t, g.frame(t).envFrame); s.Err != nil {
		t.Fatalf("frame = %v", s.Err)
	}
}

// scriptReader returns its steps' data and errors in order: a step's data
// may span several Reads, and its error comes with the last of them.
type scriptReader struct {
	steps []scriptStep
}

type scriptStep struct {
	data string
	err  error
}

func (s *scriptReader) Read(p []byte) (int, error) {
	if len(s.steps) == 0 {
		return 0, io.EOF
	}
	st := &s.steps[0]
	n := copy(p, st.data)
	st.data = st.data[n:]
	if st.data != "" {
		return n, nil
	}
	err := st.err
	s.steps = s.steps[1:]
	return n, err
}

// boundedReader returns exactly the allowed bytes of a source of that size
// and fails a larger one without returning any byte past the limit, for
// every shape of final Read.
func TestBoundedReader(t *testing.T) {
	const limit = 4
	tests := []struct {
		name  string
		steps []scriptStep
		want  string
		err   error // nil means the source ended cleanly
	}{
		{"under", []scriptStep{{"abc", io.EOF}}, "abc", nil},
		{"exact with EOF", []scriptStep{{"abcd", io.EOF}}, "abcd", nil},
		{"exact then EOF", []scriptStep{{"abcd", nil}, {"", io.EOF}}, "abcd", nil},
		{"exact then empty reads", []scriptStep{{"abcd", nil}, {"", nil}, {"", nil}, {"", io.EOF}}, "abcd", nil},
		{"exact in chunks", []scriptStep{{"ab", nil}, {"cd", io.EOF}}, "abcd", nil},
		{"over with EOF", []scriptStep{{"abcde", io.EOF}}, "abcd", errPortLimit},
		{"over then EOF", []scriptStep{{"abcde", nil}, {"", io.EOF}}, "abcd", errPortLimit},
		{"over in chunks", []scriptStep{{"ab", nil}, {"cd", nil}, {"e", io.EOF}}, "abcd", errPortLimit},
		{"over across boundary", []scriptStep{{"abc", nil}, {"de", nil}, {"", io.EOF}}, "abcd", errPortLimit},
		{"over after empty reads", []scriptStep{{"abcd", nil}, {"", nil}, {"ef", io.EOF}}, "abcd", errPortLimit},
		{"source error", []scriptStep{{"ab", errSynthetic}}, "ab", errSynthetic},
	}
	for _, tt := range tests {
		for _, size := range []int{1, 3, 64} {
			b := &boundedReader{r: &scriptReader{steps: slices.Clone(tt.steps)}, n: limit}
			var got []byte
			var err error
			buf := make([]byte, size)
			for {
				n, rerr := b.Read(buf)
				got = append(got, buf[:n]...)
				if rerr != nil {
					err = rerr
					break
				}
			}
			if err == io.EOF {
				err = nil
			}
			if string(got) != tt.want || !errors.Is(err, tt.err) || (tt.err == nil) != (err == nil) {
				t.Errorf("%s/%d: read %q, %v; want %q, %v", tt.name, size, got, err, tt.want, tt.err)
			}
			if b.hit != errors.Is(tt.err, errPortLimit) {
				t.Errorf("%s/%d: hit = %v", tt.name, size, b.hit)
			}
			if _, again := b.Read(buf); err == nil && again != io.EOF || errors.Is(err, errPortLimit) && again != errPortLimit {
				t.Errorf("%s/%d: later Read = %v", tt.name, size, again)
			}
		}
	}
}

func TestPortRedirectNotFollowed(t *testing.T) {
	app := newApp(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/next", http.StatusFound)
	})
	a := newFakeAPI(t, portProtocol)
	tr := a.transport(t)
	g := newGateway(t, a, tr)
	a.servePortForward(app.Listener.Addr(), "")
	resp, err := testClient().Get(g.server.URL + "/start")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusFound || resp.Header.Get("Location") != "/next" {
		t.Fatalf("response %d %v", resp.StatusCode, resp.Header)
	}
	if s := waitSettled(t, g.frame(t).envFrame); s.Err != nil {
		t.Fatal(s.Err)
	}
	if n := len(app.requestList()); n != 1 {
		t.Fatalf("app saw %d requests", n)
	}
}

func TestPortRefusals(t *testing.T) {
	a := newFakeAPI(t, portProtocol)
	tr := a.transport(t)
	tests := []struct {
		name string
		req  func() *http.Request
		code int
	}{
		{"connect", func() *http.Request { return httptest.NewRequest(http.MethodConnect, "/", nil) }, http.StatusMethodNotAllowed},
		{"upgrade", func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.Header.Set("Connection", "Upgrade")
			r.Header.Set("Upgrade", "websocket")
			return r
		}, http.StatusNotImplemented},
		{"bareupgrade", func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.Header.Set("Upgrade", "h2c")
			return r
		}, http.StatusNotImplemented},
		{"size", func() *http.Request {
			r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("x"))
			r.ContentLength = maxPortRequestBody + 1
			return r
		}, http.StatusRequestEntityTooLarge},
		// httptest.ResponseRecorder has no deadline support. A request
		// without a body needs only the write deadline.
		{"writedeadline", func() *http.Request { return httptest.NewRequest(http.MethodGet, "/", nil) }, http.StatusInternalServerError},
		{"bodyreaddeadline", func() *http.Request {
			return httptest.NewRequest(http.MethodPost, "/", strings.NewReader("x"))
		}, http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A gateway that wrongly proceeds is ended by this context.
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			w := httptest.NewRecorder()
			p := tr.servePort(w, tt.req().WithContext(ctx), portTarget{Pod: "env-0", Port: testPort})
			s := waitSettled(t, p.envFrame)
			if w.Code != tt.code || p.result().Status != tt.code || s.Err == nil {
				t.Fatalf("code = %d status = %d err = %v; want %d", w.Code, p.result().Status, s.Err, tt.code)
			}
			if strings.HasSuffix(tt.name, "deadline") && !errors.Is(s.Err, errPortDeadline) {
				t.Fatalf("err = %v", s.Err)
			}
		})
	}
	// A body's read deadline is checked; then the write deadline is.
	t.Run("readonlydeadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		w := readDeadlineRecorder{httptest.NewRecorder()}
		r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("x")).WithContext(ctx)
		p := tr.servePort(w, r, portTarget{Pod: "env-0", Port: testPort})
		s := waitSettled(t, p.envFrame)
		if w.Code != http.StatusInternalServerError || !errors.Is(s.Err, errPortDeadline) {
			t.Fatalf("code = %d err = %v", w.Code, s.Err)
		}
	})
	if n := len(a.requestList()); n != 0 {
		t.Fatalf("api saw %d requests", n)
	}
}

// readDeadlineRecorder supports read deadlines but not write deadlines.
type readDeadlineRecorder struct {
	*httptest.ResponseRecorder
}

func (readDeadlineRecorder) SetReadDeadline(time.Time) error { return nil }

func TestPortInvalidTarget(t *testing.T) {
	tr, err := newEnvTransport(&rest.Config{Host: "https://127.0.0.1:6443"}, "workloads")
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []portTarget{{Pod: "env-0", Port: 0}, {Pod: "env-0", Port: 65536}, {Pod: "Bad", Port: 80}} {
		w := httptest.NewRecorder()
		p := tr.servePort(w, httptest.NewRequest(http.MethodGet, "/", nil), target)
		if s := waitSettled(t, p.envFrame); s.Err == nil || w.Code != http.StatusInternalServerError {
			t.Errorf("%v: code %d err %v", target, w.Code, s.Err)
		}
	}
}

func TestPortServerSentEvents(t *testing.T) {
	release := make(chan struct{})
	app := newApp(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: one\n\n")
		w.(http.Flusher).Flush()
		<-release
		io.WriteString(w, "data: two\n\n")
	})
	a := newFakeAPI(t, portProtocol)
	tr := a.transport(t)
	g := newGateway(t, a, tr)
	a.servePortForward(app.Listener.Addr(), "")
	resp, err := testClient().Get(g.server.URL + "/events")
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	type read struct {
		line string
		err  error
	}
	first := make(chan read, 1)
	go func() {
		line, err := br.ReadString('\n')
		first <- read{line, err}
	}()
	var got read
	select {
	case got = <-first:
	case <-time.After(testWait):
		close(release)
		t.Fatal("first event not delivered while the app holds the second")
	}
	close(release)
	if got.err != nil || got.line != "data: one\n" {
		t.Fatalf("first line %q, %v", got.line, got.err)
	}
	tail, _ := io.ReadAll(br)
	if string(tail) != "\ndata: two\n\n" {
		t.Fatalf("tail = %q", tail)
	}
	if s := waitSettled(t, g.frame(t).envFrame); s.Err != nil {
		t.Fatal(s.Err)
	}
}

func TestPortClientDisconnect(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	app := newApp(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	a := newFakeAPI(t, portProtocol)
	tr := a.transport(t)
	g := newGateway(t, a, tr)
	a.servePortForward(app.Listener.Addr(), "")
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, g.server.URL+"/wait", nil)
	resp, err := testClient().Do(req)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	resp.Body.Close()
	s := waitSettled(t, g.frame(t).envFrame)
	if !errors.Is(s.Err, errEnvCanceled) || !errors.Is(s.Err, errPortClient) {
		t.Fatalf("frame = %v", s.Err)
	}
}

// A failed header flush ends the exchange with that error: the Pod's
// connection is closed once, so the response body's Close cannot wait on a
// response that is still streaming.
func TestPortHeaderFlushError(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	app := newApp(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	a := newFakeAPI(t, portProtocol)
	tr := a.transport(t)
	g := newGateway(t, a, tr)
	g.failFlush.Store(true)
	a.servePortForward(app.Listener.Addr(), "")
	resp, err := testClient().Get(g.server.URL + "/wait")
	if errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("client deadline rescued header flush failure: %v", err)
	}
	if err == nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	p := g.frame(t)
	s := waitSettled(t, p.envFrame)
	if !errors.Is(s.Err, errSynthetic) || !strings.Contains(s.Err.Error(), "flush kubernetes port gateway response header") {
		t.Fatalf("frame = %v", s.Err)
	}
	p.mu.Lock()
	closed := p.conn != nil && p.connErr.done
	p.mu.Unlock()
	if !closed {
		t.Fatal("port-forward connection not closed")
	}
}

func TestPortRemoteError(t *testing.T) {
	app := newApp(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok")
	})
	a := newFakeAPI(t, portProtocol)
	tr := a.transport(t)
	g := newGateway(t, a, tr)
	a.servePortForward(app.Listener.Addr(), "unable to forward")
	resp, err := testClient().Get(g.server.URL + "/")
	if err == nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	s := waitSettled(t, g.frame(t).envFrame)
	if s.Err == nil || !strings.Contains(s.Err.Error(), "unable to forward") {
		t.Fatalf("frame = %v", s.Err)
	}
}

func TestRemoveHopHeaders(t *testing.T) {
	h := http.Header{
		"Connection":        {"close, X-A"},
		"X-A":               {"1"},
		"Keep-Alive":        {"timeout=5"},
		"Transfer-Encoding": {"chunked"},
		"Te":                {"trailers"},
		"X-Keep":            {"2"},
	}
	removeHopHeaders(h)
	if len(h) != 1 || h.Get("X-Keep") != "2" {
		t.Fatalf("headers = %v", h)
	}
}

// Under the HTTP/1 close policy every gateway response carries the literal
// Connection: close header, and a request without a body is never given a
// read deadline. This checks the policy over sequential HTTP/1 requests; it
// does not test connection reuse.
func TestPortHTTP1ClosePolicy(t *testing.T) {
	app := newApp(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok")
	})
	a := newFakeAPI(t, portProtocol)
	tr := a.transport(t)
	g := newGateway(t, a, tr)
	client := &http.Client{Timeout: testWait, Transport: &http.Transport{}}
	t.Cleanup(client.CloseIdleConnections)
	for i := range 2 {
		a.servePortForward(app.Listener.Addr(), "")
		resp, err := client.Get(g.server.URL + "/")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || string(body) != "ok" || !resp.Close {
			t.Fatalf("request %d: %d %q close=%v", i, resp.StatusCode, body, resp.Close)
		}
		if s := waitSettled(t, g.frame(t).envFrame); s.Err != nil {
			t.Fatalf("request %d: frame = %v", i, s.Err)
		}
	}
	if n := g.reads.Load(); n != 0 {
		t.Fatalf("gateway set %d read deadlines on requests without bodies", n)
	}
}

// A client that leaves after the handler ended the exchange does not abort
// or fail it; one that leaves before does.
func TestPortClientGoneOrdering(t *testing.T) {
	p := &portFrame{envFrame: newEnvFrame()}
	p.enter()
	if gone := p.end(); gone {
		t.Fatal("gone before any disconnect")
	}
	p.clientGone()
	if p.isClosing() || p.expired() {
		t.Fatal("disconnect after end aborted the frame")
	}
	p.leave(nil)

	q := &portFrame{envFrame: newEnvFrame()}
	q.enter()
	q.clientGone()
	if !q.isClosing() {
		t.Fatal("disconnect before end did not abort the frame")
	}
	if gone := q.end(); !gone {
		t.Fatal("disconnect not recorded")
	}
	q.leave(nil)
	if s := q.state(); errors.Is(s.Err, errEnvCanceled) {
		t.Fatalf("clientGone recorded a cancel itself: %v", s.Err)
	}
}
