package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/tmc/skygo/internal/environment"
)

// forwardSeen is what the fixture route received.
type forwardSeen struct {
	path, rawPath, query, auth, proxyAuth string
}

// startForward serves a forward of environment id to upstream with token.
// The returned stop cancels it and waits for serveForward to return.
func startForward(t *testing.T, upstream, id, token string) (base string, stop func() error) {
	t.Helper()
	target, err := forwardURL(upstream, id)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveForward(ctx, ln, target, token, io.Discard) }()
	var once sync.Once
	var stopErr error
	stop = func() error {
		once.Do(func() {
			cancel()
			stopErr = <-done
		})
		return stopErr
	}
	t.Cleanup(func() { stop() })
	return "http://" + ln.Addr().String(), stop
}

func TestServeForward(t *testing.T) {
	var mu sync.Mutex
	var seen []forwardSeen
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, forwardSeen{r.URL.Path, r.URL.RawPath, r.URL.RawQuery, r.Header.Get("Authorization"), r.Header.Get("Proxy-Authorization")})
		mu.Unlock()
		if strings.HasSuffix(r.URL.Path, "/redirect") {
			http.Redirect(w, r, "http://example.invalid/", http.StatusFound)
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer upstream.Close()
	id := strings.Repeat("a", 32)
	base, stop := startForward(t, upstream.URL, id, "fwd-token")
	prefix := "/environments/" + id + "/forward"
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for _, tc := range []struct {
		name   string
		path   string
		header map[string]string
		status int
		want   *forwardSeen
	}{
		{"root", "/", nil, http.StatusOK, &forwardSeen{path: prefix + "/"}},
		{"query", "/a/b?x=1&y=%2F", nil, http.StatusOK, &forwardSeen{path: prefix + "/a/b", query: "x=1&y=%2F"}},
		{"escaped", "/a%2Fb", nil, http.StatusOK, &forwardSeen{path: prefix + "/a/b", rawPath: prefix + "/a%2Fb"}},
		{"credentials", "/c", map[string]string{"Authorization": "Bearer local", "Proxy-Authorization": "Basic eDp5"}, http.StatusOK, &forwardSeen{path: prefix + "/c"}},
		{"redirect", "/redirect", nil, http.StatusFound, &forwardSeen{path: prefix + "/redirect"}},
		{"upgrade", "/ws", map[string]string{"Connection": "Upgrade", "Upgrade": "websocket"}, http.StatusNotImplemented, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mu.Lock()
			seen = nil
			mu.Unlock()
			req, err := http.NewRequest("GET", base+tc.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			for k, v := range tc.header {
				req.Header.Set(k, v)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != tc.status {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.status)
			}
			mu.Lock()
			defer mu.Unlock()
			if tc.want == nil {
				if len(seen) != 0 {
					t.Fatalf("relayed %v, want nothing", seen)
				}
				return
			}
			want := *tc.want
			want.auth = "Bearer fwd-token"
			if len(seen) != 1 || seen[0] != want {
				t.Fatalf("relayed %+v, want %+v", seen, want)
			}
		})
	}

	// CONNECT is refused, not tunneled.
	conn, err := net.Dial("tcp", strings.TrimPrefix(base, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(conn, "CONNECT example.invalid:443 HTTP/1.1\r\nHost: example.invalid:443\r\n\r\n")
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("CONNECT status = %d, want 501", resp.StatusCode)
	}

	if err := stop(); err != nil {
		t.Fatalf("stop = %v", err)
	}
	if c, err := net.Dial("tcp", strings.TrimPrefix(base, "http://")); err == nil {
		c.Close()
		t.Fatal("listener still accepts after stop")
	}
}

// TestServeForwardDrain checks that stopping waits for an exchange in
// progress to finish instead of cutting it off.
func TestServeForwardDrain(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		fmt.Fprint(w, "late")
	}))
	defer upstream.Close()
	base, stop := startForward(t, upstream.URL, strings.Repeat("b", 32), "fwd-token")
	type result struct {
		body string
		err  error
	}
	got := make(chan result, 1)
	go func() {
		resp, err := http.Get(base + "/slow")
		if err != nil {
			got <- result{err: err}
			return
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		got <- result{string(b), err}
	}()
	<-entered
	stopped := make(chan error, 1)
	go func() { stopped <- stop() }()
	// Shutdown closes the listener first; once a dial fails it is draining.
	for {
		c, err := net.Dial("tcp", strings.TrimPrefix(base, "http://"))
		if err != nil {
			break
		}
		c.Close()
	}
	select {
	case err := <-stopped:
		t.Fatalf("stop returned %v before the exchange finished", err)
	default:
	}
	close(release)
	r := <-got
	if r.err != nil || r.body != "late" {
		t.Fatalf("exchange = %q, %v; want late", r.body, r.err)
	}
	if err := <-stopped; err != nil {
		t.Fatalf("stop = %v", err)
	}
}

// forwardHeldTransport models an upstream exchange that ignores its
// request's cancellation: RoundTrip closes entered and returns only after
// release is closed. ReverseProxy closes the response body before its
// handler returns, so finished is set before the exchange completes.
type forwardHeldTransport struct {
	entered, release chan struct{}
	finished         atomic.Bool
}

func newForwardHeldTransport() *forwardHeldTransport {
	return &forwardHeldTransport{entered: make(chan struct{}), release: make(chan struct{})}
}

func (h *forwardHeldTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	close(h.entered)
	<-h.release
	return &http.Response{
		StatusCode:    http.StatusOK,
		Header:        make(http.Header),
		Body:          forwardHeldBody{strings.NewReader("late"), h},
		ContentLength: -1,
		Request:       r,
	}, nil
}

type forwardHeldBody struct {
	io.Reader
	h *forwardHeldTransport
}

func (b forwardHeldBody) Close() error {
	b.h.finished.Store(true)
	return nil
}

// forwardStopped is what relayForward returned, and whether the held
// exchange had finished at that moment.
type forwardStopped struct {
	err      error
	finished bool
}

// startHeldForward runs relayForward on ln through h and issues one
// request. The returned channels report relayForward's return and the
// local client's outcome.
func startHeldForward(t *testing.T, ctx context.Context, ln net.Listener, h *forwardHeldTransport, drain time.Duration) (done <-chan forwardStopped, failed <-chan error) {
	t.Helper()
	target, err := forwardURL("http://upstream.invalid", strings.Repeat("d", 32))
	if err != nil {
		t.Fatal(err)
	}
	d := make(chan forwardStopped, 1)
	go func() {
		err := relayForward(ctx, ln, target, "fwd-token", io.Discard, h, drain)
		d <- forwardStopped{err, h.finished.Load()}
	}()
	f := make(chan error, 1)
	go func() {
		client := &http.Client{Transport: &http.Transport{}}
		resp, err := client.Get("http://" + ln.Addr().String() + "/held")
		if err == nil {
			_, err = io.ReadAll(resp.Body)
			resp.Body.Close()
			if err == nil {
				err = fmt.Errorf("status %d", resp.StatusCode)
			}
		}
		f <- err
	}()
	return d, f
}

// TestServeForwardJoinsClosedExchange holds an exchange past the drain
// bound, so Shutdown times out and Close drops the local connection, and
// checks that relayForward returns only after the held exchange finishes.
// The check before release is a snapshot; the deterministic check is that
// the exchange had finished when relayForward returned.
func TestServeForwardJoinsClosedExchange(t *testing.T) {
	h := newForwardHeldTransport()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done, failed := startHeldForward(t, ctx, ln, h, time.Millisecond)
	<-h.entered
	cancel()
	// The handler cannot return while held, so Shutdown cannot finish and
	// only Close can end the local exchange.
	select {
	case s := <-done:
		t.Fatalf("relayForward returned %v while the exchange was held", s.err)
	case <-failed:
	}
	select {
	case s := <-done:
		t.Fatalf("relayForward returned %v after closing, while the exchange was held", s.err)
	default:
	}
	close(h.release)
	s := <-done
	if s.err != nil {
		t.Fatalf("relayForward = %v", s.err)
	}
	if !s.finished {
		t.Fatal("relayForward returned before the held exchange finished")
	}
}

var errForwardBroken = errors.New("listener broken")

// forwardBrokenListener accepts one connection, then fails once broken is
// closed.
type forwardBrokenListener struct {
	net.Listener
	accepted bool
	broken   chan struct{}
}

func (l *forwardBrokenListener) Accept() (net.Conn, error) {
	if !l.accepted {
		l.accepted = true
		return l.Listener.Accept()
	}
	<-l.broken
	return nil, errForwardBroken
}

// TestServeForwardJoinsAfterServeError breaks the listener while an
// exchange is held and checks that relayForward closes the connection,
// waits for the exchange, and returns the serving error. Until release,
// the handler cannot return, so relayForward cannot either; the first
// select is decided by whichever of the two can happen.
func TestServeForwardJoinsAfterServeError(t *testing.T) {
	h := newForwardHeldTransport()
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln := &forwardBrokenListener{Listener: inner, broken: make(chan struct{})}
	done, failed := startHeldForward(t, context.Background(), ln, h, forwardDrain)
	<-h.entered
	close(ln.broken)
	select {
	case s := <-done:
		t.Fatalf("relayForward returned %v while the exchange was held", s.err)
	case <-failed:
	}
	close(h.release)
	s := <-done
	if !errors.Is(s.err, errForwardBroken) {
		t.Fatalf("relayForward = %v, want %v", s.err, errForwardBroken)
	}
	if !s.finished {
		t.Fatal("relayForward returned before the held exchange finished")
	}
}

func TestCheckLoopback(t *testing.T) {
	for _, tc := range []struct {
		addr string
		ok   bool
	}{
		{"127.0.0.1:0", true},
		{"127.0.0.2:8080", true},
		{"[::1]:0", true},
		{"localhost:0", false},
		{"0.0.0.0:0", false},
		{"[::]:0", false},
		{"192.0.2.1:0", false},
		{":0", false},
		{"127.0.0.1", false},
	} {
		if err := checkLoopback(tc.addr); (err == nil) != tc.ok {
			t.Errorf("checkLoopback(%q) = %v, want ok %t", tc.addr, err, tc.ok)
		}
	}
}

func TestForwardURL(t *testing.T) {
	id := strings.Repeat("c", 32)
	for _, tc := range []struct {
		endpoint, want string
	}{
		{"http://127.0.0.1:8080", "http://127.0.0.1:8080/environments/" + id + "/forward"},
		{"https://sky.example/base/", "https://sky.example/base/environments/" + id + "/forward"},
		{"ftp://sky.example", ""},
		{"http://sky.example?x=1", ""},
		{"http://user@sky.example", ""},
		{"sky.example:8080", ""},
	} {
		u, err := forwardURL(tc.endpoint, id)
		if tc.want == "" {
			if err == nil {
				t.Errorf("forwardURL(%q) = %v, want error", tc.endpoint, u)
			}
			continue
		}
		if err != nil || u.String() != tc.want {
			t.Errorf("forwardURL(%q) = %v, %v; want %s", tc.endpoint, u, err, tc.want)
		}
	}
}

// TestEnvironmentForwardRefusals checks the refusals made before any
// listener starts.
func TestEnvironmentForwardRefusals(t *testing.T) {
	srv := serveEnvironments(t, &envInfo{epoch: 1}, &envService{})
	ok := serveEnvironments(t, environmentsInfo(), &envService{})
	t.Setenv("SKYGO_TOKEN", "")
	id := strings.Repeat("1", 32)
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"bad_id", []string{"--endpoint", ok.URL, "env", "forward", "ABC"}, "32 lowercase hexadecimal"},
		{"not_loopback", []string{"--endpoint", ok.URL, "env", "forward", id, "--listen", "0.0.0.0:0"}, "loopback IP address"},
		{"no_capability", []string{"--endpoint", srv.URL, "env", "forward", id}, "environments-v1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := execute(context.Background(), tc.args...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
			if tc.name == "no_capability" && connect.CodeOf(err) != connect.CodeFailedPrecondition {
				t.Fatalf("code = %v, want failed_precondition", connect.CodeOf(err))
			}
		})
	}
}

// TestEnvironmentForwardEngineRefusals relays through the CLI's forward
// listener to the real forward route of control-auth's engine fixture
// (environment_engine_test.go). A dot segment, plain or escaped, is refused
// by the route with 404 and no redirect, and an upgrade is refused by the
// listener; none reaches the modeled runtime, whose answer names the port.
func TestEnvironmentForwardEngineRefusals(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("real retention filesystem sampling requires Linux or Darwin")
	}
	f := newEnvironmentEngine(t)
	st := f.current()
	info, err := st.store.RetentionInfo(st.actor)
	if err != nil {
		t.Fatal(err)
	}
	spec := environment.Spec{Name: "web", Target: engineTarget, Image: "task:v1", CPUMillis: 100, MemoryBytes: 1 << 20, Port: environment.Port}
	v, err := st.controller.Create(st.actor, info.Domain, "refusals-1", spec)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := st.controller.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	token, err := os.ReadFile(f.tokenFile)
	if err != nil {
		t.Fatal(err)
	}
	base, _ := startForward(t, f.server.URL, v.ID, strings.TrimSpace(string(token)))
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for _, tc := range []struct {
		name   string
		path   string
		header map[string]string
		status int
		body   string // the whole body when status is 200
	}{
		{"relayed", "/ok", nil, http.StatusOK, fmt.Sprintf("GET /ok? port=%d authorization=false\n", environment.Port)},
		{"dot_dot", "/a/../b", nil, http.StatusNotFound, ""},
		{"dot", "/a/./b", nil, http.StatusNotFound, ""},
		{"escaped_dot_dot", "/%2e%2e/x", nil, http.StatusNotFound, ""},
		{"upgrade", "/ws", map[string]string{"Connection": "Upgrade", "Upgrade": "websocket"}, http.StatusNotImplemented, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+tc.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			for k, v := range tc.header {
				req.Header.Set(k, v)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			b, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != tc.status {
				t.Fatalf("status = %d %q, want %d", resp.StatusCode, b, tc.status)
			}
			if loc := resp.Header.Get("Location"); loc != "" {
				t.Fatalf("Location = %q, want none", loc)
			}
			if tc.status == http.StatusOK {
				if string(b) != tc.body {
					t.Fatalf("body = %q, want %q", b, tc.body)
				}
			} else if strings.Contains(string(b), "port=") {
				t.Fatalf("body = %q: refused request reached the runtime", b)
			}
		})
	}
}
