package dashboard

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
)

// logService is a generated Connect handler whose only implemented method
// is StreamJobLogs. GetRetention is Unimplemented, so the dashboard selects
// legacy logs as the CLI does against an older server.
type logService struct {
	skyv1connect.UnimplementedControlServiceHandler
	stream func(context.Context, *connect.ServerStream[v1.StreamJobLogsResponse]) error
}

func (l *logService) StreamJobLogs(ctx context.Context, _ *connect.Request[v1.StreamJobLogsRequest], s *connect.ServerStream[v1.StreamJobLogsResponse]) error {
	return l.stream(ctx, s)
}

// live is a dashboard served on a real loopback socket in front of a
// generated handler, with a client that acts as one browser tab.
type live struct {
	*server
	base   string
	client *http.Client
	tab    *tab
	stop   func() time.Duration
}

// startLive starts a live dashboard and logs its client in.
func startLive(t *testing.T, svc *logService, adjust func(*server)) *live {
	t.Helper()
	l := startFresh(t, svc, adjust)
	l.signIn(t)
	return l
}

// startFresh starts a live dashboard that no one has logged in to, so its
// login code is unspent.
func startFresh(t *testing.T, svc *logService, adjust func(*server)) *live {
	t.Helper()
	_, handler := skyv1connect.NewControlServiceHandler(svc)
	api := httptest.NewServer(handler)
	t.Cleanup(api.Close)
	ln, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s, err := newServer(Config{Client: skyv1connect.NewControlServiceClient(api.Client(), api.URL), Timeout: 2 * time.Second}, ln.Addr().String(), time.Now, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if adjust != nil {
		adjust(s)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- s.serve(ctx, ln) }()
	var stopped bool
	l := &live{server: s, base: "http://" + s.host}
	l.stop = func() time.Duration {
		if stopped {
			return 0
		}
		stopped = true
		start := time.Now()
		cancel()
		select {
		case err := <-served:
			if err != nil {
				t.Errorf("serve: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("dashboard did not stop")
		}
		return time.Since(start)
	}
	t.Cleanup(func() { l.stop() })
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	l.tab = &tab{host: s.host}
	l.client = &http.Client{Jar: jar, Transport: l.tab, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return l
}

// signIn logs l's client in with the login code, as the dashboard's script
// does, and keeps the session proof for later requests.
func (l *live) signIn(t *testing.T) {
	t.Helper()
	req, err := http.NewRequest("POST", l.base+"/login", strings.NewReader(url.Values{"code": {l.code.value}}.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Origin", l.base)
	req.Header.Set(loginHeader, "1")
	resp, err := l.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("login = %d %v", resp.StatusCode, err)
	}
	l.tab.proof = string(proof)
}

// attempts returns the number of wrong login codes tried so far.
func (l *live) attempts() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.code.attempts
}

// tab sends the session proof to the dashboard's host only, as the
// dashboard's script does from its own origin.
type tab struct {
	host, proof string
}

func (t *tab) RoundTrip(r *http.Request) (*http.Response, error) {
	if t.proof != "" && r.URL.Host == t.host {
		r = r.Clone(r.Context())
		r.Header.Set(proofHeader, t.proof)
	}
	return http.DefaultTransport.RoundTrip(r)
}

func (l *live) text(t *testing.T, path string) string {
	t.Helper()
	resp, err := l.client.Get(l.base + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "text/plain; charset=utf-8" {
		t.Fatalf("%s = %d %s", path, resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	return string(data)
}

// raw sends a hand-written request and returns the response status line, or
// "" if the connection closed without one.
func (l *live) raw(t *testing.T, request string, stall time.Duration) (string, time.Duration) {
	t.Helper()
	conn, err := net.Dial("tcp", l.host)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	start := time.Now()
	if _, err := io.WriteString(conn, strings.ReplaceAll(request, "HOST", l.host)); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(stall))
	line, _ := bufio.NewReader(conn).ReadString('\n')
	return strings.TrimSpace(line), time.Since(start)
}

func chunk(data string) *v1.StreamJobLogsResponse {
	return &v1.StreamJobLogsResponse{Data: []byte(data)}
}

func TestSlowBody(t *testing.T) {
	l := startFresh(t, &logService{}, func(s *server) { s.bodyTimeout = 200 * time.Millisecond })
	if !l.codeAvailable() || l.attempts() != 0 {
		t.Fatal("fresh dashboard has no usable login code")
	}
	// A POST body that stops arriving is cut off at the body deadline and
	// costs no login attempt.
	login := "POST /login HTTP/1.1\r\nHost: HOST\r\nOrigin: http://HOST\r\nX-Skygo-Login: 1\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: 40\r\n\r\ncode="
	status, elapsed := l.raw(t, login, 5*time.Second)
	if !strings.HasPrefix(status, "HTTP/1.1 400") || elapsed > 3*time.Second {
		t.Errorf("stalled login body: %q after %v", status, elapsed)
	}
	if !l.codeAvailable() || l.attempts() != 0 {
		t.Error("stalled login body counted as an attempt")
	}
	// A GET that declares a body is refused without reading it.
	status, elapsed = l.raw(t, "GET /jobs HTTP/1.1\r\nHost: HOST\r\nContent-Length: 1000\r\n\r\n", 5*time.Second)
	if !strings.HasPrefix(status, "HTTP/1.1 400") || elapsed > time.Second {
		t.Errorf("GET with body: %q after %v", status, elapsed)
	}
	status, _ = l.raw(t, "POST /logout HTTP/1.1\r\nHost: HOST\r\nOrigin: http://HOST\r\nContent-Length: 4096\r\n\r\n", 5*time.Second)
	if !strings.HasPrefix(status, "HTTP/1.1 413") {
		t.Errorf("oversized POST: %q", status)
	}
	// Slow headers end at ReadHeaderTimeout with no response.
	status, elapsed = l.raw(t, "GET /jobs HTTP/1.1\r\nHost: HOST\r\n", 10*time.Second)
	if status != "" && !strings.HasPrefix(status, "HTTP/1.1 408") || elapsed > 8*time.Second {
		t.Errorf("stalled headers: %q after %v", status, elapsed)
	}
	// The code still logs in.
	l.signIn(t)
}

// TestFollowOutlivesBodyDeadline checks that no connection deadline cancels
// a stream: output keeps arriving well past the body deadline.
func TestFollowOutlivesBodyDeadline(t *testing.T) {
	svc := &logService{stream: func(ctx context.Context, s *connect.ServerStream[v1.StreamJobLogsResponse]) error {
		for _, data := range []string{"a", "b", "c"} {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(300 * time.Millisecond):
			}
			c := chunk(data)
			c.Complete = data == "c"
			if err := s.Send(c); err != nil {
				return err
			}
		}
		return nil
	}}
	l := startLive(t, svc, func(s *server) { s.bodyTimeout = 100 * time.Millisecond })
	if got := l.text(t, "/jobs/abc/logs.txt?follow=1"); got != "abc" {
		t.Errorf("follow = %q, want %q", got, "abc")
	}
}

func TestFollowLimitDuration(t *testing.T) {
	svc := &logService{stream: func(ctx context.Context, s *connect.ServerStream[v1.StreamJobLogsResponse]) error {
		if err := s.Send(chunk("a")); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	}}
	l := startLive(t, svc, func(s *server) { s.followLimit = 300 * time.Millisecond })
	start := time.Now()
	got := l.text(t, "/jobs/abc/logs.txt?follow=1")
	if want := "a\n-- stream ended: deadline_exceeded --\n"; got != want {
		t.Errorf("follow = %q, want %q", got, want)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("follow lasted %v", elapsed)
	}
}

func TestResponseCap(t *testing.T) {
	part := strings.Repeat("x", 600<<10)
	svc := &logService{stream: func(ctx context.Context, s *connect.ServerStream[v1.StreamJobLogsResponse]) error {
		for range 3 {
			if err := s.Send(chunk(part)); err != nil {
				return err
			}
		}
		return nil
	}}
	l := startLive(t, svc, nil)
	got := l.text(t, "/jobs/abc/logs.txt")
	body, marker, ok := strings.Cut(got, "\n-- response limit reached --\n")
	if !ok || marker != "" || len(body) != maxLogText || strings.Trim(body, "x") != "" {
		t.Errorf("capped response: %d bytes, marker %v, trailing %q", len(body), ok, marker)
	}
}

func TestShutdownCancelsFollow(t *testing.T) {
	entered := make(chan struct{})
	svc := &logService{stream: func(ctx context.Context, s *connect.ServerStream[v1.StreamJobLogsResponse]) error {
		if err := s.Send(chunk("a")); err != nil {
			return err
		}
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}}
	l := startLive(t, svc, nil)
	resp, err := l.client.Get(l.base + "/jobs/abc/logs.txt?follow=1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	<-entered
	body := make(chan []byte, 1)
	go func() {
		data, _ := io.ReadAll(resp.Body)
		body <- data
	}()
	if elapsed := l.stop(); elapsed > 2*time.Second {
		t.Errorf("shutdown with an open follow took %v", elapsed)
	}
	select {
	case data := <-body:
		if !bytes.HasPrefix(data, []byte("a\n-- stream ended: ")) {
			t.Errorf("follow body = %q", data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("follow did not end")
	}
}

// held sends a request whose body never fully arrives and reads until the
// server closes the connection. It reports the status line and whether the
// connection closed within wait.
func (l *live) held(t *testing.T, request string, wait time.Duration) (string, bool) {
	t.Helper()
	conn, err := net.Dial("tcp", l.host)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, strings.ReplaceAll(request, "HOST", l.host)); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(wait))
	data, err := io.ReadAll(conn)
	status, _, _ := strings.Cut(string(data), "\r\n")
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return status, false
	}
	return status, true
}

// TestBodyDrain holds request bodies open on refused and ordinary requests.
// net/http drains an unread body after the handler returns; the read
// deadline set before any refusal must end that drain and close the
// connection, and nothing may stay pinned at shutdown.
func TestBodyDrain(t *testing.T) {
	l := startLive(t, &logService{}, func(s *server) { s.bodyTimeout = 200 * time.Millisecond })
	tests := []struct {
		name, request, status string
	}{
		{"hostile host", "GET /jobs HTTP/1.1\r\nHost: evil.example\r\nContent-Length: 10\r\n\r\nab", "HTTP/1.1 421"},
		{"hostile host bodiless method", "POST /logout HTTP/1.1\r\nHost: 127.0.0.1:1\r\nContent-Length: 10\r\n\r\n", "HTTP/1.1 421"},
		{"hostile origin", "POST /logout HTTP/1.1\r\nHost: HOST\r\nOrigin: http://evil.example\r\nContent-Length: 10\r\n\r\nab", "HTTP/1.1 403"},
		{"unsupported method", "PUT /jobs HTTP/1.1\r\nHost: HOST\r\nContent-Length: 10\r\n\r\n", "HTTP/1.1 405"},
		{"options asterisk", "OPTIONS * HTTP/1.1\r\nHost: HOST\r\nContent-Length: 4000\r\n\r\nab", "HTTP/1.1 405"},
		{"get with body", "GET /jobs HTTP/1.1\r\nHost: HOST\r\nContent-Length: 10\r\n\r\nab", "HTTP/1.1 400"},
		{"chunked get", "GET /jobs/abc/logs.txt?follow=1 HTTP/1.1\r\nHost: HOST\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nab\r\n", "HTTP/1.1 400"},
		{"same-origin logout", "POST /logout HTTP/1.1\r\nHost: HOST\r\nOrigin: http://HOST\r\nContent-Length: 10\r\n\r\nab", "HTTP/1.1 400"},
		{"login", "POST /login HTTP/1.1\r\nHost: HOST\r\nOrigin: http://HOST\r\nContent-Length: 10\r\n\r\nab", "HTTP/1.1 400"},
	}
	for _, tt := range tests {
		start := time.Now()
		status, closed := l.held(t, tt.request, 3*time.Second)
		if !strings.HasPrefix(status, tt.status) || !closed {
			t.Errorf("%s: status %q, closed %v after %v; want %s and a closed connection", tt.name, status, closed, time.Since(start), tt.status)
		}
	}
	if elapsed := l.stop(); elapsed > 2*time.Second {
		t.Errorf("shutdown after held bodies took %v", elapsed)
	}
}

func TestLogMarkers(t *testing.T) {
	half := strings.Repeat("y", maxLogText/2)
	tests := []struct {
		name   string
		chunks []*v1.StreamJobLogsResponse
		want   string
	}{
		{"complete", []*v1.StreamJobLogsResponse{{Data: []byte("a")}, {Data: []byte("b"), Complete: true}}, "ab"},
		{"snapshot of a running job", []*v1.StreamJobLogsResponse{{Data: []byte("ab")}, {}}, "ab\n-- output incomplete: the job may write more --\n"},
		{"exactly the limit, truncated", []*v1.StreamJobLogsResponse{{Data: []byte(half)}, {Data: []byte(half), Truncated: true, Complete: true}}, half + half + "\n-- output truncated: retained output limit reached --\n"},
		{"truncated and incomplete", []*v1.StreamJobLogsResponse{{Data: []byte("a"), Truncated: true}}, "a\n-- output truncated: retained output limit reached --\n-- output incomplete: the job may write more --\n"},
	}
	for _, tt := range tests {
		svc := &logService{stream: func(ctx context.Context, s *connect.ServerStream[v1.StreamJobLogsResponse]) error {
			for _, c := range tt.chunks {
				if err := s.Send(c); err != nil {
					return err
				}
			}
			return nil
		}}
		l := startLive(t, svc, nil)
		if got := l.text(t, "/jobs/abc/logs.txt"); got != tt.want {
			if len(got) > 200 {
				got = "..." + got[len(got)-200:]
			}
			t.Errorf("%s: logs.txt ends %q", tt.name, got)
		}
		l.stop()
	}
}

// TestStalledBodiesKeepSlots fills the server with more stalled POST bodies
// than it has request slots. Reading a body happens before a slot is taken,
// so the login page and an authenticated read still succeed meanwhile.
func TestStalledBodiesKeepSlots(t *testing.T) {
	svc := &logService{stream: func(ctx context.Context, s *connect.ServerStream[v1.StreamJobLogsResponse]) error {
		return s.Send(&v1.StreamJobLogsResponse{Data: []byte("ok"), Complete: true})
	}}
	l := startLive(t, svc, func(s *server) { s.bodyTimeout = 3 * time.Second })
	var conns []net.Conn
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	for _, path := range []string{"/login", "/logout", "/jobs"} {
		for range maxRequests {
			c, err := net.Dial("tcp", l.host)
			if err != nil {
				t.Fatal(err)
			}
			conns = append(conns, c)
			req := "POST " + path + " HTTP/1.1\r\nHost: " + l.host + "\r\nOrigin: http://" + l.host + "\r\nX-Skygo-Login: 1\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: 1000\r\n\r\n"
			if _, err := io.WriteString(c, req); err != nil {
				t.Fatal(err)
			}
		}
	}
	time.Sleep(300 * time.Millisecond) // let the server read every header
	resp, err := http.Get(l.base + "/login")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("login page with stalled bodies = %d", resp.StatusCode)
	}
	if got := l.text(t, "/jobs/abc/logs.txt"); got != "ok" {
		t.Errorf("authenticated read with stalled bodies = %q", got)
	}
}

func TestExpectContinue(t *testing.T) {
	l := startFresh(t, &logService{}, func(s *server) { s.bodyTimeout = 5 * time.Second })
	if !l.codeAvailable() || l.attempts() != 0 {
		t.Fatal("fresh dashboard has no usable login code")
	}
	for _, path := range []string{"/login", "/logout"} {
		start := time.Now()
		status, closed := l.held(t, "POST "+path+" HTTP/1.1\r\nHost: HOST\r\nOrigin: http://HOST\r\nX-Skygo-Login: 1\r\nExpect: 100-continue\r\nContent-Length: 10\r\n\r\n", 3*time.Second)
		if elapsed := time.Since(start); !strings.HasPrefix(status, "HTTP/1.1 417") || !closed || elapsed > time.Second {
			t.Errorf("%s with Expect: %q, closed %v, after %v", path, status, closed, elapsed)
		}
	}
	if !l.codeAvailable() || l.attempts() != 0 {
		t.Error("Expect request spent the login code or counted as an attempt")
	}
	// The code still logs in.
	l.signIn(t)
}
