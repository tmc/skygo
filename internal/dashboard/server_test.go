package dashboard

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const testHost = "127.0.0.1:4321"

// fakeReader answers unary reads from fixed messages and counts every call.
// Its streams always fail, since a client stream cannot be built without a
// server; the external tests cover streaming against the real server.
type fakeReader struct {
	calls atomic.Int32
	jobs  []*v1.Job
	job   *v1.Job
	err   error

	retentionCalls atomic.Int32
	retention      *v1.GetRetentionResponse
	retentionErr   error
}

func (f *fakeReader) call() error { f.calls.Add(1); return f.err }

func (f *fakeReader) GetServerInfo(context.Context, *connect.Request[v1.GetServerInfoRequest]) (*connect.Response[v1.GetServerInfoResponse], error) {
	if err := f.call(); err != nil {
		return nil, err
	}
	return connect.NewResponse(&v1.GetServerInfoResponse{Capabilities: []v1.Capability{v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1}}), nil
}

func (f *fakeReader) ListTargets(context.Context, *connect.Request[v1.ListTargetsRequest]) (*connect.Response[v1.ListTargetsResponse], error) {
	if err := f.call(); err != nil {
		return nil, err
	}
	return connect.NewResponse(&v1.ListTargetsResponse{Targets: []*v1.Target{{Name: `t"><script>alert(1)</script>`, Kind: "local", Labels: map[string]string{"b": "2", "a": "1"}}}}), nil
}

func (f *fakeReader) GetRetention(context.Context, *connect.Request[v1.GetRetentionRequest]) (*connect.Response[v1.GetRetentionResponse], error) {
	f.retentionCalls.Add(1)
	if err := f.call(); err != nil {
		return nil, err
	}
	if f.retentionErr != nil {
		return nil, f.retentionErr
	}
	if f.retention != nil {
		return connect.NewResponse(f.retention), nil
	}
	return connect.NewResponse(&v1.GetRetentionResponse{}), nil
}

func (f *fakeReader) ListJobs(context.Context, *connect.Request[v1.ListJobsRequest]) (*connect.Response[v1.ListJobsResponse], error) {
	if err := f.call(); err != nil {
		return nil, err
	}
	return connect.NewResponse(&v1.ListJobsResponse{Jobs: f.jobs, NextPageToken: "next&token=<x>"}), nil
}

func (f *fakeReader) GetJob(context.Context, *connect.Request[v1.GetJobRequest]) (*connect.Response[v1.GetJobResponse], error) {
	if err := f.call(); err != nil {
		return nil, err
	}
	return connect.NewResponse(&v1.GetJobResponse{Job: f.job}), nil
}

func (f *fakeReader) StreamJobLogs(context.Context, *connect.Request[v1.StreamJobLogsRequest]) (*connect.ServerStreamForClient[v1.StreamJobLogsResponse], error) {
	f.calls.Add(1)
	return nil, connect.NewError(connect.CodeUnavailable, errors.New("no stream"))
}

func (f *fakeReader) StreamRetainedJobLogs(context.Context, *connect.Request[v1.StreamRetainedJobLogsRequest]) (*connect.ServerStreamForClient[v1.StreamRetainedJobLogsResponse], error) {
	f.calls.Add(1)
	return nil, connect.NewError(connect.CodeUnavailable, errors.New("no stream"))
}

// testServer is a server with a settable clock and known random bytes.
type testServer struct {
	*server
	clock time.Time
}

func newTestServer(t *testing.T, r Reader) *testServer {
	t.Helper()
	ts := &testServer{clock: time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)}
	s, err := newServer(Config{Client: r, Timeout: time.Second}, testHost, func() time.Time { return ts.clock }, bytes.NewReader(sequence(1024)))
	if err != nil {
		t.Fatal(err)
	}
	ts.server = s
	return ts
}

// sequence returns n bytes counting up from 0, so that the login code and
// the session token drawn from it differ.
func sequence(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

// creds are what a logged-in tab holds: the cookie and the session proof.
type creds struct {
	cookie *http.Cookie
	proof  string
}

// do serves one request. POSTs carry the dashboard origin; a carries the
// cookie and, if set, the proof header.
func (ts *testServer) do(t *testing.T, method, target string, body url.Values, a *creds) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body != nil {
		r = httptest.NewRequest(method, target, strings.NewReader(body.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	} else {
		r = httptest.NewRequest(method, target, nil)
	}
	r.Host = testHost
	if method == http.MethodPost {
		r.Header.Set("Origin", "http://"+testHost)
	}
	if a != nil && a.cookie != nil {
		r.AddCookie(a.cookie)
	}
	if a != nil && a.proof != "" {
		r.Header.Set(proofHeader, a.proof)
	}
	w := httptest.NewRecorder()
	ts.ServeHTTP(w, r)
	return w
}

// login posts code as the dashboard's script does and returns the new
// credentials, or nil.
func (ts *testServer) login(t *testing.T, code string) (*httptest.ResponseRecorder, *creds) {
	t.Helper()
	r := httptest.NewRequest("POST", "/login", strings.NewReader(url.Values{"code": {code}}.Encode()))
	r.Host = testHost
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Set("Origin", "http://"+testHost)
	r.Header.Set(loginHeader, "1")
	w := httptest.NewRecorder()
	ts.ServeHTTP(w, r)
	for _, c := range w.Result().Cookies() {
		if c.Name == ts.server.cookie && c.Value != "" && w.Code == http.StatusOK {
			return w, &creds{cookie: c, proof: w.Body.String()}
		}
	}
	return w, nil
}

func (ts *testServer) session(t *testing.T) *creds {
	t.Helper()
	w, c := ts.login(t, ts.code.value)
	if c == nil {
		t.Fatalf("login failed: %d %s", w.Code, w.Body)
	}
	return c
}

func TestLogin(t *testing.T) {
	ts := newTestServer(t, &fakeReader{})
	if len(ts.code.value) != 32 {
		t.Fatalf("code %q is not 128 bits of hex", ts.code.value)
	}
	if w := ts.do(t, "GET", "/jobs", nil, nil); w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/login" {
		t.Errorf("unauthenticated /jobs = %d %q", w.Code, w.Header().Get("Location"))
	}
	if w := ts.do(t, "GET", "/login?code="+ts.code.value, nil, nil); w.Code != http.StatusOK || len(w.Result().Cookies()) != 0 {
		t.Errorf("GET /login?code= = %d with cookies %v", w.Code, w.Result().Cookies())
	}
	// A plain form post, without the script's header, is refused unspent.
	if w := ts.do(t, "POST", "/login", url.Values{"code": {ts.code.value}}, nil); w.Code != http.StatusBadRequest || len(w.Result().Cookies()) != 0 {
		t.Errorf("login without %s = %d", loginHeader, w.Code)
	}
	// The code is read from the body only, never the URL.
	r := httptest.NewRequest("POST", "/login?code="+ts.code.value, strings.NewReader(""))
	r.Host = testHost
	r.Header.Set("Origin", "http://"+testHost)
	r.Header.Set(loginHeader, "1")
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	ts.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest || len(w.Result().Cookies()) != 0 {
		t.Errorf("code in POST URL = %d, want 400", w.Code)
	}
	if !ts.codeAvailable() {
		t.Fatal("refused login spent the code")
	}
	w, c := ts.login(t, ts.code.value)
	if c == nil || w.Code != http.StatusOK || !strings.HasPrefix(w.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("login = %d %q", w.Code, w.Header().Get("Content-Type"))
	}
	k := c.cookie
	if k.Name != "skygo_dashboard_4321" || !k.HttpOnly || k.SameSite != http.SameSiteStrictMode || k.Path != "/" || k.MaxAge != 0 || !k.Expires.IsZero() || len(k.Value) != 64 || strings.Contains(k.Value, ts.code.value) {
		t.Errorf("session cookie = %+v", k)
	}
	if len(c.proof) != 64 || c.proof == k.Value || strings.Contains(c.proof, ts.code.value) {
		t.Errorf("session proof %q is not a distinct 256-bit secret", c.proof)
	}
	if w := ts.do(t, "GET", "/jobs", nil, c); w.Code != http.StatusOK {
		t.Errorf("/jobs with session = %d", w.Code)
	}
	if w, c2 := ts.login(t, ts.code.value); c2 != nil || w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "no longer valid") {
		t.Errorf("replayed code = %d %v", w.Code, c2)
	}
	if w := ts.do(t, "GET", "/jobs", nil, &creds{cookie: &http.Cookie{Name: k.Name, Value: strings.Repeat("0", 64)}, proof: c.proof}); w.Code != http.StatusForbidden || strings.Contains(w.Body.String(), "secret") {
		t.Errorf("foreign cookie with live proof = %d, want 403", w.Code)
	}
	if w := ts.do(t, "POST", "/logout", url.Values{}, c); w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/login" {
		t.Errorf("logout = %d", w.Code)
	}
	if w := ts.do(t, "GET", "/jobs", nil, c); w.Code != http.StatusUnauthorized {
		t.Errorf("credentials after logout = %d", w.Code)
	}
	if w := ts.do(t, "GET", "/jobs", nil, &creds{cookie: k}); w.Code != http.StatusSeeOther {
		t.Errorf("cookie after logout = %d", w.Code)
	}
}

// TestSessionProof checks that the cookie alone, as another local server
// could receive it, authorizes no data, no log stream and no log out.
func TestSessionProof(t *testing.T) {
	f := &fakeReader{jobs: []*v1.Job{{Id: "secret-job"}}, job: &v1.Job{Id: "secret-job"}}
	ts := newTestServer(t, f)
	c := ts.session(t)
	cookieOnly := &creds{cookie: c.cookie}
	for _, path := range []string{"/jobs", "/jobs/abc", "/jobs/abc/logs", "/jobs/abc/logs.txt", "/jobs/abc/logs.txt?follow=1", "/targets", "/info"} {
		w := ts.do(t, "GET", path, nil, cookieOnly)
		body := w.Body.String()
		if w.Code != http.StatusOK || !strings.Contains(body, `id="view"`) || !strings.HasPrefix(w.Header().Get("Content-Type"), "text/html") || strings.Contains(body, "secret-job") {
			t.Errorf("cookie-only %s = %d %s:\n%s", path, w.Code, w.Header().Get("Content-Type"), body)
		}
		for _, wrong := range []*creds{{cookie: c.cookie, proof: strings.Repeat("0", 64)}, {cookie: c.cookie, proof: c.cookie.Value}} {
			if w := ts.do(t, "GET", path, nil, wrong); w.Code != http.StatusUnauthorized || strings.Contains(w.Body.String(), "secret-job") {
				t.Errorf("%s with cookie and proof %q = %d", path, wrong.proof, w.Code)
			}
		}
		// A live proof without the cookie is refused with 403, not 401,
		// so the script keeps the proof.
		if w := ts.do(t, "GET", path, nil, &creds{proof: c.proof}); w.Code != http.StatusForbidden || strings.Contains(w.Body.String(), "secret-job") {
			t.Errorf("%s with proof alone = %d", path, w.Code)
		}
	}
	if n := f.calls.Load(); n != 0 {
		t.Errorf("requests without the proof made %d RPCs", n)
	}
	if w := ts.do(t, "POST", "/logout", url.Values{}, cookieOnly); w.Code != http.StatusForbidden {
		t.Errorf("cookie-only logout = %d", w.Code)
	}
	for _, path := range []string{"/jobs", "/jobs/secret-job"} {
		if w := ts.do(t, "GET", path, nil, c); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "secret-job") || strings.Contains(w.Body.String(), `id="view"`) {
			t.Errorf("%s with cookie and proof = %d:\n%s", path, w.Code, w.Body)
		}
	}
	// A same-named cookie planted by another port, sent first, does not
	// hide the real one.
	r := httptest.NewRequest("GET", "/jobs", nil)
	r.Host = testHost
	r.AddCookie(&http.Cookie{Name: c.cookie.Name, Value: "planted"})
	r.AddCookie(c.cookie)
	r.Header.Set(proofHeader, c.proof)
	w := httptest.NewRecorder()
	ts.ServeHTTP(w, r)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "secret-job") {
		t.Errorf("with a planted cookie first = %d", w.Code)
	}
}

func TestLoginAttempts(t *testing.T) {
	ts := newTestServer(t, &fakeReader{})
	for i := range codeAttempts {
		if w, c := ts.login(t, strings.Repeat("0", 32)); c != nil || w.Code != http.StatusForbidden {
			t.Fatalf("wrong code %d = %d", i, w.Code)
		}
	}
	if w, c := ts.login(t, ts.code.value); c != nil || !strings.Contains(w.Body.String(), "no longer valid") {
		t.Errorf("correct code after %d failures = %d", codeAttempts, w.Code)
	}
	if w, _ := ts.login(t, ""); w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "login code required") {
		t.Errorf("empty code = %d %s", w.Code, w.Body)
	}
}

func TestLoginExpiry(t *testing.T) {
	ts := newTestServer(t, &fakeReader{})
	ts.clock = ts.clock.Add(codeLifetime)
	if _, c := ts.login(t, ts.code.value); c != nil {
		t.Error("expired code logged in")
	}
	if w := ts.do(t, "GET", "/login", nil, nil); strings.Contains(w.Body.String(), "<form") {
		t.Error("login form offered after expiry")
	}
}

func TestSessionExpiry(t *testing.T) {
	ts := newTestServer(t, &fakeReader{})
	c := ts.session(t)
	ts.clock = ts.clock.Add(idleLifetime - time.Second)
	if w := ts.do(t, "GET", "/jobs", nil, c); w.Code != http.StatusOK {
		t.Fatalf("active session = %d", w.Code)
	}
	ts.clock = ts.clock.Add(idleLifetime)
	if w := ts.do(t, "GET", "/jobs", nil, c); w.Code != http.StatusUnauthorized {
		t.Errorf("idle session = %d", w.Code)
	}

	ts = newTestServer(t, &fakeReader{})
	c = ts.session(t)
	start := ts.clock
	for ts.clock.Add(idleLifetime / 2).Before(start.Add(sessionLifetime)) {
		ts.clock = ts.clock.Add(idleLifetime / 2)
		if w := ts.do(t, "GET", "/jobs", nil, c); w.Code != http.StatusOK {
			t.Fatalf("session at %v = %d", ts.clock.Sub(start), w.Code)
		}
	}
	ts.clock = start.Add(sessionLifetime)
	if w := ts.do(t, "GET", "/jobs", nil, c); w.Code != http.StatusUnauthorized {
		t.Errorf("session past %v = %d", sessionLifetime, w.Code)
	}
}

func TestRequestChecks(t *testing.T) {
	f := &fakeReader{}
	ts := newTestServer(t, f)
	c := ts.session(t)
	header := func(r *http.Request) *httptest.ResponseRecorder {
		r.AddCookie(c.cookie)
		r.Header.Set(proofHeader, c.proof)
		w := httptest.NewRecorder()
		ts.ServeHTTP(w, r)
		return w
	}
	for _, host := range []string{"localhost:4321", "127.0.0.1", "127.0.0.1:4322", "[::1]:4321", "evil.example"} {
		r := httptest.NewRequest("GET", "/jobs", nil)
		r.Host = host
		if w := header(r); w.Code != http.StatusMisdirectedRequest {
			t.Errorf("Host %q = %d", host, w.Code)
		}
	}
	for _, origin := range [][]string{{"http://evil.example"}, {"null"}, {"http://localhost:4321"}, {"https://" + testHost}, {"http://" + testHost, "http://" + testHost}, nil} {
		r := httptest.NewRequest("POST", "/logout", nil)
		r.Host = testHost
		for _, o := range origin {
			r.Header.Add("Origin", o)
		}
		if w := header(r); w.Code != http.StatusForbidden {
			t.Errorf("Origin %q = %d", origin, w.Code)
		}
	}
	r := httptest.NewRequest("POST", "/login", strings.NewReader("code="+ts.code.value))
	r.Host = testHost
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Set("Sec-Fetch-Site", "cross-site")
	if w := header(r); w.Code != http.StatusForbidden {
		t.Errorf("cross-site fetch = %d", w.Code)
	}
	for _, path := range []string{"/jobs", "/jobs/abc", "/jobs/abc/logs", "/jobs/abc/logs.txt", "/targets", "/info"} {
		for _, method := range []string{"POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "TRACE"} {
			if w := ts.do(t, method, path, url.Values{}, c); w.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s %s = %d", method, path, w.Code)
			}
		}
	}
	if w := ts.do(t, "POST", "/login", url.Values{"code": {strings.Repeat("x", 2*maxBody)}}, nil); w.Code != http.StatusRequestEntityTooLarge || w.Header().Get("Connection") != "close" {
		t.Errorf("oversized login = %d", w.Code)
	}
	for _, tt := range []struct {
		method string
		code   int
	}{{"GET", http.StatusBadRequest}, {"HEAD", http.StatusMethodNotAllowed}, {"PUT", http.StatusMethodNotAllowed}, {"DELETE", http.StatusMethodNotAllowed}} {
		r := httptest.NewRequest(tt.method, "/jobs", strings.NewReader("x"))
		r.Host = testHost
		if w := header(r); w.Code != tt.code || w.Header().Get("Connection") != "close" {
			t.Errorf("%s with body = %d, want %d and a closed connection", tt.method, w.Code, tt.code)
		}
	}
	r = httptest.NewRequest("GET", "/jobs", nil)
	r.Host = testHost
	r.TransferEncoding = []string{"chunked"}
	if w := header(r); w.Code != http.StatusBadRequest {
		t.Errorf("chunked GET = %d", w.Code)
	}
	if f.calls.Load() != 0 {
		t.Errorf("refused requests made %d RPCs", f.calls.Load())
	}
}

func TestHeaders(t *testing.T) {
	ts := newTestServer(t, &fakeReader{job: &v1.Job{Id: "abc"}})
	c := ts.session(t)
	for _, path := range []string{"/", "/login", "/jobs", "/jobs/abc", "/jobs/abc/logs", "/jobs/abc/logs.txt", "/jobs/!/logs", "/targets", "/info", "/static/style.css", "/static/app.js", "/missing"} {
		for i, a := range []*creds{nil, {cookie: c.cookie}, c} {
			w := ts.do(t, "GET", path, nil, a)
			h := w.Header()
			if h.Get("Content-Security-Policy") != csp || h.Get("X-Content-Type-Options") != "nosniff" || h.Get("Cache-Control") != "no-store" || h.Get("Referrer-Policy") != "no-referrer" || h.Get("X-Frame-Options") != "DENY" {
				t.Errorf("%s (credentials %d): headers %v", path, i, h)
			}
		}
	}
	if !strings.Contains(csp, "script-src 'self';") || strings.Contains(csp, "unsafe") {
		t.Errorf("CSP %q allows more than same-origin script files", csp)
	}
	if w := ts.do(t, "GET", "/static/app.js", nil, nil); w.Code != http.StatusOK || w.Header().Get("Content-Type") != "text/javascript; charset=utf-8" || !strings.Contains(w.Body.String(), proofHeader) {
		t.Errorf("app.js = %d %s", w.Code, w.Header().Get("Content-Type"))
	}
	r := httptest.NewRequest("GET", "/jobs", nil)
	r.Host = "evil.example"
	w := httptest.NewRecorder()
	ts.ServeHTTP(w, r)
	if w.Header().Get("Content-Security-Policy") != csp {
		t.Error("refused request lacks headers")
	}
}

func TestInvalidInputNoRPC(t *testing.T) {
	f := &fakeReader{}
	ts := newTestServer(t, f)
	c := ts.session(t)
	missing := ""
	for _, path := range []string{
		"/jobs/" + url.PathEscape("<script>"), "/jobs/" + strings.Repeat("a", 129), "/jobs/a.b/logs", "/jobs/a%20b/logs.txt",
	} {
		w := ts.do(t, "GET", path, nil, c)
		if w.Code != http.StatusNotFound {
			t.Errorf("%s = %d", path, w.Code)
		}
		if missing == "" {
			missing = w.Body.String()
		} else if w.Body.String() != missing {
			t.Errorf("%s: 404 body differs", path)
		}
	}
	for _, path := range []string{
		"/jobs?page_token=" + strings.Repeat("a", maxPageToken+1), "/jobs?page_token=%ff",
		"/jobs/abc/logs?offset=-1", "/jobs/abc/logs?offset=1048577", "/jobs/abc/logs?attempt=0", "/jobs/abc/logs?attempt=11",
		"/jobs/abc/logs?cursor=x&offset=1", "/jobs/abc/logs?cursor=" + strings.Repeat("c", maxCursor+1), "/jobs/abc/logs.txt?cursor=%ff",
	} {
		if w := ts.do(t, "GET", path, nil, c); w.Code != http.StatusBadRequest {
			t.Errorf("%s = %d", path, w.Code)
		}
	}
	if f.calls.Load() != 0 {
		t.Errorf("invalid input made %d RPCs", f.calls.Load())
	}
}

func TestFailure(t *testing.T) {
	expired := connect.NewError(connect.CodeFailedPrecondition, errors.New("expired"))
	detail, err := connect.NewErrorDetail(&v1.RetentionExpiry{Reason: v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_LOGS_EXPIRED})
	if err != nil {
		t.Fatal(err)
	}
	expired.AddDetail(detail)
	tests := []struct {
		err  error
		code int
		want string
	}{
		{connect.NewError(connect.CodeUnauthenticated, errors.New("token revoked secret-detail")), http.StatusUnauthorized, "credential rejected"},
		{connect.NewError(connect.CodeNotFound, errors.New("job abc owned by alice")), http.StatusNotFound, "not found"},
		{connect.NewError(connect.CodePermissionDenied, errors.New("reader")), http.StatusForbidden, "permission denied"},
		{connect.NewError(connect.CodeUnavailable, errors.New("dial")), http.StatusBadGateway, "server unavailable"},
		{expired, http.StatusGone, "logs expired"},
		{connect.NewError(connect.CodeInternal, errors.New("boom")), http.StatusBadGateway, "internal: boom"},
	}
	for _, tt := range tests {
		f := &fakeReader{err: tt.err}
		ts := newTestServer(t, f)
		c := ts.session(t)
		w := ts.do(t, "GET", "/jobs", nil, c)
		if w.Code != tt.code || !strings.Contains(w.Body.String(), tt.want) {
			t.Errorf("%v: %d %s", tt.err, w.Code, w.Body)
		}
		if f.calls.Load() != 1 {
			t.Errorf("%v: %d RPCs, want 1 (no retry)", tt.err, f.calls.Load())
		}
		if strings.Contains(w.Body.String(), "secret-detail") || strings.Contains(w.Body.String(), "alice") {
			t.Errorf("%v: server message disclosed", tt.err)
		}
	}
}

func TestNoMutationRoutes(t *testing.T) {
	ts := newTestServer(t, &fakeReader{})
	c := ts.session(t)
	for _, path := range []string{"/jobs", "/jobs/abc", "/jobs/abc/logs", "/jobs/abc/cancel", "/jobs/abc/result", "/jobs/abc/download", "/artifacts", "/submit", "/prune"} {
		for _, method := range []string{"POST", "PUT", "DELETE"} {
			if w := ts.do(t, method, path, url.Values{}, c); w.Code != http.StatusNotFound && w.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s %s = %d", method, path, w.Code)
			}
		}
	}
}

func TestNewServer(t *testing.T) {
	for _, cfg := range []Config{{}, {Client: &fakeReader{}}, {Client: &fakeReader{}, Timeout: -1}} {
		if _, err := newServer(cfg, testHost, time.Now, bytes.NewReader(make([]byte, 64))); err == nil {
			t.Errorf("newServer(%+v) succeeded", cfg)
		}
	}
	if _, err := newServer(Config{Client: &fakeReader{}, Timeout: time.Second}, testHost, time.Now, bytes.NewReader(nil)); err == nil {
		t.Error("newServer succeeded without randomness")
	}
}

func TestJobRetention(t *testing.T) {
	until := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		retention *v1.GetRetentionResponse
		err       error
		want      []string
	}{
		{"disabled", &v1.GetRetentionResponse{}, nil, []string{"<dt>Retention</dt><dd>disabled</dd>"}},
		{"enabled", &v1.GetRetentionResponse{Enabled: true, AcceptUntil: timestamppb.New(until), Admin: &v1.RetentionAdmin{}}, nil, []string{"<dt>Retention</dt><dd>enabled</dd>", "2026-11-01T00:00:00Z"}},
		{"unimplemented", nil, connect.NewError(connect.CodeUnimplemented, errors.New("old server")), []string{"<dt>Retention</dt><dd>not supported by this server</dd>"}},
		{"denied", nil, connect.NewError(connect.CodePermissionDenied, errors.New("secret-detail")), []string{"<dt>Retention</dt><dd>unavailable (permission_denied)</dd>"}},
		{"transport", nil, errors.New("dial secret-detail"), []string{"<dt>Retention</dt><dd>unavailable</dd>"}},
	}
	for _, tt := range tests {
		f := &fakeReader{job: &v1.Job{Id: "abc"}, retention: tt.retention, retentionErr: tt.err}
		ts := newTestServer(t, f)
		c := ts.session(t)
		w := ts.do(t, "GET", "/jobs/abc", nil, c)
		body := w.Body.String()
		if w.Code != http.StatusOK {
			t.Errorf("%s: job page = %d", tt.name, w.Code)
		}
		if n := f.retentionCalls.Load(); n != 1 {
			t.Errorf("%s: GetRetention called %d times, want 1", tt.name, n)
		}
		for _, want := range tt.want {
			if !strings.Contains(body, want) {
				t.Errorf("%s: page lacks %q:\n%s", tt.name, want, body)
			}
		}
		if strings.Contains(body, "secret-detail") {
			t.Errorf("%s: server error text disclosed", tt.name)
		}
	}

	// A job the credential cannot see costs no retention call.
	f := &fakeReader{err: connect.NewError(connect.CodeNotFound, errors.New("missing"))}
	ts := newTestServer(t, f)
	if w := ts.do(t, "GET", "/jobs/abc", nil, ts.session(t)); w.Code != http.StatusNotFound || f.retentionCalls.Load() != 0 {
		t.Errorf("missing job = %d with %d retention calls", w.Code, f.retentionCalls.Load())
	}
}
