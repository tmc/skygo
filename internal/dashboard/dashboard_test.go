package dashboard_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/cli"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/dashboard"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
)

// The generated client is the production Reader.
var _ dashboard.Reader = skyv1connect.ControlServiceClient(nil)

// logBackend supplies log reads for jobs accepted by the fixture. Tests
// mark jobs running in the store; scheduling and observation are not run.
type logBackend struct {
	read func(context.Context, int64, int) (backend.LogChunk, error)
}

func (*logBackend) Check(backend.Task) error { return nil }
func (*logBackend) Start(context.Context, string, backend.Task) error {
	return errors.New("unexpected start")
}
func (*logBackend) Observe(context.Context, backend.Reference) (backend.Status, error) {
	return backend.Status{}, errors.New("unexpected observe")
}
func (*logBackend) Cancel(context.Context, backend.Reference) error {
	return errors.New("unexpected cancel")
}
func (b *logBackend) Logs(ctx context.Context, _ backend.Reference, offset int64, size int) (backend.LogChunk, error) {
	return b.read(ctx, offset, size)
}

// payload serves fixed log bytes and then completes.
func payload(data []byte) *logBackend {
	return &logBackend{read: func(_ context.Context, offset int64, size int) (backend.LogChunk, error) {
		start := min(int(offset), len(data))
		end := min(start+size, len(data))
		return backend.LogChunk{Data: data[start:end], NextOffset: int64(end), Complete: end == len(data)}, nil
	}}
}

// blocking serves "hello\n", signals entered at the next read and holds that
// read until the server cancels it.
func blocking(entered chan struct{}) *logBackend {
	return &logBackend{read: func(ctx context.Context, offset int64, _ int) (backend.LogChunk, error) {
		if offset == 0 {
			return backend.LogChunk{Data: []byte("hello\n"), NextOffset: 6}, nil
		}
		select {
		case entered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return backend.LogChunk{}, ctx.Err()
	}}
}

// fixture is a real store, scoped auth file, control service and Connect
// handler with operators alice and bob, a reader and an admin.
type fixture struct {
	path     string
	tokens   map[string]string
	files    map[string]string
	ids      map[string]string
	store    *store.Store
	engine   *auth.Engine
	retained bool
	url      string
	api      skyv1connect.ControlServiceClient

	mu    sync.Mutex
	calls []string // RPC method names received, in order
}

// take returns and clears the RPC methods received since the last take.
func (f *fixture) take() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	calls := f.calls
	f.calls = nil
	return calls
}

// newFixture starts the fixture server. With retained set, the store enables
// bounded retention (without local output) before the server starts.
func newFixture(t *testing.T, b *logBackend, retained bool) *fixture {
	t.Helper()
	// The parent of auth.json and the token files must be private; t.TempDir
	// follows the host umask.
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(dir, "state")
	s, err := store.Open(state)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if s != nil {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	f := &fixture{path: filepath.Join(dir, "auth.json"), tokens: make(map[string]string), files: make(map[string]string), ids: make(map[string]string)}
	if _, err := auth.Initialize(f.path, filepath.Join(dir, "legacy.token"), s.Owner()); err != nil {
		t.Fatal(err)
	}
	for _, p := range []struct {
		name string
		role auth.Role
	}{{"alice", auth.Operator}, {"bob", auth.Operator}, {"reader", auth.Reader}, {"admin", auth.Admin}} {
		if err := auth.AddPrincipal(f.path, p.name, p.role); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, p.name+".token")
		info, err := auth.Issue(f.path, p.name, path, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		f.tokens[p.name], f.files[p.name], f.ids[p.name] = strings.TrimSuffix(string(data), "\n"), path, info.ID
	}
	if err := s.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	if retained {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		s = nil
		policy := store.DefaultRetentionPolicy()
		policy.LocalOutputEnabled = false
		if _, err := store.EnableRetention(state, policy); err != nil {
			t.Fatal(err)
		}
		if s, err = store.Open(state); err != nil {
			t.Fatal(err)
		}
	}
	f.store, f.retained = s, retained
	engine, err := auth.NewFile(f.path, s.Owner())
	if err != nil {
		t.Fatal(err)
	}
	f.engine = engine
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); engine.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	c, err := control.New(s, []backend.Target{{Name: "test", Kind: "test", Binding: "test-route", Backend: b}}, engine)
	if err != nil {
		t.Fatal(err)
	}
	handler := rpc.NewHandler(c, engine)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.calls = append(f.calls, path.Base(r.URL.Path))
		f.mu.Unlock()
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	f.url = server.URL
	f.api = skyv1connect.NewControlServiceClient(server.Client(), server.URL)
	return f
}

func (f *fixture) running(t *testing.T, principal string) string {
	t.Helper()
	ctx := context.Background()
	task := &v1.Task{Argv: []string{"unused", principal}, Env: map[string]string{"VISIBLE_NAME": "hidden-value-sentinel"}}
	var id string
	if f.retained {
		// This log-read fixture does not run the control loop or its storage
		// sampler. Accept retained jobs in the real scoped store instead.
		actor, err := f.engine.Authenticate("Bearer " + f.tokens[principal])
		if err != nil {
			t.Fatal(err)
		}
		actor, err = f.engine.Check(actor, auth.Mutate)
		if err != nil {
			t.Fatal(err)
		}
		info, err := f.store.RetentionInfo(actor)
		if err != nil {
			t.Fatal(err)
		}
		if !info.Enabled {
			t.Fatal("retention disabled")
		}
		job, err := f.store.SubmitRetained(actor, info.Domain, "dashboard", backend.Task{Argv: task.Argv, Env: task.Env}, "test", "test-route", backend.RemoteOutput, 0)
		if err != nil {
			t.Fatal(err)
		}
		id = job.ID
	} else {
		resp, err := f.api.SubmitJob(ctx, authorized(f.tokens[principal], &v1.SubmitJobRequest{RequestId: "dashboard", Task: task}))
		if err != nil {
			t.Fatal(err)
		}
		id = resp.Msg.Job.Id
	}
	if _, err := f.store.Update(id, func(j *backend.Job) error { j.Status.State = backend.Running; return nil }); err != nil {
		t.Fatal(err)
	}
	return id
}

func authorized[T any](token string, msg *T) *connect.Request[T] {
	r := connect.NewRequest(msg)
	r.Header().Set("Authorization", "Bearer "+token)
	return r
}

// bearer attaches a credential to requests for one host, as the CLI's
// dashboard transport does; its client follows no redirects.
type bearer struct{ token, host string }

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Host != b.host {
		return http.DefaultTransport.RoundTrip(r)
	}
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return http.DefaultTransport.RoundTrip(r)
}

// codeLog captures Serve's log and reports the login code once written.
type codeLog struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	ready chan struct{}
	once  sync.Once
}

var codeLine = regexp.MustCompile(`login code: ([0-9a-f]{32})\n`)

func (l *codeLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf.Write(p)
	if codeLine.Match(l.buf.Bytes()) {
		l.once.Do(func() { close(l.ready) })
	}
	return len(p), nil
}

func (l *codeLog) code(t *testing.T) string {
	t.Helper()
	select {
	case <-l.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("dashboard did not print a login code")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return codeLine.FindStringSubmatch(l.buf.String())[1]
}

// browser is a logged-in HTTP client of one running dashboard. Like a
// browser, its cookie jar sends cookies to every port of the host; like the
// dashboard's script, it sends the session proof only to the dashboard.
type browser struct {
	base   string
	client *http.Client
	tab    *tab
	stop   func() error
}

// tab adds the session proof header to requests for one host.
type tab struct {
	host, proof string
}

func (t *tab) RoundTrip(r *http.Request) (*http.Response, error) {
	if t.proof != "" && r.URL.Host == t.host {
		r = r.Clone(r.Context())
		r.Header.Set("X-Skygo-Session", t.proof)
	}
	return http.DefaultTransport.RoundTrip(r)
}

func (f *fixture) dashboard(t *testing.T, principal string) *browser {
	t.Helper()
	ln, err := dashboard.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := url.Parse(f.url)
	if err != nil {
		t.Fatal(err)
	}
	client := skyv1connect.NewControlServiceClient(&http.Client{
		Transport:     bearer{f.tokens[principal], endpoint.Host},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}, f.url)
	log := &codeLog{ready: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() {
		served <- dashboard.Serve(ctx, ln, dashboard.Config{Client: client, Timeout: 5 * time.Second}, log)
	}()
	var once sync.Once
	var result error
	stop := func() error {
		once.Do(func() {
			cancel()
			select {
			case result = <-served:
			case <-time.After(10 * time.Second):
				result = errors.New("dashboard did not stop")
			}
		})
		return result
	}
	t.Cleanup(func() {
		if err := stop(); err != nil {
			t.Error(err)
		}
	})
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	// Timeout turns a stream that should have been refused or ended into a
	// failure rather than a hang.
	b := &browser{base: "http://" + ln.Addr().String(), stop: stop, tab: &tab{host: ln.Addr().String()}}
	b.client = &http.Client{
		Jar:           jar,
		Transport:     b.tab,
		Timeout:       20 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	req, err := http.NewRequest("POST", b.base+"/login", strings.NewReader(url.Values{"code": {log.code(t)}}.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Origin", b.base)
	req.Header.Set("X-Skygo-Login", "1")
	resp, err := b.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusOK || len(proof) != 64 {
		t.Fatalf("login = %d %v", resp.StatusCode, err)
	}
	b.tab.proof = string(proof)
	return b
}

func (b *browser) get(t *testing.T, path string) (int, string) {
	t.Helper()
	resp, err := b.client.Get(b.base + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(body)
}

func TestIsolation(t *testing.T) {
	f := newFixture(t, payload([]byte("alice output\n")), false)
	alice, bob := f.running(t, "alice"), f.running(t, "bob")
	random := strings.Repeat("e", 32)
	d := f.dashboard(t, "bob")
	for _, suffix := range []string{"", "/logs", "/logs.txt", "/logs.txt?follow=1"} {
		code, foreign := d.get(t, "/jobs/"+alice+suffix)
		wantCode, missing := d.get(t, "/jobs/"+random+suffix)
		if code != http.StatusNotFound || wantCode != http.StatusNotFound || foreign != missing {
			t.Errorf("%s: foreign %d and missing %d differ:\n%s\n%s", suffix, code, wantCode, foreign, missing)
		}
		if code, invalid := d.get(t, "/jobs/a.b"+suffix); code != http.StatusNotFound || invalid != missing {
			t.Errorf("%s: invalid %d differs from missing:\n%s\n%s", suffix, code, invalid, missing)
		}
		if strings.Contains(foreign, "alice output") {
			t.Errorf("%s: foreign logs disclosed", suffix)
		}
	}
	if _, list := d.get(t, "/jobs"); strings.Contains(list, alice) || !strings.Contains(list, bob) {
		t.Errorf("bob's list = %s", list)
	}
	if _, list := f.dashboard(t, "admin").get(t, "/jobs"); !strings.Contains(list, alice) || !strings.Contains(list, bob) {
		t.Errorf("admin's list = %s", list)
	}
	code, page := f.dashboard(t, "alice").get(t, "/jobs/"+alice)
	if code != http.StatusOK || !strings.Contains(page, alice) || !strings.Contains(page, "VISIBLE_NAME") || strings.Contains(page, "hidden-value-sentinel") {
		t.Errorf("alice's job page = %d %s", code, page)
	}
}

func TestRevocation(t *testing.T) {
	entered := make(chan struct{}, 8)
	f := newFixture(t, blocking(entered), false)
	id := f.running(t, "alice")
	d := f.dashboard(t, "alice")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", d.base+"/jobs/"+id+"/logs.txt?follow=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := d.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body := make(chan string, 1)
	go func() {
		data, _ := io.ReadAll(resp.Body)
		body <- string(data)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("log stream did not start")
	}
	if err := auth.Revoke(f.path, f.ids["alice"]); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-body:
		if want := "hello\n\n-- stream ended: unauthenticated --\n"; got != want {
			t.Errorf("follow body = %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("revocation did not end the stream")
	}
	if code, page := d.get(t, "/jobs"); code != http.StatusUnauthorized || !strings.Contains(page, "credential rejected") {
		t.Errorf("next page = %d %s", code, page)
	}
	j, err := f.store.Get(id)
	if err != nil || j.CancelRequested || j.Status.State != backend.Running {
		t.Errorf("revocation changed the job: %+v %v", j.Status, err)
	}
}

func TestFollowLimit(t *testing.T) {
	entered := make(chan struct{}, 8)
	f := newFixture(t, blocking(entered), false)
	id := f.running(t, "alice")
	d := f.dashboard(t, "alice")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var bodies []io.Closer
	defer func() {
		for _, b := range bodies {
			b.Close()
		}
	}()
	for i := range 4 {
		req, err := http.NewRequestWithContext(ctx, "GET", d.base+"/jobs/"+id+"/logs.txt?follow=1", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := d.client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, resp.Body)
		if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "text/plain; charset=utf-8" || resp.Header.Get("X-Content-Type-Options") != "nosniff" {
			t.Fatalf("follow %d = %d %v", i, resp.StatusCode, resp.Header)
		}
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatalf("follow %d did not start", i)
		}
	}
	if code, body := d.get(t, "/jobs/"+id+"/logs.txt?follow=1"); code != http.StatusServiceUnavailable {
		t.Errorf("fifth follow = %d %s", code, body)
	}
	cancel()
}

func TestNoCredentialInResponses(t *testing.T) {
	f := newFixture(t, payload([]byte("output\n")), false)
	id := f.running(t, "alice")
	d := f.dashboard(t, "alice")
	for _, path := range []string{"/", "/login", "/jobs", "/jobs/" + id, "/jobs/" + id + "/logs", "/jobs/" + id + "/logs.txt", "/targets", "/info", "/static/style.css"} {
		resp, err := d.client.Get(d.base + path)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		var head strings.Builder
		resp.Header.Write(&head)
		for name, token := range f.tokens {
			if strings.Contains(string(body), token) || strings.Contains(head.String(), token) || strings.Contains(path, token) {
				t.Errorf("%s: %s's credential disclosed", path, name)
			}
		}
		if strings.Contains(string(body), d.tab.proof) || strings.Contains(head.String(), d.tab.proof) {
			t.Errorf("%s: session proof disclosed", path)
		}
		if strings.Contains(string(body), "hidden-value-sentinel") {
			t.Errorf("%s: environment value disclosed", path)
		}
	}
}

// TestOtherPortHarvest plays a hostile server on another loopback port.
// The logged-in browser visits it, which hands it the dashboard cookie, but
// not the proof; replaying the harvested cookie reads no data, opens no
// stream and cannot log out.
func TestOtherPortHarvest(t *testing.T) {
	f := newFixture(t, payload([]byte("harvest-sentinel\n")), false)
	id := f.running(t, "alice")
	d := f.dashboard(t, "alice")
	var mu sync.Mutex
	var harvested []string
	var proofSeen bool
	hostile := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		harvested = append(harvested, r.Header.Get("Cookie"))
		proofSeen = proofSeen || r.Header.Get("X-Skygo-Session") != ""
		mu.Unlock()
	}))
	defer hostile.Close()
	if !strings.HasPrefix(hostile.URL, "http://127.0.0.1:") {
		t.Fatalf("hostile server at %s, want another 127.0.0.1 port", hostile.URL)
	}
	resp, err := d.client.Get(hostile.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	mu.Lock()
	cookie := strings.Join(harvested, "; ")
	leaked := proofSeen
	mu.Unlock()
	if !strings.Contains(cookie, "skygo_dashboard_") {
		t.Fatalf("hostile server received cookies %q; the harvest premise does not hold", cookie)
	}
	if leaked {
		// A harness self-check: only the test's tab transport adds the
		// header, as only the dashboard's script does in a browser.
		t.Fatal("test harness sent the session proof to another port")
	}

	replay := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	f.take()
	for _, path := range []string{"/jobs", "/jobs/" + id, "/jobs/" + id + "/logs", "/jobs/" + id + "/logs.txt", "/jobs/" + id + "/logs.txt?follow=1", "/targets", "/info"} {
		for _, proof := range []string{"", strings.Repeat("0", 64)} {
			req, err := http.NewRequest("GET", d.base+path, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Cookie", cookie)
			if proof != "" {
				req.Header.Set("X-Skygo-Session", proof)
			}
			resp, err := replay.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(body), id) || strings.Contains(string(body), "harvest-sentinel") {
				t.Errorf("replayed cookie read %s (proof %q): %d\n%s", path, proof, resp.StatusCode, body)
			}
			typ := resp.Header.Get("Content-Type")
			if proof == "" {
				// The cookie alone gets the static shell, which holds no data.
				if resp.StatusCode != http.StatusOK || !strings.HasPrefix(typ, "text/html") || !strings.Contains(string(body), `id="view"`) {
					t.Errorf("replayed cookie %s = %d %s, want the static shell:\n%s", path, resp.StatusCode, typ, body)
				}
			} else if resp.StatusCode != http.StatusUnauthorized || string(body) != "session required\n" {
				// A wrong proof gets the fixed denial.
				t.Errorf("replayed cookie %s with a wrong proof = %d %q, want 401 %q", path, resp.StatusCode, body, "session required\n")
			}
		}
	}
	if calls := f.take(); len(calls) != 0 {
		t.Errorf("replayed cookie reached the server: %q", calls)
	}
	req, err := http.NewRequest("POST", d.base+"/logout", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Cookie", cookie)
	req.Header.Set("Origin", d.base)
	if resp, err := replay.Do(req); err != nil || resp.StatusCode != http.StatusForbidden {
		t.Errorf("replayed logout = %v %v", resp, err)
	} else {
		resp.Body.Close()
	}
	// The real tab, with cookie and proof, still reads.
	if code, body := d.get(t, "/jobs/"+id+"/logs.txt"); code != http.StatusOK || body != "harvest-sentinel\n" {
		t.Errorf("logged-in tab = %d %q", code, body)
	}
	// A page on the other port plants a same-named cookie with a longer
	// path, which the browser then sends first. The real tab still reads.
	u, err := url.Parse(hostile.URL + "/jobs/")
	if err != nil {
		t.Fatal(err)
	}
	d.client.Jar.SetCookies(u, []*http.Cookie{{Name: d.cookieName(t), Value: "planted", Path: "/jobs"}})
	if code, body := d.get(t, "/jobs/"+id+"/logs.txt"); code != http.StatusOK || body != "harvest-sentinel\n" {
		t.Errorf("logged-in tab with a planted cookie = %d %q", code, body)
	}
}

// cookieName returns the name of the dashboard's session cookie.
func (b *browser) cookieName(t *testing.T) string {
	t.Helper()
	u, err := url.Parse(b.base)
	if err != nil {
		t.Fatal(err)
	}
	return "skygo_dashboard_" + u.Port()
}

func TestShutdownEndsStreams(t *testing.T) {
	entered := make(chan struct{}, 8)
	f := newFixture(t, blocking(entered), false)
	id := f.running(t, "alice")
	d := f.dashboard(t, "alice")
	resp, err := d.client.Get(d.base + "/jobs/" + id + "/logs.txt?follow=1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("log stream did not start")
	}
	start := time.Now()
	if err := d.stop(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("shutdown took %v", elapsed)
	}
	data, _ := io.ReadAll(resp.Body)
	if !strings.HasPrefix(string(data), "hello\n") {
		t.Errorf("follow body = %q", data)
	}
	j, err := f.store.Get(id)
	if err != nil || j.CancelRequested || j.Status.State != backend.Running {
		t.Errorf("shutdown changed the job: %+v %v", j.Status, err)
	}
}

// TestLogsMatchCLI pins the dashboard's log selection to "skygo jobs logs"
// against the same canonical server: equal bytes on success, and failure
// exactly when the CLI fails.
func TestLogsMatchCLI(t *testing.T) {
	t.Setenv("SKYGO_TOKEN", "")
	t.Setenv("SKYGO_ENDPOINT", "")
	data := []byte("plain\n<b>bold</b>\x1b[31mred\x1b[0m\n\xff\xfe invalid\x00nul\n" + strings.Repeat("0123456789abcdef", 8192))
	for _, retained := range []bool{false, true} {
		t.Run(fmt.Sprintf("retained=%v", retained), func(t *testing.T) {
			testLogsMatchCLI(t, data, newFixture(t, payload(data), retained), retained)
		})
	}
}

// statusMarkers matches the truncation and incompleteness markers that
// logs.txt appends to output not known to be whole.
var statusMarkers = regexp.MustCompile(`\n(-- output (truncated|incomplete)[^\n]* --\n)+$`)

func testLogsMatchCLI(t *testing.T, data []byte, f *fixture, retained bool) {
	id := f.running(t, "alice")
	foreign := f.running(t, "bob")
	d := f.dashboard(t, "alice")
	tests := []struct {
		name  string
		job   string
		query url.Values
		args  []string
	}{
		{"all", id, nil, nil},
		{"offset", id, url.Values{"offset": {"3"}}, []string{"--offset", "3"}},
		{"end", id, url.Values{"offset": {"1048576"}}, []string{"--offset", "1048576"}},
		{"first attempt", id, url.Values{"attempt": {"1"}}, []string{"--attempt", "1"}},
		{"missing attempt", id, url.Values{"attempt": {"2"}}, []string{"--attempt", "2"}},
		{"cursor", id, url.Values{"cursor": {""}}, []string{"--cursor", ""}},
		{"foreign", foreign, nil, nil},
		{"missing", strings.Repeat("d", 32), nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := "/jobs/" + tt.job + "/logs.txt"
			if len(tt.query) > 0 {
				path += "?" + tt.query.Encode()
			}
			f.take()
			code, got := d.get(t, path)
			dashboardCalls := f.take()
			c := cli.New(nil)
			var out, errout bytes.Buffer
			c.SetOut(&out)
			c.SetErr(&errout)
			c.SetArgs(append([]string{"--endpoint", f.url, "--token-file", f.files["alice"], "jobs", "logs", tt.job}, tt.args...))
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err := c.ExecuteContext(ctx)
			cliCalls := f.take()
			// Equal bytes cannot show which log RPC was chosen; the RPC
			// sequence does.
			if !slices.Equal(dashboardCalls, cliCalls) {
				t.Errorf("dashboard RPCs %q, CLI RPCs %q", dashboardCalls, cliCalls)
			}
			if (code == http.StatusOK) != (err == nil) {
				t.Fatalf("dashboard %d, CLI error %v (%s)", code, err, errout.String())
			}
			// The CLI reports truncation on stderr and incompleteness not at
			// all; logs.txt appends marker lines. Legacy fixture output is
			// complete, so it must carry none. Retained output may.
			if retained {
				got = statusMarkers.ReplaceAllString(got, "")
			}
			if err == nil && got != out.String() {
				t.Errorf("dashboard bytes differ from CLI:\n%q\n%q", got, out.String())
			}
		})
	}
	status := "disabled"
	if retained {
		status = "enabled"
	}
	if code, page := d.get(t, "/jobs/"+id); code != http.StatusOK || !strings.Contains(page, "<dt>Retention</dt><dd>"+status+"</dd>") {
		t.Errorf("job page retention = %d, want %s:\n%s", code, status, page)
	}
	if retained {
		return
	}
	if _, got := d.get(t, "/jobs/"+id+"/logs.txt"); got != string(data) {
		t.Errorf("logs.txt = %d bytes, want the %d backend bytes", len(got), len(data))
	}
}

func TestListen(t *testing.T) {
	for _, addr := range []string{"0.0.0.0:0", "localhost:0", "10.0.0.1:0", "127.0.0.2:0", "[::ffff:127.0.0.1]:0", "[::1%lo0]:0", "[::]:0", "127.0.0.1", ":0", ""} {
		ln, err := dashboard.Listen(addr)
		if err == nil {
			ln.Close()
			t.Errorf("Listen(%q) succeeded", addr)
		} else if err.Error() != "dashboard listens only on a loopback address" {
			t.Errorf("Listen(%q) = %v", addr, err)
		}
	}
	if _, err := dashboard.Listen("127.0.0.1:65536"); err == nil {
		t.Error("Listen accepted an invalid port")
	}
	for _, addr := range []string{"127.0.0.1:0", "[::1]:0"} {
		ln, err := dashboard.Listen(addr)
		if err != nil {
			if addr == "[::1]:0" {
				t.Logf("Listen(%q): %v (no IPv6 loopback)", addr, err)
				continue
			}
			t.Fatal(err)
		}
		host, _, _ := net.SplitHostPort(ln.Addr().String())
		ln.Close()
		if want, _, _ := net.SplitHostPort(addr); host != strings.Trim(want, "[]") {
			t.Errorf("Listen(%q) bound %s", addr, ln.Addr())
		}
	}
}

func TestServeRefusesNonLoopback(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	wrapped := otherAddr{ln}
	var log bytes.Buffer
	err = dashboard.Serve(context.Background(), wrapped, dashboard.Config{Client: skyv1connect.NewControlServiceClient(http.DefaultClient, "http://127.0.0.1:1"), Timeout: time.Second}, &log)
	if err == nil || log.Len() != 0 {
		t.Fatalf("Serve = %v, log %q", err, log.String())
	}
}

// otherAddr reports a non-loopback address for a real listener.
type otherAddr struct{ net.Listener }

func (otherAddr) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 80} }

// TestReadOnly checks that the package source names no mutating or
// transferring RPC and never holds the full generated client.
func TestReadOnly(t *testing.T) {
	forbidden := map[string]bool{
		"SubmitJob": true, "SubmitRetryJob": true, "SubmitRetainedJob": true, "SubmitWorkspaceJob": true, "SubmitDataJob": true,
		"CancelJob": true, "PruneJobs": true, "PlanTask": true, "QuoteTask": true, "RemoveArtifact": true,
		"BeginArtifactUpload": true, "GetArtifactUpload": true, "WriteArtifactChunk": true, "FinalizeArtifactUpload": true, "CancelArtifactUpload": true,
		"BeginArtifactDownload": true, "ReadArtifactChunk": true, "CloseArtifactDownload": true, "ControlServiceClient": true,
	}
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		ast.Inspect(file, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && forbidden[id.Name] {
				t.Errorf("%s: %s names %s", name, fset.Position(id.Pos()), id.Name)
			}
			return true
		})
	}
	if checked == 0 {
		t.Fatal("no package sources checked")
	}
}
