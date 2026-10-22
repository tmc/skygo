package facade

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/integrations/skycompat/wire"
)

func TestOptions(t *testing.T) {
	for _, endpoint := range []string{"", "ftp://example.test", "http://user@example.test", "http://example.test?x=1", "http://example.test#x", "http://"} {
		t.Run(endpoint, func(t *testing.T) {
			if _, err := NewHandler(Options{Endpoint: endpoint}); err == nil {
				t.Fatal("accepted invalid endpoint")
			}
		})
	}
}

type fixture struct {
	binary, dir, auth, endpoint, admin, alice, bob, alicePath string
	facade                                                    *httptest.Server
	h                                                         *handler
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	binary := os.Getenv("SKYCOMPAT_CORE_BINARY")
	if binary == "" {
		t.Skip("set SKYCOMPAT_CORE_BINARY for real canonical-process qualification")
	}
	dir := t.TempDir()
	// auth.json and the tokens are written here, and their parent must be
	// private; t.TempDir follows the umask.
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	f := &fixture{binary: binary, dir: dir, auth: filepath.Join(dir, "auth.json")}
	adminPath := filepath.Join(dir, "admin.token")
	f.run(t, "auth", "init", "--data-dir", filepath.Join(dir, "state"), "--auth-file", f.auth, "--token-output", adminPath)
	f.admin = readToken(t, adminPath)
	for _, principal := range []struct{ name, role string }{{"alice", "operator"}, {"bob", "reader"}} {
		f.run(t, "auth", "principal", "add", principal.name, "--role", principal.role, "--auth-file", f.auth)
		f.run(t, "auth", "token", "issue", principal.name, "--auth-file", f.auth, "--output", filepath.Join(dir, principal.name+".token"), "--expires-in", "24h")
	}
	f.alicePath = filepath.Join(dir, "alice.token")
	f.alice = readToken(t, f.alicePath)
	f.bob = readToken(t, filepath.Join(dir, "bob.token"))
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.endpoint = "http://" + l.Addr().String()
	address := l.Addr().String()
	l.Close()
	log, err := os.Create(filepath.Join(dir, "core.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "server", "--data-dir", filepath.Join(dir, "state"), "--auth-file", f.auth, "--listen", address)
	cmd.Env = withoutTokenEnv(os.Environ())
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait(); log.Close() }()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case <-done:
		case <-time.After(8 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	})
	client := skyv1connect.NewControlServiceClient(http.DefaultClient, f.endpoint)
	eventually(t, 5*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		req := connect.NewRequest(&skyv1.GetServerInfoRequest{})
		req.Header().Set("Authorization", "Bearer "+f.admin)
		_, err := client.GetServerInfo(ctx, req)
		return err == nil
	})
	raw, err := NewHandler(Options{Endpoint: f.endpoint})
	if err != nil {
		t.Fatal(err)
	}
	f.h = raw.(*handler)
	f.facade = httptest.NewServer(raw)
	t.Cleanup(f.facade.Close)
	t.Cleanup(func() {
		req := connect.NewRequest(&skyv1.ListJobsRequest{})
		req.Header().Set("Authorization", "Bearer "+f.admin)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		resp, err := client.ListJobs(ctx, req)
		if err != nil {
			t.Error("final job inspection:", err)
		} else if len(resp.Msg.Jobs) != 0 {
			t.Error("validation created jobs")
		}
	})
	return f
}
func withoutTokenEnv(env []string) []string {
	out := []string{}
	for _, e := range env {
		if !strings.HasPrefix(e, "SKYGO_TOKEN=") {
			out = append(out, e)
		}
	}
	return out
}
func readToken(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(b))
}
func (f *fixture) run(t *testing.T, args ...string) {
	t.Helper()
	cmd := exec.Command(f.binary, args...)
	cmd.Env = withoutTokenEnv(os.Environ())
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("canonical command %s: %v: %s", args[0], err, out)
	}
}
func eventually(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if fn() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
func (f *fixture) request(t *testing.T, method, path, token, body string, headers map[string]string) (int, []byte, http.Header) {
	t.Helper()
	req, err := http.NewRequest(method, f.facade.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set(wire.APIVersionHeader, "67")
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := f.facade.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(b, []byte(f.alice)) || bytes.Contains(b, []byte(f.admin)) {
		t.Fatal("credential in response")
	}
	return resp.StatusCode, b, resp.Header
}
func validateBody(t *testing.T, task string) string {
	t.Helper()
	b, err := json.Marshal(map[string]any{"dag": "name: null\n---\n" + task, "env_vars": map[string]string{}})
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestCanonicalHealthAndValidate(t *testing.T) {
	f := newFixture(t)
	for _, token := range []string{f.admin, f.alice, f.bob} {
		status, b, h := f.request(t, "GET", "/api/health", token, "", nil)
		if status != 200 || !bytes.Contains(b, []byte(`"status":"healthy"`)) || h.Get(wire.VersionHeader) != version {
			t.Fatalf("health: %d %s", status, b)
		}
		var payload map[string]any
		if json.Unmarshal(b, &payload) != nil || payload["api_version"] != "67" || payload["version"] != version || payload["version_on_disk"] != version || payload["commit"] != "" || payload["user"] != nil || payload["telemetry_enabled"] != false {
			t.Fatalf("health shape: %s", b)
		}
		status, b, _ = f.request(t, "POST", "/validate", token, validateBody(t, "run: echo inert\nresources: {}\nnum_nodes: 1\nfile_mounts: {}\nvolumes: {}\n"), nil)
		if status != 200 || string(b) != "null" {
			t.Fatalf("validate: %d %s", status, b)
		}
	}
	for _, token := range []string{"", "bad"} {
		status, b, _ := f.request(t, "GET", "/api/health", token, "", nil)
		if status != 200 || !bytes.Contains(b, []byte(`"status":"needs_auth"`)) {
			t.Fatalf("health missing auth: %d %s", status, b)
		}
		status, _, _ = f.request(t, "POST", "/validate", token, validateBody(t, "run: echo inert\n"), nil)
		if status != 401 {
			t.Fatalf("validate unauthenticated: %d", status)
		}
	}
	for _, task := range []string{"run: echo one\nrun: echo two\n", "run: &x echo inert\nsetup: *x\n", "run: echo inert\nsetup: touch marker\n", "run: echo inert\nworkdir: /server/path\n", "run: echo inert\nnum_nodes: 2\n", "run: echo inert\nresources: {cpus: 2}\n", "run: echo inert\nfile_mounts: {/out: /secret}\n", "run: echo inert\nenvs: {TOKEN: null}\n", "run: echo inert\nunknown_secret: payload\n"} {
		status, b, _ := f.request(t, "POST", "/validate", f.alice, validateBody(t, task), nil)
		if status != 400 || !bytes.Contains(b, []byte(`"type":"ValueError"`)) || bytes.Contains(b, []byte("unknown_secret")) || bytes.Contains(b, []byte("touch marker")) {
			t.Fatalf("negative validation: %d %s", status, b)
		}
	}
	for _, task := range []string{"run: echo inert\nsetup: ''\nworkdir: ''\n", "run: echo inert\nresources: {disk_size: 256, use_spot: false}\n"} {
		status, b, _ := f.request(t, "POST", "/validate", f.alice, validateBody(t, task), nil)
		if status != 200 {
			t.Fatalf("benign defaults: %d %s", status, b)
		}
	}
	for _, tc := range []struct {
		method, path string
		headers      map[string]string
		want         int
	}{{"GET", "/missing", nil, 404}, {"POST", "/api/health", nil, 405}, {"GET", "/api/health?x=1", nil, 400}, {"POST", "/validate", map[string]string{wire.APIVersionHeader: "66"}, 400}, {"POST", "/validate", map[string]string{wire.APIVersionHeader: ""}, 400}, {"POST", "/validate", map[string]string{"Content-Type": "text/plain"}, 415}} {
		status, _, _ := f.request(t, tc.method, tc.path, f.alice, validateBody(t, "run: echo inert\n"), tc.headers)
		if status != tc.want {
			t.Errorf("%s %s status %d want %d", tc.method, tc.path, status, tc.want)
		}
	}
}

func TestCanonicalRevokeAndUnavailable(t *testing.T) {
	f := newFixture(t)
	f.run(t, "auth", "token", "revoke", strings.Split(f.alice, "_")[1], "--auth-file", f.auth)
	eventually(t, 3*time.Second, func() bool {
		status, _, _ := f.request(t, "POST", "/validate", f.alice, validateBody(t, "run: echo inert\n"), nil)
		return status == 401
	})
	status, _, _ := f.request(t, "POST", "/validate", f.bob, validateBody(t, "run: echo inert\n"), nil)
	if status != 200 {
		t.Fatal("reader lost validation")
	}
	held := f.auth + ".held"
	if err := os.Rename(f.auth, held); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Rename(held, f.auth) })
	eventually(t, 3*time.Second, func() bool {
		status, _, _ := f.request(t, "POST", "/validate", f.bob, validateBody(t, "run: echo inert\n"), nil)
		return status == 503
	})
	if err := os.Rename(held, f.auth); err != nil {
		t.Fatal(err)
	}
	eventually(t, 3*time.Second, func() bool {
		status, _, _ := f.request(t, "POST", "/validate", f.bob, validateBody(t, "run: echo inert\n"), nil)
		return status == 200
	})
}

func TestSlowBodyAndDisconnect(t *testing.T) {
	f := newFixture(t)
	address := strings.TrimPrefix(f.facade.URL, "http://")
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = fmt.Fprintf(conn, "POST /validate HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n%s: 67\r\nAuthorization: Bearer %s\r\n\r\n{", wire.APIVersionHeader, f.alice)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(7 * time.Second))
	_, _ = io.ReadAll(conn)
	conn.Close()
	elapsed := time.Since(start)
	if elapsed < 4*time.Second || elapsed > 6500*time.Millisecond {
		t.Fatalf("slow body deadline: %s", elapsed)
	}
	eventually(t, time.Second, func() bool { return len(f.h.admitted) == 0 })
	conn, err = net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = fmt.Fprintf(conn, "POST /validate HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n%s: 67\r\nAuthorization: Bearer %s\r\n\r\n{", wire.APIVersionHeader, f.alice)
	conn.Close()
	eventually(t, time.Second, func() bool { return len(f.h.admitted) == 0 })
	status, _, _ := f.request(t, "GET", "/api/health", f.bob, "", nil)
	if status != 200 {
		t.Fatal("health failed after disconnect")
	}
}

func TestHeaderBounds(t *testing.T) {
	f := newFixture(t)
	for _, header := range []string{"Authorization: Bearer bad\r\nAuthorization: Bearer bad\r\n", wire.APIVersionHeader + ": 67\r\n" + wire.APIVersionHeader + ": 67\r\n"} {
		conn, err := net.Dial("tcp", strings.TrimPrefix(f.facade.URL, "http://"))
		if err != nil {
			t.Fatal(err)
		}
		extra := wire.APIVersionHeader + ": 67\r\n"
		want := 401
		if strings.HasPrefix(header, wire.APIVersionHeader) {
			extra = ""
			want = 400
		}
		_, _ = fmt.Fprintf(conn, "GET /api/health HTTP/1.1\r\nHost: test\r\n%s%sConnection: close\r\n\r\n", extra, header)
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			conn.Close()
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		conn.Close()
		if resp.StatusCode != want {
			t.Fatalf("duplicate header: %d want %d", resp.StatusCode, want)
		}
	}
}

func TestCanonicalExpiry(t *testing.T) {
	f := newFixture(t)
	path := filepath.Join(f.dir, "short.token")
	f.run(t, "auth", "token", "issue", "bob", "--auth-file", f.auth, "--output", path, "--expires-in", "5s")
	token := readToken(t, path)
	eventually(t, 3*time.Second, func() bool {
		status, _, _ := f.request(t, "POST", "/validate", token, validateBody(t, "run: echo inert\n"), nil)
		return status == 200
	})
	eventually(t, 7*time.Second, func() bool {
		status, _, _ := f.request(t, "POST", "/validate", token, validateBody(t, "run: echo inert\n"), nil)
		return status == 401
	})
	status, _, _ := f.request(t, "POST", "/validate", f.bob, validateBody(t, "run: echo inert\n"), nil)
	if status != 200 {
		t.Fatal("expired token affected another credential")
	}
}

func TestDeadlineSupport(t *testing.T) {
	h, err := NewHandler(Options{Endpoint: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, httptest.NewRequest("GET", "http://test/api/health", nil))
	if recorder.Code != 503 || !strings.Contains(recorder.Body.String(), "transport unavailable") {
		t.Fatalf("unbounded writer accepted: %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestUnknownRouteSlowBody(t *testing.T) {
	h, err := NewHandler(Options{Endpoint: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	defer server.Close()
	conn, err := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	start := time.Now()
	// Leave the body unfinished and allow keep-alive. net/http must not drain
	// beyond the handler's socket deadline while completing the rejection.
	_, err = fmt.Fprint(conn, "POST /unknown HTTP/1.1\r\nHost: test\r\nContent-Length: 1000\r\n\r\n{")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(7 * time.Second))
	_, _ = io.ReadAll(conn)
	if elapsed := time.Since(start); elapsed < 4*time.Second || elapsed > 6500*time.Millisecond {
		t.Fatalf("unknown-route body drain: %s", elapsed)
	}
}
