package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// TestDashboardScripts runs "skygo dashboard" from the production executable
// against a real scoped-auth server, and drives it over loopback HTTP as a
// browser would: login with the printed code, read pages, and attempt
// refused requests. No browser renders anything here.
func TestDashboardScripts(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the server executable")
	}
	exe := filepath.Join(t.TempDir(), "skygo")
	args := []string{"build", "-o", exe}
	if mode := testing.CoverMode(); mode != "" {
		args = append(args, "-cover", "-covermode="+mode, "-coverpkg=github.com/tmc/skygo/...")
	}
	build := exec.Command("go", append(args, "../../cmd/skygo")...)
	build.Env = append(os.Environ(), "GOWORK=off", "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, out)
	}
	files, err := filepath.Glob("testdata/dashboard/*.txt")
	if err != nil || len(files) == 0 {
		t.Fatalf("dashboard scripts: %v (%d files)", err, len(files))
	}
	for _, file := range files {
		t.Run(strings.TrimSuffix(filepath.Base(file), ".txt"), func(t *testing.T) {
			runDashboardScript(t, exe, file)
		})
	}
}

func runDashboardScript(t *testing.T, exe, file string) {
	t.Helper()
	// The parent of auth.json must be private; t.TempDir follows the umask.
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	data, authFile := filepath.Join(dir, "state"), filepath.Join(dir, "auth.json")
	var env []string
	if cover := coverDir(t); cover != "" {
		env = append(env, "GOCOVERDIR="+cover)
	}
	srv := &serverProcess{exe: exe, address: freeAddress(t), data: data, env: env, args: []string{"--auth-file", authFile}}
	t.Cleanup(srv.cleanup)
	d := &dashboardProcess{exe: exe, env: env}
	t.Cleanup(func() { d.cleanup(t) })
	e := executableScriptEngine(exe, nil, env)
	e.Cmds["server"] = srv.command()
	e.Cmds["taskfile"] = coverageTaskfile()
	e.Cmds["job-id"] = jobID()
	e.Cmds["dashboard"] = d.command()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	scripttest.Test(t, ctx, e, []string{"SKYGO_TOKEN=", "SKYGO_ENDPOINT=http://" + srv.address, "DATA=" + data, "AUTH=" + authFile}, file)
}

// dashboardProcess runs one "skygo dashboard" at a time and is its browser.
// The login code and session cookie stay in this process; scripts see only
// statuses and bodies, and every body is checked for the credential.
type dashboardProcess struct {
	exe  string
	args []string // before the dashboard's own arguments, as in executableScriptEngine
	env  []string

	// Process fixtures only: wrap may replace the started child's signal,
	// kill and done with fault-injecting wrappers of the originals, and
	// nonzero bounds shorten stop's waits.
	wrap               func(*dashboardChild)
	stopWait, killWait time.Duration

	mu     sync.Mutex
	child  *dashboardChild
	diag   *notifyBuffer
	base   string
	code   string
	proof  string
	secret []byte
	client *http.Client
}

// dashboardChild is one started dashboard. It keeps the original process's
// signal and kill, the relay of its single Wait, and the result of the one
// interrupt and the at most one kill, until that Wait result is received.
// Only then is it joined and its stop outcome fixed. An error from Signal or
// Kill, os.ErrProcessDone included, is not a Wait result.
type dashboardChild struct {
	pid    int
	signal func(os.Signal) error
	kill   func() error
	done   <-chan error
	dir    string // working directory, removed only once joined

	signaled, killed   bool
	signalErr, killErr error

	joined bool
	out    string // the output stop returned, set when joined
	err    error  // the error stop returned, set when joined
}

var errDashboardNotJoined = errors.New("dashboard exit not observed")

// wait waits up to bound for the original Wait result.
func (c *dashboardChild) wait(bound time.Duration) (joined bool, err error) {
	timer := time.NewTimer(bound)
	defer timer.Stop()
	select {
	case err := <-c.done:
		return true, err
	case <-timer.C:
		return false, nil
	}
}

var dashboardStart = regexp.MustCompile(`(?m)^dashboard: (http://[^/\s]+)/\nlogin code: ([0-9a-f]{32})\n`)

func (d *dashboardProcess) command() script.Cmd {
	return script.Command(script.CmdUsage{
		Summary: "run and drive skygo dashboard",
		Args:    "start token-file | login | get path | request method path [origin=O] [host=H] [body=B] [proof=P] | stop",
		Detail:  []string{"get and request write the body to stdout and 'HTTP <status> [<location>]' to stderr.", "After login, requests carry the session proof header as the dashboard's script does; proof= sends none, proof=P sends P."},
	}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) == 0 {
			return nil, script.ErrUsage
		}
		switch {
		case args[0] == "start" && len(args) == 2:
			return nil, d.start(s, args[1])
		case args[0] == "login" && len(args) == 1:
			return d.login()
		case args[0] == "get" && len(args) == 2:
			return d.respond(d.do("GET", args[1], nil))
		case args[0] == "request" && len(args) >= 3:
			return d.respond(d.do(args[1], args[2], args[3:]))
		case args[0] == "stop" && len(args) == 1:
			out, diag, err := d.stop()
			return func(*script.State) (string, string, error) { return out, diag, err }, nil
		}
		return nil, script.ErrUsage
	})
}

func (d *dashboardProcess) start(s *script.State, tokenFile string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.child != nil && !d.child.joined {
		return errors.New("dashboard already running")
	}
	secret, err := os.ReadFile(s.Path(tokenFile))
	if err != nil {
		return err
	}
	// The dashboard gets its own working directory, outside the test's
	// temporary directories, so that a dashboard whose exit was never
	// observed does not have its directory removed by test cleanup.
	dir, err := os.MkdirTemp("", "skygo-dashboard-")
	if err != nil {
		return err
	}
	args := append(append([]string(nil), d.args...), "--token-file", s.Path(tokenFile), "dashboard")
	cmd := exec.Command(d.exe, args...)
	cmd.Dir = dir
	cmd.Env = append(s.Environ(), d.env...)
	d.diag = newNotifyBuffer()
	cmd.Stdout = d.diag
	cmd.Stderr = d.diag
	if err := cmd.Start(); err != nil {
		os.Remove(dir)
		return err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	c := &dashboardChild{pid: cmd.Process.Pid, signal: cmd.Process.Signal, kill: cmd.Process.Kill, done: done, dir: dir}
	if d.wrap != nil {
		d.wrap(c)
	}
	d.child, d.secret = c, bytes.TrimSpace(secret)
	d.base, d.code, d.proof, d.client = "", "", "", nil

	ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
	defer cancel()
	if err := d.diag.await(ctx, dashboardStart); err != nil {
		return err
	}
	m := dashboardStart.FindStringSubmatch(d.diag.String())
	d.base, d.code = m[1], m[2]
	jar, err := cookiejar.New(nil)
	if err != nil {
		return err
	}
	d.client = &http.Client{Jar: jar, Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return nil
}

// login posts the code as the dashboard's script does and keeps the session
// proof from the response; the script sees only the status.
func (d *dashboardProcess) login() (script.WaitFunc, error) {
	d.mu.Lock()
	code := d.code
	d.mu.Unlock()
	resp, err := d.do("POST", "/login", []string{"body=" + url.Values{"code": {code}}.Encode(), "login=1"})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<10))
	if err != nil {
		return nil, err
	}
	status := fmt.Sprintf("HTTP %d\n", resp.StatusCode)
	if resp.StatusCode != http.StatusOK {
		return func(*script.State) (string, string, error) { return string(body), status, nil }, nil
	}
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).Match(body) {
		return nil, fmt.Errorf("login returned a malformed session proof (%d bytes)", len(body))
	}
	d.mu.Lock()
	d.proof = string(body)
	d.mu.Unlock()
	return func(*script.State) (string, string, error) { return "", status, nil }, nil
}

// do sends a request to the dashboard. Options: origin=O sets Origin (the
// default for POST is the dashboard origin; origin= sends none), host=H
// sets Host, body=B sends a form body, proof=P replaces the session proof
// (proof= sends none), and login=1 marks a login by the dashboard's script.
func (d *dashboardProcess) do(method, path string, opts []string) (*http.Response, error) {
	d.mu.Lock()
	base, client, proof := d.base, d.client, d.proof
	d.mu.Unlock()
	if client == nil {
		return nil, errors.New("dashboard not started")
	}
	var body io.Reader
	origin, host, login := "", "", false
	if method == "POST" {
		origin = base
	}
	for _, opt := range opts {
		k, v, ok := strings.Cut(opt, "=")
		switch {
		case ok && k == "origin":
			origin = v
		case ok && k == "host":
			host = v
		case ok && k == "body":
			body = strings.NewReader(v)
		case ok && k == "proof":
			proof = v
		case ok && k == "login" && v == "1":
			login = true
		default:
			return nil, fmt.Errorf("unknown option %q", opt)
		}
	}
	req, err := http.NewRequest(method, base+path, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	if proof != "" {
		req.Header.Set("X-Skygo-Session", proof)
	}
	if login {
		req.Header.Set("X-Skygo-Login", "1")
	}
	if host != "" {
		req.Host = host
	}
	return client.Do(req)
}

func (d *dashboardProcess) respond(resp *http.Response, err error) (script.WaitFunc, error) {
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	secret, code, proof := d.secret, d.code, d.proof
	d.mu.Unlock()
	var head bytes.Buffer
	resp.Header.Write(&head)
	for _, b := range [][]byte{body, head.Bytes()} {
		if len(secret) > 0 && bytes.Contains(b, secret) {
			return nil, errors.New("dashboard response contains the bearer credential")
		}
		if code != "" && bytes.Contains(b, []byte(code)) {
			return nil, errors.New("dashboard response contains the login code")
		}
		if proof != "" && bytes.Contains(b, []byte(proof)) {
			return nil, errors.New("dashboard response contains the session proof")
		}
	}
	status := fmt.Sprintf("HTTP %d", resp.StatusCode)
	if loc := resp.Header.Get("Location"); loc != "" {
		status += " " + loc
	}
	return func(*script.State) (string, string, error) { return string(body), status + "\n", nil }, nil
}

// stop interrupts the dashboard and waits for it to exit. Its output, minus
// the login code, becomes the command's stdout; it must exit 0 and must not
// have printed the credential.
//
// The dashboard is interrupted once and killed at most once, after 15s. If
// its exit is not observed 10s after that, stop reports it and keeps the
// child: a later stop only waits again. Once the exit is observed, stop
// returns the same outcome every time, every error of the interrupt, the
// kill and the exit included.
func (d *dashboardProcess) stop() (string, string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	c := d.child
	if c == nil {
		return "", "", errors.New("dashboard not running")
	}
	if c.joined {
		return c.out, "", c.err
	}
	d.client = nil
	if !c.signaled {
		c.signaled = true
		if err := c.signal(os.Interrupt); err != nil {
			c.signalErr = fmt.Errorf("interrupt dashboard: %w", err)
		}
	}
	joined, err := c.wait(stopBound(d.stopWait, 15*time.Second))
	if !joined && !c.killed {
		c.killed = true
		if err := c.kill(); err != nil {
			c.killErr = fmt.Errorf("kill dashboard: %w", err)
		}
		joined, err = c.wait(stopBound(d.killWait, 10*time.Second))
	}
	if !joined {
		held := fmt.Errorf("dashboard pid %d: %w; working directory %s held", c.pid, errDashboardNotJoined, c.dir)
		return "", "", errors.Join(held, c.signalErr, c.killErr)
	}
	errs := []error{c.signalErr, c.killErr}
	if c.killed {
		errs = append(errs, errors.New("dashboard did not stop within its bound and was killed"))
	}
	if err != nil {
		errs = append(errs, fmt.Errorf("dashboard exit: %w", err))
	}
	out := strings.ReplaceAll(d.diag.String(), d.code, "<code>")
	if len(d.secret) > 0 && strings.Contains(out, string(d.secret)) {
		out, errs = "", append(errs, errors.New("dashboard output contains the bearer credential"))
	}
	if d.proof != "" && strings.Contains(out, d.proof) {
		out, errs = "", append(errs, errors.New("dashboard output contains the session proof"))
	}
	if err := os.RemoveAll(c.dir); err != nil {
		errs = append(errs, err)
	}
	c.joined, c.out, c.err = true, out, errors.Join(errs...)
	d.proof = ""
	return c.out, "", c.err
}

// stopBound returns d if it is set, and def otherwise.
func stopBound(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}

// cleanup stops a dashboard the script left running and reports a failure,
// including a dashboard whose exit was not observed.
func (d *dashboardProcess) cleanup(t testing.TB) {
	d.mu.Lock()
	running := d.child != nil && !d.child.joined
	d.mu.Unlock()
	if !running {
		return
	}
	if _, _, err := d.stop(); err != nil {
		t.Errorf("stop dashboard: %v", err)
	}
}

// TestDashboardWorkload stands in for the dashboard in the stop tests, which
// start it through dashboardProcess. It prints the dashboard's startup lines,
// then runs until interrupted ("interrupt") or, ignoring interrupts, until
// killed ("ignore"). Either way it ends by itself after a minute.
func TestDashboardWorkload(t *testing.T) {
	mode := os.Getenv("SKYGO_DASHBOARD_WORKLOAD")
	if mode == "" {
		return
	}
	interrupted := make(chan os.Signal, 1)
	signal.Notify(interrupted, os.Interrupt)
	fmt.Printf("dashboard: http://127.0.0.1:1/\nlogin code: %s\n", strings.Repeat("ab", 16))
	timeout := time.After(time.Minute)
	for {
		select {
		case <-interrupted:
			if mode == "interrupt" {
				return
			}
		case <-timeout:
			return
		}
	}
}

var (
	errDashboardSignalFixture = errors.New("injected signal error")
	errDashboardKillFixture   = errors.New("injected kill error")
)

// dashboardStopFixture is a dashboardProcess running TestDashboardWorkload
// through the normal start path. The started child's signal and kill are
// wrapped to count the calls that actually reach them.
type dashboardStopFixture struct {
	d       *dashboardProcess
	child   *dashboardChild // as started, wrappers included
	kill    func() error    // the original process's Kill, for the test's own cleanup
	signals atomic.Int32
	kills   atomic.Int32
}

// startDashboardStop starts the workload in mode. inject may replace the
// child's signal, kill or done with fault-injecting wrappers of the
// originals; the counting wrappers go around whatever inject installs.
func startDashboardStop(t *testing.T, mode string, inject func(*dashboardChild)) *dashboardStopFixture {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("os.Interrupt cannot be sent on Windows")
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "token"), []byte("workload-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	s, err := script.NewState(ctx, dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	f := &dashboardStopFixture{}
	f.d = &dashboardProcess{
		exe:      exe,
		args:     []string{"-test.run=^TestDashboardWorkload$", "--"},
		env:      []string{"SKYGO_DASHBOARD_WORKLOAD=" + mode},
		stopWait: 2 * time.Second,
		killWait: time.Second,
		wrap: func(c *dashboardChild) {
			f.kill = c.kill
			if inject != nil {
				inject(c)
			}
			send, kill := c.signal, c.kill
			c.signal = func(sig os.Signal) error {
				f.signals.Add(1)
				return send(sig)
			}
			c.kill = func() error {
				f.kills.Add(1)
				return kill()
			}
			f.child = c
		},
	}
	// Cleanups run last first: the workload is killed directly, so that it
	// never outlives the test, and then the harness cleanup reports.
	t.Cleanup(func() { f.d.cleanup(t) })
	t.Cleanup(func() {
		if f.kill != nil {
			f.kill()
		}
	})
	if err := f.d.start(s, "token"); err != nil {
		t.Fatal(err)
	}
	return f
}

// checkJoined checks that stop observed the original exit and removed the
// dashboard's working directory.
func (f *dashboardStopFixture) checkJoined(t *testing.T, out string) {
	t.Helper()
	f.d.mu.Lock()
	c, joined := f.d.child, f.child.joined
	f.d.mu.Unlock()
	if c != f.child || !joined {
		t.Errorf("dashboard child replaced or not joined")
	}
	if !strings.Contains(out, "login code: <code>") {
		t.Errorf("stop output = %q, want the redacted startup lines", out)
	}
	if _, err := os.Stat(f.child.dir); !os.IsNotExist(err) {
		t.Errorf("working directory after join: %v, want it removed", err)
	}
}

// checkHeld checks that stop kept the original child, unjoined, and its
// working directory.
func (f *dashboardStopFixture) checkHeld(t *testing.T) {
	t.Helper()
	f.d.mu.Lock()
	c, joined := f.d.child, f.child.joined
	f.d.mu.Unlock()
	if c != f.child || joined {
		t.Errorf("dashboard child replaced or joined without an observed exit")
	}
	if _, err := os.Stat(f.child.dir); err != nil {
		t.Errorf("held working directory: %v", err)
	}
}

func (f *dashboardStopFixture) checkCalls(t *testing.T, signals, kills int32) {
	t.Helper()
	if s, k := f.signals.Load(), f.kills.Load(); s != signals || k != kills {
		t.Errorf("signal and kill calls = %d, %d; want %d, %d", s, k, signals, kills)
	}
}

// TestDashboardStopSignalError delivers the interrupt but reports a Signal
// error. stop must still observe the original exit, return the error, and
// return the same outcome again.
func TestDashboardStopSignalError(t *testing.T) {
	f := startDashboardStop(t, "interrupt", func(c *dashboardChild) {
		send := c.signal
		c.signal = func(sig os.Signal) error {
			send(sig)
			return errDashboardSignalFixture
		}
	})
	out, _, err := f.d.stop()
	if !errors.Is(err, errDashboardSignalFixture) || errors.Is(err, errDashboardNotJoined) {
		t.Fatalf("stop = %v, want the signal error with an observed exit", err)
	}
	if strings.Contains(err.Error(), "dashboard exit") {
		t.Errorf("stop = %v, want a clean exit", err)
	}
	f.checkJoined(t, out)
	if out2, _, err2 := f.d.stop(); out2 != out || err2 != err {
		t.Errorf("second stop = %q, %v; want the cached outcome", out2, err2)
	}
	f.checkCalls(t, 1, 0)
}

// TestDashboardStopDelayedWait holds back the original Wait result after the
// process has exited. Until the result is received, stop reports the child
// held, keeps it and its directory, and never interrupts or kills it again;
// a kill result, os.ErrProcessDone included, is not taken as the exit.
func TestDashboardStopDelayedWait(t *testing.T) {
	exited, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f := startDashboardStop(t, "interrupt", func(c *dashboardChild) {
		original, delayed := c.done, make(chan error, 1)
		go func() {
			err := <-original
			close(exited)
			<-release
			delayed <- err
		}()
		c.done = delayed
	})
	// Registered after the fixture, so it runs before the harness cleanup.
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	if _, _, err := f.d.stop(); !errors.Is(err, errDashboardNotJoined) {
		t.Fatalf("stop = %v, want the exit not observed", err)
	}
	select {
	case <-exited:
	case <-time.After(10 * time.Second):
		t.Fatal("workload did not exit")
	}
	f.checkHeld(t)
	f.checkCalls(t, 1, 1)
	if _, _, err := f.d.stop(); !errors.Is(err, errDashboardNotJoined) {
		t.Errorf("second stop = %v, want the exit still not observed", err)
	}
	f.checkHeld(t)
	f.checkCalls(t, 1, 1)
	once.Do(func() { close(release) })
	out, _, err := f.d.stop()
	if errors.Is(err, errDashboardNotJoined) || err == nil || !strings.Contains(err.Error(), "killed") {
		t.Errorf("stop after the exit = %v, want the observed exit and the kill", err)
	}
	f.checkJoined(t, out)
	f.checkCalls(t, 1, 1)
}

// TestDashboardStopKillError runs a workload that ignores the interrupt and
// fails the kill without killing. stop and cleanup must report the child
// held; once the test kills the workload itself, stop must observe the exit
// and return the kill error with it.
func TestDashboardStopKillError(t *testing.T) {
	f := startDashboardStop(t, "ignore", func(c *dashboardChild) {
		c.kill = func() error { return errDashboardKillFixture }
	})
	_, _, err := f.d.stop()
	if !errors.Is(err, errDashboardKillFixture) || !errors.Is(err, errDashboardNotJoined) {
		t.Fatalf("stop = %v, want the kill error and the exit not observed", err)
	}
	f.checkHeld(t)
	f.checkCalls(t, 1, 1)
	rec := &dashboardTB{TB: t}
	f.d.cleanup(rec)
	if len(rec.errs) != 1 || !strings.Contains(rec.errs[0], "held") {
		t.Errorf("cleanup reported %q, want one held dashboard", rec.errs)
	}
	f.checkHeld(t)
	f.checkCalls(t, 1, 1)
	if err := f.kill(); err != nil {
		t.Fatalf("kill workload: %v", err)
	}
	out, _, err := f.d.stop()
	if !errors.Is(err, errDashboardKillFixture) || errors.Is(err, errDashboardNotJoined) || !strings.Contains(err.Error(), "dashboard exit") {
		t.Errorf("stop after the exit = %v, want the kill error with the observed exit", err)
	}
	f.checkJoined(t, out)
	f.checkCalls(t, 1, 1)
}

// dashboardTB records Errorf calls instead of failing the test.
type dashboardTB struct {
	testing.TB
	errs []string
}

func (r *dashboardTB) Errorf(format string, args ...any) {
	r.errs = append(r.errs, fmt.Sprintf(format, args...))
}

// TestDashboardRedirect checks that the dashboard's RPC client neither
// follows a redirect nor sends the credential anywhere but the endpoint.
func TestDashboardRedirect(t *testing.T) {
	const token = "redirect-secret-token"
	var mu sync.Mutex
	var seen []string
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Method+" "+r.URL.Path+" auth="+r.Header.Get("Authorization"))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer other.Close()
	var endpointAuth []string
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		endpointAuth = append(endpointAuth, r.Header.Get("Authorization"))
		mu.Unlock()
		http.Redirect(w, r, other.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer endpoint.Close()

	hc, err := dashboardHTTPClient(endpoint.URL, token)
	if err != nil {
		t.Fatal(err)
	}
	client := skyv1connect.NewControlServiceClient(hc, endpoint.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := client.GetServerInfo(ctx, connect.NewRequest(&skyv1.GetServerInfoRequest{})); err == nil {
		t.Error("redirected RPC succeeded")
	}
	// A request the transport sees for another host gets no credential.
	resp, err := hc.Get(other.URL + "/direct")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(endpointAuth) != 1 || endpointAuth[0] != "Bearer "+token {
		t.Errorf("endpoint saw Authorization %q, want one bearer request", endpointAuth)
	}
	if len(seen) != 1 || seen[0] != "GET /direct auth=" {
		t.Errorf("other host saw %q, want only the direct request without credential", seen)
	}
	for _, endpoint := range []string{"", "127.0.0.1:8080", "ftp://127.0.0.1/", "http://"} {
		if _, err := dashboardHTTPClient(endpoint, token); err == nil {
			t.Errorf("dashboardHTTPClient(%q) succeeded", endpoint)
		}
	}
}
