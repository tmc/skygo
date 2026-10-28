package cli

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// TestCoverageWorkload is the program submitted by the coverage scripts.
// Files named by SKYGO_COVERAGE_DIR coordinate with the script, so no step
// depends on elapsed time.
func TestCoverageWorkload(t *testing.T) {
	mode := os.Getenv("SKYGO_COVERAGE_WORKLOAD")
	if mode == "" {
		return
	}
	dir := os.Getenv("SKYGO_COVERAGE_DIR")
	mark := func(name string) {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0600); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	switch mode {
	case "success":
		fmt.Println("done")
		os.Exit(0)
	case "hold":
		fmt.Println("holding")
		mark("ready")
	case "release":
		// Print, then wait for the script to create "release" before printing
		// again. Output after the release proves the process outlived any
		// client that was attached before it.
		fmt.Println("first")
		mark("ready")
		for {
			if _, err := os.Stat(filepath.Join(dir, "release")); err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		fmt.Println("second")
		mark("released")
	case "chatter":
		// Write to both streams until killed, so the captured output has
		// live writers.
		fmt.Println("chatter")
		fmt.Fprintln(os.Stderr, "chatter")
		mark("ready")
		for {
			fmt.Println("chatter")
			fmt.Fprintln(os.Stderr, "chatter")
			time.Sleep(time.Millisecond)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown workload %q\n", mode)
		os.Exit(1)
	}
	time.Sleep(time.Minute)
	os.Exit(0)
}

// TestCoverage runs testdata/coverage scripts against the production CLI and
// the production server executable. Each script owns a data directory, a
// loopback port and a fault-injecting proxy. When the test binary is
// instrumented, the executables are built with -cover and write their
// coverage data to a per-script directory (see coverDir).
func TestCoverage(t *testing.T) {
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
	files, err := filepath.Glob("testdata/coverage/*.txt")
	if err != nil || len(files) == 0 {
		t.Fatalf("coverage scripts: %v (%d files)", err, len(files))
	}
	for _, file := range files {
		t.Run(strings.TrimSuffix(filepath.Base(file), ".txt"), func(t *testing.T) {
			runCoverageScript(t, exe, file)
		})
	}
}

func runCoverageScript(t *testing.T, exe, file string) {
	address := freeAddress(t)
	var env []string
	if dir := coverDir(t); dir != "" {
		env = append(env, "GOCOVERDIR="+dir)
	}
	// The server's data directory is not a TempDir: it is removed only
	// after the server's original Wait result, never while it is held.
	data, err := os.MkdirTemp("", "skygo-server-")
	if err != nil {
		t.Fatal(err)
	}
	srv := &serverProcess{exe: exe, address: address, data: data, ownData: true, env: env}
	t.Cleanup(func() {
		if err := srv.finish(false); err != nil {
			t.Errorf("server: %v", err)
		}
	})
	proxy := &faultProxy{target: &url.URL{Scheme: "http", Host: address}, drop: make(map[string]int)}
	ps := httptest.NewServer(proxy)
	t.Cleanup(ps.Close)

	e := executableScriptEngine(exe, nil, env)
	bg := &background{}
	e.Cmds["skygo"] = bg.command(exe, env)
	e.Cmds["await-stdout"] = bg.awaitStdout()
	e.Cmds["interrupt"] = bg.interrupt()
	e.Cmds["server"] = srv.command()
	e.Cmds["drop-response"] = proxy.command()
	e.Cmds["taskfile"] = coverageTaskfile()
	e.Cmds["await-file"] = awaitFile()
	e.Cmds["job-id"] = jobID()
	e.Cmds["await-state"] = retryAwaitState(exe, env)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	scripttest.Test(t, ctx, e, []string{
		"SKYGO_ENDPOINT=http://" + address,
		"SKYGO_TOKEN=coverage-token",
		"PROXY=" + ps.URL,
		"DATA=" + data,
	}, file)
}

// freeAddress reserves and releases a loopback port. The server executable
// does not report its bound address, so a script must choose one in advance;
// "server start" fails clearly if another process takes it in between.
func freeAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

// serverProcess runs "skygo server" on a fixed address and data directory,
// so successive starts are restarts of the same installation.
type serverProcess struct {
	exe, address, data string
	env                []string
	args               []string // nil preserves the legacy coverage-token server
	ownData            bool     // finish removes data, only after the join

	mu  sync.Mutex
	cmd *exec.Cmd
	// out and diag are written by the process's copy goroutines until its
	// Wait returns, so they are read only after its Wait result arrives.
	out     bytes.Buffer
	diag    bytes.Buffer
	done    chan error
	stopped *serverStop // the original process's stop, once begun

	// Fixture tests set these; zero values use the original process
	// directly and the default bounds.
	signalFunc            func(*os.Process, os.Signal) error
	killFunc              func(*os.Process) error
	drainBound, killBound time.Duration
}

func (p *serverProcess) command() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "start, stop or kill the server executable", Args: "start|stop|crash"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		switch args[0] {
		case "start":
			return nil, p.start(s)
		case "stop":
			out, diag, err := p.stop(os.Interrupt)
			return func(*script.State) (string, string, error) { return out, diag, err }, nil
		case "crash":
			out, diag, err := p.crash()
			return func(*script.State) (string, string, error) { return out, diag, err }, nil
		}
		return nil, script.ErrUsage
	})
}

func (p *serverProcess) start(s *script.State) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd != nil {
		return errors.New("server already running")
	}
	p.stopped = nil
	args := []string{"server", "--listen", p.address, "--data-dir", p.data}
	if p.args == nil {
		args = append(args, "--token", "coverage-token")
	} else {
		args = append(args, p.args...)
	}
	cmd := exec.Command(p.exe, args...)
	cmd.Env = append(s.Environ(), p.env...)
	if p.args == nil {
		// The script's clients use SKYGO_TOKEN; the server already names
		// that credential explicitly and must not receive two sources.
		cmd.Env = append(cmd.Env, "SKYGO_TOKEN=")
	}
	p.out.Reset()
	p.diag.Reset()
	cmd.Stdout = &p.out
	cmd.Stderr = &p.diag
	if err := p.admit(); err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		p.retire()
		return err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	p.cmd, p.done = cmd, done

	ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
	defer cancel()
	client := &http.Client{Timeout: time.Second}
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		r, err := client.Get("http://" + p.address + "/ready")
		if err == nil {
			r.Body.Close()
			if r.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case err := <-done:
			p.cmd = nil
			p.retire()
			return fmt.Errorf("server exited during startup: %v\n%s", err, &p.diag)
		case <-ctx.Done():
			return fmt.Errorf("server not ready: %w", ctx.Err())
		case <-tick.C:
		}
	}
}

// serverStop records the stop of one original server process. Its Signal
// and Kill are each attempted at most once, and its Wait result is recorded
// once received from that process's own Wait. A Signal error, including
// os.ErrProcessDone, is not a Wait result.
type serverStop struct {
	signaled, killed, waited bool
	late                     bool // the server outlived its drain bound
	signal, kill, wait       error
}

// err reports every recorded failure, with the Wait result when there is
// one. Until the Wait result is received, the process is still owned.
func (st *serverStop) err() error {
	var errs []error
	if st.signal != nil {
		errs = append(errs, fmt.Errorf("signal server: %w", st.signal))
	}
	if st.late {
		errs = append(errs, errors.New("server did not stop within its drain bound"))
	}
	if st.kill != nil {
		errs = append(errs, fmt.Errorf("kill server: %w", st.kill))
	}
	if !st.waited {
		errs = append(errs, errors.New("server Wait not observed; original process still owned"))
	} else if st.wait != nil {
		errs = append(errs, st.wait)
	}
	return errors.Join(errs...)
}

// stop signals the server once and waits for that same process's Wait
// result, bounded by the server's own drain limits plus a margin. A server
// that outlives the bound is killed once and waited for again. If even then
// no Wait result arrives, the original stays owned: a later stop or cleanup
// neither signals nor kills it again, but only waits again for the same Wait
// result. Once that result is received, stop returns the recorded outcome
// and the server's output. Until then it returns no output: the process's
// copy goroutines may still be writing it.
func (p *serverProcess) stop(sig os.Signal) (string, string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd == nil {
		if p.stopped != nil && p.stopped.waited {
			return p.out.String(), p.diag.String(), p.stopped.err()
		}
		return "", "", errors.New("server not running")
	}
	if p.stopped == nil {
		p.stopped = &serverStop{}
	}
	st := p.stopped
	if !st.signaled {
		st.signaled = true
		st.signal = p.signalServer(sig)
		if p.awaitServer(st, cmp.Or(p.drainBound, 30*time.Second)) {
			return p.out.String(), p.diag.String(), st.err()
		}
		st.late = true
	}
	if !st.killed {
		st.killed = true
		st.kill = p.killServer()
	}
	if p.awaitServer(st, cmp.Or(p.killBound, 10*time.Second)) {
		return p.out.String(), p.diag.String(), st.err()
	}
	return "", "", st.err()
}

// awaitServer waits up to bound for the original process's Wait result. Only
// that result releases the process and retires its owner.
func (p *serverProcess) awaitServer(st *serverStop, bound time.Duration) bool {
	select {
	case err := <-p.done:
		st.waited, st.wait = true, err
		p.cmd = nil
		p.retire()
		return true
	case <-time.After(bound):
		return false
	}
}

func (p *serverProcess) signalServer(sig os.Signal) error {
	if p.signalFunc != nil {
		return p.signalFunc(p.cmd.Process, sig)
	}
	return p.cmd.Process.Signal(sig)
}

func (p *serverProcess) killServer() error {
	if p.killFunc != nil {
		return p.killFunc(p.cmd.Process)
	}
	return p.cmd.Process.Kill()
}

// crash kills the original server process without any shutdown and waits for
// that same process's Wait result. It signals no other process: local work the
// server started can survive it, and the script that started that work must
// release it. If the killed server does not exit within the bound, the
// original stays owned here, with its one Kill recorded, so cleanup only
// waits for it again.
func (p *serverProcess) crash() (string, string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd == nil {
		return "", "", errors.New("server not running")
	}
	if p.stopped != nil {
		return "", "", errors.New("server already being stopped")
	}
	st := &serverStop{signaled: true, killed: true}
	p.stopped = st
	st.kill = p.killServer()
	if !p.awaitServer(st, cmp.Or(p.killBound, 10*time.Second)) {
		return "", "", fmt.Errorf("killed server did not exit: kill %v", st.kill)
	}
	var exit *exec.ExitError
	if st.kill != nil || !errors.As(st.wait, &exit) || exit.ExitCode() != -1 {
		return p.out.String(), p.diag.String(), fmt.Errorf("server was not killed: kill %v, wait %v", st.kill, st.wait)
	}
	return p.out.String(), p.diag.String(), nil
}

func (p *serverProcess) cleanup() { p.stopRunning() }

// stopRunning stops a server the script left running and returns the
// server's stderr with any stop failure. A server that is still owned after
// a failed stop is only waited for again by a later call.
func (p *serverProcess) stopRunning() (string, error) {
	p.mu.Lock()
	running := p.cmd != nil
	p.mu.Unlock()
	if !running {
		return "", nil
	}
	_, diag, err := p.stop(os.Interrupt)
	return diag, err
}

// errServerHeld reports a server whose original Wait result was never
// received. It and its data directory stay retained.
var errServerHeld = errors.New("server Wait unknown; process and data directory retained")

// errServerDataKept reports a joined server whose data directory finish was
// told to keep.
var errServerDataKept = errors.New("server data directory retained")

// finish stops a server the script left running. Only once the original Wait
// result has been received does it remove the data directory it owns, and
// not even then if keepData is set. A server whose Wait result is unknown
// stays owned, its data directory is never removed, and finish reports the
// hold; a later finish only waits again.
func (p *serverProcess) finish(keepData bool) error {
	diag, err := p.stopRunning()
	if err != nil {
		err = fmt.Errorf("stop server: %w\n%s", err, diag)
	}
	p.mu.Lock()
	held := p.cmd != nil
	p.mu.Unlock()
	if held {
		return errors.Join(err, fmt.Errorf("%w: %s", errServerHeld, p.data))
	}
	if keepData {
		return errors.Join(err, fmt.Errorf("%w: %s", errServerDataKept, p.data))
	}
	if p.ownData {
		err = errors.Join(err, os.RemoveAll(p.data))
	}
	return err
}

// serverOwnersMax bounds the server processes owned at once.
const serverOwnersMax = 64

// serverOwners is the strong root of every server process from just before
// its start until its original Wait result is received. A server whose Wait
// result never arrives stays here for the rest of the test process. When the
// table is full, start refuses instead of dropping an owner.
var serverOwners struct {
	mu      sync.Mutex
	servers []*serverProcess
}

func (p *serverProcess) admit() error {
	serverOwners.mu.Lock()
	defer serverOwners.mu.Unlock()
	if len(serverOwners.servers) >= serverOwnersMax {
		return fmt.Errorf("%d server processes still owned; refusing another", len(serverOwners.servers))
	}
	serverOwners.servers = append(serverOwners.servers, p)
	return nil
}

func (p *serverProcess) retire() {
	serverOwners.mu.Lock()
	defer serverOwners.mu.Unlock()
	if i := slices.Index(serverOwners.servers, p); i >= 0 {
		serverOwners.servers = slices.Delete(serverOwners.servers, i, i+1)
	}
}

func serverOwned(p *serverProcess) bool {
	serverOwners.mu.Lock()
	defer serverOwners.mu.Unlock()
	return slices.Contains(serverOwners.servers, p)
}

// stopFixture runs a coverage workload as a serverProcess's original process
// through the same admission, stop, finish and cleanup code as a script's
// server, with its output captured as start captures it. Its process
// operations are wrappers around the original process that count calls and
// can add modeled failures; a modeled operation is counted but never
// delivered. Its Wait relay is start's, except that a gated fixture holds
// the original Wait result until opened.
type stopFixture struct {
	p      *serverProcess
	cmd    *exec.Cmd
	dir    string          // the workload's marker directory
	waited <-chan struct{} // closed once the original Wait has returned
	gate   chan struct{}
	open   func()
	// deliverKill sends the original process its one real Kill, for a
	// fixture whose modeled Kill was never delivered.
	deliverKill func() error
	// report receives the registered cleanup's failures; it is t.Errorf
	// unless a test replaces it to observe them.
	report func(error)

	signals, kills         int
	signalErr, killErr     error // modeled failures, added to the real result
	modelSignal, modelKill bool  // count the operation but never deliver it
}

func startStopFixture(t *testing.T, mode string, gated bool) *stopFixture {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("stop sends os.Interrupt")
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	f := &stopFixture{dir: t.TempDir(), gate: make(chan struct{})}
	f.open = sync.OnceFunc(func() { close(f.gate) })
	if !gated {
		f.open()
	}
	f.p = &serverProcess{
		signalFunc: func(pr *os.Process, sig os.Signal) error {
			f.signals++
			if f.modelSignal {
				return f.signalErr
			}
			return errors.Join(pr.Signal(sig), f.signalErr)
		},
		killFunc: func(pr *os.Process) error {
			f.kills++
			if f.modelKill {
				return f.killErr
			}
			return errors.Join(pr.Kill(), f.killErr)
		},
	}
	f.cmd = exec.Command(exe, "-test.run=^TestCoverageWorkload$")
	f.cmd.Env = append(os.Environ(), "SKYGO_COVERAGE_WORKLOAD="+mode, "SKYGO_COVERAGE_DIR="+f.dir)
	f.cmd.Stdout = &f.p.out
	f.cmd.Stderr = &f.p.diag
	if err := f.p.admit(); err != nil {
		t.Fatal(err)
	}
	if err := f.cmd.Start(); err != nil {
		f.p.retire()
		t.Fatal(err)
	}
	f.deliverKill = sync.OnceValue(f.cmd.Process.Kill)
	done := make(chan error, 1)
	waited := make(chan struct{})
	go func() {
		err := f.cmd.Wait()
		close(waited)
		<-f.gate
		done <- err
	}()
	f.waited = waited
	f.p.cmd, f.p.done = f.cmd, done
	// Cleanups run last first: the gate opens, then cleanup finishes the
	// server, reporting any failure.
	f.report = func(err error) { t.Errorf("fixture server: %v", err) }
	t.Cleanup(func() { f.cleanup(f.report) })
	t.Cleanup(f.open)
	return f
}

// cleanup ends a workload whose modeled Kill was never delivered, then
// finishes the server, reporting any failure.
func (f *stopFixture) cleanup(report func(error)) {
	if f.modelKill {
		f.deliverKill()
	}
	if err := f.p.finish(false); err != nil {
		report(err)
	}
}

// withData gives the fixture's server a data directory it owns.
func (f *stopFixture) withData(t *testing.T) {
	t.Helper()
	data, err := os.MkdirTemp("", "skygo-stop-")
	if err != nil {
		t.Fatal(err)
	}
	f.p.data, f.p.ownData = data, true
}

// awaitWaited waits for the original Wait to return, not for its result.
func (f *stopFixture) awaitWaited(t *testing.T) {
	t.Helper()
	select {
	case <-f.waited:
	case <-time.After(10 * time.Second):
		t.Fatal("workload did not exit")
	}
}

// awaitReady waits for the workload's ready marker.
func (f *stopFixture) awaitReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(f.dir, "ready")); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("workload not ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// checkJoined requires stop to have received the original Wait result before
// giving up ownership of the process.
func (f *stopFixture) checkJoined(t *testing.T) {
	t.Helper()
	f.p.mu.Lock()
	defer f.p.mu.Unlock()
	if f.p.cmd != nil || serverOwned(f.p) {
		t.Error("server still owned after stop received its Wait result")
	}
	if len(f.p.done) != 0 {
		t.Error("stop gave up the server without receiving its Wait result")
	}
	if f.p.stopped == nil || !f.p.stopped.waited || f.cmd.ProcessState == nil {
		t.Error("original server process was not waited for")
	}
}

// checkOwned requires the original process to remain owned and unjoined.
func (f *stopFixture) checkOwned(t *testing.T) {
	t.Helper()
	f.p.mu.Lock()
	defer f.p.mu.Unlock()
	if f.p.cmd != f.cmd || !serverOwned(f.p) || f.p.stopped == nil || f.p.stopped.waited {
		t.Error("server released without its Wait result")
	}
}

// TestServerProcessStopExited stops a server whose process has already been
// reaped, so Signal fails with os.ErrProcessDone. That is not a Wait result:
// stop must still receive the original one, and report the Signal failure
// with it.
func TestServerProcessStopExited(t *testing.T) {
	f := startStopFixture(t, "success", false)
	f.awaitWaited(t)
	_, _, err := f.p.stop(os.Interrupt)
	if !errors.Is(err, os.ErrProcessDone) {
		t.Errorf("stop: %v; want the Signal error", err)
	}
	if f.signals != 1 || f.kills != 0 {
		t.Errorf("%d signals, %d kills; want 1, 0", f.signals, f.kills)
	}
	f.checkJoined(t)
}

// TestServerProcessStopRunning is the positive control: Signal succeeds and
// stop returns the interrupted process's own Wait result, and a repeated stop
// returns that same outcome without touching the process.
func TestServerProcessStopRunning(t *testing.T) {
	f := startStopFixture(t, "hold", false)
	for i := range 2 {
		_, _, err := f.p.stop(os.Interrupt)
		var exit *exec.ExitError
		if errors.Is(err, os.ErrProcessDone) || !errors.As(err, &exit) {
			t.Errorf("stop %d: %v; want only the interrupted Wait result", i, err)
		}
	}
	if f.signals != 1 || f.kills != 0 {
		t.Errorf("%d signals, %d kills; want 1, 0", f.signals, f.kills)
	}
	f.checkJoined(t)
}

// TestServerProcessStopKillTimeout models a server whose Signal and Kill
// both report failure and whose Wait result is late. The interrupt is
// really delivered, so the workload ends; only its Wait result is held back.
// Later stops must neither signal nor kill again, only wait for the same
// Wait result, and return it with the recorded failures.
func TestServerProcessStopKillTimeout(t *testing.T) {
	f := startStopFixture(t, "hold", true)
	f.signalErr = errors.New("modeled signal failure")
	f.killErr = errors.New("modeled kill failure")
	f.modelKill = true
	f.p.drainBound = 100 * time.Millisecond
	f.p.killBound = time.Second
	for i := range 2 {
		_, _, err := f.p.stop(os.Interrupt)
		if !errors.Is(err, f.signalErr) || !errors.Is(err, f.killErr) {
			t.Errorf("unjoined stop %d: %v; want both recorded failures", i, err)
		}
		f.checkOwned(t)
	}
	f.awaitWaited(t)
	f.open()
	for i := range 2 {
		_, _, err := f.p.stop(os.Interrupt)
		var exit *exec.ExitError
		if !errors.Is(err, f.signalErr) || !errors.Is(err, f.killErr) || !errors.As(err, &exit) {
			t.Errorf("joined stop %d: %v; want the recorded failures with the original Wait result", i, err)
		}
	}
	if f.signals != 1 || f.kills != 1 {
		t.Errorf("%d signals, %d kills; want 1, 1", f.signals, f.kills)
	}
	f.checkJoined(t)
}

// TestServerProcessFinish is the positive control for the held namespace:
// a joined server's owned data directory is removed, with no hold.
func TestServerProcessFinish(t *testing.T) {
	f := startStopFixture(t, "hold", false)
	f.withData(t)
	err := f.p.finish(false)
	var exit *exec.ExitError
	if errors.Is(err, errServerHeld) || !errors.As(err, &exit) {
		t.Errorf("finish: %v; want only the interrupted Wait result", err)
	}
	if _, err := os.Stat(f.p.data); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("data directory after the join: %v; want it removed", err)
	}
	f.checkJoined(t)
}

// TestServerProcessHeldNamespace finishes a server whose original Wait
// result is unknown. The server and its data directory stay retained and the
// hold is reported. Once the same Wait result arrives, a later finish
// removes the directory.
func TestServerProcessHeldNamespace(t *testing.T) {
	f := startStopFixture(t, "hold", true)
	f.withData(t)
	f.modelKill = true
	f.p.drainBound = 100 * time.Millisecond
	f.p.killBound = time.Second
	if err := f.p.finish(false); !errors.Is(err, errServerHeld) {
		t.Errorf("finish: %v; want the hold reported", err)
	}
	if _, err := os.Stat(f.p.data); err != nil {
		t.Errorf("held data directory: %v", err)
	}
	f.checkOwned(t)
	f.awaitWaited(t)
	f.open()
	if err := f.p.finish(false); errors.Is(err, errServerHeld) {
		t.Errorf("finish after the join: %v; want no hold", err)
	}
	if _, err := os.Stat(f.p.data); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("data directory after the join: %v; want it removed", err)
	}
	if f.signals != 1 || f.kills != 1 {
		t.Errorf("%d signals, %d kills; want 1, 1", f.signals, f.kills)
	}
	f.checkJoined(t)
}

// TestServerProcessStopLiveOutput stops a server that keeps writing both
// streams while its Signal and Kill are modeled and undelivered. The
// unjoined stop must not read the output its copy goroutines are writing;
// run with -race to check that. After the fixture's real Kill, the joined
// stop returns the output.
func TestServerProcessStopLiveOutput(t *testing.T) {
	f := startStopFixture(t, "chatter", false)
	f.modelSignal, f.modelKill = true, true
	f.p.drainBound = 100 * time.Millisecond
	f.p.killBound = 100 * time.Millisecond
	f.awaitReady(t)
	out, diag, err := f.p.stop(os.Interrupt)
	if out != "" || diag != "" || err == nil {
		t.Errorf("unjoined stop: output %q, %q, error %v; want no output and an error", out, diag, err)
	}
	f.checkOwned(t)
	if err := f.deliverKill(); err != nil {
		t.Fatal(err)
	}
	f.p.killBound = 10 * time.Second
	out, diag, _ = f.p.stop(os.Interrupt)
	if !strings.Contains(out, "chatter") || !strings.Contains(diag, "chatter") {
		t.Errorf("joined stop output %q, %q; want the workload's output", out, diag)
	}
	if f.signals != 1 || f.kills != 1 {
		t.Errorf("%d signals, %d kills; want 1, 1", f.signals, f.kills)
	}
	f.checkJoined(t)
}

// TestServerProcessFixtureCleanupReports leaves a server running when its
// subtest ends. The fixture's registered cleanup must stop it and report the
// failed stop rather than discard it.
func TestServerProcessFixtureCleanupReports(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("stop sends os.Interrupt")
	}
	signalErr := errors.New("modeled signal failure")
	var reported []error
	var f *stopFixture
	t.Run("unstopped", func(t *testing.T) {
		f = startStopFixture(t, "hold", false)
		f.signalErr = signalErr
		f.report = func(err error) { reported = append(reported, err) }
	})
	if f == nil {
		return
	}
	if len(reported) != 1 || !errors.Is(reported[0], signalErr) {
		t.Errorf("registered cleanup reported %v; want the stop failure", reported)
	}
	f.checkJoined(t)
}

// TestServerProcessFinishKeepData finishes a joined server whose data
// directory it is told to keep: the directory stays and its retention is
// reported.
func TestServerProcessFinishKeepData(t *testing.T) {
	f := startStopFixture(t, "hold", false)
	f.withData(t)
	err := f.p.finish(true)
	if !errors.Is(err, errServerDataKept) || errors.Is(err, errServerHeld) {
		t.Errorf("finish: %v; want the kept data directory reported", err)
	}
	if _, err := os.Stat(f.p.data); err != nil {
		t.Errorf("kept data directory: %v", err)
	}
	f.checkJoined(t)
}

// TestServerProcessOwnersFull fills the owner table with owners that have no
// process. The next admission must be refused, with no owner dropped.
func TestServerProcessOwnersFull(t *testing.T) {
	var owners []*serverProcess
	t.Cleanup(func() {
		for _, p := range owners {
			p.retire()
		}
	})
	for {
		p := new(serverProcess)
		if p.admit() != nil {
			break
		}
		owners = append(owners, p)
		if len(owners) > serverOwnersMax {
			t.Fatalf("owner table admitted %d owners; bound is %d", len(owners), serverOwnersMax)
		}
	}
	extra := new(serverProcess)
	if extra.admit() == nil {
		extra.retire()
		t.Fatal("full owner table admitted another owner")
	}
	for i, p := range owners {
		if !serverOwned(p) {
			t.Errorf("owner %d dropped from the full table", i)
		}
	}
}

// faultProxy forwards to the server. An armed path has its next response
// discarded after the server has handled the request: the client loses the
// connection although the operation took effect.
type faultProxy struct {
	target *url.URL
	mu     sync.Mutex
	drop   map[string]int
}

func (p *faultProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rp := &httputil.ReverseProxy{
		Rewrite:       func(pr *httputil.ProxyRequest) { pr.SetURL(p.target) },
		FlushInterval: -1,
	}
	p.mu.Lock()
	drop := p.drop[r.URL.Path] > 0
	if drop {
		p.drop[r.URL.Path]--
	}
	p.mu.Unlock()
	if !drop {
		rp.ServeHTTP(w, r)
		return
	}
	rp.ServeHTTP(httptest.NewRecorder(), r)
	// Close the client connection without a response.
	panic(http.ErrAbortHandler)
}

func (p *faultProxy) command() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "discard the next proxied response for an RPC path", Args: "path"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		p.mu.Lock()
		p.drop[args[0]]++
		p.mu.Unlock()
		return nil, nil
	})
}

// background runs the CLI like executableScriptEngine, but also in the
// background, where its output can be awaited and it can be interrupted.
type background struct {
	mu      sync.Mutex
	process *bgProcess
}

type bgProcess struct {
	cmd    *exec.Cmd
	stdout *notifyBuffer
}

func (b *background) command(exe string, env []string) script.Cmd {
	return script.Command(script.CmdUsage{Summary: "run the CLI executable", Async: true}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		cmd := exec.CommandContext(s.Context(), exe, args...)
		cmd.Dir = s.Getwd()
		cmd.Env = append(s.Environ(), env...)
		out := newNotifyBuffer()
		var diag bytes.Buffer
		cmd.Stdout = out
		cmd.Stderr = &diag
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		b.mu.Lock()
		b.process = &bgProcess{cmd: cmd, stdout: out}
		b.mu.Unlock()
		return func(s *script.State) (string, string, error) {
			err := cmd.Wait()
			if setErr := s.Setenv("SKYGO_EXIT_CODE", strconv.Itoa(cmd.ProcessState.ExitCode())); setErr != nil {
				return out.String(), diag.String(), setErr
			}
			return out.String(), diag.String(), err
		}, nil
	})
}

func (b *background) last() (*bgProcess, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.process == nil {
		return nil, errors.New("no skygo command started")
	}
	return b.process, nil
}

func (b *background) awaitStdout() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "wait until the most recent skygo command's stdout matches", Args: "regexp"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		re, err := regexp.Compile(args[0])
		if err != nil {
			return nil, err
		}
		p, err := b.last()
		if err != nil {
			return nil, err
		}
		ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
		defer cancel()
		return nil, p.stdout.await(ctx, re)
	})
}

func (b *background) interrupt() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "send an interrupt to the most recent skygo command"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 0 {
			return nil, script.ErrUsage
		}
		p, err := b.last()
		if err != nil {
			return nil, err
		}
		return nil, p.cmd.Process.Signal(os.Interrupt)
	})
}

// notifyBuffer is a bytes.Buffer whose writes wake waiters.
type notifyBuffer struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	changed chan struct{}
}

func newNotifyBuffer() *notifyBuffer { return &notifyBuffer{changed: make(chan struct{})} }

func (b *notifyBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, err := b.buf.Write(p)
	close(b.changed)
	b.changed = make(chan struct{})
	return n, err
}

func (b *notifyBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *notifyBuffer) await(ctx context.Context, re *regexp.Regexp) error {
	for {
		b.mu.Lock()
		ok, changed := re.Match(b.buf.Bytes()), b.changed
		b.mu.Unlock()
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("stdout %q does not match %q: %w", b.String(), re, ctx.Err())
		case <-changed:
		}
	}
}

func coverageTaskfile() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "write a task running TestCoverageWorkload", Args: "file mode"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		exe, err := os.Executable()
		if err != nil {
			return nil, err
		}
		task := map[string]any{
			"argv": []string{exe, "-test.run=^TestCoverageWorkload$"},
			"env":  map[string]string{"SKYGO_COVERAGE_WORKLOAD": args[1], "SKYGO_COVERAGE_DIR": s.Getwd()},
		}
		data, err := json.Marshal(task)
		if err != nil {
			return nil, err
		}
		return nil, os.WriteFile(s.Path(args[0]), data, 0600)
	})
}

func awaitFile() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "wait until a file exists", Args: "file"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
		defer cancel()
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			if _, err := os.Stat(s.Path(args[0])); err == nil {
				return nil, nil
			} else if !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("%s: %w", args[0], ctx.Err())
			case <-tick.C:
			}
		}
	})
}

func jobID() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "save the previous JSON job ID in an environment variable", Args: "name"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		var job struct{ ID string }
		if err := json.Unmarshal([]byte(s.Stdout()), &job); err != nil {
			return nil, err
		}
		if job.ID == "" {
			return nil, errors.New("job ID is empty")
		}
		return nil, s.Setenv(args[0], job.ID)
	})
}
