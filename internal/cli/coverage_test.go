package cli

import (
	"bytes"
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
	data := t.TempDir()
	address := freeAddress(t)
	var env []string
	if dir := coverDir(t); dir != "" {
		env = append(env, "GOCOVERDIR="+dir)
	}
	srv := &serverProcess{exe: exe, address: address, data: data, env: env}
	t.Cleanup(srv.cleanup)
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

	mu   sync.Mutex
	cmd  *exec.Cmd
	out  bytes.Buffer
	diag bytes.Buffer
	done chan error
}

func (p *serverProcess) command() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "start or stop the server executable", Args: "start|stop"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		switch args[0] {
		case "start":
			return nil, p.start(s)
		case "stop":
			out, diag, err := p.stop(os.Interrupt)
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
	cmd := exec.Command(p.exe, "server", "--listen", p.address, "--data-dir", p.data, "--token", "coverage-token")
	cmd.Env = append(s.Environ(), p.env...)
	p.out.Reset()
	p.diag.Reset()
	cmd.Stdout = &p.out
	cmd.Stderr = &p.diag
	if err := cmd.Start(); err != nil {
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
			return fmt.Errorf("server exited during startup: %v\n%s", err, &p.diag)
		case <-ctx.Done():
			return fmt.Errorf("server not ready: %w", ctx.Err())
		case <-tick.C:
		}
	}
}

// stop signals the server and waits for it to exit, bounded by the server's
// own drain limits plus a margin.
func (p *serverProcess) stop(sig os.Signal) (string, string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd == nil {
		return "", "", errors.New("server not running")
	}
	cmd := p.cmd
	p.cmd = nil
	if err := cmd.Process.Signal(sig); err != nil {
		return "", "", err
	}
	select {
	case err := <-p.done:
		return p.out.String(), p.diag.String(), err
	case <-time.After(30 * time.Second):
		cmd.Process.Kill()
		<-p.done
		return p.out.String(), p.diag.String(), errors.New("server did not stop")
	}
}

func (p *serverProcess) cleanup() {
	p.mu.Lock()
	running := p.cmd != nil
	p.mu.Unlock()
	if running {
		p.stop(os.Interrupt)
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
