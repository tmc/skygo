package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf16"

	"connectrpc.com/connect"
	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"google.golang.org/protobuf/encoding/protojson"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// TestScripts runs the grammar, preview, refusal and skygo acceptance
// scripts. They run this test binary as the actual skycompat command and
// the actual skygo client and server built from the root module.
func TestScripts(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, skygo: buildSkygo(t), stdin: map[*script.State]string{}, fakes: map[*script.State]*fake{}, servers: map[*script.State]*exec.Cmd{}}
	sentinel := listenSentinel(t)
	importDir, err := filepath.Abs(filepath.Join("..", "..", "..", "..", "skycompat", "testdata", "import"))
	if err != nil {
		t.Fatal(err)
	}
	e := script.NewEngine()
	e.Cmds = scripttest.DefaultCmds()
	e.Conds = scripttest.DefaultConds()
	// skycompat must not open connections: every endpoint and proxy it
	// might consult names a listener that counts accepted connections.
	e.Cmds["skycompat"] = h.program(exe, "SKYCOMPAT_TEST_HELPER=1", "SKYGO_ENDPOINT="+sentinel, "HTTP_PROXY="+sentinel, "HTTPS_PROXY="+sentinel, "ALL_PROXY="+sentinel)
	e.Cmds["skygo"] = h.program(h.skygo)
	e.Cmds["stdin"] = h.stdinCmd()
	e.Cmds["fakeserver"] = h.fakeCmd()
	e.Cmds["received"] = h.receivedCmd()
	e.Cmds["skygo-server"] = h.serverCmd()
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + t.TempDir(), "TMPDIR=" + os.TempDir(), "SKYGO_TOKEN=test-token", "IMPORT=" + importDir}
	t.Cleanup(h.close)
	scripttest.Test(t, context.Background(), e, env, "testdata/script/*.txt")
}

// buildSkygo builds the root module's skygo command. The go command is a
// labeled developer tool of this test, not of skycompat.
func buildSkygo(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	mod, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil || !strings.HasPrefix(string(mod), "module github.com/tmc/skygo\n") {
		t.Fatalf("root module not found at %s: %v", root, err)
	}
	exe := filepath.Join(t.TempDir(), "skygo")
	cmd := exec.Command("go", "build", "-o", exe, "./cmd/skygo")
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build skygo: %v\n%s", err, out)
	}
	return exe
}

// listenSentinel returns the URL of a loopback listener and fails the test
// at cleanup if anything connected to it.
func listenSentinel(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var accepted atomic.Int64
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			c.Close()
		}
	}()
	t.Cleanup(func() {
		l.Close()
		if n := accepted.Load(); n != 0 {
			t.Errorf("skycompat opened %d network connections", n)
		}
	})
	return "http://" + l.Addr().String()
}

type harness struct {
	t     *testing.T
	skygo string

	mu      sync.Mutex
	stdin   map[*script.State]string
	fakes   map[*script.State]*fake
	servers map[*script.State]*exec.Cmd
}

// program runs an executable with the script's environment plus env.
// It records the exit status in $EXIT.
func (h *harness) program(path string, env ...string) script.Cmd {
	return script.Command(script.CmdUsage{Summary: "run " + filepath.Base(path), Args: "args..."}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		cmd := exec.CommandContext(s.Context(), path, args...)
		cmd.Dir = s.Getwd()
		cmd.Env = append(s.Environ(), env...)
		h.mu.Lock()
		if file, ok := h.stdin[s]; ok {
			delete(h.stdin, s)
			b, err := os.ReadFile(s.Path(file))
			if err != nil {
				h.mu.Unlock()
				return nil, err
			}
			cmd.Stdin = bytes.NewReader(b)
		}
		h.mu.Unlock()
		var out, diag bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &diag
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		return func(s *script.State) (string, string, error) {
			err := cmd.Wait()
			if setErr := s.Setenv("EXIT", fmt.Sprint(cmd.ProcessState.ExitCode())); setErr != nil {
				return out.String(), diag.String(), setErr
			}
			return out.String(), diag.String(), err
		}, nil
	})
}

func (h *harness) stdinCmd() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "use file as the next command's standard input", Args: "file"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		h.mu.Lock()
		h.stdin[s] = args[0]
		h.mu.Unlock()
		return nil, nil
	})
}

// fake is a Connect server that records each call and each planned task.
// It implements only PlanTask and GetServerInfo. Discovery advertises the
// local target with workdir-snapshot-v1, so a workspace task can be planned,
// and no endpoint capability, so the artifact snapshot service is absent and
// a workspace task cannot be submitted.
type fake struct {
	skyv1connect.UnimplementedControlServiceHandler
	srv *httptest.Server
	mu  sync.Mutex
	log bytes.Buffer
}

func (f *fake) PlanTask(_ context.Context, r *connect.Request[skyv1.PlanTaskRequest]) (*connect.Response[skyv1.PlanTaskResponse], error) {
	b, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(r.Msg.GetTask())
	if err != nil {
		return nil, err
	}
	var indented bytes.Buffer
	if err := json.Indent(&indented, b, "", "  "); err != nil {
		return nil, err
	}
	f.mu.Lock()
	fmt.Fprintf(&f.log, "%s\n", asciiJSON(indented.String()))
	f.mu.Unlock()
	return connect.NewResponse(&skyv1.PlanTaskResponse{Target: "local"}), nil
}

func (f *fake) GetServerInfo(context.Context, *connect.Request[skyv1.GetServerInfoRequest]) (*connect.Response[skyv1.GetServerInfoResponse], error) {
	return connect.NewResponse(&skyv1.GetServerInfoResponse{Targets: []*skyv1.Target{{Name: "local", Kind: "local", Capabilities: []skyv1.Capability{skyv1.Capability_CAPABILITY_WORKDIR_SNAPSHOT_V1}}}}), nil
}

// asciiJSON escapes every non-ASCII character and DEL in JSON text, which
// keeps script expectations visible without changing the encoded values.
func asciiJSON(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r < 0x7f:
			b.WriteRune(r)
		case r > 0xffff:
			r1, r2 := utf16.EncodeRune(r)
			fmt.Fprintf(&b, `\u%04x\u%04x`, r1, r2)
		default:
			fmt.Fprintf(&b, `\u%04x`, r)
		}
	}
	return b.String()
}

func (h *harness) fakeCmd() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "start a recording Connect server and point SKYGO_ENDPOINT at it"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 0 {
			return nil, script.ErrUsage
		}
		f := &fake{}
		path, handler := skyv1connect.NewControlServiceHandler(f)
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			f.mu.Lock()
			fmt.Fprintf(&f.log, "%s\n", r.URL.Path)
			f.mu.Unlock()
			mux.ServeHTTP(w, r)
		}))
		h.mu.Lock()
		if h.fakes[s] != nil {
			h.mu.Unlock()
			f.srv.Close()
			return nil, errors.New("fake server already running")
		}
		h.fakes[s] = f
		h.mu.Unlock()
		return nil, s.Setenv("SKYGO_ENDPOINT", f.srv.URL)
	})
}

func (h *harness) receivedCmd() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "print and clear the calls the fake server received"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		h.mu.Lock()
		f := h.fakes[s]
		h.mu.Unlock()
		if f == nil || len(args) != 0 {
			return nil, script.ErrUsage
		}
		f.mu.Lock()
		out := f.log.String()
		f.log.Reset()
		f.mu.Unlock()
		return func(*script.State) (string, string, error) { return out, "", nil }, nil
	})
}

// serverCmd starts or stops the actual skygo server with a new data
// directory in the script's work directory.
func (h *harness) serverCmd() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "start or stop an actual skygo server", Args: "start|stop"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		switch args[0] {
		case "start":
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				return nil, err
			}
			addr := l.Addr().String()
			l.Close()
			cmd := exec.Command(h.skygo, "server", "--listen", addr, "--data-dir", s.Path("server-data"))
			cmd.Env = s.Environ() // SKYGO_TOKEN is the server's credential.
			var diag bytes.Buffer
			cmd.Stderr = &diag
			h.mu.Lock()
			if h.servers[s] != nil {
				h.mu.Unlock()
				return nil, errors.New("server already running")
			}
			if err := cmd.Start(); err != nil {
				h.mu.Unlock()
				return nil, err
			}
			h.servers[s] = cmd
			h.mu.Unlock()
			if err := ready(s.Context(), addr); err != nil {
				return nil, fmt.Errorf("%w\n%s", err, &diag)
			}
			return nil, s.Setenv("SKYGO_ENDPOINT", "http://"+addr)
		case "stop":
			h.mu.Lock()
			cmd := h.servers[s]
			delete(h.servers, s)
			h.mu.Unlock()
			if cmd == nil {
				return nil, errors.New("server not running")
			}
			return nil, stop(cmd)
		}
		return nil, script.ErrUsage
	})
}

func ready(ctx context.Context, addr string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	client := &http.Client{Timeout: time.Second}
	for {
		r, err := client.Get("http://" + addr + "/ready")
		if err == nil {
			r.Body.Close()
			if r.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("server not ready: %w", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// stop interrupts a server and waits for its graceful exit.
func stop(cmd *exec.Cmd) error {
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		return err
	case <-time.After(30 * time.Second):
		cmd.Process.Kill()
		<-done
		return errors.New("server did not stop")
	}
}

func (h *harness) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, f := range h.fakes {
		f.srv.Close()
	}
	for s, cmd := range h.servers {
		h.t.Errorf("script left a server running")
		stop(cmd)
		delete(h.servers, s)
	}
}
