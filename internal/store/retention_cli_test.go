package store_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// These tests run the production CLI against actual HTTP/RPC/control/store/local
// code with a test-only store clock. They do not qualify server.Run assembly.
func TestRetentionClockScripts(t *testing.T) {
	exe := retentionClockBinary(t)
	for _, name := range []string{"prune_dry_run", "prune_metadata_only_no_native", "request_epoch_expired", "logs_cursor_expired", "cleanup_failure_charge", "prune_obligation_output_only", "prune_attempt_history", "logs_lease_completion", "prune_active_protected", "prune_backoff_protected", "request_result_expired"} {
		t.Run(name, func(t *testing.T) {
			if name == "cleanup_failure_charge" && os.Geteuid() == 0 {
				t.Skip("real directory unlink permission denial requires a non-root gate")
			}
			if (name == "prune_attempt_history" || name == "prune_backoff_protected") && runtime.GOOS != "linux" {
				t.Skip("real retry execution requires qualified Linux; separate lease script remains cross-platform")
			}
			runRetentionClockScript(t, exe, name)
		})
	}
}

func TestRetentionClockWorkload(t *testing.T) {
	mode := os.Getenv("SKYGO_RETENTION_CLOCK_WORKLOAD")
	if mode == "" {
		return
	}
	dir := os.Getenv("SKYGO_RETENTION_CLOCK_DIR")
	f, err := os.OpenFile(filepath.Join(dir, "starts"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(99)
	}
	if _, err := f.WriteString("start\n"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(99)
	}
	f.Close()
	fmt.Println("clock workload output")
	if mode == "hold" {
		if err := os.WriteFile(filepath.Join(dir, "ready"), nil, 0600); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(99)
		}
		deadline := time.Now().Add(90 * time.Second)
		for {
			_, err := os.Stat(filepath.Join(dir, "release"))
			if err == nil {
				break
			}
			if !errors.Is(err, os.ErrNotExist) || time.Now().After(deadline) {
				fmt.Fprintln(os.Stderr, "owned workload release failed:", err)
				os.Exit(99)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if mode == "failonce" {
		f, err := os.OpenFile(filepath.Join(dir, "first"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err == nil {
			f.Close()
			fmt.Println("first attempt only")
			os.Exit(7)
		}
		if !errors.Is(err, os.ErrExist) {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(99)
		}
		fmt.Println("second attempt only")
	}
	os.Exit(0)
}

func retentionClockBinary(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("builds/runs actual CLI with real handler and injected test clock")
	}
	fixture, want := os.Getenv("SKYGO_RETENTION_BINARY"), os.Getenv("SKYGO_RETENTION_BINARY_SHA256")
	if fixture != "" || want != "" {
		if fixture == "" || len(want) != 64 {
			t.Fatal("provide retention binary and exact SHA256 together")
		}
		path, err := filepath.Abs(fixture)
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != want {
			t.Fatal("retention binary SHA256 mismatch")
		}
		t.Logf("provided CLI SHA256 %s; instrumentation is artifact-qualified", want)
		return path
	}
	exe := filepath.Join(t.TempDir(), "skygo")
	args := []string{"build", "-o", exe}
	if mode := testing.CoverMode(); mode != "" {
		args = append(args, "-cover", "-covermode="+mode, "-coverpkg=github.com/tmc/skygo/...")
	}
	buildctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(buildctx, "go", append(args, "../../cmd/skygo")...)
	cmd.WaitDelay = 5 * time.Second
	cmd.Env = append(os.Environ(), "GOWORK=off", "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, out)
	}
	return exe
}

func retentionClockCover(t *testing.T) string {
	if testing.CoverMode() == "" {
		return ""
	}
	root := t.TempDir()
	if f := flag.Lookup("test.gocoverdir"); f != nil && f.Value.String() != "" {
		root = f.Value.String()
	}
	dir, err := os.MkdirTemp(root, "retention-cli-children-")
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

type retentionClockGate struct {
	mu               sync.Mutex
	entered, release chan struct{}
	once             sync.Once
}

func (g *retentionClockGate) arm() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.release != nil {
		return errors.New("barrier already armed")
	}
	g.entered = make(chan struct{})
	g.release = make(chan struct{})
	g.once = sync.Once{}
	return nil
}
func (g *retentionClockGate) pass() {
	g.mu.Lock()
	if g.release == nil {
		g.mu.Unlock()
		return
	}
	release := g.release
	g.once.Do(func() { close(g.entered) })
	g.mu.Unlock()
	<-release
}
func (g *retentionClockGate) unblock() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.release != nil {
		close(g.release)
		g.release = nil
	}
}
func (g *retentionClockGate) await(ctx context.Context) error {
	g.mu.Lock()
	entered := g.entered
	g.mu.Unlock()
	if entered == nil {
		return errors.New("barrier is not armed")
	}
	select {
	case <-entered:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type retentionClockBackend struct {
	backend.Backend
	logs, start *retentionClockGate
	reads       *atomic.Int64
}

func (b retentionClockBackend) Start(ctx context.Context, id string, task backend.Task) error {
	b.start.pass()
	return b.Backend.Start(ctx, id, task)
}

func (b retentionClockBackend) Logs(ctx context.Context, ref backend.Reference, offset int64, limit int) (backend.LogChunk, error) {
	b.reads.Add(1)
	b.logs.pass()
	return b.Backend.Logs(ctx, ref, offset, limit)
}

type retentionClockFixture struct {
	t                            *testing.T
	dir, exe, route              string
	env                          []string
	s                            *store.Store
	b                            *local.Backend
	engine                       *auth.Engine
	actor                        auth.Actor
	now                          atomic.Int64
	initial                      store.RetentionInfo
	mu                           sync.Mutex
	c                            *control.Control
	handler                      http.Handler
	cancel                       context.CancelFunc
	done                         chan error
	native, output, reads        atomic.Int64
	removeResults                chan error
	logs, remove, http, starting retentionClockGate
}

func newRetentionClockFixture(t *testing.T, exe, name string) *retentionClockFixture {
	t.Helper()
	f := &retentionClockFixture{t: t, dir: t.TempDir(), exe: exe, route: "local", removeResults: make(chan error, 32)}
	if err := os.Chmod(f.dir, 0700); err != nil {
		t.Fatal(err)
	}
	policy := store.DefaultRetentionPolicy()
	policy.Window = time.Hour
	if name == "prune_dry_run" {
		policy.CleanupRecords = 1
	}
	if name == "request_result_expired" {
		policy.Jobs = 1
	}
	info, err := store.EnableRetention(f.dir, policy)
	if err != nil {
		t.Fatal(err)
	}
	f.initial = info
	f.s, err = store.Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		f.starting.unblock()
		f.logs.unblock()
		f.remove.unblock()
		f.http.unblock()
		if err := os.Chmod(filepath.Join(f.dir, "logs"), 0700); err != nil {
			t.Error(err)
		}
		if err := f.stop(); err != nil {
			t.Error(err)
		}
		if f.b != nil {
			if err := f.b.Close(); err != nil {
				t.Error(err)
			}
		}
		if err := f.s.Close(); err != nil {
			t.Error(err)
		}
	})
	f.now.Store(time.Now().UTC().UnixNano())
	store.SetClockForTest(f.s, func() time.Time { return time.Unix(0, f.now.Load()).UTC() })
	actual, enabled, err := f.s.RetentionPolicy()
	if err != nil || !enabled {
		t.Fatalf("actual retention policy: %v enabled%t", err, enabled)
	}
	f.b, err = local.NewRetained(filepath.Join(f.dir, "logs"), local.RetentionConfig{Owner: f.s.Owner(), OutputUnit: actual.OutputUnit, Volume: actual.Volume, OutputBytes: actual.OutputBytes, OutputDomains: actual.OutputDomains, OutputFiles: actual.OutputFiles})
	if err != nil {
		t.Fatal(err)
	}
	f.engine = auth.NewLegacy("retention-clock-token")
	f.actor, err = f.engine.Authenticate("Bearer retention-clock-token")
	if err != nil {
		t.Fatal(err)
	}
	if dir := retentionClockCover(t); dir != "" {
		f.env = []string{"GOCOVERDIR=" + dir}
	}
	f.rebuildControl()
	return f
}

func (f *retentionClockFixture) rebuildControl() {
	binding, err := backend.LocalBinding(f.s.Owner())
	if err != nil {
		f.t.Fatal(err)
	}
	callbacks := *f.b.Retention()
	remove := callbacks.RemoveOutput
	native := callbacks.RemoveExecution
	callbacks.RemoveOutput = func(ctx context.Context, ref backend.OutputReference) error {
		f.output.Add(1)
		f.remove.pass()
		err := remove(ctx, ref)
		select {
		case f.removeResults <- err:
		default:
		}
		return err
	}
	callbacks.RemoveExecution = func(ctx context.Context, ref backend.Reference) error { f.native.Add(1); return native(ctx, ref) }
	target := backend.Target{Name: f.route, Kind: "local", Binding: binding, Backend: retentionClockBackend{Backend: f.b, logs: &f.logs, start: &f.starting, reads: &f.reads}, SupportsRetry: f.b.SupportsRetry(), OutputStorage: backend.LocalOutput, OutputReservation: f.b.OutputReservation(), Retention: &callbacks}
	if err := f.s.BindOutputStorage(f.route, binding, backend.LocalOutput, f.b.OutputReservation()); err != nil {
		f.t.Fatal(err)
	}
	c, err := control.NewWithLocalOutput(f.s, []backend.Target{target}, f.engine, control.LocalOutputConfig{Target: f.route, Binding: binding, BindOutput: f.b.BindOutput, OutputUsage: func(ctx context.Context) (uint64, uint64, error) {
		u, err := f.b.OutputUsage(ctx)
		return u.AllocatedBytes, u.Files, err
	}})
	if err != nil {
		f.t.Fatal(err)
	}
	f.mu.Lock()
	f.c = c
	f.handler = rpc.NewHandler(c, f.engine)
	f.mu.Unlock()
}
func (f *retentionClockFixture) start() error {
	f.mu.Lock()
	if f.cancel != nil {
		f.mu.Unlock()
		return errors.New("controller already running")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	c := f.c
	go func() { done <- c.Run(ctx) }()
	f.cancel, f.done = cancel, done
	f.mu.Unlock()
	// Wait for the actual Run-owned filesystem sample; no fabricated metrics.
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for !c.StorageReady() {
		select {
		case err := <-done:
			// Preserve the result for stop and cleanup to join consistently.
			done <- err
			return fmt.Errorf("controller stopped before readiness: %v", err)
		case <-deadline.C:
			return errors.New("actual controller storage sample did not become ready")
		case <-tick.C:
		}
	}
	return nil
}
func (f *retentionClockFixture) stop() error {
	f.mu.Lock()
	cancel, done := f.cancel, f.done
	f.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case err := <-done:
		f.mu.Lock()
		if f.done == done {
			f.cancel, f.done = nil, nil
		}
		f.mu.Unlock()
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	case <-time.After(15 * time.Second):
		return errors.New("controller did not stop")
	}
}
func (f *retentionClockFixture) quiesce() error {
	f.mu.Lock()
	c := f.c
	f.mu.Unlock()
	jobs, more, err := c.List(f.actor, 100, "")
	if err != nil {
		return err
	}
	if more != "" {
		return errors.New("quiescence fixture exceeds bounded page")
	}
	for _, j := range jobs {
		if !j.Status.State.Terminal() {
			return errors.New("finish all workloads before quiescence")
		}
	}
	if err := f.stop(); err != nil {
		return err
	}
	f.rebuildControl()
	return nil
}
func (f *retentionClockFixture) serving(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	h := f.handler
	f.mu.Unlock()
	if strings.HasSuffix(r.URL.Path, "/StreamRetainedJobLogs") {
		h.ServeHTTP(retentionClockWriter{ResponseWriter: w, gate: &f.http}, r)
	} else {
		h.ServeHTTP(w, r)
	}
}

type retentionClockWriter struct {
	http.ResponseWriter
	gate *retentionClockGate
}

func (w retentionClockWriter) Write(p []byte) (int, error) {
	w.gate.pass()
	return w.ResponseWriter.Write(p)
}
func (w retentionClockWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
func (w retentionClockWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func runRetentionClockScript(t *testing.T, exe, name string) {
	f := newRetentionClockFixture(t, exe, name)
	server := httptest.NewServer(http.HandlerFunc(f.serving))
	t.Cleanup(func() {
		f.starting.unblock()
		f.logs.unblock()
		f.remove.unblock()
		f.http.unblock()
		server.Close()
	})
	e := script.NewEngine()
	e.Cmds = scripttest.DefaultCmds()
	e.Conds = scripttest.DefaultConds()
	e.Cmds["skygo"] = f.cliCommand()
	e.Cmds["skygo-stream"] = f.cliCommand()
	e.Cmds["controller"] = script.Command(script.CmdUsage{Summary: "control the real coordinator only at fixture barriers", Args: "start|quiesce"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		switch args[0] {
		case "start":
			return nil, f.start()
		case "quiesce":
			return nil, f.quiesce()
		}
		return nil, script.ErrUsage
	})
	e.Cmds["clock"] = script.Command(script.CmdUsage{Summary: "advance the actual store test clock without maintenance", Args: "accept|retry|legacy|rollback"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		f.mu.Lock()
		running := f.cancel != nil
		f.mu.Unlock()
		if running {
			return nil, errors.New("quiesce coordinator before domain-clock test")
		}
		var now time.Time
		switch args[0] {
		case "accept":
			now = f.initial.AcceptUntil
		case "retry":
			now = f.initial.RetryUntil
		case "legacy":
			now = f.initial.LegacyCutoff
		case "rollback":
			now = f.initial.AcceptUntil.Add(-2 * time.Hour)
		default:
			return nil, script.ErrUsage
		}
		f.now.Store(now.UnixNano())
		return nil, nil
	})
	e.Cmds["reopen-store"] = script.Command(script.CmdUsage{Summary: "reopen actual durable store with the same test clock"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 0 {
			return nil, script.ErrUsage
		}
		f.mu.Lock()
		running := f.cancel != nil
		f.mu.Unlock()
		if running {
			return nil, errors.New("quiesce before reopening store")
		}
		if err := f.s.Close(); err != nil {
			return nil, err
		}
		opened, err := store.Open(f.dir)
		if err != nil {
			return nil, err
		}
		f.s = opened
		store.SetClockForTest(f.s, func() time.Time { return time.Unix(0, f.now.Load()).UTC() })
		f.rebuildControl()
		return nil, nil
	})
	e.Cmds["replace-target"] = script.Command(script.CmdUsage{Summary: "reconstruct the real control with the original route removed"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 0 {
			return nil, script.ErrUsage
		}
		f.mu.Lock()
		running := f.cancel != nil
		f.mu.Unlock()
		if running {
			return nil, errors.New("quiesce before replacing target configuration")
		}
		f.route = "replacement"
		f.rebuildControl()
		return nil, nil
	})
	e.Cmds["same-json"] = script.Command(script.CmdUsage{Summary: "compare actual JSON independent of whitespace", Args: "file"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		before, err := os.ReadFile(s.Path(args[0]))
		if err != nil {
			return nil, err
		}
		var a, b any
		if err := json.Unmarshal(before, &a); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(s.Stdout()), &b); err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(a, b) {
			return nil, errors.New("JSON values differ")
		}
		return nil, nil
	})
	e.Cmds["equal-env"] = script.Command(script.CmdUsage{Summary: "compare two observed scalar values", Args: "name name"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		a, ok := s.LookupEnv(args[0])
		if !ok {
			return nil, errors.New("first observed value missing")
		}
		b, ok := s.LookupEnv(args[1])
		if !ok {
			return nil, errors.New("second observed value missing")
		}
		if a != b {
			return nil, fmt.Errorf("observed scalar values differ: %s != %s", a, b)
		}
		return nil, nil
	})
	e.Cmds["db-save"] = f.dbCommand(false)
	e.Cmds["db-unchanged"] = f.dbCommand(true)
	e.Cmds["taskfile"] = f.taskCommand()
	e.Cmds["json-value"] = clockJSONValue()
	e.Cmds["domain"] = script.Command(script.CmdUsage{Summary: "save actual discovered domain", Args: "variable"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		var v struct {
			Domain struct{ Lineage, Epoch string } `json:"domain"`
		}
		if err := json.Unmarshal([]byte(s.Stdout()), &v); err != nil {
			return nil, err
		}
		if len(v.Domain.Lineage) != 32 || v.Domain.Epoch == "" {
			return nil, errors.New("domain missing")
		}
		return nil, s.Setenv(args[0], v.Domain.Lineage+":"+v.Domain.Epoch)
	})
	e.Cmds["assert-native"] = script.Command(script.CmdUsage{Summary: "assert actual native cleanup invocation count", Args: "count"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		want, err := strconv.ParseInt(args[0], 10, 64)
		if err != nil {
			return nil, err
		}
		if f.native.Load() != want {
			return nil, fmt.Errorf("native cleanup calls=%d; want%d", f.native.Load(), want)
		}
		return nil, nil
	})
	e.Cmds["assert-output"] = script.Command(script.CmdUsage{Summary: "assert actual output cleanup invocation count", Args: "count"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		want, err := strconv.ParseInt(args[0], 10, 64)
		if err != nil {
			return nil, err
		}
		if got := f.output.Load(); got != want {
			return nil, fmt.Errorf("output cleanup calls=%d; want %d", got, want)
		}
		return nil, nil
	})
	e.Cmds["usage"] = script.Command(script.CmdUsage{Summary: "save an observed quota charge by domain", Args: "domain variable"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		var info struct {
			Admin struct {
				Usage []struct{ Domain, Used string }
			}
		}
		if err := json.Unmarshal([]byte(s.Stdout()), &info); err != nil {
			return nil, err
		}
		for _, u := range info.Admin.Usage {
			if u.Domain == "RETENTION_QUOTA_DOMAIN_"+args[0] {
				if u.Used == "" {
					u.Used = "0"
				}
				return nil, s.Setenv(args[1], u.Used)
			}
		}
		return nil, errors.New("quota domain missing from actual response")
	})
	e.Cmds["positive-env"] = script.Command(script.CmdUsage{Summary: "require an observed positive quota charge", Args: "variable"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		value, ok := s.LookupEnv(args[0])
		if !ok {
			return nil, errors.New("observed charge missing")
		}
		n, err := strconv.ParseUint(value, 10, 64)
		if err != nil || n == 0 {
			return nil, fmt.Errorf("observed charge is not positive: %q", value)
		}
		return nil, nil
	})
	e.Cmds["log-reads"] = script.Command(script.CmdUsage{Summary: "capture or compare actual backend log-read count", Args: "save|unchanged variable"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		value := strconv.FormatInt(f.reads.Load(), 10)
		if args[0] == "save" {
			return nil, s.Setenv(args[1], value)
		}
		if args[0] != "unchanged" {
			return nil, script.ErrUsage
		}
		before, ok := s.LookupEnv(args[1])
		if !ok || before != value {
			return nil, errors.New("actual backend log reads changed")
		}
		return nil, nil
	})
	e.Cmds["history-attempts"] = script.Command(script.CmdUsage{Summary: "bind two distinct actual attempts to the logical job", Args: "job first-variable second-variable"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 3 {
			return nil, script.ErrUsage
		}
		var job struct {
			ID       string
			Attempts []struct {
				ID      string
				Ordinal uint32
			}
		}
		if err := json.Unmarshal([]byte(s.Stdout()), &job); err != nil {
			return nil, err
		}
		if job.ID != args[0] || len(job.Attempts) != 2 {
			return nil, errors.New("expected exactly two actual attempts for the logical job")
		}
		for i, attempt := range job.Attempts {
			id, err := hex.DecodeString(attempt.ID)
			if err != nil || len(id) != 16 || attempt.ID != strings.ToLower(attempt.ID) || attempt.Ordinal != uint32(i+1) {
				return nil, errors.New("invalid actual attempt identity or ordinal")
			}
		}
		if job.Attempts[0].ID != job.ID || job.Attempts[1].ID == job.ID {
			return nil, errors.New("first attempt must equal logical ID and second must be distinct")
		}
		if err := s.Setenv(args[1], job.Attempts[0].ID); err != nil {
			return nil, err
		}
		return nil, s.Setenv(args[2], job.Attempts[1].ID)
	})
	e.Cmds["attempt-logs"] = script.Command(script.CmdUsage{Summary: "bind every actual log message and distinguish selected output", Args: "attempt-id [required-text forbidden-text]"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 && len(args) != 3 {
			return nil, script.ErrUsage
		}
		decoder := json.NewDecoder(strings.NewReader(s.Stdout()))
		var data bytes.Buffer
		messages := 0
		for {
			var message struct {
				AttemptID string `json:"attempt_id"`
				Data      []byte
				Cursor    string
			}
			err := decoder.Decode(&message)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			if message.AttemptID != args[0] || message.Cursor == "" {
				return nil, errors.New("selected log response escaped its actual fixed attempt")
			}
			data.Write(message.Data)
			messages++
		}
		if messages == 0 {
			return nil, errors.New("no actual selected log messages")
		}
		if len(args) == 3 && (!bytes.Contains(data.Bytes(), []byte(args[1])) || bytes.Contains(data.Bytes(), []byte(args[2]))) {
			return nil, errors.New("selected log output belongs to another attempt or lacks its own marker")
		}
		return nil, nil
	})
	e.Cmds["cursor"] = script.Command(script.CmdUsage{Summary: "save last cursor from actual JSON log messages", Args: "variable"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		decoder := json.NewDecoder(strings.NewReader(s.Stdout()))
		last := ""
		for decoder.More() {
			var value struct{ Cursor string }
			if err := decoder.Decode(&value); err != nil {
				return nil, err
			}
			if value.Cursor == "" {
				return nil, errors.New("actual log message has no cursor")
			}
			last = value.Cursor
		}
		if last == "" {
			return nil, errors.New("no actual log cursor")
		}
		return nil, s.Setenv(args[0], last)
	})
	e.Cmds["barrier"] = f.barrierCommand()
	e.Cmds["await-state"] = f.awaitCommand(false)
	e.Cmds["await-output-usage"] = f.awaitCommand(true)
	e.Cmds["output-permission"] = script.Command(script.CmdUsage{Summary: "change only the owned output directory write permission", Args: "deny|restore"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		if os.Geteuid() == 0 {
			return nil, errors.New("permission-denial fixture requires non-root execution")
		}
		mode := os.FileMode(0500)
		if args[0] == "restore" {
			mode = 0700
		} else if args[0] != "deny" {
			return nil, script.ErrUsage
		}
		return nil, os.Chmod(filepath.Join(f.dir, "logs"), mode)
	})
	e.Cmds["await-prunable"] = script.Command(script.CmdUsage{Summary: "wait for actual dry-run selection after real stream completion"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 0 {
			return nil, script.ErrUsage
		}
		ctx, cancel := context.WithTimeout(s.Context(), 15*time.Second)
		defer cancel()
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			cmd := exec.CommandContext(ctx, f.exe, "--json", "jobs", "prune", "--before", "9999-12-31T23:59:59Z", "--metadata-only", "--ack-unverified-terminal")
			cmd.Env = append(s.Environ(), f.env...)
			data, err := cmd.Output()
			if err != nil {
				return nil, err
			}
			var result struct{ Selected uint32 }
			if err := json.Unmarshal(data, &result); err != nil {
				return nil, err
			}
			if result.Selected == 1 {
				return nil, nil
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-tick.C:
			}
		}
	})
	e.Cmds["await-remove-failure"] = script.Command(script.CmdUsage{Summary: "wait for actual local RemoveOutput failure"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 0 {
			return nil, script.ErrUsage
		}
		ctx, cancel := context.WithTimeout(s.Context(), 15*time.Second)
		defer cancel()
		select {
		case err := <-f.removeResults:
			if err == nil {
				return nil, errors.New("actual RemoveOutput unexpectedly succeeded")
			}
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	scripttest.Test(t, ctx, e, []string{"SKYGO_ENDPOINT=" + server.URL, "SKYGO_TOKEN=retention-clock-token", "DATA=" + f.dir}, "testdata/retention-clock/"+name+".txt")
}

func (f *retentionClockFixture) cliCommand() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "execute the real production CLI", Async: true}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		cmd := exec.CommandContext(s.Context(), f.exe, args...)
		cmd.Dir = s.Getwd()
		cmd.Env = append(s.Environ(), f.env...)
		var out, diag bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &diag
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		return func(s *script.State) (string, string, error) {
			err := cmd.Wait()
			if cmd.ProcessState != nil {
				if e := s.Setenv("SKYGO_EXIT_CODE", strconv.Itoa(cmd.ProcessState.ExitCode())); e != nil {
					return out.String(), diag.String(), e
				}
			}
			return out.String(), diag.String(), err
		}, nil
	})
}
func (f *retentionClockFixture) dbCommand(compare bool) script.Cmd {
	return script.Command(script.CmdUsage{Summary: "hash the same open quiescent DB around an actual RPC", Args: "snapshot"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		f.mu.Lock()
		running := f.cancel != nil
		f.mu.Unlock()
		if running {
			return nil, errors.New("DB byte comparison requires no Run/audit goroutine")
		}
		data, err := os.ReadFile(filepath.Join(f.dir, "jobs.db"))
		if err != nil {
			return nil, err
		}
		if !compare {
			return nil, os.WriteFile(s.Path(args[0]), data, 0600)
		}
		before, err := os.ReadFile(s.Path(args[0]))
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(before, data) {
			return nil, errors.New("actual open database bytes changed")
		}
		return nil, nil
	})
}
func (f *retentionClockFixture) taskCommand() script.Cmd {
	exe, err := os.Executable()
	if err != nil {
		f.t.Fatal(err)
	}
	return script.Command(script.CmdUsage{Summary: "write actual Go workload task", Args: "file success|hold|failonce|backoff"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		if args[1] != "success" && args[1] != "hold" && args[1] != "failonce" && args[1] != "backoff" {
			return nil, script.ErrUsage
		}
		mode := args[1]
		if mode == "backoff" {
			mode = "failonce"
		}
		env := map[string]string{"SKYGO_RETENTION_CLOCK_WORKLOAD": mode, "SKYGO_RETENTION_CLOCK_DIR": s.Getwd(), "RETENTION_PRIVATE_PAYLOAD": "must-not-appear"}
		for _, entry := range f.env {
			key, value, _ := strings.Cut(entry, "=")
			env[key] = value
		}
		task := map[string]any{"argv": []string{exe, "-test.run=^TestRetentionClockWorkload$"}, "env": env}
		if args[1] == "failonce" || args[1] == "backoff" {
			if runtime.GOOS != "linux" {
				return nil, errors.New("actual retry helper requires qualified Linux")
			}
			delay := 1
			if args[1] == "backoff" {
				delay = 60
			}
			task["retry"] = map[string]any{"max_attempts": 2, "delay_seconds": delay, "exit_codes": []int{7}}
		}
		data, err := json.Marshal(task)
		if err != nil {
			return nil, err
		}
		return nil, os.WriteFile(s.Path(args[0]), data, 0600)
	})
}
func clockJSONValue() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "save scalar from actual JSON", Args: "path variable"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		var value any
		if err := json.Unmarshal([]byte(s.Stdout()), &value); err != nil {
			return nil, err
		}
		for _, key := range strings.Split(args[0], ".") {
			switch v := value.(type) {
			case map[string]any:
				var ok bool
				value, ok = v[key]
				if !ok {
					return nil, errors.New("JSON field missing")
				}
			case []any:
				n, err := strconv.Atoi(key)
				if err != nil || n < 0 || n >= len(v) {
					return nil, errors.New("JSON array index invalid")
				}
				value = v[n]
			default:
				return nil, errors.New("JSON path invalid")
			}
		}
		switch v := value.(type) {
		case string:
			return nil, s.Setenv(args[1], v)
		case bool:
			return nil, s.Setenv(args[1], strconv.FormatBool(v))
		case float64:
			return nil, s.Setenv(args[1], strconv.FormatFloat(v, 'f', -1, 64))
		default:
			return nil, errors.New("JSON value is not scalar")
		}
	})
}
func (f *retentionClockFixture) barrierCommand() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "control actual callback or HTTP completion barriers", Args: "start|logs|remove|http arm|await|release"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		var gate *retentionClockGate
		switch args[0] {
		case "start":
			gate = &f.starting
		case "logs":
			gate = &f.logs
		case "remove":
			gate = &f.remove
		case "http":
			gate = &f.http
		default:
			return nil, script.ErrUsage
		}
		switch args[1] {
		case "arm":
			return nil, gate.arm()
		case "release":
			gate.unblock()
			return nil, nil
		case "await":
			ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
			defer cancel()
			return nil, gate.await(ctx)
		}
		return nil, script.ErrUsage
	})
}
func (f *retentionClockFixture) awaitCommand(output bool) script.Cmd {
	return script.Command(script.CmdUsage{Summary: "poll only actual CLI job or output quota", Args: "job state | used"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if output && len(args) != 1 || !output && len(args) != 2 {
			return nil, script.ErrUsage
		}
		ctx, cancel := context.WithTimeout(s.Context(), 15*time.Second)
		defer cancel()
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			argv := []string{"--json", "jobs", "retention"}
			if !output {
				argv = []string{"--json", "jobs", "get", args[0]}
			}
			cmd := exec.CommandContext(ctx, f.exe, argv...)
			cmd.Env = append(s.Environ(), f.env...)
			data, err := cmd.Output()
			if err != nil {
				return nil, err
			}
			matched := false
			if !output {
				var j struct{ State string }
				if err := json.Unmarshal(data, &j); err != nil {
					return nil, err
				}
				matched = j.State == "JOB_STATE_"+strings.ToUpper(args[1])
			} else {
				var info struct {
					Admin struct {
						Usage []struct{ Domain, Used string }
					}
				}
				if err := json.Unmarshal(data, &info); err != nil {
					return nil, err
				}
				for _, u := range info.Admin.Usage {
					if u.Domain == "RETENTION_QUOTA_DOMAIN_OUTPUT_BYTES" {
						used := u.Used
						if used == "" {
							used = "0"
						}
						matched = used == args[0]
					}
				}
			}
			if matched {
				return func(*script.State) (string, string, error) { return string(data), "", nil }, nil
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-tick.C:
			}
		}
	})
}
