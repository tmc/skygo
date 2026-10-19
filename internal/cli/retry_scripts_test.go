package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"google.golang.org/protobuf/proto"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// TestRetryWorkload uses files owned by one script to expose actual process
// execution. Neither the script nor this helper writes the daemon's database.
func TestRetryWorkload(t *testing.T) {
	mode := os.Getenv("SKYGO_RETRY_WORKLOAD")
	if mode == "" {
		return
	}
	dir := os.Getenv("SKYGO_RETRY_DIR")
	first, err := os.OpenFile(filepath.Join(dir, "first"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err == nil {
		first.Close()
		fmt.Println("attempt one")
		if mode == "hold" || mode == "follow" {
			// Release markers synchronize product behavior. This independent
			// bound prevents a failed cleanup from leaving an orphaned helper.
			deadline := time.Now().Add(2 * time.Minute)
			for {
				if _, err := os.Stat(filepath.Join(dir, "release")); err == nil {
					break
				} else if !errors.Is(err, os.ErrNotExist) {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(99)
				}
				if time.Now().After(deadline) {
					fmt.Fprintln(os.Stderr, "retry workload release marker timed out")
					os.Exit(99)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		os.Exit(7)
	}
	if !errors.Is(err, os.ErrExist) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(99)
	}
	second, err := os.OpenFile(filepath.Join(dir, "second"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(99)
	}
	second.Close()
	fmt.Println("attempt two")
	os.Exit(0)
}

// TestRetryScripts runs the production CLI and daemon. Linux is currently the
// only qualified local retry backend; validation and old-server tests below
// remain enabled on other platforms.
func TestRetryScripts(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("local retry requires qualified Linux nonreaping wait")
	}
	exe := retryScriptBinary(t)
	for _, name := range []string{"success", "follow", "cancel_backoff", "restart_unknown", "lost_response", "scoped", "original_request"} {
		t.Run(name, func(t *testing.T) { runRetryScript(t, exe, exe, "testdata/retry/"+name+".txt") })
	}
}

func TestRetryValidationScripts(t *testing.T) {
	exe := retryScriptBinary(t)
	runRetryScript(t, exe, exe, "testdata/retry/validation.txt")
}

// TestRetryOldServerScripts requires a source-archived S04 binary. Its SHA is
// provided by the caller and checked before execution; no old daemon is rebuilt
// from the current protobuf or used against a new-format database.
func TestRetryOldServerScripts(t *testing.T) {
	old, want := os.Getenv("SKYGO_S04_BINARY"), os.Getenv("SKYGO_S04_BINARY_SHA256")
	if old == "" || want == "" {
		t.Skip("set SKYGO_S04_BINARY and SKYGO_S04_BINARY_SHA256 to the archived S04 daemon")
	}
	old, err := filepath.Abs(old)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(old)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("S04 binary SHA256 = %s; want %s", got, want)
	}
	t.Logf("archived S04 binary SHA256 %s", want)
	exe := retryScriptBinary(t)
	runRetryScript(t, exe, old, "testdata/retry/old_server.txt")
}

func retryScriptBinary(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("builds and runs production CLI and daemon")
	}
	// A parent-qualified cross-compiled executable lets the same scripts run
	// on a host without a Go installation. It is an explicit test fixture,
	// never selected from PATH or silently substituted for a local build.
	fixture, want := os.Getenv("SKYGO_RETRY_BINARY"), os.Getenv("SKYGO_RETRY_BINARY_SHA256")
	if fixture != "" || want != "" {
		if fixture == "" || want == "" {
			t.Fatal("set both SKYGO_RETRY_BINARY and SKYGO_RETRY_BINARY_SHA256")
		}
		var err error
		fixture, err = filepath.Abs(fixture)
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(fixture)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		if got := hex.EncodeToString(sum[:]); got != want {
			t.Fatalf("retry CLI fixture SHA256 = %s; want %s", got, want)
		}
		t.Logf("provided retry CLI fixture SHA256 %s (coverage depends on fixture build)", want)
		return fixture
	}
	exe := filepath.Join(t.TempDir(), "skygo")
	args := []string{"build", "-o", exe}
	if mode := testing.CoverMode(); mode != "" {
		args = append(args, "-cover", "-covermode="+mode, "-coverpkg=github.com/tmc/skygo/...")
	}
	cmd := exec.Command("go", append(args, "../../cmd/skygo")...)
	cmd.Env = append(os.Environ(), "GOWORK=off", "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build retry CLI: %v\n%s", err, out)
	}
	return exe
}

func runRetryScript(t *testing.T, exe, daemon, file string) {
	t.Helper()
	var env []string
	if dir := coverDir(t); dir != "" {
		env = append(env, "GOCOVERDIR="+dir)
	}
	process := &serverProcess{exe: daemon, address: freeAddress(t), data: t.TempDir(), env: env}
	t.Cleanup(process.cleanup)
	authFile := filepath.Join(t.TempDir(), "auth.json")
	privateDir := func(label, dir string) error {
		info, err := os.Stat(dir)
		if err != nil {
			return err
		}
		t.Logf("scoped %s parent mode before: %04o", label, info.Mode().Perm())
		if err := os.Chmod(dir, 0700); err != nil {
			return err
		}
		info, err = os.Stat(dir)
		if err != nil {
			return err
		}
		t.Logf("scoped %s parent mode after: %04o", label, info.Mode().Perm())
		if !info.IsDir() || info.Mode().Perm() != 0700 {
			return fmt.Errorf("scoped %s parent is not private", label)
		}
		return nil
	}
	if filepath.Base(file) == "scoped.txt" {
		// Go's numbered TempDir children use 0777 before the process umask.
		// Authentication fixtures require private parents even with umask 0002.
		for label, dir := range map[string]string{"data": process.data, "auth": filepath.Dir(authFile)} {
			if err := privateDir(label, dir); err != nil {
				t.Fatal(err)
			}
		}
		process.args = []string{"--auth-file", authFile}
	}
	target := &url.URL{Scheme: "http", Host: process.address}
	proxy := httputil.NewSingleHostReverseProxy(target)
	var mu sync.Mutex
	calls := make(map[string]int)
	var submitted []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls[r.URL.Path]++
		mu.Unlock()
		if strings.HasSuffix(r.URL.Path, "/SubmitRetryJob") {
			data, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			if err != nil {
				http.Error(w, "read submission", 500)
				return
			}
			r.Body.Close()
			r.Body = io.NopCloser(bytes.NewReader(data))
			mu.Lock()
			submitted = append([]byte(nil), data...)
			mu.Unlock()
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	fault := &faultProxy{target: target, drop: make(map[string]int)}
	dropServer := httptest.NewServer(fault)
	t.Cleanup(dropServer.Close)
	e := executableScriptEngine(exe, nil, env)
	bg := &background{}
	e.Cmds["skygo"] = bg.command(exe, env)
	e.Cmds["await-stdout"] = bg.awaitStdout()
	e.Cmds["server"] = process.command()
	if filepath.Base(file) == "scoped.txt" {
		e.Cmds["private-workdir"] = script.Command(script.CmdUsage{Summary: "record and secure this script's token-output directory"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
			if len(args) != 0 {
				return nil, script.ErrUsage
			}
			return nil, privateDir("work", s.Getwd())
		})
	}
	e.Cmds["job-id"] = jobID()
	e.Cmds["await-file"] = awaitFile()
	e.Cmds["drop-response"] = fault.command()
	e.Cmds["retry-task"] = retryTaskfile(t)
	e.Cmds["await-state"] = retryAwaitState(exe, env)
	e.Cmds["history"] = retryHistory()
	e.Cmds["original-request"] = script.Command(script.CmdUsage{Summary: "check original task bytes forwarded through the retry method"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 0 {
			return nil, script.ErrUsage
		}
		mu.Lock()
		data := append([]byte(nil), submitted...)
		mu.Unlock()
		var body skyv1.SubmitJobRequest
		if err := proto.Unmarshal(data, &body); err != nil {
			return nil, err
		}
		task := body.GetTask()
		policy := task.GetRetry()
		codes := policy.GetExitCodes()
		if task.GetTarget() != "" || policy.GetDelaySeconds() != 0 || len(codes) != 3 || codes[0] != 7 || codes[1] != 3 || codes[2] != 7 {
			return nil, errors.New("CLI rewrote original retry task")
		}
		return nil, nil
	})
	e.Cmds["total-calls"] = script.Command(script.CmdUsage{Summary: "check all forwarded daemon calls", Args: "count"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		want, err := strconv.Atoi(args[0])
		if err != nil {
			return nil, err
		}
		mu.Lock()
		got := 0
		for _, n := range calls {
			got += n
		}
		mu.Unlock()
		if got != want {
			return nil, fmt.Errorf("total forwarded calls = %d; want %d", got, want)
		}
		return nil, nil
	})
	e.Cmds["calls"] = script.Command(script.CmdUsage{Summary: "check actual daemon RPC calls", Args: "method count"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		want, err := strconv.Atoi(args[1])
		if err != nil {
			return nil, err
		}
		mu.Lock()
		got := calls["/skygo.v1.ControlService/"+args[0]]
		mu.Unlock()
		if got != want {
			return nil, fmt.Errorf("%s calls = %d; want %d", args[0], got, want)
		}
		return nil, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	scripttest.Test(t, ctx, e, []string{"SKYGO_ENDPOINT=" + srv.URL, "SKYGO_TOKEN=coverage-token", "PROXY=" + dropServer.URL, "DATA=" + process.data, "AUTH=" + authFile}, file)
}

func retryTaskfile(t *testing.T) script.Cmd {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cover := coverDir(t)
	return script.Command(script.CmdUsage{Summary: "write task using the real retry workload helper", Args: "file mode delay"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 3 {
			return nil, script.ErrUsage
		}
		delay, err := strconv.Atoi(args[2])
		if err != nil {
			return nil, err
		}
		env := map[string]string{"SKYGO_RETRY_WORKLOAD": args[1], "SKYGO_RETRY_DIR": s.Getwd()}
		if cover != "" {
			env["GOCOVERDIR"] = cover
		}
		task := map[string]any{"argv": []string{exe, "-test.run=^TestRetryWorkload$"}, "env": env}
		if args[1] != "default" {
			codes := []int{7}
			if args[1] == "original" {
				codes = []int{7, 3, 7}
			}
			task["retry"] = map[string]any{"max_attempts": 2, "delay_seconds": delay, "exit_codes": codes}
		}
		data, err := json.Marshal(task)
		if err != nil {
			return nil, err
		}
		return nil, os.WriteFile(s.Path(args[0]), data, 0600)
	})
}

type retryScriptJob struct {
	ID       string `json:"id"`
	State    string `json:"state"`
	Attempts []struct {
		ID       string `json:"id"`
		Ordinal  uint32 `json:"ordinal"`
		State    string `json:"state"`
		ExitCode *int32 `json:"exit_code"`
	} `json:"attempts"`
	NextAttemptAt string `json:"next_attempt_at"`
}

// Await-state polls the public CLI; it never reads or manufactures durable rows.
func retryAwaitState(exe string, env []string) script.Cmd {
	return script.Command(script.CmdUsage{Summary: "wait for an actual CLI job state", Args: "job state"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		ctx, cancel := context.WithTimeout(s.Context(), 15*time.Second)
		defer cancel()
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			cmd := exec.CommandContext(ctx, exe, "--json", "jobs", "get", args[0])
			cmd.Env = append(s.Environ(), env...)
			out, err := cmd.Output()
			if err != nil {
				return nil, err
			}
			var j retryScriptJob
			if err := json.Unmarshal(out, &j); err != nil {
				return nil, err
			}
			if j.State == "JOB_STATE_"+strings.ToUpper(args[1]) {
				return func(*script.State) (string, string, error) { return string(out), "", nil }, nil
			}
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("await %s: last state %s: %w", args[1], j.State, ctx.Err())
			case <-tick.C:
			}
		}
	})
}

func retryHistory() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "check bounded immutable attempt history from previous JSON", Args: "state..."}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		var j retryScriptJob
		if err := json.Unmarshal([]byte(s.Stdout()), &j); err != nil {
			return nil, err
		}
		if len(j.Attempts) != len(args) {
			return nil, fmt.Errorf("history has %d attempts; want %d", len(j.Attempts), len(args))
		}
		seen := make(map[string]bool)
		for i, a := range j.Attempts {
			if a.ID == "" || (i == 0 && a.ID != j.ID) || (i > 0 && a.ID == j.ID) || seen[a.ID] || a.Ordinal != uint32(i+1) || a.State != "JOB_STATE_"+strings.ToUpper(args[i]) {
				return nil, fmt.Errorf("attempt %d has invalid identity/ordinal/state: %+v", i+1, a)
			}
			seen[a.ID] = true
			if args[i] == "failed" && (a.ExitCode == nil || *a.ExitCode != 7) {
				return nil, fmt.Errorf("failed attempt %d lacks exit 7", i+1)
			}
			if args[i] == "succeeded" && (a.ExitCode == nil || *a.ExitCode != 0) {
				return nil, fmt.Errorf("successful attempt %d lacks exit 0", i+1)
			}
		}
		if j.State != "JOB_STATE_RETRYING" && j.NextAttemptAt != "" {
			return nil, errors.New("non-retrying job retains retry deadline")
		}
		if j.State == "JOB_STATE_RETRYING" && j.NextAttemptAt == "" {
			return nil, errors.New("retrying job lacks retry deadline")
		}
		return nil, nil
	})
}
