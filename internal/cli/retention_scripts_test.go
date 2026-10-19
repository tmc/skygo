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
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/store"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// TestRetentionWorkload creates only script-owned execution markers and output.
// Retention decisions, clocks and durable state stay in production code.
func TestRetentionWorkload(t *testing.T) {
	mode := os.Getenv("SKYGO_RETENTION_WORKLOAD")
	if mode == "" {
		return
	}
	dir := os.Getenv("SKYGO_RETENTION_WORKDIR")
	f, err := os.OpenFile(filepath.Join(dir, "starts"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(99)
	}
	if _, err = f.WriteString("start\n"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(99)
	}
	if err = f.Close(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(99)
	}
	fmt.Println("retained workload output")
	if mode == "failonce" {
		first, err := os.OpenFile(filepath.Join(dir, "first-retry"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err == nil {
			first.Close()
			os.Exit(7)
		}
		if !errors.Is(err, os.ErrExist) {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(99)
		}
	}
	if mode == "hold" {
		if err := os.WriteFile(filepath.Join(dir, "ready"), nil, 0600); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(99)
		}
		deadline := time.Now().Add(2 * time.Minute)
		for {
			if _, err := os.Stat(filepath.Join(dir, "release")); err == nil {
				break
			} else if !errors.Is(err, os.ErrNotExist) {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(99)
			}
			if time.Now().After(deadline) {
				fmt.Fprintln(os.Stderr, "workload release timed out")
				os.Exit(99)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	os.Exit(0)
}

func retentionScriptBinary(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("builds and runs actual production CLI and daemon")
	}
	fixture, want := os.Getenv("SKYGO_RETENTION_BINARY"), os.Getenv("SKYGO_RETENTION_BINARY_SHA256")
	if fixture != "" || want != "" {
		return retentionPinnedBinary(t, fixture, want)
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
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOWORK=off")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build retention CLI: %v\n%s", err, out)
	}
	return exe
}

func retentionPinnedBinary(t *testing.T, path, want string) string {
	t.Helper()
	if path == "" || len(want) != 64 {
		t.Fatal("provide both the executable path and exact SHA256")
	}
	path, err := filepath.Abs(path)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != want {
		t.Fatalf("executable SHA256 mismatch: %s", filepath.Base(path))
	}
	t.Logf("executable %s SHA256 %s; fixture instrumentation is caller-qualified", filepath.Base(path), want)
	return path
}

func TestRetentionDaemonScripts(t *testing.T) {
	exe := retentionScriptBinary(t)
	for _, name := range []string{"retention_disabled", "retention_validation", "prune_quota_recovery", "prune_unknown_protected", "request_result_expired", "request_scope_isolation", "prune_restart", "pagination_holes", "compact_restore_lineage", "retention_discovery_faults"} {
		t.Run(name, func(t *testing.T) { runRetentionDaemonScript(t, exe, exe, "testdata/retention/"+name+".txt", "") })
	}
}

func TestRetentionS05Scripts(t *testing.T) {
	old := retentionPinnedBinary(t, os.Getenv("SKYGO_S05_BINARY"), os.Getenv("SKYGO_S05_BINARY_SHA256"))
	exe := retentionScriptBinary(t)
	runRetentionDaemonScript(t, exe, old, "testdata/retention/retention_old_server.txt", old)
	runRetentionDaemonScript(t, exe, exe, "testdata/retention/retention_successor_refusal.txt", old)
}

func runRetentionDaemonScript(t *testing.T, exe, daemon, file, old string) {
	t.Helper()
	var env []string
	if dir := coverDir(t); dir != "" {
		env = append(env, "GOCOVERDIR="+dir)
	}
	data := t.TempDir()
	authFile := filepath.Join(t.TempDir(), "auth.json")
	for _, dir := range []string{data, filepath.Dir(authFile)} {
		if err := os.Chmod(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	process := &serverProcess{exe: daemon, address: freeAddress(t), data: data, env: env}
	if filepath.Base(file) == "request_scope_isolation.txt" {
		process.args = []string{"--auth-file", authFile}
	}
	t.Cleanup(process.cleanup)
	t.Cleanup(func() {
		if err := os.Chmod(filepath.Join(data, "logs"), 0700); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Error(err)
		}
	})
	target := &url.URL{Scheme: "http", Host: process.address}
	proxy := httputil.NewSingleHostReverseProxy(target)
	var mu sync.Mutex
	calls := make(map[string]int)
	discoveryFault := ""
	var freshDiagnostic *notifyBuffer
	var freshOrderErr error
	var freshObserved bool
	proxy.ModifyResponse = func(response *http.Response) error {
		if !strings.HasSuffix(response.Request.URL.Path, "/GetRetention") {
			return nil
		}
		mu.Lock()
		fault := discoveryFault
		discoveryFault = ""
		mu.Unlock()
		if fault == "" {
			return nil
		}
		if err := response.Body.Close(); err != nil {
			return err
		}
		body := "{"
		if fault == "permission" {
			response.StatusCode = http.StatusForbidden
			body = `{"code":"permission_denied","message":"owned proxy rejected discovery"}`
		}
		response.Body = io.NopCloser(strings.NewReader(body))
		response.ContentLength = int64(len(body))
		response.Header.Set("Content-Type", "application/json")
		response.Header.Set("Content-Length", strconv.Itoa(len(body)))
		return nil
	}
	counted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls[r.URL.Path]++
		var receipt *notifyBuffer
		if strings.HasSuffix(r.URL.Path, "/SubmitRetainedJob") && freshDiagnostic != nil {
			receipt, freshDiagnostic = freshDiagnostic, nil
		}
		mu.Unlock()
		if receipt != nil {
			// Hold actual acceptance until the child has emitted its complete
			// original generated hint. Printing only after a failed response
			// cannot satisfy this barrier; no request is manufactured here.
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			err := receipt.await(ctx, regexp.MustCompile(`^request-domain: [0-9a-f]{32}:[1-9][0-9]*
request-id: [0-9a-f]{32}
task-file: "task.json"
`))
			cancel()
			if err != nil {
				mu.Lock()
				freshOrderErr = err
				mu.Unlock()
				http.Error(w, "complete fresh hint was absent before acceptance", http.StatusInternalServerError)
				return
			}
			mu.Lock()
			freshObserved = true
			mu.Unlock()
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(counted.Close)
	countedTarget, err := url.Parse(counted.URL)
	if err != nil {
		t.Fatal(err)
	}
	// Faulted calls still cross the single actual forwarded-RPC counter.
	fault := &faultProxy{target: countedTarget, drop: make(map[string]int)}
	faulted := httptest.NewServer(fault)
	t.Cleanup(faulted.Close)
	e := executableScriptEngine(exe, nil, env)
	e.Conds["non-root"] = script.Condition("real unlink permission denial requires a non-root owner", func(*script.State) (bool, error) { return os.Geteuid() != 0, nil })
	e.Conds["linux-retry"] = script.Condition("actual local retry execution is qualified on Linux", func(*script.State) (bool, error) { return runtime.GOOS == "linux", nil })
	e.Cmds["discovery-fault"] = script.Command(script.CmdUsage{Summary: "corrupt or deny one real daemon discovery response at the proxy", Args: "malformed|permission"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 || (args[0] != "malformed" && args[0] != "permission") {
			return nil, script.ErrUsage
		}
		mu.Lock()
		defer mu.Unlock()
		if discoveryFault != "" {
			return nil, errors.New("discovery fault already armed")
		}
		discoveryFault = args[0]
		return nil, nil
	})
	e.Cmds["fresh-run"] = script.Command(script.CmdUsage{Summary: "run the actual fresh CLI with a pre-acceptance diagnostic barrier", Async: true}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 0 {
			return nil, script.ErrUsage
		}
		diagnostic := newNotifyBuffer()
		mu.Lock()
		if freshDiagnostic != nil {
			mu.Unlock()
			return nil, errors.New("fresh request barrier already armed")
		}
		freshDiagnostic, freshOrderErr, freshObserved = diagnostic, nil, false
		mu.Unlock()
		cmd := exec.CommandContext(s.Context(), exe, "--endpoint", faulted.URL, "run", "-f", "task.json")
		cmd.Dir = s.Getwd()
		cmd.Env = append(s.Environ(), env...)
		var output bytes.Buffer
		cmd.Stdout, cmd.Stderr = &output, diagnostic
		if err := cmd.Start(); err != nil {
			mu.Lock()
			freshDiagnostic = nil
			mu.Unlock()
			return nil, err
		}
		return func(*script.State) (string, string, error) {
			err := cmd.Wait()
			mu.Lock()
			orderErr, missing := freshOrderErr, freshDiagnostic != nil
			freshDiagnostic = nil
			mu.Unlock()
			if orderErr != nil {
				err = fmt.Errorf("fresh hint was not observed before actual acceptance: %w", orderErr)
			} else if missing {
				err = errors.New("fresh CLI never reached actual retained submission")
			}
			return output.String(), diagnostic.String(), err
		}, nil
	})
	e.Cmds["fresh-order"] = script.Command(script.CmdUsage{Summary: "require complete original hint before actual fresh acceptance"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 0 {
			return nil, script.ErrUsage
		}
		mu.Lock()
		observed, err := freshObserved, freshOrderErr
		mu.Unlock()
		if !observed || err != nil {
			return nil, errors.New("fresh request hint did not precede actual acceptance")
		}
		return nil, nil
	})
	e.Cmds["save-fresh-hint"] = script.Command(script.CmdUsage{Summary: "capture the actual generated domain/identity/task receipt", Args: "expected-domain receipt"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		lines := strings.SplitN(s.Stderr(), "\n", 4)
		if len(lines) != 4 || lines[0] != "request-domain: "+args[0] || !strings.HasPrefix(lines[1], "request-id: ") || !strings.HasPrefix(lines[2], "task-file: ") {
			return nil, errors.New("complete fresh request hint was not printed before the lost-response diagnostic")
		}
		id := strings.TrimPrefix(lines[1], "request-id: ")
		if !validHexID(id) {
			return nil, errors.New("fresh request ID is not an actual generated identity")
		}
		file, err := strconv.Unquote(strings.TrimPrefix(lines[2], "task-file: "))
		if err != nil || file != "task.json" {
			return nil, errors.New("fresh hint changed its original task file")
		}
		if err := os.WriteFile(s.Path(args[1]), []byte(strings.Join(lines[:3], "\n")+"\n"), 0600); err != nil {
			return nil, err
		}
		for _, value := range [][2]string{{"FRESH_DOMAIN", args[0]}, {"FRESH_ID", id}, {"FRESH_TASK", file}} {
			if err := s.Setenv(value[0], value[1]); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	e.Cmds["same-hint"] = script.Command(script.CmdUsage{Summary: "compare the replay receipt to the complete original fresh hint", Args: "receipt"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		before, err := os.ReadFile(s.Path(args[0]))
		if err != nil {
			return nil, err
		}
		lines := strings.SplitN(s.Stderr(), "\n", 4)
		if len(lines) < 3 || string(before) != strings.Join(lines[:3], "\n")+"\n" {
			return nil, errors.New("replay did not preserve the complete original request hint")
		}
		return nil, nil
	})
	e.Cmds["output-permission"] = script.Command(script.CmdUsage{Summary: "hold only the owned output unlink permission", Args: "deny|restore"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 || (args[0] != "deny" && args[0] != "restore") {
			return nil, script.ErrUsage
		}
		if os.Geteuid() == 0 {
			return nil, errors.New("actual unlink permission denial requires a non-root owner")
		}
		mode := os.FileMode(0500)
		if args[0] == "restore" {
			mode = 0700
		}
		process.mu.Lock()
		dir := process.data
		process.mu.Unlock()
		return nil, os.Chmod(filepath.Join(dir, "logs"), mode)
	})
	e.Cmds["cleanup-checkpoint"] = retentionCleanupCheckpoint(process)
	e.Cmds["server"] = process.command()
	e.Cmds["job-id"] = jobID()
	e.Cmds["await-file"] = awaitFile()
	e.Cmds["drop-response"] = fault.command()
	e.Cmds["taskfile"] = retentionTaskfile(t)
	e.Cmds["private-workdir"] = script.Command(script.CmdUsage{Summary: "secure this script's credential parent"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 0 {
			return nil, script.ErrUsage
		}
		return nil, os.Chmod(s.Getwd(), 0700)
	})
	e.Cmds["retention-domain"] = retentionDomainCommand()
	e.Cmds["json-field"] = retentionJSONField()
	e.Cmds["retention-usage"] = retentionUsageCommand()
	e.Cmds["jobs-exact"] = script.Command(script.CmdUsage{Summary: "compare the exact actual list order and members", Args: "id..."}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		var response struct{ Jobs []struct{ ID string } }
		if err := json.Unmarshal([]byte(s.Stdout()), &response); err != nil {
			return nil, err
		}
		if len(response.Jobs) != len(args) {
			return nil, fmt.Errorf("actual list has %d jobs; want %d", len(response.Jobs), len(args))
		}
		for i, job := range response.Jobs {
			if job.ID != args[i] {
				return nil, fmt.Errorf("job at index %d=%s; want %s", i, job.ID, args[i])
			}
		}
		return nil, nil
	})
	e.Cmds["event-count"] = script.Command(script.CmdUsage{Summary: "count actual workload starts", Args: "count"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		want, err := strconv.Atoi(args[0])
		if err != nil {
			return nil, err
		}
		data, err := os.ReadFile(s.Path("starts"))
		if errors.Is(err, os.ErrNotExist) {
			data = nil
		} else if err != nil {
			return nil, err
		}
		if got := bytes.Count(data, []byte("start\n")); got != want {
			return nil, fmt.Errorf("actual starts=%d; want%d", got, want)
		}
		return nil, nil
	})
	e.Cmds["calls"] = script.Command(script.CmdUsage{Summary: "check actual forwarded RPC count", Args: "method count"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		want, err := strconv.Atoi(args[1])
		if err != nil {
			return nil, err
		}
		mu.Lock()
		got := calls["/skygo.v1.ControlService/"+args[0]]
		if args[0] == "all" {
			got = 0
			for _, n := range calls {
				got += n
			}
		}
		mu.Unlock()
		if got != want {
			return nil, fmt.Errorf("%s calls=%d; want%d", args[0], got, want)
		}
		return nil, nil
	})
	e.Cmds["server-directory"] = script.Command(script.CmdUsage{Summary: "select restored state while daemon stopped", Args: "directory"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		process.mu.Lock()
		defer process.mu.Unlock()
		if process.cmd != nil {
			return nil, errors.New("server must be stopped")
		}
		process.data = s.Path(args[0])
		return nil, nil
	})
	e.Cmds["copy-outputs"] = script.Command(script.CmdUsage{Summary: "copy stopped owned output alongside a DB snapshot", Args: "restored-directory"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		process.mu.Lock()
		running := process.cmd != nil
		source := filepath.Join(process.data, "logs")
		process.mu.Unlock()
		if running {
			return nil, errors.New("stop server before output backup")
		}
		destination := filepath.Join(s.Path(args[0]), "logs")
		if err := os.MkdirAll(destination, 0700); err != nil {
			return nil, err
		}
		entries, err := os.ReadDir(source)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.Type()&os.ModeSymlink != 0 || entry.IsDir() || (!strings.HasSuffix(entry.Name(), ".log") && !strings.HasSuffix(entry.Name(), ".truncated")) {
				return nil, errors.New("unexpected output backup entry")
			}
			data, err := os.ReadFile(filepath.Join(source, entry.Name()))
			if err != nil {
				return nil, err
			}
			if err := os.WriteFile(filepath.Join(destination, entry.Name()), data, 0600); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	e.Cmds["await-auth"] = awaitAuth(exe, env)
	e.Cmds["await-job-state"] = retentionAwait(exe, env, false)
	e.Cmds["await-usage"] = retentionAwait(exe, env, true)
	if old != "" {
		e.Cmds["old-state-refusal"] = retentionOldStateRefusal(old, process)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	scripttest.Test(t, ctx, e, []string{"SKYGO_ENDPOINT=" + counted.URL, "SKYGO_TOKEN=coverage-token", "PROXY=" + faulted.URL, "DATA=" + data, "AUTH=" + authFile}, file)
}

func retentionTaskfile(t *testing.T) script.Cmd {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	coverage := coverDir(t)
	return script.Command(script.CmdUsage{Summary: "write a task using an actual Go workload", Args: "file success|hold|failonce"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 || (args[1] != "success" && args[1] != "hold" && args[1] != "failonce") {
			return nil, script.ErrUsage
		}
		env := map[string]string{"SKYGO_RETENTION_WORKLOAD": args[1], "SKYGO_RETENTION_WORKDIR": s.Getwd(), "RETENTION_PAYLOAD_SENTINEL": "private-task-value"}
		if coverage != "" {
			env["GOCOVERDIR"] = coverage
		}
		task := map[string]any{"argv": []string{exe, "-test.run=^TestRetentionWorkload$"}, "env": env}
		if args[1] == "failonce" {
			if runtime.GOOS != "linux" {
				return nil, errors.New("actual retry helper requires qualified Linux")
			}
			task["retry"] = map[string]any{"max_attempts": 2, "delay_seconds": 1, "exit_codes": []int{7}}
		}
		data, err := json.Marshal(task)
		if err != nil {
			return nil, err
		}
		return nil, os.WriteFile(s.Path(args[0]), data, 0600)
	})
}

func retentionDomainCommand() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "save the actual explicit request domain", Args: "variable"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		var value struct {
			RequestDomain string `json:"request_domain"`
			Domain        struct {
				Lineage string `json:"lineage"`
				Epoch   string `json:"epoch"`
			} `json:"domain"`
		}
		if err := json.Unmarshal([]byte(s.Stdout()), &value); err != nil {
			return nil, err
		}
		text := value.RequestDomain
		if text == "" {
			text = value.Domain.Lineage + ":" + value.Domain.Epoch
		}
		if _, err := parseRequestDomain(text); err != nil {
			return nil, err
		}
		return nil, s.Setenv(args[0], text)
	})
}

func retentionJSONField() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "save a scalar from actual JSON", Args: "dotted.path variable"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		var value any
		if err := json.Unmarshal([]byte(s.Stdout()), &value); err != nil {
			return nil, err
		}
		for _, key := range strings.Split(args[0], ".") {
			switch object := value.(type) {
			case map[string]any:
				var ok bool
				value, ok = object[key]
				if !ok {
					return nil, fmt.Errorf("missing JSON field %s", key)
				}
			case []any:
				i, err := strconv.Atoi(key)
				if err != nil || i < 0 || i >= len(object) {
					return nil, errors.New("JSON array index outside actual response")
				}
				value = object[i]
			default:
				return nil, errors.New("JSON path is not an object or array")
			}
		}
		switch value := value.(type) {
		case string:
			return nil, s.Setenv(args[1], value)
		case bool:
			return nil, s.Setenv(args[1], strconv.FormatBool(value))
		case float64:
			return nil, s.Setenv(args[1], strconv.FormatFloat(value, 'f', -1, 64))
		default:
			return nil, errors.New("JSON field is not scalar")
		}
	})
}

type retentionScriptUsage struct {
	Admin *struct {
		Usage []struct {
			Domain string `json:"domain"`
			Used   string `json:"used"`
			Limit  string `json:"limit"`
		} `json:"usage"`
	} `json:"admin"`
}

func usageValue(data []byte, domain string) (uint64, error) {
	var info retentionScriptUsage
	if err := json.Unmarshal(data, &info); err != nil {
		return 0, err
	}
	if info.Admin == nil {
		return 0, errors.New("admin usage missing")
	}
	for _, value := range info.Admin.Usage {
		if value.Domain == "RETENTION_QUOTA_DOMAIN_"+domain {
			if value.Used == "" {
				return 0, nil
			}
			return strconv.ParseUint(value.Used, 10, 64)
		}
	}
	return 0, errors.New("usage domain missing")
}
func retentionUsageCommand() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "assert one actual quota counter", Args: "DOMAIN used"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		want, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			return nil, err
		}
		got, err := usageValue([]byte(s.Stdout()), args[0])
		if err != nil {
			return nil, err
		}
		if got != want {
			return nil, fmt.Errorf("%s used=%d; want%d", args[0], got, want)
		}
		return nil, nil
	})
}
func retentionAwait(exe string, env []string, usage bool) script.Cmd {
	return script.Command(script.CmdUsage{Summary: "wait for an actual CLI state or quota counter", Args: "id state | DOMAIN used"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		ctx, cancel := context.WithTimeout(s.Context(), 15*time.Second)
		defer cancel()
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			cmdargs := []string{"--json", "jobs", "get", args[0]}
			if usage {
				cmdargs = []string{"--json", "jobs", "retention"}
			}
			cmd := exec.CommandContext(ctx, exe, cmdargs...)
			cmd.Env = append(s.Environ(), env...)
			out, err := cmd.Output()
			if err != nil {
				return nil, err
			}
			match := false
			if usage {
				got, err := usageValue(out, args[0])
				if err != nil {
					return nil, err
				}
				want, err := strconv.ParseUint(args[1], 10, 64)
				if err != nil {
					return nil, err
				}
				match = got == want
			} else {
				var job struct {
					State string `json:"state"`
				}
				if err := json.Unmarshal(out, &job); err != nil {
					return nil, err
				}
				match = job.State == "JOB_STATE_"+strings.ToUpper(args[1])
			}
			if match {
				return func(*script.State) (string, string, error) { return string(out), "", nil }, nil
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-tick.C:
			}
		}
	})
}
func retentionOldStateRefusal(old string, p *serverProcess) script.Cmd {
	return script.Command(script.CmdUsage{Summary: "require actual S05 schema refusal without modifying bytes"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 0 {
			return nil, script.ErrUsage
		}
		p.mu.Lock()
		running := p.cmd != nil
		dir := p.data
		p.mu.Unlock()
		if running {
			return nil, errors.New("stop server before old schema gate")
		}
		path := filepath.Join(dir, "jobs.db")
		before, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		ctx, cancel := context.WithTimeout(s.Context(), 5*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, old, "server", "--listen", "127.0.0.1:0", "--data-dir", dir, "--token", "coverage-token")
		cmd.Env = append(s.Environ(), "SKYGO_TOKEN=")
		output, runerr := cmd.CombinedOutput()
		after, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if ctx.Err() != nil {
			return nil, errors.New("old daemon failed to refuse successor promptly")
		}
		if runerr == nil || !strings.Contains(string(output), "schema") || !bytes.Equal(before, after) {
			return nil, fmt.Errorf("old schema refusal=%v unchanged=%t output=%s", runerr, bytes.Equal(before, after), output)
		}
		return nil, nil
	})
}

// retentionCleanupCheckpoint observes a real outstanding record only while the
// production daemon has released its database. Open may perform bookkeeping;
// this is logical immutable-record/charge comparison, not DB byte immutability.
func retentionCleanupCheckpoint(p *serverProcess) script.Cmd {
	return script.Command(script.CmdUsage{Summary: "compare actual pending cleanup identity and charges across daemon restart", Args: "save|same job snapshot"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 3 || (args[0] != "save" && args[0] != "same") {
			return nil, script.ErrUsage
		}
		p.mu.Lock()
		running, dir := p.cmd != nil, p.data
		p.mu.Unlock()
		if running {
			return nil, errors.New("stop the actual daemon before the pending cleanup checkpoint")
		}
		read := func() (data []byte, err error) {
			db, err := store.Open(dir)
			if err != nil {
				return nil, err
			}
			defer func() { err = errors.Join(err, db.Close()) }()
			record, found, err := db.NextCleanup(0)
			if err != nil {
				return nil, err
			}
			if !found || record.Operation != store.OutputCleanup || record.Output.JobID != args[1] || record.HeaderID != args[1] || record.Output.ClosureID == "" || record.Output.Generation != 1 || record.Execution.ID != "" || len(record.Execution.Task.Argv) != 0 || len(record.Execution.Task.Env) != 0 || record.TaskDigest != ([32]byte{}) || record.Stopped || record.Proof.State != "" {
				return nil, errors.New("expected the actual taskless output cleanup for the accepted job")
			}
			engine := auth.NewLegacy("coverage-token")
			actor, err := engine.Authenticate("Bearer coverage-token")
			if err != nil {
				return nil, err
			}
			info, err := db.RetentionInfo(actor)
			if err != nil {
				return nil, err
			}
			if info.PendingCleanupRecords != 1 {
				return nil, errors.New("expected exactly one real outstanding cleanup operation")
			}
			return json.Marshal(struct {
				Record  store.CleanupRecord
				Usage   []store.Usage
				Pending uint64
			}{record, info.Usage, info.PendingCleanupRecords})
		}
		data, err := read()
		if err != nil {
			return nil, err
		}
		if args[0] == "save" {
			return nil, os.WriteFile(s.Path(args[2]), data, 0600)
		}
		before, err := os.ReadFile(s.Path(args[2]))
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(before, data) {
			return nil, errors.New("actual pending cleanup identity or quota charges changed across restart")
		}
		return nil, nil
	})
}
