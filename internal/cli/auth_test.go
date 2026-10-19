package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// TestAuthScripts exercises local credential administration and scoped RPCs
// through the production executable, with real daemon reloads and restarts.
func TestAuthScripts(t *testing.T) {
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
	files, err := filepath.Glob("testdata/auth/*.txt")
	if err != nil || len(files) == 0 {
		t.Fatalf("auth scripts: %v (%d files)", err, len(files))
	}
	for _, file := range files {
		t.Run(strings.TrimSuffix(filepath.Base(file), ".txt"), func(t *testing.T) {
			runAuthScript(t, exe, file)
		})
	}
}

func runAuthScript(t *testing.T, exe, file string) {
	t.Helper()
	dir := t.TempDir()
	data, authFile := filepath.Join(dir, "state"), filepath.Join(dir, "auth.json")
	var env []string
	if cover := coverDir(t); cover != "" {
		env = append(env, "GOCOVERDIR="+cover)
	}
	srv := &serverProcess{exe: exe, address: freeAddress(t), data: data, env: env, args: []string{"--auth-file", authFile}}
	t.Cleanup(srv.cleanup)
	e := executableScriptEngine(exe, nil, env)
	bg := &background{}
	e.Cmds["skygo"] = bg.command(exe, env)
	e.Cmds["await-stdout"] = bg.awaitStdout()
	e.Cmds["server"] = srv.command()
	e.Cmds["taskfile"] = coverageTaskfile()
	e.Cmds["await-file"] = awaitFile()
	e.Cmds["job-id"] = jobID()
	e.Cmds["token-id"] = script.Command(script.CmdUsage{Summary: "read a public credential ID from a private token file", Args: "file variable"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		data, err := os.ReadFile(s.Path(args[0]))
		if err != nil {
			return nil, err
		}
		parts := regexp.MustCompile(`^skygo_([0-9a-f]{32})_[A-Za-z0-9_-]+\n?$`).FindSubmatch(data)
		if parts == nil {
			return nil, errors.New("invalid credential format")
		}
		return nil, s.Setenv(args[1], string(parts[1]))
	})
	e.Cmds["page-token"] = script.Command(script.CmdUsage{Summary: "save a nonempty continuation token", Args: "variable"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		// Protobuf JSON emits a simple base64url cursor, without JSON escapes.
		m := regexp.MustCompile(`"next_page_token"\s*:\s*"([^"\\]+)"`).FindStringSubmatch(s.Stdout())
		if m == nil {
			return nil, errors.New("missing continuation token")
		}
		return nil, s.Setenv(args[0], m[1])
	})
	e.Cmds["await-auth"] = awaitAuth(exe, env)
	e.Cmds["server-directory"] = script.Command(script.CmdUsage{Summary: "select a restored state directory while the daemon is stopped", Args: "directory"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		srv.mu.Lock()
		defer srv.mu.Unlock()
		if srv.cmd != nil {
			return nil, errors.New("server must be stopped")
		}
		srv.data = s.Path(args[0])
		return nil, nil
	})
	// The next wait after an auth edit must close the follower within the
	// documented two-second refresh envelope. Ordinary CLI waits are unchanged.
	var deadline time.Time
	cli := e.Cmds["skygo"]
	e.Cmds["skygo"] = script.Command(*cli.Usage(), func(s *script.State, args ...string) (script.WaitFunc, error) {
		wait, err := cli.Run(s, args...)
		if err != nil || wait == nil {
			return wait, err
		}
		process, err := bg.last()
		if err != nil {
			return nil, err
		}
		return func(s *script.State) (string, string, error) {
			if deadline.IsZero() {
				return wait(s)
			}
			remaining := time.Until(deadline)
			deadline = time.Time{}
			type result struct {
				out, diag string
				err       error
			}
			done := make(chan result, 1)
			go func() {
				out, diag, err := wait(s)
				done <- result{out, diag, err}
			}()
			timer := time.NewTimer(max(remaining, 0))
			defer timer.Stop()
			select {
			case r := <-done:
				return r.out, r.diag, r.err
			case <-timer.C:
				_ = process.cmd.Process.Kill()
				r := <-done // finish State access before the script can continue
				return r.out, r.diag + "\nauth stream timeout\n", errors.New("auth update did not close follower within two seconds")
			}
		}, nil
	})
	e.Cmds["auth-window"] = script.Command(script.CmdUsage{Summary: "bound the next follower wait after an auth edit"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 0 {
			return nil, script.ErrUsage
		}
		deadline = time.Now().Add(2 * time.Second)
		return nil, nil
	})
	e.Cmds["fenced-refusal"] = fencedRefusal(t)
	e.Conds["s01-binary"] = script.Condition("an exact S01 fenced binary is supplied", func(s *script.State) (bool, error) {
		return os.Getenv("SKYGO_S01_BINARY") != "", nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	scripttest.Test(t, ctx, e, []string{"SKYGO_TOKEN=", "SKYGO_ENDPOINT=http://" + srv.address, "DATA=" + data, "AUTH=" + authFile}, file)
}

// awaitAuth polls production CLI reads after a supported auth-file edit. It
// prints neither token bytes nor intercepted diagnostics, including on failure.
func awaitAuth(exe string, env []string) script.Cmd {
	return script.Command(script.CmdUsage{Summary: "wait for a live credential policy change", Args: "allowed|unauthenticated|permission_denied token-file CLI-args..."}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) < 3 || (args[0] != "allowed" && args[0] != "unauthenticated" && args[0] != "permission_denied") {
			return nil, script.ErrUsage
		}
		ctx, cancel := context.WithTimeout(s.Context(), 2*time.Second)
		defer cancel()
		limit, _ := ctx.Deadline()
		began := time.Now()
		var observations []string
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			cmd := exec.CommandContext(ctx, exe, append([]string{"--token-file", s.Path(args[1])}, args[2:]...)...)
			cmd.Dir = s.Getwd()
			cmd.Env = append(s.Environ(), env...)
			var diag bytes.Buffer
			cmd.Stderr = &diag
			beforeStart := time.Now()
			startErr := cmd.Start()
			started := startErr == nil
			startTime := time.Since(beforeStart)
			beforeWait := time.Now()
			err := startErr
			if started {
				err = cmd.Wait()
			}
			waitTime := time.Since(beforeWait)
			status := -1
			if cmd.ProcessState != nil {
				status = cmd.ProcessState.ExitCode()
			}
			category := authDiagnosticCategory(diag.String())
			ctxErr := ctx.Err()
			expired := ctxErr != nil || !time.Now().Before(limit)
			var exit *exec.ExitError
			normalDenial := errors.As(err, &exit) && exit.ExitCode() == 1
			if started && !expired && ((args[0] == "allowed" && err == nil && status == 0) || (args[0] != "allowed" && normalDenial && category == args[0])) {
				return nil, nil
			}
			observations = append(observations, fmt.Sprintf("attempt=%d elapsed_us=%d start_us=%d wait_us=%d started=%t exit=%d start_error=%s wait_error=%s context=%s expired=%t diagnostic=%s diagnostic_bytes=%d", len(observations)+1, time.Since(began).Microseconds(), startTime.Microseconds(), waitTime.Microseconds(), started, status, authProbeErrorCategory(startErr), authProbeErrorCategory(err), authProbeErrorCategory(ctxErr), expired, category, diag.Len()))
			select {
			case <-ctx.Done():
				for _, observation := range observations {
					s.Logf("auth probe %s\n", observation)
				}
				return nil, errors.New("auth update not observed within two seconds")
			case <-tick.C:
			}
		}
	})
}

// Diagnostic labels are a closed vocabulary. Never return source bytes, even
// when a client fails before reaching HTTP or an unexpected error is returned.
func authDiagnosticCategory(diag string) string {
	for _, code := range []string{"unauthenticated", "unavailable", "permission_denied", "not_found", "invalid_argument", "deadline_exceeded", "canceled"} {
		if strings.HasPrefix(diag, code+":") {
			return code
		}
	}
	if strings.HasPrefix(diag, "choose only one credential source:") {
		return "credential_source"
	}
	if strings.HasPrefix(diag, "token file must be") || strings.HasPrefix(diag, "invalid bearer credential") {
		return "credential_file"
	}
	if diag == "" {
		return "empty"
	}
	return "other"
}

func authProbeErrorCategory(err error) string {
	if err == nil {
		return "none"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, os.ErrNotExist) {
		return "not_exist"
	}
	if errors.Is(err, os.ErrPermission) {
		return "permission"
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return "exit"
	}
	return "other"
}

func fencedRefusal(t *testing.T) script.Cmd {
	return script.Command(script.CmdUsage{Summary: "verify the supplied S01 binary refuses a scoped database without modification", Args: "database"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		exe, want := os.Getenv("SKYGO_S01_BINARY"), os.Getenv("SKYGO_S01_SHA256")
		binary, err := os.ReadFile(exe)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(binary)
		if len(want) != 64 || hex.EncodeToString(sum[:]) != want {
			return nil, errors.New("S01 fixture binary hash mismatch")
		}
		before, err := os.ReadFile(s.Path(args[0]))
		if err != nil {
			return nil, err
		}
		dir := t.TempDir()
		path := filepath.Join(dir, "jobs.db")
		if err := os.WriteFile(path, before, 0600); err != nil {
			return nil, err
		}
		ctx, cancel := context.WithTimeout(s.Context(), 5*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, exe, "server", "--listen", "127.0.0.1:0", "--data-dir", dir)
		cmd.Env = append(s.Environ(), "SKYGO_TOKEN=")
		out, runErr := cmd.CombinedOutput()
		if ctx.Err() != nil || runErr == nil || !strings.Contains(string(out), "schema") {
			return nil, fmt.Errorf("S01 fixture did not promptly refuse scoped schema: %v", runErr)
		}
		after, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(before, after) {
			return nil, errors.New("S01 refusal modified scoped state")
		}
		stateHash := sha256.Sum256(before)
		s.Logf("S01 binary SHA256 %s refused schema2; database SHA256 %x unchanged (%d bytes)\n", want, stateHash, len(before))
		return nil, nil
	})
}
