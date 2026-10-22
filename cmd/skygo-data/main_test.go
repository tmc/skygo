package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// helperCLICommand keeps at most one original build/helper command for this
// serial case. Unknown outcomes retain the exact command, actual errors and
// namespace; the keeper is custody only, never proof a process is still alive.
type helperCLICommand struct {
	command                  *exec.Cmd
	startErr, waitErr, cause error
	directory                string
	held                     bool
}

func (f *helperCLICommand) hold(t *testing.T, phase string, reason error) {
	t.Helper()
	f.cause = errors.Join(f.startErr, f.waitErr, reason)
	if !f.held {
		f.held = true
		wait := make(chan struct{})
		go func() { <-wait; runtime.KeepAlive(f) }()
	}
	t.Fatalf("helper CLI %s: original command/error retained at %s: %v", phase, f.directory, f.cause)
}

// cliExit classifies ONLY this original Wait's normal CLI results. A nil Wait
// has positively returned; a nonnil result must itself be the actual ExitError
// with an exited (not signaled) ProcessState. No other Wait error permits reading
// command.ProcessState or inferring a join from a timeout or missing status.
func (f *helperCLICommand) cliExit() (int, bool) {
	if f.waitErr == nil {
		state := f.command.ProcessState
		if state != nil && state.Exited() && state.Success() {
			return 0, true
		}
		return 0, false
	}
	exit, ok := f.waitErr.(*exec.ExitError)
	if !ok || exit.ProcessState == nil || !exit.Exited() {
		return 0, false
	}
	if f.command.ProcessState != exit.ProcessState {
		return 0, false
	}
	code := exit.ExitCode()
	return code, code == 1 || code == 2
}

// These scripts run the actual built entrypoint and Run implementation. They
// never install token files, open fixed workspace roots, execute a workload or
// contact a server. Help/grammar and invalid configuration must refuse first.
func TestCLIScripts(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261020-data-helper-cli-tests")
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	base, err := os.MkdirTemp(parent, "case-")
	if err != nil {
		t.Fatal(err)
	}
	custody := &helperCLICommand{directory: base}
	t.Cleanup(func() {
		if t.Failed() || custody.held {
			t.Logf("retaining failed helper CLI case: %s", base)
			return
		}
		if err := os.RemoveAll(base); err != nil {
			t.Errorf("remove completed helper CLI case: %v", err)
		}
	})
	for _, name := range []string{"bin", "tmp", "home", "work"} {
		if err := os.Mkdir(filepath.Join(base, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	exe := filepath.Join(base, "bin", "skygo-data")
	args := []string{"build", "-o", exe}
	var runtimeEnv []string
	if mode := testing.CoverMode(); mode != "" {
		args = append(args, "-cover", "-covermode="+mode, "-coverpkg=github.com/tmc/skygo/cmd/skygo-data,github.com/tmc/skygo/internal/remotehelper")
		coverage := filepath.Join(base, "coverage")
		if err := os.Mkdir(coverage, 0700); err != nil {
			t.Fatal(err)
		}
		runtimeEnv = append(runtimeEnv, "GOCOVERDIR="+coverage)
	}
	args = append(args, ".")
	build := exec.CommandContext(ctx, "go", args...)
	build.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off", "CGO_ENABLED=0", "GOTMPDIR="+filepath.Join(base, "tmp"))
	var buildOutput bytes.Buffer
	build.Stdout, build.Stderr = &buildOutput, &buildOutput
	custody.command = build // Publish the original before Start, including errors.
	custody.startErr = build.Start()
	if custody.startErr != nil {
		custody.hold(t, "build Start", nil)
	}
	custody.waitErr = build.Wait()
	if code, settled := custody.cliExit(); !settled || code != 0 {
		t.Logf("build helper output:\n%s", &buildOutput)
		custody.hold(t, "build Wait", errors.New("build did not return an expected successful exit"))
	}
	custody.command = nil // This original positively exited0; no retry handle.
	files, err := filepath.Glob("testdata/*.txt")
	if err != nil || len(files) == 0 {
		t.Fatalf("helper CLI scripts: %v (%d files)", err, len(files))
	}
	for _, file := range files {
		if !t.Run(filepath.Base(file), func(t *testing.T) {
			body, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			// Verbose scripttest logs its environment. Supply only scoped paths
			// and synthetic script values, never the caller's inherited secrets.
			env := []string{"HOME=" + filepath.Join(base, "home"), "TMPDIR=" + filepath.Join(base, "tmp")}
			s, err := script.NewState(ctx, filepath.Join(base, "work"), env)
			if err != nil {
				t.Fatal(err)
			}
			engine := script.NewEngine()
			engine.Cmds = scripttest.DefaultCmds()
			engine.Conds = scripttest.DefaultConds()
			engine.Cmds["skygo-data"] = script.Command(script.CmdUsage{Summary: "run the production data helper"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
				if custody.held {
					t.Fatal("prior original helper command remains held")
				}
				cmd := exec.CommandContext(s.Context(), exe, args...)
				cmd.Dir = s.Getwd()
				cmd.Env = append(s.Environ(), runtimeEnv...)
				var stdout, stderr bytes.Buffer
				cmd.Stdout, cmd.Stderr = &stdout, &stderr
				custody.command = cmd
				custody.startErr, custody.waitErr = cmd.Start(), nil
				if custody.startErr != nil {
					custody.hold(t, "Start", nil)
				}
				return func(s *script.State) (string, string, error) {
					custody.waitErr = cmd.Wait()
					code, settled := custody.cliExit()
					if !settled {
						custody.hold(t, "Wait", errors.New("helper did not return an expected normal CLI exit"))
					}
					err := custody.waitErr
					custody.command = nil
					if setErr := s.Setenv("SKYGO_DATA_EXIT_CODE", strconv.Itoa(code)); setErr != nil {
						return stdout.String(), stderr.String(), setErr
					}
					return stdout.String(), stderr.String(), err
				}, nil
			})
			scripttest.Run(t, engine, s, file, bytes.NewReader(body))
		}) {
			return // Fail-stop: no later script may replace the held original.
		}
	}
}
