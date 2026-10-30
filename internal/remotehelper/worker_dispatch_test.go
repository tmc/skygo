//go:build linux || darwin

package remotehelper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

// These fixtures invoke the same dispatcher as Run with private paths. The run
// operation records its arguments: it does not qualify kernel exec or an image.
func TestWorkerDispatch(t *testing.T) {
	for _, verb := range []string{"worker", "worker-ready"} {
		t.Run(verb, func(t *testing.T) {
			f := workerFiles(t)
			env, token := workerMember(t, backend.WorkerIndependent, 1, 2)
			if verb == "worker" {
				env = append(env, "SKYPILOT_NODE_RANK=foreign", "SKYPILOT_NODE_RANK=duplicate", "SKYPILOT_NUM_NODES=77")
			}
			if err := os.WriteFile(f.paths.start, []byte(token), 0600); err != nil {
				t.Fatal(err)
			}
			if verb == "worker-ready" {
				if err := os.WriteFile(f.paths.prepared, []byte(workerTestUID), 0600); err != nil {
					t.Fatal(err)
				}
			}
			ops := systemWorkerOps()
			calls := 0
			argv := []string{"train", "--epochs", "2"}
			wantEnv := slices.DeleteFunc(slices.Clone(env), func(s string) bool { return strings.HasPrefix(s, "SKYPILOT_NODE_RANK=") })
			wantEnv = append(wantEnv, "SKYPILOT_NODE_RANK=1")
			ops.run = func(dir string, args, gotEnv []string) error {
				calls++
				if dir != "." || !slices.Equal(args, argv) || !slices.Equal(gotEnv, wantEnv) {
					return errors.New("MODELED run payload changed")
				}
				return nil
			}
			args := []string{verb}
			if verb == "worker" {
				args = append(args, "--")
				args = append(args, argv...)
			}
			var stdout, stderr bytes.Buffer
			var failure helperFailure
			code := runHelper(context.Background(), args, env, &stdout, &stderr, paths{}, f.paths, ops, &failure)
			wantCalls := 0
			if verb == "worker" {
				wantCalls = 1
			}
			if code != 0 || calls != wantCalls || stdout.Len() != 0 || stderr.Len() != 0 || failure.err != nil {
				t.Fatalf("dispatch code=%d calls=%d stdout=%q stderr=%q held=%v", code, calls, stdout.String(), stderr.String(), failure.err)
			}
			if verb == "worker-ready" {
				code = runHelper(context.Background(), args, workerReplace(env, "SKYGO_POD_UID", "foreign"), &stdout, &stderr, paths{}, f.paths, ops, &failure)
				if code != 1 || calls != 0 || stdout.Len() != 0 || stderr.Len() != 0 || !errors.Is(failure.err, errConfiguration) {
					t.Fatal("foreign readiness accepted or printed")
				}
			}
		})
	}
}

func TestWorkerDispatchInstall(t *testing.T) {
	for _, valid := range []bool{true, false} {
		name := "digest-mismatch"
		if valid {
			name = "matching-digest"
		}
		t.Run(name, func(t *testing.T) {
			f := workerFiles(t)
			p := paths{self: filepath.Join(f.dir, "source"), helper: filepath.Join(f.dir, "helper")}
			// This is digest/copy evidence for test bytes, not an executable image.
			payload := []byte("MODELED helper image bytes\n")
			if err := os.WriteFile(p.self, payload, 0555); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(p.helper, 0700); err != nil {
				t.Fatal(err)
			}
			hash := sha256.Sum256(payload)
			digest := hex.EncodeToString(hash[:])
			if !valid {
				digest = strings.Repeat("0", 64)
			}
			var stdout, stderr bytes.Buffer
			var failure helperFailure
			code := runHelper(context.Background(), []string{"install"}, []string{"SKYGO_DATA_HELPER_SHA256=" + digest}, &stdout, &stderr, p, f.paths, systemWorkerOps(), &failure)
			wantCode := 1
			if valid {
				wantCode = 0
			}
			if code != wantCode || stdout.Len() != 0 || stderr.Len() != 0 {
				t.Fatalf("install code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
			}
			if valid {
				installed := filepath.Join(p.helper, "bin", "skygo-data")
				got, err := os.ReadFile(installed)
				if err != nil || !bytes.Equal(got, payload) {
					t.Fatalf("installed bytes=%q err=%v", got, err)
				}
				info, err := os.Stat(installed)
				if err != nil || info.Mode().Perm() != 0555 || failure.err != nil {
					t.Fatalf("installed mode/error: %v %v", info, err)
				}
			} else if failure.err == nil {
				t.Fatal("digest mismatch lost")
			}
		})
	}
}

func TestWorkerDispatchMalformed(t *testing.T) {
	for _, args := range [][]string{{"install", "extra"}, {"worker"}, {"worker", "--"}, {"worker", "train"}, {"worker", "--", ""}, {"worker-ready", "extra"}} {
		var stdout, stderr bytes.Buffer
		var failure helperFailure
		ops := workerOps{
			open: func(string, int, os.FileMode) (workerFile, error) {
				t.Fatal("opened file for malformed command")
				return nil, os.ErrInvalid
			},
			rename: func(string, string) error { t.Fatal("renamed malformed command file"); return os.ErrInvalid },
			run:    func(string, []string, []string) error { t.Fatal("ran malformed command"); return os.ErrInvalid },
		}
		if code := runHelper(context.Background(), args, nil, &stdout, &stderr, paths{}, workerPaths{}, ops, &failure); code != 2 || stdout.Len() != 0 || stderr.Len() != 0 || failure.err != nil {
			t.Fatalf("%q code=%d stdout=%q stderr=%q held=%v", args, code, stdout.String(), stderr.String(), failure.err)
		}
	}
}

func TestWorkerDispatchRetainsOriginal(t *testing.T) {
	for _, verb := range []string{"worker", "worker-ready"} {
		t.Run(verb, func(t *testing.T) {
			f := workerFiles(t)
			env, token := workerMember(t, backend.WorkerIndependent, 1, 2)
			if err := os.WriteFile(f.paths.start, []byte(token), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(f.paths.prepared, []byte(workerTestUID), 0600); err != nil {
				t.Fatal(err)
			}
			selected := f.paths.start
			args := []string{verb}
			if verb == "worker" {
				args = append(args, "--", "train")
			} else {
				selected = f.paths.prepared
			}
			reading, closing := errors.New("MODELED original Read failure"), errors.New("MODELED original first Close failure")
			ops := systemWorkerOps()
			open := ops.open
			opens, runs := 0, 0
			var original *workerFaultFile
			ops.open = func(name string, flags int, mode os.FileMode) (workerFile, error) {
				opens++
				file, err := open(name, flags, mode)
				if err != nil {
					return nil, err
				}
				if name == selected {
					original = &workerFaultFile{File: file.(*os.File), readErr: reading, closeErr: closing}
					return original, nil
				}
				return file, nil
			}
			ops.run = func(string, []string, []string) error { runs++; return nil }
			var stdout, stderr bytes.Buffer
			var failure helperFailure
			if code := runHelper(context.Background(), args, env, &stdout, &stderr, paths{}, f.paths, ops, &failure); code != 1 {
				t.Fatalf("fault code=%d", code)
			}
			f.hold(failure.err)
			var held *closeFailure
			if !errors.Is(failure.err, reading) || !errors.Is(failure.err, closing) || !errors.As(failure.err, &held) || held.owner != original || original.closes != 1 || runs != 0 {
				t.Fatalf("original lost: %v owner=%v", failure.err, held)
			}
			before := opens
			first := failure.err
			if code := runHelper(context.Background(), args, env, &stdout, &stderr, paths{}, f.paths, ops, &failure); code != 1 || opens != before || original.closes != 1 || runs != 0 || failure.err != first {
				t.Fatal("held dispatcher performed another operation or replaced original")
			}
			if stdout.Len() != 0 || stderr.Len() != 0 {
				t.Fatal("worker refusal printed")
			}
		})
	}
}

func TestWorkerDispatchJoinsReportFailure(t *testing.T) {
	f := workerFiles(t)
	env, _ := workerMember(t, backend.WorkerIndependent, 1, 2)
	if err := os.WriteFile(f.paths.start, []byte("foreign"), 0600); err != nil {
		t.Fatal(err)
	}
	writing, closing := errors.New("MODELED report Write failure"), errors.New("MODELED report first Close failure")
	ops := systemWorkerOps()
	open := ops.open
	var original *workerFaultFile
	ops.open = func(name string, flags int, mode os.FileMode) (workerFile, error) {
		file, err := open(name, flags, mode)
		if err != nil {
			return nil, err
		}
		if name == f.paths.termination {
			original = &workerFaultFile{File: file.(*os.File), writeErr: writing, closeErr: closing}
			return original, nil
		}
		return file, nil
	}
	ops.run = func(string, []string, []string) error { t.Fatal("ran refused token"); return nil }
	var stdout, stderr bytes.Buffer
	var failure helperFailure
	code := runHelper(context.Background(), []string{"worker", "--", "train"}, env, &stdout, &stderr, paths{}, f.paths, ops, &failure)
	f.hold(failure.err)
	var held *closeFailure
	if code != workerExitRefused || !errors.Is(failure.err, errWorkerRefused) || !errors.Is(failure.err, writing) || !errors.Is(failure.err, closing) || !errors.As(failure.err, &held) || held.owner != original || original.closes != 1 || stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("report dispatch code=%d held=%v", code, failure.err)
	}
}

const workerDispatchChild = "SKYGO_TEST_WORKER_DISPATCH_CHILD"

func TestWorkerDispatchChild(t *testing.T) {
	if os.Getenv(workerDispatchChild) != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			os.Exit(Run(context.Background(), os.Args[i+1:], nil, os.Stdout, os.Stderr))
		}
	}
	os.Exit(99)
}

// Unknown originals remain strongly owned. Cmd.Wait is called exactly once;
// its successful return also joins os/exec's stdout/stderr copy goroutines.
var workerDispatchHeld struct {
	sync.Mutex
	commands []*exec.Cmd
	waits    []<-chan error
	streams  []*bytes.Buffer
	errors   []error
}

func TestWorkerPublicRefusals(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name string
		args []string
		code int
	}{
		{"install-grammar", []string{"install", "extra"}, 2},
		{"worker-grammar", []string{"worker", "--"}, 2},
		{"ready-grammar", []string{"worker-ready", "extra"}, 2},
		{"ready-identity", []string{"worker-ready"}, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// Public grammar refusal and ready's missing identity do not open fixed
			// paths. This is actual subprocess qualification, not successful exec.
			args := append([]string{"-test.run=^TestWorkerDispatchChild$", "--"}, tt.args...)
			cmd := exec.Command(executable, args...)
			cmd.Env = []string{workerDispatchChild + "=1"}
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			stdout, stderr := new(bytes.Buffer), new(bytes.Buffer)
			cmd.Stdout, cmd.Stderr = stdout, stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			pid := cmd.Process.Pid
			wait := make(chan error, 1)
			go func() { wait <- cmd.Wait() }()
			retain := func(cause error) {
				workerDispatchHeld.Lock()
				workerDispatchHeld.commands = append(workerDispatchHeld.commands, cmd)
				workerDispatchHeld.waits = append(workerDispatchHeld.waits, wait)
				workerDispatchHeld.streams = append(workerDispatchHeld.streams, stdout, stderr)
				workerDispatchHeld.errors = append(workerDispatchHeld.errors, cause)
				workerDispatchHeld.Unlock()
			}
			var waited error
			select {
			case waited = <-wait:
			case <-time.After(10 * time.Second):
				// Kill uses the still-owned Process handle, never a reaped PGID.
				killErr := cmd.Process.Kill()
				select {
				case waited = <-wait:
					t.Errorf("original exceeded deadline: kill=%v wait=%v", killErr, waited)
				case <-time.After(5 * time.Second):
					retain(killErr)
					t.Fatalf("HOLD original pid=%d: Wait remains unknown", pid)
				}
			}
			if cmd.ProcessState == nil {
				retain(waited)
				t.Fatalf("HOLD pid=%d has no terminal state", pid)
			}
			groupErr := syscall.Kill(-pid, 0)
			if !errors.Is(groupErr, syscall.ESRCH) {
				retain(groupErr)
				t.Fatalf("HOLD pid=%d original group absence unknown: %v", pid, groupErr)
			}
			if cmd.ProcessState.ExitCode() != tt.code || stdout.Len() != 0 || stderr.Len() != 0 {
				t.Fatalf("pid=%d code=%d want=%d stdout=%q stderr=%q wait=%v", pid, cmd.ProcessState.ExitCode(), tt.code, stdout.String(), stderr.String(), waited)
			}
			t.Logf("actual public refusal pid=%d Wait=%v code=%d PGID=ABSENT stdout/stderr=joined", pid, waited, cmd.ProcessState.ExitCode())
		})
	}
}

func TestWorkerDispatchIdentityRefusals(t *testing.T) {
	for _, key := range []string{"RANK", "SKYGO_POD_UID", "SKYGO_TASK_SHA256"} {
		t.Run(key, func(t *testing.T) {
			f := workerFiles(t)
			env, _ := workerMember(t, backend.WorkerIndependent, 1, 2)
			value := "foreign"
			if key == "SKYGO_POD_UID" {
				value = "foreign uid"
			}
			env = workerReplace(env, key, value)
			ops := systemWorkerOps()
			runs := 0
			ops.run = func(string, []string, []string) error { runs++; return nil }
			var stdout, stderr bytes.Buffer
			var failure helperFailure
			code := runHelper(context.Background(), []string{"worker", "--", "train"}, env, &stdout, &stderr, paths{}, f.paths, ops, &failure)
			if code != 1 || runs != 0 || stdout.Len() != 0 || stderr.Len() != 0 || !errors.Is(failure.err, errConfiguration) {
				t.Fatalf("identity refusal code=%d runs=%d held=%v", code, runs, failure.err)
			}
			if _, err := os.Stat(f.paths.prepared); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("published readiness for malformed identity")
			}
		})
	}
}

func TestWorkerDispatchForeignToken(t *testing.T) {
	f := workerFiles(t)
	env, token := workerMember(t, backend.WorkerIndependent, 1, 2)
	env = workerReplace(env, "SKYGO_POD_UID", "valid-replacement-uid")
	if err := os.WriteFile(f.paths.start, []byte(token), 0600); err != nil {
		t.Fatal(err)
	}
	ops := systemWorkerOps()
	runs := 0
	ops.run = func(string, []string, []string) error { runs++; return nil }
	var stdout, stderr bytes.Buffer
	var failure helperFailure
	code := runHelper(context.Background(), []string{"worker", "--", "train"}, env, &stdout, &stderr, paths{}, f.paths, ops, &failure)
	if code != workerExitRefused || runs != 0 || stdout.Len() != 0 || stderr.Len() != 0 || !errors.Is(failure.err, errWorkerRefused) {
		t.Fatalf("foreign token code=%d runs=%d held=%v", code, runs, failure.err)
	}
}
