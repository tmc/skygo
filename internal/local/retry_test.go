package local

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

func retryTask(mode string) backend.Task {
	t := task(mode)
	t.Retry = &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7}}
	return t
}

func requireRetry(t *testing.T, b *Backend) {
	t.Helper()
	if !b.SupportsRetry() {
		t.Skip("native nonreaping retry lifecycle is not qualified on this platform")
	}
}

func TestRetryAdmission(t *testing.T) {
	b := newBackend(t)
	err := b.Check(retryTask("fail"))
	if b.SupportsRetry() != (err == nil) {
		t.Fatalf("SupportsRetry=%v, Check=%v", b.SupportsRetry(), err)
	}
	single := task("success")
	single.Retry = &backend.RetryPolicy{MaxAttempts: 1}
	if err := b.Check(single); err != nil {
		t.Fatal(err)
	}
	invalid := retryTask("fail")
	invalid.Retry.ExitCodes = nil
	if err := b.Check(invalid); err == nil {
		t.Fatal("invalid retry policy admitted")
	}
}

func TestRetryStopIntent(t *testing.T) {
	for _, reaping := range []bool{false, true} {
		t.Run(map[bool]string{false: "signal-error", true: "reaping"}[reaping], func(t *testing.T) {
			b := newBackend(t)
			calls := 0
			signalErr := errors.New("signal failed")
			b.retryOps.kill = func(*exec.Cmd) error { calls++; return signalErr }
			e := &execution{cmd: &exec.Cmd{}, status: backend.Status{State: backend.Running, Instance: "owned"}, retry: &retryExecution{reaping: reaping}}
			err := b.stop(e, backend.Canceled, "operator")
			if !e.retry.intent || e.status.State != backend.Canceled || e.status.Instance != "owned" {
				t.Fatalf("intent not retained: %+v", e)
			}
			if reaping && (err != nil || calls != 0) {
				t.Fatalf("signaled after reaping: calls=%d err=%v", calls, err)
			}
			if !reaping && (!errors.Is(err, signalErr) || calls != 1) {
				t.Fatalf("signal error=%v, calls=%d", err, calls)
			}
		})
	}
}

func TestRetryLifecycle(t *testing.T) {
	for _, tt := range []struct {
		mode  string
		code  int32
		state backend.State
	}{{"fail", 7, backend.Failed}, {"success", 0, backend.Succeeded}, {"output", 0, backend.Succeeded}} {
		t.Run(tt.mode, func(t *testing.T) {
			b := newBackend(t)
			requireRetry(t, b)
			id := strings.Repeat("8", 32)
			if err := b.Start(context.Background(), id, retryTask(tt.mode)); err != nil {
				t.Fatal(err)
			}
			s := completed(t, b, id)
			if s.State != tt.state || s.ExitCode == nil || *s.ExitCode != tt.code || s.Termination != backend.StoppedWithExitCode || !validID.MatchString(s.Instance) {
				t.Fatalf("status=%+v", s)
			}
			ref := backend.Reference{ID: id, Instance: s.Instance}
			if _, err := b.Observe(context.Background(), ref); err != nil {
				t.Fatal(err)
			}
			ref.Instance = "replacement"
			if _, err := b.Observe(context.Background(), ref); !errors.Is(err, backend.ErrIdentityMismatch) {
				t.Fatalf("Observe mismatch=%v", err)
			}
			if err := b.Cancel(context.Background(), ref); !errors.Is(err, backend.ErrIdentityMismatch) {
				t.Fatalf("Cancel mismatch=%v", err)
			}
			if _, err := b.Logs(context.Background(), ref, 0, 1); !errors.Is(err, backend.ErrIdentityMismatch) {
				t.Fatalf("Logs mismatch=%v", err)
			}
			restarted, err := New(b.directory)
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			if s, err := restarted.Observe(context.Background(), backend.Reference{ID: id, Instance: s.Instance}); err != nil || s.State != backend.Unknown || s.Termination != backend.UnknownTermination {
				t.Fatalf("restart=%+v %v", s, err)
			}
		})
	}
}

func TestRetryLifecycleOrder(t *testing.T) {
	b := newBackend(t)
	requireRetry(t, b)
	var mu sync.Mutex
	var trace []string
	add := func(s string) { mu.Lock(); trace = append(trace, s); mu.Unlock() }
	ops := b.retryOps
	b.retryOps.wait = func(c *exec.Cmd) error { err := ops.wait(c); add("pinned"); return err }
	b.retryOps.kill = func(c *exec.Cmd) error { add("signal"); return ops.kill(c) }
	b.retryOps.absent = func(c *exec.Cmd) error { add("after-reap-probe"); return ops.absent(c) }
	id := strings.Repeat("9", 32)
	if err := b.Start(context.Background(), id, retryTask("fail")); err != nil {
		t.Fatal(err)
	}
	if s := completed(t, b, id); s.Termination != backend.StoppedWithExitCode {
		t.Fatalf("status=%+v", s)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(trace) < 3 || trace[0] != "pinned" || trace[1] != "signal" {
		t.Fatalf("trace=%v", trace)
	}
	for _, event := range trace[2:] {
		if event != "after-reap-probe" {
			t.Fatalf("signal after reap: %v", trace)
		}
	}
}

func TestRetryProofFailures(t *testing.T) {
	for _, mode := range []string{"pin", "cleanup", "absence", "output"} {
		t.Run(mode, func(t *testing.T) {
			b := newBackend(t)
			requireRetry(t, b)
			failure := errors.New("injected lifecycle failure")
			switch mode {
			case "pin":
				wait := b.retryOps.wait
				b.retryOps.wait = func(c *exec.Cmd) error {
					if err := wait(c); err != nil {
						return err
					}
					return failure
				}
				b.retryOps.kill = func(*exec.Cmd) error { t.Error("signal after lost pin"); return nil }
			case "cleanup":
				b.retryOps.kill = func(*exec.Cmd) error { return failure }
			case "absence":
				b.retryOps.absent = func(*exec.Cmd) error { return failure }
			}
			id := strings.Repeat("a", 32)
			task := retryTask("fail")
			if mode == "output" {
				task = retryTask("retention")
				task.Env["SKYGO_RELEASE"] = filepath.Join(t.TempDir(), "release")
			}
			if err := b.Start(context.Background(), id, task); err != nil {
				t.Fatal(err)
			}
			if mode == "output" {
				b.mu.Lock()
				out := b.jobs[id].output
				b.mu.Unlock()
				out.mu.Lock()
				out.file.Close()
				out.mu.Unlock()
				if err := os.WriteFile(task.Env["SKYGO_RELEASE"], nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			s := completed(t, b, id)
			if s.Termination != backend.UnknownTermination || s.State != backend.Failed || s.Reason == "" {
				t.Fatalf("status=%+v", s)
			}
		})
	}
}

func TestRetryCancellation(t *testing.T) {
	for _, mode := range []string{"cancel", "timeout", "close"} {
		t.Run(mode, func(t *testing.T) {
			b := newBackend(t)
			requireRetry(t, b)
			id := strings.Repeat("b", 32)
			task := retryTask("sleep")
			if mode == "timeout" {
				task.TimeoutSeconds = 1
			}
			if err := b.Start(context.Background(), id, task); err != nil {
				t.Fatal(err)
			}
			if mode == "cancel" {
				if err := b.Cancel(context.Background(), backend.Reference{ID: id}); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "close" {
				if err := b.Close(); err != nil {
					t.Fatal(err)
				}
			}
			s := completed(t, b, id)
			if s.Termination != backend.UnknownTermination || s.Instance == "" {
				t.Fatalf("status=%+v", s)
			}
			want := backend.Canceled
			if mode == "timeout" {
				want = backend.Failed
			}
			if s.State != want {
				t.Fatalf("status=%+v want=%s", s, want)
			}
		})
	}
}

func TestRetryInheritedOutput(t *testing.T) {
	b := newBackend(t)
	requireRetry(t, b)
	task := retryTask("orphan-fail")
	task.Env["SKYGO_TICK"] = filepath.Join(t.TempDir(), "tick")
	id := strings.Repeat("c", 32)
	if err := b.Start(context.Background(), id, task); err != nil {
		t.Fatal(err)
	}
	s := completed(t, b, id)
	if s.State != backend.Failed || s.ExitCode == nil || *s.ExitCode != 7 {
		t.Fatalf("status=%+v", s)
	}
	before, err := os.ReadFile(task.Env["SKYGO_TICK"])
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	after, err := os.ReadFile(task.Env["SKYGO_TICK"])
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("owned descendant still writes after completion")
	}
	// A zombie retained by the host's subreaper may conservatively suppress proof.
	// Never force that probe to pass, or infer whole-tree cleanup from this check.
}

func TestRetryReasonBound(t *testing.T) {
	got := retryReason(strings.Repeat("界", 500))
	if len(got) > 1024 || len([]rune(got)) != 341 {
		t.Fatalf("bounded reason bytes=%d runes=%d", len(got), len([]rune(got)))
	}
}

func TestRetryCancelAfterReap(t *testing.T) {
	b := newBackend(t)
	requireRetry(t, b)
	reached := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	signals := 0
	ops := b.retryOps
	b.retryOps.kill = func(c *exec.Cmd) error { signals++; return ops.kill(c) }
	b.retryOps.absent = func(c *exec.Cmd) error { once.Do(func() { close(reached) }); <-release; return ops.absent(c) }
	id := strings.Repeat("e", 32)
	if err := b.Start(context.Background(), id, retryTask("fail")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-reached:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("waiter did not reach post-reap probe")
	}
	if err := b.Cancel(context.Background(), backend.Reference{ID: id}); err != nil {
		close(release)
		t.Fatal(err)
	}
	close(release)
	s := completed(t, b, id)
	if s.State != backend.Canceled || s.Termination != backend.UnknownTermination || signals != 1 {
		t.Fatalf("status=%+v signals=%d", s, signals)
	}
}

func TestRetryPinFailureWhileLive(t *testing.T) {
	b := newBackend(t)
	requireRetry(t, b)
	marker := filepath.Join(t.TempDir(), "ready")
	failure := errors.New("waitid failed while original leader is alive")
	b.retryOps.wait = func(*exec.Cmd) error {
		deadline := time.Now().Add(3 * time.Second)
		for {
			if _, err := os.Stat(marker); err == nil {
				return failure
			}
			if time.Now().After(deadline) {
				return errors.New("helper did not become ready")
			}
			time.Sleep(time.Millisecond)
		}
	}
	b.retryOps.kill = func(*exec.Cmd) error { t.Error("group signal after failed wait pin"); return nil }
	id := strings.Repeat("f", 32)
	if err := b.Start(context.Background(), id, retryProgram("live", marker)); err != nil {
		t.Fatal(err)
	}
	s := completed(t, b, id)
	if s.Termination != backend.UnknownTermination || s.State != backend.Failed || !strings.Contains(s.Reason, failure.Error()) {
		t.Fatalf("status=%+v", s)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	e := b.jobs[id]
	b.mu.Unlock()
	select {
	case <-e.retry.reaped:
	default:
		t.Fatal("original child was not reaped")
	}
	if _, err := e.retry.reader.Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("reader not closed: %v", err)
	}
	if _, err := e.output.file.Write([]byte("x")); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("retained output not closed: %v", err)
	}
}

func TestRetryCancellationRedelivery(t *testing.T) {
	for _, mode := range []string{"cancel", "close"} {
		t.Run(mode, func(t *testing.T) {
			b := newBackend(t)
			requireRetry(t, b)
			calls := 0
			ops := b.retryOps
			failure := errors.New("first delivery failed")
			b.retryOps.kill = func(c *exec.Cmd) error {
				calls++
				if calls == 1 {
					return failure
				}
				return ops.kill(c)
			}
			id := strings.Repeat("7", 32)
			if err := b.Start(context.Background(), id, retryTask("sleep")); err != nil {
				t.Fatal(err)
			}
			if err := b.Cancel(context.Background(), backend.Reference{ID: id}); !errors.Is(err, failure) {
				t.Fatalf("first Cancel=%v", err)
			}
			if mode == "cancel" {
				if err := b.Cancel(context.Background(), backend.Reference{ID: id}); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := b.Close(); err != nil {
					t.Fatal(err)
				}
			}
			s := completed(t, b, id)
			if s.State != backend.Canceled || s.Reason != "canceled by operator" || s.Termination != backend.UnknownTermination || calls < 2 {
				t.Fatalf("status=%+v signal calls=%d", s, calls)
			}
		})
	}
}

func TestRetryOutputFailureDrains(t *testing.T) {
	b := newBackend(t)
	requireRetry(t, b)
	marker := filepath.Join(t.TempDir(), "release")
	id := strings.Repeat("6", 32)
	if err := b.Start(context.Background(), id, retryProgram("retention-flood", marker)); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	out := b.jobs[id].output
	b.mu.Unlock()
	out.mu.Lock()
	out.file.Close()
	out.mu.Unlock()
	if err := os.WriteFile(marker, nil, 0600); err != nil {
		t.Fatal(err)
	}
	s := completed(t, b, id)
	if s.State != backend.Failed || s.Termination != backend.UnknownTermination || !strings.Contains(s.Reason, "retain output") {
		t.Fatalf("status=%+v", s)
	}
}

func TestRetryMissingForkHandle(t *testing.T) {
	for _, mode := range []string{"recovered-handle", "open-failure"} {
		t.Run(mode, func(t *testing.T) {
			b := newBackend(t)
			requireRetry(t, b)
			b.retryOps.prepare = func(*exec.Cmd) {} // Deterministically omit the dedicated fork handle.
			b.retryOps.kill = func(*exec.Cmd) error { t.Error("group signal on failed startup"); return nil }
			if mode == "open-failure" {
				b.retryOps.recoverHandle = func(*exec.Cmd) error { return errors.New("handle acquisition failed") }
			}
			id := strings.Repeat("5", 32)
			err := b.Start(context.Background(), id, retryProgram("bounded-live", ""))
			if err == nil || !strings.Contains(err.Error(), "fork did not return") {
				t.Fatalf("Start=%v", err)
			}
			s := completed(t, b, id)
			if s.State != backend.Failed || s.Termination != backend.UnknownTermination {
				t.Fatalf("status=%+v", s)
			}
			b.mu.Lock()
			e := b.jobs[id]
			b.mu.Unlock()
			if _, err := e.retry.reader.Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("reader not closed: %v", err)
			}
			if _, err := e.output.file.Write([]byte("x")); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("output not closed: %v", err)
			}
			closeErr := b.Close()
			if mode == "recovered-handle" && closeErr != nil {
				t.Fatal(closeErr)
			}
			if mode == "open-failure" && closeErr == nil {
				t.Fatal("unreaped original child was silently accepted")
			}
			select {
			case <-e.retry.reaped:
			case <-time.After(3 * time.Second):
				t.Fatal("bounded original helper did not eventually reap")
			}
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
			if retryHandleReady(e.cmd) {
				t.Fatal("dedicated handle was retained after completion")
			}
		})
	}
}
