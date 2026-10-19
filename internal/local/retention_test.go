package local

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

const retentionOwner = "0123456789abcdef0123456789abcdef"

func retentionConfig(t *testing.T, directory string) RetentionConfig {
	t.Helper()
	unit, volume, err := outputFilesystem(directory)
	if err != nil {
		t.Fatal(err)
	}
	return RetentionConfig{Owner: retentionOwner, OutputUnit: unit, Volume: volume, OutputBytes: 256 << 20, OutputDomains: 256, OutputFiles: 512}
}
func retainedBackend(t *testing.T) *Backend {
	t.Helper()
	dir := t.TempDir()
	b, err := NewRetained(dir, retentionConfig(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}
func retainedReference(t *testing.T, b *Backend, id string) backend.OutputReference {
	t.Helper()
	binding, err := backend.LocalBinding(retentionOwner)
	if err != nil {
		t.Fatal(err)
	}
	return backend.OutputReference{Owner: retentionOwner, JobID: id, AttemptID: id, Binding: binding, Generation: 1}
}
func startRetained(t *testing.T, b *Backend, id string, task backend.Task) backend.OutputReference {
	t.Helper()
	ref := retainedReference(t, b, id)
	if err := b.BindOutput(ref); err != nil {
		t.Fatal(err)
	}
	if err := b.Start(context.Background(), id, task); err != nil {
		t.Fatal(err)
	}
	return ref
}
func closureReference(t *testing.T, b *Backend, ref backend.OutputReference) backend.OutputReference {
	t.Helper()
	receipt, err := b.Retention().CheckOutputClosed(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	ref.ClosureID, ref.PayloadReleased = receipt.ClosureID, receipt.PayloadReleased
	return ref
}

func TestRetainedCompletion(t *testing.T) {
	for _, mode := range []string{"success", "fail", "output", "cancel", "timeout", "start-failure"} {
		t.Run(mode, func(t *testing.T) {
			b := retainedBackend(t)
			id := strings.Repeat("1", 32)
			ref := retainedReference(t, b, id)
			program := task(mode)
			if mode == "cancel" || mode == "timeout" {
				program = task("sleep")
			}
			if mode == "timeout" {
				program.TimeoutSeconds = 1
			}
			if mode == "start-failure" {
				program.Argv = []string{filepath.Join(t.TempDir(), "missing-program")}
			}
			if err := b.BindOutput(ref); err != nil {
				t.Fatal(err)
			}
			err := b.Start(context.Background(), id, program)
			if (err != nil) != (mode == "start-failure") {
				t.Fatalf("Start=%v", err)
			}
			if mode == "cancel" {
				if err := b.Cancel(context.Background(), backend.Reference{ID: id}); err != nil {
					t.Fatal(err)
				}
			}
			status := completed(t, b, id)
			if status.Instance != "" || status.Termination != backend.UnknownTermination {
				t.Fatalf("default identity/proof changed: %+v", status)
			}
			ref = closureReference(t, b, ref)
			if !ref.PayloadReleased || !validClosure(ref.ClosureID) {
				t.Fatalf("closure=%+v", ref)
			}
			b.mu.Lock()
			e := b.jobs[id]
			if e.cmd != nil || e.output.file != nil || e.output.err != nil || e.owned.reader != nil || e.owned.handleErr != nil || e.owned.waitErr != nil || e.owned.outputResult != (defaultOutputResult{}) {
				t.Error("finished payload references remain")
			}
			b.mu.Unlock()
			if err := b.Cancel(context.Background(), backend.Reference{ID: id}); err != nil {
				t.Fatal(err)
			}
			if err := b.Retention().RemoveExecution(context.Background(), backend.Reference{ID: id, Task: program, Binding: ref.Binding}); err == nil {
				t.Fatal("default gained execution cleanup")
			}
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRetainedClosureBeforeReap(t *testing.T) {
	b := retainedBackend(t)
	id := strings.Repeat("2", 32)
	t.Setenv("SKYGO_RETENTION_INHERITED", strings.Repeat("inherited-payload-", 1024))
	program := defaultTask(7)
	program.Argv = append(program.Argv, "argv-payload-sentinel")
	program.Directory = t.TempDir()
	program.Env["SECRET"] = strings.Repeat("task-payload-", 1024)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	ops := b.defaultOps
	b.defaultOps.reap = func(cmd *exec.Cmd) error { close(entered); <-release; return ops.reap(cmd) }
	ref := startRetained(t, b, id, program)
	awaitDefault(t, entered, "owned reaper")
	deadline := time.Now().Add(3 * time.Second)
	var receipt backend.OutputReceipt
	for {
		var err error
		receipt, err = b.Retention().CheckOutputClosed(context.Background(), ref)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	if receipt.PayloadReleased {
		t.Fatal("blocked original reaper released payload")
	}
	elevated := ref
	elevated.ClosureID = receipt.ClosureID
	elevated.PayloadReleased = true
	if _, err := b.Retention().CheckOutputClosed(context.Background(), elevated); err == nil {
		t.Fatal("caller elevated payload release")
	}
	if err := b.Retention().RemoveOutput(context.Background(), elevated); err == nil {
		t.Fatal("deletion accepted elevated payload fact")
	}
	b.mu.Lock()
	e := b.jobs[id]
	if e.cmd == nil || e.cmd.Dir != program.Directory || !strings.Contains(strings.Join(e.cmd.Args, " "), "argv-payload-sentinel") || !strings.Contains(strings.Join(e.cmd.Env, " "), "inherited-payload-") || !strings.Contains(strings.Join(e.cmd.Env, " "), "task-payload-") {
		t.Error("real owner did not retain expected payload before join")
	}
	b.mu.Unlock()
	if err := b.Cancel(context.Background(), backend.Reference{ID: id}); err != nil {
		t.Fatal(err)
	}
	if err := b.stop(e, backend.Failed, "execution timed out"); err != nil {
		t.Fatal(err)
	}
	unblock()
	status := completed(t, b, id)
	if status.State != backend.Failed || status.ExitCode == nil || *status.ExitCode != 7 {
		t.Fatalf("status=%+v", status)
	}
	closed := closureReference(t, b, ref)
	if closed.ClosureID != receipt.ClosureID || !closed.PayloadReleased {
		t.Fatalf("closure identity/release=%+v", closed)
	}
	if err := b.stop(e, backend.Canceled, "late timer"); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRetainedOutputIdentityAndMeasurement(t *testing.T) {
	b := retainedBackend(t)
	id := strings.Repeat("3", 32)
	ref := startRetained(t, b, id, task("output"))
	completed(t, b, id)
	ref = closureReference(t, b, ref)
	usage, err := b.OutputUsage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var allocated uint64
	for _, suffix := range []string{".log", ".truncated"} {
		info, err := os.Stat(filepath.Join(b.directory, id+suffix))
		if err != nil {
			t.Fatal(err)
		}
		n, err := allocatedOutput(info)
		if err != nil {
			t.Fatal(err)
		}
		allocated += n
	}
	if usage.Files != 2 || usage.LogicalBytes != backend.MaxLogBytes+uint64(len("truncated\n")) || usage.AllocatedBytes != allocated {
		t.Fatalf("measurement=%+v allocated=%d", usage, allocated)
	}
	if b.OutputReservation() != uint64(backend.MaxLogBytes)+4*b.retention.config.OutputUnit {
		t.Fatal("reservation arithmetic changed")
	}
	for _, change := range []func(*backend.OutputReference){
		func(r *backend.OutputReference) { r.Owner = strings.Repeat("f", 32) }, func(r *backend.OutputReference) { r.JobID = strings.Repeat("f", 32) }, func(r *backend.OutputReference) { r.AttemptID = strings.Repeat("f", 32) }, func(r *backend.OutputReference) { r.Binding = "wrong" }, func(r *backend.OutputReference) { r.Generation++ }, func(r *backend.OutputReference) { r.ClosureID = "forged" },
	} {
		bad := ref
		change(&bad)
		if err := b.Retention().RemoveOutput(context.Background(), bad); err == nil {
			t.Fatalf("accepted wrong identity %+v", bad)
		}
	}
	if err := b.Retention().RemoveOutput(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	// A lost success reply retries the same receipt without creating a tombstone.
	if err := b.Retention().RemoveOutput(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	usage, err = b.OutputUsage(context.Background())
	if err != nil || usage != (OutputUsage{}) {
		t.Fatalf("after deletion=%+v,%v", usage, err)
	}
	b.mu.Lock()
	owned := b.jobs[id] != nil
	domains := len(b.retention.outputs)
	b.mu.Unlock()
	if !owned || domains != 0 {
		t.Fatalf("output-only changed native ownership: owned=%v domains=%d", owned, domains)
	}
	if status, err := b.Observe(context.Background(), backend.Reference{ID: id}); err != nil || status.State != backend.Succeeded {
		t.Fatalf("Observe=%+v,%v", status, err)
	}
}

func TestRetainedBlockedWriter(t *testing.T) {
	b := retainedBackend(t)
	id := strings.Repeat("4", 32)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	ops := b.defaultOps
	var enter sync.Once
	b.defaultOps.write = func(out *output, p []byte) (int, error) {
		enter.Do(func() { close(entered) })
		<-release
		return ops.write(out, p)
	}
	ref := startRetained(t, b, id, task("output"))
	awaitDefault(t, entered, "blocked owned writer")
	if _, err := b.Retention().CheckOutputClosed(context.Background(), ref); err == nil {
		t.Fatal("blocked writer supplied closure")
	}
	before := time.Now()
	err := b.Close()
	if err == nil || !strings.Contains(err.Error(), "timed out") || time.Since(before) > 4*time.Second {
		t.Fatalf("bounded Close=%v duration=%v", err, time.Since(before))
	}
	b.mu.Lock()
	e := b.jobs[id]
	kept := e.cmd != nil && !e.retained.released
	b.mu.Unlock()
	if !kept {
		t.Fatal("blocked writer payload was sanitized")
	}
	unblock()
	completed(t, b, id)
	if ref = closureReference(t, b, ref); !ref.PayloadReleased {
		t.Fatal("joined writer did not release payload")
	}
}

func TestRetainedCloseFailure(t *testing.T) {
	b := retainedBackend(t)
	id := strings.Repeat("5", 32)
	failure := errors.New("retained close failed")
	ops := b.defaultOps
	b.defaultOps.closeOutput = func(out *output) error { return errors.Join(ops.closeOutput(out), failure) }
	ref := startRetained(t, b, id, defaultTask(0))
	completed(t, b, id)
	if _, err := b.Retention().CheckOutputClosed(context.Background(), ref); err == nil {
		t.Fatal("failed close supplied closure")
	}
	b.mu.Lock()
	e := b.jobs[id]
	retained := e.cmd != nil && !e.retained.released
	b.mu.Unlock()
	if !retained {
		t.Fatal("failed close released payload")
	}
	if err := b.Close(); !errors.Is(err, failure) {
		t.Fatalf("Close=%v", err)
	}
}

func TestRetainedRestartAndAbsence(t *testing.T) {
	dir := t.TempDir()
	config := retentionConfig(t, dir)
	id := strings.Repeat("6", 32)
	first, err := NewRetained(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	ref := startRetained(t, first, id, defaultTask(0))
	completed(t, first, id)
	ref = closureReference(t, first, ref)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	// A new key cannot authenticate old receipts by itself. Trusted startup data
	// must validate and install the exact persisted reservation and closure first.
	config.Outputs = []backend.OutputReference{ref}
	restarted, err := NewRetained(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	status, err := restarted.Observe(context.Background(), backend.Reference{ID: id})
	if err != nil || status.State != backend.Unknown || status.Termination != backend.UnknownTermination {
		t.Fatalf("restart=%+v,%v", status, err)
	}
	bad := ref
	bad.JobID = strings.Repeat("e", 32)
	if err := restarted.Retention().RemoveOutput(context.Background(), bad); err == nil {
		t.Fatal("historical identity mismatch accepted")
	}
	if err := restarted.Retention().RemoveOutput(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Retention().RemoveOutput(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	// This backend received no historical reservation: changed process key refuses
	// the old MAC even when both paths happen to be absent.
	blank, err := NewRetained(dir, retentionConfig(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blank.Close() })
	if err := blank.Retention().RemoveOutput(context.Background(), ref); err == nil {
		t.Fatal("changed-key absence accepted historical receipt")
	}
	if _, err := blank.Retention().CheckOutputClosed(context.Background(), ref); err == nil {
		t.Fatal("absence fabricated payload/closure proof")
	}
}

func TestRetainedAdmissionAndReplacement(t *testing.T) {
	b := retainedBackend(t)
	id := strings.Repeat("7", 32)
	ref := retainedReference(t, b, id)
	if err := b.Start(context.Background(), id, defaultTask(0)); err == nil {
		t.Fatal("unbound Start accepted")
	}
	if err := b.BindOutput(ref); err != nil {
		t.Fatal(err)
	}
	b.retention.config.OutputBytes = b.OutputReservation() - 1
	if err := b.Start(context.Background(), id, defaultTask(0)); err == nil {
		t.Fatal("unreserved Start accepted")
	}
	if _, err := os.Stat(filepath.Join(b.directory, id+".log")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("refused start created output: %v", err)
	}
	b.retention.config.OutputBytes = 256 << 20
	if err := b.Start(context.Background(), id, defaultTask(0)); err != nil {
		t.Fatal(err)
	}
	completed(t, b, id)
	ref = closureReference(t, b, ref)
	original := filepath.Join(b.directory, id+".log")
	saved := filepath.Join(t.TempDir(), "original")
	if err := os.Rename(original, saved); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(original, []byte("replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := b.Retention().RemoveOutput(context.Background(), ref); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("replacement delete=%v", err)
	}
	got, err := os.ReadFile(original)
	if err != nil || string(got) != "replacement" {
		t.Fatalf("replacement changed: %q,%v", got, err)
	}
}

func TestRetainedDisabled(t *testing.T) {
	b := newBackend(t)
	id := strings.Repeat("8", 32)
	if b.Retention() != nil || b.OutputReservation() != 0 {
		t.Fatal("ordinary New enabled retention")
	}
	if _, err := b.OutputUsage(context.Background()); err == nil {
		t.Fatal("disabled sample fabricated zero")
	}
	if err := b.Start(context.Background(), id, defaultTask(0)); err != nil {
		t.Fatal(err)
	}
	completed(t, b, id)
	b.mu.Lock()
	kept := b.jobs[id].cmd != nil
	b.mu.Unlock()
	if !kept {
		t.Fatal("disabled command lifetime changed")
	}
}

func TestRetainedRetry(t *testing.T) {
	b := retainedBackend(t)
	if !b.SupportsRetry() {
		t.Skip("qualified retry process handles unavailable on this host")
	}
	id := strings.Repeat("9", 32)
	program := task("fail")
	program.Retry = &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7}}
	ref := startRetained(t, b, id, program)
	status := completed(t, b, id)
	ref = closureReference(t, b, ref)
	if status.Termination != backend.StoppedWithExitCode || status.ExitCode == nil || *status.ExitCode != 7 || !ref.PayloadReleased {
		t.Fatalf("retry=%+v,%+v", status, ref)
	}
	execution := backend.Reference{ID: id, Task: program, Binding: ref.Binding, Instance: status.Instance}
	wrong := execution
	wrong.Task = task("success")
	wrong.Task.Retry = program.Retry
	if err := b.Retention().RemoveExecution(context.Background(), wrong); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("wrong task cleanup=%v", err)
	}
	if err := b.Retention().RemoveExecution(context.Background(), execution); err != nil {
		t.Fatal(err)
	}
	// Native map retirement and output deletion are independent operations.
	if err := b.Retention().RemoveOutput(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	if err := b.Retention().RemoveExecution(context.Background(), execution); err != nil {
		t.Fatal(err)
	}
	if err := b.Retention().RemoveOutput(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Retention().CheckOutputClosed(context.Background(), ref); err == nil {
		t.Fatal("MAC-only absence fabricated released proof")
	}
	b.mu.Lock()
	jobs, outputs := len(b.jobs), len(b.retention.outputs)
	b.mu.Unlock()
	if jobs != 0 || outputs != 0 {
		t.Fatalf("retired maps: jobs=%d outputs=%d", jobs, outputs)
	}
}

func TestRetainedUnitPolicy(t *testing.T) {
	for _, unit := range []uint64{0, 1, 4095, 4097, 65537} {
		t.Run(fmt.Sprint(unit), func(t *testing.T) {
			dir := t.TempDir()
			cfg := retentionConfig(t, dir)
			cfg.OutputUnit = unit
			if _, err := NewRetained(dir, cfg); err == nil {
				t.Fatal("invalid unit accepted")
			}
		})
	}
	dir := t.TempDir()
	cfg := retentionConfig(t, dir)
	cfg.Volume++
	if _, err := NewRetained(dir, cfg); err == nil {
		t.Fatal("changed volume accepted")
	}
}

func TestRetainedDeleteSyncRetry(t *testing.T) {
	for _, test := range []struct {
		name              string
		failAt, wantCalls int
		logRemains        bool
	}{{"prefix", 1, 3, true}, {"final", 2, 4, false}} {
		t.Run(test.name, func(t *testing.T) {
			b := retainedBackend(t)
			id := strings.Repeat("a", 32)
			ref := startRetained(t, b, id, defaultTask(0))
			completed(t, b, id)
			ref = closureReference(t, b, ref)
			failure := errors.New("directory sync failed")
			calls := 0
			b.retention.syncOutput = func(path string) error {
				calls++
				if calls == test.failAt {
					return failure
				}
				return syncOutputDirectory(path)
			}
			if err := b.Retention().RemoveOutput(context.Background(), ref); !errors.Is(err, failure) {
				t.Fatalf("first remove=%v", err)
			}
			b.mu.Lock()
			pending := b.retention.outputs[id] != nil && !b.retention.outputs[id].removed
			b.mu.Unlock()
			if !pending {
				t.Fatal("failed sync retired deletion identity")
			}
			_, err := os.Stat(filepath.Join(b.directory, id+".log"))
			if test.logRemains && err != nil || !test.logRemains && !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("partial log presence=%v, want remains=%v", err, test.logRemains)
			}
			if err := b.Retention().RemoveOutput(context.Background(), ref); err != nil {
				t.Fatal(err)
			}
			if calls != test.wantCalls {
				t.Fatalf("sync calls=%d want%d", calls, test.wantCalls)
			}
			bad := ref
			bad.JobID = strings.Repeat("f", 32)
			if err := b.Retention().RemoveOutput(context.Background(), bad); err == nil {
				t.Fatal("MAC-only absence accepted wrong tuple")
			}
			if err := b.Retention().RemoveOutput(context.Background(), ref); err != nil {
				t.Fatal(err)
			}
			if _, err := b.Retention().CheckOutputClosed(context.Background(), ref); err == nil {
				t.Fatal("MAC-only absence fabricated release proof")
			}
		})
	}
}

func TestRetainedHistoricalBound(t *testing.T) {
	dir := t.TempDir()
	cfg := retentionConfig(t, dir)
	binding, _ := backend.LocalBinding(retentionOwner)
	for i := 0; i < 256; i++ {
		id := fmt.Sprintf("%032x", i)
		cfg.Outputs = append(cfg.Outputs, backend.OutputReference{Owner: retentionOwner, JobID: id, AttemptID: id, Binding: binding, Generation: 1, ClosureID: "offline-closed-lineage", PayloadReleased: true})
	}
	// Lower configured thresholds do not prevent safe collection of trusted
	// grandfathered reservations above the new policy limits.
	cfg.OutputDomains = 1
	cfg.OutputBytes = 1
	cfg.OutputFiles = 1
	b, err := NewRetained(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.BindOutput(retainedReference(t, b, strings.Repeat("f", 32))); err == nil {
		t.Fatal("overquota startup grew output set")
	}
	for _, ref := range cfg.Outputs {
		if err := b.Retention().RemoveOutput(context.Background(), ref); err != nil {
			t.Fatal(err)
		}
	}
	b.retention.config.OutputDomains = 256
	b.retention.config.OutputBytes = 256 << 20
	b.retention.config.OutputFiles = 512
	live := (uint64(256) << 20) / b.OutputReservation()
	for i := uint64(0); i < live; i++ {
		id := fmt.Sprintf("%032x", i+256)
		if err := b.BindOutput(retainedReference(t, b, id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.BindOutput(retainedReference(t, b, strings.Repeat("e", 32))); err == nil {
		t.Fatal("byte budget ignored")
	}
	if len(b.retention.outputs) != 256+int(live) {
		t.Fatalf("bounded historical/live rows=%d", len(b.retention.outputs))
	}
}

func TestRetainedRetryLateReaper(t *testing.T) {
	b := retainedBackend(t)
	if !b.SupportsRetry() {
		t.Skip("native qualified retry handles unavailable")
	}
	id := strings.Repeat("b", 32)
	ref := retainedReference(t, b, id)
	program := backend.Task{Argv: []string{os.Args[0], "-test.run=^TestDefaultProgram$"}, Env: map[string]string{"SKYGO_DEFAULT_PROGRAM": "bounded", "PAYLOAD_SENTINEL": strings.Repeat("pending-reaper-", 1024)}, Retry: &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7}}}
	// Fork the real owned child without a dedicated retry handle. Deny its
	// original-child fallback: only its natural bounded lifetime may finish Wait.
	b.retryOps.prepare = func(cmd *exec.Cmd) { configure(cmd) }
	denied := errors.New("original handle denied")
	b.retryOps.recoverHandle = func(*exec.Cmd) error { return denied }
	if err := b.BindOutput(ref); err != nil {
		t.Fatal(err)
	}
	if err := b.Start(context.Background(), id, program); !errors.Is(err, denied) {
		t.Fatalf("Start=%v", err)
	}
	status := completed(t, b, id)
	if status.Termination != backend.UnknownTermination || status.ExitCode != nil {
		t.Fatalf("uncertain reaper=%+v", status)
	}
	closed := closureReference(t, b, ref)
	if closed.PayloadReleased {
		t.Fatal("live sole reaper released payload")
	}
	b.mu.Lock()
	e := b.jobs[id]
	cmd := e.cmd
	pending := cmd != nil && strings.Contains(strings.Join(cmd.Env, " "), "pending-reaper-")
	b.mu.Unlock()
	if !pending {
		t.Fatal("actual pending Wait owner lost command payload")
	}
	if err := b.Close(); err == nil || !strings.Contains(err.Error(), "reaping remains uncertain") {
		t.Fatalf("Close=%v", err)
	}
	awaitDefault(t, e.retry.reaped, "original bounded child reaping")
	deadline := time.Now().Add(3 * time.Second)
	for {
		b.mu.Lock()
		released := e.retained.released
		b.mu.Unlock()
		if released {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("late sole reaper did not release payload")
		}
		time.Sleep(time.Millisecond)
	}
	b.mu.Lock()
	if e.cmd != nil || e.retry.reader != nil || e.retry.startErr != nil || e.output.file != nil {
		t.Error("late owner retained payload references")
	}
	b.mu.Unlock()
	if status, err := b.Observe(context.Background(), backend.Reference{ID: id}); err != nil || status.Termination != backend.UnknownTermination || status.ExitCode != nil {
		t.Fatalf("late finish invented execution proof: %+v %v", status, err)
	}
}

func TestRetainedInventoryProtection(t *testing.T) {
	for _, name := range []string{"unknown", "symlink", "duplicate", "overfull"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := retentionConfig(t, dir)
			binding, _ := backend.LocalBinding(retentionOwner)
			id := strings.Repeat("c", 32)
			ref := backend.OutputReference{Owner: retentionOwner, JobID: id, AttemptID: id, Binding: binding, Generation: 1, ClosureID: "offline-closed-lineage", PayloadReleased: true}
			cfg.Outputs = []backend.OutputReference{ref}
			path := filepath.Join(dir, id+".log")
			switch name {
			case "unknown":
				path = filepath.Join(dir, "unexplained")
				if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				target := filepath.Join(t.TempDir(), "target")
				if err := os.WriteFile(target, []byte("keep"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			case "duplicate":
				cfg.Outputs = append(cfg.Outputs, ref)
			case "overfull":
				if err := os.WriteFile(path, []byte(strings.Repeat("x", backend.MaxLogBytes+1)), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := NewRetained(dir, cfg); err == nil {
				t.Fatal("invalid inventory accepted")
			}
			if name != "duplicate" {
				if _, err := os.Lstat(path); err != nil {
					t.Fatalf("invalid inventory was modified: %v", err)
				}
			}
		})
	}
}

func TestRetainedRootChangeHoldsAdmission(t *testing.T) {
	b := retainedBackend(t)
	id := strings.Repeat("d", 32)
	ref := retainedReference(t, b, id)
	if err := b.BindOutput(ref); err != nil {
		t.Fatal(err)
	}
	saved := b.directory + "-saved"
	if err := os.Rename(b.directory, saved); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(b.directory, 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(b.directory); _ = os.Rename(saved, b.directory) })
	if _, err := b.OutputUsage(context.Background()); err == nil {
		t.Fatal("replacement root became a valid zero sample")
	}
	if err := b.Start(context.Background(), id, defaultTask(0)); err == nil {
		t.Fatal("replacement root accepted Start")
	}
	if _, err := os.Stat(filepath.Join(b.directory, id+".log")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("held Start modified replacement root: %v", err)
	}
}

func TestRetainedInterruptedTruncatedDeletion(t *testing.T) {
	dir := t.TempDir()
	cfg := retentionConfig(t, dir)
	first, err := NewRetained(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	id := strings.Repeat("f", 32)
	ref := startRetained(t, first, id, task("output"))
	completed(t, first, id)
	ref = closureReference(t, first, ref)
	failure := errors.New("interrupted before second unlink")
	var removed, trace []string
	first.retention.syncOutput = func(path string) error { trace = append(trace, "sync"); return syncOutputDirectory(path) }
	first.retention.removeFile = func(path string) error {
		removed = append(removed, filepath.Ext(path))
		trace = append(trace, "unlink"+filepath.Ext(path))
		if len(removed) == 2 {
			return failure
		}
		return os.Remove(path)
	}
	if err := first.Retention().RemoveOutput(context.Background(), ref); !errors.Is(err, failure) {
		t.Fatalf("partial deletion=%v", err)
	}
	if strings.Join(trace, ",") != "unlink.truncated,sync,unlink.log" {
		t.Fatalf("durable deletion prefix=%v", trace)
	}
	if strings.Join(removed, ",") != ".truncated,.log" {
		t.Fatalf("unlink order=%v", removed)
	}
	if _, err := os.Stat(filepath.Join(dir, id+".log")); err != nil {
		t.Fatalf("surviving log=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, id+".truncated")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("first unlink=%v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	// Persisted closed-output authority is transferred exactly as by R1 startup;
	// the partial filesystem state supplies no new closure or stopped evidence.
	cfg.Outputs = []backend.OutputReference{ref}
	restarted, err := NewRetained(dir, cfg)
	if err != nil {
		t.Fatalf("restart after one unlink=%v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	if status, err := restarted.Observe(context.Background(), backend.Reference{ID: id}); err != nil || status.State != backend.Unknown || status.Termination != backend.UnknownTermination {
		t.Fatalf("restart evidence=%+v,%v", status, err)
	}
	if err := restarted.Retention().RemoveOutput(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Retention().RemoveOutput(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	sample, err := restarted.OutputUsage(context.Background())
	if err != nil || sample != (OutputUsage{}) {
		t.Fatalf("final output=%+v,%v", sample, err)
	}
}

type retentionPayloadError struct{ task backend.Task }

func (e *retentionPayloadError) Error() string {
	return strings.Repeat("retained-error-payload-", 1024)
}

func TestRetainedErrorPayloadReleased(t *testing.T) {
	b := retainedBackend(t)
	id := strings.Repeat("0", 32)
	program := backend.Task{Argv: []string{os.Args[0], "-test.run=^TestDefaultProgram$", "error-argv-sentinel"}, Directory: t.TempDir(), Env: map[string]string{"SKYGO_DEFAULT_PROGRAM": "bounded", "TASK_ERROR_SENTINEL": strings.Repeat("error-env-", 1024)}}
	failure := &retentionPayloadError{task: program}
	ops := b.defaultOps
	b.defaultOps.wait = func(*exec.Cmd) error {
		// This fault callback owns one call. Drop the fixture's own captured Task
		// before returning it to the real lifecycle error owners under test.
		b.defaultOps.wait = ops.wait
		return failure
	}
	b.defaultOps.openHandle = func(*exec.Cmd) (defaultHandle, error) {
		return defaultHandle{fd: -1}, errors.New("test owned handle unavailable")
	}
	b.defaultOps.kill = func(*exec.Cmd) error { t.Error("group signal without positive terminal record"); return nil }
	ref := startRetained(t, b, id, program)
	status := completed(t, b, id)
	if status.Termination != backend.UnknownTermination || status.Instance != "" || len(status.Reason) > 1024 {
		t.Fatalf("error status=%+v", status)
	}
	ref = closureReference(t, b, ref)
	if !ref.PayloadReleased {
		t.Fatal("joined error owners did not release payload")
	}
	b.mu.Lock()
	e := b.jobs[id]
	cleared := e.cmd == nil && e.output.file == nil && e.output.err == nil && e.owned.reader == nil && e.owned.handleErr == nil && e.owned.waitErr == nil && e.owned.outputResult == (defaultOutputResult{})
	cleanup := e.owned.cleanupErr
	b.mu.Unlock()
	if !cleared {
		t.Fatal("completed error owners retained payload captures")
	}
	var retained *retentionPayloadError
	if cleanup == nil || errors.As(cleanup, &retained) || len(cleanup.Error()) > 1024 {
		t.Fatalf("cleanup retained payload graph: %T", cleanup)
	}
	if err := b.Close(); err == nil || errors.As(err, &retained) {
		t.Fatalf("Close lost uncertainty or retained task: %T", err)
	}
}

func TestRetainedBlockedFileClose(t *testing.T) {
	b := retainedBackend(t)
	id := strings.Repeat("a", 32)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	ops := b.defaultOps
	b.defaultOps.closeOutput = func(out *output) error { close(entered); <-release; return ops.closeOutput(out) }
	ref := startRetained(t, b, id, defaultTask(0))
	awaitDefault(t, entered, "blocked original file Close")
	if _, err := b.Retention().CheckOutputClosed(context.Background(), ref); err == nil {
		t.Fatal("blocked Close supplied output proof")
	}
	b.mu.Lock()
	e := b.jobs[id]
	kept := e.cmd != nil && !e.retained.released
	b.mu.Unlock()
	if !kept {
		t.Fatal("active Close owner payload released")
	}
	before := time.Now()
	err := b.Close()
	if err == nil || !strings.Contains(err.Error(), "timed out") || time.Since(before) > 4*time.Second {
		t.Fatalf("bounded Close=%v duration=%v", err, time.Since(before))
	}
	unblock()
	completed(t, b, id)
	ref = closureReference(t, b, ref)
	if !ref.PayloadReleased {
		t.Fatal("finished original file Close did not release payload")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}
