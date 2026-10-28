//go:build linux || darwin

package local

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

// The child, pipe, Wait, group closure and later output writes are real; only
// the first retention failure is MODELED by the retainOutput seam. Later
// chunks would reach the real log and cross MaxLogBytes, whose successful
// truncation marker used to reset the first error.
func TestDurableOutputFirstErrorIsFinal(t *testing.T) {
	f := newDurableFixture(t, "0")
	f.task.Argv = []string{"/bin/sh", "-c", fmt.Sprintf("head -c %d /dev/zero", 2*backend.MaxLogBytes)}
	in := f.writeIntent()
	logPath := filepath.Join(f.logs, f.id+".log")
	marker := filepath.Join(f.logs, f.id+".truncated")
	log, err := os.OpenFile(logPath, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		log.Close()
		t.Fatal(err)
	}
	cmd := exec.Command(f.task.Argv[0], f.task.Argv[1:]...)
	cmd.Stdout, cmd.Stderr = writer, writer
	configure(cmd)
	if err := cmd.Start(); err != nil {
		reader.Close()
		writer.Close()
		log.Close()
		t.Fatal(err)
	}
	writer.Close()
	injected := errors.New("injected transient write failure")
	calls := 0
	s := &supervisor{dir: filepath.Join(f.state, f.id), intent: in, cmd: cmd,
		out: &output{file: log, marker: marker}}
	s.retainOutput = func(p []byte) error {
		calls++
		if calls == 1 {
			return injected
		}
		// A recovered later write, as output.Write would perform it.
		if _, err := s.out.Write(p); err != nil {
			return err
		}
		s.out.mu.Lock()
		defer s.out.mu.Unlock()
		return s.out.err
	}
	if err := s.wait(reader); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Errorf("retention attempted %d times after the first failure, want 1", calls)
	}
	if info, err := os.Stat(logPath); err != nil || info.Size() != 0 {
		t.Errorf("log after failed retention: %v, %v", info, err)
	}
	if _, err := os.Lstat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("truncation marker written after failed retention: %v", err)
	}
	var status durableStatus
	if err := readDurable(s.dir, durableStatusFile, maxDurableRecord, &status); err != nil {
		t.Fatal(err)
	}
	if status.State != backend.Failed || status.Reason != "retain output: "+injected.Error() ||
		status.OutputComplete || !status.GroupClosed || status.ExitCode == nil || *status.ExitCode != 0 {
		t.Fatalf("status after transient retention failure = %+v", status)
	}
	b := f.backend()
	defer b.Close()
	got, err := b.Observe(context.Background(), f.ref(""))
	if err != nil || got.State != backend.Failed || got.Instance != in.Instance {
		t.Fatalf("Observe = %+v, %v", got, err)
	}
}

// heldReservation returns the admission view's reservation for f's job.
func (f *durableFixture) heldReservation(b *Backend) (*resourceReservation, error) {
	f.t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	held, err := b.allResourceReservationsLocked()
	if err != nil {
		return nil, err
	}
	return held[f.id], nil
}

func (f *durableFixture) exists(name string) bool {
	f.t.Helper()
	_, err := os.Lstat(filepath.Join(f.state, f.id, name))
	return err == nil
}

// The empty restored job directory is MODELED damage; no workload runs.
func TestDurableMissingIntentOnRestoreHoldsAllocation(t *testing.T) {
	f := newDurableFixture(t, "0")
	for _, dir := range []string{f.state, filepath.Join(f.state, f.id)} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	b := f.backend()
	defer b.Close()
	for i := 0; i < 2; i++ {
		s, err := b.Observe(context.Background(), f.ref(""))
		if err == nil || s.State != "" {
			t.Fatalf("Observe %d of a record without intent = %+v, %v", i, s, err)
		}
		b.mu.Lock()
		r := b.durableReservationsLocked()
		b.mu.Unlock()
		if len(r) != 1 || r[0].ID != f.id || r[0].Valid {
			t.Fatalf("after Observe %d: reservations %+v", i, r)
		}
		if _, err := f.heldReservation(b); !errors.Is(err, ErrInventory) {
			t.Fatalf("after Observe %d: admission = %v, want unknown allocation", i, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := b.Cancel(ctx, f.ref("")); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Cancel = %v", err)
	}
	if err := b.Start(context.Background(), f.id, f.task); err == nil {
		t.Fatal("restarted backend started a job with a damaged record")
	}
	if f.exists(durableClaimFile) || f.exists(durableCancelFile) || f.exists(durableStatusFile) {
		t.Fatal("observation of a record without intent wrote a record")
	}
	if n := f.launches(); n != 0 {
		t.Fatalf("workload launched %d times", n)
	}
}

// The intent removal after a valid restore is MODELED damage; no workload runs.
func TestDurableRemovedIntentAfterRestoreHoldsAllocation(t *testing.T) {
	f := newDurableFixture(t, "0")
	f.task.CPUMillis = 1000
	f.task.Accelerator, f.task.AcceleratorCount = "GB10", 1
	in := f.writeIntent()
	in.Launch = durableLaunch{CPUMillis: 1000, GPUs: []string{gpuA}, Env: durableResourceEnvironment([]string{gpuA})}
	f.write(durableIntentFile, in)
	b := f.backend()
	defer b.Close()
	check := func(when string) {
		t.Helper()
		r, err := f.heldReservation(b)
		if err != nil || r == nil || r.cpuMillis != 1000 || len(r.gpus) != 1 || r.gpus[0].uuid != gpuA {
			t.Fatalf("%s: reservation %+v, %v", when, r, err)
		}
	}
	check("restored")
	if err := os.Remove(filepath.Join(f.state, f.id, durableIntentFile)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		s, err := b.Observe(context.Background(), f.ref(""))
		if err == nil || s.State != "" {
			t.Fatalf("Observe %d after intent removal = %+v, %v", i, s, err)
		}
		check(fmt.Sprintf("after Observe %d", i))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := b.Cancel(ctx, f.ref(in.Instance)); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Cancel = %v", err)
	}
	check("after Cancel")
	if f.exists(durableClaimFile) || f.exists(durableCancelFile) || f.exists(durableStatusFile) {
		t.Fatal("observation of a record without intent wrote a record")
	}
	if n := f.launches(); n != 0 {
		t.Fatalf("workload launched %d times", n)
	}
}

// A real Start that fails before writing any intent launched no supervisor,
// so its own backend may still release the allocation.
func TestDurablePrelaunchFailureReleases(t *testing.T) {
	f := newDurableFixture(t, "0")
	b := f.backend()
	defer b.Close()
	if err := os.WriteFile(filepath.Join(f.logs, f.id+".log"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := b.Start(context.Background(), f.id, f.task); err == nil {
		t.Fatal("Start reused existing output")
	}
	foreign := f.ref("")
	foreign.Task.Argv = []string{"/bin/true"}
	if _, err := b.Observe(context.Background(), foreign); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("foreign Observe = %v", err)
	}
	s, err := b.Observe(context.Background(), f.ref(""))
	if err != nil || s.State != backend.Failed || s.Instance != "" {
		t.Fatalf("Observe = %+v, %v", s, err)
	}
	b.mu.Lock()
	r := b.durableReservationsLocked()
	b.mu.Unlock()
	if len(r) != 0 {
		t.Fatalf("reservations %+v", r)
	}
	if n := f.launches(); n != 0 {
		t.Fatalf("workload launched %d times", n)
	}
}

// startingJob MODELS this backend's Start paused before it publishes the
// intent: startDurable has reserved the private directory and published its
// record with starting set, and launchDurable has not yet written anything.
func (f *durableFixture) startingJob(b *Backend) *durableJob {
	f.t.Helper()
	if err := os.Mkdir(filepath.Join(f.state, f.id), 0700); err != nil {
		f.t.Fatal(err)
	}
	j := &durableJob{launch: durableLaunch{Env: durableResourceEnvironment(nil)}, task: backend.CloneTask(f.task), valid: true, starting: true}
	b.mu.Lock()
	b.durable.jobs[f.id] = j
	b.mu.Unlock()
	return j
}

// The starting frontier is MODELED by startingJob. The supervisor that later
// claims, the workload and the delivered cancellation are real.
func TestDurableCancelAwaitsStartingInstance(t *testing.T) {
	f := newDurableFixture(t, "30")
	b := f.backend()
	defer b.Close()
	f.cleanupJob()
	f.startingJob(b)
	dir := filepath.Join(f.state, f.id)

	s, err := b.Observe(context.Background(), f.ref(""))
	if err != nil || s.State != backend.Starting || s.Instance != "" {
		t.Fatalf("Observe before intent = %+v, %v", s, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- b.Cancel(ctx, f.ref("")) }()
	time.Sleep(4 * durablePoll)
	select {
	case err := <-done:
		t.Fatalf("Cancel returned %v before the instance was published", err)
	default:
	}
	if f.exists(durableCancelFile) {
		t.Fatal("cancel request written before the instance was known")
	}

	in := f.writeIntent() // the paused Start publishes its intent
	f.waitFile(durableCancelFile)
	var c durableCancel
	if err := readDurable(dir, durableCancelFile, maxDurableRecord, &c); err != nil {
		t.Fatal(err)
	}
	if c != (durableCancel{Version: durableVersion, Instance: in.Instance}) {
		t.Fatalf("cancel request %+v, want instance %s", c, in.Instance)
	}
	if f.exists(durableClaimFile) {
		t.Fatal("an observer fenced the starting supervisor")
	}

	// The original supervisor now claims, launches and delivers the request.
	log, err := os.OpenFile(filepath.Join(f.logs, f.id+".log"), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	readyR, readyW, err := os.Pipe()
	if err != nil {
		log.Close()
		t.Fatal(err)
	}
	defer readyR.Close()
	defer readyW.Close()
	supervised := make(chan error, 1)
	go func() {
		supervised <- superviseWithIdentity(dir, log, readyW, func(cmd *exec.Cmd) (processIdentity, error) {
			return identifyProcess(cmd.Process.Pid)
		})
	}()
	if err := <-done; err != nil {
		t.Fatalf("Cancel = %v", err)
	}
	if err := <-supervised; err != nil {
		t.Fatalf("supervisor = %v", err)
	}
	s, err = b.Observe(context.Background(), f.ref(in.Instance))
	if err != nil || s.State != backend.Canceled || s.Instance != in.Instance {
		t.Fatalf("after cancel = %+v, %v", s, err)
	}
	if err := b.Cancel(ctx, f.ref(in.Instance)); err != nil {
		t.Fatalf("repeated Cancel = %v", err)
	}
	if err := b.Start(context.Background(), f.id, f.task); err == nil {
		t.Fatal("started the job again")
	}
	if n := f.launches(); n > 1 {
		t.Fatalf("workload launched %d times", n)
	}
}

// The starting frontier is MODELED by startingJob; no workload runs.
func TestDurableStartingRefusesForeignReference(t *testing.T) {
	f := newDurableFixture(t, "0")
	b := f.backend()
	defer b.Close()
	f.startingJob(b)
	foreign := f.ref("")
	foreign.Task.Argv = []string{"/bin/true"}
	other := f.ref("local-" + strings.Repeat("b", 32))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err := b.Observe(ctx, foreign); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("foreign task before intent: Observe = %v", err)
	}
	if err := b.Cancel(ctx, foreign); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("foreign task before intent: Cancel = %v", err)
	}
	if s, err := b.Observe(ctx, other); err == nil {
		t.Fatalf("unpublished instance accepted: %+v", s)
	}

	in := f.writeIntent()
	s, err := b.Observe(ctx, f.ref(""))
	if err != nil || s.State != backend.Starting || s.Instance != in.Instance {
		t.Fatalf("Observe after intent = %+v, %v", s, err)
	}
	for _, ref := range []backend.Reference{foreign, other} {
		if err := b.Cancel(ctx, ref); !errors.Is(err, backend.ErrIdentityMismatch) {
			t.Fatalf("Cancel(%+v) = %v", ref, err)
		}
	}
	if f.exists(durableCancelFile) || f.exists(durableClaimFile) {
		t.Fatal("foreign reference wrote a record")
	}
}

// The starting frontier and the existing requests are MODELED; no workload runs.
func TestDurableCancelValidatesExistingRequest(t *testing.T) {
	for _, name := range []string{"empty instance", "other instance", "malformed", "matching"} {
		t.Run(name, func(t *testing.T) {
			f := newDurableFixture(t, "0")
			b := f.backend()
			defer b.Close()
			f.startingJob(b)
			in := f.writeIntent()
			path := filepath.Join(f.state, f.id, durableCancelFile)
			switch name {
			case "empty instance":
				f.write(durableCancelFile, durableCancel{Version: durableVersion})
			case "other instance":
				f.write(durableCancelFile, durableCancel{Version: durableVersion, Instance: "local-" + strings.Repeat("b", 32)})
			case "malformed":
				if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			case "matching":
				f.write(durableCancelFile, durableCancel{Version: durableVersion, Instance: in.Instance})
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 4*durablePoll)
			defer cancel()
			err = b.Cancel(ctx, f.ref(""))
			if name == "matching" {
				// Already delivered: Cancel awaits the original supervisor.
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Cancel with matching request = %v", err)
				}
			} else if err == nil || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Cancel with %s request = %v", name, err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("cancel request changed: %q, %v", after, err)
			}
		})
	}
}
