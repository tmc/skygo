//go:build linux || darwin

package local

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

// readyGateEnv names a gate directory. A supervisor launched while it is set
// runs as gatedSupervisor.
const readyGateEnv = "SKYGO_DURABLE_TEST_READY_GATE"

// gatedSupervisor is RunSupervisor with its readiness held at a gate. It runs
// the real supervise with the inherited log, but gives it a private readiness
// pipe. After supervise reports "ok" (it has claimed the job, started the
// workload and recorded it) the gate creates gate/entered and waits for
// gate/release, or for supervise to return, before forwarding the message to
// the inherited descriptor 4. It records the forward's result in
// gate/forwarded. Only the delay is modeled: the supervisor process, claim,
// workload and its reaping are the original ones.
func gatedSupervisor(args []string, gate string) int {
	if len(args) != 1 {
		return 2
	}
	signal.Ignore(syscall.SIGHUP, syscall.SIGINT)
	log := os.NewFile(3, "log")
	ready := os.NewFile(4, "ready")
	if log == nil || ready == nil {
		return 2
	}
	closeOnExec(log)
	closeOnExec(ready)
	defer ready.Close()
	r, w, err := os.Pipe() // close-on-exec: the workload inherits neither end
	if err != nil {
		fmt.Fprintf(ready, "fail: %v\n", err)
		return 1
	}
	done := make(chan struct{})
	forwarded := make(chan struct{})
	go func() {
		defer close(forwarded)
		line, _ := bufio.NewReader(r).ReadString('\n')
		r.Close()
		if line == "ok\n" {
			os.WriteFile(filepath.Join(gate, "entered"), nil, 0600)
			awaitRelease(gate, done)
		}
		result := "forwarded"
		if _, err := io.WriteString(ready, line); err != nil {
			result = err.Error()
		}
		os.WriteFile(filepath.Join(gate, "forwarded"), []byte(result), 0600)
	}()
	err = supervise(args[0], log, w)
	if err != nil {
		fmt.Fprintf(w, "fail: %v\n", err)
	}
	w.Close()
	close(done)
	<-forwarded
	if err != nil {
		return 1
	}
	return 0
}

// awaitRelease waits for gate/release or done.
func awaitRelease(gate string, done <-chan struct{}) {
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, err := os.Lstat(filepath.Join(gate, "release")); err == nil {
			return
		}
		select {
		case <-done:
			return
		case <-tick.C:
		}
	}
}

// TestDurableStartCanceledAwaitingReadiness cancels Start while it waits for
// the readiness of a supervisor that has claimed the job and started its
// workload. Start must return the cancellation promptly and leave that same
// supervisor and workload running, unfenced and without a cancel request, for
// a later Observe and Cancel.
//
// The readiness delay is modeled by gatedSupervisor. The supervisor process,
// its descriptors, claim and workload, and Start's own readiness pipe and
// waiter, are the original ones.
func TestDurableStartCanceledAwaitingReadiness(t *testing.T) {
	tests := []struct {
		name string
		ctx  func() (context.Context, context.CancelFunc)
	}{
		{"no deadline", func() (context.Context, context.CancelFunc) {
			return context.WithCancel(context.Background())
		}},
		{"far deadline", func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), time.Hour)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newDurableFixture(t, "30")
			gate := t.TempDir()
			t.Setenv(readyGateEnv, gate)
			b := f.backend()
			ctx, cancel := tt.ctx()
			done := make(chan struct{})
			var startErr error
			go func() {
				defer close(done)
				startErr = b.Start(ctx, f.id, f.task)
			}()
			t.Cleanup(func() { f.releaseGated(b, gate, cancel, done) })

			dir := filepath.Join(f.state, f.id)
			waitFor(t, "gated readiness", func() bool {
				_, err := os.Lstat(filepath.Join(gate, "entered"))
				return err == nil
			})
			waitFor(t, "workload output", func() bool {
				data, _ := os.ReadFile(filepath.Join(f.logs, f.id+".log"))
				return strings.HasPrefix(string(data), "begin\n")
			})
			select {
			case <-done:
				t.Fatalf("Start returned before readiness: %v", startErr)
			default:
			}
			intent, err := os.ReadFile(filepath.Join(dir, durableIntentFile))
			if err != nil {
				t.Fatal(err)
			}

			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("Start did not return after cancellation")
			}
			if !errors.Is(startErr, context.Canceled) {
				t.Fatalf("Start = %v, want %v", startErr, context.Canceled)
			}

			b.mu.Lock()
			j := b.durable.jobs[f.id]
			exited, prelaunch, starting := j.exited, j.prelaunch, j.starting
			b.mu.Unlock()
			if exited == nil || prelaunch || starting {
				t.Fatalf("job exited=%v prelaunch=%v starting=%v", exited, prelaunch, starting)
			}
			select {
			case <-exited:
				t.Fatal("original supervisor exited after Start was canceled")
			default:
			}
			if _, err := os.Lstat(filepath.Join(dir, durableCancelFile)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("cancel request after Start cancellation: %v", err)
			}
			if after, err := os.ReadFile(filepath.Join(dir, durableIntentFile)); err != nil || !bytes.Equal(after, intent) {
				t.Fatalf("intent changed: %v", err)
			}
			var in durableIntent
			if err := readDurable(dir, durableIntentFile, maxDurableIntent, &in); err != nil {
				t.Fatal(err)
			}
			var c durableClaim
			if err := readDurable(dir, durableClaimFile, maxDurableRecord, &c); err != nil {
				t.Fatal(err)
			}
			if c.Fenced || c.Supervisor == nil || c.Instance != in.Instance {
				t.Fatalf("claim %+v for instance %s", c, in.Instance)
			}
			r := f.running()
			if r.Instance != in.Instance || r.Supervisor != *c.Supervisor {
				t.Fatalf("running %+v, claim %+v", r, c)
			}
			if alive, err := r.Child.alive(); err != nil || !alive {
				t.Fatalf("workload alive = %v, %v", alive, err)
			}
			if err := b.Start(context.Background(), f.id, f.task); err == nil {
				t.Fatal("canceled Start's job started again")
			}
			f.expectRunning(b, in.Instance)

			// The late readiness reaches Start's closed pipe and must not
			// disturb the job.
			if err := os.WriteFile(filepath.Join(gate, "release"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			waitFor(t, "readiness forward", func() bool {
				_, err := os.Lstat(filepath.Join(gate, "forwarded"))
				return err == nil
			})
			if msg, _ := os.ReadFile(filepath.Join(gate, "forwarded")); !strings.Contains(string(msg), "broken pipe") {
				t.Fatalf("late readiness forward: %q, want broken pipe", msg)
			}
			f.expectRunning(b, in.Instance)
			if alive, err := r.Child.alive(); err != nil || !alive {
				t.Fatalf("workload alive = %v, %v after late readiness", alive, err)
			}

			cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer ccancel()
			if err := b.Cancel(cctx, f.ref(in.Instance)); err != nil {
				t.Fatal(err)
			}
			s, err := b.Observe(cctx, f.ref(in.Instance))
			if err != nil || s.State != backend.Canceled || s.Instance != in.Instance {
				t.Fatalf("after cancel: %+v, %v", s, err)
			}
			select {
			case <-exited:
			case <-time.After(10 * time.Second):
				t.Fatal("original supervisor was not reaped")
			}
			if alive, err := r.Child.alive(); err != nil || alive {
				t.Fatalf("workload alive = %v, %v after cancel", alive, err)
			}
			if n := f.launches(); n != 1 {
				t.Fatalf("workload launched %d times", n)
			}
		})
	}
}

func (f *durableFixture) expectRunning(b *Backend, instance string) {
	f.t.Helper()
	s, err := b.Observe(context.Background(), f.ref(""))
	if err != nil || s.State != backend.Running || s.Instance != instance {
		f.t.Fatalf("observe: %+v, %v; want running %s", s, err, instance)
	}
}

// releaseGated is the bounded cleanup for a gated Start. It releases the gate,
// cancels and joins Start, cancels the job through its own supervisor and
// joins that supervisor's original waiter. It never signals a process by PID.
func (f *durableFixture) releaseGated(b *Backend, gate string, cancel context.CancelFunc, done <-chan struct{}) {
	defer b.Close()
	os.WriteFile(filepath.Join(gate, "release"), nil, 0600)
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		f.t.Error("cleanup: Start did not return")
		return
	}
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	if err := b.Cancel(ctx, f.ref("")); err != nil {
		f.t.Errorf("cleanup cancel: %v", err)
	}
	var exited chan struct{}
	b.mu.Lock()
	if j := b.durable.jobs[f.id]; j != nil {
		exited = j.exited
	}
	b.mu.Unlock()
	if exited != nil {
		select {
		case <-exited:
		case <-time.After(10 * time.Second):
			f.t.Error("cleanup: supervisor was not reaped")
		}
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
