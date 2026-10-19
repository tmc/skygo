package local

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

func TestDefaultProgram(t *testing.T) {
	mode := os.Getenv("SKYGO_DEFAULT_PROGRAM")
	if mode == "" {
		return
	}
	switch mode {
	case "exit":
		fmt.Fprint(os.Stdout, "leader output\n")
		code, _ := strconv.Atoi(os.Getenv("SKYGO_DEFAULT_EXIT"))
		os.Exit(code)
	case "pipe-holder":
		deadline := time.Now().Add(8 * time.Second)
		for {
			if _, err := os.Stat(os.Getenv("SKYGO_DEFAULT_RELEASE")); err == nil {
				break
			}
			if time.Now().After(deadline) {
				os.Exit(8)
			}
			time.Sleep(time.Millisecond)
		}
		os.Stdout.Close()
		os.Stderr.Close()
	case "control":
		deadline := time.Now().Add(8 * time.Second)
		for {
			if err := os.WriteFile(os.Getenv("SKYGO_DEFAULT_TICK"), []byte(time.Now().String()), 0600); err != nil {
				os.Exit(8)
			}
			if _, err := os.Stat(os.Getenv("SKYGO_DEFAULT_RELEASE")); err == nil {
				break
			}
			if time.Now().After(deadline) {
				os.Exit(8)
			}
			time.Sleep(10 * time.Millisecond)
		}
		fmt.Fprint(os.Stdout, "control done\n")
	case "bounded":
		time.Sleep(time.Second)
	}
	os.Exit(0)
}

func defaultTask(code int) backend.Task {
	return backend.Task{Argv: []string{os.Args[0], "-test.run=^TestDefaultProgram$"}, Env: map[string]string{"SKYGO_DEFAULT_PROGRAM": "exit", "SKYGO_DEFAULT_EXIT": strconv.Itoa(code)}}
}

func awaitDefault(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for " + what)
	}
}
func ownedExecution(t *testing.T, b *Backend, id string) *execution {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.jobs[id]
	if e == nil || e.owned == nil {
		t.Fatal("default execution missing")
	}
	return e
}

func TestDefaultGraceAndHandoff(t *testing.T) {
	for _, code := range []int{0, 7} {
		for _, late := range []bool{false, true} {
			t.Run(fmt.Sprintf("exit%d/late%v", code, late), func(t *testing.T) {
				b := newBackend(t)
				ops := b.defaultOps
				id := strings.Repeat("3", 32)
				release := filepath.Join(t.TempDir(), "holder-release")
				var holder *exec.Cmd
				joined := false
				t.Cleanup(func() {
					if holder != nil && holder.Process != nil && !joined {
						holder.Process.Kill()
						holder.Wait()
					}
				})
				pin := make(chan struct{})
				fence := make(chan struct{})
				resume := make(chan struct{})
				var resumeOnce sync.Once
				releaseReaper := func() { resumeOnce.Do(func() { close(resume) }) }
				t.Cleanup(releaseReaper)
				var traceMu sync.Mutex
				var trace []string
				add := func(s string) { traceMu.Lock(); trace = append(trace, s); traceMu.Unlock() }
				b.defaultOps.start = func(c *exec.Cmd) error {
					holder = exec.Command(os.Args[0], "-test.run=^TestDefaultProgram$")
					holder.Env = append(os.Environ(), "SKYGO_DEFAULT_PROGRAM=pipe-holder", "SKYGO_DEFAULT_RELEASE="+release)
					holder.Stdout, holder.Stderr = c.Stdout, c.Stderr
					configure(holder)
					if err := holder.Start(); err != nil {
						return err
					}
					return ops.start(c)
				}
				b.defaultOps.wait = func(c *exec.Cmd) error { err := ops.wait(c); add("terminal-record"); close(pin); return err }
				b.defaultOps.kill = func(c *exec.Cmd) error { add("group-signal"); return ops.kill(c) }
				b.defaultOps.reap = func(c *exec.Cmd) error { add("handoff"); close(fence); <-resume; add("sole-wait"); return ops.reap(c) }
				if err := b.Start(context.Background(), id, defaultTask(code)); err != nil {
					t.Fatal(err)
				}
				awaitDefault(t, pin, "terminal leader")
				if !late {
					if err := os.WriteFile(release, nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
				awaitDefault(t, fence, "signal fence")
				e := ownedExecution(t, b, id)
				// Real terminal record and permanent fence already exist; no caller may
				// relabel that outcome or send another group signal before/after Wait.
				if err := b.Cancel(context.Background(), backend.Reference{ID: id}); err != nil {
					t.Fatal(err)
				}
				if err := b.stop(e, backend.Failed, "execution timed out"); err != nil {
					t.Fatal(err)
				}
				if late {
					if err := os.WriteFile(release, nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
				releaseReaper()
				s := completed(t, b, id)
				want := backend.Succeeded
				if code != 0 {
					want = backend.Failed
				}
				if s.State != want || s.ExitCode == nil || *s.ExitCode != int32(code) || s.Instance != "" || s.Termination != backend.UnknownTermination {
					t.Fatalf("status=%+v", s)
				}
				reason := ""
				if code != 0 {
					reason = "exit status 7"
				} else if late {
					reason = "output pipes remained open after exit; later output discarded"
				}
				if s.Reason != reason {
					t.Fatalf("reason=%q want=%q", s.Reason, reason)
				}
				if err := b.Cancel(context.Background(), backend.Reference{ID: id}); err != nil {
					t.Fatal(err)
				}
				chunk, err := b.Logs(context.Background(), backend.Reference{ID: id}, 0, backend.MaxChunkBytes)
				if err != nil || string(chunk.Data) != "leader output\n" || !chunk.Complete {
					t.Fatalf("logs=%+v err=%v", chunk, err)
				}
				done := make(chan error, 1)
				go func() { done <- holder.Wait() }()
				select {
				case err := <-done:
					joined = true
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(3 * time.Second):
					holder.Process.Kill()
					<-done
					joined = true
					t.Fatal("original holder did not exit")
				}
				traceMu.Lock()
				got := strings.Join(trace, ",")
				traceMu.Unlock()
				if got != "terminal-record,group-signal,handoff,sole-wait" {
					t.Fatalf("trace=%s", got)
				}
			})
		}
	}
}

func TestDefaultBlockedWriterClose(t *testing.T) {
	b := newBackend(t)
	ops := b.defaultOps
	id := strings.Repeat("4", 32)
	blocked := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var active atomic.Bool
	var closes atomic.Int32
	b.defaultOps.write = func(o *output, p []byte) (int, error) {
		if strings.HasSuffix(o.marker, id+".truncated") {
			o.mu.Lock()
			active.Store(true)
			close(blocked)
			<-release
			active.Store(false)
			o.mu.Unlock()
		}
		return ops.write(o, p)
	}
	b.defaultOps.closeOutput = func(o *output) error {
		if strings.HasSuffix(o.marker, id+".truncated") {
			if active.Load() {
				t.Error("closed output under active writer")
			}
			closes.Add(1)
		}
		return ops.closeOutput(o)
	}
	if err := b.Start(context.Background(), id, defaultTask(0)); err != nil {
		t.Fatal(err)
	}
	awaitDefault(t, blocked, "blocked writer")
	e := ownedExecution(t, b, id)
	awaitDefault(t, e.owned.reaped, "leader reap independent from writer")
	select {
	case <-e.done:
		t.Fatal("published clean completion with blocked writer")
	default:
	}
	other := strings.Repeat("5", 32)
	if err := b.Start(context.Background(), other, task("fail")); err != nil {
		t.Fatal(err)
	}
	if s := completed(t, b, other); s.State != backend.Failed {
		t.Fatalf("other=%+v", s)
	}
	if err := b.Cancel(context.Background(), backend.Reference{ID: id}); err != nil {
		t.Fatal(err)
	}
	s, err := b.Observe(context.Background(), backend.Reference{ID: id})
	if err != nil || s.State != backend.Running {
		t.Fatalf("pending=%+v %v", s, err)
	}
	started := time.Now()
	err = b.Close()
	if err == nil || !strings.Contains(err.Error(), "output cleanup") {
		t.Fatalf("Close=%v", err)
	}
	if elapsed := time.Since(started); elapsed > 4*time.Second {
		t.Fatalf("Close took %v", elapsed)
	}
	if closes.Load() != 0 {
		t.Fatal("finalized blocked output")
	}
	unblock()
	if s := completed(t, b, id); s.State != backend.Succeeded || s.Reason != "output pipes remained open after exit; later output discarded" {
		t.Fatalf("final=%+v", s)
	}
	if closes.Load() != 1 {
		t.Fatalf("output close calls=%d", closes.Load())
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultBlockedFinalizer(t *testing.T) {
	b := newBackend(t)
	ops := b.defaultOps
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	b.defaultOps.closeOutput = func(o *output) error { close(entered); <-release; return ops.closeOutput(o) }
	id := strings.Repeat("6", 32)
	if err := b.Start(context.Background(), id, defaultTask(0)); err != nil {
		t.Fatal(err)
	}
	awaitDefault(t, entered, "blocked file finalizer")
	e := ownedExecution(t, b, id)
	awaitDefault(t, e.owned.reaped, "reap while finalizer blocked")
	select {
	case <-e.done:
		t.Fatal("published completion before file finalization")
	default:
	}
	unblock()
	if s := completed(t, b, id); s.State != backend.Succeeded || s.Reason != "" {
		t.Fatalf("EOF was incorrectly reported discarded: %+v", s)
	}
}

func TestDefaultOptionalHandleFailure(t *testing.T) {
	for _, name := range []string{"unavailable", "permission"} {
		t.Run(name, func(t *testing.T) {
			b := newBackend(t)
			ops := b.defaultOps
			var starts, opens int
			b.defaultOps.start = func(c *exec.Cmd) error { starts++; return ops.start(c) }
			b.defaultOps.openHandle = func(*exec.Cmd) (defaultHandle, error) { opens++; return defaultHandle{fd: -1}, errors.New(name) }
			id := strings.Repeat("7", 32)
			if err := b.Start(context.Background(), id, defaultTask(0)); err != nil {
				t.Fatal(err)
			}
			s := completed(t, b, id)
			if s.State != backend.Succeeded || s.Reason != "" || s.Instance != "" || s.Termination != backend.UnknownTermination || starts != 1 || opens != 1 {
				t.Fatalf("status=%+v starts=%d opens=%d", s, starts, opens)
			}
		})
	}
}

func TestDefaultStartFailureNoReplay(t *testing.T) {
	b := newBackend(t)
	var starts, opens int
	failure := errors.New("clone was denied before launch")
	b.defaultOps.start = func(*exec.Cmd) error { starts++; return failure }
	b.defaultOps.openHandle = func(*exec.Cmd) (defaultHandle, error) { opens++; return defaultHandle{fd: -1}, nil }
	id := strings.Repeat("8", 32)
	if err := b.Start(context.Background(), id, defaultTask(0)); !errors.Is(err, failure) {
		t.Fatalf("Start=%v", err)
	}
	s := completed(t, b, id)
	if s.State != backend.Failed || s.ExitCode != nil || s.Instance != "" || s.Termination != backend.UnknownTermination || starts != 1 || opens != 0 {
		t.Fatalf("status=%+v starts=%d opens=%d", s, starts, opens)
	}
}

func TestDefaultCancellationRedelivery(t *testing.T) {
	for _, mode := range []string{"cancel", "close"} {
		t.Run(mode, func(t *testing.T) {
			b := newBackend(t)
			ops := b.defaultOps
			calls := 0
			failure := errors.New("first delivery failed")
			b.defaultOps.kill = func(c *exec.Cmd) error {
				calls++
				if calls == 1 {
					return failure
				}
				return ops.kill(c)
			}
			id := strings.Repeat("9", 32)
			if err := b.Start(context.Background(), id, task("sleep")); err != nil {
				t.Fatal(err)
			}
			if err := b.Cancel(context.Background(), backend.Reference{ID: id}); !errors.Is(err, failure) {
				t.Fatalf("first Cancel=%v", err)
			}
			e := ownedExecution(t, b, id)
			b.mu.Lock()
			intent, deliveries := e.owned.intent, calls
			b.mu.Unlock()
			if intent || deliveries != 1 {
				t.Fatalf("failed delivery intent=%v calls=%d", intent, deliveries)
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
			reason := "canceled by operator"
			if mode == "close" {
				reason = "server shutting down"
			}
			if s.State != backend.Canceled || s.Reason != reason || s.Instance != "" || s.Termination != backend.UnknownTermination || calls < 2 {
				t.Fatalf("status=%+v calls=%d", s, calls)
			}
		})
	}
}

func TestDefaultUnresolvedWaitNoHandle(t *testing.T) {
	b, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	id := strings.Repeat("a", 32)
	ops := b.defaultOps
	b.defaultOps.closeOutput = func(o *output) error {
		b.mu.Lock()
		closed := b.jobs[id].owned.signalClosed
		b.mu.Unlock()
		if !closed {
			t.Error("output finalized before failed-wait signal fence")
		}
		return ops.closeOutput(o)
	}
	failure := errors.New("unrecognized terminal record")
	b.defaultOps.wait = func(*exec.Cmd) error { return failure }
	b.defaultOps.openHandle = func(*exec.Cmd) (defaultHandle, error) { return defaultHandle{fd: -1}, errors.New("handle unavailable") }
	b.defaultOps.kill = func(*exec.Cmd) error { t.Error("group signal after lost terminal record"); return nil }
	// Block the sole reaper under test ownership, independent from any output
	// worker. The actual child is bounded and its exact Wait is still called once.
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var owned *execution
	t.Cleanup(func() {
		unblock()
		if owned != nil {
			select {
			case <-owned.owned.reaped:
			case <-time.After(5 * time.Second):
				t.Error("original reaper did not join during fault cleanup")
			}
		}
		// Cleanup uncertainty is the asserted result of this fault, not ignored
		// production success. The original sole child reaper must still be joined.
		if err := b.Close(); owned != nil && err == nil {
			t.Error("fault cleanup unexpectedly reported success")
		}
	})
	b.defaultOps.reap = func(c *exec.Cmd) error { close(entered); <-release; return ops.reap(c) }
	task := backend.Task{Argv: []string{os.Args[0], "-test.run=^TestDefaultProgram$"}, Env: map[string]string{"SKYGO_DEFAULT_PROGRAM": "bounded"}}
	if err := b.Start(context.Background(), id, task); err != nil {
		t.Fatal(err)
	}
	awaitDefault(t, entered, "sole reaper entry")
	e := ownedExecution(t, b, id)
	owned = e
	awaitDefault(t, e.owned.outputDone, "owned output cleanup independent from reaper")
	if err := b.Cancel(context.Background(), backend.Reference{ID: id}); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	err = b.Close()
	if err == nil || time.Since(started) > 4*time.Second {
		t.Fatalf("Close=%v duration=%v", err, time.Since(started))
	}
	unblock()
	s := completed(t, b, id)
	if s.State != backend.Failed || !strings.Contains(s.Reason, failure.Error()) || s.Termination != backend.UnknownTermination || s.Instance != "" {
		t.Fatalf("status=%+v", s)
	}
	// The original child is now reaped, but group cleanup was never established.
	if err := b.Close(); err == nil {
		t.Fatal("lost group ownership was silently reported clean")
	}

}

func TestDefaultRealStartFailure(t *testing.T) {
	b := newBackend(t)
	id := strings.Repeat("c", 32)
	task := backend.Task{Argv: []string{filepath.Join(t.TempDir(), "missing-program")}}
	if err := b.Start(context.Background(), id, task); err == nil {
		t.Fatal("missing executable started")
	}
	e := ownedExecution(t, b, id)
	b.mu.Lock()
	closed := e.owned.signalClosed
	b.mu.Unlock()
	if !closed {
		t.Fatal("failed startup left signal admission open")
	}
	if err := b.Cancel(context.Background(), backend.Reference{ID: id}); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	s := completed(t, b, id)
	if s.State != backend.Failed || s.ExitCode != nil || s.Instance != "" || s.Termination != backend.UnknownTermination {
		t.Fatalf("status=%+v", s)
	}
}

func TestDefaultFinalSignalError(t *testing.T) {
	for _, mode := range []string{"absent", "live", "probe-error"} {
		t.Run(mode, func(t *testing.T) {
			b, err := New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { b.Close() }) // Each expected cleanup result is asserted below.
			deliveryErr := errors.New("group delivery denied")
			probeErr := errors.New("group probe denied")
			signals, probes := 0, 0
			b.defaultOps.kill = func(*exec.Cmd) error { signals++; return deliveryErr }
			b.defaultOps.absent = func(c *exec.Cmd) error {
				probes++
				if c.ProcessState == nil {
					t.Error("absence probe before sole Wait")
				}
				if mode == "absent" {
					return nil
				}
				if mode == "probe-error" {
					return probeErr
				}
				return errors.New("numeric group exists or was reused")
			}
			id := strings.Repeat("d", 32)
			if err := b.Start(context.Background(), id, defaultTask(0)); err != nil {
				t.Fatal(err)
			}
			s := completed(t, b, id)
			if s.State != backend.Succeeded || s.Reason != "" || s.Instance != "" || s.Termination != backend.UnknownTermination {
				t.Fatalf("status=%+v", s)
			}
			if err := b.Cancel(context.Background(), backend.Reference{ID: id}); err != nil {
				t.Fatal(err)
			}
			err = b.Close()
			if mode == "absent" && err != nil {
				t.Fatalf("absent group: %v", err)
			}
			if mode != "absent" && !errors.Is(err, deliveryErr) {
				t.Fatalf("uncertain group: %v", err)
			}
			if mode == "probe-error" && !errors.Is(err, probeErr) {
				t.Fatalf("lost probe uncertainty: %v", err)
			}
			if signals != 1 || probes == 0 {
				t.Fatalf("signals=%d probes=%d", signals, probes)
			}
		})
	}
}
