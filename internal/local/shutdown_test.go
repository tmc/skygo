//go:build darwin || linux

package local

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
)

// This fixture never removes a namespace after an uncertain original producer
// or Close. Tests release/join their original blocked calls before this cleanup.
type shutdownFixture struct {
	backend   *Backend
	directory string
	files     *artifact.Files
	root      *os.Root
	held      error
	wantHeld  error
}

func newShutdownFixture(t *testing.T) *shutdownFixture {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(home, "tmp", "skypilot", "20261019-local-shutdown-query", "tests")
	if err := os.MkdirAll(base, 0700); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(base)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		t.Fatal("shutdown test parent must be a real private directory")
	}
	dir, err := os.MkdirTemp(base, "local-")
	if err != nil {
		t.Fatal(err)
	}
	f := &shutdownFixture{directory: dir}
	t.Cleanup(func() { f.cleanup(t) })
	f.backend, err = New(filepath.Join(dir, "logs"))
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	return f
}

func (f *shutdownFixture) cleanup(t *testing.T) {
	t.Helper()
	if f.held == nil && f.backend != nil {
		closeErr := f.backend.Close()
		checkErr := f.backend.CheckShutdown()
		f.held = errors.Join(closeErr, checkErr)
	}
	if f.held == nil && f.files != nil {
		f.held = f.files.Close()
	}
	if f.held == nil && f.root != nil {
		f.held = f.root.Close()
	}
	if f.held == nil {
		f.held = os.RemoveAll(f.directory)
	}
	if f.held != nil {
		if f.wantHeld != nil && errors.Is(f.held, f.wantHeld) {
			t.Logf("expected original shutdown owner retained at %s: %v", f.directory, f.held)
		} else {
			t.Errorf("retaining original shutdown fixture at %s: %v", f.directory, f.held)
		}
		lifetime := make(chan struct{})
		go func(original *shutdownFixture) { <-lifetime; runtime.KeepAlive(original) }(f)
	}
}

func shutdownExecutionFor(t *testing.T, b *Backend, id string) *execution {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.jobs[id]
	if e == nil {
		t.Fatal("original execution missing")
	}
	return e
}

func TestCheckShutdownActualDefaultAndRetry(t *testing.T) {
	for _, retry := range []bool{false, true} {
		for _, code := range []int{0, 7} {
			t.Run(fmt.Sprintf("retry=%v/exit=%d", retry, code), func(t *testing.T) {
				f := newShutdownFixture(t)
				b := f.backend
				if retry {
					requireRetry(t, b)
				}
				if err := b.CheckShutdown(); err != nil {
					t.Fatal(err)
				}
				task := defaultTask(code)
				if retry {
					task.Retry = &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7}}
				}
				id := strings.Repeat("a", 32)
				if err := b.Start(context.Background(), id, task); err != nil {
					t.Fatal(err)
				}
				e := shutdownExecutionFor(t, b, id)
				awaitDefault(t, e.done, "original execution publication")
				if err := b.CheckShutdown(); err != nil {
					t.Fatal(err)
				}
				b.mu.Lock()
				published, closed, diagnostic := e.shutdownPublished, e.shutdownClosed, e.shutdownErr
				b.mu.Unlock()
				if !published || !closed {
					t.Fatal("query accepted missing original physical publication")
				}
				if code != 0 && diagnostic == nil {
					t.Fatal("actual nonzero diagnostic was discarded")
				}
				status, err := b.Observe(context.Background(), backend.Reference{ID: id})
				if err != nil || status.ExitCode == nil || *status.ExitCode != int32(code) {
					t.Fatal("query changed execution status", status, err)
				}
			})
		}
	}
}

func TestCheckShutdownBlockedOriginalOutput(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(fmt.Sprint(retry), func(t *testing.T) {
			f := newShutdownFixture(t)
			b := f.backend
			if retry {
				requireRetry(t, b)
			}
			entered, resume := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(resume) }) }
			task := defaultTask(0)
			if retry {
				task.Retry = &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7}}
			}
			ops := b.defaultOps
			// Retry does not use defaultOps.closeOutput: gate its original native
			// wait instead, then let that SAME waiter perform normal output closure.
			if retry {
				realWait := b.retryOps.wait
				b.retryOps.wait = func(cmd *exec.Cmd) error { close(entered); <-resume; return realWait(cmd) }
			} else {
				b.defaultOps.closeOutput = func(out *output) error { close(entered); <-resume; return ops.closeOutput(out) }
			}
			id := strings.Repeat("b", 32)
			if err := b.Start(context.Background(), id, task); err != nil {
				unblock()
				t.Fatal(err)
			}
			e := shutdownExecutionFor(t, b, id)
			t.Cleanup(func() { unblock(); awaitDefault(t, e.done, "original blocked producer return") })
			awaitDefault(t, entered, "original blocked producer")
			if err := b.CheckShutdown(); err == nil {
				t.Fatal("blocked original producer accepted")
			}
			unblock()
			awaitDefault(t, e.done, "original producer completion")
			if err := b.CheckShutdown(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCheckShutdownFailedNativeStartStaysHeld(t *testing.T) {
	f := newShutdownFixture(t)
	b := f.backend
	id := strings.Repeat("c", 32)
	err := b.Start(context.Background(), id, backend.Task{Argv: []string{filepath.Join(f.directory, "does-not-exist")}})
	if err == nil {
		t.Fatal("missing program started")
	}
	var original *os.PathError
	if !errors.As(err, &original) {
		t.Fatal("actual native Start cause missing", err)
	}
	f.wantHeld = original
	e := shutdownExecutionFor(t, b, id)
	awaitDefault(t, e.done, "failed Start returned owners")
	if err := b.CheckShutdown(); !errors.Is(err, original) {
		t.Fatal("failed native Start became positive no-call proof", err)
	}
}

func TestCheckShutdownPositivePhysicalDiagnostic(t *testing.T) {
	f := newShutdownFixture(t)
	b := f.backend
	diagnostic := errors.New("simulated returned data-write diagnostic")
	b.defaultOps.write = func(*output, []byte) (int, error) { return 0, diagnostic }
	id := strings.Repeat("d", 32)
	if err := b.Start(context.Background(), id, defaultTask(0)); err != nil {
		t.Fatal(err)
	}
	e := shutdownExecutionFor(t, b, id)
	awaitDefault(t, e.done, "actual original native and output joins")
	if err := b.CheckShutdown(); err != nil {
		t.Fatal("data diagnostic replaced positive physical receipts", err)
	}
	b.mu.Lock()
	kept := errors.Is(e.shutdownErr, diagnostic)
	b.mu.Unlock()
	if !kept {
		t.Fatal("query discarded diagnostic identity")
	}
}

func TestCheckShutdownRejectsSyntheticStatusAndMissingPublication(t *testing.T) {
	for _, published := range []bool{false, true} {
		done := make(chan struct{})
		close(done)
		cause := errors.New("modeled unresolved original owner")
		e := &execution{done: done, owned: &defaultExecution{}, status: backend.Status{State: backend.Succeeded}, shutdownPublished: published, shutdownErr: cause}
		b := &Backend{jobs: map[string]*execution{"modeled": e}}
		if err := b.CheckShutdown(); !errors.Is(err, cause) {
			t.Fatal("status/done manufactured physical closure", err)
		}
	}
	var absent *Backend
	if absent.CheckShutdown() == nil || new(Backend).CheckShutdown() == nil {
		t.Fatal("unusable backend accepted")
	}
	done := make(chan struct{})
	close(done)
	detached := &Backend{jobs: map[string]*execution{"invalid-detached": {done: done}}}
	if detached.CheckShutdown() == nil {
		t.Fatal("nil workspace manufactured original closure receipt")
	}
}

func TestCheckShutdownStickyEarlyOutput(t *testing.T) {
	f := newShutdownFixture(t)
	b := f.backend
	file, err := os.Create(filepath.Join(f.directory, "early-output"))
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		f.held = err
		t.Fatal(err)
	}
	// Actual already-closed Go File error: custody policy, not a kernel fault.
	out := &output{file: file}
	b.mu.Lock()
	original := b.closeUnstartedOutput(out)
	b.mu.Unlock()
	if !errors.Is(original, os.ErrClosed) {
		f.held = original
		t.Fatal(original)
	}
	f.wantHeld = original
	if err := b.CheckShutdown(); !errors.Is(err, original) {
		t.Fatal("empty jobs concealed pre-execution owner", err)
	}
	if b.unstartedOutput != out || b.unstartedOutputErr != original {
		t.Fatal("query changed original output custody")
	}
}

func ExampleBackend_CheckShutdown() {
	home, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	base := filepath.Join(home, "tmp", "skypilot", "20261019-local-shutdown-query", "example")
	if err := os.MkdirAll(base, 0700); err != nil {
		panic(err)
	}
	dir, err := os.MkdirTemp(base, "idle-")
	if err != nil {
		panic(err)
	}
	b, err := New(dir)
	if err != nil {
		panic(err)
	}
	err = b.CheckShutdown() // no execution or workspace owner was installed
	fmt.Println(err == nil)
	if err != nil {
		panic(err)
	} // preserve the namespace on any uncertainty
	if err := b.Close(); err != nil {
		panic(err)
	}
	if err := os.RemoveAll(dir); err != nil {
		panic(err)
	}
	// Output: true
}
