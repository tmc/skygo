package local

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

type executionFixture struct {
	backend   *Backend
	directory string
	held      bool
	closeErr  error // first actual nonnil Backend.Close result; never replaced
	retained  bool
}

// Test-only strong custody lasts until test-process exit. Each actual failed
// fixture is appended once; it retains the same Backend/jobs/stdio/error/path.
// This is not a production owner registry or a promise of a live numeric FD.
var executionFixtureHolds struct {
	sync.Mutex
	frames []*executionFixture
}

func (f *executionFixture) close() error {
	executionFixtureHolds.Lock()
	if f.closeErr != nil {
		err := f.closeErr
		executionFixtureHolds.Unlock()
		return err
	}
	executionFixtureHolds.Unlock()
	err := f.backend.Close()
	if err == nil {
		return nil
	}
	executionFixtureHolds.Lock()
	defer executionFixtureHolds.Unlock()
	if f.closeErr == nil {
		f.closeErr = err
	}
	if !f.retained {
		executionFixtureHolds.frames = append(executionFixtureHolds.frames, f)
		f.retained = true
	}
	return f.closeErr
}

func newExecutionFixture(t *testing.T, retained bool) *executionFixture {
	t.Helper()
	if !supported() {
		t.Skip("local execution requires Linux or macOS")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(home, "tmp", "skypilot", "20261019-local-execution-closure", "tests")
	if err := os.MkdirAll(base, 0700); err != nil {
		t.Fatal(err)
	}
	directory, err := os.MkdirTemp(base, "execution-")
	if err != nil {
		t.Fatal(err)
	}
	f := &executionFixture{directory: directory}
	if retained {
		f.backend, err = NewRetained(directory, retentionConfig(t, directory))
	} else {
		f.backend, err = New(directory)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// The actual Backend audit must be positive before its test namespace
		// can disappear. Never retry an uncertain original Close or remove its
		// evidence merely because a diagnostic deadline or a test returned.
		if err := f.close(); err != nil {
			if !f.held {
				t.Errorf("original execution remains held: %v", err)
			}
			t.Logf("preserving original execution frame and namespace: %s", directory)
			return
		}
		if f.held {
			t.Error("expected held original frame lost its uncertainty")
			return
		}
		if err := os.RemoveAll(directory); err != nil {
			t.Error(err)
		}
	})
	return f
}

func TestExecutionPhysicalClosureAndDiagnostics(t *testing.T) {
	for _, retry := range []bool{false, true} {
		for _, code := range []int{0, 7} {
			t.Run(fmt.Sprintf("retry%v/exit%d", retry, code), func(t *testing.T) {
				f := newExecutionFixture(t, true)
				b := f.backend
				if retry {
					requireRetry(t, b)
				}
				program := defaultTask(code)
				if retry {
					program.Retry = &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7}}
				}
				id := strings.Repeat("1", 32)
				startRetained(t, b, id, program)
				b.mu.Lock()
				e := b.jobs[id]
				b.mu.Unlock()
				awaitDefault(t, e.done, "original execution return")
				b.mu.Lock()
				published, closed, diagnostic := e.shutdownPublished, e.shutdownClosed, e.shutdownErr
				shed := e.cmd == nil && e.output.file == nil && e.stdioWriter == nil
				status := e.status
				b.mu.Unlock()
				if !published || !closed || !shed {
					t.Fatalf("published=%v closed=%v shed=%v diagnostic=%v", published, closed, shed, diagnostic)
				}
				if status.ExitCode == nil || *status.ExitCode != int32(code) {
					t.Fatalf("status=%+v", status)
				}
				if code != 0 {
					var exitErr *exec.ExitError
					if !errors.As(diagnostic, &exitErr) {
						t.Fatalf("original ExitError lost after shedding: %v", diagnostic)
					}
				}
				if err := f.close(); err != nil {
					t.Fatalf("historic diagnostic blocked positive closure: %v", err)
				}
			})
		}
	}
}

func TestExecutionReturnedDataErrorStillCloses(t *testing.T) {
	f := newExecutionFixture(t, true)
	b := f.backend
	dataErr := errors.New("returned retention diagnostic")
	b.defaultOps.write = func(*output, []byte) (int, error) { return 0, dataErr }
	id := strings.Repeat("2", 32)
	ref := startRetained(t, b, id, defaultTask(0))
	e := ownedExecution(t, b, id)
	awaitDefault(t, e.done, "original data-error execution")
	b.mu.Lock()
	closed, diagnostic, shed := e.shutdownClosed, e.shutdownErr, e.cmd == nil && e.output.file == nil
	state := e.status.State
	b.mu.Unlock()
	if !closed || !shed || !errors.Is(diagnostic, dataErr) || state != backend.Failed {
		t.Fatalf("closed=%v shed=%v state=%v diagnostic=%v", closed, shed, state, diagnostic)
	}
	ref = closureReference(t, b, ref)
	if !ref.PayloadReleased {
		t.Fatal("returned data error retained positively closed native payload")
	}
	if err := f.close(); err != nil {
		t.Fatal(err)
	}
}

func TestExecutionBlockedOriginalOutputOwner(t *testing.T) {
	f := newExecutionFixture(t, true)
	b := f.backend
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	ops := b.defaultOps
	b.defaultOps.closeOutput = func(out *output) error { close(entered); <-resume; return ops.closeOutput(out) }
	id := strings.Repeat("3", 32)
	startRetained(t, b, id, defaultTask(0))
	e := ownedExecution(t, b, id)
	// This cleanup is later than the fixture's: release and positively join
	// the actual original callback before Backend/resource/namespace cleanup.
	t.Cleanup(func() { unblock(); <-e.done })
	awaitDefault(t, entered, "original output Close entry")
	b.mu.Lock()
	published, shed := e.shutdownPublished, e.cmd == nil
	b.mu.Unlock()
	if published || shed {
		t.Fatal("blocked output owner was published or discarded")
	}
	select {
	case <-e.done:
		t.Fatal("blocked original output owner returned")
	default:
	}
	unblock()
	awaitDefault(t, e.done, "original output callback return")
	b.mu.Lock()
	closed := e.shutdownClosed
	b.mu.Unlock()
	if !closed {
		t.Fatal("positive original output return did not complete physical snapshot")
	}
}

func TestExecutionFailedStartupRetainsOriginalStdio(t *testing.T) {
	f := newExecutionFixture(t, false)
	f.held = true
	b := f.backend
	startErr := errors.New("startup seam refused before native call")
	var originalWriter *os.File
	b.defaultOps.start = func(cmd *exec.Cmd) error {
		originalWriter = cmd.Stdout.(*os.File)
		if err := originalWriter.Close(); err != nil {
			return errors.Join(startErr, err)
		}
		return startErr
	}
	id := strings.Repeat("4", 32)
	err := b.Start(context.Background(), id, defaultTask(0))
	if !errors.Is(err, startErr) || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("actual startup/stdio errors not joined: %v", err)
	}
	e := ownedExecution(t, b, id)
	awaitDefault(t, e.done, "failed original startup")
	b.mu.Lock()
	valid := e.shutdownPublished && !e.shutdownClosed && e.cmd != nil && e.stdioWriter == originalWriter && errors.Is(e.shutdownErr, startErr) && errors.Is(e.shutdownErr, os.ErrClosed)
	b.mu.Unlock()
	if !valid {
		t.Fatal("failed Start lost original stdio/error or gained closure")
	}
	actualCloseErr := f.close()
	if actualCloseErr == nil || f.close() != actualCloseErr {
		t.Fatal("fixture lost its exact first original Close error")
	}
	executionFixtureHolds.Lock()
	matches := 0
	for _, original := range executionFixtureHolds.frames {
		if original == f {
			matches++
		}
	}
	kept := f.closeErr == actualCloseErr && f.backend == b && f.backend.jobs[id] == e && e.stdioWriter == originalWriter
	executionFixtureHolds.Unlock()
	if matches != 1 || !kept {
		t.Fatal("same held fixture/backend/job/stdio/error frame was not strongly retained exactly once")
	}
	// This is an actual Go already-closed policy vector after a positive Close,
	// not an injected kernel Close failure or proof that a native child ran.
}

func TestExecutionRawLogCloseErrorSurvivesNormalization(t *testing.T) {
	f := newExecutionFixture(t, true)
	f.held = true
	b := f.backend
	b.defaultOps.closeOutput = func(out *output) error {
		if err := out.file.Close(); err != nil {
			return err
		}
		return out.file.Close() // real already-closed policy error, not native fault proof
	}
	id := strings.Repeat("5", 32)
	startRetained(t, b, id, defaultTask(0))
	e := ownedExecution(t, b, id)
	awaitDefault(t, e.done, "original log Close policy error")
	b.mu.Lock()
	defer b.mu.Unlock()
	if e.shutdownClosed || e.cmd == nil || e.output.file == nil || !errors.Is(e.shutdownErr, os.ErrClosed) || e.owned.outputResult.closeErr != nil || !errors.Is(e.owned.outputResult.rawCloseErr, os.ErrClosed) {
		t.Fatal("normalized Close discarded original raw error or owner")
	}
}

func TestExecutionReaderCloseIsOnce(t *testing.T) {
	files := newCustodyFiles(t)
	original := files.create(t, "reader")
	e := &execution{owned: &defaultExecution{reader: original.file}}
	original.closeAttempted = true // any actual error must preserve this original owner
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); results <- e.closeReader() }()
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("reader Close was repeated: %v", err)
		}
	}
	original.closeAttempted, original.joined = true, true // the one original Close returned nil
	if err := e.closeReader(); err != nil {
		t.Fatal("cached positive Close changed", err)
	}
}

// TestExecutionClosureProgram is a finite, single-process original Wait fixture.
func TestExecutionClosureProgram(t *testing.T) {
	if os.Getenv("SKYGO_CLOSURE_PROGRAM") == "" {
		return
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(os.Getenv("SKYGO_CLOSURE_RELEASE")); err == nil {
			os.Exit(7)
		}
		if time.Now().After(deadline) {
			os.Exit(9)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestExecutionLateOriginalRetryWait(t *testing.T) {
	f := newExecutionFixture(t, false)
	b := f.backend
	requireRetry(t, b)
	f.held = true
	pinErr := errors.New("test removed original pin before terminal observation")
	b.retryOps.wait = func(cmd *exec.Cmd) error {
		// Existing operation seam deliberately removes the original retry handle
		// via its own single positive Close. This forces Held, not native proof.
		return errors.Join(pinErr, releaseRetry(cmd))
	}
	release := filepath.Join(f.directory, "release")
	program := backend.Task{Argv: []string{os.Args[0], "-test.run=^TestExecutionClosureProgram$"}, Env: map[string]string{"SKYGO_CLOSURE_PROGRAM": "1", "SKYGO_CLOSURE_RELEASE": release}, Retry: &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7}}}
	id := strings.Repeat("6", 32)
	var once sync.Once
	unblock := func() {
		once.Do(func() {
			if err := os.WriteFile(release, nil, 0600); err != nil {
				t.Error(err)
			}
		})
	}
	startErr := b.Start(context.Background(), id, program)
	b.mu.Lock()
	e := b.jobs[id]
	b.mu.Unlock()
	if e != nil {
		t.Cleanup(func() { unblock(); <-e.retry.reaped })
	} // original Wait before fixture cleanup
	if startErr != nil {
		t.Fatal(startErr)
	}
	awaitDefault(t, e.done, "incomplete original retry snapshot")
	b.mu.Lock()
	before := e.shutdownErr
	published, closed := e.shutdownPublished, e.shutdownClosed
	status := e.status
	b.mu.Unlock()
	if !published || closed || !errors.Is(before, pinErr) || !strings.Contains(before.Error(), "reaping remains uncertain") {
		t.Fatalf("incomplete snapshot=%v/%v %v", published, closed, before)
	}
	select {
	case <-e.retry.reaped:
		t.Fatal("unreleased original Wait already returned")
	default:
	}
	unblock()
	awaitDefault(t, e.retry.reaped, "same original late Wait")
	b.mu.Lock()
	var exitErr *exec.ExitError
	actual := e.retry.waitErr
	after := e.shutdownErr
	unchanged := !e.shutdownClosed && e.status.State == status.State && e.status.Reason == status.Reason && e.status.ExitCode == status.ExitCode && e.status.Termination == status.Termination
	b.mu.Unlock()
	if !errors.As(actual, &exitErr) || exitErr.ExitCode() != 7 || !errors.Is(after, actual) || !errors.Is(after, before) || !unchanged {
		t.Fatalf("late original result=%v bundle=%v unchanged=%v", actual, after, unchanged)
	}
}

func TestExecutionCanceledRetryRetainsSignalDiagnostic(t *testing.T) {
	f := newExecutionFixture(t, false)
	b := f.backend
	requireRetry(t, b)
	ops := b.retryOps
	firstErr := errors.New("first returned cancel delivery diagnostic")
	var calls atomic.Int32
	b.retryOps.kill = func(cmd *exec.Cmd) error {
		if calls.Add(1) == 1 {
			return firstErr
		}
		return ops.kill(cmd)
	}
	release := filepath.Join(f.directory, "release")
	program := backend.Task{Argv: []string{os.Args[0], "-test.run=^TestExecutionClosureProgram$"}, Env: map[string]string{"SKYGO_CLOSURE_PROGRAM": "1", "SKYGO_CLOSURE_RELEASE": release}, Retry: &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7}}}
	id := strings.Repeat("7", 32)
	startErr := b.Start(context.Background(), id, program)
	b.mu.Lock()
	e := b.jobs[id]
	b.mu.Unlock()
	if e != nil {
		t.Cleanup(func() {
			if err := os.WriteFile(release, nil, 0600); err != nil {
				t.Error(err)
			}
			<-e.done
		})
	}
	if startErr != nil {
		t.Fatal(startErr)
	}
	if err := b.Cancel(context.Background(), backend.Reference{ID: id}); !errors.Is(err, firstErr) {
		t.Fatalf("Cancel=%v", err)
	}
	if err := os.WriteFile(release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	awaitDefault(t, e.done, "original canceled retry cleanup")
	b.mu.Lock()
	closed, diagnostic, state := e.shutdownClosed, e.shutdownErr, e.status.State
	b.mu.Unlock()
	if !closed || !errors.Is(diagnostic, firstErr) || state != backend.Canceled {
		t.Fatalf("closed=%v state=%v diagnostic=%v", closed, state, diagnostic)
	}
	if err := f.close(); err != nil {
		t.Fatal(err)
	}
}

func TestExecutionCloseResolvesItsDeliveryDiagnostic(t *testing.T) {
	f := newExecutionFixture(t, false)
	b := f.backend
	requireRetry(t, b)
	release := filepath.Join(f.directory, "release")
	ops := b.retryOps
	firstErr := errors.New("Close delivery returned a resolved diagnostic")
	var calls atomic.Int32
	b.retryOps.kill = func(cmd *exec.Cmd) error {
		if calls.Add(1) == 1 {
			return errors.Join(firstErr, os.WriteFile(release, nil, 0600))
		}
		return ops.kill(cmd)
	}
	program := backend.Task{Argv: []string{os.Args[0], "-test.run=^TestExecutionClosureProgram$"}, Env: map[string]string{"SKYGO_CLOSURE_PROGRAM": "1", "SKYGO_CLOSURE_RELEASE": release}, Retry: &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7}}}
	id := strings.Repeat("8", 32)
	startErr := b.Start(context.Background(), id, program)
	b.mu.Lock()
	e := b.jobs[id]
	b.mu.Unlock()
	if e != nil {
		t.Cleanup(func() {
			if err := os.WriteFile(release, nil, 0600); err != nil {
				t.Error(err)
			}
			<-e.done
		})
	}
	if startErr != nil {
		t.Fatal(startErr)
	}
	if err := f.close(); err != nil {
		t.Fatalf("positive later native fence did not resolve Close delivery: %v", err)
	}
	b.mu.Lock()
	closed, diagnostic := e.shutdownClosed, e.shutdownErr
	b.mu.Unlock()
	if !closed || !errors.Is(diagnostic, firstErr) {
		t.Fatalf("closed=%v diagnostic=%v", closed, diagnostic)
	}
}
