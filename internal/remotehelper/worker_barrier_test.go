package remotehelper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

const workerTestID = "0123456789abcdef0123456789abcdef"
const workerTestUID = "6f1c2b0e-0000-4000-8000-000000000001"

func workerMember(t *testing.T, mode backend.WorkerMode, rank, count uint8) ([]string, string) {
	t.Helper()
	task := backend.Task{Argv: []string{"train"}, Image: "trainer:v1"}
	raw, err := backend.TaskBytes(task)
	if err != nil {
		t.Fatal(err)
	}
	group := backend.WorkerGroup{Execution: backend.Reference{ID: workerTestID, Task: task, Binding: "kube-binding"}, AttemptID: workerTestID, TaskDigest: sha256.Sum256(raw), Spec: backend.WorkerSpec{Count: count, Mode: mode}}
	ref := backend.WorkerReference{Group: group, Rank: rank, Pod: backend.WorkerResource{Name: "worker", UID: workerTestUID}}
	if mode == backend.WorkerTCP {
		ref.Service = backend.WorkerResource{Name: "master", UID: "service-uid"}
	}
	token, err := backend.WorkerStartToken(ref)
	if err != nil {
		t.Fatal(err)
	}
	env := []string{"PATH=/bin", "SKYGO_JOB_ID=" + group.Execution.ID, "SKYGO_ATTEMPT_ID=" + group.AttemptID, "SKYGO_WORKER_MODE=" + string(mode), "RANK=" + strconv.Itoa(int(rank)), "WORLD_SIZE=" + strconv.Itoa(int(count)), "SKYGO_POD_UID=" + workerTestUID, "SKYGO_TASK_SHA256=" + hex.EncodeToString(group.TaskDigest[:])}
	return env, hex.EncodeToString(token[:])
}

func workerReplace(env []string, key, value string) []string {
	out := slices.Clone(env)
	for i, entry := range out {
		if strings.HasPrefix(entry, key+"=") {
			out[i] = key + "=" + value
		}
	}
	return out
}

// A failed or unjoined original retains its directory, not a t.TempDir cleanup.
// These slots are test custody for modeled first-Close failures, not a registry
// of native executions or evidence that a failed Close joined its descriptor.
var workerTestHeld struct {
	sync.Mutex
	errors   []error
	fixtures []*workerFixture
}

type workerFixture struct {
	dir    string
	paths  workerPaths
	active bool
	held   error
}

func workerFiles(t *testing.T) *workerFixture {
	t.Helper()
	dir, err := os.MkdirTemp("", "skygo-worker-")
	if err != nil {
		t.Fatal(err)
	}
	f := &workerFixture{dir: dir, paths: workerPaths{prepared: filepath.Join(dir, "prepared"), start: filepath.Join(dir, "start"), termination: filepath.Join(dir, "termination")}}
	for _, name := range []string{f.paths.start, f.paths.termination} {
		if err := os.WriteFile(name, nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		if f.active || f.held != nil || t.Failed() {
			workerTestHeld.Lock()
			workerTestHeld.fixtures = append(workerTestHeld.fixtures, f)
			workerTestHeld.Unlock()
			t.Logf("retain worker original fixture %s active=%t held=%v", dir, f.active, f.held)
			return
		}
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	return f
}
func (f *workerFixture) hold(err error) {
	var closed *closeFailure
	if errors.As(err, &closed) {
		f.held = err
		workerTestHeld.Lock()
		workerTestHeld.errors = append(workerTestHeld.errors, err)
		workerTestHeld.Unlock()
	}
}

func TestWorkerTokenMatchesCurrentControl(t *testing.T) {
	for _, mode := range []backend.WorkerMode{backend.WorkerIndependent, backend.WorkerTCP} {
		for _, member := range [][2]uint8{{0, 2}, {1, 2}, {15, 16}} {
			env, want := workerMember(t, mode, member[0], member[1])
			id, err := workerEnv(env)
			if err != nil || id.token != want || id.uid != workerTestUID {
				t.Fatalf("%s %v: token parity failed: %v", mode, member, err)
			}
		}
	}
}

func TestWorkerIdentityAndTokenRefusals(t *testing.T) {
	env, token := workerMember(t, backend.WorkerIndependent, 1, 2)
	for _, key := range []string{"SKYGO_JOB_ID", "SKYGO_ATTEMPT_ID", "SKYGO_WORKER_MODE", "RANK", "WORLD_SIZE", "SKYGO_POD_UID", "SKYGO_TASK_SHA256"} {
		t.Run(key, func(t *testing.T) {
			missing := slices.DeleteFunc(slices.Clone(env), func(s string) bool { return strings.HasPrefix(s, key+"=") })
			if _, err := workerEnv(missing); err == nil {
				t.Fatal("accepted missing identity")
			}
			duplicate := append(slices.Clone(env), key+"=foreign")
			if _, err := workerEnv(duplicate); err == nil {
				t.Fatal("accepted duplicate identity")
			}
		})
	}
	for _, tt := range []struct{ key, value string }{{"RANK", "+1"}, {"RANK", "2"}, {"WORLD_SIZE", "1"}, {"WORLD_SIZE", "17"}, {"SKYGO_JOB_ID", strings.ToUpper(workerTestID)}, {"SKYGO_WORKER_MODE", "gang"}, {"SKYGO_POD_UID", "a b"}, {"SKYGO_TASK_SHA256", "00"}} {
		if _, err := workerEnv(workerReplace(env, tt.key, tt.value)); err == nil {
			t.Fatalf("accepted %s=%s", tt.key, tt.value)
		}
	}
	for _, tt := range []struct{ key, value string }{{"SKYGO_JOB_ID", strings.Repeat("f", 32)}, {"SKYGO_ATTEMPT_ID", strings.Repeat("f", 32)}, {"SKYGO_WORKER_MODE", "tcp"}, {"RANK", "0"}, {"WORLD_SIZE", "3"}, {"SKYGO_POD_UID", "replacement"}, {"SKYGO_TASK_SHA256", strings.Repeat("0", 64)}} {
		id, err := workerEnv(workerReplace(env, tt.key, tt.value))
		if err != nil || id.token == token {
			t.Fatalf("identity %s not bound: %v", tt.key, err)
		}
	}
	id, err := workerEnv(env)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{token + "\n", " " + token, strings.ToUpper(token), token[:63], strings.Repeat("a", 64)} {
		if decideWorker(raw, id) != workerRefuse {
			t.Fatal("accepted foreign/noncanonical token")
		}
	}
	if decideWorker("", id) != workerWait || decideWorker(token, id) != workerRelease {
		t.Fatal("invalid wait/release decision")
	}
}

func TestWorkerBarrierReleaseAndReadiness(t *testing.T) {
	// MODELED run records the exact call. No kernel exec/main-success is claimed.
	f := workerFiles(t)
	env, token := workerMember(t, backend.WorkerTCP, 1, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	ops := systemWorkerOps()
	ops.run = func(dir string, args, gotEnv []string) error {
		if dir != "." || !slices.Equal(args, []string{"train", "--rank"}) || !slices.Equal(append(slices.Clone(env), "SKYPILOT_NODE_RANK=1"), gotEnv) {
			return errors.New("modeled run payload changed")
		}
		calls.Add(1)
		return nil
	}
	done := make(chan error, 1)
	f.active = true
	go func() { done <- workerBarrier(ctx, []string{"train", "--rank"}, env, f.paths, ops) }()
	joined := false
	join := func() error {
		select {
		case err := <-done:
			f.active = false
			joined = true
			f.hold(err)
			return err
		case <-time.After(5 * time.Second):
			t.Fatalf("HOLD: original worker did not return; %s", f.dir)
			return nil
		}
	}
	defer func() {
		cancel()
		if !joined {
			_ = join()
		}
	}()
	until := time.Now().Add(5 * time.Second)
	for workerReady(env, f.paths, systemWorkerOps()) != nil {
		if time.Now().After(until) {
			t.Fatal("prepared marker missing")
		}
		time.Sleep(time.Millisecond)
	}
	if calls.Load() != 0 {
		t.Fatal("readiness ran argv")
	}
	if err := os.WriteFile(f.paths.start, []byte(token), 0600); err != nil {
		t.Fatal(err)
	}
	if err := join(); err != nil || calls.Load() != 1 {
		t.Fatalf("release = %v, calls=%d", err, calls.Load())
	}
	if err := workerReady(workerReplace(env, "SKYGO_POD_UID", "replacement"), f.paths, systemWorkerOps()); err == nil {
		t.Fatal("foreign Pod ready")
	}
}

func TestWorkerBarrierCanceledOrExpired(t *testing.T) {
	for _, expired := range []bool{false, true} {
		f := workerFiles(t)
		env, token := workerMember(t, backend.WorkerIndependent, 0, 2)
		if err := os.WriteFile(f.paths.start, []byte(token), 0600); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		want := context.Canceled
		if expired {
			cancel()
			ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			want = context.DeadlineExceeded
		} else {
			cancel()
		}
		var calls int
		ops := systemWorkerOps()
		ops.run = func(string, []string, []string) error { calls++; return nil }
		err := workerBarrier(ctx, []string{"train"}, env, f.paths, ops)
		cancel()
		if !errors.Is(err, want) || calls != 0 {
			t.Fatalf("pre-stopped barrier=%v calls=%d", err, calls)
		}
		if _, err := os.Stat(f.paths.prepared); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("published readiness after stop")
		}
	}
}

// workerFaultFile performs the original I/O, then MODELS the selected error.
// Close calls the actual original once before returning a MODELED uncertainty;
// this is not a kernel Close-failure or physical-live-descriptor qualification.
type workerFaultFile struct {
	*os.File
	readErr, writeErr, syncErr, closeErr error
	short                                bool
	afterRead                            func()
	closes                               int
}

func (f *workerFaultFile) Read(p []byte) (int, error) {
	n, err := f.File.Read(p)
	if f.afterRead != nil {
		f.afterRead()
	}
	if f.readErr != nil {
		return n, errors.Join(err, f.readErr)
	}
	return n, err
}
func (f *workerFaultFile) Write(p []byte) (int, error) {
	if f.short {
		return 0, nil
	}
	n, err := f.File.Write(p)
	return n, errors.Join(err, f.writeErr)
}
func (f *workerFaultFile) WriteString(s string) (int, error) { return f.Write([]byte(s)) }
func (f *workerFaultFile) Sync() error                       { return errors.Join(f.File.Sync(), f.syncErr) }
func (f *workerFaultFile) Close() error                      { f.closes++; return errors.Join(f.File.Close(), f.closeErr) }

func TestWorkerBarrierOriginalFileFaults(t *testing.T) {
	for _, stage := range []string{"read", "read-close", "write", "short-write", "sync", "write-close", "sync-close", "close"} {
		t.Run(stage, func(t *testing.T) {
			f := workerFiles(t)
			env, token := workerMember(t, backend.WorkerIndependent, 1, 2)
			if err := os.WriteFile(f.paths.start, []byte(token), 0600); err != nil {
				t.Fatal(err)
			}
			primary, closing := errors.New("MODELED primary "+stage), errors.New("MODELED first Close "+stage)
			ops := systemWorkerOps()
			open := ops.open
			var original *workerFaultFile
			renames, runs := 0, 0
			ops.open = func(name string, flags int, mode os.FileMode) (workerFile, error) {
				file, err := open(name, flags, mode)
				if err != nil {
					return nil, err
				}
				selected := strings.HasPrefix(stage, "read") && name == f.paths.start || !strings.HasPrefix(stage, "read") && strings.HasSuffix(name, ".tmp")
				if !selected {
					return file, nil
				}
				actual := file.(*os.File)
				original = &workerFaultFile{File: actual}
				switch stage {
				case "read", "read-close":
					original.readErr = primary
				case "write", "write-close":
					original.writeErr = primary
				case "short-write":
					original.short = true
				case "sync", "sync-close":
					original.syncErr = primary
				}
				if strings.Contains(stage, "close") {
					original.closeErr = closing
				}
				return original, nil
			}
			ops.rename = func(a, b string) error { renames++; return os.Rename(a, b) }
			ops.run = func(string, []string, []string) error { runs++; return nil }
			err := workerBarrier(context.Background(), []string{"train"}, env, f.paths, ops)
			f.hold(err)
			if err == nil || original == nil || original.closes != 1 || runs != 0 {
				t.Fatalf("fault lost: %v closes=%v runs=%d", err, original, runs)
			}
			if !strings.HasPrefix(stage, "read") && renames != 0 {
				t.Fatal("published marker after failed file operation")
			}
			if stage == "short-write" {
				if !errors.Is(err, io.ErrShortWrite) {
					t.Fatal("short write lost")
				}
			} else if stage != "close" && !errors.Is(err, primary) {
				t.Fatal("primary lost")
			}
			if strings.Contains(stage, "close") {
				var held *closeFailure
				if !errors.Is(err, closing) || !errors.As(err, &held) || held.owner != original {
					t.Fatal("first Close owner/cause lost")
				}
			}
		})
	}
}

func TestWorkerCancellationDuringRead(t *testing.T) {
	f := workerFiles(t)
	env, token := workerMember(t, backend.WorkerIndependent, 1, 2)
	if err := os.WriteFile(f.paths.start, []byte(token), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ops := systemWorkerOps()
	open := ops.open
	var runs int
	ops.open = func(name string, flag int, mode os.FileMode) (workerFile, error) {
		file, err := open(name, flag, mode)
		if err != nil {
			return nil, err
		}
		if name == f.paths.start {
			return &workerFaultFile{File: file.(*os.File), afterRead: cancel}, nil
		}
		return file, nil
	}
	ops.run = func(string, []string, []string) error { runs++; return nil }
	if err := workerBarrier(ctx, []string{"train"}, env, f.paths, ops); !errors.Is(err, context.Canceled) || runs != 0 {
		t.Fatalf("read cancellation=%v runs=%d", err, runs)
	}
}

func TestWorkerProjectedTokenAndLateCancel(t *testing.T) {
	f := workerFiles(t)
	env, token := workerMember(t, backend.WorkerIndependent, 1, 2)
	target := filepath.Join(f.dir, "projected-token")
	if err := os.WriteFile(target, []byte(token), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(f.paths.start); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, f.paths.start); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ops := systemWorkerOps()
	ops.run = func(string, []string, []string) error { cancel(); return nil }
	if err := workerBarrier(ctx, []string{"train"}, env, f.paths, ops); err != nil {
		t.Fatal(err)
	}
	// MODELED run begins after the final check; late cancellation cannot establish no-launch.
	if ctx.Err() != context.Canceled {
		t.Fatal("late run not reached")
	}
}

func TestWorkerReportOriginalFaults(t *testing.T) {
	f := workerFiles(t)
	ops := systemWorkerOps()
	open := ops.open
	writing, closing := errors.New("MODELED write"), errors.New("MODELED close")
	var original *workerFaultFile
	ops.open = func(name string, flag int, mode os.FileMode) (workerFile, error) {
		file, err := open(name, flag, mode)
		if err != nil {
			return nil, err
		}
		original = &workerFaultFile{File: file.(*os.File), writeErr: writing, closeErr: closing}
		return original, nil
	}
	err := workerReport(f.paths, errWorkerRefused, ops)
	f.hold(err)
	var held *closeFailure
	if !errors.Is(err, writing) || !errors.Is(err, closing) || !errors.As(err, &held) || held.owner != original || original.closes != 1 {
		t.Fatalf("report owner/cause lost: %v", err)
	}
	data, err := os.ReadFile(f.paths.termination)
	if err != nil || !bytes.Equal(data, []byte("skygo-worker: start token refused\n")) {
		t.Fatalf("unsafe report: %q %v", data, err)
	}
}

func TestWorkerReadyFileRefusals(t *testing.T) {
	env, _ := workerMember(t, backend.WorkerIndependent, 0, 2)
	for _, kind := range []string{"directory", "oversize", "foreign", "close"} {
		t.Run(kind, func(t *testing.T) {
			f := workerFiles(t)
			ops := systemWorkerOps()
			var original *workerFaultFile
			closing := errors.New("MODELED ready first Close")
			switch kind {
			case "directory":
				if err := os.Mkdir(f.paths.prepared, 0700); err != nil {
					t.Fatal(err)
				}
			case "oversize":
				if err := os.WriteFile(f.paths.prepared, bytes.Repeat([]byte("x"), 129), 0600); err != nil {
					t.Fatal(err)
				}
			case "foreign":
				if err := os.WriteFile(f.paths.prepared, []byte("foreign"), 0600); err != nil {
					t.Fatal(err)
				}
			case "close":
				if err := os.WriteFile(f.paths.prepared, []byte(workerTestUID), 0600); err != nil {
					t.Fatal(err)
				}
				open := ops.open
				ops.open = func(name string, flags int, mode os.FileMode) (workerFile, error) {
					file, err := open(name, flags, mode)
					if err != nil {
						return nil, err
					}
					original = &workerFaultFile{File: file.(*os.File), closeErr: closing}
					return original, nil
				}
			}
			err := workerReady(env, f.paths, ops)
			f.hold(err)
			if err == nil {
				t.Fatal("invalid original certified ready")
			}
			if kind == "close" {
				var held *closeFailure
				if !errors.Is(err, closing) || !errors.As(err, &held) || held.owner != original || original.closes != 1 {
					t.Fatal("ready lost first Close owner")
				}
			}
		})
	}
}

// The actual original Close blocks until released by this test; its eventual
// returned uncertainty is MODELED. The exact goroutine joins before cleanup.
type workerBlockedClose struct {
	*workerFaultFile
	entered, release chan struct{}
}

func (f *workerBlockedClose) Close() error {
	close(f.entered)
	<-f.release
	return f.workerFaultFile.Close()
}
func TestWorkerBlockedFirstClose(t *testing.T) {
	f := workerFiles(t)
	env, token := workerMember(t, backend.WorkerIndependent, 0, 2)
	if err := os.WriteFile(f.paths.start, []byte(token), 0600); err != nil {
		t.Fatal(err)
	}
	ops := systemWorkerOps()
	open := ops.open
	var original *workerBlockedClose
	var runs atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	closing := errors.New("MODELED blocked original first Close")
	ops.open = func(name string, flags int, mode os.FileMode) (workerFile, error) {
		file, err := open(name, flags, mode)
		if err != nil {
			return nil, err
		}
		if name == f.paths.start {
			original = &workerBlockedClose{workerFaultFile: &workerFaultFile{File: file.(*os.File), closeErr: closing}, entered: entered, release: release}
			return original, nil
		}
		return file, nil
	}
	ops.run = func(string, []string, []string) error { runs.Add(1); return nil }
	done := make(chan error, 1)
	f.active = true
	go func() { done <- workerBarrier(context.Background(), []string{"train"}, env, f.paths, ops) }()
	joined := false
	released := false
	defer func() {
		if !released {
			close(release)
		}
		if !joined {
			select {
			case err := <-done:
				f.active = false
				f.hold(err)
			case <-time.After(5 * time.Second):
				t.Error("HOLD: original blocked-Close goroutine remains active")
			}
		}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("original Close not reached")
	}
	if runs.Load() != 0 {
		t.Fatal("ran argv before Close joined")
	}
	select {
	case <-done:
		t.Fatal("barrier returned while original Close blocked")
	default:
	}
	close(release)
	released = true
	select {
	case err := <-done:
		joined = true
		f.active = false
		f.hold(err)
		var held *closeFailure
		if !errors.Is(err, closing) || !errors.As(err, &held) || held.owner != original || original.closes != 1 || runs.Load() != 0 {
			t.Fatalf("blocked original lost: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("HOLD: released original did not join")
	}
}

func TestWorkerRuntimeRank(t *testing.T) {
	for _, rank := range []uint8{0, 1} {
		t.Run(strconv.Itoa(int(rank)), func(t *testing.T) {
			f := workerFiles(t)
			env, token := workerMember(t, backend.WorkerIndependent, rank, 2)
			env = append(env, "SKYPILOT_NODE_RANK=foreign", "SKYPILOT_NODE_RANK=0", "SKYPILOT_NUM_NODES=operator-value")
			before := slices.Clone(env)
			if err := os.WriteFile(f.paths.start, []byte(token), 0600); err != nil {
				t.Fatal(err)
			}
			calls := 0
			ops := systemWorkerOps()
			ops.run = func(_ string, _ []string, got []string) error {
				calls++
				rankCount := 0
				for _, entry := range got {
					if strings.HasPrefix(entry, "SKYPILOT_NODE_RANK=") {
						rankCount++
						if entry != "SKYPILOT_NODE_RANK="+strconv.Itoa(int(rank)) {
							return errors.New("original rank replaced")
						}
					}
				}
				if rankCount != 1 || !slices.Contains(got, "SKYPILOT_NUM_NODES=operator-value") {
					return errors.New("runtime overrides changed")
				}
				return nil
			}
			if err := workerBarrier(context.Background(), []string{"train"}, env, f.paths, ops); err != nil || calls != 1 {
				t.Fatalf("runtime rank: %v calls=%d", err, calls)
			}
			if !slices.Equal(env, before) {
				t.Fatal("mutated identity environment")
			}
		})
	}
}
