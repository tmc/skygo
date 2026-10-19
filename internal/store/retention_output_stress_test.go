//go:build linux

package store_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
	"golang.org/x/sys/unix"
)

// TestOutputPoolHelper is only entered by the owned local adapter child. Exiting
// directly keeps the testing package's PASS line out of the measured log prefix.
func TestOutputPoolHelper(t *testing.T) {
	if os.Getenv("SKYGO_OUTPUT_POOL_HELPER") != "1" {
		return
	}
	time.AfterFunc(45*time.Second, func() { os.Exit(124) })
	chunk := bytes.Repeat([]byte{'x'}, backend.MaxChunkBytes)
	for i := 0; i < backend.MaxLogBytes/backend.MaxChunkBytes+1; i++ {
		if n, err := os.Stdout.Write(chunk); err != nil || n != len(chunk) {
			os.Exit(125)
		}
	}
	os.Exit(0)
}

// TestRetentionOutputPool is deliberately opt-in: its logs occupy about 256 MiB.
// The directory and JSONL evidence survive failure for the external supervisor.
func TestRetentionOutputPool(t *testing.T) {
	if os.Getenv("SKYGO_OUTPUT_POOL_STRESS") != "1" {
		t.Skip("set SKYGO_OUTPUT_POOL_STRESS=1 for the physical output pool gate")
	}
	root := os.Getenv("SKYGO_OUTPUT_POOL_DIR")
	if !filepath.IsAbs(root) {
		t.Fatal("SKYGO_OUTPUT_POOL_DIR must be an absolute, nonexistent private task directory")
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	outputPoolFree(t, root, 8<<30)
	evidence, err := os.OpenFile(filepath.Join(root, "events.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer evidence.Close()
	f := &outputPoolFixture{t: t, root: root, dir: filepath.Join(root, "data"), writer: json.NewEncoder(evidence)}
	ctx, cancel := context.WithTimeout(context.Background(), 28*time.Minute)
	defer cancel()
	f.ctx = ctx
	defer f.close()
	s, err := store.Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := store.EnableRetention(f.dir, store.DefaultRetentionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	f.domain = info.Domain
	f.policy = info.Policy
	f.engine = auth.NewLegacy("")
	f.actor, err = f.engine.Authenticate("")
	if err != nil {
		t.Fatal(err)
	}
	f.open()
	if !f.b.SupportsRetry() {
		t.Fatal("this opt-in Linux gate requires actual pidfd retry support; no qualification by skip")
	}
	reserve := f.b.OutputReservation()
	unit := f.policy.OutputUnit
	var measured unix.Statfs_t
	if err := unix.Statfs(filepath.Join(f.dir, "logs"), &measured); err != nil {
		t.Fatal(err)
	}
	measuredUnit := uint64(4096)
	for measuredUnit < uint64(measured.Bsize) {
		measuredUnit *= 2
	}
	if measured.Bsize <= 0 || measuredUnit != unit || outputPoolStat(t, filepath.Join(f.dir, "logs")).Device != f.policy.Volume {
		t.Fatal("persisted policy differs from actual output filesystem")
	}
	if reserve != uint64(backend.MaxLogBytes)+4*unit {
		t.Fatal("adapter reservation differs from measured policy")
	}
	n := min(f.policy.OutputDomains, f.policy.OutputFiles/2, f.policy.OutputBytes/reserve)
	if n < 204 || n > 252 || (n+1)*reserve <= f.policy.OutputBytes {
		t.Fatalf("unexpected supported geometry A=%d R=%d N=%d", unit, reserve, n)
	}
	f.event("geometry", map[string]any{"allocation_unit": unit, "volume": f.policy.Volume, "reserve": reserve, "domains": n, "files": 2 * n, "reserved_bytes": n * reserve, "slack": f.policy.OutputBytes - n*reserve, "policy": f.policy})
	f.start()
	jobs := make([]backend.Job, 0, n)
	for i := uint64(0); i < n; i++ {
		jobs = append(jobs, f.run(fmt.Sprintf("pool-%03d", i), false))
	}
	f.assertPool(n, reserve)
	if f.b.starts.Load() != n || f.b.binds.Load() != n {
		t.Fatal("saturated population did not bind/start exactly once per job")
	}
	saturationCutoff := jobs[len(jobs)-1].UpdatedAt.Add(time.Nanosecond)
	inventory := f.inventory(jobs)
	f.event("saturation", map[string]any{"inventory": inventory, "starts": f.b.starts.Load(), "bindings": f.b.binds.Load()})
	// The running scheduler reports a hold; submission itself is not the output
	// reservation transaction. This row later proves recovery after real credit.
	queued := f.submit("scheduler-overflow", false)
	beforeStarts, beforeBinds := f.b.starts.Load(), f.b.binds.Load()
	f.until("scheduler capacity hold", func() bool {
		j, e := f.s.Get(queued.ID)
		return e == nil && j.Status.State == backend.Queued && j.Status.Reason == "retention capacity unavailable; execution held"
	})
	if f.b.starts.Load() != beforeStarts || f.b.binds.Load() != beforeBinds {
		t.Fatal("overflow reached binder or Start")
	}
	f.assertNoOutput(queued)
	held, err := f.s.Get(queued.ID)
	if err != nil {
		t.Fatal(err)
	}
	f.event("scheduler_hold", held)
	// Persist only real closure receipts. Default execution proof is still unknown,
	// so Whole must mark zero and must never dispatch native removal.
	marked := f.prune(store.PruneSelection{Before: saturationCutoff, Mode: store.Whole, Limit: 100}, true)
	if marked != 0 {
		t.Fatalf("default Whole marked %d jobs without native proof", marked)
	}
	refs := f.refs()
	if len(refs) != int(n) {
		t.Fatal("missing saturated references")
	}
	for _, r := range refs {
		if r.ClosureID == "" || !r.PayloadReleased {
			t.Fatal("genuine closed/released receipt was not persisted")
		}
	}
	if f.b.native.Load() != 0 || f.b.badNative.Load() != 0 {
		t.Fatal("initial default population invoked native cleanup")
	}
	if !f.stop() {
		t.Fatal("unquiesced original coordinator; phase stopped")
	}
	// No Run races the explicit Store boundary transaction. Its rejected write
	// must roll back both started state and output identity, with typed old usage.
	direct, err := f.s.SubmitRetained(f.actor, f.domain, "direct-overflow", f.task(false), "local", f.binding, backend.LocalOutput, reserve)
	if err != nil {
		t.Fatal(err)
	}
	f.accepted++
	_, started, err := f.s.BeginAttempt(direct.ID, direct.CurrentAttemptID(), reserve, time.Now().UTC())
	var capacity *store.CapacityError
	if started || !errors.As(err, &capacity) || capacity.Domain != store.OutputBytesQuota || capacity.Used != n*reserve || capacity.Limit != f.policy.OutputBytes {
		t.Fatalf("overflow transaction: start=%v capacity=%+v error=%v", started, capacity, err)
	}
	got, err := f.s.Get(direct.ID)
	if err != nil || !reflect.DeepEqual(got, direct) {
		t.Fatalf("overflow mutated job: %+v %v", got, err)
	}
	f.assertNoOutput(direct)
	if _, err = f.c.Cancel(f.actor, direct.ID); err != nil {
		t.Fatal(err)
	}
	if _, started, err = f.s.BeginAttempt(direct.ID, direct.CurrentAttemptID(), reserve, time.Now().UTC()); err != nil || started {
		t.Fatalf("early cancel: %v %v", started, err)
	}
	f.assertPool(n, reserve)
	f.event("typed_refusal", map[string]any{"domain": capacity.Domain, "used": capacity.Used, "limit": capacity.Limit, "job": got, "starts": beforeStarts, "bindings": beforeBinds})
	// A read-only one-page dry run uses a fresh coordinator with no background
	// writer. All N closure receipts were already persisted by the real flow.
	beforeDB := f.databaseDigest()
	beforeInfo := f.info()
	dry, err := control.NewWithLocalOutput(f.s, []backend.Target{f.target()}, f.engine, f.config())
	if err != nil {
		t.Fatal(err)
	}
	page, _, err := dry.Prune(f.ctx, f.actor, store.PruneSelection{Before: saturationCutoff, Mode: store.OutputOnly, Limit: 1}, false, "")
	if err != nil || page.Marked != 0 || page.Selected != 1 {
		t.Fatalf("dry run: %+v %v", page, err)
	}
	f.pages++
	if beforeDB != f.databaseDigest() || !reflect.DeepEqual(beforeInfo, f.info()) {
		t.Fatal("quiescent dry run changed DB bytes or counters")
	}
	f.event("dry_run", map[string]any{"page": page, "database_sha256": fmt.Sprintf("%x", beforeDB)})
	// Close original adapter-owned handles before Store.Close, and reconstruct
	// output authority only from the bounded trusted reservation index.
	if !f.closeBackend() {
		t.Fatal("unquiesced original local backend; phase stopped")
	}
	if err = f.s.Close(); err != nil {
		t.Fatal(err)
	}
	f.s = nil
	f.open()
	if !reflect.DeepEqual(refs, f.refs()) {
		t.Fatal("restart changed trusted reservations")
	}
	restarted := f.inventory(jobs)
	if !reflect.DeepEqual(inventory, restarted) {
		t.Fatal("restart changed actual file identities or allocation")
	}
	for _, j := range jobs {
		status, e := f.b.Backend.Observe(f.ctx, backend.Reference{ID: j.CurrentAttemptID(), Task: j.Task, Binding: f.binding})
		if e != nil || status.Termination != backend.UnknownTermination || status.Instance != "" {
			t.Fatalf("restart adopted native identity: %+v %v", status, e)
		}
	}
	f.event("restart", map[string]any{"trusted_references": f.refs(), "inventory": restarted, "native_identity_adopted": false})
	f.start()
	// Only the first actual job predates this cutoff. MetadataOnly preserves its
	// output reservation and leaves a taskless needs_manual execution obligation.
	selection := store.PruneSelection{Before: jobs[0].UpdatedAt.Add(time.Nanosecond), Mode: store.MetadataOnly, AcknowledgeUnverifiedTerminal: true, Limit: 100}
	if f.prune(selection, true) != 1 {
		t.Fatal("metadata-only did not mark exactly the first default job")
	}
	obligations, err := f.s.ListObligations(f.actor, 0, 0, 100)
	if err != nil || len(obligations.Items) != 1 || !obligations.Items[0].NeedsManual || obligations.Items[0].JobID != jobs[0].ID {
		t.Fatalf("manual obligation: %+v %v", obligations, err)
	}
	manual := obligations.Items[0]
	f.assertPool(n, reserve)
	// OutputOnly on the remaining live metadata must not use native callbacks.
	if f.prune(store.PruneSelection{Before: saturationCutoff, Mode: store.OutputOnly, Limit: 100}, true) != uint32(n-1) {
		t.Fatal("logs-only did not mark all remaining default outputs")
	}
	if f.prune(store.PruneSelection{Mode: store.OutputOnly, ObligationID: manual.ID}, true) != 1 {
		t.Fatal("manual output did not mark")
	}
	f.untilFor("real unlink and queued recovery", 12*time.Minute, func() bool {
		j, e := f.s.Get(queued.ID)
		if e != nil || j.Status.State != backend.Succeeded {
			return false
		}
		b, files, e := f.s.OutputReservations()
		return e == nil && b == reserve && files == 2
	})
	f.closed(queued)
	if f.b.native.Load() != 0 || f.b.badNative.Load() != 0 {
		t.Fatal("default/output-only invoked native cleanup")
	}
	for _, j := range jobs {
		for _, suffix := range []string{".log", ".truncated"} {
			if _, e := os.Lstat(filepath.Join(f.dir, "logs", j.CurrentAttemptID()+suffix)); !errors.Is(e, os.ErrNotExist) {
				t.Fatalf("output not unlinked: %s %v", j.ID, e)
			}
		}
	}
	obligations, err = f.s.ListObligations(f.actor, 0, 0, 100)
	if err != nil || len(obligations.Items) != 1 || !obligations.Items[0].NeedsManual || obligations.Items[0].OutputBytes != 0 || !obligations.Items[0].ExecutionPending {
		t.Fatalf("manual native obligation lost: %+v %v", obligations, err)
	}
	f.event("default_gc_and_recovery", map[string]any{"manual": obligations.Items[0], "recovered": queued.ID, "native_callbacks": f.b.native.Load()})
	// A bounded partial refill proves post-credit admission without claiming a
	// second complete population. Two additional retry jobs use real Linux proof.
	refill := f.run("refill", false)
	f.closed(refill)
	nativeJobs := []backend.Job{f.run("native-0", true), f.run("native-1", true)}
	for _, j := range nativeJobs {
		if j.Status.Termination != backend.StoppedWithExitCode || j.Status.ExitCode == nil || *j.Status.ExitCode != 0 || j.Status.Instance == "" {
			t.Fatalf("missing real retry-native proof: %+v", j)
		}
	}
	f.assertPool(4, reserve)
	if f.prune(store.PruneSelection{Before: time.Now().Add(time.Hour), Mode: store.Whole, Limit: 100}, true) != 3 {
		t.Fatal("Whole must select two proved retry jobs and the never-started canceled overflow row")
	}
	f.until("native cleanup", func() bool { return f.b.native.Load() == 2 && f.info().PendingCleanupRecords == 0 })
	if f.b.starts.Load() != 4 || f.b.binds.Load() != 4 {
		t.Fatal("restarted adapter did not bind/start exactly once for each of four new jobs")
	}
	if f.b.badNative.Load() != 0 || f.b.nativeCalls.Load() != 2 {
		t.Fatal("native callback received default/unproved execution or repeated dispatch")
	}
	if f.prune(store.PruneSelection{Before: time.Now().Add(time.Hour), Mode: store.OutputOnly, Limit: 100}, true) != 2 {
		t.Fatal("partial refill output cleanup mismatch")
	}
	f.until("final output credit", func() bool {
		b, files, e := f.s.OutputReservations()
		return e == nil && b == 0 && files == 0 && f.info().PendingCleanupRecords == 0
	})
	f.assertPool(0, reserve)
	if f.b.badNative.Load() != 0 || f.b.nativeCalls.Load() != 2 {
		t.Fatal("logs-only cleanup crossed the native proof boundary")
	}
	if f.b.outputCalls.Load() != n+4 {
		t.Fatalf("output callbacks=%d want=%d", f.b.outputCalls.Load(), n+4)
	}
	if f.pages > 20 {
		t.Fatalf("prune page budget exceeded: %d", f.pages)
	}
	if f.accepted > 2*(n+1)+2 {
		t.Fatal("accepted job budget exceeded")
	}
	f.event("complete", map[string]any{"accepted": f.accepted, "pages": f.pages, "native_callbacks": f.b.nativeCalls.Load(), "output_callbacks": f.b.outputCalls.Load(), "manual_execution_obligation_preserved": true, "second_complete_population": false})
	if !f.stop() {
		t.Fatal("unquiesced original coordinator; phase stopped")
	}
	if !f.closeBackend() {
		t.Fatal("unquiesced original local backend; phase stopped")
	}
	if err = f.s.Close(); err != nil {
		t.Fatal(err)
	}
	f.s = nil
}

type outputPoolFixture struct {
	t                         *testing.T
	root, dir, binding        string
	ctx                       context.Context
	s                         *store.Store
	b                         *outputPoolBackend
	c                         *control.Control
	engine                    *auth.Engine
	actor                     auth.Actor
	domain                    store.RequestDomain
	policy                    store.RetentionPolicy
	cancel                    context.CancelFunc
	done                      chan error
	writer                    *json.Encoder
	unquiesced, backendClosed bool
	pages                     int
	accepted                  uint64
}

type outputPoolBackend struct {
	*local.Backend
	gate                             sync.RWMutex
	stopped                          bool
	starts, binds, native, badNative atomic.Uint64
	nativeCalls, outputCalls         atomic.Uint64
}

func (b *outputPoolBackend) enter() (func(), error) {
	b.gate.RLock()
	if b.stopped {
		b.gate.RUnlock()
		return nil, context.Canceled
	}
	return b.gate.RUnlock, nil
}
func (b *outputPoolBackend) Start(ctx context.Context, id string, task backend.Task) error {
	leave, err := b.enter()
	if err != nil {
		return err
	}
	defer leave()
	b.starts.Add(1)
	return b.Backend.Start(ctx, id, task)
}
func (b *outputPoolBackend) Observe(ctx context.Context, r backend.Reference) (backend.Status, error) {
	leave, err := b.enter()
	if err != nil {
		return backend.Status{}, err
	}
	defer leave()
	return b.Backend.Observe(ctx, r)
}
func (b *outputPoolBackend) Cancel(ctx context.Context, r backend.Reference) error {
	leave, err := b.enter()
	if err != nil {
		return err
	}
	defer leave()
	return b.Backend.Cancel(ctx, r)
}
func (b *outputPoolBackend) Logs(ctx context.Context, r backend.Reference, offset int64, limit int) (backend.LogChunk, error) {
	leave, err := b.enter()
	if err != nil {
		return backend.LogChunk{}, err
	}
	defer leave()
	return b.Backend.Logs(ctx, r, offset, limit)
}

func (f *outputPoolFixture) open() {
	f.t.Helper()
	if f.unquiesced {
		f.t.Fatal("cannot reopen unquiesced fixture")
	}
	var err error
	f.s, err = store.Open(f.dir)
	if err != nil {
		f.t.Fatal(err)
	}
	f.binding, err = backend.LocalBinding(f.s.Owner())
	if err != nil {
		f.t.Fatal(err)
	}
	refs := f.refs()
	b, err := local.NewRetained(filepath.Join(f.dir, "logs"), local.RetentionConfig{Owner: f.s.Owner(), OutputUnit: f.policy.OutputUnit, Volume: f.policy.Volume, OutputBytes: f.policy.OutputBytes, OutputDomains: f.policy.OutputDomains, OutputFiles: f.policy.OutputFiles, Outputs: refs})
	if err != nil {
		f.t.Fatal(err)
	}
	f.b = &outputPoolBackend{Backend: b}
	f.backendClosed = false
	f.c, err = control.NewWithLocalOutput(f.s, []backend.Target{f.target()}, f.engine, f.config())
	if err != nil {
		f.t.Fatal(err)
	}
}
func (f *outputPoolFixture) target() backend.Target {
	b := f.b
	r := b.Backend.Retention()
	closed, remove, native := r.CheckOutputClosed, r.RemoveOutput, r.RemoveExecution
	r.CheckOutputClosed = func(ctx context.Context, ref backend.OutputReference) (backend.OutputReceipt, error) {
		leave, e := b.enter()
		if e != nil {
			return backend.OutputReceipt{}, e
		}
		defer leave()
		return closed(ctx, ref)
	}
	r.RemoveOutput = func(ctx context.Context, ref backend.OutputReference) error {
		leave, e := b.enter()
		if e != nil {
			return e
		}
		defer leave()
		b.outputCalls.Add(1)
		return remove(ctx, ref)
	}
	r.RemoveExecution = func(ctx context.Context, ref backend.Reference) error {
		leave, e := b.enter()
		if e != nil {
			return e
		}
		defer leave()
		b.nativeCalls.Add(1)
		if ref.Task.Retry == nil || ref.Task.Retry.MaxAttempts < 2 || ref.Instance == "" {
			b.badNative.Add(1)
			return errors.New("forbidden default or unproved native cleanup")
		}
		e = native(ctx, ref)
		if e == nil {
			b.native.Add(1)
		}
		return e
	}
	return backend.Target{Name: "local", Kind: "local", Backend: b, Binding: f.binding, SupportsRetry: b.SupportsRetry(), OutputStorage: backend.LocalOutput, OutputReservation: b.OutputReservation(), Retention: r}
}
func (f *outputPoolFixture) config() control.LocalOutputConfig {
	b, s := f.b, f.s
	return control.LocalOutputConfig{Target: "local", Binding: f.binding, BindOutput: func(ref backend.OutputReference) error {
		leave, e := b.enter()
		if e != nil {
			return e
		}
		defer leave()
		committed, e := s.OutputReference(ref.JobID, ref.AttemptID)
		if e != nil {
			return e
		}
		if committed != ref {
			return errors.New("binder did not receive committed identity")
		}
		b.binds.Add(1)
		return b.BindOutput(ref)
	}, OutputUsage: func(ctx context.Context) (uint64, uint64, error) {
		leave, e := b.enter()
		if e != nil {
			return 0, 0, e
		}
		defer leave()
		u, e := b.OutputUsage(ctx)
		return u.AllocatedBytes, u.Files, e
	}}
}
func (f *outputPoolFixture) start() {
	f.t.Helper()
	if f.unquiesced {
		f.t.Fatal("cannot start replacement for unquiesced fixture")
	}
	if f.done != nil {
		f.t.Fatal("coordinator already running")
	}
	var ctx context.Context
	ctx, f.cancel = context.WithCancel(f.ctx)
	f.done = make(chan error, 1)
	go func() { f.done <- f.c.Run(ctx) }()
	f.until("actual storage observation", f.c.StorageReady)
}

// hold is permanent for this fixture. The Store and original ownership are
// deliberately retained rather than reused behind an uncertain writer/child.
func (f *outputPoolFixture) hold(reason string) bool {
	f.unquiesced = true
	f.t.Error(reason)
	if f.writer != nil {
		if err := f.writer.Encode(struct {
			Time         time.Time
			Kind, Reason string
		}{time.Now().UTC(), "UNQUIESCED_HOLD", reason}); err != nil {
			f.t.Error(err)
		}
	}
	return false
}

// lockCallbacks bounds acquisition of the original callback gate. It creates
// no replacement worker and never treats timeout as callback completion.
func (f *outputPoolFixture) lockCallbacks() bool {
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		if f.b.gate.TryLock() {
			return true
		}
		select {
		case <-deadline.C:
			return f.hold("original callback owners did not drain; Store retained")
		case <-tick.C:
		}
	}
}
func (f *outputPoolFixture) stop() bool {
	f.t.Helper()
	if f.unquiesced {
		return false
	}
	if f.done == nil {
		return true
	}
	f.cancel()
	select {
	case err := <-f.done:
		f.done = nil
		if err != nil && !errors.Is(err, context.Canceled) {
			return f.hold(fmt.Sprintf("original Run returned error; phase stopped: %v", err))
		}
	case <-time.After(15 * time.Second):
		return f.hold("original Run did not return; Store retained")
	}
	// Future callbacks fail before Store/filesystem I/O; acquisition proves all
	// prior callbacks returned. Failure must not advance any mutation phase.
	if !f.lockCallbacks() {
		return false
	}
	f.b.stopped = true
	f.b.gate.Unlock()
	return true
}
func (f *outputPoolFixture) closeBackend() bool {
	f.t.Helper()
	if f.unquiesced {
		return false
	}
	if f.b == nil || f.backendClosed {
		return true
	}
	if !f.lockCallbacks() {
		return false
	}
	f.b.stopped = true
	err := f.b.Backend.Close()
	f.b.gate.Unlock()
	if err != nil {
		return f.hold(fmt.Sprintf("original local handle join uncertain; Store retained: %v", err))
	}
	f.backendClosed = true
	return true
}
func (f *outputPoolFixture) close() {
	if !f.stop() || !f.closeBackend() {
		return
	}
	if f.s != nil {
		if err := f.s.Close(); err != nil {
			f.t.Error(err)
		}
		f.s = nil
	}
}
func (f *outputPoolFixture) task(retry bool) backend.Task {
	executable, err := os.Executable()
	if err != nil {
		f.t.Fatal(err)
	}
	task := backend.Task{Argv: []string{executable, "-test.run=^TestOutputPoolHelper$"}, Env: map[string]string{"SKYGO_OUTPUT_POOL_HELPER": "1"}, Directory: f.root, TimeoutSeconds: 60}
	if retry {
		task.Retry = &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7}}
	}
	return task
}
func (f *outputPoolFixture) submit(id string, retry bool) backend.Job {
	f.t.Helper()
	if f.unquiesced {
		f.t.Fatal("cannot mutate unquiesced fixture")
	}
	f.until("admission sample", f.c.StorageReady)
	j, err := f.c.SubmitRetained(f.actor, f.domain, id, f.task(retry))
	if err != nil {
		f.t.Fatal(err)
	}
	f.accepted++
	return j
}
func (f *outputPoolFixture) run(id string, retry bool) backend.Job {
	f.t.Helper()
	j := f.submit(id, retry)
	f.until("helper terminal "+j.ID, func() bool { var err error; j, err = f.s.Get(j.ID); return err == nil && j.Status.State.Terminal() })
	if j.Status.State != backend.Succeeded {
		f.t.Fatalf("helper did not succeed: %+v", j)
	}
	if !retry && (j.Task.Retry != nil || j.Status.Termination != backend.UnknownTermination || j.Status.Instance != "") {
		f.t.Fatalf("default gained native evidence: %+v", j)
	}
	f.closed(j)
	return j
}
func (f *outputPoolFixture) closed(j backend.Job) {
	f.t.Helper()
	ref, err := f.s.OutputReference(j.ID, j.CurrentAttemptID())
	if err != nil {
		f.t.Fatal(err)
	}
	f.until("real writer and payload closure", func() bool {
		receipt, err := f.b.Backend.Retention().CheckOutputClosed(f.ctx, ref)
		return err == nil && receipt.ClosureID != "" && receipt.PayloadReleased
	})
}
func (f *outputPoolFixture) until(what string, fn func() bool) {
	f.t.Helper()
	f.untilFor(what, 90*time.Second, fn)
}
func (f *outputPoolFixture) untilFor(what string, timeout time.Duration, fn func() bool) {
	f.t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		outputPoolFree(f.t, f.root, 2<<30)
		if fn() {
			return
		}
		select {
		case <-f.ctx.Done():
			f.t.Fatalf("%s: %v", what, f.ctx.Err())
		case <-deadline.C:
			f.t.Fatalf("timed out waiting for %s", what)
		case <-tick.C:
		}
	}
}
func (f *outputPoolFixture) info() store.RetentionInfo {
	f.t.Helper()
	info, err := f.s.RetentionInfo(f.actor)
	if err != nil {
		f.t.Fatal(err)
	}
	return info
}
func (f *outputPoolFixture) assertPool(n, reserve uint64) {
	f.t.Helper()
	info := f.info()
	want := map[store.QuotaDomain]uint64{store.OutputBytesQuota: n * reserve, store.OutputDomainsQuota: n, store.OutputFilesQuota: 2 * n}
	for _, u := range info.Usage {
		if w, ok := want[u.Domain]; ok && u.Used != w {
			f.t.Fatalf("quota %d used=%d want=%d", u.Domain, u.Used, w)
		}
	}
	u, err := f.b.Backend.OutputUsage(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	if u.Files != 2*n || u.LogicalBytes != n*(backend.MaxLogBytes+uint64(len("truncated\n"))) {
		f.t.Fatalf("actual inventory %+v for %d domains", u, n)
	}
	entries, err := os.ReadDir(filepath.Join(f.dir, "logs"))
	if err != nil {
		f.t.Fatal(err)
	}
	var allocated, logical uint64
	for _, entry := range entries {
		path := filepath.Join(f.dir, "logs", entry.Name())
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() {
			f.t.Fatalf("unexpected output inventory: %s %v", path, err)
		}
		stat := outputPoolStat(f.t, path)
		allocated += stat.Allocated
		logical += uint64(stat.Bytes)
	}
	if uint64(len(entries)) != u.Files || allocated != u.AllocatedBytes || logical != u.LogicalBytes {
		f.t.Fatal("adapter sample differs from independently measured inventory")
	}
	if u.AllocatedBytes > n*reserve {
		f.t.Fatal("actual output exceeds reservation")
	}
	f.event("pool", map[string]any{"usage": info.Usage, "physical": u, "db": outputPoolStat(f.t, filepath.Join(f.dir, "jobs.db")), "root": outputPoolStat(f.t, filepath.Join(f.dir, "logs"))})
}
func (f *outputPoolFixture) refs() []backend.OutputReference {
	f.t.Helper()
	var all []backend.OutputReference
	after := ""
	for pages := 0; pages < 4; pages++ {
		refs, next, err := f.s.ListOutputReferences(after, 100)
		if err != nil {
			f.t.Fatal(err)
		}
		all = append(all, refs...)
		if next == "" {
			return all
		}
		after = next
	}
	f.t.Fatal("output reference page bound exceeded")
	return nil
}
func (f *outputPoolFixture) assertNoOutput(j backend.Job) {
	f.t.Helper()
	if _, err := f.s.OutputReference(j.ID, j.CurrentAttemptID()); !errors.Is(err, store.ErrNotFound) {
		f.t.Fatalf("overflow acquired output reference: %v", err)
	}
	for _, suffix := range []string{".log", ".truncated"} {
		if _, err := os.Lstat(filepath.Join(f.dir, "logs", j.CurrentAttemptID()+suffix)); !errors.Is(err, os.ErrNotExist) {
			f.t.Fatalf("overflow acquired output file: %v", err)
		}
	}
}

type outputPoolFile struct {
	Name                     string
	Bytes                    int64
	Allocated, Device, Inode uint64
}

func outputPoolStat(t *testing.T, path string) outputPoolFile {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Blocks < 0 {
		t.Fatal("stat_blocks unavailable")
	}
	return outputPoolFile{filepath.Base(path), info.Size(), uint64(st.Blocks) * 512, uint64(st.Dev), st.Ino}
}
func (f *outputPoolFixture) inventory(jobs []backend.Job) []outputPoolFile {
	f.t.Helper()
	var files []outputPoolFile
	for _, j := range jobs {
		for _, suffix := range []string{".log", ".truncated"} {
			path := filepath.Join(f.dir, "logs", j.CurrentAttemptID()+suffix)
			file := outputPoolStat(f.t, path)
			info, err := os.Lstat(path)
			if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
				f.t.Fatalf("invalid private output %s: %v", path, err)
			}
			if suffix == ".log" && file.Bytes != backend.MaxLogBytes {
				f.t.Fatalf("log bytes=%d", file.Bytes)
			}
			if suffix == ".truncated" {
				data, err := os.ReadFile(path)
				if err != nil || string(data) != "truncated\n" {
					f.t.Fatal("missing genuine truncation marker", err)
				}
			}
			files = append(files, file)
		}
	}
	return files
}
func (f *outputPoolFixture) databaseDigest() [32]byte {
	f.t.Helper()
	file, err := os.Open(filepath.Join(f.dir, "jobs.db"))
	if err != nil {
		f.t.Fatal(err)
	}
	defer file.Close()
	h := sha256.New()
	if _, err = io.Copy(h, file); err != nil {
		f.t.Fatal(err)
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest
}
func (f *outputPoolFixture) prune(selection store.PruneSelection, execute bool) uint32 {
	f.t.Helper()
	if f.unquiesced {
		f.t.Fatal("cannot prune unquiesced fixture")
	}
	var marked uint32
	token := ""
	for {
		if f.pages >= 20 {
			f.t.Fatal("prune page budget exceeded")
		}
		if execute {
			f.until("fresh maintenance sample", f.c.StorageReady)
		}
		page, next, err := f.c.Prune(f.ctx, f.actor, selection, execute, token)
		f.pages++
		if err != nil {
			f.t.Fatal(err)
		}
		f.event("prune", map[string]any{"selection": selection, "execute": execute, "page": page, "continued": next != ""})
		marked += page.Marked
		if next == "" {
			return marked
		}
		token = next
	}
}
func (f *outputPoolFixture) event(kind string, value any) {
	f.t.Helper()
	workspace := outputPoolWorkspace(f.t, f.root)
	if err := f.writer.Encode(struct {
		Time      time.Time
		Kind      string
		Value     any
		Workspace outputPoolWorkspaceUsage
	}{time.Now().UTC(), kind, value, workspace}); err != nil {
		f.t.Fatal(err)
	}
}
func outputPoolFree(t *testing.T, path string, minimum uint64) {
	t.Helper()
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		t.Fatal(err)
	}
	if stat.Bsize <= 0 || stat.Bavail > ^uint64(0)/uint64(stat.Bsize) {
		t.Fatal("invalid filesystem free sample")
	}
	free := stat.Bavail * uint64(stat.Bsize)
	if free < minimum {
		t.Fatalf("free space %d below required %d", free, minimum)
	}
}

// Filesystem allocation is sampled separately from the logical reserve. This
// bounds the fixture's actual workspace without fillers or quota changes.
type outputPoolWorkspaceUsage struct{ Logical, Allocated, Files, Free uint64 }

func outputPoolWorkspace(t *testing.T, root string) outputPoolWorkspaceUsage {
	t.Helper()
	var usage outputPoolWorkspaceUsage
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return errors.New("unexpected workspace symlink")
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok || st.Blocks < 0 {
			return errors.New("workspace allocation unavailable")
		}
		usage.Allocated += uint64(st.Blocks) * 512
		if info.Mode().IsRegular() {
			usage.Logical += uint64(info.Size())
			usage.Files++
		}
		if usage.Allocated > 1<<30 || usage.Logical > 1<<30 || usage.Files > 1024 {
			return errors.New("physical output workspace budget exceeded")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var stat unix.Statfs_t
	if err := unix.Statfs(root, &stat); err != nil {
		t.Fatal(err)
	}
	if stat.Bsize <= 0 || stat.Bavail > ^uint64(0)/uint64(stat.Bsize) {
		t.Fatal("invalid filesystem free sample")
	}
	usage.Free = stat.Bavail * uint64(stat.Bsize)
	if usage.Free < 2<<30 {
		t.Fatal("free space below 2 GiB during fixture")
	}
	return usage
}
