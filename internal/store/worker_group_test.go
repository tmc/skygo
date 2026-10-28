package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/workeradmission"
	bolt "go.etcd.io/bbolt"
)

// These tests drive real Store transactions with real file-authenticated
// actors and delegations. Worker receipts and observations are MODELED
// backend values; no worker backend, Control route or native resource runs.
type workerFixture struct {
	s         *Store
	dir       string
	actors    map[string]auth.Actor
	authority map[string]auth.Delegation
	domain    RequestDomain
}

func workerPolicy() RetentionPolicy {
	p := DefaultRetentionPolicy()
	p.LocalOutputEnabled = false
	return p
}

func newWorkerFixture(t *testing.T) *workerFixture {
	t.Helper()
	return newWorkerFixturePolicy(t, workerPolicy())
}

func newWorkerFixturePolicy(t *testing.T, p RetentionPolicy) *workerFixture {
	t.Helper()
	sf := newScopeFixture(t)
	f := &workerFixture{dir: filepath.Dir(sf.s.db.Path()), actors: sf.actors, authority: map[string]auth.Delegation{}}
	for _, id := range []string{"alice", "bob"} {
		d, err := sf.engine.Delegate(sf.actors[id])
		if err != nil {
			t.Fatal(err)
		}
		f.authority[id] = d
	}
	if err := sf.s.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := EnableRetention(f.dir, p)
	if err != nil {
		t.Fatal(err)
	}
	f.domain = info.Domain
	f.open(t)
	return f
}

func (f *workerFixture) open(t *testing.T) {
	t.Helper()
	s, err := Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	f.s = s
}

func (f *workerFixture) reopen(t *testing.T) {
	t.Helper()
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	f.open(t)
}

// restored round-trips the stopped store through Backup and Restore and
// returns group id from the opened restored copy. The original is reopened.
func (f *workerFixture) restored(t *testing.T, id string) WorkerGroup {
	t.Helper()
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(t.TempDir(), "snapshot.db")
	if err := Backup(f.dir, snapshot); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "restored")
	if err := Restore(snapshot, dir); err != nil {
		t.Fatal(err)
	}
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	g, err := s.WorkerGroup(id)
	if cerr := s.Close(); cerr != nil {
		t.Fatal(cerr)
	}
	if err != nil {
		t.Fatal(err)
	}
	f.open(t)
	return g
}

// durable checks that group id is unchanged by Close/Open and by
// Backup/Restore, and returns it.
func (f *workerFixture) durable(t *testing.T, id string) WorkerGroup {
	t.Helper()
	before := f.group(t, id)
	f.reopen(t)
	after := f.group(t, id)
	restored := f.restored(t, id)
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(before, restored) {
		t.Fatalf("group changed across reopen or restore:\n%+v\n%+v\n%+v", before, after, restored)
	}
	return after
}

var workerBudget = workeradmission.Budget{Ranks: 64, CPUMillis: 1 << 40, MemoryBytes: 1 << 50, Accelerators: 64}

var workerT0 = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

func workerNowUTC() time.Time { return time.Now().UTC() }

func (f *workerFixture) request(who, id string, n uint8) WorkerGroupRequest {
	return WorkerGroupRequest{Domain: f.domain, RequestID: id, Task: backend.Task{Argv: []string{"train", id}, Image: "trainer:v1"}, Spec: backend.WorkerSpec{Count: n, Mode: backend.WorkerIndependent}, Target: "kube", Binding: "kube-binding", Authority: f.authority[who], Demand: workeradmission.Demand{Ranks: uint32(n), CPUMillis: 1000, MemoryBytes: 1 << 30}}
}

func (f *workerFixture) submit(t *testing.T, who string, r WorkerGroupRequest) WorkerGroup {
	t.Helper()
	g, err := f.s.SubmitWorkerGroup(f.actors[who], r)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func (f *workerFixture) group(t *testing.T, id string) WorkerGroup {
	t.Helper()
	g, err := f.s.WorkerGroup(id)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func (f *workerFixture) admit(t *testing.T, g WorkerGroup) WorkerGroup {
	t.Helper()
	g, ok, err := f.s.AdmitWorkerGroup(g.Job.ID, g.Revision, workerBudget, workerNowUTC())
	if err != nil || !ok {
		t.Fatalf("admit: %v %v", ok, err)
	}
	return g
}

// active submits and admits a group of n ranks.
func (f *workerFixture) active(t *testing.T, who, id string, n uint8) WorkerGroup {
	t.Helper()
	return f.admit(t, f.submit(t, who, f.request(who, id, n)))
}

func (f *workerFixture) intend(t *testing.T, g WorkerGroup, rank uint8, e WorkerEffect) (WorkerToken, WorkerGroup) {
	t.Helper()
	tok, g, err := f.s.IntendWorker(g.Job.ID, g.Revision, rank, e, workerNowUTC())
	if err != nil {
		t.Fatalf("intend %d/%d: %v", rank, e, err)
	}
	return tok, g
}

func workerPod(id string, rank uint8) backend.WorkerResource {
	return backend.WorkerResource{Name: fmt.Sprintf("rank-%d", rank), UID: fmt.Sprintf("uid-%s-%d", id[:8], rank)}
}

func workerContainer(rank uint8) string { return fmt.Sprintf("container-%d", rank) }

func (f *workerFixture) create(t *testing.T, g WorkerGroup, rank uint8) WorkerGroup {
	t.Helper()
	tok, g := f.intend(t, g, rank, WorkerCreate)
	g, err := f.s.RecordWorkerCreated(tok, backend.WorkerCreated{Pod: workerPod(g.Job.ID, rank)}, "", workerNowUTC())
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func (f *workerFixture) createAll(t *testing.T, g WorkerGroup) WorkerGroup {
	t.Helper()
	for r := range g.Group.Spec.Count {
		g = f.create(t, g, r)
	}
	return g
}

func (f *workerFixture) release(t *testing.T, g WorkerGroup, rank uint8) WorkerGroup {
	t.Helper()
	tok, g := f.intend(t, g, rank, WorkerRelease)
	g, err := f.s.RecordWorkerReleased(tok, backend.WorkerReleased{Pod: g.Members[rank].Pod, Acknowledged: true, Revision: "17"}, "", workerNowUTC())
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func (f *workerFixture) cancelMember(t *testing.T, g WorkerGroup, rank uint8) WorkerGroup {
	t.Helper()
	tok, g := f.intend(t, g, rank, WorkerCancel)
	g, err := f.s.RecordWorkerCanceled(tok, backend.WorkerCanceled{Pod: g.Members[rank].Pod, Acknowledged: true}, "", workerNowUTC())
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func (f *workerFixture) remove(t *testing.T, g WorkerGroup, rank uint8) WorkerGroup {
	t.Helper()
	tok, g := f.intend(t, g, rank, WorkerRemove)
	g, err := f.s.RecordWorkerRemoved(tok, backend.WorkerRemoval{Pod: g.Members[rank].Pod, PodRemoved: true}, "", workerNowUTC())
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func (f *workerFixture) observe(t *testing.T, g WorkerGroup, rank uint8, o backend.WorkerObservation) WorkerGroup {
	t.Helper()
	g, err := f.s.RecordWorkerObservation(g.Job.ID, rank, o, workerNowUTC())
	if err != nil {
		t.Fatalf("observe %d: %v", rank, err)
	}
	return g
}

// cancel sets CancelRequested through the ordinary scoped update path.
func (f *workerFixture) cancel(t *testing.T, who, id string) {
	t.Helper()
	if _, err := f.s.UpdateFor(f.actors[who], id, func(j *backend.Job) error { j.CancelRequested = true; return nil }); err != nil {
		t.Fatal(err)
	}
}

func workerRunning(m WorkerMember, started time.Time) backend.WorkerObservation {
	return backend.WorkerObservation{Pod: m.Pod, Status: backend.Status{State: backend.Running, Instance: m.Pod.UID}, Start: &backend.ContainerStart{ContainerID: workerContainer(m.Rank), StartedAt: started}}
}

func workerExit(m WorkerMember, code int32) backend.WorkerObservation {
	state := backend.Succeeded
	if code != 0 {
		state = backend.Failed
	}
	c := code
	return backend.WorkerObservation{Pod: m.Pod, Status: backend.Status{State: state, ExitCode: &c, Termination: backend.StoppedWithExitCode, Instance: m.Pod.UID}, Exit: &backend.ContainerExit{ContainerID: workerContainer(m.Rank), ExitCode: code, StartedAt: workerT0, FinishedAt: workerT0.Add(time.Minute)}}
}

// finish drives every rank of an admitted group through create, release,
// exit code and removal.
func (f *workerFixture) finish(t *testing.T, g WorkerGroup, codes ...int32) WorkerGroup {
	t.Helper()
	g = f.createAll(t, g)
	for r := range g.Group.Spec.Count {
		g = f.release(t, g, r)
	}
	for r := range g.Group.Spec.Count {
		g = f.observe(t, g, r, workerExit(g.Members[r], codes[r]))
	}
	for r := range g.Group.Spec.Count {
		g = f.remove(t, g, r)
	}
	return g
}

func (f *workerFixture) usage(t *testing.T) retentionUsage {
	t.Helper()
	var u retentionUsage
	if err := f.s.db.View(func(tx *bolt.Tx) error {
		var e error
		u, e = usage(tx)
		return e
	}); err != nil {
		t.Fatal(err)
	}
	return u
}

func (f *workerFixture) prune(t *testing.T, sel PruneSelection) PrunePage {
	t.Helper()
	if sel.Before.IsZero() {
		sel.Before = time.Now().Add(time.Hour)
	}
	sel.AcknowledgeUnverifiedTerminal = sel.Mode == MetadataOnly
	p, err := f.s.Prune(f.actors["legacy"], sel, true, nil, nil, workerNowUTC())
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func workerReleased(p PrunePage, q QuotaDomain) uint64 {
	for _, x := range p.Transfers {
		if x.Domain == q {
			return x.Released
		}
	}
	return 0
}

func (f *workerFixture) rows(t *testing.T) int {
	t.Helper()
	n := 0
	if err := f.s.db.View(func(tx *bolt.Tx) error {
		n = tx.Bucket(workerGroupsBucket).Stats().KeyN
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

// refused checks that err is a refusal and that it wrote nothing.
func (f *workerFixture) refused(t *testing.T, before map[string]string, err error, want string) {
	t.Helper()
	if err == nil || want != "" && !strings.Contains(err.Error(), want) {
		t.Fatalf("got %v, want refusal %q", err, want)
	}
	workflowUnchanged(t, f.s, before)
}

func TestWorkerGroupSubmitAndReplay(t *testing.T) {
	f := newWorkerFixture(t)
	r := f.request("alice", "train-1", 3)
	g := f.submit(t, "alice", r)
	if g.Phase != WorkerGroupQueued || g.Reserved || g.Revision != 1 || g.Job.Status.State != backend.Queued || bytes.Contains(workersJobBytes(t, f.s, g.Job.ID), []byte(`"workers"`)) || g.Job.Status.Instance != "" || len(g.Members) != 3 || g.Group.AttemptID != g.Job.ID || g.Group.Execution.Binding != "kube-binding" || g.Principal != "alice" {
		t.Fatalf("accepted %+v", g)
	}
	stored := f.group(t, g.Job.ID)
	if replay := f.submit(t, "alice", r); replay.Job.ID != g.Job.ID || replay.Revision != g.Revision || !reflect.DeepEqual(replay, stored) {
		t.Fatalf("replay %+v, want %+v", replay, stored)
	}
	before := workflowDatabase(t, f.s)
	other := r
	other.Task = backend.Task{Argv: []string{"other"}}
	_, err := f.s.SubmitWorkerGroup(f.actors["alice"], other)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("new task: %v", err)
	}
	other = r
	other.Spec.Count, other.Demand.Ranks = 4, 4
	_, err = f.s.SubmitWorkerGroup(f.actors["alice"], other)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("new spec: %v", err)
	}
	// An ordinary request cannot use the group's request ID.
	_, err = f.s.SubmitRetained(f.actors["alice"], f.domain, "train-1", r.Task, "kube", "kube-binding", backend.RemoteOutput, 0)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("ordinary replay of group request: %v", err)
	}
	workflowUnchanged(t, f.s, before)
	// A group cannot use an ordinary request ID, even with the same task.
	plain, err := f.s.SubmitRetained(f.actors["alice"], f.domain, "plain-1", backend.Task{Argv: []string{"train", "plain-1"}, Image: "trainer:v1"}, "kube", "kube-binding", backend.RemoteOutput, 0)
	if err != nil {
		t.Fatal(err)
	}
	before = workflowDatabase(t, f.s)
	_, err = f.s.SubmitWorkerGroup(f.actors["alice"], f.request("alice", "plain-1", 2))
	f.refused(t, before, err, ErrConflict.Error())
	// Scoped reads: a foreign group and an ordinary job are not found.
	if _, err := f.s.WorkerGroupFor(f.actors["bob"], g.Job.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign read: %v", err)
	}
	if _, err := f.s.WorkerGroupFor(f.actors["alice"], plain.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ordinary read: %v", err)
	}
	if got, err := f.s.WorkerGroupFor(f.actors["alice"], g.Job.ID); err != nil || !reflect.DeepEqual(got, stored) {
		t.Fatalf("own read: %v", err)
	}
	f.durable(t, g.Job.ID)
}

func TestWorkerGroupSubmitRefusals(t *testing.T) {
	f := newWorkerFixture(t)
	limit := workerDemandLimit.CPUMillis / 3
	for _, tt := range []struct {
		name string
		edit func(*WorkerGroupRequest)
		want string
	}{
		{"request id", func(r *WorkerGroupRequest) { r.RequestID = "bad id" }, "invalid request ID"},
		{"epoch zero", func(r *WorkerGroupRequest) { r.Domain.Epoch = 0 }, "invalid request domain"},
		{"lineage", func(r *WorkerGroupRequest) { r.Domain.Lineage = "x" }, "invalid request domain"},
		{"target", func(r *WorkerGroupRequest) { r.Target = "" }, "invalid worker group target"},
		{"long target", func(r *WorkerGroupRequest) { r.Target = strings.Repeat("t", 129) }, "invalid worker group target"},
		{"binding", func(r *WorkerGroupRequest) { r.Binding = "" }, "invalid worker group target"},
		{"task retry", func(r *WorkerGroupRequest) { r.Task.Retry = &backend.RetryPolicy{MaxAttempts: 2} }, "do not support"},
		{"task setup", func(r *WorkerGroupRequest) { r.Task.Setup = []string{"x"} }, "do not support"},
		{"task outputs", func(r *WorkerGroupRequest) { r.Task.Outputs = []string{"out"} }, "do not support"},
		{"task data", func(r *WorkerGroupRequest) {
			r.Task.DataMounts = []backend.DataMount{{Reference: "d", Destination: "/mnt/d", ReadOnly: true}}
		}, "do not support"},
		{"task size", func(r *WorkerGroupRequest) { r.Task.Argv = []string{strings.Repeat("x", 256<<10)} }, "task exceeds limit"},
		{"one worker", func(r *WorkerGroupRequest) { r.Spec.Count, r.Demand.Ranks = 1, 1 }, "invalid worker group spec"},
		{"tcp", func(r *WorkerGroupRequest) { r.Spec.Mode = backend.WorkerTCP }, "invalid worker group spec"},
		{"seventeen", func(r *WorkerGroupRequest) { r.Spec.Count, r.Demand.Ranks = 17, 17 }, "invalid worker group spec"},
		{"ranks", func(r *WorkerGroupRequest) { r.Demand.Ranks = 3 }, "invalid worker demand"},
		{"zero cpu", func(r *WorkerGroupRequest) { r.Demand.CPUMillis = 0 }, "invalid worker demand"},
		{"zero memory", func(r *WorkerGroupRequest) { r.Demand.MemoryBytes = 0 }, "invalid worker demand"},
		{"overflow", func(r *WorkerGroupRequest) { r.Demand.CPUMillis = 1 << 63 }, "worker demand"},
		// The boundary is concrete: Count 3 at workerDemandLimit/3 is
		// exactly the limit, and one more per rank exceeds it.
		{"over limit", func(r *WorkerGroupRequest) {
			r.Spec.Count, r.Demand = 3, workeradmission.Demand{Ranks: 3, CPUMillis: limit + 1, MemoryBytes: 1}
		}, "worker demand"},
		{"authority", func(r *WorkerGroupRequest) { r.Authority = f.authority["bob"] }, "invalid worker group authority"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := f.request("alice", "refused", 2)
			tt.edit(&r)
			before := workflowDatabase(t, f.s)
			_, err := f.s.SubmitWorkerGroup(f.actors["alice"], r)
			f.refused(t, before, err, tt.want)
		})
	}
	r := f.request("alice", "boundary", 3)
	r.Demand = workeradmission.Demand{Ranks: 3, CPUMillis: limit, MemoryBytes: limit, Accelerators: 1}
	if 3*limit != workerDemandLimit.CPUMillis {
		t.Fatal("boundary row is not at the limit")
	}
	f.submit(t, "alice", r)
	// Accelerators may be zero; CPU and memory may not.
	if g := f.submit(t, "alice", f.request("alice", "no-accelerators", 2)); g.Demand.Accelerators != 0 {
		t.Fatal("accelerators changed")
	}
	if _, err := f.s.SubmitWorkerGroup(f.actors["reader"], f.request("alice", "reader", 2)); !errors.Is(err, auth.ErrPermissionDenied) {
		t.Fatalf("reader: %v", err)
	}
}

// TestWorkerGroupStoredTaskWorkers replaces the typed task-workers request
// case, which backend.Task can no longer express. A dedicated group carries
// its request only in Spec: its job's task bytes hold no workers member, and
// ordinary Begin refuses the group's job by its relation. A MODELED raw
// workers member in the group's stored job is then refused by every job read
// and by Open, with no byte changed.
func TestWorkerGroupStoredTaskWorkers(t *testing.T) {
	f := newWorkerFixture(t)
	g := f.submit(t, "alice", f.request("alice", "raw-workers", 2))
	if bytes.Contains(workersJobBytes(t, f.s, g.Job.ID), []byte(`"workers"`)) || len(g.Members) != 2 {
		t.Fatalf("group job %+v", g)
	}
	before := workflowDatabase(t, f.s)
	if _, start, err := f.s.BeginAttempt(g.Job.ID, g.Job.ID, 0, workerNowUTC()); !errors.Is(err, errWorkerRoute) || start {
		t.Fatalf("ordinary begin of a group: start=%v err=%v", start, err)
	}
	workflowUnchanged(t, f.s, before)
	if err := f.s.db.Update(workersJobEdit(g.Job.ID, workersTask(`"workers":`+workersSpec))); err != nil {
		t.Fatal(err)
	}
	before = workflowDatabase(t, f.s)
	if _, err := f.s.Get(g.Job.ID); !errors.Is(err, errWorkerRoute) {
		t.Fatalf("get: %v, want worker refusal", err)
	}
	if _, err := f.s.WorkerGroup(g.Job.ID); !errors.Is(err, errWorkerRoute) {
		t.Fatalf("group read: %v, want worker refusal", err)
	}
	if _, start, err := f.s.BeginAttempt(g.Job.ID, g.Job.ID, 0, workerNowUTC()); !errors.Is(err, errWorkerRoute) || start {
		t.Fatalf("begin: start=%v err=%v, want worker refusal", start, err)
	}
	workflowUnchanged(t, f.s, before)
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	environmentRefused(t, f.dir, "stored job requests workers")
}

// TestWorkerGroupAcceptanceAtomic refuses a group after its job row is
// written; the whole acceptance rolls back.
func TestWorkerGroupAcceptanceAtomic(t *testing.T) {
	p := workerPolicy()
	p.Jobs = 1
	f := newWorkerFixturePolicy(t, p)
	f.submit(t, "alice", f.request("alice", "first", 2))
	before := workflowDatabase(t, f.s)
	_, err := f.s.SubmitWorkerGroup(f.actors["alice"], f.request("alice", "second", 2))
	var capacity *CapacityError
	if !errors.As(err, &capacity) || capacity.Domain != JobsQuota {
		t.Fatalf("got %v, want jobs capacity", err)
	}
	workflowUnchanged(t, f.s, before)
}

// metadataLimit sets the installation's MetadataBytes cap to n.
func (f *workerFixture) metadataLimit(t *testing.T, n uint64) {
	t.Helper()
	if err := f.s.db.Update(func(tx *bolt.Tx) error {
		st, e := loadRetention(tx)
		if e != nil {
			return e
		}
		st.Policy.MetadataBytes = n
		return putJSON(retentionRoot(tx), retentionStateKey, st)
	}); err != nil {
		t.Fatal(err)
	}
}

// TestWorkerGroupReserveCapacity refuses a group whose ordinary job charge
// fits MetadataBytes but whose worker reserve does not. The refusal comes
// from the reserve, after trackJob, and the whole acceptance rolls back.
func TestWorkerGroupReserveCapacity(t *testing.T) {
	f := newWorkerFixture(t)
	// The principal's first job carries its scope charge.
	if _, err := f.s.SubmitRetained(f.actors["alice"], f.domain, "warm-0", backend.Task{Argv: []string{"warm"}}, "kube", "kube-binding", backend.RemoteOutput, 0); err != nil {
		t.Fatal(err)
	}
	u0 := f.usage(t)
	if _, err := f.s.SubmitRetained(f.actors["alice"], f.domain, "twin-1", backend.Task{Argv: []string{"train", "twin-1"}, Image: "trainer:v1"}, "kube", "kube-binding", backend.RemoteOutput, 0); err != nil {
		t.Fatal(err)
	}
	used := f.usage(t)[MetadataBytesQuota]
	ordinary := used - u0[MetadataBytesQuota]
	limit := used + ordinary + workerGroupReserve(2) - 1
	f.metadataLimit(t, limit)
	r := f.request("alice", "grp-01", 2)
	before := workflowDatabase(t, f.s)
	_, err := f.s.SubmitWorkerGroup(f.actors["alice"], r)
	var capacity *CapacityError
	if !errors.As(err, &capacity) || capacity.Domain != MetadataBytesQuota || capacity.Used != used+ordinary || capacity.Limit != limit {
		t.Fatalf("got %v, want metadata capacity at used %d limit %d", err, used+ordinary, limit)
	}
	workflowUnchanged(t, f.s, before)
	f.reopen(t)
	workflowUnchanged(t, f.s, before)
	if live, err := f.s.WorkerGroups(0, 10); err != nil || len(live) != 0 {
		t.Fatalf("live: %v %d", err, len(live))
	}
	// One more byte accepts the same request with exactly the reserve.
	f.metadataLimit(t, limit+1)
	g := f.submit(t, "alice", r)
	if u := f.usage(t)[MetadataBytesQuota]; u != limit+1 {
		t.Fatalf("metadata usage %d, want %d", u, limit+1)
	}
	f.durable(t, g.Job.ID)
}

func TestWorkerGroupReplayGates(t *testing.T) {
	f := newWorkerFixture(t)
	r := f.request("alice", "gated", 2)
	g := f.submit(t, "alice", r)
	expired := func(err error, want ExpiryReason) {
		t.Helper()
		var e *ExpiryError
		if !errors.As(err, &e) || e.Reason != want {
			t.Fatalf("got %v, want expiry %d", err, want)
		}
	}
	other := r
	other.Domain.Lineage = strings.Repeat("e", 32)
	_, err := f.s.SubmitWorkerGroup(f.actors["alice"], other)
	expired(err, RequestLineageChanged)
	other = r
	other.Domain.Epoch = r.Domain.Epoch + 7
	if _, err := f.s.SubmitWorkerGroup(f.actors["alice"], other); err == nil || !strings.Contains(err.Error(), "unknown request epoch") {
		t.Fatalf("unknown epoch: %v", err)
	}
	info, err := f.s.RetentionInfo(f.actors["alice"])
	if err != nil {
		t.Fatal(err)
	}
	// After the accept window a new request is refused, and the original
	// still replays.
	f.s.now = func() time.Time { return info.AcceptUntil.Add(time.Second) }
	_, err = f.s.SubmitWorkerGroup(f.actors["alice"], f.request("alice", "late", 2))
	expired(err, RequestEpochExpired)
	if replay := f.submit(t, "alice", r); replay.Job.ID != g.Job.ID {
		t.Fatal("replay in a closed epoch did not return the original")
	}
	// After the retry window the epoch retires and replay expires.
	f.s.now = func() time.Time { return info.RetryUntil.Add(time.Second) }
	_, err = f.s.SubmitWorkerGroup(f.actors["alice"], r)
	expired(err, RequestEpochExpired)
}

func TestWorkerGroupRequiresRetention(t *testing.T) {
	sf := newScopeFixture(t)
	d, err := sf.engine.Delegate(sf.actors["alice"])
	if err != nil {
		t.Fatal(err)
	}
	f := &workerFixture{s: sf.s, actors: sf.actors, authority: map[string]auth.Delegation{"alice": d}, domain: RequestDomain{Lineage: strings.Repeat("a", 32), Epoch: 1}}
	before := workflowDatabase(t, f.s)
	_, err = f.s.SubmitWorkerGroup(f.actors["alice"], f.request("alice", "r", 2))
	f.refused(t, before, err, ErrRetentionDisabled.Error())
}

// TestWorkerGroupOrdinaryEntrypoints pins D2 (a): Active excludes groups,
// and ordinary transitions refuse a group with no write although its task
// has no Workers.
func TestWorkerGroupOrdinaryEntrypoints(t *testing.T) {
	f := newWorkerFixture(t)
	plain, err := f.s.SubmitRetained(f.actors["alice"], f.domain, "plain", backend.Task{Argv: []string{"plain"}}, "kube", "kube-binding", backend.RemoteOutput, 0)
	if err != nil {
		t.Fatal(err)
	}
	running := f.create(t, f.active(t, "alice", "running", 2), 0)
	queued := f.submit(t, "alice", f.request("alice", "queued", 2))
	active, err := f.s.Active()
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 1 || active[0].ID != plain.ID {
		t.Fatalf("Active = %+v, want only the ordinary job", active)
	}
	for _, g := range []WorkerGroup{queued, running} {
		before := workflowDatabase(t, f.s)
		_, start, err := f.s.BeginAttempt(g.Job.ID, g.Job.ID, 0, workerNowUTC())
		if !errors.Is(err, errWorkerRoute) || start {
			t.Fatalf("BeginAttempt: %v %v", start, err)
		}
		_, err = f.s.ObserveAttempt(g.Job.ID, g.Job.ID, backend.Status{State: backend.Running}, workerNowUTC())
		if !errors.Is(err, errWorkerRoute) {
			t.Fatalf("ObserveAttempt: %v", err)
		}
		_, err = f.s.Update(g.Job.ID, func(j *backend.Job) error { j.Status.State = backend.Failed; return nil })
		if !errors.Is(err, errWorkerRoute) {
			t.Fatalf("Update status: %v", err)
		}
		_, err = f.s.UpdateFor(f.actors["alice"], g.Job.ID, func(j *backend.Job) error { j.Status.Reason = "x"; return nil })
		if !errors.Is(err, errWorkerRoute) {
			t.Fatalf("UpdateFor status: %v", err)
		}
		workflowUnchanged(t, f.s, before)
	}
	// Cancellation intent alone is the ordinary path's only group write.
	f.cancel(t, "alice", running.Job.ID)
	if g := f.durable(t, running.Job.ID); !g.Job.CancelRequested || !g.Stopping || g.Phase != WorkerGroupActive {
		t.Fatalf("canceled group %+v", g)
	}
}

// TestWorkerGroupLiveCap fills 64 live groups at workerDemandLimit on one
// Binding. Admission sums them without overflow, a 65th submission is
// ErrFull exactly once under a race, and a Done group frees its slot.
func TestWorkerGroupLiveCap(t *testing.T) {
	f := newWorkerFixture(t)
	third := workerDemandLimit.CPUMillis / 3
	big := func(id string) WorkerGroupRequest {
		r := f.request("alice", id, 3)
		r.Demand = workeradmission.Demand{Ranks: 3, CPUMillis: third, MemoryBytes: third, Accelerators: 1}
		return r
	}
	var groups []WorkerGroup
	for i := range 63 {
		groups = append(groups, f.submit(t, "alice", big(fmt.Sprintf("cap-%02d", i))))
	}
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = f.s.SubmitWorkerGroup(f.actors["alice"], big(fmt.Sprintf("race-%d", i)))
		}()
	}
	wg.Wait()
	if (errs[0] == nil) == (errs[1] == nil) || !errors.Is(errors.Join(errs[0], errs[1]), ErrFull) {
		t.Fatalf("race: %v", errs)
	}
	before := workflowDatabase(t, f.s)
	_, err := f.s.SubmitWorkerGroup(f.actors["bob"], f.request("bob", "full", 2))
	f.refused(t, before, err, ErrFull.Error())
	// The head and every sum fit in 64 bits.
	budget := workeradmission.Budget{Ranks: 1 << 20, CPUMillis: 1<<64 - 1, MemoryBytes: 1<<64 - 1, Accelerators: 1 << 20}
	head, ok, err := f.s.AdmitWorkerGroup(groups[0].Job.ID, groups[0].Revision, budget, workerNowUTC())
	if err != nil || !ok {
		t.Fatalf("admit at limit: %v %v", ok, err)
	}
	if _, ok, err := f.s.AdmitWorkerGroup(groups[1].Job.ID, groups[1].Revision, budget, workerNowUTC()); err != nil || !ok {
		t.Fatalf("admit second at limit: %v %v", ok, err)
	}
	// Cancel a queued group; S1 settles it Done, which frees a slot.
	f.cancel(t, "alice", groups[2].Job.ID)
	if g, _, err := f.s.AdmitWorkerGroup(groups[2].Job.ID, groups[2].Revision, budget, workerNowUTC()); err != nil || g.Phase != WorkerGroupDone {
		t.Fatalf("settle: %v %+v", err, g)
	}
	f.submit(t, "bob", f.request("bob", "freed", 2))
	if head.Phase != WorkerGroupAdmitted {
		t.Fatal("head not admitted")
	}
	f.reopen(t)
	if err := f.s.db.View(validateSnapshot); err != nil {
		t.Fatal(err)
	}
	// A 65th live group is corruption: revive the settled group's rows.
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	snapshots := t.TempDir()
	snapshot := filepath.Join(snapshots, "jobs.db")
	if err := Backup(f.dir, snapshot); err != nil {
		t.Fatal(err)
	}
	editFixture(t, f.dir, false, workerRevive(groups[2].Job.ID))
	editFixture(t, snapshots, false, workerRevive(groups[2].Job.ID))
	environmentRefused(t, f.dir, "too many live worker groups")
	workersRestoreRefused(t, snapshot, "too many live worker groups")
}

func TestWorkerGroupAdmissionOrder(t *testing.T) {
	f := newWorkerFixture(t)
	first := f.submit(t, "alice", f.request("alice", "first", 4))
	second := f.submit(t, "bob", f.request("bob", "second", 2))
	other := f.request("bob", "elsewhere", 2)
	other.Binding = "other-binding"
	elsewhere := f.submit(t, "bob", other)
	budget := workeradmission.Budget{Ranks: 4, CPUMillis: 1 << 40, MemoryBytes: 1 << 50}
	// The head fits but the second does not fit beside it: no skip-ahead.
	before := workflowDatabase(t, f.s)
	if _, ok, err := f.s.AdmitWorkerGroup(second.Job.ID, second.Revision, budget, workerNowUTC()); err != nil || ok {
		t.Fatalf("skip-ahead: %v %v", ok, err)
	}
	workflowUnchanged(t, f.s, before)
	if _, ok, err := f.s.AdmitWorkerGroup(elsewhere.Job.ID, elsewhere.Revision, budget, workerNowUTC()); err != nil || !ok {
		t.Fatalf("other binding: %v %v", ok, err)
	}
	g, ok, err := f.s.AdmitWorkerGroup(first.Job.ID, first.Revision, budget, workerNowUTC())
	if err != nil || !ok || !g.Reserved || g.Phase != WorkerGroupAdmitted {
		t.Fatalf("head: %v %v %+v", ok, err, g)
	}
	// The held reservation keeps the second out.
	if _, ok, err := f.s.AdmitWorkerGroup(second.Job.ID, second.Revision, budget, workerNowUTC()); err != nil || ok {
		t.Fatalf("held budget: %v %v", ok, err)
	}
	// A canceled head never blocks: cancel a new head behind nothing.
	third := f.submit(t, "alice", f.request("alice", "third", 2))
	f.cancel(t, "alice", g.Job.ID)
	g = f.group(t, g.Job.ID)
	if g, err = f.s.SettleWorkerGroup(g.Job.ID, g.Revision, workerNowUTC()); err != nil || g.Phase != WorkerGroupDone || g.Reserved || g.Job.Status.State != backend.Canceled {
		t.Fatalf("settle canceled admitted: %v %+v", err, g)
	}
	f.cancel(t, "bob", second.Job.ID)
	if _, ok, err := f.s.AdmitWorkerGroup(third.Job.ID, third.Revision, budget, workerNowUTC()); err != nil || !ok {
		t.Fatalf("canceled queued head blocked: %v %v", ok, err)
	}
	f.durable(t, third.Job.ID)
}

// TestWorkerGroupAdmissionReverseOrder admits strictly in acceptance order
// even when the budget fits every queued group: the second waits for the
// head, and a refused call writes nothing on any Binding.
func TestWorkerGroupAdmissionReverseOrder(t *testing.T) {
	f := newWorkerFixture(t)
	first := f.submit(t, "alice", f.request("alice", "first", 2))
	second := f.submit(t, "bob", f.request("bob", "second", 2))
	other := f.request("bob", "elsewhere", 2)
	other.Binding = "other-binding"
	elsewhere := f.submit(t, "bob", other)
	budget := workeradmission.Budget{Ranks: 4, CPUMillis: 1 << 40, MemoryBytes: 1 << 50}
	before := workflowDatabase(t, f.s)
	g, ok, err := f.s.AdmitWorkerGroup(second.Job.ID, second.Revision, budget, workerNowUTC())
	if err != nil || ok || g.Phase != WorkerGroupQueued || g.Reserved {
		t.Fatalf("second before head: %v %v %+v", ok, err, g)
	}
	workflowUnchanged(t, f.s, before)
	for _, w := range []WorkerGroup{first, second, elsewhere} {
		if g := f.durable(t, w.Job.ID); g.Phase != WorkerGroupQueued || g.Reserved || g.Revision != w.Revision {
			t.Fatalf("%s changed: %+v", w.RequestID, g)
		}
	}
	for _, w := range []WorkerGroup{first, second} {
		g, ok, err := f.s.AdmitWorkerGroup(w.Job.ID, w.Revision, budget, workerNowUTC())
		if err != nil || !ok || g.Phase != WorkerGroupAdmitted || !g.Reserved {
			t.Fatalf("admit %s: %v %v %+v", w.RequestID, ok, err, g)
		}
	}
	if g := f.durable(t, elsewhere.Job.ID); g.Phase != WorkerGroupQueued || g.Reserved || g.Revision != elsewhere.Revision {
		t.Fatalf("other binding changed: %+v", g)
	}
	if _, ok, err := f.s.AdmitWorkerGroup(elsewhere.Job.ID, elsewhere.Revision, budget, workerNowUTC()); err != nil || !ok {
		t.Fatalf("other binding: %v %v", ok, err)
	}
}

// TestWorkerGroupAdmitExceedsBudget pins D11: a head that can never fit the
// supplied budget is an error wrapping ErrExceedsBudget with no row written
// for any principal; a corrected budget then admits in order.
func TestWorkerGroupAdmitExceedsBudget(t *testing.T) {
	f := newWorkerFixture(t)
	r := f.request("alice", "gpu", 2)
	r.Demand.Accelerators = 1
	gpu := f.submit(t, "alice", r)
	cpu := f.submit(t, "bob", f.request("bob", "cpu", 2))
	for _, budget := range []workeradmission.Budget{
		{Ranks: 16, CPUMillis: 1 << 40, MemoryBytes: 1 << 50},
		{Ranks: 16, CPUMillis: 1 << 40, MemoryBytes: 1 << 50, Accelerators: 1},
		{Ranks: 16, CPUMillis: 1999, MemoryBytes: 1 << 50, Accelerators: 16},
		{Ranks: 3, CPUMillis: 1 << 40, MemoryBytes: 1<<31 - 1, Accelerators: 16},
	} {
		for _, g := range []WorkerGroup{gpu, cpu} {
			before := workflowDatabase(t, f.s)
			_, ok, err := f.s.AdmitWorkerGroup(g.Job.ID, g.Revision, budget, workerNowUTC())
			if !errors.Is(err, workeradmission.ErrExceedsBudget) || ok {
				t.Fatalf("budget %+v on %s: %v %v", budget, g.RequestID, ok, err)
			}
			workflowUnchanged(t, f.s, before)
		}
	}
	if g := f.durable(t, gpu.Job.ID); g.Phase != WorkerGroupQueued || g.Job.Status.State != backend.Queued || g.Revision != gpu.Revision {
		t.Fatalf("misfit head changed: %+v", g)
	}
	fixed := workeradmission.Budget{Ranks: 16, CPUMillis: 1 << 40, MemoryBytes: 1 << 50, Accelerators: 2}
	// The second waits for the head, then fits beside it.
	if _, ok, err := f.s.AdmitWorkerGroup(cpu.Job.ID, cpu.Revision, fixed, workerNowUTC()); err != nil || ok {
		t.Fatalf("corrected second before head: %v %v", ok, err)
	}
	if _, ok, err := f.s.AdmitWorkerGroup(gpu.Job.ID, gpu.Revision, fixed, workerNowUTC()); err != nil || !ok {
		t.Fatalf("corrected head: %v %v", ok, err)
	}
	if _, ok, err := f.s.AdmitWorkerGroup(cpu.Job.ID, cpu.Revision, fixed, workerNowUTC()); err != nil || !ok {
		t.Fatalf("corrected second: %v %v", ok, err)
	}
	// An explicit scoped cancel also clears a misfit head.
	h := f.request("alice", "huge", 16)
	huge := f.submit(t, "alice", h)
	behind := f.submit(t, "bob", f.request("bob", "behind", 2))
	small := workeradmission.Budget{Ranks: 8, CPUMillis: 1 << 40, MemoryBytes: 1 << 50, Accelerators: 16}
	if _, _, err := f.s.AdmitWorkerGroup(behind.Job.ID, behind.Revision, small, workerNowUTC()); !errors.Is(err, workeradmission.ErrExceedsBudget) {
		t.Fatalf("huge head: %v", err)
	}
	f.cancel(t, "alice", huge.Job.ID)
	if _, ok, err := f.s.AdmitWorkerGroup(behind.Job.ID, behind.Revision, small, workerNowUTC()); err != nil || !ok {
		t.Fatalf("after cancel: %v %v", ok, err)
	}
}

// TestWorkerGroupDegenerateBudget has one row per degenerate clause; each
// row is refused by that clause alone.
func TestWorkerGroupDegenerateBudget(t *testing.T) {
	f := newWorkerFixture(t)
	a := f.submit(t, "alice", f.request("alice", "a", 2))
	f.submit(t, "bob", f.request("bob", "b", 2))
	for _, budget := range []workeradmission.Budget{
		{},
		{Ranks: 1, CPUMillis: 1 << 40, MemoryBytes: 1 << 50, Accelerators: 64},
		{Ranks: 16, CPUMillis: 0, MemoryBytes: 1 << 50, Accelerators: 64},
		{Ranks: 16, CPUMillis: 1 << 40, MemoryBytes: 0, Accelerators: 64},
	} {
		before := workflowDatabase(t, f.s)
		_, ok, err := f.s.AdmitWorkerGroup(a.Job.ID, a.Revision, budget, workerNowUTC())
		if ok {
			t.Fatal("admitted")
		}
		f.refused(t, before, err, "invalid worker budget")
	}
}

func TestWorkerGroupSettleQueuedAndAdmitted(t *testing.T) {
	f := newWorkerFixture(t)
	// S1 inside Admit writes the canceled aggregate.
	q := f.submit(t, "alice", f.request("alice", "queued", 2))
	f.cancel(t, "alice", q.Job.ID)
	g, ok, err := f.s.AdmitWorkerGroup(q.Job.ID, q.Revision, workerBudget, workerNowUTC())
	if err != nil || ok || g.Phase != WorkerGroupDone || g.Job.Status.State != backend.Canceled {
		t.Fatalf("admit S1: %v %v %+v", ok, err, g)
	}
	f.durable(t, q.Job.ID)
	// Admitted, canceled, settled: the reservation is released.
	a := f.active(t, "alice", "admitted", 2)
	f.cancel(t, "alice", a.Job.ID)
	a = f.group(t, a.Job.ID)
	if a, err = f.s.SettleWorkerGroup(a.Job.ID, a.Revision, workerNowUTC()); err != nil || a.Phase != WorkerGroupDone || a.Reserved || a.Job.Status.State != backend.Canceled {
		t.Fatalf("settle canceled: %v %+v", err, a)
	}
	// Admitted, revoked: Failed, authority lost.
	v := f.active(t, "bob", "revoked", 2)
	v, err = f.s.RevokeWorkerGroup(v.Job.ID, v.Revision, fmt.Errorf("check: %w", auth.ErrUnauthenticated), workerNowUTC())
	if err != nil || v.Phase != WorkerGroupDone || v.Reserved || v.Job.Status.State != backend.Failed || v.Job.Status.Reason != "worker group authority lost" {
		t.Fatalf("revoke admitted: %v %+v", err, v)
	}
	f.durable(t, v.Job.ID)
	// A stale revision writes nothing.
	s := f.submit(t, "alice", f.request("alice", "stale", 2))
	before := workflowDatabase(t, f.s)
	_, _, err = f.s.AdmitWorkerGroup(s.Job.ID, s.Revision+1, workerBudget, workerNowUTC())
	f.refused(t, before, err, ErrWorkerGroupStale.Error())
}

func TestWorkerGroupLifecycle(t *testing.T) {
	f := newWorkerFixture(t)
	g := f.active(t, "alice", "life", 3)
	g = f.createAll(t, g)
	if g.Phase != WorkerGroupActive || g.Job.Status.State != backend.Starting {
		t.Fatalf("all created: %+v", g)
	}
	// Created and unobserved stays Starting through reopen and restore.
	g = f.durable(t, g.Job.ID)
	for r := range g.Group.Spec.Count {
		g = f.release(t, g, r)
	}
	if g.Job.Status.State != backend.Starting || !g.Members[0].ReleaseAck {
		t.Fatal("a release acknowledgement is not running")
	}
	g = f.observe(t, g, 0, workerRunning(g.Members[0], workerT0))
	if g.Job.Status.State != backend.Running {
		t.Fatalf("running: %+v", g.Job.Status)
	}
	for r := range g.Group.Spec.Count {
		g = f.observe(t, g, r, workerExit(g.Members[r], 0))
	}
	if g.Job.Status.State != backend.Succeeded || g.Phase != WorkerGroupActive || !g.Reserved || !g.Stopping {
		t.Fatalf("exited, not removed: %+v", g)
	}
	for r := range g.Group.Spec.Count {
		if !g.Reserved || g.Phase == WorkerGroupDone {
			t.Fatal("reservation released before every member settled")
		}
		g = f.remove(t, g, r)
	}
	if g.Phase != WorkerGroupDone || g.Reserved || g.Job.Status.State != backend.Succeeded || g.Job.Status.Instance != "" || g.Job.Status.ExitCode != nil {
		t.Fatalf("done: %+v", g)
	}
	f.durable(t, g.Job.ID)
	if live, err := f.s.WorkerGroups(0, 10); err != nil || len(live) != 0 {
		t.Fatalf("WorkerGroups after done: %v %d", err, len(live))
	}
	before := workflowDatabase(t, f.s)
	for e := WorkerCreate; e <= WorkerRemove; e++ {
		_, _, err := f.s.IntendWorker(g.Job.ID, g.Revision, 0, e, workerNowUTC())
		f.refused(t, before, err, "worker group is done")
	}
	_, err := f.s.RecordWorkerObservation(g.Job.ID, 0, workerExit(g.Members[0], 0), workerNowUTC())
	f.refused(t, before, err, "worker group is done")
}

// TestWorkerGroupActiveSettles drives an Active group to Done after
// revocation (A4) and after an ordinary cancel (A2).
func TestWorkerGroupActiveSettles(t *testing.T) {
	for _, tt := range []struct {
		name   string
		revoke bool
		state  backend.State
		reason string
	}{
		{"revoked", true, backend.Failed, "worker group authority lost"},
		{"canceled", false, backend.Canceled, "canceled"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newWorkerFixture(t)
			g := f.createAll(t, f.active(t, "alice", "settle", 2))
			g = f.release(t, g, 0)
			var err error
			if tt.revoke {
				if g, err = f.s.RevokeWorkerGroup(g.Job.ID, g.Revision, auth.ErrPermissionDenied, workerNowUTC()); err != nil {
					t.Fatal(err)
				}
			} else {
				f.cancel(t, "alice", g.Job.ID)
				g = f.group(t, g.Job.ID)
				if g, err = f.s.SettleWorkerGroup(g.Job.ID, g.Revision, workerNowUTC()); err != nil {
					t.Fatal(err)
				}
			}
			if g.Job.Status.State != tt.state || g.Job.Status.Reason != tt.reason || g.Phase != WorkerGroupActive || !g.Stopping || !g.Reserved {
				t.Fatalf("stopping: %+v", g)
			}
			// Rank 0 was released: it needs an exit before removal.
			g = f.cancelMember(t, g, 0)
			before := workflowDatabase(t, f.s)
			_, _, err = f.s.IntendWorker(g.Job.ID, g.Revision, 0, WorkerRemove, workerNowUTC())
			f.refused(t, before, err, "stop proof")
			g = f.observe(t, g, 0, workerExit(g.Members[0], 137))
			g = f.remove(t, g, 0)
			// Rank 1 was never released: removal alone settles it.
			g = f.remove(t, g, 1)
			if g.Phase != WorkerGroupDone || g.Reserved || g.Job.Status.State != tt.state {
				t.Fatalf("settled: %+v", g)
			}
			f.durable(t, g.Job.ID)
		})
	}
}

// TestWorkerGroupCancelAndExitOrder: a kill exit after cancel reads
// Canceled; a failure observed before the cancel stays Failed.
func TestWorkerGroupCancelAndExitOrder(t *testing.T) {
	f := newWorkerFixture(t)
	g := f.createAll(t, f.active(t, "alice", "kill", 2))
	g = f.release(t, f.release(t, g, 0), 1)
	f.cancel(t, "alice", g.Job.ID)
	g = f.observe(t, f.group(t, g.Job.ID), 0, workerExit(g.Members[0], 137))
	if g.Job.Status.State != backend.Canceled {
		t.Fatalf("cancel then kill: %+v", g.Job.Status)
	}
	h := f.createAll(t, f.active(t, "alice", "fail", 2))
	h = f.release(t, f.release(t, h, 0), 1)
	h = f.observe(t, h, 1, workerExit(h.Members[1], 1))
	if h.Job.Status.State != backend.Failed || h.Job.Status.Reason != "worker rank 1 exited with code 1" {
		t.Fatalf("failure: %+v", h.Job.Status)
	}
	f.cancel(t, "alice", h.Job.ID)
	if h = f.observe(t, f.group(t, h.Job.ID), 0, workerExit(h.Members[0], 137)); h.Job.Status.State != backend.Failed {
		t.Fatalf("failure then cancel: %+v", h.Job.Status)
	}
	// Observation after cancel is still recorded with the rank's own code.
	k := f.createAll(t, f.active(t, "alice", "observed", 2))
	k = f.release(t, k, 0)
	f.cancel(t, "alice", k.Job.ID)
	k = f.observe(t, f.group(t, k.Job.ID), 0, workerRunning(k.Members[0], workerT0))
	k = f.observe(t, k, 0, workerExit(k.Members[0], 1))
	k = f.durable(t, k.Job.ID)
	if o := k.Members[0].Observed; k.Job.Status.State != backend.Canceled || o.State != backend.Failed || o.ExitCode == nil || *o.ExitCode != 1 {
		t.Fatalf("observe after cancel: %+v %+v", k.Job.Status, o)
	}
}

func TestWorkerGroupStoppingSurvivesReopen(t *testing.T) {
	f := newWorkerFixture(t)
	c := f.create(t, f.active(t, "alice", "canceled", 2), 0)
	f.cancel(t, "alice", c.Job.ID)
	if g := f.durable(t, c.Job.ID); !g.Stopping || g.Phase != WorkerGroupActive {
		t.Fatalf("canceled: %+v", g)
	}
	v := f.create(t, f.active(t, "alice", "revoked", 2), 0)
	v, err := f.s.RevokeWorkerGroup(v.Job.ID, v.Revision, auth.ErrUnauthenticated, workerNowUTC())
	if err != nil {
		t.Fatal(err)
	}
	if g := f.durable(t, v.Job.ID); !g.Stopping || !g.Revoked || g.Job.Status.State != backend.Failed {
		t.Fatalf("revoked: %+v", g)
	}
	h := f.active(t, "alice", "held", 2)
	tok, h := f.intend(t, h, 0, WorkerCreate)
	if h, err = f.s.RecordWorkerCreated(tok, backend.WorkerCreated{}, "timeout", workerNowUTC()); err != nil || h.Phase != WorkerGroupHeld {
		t.Fatalf("held: %v %+v", err, h)
	}
	f.cancel(t, "alice", h.Job.ID)
	if g := f.durable(t, h.Job.ID); g.Phase != WorkerGroupHeld || g.Stopping || !g.Reserved {
		t.Fatalf("held after cancel: %+v", g)
	}
}

// Each case owns its fixture: durable replaces the Store with one closed by
// the case's own cleanup.
func TestWorkerGroupIntents(t *testing.T) {
	now := workerNowUTC()
	t.Run("time", func(t *testing.T) {
		f := newWorkerFixture(t)
		g := f.active(t, "alice", "time", 2)
		before := workflowDatabase(t, f.s)
		for _, at := range []time.Time{{}, time.Date(9999, 12, 31, 23, 0, 0, 0, time.FixedZone("west", -2*3600))} {
			_, _, err := f.s.IntendWorker(g.Job.ID, g.Revision, 0, WorkerCreate, at)
			f.refused(t, before, err, "invalid worker time")
		}
	})
	t.Run("cancel races create", func(t *testing.T) {
		f := newWorkerFixture(t)
		g := f.active(t, "alice", "race", 2)
		f.cancel(t, "alice", g.Job.ID)
		before := workflowDatabase(t, f.s)
		_, _, err := f.s.IntendWorker(g.Job.ID, g.Revision, 0, WorkerCreate, now)
		f.refused(t, before, err, "cancellation requested")
	})
	t.Run("interleaved ranks and effects", func(t *testing.T) {
		f := newWorkerFixture(t)
		g := f.active(t, "alice", "interleave", 2)
		t0, g := f.intend(t, g, 0, WorkerCreate)
		t1, g := f.intend(t, g, 1, WorkerCreate)
		if _, err := f.s.RecordWorkerCreated(t1, backend.WorkerCreated{Pod: workerPod(g.Job.ID, 1)}, "", now); err != nil {
			t.Fatal(err)
		}
		g, err := f.s.RecordWorkerCreated(t0, backend.WorkerCreated{Pod: workerPod(g.Job.ID, 0)}, "", now)
		if err != nil || g.Members[0].Create.State != WorkerRecorded || g.Members[1].Create.State != WorkerRecorded {
			t.Fatalf("interleaved creates: %v %+v", err, g)
		}
		r, g := f.intend(t, g, 0, WorkerRelease)
		before := workflowDatabase(t, f.s)
		_, _, err = f.s.IntendWorker(g.Job.ID, g.Revision, 0, WorkerRelease, now)
		f.refused(t, before, err, "already intended")
		c, g := f.intend(t, g, 0, WorkerCancel)
		if c.Seq != r.Seq+1 {
			t.Fatalf("sequences %d %d", r.Seq, c.Seq)
		}
		if _, err := f.s.RecordWorkerReleased(r, backend.WorkerReleased{Pod: g.Members[0].Pod, Acknowledged: true}, "", now); err != nil {
			t.Fatal(err)
		}
		g, err = f.s.RecordWorkerCanceled(c, backend.WorkerCanceled{Pod: g.Members[0].Pod, Acknowledged: true}, "", now)
		if err != nil || g.Members[0].Release.State != WorkerRecorded || g.Members[0].Cancel.State != WorkerRecorded || !g.Members[0].CancelAck {
			t.Fatalf("interleaved effects: %v %+v", err, g.Members[0])
		}
		// A resolved token is stale and writes nothing.
		before = workflowDatabase(t, f.s)
		_, err = f.s.RecordWorkerCanceled(c, backend.WorkerCanceled{Pod: g.Members[0].Pod}, "", now)
		f.refused(t, before, err, ErrWorkerGroupStale.Error())
		bad := c
		bad.Gen[0] ^= 1
		_, err = f.s.RecordWorkerCanceled(bad, backend.WorkerCanceled{Pod: g.Members[0].Pod}, "", now)
		f.refused(t, before, err, ErrWorkerGroupStale.Error())
		f.durable(t, g.Job.ID)
	})
	t.Run("create once", func(t *testing.T) {
		f := newWorkerFixture(t)
		g := f.active(t, "alice", "once", 2)
		tok, g := f.intend(t, g, 0, WorkerCreate)
		// A receipt for another pod's shape is invalid: Unknown, held, and
		// never Recorded.
		g, err := f.s.RecordWorkerCreated(tok, backend.WorkerCreated{Pod: backend.WorkerResource{Name: "Bad Name", UID: "u"}}, "", now)
		if err != nil || g.Members[0].Create.State != WorkerUnknown || g.Phase != WorkerGroupHeld || g.Members[0].Pod != (backend.WorkerResource{}) || g.Members[0].Create.Error == "" {
			t.Fatalf("invalid create: %v %+v", err, g)
		}
		before := workflowDatabase(t, f.s)
		_, _, err = f.s.IntendWorker(g.Job.ID, g.Revision, 0, WorkerCreate, now)
		f.refused(t, before, err, "not active")
		_, _, err = f.s.IntendWorker(g.Job.ID, g.Revision, 1, WorkerCreate, now)
		f.refused(t, before, err, "not active")
		f.durable(t, g.Job.ID)
	})
	t.Run("complete receipt with error", func(t *testing.T) {
		f := newWorkerFixture(t)
		g := f.active(t, "alice", "callerr", 2)
		tok, g := f.intend(t, g, 0, WorkerCreate)
		g, err := f.s.RecordWorkerCreated(tok, backend.WorkerCreated{Pod: workerPod(g.Job.ID, 0)}, "context deadline exceeded", now)
		m := g.Members[0]
		if err != nil || m.Create.State != WorkerRecorded || m.Pod != workerPod(g.Job.ID, 0) || m.Create.Error != "context_deadline_exceeded" || g.Phase != WorkerGroupActive {
			t.Fatalf("complete with error: %v %+v", err, g)
		}
		f.durable(t, g.Job.ID)
	})
	t.Run("release order", func(t *testing.T) {
		f := newWorkerFixture(t)
		g := f.create(t, f.active(t, "alice", "order", 2), 0)
		before := workflowDatabase(t, f.s)
		_, _, err := f.s.IntendWorker(g.Job.ID, g.Revision, 0, WorkerRelease, now)
		f.refused(t, before, err, "before every create")
		g = f.create(t, g, 1)
		// Release is refused after a Cancel intent, a Remove intent, or
		// a removal.
		c, g := f.intend(t, g, 0, WorkerCancel)
		before = workflowDatabase(t, f.s)
		_, _, err = f.s.IntendWorker(g.Job.ID, g.Revision, 0, WorkerRelease, now)
		f.refused(t, before, err, "cannot be released")
		if g, err = f.s.RecordWorkerCanceled(c, backend.WorkerCanceled{Pod: g.Members[0].Pod}, "", now); err != nil {
			t.Fatal(err)
		}
		_, g = f.intend(t, g, 1, WorkerRemove)
		before = workflowDatabase(t, f.s)
		_, _, err = f.s.IntendWorker(g.Job.ID, g.Revision, 1, WorkerRelease, now)
		f.refused(t, before, err, "cannot be released")
	})
	t.Run("invalid echoes resolve unknown", func(t *testing.T) {
		f := newWorkerFixture(t)
		g := f.createAll(t, f.active(t, "alice", "echo", 2))
		wrong := backend.WorkerResource{Name: "other", UID: "other-uid"}
		r, g := f.intend(t, g, 0, WorkerRelease)
		g, err := f.s.RecordWorkerReleased(r, backend.WorkerReleased{Pod: wrong, Acknowledged: true}, "", now)
		if err != nil || g.Members[0].Release.State != WorkerUnknown || g.Members[0].ReleaseAck {
			t.Fatalf("release echo: %v %+v", err, g.Members[0])
		}
		c, g := f.intend(t, g, 0, WorkerCancel)
		if g, err = f.s.RecordWorkerCanceled(c, backend.WorkerCanceled{Pod: wrong, Acknowledged: true}, "", now); err != nil || g.Members[0].Cancel.State != WorkerUnknown || g.Members[0].CancelAck {
			t.Fatalf("cancel echo: %v %+v", err, g.Members[0])
		}
		d, g := f.intend(t, g, 1, WorkerRemove)
		if g, err = f.s.RecordWorkerRemoved(d, backend.WorkerRemoval{Pod: wrong, PodRemoved: true}, "", now); err != nil || g.Members[1].Remove.State != WorkerUnknown || g.Members[1].Removed {
			t.Fatalf("remove echo: %v %+v", err, g.Members[1])
		}
		// Nothing settles on an invalid echo.
		if g.Phase == WorkerGroupDone || g.Members[1].Removed {
			t.Fatal("settled on an invalid echo")
		}
		f.durable(t, g.Job.ID)
	})
	t.Run("terminal blocks create and release only", func(t *testing.T) {
		f := newWorkerFixture(t)
		g := f.createAll(t, f.active(t, "alice", "terminal", 3))
		g = f.release(t, g, 0)
		g = f.observe(t, g, 0, workerExit(g.Members[0], 3))
		if g.Job.Status.State != backend.Failed {
			t.Fatal("not terminal")
		}
		before := workflowDatabase(t, f.s)
		_, _, err := f.s.IntendWorker(g.Job.ID, g.Revision, 1, WorkerRelease, now)
		f.refused(t, before, err, "terminal")
		g = f.cancelMember(t, g, 1)
		g = f.remove(t, g, 1)
		// Removed is final: no further Remove intent.
		before = workflowDatabase(t, f.s)
		_, _, err = f.s.IntendWorker(g.Job.ID, g.Revision, 1, WorkerRemove, now)
		f.refused(t, before, err, "already removed")
	})
	t.Run("remove requires pod", func(t *testing.T) {
		f := newWorkerFixture(t)
		g := f.active(t, "alice", "nopod", 2)
		before := workflowDatabase(t, f.s)
		for _, e := range []WorkerEffect{WorkerCancel, WorkerRemove} {
			_, _, err := f.s.IntendWorker(g.Job.ID, g.Revision, 0, e, now)
			f.refused(t, before, err, "original pod")
		}
	})
	t.Run("done waits for in-flight calls", func(t *testing.T) {
		f := newWorkerFixture(t)
		g := f.createAll(t, f.active(t, "alice", "inflight", 2))
		f.cancel(t, "alice", g.Job.ID)
		g = f.remove(t, f.group(t, g.Job.ID), 1)
		g = f.remove(t, g, 0)
		if g.Phase != WorkerGroupDone {
			t.Fatalf("never-released members did not settle: %+v", g)
		}
		h := f.createAll(t, f.active(t, "alice", "inflight-2", 2))
		f.cancel(t, "alice", h.Job.ID)
		h = f.remove(t, f.group(t, h.Job.ID), 1)
		c, h := f.intend(t, h, 0, WorkerCancel)
		h = f.remove(t, h, 0)
		if h.Phase != WorkerGroupActive || !h.Reserved {
			t.Fatalf("done while a cancel was in flight: %+v", h)
		}
		h, err := f.s.RecordWorkerCanceled(c, backend.WorkerCanceled{Pod: h.Members[0].Pod, Acknowledged: true}, "", now)
		if err != nil || h.Phase != WorkerGroupDone {
			t.Fatalf("after receipt: %v %+v", err, h)
		}
		f.durable(t, h.Job.ID)
	})
}

func TestWorkerGroupObservation(t *testing.T) {
	now := workerNowUTC()
	// ready returns a fixture owned by t and a released group in it.
	ready := func(t *testing.T, id string) (*workerFixture, WorkerGroup) {
		t.Helper()
		f := newWorkerFixture(t)
		g := f.createAll(t, f.active(t, "alice", id, 2))
		return f, f.release(t, f.release(t, g, 0), 1)
	}
	// check returns the durable group; code -1 means no exit code.
	check := func(t *testing.T, f *workerFixture, g WorkerGroup, rank uint8, state backend.State, code int32) WorkerGroup {
		t.Helper()
		g = f.durable(t, g.Job.ID)
		o := g.Members[rank].Observed
		if o == nil || o.State != state || (o.ExitCode == nil) != (code < 0) || code >= 0 && *o.ExitCode != code {
			t.Fatalf("rank %d: %+v, want %s %d", rank, o, state, code)
		}
		return g
	}
	t.Run("terminal without exit is unknown", func(t *testing.T) {
		f, g := ready(t, "noexit")
		m := g.Members[0]
		g = f.observe(t, g, 0, backend.WorkerObservation{Pod: m.Pod, Status: backend.Status{State: backend.Failed}})
		check(t, f, g, 0, backend.Unknown, -1)
		g = f.observe(t, g, 1, backend.WorkerObservation{Pod: g.Members[1].Pod, Status: backend.Status{State: backend.Canceled}})
		check(t, f, g, 1, backend.Unknown, -1)
	})
	t.Run("exit is frozen", func(t *testing.T) {
		f, g := ready(t, "frozen")
		g = f.observe(t, g, 0, workerExit(g.Members[0], 1))
		g = f.remove(t, g, 0)
		g = f.observe(t, g, 0, backend.WorkerObservation{Pod: g.Members[0].Pod, Status: backend.Status{State: backend.Unknown}})
		later := workerExit(g.Members[0], 0)
		g = f.observe(t, g, 0, later)
		run := workerRunning(g.Members[0], workerT0.Add(time.Hour))
		run.Start.ContainerID = "other-container"
		g = f.observe(t, g, 0, run)
		g = check(t, f, g, 0, backend.Failed, 1)
		if o := g.Members[0].Observed; !o.Started.Equal(workerT0) || o.Container != workerContainer(0) {
			t.Fatalf("frozen exit changed: %+v", o)
		}
	})
	t.Run("state derived from exit code", func(t *testing.T) {
		f, g := ready(t, "derived")
		a := workerExit(g.Members[0], 0)
		a.Status.State = backend.Running
		g = f.observe(t, g, 0, a)
		b := workerExit(g.Members[1], 137)
		b.Status.State = backend.Succeeded
		g = f.observe(t, g, 1, b)
		check(t, f, g, 0, backend.Succeeded, 0)
		check(t, f, g, 1, backend.Failed, 137)
	})
	t.Run("exit overwrites a later start", func(t *testing.T) {
		f, g := ready(t, "ordering")
		g = f.observe(t, g, 0, workerRunning(g.Members[0], workerT0.Add(time.Hour)))
		g = f.observe(t, g, 0, workerExit(g.Members[0], 0))
		g = check(t, f, g, 0, backend.Succeeded, 0)
		if o := g.Members[0].Observed; o.Finished.Before(o.Started) {
			t.Fatalf("finished before started: %+v", o)
		}
	})
	t.Run("exit without running", func(t *testing.T) {
		f, g := ready(t, "direct")
		g = f.observe(t, g, 0, workerExit(g.Members[0], 0))
		g = check(t, f, g, 0, backend.Succeeded, 0)
		if o := g.Members[0].Observed; o.Container != workerContainer(0) || !o.Started.Equal(workerT0) {
			t.Fatalf("exit fields: %+v", o)
		}
	})
	t.Run("running never regresses", func(t *testing.T) {
		f, g := ready(t, "regress")
		g = f.observe(t, g, 0, workerRunning(g.Members[0], workerT0))
		g = f.observe(t, g, 0, backend.WorkerObservation{Pod: g.Members[0].Pod, Status: backend.Status{State: backend.Queued}})
		g = check(t, f, g, 0, backend.Unknown, -1)
		if g.Members[0].Observed.Container != workerContainer(0) || g.Job.Status.State != backend.Running {
			t.Fatalf("regressed: %+v %+v", g.Members[0].Observed, g.Job.Status)
		}
	})
	t.Run("time zones", func(t *testing.T) {
		f, g := ready(t, "zone")
		zone := time.FixedZone("east", 14*3600)
		x := workerExit(g.Members[0], 0)
		x.Exit.StartedAt = time.Date(9999, 12, 31, 22, 0, 0, 0, time.UTC).In(zone)
		x.Exit.FinishedAt = time.Date(9999, 12, 31, 23, 0, 0, 0, time.UTC).In(zone)
		g = f.observe(t, g, 0, x)
		check(t, f, g, 0, backend.Succeeded, 0)
	})
	t.Run("refusals", func(t *testing.T) {
		f, g := ready(t, "refuse")
		g = f.observe(t, g, 0, workerRunning(g.Members[0], workerT0))
		m := g.Members[0]
		mismatch := workerExit(m, 0)
		mismatch.Exit.ContainerID = "other"
		badExit := workerExit(m, 0)
		badExit.Exit.FinishedAt = workerT0.Add(-time.Hour)
		noProof := workerExit(m, 0)
		noProof.Status.Termination = backend.UnknownTermination
		wrongPod := workerExit(m, 0)
		wrongPod.Pod.UID = "other"
		wrongInstance := workerExit(m, 0)
		wrongInstance.Status.Instance = "other"
		startless := workerRunning(m, workerT0)
		startless.Start = nil
		for _, o := range []backend.WorkerObservation{mismatch, badExit, noProof, wrongPod, wrongInstance, startless} {
			before := workflowDatabase(t, f.s)
			_, err := f.s.RecordWorkerObservation(g.Job.ID, 0, o, now)
			f.refused(t, before, err, "")
		}
		h := f.active(t, "alice", "uncreated", 2)
		before := workflowDatabase(t, f.s)
		_, err := f.s.RecordWorkerObservation(h.Job.ID, 0, backend.WorkerObservation{Status: backend.Status{State: backend.Queued}}, now)
		f.refused(t, before, err, "identity mismatch")
	})
}

// TestWorkerGroupGeneration: an intent of this process is never declared
// lost, even across Close and Open, and its original receipt still lands.
func TestWorkerGroupGeneration(t *testing.T) {
	f := newWorkerFixture(t)
	g := f.active(t, "alice", "gen", 2)
	tok, g := f.intend(t, g, 0, WorkerCreate)
	f.reopen(t)
	before := workflowDatabase(t, f.s)
	_, err := f.s.RecordWorkerLost(tok, workerNowUTC())
	if !errors.Is(err, errWorkerIntentLive) {
		t.Fatalf("lost: %v", err)
	}
	workflowUnchanged(t, f.s, before)
	g, err = f.s.RecordWorkerCreated(tok, backend.WorkerCreated{Pod: workerPod(g.Job.ID, 0)}, "", workerNowUTC())
	if err != nil || g.Members[0].Create.State != WorkerRecorded {
		t.Fatalf("receipt after refused lost: %v %+v", err, g)
	}
	if tok.Gen != workerGen() || g.Members[0].Create.Gen != tok.Gen {
		t.Fatal("generation not stored")
	}
}

// TestWorkerGroupLostAfterRestart writes a Create intent in a child process
// that exits without a receipt; this process then declares it lost.
func TestWorkerGroupLostAfterRestart(t *testing.T) {
	if dir := os.Getenv("SKYGO_WORKER_GROUP_CHILD"); dir != "" {
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		live, err := s.WorkerGroups(0, 1)
		if err != nil || len(live) != 1 {
			t.Fatalf("child: %v", err)
		}
		if _, _, err := s.IntendWorker(live[0].Job.ID, live[0].Revision, 0, WorkerCreate, time.Now()); err != nil {
			t.Fatal(err)
		}
		return
	}
	f := newWorkerFixture(t)
	g := f.active(t, "alice", "restart", 2)
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^TestWorkerGroupLostAfterRestart$", "-test.count=1")
	cmd.Env = append(os.Environ(), "SKYGO_WORKER_GROUP_CHILD="+f.dir)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child: %v\n%s", err, out)
	}
	f.open(t)
	g = f.group(t, g.Job.ID)
	c := g.Members[0].Create
	if c.State != WorkerIntended || c.Gen == workerGen() {
		t.Fatalf("child intent: %+v", c)
	}
	tok := WorkerToken{Job: g.Job.ID, Rank: 0, Effect: WorkerCreate, Seq: c.Seq, Gen: c.Gen}
	g, err = f.s.RecordWorkerLost(tok, workerNowUTC())
	if err != nil || g.Members[0].Create.State != WorkerUnknown || g.Members[0].Create.Error != "lost" || g.Phase != WorkerGroupHeld || !g.Reserved {
		t.Fatalf("lost: %v %+v", err, g)
	}
	f.durable(t, g.Job.ID)
}

func TestWorkerGroupRevokeCauses(t *testing.T) {
	f := newWorkerFixture(t)
	g := f.createAll(t, f.active(t, "alice", "revoke", 2))
	before := workflowDatabase(t, f.s)
	for _, cause := range []error{nil, auth.ErrUnavailable, fmt.Errorf("check: %w", auth.ErrUnavailable), errors.New("disk"), fmt.Errorf("x: %w", auth.ErrInvalid), auth.ErrConflict} {
		_, err := f.s.RevokeWorkerGroup(g.Job.ID, g.Revision, cause, workerNowUTC())
		f.refused(t, before, err, "authentication refusal")
	}
	g, err := f.s.RevokeWorkerGroup(g.Job.ID, g.Revision, fmt.Errorf("check: %w", auth.ErrPermissionDenied), workerNowUTC())
	if err != nil || !g.Revoked {
		t.Fatalf("revoke: %v", err)
	}
	before = workflowDatabase(t, f.s)
	_, _, err = f.s.IntendWorker(g.Job.ID, g.Revision, 0, WorkerRelease, workerNowUTC())
	f.refused(t, before, err, "revoked")
	g = f.remove(t, g, 0)
	if g.Members[0].Remove.State != WorkerRecorded {
		t.Fatal("remove after revoke")
	}
}

// TestWorkerGroupHeldPinsCapacity pins D9: a Held group keeps its
// reservation, counts as live and blocks its Binding across reopen.
func TestWorkerGroupHeldPinsCapacity(t *testing.T) {
	f := newWorkerFixture(t)
	g := f.active(t, "alice", "held", 2)
	tok, g := f.intend(t, g, 0, WorkerCreate)
	g, err := f.s.RecordWorkerCreated(tok, backend.WorkerCreated{}, "", workerNowUTC())
	if err != nil || g.Phase != WorkerGroupHeld {
		t.Fatal(err)
	}
	next := f.submit(t, "bob", f.request("bob", "next", 2))
	f.reopen(t)
	budget := workeradmission.Budget{Ranks: 3, CPUMillis: 1 << 40, MemoryBytes: 1 << 50}
	if _, ok, err := f.s.AdmitWorkerGroup(next.Job.ID, next.Revision, budget, workerNowUTC()); err != nil || ok {
		t.Fatalf("admitted beside a held group: %v %v", ok, err)
	}
	g = f.group(t, g.Job.ID)
	if g, err = f.s.SettleWorkerGroup(g.Job.ID, g.Revision, workerNowUTC()); err != nil || g.Phase != WorkerGroupHeld || !g.Reserved {
		t.Fatalf("held settled: %v %+v", err, g)
	}
	live, err := f.s.WorkerGroups(0, 10)
	if err != nil || len(live) != 2 {
		t.Fatalf("live: %v %d", err, len(live))
	}
}

// TestWorkerGroupUnsettleablePins pins named D9 triggers: a released rank
// with no exit cannot be removed, and a removal that never proves PodRemoved
// keeps the group reserved.
func TestWorkerGroupUnsettleablePins(t *testing.T) {
	f := newWorkerFixture(t)
	g := f.createAll(t, f.active(t, "alice", "pin", 2))
	g = f.release(t, g, 0)
	f.cancel(t, "alice", g.Job.ID)
	g = f.cancelMember(t, f.group(t, g.Job.ID), 0)
	before := workflowDatabase(t, f.s)
	_, _, err := f.s.IntendWorker(g.Job.ID, g.Revision, 0, WorkerRemove, workerNowUTC())
	f.refused(t, before, err, "stop proof")
	tok, g := f.intend(t, g, 1, WorkerRemove)
	g, err = f.s.RecordWorkerRemoved(tok, backend.WorkerRemoval{}, "deadline", workerNowUTC())
	if err != nil || g.Members[1].Remove.State != WorkerUnknown {
		t.Fatal(err)
	}
	tok, g = f.intend(t, g, 1, WorkerRemove)
	if g, err = f.s.RecordWorkerRemoved(tok, backend.WorkerRemoval{Pod: g.Members[1].Pod}, "", workerNowUTC()); err != nil {
		t.Fatal(err)
	}
	g = f.durable(t, g.Job.ID)
	if g.Members[1].Removed || g.Phase != WorkerGroupActive || !g.Reserved || !g.Stopping {
		t.Fatalf("unsettleable group: %+v", g)
	}
	if g, err = f.s.SettleWorkerGroup(g.Job.ID, g.Revision, workerNowUTC()); err != nil || g.Phase == WorkerGroupDone {
		t.Fatalf("settled without proof: %v %+v", err, g)
	}
}

// TestWorkerGroupClearing: a re-intent clears the effect's Error and, for
// Cancel, CancelAck, so every row validates through reopen and restore.
func TestWorkerGroupClearing(t *testing.T) {
	f := newWorkerFixture(t)
	now := workerNowUTC()
	g := f.createAll(t, f.active(t, "alice", "clear", 2))
	c, g := f.intend(t, g, 0, WorkerCancel)
	g, err := f.s.RecordWorkerCanceled(c, backend.WorkerCanceled{}, "refused", now)
	if err != nil || g.Members[0].Cancel.Error != "refused" {
		t.Fatal(err)
	}
	c, g = f.intend(t, g, 0, WorkerCancel)
	if g.Members[0].Cancel.Error != "" {
		t.Fatal("intent kept the error")
	}
	f.durable(t, g.Job.ID)
	g = f.group(t, g.Job.ID)
	if g, err = f.s.RecordWorkerCanceled(c, backend.WorkerCanceled{Pod: g.Members[0].Pod, Acknowledged: true}, "", now); err != nil || !g.Members[0].CancelAck {
		t.Fatal(err)
	}
	_, g = f.intend(t, g, 0, WorkerCancel)
	if g = f.durable(t, g.Job.ID); g.Members[0].CancelAck {
		t.Fatal("cancel intent kept the acknowledgement")
	}
	r, g := f.intend(t, g, 1, WorkerRemove)
	if g, err = f.s.RecordWorkerRemoved(r, backend.WorkerRemoval{}, "", now); err != nil || g.Members[1].Remove.State != WorkerUnknown {
		t.Fatal(err)
	}
	r, g = f.intend(t, g, 1, WorkerRemove)
	if g.Members[1].Remove.Error != "" {
		t.Fatal("remove intent kept the error")
	}
	if g, err = f.s.RecordWorkerRemoved(r, backend.WorkerRemoval{Pod: g.Members[1].Pod}, "", now); err != nil || g.Members[1].Remove.State != WorkerRecorded || g.Members[1].Removed {
		t.Fatal(err)
	}
	f.durable(t, g.Job.ID)
}

// TestWorkerGroupPruneSkipsLive: Prune never retires a group that is not
// Done, even one whose aggregate is terminal.
func TestWorkerGroupPruneSkipsLive(t *testing.T) {
	f := newWorkerFixture(t)
	stopping := f.createAll(t, f.active(t, "alice", "stopping", 2))
	f.cancel(t, "alice", stopping.Job.ID)
	stopping = f.group(t, stopping.Job.ID)
	stopping, err := f.s.SettleWorkerGroup(stopping.Job.ID, stopping.Revision, workerNowUTC())
	if err != nil || stopping.Job.Status.State != backend.Canceled || stopping.Phase != WorkerGroupActive {
		t.Fatalf("stopping: %v %+v", err, stopping)
	}
	held := f.active(t, "alice", "held", 2)
	tok, held := f.intend(t, held, 0, WorkerCreate)
	if _, err := f.s.RecordWorkerCreated(tok, backend.WorkerCreated{}, "", workerNowUTC()); err != nil {
		t.Fatal(err)
	}
	f.cancel(t, "alice", held.Job.ID)
	f.submit(t, "alice", f.request("alice", "queued", 2))
	rows := f.rows(t)
	for _, mode := range []PruneMode{Whole, MetadataOnly, OutputOnly} {
		p := f.prune(t, PruneSelection{Mode: mode})
		if p.Selected != 0 || p.Marked != 0 || len(p.Transfers) != 0 {
			t.Fatalf("mode %d pruned a live group: %+v", mode, p)
		}
		if len(p.Skips) != 1 || p.Skips[0] != (PruneSkip{"worker_group", 3}) {
			t.Fatalf("mode %d skips: %+v", mode, p.Skips)
		}
	}
	if f.rows(t) != rows {
		t.Fatal("prune changed live group rows")
	}
	f.durable(t, stopping.Job.ID)
}

// TestWorkerGroupPruneRetiresDone: Prune retires a Done group's rows with
// its job and refunds exactly the group reserve, measured against an
// ordinary twin job of the same shape.
func TestWorkerGroupPruneRetiresDone(t *testing.T) {
	for _, mode := range []PruneMode{Whole, MetadataOnly} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			f := newWorkerFixture(t)
			// The principal's first job carries its scope charge.
			if _, err := f.s.SubmitRetained(f.actors["alice"], f.domain, "warm-0", backend.Task{Argv: []string{"warm"}}, "kube", "kube-binding", backend.RemoteOutput, 0); err != nil {
				t.Fatal(err)
			}
			u0 := f.usage(t)
			twin, err := f.s.SubmitRetained(f.actors["alice"], f.domain, "twin-1", backend.Task{Argv: []string{"train", "twin-1"}, Image: "trainer:v1"}, "kube", "kube-binding", backend.RemoteOutput, 0)
			if err != nil {
				t.Fatal(err)
			}
			u1 := f.usage(t)
			r := f.request("alice", "grp-01", 2)
			g := f.submit(t, "alice", r)
			u2 := f.usage(t)
			reserve := workerGroupReserve(2)
			if (u2[MetadataBytesQuota]-u1[MetadataBytesQuota])-(u1[MetadataBytesQuota]-u0[MetadataBytesQuota]) != reserve {
				t.Fatalf("acceptance charge %d, ordinary %d, want reserve %d", u2[MetadataBytesQuota]-u1[MetadataBytesQuota], u1[MetadataBytesQuota]-u0[MetadataBytesQuota], reserve)
			}
			if _, err := f.s.Update(twin.ID, func(j *backend.Job) error {
				j.CancelRequested = true
				j.Status = backend.Status{State: backend.Canceled, Reason: "canceled"}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			// The live group is skipped while its twin is retired.
			p1 := f.prune(t, PruneSelection{Mode: mode})
			if p1.Selected != 1 || !slicesContainsSkip(p1.Skips, PruneSkip{"worker_group", 1}) {
				t.Fatalf("first prune: %+v", p1)
			}
			g = f.finish(t, f.admit(t, g), 0, 0)
			if g.Phase != WorkerGroupDone {
				t.Fatalf("finished: %+v", g)
			}
			before := f.usage(t)
			p2 := f.prune(t, PruneSelection{Mode: mode})
			if p2.Selected != 1 {
				t.Fatalf("second prune: %+v", p2)
			}
			if got := workerReleased(p2, MetadataBytesQuota) - workerReleased(p1, MetadataBytesQuota); got != reserve {
				t.Fatalf("refund %d beyond the ordinary twin, want %d", got, reserve)
			}
			if after := f.usage(t); before[MetadataBytesQuota]-after[MetadataBytesQuota] != workerReleased(p2, MetadataBytesQuota) {
				t.Fatal("usage does not match the page")
			}
			if f.rows(t) != 0 {
				t.Fatal("group rows survived prune")
			}
			if _, err := f.s.WorkerGroup(g.Job.ID); !errors.Is(err, ErrNotFound) {
				t.Fatalf("pruned group: %v", err)
			}
			var expired *ExpiryError
			if _, err := f.s.SubmitWorkerGroup(f.actors["alice"], r); !errors.As(err, &expired) || expired.Reason != ResultExpired || expired.JobID != g.Job.ID {
				t.Fatalf("replay after prune: %v", err)
			}
			f.reopen(t)
			if err := f.s.db.View(validateSnapshot); err != nil {
				t.Fatal(err)
			}
			if err := f.s.Close(); err != nil {
				t.Fatal(err)
			}
			snapshot := filepath.Join(t.TempDir(), "snapshot.db")
			if err := Backup(f.dir, snapshot); err != nil {
				t.Fatal(err)
			}
			if err := Restore(snapshot, filepath.Join(t.TempDir(), "restored")); err != nil {
				t.Fatal(err)
			}
			f.open(t)
		})
	}
}

func slicesContainsSkip(skips []PruneSkip, want PruneSkip) bool {
	for _, s := range skips {
		if s == want {
			return true
		}
	}
	return false
}

// TestWorkerGroupPruneKeepsOutputOnly: an output-only prune has no output to
// retire and keeps a Done group's rows and reserve.
func TestWorkerGroupPruneKeepsOutputOnly(t *testing.T) {
	f := newWorkerFixture(t)
	g := f.submit(t, "alice", f.request("alice", "done", 2))
	f.cancel(t, "alice", g.Job.ID)
	if g, _, err := f.s.AdmitWorkerGroup(g.Job.ID, g.Revision, workerBudget, workerNowUTC()); err != nil || g.Phase != WorkerGroupDone {
		t.Fatalf("settle: %v %+v", err, g)
	}
	before := workflowDatabase(t, f.s)
	p := f.prune(t, PruneSelection{Mode: OutputOnly})
	if p.Selected != 0 || !slicesContainsSkip(p.Skips, PruneSkip{"current_output", 1}) {
		t.Fatalf("output-only: %+v", p)
	}
	workflowUnchanged(t, f.s, before)
}

// TestWorkerGroupPruneDryRun plans the retirement and refund of a Done
// group without writing.
func TestWorkerGroupPruneDryRun(t *testing.T) {
	f := newWorkerFixture(t)
	g := f.submit(t, "alice", f.request("alice", "done", 3))
	f.cancel(t, "alice", g.Job.ID)
	if _, _, err := f.s.AdmitWorkerGroup(g.Job.ID, g.Revision, workerBudget, workerNowUTC()); err != nil {
		t.Fatal(err)
	}
	before := workflowDatabase(t, f.s)
	p, err := f.s.Prune(f.actors["legacy"], PruneSelection{Before: time.Now().Add(time.Hour), Mode: Whole}, false, nil, nil, workerNowUTC())
	if err != nil || p.Selected != 1 || p.Marked != 0 || workerReleased(p, MetadataBytesQuota) < workerGroupReserve(3) {
		t.Fatalf("dry run: %v %+v", err, p)
	}
	workflowUnchanged(t, f.s, before)
}

// TestWorkerGroupPruneRollback: the retirement of a Done group is part of
// its prune transaction, and a group that is not Done is never retired.
func TestWorkerGroupPruneRollback(t *testing.T) {
	f := newWorkerFixture(t)
	done := f.submit(t, "alice", f.request("alice", "done", 2))
	f.cancel(t, "alice", done.Job.ID)
	if _, _, err := f.s.AdmitWorkerGroup(done.Job.ID, done.Revision, workerBudget, workerNowUTC()); err != nil {
		t.Fatal(err)
	}
	live := f.active(t, "alice", "live", 2)
	before := workflowDatabase(t, f.s)
	injected := errors.New("rollback prune")
	err := f.s.db.Update(func(tx *bolt.Tx) error {
		m, err := newRetentionMutation(tx)
		if err != nil {
			return err
		}
		if err := m.dropWorkerGroup(live.Job.ID); err == nil || err.Error() != "worker group is not done" {
			t.Errorf("retired a live group: %v", err)
		}
		if err := m.dropWorkerGroup(done.Job.ID); err != nil {
			return err
		}
		if m.initial[MetadataBytesQuota]-m.used[MetadataBytesQuota] != workerGroupReserve(2) {
			t.Errorf("planned refund %d", m.initial[MetadataBytesQuota]-m.used[MetadataBytesQuota])
		}
		if err := m.commit(); err != nil {
			return err
		}
		if workerGroupJob(tx, done.Job.ID) || tx.Bucket(workerGroupsBucket).Get(workerMemberKey(done.Job.ID, 1)) != nil {
			t.Error("rows survived the planned retirement")
		}
		return injected
	})
	if !errors.Is(err, injected) {
		t.Fatalf("transaction: %v", err)
	}
	workflowUnchanged(t, f.s, before)
	f.durable(t, done.Job.ID)
}

// TestWorkerGroupRetirementOverTime accepts more than 64 groups of the
// largest size, with tasks near the task limit, over time under a small job
// quota: without Prune the next group is refused, and Prune makes room each
// round. A prune page changes at most maxChangeBytes, so each round may take
// several pages.
func TestWorkerGroupRetirementOverTime(t *testing.T) {
	p := workerPolicy()
	p.Jobs = 8
	f := newWorkerFixturePolicy(t, p)
	arg := strings.Repeat("x", 250<<10)
	n := 0
	for round := range 9 {
		for i := range 8 {
			r := f.request("alice", fmt.Sprintf("r%d-%d", round, i), backend.MaxWorkers)
			r.Task.Argv = []string{arg, r.RequestID}
			g := f.submit(t, "alice", r)
			f.cancel(t, "alice", g.Job.ID)
			if g, _, err := f.s.AdmitWorkerGroup(g.Job.ID, g.Revision, workerBudget, workerNowUTC()); err != nil || g.Phase != WorkerGroupDone {
				t.Fatalf("settle %d: %v", n, err)
			}
			n++
		}
		before := workflowDatabase(t, f.s)
		_, err := f.s.SubmitWorkerGroup(f.actors["alice"], f.request("alice", fmt.Sprintf("full-%d", round), 2))
		var capacity *CapacityError
		if !errors.As(err, &capacity) || capacity.Domain != JobsQuota {
			t.Fatalf("round %d without prune: %v", round, err)
		}
		workflowUnchanged(t, f.s, before)
		for pruned := uint32(0); pruned < 8; {
			page := f.prune(t, PruneSelection{Mode: Whole})
			if page.Selected == 0 {
				t.Fatalf("round %d prune: %+v", round, page)
			}
			pruned += page.Selected
		}
	}
	if n <= maxLiveWorkerGroups || f.rows(t) != 0 {
		t.Fatalf("accepted %d, rows %d", n, f.rows(t))
	}
	f.reopen(t)
	if err := f.s.db.View(func(tx *bolt.Tx) error {
		if seq := tx.Bucket(workerGroupsBucket).Sequence(); seq != uint64(n) {
			t.Errorf("sequence %d, want %d", seq, n)
		}
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}
}

// workerEditGroup and workerEditMember rewrite one stored row with fn,
// without validation, for corruption fixtures.
func workerEditGroup(id string, fn func(*workerGroupRecord)) func(*bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		b := tx.Bucket(workerGroupsBucket)
		var g workerGroupRecord
		if err := json.Unmarshal(b.Get(workerGroupKey(id)), &g); err != nil {
			return err
		}
		fn(&g)
		raw, err := json.Marshal(g)
		if err != nil {
			return err
		}
		return b.Put(workerGroupKey(id), raw)
	}
}

func workerEditMember(id string, rank uint8, fn func(*workerMemberRecord)) func(*bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		b := tx.Bucket(workerGroupsBucket)
		var m workerMemberRecord
		if err := json.Unmarshal(b.Get(workerMemberKey(id, rank)), &m); err != nil {
			return err
		}
		fn(&m)
		raw, err := json.Marshal(m)
		if err != nil {
			return err
		}
		return b.Put(workerMemberKey(id, rank), raw)
	}
}

// workerRevive turns a settled, never-created Done group back into a live
// Active group with one recorded Create.
func workerRevive(id string) func(*bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		if err := workerEditGroup(id, func(g *workerGroupRecord) { g.Phase, g.Reserved = WorkerGroupActive, true })(tx); err != nil {
			return err
		}
		return workerEditMember(id, 0, func(m *workerMemberRecord) {
			m.Counter = 1
			m.Create = workerEffectRecord{State: WorkerRecorded, Seq: 1, Gen: workerGenString(workerGen()), At: workerT0}
			p := workerPod(id, 0)
			m.Pod = workerPodRecord{p.Name, p.UID}
		})(tx)
	}
}

// workerRemovedCreate gives rank of group id a Create in state create with
// removal-shaped evidence: the original Pod, a Recorded Remove, Removed, and
// no Release.
func workerRemovedCreate(id string, rank uint8, create WorkerIntent) func(*bolt.Tx) error {
	return workerEditMember(id, rank, func(m *workerMemberRecord) {
		gen := workerGenString(workerGen())
		m.Counter = 2
		m.Create = workerEffectRecord{State: create, Seq: 1, Gen: gen, At: workerT0}
		if create == WorkerUnknown {
			m.Create.Error = "lost"
		}
		p := workerPod(id, rank)
		m.Pod = workerPodRecord{p.Name, p.UID}
		m.Remove = workerEffectRecord{State: WorkerRecorded, Seq: 2, Gen: gen, At: workerT0}
		m.Removed = true
	})
}

// TestWorkerGroupDoneOpens is the positive side of the Done corruption rows:
// a never-created Done group, and one whose Recorded Create was retired by
// Remove, open and restore unchanged.
func TestWorkerGroupDoneOpens(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(id string) func(*bolt.Tx) error
	}{
		{"never created", nil},
		{"recorded create retired", func(id string) func(*bolt.Tx) error { return workerRemovedCreate(id, 0, WorkerRecorded) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir, _, done := workerCorruptible(t)
			if tt.edit != nil {
				editFixture(t, dir, false, tt.edit(done.Job.ID))
			}
			f := &workerFixture{dir: dir}
			f.open(t)
			if g := f.durable(t, done.Job.ID); g.Phase != WorkerGroupDone || g.Job.Status.State != backend.Canceled {
				t.Fatalf("done group: %+v", g)
			}
		})
	}
}

// TestWorkerMemberSettled pins settled: removal evidence never settles an
// Unknown Create.
func TestWorkerMemberSettled(t *testing.T) {
	gen := workerGenString(workerGen())
	effect := func(s WorkerIntent, seq uint64) workerEffectRecord {
		return workerEffectRecord{State: s, Seq: seq, Gen: gen, At: workerT0}
	}
	pod := workerPodRecord{"rank-0", "uid-0"}
	for _, tt := range []struct {
		name string
		m    workerMemberRecord
		want bool
	}{
		{"never created", workerMemberRecord{}, true},
		{"recorded removed", workerMemberRecord{Create: effect(WorkerRecorded, 1), Pod: pod, Remove: effect(WorkerRecorded, 2), Removed: true}, true},
		{"unknown removed", workerMemberRecord{Create: effect(WorkerUnknown, 1), Pod: pod, Remove: effect(WorkerRecorded, 2), Removed: true}, false},
		{"recorded not removed", workerMemberRecord{Create: effect(WorkerRecorded, 1), Pod: pod}, false},
		{"remove in flight", workerMemberRecord{Create: effect(WorkerRecorded, 1), Pod: pod, Remove: effect(WorkerIntended, 2)}, false},
	} {
		if got := tt.m.settled(); got != tt.want {
			t.Errorf("%s: settled = %v, want %v", tt.name, got, tt.want)
		}
	}
}

// workerCorruptible returns a closed installation with an Active group of
// three ranks and a Done group of two:
//
//	rank 0: created, released, running
//	rank 1: created, cancel recorded with acknowledgement
//	rank 2: created
func workerCorruptible(t *testing.T) (dir string, active, done WorkerGroup) {
	t.Helper()
	f := newWorkerFixture(t)
	active = f.createAll(t, f.active(t, "alice", "active", 3))
	active = f.release(t, active, 0)
	active = f.observe(t, active, 0, workerRunning(active.Members[0], workerT0))
	active = f.cancelMember(t, active, 1)
	done = f.submit(t, "bob", f.request("bob", "done", 2))
	f.cancel(t, "bob", done.Job.ID)
	done, _, err := f.s.AdmitWorkerGroup(done.Job.ID, done.Revision, workerBudget, workerNowUTC())
	if err != nil || done.Phase != WorkerGroupDone {
		t.Fatalf("done: %v %+v", err, done)
	}
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	return f.dir, active, done
}

// TestWorkerGroupCorruptionRefused refuses each corrupt row through Open
// and Restore without changing a byte.
func TestWorkerGroupCorruptionRefused(t *testing.T) {
	type edit func(a, d WorkerGroup) func(*bolt.Tx) error
	group := func(fn func(*workerGroupRecord)) edit {
		return func(a, _ WorkerGroup) func(*bolt.Tx) error { return workerEditGroup(a.Job.ID, fn) }
	}
	member := func(rank uint8, fn func(*workerMemberRecord)) edit {
		return func(a, _ WorkerGroup) func(*bolt.Tx) error { return workerEditMember(a.Job.ID, rank, fn) }
	}
	raw := func(key func(a, d WorkerGroup) []byte, fn func(b *bolt.Bucket, k []byte) error) edit {
		return func(a, d WorkerGroup) func(*bolt.Tx) error {
			return func(tx *bolt.Tx) error { return fn(tx.Bucket(workerGroupsBucket), key(a, d)) }
		}
	}
	groupKey := func(a, _ WorkerGroup) []byte { return workerGroupKey(a.Job.ID) }
	memberKey := func(a, _ WorkerGroup) []byte { return workerMemberKey(a.Job.ID, 0) }
	other := strings.Repeat("e", 32)
	zone := time.FixedZone("east", 3600)
	for _, tt := range []struct {
		name, want string
		edit       edit
	}{
		{"group JSON", "invalid worker group record", raw(groupKey, func(b *bolt.Bucket, k []byte) error { return b.Put(k, []byte("{")) })},
		{"group unknown field", "invalid worker group record", raw(groupKey, func(b *bolt.Bucket, k []byte) error {
			v := b.Get(k)
			return b.Put(k, append(bytes.Clone(v[:len(v)-1]), []byte(`,"x":1}`)...))
		})},
		{"group encoding", "invalid worker group record encoding", raw(groupKey, func(b *bolt.Bucket, k []byte) error {
			return b.Put(k, append(bytes.Clone(b.Get(k)), ' '))
		})},
		{"demand zero cpu", "invalid worker group spec", group(func(g *workerGroupRecord) { g.Demand.CPUMillis = 0 })},
		{"demand zero memory", "invalid worker group spec", group(func(g *workerGroupRecord) { g.Demand.MemoryBytes = 0 })},
		{"demand ranks", "invalid worker group spec", group(func(g *workerGroupRecord) { g.Demand.Ranks = 2 })},
		{"demand over limit", "invalid worker group spec", group(func(g *workerGroupRecord) { g.Demand.CPUMillis = workerDemandLimit.CPUMillis/3 + 1 })},
		{"spec digest", "invalid worker group spec", group(func(g *workerGroupRecord) { g.SpecDigest = strings.Repeat("0", 64) })},
		{"spec mode", "invalid worker group spec", group(func(g *workerGroupRecord) {
			g.Spec.Mode = backend.WorkerTCP
			g.SpecDigest = workerSpecDigest(g.Spec)
		})},
		{"reserved phase", "invalid worker group record", group(func(g *workerGroupRecord) { g.Reserved = false })},
		{"phase", "invalid worker group record", group(func(g *workerGroupRecord) { g.Phase = WorkerGroupDone + 1 })},
		{"zero revision", "invalid worker group record", group(func(g *workerGroupRecord) { g.Revision = 0 })},
		{"created zone", "invalid worker group record", group(func(g *workerGroupRecord) { g.CreatedAt = g.CreatedAt.In(zone) })},
		{"authority", "invalid worker group identity", group(func(g *workerGroupRecord) { g.Authority.Principal = "bob" })},
		{"principal", "worker group principal mismatch", group(func(g *workerGroupRecord) { g.Principal, g.Authority.Principal = "bob", "bob" })},
		{"request", "worker group retained metadata mismatch", group(func(g *workerGroupRecord) { g.RequestID = "other" })},
		{"epoch", "worker group retained metadata mismatch", group(func(g *workerGroupRecord) { g.Domain.Epoch++ })},
		{"task digest", "invalid worker group task", group(func(g *workerGroupRecord) { g.TaskDigest = strings.Repeat("0", 64) })},
		{"done unsettled", "done worker group has an unsettled member", group(func(g *workerGroupRecord) { g.Phase, g.Reserved = WorkerGroupDone, false })},
		{"done unknown create", "done worker group has an unknown create", func(_, d WorkerGroup) func(*bolt.Tx) error {
			return workerRemovedCreate(d.Job.ID, 0, WorkerUnknown)
		}},
		{"done unknown create last rank", "done worker group has an unknown create", func(_, d WorkerGroup) func(*bolt.Tx) error {
			return workerRemovedCreate(d.Job.ID, 1, WorkerUnknown)
		}},
		{"queued with intents", "worker group intent before activation", group(func(g *workerGroupRecord) { g.Phase, g.Reserved = WorkerGroupQueued, false })},
		{"held without unknown", "invalid held worker group", group(func(g *workerGroupRecord) { g.Phase = WorkerGroupHeld })},
		{"active without create", "invalid active worker group", func(_, d WorkerGroup) func(*bolt.Tx) error {
			return workerEditGroup(d.Job.ID, func(g *workerGroupRecord) { g.Phase, g.Reserved = WorkerGroupActive, true })
		}},
		{"queued terminal", "invalid queued worker group status", func(_, d WorkerGroup) func(*bolt.Tx) error {
			return workerEditGroup(d.Job.ID, func(g *workerGroupRecord) { g.Phase = WorkerGroupQueued })
		}},
		{"group key", "", func(a, d WorkerGroup) func(*bolt.Tx) error {
			return func(tx *bolt.Tx) error {
				b := tx.Bucket(workerGroupsBucket)
				return b.Put(workerGroupKey(d.Job.ID), bytes.Clone(b.Get(workerGroupKey(a.Job.ID))))
			}
		}},
		{"member JSON", "invalid worker member record", raw(memberKey, func(b *bolt.Bucket, k []byte) error { return b.Put(k, []byte("{")) })},
		{"member encoding", "invalid worker member record encoding", raw(memberKey, func(b *bolt.Bucket, k []byte) error {
			return b.Put(k, append(bytes.Clone(b.Get(k)), ' '))
		})},
		{"member key", "worker member key mismatch", func(a, _ WorkerGroup) func(*bolt.Tx) error {
			return func(tx *bolt.Tx) error {
				b := tx.Bucket(workerGroupsBucket)
				return b.Put(workerMemberKey(a.Job.ID, 2), bytes.Clone(b.Get(workerMemberKey(a.Job.ID, 0))))
			}
		}},
		{"orphan member", "orphan worker member", func(_, _ WorkerGroup) func(*bolt.Tx) error {
			return func(tx *bolt.Tx) error {
				data, err := encodeWorkerMemberRecord(workerMemberRecord{Job: other})
				if err != nil {
					return err
				}
				return tx.Bucket(workerGroupsBucket).Put(workerMemberKey(other, 0), data)
			}
		}},
		{"missing member", "missing worker member", raw(func(a, _ WorkerGroup) []byte { return workerMemberKey(a.Job.ID, 2) }, func(b *bolt.Bucket, k []byte) error { return b.Delete(k) })},
		{"extra member", "worker member rank out of range", func(a, _ WorkerGroup) func(*bolt.Tx) error {
			return func(tx *bolt.Tx) error {
				data, err := encodeWorkerMemberRecord(workerMemberRecord{Job: a.Job.ID, Rank: 3})
				if err != nil {
					return err
				}
				return tx.Bucket(workerGroupsBucket).Put(workerMemberKey(a.Job.ID, 3), data)
			}
		}},
		{"unknown row", "unknown worker group row", raw(groupKey, func(b *bolt.Bucket, _ []byte) error { return b.Put([]byte("x/1"), []byte("1")) })},
		{"nested bucket", "unexpected worker group nested bucket", raw(groupKey, func(b *bolt.Bucket, _ []byte) error {
			_, err := b.CreateBucket([]byte("sessions"))
			return err
		})},
		{"sequence", "worker group count exceeds sequence", raw(groupKey, func(b *bolt.Bucket, _ []byte) error { return b.SetSequence(1) })},
		{"zero generation", "invalid worker effect generation", member(0, func(m *workerMemberRecord) { m.Create.Gen = strings.Repeat("0", 32) })},
		{"upper generation", "invalid worker effect generation", member(0, func(m *workerMemberRecord) { m.Create.Gen = strings.Repeat("A", 32) })},
		{"short generation", "invalid worker effect generation", member(0, func(m *workerMemberRecord) { m.Create.Gen = m.Create.Gen[:30] })},
		{"missing generation", "invalid worker effect state", member(0, func(m *workerMemberRecord) { m.Create.Gen = "" })},
		{"missing time", "invalid worker effect state", member(0, func(m *workerMemberRecord) { m.Release.At = time.Time{} })},
		{"effect zone", "invalid worker effect state", member(0, func(m *workerMemberRecord) { m.Release.At = m.Release.At.In(zone) })},
		{"effect state", "invalid worker effect state", member(0, func(m *workerMemberRecord) { m.Release.State = WorkerUnknown + 1 })},
		{"sequence above counter", "invalid worker effect sequence", member(0, func(m *workerMemberRecord) { m.Counter = 1 })},
		{"repeated sequence", "invalid worker effect sequence", member(0, func(m *workerMemberRecord) { m.Release.Seq = m.Create.Seq })},
		{"counter without effects", "invalid worker member counter", func(_, d WorkerGroup) func(*bolt.Tx) error {
			return workerEditMember(d.Job.ID, 0, func(m *workerMemberRecord) { m.Counter = 1 })
		}},
		{"recorded error", "invalid worker effect error", member(0, func(m *workerMemberRecord) { m.Release.Error = "x" })},
		{"unsanitized error", "invalid worker effect error", member(0, func(m *workerMemberRecord) { m.Create.Error = "a b" })},
		{"cancel acknowledgement", "invalid worker acknowledgement", member(0, func(m *workerMemberRecord) { m.CancelAck = true })},
		{"release revision", "invalid worker acknowledgement", member(2, func(m *workerMemberRecord) { m.Revision = "1" })},
		{"release acknowledgement", "invalid worker acknowledgement", member(2, func(m *workerMemberRecord) { m.ReleaseAck = true })},
		{"removed", "invalid worker removal", member(2, func(m *workerMemberRecord) { m.Removed = true })},
		{"recorded without pod", "invalid worker pod", member(2, func(m *workerMemberRecord) { m.Pod = workerPodRecord{} })},
		{"pod without create", "invalid worker pod", func(_, d WorkerGroup) func(*bolt.Tx) error {
			return workerEditMember(d.Job.ID, 0, func(m *workerMemberRecord) { m.Pod = workerPodRecord{"rank-0", "uid"} })
		}},
		{"pod identity", "invalid worker pod identity", member(2, func(m *workerMemberRecord) { m.Pod.Name = "Rank" })},
		{"release before create", "worker release before every create", member(2, func(m *workerMemberRecord) {
			m.Create.State, m.Pod = WorkerIntended, workerPodRecord{}
		})},
		{"cancel without pod", "invalid worker effect order", member(1, func(m *workerMemberRecord) {
			m.Create.State, m.Create.Error, m.Pod = WorkerUnknown, "lost", workerPodRecord{}
		})},
		{"observed exit code running", "invalid worker running observation", member(0, func(m *workerMemberRecord) {
			code := int32(0)
			m.Observed.ExitCode = &code
		})},
		{"observed zone", "invalid worker observation", member(0, func(m *workerMemberRecord) { m.Observed.At = m.Observed.At.In(zone) })},
		{"observed node", "invalid worker observation", member(0, func(m *workerMemberRecord) { m.Observed.Node = "a b" })},
		{"observed unknown", "invalid worker unknown observation", member(0, func(m *workerMemberRecord) {
			m.Observed.State, m.Observed.Started = backend.Unknown, time.Time{}
		})},
		{"observed state", "invalid worker observation state", member(0, func(m *workerMemberRecord) { m.Observed.State = backend.Retrying })},
		{"observed without pod", "worker observation without pod", func(_, d WorkerGroup) func(*bolt.Tx) error {
			return workerEditMember(d.Job.ID, 0, func(m *workerMemberRecord) {
				m.Observed = &workerObservedRecord{State: backend.Queued, At: workerT0}
			})
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir, active, done := workerCorruptible(t)
			snapshots := t.TempDir()
			if err := Backup(dir, filepath.Join(snapshots, "jobs.db")); err != nil {
				t.Fatal(err)
			}
			editFixture(t, dir, false, tt.edit(active, done))
			environmentRefused(t, dir, tt.want)
			editFixture(t, snapshots, false, tt.edit(active, done))
			want := tt.want
			if want == "" {
				want = "validate restore snapshot"
			}
			workersRestoreRefused(t, filepath.Join(snapshots, "jobs.db"), want)
		})
	}
}

// TestWorkerGroupRecordSizes encodes the largest valid group and member
// rows: every bounded text at its limit, worst-case JSON escaping in the
// opaque identities, and the widest numbers, states and times.
func TestWorkerGroupRecordSizes(t *testing.T) {
	end := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	gen := workerGenString([16]byte{1})
	text := strings.Repeat("e", workerErrorLimit)
	opaque := strings.Repeat("<", 128)
	code := int32(0)
	observed := &workerObservedRecord{State: backend.Succeeded, Gated: true, Container: opaque, Started: end, ExitCode: &code, Finished: end, Node: strings.Repeat("n", workerNodeLimit), At: end}
	seq := func(i uint64) uint64 { return math.MaxUint64 - i }
	unknown := workerMemberRecord{
		Job:      strings.Repeat("f", 32),
		Rank:     backend.MaxWorkers - 1,
		Counter:  math.MaxUint64,
		Create:   workerEffectRecord{WorkerRecorded, seq(3), gen, end, text},
		Release:  workerEffectRecord{WorkerUnknown, seq(2), gen, end, text},
		Cancel:   workerEffectRecord{WorkerUnknown, seq(1), gen, end, text},
		Remove:   workerEffectRecord{WorkerUnknown, seq(0), gen, end, text},
		Pod:      workerPodRecord{strings.Repeat("p", 63), opaque},
		Observed: observed,
	}
	acked := unknown
	acked.Release = workerEffectRecord{WorkerRecorded, seq(2), gen, end, ""}
	acked.Cancel = workerEffectRecord{WorkerRecorded, seq(1), gen, end, ""}
	acked.Remove = workerEffectRecord{WorkerRecorded, seq(0), gen, end, ""}
	acked.ReleaseAck, acked.Revision, acked.CancelAck, acked.Removed = true, strings.Repeat("r", workerRevisionLimit), true, true
	for name, m := range map[string]workerMemberRecord{"unknown": unknown, "acknowledged": acked} {
		data, err := encodeWorkerMemberRecord(m)
		if err != nil {
			t.Fatalf("%s member: %v", name, err)
		}
		if _, err := decodeWorkerMemberRecord(data); err != nil {
			t.Fatalf("%s member round trip: %v", name, err)
		}
		t.Logf("%s member: %d of %d bytes", name, len(data), workerMemberLimit)
	}
	spec := backend.WorkerSpec{Count: backend.MaxWorkers, Mode: backend.WorkerIndependent}
	principal := "p" + strings.Repeat("9", 63)
	var digest [32]byte
	for i := range digest {
		digest[i] = 255
	}
	g := workerGroupRecord{
		Job:        strings.Repeat("f", 32),
		Principal:  principal,
		Domain:     RequestDomain{Lineage: strings.Repeat("f", 32), Epoch: math.MaxUint64},
		RequestID:  strings.Repeat("R", 128),
		Authority:  auth.Delegation{Principal: principal, TokenID: strings.Repeat("f", 32), Digest: digest},
		TaskDigest: strings.Repeat("f", 64),
		Spec:       spec,
		SpecDigest: workerSpecDigest(spec),
		Demand:     workeradmission.Demand{Ranks: backend.MaxWorkers, CPUMillis: workerDemandLimit.CPUMillis / backend.MaxWorkers, MemoryBytes: workerDemandLimit.MemoryBytes / backend.MaxWorkers, Accelerators: math.MaxUint32},
		Phase:      WorkerGroupDone,
		Revoked:    true,
		Revision:   math.MaxUint64,
		CreatedAt:  end,
	}
	data, err := encodeWorkerGroupRecord(g)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeWorkerGroupRecord(data); err != nil {
		t.Fatal(err)
	}
	t.Logf("group: %d of %d bytes", len(data), workerGroupLimit)
	if workerGroupReserve(backend.MaxWorkers) != workerGroupLimit+backend.MaxWorkers*workerMemberLimit {
		t.Fatal("reserve does not cover the row bounds")
	}
}
