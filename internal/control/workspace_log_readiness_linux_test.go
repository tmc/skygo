//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

const workspaceProgressHeld = "workspace progress observation invalid; execution held"

// logReadiness drives the real Control loop, Store and original Local sequencer
// by hand. Custody is the existing retirement fixture frame: a private manual
// namespace, positive joins before close, and HOLD retention on any failure.
// Until physical maintenance runs, the real CheckGrowth refuses materialization.
type logReadiness struct {
	c        *Control
	b        *local.Backend
	s        *store.Store
	root     *os.Root
	state    *loopState
	m        *maintenanceState
	ctx      context.Context
	log      string
	gate     string
	setups   string
	mains    string
	accepted backend.Job
	physical bool
}

func newLogReadiness(t *testing.T) *logReadiness {
	t.Helper()
	f := newRetirementFixtureFrame(t)
	acquire := func(err error) {
		t.Helper()
		if err != nil {
			f.acquisition = err
			t.Fatal(err)
		}
	}
	name := filepath.Join(f.path, "artifacts")
	directory := filepath.Join(f.path, "store")
	logs := filepath.Join(f.path, "local")
	for _, name := range []string{name, directory, logs} {
		acquire(os.Mkdir(name, 0700))
	}
	var err error
	f.root, err = os.OpenRoot(name)
	acquire(err)
	info, err := f.root.Lstat(".")
	acquire(err)
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		acquire(errors.New("missing actual root identity"))
	}
	identity := artifact.FileID{Device: uint64(stat.Dev), Inode: uint64(stat.Ino)}
	facts, err := artifact.Initialize(f.root, identity, strings.Repeat("a", 32))
	acquire(err)
	f.files, err = artifact.Open(f.root, identity)
	acquire(err)
	_, err = store.InstallArtifacts(directory, store.ArtifactInstallation{Enabled: true, ID: facts.ID, Unit: facts.Unit, Volume: facts.Volume})
	acquire(err)
	f.store, err = store.Open(directory)
	acquire(err)
	f.local, err = local.New(logs)
	acquire(err)
	binding, err := backend.LocalBinding(f.store.Owner())
	acquire(err)
	target := backend.Target{Name: "local", Binding: binding, Backend: f.local, OutputStorage: backend.RemoteOutput}
	// Literal identity tests exact forwarding/CAS, NOT self capture or death.
	producer := local.ServerLifetime{Host: strings.Repeat("1", 32), Boot: strings.Repeat("2", 32), NamespaceInode: 9, PID: 42}
	f.control, err = NewWithConfig(f.store, []backend.Target{target}, testEngine, Config{ServerLifetime: producer, Artifacts: f.files, LocalBackend: f.local, LocalWorkspace: &LocalWorkspaceConfig{Target: target.Name, Binding: binding, Backend: f.local, Roots: f.root}})
	acquire(err)
	c := f.control
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	f.cancel = cancel
	f.state = c.newLoop(ctx)
	f.maintenance = newMaintenance(ctx)
	c.run = &runFrame{state: f.state, maintenance: f.maintenance, done: make(chan struct{})}
	if !c.WorkspaceTargetEnabled(target.Name, binding) {
		t.Fatal("HOLD actual Local ownership prerequisites unavailable")
	}
	r := &logReadiness{c: c, b: f.local, s: f.store, root: f.root, state: f.state, m: f.maintenance, ctx: ctx,
		gate: filepath.Join(f.path, "release-setup"), setups: filepath.Join(f.path, "setup-count"), mains: filepath.Join(f.path, "main-count")}
	// Registered before any later Fatal and run before frame close: the sole
	// setup process exits once the gate exists. Joins remain the frame's.
	t.Cleanup(func() {
		if err := os.WriteFile(r.gate, nil, 0600); err != nil {
			t.Error(err)
		}
	})
	input := actualWorkspaceInput(t, c)
	setup := fmt.Sprintf("printf x >> '%s'; printf setup-bytes; while [ ! -e '%s' ]; do sleep 0.05; done", r.setups, r.gate)
	main := fmt.Sprintf("printf y >> '%s'; printf main-bytes; printf result > result.txt", r.mains)
	task := backend.Task{Setup: []string{"/bin/sh", "-c", setup}, Argv: []string{"/bin/sh", "-c", main}, Workdir: &input, Outputs: []string{"result.txt"}, TimeoutSeconds: 25}
	r.accepted, err = c.SubmitWorkspace(testActor(), "log-readiness", task)
	if err != nil {
		t.Fatal(err)
	}
	r.log = filepath.Join(logs, r.accepted.CurrentAttemptID()+".log")
	return r
}

// step performs one loop iteration and reports whether a target result was
// integrated. Physical maintenance is dispatched only after release.
func (r *logReadiness) step(t *testing.T) bool {
	t.Helper()
	if err := r.c.schedule(r.ctx, r.state); err != nil {
		t.Fatal(err)
	}
	if r.physical && r.m.pending == nil {
		if err := r.c.dispatchPhysical(r.m); err != nil {
			t.Fatal(err)
		}
	}
	timer := time.NewTimer(2 * time.Millisecond)
	defer timer.Stop()
	select {
	case result := <-r.m.results:
		if err := r.c.receiveMaintenance(r.ctx, r.m, result); err != nil {
			t.Fatal(err)
		}
	case result := <-r.state.results:
		if err := r.c.receiveOperation(r.ctx, r.state, result); err != nil {
			t.Fatal(err)
		}
		return true
	case <-timer.C:
	case <-r.ctx.Done():
		t.Fatal("HOLD original workspace did not settle before observation deadline")
	}
	return false
}

func (r *logReadiness) current(t *testing.T) backend.Job {
	t.Helper()
	j, err := r.s.Get(r.accepted.ID)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func (r *logReadiness) until(t *testing.T, cond func(backend.Job) bool) backend.Job {
	t.Helper()
	for {
		if j := r.current(t); cond(j) {
			return j
		}
		r.step(t)
	}
}

// received steps until a target result is integrated, leaving no pending op.
func (r *logReadiness) received(t *testing.T) {
	t.Helper()
	for !r.step(t) {
	}
	if r.state.workers[0].pending != nil {
		t.Fatal("integrated result left a pending operation")
	}
}

func (r *logReadiness) count(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(name)
	if errors.Is(err, fs.ErrNotExist) {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestWorkspaceLogReadinessMilestones(t *testing.T) {
	for _, mode := range []string{"complete", "cancel"} {
		t.Run(mode, func(t *testing.T) { workspaceLogReadiness(t, mode) })
	}
}

func workspaceLogReadiness(t *testing.T, mode string) {
	r := newLogReadiness(t)
	c, actor := r.c, testActor()
	pinned := r.until(t, func(j backend.Job) bool { return currentStatus(j).Instance != "" })
	id, attempt, instance, attempts := pinned.ID, pinned.CurrentAttemptID(), currentStatus(pinned).Instance, len(pinned.Attempts)
	ref := reference(pinned)
	same := func(j backend.Job, state backend.State) {
		t.Helper()
		if j.Status.State != state || currentStatus(j).Instance != instance || j.CurrentAttemptID() != attempt || len(j.Attempts) != attempts || j.CancelRequested {
			t.Fatalf("original frame changed: want %s got %+v", state, j.Status)
		}
	}
	for {
		o, err := r.b.WorkspaceObservation(ref)
		if err != nil {
			t.Fatal(err)
		}
		if o.Phase == "waiting-materialization" {
			break
		}
		r.step(t)
	}
	// Two more original observe results integrate while admission is held.
	r.received(t)
	r.received(t)
	j := r.current(t)
	same(j, backend.Starting)
	if status, err := r.b.Observe(r.ctx, ref); err != nil || status.State != backend.Starting || status.Instance != instance {
		t.Fatalf("held Local=%+v %v", status, err)
	}
	if _, err := os.Lstat(r.log); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("output before admission: %v", err)
	}
	if _, err := r.root.Lstat("workspace-" + attempt); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("root before admission: %v", err)
	}
	if r.count(t, r.setups) != "" {
		t.Fatal("setup ran before materialization")
	}
	if chunk, err := c.readLogs(r.ctx, actor, id, attempt, 0); err != nil || len(chunk.Data) != 0 || chunk.Complete {
		t.Fatalf("pending STARTING read=%+v %v", chunk, err)
	}
	if _, err := c.readLogs(r.ctx, actor, id, attempt, 1); !errors.Is(err, ErrLogsUnavailable) {
		t.Fatalf("nonzero STARTING offset=%v", err)
	}
	// Stale frames never persist a milestone, even a well-formed Running one.
	running := local.WorkspaceObservation{Phase: "setup", Status: backend.Status{State: backend.Running, Instance: instance}}
	stale := map[string]func(*backend.Job){
		"attempt": func(s *backend.Job) {
			s.Attempts = append(s.Attempts, backend.Attempt{ID: strings.Repeat("e", 32), Status: currentStatus(*s)})
		},
		"request": func(s *backend.Job) { s.Task.Argv = []string{"/bin/true"} },
		"target":  func(s *backend.Job) { s.TargetBinding = "other-binding" },
		"instance": func(s *backend.Job) {
			s.Status.Instance = strings.Repeat("f", 32)
			setAttemptInstance(s, s.Status.Instance)
		},
	}
	base := r.current(t)
	for name, change := range stale {
		frame := cloneJob(j)
		change(&frame)
		observation := running
		if err := c.integrateWorkspace(r.state, &r.state.workers[0], operationResult{operation: operation{kind: observeOperation, job: frame}, workspace: &observation}); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if after := r.current(t); after.Status.Reason != j.Status.Reason {
			t.Fatalf("%s stale frame changed reason %q", name, after.Status.Reason)
		} else if !after.UpdatedAt.Equal(base.UpdatedAt) {
			t.Fatalf("%s stale frame wrote Store", name)
		} else {
			same(after, backend.Starting)
		}
	}
	if err := r.b.Start(r.ctx, attempt, pinned.Task); err == nil {
		t.Fatal("second Start accepted")
	}

	r.physical = true
	j = r.until(t, func(j backend.Job) bool {
		if j.Status.State.Terminal() {
			t.Fatalf("terminal before Running milestone: %+v", j.Status)
		}
		return j.Status.State == backend.Running
	})
	same(j, backend.Running)
	if info, err := os.Lstat(r.log); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("Running without actual output: %v", err)
	}
	if status, err := r.b.Observe(r.ctx, ref); err != nil || status.State != backend.Running || status.Instance != instance {
		t.Fatalf("Running Local=%+v %v", status, err)
	}
	var chunk backend.LogChunk
	for !strings.Contains(string(chunk.Data), "setup-bytes") {
		var err error
		if chunk, err = c.readLogs(r.ctx, actor, id, attempt, 0); err != nil {
			t.Fatal(err)
		}
		r.step(t)
	}
	if string(chunk.Data) != "setup-bytes" || chunk.Complete {
		t.Fatalf("setup log=%+v", chunk)
	}
	// Missing RUNNING output is refused, never reclassified as pending.
	if err := os.Rename(r.log, r.log+".aside"); err != nil {
		t.Fatal(err)
	}
	_, missing := c.readLogs(r.ctx, actor, id, attempt, 0)
	if err := os.Rename(r.log+".aside", r.log); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(missing, ErrLogsUnavailable) {
		t.Fatalf("missing RUNNING output=%v", missing)
	}
	changed := ref
	changed.Instance = strings.Repeat("f", 32)
	if _, err := r.b.Logs(r.ctx, changed, 0, 1024); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("changed Instance read=%v", err)
	}
	// Client interruption is not cancellation.
	interrupted, stop := context.WithCancel(r.ctx)
	stop()
	if _, err := c.readLogs(interrupted, actor, id, attempt, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted read=%v", err)
	}
	same(r.current(t), backend.Running)
	// Repeated same-Instance Running performs no Store write.
	before := r.current(t)
	if err := c.workspaceProgress(before, running); err != nil {
		t.Fatal(err)
	}
	if after := r.current(t); !after.UpdatedAt.Equal(before.UpdatedAt) {
		t.Fatal("repeated milestone rewrote Store")
	}
	code := int32(3)
	invalid := map[string]local.WorkspaceObservation{
		"downgrade":          {Status: backend.Status{State: backend.Starting, Instance: instance}},
		"foreign":            {Status: backend.Status{State: backend.Running, Instance: strings.Repeat("f", 32)}},
		"missing":            {Status: backend.Status{State: backend.Running}},
		"terminal":           {Status: backend.Status{State: backend.Succeeded, Instance: instance}},
		"unknown":            {Status: backend.Status{State: backend.Unknown, Instance: instance}},
		"writer":             {Status: backend.Status{State: backend.Running, Instance: instance}, Writer: &local.WorkspaceWriterClosure{}},
		"outcome":            {Status: backend.Status{State: backend.Running, Instance: instance}, Outcome: local.WorkspaceTaskExit},
		"malformedExitCode":  {Status: backend.Status{State: backend.Running, Instance: instance, ExitCode: &code}},
		"stoppedTermination": {Status: backend.Status{State: backend.Running, Instance: instance, Termination: backend.StoppedWithExitCode}},
		// An unrecognized Termination value, not UnknownTermination.
		"unknownTermination": {Status: backend.Status{State: backend.Running, Instance: instance, Termination: backend.StoppedWithExitCode + 1}},
		"stoppedExitCode":    {Status: backend.Status{State: backend.Running, Instance: instance, ExitCode: &code, Termination: backend.StoppedWithExitCode}},
	}
	for name, o := range invalid {
		// Each case starts from its own diagnostic baseline on the SAME original
		// job: only Reason is reset, never ownership, Instance, cancel, status
		// state or attempts. A case that is not rejected leaves the sentinel.
		sentinel := "log-readiness baseline " + name
		if _, err := r.s.Update(id, func(s *backend.Job) error {
			s.Status.Reason = sentinel
			return nil
		}); err != nil {
			t.Fatalf("%s baseline: %v", name, err)
		}
		before := r.current(t)
		same(before, backend.Running)
		if before.Status.Reason != sentinel {
			t.Fatalf("%s baseline reason %q", name, before.Status.Reason)
		}
		// The valid same-Instance Running twin is accepted without a Store write.
		if err := c.workspaceProgress(before, running); err != nil {
			t.Fatalf("%s twin: %v", name, err)
		}
		if twin := r.current(t); !twin.UpdatedAt.Equal(before.UpdatedAt) || twin.Status.Reason != sentinel {
			t.Fatalf("%s twin changed Store: %q", name, twin.Status.Reason)
		}
		if err := c.workspaceProgress(before, o); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		after := r.current(t)
		same(after, backend.Running)
		if after.Status.Reason != workspaceProgressHeld {
			t.Fatalf("%s not held: %q", name, after.Status.Reason)
		}
	}

	if mode == "complete" {
		if err := os.WriteFile(r.gate, nil, 0600); err != nil {
			t.Fatal(err)
		}
	} else if _, err := c.Cancel(actor, id); err != nil {
		t.Fatal(err)
	}
	for {
		if p := r.state.workers[0].completion; p != nil && p.acked {
			break
		}
		r.step(t)
	}
	p := r.state.workers[0].completion
	terminal := r.current(t)
	if currentStatus(terminal).Instance != instance || terminal.CurrentAttemptID() != attempt || len(terminal.Attempts) != attempts {
		t.Fatalf("terminal frame changed: %+v", terminal.Status)
	}
	if p.observation.Writer == nil || p.observation.Writer.Execution.Instance != instance || !p.durable {
		t.Fatalf("completion without original writer: %+v", p.observation)
	}
	chunk, err := c.readLogs(r.ctx, actor, id, attempt, 0)
	if err != nil {
		t.Fatal(err)
	}
	switch mode {
	case "complete":
		if terminal.Status.State != backend.Succeeded || p.observation.Outcome != local.WorkspaceTaskExit {
			t.Fatalf("completion=%+v outcome=%d", terminal.Status, p.observation.Outcome)
		}
		if string(chunk.Data) != "setup-bytesmain-bytes" || r.count(t, r.mains) != "y" {
			t.Fatalf("completed log=%q main=%q", chunk.Data, r.count(t, r.mains))
		}
		if result, err := c.ArtifactResultFor(actor, id, attempt); err != nil || result.State != "available" {
			t.Fatalf("result=%+v %v", result, err)
		}
	case "cancel":
		if terminal.Status.State != backend.Canceled || p.observation.Outcome != local.WorkspaceCanceled {
			t.Fatalf("cancel=%+v outcome=%d", terminal.Status, p.observation.Outcome)
		}
		if string(chunk.Data) != "setup-bytes" || r.count(t, r.mains) != "" {
			t.Fatalf("canceled log=%q main=%q", chunk.Data, r.count(t, r.mains))
		}
	}
	if r.count(t, r.setups) != "x" {
		t.Fatalf("setup count=%q", r.count(t, r.setups))
	}
	// Retire the SAME original frame so fixture close positively joins.
	for r.m.pending != nil {
		r.step(t)
	}
	for r.state.workers[0].completion != nil {
		if r.m.pending == nil {
			p.retirement.due = time.Time{}
			admitted, err := c.dispatchWorkspaceRetirement(r.m, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if admitted && r.m.pending != nil {
				if err := c.receiveMaintenance(r.ctx, r.m, waitValue(t, r.m.results)); err != nil {
					t.Fatal(err)
				}
				continue
			}
		}
		r.step(t) // bounded by the original fixture deadline
	}
	r.physical = false
	for r.m.pending != nil {
		r.step(t)
	}
	if _, err := r.root.Lstat("workspace-" + attempt); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("original root remains: %v", err)
	}
}

func setAttemptInstance(j *backend.Job, instance string) {
	if n := len(j.Attempts); n != 0 {
		j.Attempts[n-1].Status.Instance = instance
	}
}
