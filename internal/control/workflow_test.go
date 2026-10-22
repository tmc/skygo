//go:build darwin || linux

package control

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
	"github.com/tmc/skygo/internal/workflow"
)

// workflowBackend is a MODELED ordinary target: Check passes, Start is
// accepted and Observe reports success. It qualifies no native backend.
type workflowBackend struct {
	checks, starts atomic.Int32
}

func (b *workflowBackend) Check(backend.Task) error { b.checks.Add(1); return nil }
func (b *workflowBackend) Start(context.Context, string, backend.Task) error {
	b.starts.Add(1)
	return nil
}
func (b *workflowBackend) Observe(context.Context, backend.Reference) (backend.Status, error) {
	return backend.Status{State: backend.Succeeded}, nil
}
func (b *workflowBackend) Cancel(context.Context, backend.Reference) error { return nil }
func (b *workflowBackend) Logs(context.Context, backend.Reference, int64, int) (backend.LogChunk, error) {
	return backend.LogChunk{}, nil
}

// newWorkflowFixture is the data fixture's actual Store, Files, root and
// file authority with two configured routes: the modeled data target and an
// ordinary "plain" target. No Run is started.
func newWorkflowFixture(t *testing.T, retained bool) (*submissionDataFixture, *workflowBackend) {
	t.Helper()
	f := newSubmissionDataFixture(t, retained, true)
	plain := &workflowBackend{}
	targets := []backend.Target{f.target(), {Name: "plain", Binding: "plain-binding", Kind: "modeled", Backend: plain, OutputStorage: backend.RemoteOutput}}
	c, err := NewWithConfig(f.store, targets, f.engine, Config{Artifacts: f.files})
	if err != nil {
		f.hold(err)
		t.Fatal(err)
	}
	f.control = c
	f.sample(t)
	return f, plain
}

func plainGraph(names ...string) workflow.Graph {
	var g workflow.Graph
	for _, name := range names {
		g.Nodes = append(g.Nodes, workflow.Node{Name: name, Task: backend.Task{Argv: []string{"work", name}, Target: "plain"}})
	}
	return g
}

func (f *submissionDataFixture) submitWorkflow(t *testing.T, request string, g workflow.Graph) store.Workflow {
	t.Helper()
	w, err := f.control.SubmitWorkflow(f.actor, f.domain, request, g)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// workflowCandidates collects n distinct candidates from Store's bounded
// selector, which returns at most four per call.
func workflowCandidates(t *testing.T, s *store.Store, n int) []store.WorkflowCandidate {
	t.Helper()
	var out []store.WorkflowCandidate
	seen := make(map[workflowKey]bool)
	for i := 0; i < 2*n+4 && len(out) < n; i++ {
		candidates, err := s.WorkflowCandidates()
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range candidates {
			if k := (workflowKey{c.Workflow, c.Node}); !seen[k] {
				seen[k] = true
				out = append(out, c)
			}
		}
	}
	if len(out) != n {
		t.Fatalf("selector returned %d candidates, want %d", len(out), n)
	}
	return out
}

func countJobs(t *testing.T, s *store.Store) int {
	t.Helper()
	return len(storeJobs(t, s, 100))
}

// storeJobs returns up to limit global jobs through the actual current
// sequence high-water mark; List's max bound is inclusive, never a default.
func storeJobs(t *testing.T, s *store.Store, limit int) []backend.Job {
	t.Helper()
	last, err := s.LastSequence()
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := s.List(0, last, limit)
	if err != nil {
		t.Fatal(err)
	}
	return jobs
}

func workflowTestRef() *artifact.Ref {
	return &artifact.Ref{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Size: 1}
}

func TestWorkflowGraphValidation(t *testing.T) {
	producer := workflow.Node{Name: "build", Task: backend.Task{Argv: []string{"make"}, Workdir: workflowTestRef(), Outputs: []string{"out"}}}
	consumer := func(task backend.Task) workflow.Graph {
		return workflow.Graph{Nodes: []workflow.Node{producer, {Name: "test", Task: task, DependsOn: []string{"build"}, Input: &workflow.Input{Node: "build", Slot: "result"}}}}
	}
	tests := []struct {
		name string
		g    workflow.Graph
		ok   bool
	}{
		{"template", consumer(backend.Task{Argv: []string{"test"}, Setup: []string{"prepare"}, Outputs: []string{"report"}}), true},
		{"template argv", consumer(backend.Task{Setup: []string{"prepare"}}), false},
		{"template env", consumer(backend.Task{Argv: []string{"test"}, Env: map[string]string{"1BAD": "x"}}), false},
		{"template resources", consumer(backend.Task{Argv: []string{"test"}, CPUMillis: -1}), false},
		{"template setup", consumer(backend.Task{Argv: []string{"test"}, Setup: []string{""}}), false},
		{"template directory", consumer(backend.Task{Argv: []string{"test"}, Directory: "/src"}), false},
		{"template retry", consumer(backend.Task{Argv: []string{"test"}, Retry: &backend.RetryPolicy{MaxAttempts: 11}}), false},
		{"template mount", consumer(backend.Task{Argv: []string{"test"}, Setup: []string{"prepare"}, Outputs: []string{"report"}, DataMounts: []backend.DataMount{{Reference: "cache", Destination: "/mnt/skygo/cache"}}}), true},
		{"template mount alias", consumer(backend.Task{Argv: []string{"test"}, DataMounts: []backend.DataMount{{Reference: "BAD!", Destination: "/mnt/skygo/cache"}}}), false},
		{"template mount destination", consumer(backend.Task{Argv: []string{"test"}, DataMounts: []backend.DataMount{{Reference: "cache", Destination: "relative"}}}), false},
		{"ordinary outputs without workdir", workflow.Graph{Nodes: []workflow.Node{{Name: "a", Task: backend.Task{Argv: []string{"a"}, Outputs: []string{"out"}}}}}, false},
		{"ordinary workdir", workflow.Graph{Nodes: []workflow.Node{producer}}, true},
		{"data", workflow.Graph{Nodes: []workflow.Node{{Name: "a", Task: submissionDataTask()}}}, true},
		{"cycle", workflow.Graph{Nodes: []workflow.Node{{Name: "a", Task: backend.Task{Argv: []string{"a"}}, DependsOn: []string{"a"}}}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before, err := workflow.Canonical(tt.g)
			err = validateGraph(tt.g)
			if tt.ok != (err == nil) || err != nil && !errors.Is(err, ErrInvalid) {
				t.Fatalf("validateGraph = %v, want ok %v", err, tt.ok)
			}
			if after, e := workflow.Canonical(tt.g); before != nil && (e != nil || !bytes.Equal(before, after)) {
				t.Fatal("validation changed the caller's graph")
			}
		})
	}
}

// A node with an input gets the portable data mount rules at acceptance, not
// only once its producer publishes. A refusal leaves no workflow, child or
// request binding, and the caller's graph bytes are unchanged.
func TestWorkflowTemplateDataMounts(t *testing.T) {
	f, _ := newWorkflowFixture(t, false)
	ref, _ := f.input(t)
	graph := func(mounts ...backend.DataMount) workflow.Graph {
		producer := workflow.Node{Name: "build", Task: backend.Task{Argv: []string{"make"}, Workdir: &ref, Outputs: []string{"out"}}}
		consumer := workflow.Node{Name: "test", Task: backend.Task{Argv: []string{"test"}, Setup: []string{"prepare"}, Outputs: []string{"report"}, DataMounts: mounts}, DependsOn: []string{"build"}, Input: &workflow.Input{Node: "build", Slot: "result"}}
		return workflow.Graph{Nodes: []workflow.Node{producer, consumer}}
	}
	var many []backend.DataMount
	for i := 0; i < 17; i++ {
		many = append(many, backend.DataMount{Reference: "cache", Destination: fmt.Sprintf("/mnt/skygo/d%d", i)})
	}
	for _, tt := range []struct {
		name   string
		mounts []backend.DataMount
	}{
		{"alias", []backend.DataMount{{Reference: "BAD!", Destination: "/mnt/skygo/cache"}}},
		{"destination", []backend.DataMount{{Reference: "cache", Destination: "relative"}}},
		{"overlap", []backend.DataMount{{Reference: "cache", Destination: "/mnt/skygo/cache"}, {Reference: "other", Destination: "/mnt/skygo/cache/child"}}},
		{"count", many},
	} {
		t.Run(tt.name, func(t *testing.T) {
			g := graph(tt.mounts...)
			before, err := workflow.Canonical(g)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = f.control.SubmitWorkflow(f.actor, f.domain, "mounts", g); !errors.Is(err, ErrInvalid) {
				t.Fatalf("malformed template mount accepted: %v", err)
			}
			if after, err := workflow.Canonical(g); err != nil || !bytes.Equal(before, after) {
				t.Fatal("validation changed the caller's graph")
			}
			ws, _, err := f.control.ListWorkflows(f.actor, 10, "")
			if err != nil || len(ws) != 0 || countJobs(t, f.store) != 0 {
				t.Fatalf("refusal left %d workflows: %v", len(ws), err)
			}
		})
	}
	// The same request then binds a valid unresolved template with a mount.
	w := f.submitWorkflow(t, "mounts", graph(backend.DataMount{Reference: "cache", Destination: "/mnt/skygo/cache"}))
	if len(w.Nodes) != 2 || w.Nodes[1].JobID != "" || len(w.Graph.Nodes[1].Task.DataMounts) != 1 {
		t.Fatalf("valid template mount: %#v", w.Nodes)
	}
}

func TestWorkflowValidationKeepsGraphBytes(t *testing.T) {
	// Validation normalizes a copy. The caller's retry policy, and so the
	// canonical graph bytes Store digests, stay as submitted.
	g := workflow.Graph{Nodes: []workflow.Node{{Name: "a", Task: backend.Task{Argv: []string{"a"}, Retry: &backend.RetryPolicy{MaxAttempts: 1}}}}}
	before, err := workflow.Canonical(g)
	if err != nil {
		t.Fatal(err)
	}
	if err = validateGraph(g); err != nil {
		t.Fatal(err)
	}
	after, err := workflow.Canonical(g)
	if err != nil || !bytes.Equal(before, after) || g.Nodes[0].Task.Retry == nil {
		t.Fatal("validation normalized the stored graph")
	}
}

func TestWorkflowReplayPrecedesTransientRefusal(t *testing.T) {
	f, _ := newWorkflowFixture(t, false)
	g := plainGraph("a", "b")
	first := f.submitWorkflow(t, "replay", g)
	f.control.failed.Store(true)
	same, err := f.control.SubmitWorkflow(f.actor, nil, "replay", g)
	if err != nil || same.ID != first.ID || len(same.Nodes) != 2 {
		t.Fatalf("exact replay refused by transient failure: %#v %v", same, err)
	}
	if _, err = f.control.SubmitWorkflow(f.actor, nil, "fresh", g); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("new parent accepted while failed: %v", err)
	}
	f.control.failed.Store(false)
	if _, err = f.control.SubmitWorkflow(f.actor, nil, "replay", plainGraph("a")); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("different graph replayed: %v", err)
	}
	if _, err = f.control.SubmitWorkflow(f.actor, nil, "replay", workflow.Graph{Nodes: []workflow.Node{{Name: "a"}}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid graph reached lookup: %v", err)
	}
	if _, err = f.control.SubmitWorkflow(f.actor, nil, "bad request", g); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid request id: %v", err)
	}
}

func TestWorkflowListCursor(t *testing.T) {
	f, _ := newWorkflowFixture(t, true)
	var ids []string
	for i := 0; i < 3; i++ {
		ids = append(ids, f.submitWorkflow(t, fmt.Sprint("list-", i), plainGraph("a")).ID)
	}
	var got []string
	token := ""
	for page := 0; page < 3; page++ {
		ws, next, err := f.control.ListWorkflows(f.actor, 1, token)
		if err != nil || len(ws) != 1 || (next == "") != (page == 2) {
			t.Fatalf("page %d: %d %q %v", page, len(ws), next, err)
		}
		got = append(got, ws[0].ID)
		if page == 0 {
			first := next
			t.Run("tampered", func(t *testing.T) { testWorkflowCursorScope(t, f, first) })
		}
		token = next
	}
	if strings.Join(got, ",") != strings.Join(ids, ",") {
		t.Fatalf("listing order %v, want %v", got, ids)
	}
	if _, _, err := f.control.ListWorkflows(f.actor, 201, ""); !errors.Is(err, ErrInvalid) {
		t.Fatalf("oversized page: %v", err)
	}
}

func testWorkflowCursorScope(t *testing.T, f *submissionDataFixture, token string) {
	cur, err := decodeWorkflowCursor(token)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = decodeRetentionCursor(token); !errors.Is(err, ErrInvalid) {
		t.Fatal("job listing accepted a workflow cursor")
	}
	if _, _, err = f.control.List(f.actor, 1, token); !errors.Is(err, ErrInvalid) {
		t.Fatalf("job List accepted a workflow cursor: %v", err)
	}
	tests := []struct {
		name   string
		change func(*retentionCursor)
	}{
		{"kind", func(c *retentionCursor) { c.Kind = "list" }},
		{"max", func(c *retentionCursor) { c.Max = c.After + 1 }},
		{"selector", func(c *retentionCursor) { c.Selector = "x" }},
		{"principal", func(c *retentionCursor) { c.Principal = "other" }},
		{"global", func(c *retentionCursor) { c.Global = !c.Global }},
		{"lineage", func(c *retentionCursor) { c.Lineage = strings.Repeat("1", 32) }},
		{"owner", func(c *retentionCursor) { c.Owner = strings.Repeat("2", 32) }},
		{"scoped", func(c *retentionCursor) { c.Scoped = !c.Scoped }},
		{"after", func(c *retentionCursor) { c.After = 0 }},
		{"version", func(c *retentionCursor) { c.Version = 2 }},
	}
	for _, tt := range tests {
		changed := cur
		tt.change(&changed)
		forged, err := encodeRetentionCursor(changed)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err = f.control.ListWorkflows(f.actor, 1, forged); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: forged cursor accepted: %v", tt.name, err)
		}
	}
}

func TestWorkflowReturnedValuesDoNotAlias(t *testing.T) {
	f, _ := newWorkflowFixture(t, false)
	g := plainGraph("a")
	w := f.submitWorkflow(t, "alias", g)
	g.Nodes[0].Task.Argv[0] = "changed-input"
	w.Graph.Nodes[0].Task.Argv[0] = "changed-output"
	got, err := f.control.GetWorkflow(f.actor, w.ID)
	if err != nil || got.Graph.Nodes[0].Task.Argv[0] != "work" {
		t.Fatalf("durable graph aliased caller memory: %#v %v", got.Graph, err)
	}
	if _, err = f.control.GetWorkflow(f.actor, "not-an-id"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid id: %v", err)
	}
	canceled, err := f.control.CancelWorkflow(f.actor, w.ID)
	if err != nil || !canceled.Canceled {
		t.Fatalf("cancel: %#v %v", canceled, err)
	}
}

func TestWorkflowAdmitsAtMostFourPerTurn(t *testing.T) {
	f, _ := newWorkflowFixture(t, false)
	for i := 0; i < 6; i++ {
		f.submitWorkflow(t, fmt.Sprint("turn-", i), plainGraph("a"))
	}
	s := &loopState{workflowHeld: make(map[workflowHold]time.Time)}
	var checks []*workflowCheck
	for _, candidate := range workflowCandidates(t, f.store, 6) {
		p := &workflowCheck{candidate: candidate}
		if err := f.control.planWorkflow(p); err != nil {
			t.Fatal(err)
		}
		if len(p.routes) != 1 || p.routes[0].target.Name != "plain" || p.routes[0].policy != nil {
			t.Fatalf("plain node routes: %#v", p.routes)
		}
		p.checked = true
		checks = append(checks, p)
	}
	s.workflows = append(s.workflows, checks...)
	if err := f.control.advanceWorkflows(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if n := countJobs(t, f.store); n != 4 || len(s.workflows) != 2 {
		t.Fatalf("first turn admitted %d, retained %d", n, len(s.workflows))
	}
	if err := f.control.advanceWorkflows(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if n := countJobs(t, f.store); n != 6 || len(s.workflows) != 0 {
		t.Fatalf("second turn admitted %d total, retained %d", n, len(s.workflows))
	}
	// A reselected candidate, as after restart, returns its bound child.
	if err := f.control.admitWorkflow(checks[0]); err != nil {
		t.Fatal(err)
	}
	if n := countJobs(t, f.store); n != 6 {
		t.Fatalf("repeated admission created a child: %d jobs", n)
	}
}

func TestWorkflowDataNodeAdmitsOnConfiguredRoute(t *testing.T) {
	f, plain := newWorkflowFixture(t, false)
	w := f.submitWorkflow(t, "data", workflow.Graph{Nodes: []workflow.Node{{Name: "train", Task: submissionDataTask()}}})
	p := &workflowCheck{candidate: workflowCandidates(t, f.store, 1)[0]}
	if err := f.control.planWorkflow(p); err != nil {
		t.Fatal(err)
	}
	route, err := f.control.dataTargetFor(backend.Job{Target: "data", TargetBinding: "modeled-profile-v1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.routes) != 1 || p.routes[0].target.Name != "data" || p.routes[0].policy == nil || *p.routes[0].policy != route.policy {
		t.Fatalf("data node routes: %#v", p.routes)
	}
	p.checked = true
	s := &loopState{workflows: []*workflowCheck{p}, workflowHeld: make(map[workflowHold]time.Time)}
	if err = f.control.advanceWorkflows(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	got, err := f.control.GetWorkflow(f.actor, w.ID)
	if err != nil || got.Nodes[0].JobID == "" {
		t.Fatalf("data node not admitted: %#v %v", got, err)
	}
	j, err := f.store.Get(got.Nodes[0].JobID)
	if err != nil || j.Target != "data" || j.TargetBinding != "modeled-profile-v1" {
		t.Fatalf("data child route: %#v %v", j, err)
	}
	row, err := f.store.GetRemoteAttempt(store.RemoteKey{JobID: j.ID, AttemptID: j.CurrentAttemptID()})
	if err != nil || row.Policy != route.policy {
		t.Fatalf("data child policy: %#v %v", row, err)
	}
	if f.backend.native.Load() != 0 || plain.starts.Load() != 0 {
		t.Fatal("admission started native work")
	}
}

func TestWorkflowLostAuthorityAdmitsNothing(t *testing.T) {
	f, _ := newWorkflowFixture(t, false)
	f.prepareRevocation(t)
	f.submitWorkflow(t, "revoked", plainGraph("a"))
	p := &workflowCheck{candidate: workflowCandidates(t, f.store, 1)[0]}
	if err := f.control.planWorkflow(p); err != nil {
		t.Fatal(err)
	}
	p.checked = true
	// Revoke after the route check passed, before admission.
	if err := auth.Revoke(f.configPath, f.actor.TokenID()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for f.engine.CheckDelegation(p.candidate.Authority) == nil {
		if time.Now().After(deadline) {
			t.Fatal("auth refresh did not observe revocation")
		}
		select {
		case <-f.engine.Changed():
		case <-time.After(100 * time.Millisecond):
		}
	}
	s := &loopState{workflows: []*workflowCheck{p}, workflowHeld: make(map[workflowHold]time.Time)}
	if err := f.control.advanceWorkflows(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if n := countJobs(t, f.store); n != 0 || len(s.workflows) != 0 {
		t.Fatalf("revoked authority admitted %d children", n)
	}
	if _, held := s.workflowHeld[p.hold()]; !held {
		t.Fatal("refused candidate not held")
	}
	q := &workflowCheck{candidate: p.candidate}
	if err := f.control.planWorkflow(q); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("planning with revoked authority: %v", err)
	}
}

func TestWorkflowCheckResultIdentity(t *testing.T) {
	f, _ := newWorkflowFixture(t, false)
	g := workflow.Graph{Nodes: []workflow.Node{{Name: "a", Task: backend.Task{Argv: []string{"a"}}}}}
	f.submitWorkflow(t, "identity", g)
	p := &workflowCheck{candidate: workflowCandidates(t, f.store, 1)[0]}
	if err := f.control.planWorkflow(p); err != nil {
		t.Fatal(err)
	}
	if len(p.routes) != 2 || p.routes[0].target.Name != "data" || p.routes[1].target.Name != "plain" {
		t.Fatalf("routes not in configured order: %#v", p.routes)
	}
	op := operation{kind: workflowCheckOperation, workflow: p}
	s := &loopState{workers: []targetWorker{{target: f.control.targets[0], pending: &op}, {target: f.control.targets[1]}}, workflows: []*workflowCheck{p}, workflowHeld: make(map[workflowHold]time.Time)}
	p.checking = true
	// An equal continuation that is not the original request does not match.
	copied := *p
	foreign := operationResult{operation: operation{kind: workflowCheckOperation, workflow: &copied}}
	if err := f.control.receiveOperation(context.Background(), s, foreign); !errors.Is(err, ErrUnavailable) || s.unmatched == nil || s.workers[0].pending != &op {
		t.Fatalf("foreign check result integrated: %v", err)
	}
	s.unmatched = nil
	// The original failing check settles its worker and moves to the next route.
	if err := f.control.receiveOperation(context.Background(), s, operationResult{operation: op, err: errors.New("modeled refusal")}); err != nil {
		t.Fatal(err)
	}
	if s.workers[0].pending != nil || s.workers[0].received != nil || p.checking || p.checked || p.routes[0].target.Name != "plain" {
		t.Fatalf("failed check did not continue: %#v", p)
	}
	// During preparation a passing check is dropped, never retained to admit.
	op = operation{kind: workflowCheckOperation, workflow: p}
	s.workers[1].pending = &op
	s.preparing = true
	if err := f.control.receiveOperation(context.Background(), s, operationResult{operation: op}); err != nil {
		t.Fatal(err)
	}
	if s.workers[1].pending != nil || p.checked || len(s.workflows) != 0 {
		t.Fatal("check result retained during shutdown preparation")
	}
}

func TestWorkflowHeldCheckYieldsToShutdown(t *testing.T) {
	f, plain := newWorkflowFixture(t, false)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	f.backend.check = func(task backend.Task) error {
		if len(task.DataMounts) != 0 {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
		}
		return backend.ValidateDataTask(task, nil)
	}
	job, err := f.control.Submit(f.actor, "plain", backend.Task{Argv: []string{"work"}, Target: "plain"})
	if err != nil {
		t.Fatal(err)
	}
	w := f.submitWorkflow(t, "held", workflow.Graph{Nodes: []workflow.Node{{Name: "train", Task: submissionDataTask()}}})
	cancel, done := beginRun(t, f.control)
	preparedBoundary := false
	releaseOnce := sync.OnceFunc(func() { close(release) })
	t.Cleanup(func() {
		releaseOnce()
		if preparedBoundary {
			drain, stop := context.WithTimeout(context.Background(), 5*time.Second)
			if err := f.control.DrainArtifacts(drain); err != nil {
				f.hold(err)
				t.Error(err)
			}
			stop()
		}
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			f.hold(errors.New("original workflow Run did not return"))
			t.Error("original Run remains held")
			return
		}
		ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if e := f.control.Join(ctx); e != nil {
			f.hold(e)
			t.Error(e)
		}
	})
	waitValue(t, entered)
	// The coordinator and the other target progress while Check is held.
	deadline := time.Now().Add(5 * time.Second)
	for {
		j, err := f.store.Get(job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if j.Status.State == backend.Succeeded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("other target held behind workflow Check: %s", j.Status.State)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if plain.starts.Load() != 1 {
		t.Fatal("other target did not start exactly once")
	}
	if err = f.control.BeginShutdown(); err != nil {
		t.Fatal(err)
	}
	blocked, stop := context.WithTimeout(context.Background(), 50*time.Millisecond)
	err = f.control.WaitShutdownPrepared(blocked)
	stop()
	if err == nil {
		t.Fatal("preparation forgot held original Check")
	}
	releaseOnce()
	prepared, end := context.WithTimeout(context.Background(), 5*time.Second)
	err = f.control.WaitShutdownPrepared(prepared)
	end()
	if err != nil {
		t.Fatal(err)
	}
	preparedBoundary = true
	got, err := f.control.GetWorkflow(f.actor, w.ID)
	if err != nil || got.Nodes[0].JobID != "" {
		t.Fatalf("shutdown admitted a child: %#v %v", got.Nodes, err)
	}
	if n := countJobs(t, f.store); n != 1 || f.backend.checks.Load() != 1 || f.backend.native.Load() != 0 {
		t.Fatalf("jobs %d, data checks %d, native %d", n, f.backend.checks.Load(), f.backend.native.Load())
	}
}

func TestWorkflowRunAdmitsCheckedChild(t *testing.T) {
	f, plain := newWorkflowFixture(t, false)
	f.submitWorkflow(t, "run", plainGraph("a"))
	cancel, done := beginRun(t, f.control)
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			f.hold(errors.New("original workflow Run did not return"))
			t.Error("original Run remains held")
			return
		}
		ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if e := f.control.Join(ctx); e != nil {
			f.hold(e)
			t.Error(e)
		}
	})
	// A finished workflow may retire, so watch its one child Job instead.
	deadline := time.Now().Add(5 * time.Second)
	for {
		// Children accepted after the previous poll lie past its high-water mark.
		jobs := storeJobs(t, f.store, 10)
		if len(jobs) > 1 {
			t.Fatalf("duplicate children: %d", len(jobs))
		}
		if len(jobs) == 1 && jobs[0].Status.State == backend.Succeeded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("workflow node did not run: %#v", jobs)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if plain.checks.Load() != 1 || plain.starts.Load() != 1 {
		t.Fatalf("checks %d starts %d", plain.checks.Load(), plain.starts.Load())
	}
}

// workflowLoop is coordinator state for c with no worker goroutines: the
// test receives each dispatched request from its worker's input itself.
func workflowLoop(c *Control) *loopState {
	s := &loopState{workers: make([]targetWorker, len(c.targets)), due: make(map[string]time.Time), workflowHeld: make(map[workflowHold]time.Time)}
	for i, target := range c.targets {
		s.workers[i] = targetWorker{target: target, input: make(chan operation, 1), done: make(chan struct{})}
	}
	return s
}

func TestWorkflowBusyRouteDoesNotStopIntake(t *testing.T) {
	f, _ := newWorkflowFixture(t, false)
	for i := 0; i < 40; i++ {
		f.submitWorkflow(t, fmt.Sprint("held-", i), workflow.Graph{Nodes: []workflow.Node{{Name: "train", Task: submissionDataTask()}}})
	}
	w := f.submitWorkflow(t, "plain", plainGraph("a"))
	ctx := context.Background()
	s := workflowLoop(f.control)
	s.workers[0].pending = &operation{kind: observeOperation} // "data" stays busy.
	plain := &s.workers[1]
	for turn := 0; turn < 200; turn++ {
		s.workflowDue = time.Time{}
		if err := f.control.advanceWorkflows(ctx, s); err != nil {
			t.Fatal(err)
		}
		if len(s.workflows) > 2 {
			t.Fatalf("turn %d retained %d continuations behind a busy route", turn, len(s.workflows))
		}
		if plain.pending != nil {
			op := <-plain.input
			if err := f.control.receiveOperation(ctx, s, operationResult{operation: op}); err != nil {
				t.Fatal(err)
			}
		}
		got, err := f.control.GetWorkflow(f.actor, w.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Nodes[0].JobID != "" {
			return
		}
	}
	t.Fatal("workflow for an idle target starved behind a busy target")
}

func TestWorkflowPollErrorPausesOnlyIntake(t *testing.T) {
	f, _ := newWorkflowFixture(t, false)
	job, err := f.control.Submit(f.actor, "plain-job", backend.Task{Argv: []string{"work"}, Target: "plain"})
	if err != nil {
		t.Fatal(err)
	}
	s := workflowLoop(f.control)
	polls := 0
	s.workflowPoll = func() ([]store.WorkflowCandidate, error) {
		polls++
		return nil, errors.New("modeled poll failure")
	}
	ctx := context.Background()
	if err = f.control.schedule(ctx, s); err != nil {
		t.Fatalf("poll failure stopped reconciliation: %v", err)
	}
	if p := s.workers[1].pending; p == nil || p.kind != startOperation || p.job.ID != job.ID {
		t.Fatal("ordinary job did not start beside a failed workflow poll")
	}
	if err = f.control.schedule(ctx, s); err != nil {
		t.Fatal(err)
	}
	if polls != 1 || time.Until(s.workflowDue) < workflowHoldTime-time.Second {
		t.Fatalf("failed poll not backed off: %d polls, due in %v", polls, time.Until(s.workflowDue))
	}
}

func TestWorkflowFullHoldTablePausesIntake(t *testing.T) {
	f, _ := newWorkflowFixture(t, false)
	const n = maxWorkflowHolds + 2
	for i := 0; i < n; i++ {
		f.submitWorkflow(t, fmt.Sprint("refused-", i), plainGraph("a"))
	}
	ctx := context.Background()
	s := workflowLoop(f.control)
	plain := &s.workers[1]
	checked := make(map[workflowKey]time.Time)
	for turn := 0; turn < 20*n; turn++ {
		s.workflowDue = time.Time{}
		if err := f.control.advanceWorkflows(ctx, s); err != nil {
			t.Fatal(err)
		}
		if plain.pending == nil {
			continue
		}
		op := <-plain.input
		k := op.workflow.key()
		if at, ok := checked[k]; ok && time.Since(at) < workflowHoldTime {
			t.Fatalf("node %v checked again inside its backoff", k)
		}
		checked[k] = time.Now()
		if err := f.control.receiveOperation(ctx, s, operationResult{operation: op, err: errors.New("modeled refusal")}); err != nil {
			t.Fatal(err)
		}
	}
	if len(checked) < maxWorkflowHolds {
		t.Fatalf("only %d nodes were checked", len(checked))
	}
}
