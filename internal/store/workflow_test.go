package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/workflow"
	bolt "go.etcd.io/bbolt"
)

// These tests drive real Store transactions with real file-authenticated
// actors and delegations. Workspace writer, seal and remote container
// receipts are MODELED inputs, as in the artifact and remote tests; no
// Control scheduling, native execution or physical retirement is proved.
type workflowFixture struct {
	s         *Store
	dir       string
	alice     auth.Actor
	bob       auth.Actor
	reader    auth.Actor
	admin     auth.Actor
	authority auth.Delegation
	domain    *RequestDomain
}

func newWorkflowFixture(t *testing.T) *workflowFixture {
	t.Helper()
	sf := newScopeFixture(t)
	authority, err := sf.engine.Delegate(sf.actors["alice"])
	if err != nil {
		t.Fatal(err)
	}
	return &workflowFixture{s: sf.s, dir: filepath.Dir(sf.s.db.Path()), alice: sf.actors["alice"], bob: sf.actors["bob"], reader: sf.actors["reader"], admin: sf.actors["legacy"], authority: authority}
}

func (f *workflowFixture) restart(t *testing.T) {
	t.Helper()
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	f.s = s
}

func (f *workflowFixture) enable(t *testing.T, p RetentionPolicy) {
	t.Helper()
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := EnableRetention(f.dir, p)
	if err != nil {
		t.Fatal(err)
	}
	f.domain = &info.Domain
	s, err := Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	f.s = s
}

// install installs artifact metadata, keeping the store's clock.
func (f *workflowFixture) install(t *testing.T) {
	t.Helper()
	clock := f.s.now().UTC()
	artifactTestInstall(t, f.s)
	f.s.now = func() time.Time { return clock }
}

func (f *workflowFixture) submit(t *testing.T, request string, g workflow.Graph) Workflow {
	t.Helper()
	w, err := f.s.SubmitWorkflow(f.alice, f.domain, request, g, f.authority)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func (f *workflowFixture) admission(c WorkflowCandidate) WorkflowAdmission {
	a := WorkflowAdmission{Candidate: c, Target: "remote", Binding: "test-binding"}
	if f.domain != nil {
		a.Storage = backend.RemoteOutput
	}
	return a
}

func (f *workflowFixture) candidates(t *testing.T) []WorkflowCandidate {
	t.Helper()
	out, err := f.s.WorkflowCandidates()
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// admitOne admits the single expected candidate for node.
func (f *workflowFixture) admitOne(t *testing.T, w Workflow, node string) backend.Job {
	t.Helper()
	for range 4 {
		for _, c := range f.candidates(t) {
			if c.Workflow == w.ID && w.Graph.Nodes[c.Node].Name == node {
				j, err := f.s.AdmitWorkflowChild(f.admission(c))
				if err != nil {
					t.Fatal(err)
				}
				return j
			}
		}
	}
	t.Fatalf("node %s never became a candidate", node)
	return backend.Job{}
}

func (f *workflowFixture) finish(t *testing.T, id string, state backend.State) {
	t.Helper()
	if _, err := f.s.Update(id, func(j *backend.Job) error { j.Status.State = state; return nil }); err != nil {
		t.Fatal(err)
	}
}

func (f *workflowFixture) get(t *testing.T, id string) Workflow {
	t.Helper()
	w, err := f.s.GetWorkflowFor(f.alice, id)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func (f *workflowFixture) valid(t *testing.T) {
	t.Helper()
	if err := f.s.db.View(validateSnapshot); err != nil {
		t.Fatal(err)
	}
}

func workflowGraph(spec ...string) workflow.Graph {
	// Each spec is "name" or "name<dep,dep".
	var g workflow.Graph
	for _, s := range spec {
		name, deps, _ := strings.Cut(s, "<")
		n := workflow.Node{Name: name, Task: backend.Task{Argv: []string{"run", name}}}
		if deps != "" {
			n.DependsOn = strings.Split(deps, ",")
		}
		g.Nodes = append(g.Nodes, n)
	}
	return g
}

func workflowDatabase(t *testing.T, s *Store) map[string]string {
	t.Helper()
	out := make(map[string]string)
	var walk func(path string, b *bolt.Bucket) error
	walk = func(path string, b *bolt.Bucket) error {
		out[path+"\x00sequence"] = fmt.Sprint(b.Sequence())
		return b.ForEach(func(k, v []byte) error {
			if v == nil {
				return walk(path+"/"+string(k), b.Bucket(k))
			}
			out[path+"/"+string(k)] = string(v)
			return nil
		})
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error { return walk(string(name), b) })
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func workflowUnchanged(t *testing.T, s *Store, before map[string]string) {
	t.Helper()
	if after := workflowDatabase(t, s); !reflect.DeepEqual(before, after) {
		for k, v := range after {
			if before[k] != v {
				t.Errorf("changed %q", k)
			}
		}
		for k := range before {
			if _, ok := after[k]; !ok {
				t.Errorf("removed %q", k)
			}
		}
		t.Fatal("refused transaction changed the database")
	}
}

func workflowEdges(t *testing.T, s *Store) map[string]workflowInput {
	t.Helper()
	var out map[string]workflowInput
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		out = st.WorkflowInputs
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func workflowJob(t *testing.T, s *Store, id string) backend.Job {
	t.Helper()
	j, err := s.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestWorkflowControlOnlyWithoutArtifacts(t *testing.T) {
	f := newWorkflowFixture(t)
	g := workflowGraph("build", "test<build")
	w := f.submit(t, "pipeline", g)
	if len(w.Nodes) != 2 || w.Nodes[0].Disposition != WorkflowPending || w.Nodes[0].JobID != "" || w.Principal != "alice" {
		t.Fatalf("accepted: %+v", w)
	}
	before := workflowDatabase(t, f.s)
	// Exact replay precedes the delegation check of a new acceptance.
	again, err := f.s.SubmitWorkflow(f.alice, nil, "pipeline", g, auth.Delegation{})
	if err != nil || again.ID != w.ID {
		t.Fatalf("exact replay: %v %+v", err, again)
	}
	workflowUnchanged(t, f.s, before)
	if _, err = f.s.SubmitWorkflow(f.alice, nil, "new", g, auth.Delegation{}); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("new parent without delegation: %v", err)
	}
	workflowUnchanged(t, f.s, before)
	if _, err = f.s.SubmitWorkflow(f.alice, nil, "pipeline", workflowGraph("build"), f.authority); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed graph: %v", err)
	}
	workflowUnchanged(t, f.s, before)

	build := f.admitOne(t, w, "build")
	if build.Status.State != backend.Queued || !reflect.DeepEqual(build.Task, backend.Task{Argv: []string{"run", "build"}}) {
		t.Fatalf("child: %+v", build)
	}
	for _, c := range f.candidates(t) {
		if c.Workflow == w.ID {
			t.Fatalf("waiting node offered: %+v", c)
		}
	}
	f.finish(t, build.ID, backend.Succeeded)
	test := f.admitOne(t, w, "test")
	got := f.get(t, w.ID)
	if got.Nodes[0].JobID != build.ID || got.Nodes[0].State != backend.Succeeded || got.Nodes[1].JobID != test.ID || got.Nodes[1].State != backend.Queued {
		t.Fatalf("view: %+v", got)
	}
	f.restart(t)
	f.valid(t)
	if got := f.get(t, w.ID); got.Nodes[1].JobID != test.ID {
		t.Fatalf("reopen: %+v", got)
	}
}

func TestWorkflowTypedRequestUnion(t *testing.T) {
	f := newWorkflowFixture(t)
	if _, err := f.s.Submit(f.alice, "job-first", backend.Task{Argv: []string{"x"}}, "remote", "test-binding"); err != nil {
		t.Fatal(err)
	}
	before := workflowDatabase(t, f.s)
	if _, err := f.s.SubmitWorkflow(f.alice, nil, "job-first", workflowGraph("a"), f.authority); !errors.Is(err, ErrConflict) {
		t.Fatalf("workflow over job request: %v", err)
	}
	workflowUnchanged(t, f.s, before)
	f.submit(t, "workflow-first", workflowGraph("a"))
	before = workflowDatabase(t, f.s)
	if _, err := f.s.Submit(f.alice, "workflow-first", backend.Task{Argv: []string{"x"}}, "remote", "test-binding"); !errors.Is(err, ErrConflict) {
		t.Fatalf("job over workflow request: %v", err)
	}
	workflowUnchanged(t, f.s, before)
	// Namespaces are per principal.
	bob, err := f.s.SubmitWorkflow(f.bob, nil, "workflow-first", workflowGraph("a"), f.authority)
	if !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("foreign delegation: %+v %v", bob, err)
	}
}

func TestWorkflowScopedAccess(t *testing.T) {
	f := newWorkflowFixture(t)
	w := f.submit(t, "mine", workflowGraph("a"))
	if _, err := f.s.SubmitWorkflow(f.reader, nil, "read-only", workflowGraph("a"), f.authority); !errors.Is(err, auth.ErrPermissionDenied) {
		t.Fatalf("reader submit: %v", err)
	}
	if _, err := f.s.GetWorkflowFor(f.bob, w.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign get: %v", err)
	}
	if _, err := f.s.CancelWorkflowFor(f.bob, w.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign cancel: %v", err)
	}
	if _, err := f.s.GetWorkflowFor(f.alice, "not-an-id"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("invalid id: %v", err)
	}
	for _, tt := range []struct {
		name  string
		actor auth.Actor
		want  int
	}{
		{"owner", f.alice, 1},
		{"other", f.bob, 0},
		{"admin", f.admin, 1},
	} {
		page, err := f.s.ListWorkflowsFor(tt.actor, 0, 10)
		if err != nil || len(page.Workflows) != tt.want || page.More {
			t.Errorf("%s: %+v %v", tt.name, page, err)
		}
	}
	second := f.submit(t, "second", workflowGraph("a"))
	page, err := f.s.ListWorkflowsFor(f.alice, 0, 1)
	if err != nil || len(page.Workflows) != 1 || page.Workflows[0].ID != w.ID || !page.More || page.After != 1 {
		t.Fatalf("first page: %+v %v", page, err)
	}
	page, err = f.s.ListWorkflowsFor(f.alice, page.After, 1)
	if err != nil || len(page.Workflows) != 1 || page.Workflows[0].ID != second.ID || page.More {
		t.Fatalf("second page: %+v %v", page, err)
	}
}

func TestWorkflowFailureSkipsDependents(t *testing.T) {
	f := newWorkflowFixture(t)
	w := f.submit(t, "skip", workflowGraph("a", "b<a", "c<b", "d"))
	a := f.admitOne(t, w, "a")
	f.admitOne(t, w, "d")
	f.finish(t, a.ID, backend.Failed)
	f.candidates(t)
	got := f.get(t, w.ID)
	want := []WorkflowDisposition{WorkflowPending, WorkflowSkipped, WorkflowSkipped, WorkflowPending}
	for i, n := range got.Nodes {
		if n.Disposition != want[i] {
			t.Fatalf("node %d: %+v", i, got.Nodes)
		}
	}
	f.valid(t)
}

func TestWorkflowCancelRacesAdmission(t *testing.T) {
	f := newWorkflowFixture(t)
	w := f.submit(t, "cancel", workflowGraph("a", "b", "c<a"))
	var b WorkflowCandidate
	for _, c := range f.candidates(t) {
		if w.Graph.Nodes[c.Node].Name == "b" {
			b = c
		}
	}
	a := f.admitOne(t, w, "a")
	canceled, err := f.s.CancelWorkflowFor(f.alice, w.ID)
	if err != nil || !canceled.Canceled || canceled.Nodes[1].Disposition != WorkflowCanceled || canceled.Nodes[2].Disposition != WorkflowCanceled || canceled.Nodes[0].JobID != a.ID {
		t.Fatalf("cancel: %+v %v", canceled, err)
	}
	before := workflowDatabase(t, f.s)
	if _, err = f.s.AdmitWorkflowChild(f.admission(b)); !errors.Is(err, ErrWorkflowStale) {
		t.Fatalf("admission after cancel: %v", err)
	}
	workflowUnchanged(t, f.s, before)
	if again, err := f.s.CancelWorkflowFor(f.alice, w.ID); err != nil || !reflect.DeepEqual(again, canceled) {
		t.Fatalf("repeat cancel: %+v %v", again, err)
	}
	if workflowJob(t, f.s, a.ID).CancelRequested {
		t.Fatal("parent cancel delivered child intent in its own transaction")
	}
	if out := f.candidates(t); len(out) != 0 {
		t.Fatalf("canceled workflow offered: %+v", out)
	}
	if j := workflowJob(t, f.s, a.ID); !j.CancelRequested || j.Status.State.Terminal() {
		t.Fatalf("child intent: %+v", j)
	}
	f.valid(t)
}

func TestWorkflowStaleCandidate(t *testing.T) {
	f := newWorkflowFixture(t)
	w := f.submit(t, "stale", workflowGraph("a"))
	c := f.candidates(t)[0]
	for _, tt := range []struct {
		name   string
		change func(*WorkflowCandidate)
	}{
		{"task", func(c *WorkflowCandidate) { c.Task.Argv = []string{"other"} }},
		{"authority", func(c *WorkflowCandidate) { c.Authority.TokenID = strings.Repeat("0", 32) }},
		{"principal", func(c *WorkflowCandidate) { c.Principal = "bob" }},
		{"node", func(c *WorkflowCandidate) { c.Node = 1 }},
		{"revision", func(c *WorkflowCandidate) { c.Revision++ }},
	} {
		changed := c
		changed.Task = backend.CloneTask(c.Task)
		tt.change(&changed)
		before := workflowDatabase(t, f.s)
		if _, err := f.s.AdmitWorkflowChild(f.admission(changed)); !errors.Is(err, ErrWorkflowStale) {
			t.Errorf("%s: %v", tt.name, err)
		}
		workflowUnchanged(t, f.s, before)
	}
	j, err := f.s.AdmitWorkflowChild(f.admission(c))
	if err != nil {
		t.Fatal(err)
	}
	// A lost reply is recovered by repeating the same admission.
	again, err := f.s.AdmitWorkflowChild(f.admission(c))
	if err != nil || again.ID != j.ID || again.Sequence != j.Sequence {
		t.Fatalf("lost-reply replay: %+v %v", again, err)
	}
	if got := f.get(t, w.ID); got.Nodes[0].JobID != j.ID {
		t.Fatalf("binding: %+v", got)
	}
}

func TestWorkflowSiblingAdmissionsShareRevision(t *testing.T) {
	f := newWorkflowFixture(t)
	w := f.submit(t, "siblings", workflowGraph("a", "b", "c"))
	c := f.candidates(t)
	if len(c) != 3 {
		t.Fatalf("candidates: %+v", c)
	}
	for _, x := range c {
		if _, err := f.s.AdmitWorkflowChild(f.admission(x)); err != nil {
			t.Fatalf("sibling %d: %v", x.Node, err)
		}
	}
	for i, n := range f.get(t, w.ID).Nodes {
		if n.JobID == "" {
			t.Fatalf("node %d unbound", i)
		}
	}
}

// A dependency skip advances the revision a planned candidate bound.
func TestWorkflowSkipInvalidatesPlannedCandidate(t *testing.T) {
	f := newWorkflowFixture(t)
	w := f.submit(t, "skip-race", workflowGraph("a", "b", "c<a"))
	a := f.admitOne(t, w, "a")
	var b WorkflowCandidate
	for _, x := range f.candidates(t) {
		if w.Graph.Nodes[x.Node].Name == "b" {
			b = x
		}
	}
	f.finish(t, a.ID, backend.Failed)
	f.candidates(t)
	if f.get(t, w.ID).Nodes[2].Disposition != WorkflowSkipped {
		t.Fatal("dependent not skipped")
	}
	before := workflowDatabase(t, f.s)
	if _, err := f.s.AdmitWorkflowChild(f.admission(b)); !errors.Is(err, ErrWorkflowStale) {
		t.Fatalf("admission across skip: %v", err)
	}
	workflowUnchanged(t, f.s, before)
	if j := f.admitOne(t, w, "b"); j.ID == "" {
		t.Fatal("fresh candidate not admitted")
	}
}

// Control may hold any candidate indefinitely. Holding one workflow's ready
// nodes must not hide another workflow's.
func TestWorkflowCandidatesFairAcrossWorkflows(t *testing.T) {
	f := newWorkflowFixture(t)
	f.submit(t, "held", workflowGraph("a", "b", "c", "d"))
	w := f.submit(t, "placeable", workflowGraph("e"))
	for range 2 {
		for _, c := range f.candidates(t) {
			if c.Workflow == w.ID {
				return
			}
		}
	}
	t.Fatal("a held workflow starved another workflow's ready node")
}

// Holding a workflow's first ready nodes must not hide its later ones.
func TestWorkflowCandidatesFairWithinWorkflow(t *testing.T) {
	f := newWorkflowFixture(t)
	g := workflowGraph("a", "b", "c", "d", "e", "f")
	f.submit(t, "wide", g)
	seen := make(map[int]bool)
	for range len(g.Nodes) {
		for _, c := range f.candidates(t) {
			seen[c.Node] = true
		}
	}
	if len(seen) != len(g.Nodes) {
		t.Fatalf("offered nodes %v of %d", seen, len(g.Nodes))
	}
}

// The selector's cursor lives only in memory: at the MetadataBytes cap a
// settlement still commits, and no transaction writes a cursor row.
func TestWorkflowCandidatesSettleAtMetadataCap(t *testing.T) {
	f := newWorkflowFixture(t)
	f.enable(t, DefaultRetentionPolicy())
	w := f.submit(t, "capped", workflowGraph("a", "b<a"))
	a := f.admitOne(t, w, "a")
	f.run(t, a, backend.Failed)
	if err := f.s.db.Update(func(tx *bolt.Tx) error {
		st, e := loadRetention(tx)
		if e != nil {
			return e
		}
		u, e := usage(tx)
		if e != nil {
			return e
		}
		st.Policy.MetadataBytes = u[MetadataBytesQuota]
		return putJSON(retentionRoot(tx), retentionStateKey, st)
	}); err != nil {
		t.Fatal(err)
	}
	metadata := func() (uint64, bool) {
		t.Helper()
		var used uint64
		var row bool
		if err := f.s.db.View(func(tx *bolt.Tx) error {
			u, e := usage(tx)
			used, row = u[MetadataBytesQuota], tx.Bucket(workflowsBucket).Get(workflowAdvanceKey) != nil
			return e
		}); err != nil {
			t.Fatal(err)
		}
		return used, row
	}
	before, row := metadata()
	if row {
		t.Fatal("read-only polls wrote a cursor row")
	}
	f.candidates(t)
	if got := f.get(t, w.ID); got.Nodes[1].Disposition != WorkflowSkipped {
		t.Fatalf("not skipped at the cap: %+v", got.Nodes)
	}
	if after, row := metadata(); row || after != before {
		t.Fatalf("settlement at the cap: cursor row %v, MetadataBytes %d -> %d", row, before, after)
	}
	f.valid(t)
}

// Every placeable node of every workflow is offered within a bounded number
// of polls while Control holds the others. Each workflow's first four nodes
// are held forever; its last two are admitted when offered.
func TestWorkflowCandidatesFairWithHeldNodes(t *testing.T) {
	for _, n := range []int{4, 8} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			f := newWorkflowFixture(t)
			g := workflowGraph("h0", "h1", "h2", "h3", "p0", "p1")
			placeable := make(map[string]bool)
			for i := range n {
				w := f.submit(t, fmt.Sprintf("held-%d", i), g)
				placeable[w.ID+"/4"], placeable[w.ID+"/5"] = true, true
			}
			// Each workflow is visited every ceil(n/4) polls and offered at
			// least one node per visit, starting one node later each pass.
			limit := 2 * len(g.Nodes) * ((n + 3) / 4)
			polls := 0
			for ; len(placeable) != 0 && polls < limit; polls++ {
				out := f.candidates(t)
				if len(out) > 4 {
					t.Fatalf("poll returned %d candidates", len(out))
				}
				for _, c := range out {
					key := fmt.Sprintf("%s/%d", c.Workflow, c.Node)
					if !placeable[key] {
						continue
					}
					if _, err := f.s.AdmitWorkflowChild(f.admission(c)); err != nil {
						t.Fatal(err)
					}
					delete(placeable, key)
				}
			}
			if len(placeable) != 0 {
				t.Fatalf("after %d polls, never offered: %v", polls, placeable)
			}
			t.Logf("%d workflows: all placeable nodes admitted in %d polls", n, polls)
			f.valid(t)
		})
	}
}

// Rotation holds on the settling path too: while another workflow's
// cancellation is delivered one child per poll, every poll commits and still
// offers each of a held workflow's nodes.
func TestWorkflowCandidatesFairWhileSettling(t *testing.T) {
	f := newWorkflowFixture(t)
	s := f.submit(t, "settling", workflowGraph("a", "b", "c", "d", "e", "f", "g", "h"))
	for bound := 0; bound < len(s.Graph.Nodes); {
		out := f.candidates(t)
		if len(out) == 0 {
			t.Fatal("settling workflow not offered")
		}
		for _, c := range out {
			if _, err := f.s.AdmitWorkflowChild(f.admission(c)); err != nil {
				t.Fatal(err)
			}
			bound++
		}
	}
	g := workflowGraph("a", "b", "c", "d", "e", "f")
	w := f.submit(t, "held", g)
	if _, err := f.s.CancelWorkflowFor(f.alice, s.ID); err != nil {
		t.Fatal(err)
	}
	seen := make(map[int]bool)
	for polls := 0; len(seen) != len(g.Nodes); polls++ {
		if polls == len(s.Graph.Nodes) {
			t.Fatalf("offered nodes %v of %d", seen, len(g.Nodes))
		}
		writes := workflowWrites(f.s)
		out := f.candidates(t)
		if workflowWrites(f.s) == writes {
			t.Fatalf("poll %d did not settle", polls)
		}
		for _, c := range out {
			if c.Workflow != w.ID {
				t.Fatalf("canceled workflow offered: %+v", c)
			}
			seen[c.Node] = true
		}
	}
	f.valid(t)
}

// workflowWrites counts committed page writes, including meta pages.
// TxStats.GetWrite has a pointer receiver, so the stats must be addressable.
func workflowWrites(s *Store) int64 {
	stats := s.db.Stats()
	return stats.TxStats.GetWrite()
}

func TestWorkflowCandidatesPollIsReadOnly(t *testing.T) {
	f := newWorkflowFixture(t)
	for i := range 6 {
		f.submit(t, fmt.Sprintf("poll-%d", i), workflowGraph("a"))
	}
	before := workflowDatabase(t, f.s)
	writes := workflowWrites(f.s)
	seen := make(map[string]bool)
	for range 2 {
		for _, c := range f.candidates(t) {
			seen[c.Workflow] = true
		}
	}
	workflowUnchanged(t, f.s, before)
	if got := workflowWrites(f.s); got != writes {
		t.Fatalf("idle polls wrote %d pages", got-writes)
	}
	if len(seen) != 6 {
		t.Fatalf("round robin reached %d of 6 workflows", len(seen))
	}
}

func TestWorkflowCandidatesSettleCommitsOnce(t *testing.T) {
	f := newWorkflowFixture(t)
	w := f.submit(t, "settle", workflowGraph("a", "b<a"))
	a := f.admitOne(t, w, "a")
	f.finish(t, a.ID, backend.Failed)
	writes := workflowWrites(f.s)
	f.candidates(t)
	if workflowWrites(f.s) == writes {
		t.Fatal("dependency failure settled without a commit")
	}
	if got := f.get(t, w.ID); got.Nodes[1].Disposition != WorkflowSkipped {
		t.Fatalf("not skipped: %+v", got.Nodes)
	}
	before := workflowDatabase(t, f.s)
	writes = workflowWrites(f.s)
	f.candidates(t)
	workflowUnchanged(t, f.s, before)
	if workflowWrites(f.s) != writes {
		t.Fatal("settled workflow still commits on every poll")
	}
	f.valid(t)
}

func TestWorkflowArtifactObligationsRequireInstallation(t *testing.T) {
	f := newWorkflowFixture(t)
	g := workflow.Graph{Nodes: []workflow.Node{
		{Name: "make", Task: backend.Task{Argv: []string{"make"}, Workdir: workflowRef('a'), Outputs: []string{"out"}}},
		{Name: "use", Task: backend.Task{Argv: []string{"use"}}, DependsOn: []string{"make"}, Input: &workflow.Input{Node: "make", Slot: "result"}},
	}}
	before := workflowDatabase(t, f.s)
	if _, err := f.s.SubmitWorkflow(f.alice, nil, "edges", g, f.authority); !errors.Is(err, ErrArtifactsDisabled) {
		t.Fatalf("edge without installation: %v", err)
	}
	workflowUnchanged(t, f.s, before)
}

// workflowPublishFixture accepts producer -> consumer and admits the producer.
func workflowPublishFixture(t *testing.T, f *workflowFixture) (Workflow, backend.Job) {
	t.Helper()
	f.install(t)
	input := artifactTestComplete(t, f.s, f.alice)
	g := workflow.Graph{Nodes: []workflow.Node{
		{Name: "make", Task: backend.Task{Argv: []string{"make"}, Workdir: &artifact.Ref{ID: input.ID, SHA256: input.SHA256, Size: int64(input.Size)}, Outputs: []string{"out"}}},
		{Name: "use", Task: backend.Task{Argv: []string{"use"}}, DependsOn: []string{"make"}, Input: &workflow.Input{Node: "make", Slot: "result"}},
	}}
	w := f.submit(t, "publish", g)
	edges := workflowEdges(t, f.s)
	e, ok := edges[workflowInputKey(w.ID, 1)]
	if len(edges) != 1 || !ok || e.Ref != nil || e.Producer != 0 || e.Consumer != 1 || e.Owner != "alice" {
		t.Fatalf("reserved edge: %+v", edges)
	}
	j := f.admitOne(t, w, "make")
	j, start, err := f.s.BeginAttempt(j.ID, j.CurrentAttemptID(), 0, f.s.now())
	if err != nil || !start {
		t.Fatalf("begin: %v %v", start, err)
	}
	j, err = f.s.ObserveAttempt(j.ID, j.CurrentAttemptID(), backend.Status{State: backend.Running, Instance: "i"}, f.s.now())
	if err != nil {
		t.Fatal(err)
	}
	return w, j
}

func TestWorkflowLocalPublicationFillsEdge(t *testing.T) {
	f := newWorkflowFixture(t)
	w, producer := workflowPublishFixture(t, f)
	// Before publication the consumer is held, not offered.
	for _, c := range f.candidates(t) {
		if c.Workflow == w.ID {
			t.Fatalf("unpublished input offered: %+v", c)
		}
	}
	revision := func() uint64 {
		var st workflowState
		if err := f.s.db.View(func(tx *bolt.Tx) (err error) { st, err = loadWorkflowState(tx, w.ID); return }); err != nil {
			t.Fatal(err)
		}
		return st.Revision
	}
	r := revision()
	ref := artifactRegressionComplete(t, f.s, producer, false)
	if revision() != r+1 {
		t.Fatal("publication did not advance the eligibility revision")
	}
	e := workflowEdges(t, f.s)[workflowInputKey(w.ID, 1)]
	if e.Ref == nil || *e.Ref != ref || e.JobID != producer.ID || e.AttemptID != producer.CurrentAttemptID() {
		t.Fatalf("published edge: %+v", e)
	}
	required, err := f.s.ArtifactRequiredObjects()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, o := range required {
		found = found || o.Reference == ref
	}
	if !found {
		t.Fatalf("required objects omit edge: %+v", required)
	}
	f.restart(t)
	f.valid(t)
	consumer := f.admitOne(t, w, "use")
	want := artifact.Ref{ID: ref.ID, SHA256: ref.SHA256, Size: int64(ref.Size)}
	if consumer.Task.Workdir == nil || *consumer.Task.Workdir != want {
		t.Fatalf("consumer workdir: %+v", consumer.Task.Workdir)
	}
	if edges := workflowEdges(t, f.s); len(edges) != 0 {
		t.Fatalf("edge survived admission: %+v", edges)
	}
	got := f.get(t, w.ID)
	if got.Nodes[1].Input == nil || *got.Nodes[1].Input != ref {
		t.Fatalf("node input: %+v", got.Nodes[1])
	}
	if err := f.s.db.View(func(tx *bolt.Tx) error {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		if p, ok := st.JobInputs[consumer.ID]; !ok || p.Ref != ref {
			return fmt.Errorf("consumer pin: %+v", st.JobInputs)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	f.valid(t)
}

func TestWorkflowFailedProducerNeverFills(t *testing.T) {
	f := newWorkflowFixture(t)
	w, producer := workflowPublishFixture(t, f)
	exec, status, completion := maintenanceFailureCallback(t, f.s, producer, maintenancePhysicalOrigin(true))
	if _, err := f.s.CompleteWorkspaceAttempt(producer.ID, exec, status, completion, f.s.now()); err != nil {
		t.Fatal(err)
	}
	if e := workflowEdges(t, f.s)[workflowInputKey(w.ID, 1)]; e.Ref != nil {
		t.Fatalf("failed producer filled edge: %+v", e)
	}
	f.candidates(t)
	if got := f.get(t, w.ID); got.Nodes[1].Disposition != WorkflowSkipped {
		t.Fatalf("consumer: %+v", got.Nodes[1])
	}
	if edges := workflowEdges(t, f.s); len(edges) != 0 {
		t.Fatalf("skip kept edge: %+v", edges)
	}
	f.valid(t)
}

// A successful producer with outputs cannot complete without a result, so
// its edge stays pending and its consumer is never offered.
func TestWorkflowSucceededWithoutResultHolds(t *testing.T) {
	f := newWorkflowFixture(t)
	w, producer := workflowPublishFixture(t, f)
	installation, err := f.s.ArtifactInstallation()
	if err != nil {
		t.Fatal(err)
	}
	exec := backend.Reference{ID: producer.CurrentAttemptID(), Task: producer.Task, Binding: producer.TargetBinding, Instance: "i"}
	completion := WorkspaceCompletion{Outcome: WorkspaceTaskExit, Writer: &WorkspaceWriterClosure{InstallationID: f.s.Owner(), RootID: installation.ID, JobID: producer.ID, Execution: exec}, Origin: maintenancePhysicalOrigin(false)}
	zero := int32(0)
	before := workflowDatabase(t, f.s)
	if _, err = f.s.CompleteWorkspaceAttempt(producer.ID, exec, backend.Status{State: backend.Succeeded, Instance: "i", ExitCode: &zero}, completion, f.s.now()); err == nil {
		t.Fatal("success without result completed")
	}
	workflowUnchanged(t, f.s, before)
	for _, c := range f.candidates(t) {
		if c.Workflow == w.ID {
			t.Fatalf("missing result offered: %+v", c)
		}
	}
	if e := workflowEdges(t, f.s)[workflowInputKey(w.ID, 1)]; e.Ref != nil {
		t.Fatalf("missing result filled edge: %+v", e)
	}
}

func TestWorkflowCancelReleasesEdges(t *testing.T) {
	f := newWorkflowFixture(t)
	w, producer := workflowPublishFixture(t, f)
	if _, err := f.s.CancelWorkflowFor(f.alice, w.ID); err != nil {
		t.Fatal(err)
	}
	if edges := workflowEdges(t, f.s); len(edges) != 0 {
		t.Fatalf("cancel kept edge: %+v", edges)
	}
	artifactRegressionComplete(t, f.s, producer, false)
	if edges := workflowEdges(t, f.s); len(edges) != 0 {
		t.Fatalf("publication after cancel filled edge: %+v", edges)
	}
	f.valid(t)
}

// run moves a child through a real attempt to a verified terminal state.
func (f *workflowFixture) run(t *testing.T, j backend.Job, state backend.State) {
	t.Helper()
	now := f.s.now()
	j, start, err := f.s.BeginAttempt(j.ID, j.CurrentAttemptID(), 0, now)
	if err != nil || !start {
		t.Fatalf("begin: %v %v", start, err)
	}
	if j, err = f.s.ObserveAttempt(j.ID, j.CurrentAttemptID(), backend.Status{State: backend.Running, Instance: "i"}, now); err != nil {
		t.Fatal(err)
	}
	code := int32(0)
	if state != backend.Succeeded {
		code = 1
	}
	if _, err = f.s.ObserveAttempt(j.ID, j.CurrentAttemptID(), backend.Status{State: state, Instance: "i", ExitCode: &code, Termination: backend.StoppedWithExitCode}, now); err != nil {
		t.Fatal(err)
	}
}

// cleanup acknowledges every dispatchable cleanup operation.
func (f *workflowFixture) cleanup(t *testing.T) int {
	t.Helper()
	n := 0
	for ; n < 16; n++ {
		r, found, err := f.s.NextCleanup(0)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			return n
		}
		if err = f.s.FinishCleanup(r.ID, r.Operation, r.Digest, f.s.now()); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("cleanup never drained")
	return n
}

func (f *workflowFixture) prune(t *testing.T, sel PruneSelection) PrunePage {
	t.Helper()
	page, err := f.s.Prune(f.admin, sel, true, nil, nil, f.s.now())
	if err != nil {
		t.Fatal(err)
	}
	return page
}

func TestWorkflowRetentionLifecycle(t *testing.T) {
	f := newWorkflowFixture(t)
	f.enable(t, DefaultRetentionPolicy())
	g := workflowGraph("a", "b<a")
	w := f.submit(t, "retained", g)
	if w.Domain != *f.domain {
		t.Fatalf("domain: %+v", w.Domain)
	}
	if _, err := f.s.SubmitWorkflow(f.alice, nil, "undomained", g, f.authority); err == nil {
		t.Fatal("enabled retention accepted a new parent without a domain")
	}
	a := f.admitOne(t, w, "a")
	f.run(t, a, backend.Succeeded)
	sel := PruneSelection{Before: f.s.now().Add(time.Hour), Mode: Whole}
	page := f.prune(t, sel)
	held := false
	for _, s := range page.Skips {
		held = held || s.Reason == "workflow_dependency"
	}
	if page.Marked != 0 || !held {
		t.Fatalf("held producer: %+v", page)
	}
	b := f.admitOne(t, w, "b")
	f.run(t, b, backend.Succeeded)
	if page = f.prune(t, sel); page.Marked != 2 {
		t.Fatalf("prune children: %+v", page)
	}
	f.valid(t)
	got := f.get(t, w.ID)
	if got.Nodes[0].State != backend.Succeeded || got.Nodes[1].State != backend.Succeeded {
		t.Fatalf("captured terminal state: %+v", got.Nodes)
	}
	// Until the last positive acknowledgement the workflow stays.
	f.candidates(t)
	f.get(t, w.ID)
	n := f.cleanup(t)
	t.Logf("acknowledged %d cleanup operations", n)
	f.valid(t)
	f.candidates(t)
	if _, err := f.s.GetWorkflowFor(f.alice, w.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retired workflow: %v", err)
	}
	_, err := f.s.SubmitWorkflow(f.alice, f.domain, "retained", g, f.authority)
	expiryIs(t, err, ResultExpired)
	expired, found, err := f.s.LookupWorkflow(f.alice, f.domain, "retained", g)
	expiryIs(t, err, ResultExpired)
	if !found || expired.ID != "" {
		t.Fatalf("expired lookup: %+v %v", expired, found)
	}
	// The per-principal high-water survives retirement.
	next := f.submit(t, "next", g)
	list, err := f.s.ListWorkflowsFor(f.alice, 0, 10)
	if err != nil || len(list.Workflows) != 1 || list.Workflows[0].ID != next.ID || list.After != 2 {
		t.Fatalf("high-water: %+v %v", list, err)
	}
	f.restart(t)
	f.valid(t)
}

// An unverified terminal child goes to manual reconciliation, which is
// not retirement: the node and reverse row stay and the workflow stays.
func TestWorkflowManualCleanupIsNotRetirement(t *testing.T) {
	f := newWorkflowFixture(t)
	f.enable(t, DefaultRetentionPolicy())
	w := f.submit(t, "manual", workflowGraph("a"))
	a := f.admitOne(t, w, "a")
	f.finish(t, a.ID, backend.Succeeded)
	page := f.prune(t, PruneSelection{Before: f.s.now().Add(time.Hour), Mode: MetadataOnly, AcknowledgeUnverifiedTerminal: true})
	if page.Marked != 1 {
		t.Fatalf("prune: %+v", page)
	}
	f.cleanup(t)
	f.candidates(t)
	got := f.get(t, w.ID)
	if got.Nodes[0].JobID != a.ID || got.Nodes[0].State != backend.Succeeded {
		t.Fatalf("manual child: %+v", got.Nodes[0])
	}
	if workflowDatabase(t, f.s)["workflows/c/"+a.ID] == "" {
		t.Fatal("reverse row dropped before acknowledgement")
	}
	f.restart(t)
	f.valid(t)
}

// A child that never started has no cleanup work: the zero-operation path
// retires it in the marking transaction itself.
func TestWorkflowZeroOperationRetirement(t *testing.T) {
	f := newWorkflowFixture(t)
	f.enable(t, DefaultRetentionPolicy())
	w := f.submit(t, "zero", workflowGraph("a"))
	a := f.admitOne(t, w, "a")
	f.finish(t, a.ID, backend.Canceled)
	page := f.prune(t, PruneSelection{Before: f.s.now().Add(time.Hour), Mode: Whole})
	if page.Marked != 1 {
		t.Fatalf("prune: %+v", page)
	}
	if n := f.cleanup(t); n != 0 {
		t.Fatalf("zero-operation child dispatched %d operations", n)
	}
	f.candidates(t)
	if _, err := f.s.GetWorkflowFor(f.alice, w.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("zero-operation retirement: %v", err)
	}
	f.valid(t)
}

func TestWorkflowRetentionActivationRekeys(t *testing.T) {
	f := newWorkflowFixture(t)
	g := workflowGraph("a")
	w := f.submit(t, "before", g)
	f.enable(t, DefaultRetentionPolicy())
	f.valid(t)
	got := f.get(t, w.ID)
	if got.Domain != (RequestDomain{f.domain.Lineage, 0}) {
		t.Fatalf("rekeyed domain: %+v", got.Domain)
	}
	f.domain = nil
	if again := f.submit(t, "before", g); again.ID != w.ID {
		t.Fatalf("legacy replay: %+v", again)
	}
	if _, err := f.s.SubmitWorkflow(f.alice, nil, "after", g, f.authority); err == nil {
		t.Fatal("new undomained parent accepted after activation")
	}
}

func TestWorkflowBackupRestoreKeepsHistory(t *testing.T) {
	f := newWorkflowFixture(t)
	f.enable(t, DefaultRetentionPolicy())
	g := workflowGraph("a", "b<a")
	w := f.submit(t, "history", g)
	a := f.admitOne(t, w, "a")
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(t.TempDir(), "snapshot.db")
	if err := Backup(f.dir, snapshot); err != nil {
		t.Fatal(err)
	}
	f.dir = filepath.Join(t.TempDir(), "restored")
	if err := Restore(snapshot, f.dir); err != nil {
		t.Fatal(err)
	}
	s, err := Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	f.s = s
	f.valid(t)
	got := f.get(t, w.ID)
	if got.Domain != *f.domain || got.Nodes[0].JobID != a.ID {
		t.Fatalf("restored history: %+v", got)
	}
	_, err = f.s.SubmitWorkflow(f.alice, f.domain, "history", g, f.authority)
	expiryIs(t, err, RequestLineageChanged)
	f.finish(t, a.ID, backend.Succeeded)
	for _, c := range f.candidates(t) {
		if c.Workflow == w.ID && c.Authority != f.authority {
			t.Fatalf("historical authority rewritten: %+v", c.Authority)
		}
	}
}

// workflowSchemaSeven is a MODEL: it rewrites a fresh installation to the C16
// schema7 layout: no workflows bucket, no workflow_inputs table, no storage
// format tag or environment pin table, root floor 7. It is the same
// construction schemaSixFixture uses one schema earlier.
func workflowSchemaSeven(t *testing.T, dir string) backend.Job {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	artifactTestInstall(t, s)
	artifactTestComplete(t, s, testActor())
	j, err := s.Submit(testActor(), "kept", backend.Task{Argv: []string{"x"}}, "remote", "test-binding")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(workflowsBucket); err != nil {
			return err
		}
		if err := tx.DeleteBucket(environmentsBucket); err != nil {
			return err
		}
		if err := tx.DeleteBucket(workerGroupsBucket); err != nil {
			return err
		}
		if err := environmentArtifactStripModel(tx); err != nil {
			return err
		}
		b := tx.Bucket(artifactsBucket)
		if err := b.DeleteBucket([]byte("workflow_inputs")); err != nil {
			return err
		}
		var root artifactRoot
		if err := artifactDecode(b.Get([]byte("state")), &root); err != nil {
			return err
		}
		root.SchemaFloor = 7
		raw, err := json.Marshal(root)
		if err != nil {
			return err
		}
		if err := b.Put([]byte("state"), raw); err != nil {
			return err
		}
		return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(7))
	}); err != nil {
		t.Fatal(err)
	}
	if err = s.db.View(validateSnapshot); err != nil {
		t.Fatalf("schema7 fixture: %v", err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	return j
}

func TestWorkflowSchemaSevenMigration(t *testing.T) {
	dir := t.TempDir()
	j := workflowSchemaSeven(t, dir)
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	before := workflowDatabase(t, s)
	if err = s.db.View(func(tx *bolt.Tx) error {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		if schemaNumber(tx) != schemaVersion || st.Root.SchemaFloor != 8 || len(st.WorkflowInputs) != 0 {
			return fmt.Errorf("migrated to %d floor %d", schemaNumber(tx), st.Root.SchemaFloor)
		}
		if k, _ := tx.Bucket(workflowsBucket).Cursor().First(); k != nil {
			return errors.New("migration wrote workflow rows")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := workflowJob(t, s, j.ID); !reflect.DeepEqual(got.Task, j.Task) {
		t.Fatalf("accepted job changed: %+v", got)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopening an already migrated store writes nothing.
	if s, err = Open(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	workflowUnchanged(t, s, before)
}

func TestWorkflowSchemaFences(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*bolt.Tx) error
		want string
	}{
		{"future", func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(schemaVersion+1)) }, ErrUnsupportedSchema.Error()},
		{"seven with workflows", func(tx *bolt.Tx) error {
			_, err := tx.CreateBucket(workflowsBucket)
			return err
		}, "workflow metadata requires schema 8"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			workflowSchemaSeven(t, dir)
			db, err := bolt.Open(filepath.Join(dir, "jobs.db"), 0600, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err = db.Update(tt.edit); err != nil {
				t.Fatal(err)
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(filepath.Join(dir, "jobs.db"))
			if err != nil {
				t.Fatal(err)
			}
			if s, err := Open(dir); err == nil || !strings.Contains(err.Error(), tt.want) {
				if s != nil {
					s.Close()
				}
				t.Fatalf("fence: %v", err)
			}
			after, err := os.ReadFile(filepath.Join(dir, "jobs.db"))
			if err != nil || !bytes.Equal(raw, after) {
				t.Fatalf("refusal wrote the database: %v", err)
			}
		})
	}
}

// A filled edge protects the producer's result from ResultOnly pruning
// until child admission moves the protection to the child's input pin.
func TestWorkflowPublishedEdgePinsResult(t *testing.T) {
	f := newWorkflowFixture(t)
	f.enable(t, DefaultRetentionPolicy())
	w, producer := workflowPublishFixture(t, f)
	// Remove the producer's workspace so only the edge protects the result.
	artifactRegressionComplete(t, f.s, producer, true)
	sel := PruneSelection{Before: f.s.now().Add(time.Hour), Mode: ResultOnly, Attempt: 1, Max: producer.Sequence, Limit: 1}
	page := f.prune(t, sel)
	inUse := false
	for _, s := range page.Skips {
		inUse = inUse || s.Reason == "result_in_use"
	}
	if page.Marked != 0 || !inUse {
		t.Fatalf("pinned result pruned: %+v", page)
	}
	f.admitOne(t, w, "use")
	if page = f.prune(t, sel); page.Marked != 0 {
		t.Fatalf("consumer pin released result: %+v", page)
	}
	f.valid(t)
}

// workflowUnchangedExceptGate is workflowUnchanged allowing only the
// retention gate's own phase row to advance.
func workflowUnchangedExceptGate(t *testing.T, s *Store, before map[string]string) {
	t.Helper()
	after := workflowDatabase(t, s)
	gate := string(retentionBucket) + "/" + string(retentionStateKey)
	b, a := maps.Clone(before), maps.Clone(after)
	delete(b, gate)
	delete(a, gate)
	if !reflect.DeepEqual(b, a) {
		t.Fatal("lookup changed the database")
	}
}

func TestWorkflowLookup(t *testing.T) {
	f := newWorkflowFixture(t)
	g := workflowGraph("a", "b<a")
	miss := func(actor auth.Actor, domain *RequestDomain, request string) {
		t.Helper()
		before := workflowDatabase(t, f.s)
		w, found, err := f.s.LookupWorkflow(actor, domain, request, g)
		if err != nil || found || w.ID != "" {
			t.Fatalf("miss %q: %+v %v %v", request, w, found, err)
		}
		workflowUnchanged(t, f.s, before)
	}
	miss(f.alice, nil, "parent")
	w := f.submit(t, "parent", g)

	// Replay returns exactly what SubmitWorkflow replays.
	got, found, err := f.s.LookupWorkflow(f.alice, nil, "parent", g)
	if err != nil || !found || !reflect.DeepEqual(got, w) {
		t.Fatalf("replay: %+v %v %v", got, found, err)
	}
	again := f.submit(t, "parent", g)
	if !reflect.DeepEqual(again, got) {
		t.Fatalf("lookup differs from submit replay: %+v", again)
	}

	// A different graph under the same request is a conflict, as is a job.
	before := workflowDatabase(t, f.s)
	if _, found, err := f.s.LookupWorkflow(f.alice, nil, "parent", workflowGraph("a")); !errors.Is(err, ErrConflict) || !found {
		t.Fatalf("mismatch: %v %v", found, err)
	}
	workflowUnchanged(t, f.s, before)
	if _, err := f.s.Submit(f.alice, "job", backend.Task{Argv: []string{"x"}}, "remote", "test-binding"); err != nil {
		t.Fatal(err)
	}
	before = workflowDatabase(t, f.s)
	if _, found, err := f.s.LookupWorkflow(f.alice, nil, "job", g); !errors.Is(err, ErrConflict) || !found {
		t.Fatalf("cross-kind: %v %v", found, err)
	}
	workflowUnchanged(t, f.s, before)

	// Namespaces are per principal; a read-only actor may look up.
	miss(f.bob, nil, "parent")
	miss(f.reader, nil, "parent")
	for _, tt := range []struct {
		name    string
		domain  *RequestDomain
		request string
	}{
		{"request", nil, ""},
		{"lineage", &RequestDomain{"not-a-lineage", 1}, "parent"},
		{"epoch", &RequestDomain{w.ID, 0}, "parent"},
	} {
		if _, _, err := f.s.LookupWorkflow(f.alice, tt.domain, tt.request, g); err == nil {
			t.Errorf("invalid %s accepted", tt.name)
		}
	}
	if _, _, err := f.s.LookupWorkflow(f.alice, &RequestDomain{w.ID, 1}, "parent", g); !errors.Is(err, ErrRetentionDisabled) {
		t.Fatalf("domain while disabled: %v", err)
	}
	f.restart(t)
	if got, found, err := f.s.LookupWorkflow(f.alice, nil, "parent", g); err != nil || !found || got.ID != w.ID {
		t.Fatalf("replay after restart: %+v %v %v", got, found, err)
	}

	// After activation an undomained parent replays only under its legacy key.
	f.enable(t, DefaultRetentionPolicy())
	got, found, err = f.s.LookupWorkflow(f.alice, nil, "parent", g)
	if err != nil || !found || got.ID != w.ID || got.Domain != (RequestDomain{f.domain.Lineage, 0}) {
		t.Fatalf("legacy nil-domain replay: %+v %v %v", got, found, err)
	}
	before = workflowDatabase(t, f.s)
	_, found, err = f.s.LookupWorkflow(f.alice, nil, "new", g)
	expiryIs(t, err, RequestDomainRequired)
	if found {
		t.Fatal("undomained miss found")
	}
	workflowUnchangedExceptGate(t, f.s, before)
	foreign := RequestDomain{w.ID, f.domain.Epoch}
	_, _, err = f.s.LookupWorkflow(f.alice, &foreign, "parent", g)
	expiryIs(t, err, RequestLineageChanged)
	workflowUnchangedExceptGate(t, f.s, before)
	_, found, err = f.s.LookupWorkflow(f.alice, f.domain, "parent", g)
	if err != nil || found {
		t.Fatalf("domained miss: %v %v", found, err)
	}
	workflowUnchangedExceptGate(t, f.s, before)
	d := f.submit(t, "domained", g)
	got, found, err = f.s.LookupWorkflow(f.alice, f.domain, "domained", g)
	if err != nil || !found || !reflect.DeepEqual(got, d) {
		t.Fatalf("domained replay: %+v %v %v", got, found, err)
	}
	if _, found, err = f.s.LookupWorkflow(f.bob, f.domain, "domained", g); err != nil || found {
		t.Fatalf("foreign domained lookup: %v %v", found, err)
	}
}
