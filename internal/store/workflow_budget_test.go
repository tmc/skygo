package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/workflow"
	bolt "go.etcd.io/bbolt"
)

// workflowDelta measures a database change as the core ledger does: key plus
// old and new value bytes per changed row, and the number of created rows.
func workflowDelta(before, after map[string]string, prefix string) (changed uint64, created, writes int) {
	seen := make(map[string]bool)
	for k, v := range after {
		seen[k] = true
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		old, ok := before[k]
		if ok && old == v {
			continue
		}
		writes++
		if !ok {
			created++
		}
		changed += uint64(len(k) + len(old) + len(v))
	}
	for k, v := range before {
		if !seen[k] && strings.HasPrefix(k, prefix) {
			writes++
			changed += uint64(len(k) + len(v))
		}
	}
	return
}

func workflowRef(c byte) *artifact.Ref {
	return &artifact.Ref{ID: strings.Repeat(string(c), 32), SHA256: strings.Repeat(string(c), 64), Size: 1}
}

func TestWorkflowRecordMaxima(t *testing.T) {
	id := strings.Repeat("f", 32)
	owner := "a" + strings.Repeat("z", 63)
	ref := &ArtifactRef{ID: id, SHA256: strings.Repeat("e", 64), Size: math.MaxUint64}
	created := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.FixedZone("", -(23*3600+59*60)))
	st := workflowState{Owner: owner, Domain: RequestDomain{id, math.MaxUint64}, RequestID: strings.Repeat("Z", 128), Graph: strings.Repeat("b", 64), Authority: auth.Delegation{Principal: owner, TokenID: id, Digest: [32]byte{255}}, Created: created, Ordinal: math.MaxUint64, Scope: math.MaxUint64, Revision: math.MaxUint64, Canceled: true}
	for range 64 {
		st.Nodes = append(st.Nodes, workflowNode{Disposition: WorkflowCanceled, Job: id, Task: strings.Repeat("c", 64), Producer: id, Attempt: id, Input: ref, Revision: math.MaxUint64, Final: backend.Succeeded, Retired: true})
	}
	data, err := encodeWorkflowState(id, st)
	if err != nil {
		t.Fatal(err)
	}
	limit := workflowStateEnvelope + 64*workflowNodeBytes
	t.Logf("state: %d of %d bytes (key %d)", len(data), limit, len(workflowStateKey(id)))
	st.Nodes = st.Nodes[:1]
	if data, err = encodeWorkflowState(id, st); err != nil {
		t.Fatalf("one maximal node: %v", err)
	}
	t.Logf("state with one node: %d of %d bytes", len(data), workflowStateEnvelope+workflowNodeBytes)

	child, err := encodeWorkflowChild(id, workflowChild{id, 63})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("child: %d+%d of %d bytes", len(workflowChildKey(id)), len(child), workflowChildBytes)

	edge, err := json.Marshal(workflowInput{Owner: owner, Workflow: id, Producer: 63, Consumer: 62, JobID: id, AttemptID: id, Ref: ref})
	if err != nil || len(edge) > 1024 {
		t.Fatalf("edge: %d bytes %v", len(edge), err)
	}
	t.Logf("edge: %d of 1024 bytes", len(edge))

	r := requestRecord{Principal: owner, Domain: RequestDomain{id, math.MaxUint64}, RequestID: strings.Repeat("Z", 128), Expired: true, WorkflowID: id, GraphDigest: strings.Repeat("b", 64)}
	request, err := encodeRetentionRecord(retainedRequestsBucket, r)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("request: %d of %d bytes", len(request), workflowRequestLimit)
	if !workflowRequest(request) || recordCharge(retainedRequestsBucket, []byte("k"), request) != 1+workflowRequestLimit {
		t.Fatal("workflow request charge is not the fixed reservation")
	}
}

// Job alternatives encode exactly as before workflows existed.
func TestWorkflowPreservesJobRecordBytes(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value any
		want  string
	}{
		{"request", requestRecord{Principal: "alice", RequestID: "r", JobID: "j"}, `"workflow_id"`},
		{"request", requestRecord{Principal: "alice", RequestID: "r", JobID: "j"}, `"graph_digest"`},
		{"job", retainedJob{Principal: "alice"}, `"workflow_id"`},
		{"job", retainedJob{Principal: "alice"}, `"node"`},
	} {
		data, err := json.Marshal(tt.value)
		if err != nil || strings.Contains(string(data), tt.want) {
			t.Errorf("%s: %s %v", tt.name, data, err)
		}
	}
}

// workflowMaxGraph returns the largest graph of shape whose canonical
// encoding fits, and the graph with the next larger padding.
func workflowMaxGraph(t *testing.T, shape func(pad string) workflow.Graph) (fit, over workflow.Graph) {
	t.Helper()
	for n := 4200; n > 0; n-- {
		g := shape(strings.Repeat("p", n))
		data, err := workflow.Canonical(g)
		if err != nil {
			over = g
			continue
		}
		if over.Nodes == nil {
			t.Fatal("padding never exceeded the graph limit")
		}
		t.Logf("graph: %d of %d bytes", len(data), workflowGraphLimit)
		return g, over
	}
	t.Fatal("no fitting graph")
	return
}

func TestWorkflowMaxGraphLedger(t *testing.T) {
	f := newWorkflowFixture(t)
	f.install(t)
	// A 64-node chain where every node after the first consumes its
	// predecessor's result: 63 input edges, one acceptance.
	chain := func(pad string) workflow.Graph {
		var g workflow.Graph
		for i := range 64 {
			n := workflow.Node{Name: fmt.Sprintf("n%02d", i), Task: backend.Task{Argv: []string{"run", pad}, Outputs: []string{"out"}}}
			if i == 0 {
				n.Task.Workdir = workflowRef('a')
			} else {
				prev := fmt.Sprintf("n%02d", i-1)
				n.DependsOn = []string{prev}
				n.Input = &workflow.Input{Node: prev, Slot: "result"}
			}
			g.Nodes = append(g.Nodes, n)
		}
		return g
	}
	fit, over := workflowMaxGraph(t, chain)
	before := workflowDatabase(t, f.s)
	if _, err := f.s.SubmitWorkflow(f.alice, nil, "over", over, f.authority); err == nil {
		t.Fatal("oversized graph accepted")
	}
	workflowUnchanged(t, f.s, before)
	tooMany := chain("p")
	tooMany.Nodes = append(tooMany.Nodes, workflow.Node{Name: "extra", Task: backend.Task{Argv: []string{"x"}}})
	if _, err := f.s.SubmitWorkflow(f.alice, nil, "too-many", tooMany, f.authority); err == nil {
		t.Fatal("65-node graph accepted")
	}
	workflowUnchanged(t, f.s, before)

	w := f.submit(t, "max", fit)
	after := workflowDatabase(t, f.s)
	changed, created, writes := workflowDelta(before, after, "")
	t.Logf("parent acceptance: %d bytes, %d created, %d writes (estimate <=71 created)", changed, created, writes)
	if changed > maxChangeBytes || created > 100 {
		t.Fatalf("parent acceptance exceeds the transaction budget")
	}
	if edges := workflowEdges(t, f.s); len(edges) != 63 {
		t.Fatalf("edges: %d", len(edges))
	}
	if again := f.submit(t, "max", fit); again.ID != w.ID {
		t.Fatal("maximal replay")
	}
	workflowUnchanged(t, f.s, after)

	before = workflowDatabase(t, f.s)
	c := f.candidates(t)
	after = workflowDatabase(t, f.s)
	changed, created, writes = workflowDelta(before, after, "")
	t.Logf("candidate turn: %d bytes, %d created, %d writes", changed, created, writes)
	if len(c) != 1 {
		t.Fatalf("candidates: %+v", c)
	}
}

func TestWorkflowChildAdmissionLedger(t *testing.T) {
	f := newWorkflowFixture(t)
	f.install(t)
	w := f.submit(t, "child", workflowGraph("a"))
	c := f.candidates(t)[0]
	before := workflowDatabase(t, f.s)
	if _, err := f.s.AdmitWorkflowChild(f.admission(c)); err != nil {
		t.Fatal(err)
	}
	after := workflowDatabase(t, f.s)
	changed, created, writes := workflowDelta(before, after, "")
	t.Logf("child admission: %d bytes, %d created, %d writes (estimate <=11 created)", changed, created, writes)
	if created > 100 || changed > maxChangeBytes {
		t.Fatal("child admission exceeds the transaction budget")
	}
	f.get(t, w.ID)
}

func TestWorkflowLocalPublicationLedger(t *testing.T) {
	f := newWorkflowFixture(t)
	f.install(t)
	input := artifactTestComplete(t, f.s, f.alice)
	// One producer feeding 63 consumers fills 63 edges in one publication.
	g := workflow.Graph{Nodes: []workflow.Node{{Name: "make", Task: backend.Task{Argv: []string{"make"}, Workdir: &artifact.Ref{ID: input.ID, SHA256: input.SHA256, Size: int64(input.Size)}, Outputs: []string{"out"}}}}}
	for i := 1; i < 64; i++ {
		g.Nodes = append(g.Nodes, workflow.Node{Name: fmt.Sprintf("use%02d", i), Task: backend.Task{Argv: []string{"use"}}, DependsOn: []string{"make"}, Input: &workflow.Input{Node: "make", Slot: "result"}})
	}
	w := f.submit(t, "fan-out", g)
	j := f.admitOne(t, w, "make")
	j, start, err := f.s.BeginAttempt(j.ID, j.CurrentAttemptID(), 0, f.s.now())
	if err != nil || !start {
		t.Fatalf("begin: %v %v", start, err)
	}
	if j, err = f.s.ObserveAttempt(j.ID, j.CurrentAttemptID(), backend.Status{State: backend.Running, Instance: "i"}, f.s.now()); err != nil {
		t.Fatal(err)
	}
	before := workflowDatabase(t, f.s)
	ref := artifactRegressionComplete(t, f.s, j, false)
	after := workflowDatabase(t, f.s)
	changed, created, writes := workflowDelta(before, after, "artifacts/")
	t.Logf("local publication: %d artifact bytes, %d created, %d artifact writes (estimate <=70)", changed, created, writes)
	n := 0
	for _, e := range workflowEdges(t, f.s) {
		if e.Ref == nil || *e.Ref != ref {
			t.Fatalf("unfilled edge: %+v", e)
		}
		n++
	}
	if n != 63 {
		t.Fatalf("filled %d edges", n)
	}
	f.valid(t)
}

func TestWorkflowRemotePublicationLedger(t *testing.T) {
	f := newRemoteTransactionFixture(t)
	f.authenticate(t)
	g := workflow.Graph{Nodes: []workflow.Node{{Name: "make", Task: f.task(t)}}}
	for i := 1; i < 64; i++ {
		g.Nodes = append(g.Nodes, workflow.Node{Name: fmt.Sprintf("use%02d", i), Task: backend.Task{Argv: []string{"use"}}, DependsOn: []string{"make"}, Input: &workflow.Input{Node: "make", Slot: "result"}})
	}
	w, err := f.s.SubmitWorkflow(f.actor, nil, "remote-fan-out", g, f.authority)
	if err != nil {
		t.Fatal(err)
	}
	c, err := f.s.WorkflowCandidates()
	if err != nil || len(c) != 1 || c[0].Workflow != w.ID {
		t.Fatalf("candidates: %+v %v", c, err)
	}
	policy := f.policy
	j, err := f.s.AdmitWorkflowChild(WorkflowAdmission{Candidate: c[0], Target: "remote", Binding: "modeled-profile-v1", Policy: &policy})
	if err != nil {
		t.Fatal(err)
	}
	k, start, h := f.begin(t, j)
	row, err := f.s.RecordRemoteCreated(k, start.Revision, RemoteCreation{Resources: remoteTransactionCreated(), Succeeded: true}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	stop, collector := remoteTransactionStop(f, 0)
	zero := int32(0)
	row, err = f.s.ObserveRemote(k, row.Revision, backend.DataObservation{Status: backend.Status{State: backend.Succeeded, Instance: row.Resources.Pod.UID, ExitCode: &zero, Termination: backend.StoppedWithExitCode}, Phase: backend.DataSettled, TaskStop: &stop, Collector: &collector}, f.now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.PollRemoteRelease(k.AttemptID, h.Output, f.now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	output, err := f.s.CheckRemoteGrant(k.AttemptID, RemoteOutput, h.Output, f.now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	upload, err := f.s.BeginRemoteOutput(output, strings.Repeat("d", 64), 1, f.now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	file := ArtifactFile{Device: 17, Inode: 800}
	if upload, err = f.s.RemotePartCreated(output, ArtifactCursor{upload.Reference.ID, upload.Revision}, file, f.now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if upload, err = f.s.RemoteChunkSynced(output, ArtifactCursor{upload.Reference.ID, upload.Revision}, file, 0, 1, strings.Repeat("e", 64), f.now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if upload, err = f.s.BeginRemoteSeal(output, ArtifactCursor{upload.Reference.ID, upload.Revision}, f.now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	seal := RemoteOutputSeal{Reference: upload.Reference, File: file, Manifest: ArtifactManifest{Reference: upload.Reference, ExpandedBytes: 1, RegularFiles: 1}}
	if err = f.s.SealRemoteOutput(output, ArtifactCursor{upload.Reference.ID, upload.Revision}, seal, f.now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	before := workflowDatabase(t, f.s)
	completed, err := f.s.CompleteRemoteAttempt(k, RemoteCompletion{Outcome: DataMainExit, Stop: &stop, Collector: &collector, Candidate: &seal}, f.now.Add(4*time.Second))
	if err != nil || completed.Status.State != backend.Succeeded {
		t.Fatalf("publish: %+v %v", completed, err)
	}
	after := workflowDatabase(t, f.s)
	changed, created, writes := workflowDelta(before, after, "artifacts/")
	t.Logf("remote publication: %d artifact bytes, %d created, %d artifact writes (estimate <=67)", changed, created, writes)
	n := 0
	for _, e := range workflowEdges(t, f.s) {
		if e.Ref == nil || *e.Ref != seal.Reference || e.JobID != j.ID || e.AttemptID != k.AttemptID {
			t.Fatalf("unfilled edge: %+v", e)
		}
		n++
	}
	if n != 63 {
		t.Fatalf("filled %d edges", n)
	}
	if err = f.s.db.View(validateSnapshot); err != nil {
		t.Fatal(err)
	}
}

func TestWorkflowCapsDisabled(t *testing.T) {
	f := newWorkflowFixture(t)
	core := func() uint64 {
		var n uint64
		if err := f.s.db.View(func(tx *bolt.Tx) error {
			n = binary.BigEndian.Uint64(tx.Bucket(metadataBucket).Get(taskBytesKey))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return n
	}
	start := core()
	pad := strings.Repeat("p", 250<<10)
	var err error
	n := 0
	for ; n < 80; n++ {
		g := workflow.Graph{Nodes: []workflow.Node{{Name: "big", Task: backend.Task{Argv: []string{fmt.Sprint(n), pad}}}}}
		if _, err = f.s.SubmitWorkflow(f.alice, nil, fmt.Sprint("big-", n), g, f.authority); err != nil {
			break
		}
	}
	var capacity *CapacityError
	if !errors.As(err, &capacity) || capacity.Domain != TaskBytesQuota {
		t.Fatalf("after %d graphs: %v", n, err)
	}
	t.Logf("disabled retention refused graph %d at %d task bytes", n, capacity.Used)
	if got := core(); got != start {
		t.Fatalf("job task counter changed: %d -> %d", start, got)
	}
	// Ordinary disabled jobs keep their own limits.
	if _, err = f.s.Submit(f.alice, "ordinary", backend.Task{Argv: []string{"x"}}, "remote", "test-binding"); err != nil {
		t.Fatal(err)
	}
	f.valid(t)
}

func TestWorkflowCapsEnabled(t *testing.T) {
	f := newWorkflowFixture(t)
	p := DefaultRetentionPolicy()
	p.TaskBytes = 3000
	f.enable(t, p)
	graph := func(c string) workflow.Graph {
		return workflow.Graph{Nodes: []workflow.Node{{Name: "a", Task: backend.Task{Argv: []string{strings.Repeat(c, 1600)}}}}}
	}
	first := f.submit(t, "first", graph("a"))
	before := workflowDatabase(t, f.s)
	_, err := f.s.SubmitWorkflow(f.alice, f.domain, "second", graph("b"), f.authority)
	var capacity *CapacityError
	if !errors.As(err, &capacity) || capacity.Domain != TaskBytesQuota {
		t.Fatalf("over cap: %v", err)
	}
	workflowUnchanged(t, f.s, before)
	// Exact replay precedes capacity.
	if again := f.submit(t, "first", graph("a")); again.ID != first.ID {
		t.Fatal("replay at capacity")
	}
	workflowUnchanged(t, f.s, before)
}
