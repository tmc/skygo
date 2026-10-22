//go:build darwin || linux

package rpc_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
	"github.com/tmc/skygo/internal/workflow"
	"google.golang.org/protobuf/proto"
)

// workflowFixture serves the real handler over real Control, Store and file
// authority. No Run is started, so no node is admitted.
type workflowFixture struct {
	path      string
	tokens    map[string]string
	ids       map[string]string
	domain    *v1.RequestDomain
	store     *store.Store
	engine    *auth.Engine
	control   *control.Control
	jobs      skyv1connect.ControlServiceClient
	workflows skyv1connect.WorkflowServiceClient
}

func newWorkflowFixture(t *testing.T, retained bool) *workflowFixture {
	t.Helper()
	dir := t.TempDir()
	// t.TempDir honors the host umask; auth requires a private parent.
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(dir, "state")
	f := &workflowFixture{path: filepath.Join(dir, "auth.json"), tokens: make(map[string]string), ids: make(map[string]string)}
	if retained {
		policy := store.DefaultRetentionPolicy()
		policy.LocalOutputEnabled = false
		info, err := store.EnableRetention(state, policy)
		if err != nil {
			t.Fatal(err)
		}
		f.domain = &v1.RequestDomain{Lineage: info.Domain.Lineage, Epoch: info.Domain.Epoch}
	}
	s, err := store.Open(state)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := auth.Initialize(f.path, filepath.Join(dir, "legacy.token"), s.Owner()); err != nil {
		t.Fatal(err)
	}
	for _, p := range []struct {
		name string
		role auth.Role
	}{{"alice", auth.Operator}, {"bob", auth.Operator}, {"reader", auth.Reader}, {"admin", auth.Admin}} {
		if err := auth.AddPrincipal(f.path, p.name, p.role); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, p.name+".token")
		info, err := auth.Issue(f.path, p.name, path, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		f.tokens[p.name], f.ids[p.name] = strings.TrimSuffix(string(data), "\n"), info.ID
	}
	if err := s.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	engine, err := auth.NewFile(f.path, s.Owner())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); engine.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	f.store, f.engine = s, engine
	f.control, err = control.New(s, []backend.Target{{Name: "test", Kind: "test", Binding: "test-route", Backend: &logBackend{}}}, engine)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(rpc.NewHandler(f.control, engine))
	t.Cleanup(server.Close)
	f.jobs = skyv1connect.NewControlServiceClient(server.Client(), server.URL)
	f.workflows = skyv1connect.NewWorkflowServiceClient(server.Client(), server.URL)
	return f
}

func (f *workflowFixture) submit(t *testing.T, principal, request string, g *v1.WorkflowGraph) *v1.Workflow {
	t.Helper()
	r, err := f.workflows.SubmitWorkflow(context.Background(), authorized(f.tokens[principal], &v1.SubmitWorkflowRequest{RequestId: request, Graph: g, Domain: f.domain}))
	if err != nil {
		t.Fatal(err)
	}
	return r.Msg.Workflow
}

// seed stores the Store form of plainWireGraph("a") with real delegated
// authority. It bypasses Control's admission guards, not Store's.
func (f *workflowFixture) seed(t *testing.T, principal string, domain *store.RequestDomain, request string) store.Workflow {
	t.Helper()
	actor, err := f.engine.Authenticate("Bearer " + f.tokens[principal])
	if err != nil {
		t.Fatal(err)
	}
	authority, err := f.engine.Delegate(actor)
	if err != nil {
		t.Fatal(err)
	}
	w, err := f.store.SubmitWorkflow(actor, domain, request, workflow.Graph{Nodes: []workflow.Node{{Name: "a", Task: backend.Task{Argv: []string{"work", "a"}}}}}, authority)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// all lists every workflow visible to principal.
func (f *workflowFixture) all(t *testing.T, principal string) []string {
	t.Helper()
	var ids []string
	token := ""
	for {
		r, err := f.workflows.ListWorkflows(context.Background(), authorized(f.tokens[principal], &v1.ListWorkflowsRequest{PageSize: 200, PageToken: token}))
		if err != nil {
			t.Fatal(err)
		}
		for _, w := range r.Msg.Workflows {
			ids = append(ids, w.Id)
		}
		if token = r.Msg.NextPageToken; token == "" {
			return ids
		}
	}
}

func plainWireGraph(names ...string) *v1.WorkflowGraph {
	g := &v1.WorkflowGraph{}
	for _, name := range names {
		g.Nodes = append(g.Nodes, &v1.WorkflowNode{Name: name, Task: &v1.Task{Argv: []string{"work", name}}})
	}
	return g
}

func expiryDetail(t *testing.T, err error) *v1.RetentionExpiry {
	t.Helper()
	var typed *connect.Error
	if !errors.As(err, &typed) {
		t.Fatalf("untyped error %v", err)
	}
	for _, detail := range typed.Details() {
		value, err := detail.Value()
		if err != nil {
			t.Fatal(err)
		}
		if expiry, ok := value.(*v1.RetentionExpiry); ok {
			return expiry
		}
	}
	t.Fatalf("no retention expiry detail in %v", err)
	return nil
}

// workflowRef is well formed; acceptance never opens a producer's workdir.
func workflowRef() *v1.ArtifactRef {
	return &v1.ArtifactRef{Id: strings.Repeat("a", 32), Sha256: strings.Repeat("b", 64), Size: 1}
}

// templateWireGraph has a workdir producer, a template consumer of its
// result, and a data node whose mount is explicitly not read-only. Its
// dependency lists are deliberately unsorted.
func templateWireGraph() *v1.WorkflowGraph {
	return &v1.WorkflowGraph{Nodes: []*v1.WorkflowNode{
		{Name: "build", Task: &v1.Task{Argv: []string{"make"}, Env: map[string]string{"MODE": "release"}, Workdir: workflowRef(), Outputs: []string{"out"}, Retry: &v1.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{1}}}},
		{Name: "test", Task: &v1.Task{Argv: []string{"test"}, Setup: []string{"prepare"}, Outputs: []string{"report"}}, DependsOn: []string{"build"}, Input: &v1.WorkflowInput{Node: "build", Slot: "result"}},
		{Name: "train", Task: &v1.Task{Argv: []string{"trainer"}, Image: "modeled-image", DataMounts: []*v1.DataMount{{Reference: "dataset", Destination: "/mnt/skygo/dataset", ReadOnly: false}}}, DependsOn: []string{"test", "build"}},
	}}
}

// templateGraph is templateWireGraph written independently in Go form; its
// canonical digest is the expected graph_sha256.
func templateGraph() workflow.Graph {
	ref := &artifact.Ref{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Size: 1}
	return workflow.Graph{Nodes: []workflow.Node{
		{Name: "build", Task: backend.Task{Argv: []string{"make"}, Env: map[string]string{"MODE": "release"}, Workdir: ref, Outputs: []string{"out"}, Retry: &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{1}}}},
		{Name: "test", Task: backend.Task{Argv: []string{"test"}, Setup: []string{"prepare"}, Outputs: []string{"report"}}, DependsOn: []string{"build"}, Input: &workflow.Input{Node: "build", Slot: "result"}},
		{Name: "train", Task: backend.Task{Argv: []string{"trainer"}, Image: "modeled-image", DataMounts: []backend.DataMount{{Reference: "dataset", Destination: "/mnt/skygo/dataset"}}}, DependsOn: []string{"build", "test"}},
	}}
}

func TestWorkflowRPCRoundTrip(t *testing.T) {
	f := artifactRPCOptions(t, nil, false, false)
	canonical, err := workflow.Canonical(templateGraph())
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(canonical)
	wantGraph := templateWireGraph()
	wantGraph.Nodes[2].DependsOn = []string{"build", "test"}
	codecs := []struct {
		name    string
		options []connect.ClientOption
	}{{"proto", nil}, {"json", []connect.ClientOption{connect.WithProtoJSON()}}}
	ids := make(map[string]string)
	for _, codec := range codecs {
		t.Run(codec.name, func(t *testing.T) {
			client := skyv1connect.NewWorkflowServiceClient(f.server.Client(), f.server.URL, codec.options...)
			ctx := context.Background()
			request := &v1.SubmitWorkflowRequest{RequestId: "round-" + codec.name, Graph: templateWireGraph()}
			submitted, err := client.SubmitWorkflow(ctx, authorized(f.tokens["alice"], request))
			if err != nil {
				t.Fatal(err)
			}
			w := submitted.Msg.Workflow
			ids[codec.name] = w.Id
			if !proto.Equal(w.Graph, wantGraph) {
				t.Fatalf("graph = %v, want %v", w.Graph, wantGraph)
			}
			if w.Graph.Nodes[2].Task.DataMounts[0] == nil || w.Graph.Nodes[2].Task.DataMounts[0].ReadOnly {
				t.Fatal("explicit read-write mount changed")
			}
			if w.GraphSha256 != hex.EncodeToString(digest[:]) || w.CreatedAt == nil || w.CancelRequested {
				t.Fatalf("header = %q %v %v", w.GraphSha256, w.CreatedAt, w.CancelRequested)
			}
			if len(w.Nodes) != 3 {
				t.Fatalf("nodes = %v", w.Nodes)
			}
			for i, n := range w.Nodes {
				if n.Name != wantGraph.Nodes[i].Name || n.Disposition != v1.WorkflowDisposition_WORKFLOW_DISPOSITION_PENDING || n.JobId != "" || n.ChildState != v1.JobState_JOB_STATE_UNSPECIFIED || n.Input != nil {
					t.Fatalf("node %d status invented: %v", i, n)
				}
			}
			replay, err := client.SubmitWorkflow(ctx, authorized(f.tokens["alice"], request))
			if err != nil || !proto.Equal(replay.Msg.Workflow, w) {
				t.Fatalf("replay = %v, %v", replay, err)
			}
			got, err := client.GetWorkflow(ctx, authorized(f.tokens["alice"], &v1.GetWorkflowRequest{Id: w.Id}))
			if err != nil || !proto.Equal(got.Msg.Workflow, w) {
				t.Fatalf("get = %v, %v", got, err)
			}
			list, err := client.ListWorkflows(ctx, authorized(f.tokens["alice"], &v1.ListWorkflowsRequest{}))
			if err != nil {
				t.Fatal(err)
			}
			want := &v1.WorkflowSummary{Id: w.Id, GraphSha256: w.GraphSha256, CreatedAt: w.CreatedAt}
			if !slices.ContainsFunc(list.Msg.Workflows, func(s *v1.WorkflowSummary) bool { return proto.Equal(s, want) }) || list.Msg.NextPageToken != "" {
				t.Fatalf("list = %v, want summary %v", list.Msg, want)
			}
			canceled, err := client.CancelWorkflow(ctx, authorized(f.tokens["alice"], &v1.CancelWorkflowRequest{Id: w.Id}))
			if err != nil {
				t.Fatal(err)
			}
			c := canceled.Msg.Workflow
			if !c.CancelRequested || c.Id != w.Id || !proto.Equal(c.Graph, w.Graph) {
				t.Fatalf("cancel = %v", c)
			}
			for i, n := range c.Nodes {
				if n.Disposition != v1.WorkflowDisposition_WORKFLOW_DISPOSITION_CANCELED || n.JobId != "" || n.ChildState != v1.JobState_JOB_STATE_UNSPECIFIED {
					t.Fatalf("canceled node %d = %v", i, n)
				}
			}
			again, err := client.CancelWorkflow(ctx, authorized(f.tokens["alice"], &v1.CancelWorkflowRequest{Id: w.Id}))
			if err != nil || !proto.Equal(again.Msg.Workflow, c) {
				t.Fatalf("repeated cancel = %v, %v", again, err)
			}
		})
	}
	if ids["proto"] == "" || ids["proto"] == ids["json"] {
		t.Fatalf("distinct requests share an identity: %v", ids)
	}
}

func TestWorkflowRPCReplayAndConflict(t *testing.T) {
	f := newWorkflowFixture(t, false)
	ctx := context.Background()
	first := f.submit(t, "alice", "same", plainWireGraph("a", "b"))
	replay := f.submit(t, "alice", "same", plainWireGraph("a", "b"))
	if replay.Id != first.Id {
		t.Fatalf("replay %s, want %s", replay.Id, first.Id)
	}
	_, err := f.workflows.SubmitWorkflow(ctx, authorized(f.tokens["alice"], &v1.SubmitWorkflowRequest{RequestId: "same", Graph: plainWireGraph("a")}))
	code(t, err, connect.CodeAlreadyExists)
	// Request identity is per principal.
	if other := f.submit(t, "bob", "same", plainWireGraph("a", "b")); other.Id == first.Id {
		t.Fatal("bob replayed alice's workflow")
	}
	if ids := f.all(t, "alice"); len(ids) != 1 || ids[0] != first.Id {
		t.Fatalf("alice workflows = %v", ids)
	}
}

func TestWorkflowRPCDomain(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		f := newWorkflowFixture(t, false)
		ctx := context.Background()
		_, err := f.workflows.SubmitWorkflow(ctx, authorized(f.tokens["alice"], &v1.SubmitWorkflowRequest{RequestId: "domain", Graph: plainWireGraph("a"), Domain: &v1.RequestDomain{Lineage: strings.Repeat("a", 32), Epoch: 1}}))
		code(t, err, connect.CodeFailedPrecondition)
		f.submit(t, "alice", "nil-domain", plainWireGraph("a"))
		if ids := f.all(t, "admin"); len(ids) != 1 {
			t.Fatalf("workflows = %v", ids)
		}
	})
	t.Run("enabled", func(t *testing.T) {
		f := newWorkflowFixture(t, true)
		ctx := context.Background()
		submit := func(domain *v1.RequestDomain) error {
			_, err := f.workflows.SubmitWorkflow(ctx, authorized(f.tokens["alice"], &v1.SubmitWorkflowRequest{RequestId: "domain", Graph: plainWireGraph("a"), Domain: domain}))
			return err
		}
		err := submit(nil)
		code(t, err, connect.CodeFailedPrecondition)
		if e := expiryDetail(t, err); e.Reason != v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_REQUEST_DOMAIN_REQUIRED || e.JobId != "" {
			t.Fatalf("nil domain expiry = %v", e)
		}
		err = submit(&v1.RequestDomain{Lineage: strings.Repeat("1", 32), Epoch: 1})
		code(t, err, connect.CodeFailedPrecondition)
		if e := expiryDetail(t, err); e.Reason != v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_REQUEST_LINEAGE_CHANGED || e.JobId != "" {
			t.Fatalf("foreign lineage expiry = %v", e)
		}
		for _, d := range []*v1.RequestDomain{{}, {Lineage: f.domain.Lineage}, {Lineage: "not-hex", Epoch: 1}} {
			code(t, submit(d), connect.CodeInvalidArgument)
		}
		if ids := f.all(t, "admin"); len(ids) != 0 {
			t.Fatalf("refused domains accepted %v", ids)
		}
		// Fresh retained acceptance needs Run's storage sample, which this
		// fixture never takes, so it is refused. Sampled Control acceptance is
		// control's TestWorkflowListCursor; this subtest covers only the RPC
		// domain path and the replay that precedes the storage guard.
		code(t, submit(f.domain), connect.CodeUnavailable)
		if ids := f.all(t, "admin"); len(ids) != 0 {
			t.Fatalf("unsampled submission accepted %v", ids)
		}
		seeded := f.seed(t, "alice", &store.RequestDomain{Lineage: f.domain.Lineage, Epoch: f.domain.Epoch}, "domain")
		if replay := f.submit(t, "alice", "domain", plainWireGraph("a")); replay.Id != seeded.ID {
			t.Fatalf("domained replay = %s, want seeded %s", replay.Id, seeded.ID)
		}
		_, err = f.workflows.SubmitWorkflow(ctx, authorized(f.tokens["alice"], &v1.SubmitWorkflowRequest{RequestId: "fresh", Graph: plainWireGraph("a"), Domain: f.domain}))
		code(t, err, connect.CodeUnavailable)
		if ids := f.all(t, "admin"); len(ids) != 1 || ids[0] != seeded.ID {
			t.Fatalf("workflows = %v, want only seeded %s", ids, seeded.ID)
		}
	})
}

// TestWorkflowRPCAudit checks that each workflow handler enqueues exactly one
// valid access event. No Run drains the fixed 256-entry queue: with 255
// entries queued, a missing event leaves the probe slot free, and an extra or
// invalid one marks audit unhealthy. Persisted rows are not read.
func TestWorkflowRPCAudit(t *testing.T) {
	for _, tt := range []struct {
		name string
		call func(f *workflowFixture, id string) error
		want connect.Code
	}{
		{"submit", func(f *workflowFixture, _ string) error {
			_, err := f.workflows.SubmitWorkflow(context.Background(), authorized(f.tokens["alice"], &v1.SubmitWorkflowRequest{RequestId: "audit-submit", Graph: plainWireGraph("a")}))
			return err
		}, 0},
		{"get", func(f *workflowFixture, id string) error {
			_, err := f.workflows.GetWorkflow(context.Background(), authorized(f.tokens["alice"], &v1.GetWorkflowRequest{Id: id}))
			return err
		}, 0},
		{"list", func(f *workflowFixture, _ string) error {
			_, err := f.workflows.ListWorkflows(context.Background(), authorized(f.tokens["alice"], &v1.ListWorkflowsRequest{}))
			return err
		}, 0},
		{"cancel", func(f *workflowFixture, id string) error {
			_, err := f.workflows.CancelWorkflow(context.Background(), authorized(f.tokens["alice"], &v1.CancelWorkflowRequest{Id: id}))
			return err
		}, 0},
		{"foreign-get", func(f *workflowFixture, id string) error {
			_, err := f.workflows.GetWorkflow(context.Background(), authorized(f.tokens["bob"], &v1.GetWorkflowRequest{Id: id}))
			return err
		}, connect.CodeNotFound},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newWorkflowFixture(t, false)
			// Store seeding records no access event.
			w := f.seed(t, "alice", nil, "audit")
			alice, err := f.engine.Authenticate("Bearer " + f.tokens["alice"])
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 255; i++ {
				if err := f.control.RecordAccess(alice, "plan", "", "allowed"); err != nil {
					t.Fatal(err)
				}
			}
			if err := tt.call(f, w.ID); tt.want != 0 {
				code(t, err, tt.want)
			} else if err != nil {
				t.Fatal(err)
			}
			if !f.control.AuditHealthy() {
				t.Fatal("workflow handler rejected or overflowed its audit category")
			}
			if err := f.control.RecordAccess(alice, "plan", "", "allowed"); err == nil || err.Error() != "audit queue full" {
				t.Fatalf("workflow handler did not enqueue its access event: %v", err)
			}
		})
	}
}

func TestWorkflowRPCScope(t *testing.T) {
	f := newWorkflowFixture(t, false)
	ctx := context.Background()
	w := f.submit(t, "alice", "mine", plainWireGraph("a"))
	for _, principal := range []string{"bob", "reader"} {
		_, err := f.workflows.GetWorkflow(ctx, authorized(f.tokens[principal], &v1.GetWorkflowRequest{Id: w.Id}))
		code(t, err, connect.CodeNotFound)
		if ids := f.all(t, principal); len(ids) != 0 {
			t.Fatalf("%s lists %v", principal, ids)
		}
	}
	_, err := f.workflows.CancelWorkflow(ctx, authorized(f.tokens["bob"], &v1.CancelWorkflowRequest{Id: w.Id}))
	code(t, err, connect.CodeNotFound)
	_, err = f.workflows.GetWorkflow(ctx, authorized(f.tokens["bob"], &v1.GetWorkflowRequest{Id: strings.Repeat("c", 32)}))
	code(t, err, connect.CodeNotFound)
	_, err = f.workflows.CancelWorkflow(ctx, authorized(f.tokens["reader"], &v1.CancelWorkflowRequest{Id: w.Id}))
	code(t, err, connect.CodePermissionDenied)
	_, err = f.workflows.SubmitWorkflow(ctx, authorized(f.tokens["reader"], &v1.SubmitWorkflowRequest{RequestId: "read", Graph: plainWireGraph("a")}))
	code(t, err, connect.CodePermissionDenied)
	// Authority is checked before the request is parsed.
	_, err = f.workflows.SubmitWorkflow(ctx, authorized(f.tokens["reader"], &v1.SubmitWorkflowRequest{RequestId: "read"}))
	code(t, err, connect.CodePermissionDenied)
	got, err := f.workflows.GetWorkflow(ctx, authorized(f.tokens["alice"], &v1.GetWorkflowRequest{Id: w.Id}))
	if err != nil || got.Msg.Workflow.CancelRequested {
		t.Fatalf("foreign cancel changed workflow: %v %v", got, err)
	}
	if ids := f.all(t, "admin"); len(ids) != 1 || ids[0] != w.Id {
		t.Fatalf("admin lists %v", ids)
	}
	_, err = f.workflows.GetWorkflow(ctx, connect.NewRequest(&v1.GetWorkflowRequest{Id: w.Id}))
	code(t, err, connect.CodeUnauthenticated)
}

func TestWorkflowRPCListCursor(t *testing.T) {
	f := newWorkflowFixture(t, false)
	ctx := context.Background()
	var want []string
	for i := range 3 {
		want = append(want, f.submit(t, "alice", fmt.Sprint("list-", i), plainWireGraph("a")).Id)
	}
	list := func(principal, token string, size int32) (*v1.ListWorkflowsResponse, error) {
		r, err := f.workflows.ListWorkflows(ctx, authorized(f.tokens[principal], &v1.ListWorkflowsRequest{PageSize: size, PageToken: token}))
		if err != nil {
			return nil, err
		}
		return r.Msg, nil
	}
	page, err := list("alice", "", 1)
	if err != nil || len(page.Workflows) != 1 || page.Workflows[0].Id != want[0] || page.NextPageToken == "" {
		t.Fatalf("first page = %v, %v", page, err)
	}
	token := page.NextPageToken
	// The listing is live: a workflow accepted now appears on a later page.
	want = append(want, f.submit(t, "alice", "list-late", plainWireGraph("a")).Id)
	got := []string{page.Workflows[0].Id}
	for token != "" {
		page, err = list("alice", token, 1)
		if err != nil || len(page.Workflows) != 1 {
			t.Fatalf("page = %v, %v", page, err)
		}
		got = append(got, page.Workflows[0].Id)
		token = page.NextPageToken
	}
	if !slices.Equal(got, want) {
		t.Fatalf("listing %v, want %v", got, want)
	}
	first, err := list("alice", "", 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = list("bob", first.NextPageToken, 1)
	code(t, err, connect.CodeInvalidArgument)
	for _, id := range []string{"job-a", "job-b"} {
		if _, err := f.jobs.SubmitJob(ctx, authorized(f.tokens["alice"], &v1.SubmitJobRequest{RequestId: id, Task: &v1.Task{Argv: []string{"unused"}}})); err != nil {
			t.Fatal(err)
		}
	}
	jobs, err := f.jobs.ListJobs(ctx, authorized(f.tokens["alice"], &v1.ListJobsRequest{PageSize: 1}))
	if err != nil || jobs.Msg.NextPageToken == "" {
		t.Fatalf("jobs = %v, %v", jobs, err)
	}
	_, err = list("alice", jobs.Msg.NextPageToken, 1)
	code(t, err, connect.CodeInvalidArgument)
	_, err = f.jobs.ListJobs(ctx, authorized(f.tokens["alice"], &v1.ListJobsRequest{PageSize: 1, PageToken: first.NextPageToken}))
	code(t, err, connect.CodeInvalidArgument)
	for _, tt := range []struct {
		token string
		size  int32
	}{{"%%%", 1}, {strings.Repeat("A", 700), 1}, {"", -1}, {"", 201}} {
		_, err = list("alice", tt.token, tt.size)
		code(t, err, connect.CodeInvalidArgument)
	}
	if page, err = list("alice", "", 0); err != nil || len(page.Workflows) != 4 || page.NextPageToken != "" {
		t.Fatalf("default page = %v, %v", page, err)
	}
}

func TestWorkflowRPCInvalidInput(t *testing.T) {
	f := newWorkflowFixture(t, false)
	ctx := context.Background()
	node := func(name string, deps ...string) *v1.WorkflowNode {
		return &v1.WorkflowNode{Name: name, Task: &v1.Task{Argv: []string{name}}, DependsOn: deps}
	}
	var wide []*v1.WorkflowNode
	for i := range 65 {
		wide = append(wide, node(fmt.Sprint("n", i)))
	}
	tests := []struct {
		name    string
		request string
		graph   *v1.WorkflowGraph
	}{
		{"no graph", "r", nil},
		{"no nodes", "r", &v1.WorkflowGraph{}},
		{"no task", "r", &v1.WorkflowGraph{Nodes: []*v1.WorkflowNode{{Name: "a"}}}},
		{"empty argv", "r", &v1.WorkflowGraph{Nodes: []*v1.WorkflowNode{{Name: "a", Task: &v1.Task{}}}}},
		{"bad name", "r", &v1.WorkflowGraph{Nodes: []*v1.WorkflowNode{node("a b")}}},
		{"duplicate name", "r", &v1.WorkflowGraph{Nodes: []*v1.WorkflowNode{node("a"), node("a")}}},
		{"self dependency", "r", &v1.WorkflowGraph{Nodes: []*v1.WorkflowNode{node("a", "a")}}},
		{"missing dependency", "r", &v1.WorkflowGraph{Nodes: []*v1.WorkflowNode{node("a", "b")}}},
		{"cycle", "r", &v1.WorkflowGraph{Nodes: []*v1.WorkflowNode{node("a", "b"), node("b", "a")}}},
		{"too many nodes", "r", &v1.WorkflowGraph{Nodes: wide}},
		{"input slot", "r", &v1.WorkflowGraph{Nodes: []*v1.WorkflowNode{
			{Name: "build", Task: &v1.Task{Argv: []string{"make"}, Workdir: workflowRef(), Outputs: []string{"out"}}},
			{Name: "test", Task: &v1.Task{Argv: []string{"test"}}, DependsOn: []string{"build"}, Input: &v1.WorkflowInput{Node: "build", Slot: "other"}},
		}}},
		{"input without dependency", "r", &v1.WorkflowGraph{Nodes: []*v1.WorkflowNode{
			{Name: "build", Task: &v1.Task{Argv: []string{"make"}, Workdir: workflowRef(), Outputs: []string{"out"}}},
			{Name: "test", Task: &v1.Task{Argv: []string{"test"}}, Input: &v1.WorkflowInput{Node: "build", Slot: "result"}},
		}}},
		{"template setup", "r", &v1.WorkflowGraph{Nodes: []*v1.WorkflowNode{
			{Name: "build", Task: &v1.Task{Argv: []string{"make"}, Workdir: workflowRef(), Outputs: []string{"out"}}},
			{Name: "test", Task: &v1.Task{Argv: []string{"test"}, Setup: []string{""}}, DependsOn: []string{"build"}, Input: &v1.WorkflowInput{Node: "build", Slot: "result"}},
		}}},
		{"outputs without workdir", "r", &v1.WorkflowGraph{Nodes: []*v1.WorkflowNode{{Name: "a", Task: &v1.Task{Argv: []string{"a"}, Outputs: []string{"out"}}}}}},
		{"request id", "bad id", plainWireGraph("a")},
		{"empty request id", "", plainWireGraph("a")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := f.workflows.SubmitWorkflow(ctx, authorized(f.tokens["alice"], &v1.SubmitWorkflowRequest{RequestId: tt.request, Graph: tt.graph}))
			code(t, err, connect.CodeInvalidArgument)
		})
	}
	for _, id := range []string{"", "x", strings.Repeat("G", 32)} {
		_, err := f.workflows.GetWorkflow(ctx, authorized(f.tokens["alice"], &v1.GetWorkflowRequest{Id: id}))
		code(t, err, connect.CodeInvalidArgument)
		_, err = f.workflows.CancelWorkflow(ctx, authorized(f.tokens["alice"], &v1.CancelWorkflowRequest{Id: id}))
		code(t, err, connect.CodeInvalidArgument)
	}
	if ids := f.all(t, "admin"); len(ids) != 0 {
		t.Fatalf("invalid input accepted %v", ids)
	}
}

func TestWorkflowRPCRevocation(t *testing.T) {
	f := newWorkflowFixture(t, false)
	ctx := context.Background()
	kept := f.submit(t, "alice", "before", plainWireGraph("a"))
	if err := auth.Revoke(f.path, f.ids["alice"]); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := f.workflows.GetWorkflow(ctx, authorized(f.tokens["alice"], &v1.GetWorkflowRequest{Id: kept.Id}))
		if connect.CodeOf(err) == connect.CodeUnauthenticated {
			break
		}
		if err != nil || time.Now().After(deadline) {
			t.Fatalf("revocation not observed: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, err := f.workflows.SubmitWorkflow(ctx, authorized(f.tokens["alice"], &v1.SubmitWorkflowRequest{RequestId: "after", Graph: plainWireGraph("a")}))
	code(t, err, connect.CodeUnauthenticated)
	_, err = f.workflows.SubmitWorkflow(ctx, authorized(f.tokens["alice"], &v1.SubmitWorkflowRequest{RequestId: "before", Graph: plainWireGraph("a")}))
	code(t, err, connect.CodeUnauthenticated)
	_, err = f.workflows.CancelWorkflow(ctx, authorized(f.tokens["alice"], &v1.CancelWorkflowRequest{Id: kept.Id}))
	code(t, err, connect.CodeUnauthenticated)
	_, err = f.workflows.ListWorkflows(ctx, authorized(f.tokens["alice"], &v1.ListWorkflowsRequest{}))
	code(t, err, connect.CodeUnauthenticated)
	if ids := f.all(t, "admin"); len(ids) != 1 || ids[0] != kept.Id {
		t.Fatalf("workflows after revocation = %v", ids)
	}
}

func TestWorkflowRPCArtifactAdmissionStopped(t *testing.T) {
	f := artifactRPCOptions(t, nil, false, false)
	client := skyv1connect.NewWorkflowServiceClient(f.server.Client(), f.server.URL)
	ctx := context.Background()
	request := &v1.SubmitWorkflowRequest{RequestId: "first", Graph: templateWireGraph()}
	first, err := client.SubmitWorkflow(ctx, authorized(f.tokens["alice"], request))
	if err != nil {
		t.Fatal(err)
	}
	f.control.StopArtifactAdmission()
	// Replay precedes the transient refusal; a new acceptance is refused.
	replay, err := client.SubmitWorkflow(ctx, authorized(f.tokens["alice"], request))
	if err != nil || replay.Msg.Workflow.Id != first.Msg.Workflow.Id {
		t.Fatalf("replay = %v, %v", replay, err)
	}
	_, err = client.SubmitWorkflow(ctx, authorized(f.tokens["alice"], &v1.SubmitWorkflowRequest{RequestId: "second", Graph: templateWireGraph()}))
	code(t, err, connect.CodeUnavailable)
	list, err := client.ListWorkflows(ctx, authorized(f.tokens["alice"], &v1.ListWorkflowsRequest{}))
	if err != nil || len(list.Msg.Workflows) != 1 {
		t.Fatalf("list = %v, %v", list, err)
	}
}

func TestWorkflowRPCNoArtifactService(t *testing.T) {
	f := newWorkflowFixture(t, false)
	_, err := f.workflows.SubmitWorkflow(context.Background(), authorized(f.tokens["alice"], &v1.SubmitWorkflowRequest{RequestId: "input", Graph: templateWireGraph()}))
	code(t, err, connect.CodeUnavailable)
	if ids := f.all(t, "admin"); len(ids) != 0 {
		t.Fatalf("workflows = %v", ids)
	}
}

func TestWorkflowRPCCapability(t *testing.T) {
	f := newWorkflowFixture(t, false)
	ctx := context.Background()
	for _, principal := range []string{"alice", "reader"} {
		info, err := f.jobs.GetServerInfo(ctx, authorized(f.tokens[principal], &v1.GetServerInfoRequest{}))
		if err != nil {
			t.Fatal(err)
		}
		if !hasCapability(info.Msg.Capabilities, v1.Capability_CAPABILITY_STATIC_WORKFLOWS_V1) {
			t.Fatalf("%s: endpoint capability missing: %v", principal, info.Msg.Capabilities)
		}
		for _, target := range info.Msg.Targets {
			if hasCapability(target.Capabilities, v1.Capability_CAPABILITY_STATIC_WORKFLOWS_V1) {
				t.Fatalf("workflow capability claimed for target %s", target.Name)
			}
		}
	}
	// A legacy credential cannot capture file authority: no advertisement and
	// no acceptance, through either service.
	legacy := setup(t, "legacy-token", false)
	info, err := legacy.client.GetServerInfo(ctx, authorized("legacy-token", &v1.GetServerInfoRequest{}))
	if err != nil || hasCapability(info.Msg.Capabilities, v1.Capability_CAPABILITY_STATIC_WORKFLOWS_V1) {
		t.Fatalf("legacy info = %v, %v", info, err)
	}
	workflows := skyv1connect.NewWorkflowServiceClient(legacy.server.Client(), legacy.server.URL)
	_, err = workflows.SubmitWorkflow(ctx, authorized("legacy-token", &v1.SubmitWorkflowRequest{RequestId: "legacy", Graph: plainWireGraph("a")}))
	code(t, err, connect.CodeUnauthenticated)
	jobs, _, err := legacy.control.List(legacy.actor, 100, "")
	if err != nil || len(jobs) != 0 {
		t.Fatalf("workflow submission fell back to jobs: %v %v", jobs, err)
	}
}
