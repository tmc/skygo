package rpc_test

import (
	"bytes"
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
	"google.golang.org/protobuf/proto"
)

// This checks wire admission and storage. The backend does not enforce resources.
func TestTaskExtensions(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	engine := auth.NewLegacy("")
	actor, err := engine.Authenticate("")
	if err != nil {
		t.Fatal(err)
	}
	targets := []backend.Target{
		{Name: "a", Kind: "same", Binding: "route-a", Backend: &logBackend{}, Labels: map[string]string{"pool": "small"}},
		{Name: "b", Kind: "same", Binding: "route-b", Backend: &logBackend{}, Labels: map[string]string{"pool": "batch"}, AcceleratorNames: []string{"gpu"}, SupportsResourceLimits: true, SupportsNodeSelector: true},
	}
	c, err := control.New(s, targets, engine)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(rpc.NewHandler(c, engine))
	defer srv.Close()
	client := skyv1connect.NewControlServiceClient(srv.Client(), srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	input := &v1.Task{Argv: []string{"program", "雪"}, Env: map[string]string{"A": "B"}, Directory: "/work", Image: "image", CpuMillis: 100, MemoryBytes: 1024, Accelerator: "gpu", AcceleratorCount: 1, TimeoutSeconds: 3, CpuLimitMillis: 200, MemoryLimitBytes: 2048, TargetLabels: map[string]string{"pool": "batch"}, NodeSelector: map[string]string{"zone": "west"}}
	plan, err := client.PlanTask(ctx, connect.NewRequest(&v1.PlanTaskRequest{Task: input}))
	if err != nil || plan.Msg.Target != "b" {
		t.Fatalf("plan = %v, %v", plan, err)
	}
	submitted, err := client.SubmitJob(ctx, connect.NewRequest(&v1.SubmitJobRequest{RequestId: "extensions", Task: input}))
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(submitted.Msg.Job.Task, input) || submitted.Msg.Job.Target != "b" {
		t.Fatalf("submitted = %v", submitted.Msg)
	}
	got, err := client.GetJob(ctx, connect.NewRequest(&v1.GetJobRequest{Id: submitted.Msg.Job.Id}))
	if err != nil || !proto.Equal(got.Msg.Job.Task, input) {
		t.Fatalf("get = %v, %v", got, err)
	}
	listed, err := client.ListJobs(ctx, connect.NewRequest(&v1.ListJobsRequest{}))
	if err != nil || len(listed.Msg.Jobs) != 1 || !proto.Equal(listed.Msg.Jobs[0].Task, input) {
		t.Fatalf("list = %v, %v", listed, err)
	}
	stored, err := c.Get(actor, submitted.Msg.Job.Id)
	if err != nil {
		t.Fatal(err)
	}
	wantBytes, _ := backend.TaskBytes(backend.Task{Argv: input.Argv, Env: input.Env, Directory: input.Directory, Image: input.Image, CPUMillis: input.CpuMillis, MemoryBytes: input.MemoryBytes, Accelerator: input.Accelerator, AcceleratorCount: input.AcceleratorCount, TimeoutSeconds: input.TimeoutSeconds, CPULimitMillis: input.CpuLimitMillis, MemoryLimitBytes: input.MemoryLimitBytes, TargetLabels: input.TargetLabels, NodeSelector: input.NodeSelector})
	gotBytes, _ := backend.TaskBytes(stored.Task)
	if !bytes.Equal(gotBytes, wantBytes) || stored.TargetBinding != "route-b" {
		t.Fatalf("stored task/binding changed: %s, %q", gotBytes, stored.TargetBinding)
	}
	info, err := client.GetServerInfo(ctx, connect.NewRequest(&v1.GetServerInfoRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	want := []*v1.Target{
		{Name: "a", Kind: "same", Labels: map[string]string{"pool": "small"}, Capabilities: []v1.Capability{1, 4}},
		{Name: "b", Kind: "same", Labels: map[string]string{"pool": "batch"}, AcceleratorNames: []string{"gpu"}, Capabilities: []v1.Capability{1, 2, 3, 4}},
	}
	if !proto.Equal(info.Msg, &v1.GetServerInfoResponse{Targets: want}) {
		t.Fatalf("info = %v", info.Msg)
	}
	advertised, err := client.ListTargets(ctx, connect.NewRequest(&v1.ListTargetsRequest{}))
	if err != nil || !proto.Equal(advertised.Msg, &v1.ListTargetsResponse{Targets: want}) {
		t.Fatalf("targets = %v, %v", advertised, err)
	}
	info.Msg.Targets[1].Labels["pool"] = "changed"
	info.Msg.Targets[1].AcceleratorNames[0] = "changed"
	again, err := client.GetServerInfo(ctx, connect.NewRequest(&v1.GetServerInfoRequest{}))
	if err != nil || !proto.Equal(again.Msg, &v1.GetServerInfoResponse{Targets: want}) {
		t.Fatalf("metadata mutated = %v, %v", again, err)
	}
	invalid := proto.Clone(input).(*v1.Task)
	invalid.CpuLimitMillis = 50
	_, err = client.SubmitJob(ctx, connect.NewRequest(&v1.SubmitJobRequest{RequestId: "invalid", Task: invalid}))
	code(t, err, connect.CodeInvalidArgument)
	jobs, _, err := c.List(actor, 100, "")
	if err != nil || len(jobs) != 1 {
		t.Fatalf("invalid submission changed jobs: %v, %v", jobs, err)
	}
}
