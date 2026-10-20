//go:build darwin || linux

package rpc_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/rpc"
	"google.golang.org/protobuf/encoding/protojson"
)

// UNRUN generated-client source oracle. Regeneration of inherited stale
// bindings needs a separate release; no generator/compiler was run here.
// Actual public Store admission supplies a queued member and actual Files input
// grant. Replay works independently of native Local execution prerequisites.
func TestWorkspaceHTTPKeepsLargeTaskEnvelopeAndQueuedReplay(t *testing.T) {
	f := artifactRPCConfigured(t, nil, true)
	ref := workspaceRPCInput(t, f)
	actor := actorForRPC(t, f, "alice")
	argv := []string{"never-started", strings.Repeat("x", 32<<10)}
	request := &v1.SubmitWorkspaceJobRequest{RequestId: "large-workspace-replay", Task: &v1.Task{Argv: argv, Workdir: ref}}
	wire, err := protojson.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) <= 16<<10 || len(wire) >= 512<<10 {
		t.Fatalf("actual JSON fixture size=%d", len(wire))
	}
	accepted, err := f.store.SubmitWorkspace(actor, nil, request.RequestId, backend.Task{Argv: argv, Workdir: &artifact.Ref{ID: ref.Id, SHA256: ref.Sha256, Size: ref.Size}}, "removed-local", "removed-binding", backend.RemoteOutput, 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		replayed, err := f.client.SubmitWorkspaceJob(ctx, authorized(f.tokens["alice"], request))
		if err != nil || replayed.Msg.Job.Id != accepted.ID || replayed.Msg.Job.State != v1.JobState_JOB_STATE_QUEUED || len(replayed.Msg.Job.Task.Argv) != 2 || replayed.Msg.Job.Task.Argv[1] != argv[1] {
			t.Fatalf("large actual queued replay=%v %v", replayed, err)
		}
		f.awaitReturned(t, skyv1connect.ControlServiceSubmitWorkspaceJobProcedure, 1)
	}
	// This proves durable QUEUED replay across the preserved wire envelope,
	// not new-MISS HTTP acceptance or fresh physical-growth qualification. The
	// original workspace Control stays unrun; no native work is qualified.
	// Valid JSON padding exceeds the transport envelope while leaving the task
	// well within its independent semantic 256KiB bound. This must fail before a
	// new requestID can mutate the actual Store's global acceptance sequence.
	over := &v1.SubmitWorkspaceJobRequest{RequestId: "oversize-workspace-wire", Task: request.Task}
	data, err := protojson.Marshal(over)
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, bytes.Repeat([]byte(" "), (513<<10)-len(data))...)
	before, err := f.store.LastSequence()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := http.NewRequestWithContext(ctx, http.MethodPost, f.server.URL+skyv1connect.ControlServiceSubmitWorkspaceJobProcedure, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	raw.Header.Set("Authorization", "Bearer "+f.tokens["alice"])
	raw.Header.Set("Content-Type", "application/json")
	response, err := f.server.Client().Do(raw)
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, 4097))
	closeErr := response.Body.Close()
	var failure struct {
		Code string `json:"code"`
	}
	if readErr != nil || closeErr != nil || len(body) > 4096 || response.StatusCode != http.StatusTooManyRequests || json.Unmarshal(body, &failure) != nil || failure.Code != "resource_exhausted" {
		t.Fatalf("oversize actual HTTP status=%d body=%s read=%v close=%v", response.StatusCode, body, readErr, closeErr)
	}
	f.awaitReturned(t, skyv1connect.ControlServiceSubmitWorkspaceJobProcedure, 1)
	after, err := f.store.LastSequence()
	if err != nil || after != before {
		t.Fatalf("oversize decode changed actual sequence %d -> %d: %v", before, after, err)
	}
}

// The installed endpoint's admission stop and the artifact-off service both
// reject workspace HTTP before dispatch. Ordinary RPC methods keep their own
// independent behavior; no transport refusal is called Control semantic proof.
func TestWorkspaceHTTPStoppedAdmissionPreservesOrdinaryRPCs(t *testing.T) {
	f := artifactRPC(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	before, err := f.store.LastSequence()
	if err != nil {
		t.Fatal(err)
	}
	f.control.StopArtifactAdmission()
	_, err = f.client.SubmitWorkspaceJob(ctx, authorized(f.tokens["alice"], &v1.SubmitWorkspaceJobRequest{}))
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("stopped workspace HTTP=%v", err)
	}
	after, err := f.store.LastSequence()
	if err != nil || after != before {
		t.Fatal("stopped transport dispatched workspace mutation")
	}
	ordinary, err := f.client.SubmitJob(ctx, authorized(f.tokens["alice"], &v1.SubmitJobRequest{RequestId: "ordinary-after-artifact-stop", Task: &v1.Task{Argv: []string{"never-started"}}}))
	if err != nil || ordinary.Msg.Job.State != v1.JobState_JOB_STATE_QUEUED {
		t.Fatalf("ordinary SubmitJob changed with artifact stop: %v %v", ordinary, err)
	}
}

func TestWorkspaceHTTPArtifactOffRejectsBeforeDispatch(t *testing.T) {
	f := artifactRPC(t, nil)
	c, err := control.New(f.store, []backend.Target{{Name: "plain", Binding: "plain", Backend: &logBackend{}}}, f.engine)
	if err != nil {
		t.Fatal(err)
	}
	h := rpc.NewHandler(c, f.engine)
	server := httptest.NewServer(h)
	defer server.Close()
	client := skyv1connect.NewControlServiceClient(server.Client(), server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	before, err := f.store.LastSequence()
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.SubmitWorkspaceJob(ctx, authorized(f.tokens["alice"], &v1.SubmitWorkspaceJobRequest{}))
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("artifact-off workspace HTTP=%v", err)
	}
	_, err = client.GetArtifactUpload(ctx, authorized(f.tokens["alice"], &v1.GetArtifactUploadRequest{Id: strings.Repeat("a", 32)}))
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("artifact-off ordinary artifact HTTP=%v", err)
	}
	after, err := f.store.LastSequence()
	if err != nil || after != before {
		t.Fatal("artifact-off admission dispatched mutation")
	}
	// No off-service request was admitted; this SAME handler positively joins its
	// empty original set. Ordinary metadata still works after artifact stop.
	joined := make(chan struct{})
	go func() { h.WaitArtifactHTTP(); close(joined) }()
	workspaceOwnerEvent(t, f, joined, "artifact-off empty HTTP owner set")
	if _, err := client.GetServerInfo(ctx, authorized(f.tokens["alice"], &v1.GetServerInfoRequest{})); err != nil {
		t.Fatalf("ordinary discovery changed with artifact-off stop: %v", err)
	}
}
