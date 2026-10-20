//go:build darwin || linux

package rpc_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
	"google.golang.org/protobuf/encoding/protojson"
)

func workspaceRPCInput(t *testing.T, f *artifactRPCFixture) *v1.ArtifactRef {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// A separate actual production service uploads the input and joins its own
	// sampler before this fixture tests queued-only workspace admission. Neither
	// the workspace Control.Run nor any workload is started.
	uploadService := f.uploadService(t)
	defer func() {
		if !uploadService.settle(t) {
			f.held = fmt.Errorf("temporary upload dependency: %w", uploadService.held)
			f.retain(t)
			t.Fatal("temporary upload service remains held; original fixture retained")
		}
	}()
	data, digest := artifactTar(t)
	upload, err := uploadService.beginUpload(t, ctx, digest, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	for offset := 0; offset < len(data); {
		end := min(offset+(64<<10), len(data))
		if _, err := uploadService.writeUpload(t, ctx, upload.Msg.Upload.Id, int64(offset), data[offset:end]); err != nil {
			t.Fatal(err)
		}
		offset = end
	}
	sealed, err := uploadService.finalizeUpload(t, ctx, upload.Msg.Upload.Id)
	if err != nil {
		t.Fatal(err)
	}
	return sealed.Msg.Reference
}

func TestWorkspaceHTTPInstalledDiscovery(t *testing.T) {
	for _, configured := range []bool{false, true} {
		t.Run(map[bool]string{false: "kind_only", true: "actual_config"}[configured], func(t *testing.T) {
			f := artifactRPCConfigured(t, nil, configured)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			info, err := f.client.GetServerInfo(ctx, authorized(f.tokens["reader"], &v1.GetServerInfoRequest{}))
			if err != nil {
				t.Fatal(err)
			}
			if !hasCapability(info.Msg.Capabilities, v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1) {
				t.Fatal("installed endpoint support missing")
			}
			want := f.control.WorkspaceTargetEnabled("test", f.binding)
			if len(info.Msg.Targets) != 1 || hasCapability(info.Msg.Targets[0].Capabilities, v1.Capability_CAPABILITY_WORKDIR_SNAPSHOT_V1) != want {
				t.Fatalf("target support=%+v want=%v", info.Msg.Targets, want)
			}
			if !configured && want {
				t.Fatal("kind local became workspace support")
			}
			targets, err := f.client.ListTargets(ctx, authorized(f.tokens["reader"], &v1.ListTargetsRequest{}))
			if err != nil || len(targets.Msg.Targets) != 1 || hasCapability(targets.Msg.Targets[0].Capabilities, v1.Capability_CAPABILITY_WORKDIR_SNAPSHOT_V1) != want {
				t.Fatalf("targets=%+v error=%v", targets, err)
			}
			f.control.StopArtifactAdmission()
			info, err = f.client.GetServerInfo(ctx, authorized(f.tokens["reader"], &v1.GetServerInfoRequest{}))
			if err != nil || !hasCapability(info.Msg.Capabilities, v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1) {
				t.Fatalf("pressure/stop erased installed support: %+v %v", info, err)
			}
		})
	}
}

func hasCapability(values []v1.Capability, want v1.Capability) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// Public Store admission/Begin supplies real durable members, without starting a
// workload or fabricating any positive writer, result or cleanup receipt.
func TestWorkspaceHTTPReplayAndResultMembership(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "explicit_attempt"}[retry], func(t *testing.T) {
			f := artifactRPC(t, nil)
			ref := workspaceRPCInput(t, f)
			actor, err := f.engine.Authenticate("Bearer " + f.tokens["alice"])
			if err != nil {
				t.Fatal(err)
			}
			task := backend.Task{Argv: []string{"never-started"}, Workdir: &artifact.Ref{ID: ref.Id, SHA256: ref.Sha256, Size: ref.Size}}
			wire := &v1.Task{Argv: []string{"never-started"}, Workdir: ref}
			if retry {
				task.Retry = &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7}}
				wire.Retry = &v1.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7}}
			}
			accepted, err := f.store.SubmitWorkspace(actor, nil, "original-workspace", task, "removed-local", "removed-binding", backend.RemoteOutput, 0)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			request := &v1.SubmitWorkspaceJobRequest{Task: wire, RequestId: "original-workspace"}
			replayed, err := f.client.SubmitWorkspaceJob(ctx, authorized(f.tokens["alice"], request))
			if err != nil || replayed.Msg.Job.Id != accepted.ID || replayed.Msg.Job.Target != "removed-local" || replayed.Msg.Job.Result == nil || replayed.Msg.Job.Result.State != v1.ArtifactResultState_ARTIFACT_RESULT_STATE_ABSENT {
				t.Fatalf("replay=%+v error=%v", replayed, err)
			}
			begun, start, err := f.store.BeginAttempt(accepted.ID, accepted.CurrentAttemptID(), 0, time.Now().UTC())
			if err != nil || !start {
				t.Fatalf("real Begin=%v %v", start, err)
			}
			got, err := f.client.GetJob(ctx, authorized(f.tokens["alice"], &v1.GetJobRequest{Id: accepted.ID}))
			if err != nil || got.Msg.Job.Result == nil || got.Msg.Job.Result.State != v1.ArtifactResultState_ARTIFACT_RESULT_STATE_HELD || got.Msg.Job.Result.Reference != nil || got.Msg.Job.Result.AttemptId != begun.CurrentAttemptID() {
				t.Fatalf("held=%+v error=%v", got, err)
			}
			if retry && (len(got.Msg.Job.Attempts) != 1 || got.Msg.Job.Attempts[0].Result == nil || got.Msg.Job.Attempts[0].Result.AttemptId != begun.CurrentAttemptID()) {
				t.Fatal("fixed attempt result omitted")
			}
			if _, err := f.client.GetJob(ctx, authorized(f.tokens["bob"], &v1.GetJobRequest{Id: accepted.ID})); connect.CodeOf(err) != connect.CodeNotFound {
				t.Fatalf("foreign member leaked: %v", err)
			}
			if _, err := f.client.SubmitWorkspaceJob(ctx, authorized(f.tokens["reader"], request)); connect.CodeOf(err) != connect.CodePermissionDenied {
				t.Fatalf("reader mutation=%v", err)
			}
			originalTask := request.Task
			request.Task = &v1.Task{Argv: []string{"changed"}, Workdir: ref}
			if _, err := f.client.SubmitWorkspaceJob(ctx, authorized(f.tokens["alice"], request)); connect.CodeOf(err) != connect.CodeAlreadyExists {
				t.Fatalf("replay conflict masked: %v", err)
			}
			jobs, err := f.client.ListJobs(ctx, authorized(f.tokens["alice"], &v1.ListJobsRequest{}))
			if err != nil || len(jobs.Msg.Jobs) != 1 || jobs.Msg.Jobs[0].Result == nil {
				t.Fatalf("list projection=%+v %v", jobs, err)
			}
			canceled, err := f.client.CancelJob(ctx, authorized(f.tokens["alice"], &v1.CancelJobRequest{Id: accepted.ID}))
			if err != nil || canceled.Msg.Job.Result == nil || canceled.Msg.Job.Result.AttemptId != begun.CurrentAttemptID() {
				t.Fatalf("cancel projection=%+v %v", canceled, err)
			}
			request.Task = originalTask
			workspaceReplayAfterStop(t, f, ctx, request, task, accepted.ID)
		})
	}
}

func TestWorkspaceHTTPLocalValidationAndOldMethods(t *testing.T) {
	f := artifactRPC(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, tt := range []struct {
		request *v1.SubmitWorkspaceJobRequest
		code    connect.Code
	}{
		{&v1.SubmitWorkspaceJobRequest{}, connect.CodeInvalidArgument},
		{&v1.SubmitWorkspaceJobRequest{RequestId: "valid", Task: &v1.Task{Argv: []string{"unused"}}}, connect.CodeInvalidArgument},
		{&v1.SubmitWorkspaceJobRequest{RequestId: "bad id", Task: &v1.Task{Argv: []string{"unused"}, Workdir: &v1.ArtifactRef{}}}, connect.CodeInvalidArgument},
		{&v1.SubmitWorkspaceJobRequest{RequestId: "valid", Task: &v1.Task{Argv: []string{"unused"}, Workdir: &v1.ArtifactRef{}}, Domain: &v1.RequestDomain{}}, connect.CodeFailedPrecondition},
	} {
		if _, err := f.client.SubmitWorkspaceJob(ctx, authorized(f.tokens["alice"], tt.request)); connect.CodeOf(err) != tt.code {
			t.Fatalf("invalid workspace code=%v want=%v", err, tt.code)
		}
	}
	ref := &v1.ArtifactRef{Id: strings.Repeat("a", 32), Sha256: strings.Repeat("b", 64), Size: 1024}
	task := &v1.Task{Argv: []string{"unused"}, Workdir: ref}
	for _, submit := range []func() error{
		func() error {
			_, err := f.client.SubmitJob(ctx, authorized(f.tokens["alice"], &v1.SubmitJobRequest{Task: task, RequestId: "old"}))
			return err
		},
		func() error {
			_, err := f.client.SubmitRetainedJob(ctx, authorized(f.tokens["alice"], &v1.SubmitRetainedJobRequest{Task: task, RequestId: "old"}))
			return err
		},
		func() error {
			_, err := f.client.SubmitRetryJob(ctx, authorized(f.tokens["alice"], &v1.SubmitJobRequest{Task: task, RequestId: "old"}))
			return err
		},
		func() error {
			_, err := f.client.PlanTask(ctx, authorized(f.tokens["alice"], &v1.PlanTaskRequest{Task: task}))
			return err
		},
		func() error {
			_, err := f.client.QuoteTask(ctx, authorized(f.tokens["alice"], &v1.QuoteTaskRequest{Task: task, Currency: "USD", Unit: v1.RateUnit_RATE_UNIT_TARGET_HOUR}))
			return err
		},
	} {
		if err := submit(); connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Fatalf("old method workspace=%v", err)
		}
	}
	before, err := f.store.LastSequenceFor(actorForRPC(t, f, "alice"))
	if err != nil {
		t.Fatal(err)
	}
	for i, body := range []string{`{"requestId":12}`, `{"requestId":"bad-wire","task":{"workdir":{"size":"bad"}}}`, `{"requestId":"large","task":{"argv":["` + strings.Repeat("x", 513<<10) + `"]}}`} {
		r, err := http.NewRequestWithContext(ctx, "POST", f.server.URL+skyv1connect.ControlServiceSubmitWorkspaceJobProcedure, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Authorization", "Bearer "+f.tokens["alice"])
		response, err := f.server.Client().Do(r)
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(io.LimitReader(response.Body, 4097))
		closeErr := response.Body.Close()
		var failure struct {
			Code string `json:"code"`
		}
		wantStatus, wantCode := http.StatusBadRequest, "invalid_argument"
		if i == 2 {
			wantStatus, wantCode = http.StatusTooManyRequests, "resource_exhausted"
		}
		if readErr != nil || closeErr != nil || len(data) > 4096 || response.StatusCode != wantStatus || json.Unmarshal(data, &failure) != nil || failure.Code != wantCode {
			t.Fatalf("malformed body status=%d body=%s read=%v close=%v", response.StatusCode, data, readErr, closeErr)
		}
	}
	after, err := f.store.LastSequenceFor(actorForRPC(t, f, "alice"))
	if err != nil || after != before {
		t.Fatalf("bad request accepted a job: %d→%d %v", before, after, err)
	}
}

func actorForRPC(t *testing.T, f *artifactRPCFixture, name string) auth.Actor {
	t.Helper()
	actor, err := f.engine.Authenticate("Bearer " + f.tokens[name])
	if err != nil {
		t.Fatal(err)
	}
	return actor
}

func TestWorkspaceHTTPDefaultResponseOmitted(t *testing.T) {
	f := artifactRPC(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	plain, err := f.client.SubmitJob(ctx, authorized(f.tokens["alice"], &v1.SubmitJobRequest{RequestId: "default", Task: &v1.Task{Argv: []string{"unused"}}}))
	if err != nil || plain.Msg.Job.Result != nil || plain.Msg.Job.Task.Workdir != nil {
		t.Fatalf("default=%+v %v", plain, err)
	}
	data, err := protojson.Marshal(plain.Msg.Job)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte(`"result"`)) || bytes.Contains(data, []byte(`"workdir"`)) {
		t.Fatalf("default fields appeared: %s", data)
	}
	uninstalled, err := control.New(f.store, []backend.Target{{Name: "plain", Binding: "plain", Backend: &logBackend{}}}, f.engine)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(rpc.NewHandler(uninstalled, f.engine))
	defer server.Close()
	client := skyv1connect.NewControlServiceClient(server.Client(), server.URL)
	info, err := client.GetServerInfo(ctx, authorized(f.tokens["reader"], &v1.GetServerInfoRequest{}))
	if err != nil || hasCapability(info.Msg.Capabilities, v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1) {
		t.Fatalf("uninstalled support=%+v %v", info, err)
	}
}

// The real explicit request domain is kept through the typed method. No current
// Local route or physical input preflight may be required for durable replay.
func TestWorkspaceHTTPExplicitDomainReplay(t *testing.T) {
	f := artifactRPCOptions(t, nil, false, true)
	ref := workspaceRPCInput(t, f)
	actor := actorForRPC(t, f, "alice")
	info, err := f.store.RetentionInfo(actor)
	if err != nil {
		t.Fatal(err)
	}
	task := backend.Task{Argv: []string{"never-started"}, Workdir: &artifact.Ref{ID: ref.Id, SHA256: ref.Sha256, Size: ref.Size}}
	accepted, err := f.store.SubmitWorkspace(actor, &info.Domain, "domain-replay", task, "old", "old-binding", backend.RemoteOutput, 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request := &v1.SubmitWorkspaceJobRequest{RequestId: "domain-replay", Domain: &v1.RequestDomain{Lineage: info.Domain.Lineage, Epoch: info.Domain.Epoch}, Task: &v1.Task{Argv: []string{"never-started"}, Workdir: ref}}
	replayed, err := f.client.SubmitWorkspaceJob(ctx, authorized(f.tokens["alice"], request))
	if err != nil || replayed.Msg.Job.Id != accepted.ID {
		t.Fatalf("domain replay=%+v %v", replayed, err)
	}
	request.Domain = nil
	if _, err := f.client.SubmitWorkspaceJob(ctx, authorized(f.tokens["alice"], request)); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("implicit domain accepted: %v", err)
	}
	originalDomain := &v1.RequestDomain{Lineage: info.Domain.Lineage, Epoch: info.Domain.Epoch}
	request.Domain = &v1.RequestDomain{Lineage: strings.Repeat("f", 32), Epoch: info.Domain.Epoch}
	if _, err := f.client.SubmitWorkspaceJob(ctx, authorized(f.tokens["alice"], request)); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("changed lineage accepted: %v", err)
	}
	request.Domain = originalDomain
	workspaceReplayAfterStop(t, f, ctx, request, task, accepted.ID)
}

// This is real Control MISS placement and Store admission, not execution. The
// reconciler is never run; the accepted workspace remains queued/unmaterialized.
func TestWorkspaceHTTPConfiguredMiss(t *testing.T) {
	f := artifactRPCConfigured(t, nil, true)
	ref := workspaceRPCInput(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request := &v1.SubmitWorkspaceJobRequest{RequestId: "configured-miss", Task: &v1.Task{Argv: []string{"never-started"}, Workdir: ref}}
	accepted, err := f.client.SubmitWorkspaceJob(ctx, authorized(f.tokens["alice"], request))
	if !f.control.WorkspaceTargetEnabled("test", f.binding) {
		if connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Fatalf("unsupported actual platform accepted: %+v %v", accepted, err)
		}
		return
	}
	if err != nil || accepted.Msg.Job.Target != "test" || accepted.Msg.Job.State != v1.JobState_JOB_STATE_QUEUED || accepted.Msg.Job.Result == nil || accepted.Msg.Job.Result.State != v1.ArtifactResultState_ARTIFACT_RESULT_STATE_ABSENT {
		t.Fatalf("actual MISS=%+v %v", accepted, err)
	}
	original, err := f.store.Get(accepted.Msg.Job.Id)
	if err != nil || original.Status.State != backend.Queued || len(original.Attempts) != 0 || original.Status.Instance != "" {
		t.Fatalf("admission executed workload: %+v %v", original, err)
	}
	again, err := f.client.SubmitWorkspaceJob(ctx, authorized(f.tokens["alice"], request))
	if err != nil || again.Msg.Job.Id != accepted.Msg.Job.Id {
		t.Fatalf("configured HTTP replay=%+v %v", again, err)
	}
	workspaceReplayAfterStop(t, f, ctx, request, original.Task, accepted.Msg.Job.Id)
}

// Control replay precedes current placement and artifact availability. HTTP
// admission still requires its own original call even for a durable replay.
func workspaceReplayAfterStop(t *testing.T, f *artifactRPCFixture, ctx context.Context, request *v1.SubmitWorkspaceJobRequest, task backend.Task, wantID string) {
	t.Helper()
	f.control.StopArtifactAdmission()
	actor := actorForRPC(t, f, "alice")
	var replayed backend.Job
	var err error
	if request.Domain == nil {
		replayed, err = f.control.SubmitWorkspace(actor, request.RequestId, task)
	} else {
		domain := store.RequestDomain{Lineage: request.Domain.Lineage, Epoch: request.Domain.Epoch}
		replayed, err = f.control.SubmitWorkspaceInDomain(actor, domain, request.RequestId, task)
	}
	if err != nil || replayed.ID != wantID {
		t.Fatalf("Control replay after artifact stop=%+v %v", replayed, err)
	}
	body, err := protojson.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	workspaceAdmissionRefused(t, f, ctx, skyv1connect.ControlServiceSubmitWorkspaceJobProcedure, string(body), http.StatusServiceUnavailable)
	got, err := f.client.GetJob(ctx, authorized(f.tokens["alice"], &v1.GetJobRequest{Id: wantID}))
	if err != nil || got.Msg.Job.Id != wantID || got.Msg.Job.Result == nil {
		t.Fatalf("workspace metadata after artifact stop=%+v %v", got, err)
	}
}
