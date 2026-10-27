package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/skycompat/tasklower"
	"google.golang.org/protobuf/proto"
)

const dataTaskYAML = `argv: [echo, hello]
target: data
directory: /task
data_mounts:
  - {reference: dataset-z, destination: /mnt/skygo/z, read_only: false}
  - {reference: dataset-a, destination: /mnt/skygo/a, read_only: true}
`

func executeDataTask(ctx context.Context, endpoint, input string, args ...string) (string, string, error) {
	cmd := New(nil)
	cmd.SetArgs(append([]string{"--endpoint", endpoint, "--token", "data-token"}, args...))
	cmd.SetIn(strings.NewReader(input))
	var out, diag bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&diag)
	err := cmd.ExecuteContext(ctx)
	return out.String(), diag.String(), err
}

func TestDataTaskYAML(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		want        string
	}{
		{"ordered false and true", dataTaskYAML, ""},
		{"omitted false", strings.Replace(dataTaskYAML, ", read_only: false", "", 1), ""},
		{"unknown mount key", strings.Replace(dataTaskYAML, "read_only: false", "readonly: false", 1), "field readonly"},
		{"duplicate mount key", strings.Replace(dataTaskYAML, "read_only: false", "read_only: false, read_only: true", 1), "already defined"},
		{"overlap", strings.Replace(dataTaskYAML, "/mnt/skygo/a", "/mnt/skygo/z/sub", 1), "overlapping"},
		{"invalid alias", strings.Replace(dataTaskYAML, "dataset-z", "Bad/Alias", 1), "alias"},
		{"relative path", strings.Replace(dataTaskYAML, "/mnt/skygo/z", "relative", 1), "destination"},
		{"multiple docs", dataTaskYAML + "---\nargv: [other]\n", "exactly one"},
		{"oversize", strings.Repeat(" ", (1<<20)+1), "exceeds 1 MiB"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			cmd.SetIn(strings.NewReader(tc.input))
			task, err := readTask(cmd, "-", tasklower.Options{})
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("error=%v; want %s", err, tc.want)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := []*v1.DataMount{{Reference: "dataset-z", Destination: "/mnt/skygo/z", ReadOnly: false}, {Reference: "dataset-a", Destination: "/mnt/skygo/a", ReadOnly: true}}
			if len(task.DataMounts) != 2 || !proto.Equal(task.DataMounts[0], want[0]) || !proto.Equal(task.DataMounts[1], want[1]) {
				t.Fatalf("mounts=%v", task.DataMounts)
			}
			if task.TimeoutSeconds != 0 {
				t.Fatal("default timeout rewritten")
			}
		})
	}
}

func TestDataPreflightRefusesBeforeSubmission(t *testing.T) {
	for _, mode := range []string{"missing-endpoint", "missing-target", "mounts-off", "missing-policy", "invalid-policy", "duplicate-target", "unimplemented", "unauthenticated", "unavailable"} {
		t.Run(mode, func(t *testing.T) {
			peer, srv := newDataProtocolPeer(t)
			peer.setMode(mode)
			_, _, err := executeDataTask(context.Background(), srv.URL, dataTaskYAML, "run", "--remote-data", "-f", "-")
			if err == nil {
				t.Fatal("unsupported request succeeded")
			}
			if peer.count("GetServerInfo") != 1 || peer.count("ALL") != 1 {
				t.Fatalf("side effects: calls=%d", peer.count("ALL"))
			}
		})
	}
}

func TestDataTargetCapabilityAndPolicyIsolation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*v1.GetServerInfoResponse)
	}{
		{"only policy", func(i *v1.GetServerInfoResponse) { i.Targets[0].Capabilities = nil }},
		{"capability on other target", func(i *v1.GetServerInfoResponse) {
			i.Targets[0].Capabilities = nil
			i.Targets[1].Capabilities = append(i.Targets[1].Capabilities, v1.Capability_CAPABILITY_REMOTE_DATA_V1, v1.Capability_CAPABILITY_DATA_MOUNTS_V1)
		}},
		{"zero policy", func(i *v1.GetServerInfoResponse) { i.Targets[0].DataPolicy = &v1.DataPolicy{} }},
		{"negative policy", func(i *v1.GetServerInfoResponse) { i.Targets[0].DataPolicy.StageSeconds = -1 }},
		{"overflow policy", func(i *v1.GetServerInfoResponse) { i.Targets[0].DataPolicy.MaxTaskSeconds = 1 << 62 }},
		{"nil target", func(i *v1.GetServerInfoResponse) { i.Targets = append(i.Targets, nil) }},
		{"unnamed target", func(i *v1.GetServerInfoResponse) { i.Targets = append(i.Targets, &v1.Target{}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer, srv := newDataProtocolPeer(t)
			info := dataProtocolInfo()
			tc.change(info)
			peer.mu.Lock()
			peer.info = info
			peer.mu.Unlock()
			_, _, err := executeDataTask(context.Background(), srv.URL, dataTaskYAML, "run", "--remote-data", "-f", "-")
			if err == nil || peer.count("ALL") != 1 {
				t.Fatalf("err=%v calls=%d", err, peer.count("ALL"))
			}
		})
	}
}

func TestDataRetryNeedsBothCapabilities(t *testing.T) {
	input := dataTaskYAML + "retry: {max_attempts: 2, delay_seconds: 0, exit_codes: [7]}\n"
	for _, remove := range []v1.Capability{v1.Capability_CAPABILITY_FINITE_JOB_RETRY_V1, v1.Capability_CAPABILITY_REMOTE_DATA_RETRY_V1} {
		t.Run(capabilityName(remove), func(t *testing.T) {
			peer, srv := newDataProtocolPeer(t)
			info := dataProtocolInfo()
			var caps []v1.Capability
			for _, c := range info.Targets[0].Capabilities {
				if c != remove {
					caps = append(caps, c)
				}
			}
			info.Targets[0].Capabilities = caps
			peer.mu.Lock()
			peer.info = info
			peer.mu.Unlock()
			_, _, err := executeDataTask(context.Background(), srv.URL, input, "run", "--remote-data", "-f", "-")
			if err == nil || peer.count("SubmitDataJob") != 0 {
				t.Fatalf("err=%v submit=%d", err, peer.count("SubmitDataJob"))
			}
		})
	}
	peer, srv := newDataProtocolPeer(t)
	_, _, err := executeDataTask(context.Background(), srv.URL, input, "run", "--remote-data", "-f", "-", "--request-id", "retry")
	if err != nil || peer.count("SubmitDataJob") != 1 || peer.count("SubmitRetryJob") != 0 {
		t.Fatalf("err=%v", err)
	}
	if got := peer.snapshotRequests()[0].Task.Retry; got == nil || got.MaxAttempts != 2 || len(got.ExitCodes) != 1 || got.ExitCodes[0] != 7 {
		t.Fatalf("retry=%v", got)
	}
}

func TestDataReplayProbeIsNarrow(t *testing.T) {
	domain := strings.Repeat("a", 32) + ":1"
	for _, tc := range []struct {
		name, mode string
		args       []string
		probe      bool
	}{
		{"drift explicit", "drift", []string{"--request-domain", domain, "--request-id", "same"}, true},
		{"policy drift", "invalid-policy", []string{"--request-domain", domain, "--request-id", "same"}, true},
		{"old discovery", "unimplemented", []string{"--request-domain", domain, "--request-id", "same"}, true},
		{"ID alone", "drift", []string{"--request-id", "same"}, false},
		{"domain alone", "drift", []string{"--request-domain", domain}, false},
		{"no hints", "drift", nil, false},
		{"revoked auth", "unauthenticated", []string{"--request-domain", domain, "--request-id", "same"}, false},
		{"transport", "unavailable", []string{"--request-domain", domain, "--request-id", "same"}, false},
		{"malformed", "duplicate-target", []string{"--request-domain", domain, "--request-id", "same"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer, srv := newDataProtocolPeer(t)
			peer.setMode(tc.mode)
			args := append([]string{"run", "--remote-data", "-f", "-"}, tc.args...)
			_, _, err := executeDataTask(context.Background(), srv.URL, dataTaskYAML, args...)
			want := 0
			if tc.probe {
				want = 1
			}
			if peer.count("SubmitDataJob") != want {
				t.Fatalf("err=%v probes=%d want=%d", err, peer.count("SubmitDataJob"), want)
			}
			if peer.count("PlanTask") != 0 || peer.count("BeginArtifactUpload") != 0 || peer.count("SubmitJob") != 0 || peer.count("SubmitWorkspaceJob") != 0 {
				t.Fatal("probe escaped data route")
			}
			if tc.mode == "drift" && err == nil {
				t.Fatal("protocol-only new ID under drift succeeded")
			}
		})
	}
}

func TestDataRunAndPlanWire(t *testing.T) {
	peer, srv := newDataProtocolPeer(t)
	if _, _, err := executeDataTask(context.Background(), srv.URL, dataTaskYAML, "plan", "--remote-data", "-f", "-"); err != nil {
		t.Fatal(err)
	}
	if peer.count("PlanTask") != 1 || peer.count("SubmitDataJob") != 0 {
		t.Fatal("plan mutated")
	}
	out, diag, err := executeDataTask(context.Background(), srv.URL, dataTaskYAML, "run", "--remote-data", "-f", "-", "--request-id", "wire")
	if err != nil || out == "" || !strings.Contains(diag, "request-id: wire") {
		t.Fatalf("out=%q diag=%q err=%v", out, diag, err)
	}
	requests := peer.snapshotRequests()
	if len(requests) != 1 {
		t.Fatalf("requests=%d", len(requests))
	}
	got := requests[0]
	if got.RequestId != "wire" || got.Domain != nil || got.Task.Target != "data" || got.Task.TimeoutSeconds != 0 || len(got.Task.DataMounts) != 2 || got.Task.DataMounts[0].ReadOnly || !got.Task.DataMounts[1].ReadOnly || got.Task.DataMounts[0].Reference != "dataset-z" {
		t.Fatalf("request=%v", got)
	}
	got.Task.DataMounts[0].Reference = "mutated-copy"
	if peer.snapshotRequests()[0].Task.DataMounts[0].Reference != "dataset-z" {
		t.Fatal("peer request snapshot aliases source")
	}
	for _, m := range []string{"SubmitJob", "SubmitWorkspaceJob", "SubmitRetryJob", "SubmitRetainedJob", "BeginArtifactUpload"} {
		if peer.count(m) != 0 {
			t.Fatalf("unexpected %s", m)
		}
	}
}

func TestDataPlanKeepsLocalRouteExplicit(t *testing.T) {
	peer, srv := newDataProtocolPeer(t)
	input := "argv: [echo]\ntarget: data\nworkdir: {id: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, sha256: bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb, size: 2048}\n"
	if _, _, err := executeDataTask(context.Background(), srv.URL, input, "plan", "-f", "-"); err == nil || !strings.Contains(err.Error(), "workdir-snapshot-v1") {
		t.Fatalf("default plan err=%v", err)
	}
	if _, _, err := executeDataTask(context.Background(), srv.URL, input, "plan", "--remote-data", "-f", "-"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := executeDataTask(context.Background(), srv.URL, input, "run", "--remote-data", "-f", "-", "--request-id", "workdir"); err != nil {
		t.Fatal(err)
	}
	r := peer.snapshotRequests()[0]
	if r.Task.Workdir == nil || r.Task.Workdir.Id != strings.Repeat("a", 32) || len(r.Task.DataMounts) != 0 {
		t.Fatalf("workdir=%v", r.Task)
	}
	if peer.count("SubmitWorkspaceJob") != 0 {
		t.Fatal("remote Workdir used Local submit")
	}
}

func TestDataInvalidInputsNeverDiscover(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		args        []string
	}{
		{"no explicit target", strings.Replace(dataTaskYAML, "target: data\n", "", 1), nil},
		{"negative timeout", dataTaskYAML + "timeout_seconds: -1\n", nil},
		{"too long", dataTaskYAML + "timeout_seconds: 604801\n", nil},
		{"no data", "argv: [echo]\ntarget: data\n", nil},
		{"quote", dataTaskYAML, []string{"plan", "--objective", "lowest-rate", "--currency", "USD"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer, srv := newDataProtocolPeer(t)
			args := tc.args
			if len(args) == 0 {
				args = []string{"run"}
			}
			args = append(args, "--remote-data", "-f", "-")
			_, _, err := executeDataTask(context.Background(), srv.URL, tc.input, args...)
			if err == nil {
				t.Fatal("invalid data accepted")
			}
			if tc.name == "quote" && !strings.Contains(err.Error(), "remote data") {
				t.Fatalf("wrong quote refusal: %v", err)
			}
			if peer.count("ALL") != 0 {
				t.Fatal("invalid data reached network")
			}
		})
	}
}

func TestDataContextCancellation(t *testing.T) {
	peer, srv := newDataProtocolPeer(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := executeDataTask(ctx, srv.URL, dataTaskYAML, "run", "--remote-data", "-f", "-"); err == nil {
		t.Fatal("canceled command succeeded")
	}
	if peer.count("SubmitDataJob") != 0 {
		t.Fatal("canceled command submitted")
	}
}

func TestDataDiscoveryRefusalDoesNotBecomeProbe(t *testing.T) {
	for _, code := range []connect.Code{connect.CodeFailedPrecondition, connect.CodePermissionDenied, connect.CodeDeadlineExceeded} {
		t.Run(code.String(), func(t *testing.T) {
			service := &dataDiscoveryError{code: code}
			path, handler := skyv1connect.NewControlServiceHandler(service)
			mux := http.NewServeMux()
			mux.Handle(path, handler)
			srv := httptest.NewServer(mux)
			t.Cleanup(srv.Close)
			_, _, err := executeDataTask(context.Background(), srv.URL, dataTaskYAML, "run", "--remote-data", "-f", "-", "--request-domain", strings.Repeat("a", 32)+":1", "--request-id", "same")
			if connect.CodeOf(err) != code || service.submits.Load() != 0 {
				t.Fatalf("err=%v submits=%d", err, service.submits.Load())
			}
		})
	}
}

type dataDiscoveryError struct {
	skyv1connect.UnimplementedControlServiceHandler
	code    connect.Code
	submits atomic.Int32
}

func (s *dataDiscoveryError) GetServerInfo(context.Context, *connect.Request[v1.GetServerInfoRequest]) (*connect.Response[v1.GetServerInfoResponse], error) {
	return nil, connect.NewError(s.code, errors.New("server-side refusal"))
}
func (s *dataDiscoveryError) SubmitDataJob(context.Context, *connect.Request[v1.SubmitDataJobRequest]) (*connect.Response[v1.SubmitDataJobResponse], error) {
	s.submits.Add(1)
	return nil, errors.New("unexpected submission")
}

// A deadline limits the existing command context, not a new per-stage budget.
func TestDataPreflightDeadline(t *testing.T) {
	peer, srv := newDataProtocolPeer(t)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if _, _, err := executeDataTask(ctx, srv.URL, dataTaskYAML, "plan", "--remote-data", "-f", "-"); err == nil {
		t.Fatal("expired plan succeeded")
	}
	if peer.count("PlanTask") != 0 || peer.count("SubmitDataJob") != 0 {
		t.Fatal("expired command crossed preflight")
	}
}

func TestDataMountCountBound(t *testing.T) {
	input := "argv: [echo]\ntarget: data\ndata_mounts:\n"
	for i := 0; i < 17; i++ {
		input += fmt.Sprintf("  - {reference: data-%02d, destination: /mnt/skygo/data-%02d, read_only: false}\n", i, i)
	}
	peer, srv := newDataProtocolPeer(t)
	if _, _, err := executeDataTask(context.Background(), srv.URL, input, "run", "--remote-data", "-f", "-"); err == nil || !strings.Contains(err.Error(), "16") {
		t.Fatalf("err=%v", err)
	}
	if peer.count("ALL") != 0 {
		t.Fatal("oversized mount list reached server")
	}
}

func TestDataInfoReadback(t *testing.T) {
	_, srv := newDataProtocolPeer(t)
	out, _, err := execute(context.Background(), "--endpoint", srv.URL, "--token", "data-token", "info", "--target", "data", "--require", "remote-data-transfer-v1,remote-data-v1,data-mounts-v1,remote-data-retry-v1")
	if err != nil || !strings.Contains(out, "remote-data-transfer-v1") || !strings.Contains(out, "data-policy data: stage=600 collection=600 default-task=86400 max-task=604800 shutdown=120 seconds") {
		t.Fatalf("out=%q err=%v", out, err)
	}
	out, _, err = execute(context.Background(), "--endpoint", srv.URL, "--token", "data-token", "--json", "info", "--target", "data", "--require", "remote-data-v1")
	if err != nil || !strings.Contains(out, `"data_policy"`) || !strings.Contains(out, `"max_task_seconds":"604800"`) {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

// Empty wire replies do not manufacture an accepted job or selected target.
func TestDataResponseRefusals(t *testing.T) {
	peer := &dataEmptyResponse{}
	path, handler := skyv1connect.NewControlServiceHandler(peer)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	for _, verb := range []string{"plan", "run"} {
		out, _, err := executeDataTask(context.Background(), srv.URL, dataTaskYAML, verb, "--remote-data", "-f", "-")
		if err == nil || out != "" {
			t.Fatalf("%s out=%q err=%v", verb, out, err)
		}
	}
}

type dataEmptyResponse struct {
	skyv1connect.UnimplementedControlServiceHandler
}

func (*dataEmptyResponse) GetServerInfo(context.Context, *connect.Request[v1.GetServerInfoRequest]) (*connect.Response[v1.GetServerInfoResponse], error) {
	return connect.NewResponse(dataProtocolInfo()), nil
}
func (*dataEmptyResponse) GetRetention(context.Context, *connect.Request[v1.GetRetentionRequest]) (*connect.Response[v1.GetRetentionResponse], error) {
	return connect.NewResponse(&v1.GetRetentionResponse{}), nil
}
func (*dataEmptyResponse) PlanTask(context.Context, *connect.Request[v1.PlanTaskRequest]) (*connect.Response[v1.PlanTaskResponse], error) {
	return connect.NewResponse(&v1.PlanTaskResponse{Target: "other"}), nil
}
func (*dataEmptyResponse) SubmitDataJob(context.Context, *connect.Request[v1.SubmitDataJobRequest]) (*connect.Response[v1.SubmitDataJobResponse], error) {
	return connect.NewResponse(&v1.SubmitDataJobResponse{}), nil
}
