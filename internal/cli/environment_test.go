package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/environmentwire"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"rsc.io/script/scripttest"
)

// envLineage is the fixture's retention lineage.
const envLineage = "0123456789abcdef0123456789abcdef"

// envInfo advertises the environment capabilities and the current request
// domain. Target "kube" has persistent environments, "kube-art" artifact
// mounts too, and "liar" returns inconsistent plans. withdrawn removes every
// target capability, leaving the endpoint's.
type envInfo struct {
	skyv1connect.UnimplementedControlServiceHandler
	endpoint  []v1.Capability
	mu        sync.Mutex
	epoch     uint64
	samples   int
	withdrawn bool
}

func (s *envInfo) GetServerInfo(context.Context, *connect.Request[v1.GetServerInfoRequest]) (*connect.Response[v1.GetServerInfoResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	persistent := []v1.Capability{v1.Capability_CAPABILITY_PERSISTENT_ENVIRONMENTS_V1}
	artifacts := []v1.Capability{v1.Capability_CAPABILITY_PERSISTENT_ENVIRONMENTS_V1, v1.Capability_CAPABILITY_ENVIRONMENT_ARTIFACTS_V1}
	if s.withdrawn {
		persistent, artifacts = nil, nil
	}
	return connect.NewResponse(&v1.GetServerInfoResponse{
		Capabilities: s.endpoint,
		Targets: []*v1.Target{
			{Name: "kube", Kind: "kubernetes", Capabilities: persistent},
			{Name: "kube-art", Kind: "kubernetes", Capabilities: artifacts},
			{Name: "liar", Kind: "fixture", Capabilities: persistent},
		},
	}), nil
}

func (s *envInfo) GetRetention(context.Context, *connect.Request[v1.GetRetentionRequest]) (*connect.Response[v1.GetRetentionResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.samples++
	return connect.NewResponse(&v1.GetRetentionResponse{
		Enabled:     true,
		Domain:      &v1.RequestDomain{Lineage: envLineage, Epoch: s.epoch},
		AcceptUntil: timestamppb.New(time.Now().Add(time.Hour)),
	}), nil
}

// retire makes epoch the current one and refuses every earlier epoch.
func retire(info *envInfo, env *envService, epoch uint64) {
	info.mu.Lock()
	info.epoch = epoch
	info.mu.Unlock()
	env.mu.Lock()
	env.live = epoch
	env.mu.Unlock()
}

// envService is a client-side fixture. It models no lifecycle. It replays a
// create by request ID and domain, refuses the same ID with another spec,
// and refuses any domain before epoch live, even for an exact replay.
// loseNext accepts the next create and then fails its reply. ID 2222… is
// returned with an undeclared phase. Exec relays no exit status for argv
// "lost" and status 3 for "fail".
type envService struct {
	skyv1connect.UnimplementedEnvironmentServiceHandler
	mu       sync.Mutex
	live     uint64
	loseNext bool
	creates  []string
	execs    int
	accepted map[string]*v1.Environment
}

func (s *envService) PlanEnvironment(_ context.Context, r *connect.Request[v1.PlanEnvironmentRequest]) (*connect.Response[v1.PlanEnvironmentResponse], error) {
	spec, err := environmentwire.SpecFromProto(r.Msg.Spec)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	sum, err := environmentwire.SpecSHA256(spec)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	p := &v1.PlanEnvironmentResponse{Spec: environmentwire.SpecToProto(spec), SpecSha256: sum, Support: v1.EnvironmentPlanSupport_ENVIRONMENT_PLAN_SUPPORT_SUPPORTED}
	if len(spec.Artifacts) != 0 {
		p.Artifacts = v1.EnvironmentPlanFact_ENVIRONMENT_PLAN_FACT_UNKNOWN
	}
	if spec.Target == "liar" {
		p.Artifacts = v1.EnvironmentPlanFact_ENVIRONMENT_PLAN_FACT_VERIFIED
	}
	return connect.NewResponse(p), nil
}

func (s *envService) CreateEnvironment(_ context.Context, r *connect.Request[v1.CreateEnvironmentRequest]) (*connect.Response[v1.CreateEnvironmentResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := r.Msg.Domain
	if d == nil || d.Lineage != envLineage || d.Epoch == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid request domain"))
	}
	s.creates = append(s.creates, fmt.Sprintf("%s@%d", r.Msg.RequestId, d.Epoch))
	if d.Epoch < s.live {
		e := connect.NewError(connect.CodeFailedPrecondition, errors.New("request epoch expired"))
		detail, err := connect.NewErrorDetail(&v1.RetentionExpiry{Reason: v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_REQUEST_EPOCH_EXPIRED})
		if err != nil {
			return nil, err
		}
		e.AddDetail(detail)
		return nil, e
	}
	spec, err := environmentwire.SpecFromProto(r.Msg.Spec)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	sum, err := environmentwire.SpecSHA256(spec)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	key := fmt.Sprintf("%s@%d", r.Msg.RequestId, d.Epoch)
	if e := s.accepted[key]; e != nil {
		if e.SpecSha256 != sum {
			return nil, connect.NewError(connect.CodeAlreadyExists, errors.New("request ID names another spec"))
		}
		return connect.NewResponse(&v1.CreateEnvironmentResponse{Environment: e}), nil
	}
	if s.accepted == nil {
		s.accepted = make(map[string]*v1.Environment)
	}
	e := &v1.Environment{Id: fmt.Sprintf("%032x", len(s.accepted)+1), Spec: environmentwire.SpecToProto(spec), SpecSha256: sum, Desired: v1.EnvironmentDesired_ENVIRONMENT_DESIRED_PRESENT, Phase: v1.EnvironmentPhase_ENVIRONMENT_PHASE_CREATING}
	s.accepted[key] = e
	if s.loseNext {
		s.loseNext = false
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("reply lost"))
	}
	return connect.NewResponse(&v1.CreateEnvironmentResponse{Environment: e}), nil
}

func (s *envService) GetEnvironment(_ context.Context, r *connect.Request[v1.GetEnvironmentRequest]) (*connect.Response[v1.GetEnvironmentResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.accepted {
		if e.Id == r.Msg.Id {
			return connect.NewResponse(&v1.GetEnvironmentResponse{Environment: e}), nil
		}
	}
	if r.Msg.Id == strings.Repeat("2", 32) {
		for _, e := range s.accepted {
			bad := proto.Clone(e).(*v1.Environment)
			bad.Id, bad.Phase = r.Msg.Id, 9
			return connect.NewResponse(&v1.GetEnvironmentResponse{Environment: bad}), nil
		}
	}
	return nil, connect.NewError(connect.CodeNotFound, errors.New("environment not found"))
}

func (s *envService) ExecEnvironmentCommand(_ context.Context, r *connect.Request[v1.ExecEnvironmentCommandRequest]) (*connect.Response[v1.ExecEnvironmentCommandResponse], error) {
	s.mu.Lock()
	s.execs++
	s.mu.Unlock()
	code := int32(0)
	switch r.Msg.Argv[0] {
	case "lost":
		return connect.NewResponse(&v1.ExecEnvironmentCommandResponse{}), nil
	case "fail":
		code = 3
	}
	return connect.NewResponse(&v1.ExecEnvironmentCommandResponse{ExitCode: &code, Stdout: []byte(strings.Join(r.Msg.Argv, " ") + "\n"), Stderr: r.Msg.Stdin}), nil
}

func serveEnvironments(t *testing.T, info *envInfo, env skyv1connect.EnvironmentServiceHandler) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(skyv1connect.NewControlServiceHandler(info))
	mux.Handle(skyv1connect.NewEnvironmentServiceHandler(env))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func environmentsInfo() *envInfo {
	return &envInfo{endpoint: []v1.Capability{v1.Capability_CAPABILITY_ENVIRONMENTS_V1}, epoch: 1}
}

func TestEnvironmentScripts(t *testing.T) {
	info, env := environmentsInfo(), &envService{live: 1}
	srv := serveEnvironments(t, info, env)
	// The group returns only after its parallel script tests finish.
	t.Run("group", func(t *testing.T) {
		scripttest.Test(t, context.Background(), scriptEngine(t), []string{"SKYGO_ENDPOINT=" + srv.URL, "DOMAIN=" + envLineage + ":1"}, "testdata/environment/client*.txt")
	})
	env.mu.Lock()
	defer env.mu.Unlock()
	// Each create ran once with the ID and domain it printed, and no
	// refusal was retried under a fresh ID or domain.
	if len(env.creates) != 4 || strings.Join(env.creates[:3], ",") != "req-1@1,req-1@1,req-1@1" || !regexp.MustCompile(`^[0-9a-f]{32}@1$`).MatchString(env.creates[3]) {
		t.Errorf("creates = %v, want req-1@1 three times, then one fresh ID in epoch 1", env.creates)
	}
	if env.execs != 5 {
		t.Errorf("exec calls = %d, want 5", env.execs)
	}
}

// TestEnvironmentCapability checks that an endpoint without
// environments-v1 receives no environment request.
func TestEnvironmentCapability(t *testing.T) {
	env := &envService{}
	srv := serveEnvironments(t, &envInfo{epoch: 1}, env)
	t.Setenv("SKYGO_TOKEN", "")
	for _, args := range [][]string{
		{"env", "list"},
		{"env", "get", strings.Repeat("1", 32)},
		{"env", "delete", strings.Repeat("1", 32), "--yes"},
		{"env", "exec", strings.Repeat("1", 32), "--", "true"},
	} {
		_, _, err := execute(context.Background(), append([]string{"--endpoint", srv.URL}, args...)...)
		if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "environments-v1") {
			t.Errorf("%v: error = %v, want failed_precondition environments-v1", args, err)
		}
	}
	env.mu.Lock()
	defer env.mu.Unlock()
	if len(env.creates) != 0 || env.execs != 0 {
		t.Fatal("environment request sent without the capability")
	}
}

// TestEnvironmentCreateRetry loses the reply to a new create, then retries
// it with the printed ID and domain. In a live epoch the retry replays the
// environment. Once the epoch has retired, the retry is refused and the
// outcome reported unknown; the current domain is never substituted.
func TestEnvironmentCreateRetry(t *testing.T) {
	for _, retired := range []bool{false, true} {
		t.Run(fmt.Sprintf("retired=%t", retired), func(t *testing.T) {
			info, env := environmentsInfo(), &envService{live: 1, loseNext: true}
			srv := serveEnvironments(t, info, env)
			t.Setenv("SKYGO_TOKEN", "")
			spec := t.TempDir() + "/spec.yaml"
			if err := os.WriteFile(spec, []byte("name: analysis\ntarget: kube\nimage: task:v1\ncpu_millis: 500\nmemory_bytes: 1073741824\n"), 0600); err != nil {
				t.Fatal(err)
			}
			_, stderr, err := execute(context.Background(), "--endpoint", srv.URL, "env", "create", "-f", spec)
			if connect.CodeOf(err) != connect.CodeUnavailable {
				t.Fatalf("first create error = %v, want unavailable", err)
			}
			m := regexp.MustCompile(`^request-domain: (\S+)\nrequest-id: ([0-9a-f]{32})\n$`).FindStringSubmatch(stderr)
			if m == nil || m[1] != envLineage+":1" {
				t.Fatalf("stderr = %q, want request-domain %s:1 and request-id", stderr, envLineage)
			}
			domain, id := m[1], m[2]
			if !strings.Contains(err.Error(), "retry with --request-domain "+domain+" --request-id "+id) {
				t.Fatalf("error = %v, want retry hint with both", err)
			}
			if retired {
				retire(info, env, 2)
			}
			out, stderr, err := execute(context.Background(), "--endpoint", srv.URL, "env", "create", "-f", spec, "--request-id", id, "--request-domain", domain)
			if !strings.HasPrefix(stderr, "request-domain: "+domain+"\nrequest-id: "+id+"\n") {
				t.Fatalf("retry stderr = %q, want original domain and ID", stderr)
			}
			if retired {
				if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "outcome unknown") || !strings.Contains(err.Error(), "do not resubmit") || strings.Contains(err.Error(), "retry with") {
					t.Fatalf("retry error = %v, want failed_precondition, outcome unknown, no retry hint", err)
				}
			} else if err != nil || !strings.Contains(out, "analysis") {
				t.Fatalf("retry = %q, %v; want the replayed environment", out, err)
			}
			want := id + "@1," + id + "@1"
			env.mu.Lock()
			got := strings.Join(env.creates, ",")
			env.mu.Unlock()
			if got != want {
				t.Fatalf("creates = %s, want %s", got, want)
			}
			info.mu.Lock()
			defer info.mu.Unlock()
			if info.samples != 1 {
				t.Fatalf("domain sampled %d times, want once", info.samples)
			}
		})
	}
}

// TestEnvironmentReplayWithdrawn creates an environment with artifact
// mounts, then withdraws the target's capabilities. A retry of the same
// request ID and domain still reaches the server and replays the original
// environment; a new create of the same spec is refused before any request.
func TestEnvironmentReplayWithdrawn(t *testing.T) {
	info, env := environmentsInfo(), &envService{live: 1}
	srv := serveEnvironments(t, info, env)
	t.Setenv("SKYGO_TOKEN", "")
	spec := t.TempDir() + "/spec.yaml"
	text := "name: analysis\ntarget: kube-art\nimage: task:v1\ncpu_millis: 500\nmemory_bytes: 1073741824\nartifacts:\n  - id: " + strings.Repeat("a", 32) + "\n    sha256: " + strings.Repeat("a", 64) + "\n    size: 1\n    mount_path: /skygo/artifacts/input\n"
	if err := os.WriteFile(spec, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	first, stderr, err := execute(context.Background(), "--endpoint", srv.URL, "env", "create", "-f", spec)
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`^request-domain: (\S+)\nrequest-id: ([0-9a-f]{32})\n$`).FindStringSubmatch(stderr)
	if m == nil {
		t.Fatalf("stderr = %q, want request-domain and request-id", stderr)
	}
	domain, id := m[1], m[2]
	info.mu.Lock()
	info.withdrawn = true
	info.mu.Unlock()

	replay, _, err := execute(context.Background(), "--endpoint", srv.URL, "env", "create", "-f", spec, "--request-id", id, "--request-domain", domain)
	if err != nil {
		t.Fatalf("replay after withdrawal: %v", err)
	}
	if replay != first {
		t.Fatalf("replay = %q, want the original %q", replay, first)
	}
	_, _, err = execute(context.Background(), "--endpoint", srv.URL, "env", "create", "-f", spec)
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "does not advertise persistent-environments-v1") {
		t.Fatalf("new create after withdrawal: %v, want failed_precondition", err)
	}

	env.mu.Lock()
	got := strings.Join(env.creates, ",")
	env.mu.Unlock()
	if want := id + "@1," + id + "@1"; got != want {
		t.Fatalf("creates = %s, want %s", got, want)
	}
	info.mu.Lock()
	defer info.mu.Unlock()
	if info.samples != 1 {
		t.Fatalf("domain sampled %d times, want once", info.samples)
	}
}

// TestEnvironmentResponseUnknown checks that each response message's own
// unknown fields are refused before its contents are used, including a list
// response with no entries.
func TestEnvironmentResponseUnknown(t *testing.T) {
	srv := serveEnvironments(t, environmentsInfo(), unknownResponses{})
	t.Setenv("SKYGO_TOKEN", "")
	id := strings.Repeat("1", 32)
	spec := t.TempDir() + "/spec.yaml"
	if err := os.WriteFile(spec, []byte("name: analysis\ntarget: kube\nimage: task:v1\ncpu_millis: 500\nmemory_bytes: 1073741824\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"create", []string{"env", "create", "-f", spec, "--request-id", "req-1", "--request-domain", envLineage + ":1"}, "CreateEnvironmentResponse with unknown fields"},
		{"get", []string{"env", "get", id}, "GetEnvironmentResponse with unknown fields"},
		{"list_empty", []string{"env", "list"}, "ListEnvironmentsResponse with unknown fields"},
		{"delete", []string{"env", "delete", id, "--yes"}, "DeleteEnvironmentResponse with unknown fields"},
		{"exec", []string{"env", "exec", id, "--", "true"}, "ExecEnvironmentCommandResponse with unknown fields"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, _, err := execute(context.Background(), append([]string{"--endpoint", srv.URL}, tc.args...)...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
			if out != "" {
				t.Fatalf("stdout = %q, want nothing", out)
			}
		})
	}
}

// unknownResponses returns valid contents in response messages that carry
// an unknown field 99 of their own. Its list is empty.
type unknownResponses struct {
	skyv1connect.UnimplementedEnvironmentServiceHandler
}

func withUnknown[M proto.Message](m M) M {
	m.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1))
	return m
}

func validEnvironment() *v1.Environment {
	spec, err := environmentwire.SpecFromProto(&v1.EnvironmentSpec{Name: "analysis", Target: "kube", Image: "task:v1", CpuMillis: 500, MemoryBytes: 1 << 30})
	if err != nil {
		panic(err)
	}
	sum, err := environmentwire.SpecSHA256(spec)
	if err != nil {
		panic(err)
	}
	return &v1.Environment{Id: strings.Repeat("1", 32), Spec: environmentwire.SpecToProto(spec), SpecSha256: sum, Desired: v1.EnvironmentDesired_ENVIRONMENT_DESIRED_PRESENT, Phase: v1.EnvironmentPhase_ENVIRONMENT_PHASE_READY}
}

func (unknownResponses) CreateEnvironment(context.Context, *connect.Request[v1.CreateEnvironmentRequest]) (*connect.Response[v1.CreateEnvironmentResponse], error) {
	return connect.NewResponse(withUnknown(&v1.CreateEnvironmentResponse{Environment: validEnvironment()})), nil
}

func (unknownResponses) GetEnvironment(context.Context, *connect.Request[v1.GetEnvironmentRequest]) (*connect.Response[v1.GetEnvironmentResponse], error) {
	return connect.NewResponse(withUnknown(&v1.GetEnvironmentResponse{Environment: validEnvironment()})), nil
}

func (unknownResponses) ListEnvironments(context.Context, *connect.Request[v1.ListEnvironmentsRequest]) (*connect.Response[v1.ListEnvironmentsResponse], error) {
	return connect.NewResponse(withUnknown(&v1.ListEnvironmentsResponse{})), nil
}

func (unknownResponses) DeleteEnvironment(context.Context, *connect.Request[v1.DeleteEnvironmentRequest]) (*connect.Response[v1.DeleteEnvironmentResponse], error) {
	return connect.NewResponse(withUnknown(&v1.DeleteEnvironmentResponse{Environment: validEnvironment()})), nil
}

func (unknownResponses) ExecEnvironmentCommand(context.Context, *connect.Request[v1.ExecEnvironmentCommandRequest]) (*connect.Response[v1.ExecEnvironmentCommandResponse], error) {
	code := int32(0)
	return connect.NewResponse(withUnknown(&v1.ExecEnvironmentCommandResponse{ExitCode: &code})), nil
}

// TestEnvironmentCreateFlags checks that a request ID and domain are given
// together and are checked before any request.
func TestEnvironmentCreateFlags(t *testing.T) {
	info, env := environmentsInfo(), &envService{live: 1}
	srv := serveEnvironments(t, info, env)
	t.Setenv("SKYGO_TOKEN", "")
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--request-id", "req-1"}, "given together"},
		{[]string{"--request-domain", envLineage + ":1"}, "given together"},
		{[]string{"--request-id", "req-1", "--request-domain", envLineage + ":0"}, "positive canonical epoch"},
		{[]string{"--request-id", "bad id", "--request-domain", envLineage + ":1"}, "request ID requires"},
	} {
		_, _, err := execute(context.Background(), append([]string{"--endpoint", srv.URL, "env", "create", "-f", "absent.yaml"}, tc.args...)...)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%v: error = %v, want %q", tc.args, err, tc.want)
		}
	}
	env.mu.Lock()
	defer env.mu.Unlock()
	info.mu.Lock()
	defer info.mu.Unlock()
	if len(env.creates) != 0 || info.samples != 0 {
		t.Fatal("request sent for invalid flags")
	}
}

// TestEnvironmentExecErrors checks that no status, whatever its code, is
// read as proof that the command did not run: each is reported unknown,
// keeps its code and is sent once.
func TestEnvironmentExecErrors(t *testing.T) {
	for _, code := range []connect.Code{connect.CodeUnavailable, connect.CodePermissionDenied, connect.CodeInvalidArgument, connect.CodeDeadlineExceeded} {
		t.Run(code.String(), func(t *testing.T) {
			f := &failExec{code: code}
			srv := serveEnvironments(t, environmentsInfo(), f)
			t.Setenv("SKYGO_TOKEN", "")
			_, _, err := execute(context.Background(), "--endpoint", srv.URL, "env", "exec", strings.Repeat("1", 32), "--", "true")
			if connect.CodeOf(err) != code || !strings.Contains(err.Error(), "exec outcome unknown; it was not resent") {
				t.Fatalf("error = %v, want %v reported unknown", err, code)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.calls != 1 {
				t.Fatalf("exec calls = %d, want 1", f.calls)
			}
		})
	}
}

type failExec struct {
	skyv1connect.UnimplementedEnvironmentServiceHandler
	code  connect.Code
	mu    sync.Mutex
	calls int
}

func (f *failExec) ExecEnvironmentCommand(context.Context, *connect.Request[v1.ExecEnvironmentCommandRequest]) (*connect.Response[v1.ExecEnvironmentCommandResponse], error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return nil, connect.NewError(f.code, errors.New("exec failed"))
}

// TestEnvironmentExecStdin checks that stdin is absent without --stdin,
// present and empty for empty input, and bounded at 64 KiB.
func TestEnvironmentExecStdin(t *testing.T) {
	var mu sync.Mutex
	var got [][]byte
	srv := serveEnvironments(t, environmentsInfo(), stdinExec{record: func(b []byte) {
		mu.Lock()
		got = append(got, b)
		mu.Unlock()
	}})
	t.Setenv("SKYGO_TOKEN", "")
	id := strings.Repeat("1", 32)
	for _, tc := range []struct {
		name  string
		flags []string
		input string
		want  []byte
		err   string
	}{
		{"absent", nil, "ignored", nil, ""},
		{"empty", []string{"--stdin"}, "", []byte{}, ""},
		{"data", []string{"--stdin"}, "piped\n", []byte("piped\n"), ""},
		{"too_large", []string{"--stdin"}, strings.Repeat("x", maxExecStdin+1), nil, "stdin exceeds 64 KiB"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mu.Lock()
			got = nil
			mu.Unlock()
			c := New(nil)
			c.SetIn(strings.NewReader(tc.input))
			c.SetOut(io.Discard)
			c.SetErr(io.Discard)
			c.SetArgs(append(append([]string{"--endpoint", srv.URL, "env", "exec", id}, tc.flags...), "--", "cat"))
			err := c.ExecuteContext(context.Background())
			mu.Lock()
			defer mu.Unlock()
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) || len(got) != 0 {
					t.Fatalf("error = %v, requests = %d; want %q and no request", err, len(got), tc.err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 || (got[0] == nil) != (tc.want == nil) || string(got[0]) != string(tc.want) {
				t.Fatalf("stdin = %q (nil %t), want %q (nil %t)", got, len(got) == 1 && got[0] == nil, tc.want, tc.want == nil)
			}
		})
	}
}

type stdinExec struct {
	skyv1connect.UnimplementedEnvironmentServiceHandler
	record func([]byte)
}

func (s stdinExec) ExecEnvironmentCommand(_ context.Context, r *connect.Request[v1.ExecEnvironmentCommandRequest]) (*connect.Response[v1.ExecEnvironmentCommandResponse], error) {
	s.record(r.Msg.Stdin)
	code := int32(0)
	return connect.NewResponse(&v1.ExecEnvironmentCommandResponse{ExitCode: &code}), nil
}

func TestReadEnvSpec(t *testing.T) {
	const valid = "name: analysis\ntarget: kube\nimage: task:v1\ncpu_millis: 500\nmemory_bytes: 1073741824\n"
	for _, tc := range []struct {
		name, input, want string
	}{
		{"valid", valid, ""},
		{"empty_artifacts", valid + "artifacts: []\n", ""},
		{"unknown_key", valid + "color: blue\n", "field color not found"},
		{"duplicate_key", valid + "name: other\n", "already defined"},
		{"null", valid + "storage_alias: null\n", "must not contain null"},
		{"null_mount", valid + "artifacts: [~]\n", "must not contain null"},
		{"alias", "name: &n analysis\ntarget: *n\nimage: task:v1\ncpu_millis: 500\nmemory_bytes: 1\n", "anchors or aliases"},
		{"merge", "<<: {name: analysis}\ntarget: kube\n", "merge keys"},
		{"two_documents", valid + "---\n" + valid, "exactly one YAML document"},
		{"not_mapping", "- a\n", "must be a mapping"},
		{"invalid", valid + "port: 22\n", "port must be 0 or 8080"},
		{"negative_size", valid + "artifacts: [{id: " + strings.Repeat("a", 32) + ", sha256: " + strings.Repeat("a", 64) + ", size: -1, mount_path: /skygo/artifacts/a}]\n", "decode environment spec"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := readEnvSpec(strings.NewReader(tc.input))
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				if s.Artifacts != nil {
					t.Fatal("empty artifacts not normalized to version 1")
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("readEnvSpec error = %v, want %q", err, tc.want)
			}
		})
	}
}
