package cli

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/tmc/skygo/internal/servingwire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"rsc.io/script/scripttest"
)

// Source-only; NOT_RUN.

// serveControlInfo advertises services on the endpoint and fixed services on
// target "kube" only. Target "jobs" is advertised without them.
type serveControlInfo struct {
	skyv1connect.UnimplementedControlServiceHandler
	endpoint []v1.Capability
}

func (s *serveControlInfo) GetServerInfo(context.Context, *connect.Request[v1.GetServerInfoRequest]) (*connect.Response[v1.GetServerInfoResponse], error) {
	return connect.NewResponse(&v1.GetServerInfoResponse{
		Capabilities: s.endpoint,
		Targets: []*v1.Target{
			{Name: "kube", Kind: "kubernetes", Capabilities: []v1.Capability{v1.Capability_CAPABILITY_FIXED_SERVICES_V1}},
			{Name: "jobs", Kind: "kubernetes"},
		},
	}), nil
}

func (s *serveControlInfo) GetRetention(context.Context, *connect.Request[v1.GetRetentionRequest]) (*connect.Response[v1.GetRetentionResponse], error) {
	return connect.NewResponse(&v1.GetRetentionResponse{
		Enabled:     true,
		Domain:      &v1.RequestDomain{Lineage: envLineage, Epoch: 1},
		AcceptUntil: timestamppb.New(time.Now().Add(time.Hour)),
	}), nil
}

// servingFixture is a client-side fixture. It models no lifecycle: it
// replays mutations by request ID and domain, refuses the same ID for
// another request, and refuses a stale expected record revision with a
// ServiceConflict detail. ID 2222… is returned with an undeclared phase, and
// ID 3333… with a name that would forge a column.
type servingFixture struct {
	skyv1connect.UnimplementedServingServiceHandler
	mu       sync.Mutex
	creates  []string
	deletes  []string
	byID     map[string]*v1.Service
	order    []string
	accepted map[string]proto.Message // key: the accepted request
	replies  map[string]*v1.Service
}

func (s *servingFixture) CreateService(_ context.Context, r *connect.Request[v1.CreateServiceRequest]) (*connect.Response[v1.CreateServiceResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := r.Msg.Domain
	if d == nil || d.Lineage != envLineage || d.Epoch == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid request domain"))
	}
	key := fmt.Sprintf("%s@%d", r.Msg.RequestId, d.Epoch)
	s.creates = append(s.creates, key)
	if prev, ok := s.accepted[key]; ok {
		if !proto.Equal(prev, r.Msg) {
			return nil, serveConflict(v1.ServiceConflictReason_SERVICE_CONFLICT_REASON_REQUEST_REUSED, 0)
		}
		return connect.NewResponse(&v1.CreateServiceResponse{Service: s.replies[key]}), nil
	}
	spec, err := servingwire.SpecFromProto(r.Msg.Spec)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	sum, err := spec.Digest()
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	for _, id := range s.order {
		if s.byID[id].Name == r.Msg.Name {
			return nil, serveConflict(v1.ServiceConflictReason_SERVICE_CONFLICT_REASON_NAME_IN_USE, 0)
		}
	}
	id := fmt.Sprintf("%032x", len(s.order)+1)
	svc := &v1.Service{
		Id: id, Name: r.Msg.Name, Target: r.Msg.Target, RecordRevision: 1,
		Desired: v1.ServiceDesired_SERVICE_DESIRED_PRESENT,
		History: &v1.ServiceHistory{
			Revisions: []*v1.ServiceRevision{{Number: 1, Spec: servingwire.SpecToProto(spec), Sha256: sum}},
			Rollouts:  []*v1.ServiceRollout{{Generation: 1, Revision: 1, Kind: v1.ServiceRolloutKind_SERVICE_ROLLOUT_KIND_CREATE, Outcome: v1.ServiceRolloutOutcome_SERVICE_ROLLOUT_OUTCOME_PENDING}},
		},
		Status: &v1.ServiceStatus{Phase: v1.ServicePhase_SERVICE_PHASE_CREATING, DesiredGeneration: 1},
	}
	if s.byID == nil {
		s.byID, s.accepted, s.replies = map[string]*v1.Service{}, map[string]proto.Message{}, map[string]*v1.Service{}
	}
	s.byID[id] = svc
	s.order = append(s.order, id)
	s.accepted[key] = proto.Clone(r.Msg)
	s.replies[key] = svc
	return connect.NewResponse(&v1.CreateServiceResponse{Service: svc}), nil
}

func (s *servingFixture) GetService(_ context.Context, r *connect.Request[v1.GetServiceRequest]) (*connect.Response[v1.GetServiceResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if svc := s.byID[r.Msg.Id]; svc != nil {
		return connect.NewResponse(&v1.GetServiceResponse{Service: svc}), nil
	}
	if r.Msg.Id == strings.Repeat("2", 32) && len(s.order) > 0 {
		bad := proto.Clone(s.byID[s.order[0]]).(*v1.Service)
		bad.Id, bad.Status.Phase = r.Msg.Id, 42
		return connect.NewResponse(&v1.GetServiceResponse{Service: bad}), nil
	}
	if r.Msg.Id == strings.Repeat("3", 32) && len(s.order) > 0 {
		bad := proto.Clone(s.byID[s.order[0]]).(*v1.Service)
		bad.Id, bad.Name = r.Msg.Id, "web\tready"
		return connect.NewResponse(&v1.GetServiceResponse{Service: bad}), nil
	}
	return nil, connect.NewError(connect.CodeNotFound, errors.New("service not found"))
}

func (s *servingFixture) ListServices(context.Context, *connect.Request[v1.ListServicesRequest]) (*connect.Response[v1.ListServicesResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out v1.ListServicesResponse
	for _, id := range s.order {
		svc := s.byID[id]
		h := svc.History
		out.Services = append(out.Services, &v1.ServiceSummary{
			Id: svc.Id, Name: svc.Name, Target: svc.Target, RecordRevision: svc.RecordRevision, Desired: svc.Desired,
			Current: h.Rollouts[len(h.Rollouts)-1], Sha256: h.Revisions[0].Sha256, Status: svc.Status,
		})
	}
	return connect.NewResponse(&out), nil
}

func (s *servingFixture) DeleteService(_ context.Context, r *connect.Request[v1.DeleteServiceRequest]) (*connect.Response[v1.DeleteServiceResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := r.Msg.Domain
	if d == nil || d.Lineage != envLineage || d.Epoch == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid request domain"))
	}
	key := fmt.Sprintf("%s@%d", r.Msg.RequestId, d.Epoch)
	s.deletes = append(s.deletes, key)
	if prev, ok := s.accepted[key]; ok {
		if !proto.Equal(prev, r.Msg) {
			return nil, serveConflict(v1.ServiceConflictReason_SERVICE_CONFLICT_REASON_REQUEST_REUSED, 0)
		}
		return connect.NewResponse(&v1.DeleteServiceResponse{Service: s.replies[key]}), nil
	}
	svc := s.byID[r.Msg.Id]
	switch {
	case svc == nil:
		return nil, connect.NewError(connect.CodeNotFound, errors.New("service not found"))
	case svc.Desired == v1.ServiceDesired_SERVICE_DESIRED_ABSENT:
		return nil, serveConflict(v1.ServiceConflictReason_SERVICE_CONFLICT_REASON_DELETED, 0)
	case r.Msg.ExpectedRecordRevision != svc.RecordRevision:
		return nil, serveConflict(v1.ServiceConflictReason_SERVICE_CONFLICT_REASON_STALE_RECORD_REVISION, svc.RecordRevision)
	}
	svc.Desired, svc.RecordRevision = v1.ServiceDesired_SERVICE_DESIRED_ABSENT, svc.RecordRevision+1
	svc.Status.Phase = v1.ServicePhase_SERVICE_PHASE_DELETING
	s.accepted[key] = proto.Clone(r.Msg)
	s.replies[key] = proto.Clone(svc).(*v1.Service)
	return connect.NewResponse(&v1.DeleteServiceResponse{Service: svc}), nil
}

// bump records a change to service id, so that a deletion decided against
// the earlier revision is stale.
func (s *servingFixture) bump(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byID[id].RecordRevision++
}

func serveConflict(reason v1.ServiceConflictReason, revision uint64) error {
	e := connect.NewError(connect.CodeAborted, errors.New("service conflict"))
	if d, err := connect.NewErrorDetail(&v1.ServiceConflict{Reason: reason, RecordRevision: revision}); err == nil {
		e.AddDetail(d)
	}
	return e
}

func serveServices(t *testing.T, info *serveControlInfo, svc skyv1connect.ServingServiceHandler) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(skyv1connect.NewControlServiceHandler(info))
	mux.Handle(skyv1connect.NewServingServiceHandler(svc))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestServeScripts(t *testing.T) {
	svc := &servingFixture{}
	srv := serveServices(t, &serveControlInfo{endpoint: []v1.Capability{v1.Capability_CAPABILITY_SERVICES_V1}}, svc)
	t.Run("group", func(t *testing.T) {
		scripttest.Test(t, context.Background(), scriptEngine(t), []string{"SKYGO_ENDPOINT=" + srv.URL, "DOMAIN=" + envLineage + ":1"}, "testdata/serve.txt")
	})
	svc.mu.Lock()
	defer svc.mu.Unlock()
	// Each create ran with the pair it printed: req-1 three times (accept,
	// replay, reused refusal), then one fresh ID; a refused spec sent none.
	if len(svc.creates) != 4 || strings.Join(svc.creates[:3], ",") != "req-1@1,req-1@1,req-1@1" || !regexp.MustCompile(`^[0-9a-f]{32}@1$`).MatchString(svc.creates[3]) {
		t.Errorf("creates = %v", svc.creates)
	}
	// One fresh delete, its exact replay, and no other.
	if len(svc.deletes) != 2 || svc.deletes[0] != svc.deletes[1] {
		t.Errorf("deletes = %v", svc.deletes)
	}
}

// TestServeStaleDelete checks that a deletion decided against a revision
// that changed before it was sent is refused as stale and not retried.
func TestServeStaleDelete(t *testing.T) {
	svc := &servingFixture{}
	srv := serveServices(t, &serveControlInfo{endpoint: []v1.Capability{v1.Capability_CAPABILITY_SERVICES_V1}}, svc)
	spec := writeServeSpec(t)
	if _, _, err := execute(context.Background(), "--endpoint", srv.URL, "serve", "up", spec, "--name", "web", "--target", "kube"); err != nil {
		t.Fatal(err)
	}
	id := fmt.Sprintf("%032x", 1)
	svc.bump(id)
	_, _, err := execute(context.Background(), "--endpoint", srv.URL, "serve", "down", id, "--request-id", "del-1", "--request-domain", envLineage+":1", "--record-revision", "1")
	if err == nil || !strings.Contains(err.Error(), "record revision now 2") {
		t.Fatalf("stale delete = %v", err)
	}
	if len(svc.deletes) != 1 {
		t.Fatalf("deletes = %v, want exactly one", svc.deletes)
	}
}

// TestServeCapability checks that an endpoint without services-v1 receives
// no serving request.
func TestServeCapability(t *testing.T) {
	svc := &servingFixture{}
	srv := serveServices(t, &serveControlInfo{}, svc)
	spec := writeServeSpec(t)
	id := strings.Repeat("1", 32)
	for _, args := range [][]string{
		{"serve", "up", spec, "--name", "web", "--target", "kube"},
		{"serve", "status"},
		{"serve", "status", id},
		{"serve", "down", id},
		{"serve", "forward", id},
	} {
		_, _, err := execute(context.Background(), append([]string{"--endpoint", srv.URL}, args...)...)
		if connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Errorf("%v: err = %v, want failed_precondition", args, err)
		}
	}
	if len(svc.creates)+len(svc.deletes) != 0 {
		t.Fatalf("serving requests sent: %v %v", svc.creates, svc.deletes)
	}
}

func TestRouteURL(t *testing.T) {
	id := strings.Repeat("a", 32)
	for _, tt := range []struct{ endpoint, want string }{
		{"http://127.0.0.1:8080", "http://127.0.0.1:8080/services/" + id + "/forward"},
		{"https://skygo.example.com/api/", "https://skygo.example.com/api/services/" + id + "/forward"},
	} {
		u, err := routeURL(tt.endpoint, "services", id)
		if err != nil || u.String() != tt.want {
			t.Errorf("routeURL(%q) = %v, %v; want %s", tt.endpoint, u, err, tt.want)
		}
	}
	for _, bad := range []string{"ftp://h", "http://u@h", "http://h/?q=1", "http://h/#f", "http://"} {
		if _, err := routeURL(bad, "services", id); err == nil {
			t.Errorf("routeURL(%q) accepted", bad)
		}
	}
}

func TestReadServiceSpec(t *testing.T) {
	for _, tt := range []struct{ name, doc, want string }{
		{"unknown", serveSpecDoc + "color: blue\n", "field color not found"},
		{"null", serveSpecDoc + "setup: null\n", "must not contain null"},
		{"setup", serveSpecDoc + "setup: [make]\n", "unsupported service feature: setup"},
		{"mounts", serveSpecDoc + "mounts:\n- {reference: data, destination: /mnt/skygo/data}\n", "unsupported service feature: mounts"},
		{"workdir", serveSpecDoc + "workdir: {id: x}\n", "unsupported service feature: workdir"},
		{"workdir empty", serveSpecDoc + "workdir: {}\n", "unsupported service feature: workdir"},
		{"workdir null", serveSpecDoc + "workdir: null\n", "must not contain null"},

		{"cpu", strings.Replace(serveSpecDoc, "replicas: 1", "replicas: 1\n  cpu: {min_replicas: 1}", 1), "unsupported service feature: cpu scaling"},
		{"cpu empty", strings.Replace(serveSpecDoc, "replicas: 1", "replicas: 1\n  cpu: {}", 1), "unsupported service feature: cpu scaling"},
		{"cpu null", strings.Replace(serveSpecDoc, "replicas: 1", "replicas: 1\n  cpu: null", 1), "must not contain null"},

		{"replicas", strings.Replace(serveSpecDoc, "replicas: 1", "replicas: 9", 1), "replicas"},
		{"tag", strings.Replace(serveSpecDoc, serveImage, "registry.example.com/web:latest", 1), "service spec"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := readServiceSpec(strings.NewReader(tt.doc))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
	s, err := readServiceSpec(strings.NewReader(serveSpecDoc))
	if err != nil {
		t.Fatal(err)
	}
	if s.Port != 8080 || s.Scaling.Replicas != 1 || len(s.NodeSelector) != 1 {
		t.Fatalf("spec %+v", s)
	}
}

const serveImage = "registry.example.com/team/web@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

const serveSpecDoc = `image: ` + serveImage + `
argv: [/bin/web, --port, "8080"]
env:
- {name: MODE, value: production}
port: 8080
readiness: {path: /healthz}
resources: {cpu_millis: 250, memory_bytes: 268435456}
node_selector: {pool: web}
scaling:
  replicas: 1
`

func writeServeSpec(t *testing.T) string {
	t.Helper()
	name := t.TempDir() + "/service.yaml"
	if err := os.WriteFile(name, []byte(serveSpecDoc), 0o600); err != nil {
		t.Fatal(err)
	}
	return name
}
