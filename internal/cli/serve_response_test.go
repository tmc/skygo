package cli

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"google.golang.org/protobuf/proto"
)

// AUTHORED_NOT_RUN. badServing is servingFixture with each response
// changed after the fixture made it: MODELED bad servers. Every bad
// response must fail the command with nothing written to stdout.
type badServing struct {
	*servingFixture
	hooks            sync.Mutex // guards the fields below
	create, get, del func(*v1.Service)
	list             func(*v1.ServiceSummary)
}

// set runs f, which sets hooks, under the hooks lock.
func (b *badServing) set(f func(b *badServing)) {
	b.hooks.Lock()
	defer b.hooks.Unlock()
	f(b)
}

func (b *badServing) hook(f *func(*v1.Service)) func(*v1.Service) {
	b.hooks.Lock()
	defer b.hooks.Unlock()
	return *f
}

func (b *badServing) CreateService(ctx context.Context, r *connect.Request[v1.CreateServiceRequest]) (*connect.Response[v1.CreateServiceResponse], error) {
	resp, err := b.servingFixture.CreateService(ctx, r)
	change := b.hook(&b.create)
	if err != nil || change == nil {
		return resp, err
	}
	s := proto.Clone(resp.Msg.Service).(*v1.Service)
	change(s)
	return connect.NewResponse(&v1.CreateServiceResponse{Service: s}), nil
}

func (b *badServing) GetService(ctx context.Context, r *connect.Request[v1.GetServiceRequest]) (*connect.Response[v1.GetServiceResponse], error) {
	resp, err := b.servingFixture.GetService(ctx, r)
	change := b.hook(&b.get)
	if err != nil || change == nil {
		return resp, err
	}
	s := proto.Clone(resp.Msg.Service).(*v1.Service)
	change(s)
	return connect.NewResponse(&v1.GetServiceResponse{Service: s}), nil
}

func (b *badServing) DeleteService(ctx context.Context, r *connect.Request[v1.DeleteServiceRequest]) (*connect.Response[v1.DeleteServiceResponse], error) {
	resp, err := b.servingFixture.DeleteService(ctx, r)
	change := b.hook(&b.del)
	if err != nil || change == nil {
		return resp, err
	}
	s := proto.Clone(resp.Msg.Service).(*v1.Service)
	change(s)
	return connect.NewResponse(&v1.DeleteServiceResponse{Service: s}), nil
}

func (b *badServing) ListServices(ctx context.Context, r *connect.Request[v1.ListServicesRequest]) (*connect.Response[v1.ListServicesResponse], error) {
	resp, err := b.servingFixture.ListServices(ctx, r)
	b.hooks.Lock()
	change := b.list
	b.hooks.Unlock()
	if err != nil || change == nil {
		return resp, err
	}
	out := proto.Clone(resp.Msg).(*v1.ListServicesResponse)
	change(out.Services[len(out.Services)-1])
	return connect.NewResponse(out), nil
}

// unknownField adds field 111, a varint, to m's unknown fields.
func unknownField(m proto.Message) { m.ProtoReflect().SetUnknown([]byte{0xf8, 0x06, 0x01}) }

var badServices = map[string]func(*v1.Service){
	"unknown top":         func(s *v1.Service) { unknownField(s) },
	"unknown status":      func(s *v1.Service) { unknownField(s.Status) },
	"unknown revision":    func(s *v1.Service) { unknownField(s.History.Revisions[0]) },
	"unknown spec":        func(s *v1.Service) { unknownField(s.History.Revisions[0].Spec.Resources) },
	"unknown rollout":     func(s *v1.Service) { unknownField(s.History.Rollouts[0]) },
	"name control":        func(s *v1.Service) { s.Name = "web\tx" },
	"target newline":      func(s *v1.Service) { s.Target = "kube\nREADY" },
	"zero record":         func(s *v1.Service) { s.RecordRevision = 0 },
	"phase undeclared":    func(s *v1.Service) { s.Status.Phase = 42 },
	"ready unobserved":    func(s *v1.Service) { s.Status.Phase = v1.ServicePhase_SERVICE_PHASE_READY },
	"digest not spec":     func(s *v1.Service) { s.History.Revisions[0].Sha256 = strings.Repeat("0", 64) },
	"spec unsupported":    func(s *v1.Service) { s.History.Revisions[0].Spec.Scaling.Replicas = 2 },
	"no rollouts":         func(s *v1.Service) { s.History.Rollouts = nil },
	"rollout unretained":  func(s *v1.Service) { s.History.Rollouts[0].Revision = 2 },
	"generation mismatch": func(s *v1.Service) { s.Status.DesiredGeneration = 2 },
	"absent but ready": func(s *v1.Service) {
		s.Status.Observation = &v1.ServiceObservation{ObservedGeneration: 1, Absent: true, ReadyReplicas: 1}
	},
	"ready ahead": func(s *v1.Service) {
		s.Status.Observation = &v1.ServiceObservation{ObservedGeneration: 1, ReadyGeneration: 2}
	},
	"reason undeclared": func(s *v1.Service) {
		s.Status.Observation = &v1.ServiceObservation{Reason: 99}
	},
	"duplicate condition": func(s *v1.Service) {
		c := &v1.ServiceCondition{Code: v1.ServiceConditionCode_SERVICE_CONDITION_CODE_TARGET_UNAVAILABLE, Generation: 1}
		s.Status.Conditions = []*v1.ServiceCondition{c, proto.Clone(c).(*v1.ServiceCondition)}
	},
	"retired with history": func(s *v1.Service) {
		s.Retired, s.Desired, s.Status.Phase = true, v1.ServiceDesired_SERVICE_DESIRED_ABSENT, v1.ServicePhase_SERVICE_PHASE_DELETED
	},
	"reapply": func(s *v1.Service) {
		s.History.Rollouts[0].Kind = v1.ServiceRolloutKind_SERVICE_ROLLOUT_KIND_REAPPLY
	},
	"first rollout update": func(s *v1.Service) {
		s.History.Rollouts[0].Kind = v1.ServiceRolloutKind_SERVICE_ROLLOUT_KIND_UPDATE
	},
	"ready all zero": func(s *v1.Service) {
		s.Status.Phase, s.Status.Observation = v1.ServicePhase_SERVICE_PHASE_READY, &v1.ServiceObservation{}
	},
	"ready zero endpoints": func(s *v1.Service) {
		s.Status.Phase, s.Status.Observation = v1.ServicePhase_SERVICE_PHASE_READY, readyObservation()
		s.Status.Observation.ReadyEndpoints = 0
	},
	"ready two replicas": func(s *v1.Service) {
		s.Status.Phase, s.Status.Observation = v1.ServicePhase_SERVICE_PHASE_READY, readyObservation()
		o := s.Status.Observation
		o.DesiredReplicas, o.UpdatedReplicas, o.ReadyReplicas, o.AvailableReplicas = 2, 2, 2, 2
	},
	"available over ready": func(s *v1.Service) {
		s.Status.Observation = &v1.ServiceObservation{ObservedGeneration: 1, DesiredReplicas: 1, AvailableReplicas: 1}
	},
	"absent desired ready": func(s *v1.Service) {
		s.Desired = v1.ServiceDesired_SERVICE_DESIRED_ABSENT
		s.Status.Phase, s.Status.Observation = v1.ServicePhase_SERVICE_PHASE_READY, readyObservation()
	},
	"scaling": func(s *v1.Service) {
		s.Status.Observation = &v1.ServiceObservation{Scaling: v1.ServiceScalingCondition_SERVICE_SCALING_CONDITION_HEALTHY}
	},
}

// readyObservation is the one observation a ready service of generation 1
// has.
func readyObservation() *v1.ServiceObservation {
	return &v1.ServiceObservation{
		ObservedGeneration: 1, ReadyGeneration: 1,
		DesiredReplicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1, ReadyEndpoints: 1,
	}
}

// TestServeReadyResponse is the positive of the ready shapes refused
// above: a server's exact ready status prints.
func TestServeReadyResponse(t *testing.T) {
	ctx := context.Background()
	b := &badServing{servingFixture: &servingFixture{}}
	srv := serveServices(t, &serveControlInfo{endpoint: []v1.Capability{v1.Capability_CAPABILITY_SERVICES_V1}}, b)
	if _, _, err := execute(ctx, "--endpoint", srv.URL, "serve", "up", writeServeSpec(t), "--name", "web", "--target", "kube"); err != nil {
		t.Fatal(err)
	}
	b.set(func(b *badServing) {
		b.get = func(s *v1.Service) {
			s.Status.Phase, s.Status.Observation = v1.ServicePhase_SERVICE_PHASE_READY, readyObservation()
		}
	})
	stdout, _, err := execute(ctx, "--endpoint", srv.URL, "serve", "status", fmt.Sprintf("%032x", 1))
	if err != nil || !strings.Contains(stdout, "ready") {
		t.Fatalf("stdout %q, err %v", stdout, err)
	}
}

func TestServeBadServiceResponses(t *testing.T) {
	for name, change := range badServices {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			spec := writeServeSpec(t)
			id := fmt.Sprintf("%032x", 1)
			for _, tt := range []struct {
				cmd   string
				setup func(b *badServing)
				args  []string
			}{
				{"up", func(b *badServing) { b.create = change }, []string{"serve", "up", spec, "--name", "web", "--target", "kube"}},
				{"status", func(b *badServing) { b.get = change }, []string{"serve", "status", id}},
				{"down", func(b *badServing) { b.del = change }, []string{"serve", "down", id}},
			} {
				// Each command and format has its own server, so that each
				// request reaches the changed response.
				for _, format := range [][]string{nil, {"--json"}} {
					b := &badServing{servingFixture: &servingFixture{}}
					srv := serveServices(t, &serveControlInfo{endpoint: []v1.Capability{v1.Capability_CAPABILITY_SERVICES_V1}}, b)
					if tt.cmd != "up" {
						if _, _, err := execute(ctx, "--endpoint", srv.URL, "serve", "up", spec, "--name", "web", "--target", "kube"); err != nil {
							t.Fatal(err)
						}
					}
					b.set(tt.setup)
					args := append(append([]string{"--endpoint", srv.URL}, format...), tt.args...)
					stdout, _, err := execute(ctx, args...)
					if err == nil || stdout != "" {
						t.Errorf("%s %v: stdout %q, err %v; want an error and no output", tt.cmd, format, stdout, err)
					}
				}
			}
		})
	}
}

// noRetention is serveControlInfo on a server with retention disabled.
type noRetention struct{ *serveControlInfo }

func (noRetention) GetRetention(context.Context, *connect.Request[v1.GetRetentionRequest]) (*connect.Response[v1.GetRetentionResponse], error) {
	return connect.NewResponse(&v1.GetRetentionResponse{}), nil
}

// TestServeRetentionDisabledText checks the service text of a create
// without a request domain; the environment text is the C1 baseline's,
// unchanged.
func TestServeRetentionDisabledText(t *testing.T) {
	info := noRetention{&serveControlInfo{endpoint: []v1.Capability{v1.Capability_CAPABILITY_SERVICES_V1}}}
	b := &badServing{servingFixture: &servingFixture{}}
	mux := http.NewServeMux()
	mux.Handle(skyv1connect.NewControlServiceHandler(info))
	mux.Handle(skyv1connect.NewServingServiceHandler(b))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	stdout, _, err := execute(context.Background(), "--endpoint", srv.URL, "serve", "up", writeServeSpec(t), "--name", "web", "--target", "kube")
	if err == nil || err.Error() != "server retains no request domain; service create requires one" || stdout != "" {
		t.Fatalf("stdout %q, err %v", stdout, err)
	}
	if len(b.creates) != 0 {
		t.Fatal("create sent without a request domain")
	}
}

func TestServeBadSummaryResponses(t *testing.T) {
	for name, change := range map[string]func(*v1.ServiceSummary){
		"unknown": func(s *v1.ServiceSummary) { unknownField(s) },
		"unknown observation": func(s *v1.ServiceSummary) {
			s.Status.Observation = &v1.ServiceObservation{}
			unknownField(s.Status.Observation)
		},
		"no current":       func(s *v1.ServiceSummary) { s.Current = nil },
		"digest":           func(s *v1.ServiceSummary) { s.Sha256 = "x" },
		"name":             func(s *v1.ServiceSummary) { s.Name = "WEB" },
		"generation":       func(s *v1.ServiceSummary) { s.Status.DesiredGeneration = 7 },
		"ready unobserved": func(s *v1.ServiceSummary) { s.Status.Phase = v1.ServicePhase_SERVICE_PHASE_READY },
		"no status":        func(s *v1.ServiceSummary) { s.Status = nil },
		"reapply":          func(s *v1.ServiceSummary) { s.Current.Kind = v1.ServiceRolloutKind_SERVICE_ROLLOUT_KIND_REAPPLY },
		"ready all zero": func(s *v1.ServiceSummary) {
			s.Status.Phase, s.Status.Observation = v1.ServicePhase_SERVICE_PHASE_READY, &v1.ServiceObservation{}
		},
		"ready zero endpoints": func(s *v1.ServiceSummary) {
			s.Status.Phase, s.Status.Observation = v1.ServicePhase_SERVICE_PHASE_READY, readyObservation()
			s.Status.Observation.ReadyEndpoints = 0
		},
		"absent desired ready": func(s *v1.ServiceSummary) {
			s.Desired = v1.ServiceDesired_SERVICE_DESIRED_ABSENT
			s.Status.Phase, s.Status.Observation = v1.ServicePhase_SERVICE_PHASE_READY, readyObservation()
		},
		"available over ready": func(s *v1.ServiceSummary) {
			s.Status.Observation = &v1.ServiceObservation{ObservedGeneration: 1, DesiredReplicas: 1, AvailableReplicas: 1}
		},
		"scaling": func(s *v1.ServiceSummary) {
			s.Status.Observation = &v1.ServiceObservation{Scaling: v1.ServiceScalingCondition_SERVICE_SCALING_CONDITION_HEALTHY}
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			spec := writeServeSpec(t)
			b := &badServing{servingFixture: &servingFixture{}}
			srv := serveServices(t, &serveControlInfo{endpoint: []v1.Capability{v1.Capability_CAPABILITY_SERVICES_V1}}, b)
			for _, n := range []string{"web", "api"} {
				if _, _, err := execute(ctx, "--endpoint", srv.URL, "serve", "up", spec, "--name", n, "--target", "kube"); err != nil {
					t.Fatal(err)
				}
			}
			b.set(func(b *badServing) { b.list = change }) // the second service; the first is valid
			for _, format := range [][]string{nil, {"--json"}} {
				args := append(append([]string{"--endpoint", srv.URL}, format...), "serve", "status")
				stdout, _, err := execute(ctx, args...)
				if err == nil || stdout != "" {
					t.Errorf("%v: stdout %q, err %v; want an error and no output", format, stdout, err)
				}
			}
		})
	}
}
