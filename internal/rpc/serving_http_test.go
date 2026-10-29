package rpc_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/serving"
	"github.com/tmc/skygo/internal/servingcontrol"
	"github.com/tmc/skygo/internal/servingwire"
	"github.com/tmc/skygo/internal/store"
)

// AUTHORED_NOT_RUN. These tests serve ServingService through the real
// handler, file authentication and Store. The runtime is a model that
// records calls and never reaches a cluster.

type idleRuntime struct{}

func (idleRuntime) Check(serving.Spec) error { return nil }
func (idleRuntime) Apply(_ context.Context, ref serving.Ref, _ serving.Revision, _ uint64) (serving.Native, error) {
	return ref.Native, errors.New("model runtime applies nothing")
}
func (idleRuntime) Observe(context.Context, serving.Ref) (serving.Observation, error) {
	return serving.Observation{}, errors.New("model runtime observes nothing")
}
func (idleRuntime) Delete(context.Context, serving.Ref) (serving.Observation, error) {
	return serving.Observation{}, errors.New("model runtime deletes nothing")
}
func (idleRuntime) Forward(context.Context, serving.Ref, http.ResponseWriter, *http.Request) error {
	return errors.New("model runtime forwards nothing")
}

type servingHTTP struct {
	state  string
	path   string
	tokens map[string]string
	store  *store.Store
	engine *auth.Engine
	domain *v1.RequestDomain
}

func newServingHTTP(t *testing.T) *servingHTTP {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	f := &servingHTTP{state: filepath.Join(dir, "state"), path: filepath.Join(dir, "auth.json"), tokens: map[string]string{}}
	info, err := store.EnableRetention(f.state, store.DefaultRetentionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	f.domain = &v1.RequestDomain{Lineage: info.Domain.Lineage, Epoch: info.Domain.Epoch}
	s, err := store.Open(f.state)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	f.store = s
	if _, err := auth.Initialize(f.path, filepath.Join(dir, "legacy.token"), s.Owner()); err != nil {
		t.Fatal(err)
	}
	for _, p := range []struct {
		name string
		role auth.Role
	}{{"alice", auth.Operator}, {"bob", auth.Operator}, {"admin", auth.Admin}} {
		if err := auth.AddPrincipal(f.path, p.name, p.role); err != nil {
			t.Fatal(err)
		}
		token := filepath.Join(dir, p.name+".token")
		if _, err := auth.Issue(f.path, p.name, token, time.Hour); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(token)
		if err != nil {
			t.Fatal(err)
		}
		f.tokens[p.name] = strings.TrimSpace(string(raw))
	}
	if err := s.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	if f.engine, err = auth.NewFile(f.path, s.Owner()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); f.engine.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	return f
}

// serve starts one server process's handler over f's Store and Engine: a
// fresh Controller, closed at cleanup.
func (f *servingHTTP) serve(t *testing.T) skyv1connect.ServingServiceClient {
	t.Helper()
	c, err := servingcontrol.New(servingcontrol.Config{Store: f.store, Auth: f.engine, Target: "kube", Binding: "kube/default", Runtime: idleRuntime{}, Limits: store.ServiceLimits{CPUMillis: 4000, MemoryBytes: 8 << 30}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	ctl, err := control.New(f.store, []backend.Target{{Name: "kube", Kind: "kubernetes", Binding: "kube/default", Backend: &logBackend{}}}, f.engine)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(rpc.NewIntegratedHandler(ctl, f.engine, nil, c))
	t.Cleanup(server.Close)
	return skyv1connect.NewServingServiceClient(server.Client(), server.URL)
}

func servingSpec(t *testing.T) *v1.ServiceSpec {
	t.Helper()
	s, err := serving.Spec{
		Image:     "registry.example.com/team/web@sha256:" + strings.Repeat("ab", 32),
		Argv:      []string{"/bin/web"},
		Port:      8080,
		Readiness: serving.Readiness{Path: "/healthz"},
		Resources: serving.Resources{CPUMillis: 250, MemoryBytes: 256 << 20},
		Scaling:   serving.Scaling{Replicas: 1},
	}.Normalize()
	if err != nil {
		t.Fatal(err)
	}
	return servingwire.SpecToProto(s)
}

func (f *servingHTTP) create(t *testing.T, client skyv1connect.ServingServiceClient, who, name string) *v1.Service {
	t.Helper()
	r, err := client.CreateService(context.Background(), authorized(f.tokens[who], &v1.CreateServiceRequest{
		RequestId: "create-" + name, Name: name, Target: "kube", Spec: servingSpec(t), Domain: f.domain,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := servingwire.CheckService(r.Msg.Service); err != nil {
		t.Fatalf("created service fails the client's check: %v", err)
	}
	return r.Msg.Service
}

func list(client skyv1connect.ServingServiceClient, token, page string) (*v1.ListServicesResponse, error) {
	r, err := client.ListServices(context.Background(), authorized(token, &v1.ListServicesRequest{PageSize: 1, PageToken: page}))
	if err != nil {
		return nil, err
	}
	return r.Msg, nil
}

// TestServingHTTPVisibility checks each principal's view through the real
// handler, file authentication and Store.
func TestServingHTTPVisibility(t *testing.T) {
	f := newServingHTTP(t)
	client := f.serve(t)
	a := f.create(t, client, "alice", "web")
	b := f.create(t, client, "bob", "api")
	for _, tt := range []struct {
		who, id string
		want    connect.Code
	}{
		{"alice", a.Id, 0},
		{"alice", b.Id, connect.CodeNotFound},
		{"bob", a.Id, connect.CodeNotFound},
		{"admin", a.Id, 0},
	} {
		r, err := client.GetService(context.Background(), authorized(f.tokens[tt.who], &v1.GetServiceRequest{Id: tt.id}))
		if connect.CodeOf(err) != tt.want && !(tt.want == 0 && err == nil) {
			t.Errorf("%s get %s = %v, want %v", tt.who, tt.id, err, tt.want)
		}
		if err == nil {
			if err := servingwire.CheckService(r.Msg.Service); err != nil {
				t.Errorf("%s get fails the client's check: %v", tt.who, err)
			}
		}
	}
	_, err := client.DeleteService(context.Background(), authorized(f.tokens["bob"], &v1.DeleteServiceRequest{
		RequestId: "delete-web", Id: a.Id, ExpectedRecordRevision: a.RecordRevision, Domain: f.domain,
	}))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("bob deletes alice's service: %v", err)
	}
	d, err := client.DeleteService(context.Background(), authorized(f.tokens["alice"], &v1.DeleteServiceRequest{
		RequestId: "delete-web", Id: a.Id, ExpectedRecordRevision: a.RecordRevision, Domain: f.domain,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := servingwire.CheckService(d.Msg.Service); err != nil || d.Msg.Service.Desired != v1.ServiceDesired_SERVICE_DESIRED_ABSENT {
		t.Fatalf("delete = %v, %v", d.Msg.Service, err)
	}
}

// TestServingHTTPListCursor checks that the page token is opaque, bound to
// its principal, canonical, bounded, and valid across a restart.
func TestServingHTTPListCursor(t *testing.T) {
	f := newServingHTTP(t)
	client := f.serve(t)
	f.create(t, client, "alice", "web")
	f.create(t, client, "alice", "api")
	f.create(t, client, "bob", "db")

	first, err := list(client, f.tokens["alice"], "")
	if err != nil {
		t.Fatal(err)
	}
	token := first.NextPageToken
	if len(first.Services) != 1 || token == "" || len(token) > 512 {
		t.Fatalf("first page = %d services, token %q", len(first.Services), token)
	}
	for _, s := range first.Services {
		if err := servingwire.CheckSummary(s); err != nil {
			t.Fatalf("listed service fails the client's check: %v", err)
		}
	}
	// Another principal's token is refused, whatever its position.
	for _, who := range []string{"bob", "admin"} {
		if _, err := list(client, f.tokens[who], token); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("%s with alice's token = %v, want invalid_argument", who, err)
		}
	}
	// Tokens this server does not issue are refused.
	flip := []byte(token)
	flip[len(flip)-1] ^= 1
	for _, bad := range []string{
		"1",                      // the old decimal ordinal
		token + "=",              // padded
		token + "A",              // trailing data
		string(flip),             // changed position or non-canonical bits
		strings.Repeat("A", 513), // over the bound
		"AQ",                     // truncated
	} {
		if _, err := list(client, f.tokens["alice"], bad); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("token %.20q = %v, want invalid_argument", bad, err)
		}
	}
	// A restarted server, with a new Controller over the same Store,
	// accepts the token and continues the listing.
	restarted := f.serve(t)
	second, err := list(restarted, f.tokens["alice"], token)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Services) != 1 || second.Services[0].Id == first.Services[0].Id || second.Services[0].Name == "db" {
		t.Fatalf("second page = %v", second.Services)
	}
}
