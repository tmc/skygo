//go:build linux || darwin

package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/environmentcontrol"
	"github.com/tmc/skygo/internal/environmentwire"
	"github.com/tmc/skygo/internal/store"
)

// Source-only; NOT_RUN. These tests use the actual server, Store, file auth
// Engine, Control, environment Controller and RPC handler. Only the runtime
// is MODELED (providerRuntime): it owns no native resource, so they prove
// mounting, routing, capability projection and shutdown custody, not any
// native environment behavior.

const providerTarget = "remote"

// providerRuntime is a MODELED environmentcontrol.Runtime of target
// providerTarget, without artifacts. Ensure observes Ready and Delete
// Deleted. Exec relays argv as stdout with exit status zero; argv[0] "hold"
// starts a frame that settles only once release is closed, whatever Cancel
// does. Forward answers 200 with the method, the escaped path and whether an
// Authorization header reached it.
type providerRuntime struct {
	release chan struct{}
	held    chan struct{} // closed when a "hold" frame starts

	mu       sync.Mutex
	frames   []*providerExec
	canceled atomic.Int32
}

func newProviderRuntime() *providerRuntime {
	return &providerRuntime{release: make(chan struct{}), held: make(chan struct{})}
}

func (*providerRuntime) Kind() string { return "modeled" }

func (*providerRuntime) Plan(principal string, spec environment.Spec, digest [32]byte) (string, []byte, error) {
	if spec.Target != providerTarget || len(spec.Artifacts) != 0 {
		return "", nil, environmentcontrol.ErrUnsupported
	}
	return "modeled-binding", []byte("modeled"), nil
}

func (*providerRuntime) Ensure(context.Context, environmentcontrol.Ref) (environmentcontrol.Observation, error) {
	return environmentcontrol.Observation{Phase: environment.Ready}, nil
}

func (*providerRuntime) Delete(context.Context, environmentcontrol.Ref) (environmentcontrol.Observation, error) {
	return environmentcontrol.Observation{Phase: environment.Deleted}, nil
}

type providerExec struct {
	rt   *providerRuntime
	done chan struct{}
	res  environmentcontrol.ExecResult
}

func (x *providerExec) Done() <-chan struct{} { return x.done }
func (x *providerExec) Cancel()               { x.rt.canceled.Add(1) }

func (x *providerExec) Result() environmentcontrol.ExecResult {
	select {
	case <-x.done:
		r := x.res
		r.Settled = true
		return r
	default:
		return environmentcontrol.ExecResult{}
	}
}

func (rt *providerRuntime) Exec(r environmentcontrol.Ref, req environmentcontrol.ExecRequest) (environmentcontrol.Exec, error) {
	x := &providerExec{rt: rt, done: make(chan struct{})}
	rt.mu.Lock()
	rt.frames = append(rt.frames, x)
	rt.mu.Unlock()
	if req.Argv[0] != "hold" {
		x.res = environmentcontrol.ExecResult{Exited: true, Stdout: []byte(strings.Join(req.Argv, " "))}
		close(x.done)
		return x, nil
	}
	close(rt.held)
	go func() {
		<-rt.release
		x.res = environmentcontrol.ExecResult{Err: errors.New("released")}
		close(x.done)
	}()
	return x, nil
}

type providerForward struct{ done chan struct{} }

func (f providerForward) Done() <-chan struct{} { return f.done }
func (providerForward) Cancel()                 {}
func (providerForward) Result() environmentcontrol.ForwardResult {
	return environmentcontrol.ForwardResult{Settled: true, Status: http.StatusOK}
}

func (*providerRuntime) Forward(w http.ResponseWriter, req *http.Request, r environmentcontrol.Ref) environmentcontrol.Forward {
	fmt.Fprintf(w, "%s %s auth=%v", req.Method, req.URL.EscapedPath(), req.Header.Get("Authorization") != "")
	done := make(chan struct{})
	close(done)
	return providerForward{done: done}
}

// join waits for every exec frame the runtime started, as a real runtime's
// Close must; a canceled frame is not joined until its Done closes.
func (rt *providerRuntime) join(ctx context.Context) error {
	rt.mu.Lock()
	frames := slices.Clone(rt.frames)
	rt.mu.Unlock()
	for _, x := range frames {
		select {
		case <-x.done:
		case <-ctx.Done():
			return fmt.Errorf("modeled exec frame unsettled: %w", ctx.Err())
		}
	}
	return nil
}

// providerIntegration is an EnvironmentProvider over the actual
// environmentcontrol.Integration and a providerRuntime. Its Close joins the
// Controller and then every runtime frame.
type providerIntegration struct {
	environmentcontrol.Integration
	targets       []backend.Target
	rt            *providerRuntime
	nilController bool
	closes        atomic.Int32
}

func newProviderIntegration() *providerIntegration {
	return &providerIntegration{rt: newProviderRuntime(), targets: []backend.Target{{
		Name: providerTarget, Kind: "modeled", Binding: "provider-fixture", Backend: &lifetimeRemote{},
		OutputStorage: backend.RemoteOutput, Retention: &backend.Retention{RemoveExecution: func(context.Context, backend.Reference) error { return nil }},
		SupportsPersistentEnvironments: true,
	}}}
}

func (in *providerIntegration) install(s *store.Store, e *auth.Engine) (Integration, error) {
	c, err := environmentcontrol.New(environmentcontrol.Config{Store: s, Auth: e, Runtime: in.rt})
	if err != nil {
		return nil, err
	}
	in.Integration = environmentcontrol.Integration{Controller: c}
	return in, nil
}

func (in *providerIntegration) Targets() []backend.Target { return in.targets }

func (in *providerIntegration) EnvironmentController() *environmentcontrol.Controller {
	if in.nilController {
		return nil
	}
	return in.Controller
}

func (in *providerIntegration) Close(ctx context.Context) error {
	in.closes.Add(1)
	return errors.Join(in.Integration.Close(ctx), in.rt.join(ctx))
}

// providerAuth activates scoped state and enabled retention offline, with
// operators alice and bob.
func providerAuth(t *testing.T, f *lifetimeServerFixture) (a *realAuth, bob string) {
	t.Helper()
	a = newRealAuth(t, f)
	if err := auth.AddPrincipal(a.path, "bob", auth.Operator); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(filepath.Dir(a.path), "bob.token")
	if _, err := auth.Issue(a.path, "bob", file, 10*time.Minute); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	policy := store.DefaultRetentionPolicy()
	policy.LocalOutputEnabled = false
	if _, err := store.EnableRetention(f.data, policy); err != nil {
		t.Fatal(err)
	}
	return a, strings.TrimSuffix(string(data), "\n")
}

func bearer[T any](msg *T, token string) *connect.Request[T] {
	r := connect.NewRequest(msg)
	r.Header().Set("Authorization", "Bearer "+token)
	return r
}

func providerSpec() environment.Spec {
	return environment.Spec{Name: "dev", Target: providerTarget, Image: "registry.example.com/idle:1",
		CPUMillis: 100, MemoryBytes: 64 << 20, Port: environment.Port}
}

func hasCapability(caps []v1.Capability, c v1.Capability) bool { return slices.Contains(caps, c) }

// TestEnvironmentProviderServes checks that an installed EnvironmentProvider
// is served: GetServerInfo reports the endpoint and the selected target's
// persistent capability, CreateEnvironment accepts in the actual retention
// domain, the Controller's Run makes it Ready, ExecEnvironmentCommand and the
// authenticated forward route reach the runtime without the bearer, and
// another principal sees nothing.
func TestEnvironmentProviderServes(t *testing.T) {
	f := lifetimeFixture(t)
	a, bob := providerAuth(t, f)
	in := newProviderIntegration()
	f.start(t, Config{DisableLocal: true, AuthFile: a.path, Install: in.install})
	a.serves(t, f)
	ctx := context.Background()

	api := skyv1connect.NewControlServiceClient(f.client, f.url)
	info, err := api.GetServerInfo(ctx, bearer(&v1.GetServerInfoRequest{}, a.alice))
	if err != nil {
		t.Fatal(err)
	}
	if !hasCapability(info.Msg.Capabilities, v1.Capability_CAPABILITY_ENVIRONMENTS_V1) {
		t.Fatalf("server capabilities = %v, want ENVIRONMENTS_V1", info.Msg.Capabilities)
	}
	if len(info.Msg.Targets) != 1 || !hasCapability(info.Msg.Targets[0].Capabilities, v1.Capability_CAPABILITY_PERSISTENT_ENVIRONMENTS_V1) ||
		hasCapability(info.Msg.Targets[0].Capabilities, v1.Capability_CAPABILITY_ENVIRONMENT_ARTIFACTS_V1) {
		t.Fatalf("targets = %v, want %s with persistent environments only", info.Msg.Targets, providerTarget)
	}
	retention, err := api.GetRetention(ctx, bearer(&v1.GetRetentionRequest{}, a.alice))
	if err != nil {
		t.Fatal(err)
	}

	envs := skyv1connect.NewEnvironmentServiceClient(f.client, f.url)
	created, err := envs.CreateEnvironment(ctx, bearer(&v1.CreateEnvironmentRequest{
		RequestId: "provider-1", Spec: environmentwire.SpecToProto(providerSpec()), Domain: retention.Msg.Domain}, a.alice))
	if err != nil {
		t.Fatal(err)
	}
	id := created.Msg.Environment.Id
	deadline := time.Now().Add(15 * time.Second)
	for {
		got, err := envs.GetEnvironment(ctx, bearer(&v1.GetEnvironmentRequest{Id: id}, a.alice))
		if err != nil {
			t.Fatal(err)
		}
		if got.Msg.Environment.Phase == v1.EnvironmentPhase_ENVIRONMENT_PHASE_READY {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("environment phase = %v, want Ready from the integration's Run", got.Msg.Environment.Phase)
		}
		time.Sleep(20 * time.Millisecond)
	}

	out, err := envs.ExecEnvironmentCommand(ctx, bearer(&v1.ExecEnvironmentCommandRequest{Id: id, Argv: []string{"echo", "hi"}}, a.alice))
	if err != nil {
		t.Fatal(err)
	}
	if string(out.Msg.Stdout) != "echo hi" || out.Msg.ExitCode == nil || *out.Msg.ExitCode != 0 {
		t.Fatalf("exec = %q, %v", out.Msg.Stdout, out.Msg.ExitCode)
	}

	forward := func(token string) (int, string) {
		req, err := http.NewRequest(http.MethodGet, f.url+"/environments/"+id+"/forward/a%2Fb?x=1", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := f.client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, string(body)
	}
	if code, body := forward(a.alice); code != http.StatusOK || body != "GET /a%2Fb auth=false" {
		t.Fatalf("forward = %d %q, want 200 with the escaped path and no credential", code, body)
	}
	if code, _ := forward("not-a-credential"); code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated forward = %d, want 401", code)
	}
	if code, _ := forward(bob); code != http.StatusNotFound {
		t.Fatalf("foreign forward = %d, want 404", code)
	}
	if _, err := envs.GetEnvironment(ctx, bearer(&v1.GetEnvironmentRequest{Id: id}, bob)); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("foreign Get = %v, want NotFound", err)
	}
	if _, err := envs.ExecEnvironmentCommand(ctx, bearer(&v1.ExecEnvironmentCommandRequest{Id: id, Argv: []string{"echo"}}, bob)); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("foreign Exec = %v, want NotFound", err)
	}
	other := providerSpec()
	other.Name, other.Target = "elsewhere", "other"
	if _, err := envs.CreateEnvironment(ctx, bearer(&v1.CreateEnvironmentRequest{
		RequestId: "provider-2", Spec: environmentwire.SpecToProto(other), Domain: retention.Msg.Domain}, a.alice)); err == nil {
		t.Fatal("Create on an unserved target succeeded")
	}

	f.stopped(t)
	if n := in.closes.Load(); n != 1 {
		t.Fatalf("%d integration Close calls, want 1", n)
	}
	released(t, f)
}

// TestEnvironmentProviderAbsent checks that a server without an
// EnvironmentProvider serves no environment endpoint and advertises no
// environment capability, and that a target claiming environment support
// outside such a provider fails startup.
func TestEnvironmentProviderAbsent(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		f := lifetimeFixture(t)
		a := newRealAuth(t, f)
		in := newTestIntegration()
		f.start(t, a.config(in))
		a.serves(t, f)
		ctx := context.Background()
		info, err := skyv1connect.NewControlServiceClient(f.client, f.url).GetServerInfo(ctx, bearer(&v1.GetServerInfoRequest{}, a.alice))
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range []v1.Capability{v1.Capability_CAPABILITY_ENVIRONMENTS_V1, v1.Capability_CAPABILITY_PERSISTENT_ENVIRONMENTS_V1, v1.Capability_CAPABILITY_ENVIRONMENT_ARTIFACTS_V1} {
			if hasCapability(info.Msg.Capabilities, c) {
				t.Fatalf("server capabilities = %v without a provider", info.Msg.Capabilities)
			}
			for _, target := range info.Msg.Targets {
				if hasCapability(target.Capabilities, c) {
					t.Fatalf("target %s capabilities = %v without a provider", target.Name, target.Capabilities)
				}
			}
		}
		_, err = skyv1connect.NewEnvironmentServiceClient(f.client, f.url).ListEnvironments(ctx, bearer(&v1.ListEnvironmentsRequest{}, a.alice))
		if connect.CodeOf(err) != connect.CodeUnimplemented {
			t.Fatalf("ListEnvironments without a provider = %v, want unimplemented", err)
		}
		f.stopped(t)
	})
	for _, tt := range []struct {
		name   string
		config func(*testIntegration) Config
	}{
		{"configured targets", func(in *testIntegration) Config {
			return Config{DisableLocal: true, Install: in.install, Targets: func(string) ([]backend.Target, error) {
				return []backend.Target{{Name: "claims", Binding: "claims-fixture", Backend: &lifetimeRemote{}, SupportsPersistentEnvironments: true}}, nil
			}}
		}},
		{"plain integration", func(in *testIntegration) Config {
			in.targets[0].SupportsPersistentEnvironments = true
			return Config{DisableLocal: true, Install: in.install}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := lifetimeFixture(t)
			in := newTestIntegration()
			cfg := tt.config(in)
			cfg.DataDir, cfg.Listen = f.data, "127.0.0.1:0"
			err := Run(context.Background(), cfg)
			if err == nil || !strings.Contains(err.Error(), "environment support requires an environment integration") {
				t.Fatalf("Run = %v, want the environment support refusal", err)
			}
			if closes(in) != 1 {
				t.Fatalf("integration calls = %q, want one Close", in.recorded())
			}
			released(t, f)
		})
	}
}

// TestEnvironmentProviderNilController checks that a provider with no
// Controller fails startup, before the listener, and is still closed.
func TestEnvironmentProviderNilController(t *testing.T) {
	f := lifetimeFixture(t)
	a, _ := providerAuth(t, f)
	in := newProviderIntegration()
	in.nilController = true
	err := Run(context.Background(), Config{DataDir: f.data, Listen: "127.0.0.1:0", DisableLocal: true, AuthFile: a.path, Install: in.install})
	if err == nil || !strings.Contains(err.Error(), "environment integration has no controller") {
		t.Fatalf("Run = %v, want the missing controller refusal", err)
	}
	if n := in.closes.Load(); n != 1 {
		t.Fatalf("%d integration Close calls, want 1", n)
	}
	released(t, f)
}

// TestEnvironmentProviderHeldExec checks that an exec frame that outlives
// its RPC, and ignores cancellation, keeps the integration's Close from
// succeeding: the server returns a RunError holding the same integration
// with its Store and authentication open, and the frame was canceled, not
// joined by fiat.
func TestEnvironmentProviderHeldExec(t *testing.T) {
	f := lifetimeFixture(t)
	a, _ := providerAuth(t, f)
	in := newProviderIntegration()
	f.start(t, Config{DisableLocal: true, AuthFile: a.path, Install: in.install})
	a.serves(t, f)
	ctx := context.Background()
	retention, err := skyv1connect.NewControlServiceClient(f.client, f.url).GetRetention(ctx, bearer(&v1.GetRetentionRequest{}, a.alice))
	if err != nil {
		t.Fatal(err)
	}
	envs := skyv1connect.NewEnvironmentServiceClient(&http.Client{}, f.url)
	created, err := envs.CreateEnvironment(ctx, bearer(&v1.CreateEnvironmentRequest{
		RequestId: "provider-held", Spec: environmentwire.SpecToProto(providerSpec()), Domain: retention.Msg.Domain}, a.alice))
	if err != nil {
		t.Fatal(err)
	}
	id := created.Msg.Environment.Id
	deadline := time.Now().Add(15 * time.Second)
	for {
		got, err := envs.GetEnvironment(ctx, bearer(&v1.GetEnvironmentRequest{Id: id}, a.alice))
		if err != nil {
			t.Fatal(err)
		}
		if got.Msg.Environment.Phase == v1.EnvironmentPhase_ENVIRONMENT_PHASE_READY {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("environment did not become Ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The caller leaves: the Controller cancels the frame and returns while
	// it is unsettled.
	call, leave := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = envs.ExecEnvironmentCommand(call, bearer(&v1.ExecEnvironmentCommandRequest{Id: id, Argv: []string{"hold"}}, a.alice))
	}()
	integrationReceive(t, in.rt.held, "held exec frame")
	leave()
	integrationReceive(t, done, "exec RPC after its caller left")

	f.cancel()
	integrationReceive(t, f.done, "server Run with a held exec frame")
	var held *RunError
	if !errors.As(f.result, &held) || held.stage != "integration closure remains held" || !errors.Is(f.result, context.DeadlineExceeded) {
		t.Fatalf("Run = %v, want a RunError holding the integration's Close", f.result)
	}
	// Retain the same held original before any further assertion can stop
	// the test.
	f.expectedHeld = context.DeadlineExceeded
	f.hold(t, f.result)
	if held.frame.integration != Integration(in) {
		t.Fatal("held frame lost the original integration")
	}
	if _, _, err := held.frame.store.RetentionPolicy(); err != nil {
		t.Fatalf("Store read in the held frame = %v, want the Store open", err)
	}
	a.engineServes(t, held.frame.engine)
	if in.rt.canceled.Load() == 0 {
		t.Fatal("held exec frame was never canceled")
	}
	// The original frame settles only through its own release; the server
	// frame stays held regardless.
	close(in.rt.release)
	if err := in.rt.join(ctx); err != nil {
		t.Fatal(err)
	}
}
