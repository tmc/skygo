//go:build linux || darwin

package server

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/environmentcontrol"
	"github.com/tmc/skygo/internal/serving"
	"github.com/tmc/skygo/internal/servingcontrol"
	"github.com/tmc/skygo/internal/store"
)

// AUTHORED_NOT_RUN. These tests compose the actual server with a
// ServingProvider, an EnvironmentProvider, or one integration that is both.
// The serving runtime is MODELED (idleServingRuntime): it applies nothing.

type idleServingRuntime struct{}

func (idleServingRuntime) Check(serving.Spec) error { return nil }
func (idleServingRuntime) Apply(_ context.Context, ref serving.Ref, _ serving.Revision, _ uint64) (serving.Native, error) {
	return ref.Native, errors.New("modeled runtime applies nothing")
}
func (idleServingRuntime) Observe(context.Context, serving.Ref) (serving.Observation, error) {
	return serving.Observation{}, errors.New("modeled runtime observes nothing")
}
func (idleServingRuntime) Delete(context.Context, serving.Ref) (serving.Observation, error) {
	return serving.Observation{}, errors.New("modeled runtime deletes nothing")
}
func (idleServingRuntime) Forward(context.Context, serving.Ref, http.ResponseWriter, *http.Request) error {
	return errors.New("modeled runtime forwards nothing")
}

// composedIntegration is a ServingProvider and, with environments, also
// an EnvironmentProvider. It counts Targets and Close calls.
type composedIntegration struct {
	env          *providerIntegration // nil without environments
	services     *servingcontrol.Controller
	withServices bool
	nilServices  bool
	targets      []backend.Target
	targetCalls  atomic.Int32
	closes       atomic.Int32
}

// servingOnly hides the EnvironmentProvider methods of a composed
// integration without environments.
type servingOnly struct{ *composedIntegration }

func (in *composedIntegration) Targets() []backend.Target {
	in.targetCalls.Add(1)
	return in.targets
}

func (in *composedIntegration) ServingController() *servingcontrol.Controller {
	if in.nilServices {
		return nil
	}
	return in.services
}

func (in *composedIntegration) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (in *composedIntegration) Close(ctx context.Context) error {
	in.closes.Add(1)
	var errs []error
	if in.services != nil {
		errs = append(errs, in.services.Close(ctx))
	}
	if in.env != nil {
		errs = append(errs, in.env.Close(ctx))
	}
	return errors.Join(errs...)
}

// composedEnvironments is composedIntegration with environments.
type composedEnvironments struct{ *composedIntegration }

func (in composedEnvironments) EnvironmentController() *environmentcontrol.Controller {
	return in.env.EnvironmentController()
}

func serviceTarget() backend.Target {
	return backend.Target{
		Name: "svc", Kind: "modeled", Binding: "svc-fixture", Backend: &lifetimeRemote{},
		OutputStorage: backend.RemoteOutput, Retention: &backend.Retention{RemoveExecution: func(context.Context, backend.Reference) error { return nil }},
	}
}

func newComposed(environments, services bool) *composedIntegration {
	in := &composedIntegration{withServices: services}
	if environments {
		in.env = newProviderIntegration()
		in.targets = append(in.targets, in.env.targets...)
	}
	if services {
		in.targets = append(in.targets, serviceTarget())
	}
	return in
}

func (in *composedIntegration) install(s *store.Store, e *auth.Engine) (Integration, error) {
	if in.env != nil {
		if _, err := in.env.install(s, e); err != nil {
			return nil, err
		}
	}
	if in.withServices {
		c, err := servingcontrol.New(servingcontrol.Config{Store: s, Auth: e, Target: "svc", Binding: "svc-fixture", Runtime: idleServingRuntime{}, Limits: store.ServiceLimits{CPUMillis: 4000, MemoryBytes: 8 << 30}})
		if err != nil {
			return in, err
		}
		in.services = c
	}
	switch {
	case in.env != nil && in.withServices:
		return composedEnvironments{in}, nil
	case in.env != nil:
		return in.env, nil
	}
	return servingOnly{in}, nil
}

func TestServingProviderComposition(t *testing.T) {
	for _, tt := range []struct {
		name                   string
		environments, services bool
	}{
		{"service only", false, true},
		{"environment only", true, false},
		{"combined", true, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := lifetimeFixture(t)
			a, _ := providerAuth(t, f)
			in := newComposed(tt.environments, tt.services)
			f.start(t, Config{DisableLocal: true, AuthFile: a.path, Install: in.install})
			a.serves(t, f)
			ctx := context.Background()
			info, err := skyv1connect.NewControlServiceClient(f.client, f.url).GetServerInfo(ctx, bearer(&v1.GetServerInfoRequest{}, a.alice))
			if err != nil {
				t.Fatal(err)
			}
			if hasCapability(info.Msg.Capabilities, v1.Capability_CAPABILITY_SERVICES_V1) != tt.services ||
				hasCapability(info.Msg.Capabilities, v1.Capability_CAPABILITY_ENVIRONMENTS_V1) != tt.environments {
				t.Fatalf("capabilities = %v", info.Msg.Capabilities)
			}
			_, err = skyv1connect.NewServingServiceClient(f.client, f.url).ListServices(ctx, bearer(&v1.ListServicesRequest{}, a.alice))
			if tt.services && err != nil || !tt.services && connect.CodeOf(err) != connect.CodeUnimplemented {
				t.Fatalf("ListServices = %v", err)
			}
			_, err = skyv1connect.NewEnvironmentServiceClient(f.client, f.url).ListEnvironments(ctx, bearer(&v1.ListEnvironmentsRequest{}, a.alice))
			if tt.environments && err != nil || !tt.environments && connect.CodeOf(err) != connect.CodeUnimplemented {
				t.Fatalf("ListEnvironments = %v", err)
			}
			f.stopped(t)
			if n := in.targetCalls.Load(); tt.services && n != 1 {
				t.Fatalf("%d Targets calls, want 1", n)
			}
			if tt.environments && !tt.services {
				if n := in.env.closes.Load(); n != 1 {
					t.Fatalf("%d integration Close calls, want 1", n)
				}
			} else if n := in.closes.Load(); n != 1 {
				t.Fatalf("%d integration Close calls, want 1", n)
			}
			released(t, f)
		})
	}
}

// TestServingProviderMissingController keeps the startup refusals: a
// serving integration without a controller, and a combined one with
// neither, fail before the listener and are closed once.
func TestServingProviderMissingController(t *testing.T) {
	for _, tt := range []struct {
		name         string
		environments bool
		want         string
	}{
		{"service only", false, "serving integration has no controller"},
		{"combined", true, "environment integration has no controller"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := lifetimeFixture(t)
			a, _ := providerAuth(t, f)
			in := newComposed(tt.environments, true)
			in.nilServices = true
			if in.env != nil {
				in.env.nilController = true
			}
			err := Run(context.Background(), Config{DataDir: f.data, Listen: "127.0.0.1:0", DisableLocal: true, AuthFile: a.path, Install: in.install})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Run = %v, want %q", err, tt.want)
			}
			if n := in.closes.Load(); n != 1 {
				t.Fatalf("%d integration Close calls, want 1", n)
			}
			if n := in.targetCalls.Load(); n != 1 {
				t.Fatalf("%d Targets calls, want 1", n)
			}
			released(t, f)
		})
	}
}
