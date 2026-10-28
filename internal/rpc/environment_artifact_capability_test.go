//go:build darwin || linux

package rpc_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/environmentcontrol"
	"github.com/tmc/skygo/internal/rpc"
)

// capabilityRuntime is a modeled environment runtime that counts every
// provider call and fails it. Discovery must never make one.
type capabilityRuntime struct{ calls atomic.Int64 }

var errCapabilityIO = errors.New("modeled runtime: unexpected provider call")

func (*capabilityRuntime) Kind() string { return "capability-model" }

func (rt *capabilityRuntime) Plan(string, environment.Spec, [32]byte) (string, []byte, error) {
	rt.calls.Add(1)
	return "", nil, errCapabilityIO
}

func (rt *capabilityRuntime) Ensure(context.Context, environmentcontrol.Ref) (environmentcontrol.Observation, error) {
	rt.calls.Add(1)
	return environmentcontrol.Observation{}, errCapabilityIO
}

func (rt *capabilityRuntime) Delete(context.Context, environmentcontrol.Ref) (environmentcontrol.Observation, error) {
	rt.calls.Add(1)
	return environmentcontrol.Observation{}, errCapabilityIO
}

func (rt *capabilityRuntime) Exec(environmentcontrol.Ref, environmentcontrol.ExecRequest) (environmentcontrol.Exec, error) {
	rt.calls.Add(1)
	return nil, errCapabilityIO
}

func (rt *capabilityRuntime) Forward(w http.ResponseWriter, _ *http.Request, _ environmentcontrol.Ref) environmentcontrol.Forward {
	rt.calls.Add(1)
	http.Error(w, "unexpected forward", http.StatusInternalServerError)
	done := make(chan struct{})
	close(done)
	return capabilityForward{done: done}
}

type capabilityForward struct{ done chan struct{} }

func (f capabilityForward) Done() <-chan struct{} { return f.done }
func (capabilityForward) Cancel()                 {}
func (capabilityForward) Result() environmentcontrol.ForwardResult {
	return environmentcontrol.ForwardResult{Settled: true, Err: errCapabilityIO, Status: http.StatusInternalServerError}
}

// capabilityMaterializer is capabilityRuntime with the Materializer methods,
// which also count and fail.
type capabilityMaterializer struct{ capabilityRuntime }

func (rt *capabilityMaterializer) Materialize(environmentcontrol.Ref, string, []environment.ArtifactMount, io.Reader) (environmentcontrol.Materialization, error) {
	rt.calls.Add(1)
	return nil, errCapabilityIO
}

func (rt *capabilityMaterializer) Bootstrap(environmentcontrol.Ref, string) (environmentcontrol.Materialization, error) {
	rt.calls.Add(1)
	return nil, errCapabilityIO
}

var _ environmentcontrol.Materializer = (*capabilityMaterializer)(nil)

// TestEnvironmentArtifactCapabilityGate checks that GetServerInfo and
// ListTargets advertise CAPABILITY_ENVIRONMENT_ARTIFACTS_V1 for a target only
// when the target claims it, the handler's Control has its actual artifact
// service, and the mounted Controller's ArtifactsEnabled holds: its Runtime is
// a Materializer and an ArtifactOpener is bound. Each case uses the actual
// scoped file authentication, Store, Files-backed Control and Controller, and
// checks that discovery made no runtime or materializer call.
func TestEnvironmentArtifactCapabilityGate(t *testing.T) {
	tests := []struct {
		name string
		// targetArtifacts is the target's SupportsEnvironmentArtifacts.
		targetArtifacts bool
		// materializer selects a Runtime that is a Materializer.
		materializer bool
		// bind binds an ArtifactOpener to the Controller.
		bind bool
		// bareControl serves, and binds as opener, a Control on the same
		// Store and Engine without the artifact service.
		bareControl bool
		// unmounted serves NewHandler, without the Controller.
		unmounted bool

		wantEnabled    bool // Controller.ArtifactsEnabled
		wantPersistent bool
		wantArtifacts  bool
	}{
		{name: "materializer missing", targetArtifacts: true, bind: true, wantPersistent: true},
		{name: "opener missing", targetArtifacts: true, materializer: true, wantPersistent: true},
		{name: "control artifacts missing", targetArtifacts: true, materializer: true, bind: true, bareControl: true, wantEnabled: true, wantPersistent: true},
		{name: "unmounted", targetArtifacts: true, materializer: true, bind: true, unmounted: true, wantEnabled: true},
		{name: "target flag false", materializer: true, bind: true, wantEnabled: true, wantPersistent: true},
		{name: "full", targetArtifacts: true, materializer: true, bind: true, wantEnabled: true, wantPersistent: true, wantArtifacts: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := artifactRPCTargetOptions(t, nil, false, true, func(target *backend.Target) {
				target.SupportsPersistentEnvironments = true
				target.SupportsEnvironmentArtifacts = tt.targetArtifacts
			})
			// Environments require enabled retention; this is the fixture's
			// actual policy and request domain, read as alice.
			retention, err := f.store.RetentionInfo(actorForRPC(t, f, "alice"))
			if err != nil {
				t.Fatal(err)
			}
			if !retention.Enabled || retention.Domain.Lineage == "" || retention.Domain.Epoch == 0 {
				t.Fatalf("retention = enabled %v, domain %+v; want enabled with a request domain", retention.Enabled, retention.Domain)
			}
			served := f.control
			if tt.bareControl {
				bare, err := control.New(f.store, []backend.Target{{
					Name: "test", Binding: f.binding, Kind: "local", Backend: &logBackend{},
					SupportsPersistentEnvironments: true, SupportsEnvironmentArtifacts: tt.targetArtifacts,
				}}, f.engine)
				if err != nil {
					t.Fatal(err)
				}
				if bare.ArtifactsEnabled() {
					t.Fatal("Control without Files reports its artifact service")
				}
				served = bare
			}

			plain := &capabilityRuntime{}
			mat := &capabilityMaterializer{}
			var rt environmentcontrol.Runtime = plain
			if tt.materializer {
				rt = mat
			}
			ctl, err := environmentcontrol.New(environmentcontrol.Config{Store: f.store, Auth: f.engine, Runtime: rt})
			if err != nil {
				t.Fatal(err)
			}
			if tt.bind {
				if err := ctl.BindArtifacts(served); err != nil {
					t.Fatal(err)
				}
			}
			if got := ctl.ArtifactsEnabled(); got != tt.wantEnabled {
				t.Fatalf("Controller.ArtifactsEnabled = %v, want %v", got, tt.wantEnabled)
			}

			var handler *rpc.HTTPHandler
			if tt.unmounted {
				handler = rpc.NewHandler(served, f.engine)
			} else {
				handler = rpc.NewEnvironmentHandler(served, f.engine, ctl)
			}
			// Registered after the fixture's cleanup, so it runs first and
			// joins every handler before the Store closes.
			server := httptest.NewServer(handler)
			t.Cleanup(server.Close)
			client := skyv1connect.NewControlServiceClient(server.Client(), server.URL)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			info, err := client.GetServerInfo(ctx, authorized(f.tokens["alice"], &v1.GetServerInfoRequest{}))
			if err != nil {
				t.Fatal(err)
			}
			if got := hasCapability(info.Msg.Capabilities, v1.Capability_CAPABILITY_ENVIRONMENTS_V1); got != !tt.unmounted {
				t.Fatalf("GetServerInfo ENVIRONMENTS_V1 = %v, want %v", got, !tt.unmounted)
			}
			listed, err := client.ListTargets(ctx, authorized(f.tokens["alice"], &v1.ListTargetsRequest{}))
			if err != nil {
				t.Fatal(err)
			}
			for source, targets := range map[string][]*v1.Target{"GetServerInfo": info.Msg.Targets, "ListTargets": listed.Msg.Targets} {
				if len(targets) != 1 || targets[0].Name != "test" {
					t.Fatalf("%s targets = %v, want one target test", source, targets)
				}
				caps := targets[0].Capabilities
				if got := hasCapability(caps, v1.Capability_CAPABILITY_PERSISTENT_ENVIRONMENTS_V1); got != tt.wantPersistent {
					t.Errorf("%s PERSISTENT_ENVIRONMENTS_V1 = %v, want %v", source, got, tt.wantPersistent)
				}
				if got := hasCapability(caps, v1.Capability_CAPABILITY_ENVIRONMENT_ARTIFACTS_V1); got != tt.wantArtifacts {
					t.Errorf("%s ENVIRONMENT_ARTIFACTS_V1 = %v, want %v", source, got, tt.wantArtifacts)
				}
			}
			if n := plain.calls.Load() + mat.calls.Load(); n != 0 {
				t.Fatalf("discovery made %d runtime or materializer calls, want 0", n)
			}
		})
	}
}
