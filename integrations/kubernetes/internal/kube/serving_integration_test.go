package kube

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/servingcontrol"
	"github.com/tmc/skygo/internal/store"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

// AUTHORED_NOT_RUN. These tests use profileAPI, which MODELS the startup
// reads with an HTTP fake. The Close test holds a runtime frame by
// admitting it directly (MODELED custody), not through a real forward.

func serviceTestConfig() ServiceConfig {
	return ServiceConfig{
		target: "kube",
		grants: map[string][]string{"alice": {testServiceImage}},
		profile: serviceProfile{
			namespaceUID: "namespace-uid-1", serviceAccountUID: "service-account-uid-1",
			pullSecrets: []string{"registry-a"}, cpuMillis: 1000, memoryBytes: 1 << 30,
		},
		// servingcontrol.New refuses limits without CPU and memory budgets.
		limits: store.ServiceLimits{CPUMillis: 4000, MemoryBytes: 8 << 30},
	}
}

func withPullSecret(a *profileAPI) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.objs["/api/v1/namespaces/workloads/secrets/registry-a"] = &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Name: "registry-a", Namespace: "workloads", UID: "secret-uid-1"},
		Type:       corev1.SecretTypeDockerConfigJson,
	}
}

// countLoads replaces loadIntegrationTarget for the test and returns the
// number of loads of each target.
func countLoads(t *testing.T) func() map[string]int {
	var mu sync.Mutex
	loads := map[string]int{}
	t.Cleanup(func() { loadIntegrationTarget = loadTarget })
	loadIntegrationTarget = func(ctx context.Context, tc TargetConfig, owner string) (backend.Target, *rest.Config, error) {
		mu.Lock()
		loads[tc.Name]++
		mu.Unlock()
		return loadTarget(ctx, tc, owner)
	}
	return func() map[string]int {
		mu.Lock()
		defer mu.Unlock()
		return map[string]int{"kube": loads["kube"], "other": loads["other"]}
	}
}

func TestNewIntegrationLoadsEachTargetOnce(t *testing.T) {
	for _, tt := range []struct {
		name               string
		environments, svcs bool
	}{
		{"services", false, true},
		{"environments", true, false},
		{"both", true, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			loads := countLoads(t)
			api := newProfileAPI(t)
			withPullSecret(api)
			s, engine := environmentStore(t, true, true)
			var env EnvironmentConfig
			var svc ServiceConfig
			if tt.environments {
				env = environmentTestConfig()
			}
			if tt.svcs {
				svc = serviceTestConfig()
			}
			got, err := NewIntegration(context.Background(), s, engine, api.targets(t), env, svc)
			if err != nil {
				t.Fatal(err)
			}
			in := got.(*environmentIntegration)
			defer closeIntegration(t, in)
			if l := loads(); l["kube"] != 1 || l["other"] != 1 {
				t.Fatalf("loads = %v, want each target once", l)
			}
			targets := in.Targets()
			if len(targets) != 2 || targets[0].Name != "kube" || targets[1].Name != "other" {
				t.Fatalf("targets = %+v", targets)
			}
			if (in.EnvironmentController() != nil) != tt.environments || (in.ServingController() != nil) != tt.svcs {
				t.Fatalf("controllers: environment %v, serving %v", in.EnvironmentController() != nil, in.ServingController() != nil)
			}
			if tt.svcs && in.svc.b != targets[0].Backend.(*Backend) {
				t.Fatal("service runtime is not on the selected target's Backend")
			}
			if tt.environments && tt.svcs && in.rt.b != in.svc.b {
				t.Fatal("environment and service runtimes on different Backends")
			}
			if w := api.writes(); len(w) != 0 {
				t.Fatalf("writes = %q", w)
			}
			// Targets is a copy; the server calls it once.
			if again := in.Targets(); &again[0] == &targets[0] {
				t.Fatal("Targets returned the integration's own slice")
			}
		})
	}
}

func TestNewIntegrationServiceProfileMismatch(t *testing.T) {
	api := newProfileAPI(t) // no pull secret
	s, engine := environmentStore(t, true, true)
	got, err := NewIntegration(context.Background(), s, engine, api.targets(t), EnvironmentConfig{}, serviceTestConfig())
	if err == nil {
		t.Fatal("missing pull secret accepted")
	}
	// The runtime was built, so the integration is returned to be closed.
	if got == nil {
		t.Fatal("integration not returned with its runtime")
	}
	closeIntegration(t, got)
	if w := api.writes(); len(w) != 0 {
		t.Fatalf("writes = %q", w)
	}
}

// TestIntegrationCloseHoldsServiceFrame: Close stops both families before
// waiting, returns an error while a service frame is held, and succeeds
// once it returns.
func TestIntegrationCloseHoldsServiceFrame(t *testing.T) {
	api := newProfileAPI(t)
	withPullSecret(api)
	s, engine := environmentStore(t, true, true)
	got, err := NewIntegration(context.Background(), s, engine, api.targets(t), environmentTestConfig(), serviceTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	in := got.(*environmentIntegration)
	release, err := in.svc.begin() // MODELED: a frame whose owner has not returned
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	err = in.Close(ctx)
	cancel()
	if err == nil {
		t.Fatal("Close returned nil with a service frame held")
	}
	if _, err := in.svc.begin(); !errors.Is(err, errServiceStopped) {
		t.Fatalf("service runtime admits after Close: %v", err)
	}
	if _, err := in.svcCtl.Get(auth.Actor{}, testServiceID); !errors.Is(err, servingcontrol.ErrClosed) {
		t.Fatalf("serving controller admits after Close: %v", err)
	}
	release()
	closeIntegration(t, in)
}
