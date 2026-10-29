//go:build linux || darwin

package server

import (
	"context"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/environmentcontrol"
	"github.com/tmc/skygo/internal/servingcontrol"
	"github.com/tmc/skygo/internal/store"
)

// absentWorkerProvider exercises actual startup admission, without a runtime
// or worker effects. A nil controller must fail before the listener opens.
type absentWorkerProvider struct {
	*testIntegration
	controllers int
}

func (in *absentWorkerProvider) WorkerController() *control.Workers { in.controllers++; return nil }
func (in *absentWorkerProvider) install(s *store.Store, e *auth.Engine) (Integration, error) {
	_, err := in.testIntegration.install(s, e)
	return in, err
}

type mixedWorkerProvider struct{ *absentWorkerProvider }

func (*mixedWorkerProvider) EnvironmentController() *environmentcontrol.Controller { return nil }

type mixedServingWorkerProvider struct{ *absentWorkerProvider }

func (*mixedServingWorkerProvider) ServingController() *servingcontrol.Controller { return nil }

func TestWorkerProviderStartupRefusal(t *testing.T) {
	for _, kind := range []string{"nil", "environments", "services"} {
		t.Run(kind, func(t *testing.T) {
			f := lifetimeFixture(t)
			a, _ := providerAuth(t, f)
			in := &absentWorkerProvider{testIntegration: newTestIntegration()}
			install := in.install
			want := "worker integration has no controller"
			if kind != "nil" {
				install = func(s *store.Store, e *auth.Engine) (Integration, error) {
					_, err := in.testIntegration.install(s, e)
					if kind == "services" {
						return &mixedServingWorkerProvider{in}, err
					}
					return &mixedWorkerProvider{in}, err
				}
				want = "workers conflict with environments and services"
			}
			err := Run(context.Background(), Config{DataDir: f.data, Listen: "127.0.0.1:0", DisableLocal: true, AuthFile: a.path, Install: install})
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("Run=%v, want %q", err, want)
			}
			if n := closes(in.testIntegration); n != 1 {
				t.Fatalf("Close calls=%d, want 1", n)
			}
			wantCalls := 1
			if kind != "nil" {
				wantCalls = 0
			}
			if in.controllers != wantCalls {
				t.Fatalf("WorkerController calls=%d, want %d", in.controllers, wantCalls)
			}
			released(t, f)
		})
	}
}
