package kube

import (
	"context"
	"errors"
	"slices"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/server"
	"github.com/tmc/skygo/internal/servingcontrol"
	"github.com/tmc/skygo/internal/store"
)

// NewIntegration returns the server integration that owns every target of
// targets, the environments of the one target environments names, if it
// has profiles, and the fixed services of the one target services names, if
// it is not the zero ServiceConfig. At least one must be configured; they
// may name the same target. s and engine must be the server's open Store
// and authentication Engine, which require scoped authentication and
// enabled retention. It is meant to be called from server.Config.Install.
//
// Every target is loaded once, as FromTarget does, and both runtimes of a
// target use the same frozen REST configuration as the target. Before it
// returns, under ctx, it verifies the environment profiles' borrowed
// objects as NewEnvironmentIntegration does, and the service profile's
// namespace UID, default service account UID and image pull secrets. It
// never creates, modifies or deletes them.
//
// An error with a non-nil Integration means the Integration owns state that
// its Close must release, as server.Config.Install requires.
//
// At most one live Integration may serve a Store and binding: the server
// installs it once, as the Store's single writer, for the lifetime of its
// dependencies. Its service controller and runtime keep custody of their
// original calls' exchanges in this process only; nothing here, and
// nothing in the Store, fences a second Integration or process. A
// replacement on the same Store must follow a positive Close of the old
// Integration, with every original returned and acknowledged. Building one
// while the old one holds an original is not supported; neither is
// recovery of an original across a crash or restart.
func NewIntegration(ctx context.Context, s *store.Store, engine *auth.Engine, targets Config, environments EnvironmentConfig, services ServiceConfig) (server.Integration, error) {
	if s == nil || engine == nil {
		return nil, errors.New("kubernetes integration requires a store and an auth engine")
	}
	withEnvironments, withServices := len(environments.profiles) != 0, services.target != ""
	if !withEnvironments && !withServices {
		return nil, errors.New("kubernetes integration requires environment or service profiles")
	}
	what := "environments"
	if !withEnvironments {
		what = "services"
	}
	if err := requireRetention(s, what); err != nil {
		return nil, err
	}
	configs := targets.Targets()
	envAt, svcAt := -1, -1
	if withEnvironments {
		if envAt = slices.IndexFunc(configs, func(t TargetConfig) bool { return t.Name == environments.target }); envAt < 0 {
			return nil, errors.New("kubernetes environment profiles name an unconfigured target")
		}
	}
	if withServices {
		if svcAt = slices.IndexFunc(configs, func(t TargetConfig) bool { return t.Name == services.target }); svcAt < 0 {
			return nil, errors.New("kubernetes service profiles name an unconfigured target")
		}
	}
	in := &environmentIntegration{artifacts: environments.artifacts}
	// owned returns in once it holds a runtime, whose Close then releases
	// its effects, and nil before.
	owned := func(err error) (server.Integration, error) {
		if in.rt != nil || in.svc != nil {
			return in, err
		}
		return nil, err
	}
	frozen := make([]backend.Target, 0, len(configs))
	for i, tc := range configs {
		target, config, err := loadIntegrationTarget(ctx, tc, s.Owner())
		if err != nil {
			return owned(err)
		}
		if i == envAt {
			b := target.Backend.(*Backend)
			if environments.artifacts && b.data == nil {
				return owned(errors.New("kubernetes environment artifacts require a data helper image"))
			}
			target.SupportsPersistentEnvironments = true
			target.SupportsEnvironmentArtifacts = environments.artifacts
			if in.rt, err = newEnvironmentRuntime(target, config, environments.podProfiles()); err != nil {
				return owned(err)
			}
		}
		if i == svcAt {
			if in.svc, err = newServiceRuntime(target, config, services); err != nil {
				in.svc = nil
				return owned(err)
			}
		}
		frozen = append(frozen, target)
	}
	in.targets = frozen
	// From here in.rt and in.svc own their effects; Close releases them.
	if withEnvironments {
		if err := in.newEnvironments(ctx, s, engine, environments); err != nil {
			return in, err
		}
	}
	if withServices {
		if err := in.newServices(ctx, s, engine, services, frozen[svcAt]); err != nil {
			return in, err
		}
	}
	return in, nil
}

// loadIntegrationTarget is loadTarget. NewIntegration calls it exactly once
// per configured target; tests replace it to count those calls.
var loadIntegrationTarget = loadTarget

// newServices constructs the serving Controller of in over its service
// runtime, after verifying the profile's borrowed objects.
func (in *environmentIntegration) newServices(ctx context.Context, s *store.Store, engine *auth.Engine, services ServiceConfig, target backend.Target) error {
	check, cancel := context.WithTimeout(ctx, serviceReadTimeout)
	err := in.svc.verifyBorrowed(check)
	cancel()
	if err != nil {
		return err
	}
	in.svcCtl, err = servingcontrol.New(servingcontrol.Config{
		Store:   s,
		Auth:    engine,
		Target:  target.Name,
		Binding: target.Binding,
		Runtime: in.svc,
		Limits:  services.limits,
		Admit:   in.svc.Admit,
	})
	return err
}
