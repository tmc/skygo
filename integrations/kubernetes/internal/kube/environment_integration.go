package kube

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/environmentcontrol"
	"github.com/tmc/skygo/internal/server"
	"github.com/tmc/skygo/internal/store"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// environmentServiceAccount is the service account whose snapshot every
// environment Pod sets explicitly.
const environmentServiceAccount = "default"

// NewEnvironmentIntegration returns the server integration that owns every
// target of targets and the environments of the one target that profiles
// names, on s and engine, which must be the server's open Store and
// authentication Engine. It is meant to be called from server.Config.Install.
//
// Environments require scoped authentication and enabled retention, whose
// request domains environment requests use. That is configured support, not
// a domain that accepts requests now: a request epoch may close at any time
// while existing environments stay readable and deletable, and each Create
// checks its own lineage and epoch in its Store transaction.
//
// Every target is constructed once, as FromTarget does; the environment
// runtime uses the same frozen REST configuration as its target. Before it
// returns, under ctx, it reads the target's namespace, default service
// account and each profile's claim and volume, and refuses any that differs
// from its profile, including a volume whose UID is not the profile's
// volume_uid. It never creates, modifies or deletes them.
//
// An error with a non-nil Integration means the Integration owns state that
// its Close must release, as server.Config.Install requires.
func NewEnvironmentIntegration(ctx context.Context, s *store.Store, engine *auth.Engine, targets Config, profiles EnvironmentConfig) (server.Integration, error) {
	if s == nil || engine == nil {
		return nil, errors.New("kubernetes environments require a store and an auth engine")
	}
	if len(profiles.profiles) == 0 {
		return nil, errors.New("kubernetes environments require profiles")
	}
	if !s.Scoped() {
		return nil, errors.New("kubernetes environments require scoped authentication")
	}
	if _, enabled, err := s.RetentionPolicy(); err != nil {
		return nil, fmt.Errorf("read retention policy: %w", err)
	} else if !enabled {
		return nil, errors.New("kubernetes environments require enabled retention")
	}
	configs := targets.Targets()
	selected := slices.IndexFunc(configs, func(t TargetConfig) bool { return t.Name == profiles.target })
	if selected < 0 {
		return nil, errors.New("kubernetes environment profiles name an unconfigured target")
	}
	in := &environmentIntegration{artifacts: profiles.artifacts}
	frozen := make([]backend.Target, 0, len(configs))
	for i, tc := range configs {
		target, config, err := loadTarget(ctx, tc, s.Owner())
		if err != nil {
			if in.rt != nil {
				return in, err
			}
			return nil, err
		}
		if i == selected {
			b := target.Backend.(*Backend)
			if profiles.artifacts && b.data == nil {
				return nil, errors.New("kubernetes environment artifacts require a data helper image")
			}
			target.SupportsPersistentEnvironments = true
			target.SupportsEnvironmentArtifacts = profiles.artifacts
			in.rt, err = newEnvironmentRuntime(target, config, profiles.podProfiles())
			if err != nil {
				return nil, err
			}
		}
		frozen = append(frozen, target)
	}
	in.targets = frozen
	// From here in.rt owns its effects; Close releases them.
	check, cancel := context.WithTimeout(ctx, environmentReadTimeout)
	err := in.rt.verifyProfiles(check)
	cancel()
	if err != nil {
		return in, err
	}
	var rt environmentcontrol.Runtime = environmentRuntimeOnly{in.rt}
	if profiles.artifacts {
		rt = in.rt
	}
	in.ctl, err = environmentcontrol.New(environmentcontrol.Config{Store: s, Auth: engine, Runtime: rt})
	if err != nil {
		return in, err
	}
	return in, nil
}

// environmentRuntimeOnly is an environment runtime without its
// Materializer methods, so that a Controller refuses specs with artifacts
// before any effect.
type environmentRuntimeOnly struct{ environmentcontrol.Runtime }

// environmentIntegration owns a set of targets and the environment Controller
// and runtime of one of them.
type environmentIntegration struct {
	targets   []backend.Target
	artifacts bool
	rt        *environmentRuntime
	ctl       *environmentcontrol.Controller // nil if construction failed first
}

var (
	_ server.EnvironmentProvider = (*environmentIntegration)(nil)
	_ server.ControlBinder       = (*environmentIntegration)(nil)
	_ server.EffectStopper       = (*environmentIntegration)(nil)
)

// Targets returns the integration's targets.
func (in *environmentIntegration) Targets() []backend.Target { return slices.Clone(in.targets) }

// EnvironmentController returns the integration's Controller.
func (in *environmentIntegration) EnvironmentController() *environmentcontrol.Controller {
	return in.ctl
}

// BindControl binds c as the opener of environment artifact reads when
// artifacts are configured, and then requires c's artifact service.
func (in *environmentIntegration) BindControl(c *control.Control) error {
	if c == nil {
		return errors.New("nil control")
	}
	if !in.artifacts {
		return nil
	}
	if !c.ArtifactsEnabled() {
		return errors.New("kubernetes environment artifacts require the server's artifact service")
	}
	return in.ctl.BindArtifacts(c)
}

// Run runs the Controller until ctx ends.
func (in *environmentIntegration) Run(ctx context.Context) error { return in.ctl.Run(ctx) }

// StopEffects stops the Controller's materializations. The runtime's other
// frames continue until Close.
func (in *environmentIntegration) StopEffects() {
	if in.ctl != nil {
		in.ctl.StopMaterializing()
	}
}

// Close stops the Controller's sessions and cancels their frames, then
// stops the runtime, cancelling every create send, exec, forward and staging
// frame it started, before it waits for anything. It then joins the
// Controller's materializations and every session owner after its last
// Store operation (environmentcontrol.Integration.Close), and finally every
// runtime frame and send. It returns an error if any is still held when ctx
// ends; the server then keeps everything they depend on. A canceled create
// send is joined, not resolved: its declared nonce stays the only authority
// on whether it applied. Close never stops or deletes an environment's Pod,
// claim or volume.
func (in *environmentIntegration) Close(ctx context.Context) error {
	var err error
	if in.ctl != nil {
		err = environmentcontrol.Integration{Controller: in.ctl, StopRuntime: in.rt.stop}.Close(ctx)
	}
	return errors.Join(err, in.rt.join(ctx))
}

// verifyProfiles reads the target's namespace and default service account
// and each profile's claim and volume, and refuses any that differs from the
// profile. It sends only reads.
func (rt *environmentRuntime) verifyProfiles(ctx context.Context) error {
	c := rt.b.client.CoreV1()
	ns, err := c.Namespaces().Get(ctx, rt.b.namespace, metav1.GetOptions{})
	if err != nil {
		return environmentReadError("namespace", err)
	}
	sa, err := c.ServiceAccounts(rt.b.namespace).Get(ctx, environmentServiceAccount, metav1.GetOptions{})
	if err != nil {
		return environmentReadError("service account", err)
	}
	keys := make([]environmentProfileKey, 0, len(rt.profiles))
	for k := range rt.profiles {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b environmentProfileKey) int {
		return cmp.Or(cmp.Compare(a.principal, b.principal), cmp.Compare(a.alias, b.alias))
	})
	for _, k := range keys {
		p := rt.profiles[k]
		if ns.Name != rt.b.namespace || ns.UID != p.namespaceUID {
			return fmt.Errorf("%w: kubernetes environment namespace differs from profile", backend.ErrIdentityMismatch)
		}
		if ns.DeletionTimestamp != nil || ns.Status.Phase == corev1.NamespaceTerminating {
			return fmt.Errorf("kubernetes environment namespace: %w", errEnvironmentObjectUnavailable)
		}
		if !environmentServiceAccountMatches(p, rt.b.namespace, sa) {
			return fmt.Errorf("%w: kubernetes environment service account differs from profile", backend.ErrIdentityMismatch)
		}
		pvc, err := c.PersistentVolumeClaims(rt.b.namespace).Get(ctx, p.pvcName, metav1.GetOptions{})
		if err != nil {
			return environmentReadError("claim", err)
		}
		if !environmentClaimMatches(p, rt.b.namespace, pvc) || pvc.Spec.VolumeName != p.volumeName {
			return fmt.Errorf("%w: kubernetes environment claim differs from profile", backend.ErrIdentityMismatch)
		}
		if pvc.DeletionTimestamp != nil || pvc.Status.Phase != corev1.ClaimBound {
			return fmt.Errorf("kubernetes environment claim: %w", errEnvironmentObjectUnavailable)
		}
		pv, err := c.PersistentVolumes().Get(ctx, p.volumeName, metav1.GetOptions{})
		if err != nil {
			return environmentReadError("volume", err)
		}
		// The profile file requires the volume UID, so a same-name volume
		// that replaced it, even with the same claimRef, is refused here.
		if err := environmentVolumeMatches(p, rt.b.namespace, p.volumeUID, pv); err != nil {
			return err
		}
	}
	return nil
}

// environmentServiceAccountMatches reports whether sa is the profile's
// snapshot of the namespace's default service account: its UID and its
// image pull secret names, in order.
func environmentServiceAccountMatches(p environmentPodProfile, namespace string, sa *corev1.ServiceAccount) bool {
	if sa.Namespace != namespace || sa.Name != environmentServiceAccount || sa.UID != p.serviceAccountUID ||
		sa.DeletionTimestamp != nil || len(sa.ImagePullSecrets) != len(p.pullSecrets) {
		return false
	}
	for i, ref := range sa.ImagePullSecrets {
		if ref.Name != p.pullSecrets[i] {
			return false
		}
	}
	return true
}

// environmentReadError classifies a failed startup read of what, without
// the object's name.
func environmentReadError(what string, err error) error {
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("kubernetes environment %s: %w", what, errEnvironmentObjectMissing)
	}
	return fmt.Errorf("read kubernetes environment %s", what)
}
