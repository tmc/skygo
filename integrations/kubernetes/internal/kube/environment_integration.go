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
	"github.com/tmc/skygo/internal/servingcontrol"
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
// its Close must release, as server.Config.Install requires. It is
// NewIntegration without services.
func NewEnvironmentIntegration(ctx context.Context, s *store.Store, engine *auth.Engine, targets Config, profiles EnvironmentConfig) (server.Integration, error) {
	if s == nil || engine == nil {
		return nil, errors.New("kubernetes environments require a store and an auth engine")
	}
	if len(profiles.profiles) == 0 {
		return nil, errors.New("kubernetes environments require profiles")
	}
	return NewIntegration(ctx, s, engine, targets, profiles, ServiceConfig{})
}

// requireRetention refuses a store without scoped authentication and
// enabled retention, which environments and services both require; what
// names the feature in the errors.
func requireRetention(s *store.Store, what string) error {
	if !s.Scoped() {
		return fmt.Errorf("kubernetes %s require scoped authentication", what)
	}
	if _, enabled, err := s.RetentionPolicy(); err != nil {
		return fmt.Errorf("read retention policy: %w", err)
	} else if !enabled {
		return fmt.Errorf("kubernetes %s require enabled retention", what)
	}
	return nil
}

// newEnvironments constructs the environment Controller of in over its
// runtime, after verifying the profiles' borrowed objects.
func (in *environmentIntegration) newEnvironments(ctx context.Context, s *store.Store, engine *auth.Engine, profiles EnvironmentConfig) error {
	check, cancel := context.WithTimeout(ctx, environmentReadTimeout)
	err := in.rt.verifyProfiles(check)
	cancel()
	if err != nil {
		return err
	}
	var rt environmentcontrol.Runtime = environmentRuntimeOnly{in.rt}
	if profiles.artifacts {
		rt = in.rt
	}
	in.ctl, err = environmentcontrol.New(environmentcontrol.Config{Store: s, Auth: engine, Runtime: rt})
	return err
}

// environmentRuntimeOnly is an environment runtime without its
// Materializer methods, so that a Controller refuses specs with artifacts
// before any effect.
type environmentRuntimeOnly struct{ environmentcontrol.Runtime }

// environmentIntegration owns a set of targets, the environment Controller
// and runtime of one of them, and the serving Controller and runtime of one
// of them. Either pair may be absent.
type environmentIntegration struct {
	targets   []backend.Target
	artifacts bool
	rt        *environmentRuntime            // nil without environments
	ctl       *environmentcontrol.Controller // nil without environments or if construction failed first
	svc       *serviceRuntime                // nil without services
	svcCtl    *servingcontrol.Controller     // nil without services or if construction failed first

	// stopped, if set, is called by Close with services once both families
	// are stopped and before anything is joined. Tests set it.
	stopped func()
}

var (
	_ server.EnvironmentProvider = (*environmentIntegration)(nil)
	_ server.ServingProvider     = (*environmentIntegration)(nil)
	_ server.ControlBinder       = (*environmentIntegration)(nil)
	_ server.EffectStopper       = (*environmentIntegration)(nil)
)

// ServingController returns the integration's serving Controller, or nil.
func (in *environmentIntegration) ServingController() *servingcontrol.Controller {
	return in.svcCtl
}

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
	if !in.artifacts || in.ctl == nil {
		return nil
	}
	if !c.ArtifactsEnabled() {
		return errors.New("kubernetes environment artifacts require the server's artifact service")
	}
	return in.ctl.BindArtifacts(c)
}

// Run runs each Controller until ctx ends or one returns, and returns once
// both have returned, with the first one's error.
func (in *environmentIntegration) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make(chan error, 2)
	n := 0
	if in.ctl != nil {
		n++
		go func() { errs <- in.ctl.Run(ctx) }()
	}
	if in.svcCtl != nil {
		n++
		go func() { errs <- in.svcCtl.Run(ctx) }()
	}
	if n == 0 {
		<-ctx.Done()
		return ctx.Err()
	}
	var err error
	for i := range n {
		e := <-errs
		if i == 0 {
			err = e
			cancel()
		}
	}
	return err
}

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
//
// With services it first stops both families before it waits for either.
// The serving Controller's admission and the service runtime stop first,
// refusing every new call, pass and forward and cancelling those in
// progress; then the environment Controller's materializations stop, and
// environmentcontrol.Integration.Close refuses every new Exec and Forward
// at entry and stops the environment runtime. Close waits for that stop
// phase, which waits on no I/O, to be acknowledged by the StopRuntime
// callback before it joins anything. It then joins both families
// concurrently: the environment as above, and the serving Controller, every
// call, pass and forward it admitted and any held result, and the service
// runtime's frames. It never deletes a service's objects or the borrowed
// namespace, service account or secrets. Without services it is unchanged.
func (in *environmentIntegration) Close(ctx context.Context) error {
	if in.svc == nil && in.svcCtl == nil {
		return in.closeEnvironments(ctx, nil)
	}
	if in.svcCtl != nil {
		in.svcCtl.Stop()
	}
	if in.svc != nil {
		in.svc.stop()
	}
	if in.ctl != nil {
		in.ctl.StopMaterializing()
	}
	stopped := make(chan struct{})
	envDone := make(chan error, 1)
	go func() { envDone <- in.closeEnvironments(ctx, stopped) }()
	// The environment's stop phase takes locks and cancels; it never waits
	// for an exchange, so this wait is not bounded by ctx.
	<-stopped
	if in.stopped != nil {
		in.stopped()
	}
	var errs []error
	if in.svcCtl != nil {
		errs = append(errs, in.svcCtl.Close(ctx))
	}
	if in.svc != nil {
		errs = append(errs, in.svc.join(ctx))
	}
	errs = append(errs, <-envDone)
	return errors.Join(errs...)
}

// closeEnvironments closes the environment Controller and joins its
// runtime, as Close does without services. If stopped is not nil it is
// closed once the Controller refuses new sessions and the runtime is
// stopped, before anything is joined.
func (in *environmentIntegration) closeEnvironments(ctx context.Context, stopped chan<- struct{}) error {
	stop := func() {
		if in.rt != nil {
			in.rt.stop()
		}
		if stopped != nil {
			close(stopped)
		}
	}
	var errs []error
	if in.ctl != nil {
		errs = append(errs, environmentcontrol.Integration{Controller: in.ctl, StopRuntime: stop}.Close(ctx))
	} else {
		stop()
	}
	if in.rt != nil {
		errs = append(errs, in.rt.join(ctx))
	}
	return errors.Join(errs...)
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
