package kube

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/server"
	"github.com/tmc/skygo/internal/store"
	"github.com/tmc/skygo/internal/workeradmission"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
)

const workerProfileReadTimeout = 30 * time.Second
const workerStepInterval = 250 * time.Millisecond

// NewWorkerIntegration constructs the worker-enabled CPU integration for profile's
// one target. Ordinary finite Jobs keep their existing Backend semantics.
// It requires the server's scoped Store with retention and
// authentication Engine, and is intended for server.Config.Install. It loads
// that target once, fixes its helper and borrowed identities in its binding,
// then reads the namespace, default service account and image pull secrets.
// The startup reads never create or modify an API object.
//
// Only the worker runtime uses the private HTTP client. It supports embedded
// static credentials, not credential plugins or file refresh. Standard
// InClusterConfig uses a token file and is refused by this worker profile.
// Its ordinary
// target client remains the original Backend's client. After BindControl,
// its mounted worker API advertises configured independent-worker support to
// file-scoped callers. It advertises neither TCP nor native qualification.
//
// A non-nil Integration returned with an error owns original work: the server
// must drain and join its Control and RPC calls, then Close it before
// releasing its Store or Engine. A failed Close retains
// that owner and is never retried. There must be only one live integration
// for a Store and binding; replacing one requires its positive Close. This
// process-local lifetime does not fence another writer or prove HA recovery.
func NewWorkerIntegration(ctx context.Context, s *store.Store, engine *auth.Engine, targets Config, profile WorkerConfig) (server.Integration, error) {
	if s == nil || engine == nil {
		return nil, errors.New("kubernetes workers require a store and an auth engine")
	}
	if err := profile.check(); err != nil {
		return nil, err
	}
	configs := targets.Targets()
	if len(configs) != 1 || configs[0].Name != profile.target {
		return nil, errors.New("kubernetes workers require their one configured target")
	}
	if configs[0].Data != nil || len(configs[0].Resources.Accelerators) != 0 {
		return nil, errors.New("kubernetes workers conflict with data and accelerators")
	}
	if err := requireRetention(s, "workers"); err != nil {
		return nil, err
	}
	target, config, err := loadTarget(ctx, configs[0], s.Owner())
	if err != nil {
		return nil, err
	}
	return newWorkerIntegration(ctx, target, config, profile)
}

// workerTargetBackend adds the worker protocol to the same original Backend. Both
// embedded pointers refer to that Backend; no mutex or admission pool is copied.
type workerTargetBackend struct {
	*Backend
	*workerRuntime
}

func newWorkerIntegration(ctx context.Context, target backend.Target, config *rest.Config, profile WorkerConfig) (server.Integration, error) {
	if err := profile.check(); err != nil {
		return nil, err
	}
	b, ok := target.Backend.(*Backend)
	if !ok || b == nil || target.Name != profile.target || target.Binding == "" || b.binding != target.Binding || b.data != nil || b.dra != nil || b.gate != nil || b.receipts != nil || len(b.policy.Accelerators) != 0 {
		return nil, errors.New("kubernetes worker target differs from its profile")
	}
	profile = profile.clone()
	binding, err := workerProfileBinding(target.Binding, b.namespace, profile)
	if err != nil {
		return nil, err
	}
	b.binding, target.Binding = binding, binding
	b.data = &dataProfile{helperImage: profile.helperImage, helperSHA256: profile.helperSHA256}
	rt, err := newWorkerRuntime(target, profile.registry, config)
	if err != nil {
		return nil, err
	}
	target.Backend = &workerTargetBackend{Backend: b, workerRuntime: rt}
	target.SupportsRetry = false
	target.DataPolicy = nil
	target.AcceleratorNames = nil
	target.SupportsRemoteData = false
	target.SupportsDataMounts = false
	target.SupportsRemoteDataRetry = false
	target.SupportsPersistentEnvironments = false
	target.SupportsEnvironmentArtifacts = false
	in := &workerIntegration{target: target, profile: profile, rt: rt, done: make(chan struct{})}
	check, cancel := context.WithTimeout(ctx, workerProfileReadTimeout)
	err = in.verifyBorrowed(check)
	cancel()
	if err != nil {
		return in, err
	}
	return in, nil
}

func workerProfileBinding(base, namespace string, p WorkerConfig) (string, error) {
	// Struct fields and the ordered secret names give one canonical encoding.
	raw, err := json.Marshal(struct {
		Version, Base, Namespace, NamespaceUID, ServiceAccountUID string
		PullSecrets                                               []string
		HelperImage, HelperSHA256                                 string
		Budget                                                    workeradmission.Budget
	}{"skygo-kubernetes-worker-profile-v1", base, namespace, string(p.namespaceUID), string(p.registry.ServiceAccountUID), p.registry.PullSecrets, p.helperImage, p.helperSHA256, p.budget})
	if err != nil {
		return "", fmt.Errorf("encode worker profile binding: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// verifyBorrowed keeps every startup response under one original worker frame.
// A failed response Close remains owned by the SDK call, never by join.
func (in *workerIntegration) verifyBorrowed(ctx context.Context) (err error) {
	h := in.rt.http
	ctx, call, err := h.begin(ctx, "worker/profile/"+in.target.Binding, false, false)
	if err != nil {
		return err
	}
	defer func() { err = call.finish(err) }()
	c := h.client.CoreV1().RESTClient()
	var ns corev1.Namespace
	if err = c.Get().Resource("namespaces").Name(in.rt.b.namespace).MaxRetries(0).Do(h.request(ctx)).Into(&ns); err != nil {
		return err
	}
	if ns.Name != in.rt.b.namespace || ns.UID != in.profile.namespaceUID || ns.DeletionTimestamp != nil || ns.Status.Phase != corev1.NamespaceActive {
		return errors.New("worker namespace differs from its profile")
	}
	var sa corev1.ServiceAccount
	if err = c.Get().Namespace(in.rt.b.namespace).Resource("serviceaccounts").Name("default").MaxRetries(0).Do(h.request(ctx)).Into(&sa); err != nil {
		return err
	}
	if sa.Name != "default" || sa.Namespace != in.rt.b.namespace || sa.UID != in.profile.registry.ServiceAccountUID || sa.DeletionTimestamp != nil || len(sa.ImagePullSecrets) != len(in.profile.registry.PullSecrets) {
		return errors.New("worker service account differs from its profile")
	}
	for i, name := range in.profile.registry.PullSecrets {
		if sa.ImagePullSecrets[i].Name != name {
			return errors.New("worker image pull secret order differs from its profile")
		}
		var secret corev1.Secret
		if err = c.Get().Namespace(in.rt.b.namespace).Resource("secrets").Name(name).MaxRetries(0).Do(h.request(ctx)).Into(&secret); err != nil {
			return err
		}
		if secret.Name != name || secret.Namespace != in.rt.b.namespace || secret.UID == "" || secret.DeletionTimestamp != nil || secret.Type != corev1.SecretTypeDockerConfigJson && secret.Type != corev1.SecretTypeDockercfg {
			return errors.New("worker image pull secret differs from its profile")
		}
	}
	return nil
}

type workerIntegration struct {
	target  backend.Target
	profile WorkerConfig
	rt      *workerRuntime

	mu       sync.Mutex
	bound    bool
	workers  *control.Workers
	started  bool
	stopped  bool
	cancel   context.CancelFunc
	done     chan struct{}
	runErr   error
	stepErr  error // most recent original Step result, retained until its successor
	close    sync.Once
	closeErr error
}

var (
	_ server.ControlBinder  = (*workerIntegration)(nil)
	_ server.WorkerProvider = (*workerIntegration)(nil)
	_ server.EffectStopper  = (*workerIntegration)(nil)
)

func (in *workerIntegration) Targets() []backend.Target {
	t := in.target
	t.Labels = maps.Clone(t.Labels)
	t.AcceleratorNames = slices.Clone(t.AcceleratorNames)
	return []backend.Target{t}
}

func (in *workerIntegration) BindControl(c *control.Control) error {
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.bound || in.started || in.stopped {
		return errors.New("worker integration cannot bind control")
	}
	in.bound = true
	w, err := control.NewWorkers(c, control.WorkerConfig{
		Budgets: map[string]workeradmission.Budget{in.target.Name: in.profile.budget},
		Modes:   map[string][]backend.WorkerMode{in.target.Name: {backend.WorkerIndependent}},
	})
	if err != nil {
		return err
	}
	in.workers = w
	return nil
}

func (in *workerIntegration) WorkerController() *control.Workers {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.workers
}

func (in *workerIntegration) Run(ctx context.Context) (err error) {
	in.mu.Lock()
	if in.started || in.stopped || in.workers == nil {
		in.mu.Unlock()
		return errors.New("worker integration cannot run")
	}
	ctx, in.cancel = context.WithCancel(ctx)
	in.started = true
	w := in.workers
	in.mu.Unlock()
	defer func() {
		in.mu.Lock()
		in.runErr = err
		close(in.done)
		in.mu.Unlock()
	}()
	tick := time.NewTicker(workerStepInterval)
	defer tick.Stop()
	var last error
	for {
		if ctx.Err() != nil {
			return nil
		}
		last = w.Step(ctx)
		in.mu.Lock()
		in.stepErr = last
		in.mu.Unlock()
		if errors.Is(last, backend.ErrWorkerUnsettled) {
			return last
		}
		select {
		case <-ctx.Done():
			// A settled Step error is not local ownership. Close inspects the
			// runner's actual pending originals after this loop has returned.
			return nil
		case <-tick.C:
		}
	}
}

func (in *workerIntegration) StopEffects() {
	in.mu.Lock()
	in.stopped = true
	cancel := in.cancel
	in.mu.Unlock()
	in.rt.http.fence()
	if cancel != nil {
		cancel()
	}
}

func (in *workerIntegration) Close(ctx context.Context) error {
	in.close.Do(func() {
		in.StopEffects()
		in.mu.Lock()
		started := in.started
		in.mu.Unlock()
		if started {
			select {
			case <-in.done:
			case <-ctx.Done():
				in.closeErr = &workerIntegrationError{in: in, cause: ctx.Err()}
				return
			}
		}
		in.mu.Lock()
		workers := in.workers
		in.mu.Unlock()
		if err := errors.Join(workers.PendingResults(), in.rt.http.join(ctx)); err != nil {
			in.closeErr = &workerIntegrationError{in: in, cause: err}
		}
	})
	return in.closeErr
}

// workerIntegrationError retains the actual controller, runtime and original
// calls after a failed Close; it is not an acknowledgement of their return.
type workerIntegrationError struct {
	in    *workerIntegration
	cause error
}

func (e *workerIntegrationError) Error() string { return "kubernetes worker integration remains held" }
func (e *workerIntegrationError) Unwrap() error { return e.cause }
