package control

import (
	"fmt"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

// dataTarget is one immutable configured route. Optional policy alone is not
// capability; the concrete backend assertion occurs once during construction.
type dataTarget struct {
	target  backend.Target
	backend backend.DataBackend
	policy  backend.DataPolicy
}

func (c *Control) configureDataTargets() error {
	var configured []dataTarget
	for _, target := range c.targets {
		if target.DataPolicy == nil {
			continue
		}
		if !target.SupportsRemoteData {
			continue
		}
		policy := *target.DataPolicy
		if err := policy.Validate(); err != nil {
			return fmt.Errorf("configure data target %q: %w", target.Name, err)
		}
		executor, ok := target.Backend.(backend.DataBackend)
		if !ok {
			return fmt.Errorf("configure data target %q: data backend required", target.Name)
		}
		configured = append(configured, dataTarget{target: cloneTarget(target), backend: executor, policy: policy})
	}
	c.dataTargets = configured
	return nil
}

func (c *Control) dataTargetFor(job backend.Job) (*dataTarget, error) {
	for i := range c.dataTargets {
		target := &c.dataTargets[i]
		if target.target.Name != job.Target {
			continue
		}
		if job.TargetBinding == "" || job.TargetBinding != target.target.Binding {
			return nil, backend.ErrIdentityMismatch
		}
		return target, nil
	}
	return nil, ErrUnsupported
}

// SubmitData accepts a data task, or replays its exact authenticated request.
// A new acceptance requires installed artifacts and a configured data route.
func (c *Control) SubmitData(actor auth.Actor, requestID string, task backend.Task) (backend.Job, error) {
	return c.submitData(actor, nil, requestID, task)
}

// SubmitDataInDomain accepts or replays a data task in the exact request domain.
// Replay preserves the original route without another backend check or grant.
func (c *Control) SubmitDataInDomain(actor auth.Actor, domain store.RequestDomain, requestID string, task backend.Task) (backend.Job, error) {
	return c.submitData(actor, &domain, requestID, task)
}

func (c *Control) submitData(actor auth.Actor, domain *store.RequestDomain, requestID string, task backend.Task) (backend.Job, error) {
	if err := c.beginExternal(); err != nil {
		return backend.Job{}, err
	}
	defer c.endExternal()
	actor, err := c.engine.Check(actor, auth.Mutate)
	if err != nil {
		return backend.Job{}, err
	}
	if actor.TokenID() == "" {
		return backend.Job{}, auth.ErrUnauthenticated
	}
	if !requestName.MatchString(requestID) {
		return backend.Job{}, fmt.Errorf("%w: request_id must contain 1..128 letters, digits, underscores or hyphens", ErrInvalid)
	}
	task, err = normalizeTask(cloneTask(task))
	if err != nil {
		return backend.Job{}, err
	}
	if err = Validate(task); err != nil {
		return backend.Job{}, err
	}
	if err = backend.ValidateDataTask(task, nil); err != nil {
		return backend.Job{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if len(task.Env) == 0 {
		task.Env = nil
	}
	var job backend.Job
	var found bool
	if domain == nil {
		job, found, err = c.store.Lookup(actor, requestID, task)
	} else {
		job, found, err = c.store.LookupRetained(actor, *domain, requestID, task)
	}
	if err != nil || found {
		return job, err
	}
	if domain == nil && c.retention.enabled {
		return backend.Job{}, &store.ExpiryError{Reason: store.RequestDomainRequired}
	}
	if c.failed.Load() {
		return backend.Job{}, ErrUnavailable
	}
	if err = c.checkDataArtifacts(); err != nil {
		return backend.Job{}, err
	}
	if c.artifactAdmissionStopped() {
		return backend.Job{}, ErrUnavailable
	}
	if err = c.storageGuard(false, false); err != nil {
		return backend.Job{}, err
	}
	// planData retains ordinary placement constraints and invokes the original
	// Backend.Check. The integration owns its complete protected layout checks.
	selected, err := c.planData(task)
	if err != nil {
		return backend.Job{}, err
	}
	route, err := c.dataTargetFor(backend.Job{Target: selected.Name, TargetBinding: selected.Binding})
	if err != nil {
		return backend.Job{}, err
	}
	actor, err = c.engine.Check(actor, auth.Mutate)
	if err != nil {
		return backend.Job{}, err
	}
	delegation, err := c.engine.Delegate(actor)
	if err != nil {
		return backend.Job{}, err
	}
	if err = c.checkDataArtifacts(); err != nil {
		return backend.Job{}, err
	}
	if err = c.storageGuard(false, false); err != nil {
		return backend.Job{}, err
	}
	// Root checks and backend planning may block. Check current file authority
	// after those calls; no external call is made under a Store transaction.
	actor, err = c.engine.Check(actor, auth.Mutate)
	if err != nil {
		return backend.Job{}, err
	}
	if err = c.engine.CheckDelegation(delegation); err != nil {
		return backend.Job{}, err
	}
	if c.failed.Load() || c.artifactAdmissionStopped() {
		return backend.Job{}, ErrUnavailable
	}
	job, err = c.store.SubmitData(actor, domain, requestID, task, route.target.Name, route.target.Binding, delegation, route.policy, time.Now().UTC())
	if err == nil {
		c.retention.wantSample.Store(c.retention.enabled)
	}
	return job, err
}

// Only the original marker-verified Files owner supplies root identity. This
// performs no open/adoption of a different Files/root and creates no metadata.
func (c *Control) checkDataArtifacts() error {
	if c.artifacts == nil || c.artifacts.files == nil {
		return ErrUnsupported
	}
	facts, err := c.artifacts.files.RootFacts()
	if err != nil {
		return err
	}
	installation, err := c.store.ArtifactInstallation()
	if err != nil {
		return err
	}
	if !installation.Enabled || facts != c.artifacts.root || facts.ID != installation.ID || facts.Unit != installation.Unit || facts.Volume != installation.Volume {
		return fmt.Errorf("%w: data artifact root identity differs", ErrUnavailable)
	}
	return nil
}
