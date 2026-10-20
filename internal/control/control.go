package control

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

var (
	ErrInvalid         = errors.New("invalid argument")
	ErrUnsupported     = errors.New("no eligible target")
	ErrUnavailable     = errors.New("control plane unavailable")
	ErrLogsUnavailable = errors.New("retained logs unavailable")
)
var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var requestName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
var jobName = regexp.MustCompile(`^[a-f0-9]{32}$`)

// Rejection explains why a configured target cannot accept a task.
type Rejection struct{ Target, Reason string }

// Control owns one reconciliation loop. New must precede use.
type Control struct {
	store                 *store.Store
	engine                *auth.Engine
	targets               []backend.Target
	failed                atomic.Bool
	auditFailed           atomic.Bool
	auditLost             atomic.Bool
	access                chan pendingAccess
	started               atomic.Bool
	runMu                 sync.Mutex
	run                   *runFrame
	retention             retentionState
	artifacts             *artifactService
	workspace             *workspaceTarget
	producer              store.OriginalServerLifetime
	recoverStoppedReaders bool
	shutdown              *shutdownState
	localBackend          *local.Backend
}

// New validates targets and orders selection by name.
func New(s *store.Store, targets []backend.Target, engine *auth.Engine) (*Control, error) {
	return NewWithConfig(s, targets, engine, Config{})
}

// NewWithLocalOutput installs the actual built-in local output binder and sampler.
// Configuration is immutable after construction; callbacks run outside control locks.
func NewWithLocalOutput(s *store.Store, targets []backend.Target, engine *auth.Engine, local LocalOutputConfig) (*Control, error) {
	return NewWithConfig(s, targets, engine, Config{LocalOutput: local})
}

// NewWithConfig installs the concrete local output, artifact and workspace services.
// Files and its original root remain owned by the caller through actual drain.
func NewWithConfig(s *store.Store, targets []backend.Target, engine *auth.Engine, config Config) (*Control, error) {
	local := config.LocalOutput
	if s == nil || len(targets) == 0 || len(targets) > 32 || engine == nil {
		return nil, fmt.Errorf("store, targets and authentication required")
	}
	producer, err := workspaceStoreProducer(config.ServerLifetime)
	if err != nil {
		return nil, err
	}
	if config.RecoverStoppedArtifactReaders {
		if config.Artifacts == nil {
			return nil, fmt.Errorf("stopped artifact reader recovery requires artifacts")
		}
		if !recoveryProducerKnown(producer) {
			return nil, fmt.Errorf("stopped artifact reader recovery requires a strict known server lifetime")
		}
	}
	targets = append([]backend.Target(nil), targets...)
	sort.Slice(targets, func(i, j int) bool { return targets[i].Name < targets[j].Name })
	for i, t := range targets {
		if t.Name == "" || t.Backend == nil || (i > 0 && targets[i-1].Name == t.Name) || !validText(t.Binding, 256, false) || !validLabels(t.Labels, 32) || !validAcceleratorNames(t.AcceleratorNames) {
			return nil, fmt.Errorf("invalid target configuration")
		}
		if !validRetentionTarget(t) {
			return nil, fmt.Errorf("invalid retention target configuration")
		}
		if err := backend.ValidateAdvice(t.Advice); err != nil {
			return nil, fmt.Errorf("invalid target facts: %w", err)
		}
		targets[i] = cloneTarget(t)
	}
	policy, enabled, err := s.RetentionPolicy()
	if err != nil {
		return nil, err
	}
	if enabled {
		for _, t := range targets {
			if t.OutputStorage == backend.LocalOutput && (!policy.LocalOutputEnabled || local.Target != t.Name || local.Binding != t.Binding || local.BindOutput == nil || local.OutputUsage == nil) {
				return nil, fmt.Errorf("enabled local output requires its actual binder and storage policy")
			}
			if t.OutputStorage == backend.LocalOutput && t.OutputReservation != uint64(backend.MaxLogBytes)+4*policy.OutputUnit {
				return nil, fmt.Errorf("local output reservation differs from enabled policy")
			}
		}
	}
	if local.BindOutput != nil || local.OutputUsage != nil || local.Target != "" || local.Binding != "" {
		if local.BindOutput == nil || local.OutputUsage == nil {
			return nil, fmt.Errorf("local output binder and sampler required together")
		}
		found := false
		for _, t := range targets {
			if t.Name == local.Target && t.Binding == local.Binding && t.OutputStorage == backend.LocalOutput {
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("local output configuration has no matching route")
		}
	}
	control := &Control{shutdown: newShutdownState(), producer: producer, recoverStoppedReaders: config.RecoverStoppedArtifactReaders, store: s, targets: targets, engine: engine, access: make(chan pendingAccess, maxPendingAccess), retention: retentionState{leases: make(map[*LogLease]struct{}), pending: make(map[string]struct{}), local: local, enabled: enabled}}
	if config.Artifacts != nil {
		artifacts, err := newArtifactService(s, config.Artifacts)
		if err != nil {
			return nil, err
		}
		control.artifacts = artifacts
	}
	if err := control.configureOwnedLocal(config); err != nil {
		return nil, err
	}
	if err := control.configureWorkspace(config.LocalWorkspace); err != nil {
		return nil, err
	}
	control.retention.wantSample.Store(enabled)
	return control, nil
}

// Validate rejects malformed tasks before capability checks or side effects.
func Validate(t backend.Task) error {
	var err error
	t, err = normalizeTask(t)
	if err != nil {
		return err
	}
	if len(t.Argv) == 0 || t.Argv[0] == "" {
		return fmt.Errorf("%w: argv requires a program", ErrInvalid)
	}
	for _, arg := range t.Argv {
		if !utf8.ValidString(arg) {
			return fmt.Errorf("%w: invalid UTF8 in argv", ErrInvalid)
		}
		if strings.ContainsRune(arg, 0) {
			return fmt.Errorf("%w: NUL in argv", ErrInvalid)
		}
	}
	for k, v := range t.Env {
		if !utf8.ValidString(k) || !utf8.ValidString(v) {
			return fmt.Errorf("%w: invalid UTF8 in environment", ErrInvalid)
		}
		if !envName.MatchString(k) || strings.ContainsRune(v, 0) {
			return fmt.Errorf("%w: invalid environment key or value", ErrInvalid)
		}
	}
	if !utf8.ValidString(t.Directory) || !utf8.ValidString(t.Image) || !utf8.ValidString(t.Target) || !utf8.ValidString(t.Accelerator) {
		return fmt.Errorf("%w: invalid UTF8 in task text", ErrInvalid)
	}
	if strings.ContainsRune(t.Directory, 0) || strings.ContainsRune(t.Image, 0) {
		return fmt.Errorf("%w: NUL in path or image", ErrInvalid)
	}
	if t.CPUMillis < 0 || t.MemoryBytes < 0 || t.AcceleratorCount < 0 || t.TimeoutSeconds < 0 || t.TimeoutSeconds > 7*24*3600 {
		return fmt.Errorf("%w: negative resource or timeout outside 0..604800 seconds", ErrInvalid)
	}
	if (t.Accelerator == "") != (t.AcceleratorCount == 0) {
		return fmt.Errorf("%w: accelerator name and positive count required together", ErrInvalid)
	}
	if t.CPULimitMillis < 0 || t.MemoryLimitBytes < 0 ||
		(t.CPULimitMillis != 0 && (t.CPUMillis <= 0 || t.CPULimitMillis < t.CPUMillis)) ||
		(t.MemoryLimitBytes != 0 && (t.MemoryBytes <= 0 || t.MemoryLimitBytes < t.MemoryBytes)) {
		return fmt.Errorf("%w: resource limit requires a positive request no greater than the limit", ErrInvalid)
	}
	if !validLabels(t.TargetLabels, 16) || !validLabels(t.NodeSelector, 16) {
		return fmt.Errorf("%w: invalid placement labels or selectors", ErrInvalid)
	}
	if err := backend.ValidateWorkspace(t); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	data, err := backend.TaskBytes(t)
	if err != nil {
		return err
	}
	if len(data) > 256<<10 {
		return fmt.Errorf("%w: task exceeds 256 KiB", ErrInvalid)
	}
	return nil
}

// Targets returns configuration metadata; callers must not invoke its backends.
func (c *Control) Targets(actor auth.Actor) ([]backend.Target, error) {
	if _, err := c.engine.Check(actor, auth.Read); err != nil {
		return nil, err
	}
	targets := make([]backend.Target, len(c.targets))
	for i, t := range c.targets {
		targets[i] = cloneTarget(t)
	}
	return targets, nil
}

// Plan selects the first eligible target without promising available capacity.
func (c *Control) Plan(actor auth.Actor, t backend.Task) (string, []Rejection, error) {
	if _, err := c.engine.Check(actor, auth.Read); err != nil {
		return "", nil, err
	}
	return c.plan(t)
}
func (c *Control) plan(t backend.Task) (string, []Rejection, error) {
	var err error
	t, err = normalizeTask(t)
	if err != nil {
		return "", nil, err
	}
	if err := Validate(t); err != nil {
		return "", nil, err
	}
	var selected string
	var rejected []Rejection
	for _, target := range c.targets {
		err := checkWorkspaceTarget(t, target, c.retention.enabled, c.workspace)
		if err != nil {
			rejected = append(rejected, Rejection{target.Name, err.Error()})
		} else if selected == "" {
			selected = target.Name
		}
	}
	if selected == "" {
		reasons := make([]string, 0, len(rejected))
		for _, r := range rejected {
			reasons = append(reasons, r.Target+": "+r.Reason)
		}
		return "", rejected, fmt.Errorf("%w: %s", ErrUnsupported, strings.Join(reasons, "; "))
	}
	return selected, rejected, nil
}

// Submit durably accepts a task or returns the job already bound to requestID.
func (c *Control) Submit(actor auth.Actor, requestID string, t backend.Task) (backend.Job, error) {
	return c.submit(actor, nil, requestID, t, false)
}

// SubmitRetained accepts or replays the exact supplied request domain.
func (c *Control) SubmitRetained(actor auth.Actor, domain store.RequestDomain, requestID string, t backend.Task) (backend.Job, error) {
	return c.submit(actor, &domain, requestID, t, false)
}
func (c *Control) submit(actor auth.Actor, domain *store.RequestDomain, requestID string, t backend.Task, workspace bool) (backend.Job, error) {
	if err := c.beginExternal(); err != nil {
		return backend.Job{}, err
	}
	defer c.endExternal()
	actor, err := c.engine.Check(actor, auth.Mutate)
	if err != nil {
		return backend.Job{}, err
	}
	if !requestName.MatchString(requestID) {
		return backend.Job{}, fmt.Errorf("%w: request_id must contain 1..128 letters, digits, underscores or hyphens", ErrInvalid)
	}
	t, err = normalizeTask(cloneTask(t))
	if err != nil {
		return backend.Job{}, err
	}
	if err := Validate(t); err != nil {
		return backend.Job{}, err
	}
	if workspace {
		if t.Workdir == nil {
			return backend.Job{}, fmt.Errorf("%w: workspace input required", ErrInvalid)
		}
		if !c.ArtifactsEnabled() {
			return backend.Job{}, ErrUnsupported
		}
	} else if t.Workdir != nil || len(t.Setup) != 0 || len(t.Outputs) != 0 {
		return backend.Job{}, fmt.Errorf("%w: workspace submission required", ErrInvalid)
	}
	if len(t.Env) == 0 {
		t.Env = nil
	}
	var job backend.Job
	var found bool
	if domain == nil {
		job, found, err = c.store.Lookup(actor, requestID, t)
	} else {
		job, found, err = c.store.LookupRetained(actor, *domain, requestID, t)
	}
	if err != nil || found {
		return job, err
	}
	if domain == nil && c.retention.enabled {
		return backend.Job{}, &store.ExpiryError{Reason: store.RequestDomainRequired}
	}
	if c.failed.Load() || workspace && c.artifactAdmissionStopped() {
		return backend.Job{}, ErrUnavailable
	}
	if err := c.storageGuard(false, false); err != nil {
		return backend.Job{}, err
	}
	target, _, err := c.plan(t)
	if err != nil {
		return backend.Job{}, err
	}
	if err = c.storageGuard(false, false); err != nil {
		return backend.Job{}, err
	}
	actor, err = c.engine.Check(actor, auth.Mutate)
	if err != nil {
		return backend.Job{}, err
	}
	if workspace && c.artifactAdmissionStopped() {
		return backend.Job{}, ErrUnavailable
	}
	configured, _ := c.target(target)
	if workspace {
		job, err := c.store.SubmitWorkspace(actor, domain, requestID, t, target, configured.Binding, configured.OutputStorage, configured.OutputReservation)
		if err == nil {
			c.retention.wantSample.Store(c.retention.enabled)
		}
		return job, err
	}
	if domain != nil {
		job, err := c.store.SubmitRetained(actor, *domain, requestID, t, target, configured.Binding, configured.OutputStorage, configured.OutputReservation)
		if err == nil {
			c.retention.wantSample.Store(true)
		}
		return job, err
	}
	return c.store.Submit(actor, requestID, t, target, configured.Binding)
}

// Get returns one accepted job.
func (c *Control) Get(actor auth.Actor, id string) (backend.Job, error) {
	actor, err := c.engine.Check(actor, auth.Read)
	if err != nil {
		return backend.Job{}, err
	}
	if !jobName.MatchString(id) {
		return backend.Job{}, fmt.Errorf("%w: invalid job id", ErrInvalid)
	}
	j, err := c.store.GetFor(actor, id)
	if errors.Is(err, store.ErrNotFound) {
		if domainErr := c.store.CheckLogDomain(actor, id, ""); domainErr != nil {
			var expired *store.ExpiryError
			if errors.As(domainErr, &expired) && expired.Reason == store.ResultExpired {
				return backend.Job{}, domainErr
			}
		}
	}
	return j, err
}

// List uses an installation- and scope-bound sparse snapshot cursor.
func (c *Control) List(actor auth.Actor, size int, token string) ([]backend.Job, string, error) {
	actor, err := c.engine.Check(actor, auth.Read)
	if err != nil {
		return nil, "", err
	}
	if size < 0 || size > 200 {
		return nil, "", fmt.Errorf("%w: page size outside 0..200", ErrInvalid)
	}
	if size == 0 {
		size = 50
	}
	info, err := c.store.RetentionInfo(actor)
	if err != nil {
		return nil, "", err
	}
	if !info.Enabled {
		return c.listLegacy(actor, size, token)
	}
	cur := c.scopeCursor(actor, "list", info.Domain.Lineage)
	max, err := c.store.LastSequenceFor(actor)
	if err != nil {
		return nil, "", err
	}
	if token == "" {
		cur.Max = max
	} else {
		parsed, e := decodeRetentionCursor(token)
		if e != nil {
			return nil, "", e
		}
		expected := cur
		expected.After, expected.Max = parsed.After, parsed.Max
		if parsed != expected || parsed.After == 0 || parsed.After >= parsed.Max || parsed.Max > max {
			return nil, "", fmt.Errorf("%w: page token scope or bounds differ", ErrInvalid)
		}
		cur = parsed
	}
	page, err := c.store.ListFor(actor, cur.After, cur.Max, size)
	if err != nil {
		return nil, "", err
	}
	next := ""
	if page.More {
		cur.After = page.After
		next, err = encodeRetentionCursor(cur)
		if err != nil {
			return nil, "", err
		}
	}
	return page.Jobs, next, nil
}

// Cancel persists cancellation intent. Terminal outcomes never change.
func (c *Control) Cancel(actor auth.Actor, id string) (backend.Job, error) {
	actor, err := c.engine.Check(actor, auth.Mutate)
	if err != nil {
		return backend.Job{}, err
	}
	if !jobName.MatchString(id) {
		return backend.Job{}, fmt.Errorf("%w: invalid job id", ErrInvalid)
	}
	return c.store.UpdateFor(actor, id, func(j *backend.Job) error {
		if !j.Status.State.Terminal() {
			j.CancelRequested = true
		}
		return nil
	})
}

func (c *Control) target(name string) (backend.Target, bool) {
	for _, t := range c.targets {
		if t.Name == name {
			return t, true
		}
	}
	return backend.Target{}, false
}

// Healthy reports whether durable reconciliation has encountered a store error.
func (c *Control) Healthy() bool { return !c.failed.Load() }

func (c *Control) listLegacy(actor auth.Actor, size int, token string) ([]backend.Job, string, error) {
	cur := legacyCursor{}
	if c.store.Scoped() {
		cur.Principal = actor.Principal()
		cur.Global = actor.Role() == auth.Admin
	}
	max, err := c.store.LastSequenceFor(actor)
	if err != nil {
		return nil, "", err
	}
	if token == "" {
		cur.Max = max
	} else {
		var parsed legacyCursor
		if err = decodeCursor(token, &parsed, 256); err != nil {
			return nil, "", err
		}
		if parsed.Principal != cur.Principal || parsed.Global != cur.Global || parsed.After == 0 || parsed.After >= parsed.Max || parsed.Max > max {
			return nil, "", fmt.Errorf("%w: page token scope or bounds differ", ErrInvalid)
		}
		cur = parsed
	}
	page, err := c.store.ListFor(actor, cur.After, cur.Max, size)
	if err != nil {
		return nil, "", err
	}
	next := ""
	if page.More {
		cur.After = page.After
		next, err = encodeCursor(cur, 256)
	}
	return page.Jobs, next, err
}
