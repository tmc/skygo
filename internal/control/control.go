package control

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
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
	store       *store.Store
	engine      *auth.Engine
	targets     []backend.Target
	failed      atomic.Bool
	auditFailed atomic.Bool
	auditLost   atomic.Bool
	access      chan pendingAccess
	started     atomic.Bool
}

// New validates targets and orders selection by name.
func New(s *store.Store, targets []backend.Target, engine *auth.Engine) (*Control, error) {
	if s == nil || len(targets) == 0 || len(targets) > 32 || engine == nil {
		return nil, fmt.Errorf("store, targets and authentication required")
	}
	targets = append([]backend.Target(nil), targets...)
	sort.Slice(targets, func(i, j int) bool { return targets[i].Name < targets[j].Name })
	for i, t := range targets {
		if t.Name == "" || t.Backend == nil || (i > 0 && targets[i-1].Name == t.Name) || !validText(t.Binding, 256, false) || !validLabels(t.Labels, 32) || !validAcceleratorNames(t.AcceleratorNames) {
			return nil, fmt.Errorf("invalid target configuration")
		}
		if err := backend.ValidateAdvice(t.Advice); err != nil {
			return nil, fmt.Errorf("invalid target facts: %w", err)
		}
		targets[i] = cloneTarget(t)
	}
	return &Control{store: s, targets: targets, engine: engine, access: make(chan pendingAccess, maxPendingAccess)}, nil
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
		if strings.ContainsRune(arg, 0) {
			return fmt.Errorf("%w: NUL in argv", ErrInvalid)
		}
	}
	for k, v := range t.Env {
		if !envName.MatchString(k) || strings.ContainsRune(v, 0) {
			return fmt.Errorf("%w: invalid environment key or value", ErrInvalid)
		}
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
		err := checkTarget(t, target)
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
	actor, err := c.engine.Check(actor, auth.Mutate)
	if err != nil {
		return backend.Job{}, err
	}
	if !requestName.MatchString(requestID) {
		return backend.Job{}, fmt.Errorf("%w: request_id must contain 1..128 letters, digits, underscores or hyphens", ErrInvalid)
	}
	t, err = normalizeTask(t)
	if err != nil {
		return backend.Job{}, err
	}
	if err := Validate(t); err != nil {
		return backend.Job{}, err
	}
	if len(t.Env) == 0 {
		t.Env = nil
	}
	job, found, err := c.store.Lookup(actor, requestID, t)
	if err != nil || found {
		return job, err
	}
	if c.failed.Load() {
		return backend.Job{}, ErrUnavailable
	}
	target, _, err := c.plan(t)
	if err != nil {
		return backend.Job{}, err
	}
	configured, _ := c.target(target)
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
	return c.store.GetFor(actor, id)
}

type cursor struct {
	After     uint64 `json:"a"`
	Max       uint64 `json:"m"`
	Principal string `json:"p,omitempty"`
	Global    bool   `json:"g,omitempty"`
}

// List uses a snapshot high-water mark; later inserts do not shift pages.
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
	var cur cursor
	if c.store.Scoped() {
		cur.Principal = actor.Principal()
		cur.Global = actor.Role() == auth.Admin
	}
	expectedPrincipal, expectedGlobal := cur.Principal, cur.Global
	if token == "" {
		var err error
		cur.Max, err = c.store.LastSequenceFor(actor)
		if err != nil {
			return nil, "", err
		}
	} else {
		raw, err := base64.RawURLEncoding.DecodeString(token)
		if err != nil || len(raw) > 256 {
			return nil, "", fmt.Errorf("%w: invalid page token", ErrInvalid)
		}
		cur = cursor{}
		if err = json.Unmarshal(raw, &cur); err != nil || cur.After == 0 || cur.After >= cur.Max {
			return nil, "", fmt.Errorf("%w: invalid page token", ErrInvalid)
		}
		if cur.Principal != expectedPrincipal || cur.Global != expectedGlobal {
			return nil, "", fmt.Errorf("%w: page token scope changed or expired during activation", ErrInvalid)
		}
		canonical, _ := json.Marshal(cur)
		if string(canonical) != string(raw) {
			return nil, "", fmt.Errorf("%w: invalid page token", ErrInvalid)
		}
		max, err := c.store.LastSequenceFor(actor)
		if err != nil {
			return nil, "", err
		}
		if cur.Max > max {
			return nil, "", fmt.Errorf("%w: page token exceeds snapshot", ErrInvalid)
		}
	}
	jobs, err := c.store.ListFor(actor, cur.After, cur.Max, size)
	if err != nil {
		return nil, "", err
	}
	next := ""
	if len(jobs) > 0 {
		if c.store.Scoped() && actor.Role() != auth.Admin {
			cur.After += uint64(len(jobs))
		} else {
			cur.After = jobs[len(jobs)-1].Sequence
		}
		if cur.After < cur.Max {
			raw, _ := json.Marshal(cur)
			next = base64.RawURLEncoding.EncodeToString(raw)
		}
	}
	return jobs, next, nil
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
