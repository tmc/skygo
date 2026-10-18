// Package control plans and reconciles durable jobs independently of transport.
package control

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"

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
	store   *store.Store
	targets []backend.Target
	failed  atomic.Bool
}

// New validates targets and orders selection by name.
func New(s *store.Store, targets []backend.Target) (*Control, error) {
	if s == nil || len(targets) == 0 {
		return nil, fmt.Errorf("store and targets required")
	}
	targets = append([]backend.Target(nil), targets...)
	sort.Slice(targets, func(i, j int) bool { return targets[i].Name < targets[j].Name })
	for i, t := range targets {
		if t.Name == "" || t.Backend == nil || (i > 0 && targets[i-1].Name == t.Name) {
			return nil, fmt.Errorf("invalid target %q", t.Name)
		}
	}
	return &Control{store: s, targets: targets}, nil
}

// Validate rejects malformed tasks before capability checks or side effects.
func Validate(t backend.Task) error {
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
	data, err := json.Marshal(t)
	if err != nil {
		return err
	}
	if len(data) > 256<<10 {
		return fmt.Errorf("%w: task exceeds 256 KiB", ErrInvalid)
	}
	return nil
}

// Targets returns configuration metadata; callers must not invoke its backends.
func (c *Control) Targets() []backend.Target { return append([]backend.Target(nil), c.targets...) }

// Plan selects the first eligible target without promising available capacity.
func (c *Control) Plan(t backend.Task) (string, []Rejection, error) {
	if err := Validate(t); err != nil {
		return "", nil, err
	}
	var selected string
	var rejected []Rejection
	for _, target := range c.targets {
		var err error
		if t.Target != "" && t.Target != target.Name {
			err = fmt.Errorf("target constraint differs")
		} else {
			err = target.Backend.Check(t)
		}
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
func (c *Control) Submit(requestID string, t backend.Task) (backend.Job, error) {
	if !requestName.MatchString(requestID) {
		return backend.Job{}, fmt.Errorf("%w: request_id must contain 1..128 letters, digits, underscores or hyphens", ErrInvalid)
	}
	if err := Validate(t); err != nil {
		return backend.Job{}, err
	}
	if len(t.Env) == 0 {
		t.Env = nil
	}
	job, found, err := c.store.Lookup(requestID, t)
	if err != nil || found {
		return job, err
	}
	if c.failed.Load() {
		return backend.Job{}, ErrUnavailable
	}
	target, _, err := c.Plan(t)
	if err != nil {
		return backend.Job{}, err
	}
	return c.store.Submit(requestID, t, target)
}

// Get returns one accepted job.
func (c *Control) Get(id string) (backend.Job, error) {
	if !jobName.MatchString(id) {
		return backend.Job{}, fmt.Errorf("%w: invalid job id", ErrInvalid)
	}
	return c.store.Get(id)
}

type cursor struct {
	After uint64 `json:"a"`
	Max   uint64 `json:"m"`
}

// List uses a snapshot high-water mark; later inserts do not shift pages.
func (c *Control) List(size int, token string) ([]backend.Job, string, error) {
	if size < 0 || size > 200 {
		return nil, "", fmt.Errorf("%w: page size outside 0..200", ErrInvalid)
	}
	if size == 0 {
		size = 50
	}
	var cur cursor
	if token == "" {
		var err error
		cur.Max, err = c.store.LastSequence()
		if err != nil {
			return nil, "", err
		}
	} else {
		raw, err := base64.RawURLEncoding.DecodeString(token)
		if err != nil || len(raw) > 100 {
			return nil, "", fmt.Errorf("%w: invalid page token", ErrInvalid)
		}
		if err = json.Unmarshal(raw, &cur); err != nil || cur.After == 0 || cur.After >= cur.Max {
			return nil, "", fmt.Errorf("%w: invalid page token", ErrInvalid)
		}
		canonical, _ := json.Marshal(cur)
		if string(canonical) != string(raw) {
			return nil, "", fmt.Errorf("%w: invalid page token", ErrInvalid)
		}
		max, err := c.store.LastSequence()
		if err != nil {
			return nil, "", err
		}
		if cur.Max > max {
			return nil, "", fmt.Errorf("%w: page token exceeds snapshot", ErrInvalid)
		}
	}
	jobs, err := c.store.List(cur.After, cur.Max, size)
	if err != nil {
		return nil, "", err
	}
	next := ""
	if len(jobs) > 0 && jobs[len(jobs)-1].Sequence < cur.Max {
		cur.After = jobs[len(jobs)-1].Sequence
		raw, _ := json.Marshal(cur)
		next = base64.RawURLEncoding.EncodeToString(raw)
	}
	return jobs, next, nil
}

// Cancel persists cancellation intent. Terminal outcomes never change.
func (c *Control) Cancel(id string) (backend.Job, error) {
	if _, err := c.Get(id); err != nil {
		return backend.Job{}, err
	}
	return c.store.Update(id, func(j *backend.Job) error {
		if !j.Status.State.Terminal() {
			j.CancelRequested = true
		}
		return nil
	})
}

func (c *Control) target(name string) backend.Backend {
	for _, t := range c.targets {
		if t.Name == name {
			return t.Backend
		}
	}
	return nil
}

// Logs reads a bounded segment; loss and truncation are explicit.
func (c *Control) Logs(ctx context.Context, id string, offset int64) (backend.LogChunk, error) {
	if offset < 0 || offset > backend.MaxLogBytes {
		return backend.LogChunk{}, fmt.Errorf("%w: invalid log offset", ErrInvalid)
	}
	j, err := c.Get(id)
	if err != nil {
		return backend.LogChunk{}, err
	}
	if j.Status.State == backend.Canceled && j.Status.Reason == "canceled before execution" {
		if offset != 0 {
			return backend.LogChunk{}, fmt.Errorf("%w: offset past log end", ErrInvalid)
		}
		return backend.LogChunk{Complete: true}, nil
	}
	if j.Status.State == backend.Queued {
		if offset != 0 {
			return backend.LogChunk{}, fmt.Errorf("%w: offset past log end", ErrInvalid)
		}
		return backend.LogChunk{}, nil
	}
	b := c.target(j.Target)
	if b == nil {
		return backend.LogChunk{}, ErrUnavailable
	}
	chunk, err := b.Logs(ctx, backend.Reference{ID: j.ID, Task: j.Task}, offset, backend.MaxChunkBytes)
	if err == nil {
		return chunk, nil
	}
	if errors.Is(err, fs.ErrInvalid) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return chunk, err
	}
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, backend.ErrIdentityMismatch) {
		return chunk, ErrLogsUnavailable
	}
	return chunk, fmt.Errorf("%w: backend log read failed", ErrUnavailable)
}

// Healthy reports whether durable reconciliation has encountered a store error.
func (c *Control) Healthy() bool { return !c.failed.Load() }

// Run reconciles serially until canceled. Only one Run may be active.
func (c *Control) Run(ctx context.Context) error {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		if err := c.reconcile(ctx); err != nil {
			c.failed.Store(true)
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}
func (c *Control) reconcile(ctx context.Context) error {
	jobs, err := c.store.Active()
	if err != nil {
		return err
	}
	active := 0
	for _, j := range jobs {
		if j.Status.State == backend.Starting || j.Status.State == backend.Running {
			active++
		}
	}
	for _, j := range jobs {
		if ctx.Err() != nil {
			return nil
		}
		// Re-read to include cancellation persisted since the active snapshot.
		j, err = c.store.Get(j.ID)
		if err != nil {
			return err
		}
		if j.Status.State == backend.Queued && j.CancelRequested {
			_, err = c.store.Update(j.ID, func(current *backend.Job) error {
				current.Status = backend.Status{State: backend.Canceled, Reason: "canceled before execution"}
				return nil
			})
			if err != nil {
				return err
			}
			continue
		}
		b := c.target(j.Target)
		if b == nil {
			if j.Status.Reason != "target is not configured" {
				_, err = c.store.Update(j.ID, func(current *backend.Job) error {
					current.Status.Reason = "target is not configured"
					return nil
				})
				if err != nil {
					return err
				}
			}
			continue
		}
		if j.Status.State == backend.Queued {
			if active >= 32 {
				continue
			}
			active++
			j, err = c.store.Update(j.ID, func(current *backend.Job) error {
				if current.CancelRequested {
					current.Status = backend.Status{State: backend.Canceled, Reason: "canceled before execution"}
				} else {
					current.Status = backend.Status{State: backend.Starting}
				}
				return nil
			})
			if err != nil {
				return err
			}
			if j.Status.State.Terminal() {
				continue
			}
			call, cancel := context.WithTimeout(ctx, 10*time.Second)
			// Ambiguous start errors are resolved by Observe, never by a second Start.
			_ = b.Start(call, j.ID, j.Task)
			cancel()
		}
		call, cancel := context.WithTimeout(ctx, 10*time.Second)
		if j.CancelRequested {
			_ = b.Cancel(call, backend.Reference{ID: j.ID, Task: j.Task})
		}
		status, observeErr := b.Observe(call, backend.Reference{ID: j.ID, Task: j.Task})
		cancel()
		if errors.Is(observeErr, backend.ErrIdentityMismatch) {
			status = backend.Status{State: backend.Unknown, Reason: "execution identity mismatch; conflicting workload left untouched"}
		} else if observeErr != nil {
			continue
		}
		if status.State == "" {
			continue
		}
		_, err = c.store.Update(j.ID, func(current *backend.Job) error {
			if !current.Status.State.Terminal() {
				current.Status = status
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}
