package control

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tmc/skygo/internal/backend"
)

const (
	missingTarget          = "target is not configured"
	unboundTarget          = "target binding is unavailable; execution held"
	changedTarget          = "target binding has changed; execution held"
	unsupportedRetryTarget = "retry execution is unsupported; execution held"
)

func validText(s string, max int, empty bool) bool {
	return (empty || s != "") && len(s) <= max && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}
func validLabels(labels map[string]string, max int) bool {
	if len(labels) > max {
		return false
	}
	for k, v := range labels {
		if !validText(k, 256, false) || !validText(v, 4096, true) {
			return false
		}
	}
	return true
}
func validAcceleratorNames(names []string) bool {
	if len(names) > 32 {
		return false
	}
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		if !validText(name, 256, false) || seen[name] {
			return false
		}
		seen[name] = true
	}
	return true
}
func cloneTarget(t backend.Target) backend.Target {
	t.Advice = backend.CloneAdvice(t.Advice)
	t.Labels = maps.Clone(t.Labels)
	t.AcceleratorNames = slices.Clone(t.AcceleratorNames)
	slices.Sort(t.AcceleratorNames)
	return t
}
func cloneTask(t backend.Task) backend.Task {
	t.Argv = slices.Clone(t.Argv)
	t.Env = maps.Clone(t.Env)
	t.TargetLabels = maps.Clone(t.TargetLabels)
	t.NodeSelector = maps.Clone(t.NodeSelector)
	if t.Retry != nil {
		p := *t.Retry
		p.ExitCodes = slices.Clone(p.ExitCodes)
		t.Retry = &p
	}
	return t
}
func matchesLabels(have, want map[string]string) bool {
	for k, v := range want {
		value, ok := have[k]
		if !ok || value != v {
			return false
		}
	}
	return true
}

// Configuration and accepted routes are immutable. The check fences every
// external call without inferring migration authority from target names/kinds.
func (c *Control) boundTarget(j backend.Job) (backend.Target, string) {
	t, ok := c.target(j.Target)
	if !ok {
		return backend.Target{}, missingTarget
	}
	if j.TargetBinding == "" {
		return backend.Target{}, unboundTarget
	}
	if j.TargetBinding != t.Binding {
		return backend.Target{}, changedTarget
	}
	return t, ""
}
func routingHeld(reason string) bool {
	return reason == missingTarget || reason == unboundTarget || reason == changedTarget || reason == unsupportedRetryTarget
}
func reference(j backend.Job) backend.Reference {
	return backend.Reference{ID: j.CurrentAttemptID(), Task: cloneTask(j.Task), Binding: j.TargetBinding, Instance: currentStatus(j).Instance}
}

func normalizeTask(t backend.Task) (backend.Task, error) {
	retry, err := backend.NormalizeRetry(t.Retry)
	if err != nil {
		return backend.Task{}, fmt.Errorf("%w: invalid retry policy", ErrInvalid)
	}
	t.Retry = retry
	return t, nil
}
func currentStatus(j backend.Job) backend.Status {
	if len(j.Attempts) != 0 {
		return j.Attempts[len(j.Attempts)-1].Status
	}
	return j.Status
}
func cloneStatus(s backend.Status) backend.Status {
	if s.ExitCode != nil {
		code := *s.ExitCode
		s.ExitCode = &code
	}
	return s
}
func cloneTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	copy := *t
	return &copy
}
func cloneJob(j backend.Job) backend.Job {
	j.Task = cloneTask(j.Task)
	j.Status = cloneStatus(j.Status)
	j.NextAttemptAt = cloneTime(j.NextAttemptAt)
	j.Attempts = slices.Clone(j.Attempts)
	for i := range j.Attempts {
		a := &j.Attempts[i]
		a.Status = cloneStatus(a.Status)
		a.StartingAt = cloneTime(a.StartingAt)
		a.FinishedAt = cloneTime(a.FinishedAt)
	}
	return j
}
func needsStart(j backend.Job) bool {
	return j.Status.State == backend.Queued || j.Status.State == backend.Retrying
}
