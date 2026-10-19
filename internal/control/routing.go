package control

import (
	"maps"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/tmc/skygo/internal/backend"
)

const (
	missingTarget = "target is not configured"
	unboundTarget = "target binding is unavailable; execution held"
	changedTarget = "target binding has changed; execution held"
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
	return reason == missingTarget || reason == unboundTarget || reason == changedTarget
}
func reference(j backend.Job) backend.Reference {
	return backend.Reference{ID: j.ID, Task: cloneTask(j.Task), Binding: j.TargetBinding}
}
