package kube

import (
	"errors"
	"slices"

	"github.com/tmc/skygo/internal/backend"
	"k8s.io/apimachinery/pkg/util/validation"
)

func (b *Backend) checkPolicy(t backend.Task) error {
	if t.CPULimitMillis < 0 || t.MemoryLimitBytes < 0 {
		return errors.New("resource limits must be nonnegative")
	}
	if t.CPULimitMillis > 0 && (t.CPUMillis == 0 || t.CPULimitMillis < t.CPUMillis) {
		return errors.New("cpu limit requires a positive request no greater than the limit")
	}
	if t.MemoryLimitBytes > 0 && (t.MemoryBytes == 0 || t.MemoryLimitBytes < t.MemoryBytes) {
		return errors.New("memory limit requires a positive request no greater than the limit")
	}
	p := b.policy
	if p.MaxCPUMillis > 0 && (t.CPUMillis > p.MaxCPUMillis || t.CPULimitMillis > p.MaxCPUMillis) {
		return errors.New("cpu quantity exceeds target policy")
	}
	if p.MaxMemoryBytes > 0 && (t.MemoryBytes > p.MaxMemoryBytes || t.MemoryLimitBytes > p.MaxMemoryBytes) {
		return errors.New("memory quantity exceeds target policy")
	}
	if p.MaxAccelerators > 0 && t.AcceleratorCount > p.MaxAccelerators {
		return errors.New("accelerator count exceeds target policy")
	}
	if len(t.NodeSelector) > 16 {
		return errors.New("too many node selectors")
	}
	for key, value := range t.NodeSelector {
		if len(validation.IsQualifiedName(key)) != 0 || len(validation.IsValidLabelValue(value)) != 0 {
			return errors.New("invalid node selector")
		}
		if !slices.Contains(p.NodeSelectors[key], value) {
			return errors.New("node selector is not allowed by target policy")
		}
	}
	if t.Accelerator != "" {
		a, ok := p.Accelerators[t.Accelerator]
		if !ok {
			return errors.New("accelerator is not allowed by target policy")
		}
		for key, value := range a.RequiredNodeSelector {
			if got, ok := t.NodeSelector[key]; !ok || got != value {
				return errors.New("accelerator requires an explicit node selector")
			}
		}
	}
	return nil
}
