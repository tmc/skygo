package control

import (
	"errors"
	"fmt"
	"slices"

	"github.com/tmc/skygo/internal/backend"
)

// Modes returns an independent copy of target's explicitly configured worker
// modes, in Independent, TCP order. Nil or zero Workers, legacy configuration,
// and an omitted or empty target entry return no modes.
//
// This memory-only query reports configured software support, not authority,
// capacity, readiness or native qualification. A handler may advertise it only
// while serving this Workers to a current file-scoped actor. No modes does not
// invalidate an already accepted request's original mode or cleanup duty.
func (w *Workers) Modes(target string) []backend.WorkerMode {
	if w == nil {
		return nil
	}
	return slices.Clone(w.modes[target])
}

// workerModes validates and copies the optional mode policy. In particular,
// a nonnil empty table must stay nonnil: it denies all new groups.
func workerModes(c *Control, config WorkerConfig) (map[string][]backend.WorkerMode, error) {
	if config.Modes == nil {
		return nil, nil
	}
	modes := make(map[string][]backend.WorkerMode, len(config.Modes))
	positive := false
	for name, entry := range config.Modes {
		if _, ok := config.Budgets[name]; !ok {
			return nil, fmt.Errorf("worker modes name unconfigured budget %q", name)
		}
		var independent, tcp bool
		for _, mode := range entry {
			switch mode {
			case backend.WorkerIndependent:
				if independent {
					return nil, fmt.Errorf("worker target %q repeats independent mode", name)
				}
				independent = true
			case backend.WorkerTCP:
				if tcp {
					return nil, fmt.Errorf("worker target %q repeats tcp mode", name)
				}
				tcp = true
			default:
				return nil, fmt.Errorf("worker target %q has an invalid mode", name)
			}
		}
		if len(entry) == 0 {
			modes[name] = nil
			continue
		}
		if !independent {
			return nil, fmt.Errorf("worker target %q requires independent mode", name)
		}
		positive = true
		modes[name] = []backend.WorkerMode{backend.WorkerIndependent}
		if tcp {
			modes[name] = append(modes[name], backend.WorkerTCP)
		}
	}
	if positive && (c.store == nil || !c.store.Scoped() || !c.retention.enabled) {
		return nil, errors.New("worker modes require scoped authentication and enabled retention")
	}
	return modes, nil
}

func (w *Workers) allowsMode(target string, mode backend.WorkerMode) bool {
	return w.modes == nil || slices.Contains(w.modes[target], mode)
}
