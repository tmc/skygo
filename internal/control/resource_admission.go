package control

import (
	"errors"

	"github.com/tmc/skygo/internal/backend"
)

// deferResourceStart returns a proven unstarted plain job to the queue. Retry
// attempts, workspace owners and retained output reservations need their own
// durable deferral transition; rolling them back here would lose accounting.
func (c *Control) deferResourceStart(j backend.Job, r operationResult) (bool, error) {
	if r.notStarted || !errors.Is(r.err, backend.ErrResourcesUnavailable) ||
		c.retention.enabled || j.Task.Retry != nil || j.Task.Workdir != nil ||
		j.Status.State != backend.Starting || j.Status.Instance != "" ||
		j.Status.ExitCode != nil || j.Status.Termination != backend.UnknownTermination ||
		!c.configuredLocalJob(j) {
		return false, nil
	}
	_, err := c.store.Update(j.ID, func(current *backend.Job) error {
		if current.Status.State == backend.Starting && sameExecution(*current, j) {
			current.Status = backend.Status{State: backend.Queued, Reason: "waiting for local resources"}
		}
		return nil
	})
	return true, err
}
