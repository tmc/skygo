package control

import (
	"context"
	"errors"

	"github.com/tmc/skygo/internal/auth"
)

const maxPendingAccess = 256
const accessDrainQuota = 4

var errAuditQueueFull = errors.New("audit queue full")

type pendingAccess struct {
	actor                       auth.Actor
	operation, resource, result string
}

// RecordAccess queues best-effort diagnostic metadata without database I/O.
// Enqueue success is not persistence. Run attempts bounded delivery; pending
// events may be lost on overflow, shutdown, crash, or persistence failure.
// Only fixed categories and an authorized job ID or empty are accepted.
func (c *Control) RecordAccess(actor auth.Actor, operation, resource, result string) error {
	if err := c.store.ValidateAccess(actor, operation, resource, result); err != nil {
		c.auditFailed.Store(true)
		return err
	}
	if !c.store.Scoped() {
		return nil
	}
	select {
	case c.access <- pendingAccess{actor, operation, resource, result}:
		return nil
	default:
		c.auditLost.Store(true)
		return errAuditQueueFull
	}
}

// AuditHealthy reports diagnostic queue/persistence health independently of
// access or reconciliation. Overflow remains visible until this Control is
// replaced; a successful persistence attempt clears a recoverable write fault.
func (c *Control) AuditHealthy() bool { return !c.auditLost.Load() && !c.auditFailed.Load() }

// drainAccess shares Run's existing lifecycle. Persistence can wait on the
// filesystem like other durable writes. RPCs enqueue without waiting for this
// persistence; shared database contention can still affect other store calls.
// Stored timestamps describe persistence attempts, not original queue times.
func (c *Control) drainAccess(ctx context.Context) {
	for i := 0; i < accessDrainQuota; i++ {
		if ctx.Err() != nil {
			return
		}
		select {
		case entry := <-c.access:
			err := c.store.RecordAccess(entry.actor, entry.operation, entry.resource, entry.result)
			c.auditFailed.Store(err != nil)
		default:
			return
		}
	}
}
