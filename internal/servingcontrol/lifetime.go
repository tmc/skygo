package servingcontrol

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"
)

// Run reconciles until ctx ends or Close begins, after each Create or
// Delete and at least every ten seconds. It discards the errors of a pass;
// Reconcile returns them.
func (c *Controller) Run(ctx context.Context) error {
	t := time.NewTicker(reconcileInterval)
	defer t.Stop()
	for {
		if err := c.Reconcile(ctx); errors.Is(err, ErrClosed) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.effects.Done():
			return ErrClosed
		case <-t.C:
		case <-c.wake:
		}
	}
}

// Close refuses new calls, passes and forwards at once, with no Store read
// or write and no runtime call; cancels the runtime calls and forwards in
// progress; and waits until every admitted call has returned after its last
// Store write. A forward returns only once its exchange's original frame
// has settled.
//
// A runtime result held uncommitted may record a write that was sent, such
// as a create acknowledged before its Native's commit failed. Close then
// retries only that commit, under the operation the result was returned
// under and over the Native its Apply was given, and makes no runtime call;
// it returns nil only once every held result is committed. A record whose
// Native another writer advanced meanwhile is never overwritten: the result
// stays held.
//
// A runtime that keeps an original call's exchange after the call returned
// is drained the same way, without a native call: each late result it
// offers is committed under its original's operation and acknowledged, and
// Close returns nil only once the runtime reports every original returned
// cleanly and each fact acknowledged. An original still pending, a failed
// response Close or a changed operation is an error now, not awaited.
//
// Otherwise, or if ctx ends first, it returns an error, even with no call
// admitted; the calls, results and originals keep their custody, and the
// caller must keep the Store, auth engine and runtime and may call Close
// again.
func (c *Controller) Close(ctx context.Context) error {
	c.Stop()
	select {
	case <-c.idle:
	case <-ctx.Done():
		return ctx.Err()
	}
	return c.settle(ctx)
}

// Stop begins Close without waiting: it refuses new calls, passes and
// forwards and cancels those in progress, and returns at once. An owner
// stopping several components stops each before it waits for any; only
// Close joins.
func (c *Controller) Stop() {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		if c.active == 0 {
			close(c.idle)
		}
	}
	c.mu.Unlock()
	c.cancel()
}

// errResultHeld reports a runtime result whose commit is not yet known.
var errResultHeld = errors.New("service runtime result held uncommitted")

// settle commits the held results once no call is admitted. It holds the
// pass gate, so that it is the only Store writer.
func (c *Controller) settle(ctx context.Context) error {
	select {
	case c.gate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-c.gate }()
	c.mu.Lock()
	ids := make([]string, 0, len(c.pending)+len(c.originals))
	for id := range c.pending {
		ids = append(ids, id)
	}
	for id := range c.originals {
		ids = append(ids, id)
	}
	c.mu.Unlock()
	slices.Sort(ids)
	ids = slices.Compact(ids)
	var errs []error
	for _, id := range ids {
		if err := c.flush(id); err != nil {
			errs = append(errs, fmt.Errorf("service %s: %w: %w", id, errResultHeld, err))
			continue
		}
		// Every offer ready now is committed and acknowledged; an original
		// still pending, or failed, holds the service. Nothing waits for
		// it here: a later Close or pass drains it again.
		if held, err := c.drain(id); held {
			errs = append(errs, fmt.Errorf("service %s: %w: %w", id, errOriginalHeld, err))
		}
	}
	return errors.Join(errs...)
}

// errOriginalHeld reports a service whose original runtime call has not
// returned, failed, or has facts not yet committed and acknowledged.
var errOriginalHeld = errors.New("service runtime original held")
