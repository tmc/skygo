package servingcontrol

import "context"

// enter admits one call — Create, Get, List, Delete or a Reconcile pass —
// until leave, which the call makes after its last Store read or write and
// signal. It refuses with ErrClosed once Close has begun, before the call
// checks authority or reads or writes anything. Forwards are admitted by
// admitForward.
func (c *Controller) enter() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	c.active++
	return nil
}

// leave ends a call admitted by enter.
func (c *Controller) leave() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active--
	if c.closed && c.active == 0 {
		close(c.idle)
	}
}

// A forward is one admitted forwarded request. It is not zero-sized, so
// that distinct forwards have distinct addresses.
type forward struct{ _ byte }

// admitForward admits one forward to service id under parent, bounded by
// MaxForwards, and returns its context and release. The context ends when
// parent does, when the service is deleted through this Controller or when
// Close begins. release must be called once, after the runtime call returns.
func (c *Controller) admitForward(parent context.Context, id string) (context.Context, func(), error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.closed:
		return nil, nil, ErrClosed
	case c.nforward >= MaxForwards:
		return nil, nil, ErrForwardLimit
	}
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(c.effects, cancel)
	f := &forward{}
	if c.forwards[id] == nil {
		c.forwards[id] = map[*forward]context.CancelFunc{}
	}
	c.forwards[id][f] = cancel
	c.nforward++
	c.active++
	release := func() {
		stop()
		cancel()
		c.mu.Lock()
		delete(c.forwards[id], f)
		if len(c.forwards[id]) == 0 {
			delete(c.forwards, id)
		}
		c.nforward--
		c.mu.Unlock()
		c.leave()
	}
	return ctx, release, nil
}

// endForwards cancels this process's forwards to service id and reports
// whether any is still admitted. Cancellation is not their end: each holds
// its admission until its runtime call returns, which a runtime makes only
// once the exchange's original frame has settled.
func (c *Controller) endForwards(id string) (admitted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cancel := range c.forwards[id] {
		cancel()
	}
	return len(c.forwards[id]) != 0
}
