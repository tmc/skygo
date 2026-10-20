package control

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// reconcileStep exercises the production coordinator and workers through one
// observation round, including Start intent/result when the single job is new.
// Routing tests use it to inspect stable fences without a background loop.
func reconcileStep(t *testing.T, c *Control) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := c.newLoop(ctx)
	defer stopTestLoop(t, cancel, s)
	for round := 0; round < 2; round++ {
		if err := c.schedule(ctx, s); err != nil {
			return err
		}
		pending := 0
		started := false
		for _, w := range s.workers {
			if w.pending != nil {
				pending++
				started = started || w.pending.kind == startOperation
			}
		}
		for i := 0; i < pending; i++ {
			select {
			case result := <-s.results:
				if err := c.integrate(s, result); err != nil {
					return err
				}
			case <-time.After(3 * time.Second):
				return fmt.Errorf("routing observation did not complete")
			}
		}
		if !started {
			return nil
		}
	}
	return nil
}
