package environmentcontrol

import "context"

// ErrSessionsStopped is the error of a session refused once sessions stop.
var ErrSessionsStopped = errSessionsStopped

// StopSessions stops c's sessions, as Integration.Close does first.
func StopSessions(c *Controller) { c.stopSessions() }

// JoinSessions stops c's sessions and waits for their owners until ctx ends.
func JoinSessions(c *Controller, ctx context.Context) error { return c.joinSessions(ctx) }

// SessionOwners returns the number of c's session owners.
func SessionOwners(c *Controller) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.owners
}

// TracksSession reports whether c tracks an unsettled session frame of
// environment id.
func TracksSession(c *Controller, id string) bool { return c.holding(id) }
