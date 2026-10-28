package environmentcontrol

import (
	"context"
	"errors"
	"net/http"

	"github.com/tmc/skygo/internal/auth"
)

// Forward relays one HTTP exchange of req to the declared port of an
// environment that actor owns, in a session durably admitted before the
// runtime is called. req's URL path and query are relayed as they are; the
// caller removes its own route prefix first. No host, port or URL comes from
// req.
//
// If Forward returns an error, nothing was written to w. Otherwise the
// runtime wrote the response, and the result's Err reports what failed. The
// exchange ends when the client leaves or the environment is deleted, which
// closes only this session. The session ends once the frame settles; a
// forward starts no remote work of its own.
//
// Like Exec, each Forward is a session owner from entry until its session's
// release is committed, and refuses at entry once sessions are stopped.
func (c *Controller) Forward(w http.ResponseWriter, req *http.Request, actor auth.Actor, id string) (ForwardResult, error) {
	if err := c.enterSession(); err != nil {
		return ForwardResult{}, err
	}
	owned := true
	defer func() {
		if owned {
			c.leaveSession()
		}
	}()
	r, sid, err := c.admit(req.Context(), actor, id, forwardSession)
	if err != nil {
		return ForwardResult{}, err
	}
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()
	if err := c.track(id, sid, cancel); err != nil {
		return ForwardResult{}, errors.Join(err, c.release(id, sid))
	}
	// A client that left, or lost its authority, after admission is not
	// relayed to, and its session ends unstarted.
	if err := c.startable(ctx, actor); err != nil {
		c.untrack(id, sid)
		return ForwardResult{}, errors.Join(err, c.release(id, sid))
	}
	f := c.runtime.Forward(w, req.WithContext(ctx), r)
	res := f.Result()
	if !res.Settled {
		_ = c.track(id, sid, func() { cancel(); f.Cancel() })
		// The settlement owns the session until its release is committed.
		owned = false
		go func() {
			defer c.leaveSession()
			<-f.Done()
			c.untrack(id, sid)
			_ = c.release(id, sid)
		}()
		return res, nil
	}
	c.untrack(id, sid)
	if err := c.release(id, sid); err != nil {
		res.Err = errors.Join(res.Err, err)
	}
	return res, nil
}
