package servingcontrol

import (
	"net/http"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/serving"
	"github.com/tmc/skygo/internal/store"
)

// Forward relays request r of actor to service id through the runtime,
// writing the response to w. It requires Mutate and that actor is the
// service's own principal; any other service, an admin's included, is
// store.ErrNotFound. Before any runtime call it requires the service Present
// with its current generation observed ready, at least one ready endpoint,
// no operation in flight, no held result and no ForwardDisabled condition;
// otherwise it is ErrNotReady. At most MaxForwards run at once. The
// exchange ends when r's context ends, when the service is deleted through
// this Controller or when Close begins; its admission ends only when the
// runtime call returns, which is once the exchange's original frame has
// settled, so a deletion of the service and Close wait for it. An error
// means nothing was written to w unless the runtime wrote a response first.
func (c *Controller) Forward(actor auth.Actor, id string, w http.ResponseWriter, r *http.Request) error {
	if !store.ValidServiceID(id) {
		return store.ErrNotFound
	}
	ctx, release, err := c.admitForward(r.Context(), id)
	if err != nil {
		return err
	}
	defer release()
	actor, err = c.auth.Check(actor, auth.Mutate)
	if err != nil {
		return err
	}
	s, err := c.store.ServiceFor(actor, id)
	if err != nil {
		return err
	}
	if s.Principal != actor.Principal() {
		return store.ErrNotFound
	}
	if !c.forwardable(s) {
		return ErrNotReady
	}
	return c.runtime.Forward(ctx, s.Ref(), w, r.WithContext(ctx))
}

// forwardable reports whether s admits a new forwarded request.
func (c *Controller) forwardable(s store.Service) bool {
	if s.Desired != serving.Present || s.Operation != nil || s.Observation == nil || c.held(s.ID) {
		return false
	}
	for _, x := range s.Conditions {
		if x.Code == store.ServiceForwardDisabled {
			return false
		}
	}
	o := s.Observation
	return s.Phase() == serving.Ready && o.ReadyGeneration == s.History.Current().Generation && o.ReadyEndpoints > 0
}
