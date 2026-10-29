package servingcontrol

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/serving"
	"github.com/tmc/skygo/internal/store"
)

// Reconcile makes one pass over every unretired service of this
// Controller's binding: it applies Present services, observes them, deletes
// Absent ones and retires those whose absence is established. Passes are
// serialized; a pass that ctx ends while it waits returns ctx's error and
// does nothing. After Close has begun it returns ErrClosed.
func (c *Controller) Reconcile(ctx context.Context) error {
	if err := c.enter(); err != nil {
		return err
	}
	defer c.leave()
	select {
	case c.gate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-c.gate }()
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx, stop := c.effectsContext(ctx)
	defer stop()
	var errs []error
	var after uint64
	for {
		p, err := c.store.Services(after, 200)
		if err != nil {
			return errors.Join(append(errs, err)...)
		}
		for _, s := range p.Services {
			if s.Binding != c.binding {
				continue
			}
			if err := c.reconcile(ctx, s.ID); err != nil {
				errs = append(errs, fmt.Errorf("service %s: %w", s.ID, err))
			}
		}
		if !p.More {
			break
		}
		after = p.After
	}
	if _, err := c.store.PruneServiceRequests(pruneLimit); err != nil {
		errs = append(errs, fmt.Errorf("prune service requests: %w", err))
	}
	return errors.Join(errs...)
}

// effectsContext returns ctx, also canceled when Close begins.
func (c *Controller) effectsContext(ctx context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.effects, cancel)
	return ctx, func() { stop(); cancel() }
}

// reconcile drives service id one step. It first commits any Native an
// earlier call returned whose commit is not known, then drains the late
// results of its original calls, and makes no runtime call until that
// commit is known and those originals are done. Each Apply and Delete
// records its original, under the operation in flight and over the Native
// it is given, immediately before it is made; none is recorded afterwards
// from a later read.
func (c *Controller) reconcile(ctx context.Context, id string) error {
	if err := c.flush(id); err != nil {
		return err
	}
	held, derr := c.drain(id)
	if held {
		return derr
	}
	s, err := c.read(id)
	if errors.Is(err, store.ErrNotFound) {
		return derr
	}
	if err != nil {
		return errors.Join(derr, err)
	}
	if s.Desired == serving.Present {
		return errors.Join(derr, c.ensure(ctx, s))
	}
	return errors.Join(derr, c.remove(ctx, s))
}

// ensure applies the in-flight Apply operation of Present service s, then
// observes it. Without an operation it only observes: this endpoint starts
// no new rollout.
func (c *Controller) ensure(ctx context.Context, s store.Service) error {
	if s.Operation == nil {
		return c.observe(ctx, s)
	}
	op := *s.Operation
	if op.Kind != store.ServiceApply {
		return errors.New("present service has a delete operation")
	}
	rev, ok := s.History.Revision(op.Revision)
	if !ok {
		return errors.New("service operation revision not retained")
	}
	var prev error // the error of the last Apply, whose Native is committed
	for range maxApply {
		// Each Apply is a future effect: it revalidates the accepted
		// authority afresh, also after an earlier Apply of this pass
		// returned and its Native was committed. Unavailability holds the
		// operation; revocation starts nothing new either, and cleanup
		// still runs once the service is deleted. The committed Native is
		// kept either way.
		if err := c.auth.CheckDelegation(s.Authority); err != nil {
			code := store.ServiceAuthorityRevoked
			if errors.Is(err, auth.ErrUnavailable) {
				code = store.ServiceAuthorityUnavailable
			}
			_, cerr := c.condition(s.ID, code, op.Generation)
			return errors.Join(prev, fmt.Errorf("check service authority: %w", err), cerr)
		}
		// The call's original is recorded, under op and over s.Native,
		// before it is made; one already recorded for op is kept.
		c.admitOriginal(s, op)
		native, aerr := c.runtime.Apply(ctx, s.Ref(), rev, op.Generation)
		if native == "" || native == s.Native {
			// prev's causes, such as a read error or a failed Close of the
			// call that returned s.Native, are kept with this call's.
			switch {
			case errors.Is(aerr, serving.ErrIdentityConflict):
				// s.Native, the last Native this pass changed, is committed:
				// the conflict is the runtime's verdict on it.
				_, cerr := c.conflicted(s, op.Generation)
				return errors.Join(prev, aerr, cerr)
			case aerr != nil:
				_, cerr := c.condition(s.ID, uncertainty(aerr), op.Generation)
				return errors.Join(prev, aerr, cerr)
			}
			return c.integrate(ctx, s)
		}
		next, err := c.persist(s.ID, result{op: op, before: s.Native, native: native, err: aerr})
		if err != nil {
			return errors.Join(prev, aerr, err)
		}
		s, prev = next, aerr
		if s.Desired != serving.Present {
			// Deleted meanwhile: the next pass deletes under the same
			// operation.
			return aerr
		}
	}
	return errors.New("service apply did not settle")
}

// conflicted records that the runtime refused s's committed Native as an
// identity conflict: a create refused for good, or recorded objects
// missing, replaced or changed. It commits an observation of that conflict,
// which makes the service Held, keeping the operation in flight, the
// Native, the history and the charges: nothing is adopted, released or
// re-created, and no runtime call is made. It writes nothing once that
// observation is recorded, and nothing if s's Native or operation changed
// meanwhile.
func (c *Controller) conflicted(s store.Service, generation uint64) (store.Service, error) {
	return c.update(s.ID, func(cur store.Service) (store.ServiceCommit, bool, error) {
		if cur.Native != s.Native || !sameOperation(cur.Operation, s.Operation) {
			return store.ServiceCommit{}, false, errors.New("service changed during apply")
		}
		if o := cur.Observation; o != nil && *o == (serving.Observation{Reason: serving.IdentityConflict}) {
			return store.ServiceCommit{}, false, nil
		}
		next, err := c.observed(cur, serving.Observation{Reason: serving.IdentityConflict})
		if err != nil {
			return next, false, err
		}
		next.Conditions = with(next.Conditions, store.ServiceTargetUnavailable, generation, c.now())
		return next, true, nil
	})
}

// integrate observes s after its operation's last Apply returned with the
// committed Native, and commits the observation with the operation cleared.
func (c *Controller) integrate(ctx context.Context, s store.Service) error {
	o, err := c.runtime.Observe(ctx, s.Ref())
	if err != nil {
		_, cerr := c.condition(s.ID, store.ServiceTargetUnavailable, s.History.Current().Generation)
		return errors.Join(err, cerr)
	}
	_, err = c.update(s.ID, func(cur store.Service) (store.ServiceCommit, bool, error) {
		if cur.Native != s.Native || !sameOperation(cur.Operation, s.Operation) {
			return store.ServiceCommit{}, false, errors.New("service changed during integration")
		}
		next, err := c.observed(cur, o)
		if err != nil {
			return next, false, err
		}
		if cur.Desired == serving.Present {
			next.Operation = nil
		}
		return next, true, nil
	})
	return err
}

// observe records a fresh observation of s, which has no operation.
func (c *Controller) observe(ctx context.Context, s store.Service) error {
	o, err := c.runtime.Observe(ctx, s.Ref())
	if err != nil {
		_, cerr := c.condition(s.ID, store.ServiceTargetUnavailable, s.History.Current().Generation)
		return errors.Join(err, cerr)
	}
	_, err = c.update(s.ID, func(cur store.Service) (store.ServiceCommit, bool, error) {
		if cur.Native != s.Native || cur.Operation != nil {
			return store.ServiceCommit{}, false, errors.New("service changed during observation")
		}
		next, err := c.observed(cur, o)
		return next, err == nil, err
	})
	return err
}

// remove deletes the native objects of Absent service s and retires it once
// its absence is established. A delete runs under the operation in flight,
// an Apply's included, or under a Delete operation committed first; a
// failed or unknown commit of that operation sends nothing.
func (c *Controller) remove(ctx context.Context, s store.Service) error {
	// A forward admitted to s holds its exchange until the exchange's
	// original frame settles. Nothing is deleted, cleared or retired, and
	// no charge or name released, until each has returned.
	if c.endForwards(s.ID) {
		return errForwardsAdmitted
	}
	switch {
	case released(s) && s.Reserved == (store.ServiceReservation{}):
		return c.retire(s)
	case released(s):
		// Deleted after an absence observed while present, with nothing in
		// flight: nothing can appear, and no operation can start on it. Its
		// reservation is released only by an absence observed afresh, which
		// is read, never deleted.
		return c.observeReleased(ctx, s)
	case s.Operation == nil:
		next, err := c.update(s.ID, func(cur store.Service) (store.ServiceCommit, bool, error) {
			if cur.Operation != nil {
				return store.ServiceCommit{}, false, nil
			}
			id, err := newID()
			if err != nil {
				return store.ServiceCommit{}, false, err
			}
			cm := cur.Commit()
			cm.Operation = &store.ServiceOperation{
				ID:         id,
				Kind:       store.ServiceDelete,
				Generation: cur.History.Current().Generation,
				Revision:   cur.History.Current().Revision,
				Started:    c.now(),
			}
			return cm, true, nil
		})
		if err != nil {
			return err
		}
		s = next
	}
	// s is a record read from the Store or returned by its commit, with a
	// real operation in flight: the call's original is recorded under it,
	// over s.Native, before the call is made.
	if s.Desired != serving.Absent || s.Operation == nil {
		return errors.New("service delete has no operation in flight")
	}
	c.admitOriginal(s, *s.Operation)
	o, err := c.runtime.Delete(ctx, s.Ref())
	if err != nil {
		_, cerr := c.condition(s.ID, store.ServiceCleanupPending, s.History.Current().Generation)
		return errors.Join(err, cerr)
	}
	s, err = c.update(s.ID, func(cur store.Service) (store.ServiceCommit, bool, error) {
		if cur.Native != s.Native || !sameOperation(cur.Operation, s.Operation) {
			return store.ServiceCommit{}, false, errors.New("service changed during deletion")
		}
		next, err := c.observed(cur, o)
		if err != nil {
			return next, false, err
		}
		if o.Absent {
			// This observation, taken after the committed Native, shows
			// every owned object gone: the operation is integrated.
			next.Operation = nil
			next.Conditions = without(next.Conditions, store.ServiceCleanupPending)
		} else {
			next.Conditions = with(next.Conditions, store.ServiceCleanupPending, cur.History.Current().Generation, c.now())
		}
		return next, true, nil
	})
	if err != nil {
		return err
	}
	if released(s) {
		return c.retire(s)
	}
	return nil
}

// observeReleased releases the reservation of released service s by an
// absence of s.Native observed afresh, and then retires it. It makes only
// the runtime's read: no delete, no Apply and no operation. A read error, an
// observation other than absence, or a record whose Native or operation
// changed meanwhile holds the service, with nothing deleted, released or
// committed: any commit of a released record releases its reservation.
func (c *Controller) observeReleased(ctx context.Context, s store.Service) error {
	o, err := c.runtime.Observe(ctx, s.Ref())
	if err != nil {
		return err
	}
	if !o.Absent {
		return errors.New("released service not observed absent")
	}
	_, err = c.update(s.ID, func(cur store.Service) (store.ServiceCommit, bool, error) {
		if cur.Native != s.Native || !released(cur) {
			return store.ServiceCommit{}, false, errors.New("service changed during observation")
		}
		next, err := c.observed(cur, o)
		return next, err == nil, err
	})
	if err != nil {
		return err
	}
	cur, err := c.read(s.ID)
	if err != nil {
		return err
	}
	if cur.Native != s.Native || !released(cur) || cur.Reserved != (store.ServiceReservation{}) {
		return errors.New("released service reservation held")
	}
	return c.retire(cur)
}

// retire retires released service s. A tombstone bound refusal leaves it a
// released record until requests are pruned.
func (c *Controller) retire(s store.Service) error {
	_, err := c.store.RetireService(s.ID, s.Revision)
	var capacity *store.ServiceCapacityError
	if errors.Is(err, store.ErrServiceStale) || errors.As(err, &capacity) {
		return nil
	}
	return err
}

func released(s store.Service) bool {
	return s.Desired == serving.Absent && s.Operation == nil && s.Observation != nil && s.Observation.Absent
}

// observed returns the commit of cur with observation o taken now, the
// current rollout settled from it and the transient conditions cleared.
func (c *Controller) observed(cur store.Service, o serving.Observation) (store.ServiceCommit, error) {
	if err := o.Validate(); err != nil {
		return store.ServiceCommit{}, fmt.Errorf("runtime observation: %w", err)
	}
	next := cur.Commit()
	if h, err := cur.History.Settle(o); err == nil {
		next.History = h
	}
	next.Observation = &o
	next.ObservedAt = c.after(cur.ObservedAt)
	next.Conditions = without(next.Conditions, store.ServiceTargetUnavailable, store.ServiceOperationUncertain,
		store.ServiceAuthorityUnavailable)
	return next, nil
}

// after returns the current time, or the instant after t if the clock has
// not passed it, so that each recorded observation is newer than the last.
func (c *Controller) after(t time.Time) time.Time {
	now := c.now()
	if !now.After(t) {
		now = t.Add(time.Nanosecond)
	}
	return now
}

// errForwardsAdmitted reports a deleted service whose forwards have not all
// returned.
var errForwardsAdmitted = errors.New("service forwards still admitted")

// A result is one Apply result whose commit is not known: the operation it
// was returned under, which is immutable while in flight, the Native the
// Apply was given, the Native it returned and the call's error. The
// returned Native may record a write that was sent, such as a create the
// target acknowledged. A late result is an offer of an original's late
// facts: it is committed only while op is still in flight, even if its
// Native is already the record's.
type result struct {
	op     store.ServiceOperation
	before serving.Native
	native serving.Native
	err    error
	late   bool
}

// errOriginalChanged reports a late result of an original whose operation
// is no longer in flight. It is never committed or acknowledged.
var errOriginalChanged = errors.New("service operation changed with original result offered")

// errNativeAdvanced reports a held result whose service's committed Native
// is neither the one its Apply was given nor the one it returned: another
// writer, such as a second Controller on the same Store and binding,
// advanced it under the same operation. The result is kept and never
// written over the newer Native.
var errNativeAdvanced = errors.New("service native state advanced with native result held")

// commitResult returns the commit of r over cur, the service's current
// record. It commits r.native only from r.before under r.op, and reports no
// change if cur already carries r.native. Any other record is
// errNativeAdvanced, or another operation's.
func commitResult(cur store.Service, r result) (store.ServiceCommit, bool, error) {
	switch {
	case r.late && (cur.Operation == nil || *cur.Operation != r.op):
		return store.ServiceCommit{}, false, errOriginalChanged
	case cur.Native == r.native:
		return store.ServiceCommit{}, false, nil
	case cur.Operation == nil || *cur.Operation != r.op:
		return store.ServiceCommit{}, false, errors.New("service operation changed with native result held")
	case cur.Native != r.before:
		return store.ServiceCommit{}, false, errNativeAdvanced
	}
	next := cur.Commit()
	next.Native = r.native
	return next, true, nil
}

// persist commits r.native as the Native of service id under r.op, still
// in flight, in place of r.before, and returns the committed record. Until
// the commit is known r is held: reconcile and Close commit it, under r.op
// and over r.before only, before any other runtime call.
func (c *Controller) persist(id string, r result) (store.Service, error) {
	c.hold(id, r)
	s, err := c.update(id, func(cur store.Service) (store.ServiceCommit, bool, error) {
		return commitResult(cur, r)
	})
	if err != nil {
		return store.Service{}, fmt.Errorf("commit service native state: %w", err)
	}
	c.drop(id, r)
	c.committed(id, r)
	return s, nil
}

// committed advances the original of service id recorded under r.op, if
// r, now known committed, followed its last committed Native.
func (c *Controller) committed(id string, r result) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.originals[id]; e != nil && e.op == r.op && e.acked == r.before {
		e.acked = r.native
	}
}

// admitOriginal records, before a runtime call of s under op, the original
// it may leave. One already recorded for s is kept: it is forgotten only
// once drain reports it done.
func (c *Controller) admitOriginal(s store.Service, op store.ServiceOperation) {
	if _, ok := c.runtime.(resultDrainer); !ok {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.originals[s.ID]; !ok {
		c.originals[s.ID] = &original{op: op, ref: s.Ref(), before: s.Native, acked: s.Native}
	}
}

// drain takes the late results of service id's originals, if it has any,
// and reports whether they still hold the service, with every cause they
// reported. It makes no native call; it may run after Stop.
//
// Each offer is committed as a late result, under the original's
// operation over the last Native of it known committed, or found already
// committed under that operation; only then is it acknowledged, with its
// own token. An equal Native is never itself an acknowledgment, and an
// offer is never acknowledged if the operation changed or another Native
// advanced the record: it stays offered and the service held. The
// original is forgotten only once the runtime reports every original
// returned and each fact acknowledged.
func (c *Controller) drain(id string) (held bool, err error) {
	c.mu.Lock()
	e := c.originals[id]
	c.mu.Unlock()
	d, ok := c.runtime.(resultDrainer)
	if e == nil || !ok {
		return false, nil
	}
	var errs []error
	for range maxApply {
		ref := e.ref
		ref.Native = e.acked
		native, token, oerr := d.Drain(ref)
		errs = append(errs, oerr)
		if token == 0 {
			if oerr != nil {
				return true, errors.Join(errs...)
			}
			c.mu.Lock()
			if c.originals[id] == e {
				delete(c.originals, id)
			}
			c.mu.Unlock()
			return false, errors.Join(errs...)
		}
		if _, err := c.persist(id, result{op: e.op, before: e.acked, native: native, err: oerr, late: true}); err != nil {
			return true, errors.Join(append(errs, err)...)
		}
		ref.Native = native
		if err := d.Ack(ref, token); err != nil {
			return true, errors.Join(append(errs, err)...)
		}
	}
	return true, errors.Join(append(errs, errors.New("service original results did not settle"))...)
}

// hold records r as the uncommitted result of service id.
func (c *Controller) hold(id string, r result) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pending[id] = r
}

// drop forgets the held result r of service id once its commit is known.
func (c *Controller) drop(id string, r result) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if p, ok := c.pending[id]; ok && p.op == r.op && p.before == r.before && p.native == r.native {
		delete(c.pending, id)
	}
}

// held reports whether service id has a result whose commit is not known.
func (c *Controller) held(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.pending[id]
	return ok
}

// flush commits the held result of service id, if any, under the operation
// it was returned under and over the Native its Apply was given. It stays
// held until that exact commit is known: a record already carrying its
// Native has it, since every Native a runtime returns is distinct. A record
// whose operation in flight is another one, or none, or whose Native is
// another, newer one, is never given it; the result stays held, with its
// operation and both Natives, and the error is returned.
func (c *Controller) flush(id string) error {
	c.mu.Lock()
	r, ok := c.pending[id]
	c.mu.Unlock()
	if !ok {
		return nil
	}
	_, err := c.update(id, func(cur store.Service) (store.ServiceCommit, bool, error) {
		return commitResult(cur, r)
	})
	if err != nil {
		return fmt.Errorf("commit held service native state: %w", err)
	}
	c.drop(id, r)
	c.committed(id, r)
	return nil
}

// update applies f to the current record of service id and commits its
// result, retrying a lost revision race. f reports false to commit nothing.
func (c *Controller) update(id string, f func(store.Service) (store.ServiceCommit, bool, error)) (store.Service, error) {
	for range maxAttempts {
		cur, err := c.read(id)
		if err != nil {
			return store.Service{}, err
		}
		next, change, err := f(cur)
		if err != nil || !change {
			return cur, err
		}
		s, err := c.commit(id, cur.Revision, next)
		if errors.Is(err, store.ErrServiceStale) {
			continue
		}
		return s, err
	}
	return store.Service{}, errContended
}

// condition records code against generation on service id, keeping
// everything else.
func (c *Controller) condition(id string, code store.ServiceConditionCode, generation uint64) (store.Service, error) {
	return c.update(id, func(cur store.Service) (store.ServiceCommit, bool, error) {
		for _, x := range cur.Conditions {
			if x.Code == code && x.Generation == generation {
				return store.ServiceCommit{}, false, nil
			}
		}
		next := cur.Commit()
		next.Conditions = with(next.Conditions, code, generation, c.now())
		return next, true, nil
	})
}

// uncertainty classifies a failed runtime call whose Native did not change.
func uncertainty(err error) store.ServiceConditionCode {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return store.ServiceOperationUncertain
	}
	return store.ServiceTargetUnavailable
}

func with(cs []store.ServiceCondition, code store.ServiceConditionCode, generation uint64, now time.Time) []store.ServiceCondition {
	out := without(cs, code)
	return append(out, store.ServiceCondition{Code: code, Generation: generation, Since: now})
}

func without(cs []store.ServiceCondition, codes ...store.ServiceConditionCode) []store.ServiceCondition {
	var out []store.ServiceCondition
	for _, x := range cs {
		keep := true
		for _, code := range codes {
			if x.Code == code {
				keep = false
			}
		}
		if keep {
			out = append(out, x)
		}
	}
	return out
}

func sameOperation(a, b *store.ServiceOperation) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
