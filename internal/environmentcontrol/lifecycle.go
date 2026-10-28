package environmentcontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/store"
)

// Create durably accepts spec under requestID in actor's request domain d,
// or returns the environment already bound to requestID in d with identical
// canonical spec bytes, even one since deleted, before the runtime is
// consulted; different bytes are store.ErrConflict. A missing or zero domain,
// or epoch zero, is ErrInvalid. An epoch closed to new requests refuses a new
// requestID, and a retired epoch even a replay, with a *store.ExpiryError.
// Acceptance records the runtime's binding and initial native state and a
// fresh nonce. Create causes no runtime effect; a later Reconcile pass
// creates the resources. A spec with artifacts is ErrUnsupported unless the
// runtime is a Materializer and an ArtifactOpener is bound.
func (c *Controller) Create(actor auth.Actor, d store.RequestDomain, requestID string, spec environment.Spec) (View, error) {
	actor, err := c.auth.Check(actor, auth.Mutate)
	if err != nil {
		return View{}, err
	}
	raw, err := environment.Canonical(spec)
	if err != nil {
		return View{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if !validDomain(d) {
		return View{}, fmt.Errorf("%w: invalid request domain", ErrInvalid)
	}
	digest := sha256.Sum256(raw)
	// The lookup writes nothing. AcceptEnvironment repeats it in its own
	// transaction, so a domain retired since is still refused there.
	e, found, err := c.store.EnvironmentRequest(actor, d, requestID, spec)
	if err != nil {
		return View{}, err
	}
	if found {
		return c.view(e)
	}
	if len(spec.Artifacts) != 0 && !c.ArtifactsEnabled() {
		return View{}, fmt.Errorf("%w: environment artifacts", ErrUnsupported)
	}
	authority, err := c.auth.Delegate(actor)
	if err != nil {
		return View{}, err
	}
	binding, native, err := c.runtime.Plan(actor.Principal(), spec.Clone(), digest)
	if err != nil {
		return View{}, err
	}
	if len(native) > MaxNativeBytes {
		return View{}, errors.New("runtime native state exceeds limit")
	}
	nonce, err := newID()
	if err != nil {
		return View{}, err
	}
	cp, err := c.encode(ledger{Nonce: nonce, Native: bytes.Clone(native), Idle: c.now()})
	if err != nil {
		return View{}, err
	}
	e, err = c.store.AcceptEnvironment(actor, d, requestID, spec, binding, authority, cp)
	if err != nil {
		return View{}, err
	}
	c.signal()
	return c.view(e)
}

// validDomain reports whether d names a lineage, 32 lowercase hexadecimal
// digits, and a nonzero epoch. Epoch zero is the Store's legacy replay
// domain, which no environment request uses.
func validDomain(d store.RequestDomain) bool {
	return len(d.Lineage) == 32 && strings.Trim(d.Lineage, "0123456789abcdef") == "" && d.Epoch != 0
}

// Delete durably records Absent intent for an environment visible to actor,
// then closes this process's sessions of it. A later Reconcile pass removes
// the owned resources; borrowed ones are never removed. Deleting an absent
// environment returns it unchanged.
func (c *Controller) Delete(actor auth.Actor, id string) (View, error) {
	actor, err := c.auth.Check(actor, auth.Mutate)
	if err != nil {
		return View{}, err
	}
	if !environment.ValidID(id) {
		return View{}, fmt.Errorf("%w: invalid environment id", ErrInvalid)
	}
	if _, err := c.store.EnvironmentFor(actor, id); err != nil {
		return View{}, err
	}
	e, _, err := c.update(id, func(e store.Environment, l *ledger) (environment.State, bool, error) {
		if e.State.Desired == environment.Absent {
			return e.State, false, nil
		}
		return deleting(), true, nil
	})
	if err != nil {
		return View{}, err
	}
	c.closeSessions(id)
	c.signal()
	return c.view(e)
}

func deleting() environment.State {
	return environment.State{Desired: environment.Absent, Phase: environment.Deleting}
}

// Run reconciles until ctx ends, after each Create or Delete and at least
// every ten seconds. It discards the errors of a pass; Reconcile returns
// them.
func (c *Controller) Run(ctx context.Context) error {
	c.mu.Lock()
	c.running = true
	c.mu.Unlock()
	t := time.NewTicker(reconcileInterval)
	defer t.Stop()
	for {
		_ = c.Reconcile(ctx)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		case <-c.wake:
		}
	}
}

// lock takes the lifecycle gate for ctx. If ctx ends first, or had ended
// when the gate was taken, lock returns ctx's error and holds nothing. The
// holder releases the gate with unlock, once.
func (c *Controller) lock(ctx context.Context) error {
	select {
	case c.gate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	// Both cases may have been ready; select chose either.
	if err := ctx.Err(); err != nil {
		c.unlock()
		return err
	}
	return nil
}

func (c *Controller) unlock() { <-c.gate }

func (c *Controller) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// Reconcile makes one pass over every environment of this Controller's
// kind: it latches idle expiry, ensures Present environments and deletes
// Absent ones, committing each observation. Passes are serialized, with
// each other and with session admission; a pass that ctx ends while it
// waits returns ctx's error and does nothing.
func (c *Controller) Reconcile(ctx context.Context) error {
	if err := c.lock(ctx); err != nil {
		return err
	}
	defer c.unlock()
	var errs []error
	var after uint64
	for {
		p, err := c.store.Environments(after, 200)
		if err != nil {
			return errors.Join(append(errs, err)...)
		}
		for _, e := range p.Environments {
			if e.Checkpoint.Kind != c.kind || e.State.Phase == environment.Deleted {
				continue
			}
			if err := c.reconcile(ctx, e.Identity.ID); err != nil {
				errs = append(errs, fmt.Errorf("environment %s: %w", e.Identity.ID, err))
			}
		}
		if !p.More {
			return errors.Join(errs...)
		}
		after = p.After
	}
}

// reconcile ensures or deletes environment id. It first commits any runtime
// result an earlier pass could not commit, and calls the runtime only once
// that commit is positively known. Each result is held as pending from the
// call's return until its commit is known; a write declared with
// ErrWritePending is dispatched by calling the runtime again with the
// committed row. Once a committed row records an incarnation of a Present
// environment with artifacts, its materialization starts.
func (c *Controller) reconcile(ctx context.Context, id string) error {
	if err := c.flush(id); err != nil {
		return err
	}
	e, l, err := c.expire(id)
	if err != nil {
		return err
	}
	for range maxPending {
		ensure := e.State.Desired == environment.Present
		var obs Observation
		var rerr error
		if ensure {
			// Future effects revalidate the accepted authority.
			if err := c.auth.CheckDelegation(e.Authority); err != nil {
				return fmt.Errorf("check environment authority: %w", err)
			}
			obs, rerr = c.runtime.Ensure(ctx, c.ref(e, l))
		} else {
			c.closeSessions(id)
			c.stopMaterializing(id)
			obs, rerr = c.runtime.Delete(ctx, c.ref(e, l))
		}
		p := c.hold(id, e.Identity, obs, ensure, rerr)
		var perr error
		e, l, perr, err = c.observe(id, p)
		if err != nil {
			return errors.Join(rerr, err)
		}
		c.drop(id, p)
		c.materialize(e, l)
		if perr != nil || !errors.Is(rerr, ErrWritePending) {
			return errors.Join(rerr, perr)
		}
	}
	return nil
}

// A pending is one runtime result of environment identity that is not yet
// known to be committed: its observation, deep copied, and the call's own
// error. It is process-local: after a restart the runtime's durable
// declarations, not this record, account for what was sent.
type pending struct {
	identity environment.Identity
	obs      Observation
	ensure   bool
	err      error
}

// hold records a runtime result of environment id as pending and returns it.
// It replaces no earlier result: flush has committed any before the call.
func (c *Controller) hold(id string, identity environment.Identity, obs Observation, ensure bool, err error) *pending {
	p := &pending{identity: identity, obs: Observation{Phase: obs.Phase, Native: bytes.Clone(obs.Native), Incarnation: obs.Incarnation}, ensure: ensure, err: err}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pending[id] = p
	return p
}

// drop forgets p, once its commit is known.
func (c *Controller) drop(id string, p *pending) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pending[id] == p {
		delete(c.pending, id)
	}
}

// flush commits environment id's pending result, if any, merging the same
// Native and phase into the current row as observe does. The result stays
// pending until the exact commit is positively known, and until then no
// runtime call for the environment and no session admission may follow.
func (c *Controller) flush(id string) error {
	c.mu.Lock()
	p := c.pending[id]
	c.mu.Unlock()
	if p == nil {
		return nil
	}
	if _, _, _, err := c.observe(id, p); err != nil {
		return fmt.Errorf("commit pending environment result: %w", errors.Join(p.err, err))
	}
	c.drop(id, p)
	return nil
}

// expire latches idle expiry of environment id, and returns its current row
// either way. Expiry requires a timeout, Present intent, a phase past
// Creating, no recorded session, open or unknown, and a full idle timeout
// since the last one ended. It shares the revision with session admission,
// so a session admitted first prevents it and a session admitted later is
// refused.
func (c *Controller) expire(id string) (store.Environment, ledger, error) {
	return c.update(id, func(e store.Environment, l *ledger) (environment.State, bool, error) {
		at, ok := idleDeadline(e.Spec, e.State, l.Idle, len(l.Sessions))
		if !ok || c.now().Before(at) {
			return e.State, false, nil
		}
		l.Expired = true
		return deleting(), true, nil
	})
}

// idleDeadline returns the earliest time idle expiry may latch, and false if
// it cannot now.
func idleDeadline(spec environment.Spec, s environment.State, idle time.Time, sessions int) (time.Time, bool) {
	if spec.IdleTimeoutSeconds == 0 || s.Desired != environment.Present || s.Phase == environment.Creating || sessions != 0 || idle.IsZero() {
		return time.Time{}, false
	}
	return idle.Add(time.Duration(spec.IdleTimeoutSeconds) * time.Second), true
}

// observe commits pending result p, from Ensure if p.ensure is set and from
// Delete otherwise, and returns the committed row. Native is committed even
// if the phase is refused, and even after a concurrent change of intent; it
// replaces only the ledger's Native. A phase the current state cannot move
// to is kept as it is, and reported as perr after a successful commit. err
// reports that the commit is not known to have happened.
//
// An observed incarnation of a Present environment is committed only while
// this process holds nothing of the committed one's materialization, and
// Ready only once this process staged every artifact of the committed
// incarnation and released its read. A commit the Store refuses with
// store.ErrEnvironmentArtifactHeld, because an artifact read is open, wrote
// nothing; it is made again without the incarnation and without Deleted.
func (c *Controller) observe(id string, p *pending) (e store.Environment, l ledger, perr, err error) {
	obs := p.obs
	if len(obs.Native) > MaxNativeBytes {
		return store.Environment{}, ledger{}, nil, errors.New("runtime native state exceeds limit")
	}
	phase := obs.Phase
	if phase != 0 && !observable(phase, p.ensure) {
		perr = fmt.Errorf("runtime reported phase %v", phase)
		phase = 0
	}
	held := false
	change := func(e store.Environment, l *ledger) (environment.State, string, bool, error) {
		if e.Identity != p.identity {
			return e.State, e.Incarnation, false, errors.New("environment identity changed")
		}
		changed := false
		inc := e.Incarnation
		if p.ensure && !held && obs.Incarnation != "" && obs.Incarnation != inc && e.State.Desired == environment.Present && !c.holdsArtifacts(id) {
			inc = obs.Incarnation
			changed = true
		}
		if obs.Native != nil && !bytes.Equal(obs.Native, l.Native) {
			l.Native = bytes.Clone(obs.Native)
			changed = true
		}
		ph := phase
		if ph == environment.Deleted && (held || c.holding(id) || c.holdsArtifacts(id) || e.State.Phase != environment.Deleting) {
			// Deleted follows only Deleting, and only once this
			// process's frames have settled and no artifact read is
			// open; Delete is called again.
			ph = environment.Deleting
		}
		if ph == environment.Ready && !c.materialized(e, inc) {
			// Ready waits for the committed incarnation's artifacts.
			// A Ready row keeps its phase, but one whose incarnation
			// changes is no longer known to be ready.
			ph = 0
			if inc != e.Incarnation && e.State.Phase == environment.Ready {
				ph = environment.Unknown
			}
		}
		if ph == 0 {
			return e.State, inc, changed, nil
		}
		next := environment.State{Desired: e.State.Desired, Phase: ph}
		if next == e.State || e.State.Next(next) != nil {
			return e.State, inc, changed, nil
		}
		switch ph {
		case environment.Ready:
			// Idle time starts at readiness.
			if now := c.now(); now.After(l.Idle) {
				l.Idle = now
			}
		case environment.Deleted:
			// Every owned resource was observed gone, and with it any
			// command a session started.
			l.Sessions = nil
		}
		return next, inc, true, nil
	}
	e, l, err = c.commitChange(id, change)
	if errors.Is(err, store.ErrEnvironmentArtifactHeld) {
		held = true
		e, l, err = c.commitChange(id, change)
	}
	if err != nil {
		return store.Environment{}, ledger{}, nil, err
	}
	return e, l, perr, nil
}

func observable(p environment.Phase, ensure bool) bool {
	if ensure {
		return p == environment.Creating || p == environment.Ready || p == environment.Unknown
	}
	return p == environment.Deleting || p == environment.Deleted || p == environment.Unknown
}

// recover settles the sessions of previous processes: an exec session's
// outcome becomes unknown and a forward session ends, since its custody
// ended with its process and it started no remote work.
func (c *Controller) recover() error {
	var after uint64
	for {
		p, err := c.store.Environments(after, 200)
		if err != nil {
			return err
		}
		for _, e := range p.Environments {
			if e.Checkpoint.Kind != c.kind || e.State.Phase == environment.Deleted {
				continue
			}
			if _, _, err := c.update(e.Identity.ID, c.orphan); err != nil {
				return fmt.Errorf("recover environment %s: %w", e.Identity.ID, err)
			}
		}
		if !p.More {
			return nil
		}
		after = p.After
	}
}

func (c *Controller) orphan(e store.Environment, l *ledger) (environment.State, bool, error) {
	changed := false
	var kept []session
	for _, s := range l.Sessions {
		switch {
		case s.Owner == c.incarnation:
		case s.Kind == forwardSession:
			changed = true
			continue
		case !s.Unknown:
			s.Unknown = true
			changed = true
		}
		kept = append(kept, s)
	}
	if !changed {
		return e.State, false, nil
	}
	l.Sessions = kept
	if now := c.now(); now.After(l.Idle) {
		l.Idle = now
	}
	return e.State, true, nil
}

// update commits change to the current row of environment id. change edits
// the decoded ledger and returns the next state and whether anything
// changed. A lost revision race reruns change on the new row. A commit that
// fails otherwise may still be durable, so the row is read back and the
// exact commit accepted. update returns the committed row and ledger, or the
// current ones if nothing changed. The committed incarnation is kept.
func (c *Controller) update(id string, change func(store.Environment, *ledger) (environment.State, bool, error)) (store.Environment, ledger, error) {
	return c.commitChange(id, func(e store.Environment, l *ledger) (environment.State, string, bool, error) {
		next, changed, err := change(e, l)
		return next, e.Incarnation, changed, err
	})
}

// commitChange is update for a change that also returns the incarnation to
// commit. Every commit carries the incarnation, the committed one unless
// change returns another.
func (c *Controller) commitChange(id string, change func(store.Environment, *ledger) (environment.State, string, bool, error)) (store.Environment, ledger, error) {
	for range maxAttempts {
		e, err := c.read(id)
		if err != nil {
			return store.Environment{}, ledger{}, err
		}
		l, err := c.decode(e)
		if err != nil {
			return store.Environment{}, ledger{}, err
		}
		next, inc, changed, err := change(e, &l)
		if err != nil {
			return store.Environment{}, ledger{}, err
		}
		if !changed {
			return e, l, nil
		}
		cp, err := c.encode(l)
		if err != nil {
			return store.Environment{}, ledger{}, err
		}
		out, err := c.commit(id, e.Revision, store.EnvironmentCommit{State: next, Checkpoint: cp, Incarnation: inc})
		if err == nil {
			return out, l, nil
		}
		if errors.Is(err, store.ErrEnvironmentStale) {
			continue
		}
		got, rerr := c.read(id)
		if rerr == nil && got.Revision == e.Revision+1 && got.State == next && got.Incarnation == inc && bytes.Equal(got.Checkpoint.Data, cp.Data) {
			return got, l, nil
		}
		return store.Environment{}, ledger{}, err
	}
	return store.Environment{}, ledger{}, errContended
}
