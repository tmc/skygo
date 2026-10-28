package environmentcontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/store"
)

// maxLedgerBytes bounds a ledger's encoding: base64 of MaxNativeBytes, at
// most 1 KiB of fixed fields and at most 256 bytes per session. It fits a
// checkpoint, so no admitted session or committed Native can be refused for
// size; the declaration below fails to compile otherwise.
const maxLedgerBytes = 4*((MaxNativeBytes+2)/3) + 1<<10 + MaxSessions*256

const _ = uint(environment.MaxCheckpointBytes - maxLedgerBytes)

// ledger is the Controller's state of one environment, kept as the data of
// its checkpoint and changed only by revision compare-and-swap. Nonce is
// fixed at acceptance. Native is the runtime's state. Idle is the start of
// the current idle period. Expired records that idle expiry latched Absent.
// Sessions are the admitted sessions, open or unknown.
type ledger struct {
	Nonce    string    `json:"nonce"`
	Native   []byte    `json:"native,omitempty"`
	Idle     time.Time `json:"idle"`
	Expired  bool      `json:"expired,omitempty"`
	Sessions []session `json:"sessions,omitempty"`
}

type sessionKind string

const (
	execSession    sessionKind = "exec"
	forwardSession sessionKind = "forward"
)

// session is one admitted session. Owner is the incarnation of the process
// that admitted it; only that process ends it. Unknown records an exec whose
// outcome is unknown; it ends only when its environment is Deleted.
type session struct {
	ID      string      `json:"id"`
	Kind    sessionKind `json:"kind"`
	Owner   string      `json:"owner"`
	Started time.Time   `json:"started"`
	Unknown bool        `json:"unknown,omitempty"`
}

func (l ledger) check() error {
	switch {
	case !environment.ValidID(l.Nonce):
		return errors.New("invalid environment nonce")
	case len(l.Native) > MaxNativeBytes:
		return errors.New("runtime native state exceeds limit")
	case l.Idle.IsZero() || l.Idle.Location() != time.UTC:
		return errors.New("invalid environment idle time")
	case len(l.Sessions) > MaxSessions:
		return errors.New("environment ledger exceeds session limit")
	}
	for i, s := range l.Sessions {
		switch {
		case !environment.ValidID(s.ID) || !environment.ValidID(s.Owner):
			return errors.New("invalid environment session id")
		case s.Kind != execSession && s.Kind != forwardSession:
			return errors.New("invalid environment session kind")
		case s.Kind == forwardSession && s.Unknown:
			return errors.New("forward session cannot be unknown")
		case s.Started.IsZero() || s.Started.Location() != time.UTC:
			return errors.New("invalid environment session start")
		}
		for _, t := range l.Sessions[:i] {
			if t.ID == s.ID {
				return errors.New("duplicate environment session")
			}
		}
	}
	return nil
}

// encodeLedger checks l and returns its only encoding.
func encodeLedger(l ledger) ([]byte, error) {
	if err := l.check(); err != nil {
		return nil, err
	}
	data, err := json.Marshal(l)
	if err != nil {
		return nil, fmt.Errorf("encode environment ledger: %w", err)
	}
	if len(data) > maxLedgerBytes {
		return nil, errors.New("environment ledger exceeds its bound")
	}
	return data, nil
}

// decodeLedger accepts only bytes that encodeLedger produces.
func decodeLedger(data []byte) (ledger, error) {
	var l ledger
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&l); err != nil {
		return ledger{}, fmt.Errorf("decode environment ledger: %w", err)
	}
	if d.Decode(new(any)) != io.EOF {
		return ledger{}, errors.New("decode environment ledger: trailing data")
	}
	want, err := encodeLedger(l)
	if err != nil {
		return ledger{}, err
	}
	if !bytes.Equal(want, data) {
		return ledger{}, errors.New("environment ledger is not canonical")
	}
	return l, nil
}

// admit durably records a new session of kind in environment id for actor,
// which must own it, and returns the environment as the runtime sees it and
// the session's id. The environment must be Present, unexpired and Ready
// when the session commits; the same revision orders admission against
// deletion and expiry.
//
// Admission holds the lifecycle gate from its flush through the session's
// commit, as a Reconcile pass holds it from each runtime call through that
// result's commit. So no pending result is committed after a newer one, and
// no session is admitted between a runtime call's return and its result's
// commit. It returns, releasing the gate, before any runtime call or frame
// wait of the session. Deletion takes no gate and stays latchable meanwhile.
//
// Admission stops waiting for the gate when ctx ends, and admits nothing
// for a caller whose ctx has ended. Once it holds the gate it checks actor's
// current authority again, so that a role, credential or availability
// change made while it waited refuses the session, with the Engine's own
// error. Exec and Forward check it once more, with startable, after the
// session's commit.
func (c *Controller) admit(ctx context.Context, actor auth.Actor, id string, kind sessionKind) (Ref, string, error) {
	actor, err := c.auth.Check(actor, auth.Mutate)
	if err != nil {
		return Ref{}, "", err
	}
	if !environment.ValidID(id) {
		return Ref{}, "", fmt.Errorf("%w: invalid environment id", ErrInvalid)
	}
	e, err := c.store.EnvironmentFor(actor, id)
	if err != nil {
		return Ref{}, "", err
	}
	// Sessions run as the owner; no role reaches another principal's
	// resources.
	if e.Principal != actor.Principal() {
		return Ref{}, "", store.ErrNotFound
	}
	if c.beforeGate != nil {
		c.beforeGate()
	}
	if err := c.lock(ctx); err != nil {
		return Ref{}, "", err
	}
	defer c.unlock()
	// A session sees only committed runtime state.
	if err := c.flush(id); err != nil {
		return Ref{}, "", err
	}
	// Authority and the caller may have changed while admission waited.
	if _, err := c.auth.Check(actor, auth.Mutate); err != nil {
		return Ref{}, "", err
	}
	if err := ctx.Err(); err != nil {
		return Ref{}, "", err
	}
	// Sessions stopped while admission waited commit nothing.
	if c.sessionsStopping() {
		return Ref{}, "", errSessionsStopped
	}
	sid, err := newID()
	if err != nil {
		return Ref{}, "", err
	}
	s := session{ID: sid, Kind: kind, Owner: c.incarnation, Started: c.now()}
	e, l, err := c.update(id, func(e store.Environment, l *ledger) (environment.State, bool, error) {
		switch {
		case e.State.Desired != environment.Present || l.Expired:
			return e.State, false, ErrClosed
		case e.State.Phase != environment.Ready:
			return e.State, false, ErrNotReady
		case kind == forwardSession && e.Spec.Port == 0:
			return e.State, false, ErrNoPort
		case len(l.Sessions) >= MaxSessions:
			return e.State, false, ErrSessionLimit
		}
		l.Sessions = append(l.Sessions, s)
		return e.State, true, nil
	})
	if err != nil {
		return Ref{}, "", err
	}
	return c.ref(e, l), sid, nil
}

// release ends this process's session sid of environment id. The idle
// period restarts.
func (c *Controller) release(id, sid string) error {
	_, _, err := c.update(id, func(e store.Environment, l *ledger) (environment.State, bool, error) {
		i := slices.IndexFunc(l.Sessions, func(s session) bool { return s.ID == sid && s.Owner == c.incarnation })
		if i < 0 {
			return e.State, false, nil
		}
		l.Sessions = slices.Delete(l.Sessions, i, i+1)
		if now := c.now(); now.After(l.Idle) {
			l.Idle = now
		}
		return e.State, true, nil
	})
	return err
}

// forget records that the outcome of this process's exec session sid is
// unknown.
func (c *Controller) forget(id, sid string) error {
	_, _, err := c.update(id, func(e store.Environment, l *ledger) (environment.State, bool, error) {
		i := slices.IndexFunc(l.Sessions, func(s session) bool { return s.ID == sid && s.Owner == c.incarnation })
		if i < 0 || l.Sessions[i].Unknown {
			return e.State, false, nil
		}
		l.Sessions[i].Unknown = true
		return e.State, true, nil
	})
	return err
}

// track records the cancel function of a session's frame, so that deletion
// and stopSessions can close it. If the environment's sessions were already
// closed, or the Controller's sessions stopped, it calls cancel and returns
// ErrClosed or errSessionsStopped; the check and the record are one step
// under c.mu, so a frame is either recorded before stopSessions collects the
// frames or canceled here.
func (c *Controller) track(id, sid string, cancel func()) error {
	c.mu.Lock()
	var err error
	switch {
	case c.sessionsStopped:
		err = errSessionsStopped
	case c.closed[id]:
		err = ErrClosed
	}
	if err != nil {
		c.mu.Unlock()
		cancel()
		return err
	}
	if c.frames[id] == nil {
		c.frames[id] = map[string]func(){}
	}
	c.frames[id][sid] = cancel
	c.mu.Unlock()
	return nil
}

// enterSession counts a new session owner, before the session's first
// authentication, Store or runtime call, unless sessions were stopped. The
// owner, or the settlement it hands its ownership to, calls leaveSession
// after its last Store operation.
func (c *Controller) enterSession() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sessionsStopped {
		return errSessionsStopped
	}
	c.owners++
	return nil
}

// leaveSession ends one session owner.
func (c *Controller) leaveSession() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.owners--
	c.sessionsIdleLocked()
}

// sessionsIdleLocked closes c.sessionsIdle once sessions are stopped and no
// owner remains. c.mu is held.
func (c *Controller) sessionsIdleLocked() {
	if !c.sessionsStopped || c.owners != 0 {
		return
	}
	select {
	case <-c.sessionsIdle:
	default:
		close(c.sessionsIdle)
	}
}

// sessionsStopping reports whether stopSessions was called.
func (c *Controller) sessionsStopping() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessionsStopped
}

// stopSessions refuses every new session owner and frame and cancels every
// tracked frame, of every environment. It returns at once: an owner already
// counted keeps its session, finishes or refuses it, and settles it in the
// Store before it leaves. A canceled frame is not a settled one. It is safe
// to call more than once.
func (c *Controller) stopSessions() {
	c.mu.Lock()
	c.sessionsStopped = true
	var cancels []func()
	for _, frames := range c.frames {
		for _, cancel := range frames {
			cancels = append(cancels, cancel)
		}
	}
	c.sessionsIdleLocked()
	c.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

// joinSessions calls stopSessions and waits until every session owner has
// left, after its last Store operation, or ctx ends. It returns nil only if
// none remains.
func (c *Controller) joinSessions(ctx context.Context) error {
	c.stopSessions()
	select {
	case <-c.sessionsIdle:
		return nil
	case <-ctx.Done():
	}
	c.mu.Lock()
	n := c.owners
	c.mu.Unlock()
	if n == 0 {
		return nil
	}
	return errors.Join(fmt.Errorf("%d environment session owners unsettled", n), ctx.Err())
}

// untrack forgets a session's frame once it has settled.
func (c *Controller) untrack(id, sid string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.frames[id], sid)
	if len(c.frames[id]) == 0 {
		delete(c.frames, id)
	}
}

// holding reports whether this process has an unsettled frame of
// environment id.
func (c *Controller) holding(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.frames[id]) != 0
}

// closeSessions cancels every frame of environment id in this process, and
// any tracked later. It waits for none of them.
func (c *Controller) closeSessions(id string) {
	c.mu.Lock()
	c.closed[id] = true
	var cancels []func()
	for _, cancel := range c.frames[id] {
		cancels = append(cancels, cancel)
	}
	c.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

// Exec runs one command in an environment that actor owns, in a session
// durably admitted before the runtime is called. The command runs until it
// exits, ctx ends, MaxExecTime passes or the environment is deleted; ending
// it cancels only this session's frame. A command that did not report an
// exit, and was not refused, leaves its session recorded as unknown: it
// blocks idle expiry until the environment is deleted. Exec returns an error
// only if no command was started.
//
// Each Exec is a session owner from entry until its last Store operation; a
// frame that outlives the call hands that ownership to its settlement. Once
// Integration.Close stops sessions, Exec refuses at entry, before any
// authentication, Store or runtime call.
func (c *Controller) Exec(ctx context.Context, actor auth.Actor, id string, req ExecRequest) (ExecResult, error) {
	if err := checkExec(req); err != nil {
		return ExecResult{}, err
	}
	if err := c.enterSession(); err != nil {
		return ExecResult{}, err
	}
	owned := true
	defer func() {
		if owned {
			c.leaveSession()
		}
	}()
	r, sid, err := c.admit(ctx, actor, id, execSession)
	if err != nil {
		return ExecResult{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, MaxExecTime)
	defer cancel()
	if err := c.track(id, sid, cancel); err != nil {
		return ExecResult{}, errors.Join(err, c.release(id, sid))
	}
	// Runtime.Exec takes no context: a caller that left, or lost its
	// authority, after admission starts no command, and its session ends
	// unstarted.
	if err := c.startable(ctx, actor); err != nil {
		c.untrack(id, sid)
		return ExecResult{}, errors.Join(err, c.release(id, sid))
	}
	x, err := c.runtime.Exec(r, ExecRequest{Argv: slices.Clone(req.Argv), Stdin: bytes.Clone(req.Stdin)})
	if err != nil {
		c.untrack(id, sid)
		return ExecResult{}, errors.Join(err, c.release(id, sid))
	}
	// From now on deletion and stopSessions also cancel the frame itself,
	// which may outlive this call. If either already happened, track
	// cancels it at once.
	_ = c.track(id, sid, func() { cancel(); x.Cancel() })
	select {
	case <-x.Done():
	case <-ctx.Done():
		x.Cancel()
	}
	res := x.Result()
	if !res.Exited && !res.Refused {
		if err := c.forget(id, sid); err != nil {
			res.Err = errors.Join(res.Err, err)
		}
	}
	if !res.Settled {
		// The settlement owns the session until its last Store operation.
		owned = false
		go func() {
			defer c.leaveSession()
			<-x.Done()
			_ = c.settleExec(id, sid, x.Result())
		}()
		return res, nil
	}
	if err := c.settleExec(id, sid, res); err != nil {
		res.Err = errors.Join(res.Err, err)
	}
	return res, nil
}

// startable reports why a session admitted for actor must not start: its
// caller's ctx has ended, or actor no longer has Mutate authority. It is the
// last check before each runtime call of a session, after admission's
// metadata I/O, which may wait, retry and read back. A change after it is
// not seen by that call.
func (c *Controller) startable(ctx context.Context, actor auth.Actor) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := c.auth.Check(actor, auth.Mutate)
	return err
}

// settleExec ends a settled exec frame's session, if its command exited or
// never started.
func (c *Controller) settleExec(id, sid string, res ExecResult) error {
	c.untrack(id, sid)
	if res.Exited || res.Refused {
		return c.release(id, sid)
	}
	return c.forget(id, sid)
}

func checkExec(req ExecRequest) error {
	if len(req.Argv) == 0 {
		return fmt.Errorf("%w: exec requires argv", ErrInvalid)
	}
	n := 0
	for _, a := range req.Argv {
		if strings.IndexByte(a, 0) >= 0 {
			return fmt.Errorf("%w: exec argv contains NUL", ErrInvalid)
		}
		n += len(a)
	}
	if n > MaxArgvBytes {
		return fmt.Errorf("%w: exec argv exceeds %d bytes", ErrInvalid, MaxArgvBytes)
	}
	if len(req.Stdin) > MaxStdinBytes {
		return fmt.Errorf("%w: exec stdin exceeds %d bytes", ErrInvalid, MaxStdinBytes)
	}
	return nil
}
