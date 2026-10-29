package gce

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"
)

// A Machine is the sole owner of one locked state directory and of at
// most one original Compute Engine request. It is not safe for concurrent
// use. The zero Machine owns nothing; its methods other than Close
// refuse.
type Machine struct {
	st      *store
	rec     *record
	client  *computeClient
	now     func() time.Time
	cut     func(point string) error // test crash cuts; nil in production
	pending *call                    // the one original not yet recorded, if any

	// Custody of failures. A call or file whose first Close failed is
	// kept here and never closed again; unsaved means rec may differ from
	// the committed state. Each fences m: fault is set and sticky.
	held     *call
	heldFile *closeError
	unsaved  bool
	fault    error

	closed   bool  // Close was called: no new admission
	lockShut bool  // the lock file's first Close has run
	lockErr  error // its result, sticky
}

var (
	errZero    = errors.New("gce: machine owns no state")
	errClosed  = errors.New("gce: machine is closed")
	errPending = errors.New("gce: an original request is still pending; the machine keeps its lock")
)

// Create returns the Machine of a new state directory stateDir for cfg.
// It validates all of cfg first. If stateDir does not exist, Create makes
// it private, syncs its entry in the parent directory, reserves fresh
// random instance and disk names and commits them with the goal Running
// before returning. If stateDir holds an intact state for the same cfg,
// Create opens it as Open does. Any other existing path is refused.
// Create calls no service.
//
// If Create or Open fails after locking the directory and a file's first
// Close failed, the returned error keeps that file and the lock, which
// this process then holds until it exits.
func Create(stateDir string, cfg Config) (*Machine, error) {
	return create(stateDir, cfg, newComputeClient())
}

// Open returns the Machine of the existing state directory stateDir,
// which must have been created with exactly cfg. Open never creates a
// state and calls no service.
func Open(stateDir string, cfg Config) (*Machine, error) {
	return open(stateDir, cfg, newComputeClient())
}

// create and open are Create and Open with the client cl, which only
// tests choose.
func create(stateDir string, cfg Config, cl *computeClient) (*Machine, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("gce: invalid config: %w", err)
	}
	if _, err := os.Lstat(stateDir); err == nil {
		return open(stateDir, cfg, cl)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("gce: state directory: %w", err)
	}
	s, err := createStore(stateDir)
	if err != nil {
		return nil, fmt.Errorf("gce: %w", err)
	}
	rec, err := newRecord(cfg)
	if err == nil {
		err = s.save(rec)
	}
	if err != nil {
		return nil, fmt.Errorf("gce: %w", s.abandon(err))
	}
	return newMachine(s, rec, cl), nil
}

func open(stateDir string, cfg Config, cl *computeClient) (*Machine, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("gce: invalid config: %w", err)
	}
	s, err := openStore(stateDir)
	if err != nil {
		return nil, fmt.Errorf("gce: %w", err)
	}
	rec, err := readState(stateDir)
	if err == nil && rec.ConfigHash != cfg.hash() {
		err = errors.New("config differs from the config the state was created with")
	}
	if err != nil {
		return nil, fmt.Errorf("gce: %w", s.abandon(err))
	}
	return newMachine(s, rec, cl), nil
}

func newMachine(s *store, rec *record, cl *computeClient) *Machine {
	return &Machine{st: s, rec: rec, client: cl, now: time.Now}
}

// usable reports why m may not admit new work.
func (m *Machine) usable() error {
	switch {
	case m == nil || m.st == nil:
		return errZero
	case m.closed:
		return errClosed
	case m.fault != nil:
		return m.fault
	}
	return nil
}

// fence records err as a reason m admits nothing more.
func (m *Machine) fence(err error) error {
	m.fault = errors.Join(m.fault, err)
	return err
}

// A causeError is a failure with one or more original causes: a reply's
// read or classification, a state commit, a body's or file's first Close.
// Its message is fixed and includes none of them, since a cause may carry
// server text, a path or anything its producer chose; errors.Is and
// errors.As reach every cause through Unwrap. If c is set, the error also
// keeps that call, whose body is never closed again.
type causeError struct {
	msg    string
	c      *call
	causes []error
}

func (e *causeError) Error() string   { return e.msg }
func (e *causeError) Unwrap() []error { return e.causes }

// causes returns a causeError with message msg and the non-nil errs.
func causes(msg string, c *call, errs ...error) *causeError {
	e := &causeError{msg: msg, c: c}
	for _, err := range errs {
		if err != nil {
			e.causes = append(e.causes, err)
		}
	}
	return e
}

// save commits m.rec. Any failure fences m and keeps m.rec as it is, as
// possibly committed: it is never rolled back. A failed first Close keeps
// its file. Only Close lifts an unsaved fence, by committing m.rec again;
// until then save writes nothing.
func (m *Machine) save() error {
	if m.unsaved || m.heldFile != nil {
		return m.fault
	}
	err := m.st.save(m.rec)
	if err == nil {
		return nil
	}
	var ce *closeError
	if errors.As(err, &ce) && m.heldFile == nil {
		m.heldFile = ce
	}
	m.unsaved = true
	return m.fence(causes("gce: state commit not confirmed; the machine admits nothing more", nil, err))
}

// keep retains the settled call c, whose first body Close failed, and
// fences m. The returned error has as causes the call's original read
// error, the reply causes given by the caller and the Close failure. c is
// never closed again.
func (m *Machine) keep(c *call, reply ...error) error {
	m.held = c
	errs := append([]error{c.err}, reply...)
	errs = append(errs, c.closeErr)
	return m.fence(causes("gce: "+c.what+": first close of the response body failed; the machine keeps the response and its lock", c, errs...))
}

// SetGoal commits g as the desired state before any effect. Absent is
// latched: once committed, no other goal is accepted. Repeating a goal
// after it was observed reached begins a new intent; repeating an
// unfinished goal continues it, without resending any original. Setting Restarted
// while a restart is unfinished continues it; otherwise it begins a new
// restart. If the commit fails, the new goal is kept as possibly
// committed and m admits nothing more.
func (m *Machine) SetGoal(g Goal) error {
	if err := m.usable(); err != nil {
		return err
	}
	if !g.valid() {
		return fmt.Errorf("gce: unknown goal %q", g)
	}
	r := m.rec
	if r.Desired == Absent && g != Absent {
		return errors.New("gce: goal absent is latched")
	}
	restart := ""
	if g == Restarted {
		restart = "stop"
		if r.Desired == Restarted && r.CompletedIntent != r.Intent {
			restart = r.Restart
		}
	}
	if g == r.Desired && restart == r.Restart && r.CompletedIntent != r.Intent {
		return nil // continue the unfinished intent, including its original
	}
	return m.beginIntent(g, restart)
}

// beginIntent commits the new goal's intent without changing an earlier
// original, which still must settle before new work can be prepared.
func (m *Machine) beginIntent(g Goal, restart string) error {
	r := m.rec
	intent := r.Intent + 1
	if intent == 0 {
		return errors.New("gce: goal intent exhausted")
	}
	r.Intent = intent
	r.Desired, r.Restart, r.Reached, r.Held = g, restart, false, ""
	return m.save()
}

// Close permanently stops m from admitting work. If an original request
// is still pending, Close returns an error and m keeps that request, its
// state and its directory lock; a later Close checks again. Once nothing
// is pending, Close records any settled reply.
//
// If the first Close of a response body or state file failed, m keeps
// that owner and its lock and every Close returns the failure. If a state
// commit was not confirmed, each Close commits the current state again,
// in full, and keeps the lock until one such commit succeeds; it never
// restores an earlier goal. Then Close releases the lock. The lock's
// release runs at most once and its result is returned by every later
// Close. Close of a nil or zero Machine returns nil.
//
// Close is synchronous, like every Machine method. It does not wait for a
// pending original, but recording a settled reply, committing the state
// and releasing the lock run file system calls (write, sync, Close) that
// no context or deadline bounds; Close returns only when they do. A
// caller that must observe a deadline does so around Close and, if the
// deadline passes first, still owns the Machine until Close returns.
func (m *Machine) Close() error {
	if m == nil || m.st == nil {
		return nil
	}
	m.closed = true
	if c := m.pending; c != nil {
		if !c.settled() {
			return errPending
		}
		if _, err := m.absorb(); err != nil && m.held == nil && m.heldFile == nil && !m.unsaved {
			return causes("gce: record settled reply failed", nil, err)
		}
	}
	if m.held != nil || m.heldFile != nil {
		return m.fault
	}
	if m.unsaved {
		if err := m.st.save(m.rec); err != nil {
			var ce *closeError
			if errors.As(err, &ce) {
				m.heldFile = ce
			}
			err = causes("gce: state commit still not confirmed; the machine keeps its lock", nil, err)
			if ce != nil {
				m.fence(err)
			}
			return err
		}
		m.unsaved = false
	}
	if !m.lockShut {
		m.lockShut = true
		if err := closeFile(lockName, m.st.lock); err != nil {
			m.lockErr = fmt.Errorf("gce: release state lock: %w", err)
		}
	}
	return m.lockErr
}
