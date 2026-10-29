package ec2ctl

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/tmc/skygo/integrations/ec2/internal/ec2api"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2vm"
)

var (
	// ErrNoState reports a state directory or state file that does not
	// exist.
	ErrNoState = errors.New("no state")

	// ErrBusy reports a state directory locked by another process.
	ErrBusy = errors.New("state is locked by another process")

	// ErrMismatch reports a state that does not belong to the
	// configuration: another profile, identity or user data.
	ErrMismatch = errors.New("state does not match the configuration")

	// ErrLatched reports a goal other than absent for a VM that wants
	// absent.
	ErrLatched = errors.New("the VM wants absent; that is latched")

	// ErrNotReached reports a Drive that stopped before its goal: the
	// record is held, or the call bound was reached.
	ErrNotReached = errors.New("the VM did not reach its goal")

	// ErrUnpersisted reports a state that may not be durably stored. The
	// Machine makes no further EC2 call until it is.
	ErrUnpersisted = errors.New("state not durably stored")
)

// A Goal is what the operator wants of the VM.
type Goal int

const (
	GoalRunning   Goal = iota + 1 // created if needed, and running
	GoalStopped                   // stopped
	GoalRestarted                 // stopped, then running again
	GoalAbsent                    // terminated; latched
)

// Limits bound one Drive.
type Limits struct {
	Steps    int           // most EC2 lifecycle steps; must be positive
	Interval time.Duration // wait before each observation after the first step
}

// A Machine is the persisted lifecycle of one VM, opened for writing. It
// holds the state directory's lock until Close.
//
// A Machine's methods must not be called concurrently. Close must wait for
// a Drive to return, even after its context is canceled. After Close, or on
// a nil or zero Machine, SetGoal, Drive and Close refuse with [os.ErrClosed]
// before any store or EC2 call: the lock may belong to another owner.
// Status of a closed Machine still returns its last state.
type Machine struct {
	s       *store // nil once closed
	st      state
	pending bool // st may not be durably stored

	closeErr error // the first Close's failure, which keeps the store
}

// Create opens the state directory dir for cfg, creating it and storing
// the record of a new VM, wanting it present and with nothing sent, if dir
// does not exist. If dir holds a state, Create opens it as Open does.
//
// Create refuses an existing directory that holds no state, because it
// cannot tell a directory whose first save failed from one whose state
// was deleted, and a new record could launch a second VM. That includes a
// directory Create made whose parent it then failed to sync: Create never
// deletes a directory, and such a directory is the operator's to remove.
//
// Create checks cfg before it touches the file system.
func Create(dir string, cfg *Config) (*Machine, error) {
	return createMachine(dir, cfg, syncDir)
}

// createMachine is Create, syncing the state directory's parent with
// syncParent.
func createMachine(dir string, cfg *Config, syncParent func(*os.File) error) (*Machine, error) {
	if err := checkConfig(cfg); err != nil {
		return nil, err
	}
	s, created, err := openStore(dir, true, syncParent)
	if err != nil {
		return nil, err
	}
	m, err := open(s, cfg)
	switch {
	case err == nil:
		return m, nil
	case !errors.Is(err, ErrNoState):
		return nil, errors.Join(err, s.close())
	case !created:
		return nil, errors.Join(fmt.Errorf("%w: directory %s exists without a state file; refusing to start a new record in it", ErrNoState, s.dir), s.close())
	}
	rec, err := ec2vm.NewRecord(&cfg.Profile, cfg.Identity, cfg.UserData)
	if err != nil {
		return nil, errors.Join(err, s.close())
	}
	m = &Machine{s: s}
	if err := m.commit(state{profile: cfg.Profile, rec: rec}); err != nil {
		return nil, errors.Join(err, s.close())
	}
	return m, nil
}

// Open opens the existing state directory dir for cfg. It refuses a state
// of another format, a corrupt state, or one whose profile, identity or
// create request differs from cfg's (ErrMismatch). It checks cfg before
// it touches the file system.
func Open(dir string, cfg *Config) (*Machine, error) {
	return openMachine(dir, cfg, syncDir)
}

// openMachine is Open, syncing the state directory's parent with
// syncParent.
func openMachine(dir string, cfg *Config, syncParent func(*os.File) error) (*Machine, error) {
	if err := checkConfig(cfg); err != nil {
		return nil, err
	}
	s, _, err := openStore(dir, false, syncParent)
	if err != nil {
		return nil, err
	}
	m, err := open(s, cfg)
	if err != nil {
		return nil, errors.Join(err, s.close())
	}
	return m, nil
}

func open(s *store, cfg *Config) (*Machine, error) {
	data, err := s.load()
	if err != nil {
		return nil, err
	}
	st, err := decodeState(data)
	if err != nil {
		return nil, err
	}
	st, err = st.verify(cfg)
	if err != nil {
		return nil, err
	}
	return &Machine{s: s, st: st}, nil
}

// Close releases the state directory's lock. A state that was not durably
// stored is lost; the durable state is the last one stored. Close
// invalidates m even if releasing reports an error; a second Close is
// [os.ErrClosed]. If releasing fails, the error wraps [ErrUnsettled] and
// keeps the original lock and directory, which are never closed again;
// m keeps that error too. A failed first Close of a temporary state file
// instead keeps that file and the lock, without closing anything else.
func (m *Machine) Close() error {
	if m.closed() {
		return os.ErrClosed
	}
	s := m.s
	m.s = nil
	m.closeErr = s.close()
	return m.closeErr
}

// closed reports whether m no longer holds the lock.
func (m *Machine) closed() bool {
	return m == nil || m.s == nil
}

// commit stores st and makes it the machine's state. If the store fails,
// st is kept pending: it may or may not be durable.
func (m *Machine) commit(st state) error {
	m.st = st
	data, err := encodeState(st)
	if err == nil {
		err = m.s.save(data)
	}
	m.pending = err != nil
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnpersisted, err)
	}
	return nil
}

// flush stores a pending state.
func (m *Machine) flush() error {
	if !m.pending {
		return nil
	}
	return m.commit(m.st)
}

// SetGoal durably stores the record's wish for g before any EC2 call acts
// on it. A VM that wants absent refuses every other goal (ErrLatched).
// GoalStopped needs a create to have been sent. GoalRestarted needs a
// running VM, or resumes a restart already stored.
func (m *Machine) SetGoal(g Goal) error {
	if m.closed() {
		return os.ErrClosed
	}
	if err := m.flush(); err != nil {
		return err
	}
	r := m.st.rec
	if r.Desired == ec2vm.DesiredAbsent && g != GoalAbsent {
		return ErrLatched
	}
	next := m.st
	var err error
	switch g {
	case GoalRunning:
		next.restart = false
		next.rec, err = r.WithDesired(ec2vm.DesiredPresent)
	case GoalStopped:
		if !r.CreateSent {
			return errors.New("ec2ctl: no create was sent; there is nothing to stop")
		}
		next.restart = false
		next.rec, err = r.WithDesired(ec2vm.DesiredStopped)
	case GoalRestarted:
		if m.st.restart {
			return nil // resume the stored restart
		}
		if r.Desired != ec2vm.DesiredPresent || r.Phase != ec2vm.PhaseRunning {
			return fmt.Errorf("ec2ctl: restart needs a running VM; it wants %v and is %v", r.Desired, r.Phase)
		}
		next.restart = true
		next.rec, err = r.WithDesired(ec2vm.DesiredStopped)
	case GoalAbsent:
		next.restart = false
		next.rec, err = r.WithDesired(ec2vm.DesiredAbsent)
	default:
		return fmt.Errorf("ec2ctl: invalid goal %d", int(g))
	}
	if err != nil {
		return err
	}
	if next.restart == m.st.restart && next.rec.Desired == r.Desired {
		return nil
	}
	return m.commit(next)
}

// Drive makes the EC2 calls that move the VM toward its goal, one
// ec2vm.Step at a time, until the goal is reached, the record is held, a
// call fails, ctx is done or lim.Steps steps were made. A held record or
// the step bound is ErrNotReached, wrapping the failed call's error if
// one held the record.
//
// Every record a Step returns, error or not, is durably stored before the
// next Step; a create is committed as sent before it is sent. If a state
// cannot be stored, Drive returns ErrUnpersisted and makes no further
// call; the next Drive or SetGoal stores it first. A failed mutation is
// never retried by Drive: running the command again is the operator's
// decision. If the state's first file Close failed, every later save and
// Close also reports ErrUnsettled and keeps the original file and lock.
// A failed read that is retryable is repeated within the bound.
// A read that observes nothing is repeated only while the stored record
// still names a read as its next step: a description by instance ID that
// finds nothing leaves the phase unknown and is repeated, but a ClientToken
// search that finds nothing holds the record (ec2vm.HoldNotObserved). A
// record so held that wants present or stopped makes no further call and is
// ErrNotReached; it is never given a new token or a reissued create. Only
// a record that wants absent goes on searching by its stored ClientToken,
// so that it can terminate what it finds.
//
// The second half of a restart, wanting present again, is stored as soon
// as a Step observes the VM stopped, and before the start is sent.
//
// A nil c makes no call: Drive then succeeds only if no call is needed.
func (m *Machine) Drive(ctx context.Context, c *ec2api.Client, lim Limits) (Status, error) {
	if m.closed() {
		return Status{}, os.ErrClosed
	}
	if lim.Steps <= 0 || lim.Interval < 0 {
		return m.Status(), errors.New("ec2ctl: invalid limits")
	}
	if err := m.flush(); err != nil {
		return m.Status(), err
	}
	opt := ec2vm.Options{
		Commit: func(ctx context.Context, r ec2vm.Record) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			next := m.st
			next.rec = r
			return m.commit(next)
		},
	}
	for n := 0; ; {
		if m.st.restart && m.st.rec.Phase == ec2vm.PhaseStopped {
			next := m.st
			next.restart = false
			r, err := next.rec.WithDesired(ec2vm.DesiredPresent)
			if err != nil {
				return m.Status(), err
			}
			next.rec = r
			if err := m.commit(next); err != nil {
				return m.Status(), err
			}
		}
		a := ec2vm.Next(m.st.rec, opt)
		if a == ec2vm.ActionNone {
			break
		}
		if n == lim.Steps {
			return m.Status(), fmt.Errorf("%w: %d steps made; next is %v", ErrNotReached, n, a)
		}
		if c == nil {
			return m.Status(), fmt.Errorf("ec2ctl: no client for the next step, %v", a)
		}
		if n > 0 && observes(a) {
			if err := wait(ctx, lim.Interval); err != nil {
				return m.Status(), err
			}
		}
		n++
		r, err := ec2vm.Step(ctx, c, m.st.rec, opt)
		next := m.st
		next.rec = r
		if cerr := m.commit(next); cerr != nil {
			return m.Status(), errors.Join(err, cerr)
		}
		if err != nil {
			if observes(a) && repeatable(err) && ctx.Err() == nil {
				continue
			}
			if r.Phase == ec2vm.PhaseHeld && ec2vm.Next(r, opt) == ec2vm.ActionNone {
				return m.Status(), fmt.Errorf("%w: held (%v): %v: %w", ErrNotReached, r.Hold, a, err)
			}
			return m.Status(), fmt.Errorf("%v: %w", a, err)
		}
	}
	st := m.Status()
	if !st.Reached() {
		return st, fmt.Errorf("%w: wants %v, is %v (hold %v)", ErrNotReached, st.Record.Desired, st.Record.Phase, st.Record.Hold)
	}
	return st, nil
}

// observes reports whether action a only reads.
func observes(a ec2vm.Action) bool {
	return a == ec2vm.ActionRecover || a == ec2vm.ActionVerifyCredits || a == ec2vm.ActionObserve
}

// repeatable reports whether a failed read may be repeated: EC2 throttled
// it, failed with a server error or the connection failed, or a read
// observed nothing yet. Drive repeats it only if ec2vm.Next still names a
// read for the stored record.
func repeatable(err error) bool {
	var e *ec2api.Error
	return errors.As(err, &e) && e.Retryable() || errors.Is(err, ec2api.ErrNotObserved)
}

func wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// A Status is a VM's record as this package last stored it, or as it
// read it back.
type Status struct {
	Record  ec2vm.Record
	Restart bool         // the record wants stopped as the first half of a restart
	Next    ec2vm.Action // the step a Drive would make next
	Pending bool         // Record may not be durably stored
}

// Status returns m's current status.
func (m *Machine) Status() Status {
	return newStatus(m.st, m.pending)
}

func newStatus(st state, pending bool) Status {
	r := st.rec
	r.Request = slices.Clone(r.Request)
	r.Reported = slices.Clone(r.Reported)
	return Status{Record: r, Restart: st.restart, Next: ec2vm.Next(r, ec2vm.Options{}), Pending: pending}
}

// Reached reports whether the record, durably stored, shows its goal:
// running for present, stopped for stopped, and for absent terminated or
// never sent. It is persisted evidence, not a fresh observation.
func (s Status) Reached() bool {
	r := s.Record
	if s.Pending || s.Restart {
		return false
	}
	switch r.Desired {
	case ec2vm.DesiredPresent:
		return r.Phase == ec2vm.PhaseRunning
	case ec2vm.DesiredStopped:
		return r.Phase == ec2vm.PhaseStopped
	case ec2vm.DesiredAbsent:
		return r.Phase == ec2vm.PhaseTerminated || !r.CreateSent
	}
	return false
}

// ReadStatus reads the committed state of directory dir, without taking
// its lock, and checks it against cfg as Open does.
func ReadStatus(dir string, cfg *Config) (Status, error) {
	if err := checkConfig(cfg); err != nil {
		return Status{}, err
	}
	data, err := readState(dir)
	if err != nil {
		return Status{}, err
	}
	st, err := decodeState(data)
	if err != nil {
		return Status{}, err
	}
	st, err = st.verify(cfg)
	if err != nil {
		return Status{}, err
	}
	return newStatus(st, false), nil
}
