package leaderlease

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	mrand "math/rand/v2"
	"strings"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// JitterFactor bounds the retry period as client-go's leader election does:
// the renew deadline must exceed JitterFactor times the retry period.
const JitterFactor = 1.2

var (
	// ErrLost reports that the Lease is no longer held: another process
	// holds it, or it was not renewed within the renew deadline.
	ErrLost = errors.New("leaderlease: lease lost")

	// ErrStopped reports that Stop ended the renewal.
	ErrStopped = errors.New("leaderlease: renewal stopped")

	// ErrReleased reports that Release gave the Lease up.
	ErrReleased = errors.New("leaderlease: lease released")
)

// Config selects one Lease and its timing.
type Config struct {
	// Namespace and Name name the Lease.
	Namespace, Name string

	// Identity is this process's holder identity. It must be unique among
	// all processes that may contend; see NewIdentity.
	Identity string

	// LeaseDuration is how long a contender waits after the Lease last
	// changed, by resourceVersion, before taking it over. It is a whole
	// number of seconds, at most math.MaxInt32, the Lease's limit.
	// RenewDeadline is how long the holder keeps trying to renew before
	// the Lease is lost; Valid, Stop and Release use it too. RetryPeriod is
	// the interval between attempts. LeaseDuration > RenewDeadline >
	// JitterFactor * RetryPeriod > 0.
	LeaseDuration, RenewDeadline, RetryPeriod time.Duration
}

func (c Config) validate() error {
	switch {
	case len(validation.IsDNS1123Label(c.Namespace)) != 0:
		return errors.New("leaderlease: invalid namespace")
	case len(validation.IsDNS1123Subdomain(c.Name)) != 0:
		return errors.New("leaderlease: invalid lease name")
	case c.Identity == "" || len(c.Identity) > 253 || strings.ContainsFunc(c.Identity, func(r rune) bool { return r <= ' ' || r == 0x7f }):
		return errors.New("leaderlease: invalid identity")
	case c.RetryPeriod <= 0 || c.RenewDeadline <= 0 || c.LeaseDuration <= 0:
		return errors.New("leaderlease: durations must be positive")
	case c.LeaseDuration%time.Second != 0:
		return errors.New("leaderlease: lease duration must be whole seconds")
	case c.LeaseDuration/time.Second > math.MaxInt32:
		return errors.New("leaderlease: lease duration exceeds the Lease's int32 seconds")
	case c.LeaseDuration <= c.RenewDeadline:
		return errors.New("leaderlease: lease duration must exceed the renew deadline")
	case c.RetryPeriod >= c.RenewDeadline:
		// Checked first so that the jittered product below stays within
		// the range of time.Duration.
		return errors.New("leaderlease: retry period must be less than the renew deadline")
	case c.RenewDeadline <= time.Duration(JitterFactor*float64(c.RetryPeriod)):
		return errors.New("leaderlease: renew deadline must exceed the jittered retry period")
	}
	return nil
}

// NewIdentity returns base followed by an underscore and 16 random hex
// digits, so that two processes with the same base, such as one Pod name
// reused, never share an identity.
func NewIdentity(base string) (string, error) {
	if base == "" || len(base) > 200 || strings.ContainsFunc(base, func(r rune) bool { return r <= ' ' || r == 0x7f }) {
		return "", errors.New("leaderlease: invalid identity base")
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base + "_" + hex.EncodeToString(b[:]), nil
}

// Held is a Lease this process acquired. Its methods are safe for
// concurrent use.
type Held struct {
	cfg  Config
	lock *leaseLock // used only by the renewal goroutine, then by Release

	stop context.CancelFunc // ends the renewal
	done chan struct{}      // closed when the renewal goroutine returned
	lost chan struct{}      // closed once, when the Lease stops being held

	mu          sync.Mutex
	renewed     time.Time // monotonic send time of the last successful write
	acquired    metav1.Time
	transitions int
	err         error // why the Lease is no longer held; nil while held
	released    bool  // Release ran
}

// Acquire blocks until this process holds the Lease named by cfg, creating
// it if it does not exist, or until ctx is done. A Lease held by another
// identity is taken over only after neither its resourceVersion nor its
// spec has changed for the longer of cfg.LeaseDuration and the record's own
// duration, as observed by this process. On ctx's end Acquire returns ctx's
// error joined with the last original API error, if any; a write
// interrupted that way may still have taken the Lease, which then expires
// unrenewed.
//
// The returned Lease is renewed until it is lost, Stop or Release; ctx no
// longer applies.
func Acquire(ctx context.Context, client kubernetes.Interface, cfg Config) (*Held, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	lock := &leaseLock{client: client.CoordinationV1().Leases(cfg.Namespace), namespace: cfg.Namespace, name: cfg.Name}
	return acquire(ctx, lock, cfg)
}

func acquire(ctx context.Context, lock *leaseLock, cfg Config) (*Held, error) {
	h := &Held{cfg: cfg, lock: lock, done: make(chan struct{}), lost: make(chan struct{})}
	var observed string      // the version of the Lease last seen
	var observedAt time.Time // when it was first seen, monotonic
	var last error           // the last original API error
	for {
		ok, err := h.tryAcquire(ctx, &observed, &observedAt)
		if ok {
			break
		}
		if err != nil {
			last = err
		}
		// Wait RetryPeriod, jittered up to JitterFactor as client-go does.
		wait := cfg.RetryPeriod + time.Duration(mrand.Float64()*(JitterFactor-1)*float64(cfg.RetryPeriod))
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, errors.Join(ctx.Err(), last)
		case <-t.C:
		}
	}
	renewCtx, stop := context.WithCancel(context.Background())
	h.stop = stop
	go h.renew(renewCtx)
	return h, nil
}

// tryAcquire makes one acquisition attempt. It reports whether the Lease
// is now held, or the original API error of a failed request.
func (h *Held) tryAcquire(ctx context.Context, observed *string, observedAt *time.Time) (bool, error) {
	id := h.cfg.Identity
	record, version, err := h.lock.get(ctx)
	if apierrors.IsNotFound(err) {
		start := time.Now()
		r := h.record(start)
		if err := h.lock.create(ctx, r); err != nil {
			return false, err
		}
		h.held(start, r)
		return true, nil
	}
	if err != nil {
		return false, err
	}
	now := time.Now()
	if version != *observed {
		*observed, *observedAt = version, now
	}
	if record.HolderIdentity != "" && record.HolderIdentity != id {
		d := max(h.cfg.LeaseDuration, time.Duration(record.LeaseDurationSeconds)*time.Second)
		if now.Sub(*observedAt) < d {
			return false, nil
		}
	}
	start := time.Now()
	r := h.record(start)
	r.LeaderTransitions = record.LeaderTransitions + 1
	if err := h.lock.update(ctx, r); err != nil {
		return false, err
	}
	h.held(start, r)
	return true, nil
}

// record returns a record that names this process as holder, acquired
// and renewed at start.
func (h *Held) record(start time.Time) resourcelock.LeaderElectionRecord {
	t := metav1.NewTime(start)
	return resourcelock.LeaderElectionRecord{HolderIdentity: h.cfg.Identity,
		LeaseDurationSeconds: int(h.cfg.LeaseDuration / time.Second), AcquireTime: t, RenewTime: t}
}

func (h *Held) held(start time.Time, r resourcelock.LeaderElectionRecord) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.renewed, h.acquired, h.transitions = start, r.AcquireTime, r.LeaderTransitions
}

// renew renews the Lease every RetryPeriod. Each round retries until it
// succeeds or the renew deadline after the last successful write passed;
// then, or as soon as another identity holds the Lease, the Lease is lost.
func (h *Held) renew(ctx context.Context) {
	defer close(h.done)
	for {
		t := time.NewTimer(h.cfg.RetryPeriod)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
		var last error
		for {
			h.mu.Lock()
			deadline := h.renewed.Add(h.cfg.RenewDeadline)
			h.mu.Unlock()
			if !time.Now().Before(deadline) {
				h.end(errors.Join(fmt.Errorf("%w: not renewed within %v", ErrLost, h.cfg.RenewDeadline), last))
				return
			}
			attempt, cancel := context.WithDeadline(ctx, deadline)
			err := h.tryRenew(attempt)
			cancel()
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return // Stop or Release
			}
			if errors.Is(err, ErrLost) {
				h.end(err)
				return
			}
			last = err
			t := time.NewTimer(min(h.cfg.RetryPeriod, time.Until(deadline)))
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
		}
	}
}

// tryRenew writes a renewed record. If the write fails, it reads the
// Lease: another holder ends it with ErrLost; otherwise it writes once more.
// It returns the original API errors.
func (h *Held) tryRenew(ctx context.Context) error {
	h.mu.Lock()
	acquired, transitions := h.acquired, h.transitions
	h.mu.Unlock()
	write := func() error {
		start := time.Now()
		r := h.record(start)
		r.AcquireTime, r.LeaderTransitions = acquired, transitions
		if err := h.lock.update(ctx, r); err != nil {
			return err
		}
		h.held(start, r)
		return nil
	}
	first := write()
	if first == nil {
		return nil
	}
	record, _, err := h.lock.get(ctx)
	if err != nil {
		return errors.Join(first, err)
	}
	if record.HolderIdentity != h.cfg.Identity {
		return fmt.Errorf("%w: held by %q", ErrLost, record.HolderIdentity)
	}
	if err := write(); err != nil {
		return errors.Join(first, err)
	}
	return nil
}

// end records why the Lease is no longer held and closes Lost, once.
func (h *Held) end(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.endLocked(err)
}

func (h *Held) endLocked(err error) {
	if h.err == nil {
		h.err = err
		close(h.lost)
	}
}

// settleLocked ends with ErrLost a Lease whose renew deadline passed while
// the renewal goroutine, now joined, was stopped before it noticed. h.mu
// must be held.
func (h *Held) settleLocked() {
	if !time.Now().Before(h.renewed.Add(h.cfg.RenewDeadline)) {
		h.endLocked(fmt.Errorf("%w: not renewed within %v", ErrLost, h.cfg.RenewDeadline))
	}
}

// Lost returns a channel that is closed once, when the Lease stops being
// held: lost, stopped or released. It is never reopened.
func (h *Held) Lost() <-chan struct{} { return h.lost }

// Err returns nil while the Lease is held, and then why it is not: an error
// wrapping ErrLost and the original API errors, ErrStopped or ErrReleased.
func (h *Held) Err() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.err
}

// Valid reports whether the Lease is held and now is less than the renew
// deadline after the send time of its last successful acquisition or
// renewal. now must come from time.Now, so that the comparison uses the
// monotonic clock. A paused process can still act after Valid returned true.
func (h *Held) Valid(now time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.err == nil && now.Sub(h.renewed) < h.cfg.RenewDeadline
}

// Stop ends the renewal and waits for its goroutine to return, without
// writing the Lease, which then expires. Use it when the guarded state was
// not closed successfully. The Lease is no longer held afterward; Err
// reports ErrLost instead of ErrStopped if the renew deadline had passed.
func (h *Held) Stop() {
	h.stop()
	<-h.done
	h.mu.Lock()
	defer h.mu.Unlock()
	h.settleLocked()
	h.endLocked(ErrStopped)
}

// Release ends the renewal, waits for its goroutine to return and, if the
// Lease is still held and valid, writes it as held by no one, so that a
// contender can acquire it at once. Call it only after the guarded state
// was closed successfully. It refuses, without writing, a Lease that is no
// longer held or whose renew deadline passed, which is then lost. It
// returns the original API error of a failed write, in which case the
// Lease expires. Either way the Lease is no longer held afterward.
func (h *Held) Release(ctx context.Context) error {
	h.stop()
	<-h.done
	h.mu.Lock()
	released := h.released
	h.released = true
	if !released {
		h.settleLocked()
	}
	err := h.err
	if !released && err == nil {
		h.endLocked(ErrReleased)
	}
	transitions := h.transitions
	h.mu.Unlock()
	if released {
		return errors.New("leaderlease: already released")
	}
	if err != nil {
		return fmt.Errorf("leaderlease: lease not held: %w", err)
	}
	now := metav1.NewTime(time.Now())
	// The write is conditional on the Lease being unchanged since this
	// process last read or wrote it.
	return h.lock.update(ctx, resourcelock.LeaderElectionRecord{LeaseDurationSeconds: 1,
		AcquireTime: now, RenewTime: now, LeaderTransitions: transitions})
}

// A leaseLock reads and writes one Lease in client-go's leader election
// record format. It keeps the Lease last read or written, so that each
// update is conditional on that resourceVersion.
type leaseLock struct {
	client          coordinationv1client.LeaseInterface
	namespace, name string
	lease           *coordinationv1.Lease
}

// get reads the Lease. It returns its record and its version: the
// resourceVersion and the spec at microsecond precision, which changes
// with every write even when the record's whole-second times do not.
func (l *leaseLock) get(ctx context.Context) (*resourcelock.LeaderElectionRecord, string, error) {
	lease, err := l.client.Get(ctx, l.name, metav1.GetOptions{})
	if err != nil {
		return nil, "", err
	}
	spec, err := json.Marshal(lease.Spec)
	if err != nil {
		return nil, "", err
	}
	l.lease = lease
	return resourcelock.LeaseSpecToLeaderElectionRecord(&lease.Spec), lease.ResourceVersion + "\x00" + string(spec), nil
}

func (l *leaseLock) create(ctx context.Context, r resourcelock.LeaderElectionRecord) error {
	lease, err := l.client.Create(ctx, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Namespace: l.namespace, Name: l.name},
		Spec:       resourcelock.LeaderElectionRecordToLeaseSpec(&r),
	}, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	l.lease = lease
	return nil
}

func (l *leaseLock) update(ctx context.Context, r resourcelock.LeaderElectionRecord) error {
	if l.lease == nil {
		return errors.New("leaderlease: lease not read")
	}
	lease := l.lease.DeepCopy()
	// Keep fields the record does not carry, which other writers may set.
	preferred, strategy := lease.Spec.PreferredHolder, lease.Spec.Strategy
	lease.Spec = resourcelock.LeaderElectionRecordToLeaseSpec(&r)
	lease.Spec.PreferredHolder, lease.Spec.Strategy = preferred, strategy
	lease, err := l.client.Update(ctx, lease, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	l.lease = lease
	return nil
}
