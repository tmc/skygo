package environmentcontrol

import (
	"context"
	"errors"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/store"
)

// ErrIncarnation reports a Materialize or Bootstrap that the runtime refused
// before it sent anything: the target instance is not the committed
// incarnation, has already bootstrapped, or, for Materialize, can no longer
// stage, or, for Bootstrap, can no longer record it. Nothing was started and
// no byte was read.
var ErrIncarnation = errors.New("environment incarnation differs")

// A Materializer is a Runtime that stages environment artifacts. It is
// optional: a Controller accepts a spec with artifacts only if its Runtime is
// a Materializer and an ArtifactOpener is bound. Otherwise Create refuses the
// spec with ErrUnsupported before Plan or any other effect, and a Runtime that
// is not a Materializer behaves exactly as before.
//
// A Materializer's Ensure reports in Observation.Incarnation the instance it
// positively observed. Its instance does not bootstrap, that is, start the
// environment's workload, until a Bootstrap of that same instance has
// recorded its release, which the Controller calls only after every artifact
// of r.Spec.Artifacts was staged in that instance by a Materialize whose
// result was success. Complete files, an idle staging lock or an ended
// Materialize never let it bootstrap. The instance itself enforces that
// barrier; a check made by a client before it starts the instance is not
// one. Ensure reports Ready only for an instance that has bootstrapped so.
type Materializer interface {
	Runtime

	// Materialize starts staging one artifact into the instance named
	// incarnation, at the MountPath of every mount in mounts. mounts are
	// all of the spec's mounts of that one artifact, in spec order: one or
	// more, with distinct MountPaths and the same ID, SHA256 and Size.
	// incarnation is the committed identity an earlier Ensure observed.
	//
	// body yields the artifact's bytes once, in order: exactly Size bytes,
	// then io.EOF. The runtime reads it at most once and stages every
	// mount from that one pass, for example by writing one destination
	// and copying it within the instance to the others; it never needs the
	// bytes again. Every staging effect is fenced to the instance named
	// incarnation, not to a name that may now resolve to another one.
	// Before it sends anything the runtime checks, as far as it can
	// observe, that the instance is that incarnation and can still stage,
	// that is, has not bootstrapped and its staging has not ended; if not,
	// it starts nothing and the error wraps ErrIncarnation. The instance
	// checks again, and also that its release is not recorded, before any
	// effect; a refusal there, after the request was sent, is not
	// ErrIncarnation but a settled failure, whose result is unknown.
	//
	// A non-nil error means that body was not read and nothing was
	// started. Otherwise the returned Materialization is the only record
	// of the call. Its Done is closed once every goroutine and connection
	// operation that reads body or writes the artifact has returned; after
	// Done the runtime never reads body and does not retain it. Cancel
	// stops only this call's operations and returns at once; it is not a
	// join. Result is Settled with a nil Err only if, when Done closed,
	// every mount's destination held exactly Size bytes with SHA256,
	// read-only, and the instance had not bootstrapped. Any other result,
	// including an error after an effect began, is unknown and never a
	// partial success. A write of a call whose result is unknown may still
	// land later, until the instance records its release.
	Materialize(r Ref, incarnation string, mounts []environment.ArtifactMount, body io.Reader) (Materialization, error)

	// Bootstrap records, in the instance named incarnation, the release of
	// its bootstrap barrier; the instance may then start its workload.
	// Only the Controller that owns the environment calls it, and only
	// after every artifact of r.Spec.Artifacts was staged in that
	// instance by a Materialize whose Done closed with a Settled result
	// with a nil Err, whose body was then closed and whose read was
	// released, and while no Materialize of that incarnation is unsettled.
	//
	// In that same instance, exclusive of every Materialize write, the
	// runtime checks that it is that incarnation and has not
	// bootstrapped, and that every artifact's destinations hold exactly
	// its Size bytes with its SHA256, read-only. It then durably records
	// the release; from then on the instance refuses every Materialize
	// write. If a check fails it records nothing. While the instance can
	// still record a release, a Bootstrap of an instance whose release is
	// already recorded for the same artifacts succeeds again, so a
	// Bootstrap of unknown result may be repeated.
	//
	// A non-nil error means nothing was sent; it wraps ErrIncarnation if
	// the instance is not that incarnation or can no longer record a
	// release, for example because its staging has ended or it has
	// bootstrapped. Otherwise the returned Materialization is the only
	// record of the call, with Done and Cancel as for Materialize; it
	// reads no body. Its Result is Settled with a nil Err only if the
	// runtime received, before Done closed, the instance's
	// acknowledgement that the release is recorded. Any other result is
	// unknown: the release may have been recorded, and the instance may
	// bootstrap. A later Ensure that reports Ready, or a workload seen
	// running, is not such an acknowledgement. After a Bootstrap of
	// unknown result, ErrIncarnation does not mean that the release was
	// not recorded.
	Bootstrap(r Ref, incarnation string) (Materialization, error)
}

// Materialization is the frame of one started Materialize or Bootstrap.
// Done, Cancel and Result are as for Exec.
type Materialization interface {
	Done() <-chan struct{}
	Cancel()
	Result() MaterializationResult
}

// MaterializationResult is a snapshot of a Materialization frame. Settled
// reports that Done is closed. A failure stays in Err once Settled.
type MaterializationResult struct {
	Settled bool
	Err     error
}

// ArtifactOpener opens the bytes of environment artifact reads the Store
// admitted. It is implemented by the owner of the artifact Files root,
// *control.Control, never by a Runtime.
type ArtifactOpener interface {
	// ArtifactProducer is the server lifetime the opener's reads are
	// admitted under. The Controller begins every read with it.
	ArtifactProducer() store.OriginalServerLifetime

	// OpenEnvironmentArtifact reserves one of the opener's bounded I/O
	// permits, waiting until ctx ends while all are held, then opens the
	// lease's exact file and verifies all of its bytes: bounded
	// verification reads, under the admitted lease and the permit, before
	// it returns. The body yields the bytes once, in order: exactly
	// Reference.Size bytes, then io.EOF. Its Close returns after any Read
	// has returned and the file is closed, and then releases the permit; a
	// Close error means the file may still be open.
	//
	// An error matching artifact.ErrFileCloseUncertain, with or without a
	// body, means a file may still be open: the caller keeps the error and
	// keeps the read protected, and never marks or finishes it. Any other
	// error with a nil body means nothing is open and no permit is held. A
	// non-nil body with an error is open and must be closed.
	OpenEnvironmentArtifact(ctx context.Context, lease store.ArtifactLease) (io.ReadCloser, error)
}

// BindArtifacts binds the opener of artifact reads, once, before Run and
// before any Create. Materialization is enabled only if the Runtime is a
// Materializer and an opener is bound. It returns an error if an opener is
// already bound or the Controller has started running.
func (c *Controller) BindArtifacts(o ArtifactOpener) error {
	if o == nil {
		return errors.New("nil artifact opener")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.artifacts != nil {
		return errors.New("artifact opener already bound")
	}
	if c.running {
		return errors.New("controller already running")
	}
	c.artifacts, c.producer = o, o.ArtifactProducer()
	return nil
}

// ArtifactsEnabled reports whether c accepts specs with artifacts: its
// Runtime is a Materializer and an ArtifactOpener is bound. It is configured
// support, not readiness or capacity; it calls neither the Runtime nor the
// opener.
func (c *Controller) ArtifactsEnabled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.materializer != nil && c.artifacts != nil
}

// materializeRetry is a frame's wait before it calls the Store or the opener
// again after a result it does not know, or while a resource is busy.
const materializeRetry = time.Second

// custody is what a frame knows of the read of one artifact of its
// incarnation. It is separate from whether the artifact was staged.
type custody int

const (
	// notStarted holds nothing: no Begin of the artifact was called, or
	// each was refused unwritten.
	notStarted custody = iota
	// unsettled holds the read: a call is running, a lease is held, a file
	// is open or its closure is uncertain, or a Begin's result is unknown.
	unsettled
	// released records that this frame marked and finished the read (S1).
	released
	// materializedElsewhere records a read of the incarnation that was
	// closed before this frame (S3). This frame did not stage it.
	materializedElsewhere
	// superseded records that the incarnation is no longer committed, or
	// the environment is gone, and that no read of it is open (S2).
	superseded
	// heldByOther records a read another reader holds; the frame holds
	// nothing and asks again while its effects run.
	heldByOther
	// heldUnknown is S4: a Begin whose result was lost may have committed
	// the read that is now held, and with an unknown producer the Store
	// can never return its lease. No lease was returned, nothing was
	// opened and no call runs.
	heldUnknown
)

// boot is what a frame knows of its instance's Bootstrap.
type boot int

const (
	// bootPending: no Bootstrap has been acknowledged. One may run, or
	// may have recorded the release without acknowledgement.
	bootPending boot = iota
	// bootReleased: a Bootstrap's acknowledgement was received.
	bootReleased
	// bootRefused: a Bootstrap was refused with ErrIncarnation. A release
	// may still have been recorded by an earlier call of unknown result;
	// the incarnation is held, and only a new one can become Ready.
	bootRefused
)

// A group is a frame's record of one distinct artifact: the custody of its
// read, and whether this frame staged it at every one of its mounts.
type group struct {
	custody custody
	staged  bool
	// kept retains, on a path that leaves a file possibly open, the lease
	// the Store returned for the read, with the uncertain error and any
	// body it came with. They own that file; nothing replaces them.
	kept []any
}

// A frame is this process's materialization of one committed incarnation of
// one environment. Its tuple, the row's Identity, IncarnationSeq and
// Incarnation, and the row's Ref and Native, are captured once from the
// committed row it was started for and never refilled.
type frame struct {
	e        store.Environment
	l        ledger
	opener   ArtifactOpener
	producer store.OriginalServerLifetime
	effects  context.Context // ends when the frame must start no new effect
	cancel   context.CancelFunc
	done     chan struct{} // closed once the frame's runFrame has returned
	// starting is held to read from a check of effects until the
	// Materialize it allows has returned, and to write by stop.
	starting sync.RWMutex

	mu     sync.Mutex
	groups []group // by artifact, in the order of groups(e.Spec)
	boot   boot
}

// stop ends f's effects, and returns once no Materialize that saw them live
// is still starting. A Materialize returns once started.
func (f *frame) stop() {
	f.cancel()
	f.starting.Lock()
	f.starting.Unlock()
}

func (f *frame) set(i int, c custody) custody {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.groups[i].custody = c
	return c
}

func (f *frame) setStaged(i int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.groups[i].staged = true
}

func (f *frame) keep(i int, owners ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.groups[i].kept = append(f.groups[i].kept, owners...)
}

func (f *frame) setBoot(b boot) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.boot = b
}

func (f *frame) staged(i int) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.groups[i].staged
}

// settled reports that f holds nothing: no running call, no lease, no open
// or uncertain file and no Begin of unknown result. Every artifact may be
// released while f's Bootstrap still runs, so f is settled only once its
// runFrame has returned, which is after that Bootstrap's Done.
func (f *frame) settled() bool {
	select {
	case <-f.done:
	default:
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return !slices.ContainsFunc(f.groups, func(g group) bool { return g.custody == unsettled })
}

// ready reports that every artifact of f's incarnation was staged by f and
// its read released, and that a Bootstrap of the instance was acknowledged.
func (f *frame) ready() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.boot == bootReleased && !slices.ContainsFunc(f.groups, func(g group) bool { return g.custody != released || !g.staged })
}

// held reports an artifact that keeps f's incarnation from becoming Ready in
// this process.
func (f *frame) held() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.boot == bootRefused || slices.ContainsFunc(f.groups, func(g group) bool {
		switch g.custody {
		case released:
			return !g.staged
		case materializedElsewhere, heldByOther, heldUnknown:
			return true
		}
		return false
	})
}

// groups returns spec's mounts grouped by artifact, in order of first mount.
// The Store admits one read per artifact and incarnation, and one Materialize
// stages every mount of a group from it.
func groups(spec environment.Spec) [][]environment.ArtifactMount {
	var out [][]environment.ArtifactMount
	index := map[store.ArtifactRef]int{}
	for _, m := range spec.Artifacts {
		ref := artifactRef(m)
		i, ok := index[ref]
		if !ok {
			i = len(out)
			index[ref] = i
			out = append(out, nil)
		}
		out[i] = append(out[i], m)
	}
	return out
}

func artifactRef(m environment.ArtifactMount) store.ArtifactRef {
	return store.ArtifactRef{ID: m.ID, SHA256: m.SHA256, Size: m.Size}
}

// producerKnown reports whether the Store can replay a read of f's producer:
// it requires a nonzero NamespaceDevice and StartTicks.
func (f *frame) producerKnown() bool {
	return f.producer.NamespaceDevice != 0 && f.producer.StartTicks != 0
}

// materialize starts the frame of e's committed incarnation if
// materialization is enabled, e is Present, has artifacts and an incarnation,
// and this process has no frame of that incarnation and holds nothing of an
// earlier one. l is e's ledger.
func (c *Controller) materialize(e store.Environment, l ledger) {
	if e.State.Desired != environment.Present || e.Incarnation == "" || len(e.Spec.Artifacts) == 0 {
		return
	}
	id := e.Identity.ID
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.materializer == nil || c.artifacts == nil || c.stopping {
		return
	}
	if old := c.materializing[id]; old != nil {
		if old.e.IncarnationSeq == e.IncarnationSeq || !old.settled() {
			return
		}
		old.cancel()
	}
	effects, cancel := context.WithCancel(context.Background())
	f := &frame{e: e, l: l, opener: c.artifacts, producer: c.producer, effects: effects, cancel: cancel, done: make(chan struct{}), groups: make([]group, len(groups(e.Spec)))}
	c.materializing[id] = f
	go func() {
		c.runFrame(f)
		cancel()
		// A waiter woken by the signal finds f settled.
		close(f.done)
		c.signal()
	}()
}

// runFrame settles each artifact of f in turn, and stops at the first one
// it did not stage and release. Once every artifact is staged and released,
// no Materialize of f runs, and f calls Bootstrap.
func (c *Controller) runFrame(f *frame) {
	for i, mounts := range groups(f.e.Spec) {
		o := c.settleArtifact(f, i, mounts)
		for o == heldByOther && c.pause(f.effects) {
			o = c.settleArtifact(f, i, mounts)
		}
		if o != released || !f.staged(i) {
			return
		}
	}
	c.bootstrap(f)
}

// bootstrap has the runtime record the release of f's instance while f's
// effects run and its authority holds, and calls Bootstrap again after a
// result it does not know. Every artifact of f was staged, its body closed
// and its read released; nothing of them is held. A Bootstrap that is
// canceled is still joined by its Done. Only an acknowledged Bootstrap lets
// the incarnation become Ready; no observation of Ensure stands for one.
func (c *Controller) bootstrap(f *frame) {
	ref := c.ref(f.e, f.l)
	for {
		var m Materialization
		err := context.Canceled
		f.starting.RLock()
		if f.effects.Err() == nil && c.auth.CheckDelegation(f.e.Authority) == nil {
			m, err = c.materializer.Bootstrap(ref, f.e.Incarnation)
		}
		f.starting.RUnlock()
		if errors.Is(err, ErrIncarnation) {
			f.setBoot(bootRefused)
			return
		}
		if err == nil {
			select {
			case <-m.Done():
			case <-f.effects.Done():
				m.Cancel()
				<-m.Done() // Cancel is not a join.
			}
			if r := m.Result(); r.Settled && r.Err == nil {
				f.setBoot(bootReleased)
				return
			}
		}
		if !c.pause(f.effects) {
			return
		}
	}
}

// settleArtifact admits, or replays, f's read of the artifact that mounts
// name, uses it while f's effects run, and releases it. Every Begin passes
// f's captured tuple unchanged; only the revision a fresh read needs is read
// again. A Begin of unknown result is retried even after f's effects end,
// until the Store's answer is known.
func (c *Controller) settleArtifact(f *frame, i int, mounts []environment.ArtifactMount) custody {
	id := f.e.Identity.ID
	ref := artifactRef(mounts[0])
	revision := f.e.Revision
	unknown := false // an earlier Begin of the tuple has an unknown result
	f.set(i, unsettled)
	for {
		if !unknown && f.effects.Err() != nil {
			return f.set(i, notStarted)
		}
		lease, err := c.begin(id, revision, f.e.IncarnationSeq, f.e.Incarnation, ref, f.producer, c.now())
		var capacity *store.ArtifactCapacityError
		switch {
		case err == nil:
			return f.set(i, c.useLease(f, i, mounts, lease))
		case errors.Is(err, store.ErrEnvironmentIncarnation), errors.Is(err, store.ErrNotFound):
			// The Store moves the incarnation, or deletes the
			// environment, only with no read open, so no read of this
			// tuple is open, this frame's included.
			return f.set(i, superseded)
		case errors.Is(err, store.ErrEnvironmentMaterialized):
			return f.set(i, materializedElsewhere)
		case errors.Is(err, store.ErrEnvironmentMaterializationHeld):
			if unknown && !f.producerKnown() {
				return f.set(i, heldUnknown)
			}
			// Any read of this frame's known producer would have been
			// replayed, so the holder is another.
			return f.set(i, heldByOther)
		case errors.As(err, &capacity):
			// Not admitted; nothing was written.
		case errors.Is(err, store.ErrEnvironmentStale):
			// Not admitted. A fresh read needs the current revision of
			// the same committed incarnation.
			cur, rerr := c.read(id)
			if rerr == nil {
				if cur.IncarnationSeq != f.e.IncarnationSeq || cur.Incarnation != f.e.Incarnation {
					return f.set(i, superseded)
				}
				revision = cur.Revision
			}
		default:
			// The read may have been admitted.
			unknown = true
		}
		if unknown {
			time.Sleep(c.retry)
		} else {
			c.pause(f.effects)
		}
	}
}

// useLease stages the artifact lease admits while f's effects run and its
// authority holds, then releases lease. It returns unsettled, keeping lease,
// if a file may still be open.
func (c *Controller) useLease(f *frame, i int, mounts []environment.ArtifactMount, lease store.ArtifactLease) custody {
	// A replayed read already closing, and a read recovered after the
	// effects ended, are only released. Future effects revalidate the
	// accepted authority.
	if lease.Closing || f.effects.Err() != nil || c.auth.CheckDelegation(f.e.Authority) != nil {
		return c.releaseRead(f, lease)
	}
	select {
	case c.copies <- struct{}{}:
	case <-f.effects.Done():
		return c.releaseRead(f, lease)
	}
	// The opener refuses effects that ended before its check of them. A
	// call already past that check may still open and verify the file
	// after they end; lease, and the opener's permit, stay held until it
	// returns and its body is closed below, unstaged.
	body, ok := c.open(f, i, lease)
	if !ok {
		<-c.copies
		return unsettled
	}
	if body == nil {
		<-c.copies
		return c.releaseRead(f, lease)
	}
	staged, err := c.stageBody(f, mounts, body)
	<-c.copies
	if err != nil {
		f.keep(i, lease, err, body)
		return unsettled
	}
	if staged {
		f.setStaged(i)
	}
	return c.releaseRead(f, lease)
}

// open opens lease's bytes. It returns false if a file may still be open,
// having kept lease, its error and any body in f. A nil body with true means
// nothing is open.
func (c *Controller) open(f *frame, i int, lease store.ArtifactLease) (io.ReadCloser, bool) {
	for {
		body, err := f.opener.OpenEnvironmentArtifact(f.effects, lease)
		switch {
		case errors.Is(err, artifact.ErrFileCloseUncertain):
			// A nil body is not proof that nothing is open.
			f.keep(i, lease, err, body)
			return nil, false
		case err != nil && body != nil:
			if cerr := body.Close(); cerr != nil {
				f.keep(i, lease, errors.Join(err, cerr), body)
				return nil, false
			}
			return nil, true
		case errors.Is(err, artifact.ErrFilesBusy):
			if !c.pause(f.effects) {
				return nil, true
			}
		case err != nil:
			return nil, true
		default:
			return body, true
		}
	}
}

// stageBody has the runtime stage body at every one of mounts and reports
// whether it succeeded. It returns only after the runtime's frame is done and
// body is closed; an error is body's Close error.
func (c *Controller) stageBody(f *frame, mounts []environment.ArtifactMount, body io.ReadCloser) (bool, error) {
	ok := false
	// The opener read the bytes to verify them; the authority and the
	// effects are checked again right before the runtime may use them.
	ref := c.ref(f.e, f.l)
	var m Materialization
	err := context.Canceled
	f.starting.RLock()
	if f.effects.Err() == nil && c.auth.CheckDelegation(f.e.Authority) == nil {
		m, err = c.materializer.Materialize(ref, f.e.Incarnation, slices.Clone(mounts), body)
	}
	f.starting.RUnlock()
	if err == nil {
		select {
		case <-m.Done():
		case <-f.effects.Done():
			m.Cancel()
			<-m.Done() // Cancel is not a join.
		}
		r := m.Result()
		ok = r.Settled && r.Err == nil
	}
	if err := body.Close(); err != nil {
		return false, err
	}
	return ok, nil
}

// releaseRead marks lease's read closing and finishes it, each retried with
// the same closure until it succeeds, after f's effects end too.
func (c *Controller) releaseRead(f *frame, lease store.ArtifactLease) custody {
	closure := store.ArtifactDownloadClosure{
		Owner:    f.e.Principal,
		LeaseID:  lease.ID,
		Revision: lease.Revision,
		Ref:      lease.Reference,
		File:     lease.File,
		RootID:   lease.RootID,
		Producer: lease.Producer,
	}
	// The Store accepts a Mark of a read already closing, and a Finish of
	// one already finished, so a lost result is retried with the same
	// closure.
	marked, isMarked := closure, false
	for {
		if !isMarked {
			if m, err := c.mark(closure); err == nil {
				marked, isMarked = m, true
			}
		}
		if isMarked && c.finish(marked, c.now()) == nil {
			return released
		}
		time.Sleep(c.retry)
	}
}

// pause waits c.retry, and reports false if ctx ended first.
func (c *Controller) pause(ctx context.Context) bool {
	t := time.NewTimer(c.retry)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// holdsArtifacts reports whether this process holds anything of a
// materialization of environment id: a running call, a lease, an open or
// uncertain file or a Begin of unknown result. Such a frame defers Deleted
// and any incarnation change.
func (c *Controller) holdsArtifacts(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	f := c.materializing[id]
	return f != nil && !f.settled()
}

// materialized reports whether e may become Ready with incarnation inc: e has
// no artifacts, or inc is e's committed incarnation and this process's frame
// of it staged every artifact and released its read.
func (c *Controller) materialized(e store.Environment, inc string) bool {
	if len(e.Spec.Artifacts) == 0 {
		return true
	}
	if inc == "" || inc != e.Incarnation {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	f := c.materializing[e.Identity.ID]
	return f != nil && f.e.IncarnationSeq == e.IncarnationSeq && f.ready()
}

// artifactsHeld reports whether this process found that e's committed
// incarnation, not yet Ready, cannot become Ready.
func (c *Controller) artifactsHeld(e store.Environment) bool {
	if e.State.Phase == environment.Ready {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	f := c.materializing[e.Identity.ID]
	return f != nil && f.e.IncarnationSeq == e.IncarnationSeq && f.held()
}

// stopMaterializing ends the effects of environment id's frame. What it holds
// is still released.
func (c *Controller) stopMaterializing(id string) {
	c.mu.Lock()
	f := c.materializing[id]
	c.mu.Unlock()
	if f != nil {
		f.stop()
	}
}
