package control

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
	"github.com/tmc/skygo/internal/workeradmission"
)

// workerCallTimeout bounds one original member call. Its expiry ends only the
// wait: the call's result, even partial, is still recorded with its token.
const workerCallTimeout = 30 * time.Second

// workerPage is the number of live groups read per Store call.
const workerPage = 16

// WorkerConfig configures dedicated worker groups.
type WorkerConfig struct {
	// Budgets maps a worker target's name to its operator-declared gang
	// budget. A zero field is zero capacity. Every named target must be
	// configured, keep remote output and implement backend.WorkerBackend.
	Budgets map[string]workeradmission.Budget
}

// Workers runs dedicated worker groups with durable main-start barriers.
// It shares Control's Store, authentication and targets; Control.Cancel records group
// cancellation. NewWorkers must precede use.
//
// Each member effect follows the Store's intent protocol: a durable intent,
// then exactly one original backend call, then its recorded result. Nothing
// is retried, adopted by name or inferred from a missing Pod, an elapsed
// time, a release acknowledgement or a cancel acknowledgement. A group whose
// Create outcome is unknown stays Held with its reservation.
//
// Legacy groups remain readable but are never adopted into the barrier protocol.
type Workers struct {
	c       *Control
	targets map[string]workerTarget
	now     func() time.Time

	mu          sync.Mutex // serializes Step
	barriers    map[string]*workerBarriers
	logMu       sync.Mutex
	logs        map[string]int
	logClosed   map[string]bool
	logFailures map[string][]*workerLogResult
}

type workerTarget struct {
	target  backend.Target
	backend backend.WorkerBarrierBackend
	budget  workeradmission.Budget
}

// NewWorkers returns the worker group runner of c.
func NewWorkers(c *Control, config WorkerConfig) (*Workers, error) {
	if c == nil || len(config.Budgets) == 0 {
		return nil, errors.New("control and worker budgets required")
	}
	w := &Workers{c: c, targets: make(map[string]workerTarget), now: time.Now, barriers: make(map[string]*workerBarriers), logs: make(map[string]int), logClosed: make(map[string]bool), logFailures: make(map[string][]*workerLogResult)}
	for name, b := range config.Budgets {
		t, ok := c.target(name)
		if !ok {
			return nil, fmt.Errorf("worker target %q is not configured", name)
		}
		wb, ok := t.Backend.(backend.WorkerBarrierBackend)
		if !ok {
			return nil, fmt.Errorf("worker target %q has no worker backend", name)
		}
		if t.OutputStorage != backend.RemoteOutput {
			return nil, fmt.Errorf("worker target %q requires remote output", name)
		}
		if b.Ranks < 2 || b.CPUMillis == 0 || b.MemoryBytes == 0 {
			return nil, fmt.Errorf("worker target %q has an invalid budget", name)
		}
		w.targets[name] = workerTarget{target: t, backend: wb, budget: b}
	}
	return w, nil
}

// Submit accepts or replays a worker group of task t with spec. A single
// independent worker normalizes to an ordinary retained job and returns a nil
// spec; its ID names no group. Otherwise the result is the group's job and
// its normalized spec.
//
// Every rank's demand is derived from the validated task: the larger of its
// request and limit for CPU and memory, and its accelerator count. CPU and
// memory requests are required; nothing the caller supplies can lower the
// charge. The first worker target that accepts the task and whose budget can
// ever hold the whole demand is selected.
func (w *Workers) Submit(actor auth.Actor, domain store.RequestDomain, requestID string, t backend.Task, spec backend.WorkerSpec) (backend.Job, *backend.WorkerSpec, error) {
	c := w.c
	if err := c.beginExternal(); err != nil {
		return backend.Job{}, nil, err
	}
	defer c.endExternal()
	actor, err := c.engine.Check(actor, auth.Mutate)
	if err != nil {
		return backend.Job{}, nil, err
	}
	if !requestName.MatchString(requestID) {
		return backend.Job{}, nil, fmt.Errorf("%w: request_id must contain 1..128 letters, digits, underscores or hyphens", ErrInvalid)
	}
	n, err := backend.NormalizeWorkers(&spec)
	if err != nil {
		return backend.Job{}, nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if n == nil {
		job, err := c.SubmitRetained(actor, domain, requestID, t)
		return job, nil, err
	}
	t, err = normalizeTask(cloneTask(t))
	if err != nil {
		return backend.Job{}, nil, err
	}
	if err := Validate(t); err != nil {
		return backend.Job{}, nil, err
	}
	if t.Retry != nil || t.Workdir != nil || len(t.Setup) != 0 || len(t.Outputs) != 0 || len(t.DataMounts) != 0 {
		return backend.Job{}, nil, fmt.Errorf("%w: worker tasks do not support retry, workspaces or data", ErrInvalid)
	}
	if len(t.Env) == 0 {
		t.Env = nil
	}
	if replay, found, err := c.store.LookupWorkerGroup(actor, domain, requestID, t, *n); err != nil {
		return backend.Job{}, nil, err
	} else if found {
		spec = replay.Group.Spec
		return replay.Job, &spec, nil
	}
	demand, err := workerDemand(t, n.Count)
	if err != nil {
		return backend.Job{}, nil, err
	}
	if c.failed.Load() {
		return backend.Job{}, nil, ErrUnavailable
	}
	if err := c.storageGuard(false, false); err != nil {
		return backend.Job{}, nil, err
	}
	target, err := w.plan(t, demand)
	if err != nil {
		return backend.Job{}, nil, err
	}
	authority, err := c.engine.Delegate(actor)
	if err != nil {
		return backend.Job{}, nil, err
	}
	if c.failed.Load() {
		return backend.Job{}, nil, ErrUnavailable
	}
	g, err := c.store.SubmitWorkerBarrierGroup(actor, store.WorkerGroupRequest{
		Domain:    domain,
		RequestID: requestID,
		Task:      t,
		Spec:      *n,
		Target:    target.Name,
		Binding:   target.Binding,
		Authority: authority,
		Demand:    demand,
	})
	if err != nil {
		return backend.Job{}, nil, err
	}
	c.retention.wantSample.Store(true)
	spec = g.Group.Spec
	return g.Job, &spec, nil
}

// workerDemand returns the per-rank demand of count ranks of t.
func workerDemand(t backend.Task, count uint8) (workeradmission.Demand, error) {
	if t.CPUMillis <= 0 || t.MemoryBytes <= 0 {
		return workeradmission.Demand{}, fmt.Errorf("%w: worker tasks require cpu and memory requests", ErrInvalid)
	}
	return workeradmission.Demand{
		Ranks:        uint32(count),
		CPUMillis:    uint64(max(t.CPUMillis, t.CPULimitMillis)),
		MemoryBytes:  uint64(max(t.MemoryBytes, t.MemoryLimitBytes)),
		Accelerators: uint32(t.AcceleratorCount),
	}, nil
}

// plan returns the first worker target, in name order, that accepts t and
// whose budget can hold d.
func (w *Workers) plan(t backend.Task, d workeradmission.Demand) (backend.Target, error) {
	var reasons []string
	for _, target := range w.c.targets {
		wt, ok := w.targets[target.Name]
		if !ok {
			continue
		}
		if err := checkWorkspaceTarget(t, target, w.c.retention.enabled, w.c.workspace); err != nil {
			reasons = append(reasons, target.Name+": "+err.Error())
			continue
		}
		if err := workeradmission.Fits(wt.budget, d); err != nil {
			reasons = append(reasons, target.Name+": "+err.Error())
			continue
		}
		return target, nil
	}
	return backend.Target{}, fmt.Errorf("%w: no worker target: %v", ErrUnsupported, reasons)
}

// WorkerRankState is one rank's state, derived from its own durable facts.
type WorkerRankState uint8

const (
	// WorkerWaiting has not started. An unscheduled or gated rank waits.
	WorkerWaiting WorkerRankState = iota + 1
	WorkerStarting
	WorkerRunning
	WorkerSucceeded
	WorkerFailed
	// WorkerCanceled was canceled and positively never ran or was retired.
	WorkerCanceled
	// WorkerUnknown has an unknown original outcome or observation. It holds
	// the group.
	WorkerUnknown
)

// WorkerRank reports one rank. ExitCode is set only from a native exit
// receipt.
type WorkerRank struct {
	Rank     uint8
	State    WorkerRankState
	Reason   string
	ExitCode *int32
}

// Ranks reports every rank of one group attempt visible to actor. The first
// attempt's ID is the job ID; a group has no other attempt. A foreign,
// missing or ordinary job, or another attempt, is store.ErrNotFound.
func (w *Workers) Ranks(actor auth.Actor, id, attempt string) (store.WorkerGroup, []WorkerRank, error) {
	g, err := w.group(actor, id, attempt)
	if err != nil {
		return store.WorkerGroup{}, nil, err
	}
	out := make([]WorkerRank, len(g.Members))
	for i, m := range g.Members {
		out[i] = workerRank(g, m)
	}
	return g, out, nil
}

// group reads the authorized group id at attempt.
func (w *Workers) group(actor auth.Actor, id, attempt string) (store.WorkerGroup, error) {
	actor, err := w.c.engine.Check(actor, auth.Read)
	if err != nil {
		return store.WorkerGroup{}, err
	}
	if !jobName.MatchString(id) || !jobName.MatchString(attempt) {
		return store.WorkerGroup{}, fmt.Errorf("%w: invalid job or attempt id", ErrInvalid)
	}
	g, err := w.c.store.WorkerGroupFor(actor, id)
	if err != nil {
		return store.WorkerGroup{}, err
	}
	if g.Group.AttemptID != attempt {
		return store.WorkerGroup{}, store.ErrNotFound
	}
	return g, nil
}

// workerRank derives m's state. The first matching rule wins: an unknown
// Create; a native exit; a canceled group's settled member; a recorded
// running observation; any other unknown effect or observation; no Create
// or an unreleased member; otherwise the observed waiting state.
func workerRank(g store.WorkerGroup, m store.WorkerMember) WorkerRank {
	r := WorkerRank{Rank: m.Rank}
	o := m.Observed
	switch {
	case m.Create.State == store.WorkerUnknown:
		r.State, r.Reason = WorkerUnknown, "create outcome unknown"
	case o != nil && o.ExitCode != nil:
		code := *o.ExitCode
		r.ExitCode = &code
		r.State = WorkerSucceeded
		if code != 0 {
			r.State, r.Reason = WorkerFailed, fmt.Sprintf("exited with code %d", code)
		}
	case g.Job.CancelRequested && (m.Create.State == store.WorkerNone || m.Removed && m.Release.State == store.WorkerNone):
		r.State, r.Reason = WorkerCanceled, "canceled before start"
	case o != nil && o.State == backend.Running:
		r.State = WorkerRunning
	case m.Release.State == store.WorkerUnknown:
		r.State, r.Reason = WorkerUnknown, "release outcome unknown"
	case o != nil && o.State == backend.Unknown:
		r.State, r.Reason = WorkerUnknown, "observation unknown"
	case m.Create.State == store.WorkerNone || m.Release.State != store.WorkerRecorded:
		r.State = WorkerWaiting
	case o != nil && o.State == backend.Starting:
		r.State = WorkerStarting
	default:
		r.State = WorkerWaiting
	}
	return r
}

// Step reconciles every live worker group once, in acceptance order. A
// group's error does not stop the others; Step returns them joined.
func (w *Workers) Step(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var errs []error
	var after uint64
	for {
		page, err := w.c.store.WorkerGroups(after, workerPage)
		if err != nil {
			return errors.Join(append(errs, err)...)
		}
		for _, g := range page {
			after = g.Job.Sequence
			if err := w.step(ctx, g); err != nil {
				errs = append(errs, fmt.Errorf("worker group %s: %w", g.Job.ID, err))
			}
		}
		if len(page) < workerPage {
			return errors.Join(errs...)
		}
	}
}

// step admits one group, then reconciles only its versioned barrier state.
// A legacy group is never dispatched through the new backend protocol.
func (w *Workers) step(ctx context.Context, g store.WorkerGroup) error {
	if err := w.latchLogFailures(g.Job.ID); err != nil {
		return err
	}
	// A returned original read needs only its Store, not a current native route
	// or renewed delegation. Finish its exact record before any later work.
	if b := w.barriers[g.Job.ID]; b != nil && b.pending != nil && b.pending.kind.isObservation() {
		_, err := b.commitPending()
		return err
	}
	t, ok := w.targets[g.Job.Target]
	if !ok || t.target.Binding != g.Job.TargetBinding || g.Barrier == nil {
		return nil
	}
	if g.Phase == store.WorkerGroupQueued {
		return w.admit(g, t)
	}
	b := w.barriers[g.Job.ID]
	if b == nil {
		b = &workerBarriers{store: storeWorkerBarriers{w.c.store}, backend: t.backend,
			check: w.c.engine.CheckDelegation, now: w.now,
			beforeRemove: func() bool { return w.fenceLogs(g.Job.ID) }, unsettled: func() bool { return w.unsettledLogs(g.Job.ID) }}
		w.barriers[g.Job.ID] = b
	}
	if err := b.step(ctx, g.Job.ID); err != nil {
		return err
	}
	if b.pending == nil && b.held == nil && !w.unsettledLogs(g.Job.ID) {
		if current, err := w.c.store.WorkerGroup(g.Job.ID); err != nil {
			return err
		} else if current.Phase == store.WorkerGroupDone {
			w.logMu.Lock()
			if w.logs[g.Job.ID] == 0 {
				delete(w.barriers, g.Job.ID)
				delete(w.logClosed, g.Job.ID)
			}
			w.logMu.Unlock()
		}
	}
	return nil
}

// admit settles a canceled or revoked Queued group, revokes one whose
// delegation was refused, or tries to admit it under its target's budget.
// Only the head of the target's queue is admitted.
func (w *Workers) admit(g store.WorkerGroup, t workerTarget) error {
	if !g.Job.CancelRequested && !g.Revoked {
		switch err := w.c.engine.CheckDelegation(g.Authority); {
		case errors.Is(err, auth.ErrUnavailable):
			return nil
		case errors.Is(err, auth.ErrUnauthenticated) || errors.Is(err, auth.ErrPermissionDenied):
			_, err = w.c.store.RevokeWorkerGroup(g.Job.ID, g.Revision, err, w.now())
			return err
		case err != nil:
			return err
		}
	}
	_, _, err := w.c.store.AdmitWorkerGroup(g.Job.ID, g.Revision, t.budget, w.now())
	return err
}

// workerBarrierTimeout bounds the wait, from a group's first recorded gate
// release, for every member to be bound and prepared.
const workerBarrierTimeout = 15 * time.Minute

// barrierDecision is a group's durable barrier decision.
type barrierDecision uint8

const (
	barrierUndecided barrierDecision = iota
	barrierStart
	barrierAbort
)

// barrierEffect names one member operation of a barrier group.
type barrierEffect uint8

const (
	barrierCreate barrierEffect = iota + 1
	barrierRelease
	barrierStartRank
	barrierCancel
	barrierRemove
	barrierRecover
)

// barrierToken names one durable intent, as store.WorkerToken does.
type barrierToken struct {
	Job    string
	Rank   uint8
	Effect barrierEffect
	Seq    uint64
	Gen    [16]byte
}

// barrierGroup is the durable state of one barrier group. Its barrier
// deadline derives from the members' recorded Release times.
type barrierGroup struct {
	Job       backend.Job
	Group     backend.WorkerGroup
	Authority auth.Delegation
	Revision  uint64
	Revoked   bool
	Done      bool
	Decision  barrierDecision
	Members   []barrierMember
}

// barrierMember is one rank of a barrier group. Pod and Service are the
// original Create returns. While Create is Unknown they are whatever
// ResolveWorker has since found, each kind separately; such a member is
// cleanup-only, and a kind not yet found is an unresolved obligation (see
// barrierUnresolved). Resolve is the outcome of the last ResolveWorker
// call of such a member. Refused records ErrWorkerCreateRefused. Observed is
// the last recorded barrier observation of exactly Pod. Stop is the first
// native stop evidence the Store recorded from an observation of exactly
// Pod's UID: backend.WorkerNeverBound or backend.WorkerTerminated, never
// backend.WorkerNeverReleased. RemoveSent reports qualified actual dispatch
// for every resource in the exact removal reference; intent alone is insufficient. Removed and ServiceRemoved record the RemoveWorker or
// RecoverWorkerRemoval flags of Pod and Service. Vanished records
// backend.ErrWorkerVanished; it holds the group for operator resolution.
type barrierMember struct {
	Rank           uint8
	Pod, Service   backend.WorkerResource
	Create         store.WorkerEffectState
	Release        store.WorkerEffectState
	Start          store.WorkerEffectState
	Cancel         store.WorkerEffectState
	Remove         store.WorkerEffectState
	Recover        store.WorkerEffectState
	Resolve        resolveOutcome
	Refused        bool
	LocalUnsettled bool
	Observed       *backend.WorkerBarrier
	ObservedAt     time.Time
	Stop           backend.WorkerStop
	RemoveSent     bool
	Removed        bool
	ServiceRemoved bool
	RemoveHadPod   bool
	RecoverHadPod  bool
	StartToken     [32]byte
	Vanished       bool
}

// resolveOutcome is the recorded outcome of one ResolveWorker call: the
// class of its error, never the error's text, and whether Control refused
// its receipt. No class is evidence of absence, and none changes what the
// member may do: a kind not found stays unresolved whatever the class. The
// zero value records no call.
type resolveOutcome struct {
	Unsettled bool
	Class     resolveClass
	Refused   bool
	At        time.Time
}

// resolveClass classifies a ResolveWorker error.
type resolveClass uint8

const (
	// resolveClean is a nil error. A rank 0 Service returned alone still
	// leaves its Pod unresolved.
	resolveClean resolveClass = iota + 1
	// resolveUncertain is an error of no other class, including the
	// target's not found.
	resolveUncertain
	// resolveInterrupted is an error wrapping context.Canceled or
	// context.DeadlineExceeded: the call ended before its reads did.
	resolveInterrupted
	// resolveMismatch is an error wrapping backend.ErrIdentityMismatch: an
	// object at a planned name differs from the plan and is not adopted.
	resolveMismatch
)

// resolveClassOf returns the class of err, a ResolveWorker error.
func resolveClassOf(err error) resolveClass {
	switch {
	case err == nil:
		return resolveClean
	case errors.Is(err, backend.ErrIdentityMismatch):
		return resolveMismatch
	case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		return resolveInterrupted
	default:
		return resolveUncertain
	}
}

// barrierReceipt is the original return of one member call. Only the field
// of the token's effect is recorded; Recover records Removed.
type barrierReceipt struct {
	Created   backend.WorkerCreated
	Refused   bool
	Released  backend.WorkerReleased
	Started   backend.WorkerStarted
	Canceled  backend.WorkerCanceled
	Removed   backend.WorkerRemoval
	Unsettled bool
}

// workerBarrierStore is the typed Store adapter used by the barrier reducer.
//
//   - IntendBarrier records a durable intent under the group revision. It
//     refuses Start without a Start decision, Release or Start after an
//     Abort decision, Release or Start of a member whose Create is not
//     Recorded, and Remove of a Vanished member, of a member with an
//     unresolved obligation, or of one whose Pod is known, not Removed and
//     without Stop.
//   - DecideWorkerBarrier records the group's decision exactly once. Start
//     requires every member created, released, observed prepared and
//     without stop evidence, and a group neither canceled, revoked nor
//     terminal; Abort is always permitted. A second call returns the stored
//     decision when it agrees and an error otherwise.
//   - RecordWorkerBarrier records an observation of exactly Pod; it never
//     clears recorded stop evidence.
//   - RecordWorkerResolved records, in one write, a ResolveWorker receipt
//     of a member whose Create is Unknown and the call's outcome, at now:
//     each kind the receipt names that the member does not yet know, and
//     the outcome in place of the last. It refuses a receipt naming another
//     object of a kind the member knows and an unknown class, and then
//     records nothing. It never changes the Create state.
//   - RecordWorkerVanished holds the member; only an operator clears it.
//   - RecordBarrierLost refuses a token of this process's generation.
//   - RevokeBarrierGroup records a refused delegation, as RevokeWorkerGroup
//     does; the group then stops.
type workerBarrierStore interface {
	BarrierGroup(id string) (barrierGroup, error)
	IntendBarrier(id string, rev uint64, rank uint8, e barrierEffect, now time.Time) (barrierToken, barrierGroup, error)
	RecordBarrierEffect(tok barrierToken, got barrierReceipt, callErr string, now time.Time) (barrierGroup, error)
	RecordBarrierLost(tok barrierToken, now time.Time) (barrierGroup, error)
	RecordWorkerBarrier(id string, rev uint64, rank uint8, got backend.WorkerBarrier, now time.Time) (barrierGroup, error)
	RecordWorkerResolved(id string, rev uint64, rank uint8, got backend.WorkerCreated, out resolveOutcome, now time.Time) (barrierGroup, error)
	RecordWorkerVanished(id string, rev uint64, rank uint8, now time.Time) (barrierGroup, error)
	DecideWorkerBarrier(id string, rev uint64, d barrierDecision, now time.Time) (barrierGroup, error)
	SettleBarrierGroup(id string, rev uint64, now time.Time) (barrierGroup, error)
	RevokeBarrierGroup(id string, rev uint64, cause error, now time.Time) (barrierGroup, error)
	LatchWorkerUnsettled(id string, rev uint64, rank uint8, original backend.WorkerReference, now time.Time) (barrierGroup, error)
}

// barrierKind is one step of a barrier group.
type barrierKind uint8

const (
	barrierWait barrierKind = iota
	stepCreate
	stepResolve
	stepRelease
	stepDecideStart
	stepDecideAbort
	stepStart
	stepCancel
	stepRemove
	stepRecover
	stepSettle
)

type barrierStep struct {
	Kind barrierKind
	Rank uint8
}

// barrierStopping reports whether g may start nothing more.
func barrierStopping(g barrierGroup) bool {
	return g.Job.CancelRequested || g.Revoked || g.Job.Status.State.Terminal() || g.Decision == barrierAbort
}

// barrierRef is the complete reference of m, with rank 0's Service in a
// WorkerTCP group.
func barrierRef(g barrierGroup, m barrierMember) backend.WorkerReference {
	r := backend.WorkerReference{Group: g.Group, Rank: m.Rank, Pod: m.Pod}
	if g.Group.Spec.Mode == backend.WorkerTCP && len(g.Members) > 0 {
		r.Service = g.Members[0].Service
	}
	return r
}

// nextBarrierStep returns g's next step at now, skipping every step in
// done. The first matching rule wins: nothing while any intent is pending;
// advance an undecided group that is not stopping; record Abort for an
// undecided stopping group; start each rank after a Start decision; and
// otherwise stop and retire every member, rank 0 of a WorkerTCP group last.
func nextBarrierStep(g barrierGroup, now time.Time, done map[barrierStep]bool) barrierStep {
	if g.Done {
		return barrierStep{}
	}
	for _, m := range g.Members {
		for _, e := range []store.WorkerEffectState{m.Create, m.Release, m.Start, m.Cancel, m.Remove, m.Recover} {
			if e.State == store.WorkerIntended {
				return barrierStep{}
			}
		}
	}
	stopping := barrierStopping(g)
	var s barrierStep
	switch {
	case g.Decision == barrierUndecided && !stopping:
		s = barrierAdvance(g, now)
	case g.Decision == barrierUndecided:
		s = barrierStep{Kind: stepDecideAbort}
	case g.Decision == barrierStart && !stopping:
		s = barrierStarts(g, done)
	default:
		s = barrierStops(g, done)
	}
	if done[s] {
		return barrierStep{}
	}
	return s
}

// barrierAdvance creates every member in rank order, then releases every
// member, then decides. Any unknown or refused Create, unknown release,
// stop evidence or deletion, or an elapsed barrier deadline aborts.
func barrierAdvance(g barrierGroup, now time.Time) barrierStep {
	for _, m := range g.Members {
		switch {
		case m.Create.State == store.WorkerNone:
			return barrierStep{Kind: stepCreate, Rank: m.Rank}
		case m.Create.State != store.WorkerRecorded || m.Refused:
			return barrierStep{Kind: stepDecideAbort}
		}
	}
	var first time.Time
	for _, m := range g.Members {
		switch m.Release.State {
		case store.WorkerNone:
			return barrierStep{Kind: stepRelease, Rank: m.Rank}
		case store.WorkerUnknown:
			return barrierStep{Kind: stepDecideAbort}
		}
		if first.IsZero() || m.Release.At.Before(first) {
			first = m.Release.At
		}
	}
	refs := make([]backend.WorkerReference, len(g.Members))
	obs := make([]backend.WorkerBarrier, len(g.Members))
	for i, m := range g.Members {
		if o := m.Observed; o != nil {
			if o.Stop != backend.WorkerStopUnproven || o.Deleting {
				return barrierStep{Kind: stepDecideAbort}
			}
			obs[i] = *o
		}
		if m.Vanished || m.Stop != backend.WorkerStopUnproven {
			return barrierStep{Kind: stepDecideAbort}
		}
		refs[i] = barrierRef(g, m)
	}
	if first.IsZero() || !now.Before(first.Add(workerBarrierTimeout)) {
		return barrierStep{Kind: stepDecideAbort}
	}
	if backend.WorkerBarrierReady(refs, obs) == nil {
		return barrierStep{Kind: stepDecideStart}
	}
	return barrierStep{}
}

// barrierStarts delivers the immutable persisted token. After a positively
// settled local call and an acknowledged Store record, an unknown delivery
// may redeliver that same token; pending or unsettled originals cannot reach
// this reducer. The backend must deduplicate task execution by that token.
func barrierStarts(g barrierGroup, done map[barrierStep]bool) barrierStep {
	for _, m := range g.Members {
		s := barrierStep{Kind: stepStart, Rank: m.Rank}
		if (m.Start.State == store.WorkerNone || m.Start.State == store.WorkerUnknown) && !done[s] {
			return s
		}
	}
	return barrierStep{}
}

// barrierStops requests termination of every live member and retires
// every member with native stop evidence. A Pod known and not removed is
// live until an observation records its Stop: Cancel is requested first,
// even of a member never released, and repeated while its outcome is
// unknown or its call failed, as when the Pod was bound between the
// backend's read and its write. Its acknowledgement proves nothing.
// A member whose Create is Unknown is resolved until every kind it may have
// made is found; until then its known resources are canceled but never
// removed. A vanished member is retired only by recovery after its
// recorded evidence and a sent removal; otherwise it holds the group. Rank
// 0 of a WorkerTCP group, which owns the Service, is retired only after
// every other member. Settle follows when every member is retired.
func barrierStops(g barrierGroup, done map[barrierStep]bool) barrierStep {
	tcp := g.Group.Spec.Mode == backend.WorkerTCP
	settled := true
	for i := len(g.Members) - 1; i >= 0; i-- {
		m := g.Members[i]
		if barrierRetired(g, m) {
			continue
		}
		settled = false
		unresolved := barrierUnresolved(g, m)
		resolve := barrierStep{Kind: stepResolve, Rank: m.Rank}
		var s barrierStep
		switch {
		case unresolved && !done[resolve]:
			s = resolve
		case barrierServiceOnlyRecovery(g, m) && m.Recover.State != store.WorkerRecorded:
			s = barrierStep{Kind: stepRecover, Rank: m.Rank}
		case m.Vanished:
			if unresolved || m.Stop == backend.WorkerStopUnproven || !m.RemoveSent || m.Recover.State == store.WorkerRecorded {
				continue
			}
			s = barrierStep{Kind: stepRecover, Rank: m.Rank}
		case m.Pod != (backend.WorkerResource{}) && !m.Removed && m.Stop == backend.WorkerStopUnproven:
			if m.Cancel.State == store.WorkerRecorded && m.Cancel.Error == "" {
				// Delivered: wait for an observation of the stop.
				continue
			}
			s = barrierStep{Kind: stepCancel, Rank: m.Rank}
		case m.Remove.State == store.WorkerUnknown:
			// An unknown original Remove is not a new removal authority.
			// Only the qualified recovery branch above may make progress.
			continue
		case unresolved:
			// Removing rank 0's Service before its Pod is found could hide
			// the Pod.
			continue
		default:
			if tcp && m.Rank == 0 && !barrierOthersRetired(g) {
				continue
			}
			s = barrierStep{Kind: stepRemove, Rank: m.Rank}
		}
		if !done[s] {
			return s
		}
	}
	if settled {
		return barrierStep{Kind: stepSettle}
	}
	return barrierStep{}
}

// barrierUnresolved reports whether m's Create is Unknown and a resource it
// may have made is not yet found: its Pod or, for rank 0 of a WorkerTCP
// group, its Service. Only the original Create refusal proves that a missing
// kind was never created; Resolve refusals and not-found observations do not.
func barrierUnresolved(g barrierGroup, m barrierMember) bool {
	if m.Create.State != store.WorkerUnknown || m.Refused {
		return false
	}
	none := backend.WorkerResource{}
	owner := g.Group.Spec.Mode == backend.WorkerTCP && m.Rank == 0
	return m.Pod == none || owner && m.Service == none
}

// barrierRetired reports whether m holds nothing: it has no unresolved
// obligation and each resource of it that is known is removed. A member
// never created, or whose Create was refused, holds only what its receipt
// names.
func barrierRetired(g barrierGroup, m barrierMember) bool {
	none := backend.WorkerResource{}
	owner := g.Group.Spec.Mode == backend.WorkerTCP && m.Rank == 0
	return !barrierUnresolved(g, m) && (m.Pod == none || m.Removed) && (!owner || m.Service == none || m.ServiceRemoved)
}

// barrierOthersRetired reports whether every member but rank 0 is retired.
func barrierOthersRetired(g barrierGroup) bool {
	for _, m := range g.Members[1:] {
		if !barrierRetired(g, m) {
			return false
		}
	}
	return true
}

// barrierMerge returns m's identity with the resources got finds that m
// does not yet know. It refuses got, reporting false and m's identity, when
// got names another object of a kind m knows or the merge is not a valid
// partial receipt of c.
func barrierMerge(m barrierMember, c backend.WorkerCreate, got backend.WorkerCreated) (backend.WorkerCreated, bool) {
	none := backend.WorkerResource{}
	known := backend.WorkerCreated{Pod: m.Pod, Service: m.Service}
	if got.Pod != none && m.Pod != none && got.Pod != m.Pod || got.Service != none && m.Service != none && got.Service != m.Service {
		return known, false
	}
	out := backend.WorkerCreated{Pod: cmp.Or(m.Pod, got.Pod), Service: cmp.Or(m.Service, got.Service)}
	if backend.ValidateWorkerCreated(c, out, true) != nil {
		return known, false
	}
	return out, true
}

// workerBarriers runs barrier groups on one target.
type workerBarriers struct {
	store   workerBarrierStore
	backend backend.WorkerBarrierBackend
	// check is the Engine's CheckDelegation.
	check func(auth.Delegation) error
	now   func() time.Time
	// pending retains an original native result until its exact Store record
	// succeeds. No native effect is repeated to repair a failed record.
	pending      *barrierResult
	held         *barrierResult
	beforeRemove func() bool
	unsettled    func() bool
}

// step reconciles group id once: it recovers earlier processes' intents,
// records one observation of every live member, and then takes each next
// step at most once. Each Create, Release and Start intent is preceded by a
// check of the group's current delegation: while the Engine is unavailable
// the group waits, and a refusal revokes it, so its next steps stop it.
func (b *workerBarriers) step(ctx context.Context, id string) error {
	if b.held != nil {
		if b.held.kind == barrierResultHeldRead && !b.held.latched {
			return b.latchHeldRead()
		}
		return nil
	}
	if b.unsettled() {
		return nil
	}
	if b.pending != nil {
		if _, err := b.commitPending(); err != nil {
			return err
		}
		if b.held != nil {
			return nil
		}
	}
	g, err := b.store.BarrierGroup(id)
	if err != nil {
		return err
	}
	for _, m := range g.Members {
		if m.LocalUnsettled {
			return nil
		}
	}
	for _, m := range g.Members {
		for _, tok := range barrierPending(g, m) {
			if g, err = b.store.RecordBarrierLost(tok, b.now()); err != nil {
				return err
			}
		}
	}
	if !barrierStopping(g) {
		var allowed bool
		if g, allowed, err = b.authorize(g); err != nil {
			return err
		}
		if !allowed && !g.Revoked {
			return nil
		}
	}
	if g, err = b.observe(ctx, g); err != nil {
		return err
	}
	if b.held != nil {
		return nil
	}
	done := make(map[barrierStep]bool)
	for {
		if b.unsettled() {
			return nil
		}
		s := nextBarrierStep(g, b.now(), done)
		if s.Kind == barrierWait {
			return nil
		}
		done[s] = true
		if (s.Kind == stepRemove || s.Kind == stepRecover || s.Kind == stepSettle) && !b.beforeRemove() {
			return nil
		}
		if s.Kind == stepCreate || s.Kind == stepRelease || s.Kind == stepStart {
			var ok bool
			if g, ok, err = b.authorize(g); err != nil {
				return err
			}
			if !ok && !g.Revoked {
				return nil
			}
			if !ok {
				continue
			}
		}
		if g, err = b.do(ctx, g, s); err != nil {
			return err
		}
		if b.held != nil {
			return nil
		}
	}
}

// authorize is Workers.authorize for a barrier group.
func (b *workerBarriers) authorize(g barrierGroup) (barrierGroup, bool, error) {
	switch err := b.check(g.Authority); {
	case err == nil:
		return g, true, nil
	case errors.Is(err, auth.ErrUnavailable):
		return g, false, nil
	case errors.Is(err, auth.ErrUnauthenticated) || errors.Is(err, auth.ErrPermissionDenied):
		next, revokeErr := b.store.RevokeBarrierGroup(g.Job.ID, g.Revision, err, b.now())
		if revokeErr != nil {
			return g, false, revokeErr
		}
		return next, false, nil
	default:
		return g, false, err
	}
}

// barrierPending returns the tokens of m's Intended effects.
func barrierPending(g barrierGroup, m barrierMember) []barrierToken {
	var out []barrierToken
	for e, x := range map[barrierEffect]store.WorkerEffectState{
		barrierCreate: m.Create, barrierRelease: m.Release, barrierStartRank: m.Start,
		barrierCancel: m.Cancel, barrierRemove: m.Remove, barrierRecover: m.Recover,
	} {
		if x.State == store.WorkerIntended {
			out = append(out, barrierToken{Job: g.Job.ID, Rank: m.Rank, Effect: e, Seq: x.Seq, Gen: x.Gen})
		}
	}
	return out
}

// observe records each original read before another backend call. A failed
// Store record keeps the exact result, identity, cause and operation time;
// another read must not substitute for that unfinished record.
func (b *workerBarriers) observe(ctx context.Context, g barrierGroup) (barrierGroup, error) {
	for i := range g.Members {
		m := g.Members[i]
		if m.Pod == (backend.WorkerResource{}) || m.Removed || m.Vanished || m.Stop != backend.WorkerStopUnproven {
			continue
		}
		if b.unsettled() {
			return g, nil
		}
		ref := cloneBarrierReference(barrierRef(g, m))
		call, cancel := context.WithTimeout(ctx, workerCallTimeout)
		got, cause := b.backend.ObserveBarrier(call, cloneBarrierReference(ref))
		cancel()
		got = cloneBarrierObservation(got)
		result := &barrierResult{group: g.Job.ID, rank: m.Rank, reference: ref, observed: got, cause: cause, at: b.now(), revision: g.Revision}
		switch {
		case errors.Is(cause, backend.ErrWorkerUnsettled):
			result.kind = barrierResultHeldRead
			b.held = result
			return g, b.latchHeldRead()
		case errors.Is(cause, backend.ErrWorkerVanished):
			result.kind = barrierResultVanished
		case cause != nil:
			continue
		case backend.ValidateWorkerBarrier(ref, got) != nil:
			continue
		default:
			result.kind = barrierResultObservation
		}
		b.pending = result
		next, err := b.commitPending()
		if err != nil {
			return g, err
		}
		g = next
	}
	return g, nil
}

// do takes step s of g and returns the next view.
func (b *workerBarriers) do(ctx context.Context, g barrierGroup, s barrierStep) (barrierGroup, error) {
	switch s.Kind {
	case stepDecideStart:
		return b.store.DecideWorkerBarrier(g.Job.ID, g.Revision, barrierStart, b.now())
	case stepDecideAbort:
		return b.store.DecideWorkerBarrier(g.Job.ID, g.Revision, barrierAbort, b.now())
	case stepSettle:
		return b.store.SettleBarrierGroup(g.Job.ID, g.Revision, b.now())
	case stepResolve:
		c := backend.WorkerCreate{Group: g.Group, Rank: s.Rank}
		call, cancel := context.WithTimeout(ctx, workerCallTimeout)
		// A resource returned with an error was found and verified before
		// the error. It and the error's class are recorded before any later
		// native work, and a failed record stops the Step with both causes.
		// Not found is not absence: the Create stays unknown either way.
		got, callErr := b.backend.ResolveWorker(call, c)
		cancel()
		raw := got
		got, ok := barrierMerge(g.Members[s.Rank], c, got)
		out := resolveOutcome{Class: resolveClassOf(callErr), Refused: !ok, Unsettled: errors.Is(callErr, backend.ErrWorkerUnsettled)}
		b.pending = &barrierResult{group: g.Job.ID, revision: g.Revision, rank: s.Rank,
			kind: barrierResultResolve, createPlan: c, rawCreated: raw, created: got, outcome: out, cause: callErr, at: b.now()}
		return b.commitPending()
	}
	e := map[barrierKind]barrierEffect{
		stepCreate: barrierCreate, stepRelease: barrierRelease, stepStart: barrierStartRank,
		stepCancel: barrierCancel, stepRemove: barrierRemove, stepRecover: barrierRecover,
	}[s.Kind]
	tok, g, err := b.store.IntendBarrier(g.Job.ID, g.Revision, s.Rank, e, b.now())
	if err != nil {
		return g, err
	}
	ref := barrierEffectReference(g, g.Members[s.Rank], e)
	got, callErr := b.call(ctx, g, s.Rank, e, ref)
	got.Unsettled = errors.Is(callErr, backend.ErrWorkerUnsettled)
	b.pending = &barrierResult{kind: barrierResultEffect, group: g.Job.ID, token: tok, reference: ref,
		receipt: got, cause: callErr, at: b.now()}
	return b.commitPending()
}

// call makes the one original backend call of effect e on rank, after its
// intent is recorded.
func (b *workerBarriers) call(ctx context.Context, g barrierGroup, rank uint8, e barrierEffect, ref backend.WorkerReference) (barrierReceipt, error) {
	m := g.Members[rank]
	ctx, cancel := context.WithTimeout(ctx, workerCallTimeout)
	defer cancel()
	var got barrierReceipt
	var err error
	switch e {
	case barrierCreate:
		got.Created, err = b.backend.CreateWorker(ctx, backend.WorkerCreate{Group: g.Group, Rank: rank})
		got.Refused = errors.Is(err, backend.ErrWorkerCreateRefused) && !errors.Is(err, backend.ErrWorkerUnsettled)
	case barrierRelease:
		got.Released, err = b.backend.ReleaseWorker(ctx, ref)
	case barrierStartRank:
		got.Started, err = b.backend.StartWorker(ctx, ref, m.StartToken)
	case barrierCancel:
		got.Canceled, err = b.backend.CancelWorker(ctx, ref)
	case barrierRemove:
		got.Removed, err = b.backend.RemoveWorker(ctx, ref)
	case barrierRecover:
		r := backend.WorkerRecovery{Ref: ref, Evidence: barrierRecoveryEvidence(g, m), RemoveSent: m.RemoveSent}
		if err = backend.ValidateWorkerRecovery(r); err == nil {
			got.Removed, err = b.backend.RecoverWorkerRemoval(ctx, r)
		}
	}
	return got, err
}

// barrierResult keeps the original return, its native cause and first Store
// failure together. Retrying this record never retries the native operation.
type barrierResultKind uint8

const (
	barrierResultEffect barrierResultKind = iota + 1
	barrierResultResolve
	barrierResultObservation
	barrierResultVanished
	barrierResultHeldRead
)

func (k barrierResultKind) isObservation() bool {
	return k == barrierResultObservation || k == barrierResultVanished
}

type barrierResult struct {
	kind             barrierResultKind
	latched          bool
	createPlan       backend.WorkerCreate
	observed         backend.WorkerBarrier
	group            string
	token            barrierToken
	reference        backend.WorkerReference
	receipt          barrierReceipt
	cause, recordErr error
	at               time.Time
	revision         uint64
	rank             uint8
	rawCreated       backend.WorkerCreated
	created          backend.WorkerCreated
	outcome          resolveOutcome
}

func (b *workerBarriers) commitPending() (barrierGroup, error) {
	p := b.pending
	var g barrierGroup
	var err error
	switch p.kind {
	case barrierResultResolve:
		g, err = b.store.RecordWorkerResolved(p.group, p.revision, p.rank, p.created, p.outcome, p.at)
	case barrierResultEffect:
		g, err = b.store.RecordBarrierEffect(p.token, p.receipt, workerCallError(p.cause), p.at)
	case barrierResultObservation, barrierResultVanished:
		g, err = b.recordPendingObservation(p)
	default:
		err = ErrInvalid
	}
	if err != nil && p.kind == barrierResultResolve {
		// An ambiguous original commit can change the group's revision. Only
		// an exact typed readback of that original observation may discharge
		// it; never issue Resolve again or rebase an unproven result.
		if current, readErr := b.store.BarrierGroup(p.group); readErr == nil && int(p.rank) < len(current.Members) {
			m := current.Members[p.rank]
			if m.Pod == p.created.Pod && m.Service == p.created.Service && m.Resolve.Class == p.outcome.Class && m.Resolve.Refused == p.outcome.Refused && m.Resolve.Unsettled == p.outcome.Unsettled && m.Resolve.At.Equal(p.at) {
				g, err = current, nil
			}
		}
	}
	if err != nil {
		if p.recordErr == nil {
			p.recordErr = err
		}
		return g, errors.Join(p.recordErr, err, p.cause)
	}
	// Store acceptance does not discharge the backend's original local owner.
	// Only the typed producer boundary distinguishes unsettled custody from a
	// positively returned operation whose native effect is still unknown.
	if errors.Is(p.cause, backend.ErrWorkerUnsettled) {
		b.held = p
	}
	b.pending = nil
	return g, nil
}

func workerCallError(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func barrierEffectReference(g barrierGroup, m barrierMember, e barrierEffect) backend.WorkerReference {
	r := barrierRef(g, m)
	if e == barrierRemove && !m.RemoveHadPod || e == barrierRecover && !m.RecoverHadPod {
		r.Pod = backend.WorkerResource{}
	}
	return r
}

// latchHeldRead retries only the trusted quarantine write. It neither reads the
// backend again nor treats a later Store revision as ownership settlement.
func (b *workerBarriers) latchHeldRead() error {
	p := b.held
	current, err := b.store.BarrierGroup(p.group)
	if err == nil {
		_, err = b.store.LatchWorkerUnsettled(p.group, current.Revision, p.rank, p.reference, p.at)
	}
	if err != nil {
		if p.recordErr == nil {
			p.recordErr = err
		}
		return errors.Join(p.recordErr, p.cause)
	}
	p.latched = true
	return nil
}

// barrierServiceOnlyRecovery qualifies only an original Pod refusal with a
// known Service whose original, locally settled removal was actually sent.
// No Pod exists, so Pod-based Vanished or native Stop is not a prerequisite.
// This live selector is never used to reinterpret a late original result.
func barrierServiceOnlyRecovery(g barrierGroup, m barrierMember) bool {
	if g.Done || g.Group.Spec.Mode != backend.WorkerTCP || m.Rank != 0 || len(g.Members) < 2 {
		return false
	}
	if m.Service != g.Members[0].Service || m.Service.UID == "" || m.ServiceRemoved {
		return false
	}
	if m.LocalUnsettled || !m.Refused || m.Pod != (backend.WorkerResource{}) || m.RemoveHadPod || m.RecoverHadPod {
		return false
	}
	if !m.RemoveSent || m.Release.State != store.WorkerNone || m.Remove.State != store.WorkerUnknown {
		return false
	}
	if m.Create.State != store.WorkerUnknown && m.Create.State != store.WorkerRecorded {
		return false
	}
	for _, member := range g.Members {
		if member.LocalUnsettled {
			return false
		}
	}
	return barrierOthersRetired(g)
}

// barrierRecoveryEvidence never persists the ephemeral no-Pod proof as Stop.
// A known Pod always needs its recorded native NeverBound or Terminated fact.
func barrierRecoveryEvidence(g barrierGroup, m barrierMember) backend.WorkerStop {
	if m.Stop == backend.WorkerNeverBound || m.Stop == backend.WorkerTerminated {
		return m.Stop
	}
	if barrierServiceOnlyRecovery(g, m) {
		return backend.WorkerNeverReleased
	}
	return backend.WorkerStopUnproven
}

func cloneBarrierReference(ref backend.WorkerReference) backend.WorkerReference {
	ref.Group.Execution.Task = cloneTask(ref.Group.Execution.Task)
	return ref
}
func cloneBarrierObservation(observation backend.WorkerBarrier) backend.WorkerBarrier {
	if observation.Exit != nil {
		exit := *observation.Exit
		exit.StartedAt = exit.StartedAt.UTC()
		exit.FinishedAt = exit.FinishedAt.UTC()
		observation.Exit = &exit
	}
	return observation
}
func sameBarrierObservation(a, b backend.WorkerBarrier) bool {
	return reflect.DeepEqual(cloneBarrierObservation(a), cloneBarrierObservation(b))
}

// recordPendingObservation never rebases an older result over newer facts.
// Revision changes alone (for example a revoke) do not change immutable native
// identity and need not discard a positive original read.
func (b *workerBarriers) recordPendingObservation(p *barrierResult) (barrierGroup, error) {
	current, err := b.store.BarrierGroup(p.group)
	if err != nil {
		return barrierGroup{}, err
	}
	if int(p.rank) >= len(current.Members) {
		return current, store.ErrWorkerGroupStale
	}
	m := current.Members[p.rank]
	if !reflect.DeepEqual(barrierRef(current, m), p.reference) {
		return current, backend.ErrIdentityMismatch
	}
	if p.kind == barrierResultVanished && m.Vanished {
		// Vanished has no separate persisted timestamp. This acknowledges only
		// the same immutable resource's sticky fact, never a particular event time.
		return current, nil
	}
	if p.kind == barrierResultObservation && m.Observed != nil && m.ObservedAt.Equal(p.at) && sameBarrierObservation(*m.Observed, p.observed) {
		return current, nil
	}
	if !m.ObservedAt.IsZero() && !m.ObservedAt.Before(p.at) {
		return current, store.ErrWorkerGroupStale
	}
	switch p.kind {
	case barrierResultObservation:
		return b.store.RecordWorkerBarrier(p.group, current.Revision, p.rank, p.observed, p.at)
	case barrierResultVanished:
		return b.store.RecordWorkerVanished(p.group, current.Revision, p.rank, p.at)
	}
	return current, ErrInvalid
}
