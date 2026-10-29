package control

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

// Logs reads one rank's own log of one group attempt visible to actor,
// starting at offset within that rank's log. There is no merged group log
// and no offset shared between ranks. limit is 1 through
// backend.MaxChunkBytes.
//
// Logs reads the rank's original Pod by its recorded UID. A rank with no
// original Pod, or whose Pod is retired, has no readable log and returns
// ErrLogsUnavailable. Complete is reported only after the rank's native exit
// receipt is recorded; until then a backend's Complete is cleared.
//
// The read can outlast the actor's authority. After it returns, and before
// any chunk or read error is returned, Logs checks ctx and the actor's
// current authority again as streamed job logs do: a canceled ctx, an
// unavailable Engine, a revoked credential or a changed role returns that
// error and no chunk. The read is not repeated and nothing is revoked.
func (w *Workers) Logs(ctx context.Context, actor auth.Actor, id, attempt string, rank uint32, offset int64, limit int) (backend.LogChunk, error) {
	if offset < 0 || offset > backend.MaxLogBytes || limit < 1 || limit > backend.MaxChunkBytes {
		return backend.LogChunk{}, fmt.Errorf("%w: log offset or limit out of range", ErrInvalid)
	}
	g, err := w.group(actor, id, attempt)
	if err != nil {
		return backend.LogChunk{}, err
	}
	if rank >= uint32(len(g.Members)) {
		return backend.LogChunk{}, fmt.Errorf("%w: worker rank out of range", ErrInvalid)
	}
	m := g.Members[rank]
	if m.Pod.UID == "" || m.Removed {
		return backend.LogChunk{}, ErrLogsUnavailable
	}
	t, ok := w.targets[g.Job.Target]
	if !ok || t.target.Binding != g.Job.TargetBinding {
		return backend.LogChunk{}, ErrLogsUnavailable
	}
	release, err := w.BeginLogs(actor, id, attempt, rank)
	if err != nil {
		return backend.LogChunk{}, err
	}
	held := false
	defer func() {
		if !held {
			release()
		}
	}()
	ref := workerReference(g, m)
	call, cancel := context.WithTimeout(ctx, workerCallTimeout)
	defer cancel()
	chunk, err := t.backend.WorkerLogs(call, ref, offset, limit)
	if errors.Is(err, backend.ErrWorkerUnsettled) {
		// Keep the exact result and its original lease. Neither a canceled
		// request nor the outer transport's completion proves local closure.
		held = true
		w.logMu.Lock()
		w.logClosed[id] = true
		w.logFailures[id] = append(w.logFailures[id], &workerLogResult{reference: ref, chunk: chunk, cause: err, release: release, at: w.now()})
		w.logMu.Unlock()
		w.mu.Lock()
		if latchErr := w.latchLogFailures(id); latchErr != nil {
			err = errors.Join(err, latchErr)
		}
		w.mu.Unlock()
	}
	if err := w.current(ctx, actor); err != nil {
		return backend.LogChunk{}, err
	}
	if err != nil {
		return backend.LogChunk{}, fmt.Errorf("%w: %w", ErrLogsUnavailable, err)
	}
	if len(chunk.Data) > limit || chunk.NextOffset != offset+int64(len(chunk.Data)) || chunk.NextOffset > backend.MaxLogBytes {
		return backend.LogChunk{}, fmt.Errorf("%w: invalid worker log chunk", backend.ErrIdentityMismatch)
	}
	if m.Observed == nil || m.Observed.ExitCode == nil {
		chunk.Complete = false
	}
	chunk.Data = append([]byte(nil), chunk.Data...)
	return chunk, nil
}

// current reports why actor may no longer receive a read it was authorized
// to start: ctx's cause, the Engine's refusal or unavailability, or
// auth.ErrPermissionDenied after a role change.
func (w *Workers) current(ctx context.Context, actor auth.Actor) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	fresh, err := w.c.engine.Check(actor, auth.Read)
	if err != nil {
		return err
	}
	if fresh.Role() != actor.Role() {
		return auth.ErrPermissionDenied
	}
	return nil
}

// BeginLogs reserves one group's log lifetime. Call the returned release once
// the original read and its enclosing response have returned. A group's first
// retirement attempt permanently fences new readers and waits for these owners.
func (w *Workers) BeginLogs(actor auth.Actor, id, attempt string, rank uint32) (func(), error) {
	if err := w.c.beginExternal(); err != nil {
		return nil, err
	}
	accepted := false
	defer func() {
		if !accepted {
			w.c.endExternal()
		}
	}()
	// Step owns a whole native-call/result-record pass. Read again under
	// that same lock so a pre-removal snapshot cannot admit a late reader.
	w.mu.Lock()
	defer w.mu.Unlock()
	if b := w.barriers[id]; b != nil && (b.pending != nil || b.held != nil) {
		return nil, ErrLogsUnavailable
	}
	g, err := w.group(actor, id, attempt)
	if err != nil {
		return nil, err
	}
	if rank >= uint32(len(g.Members)) {
		return nil, ErrInvalid
	}
	for _, member := range g.Members {
		if member.Barrier != nil && member.Barrier.LocalUnsettled {
			return nil, ErrLogsUnavailable
		}
	}
	m := g.Members[rank]
	if m.Pod.UID == "" || m.Removed {
		return nil, ErrLogsUnavailable
	}
	w.logMu.Lock()
	defer w.logMu.Unlock()
	if w.logClosed[id] || w.logs[id] >= 32 {
		return nil, ErrLogsUnavailable
	}
	w.logs[id]++
	accepted = true
	var once sync.Once
	return func() {
		once.Do(func() {
			w.logMu.Lock()
			defer w.logMu.Unlock()
			w.logs[id]--
			if w.logs[id] == 0 {
				delete(w.logs, id)
			}
			w.c.endExternal()
		})
	}, nil
}

func (w *Workers) fenceLogs(id string) bool {
	w.logMu.Lock()
	defer w.logMu.Unlock()
	w.logClosed[id] = true
	return w.logs[id] == 0
}

// workerReference includes the same original rank-zero Service for every TCP
// member, as create, barrier observations and start token delivery require.
func workerReference(g store.WorkerGroup, m store.WorkerMember) backend.WorkerReference {
	r := backend.WorkerReference{Group: g.Group, Rank: m.Rank, Pod: m.Pod}
	if g.Group.Spec.Mode == backend.WorkerTCP && len(g.Members) > 0 && g.Members[0].Barrier != nil {
		r.Service = g.Members[0].Barrier.Service
	}
	return r
}

// workerLogResult retains an unsettled original read and its exact caller
// lease. The error chain owns the failed native reader; no Close is retried.
type workerLogResult struct {
	at        time.Time
	latched   bool
	recordErr error
	reference backend.WorkerReference
	chunk     backend.LogChunk
	cause     error
	release   func()
}

func (w *Workers) unsettledLogs(id string) bool {
	w.logMu.Lock()
	defer w.logMu.Unlock()
	return len(w.logFailures[id]) != 0
}

// latchLogFailures runs under the reconciliation/admission lock. Each original
// read owner remains held even after this monotonic Store fence commits.
func (w *Workers) latchLogFailures(id string) error {
	w.logMu.Lock()
	owners := append([]*workerLogResult(nil), w.logFailures[id]...)
	w.logMu.Unlock()
	var failures []error
	for _, p := range owners {
		if p.latched {
			continue
		}
		g, err := w.c.store.WorkerGroup(id)
		if err == nil {
			_, err = w.c.store.LatchWorkerUnsettled(id, g.Revision, p.reference.Rank, p.reference, p.at)
		}
		if err != nil {
			if p.recordErr == nil {
				p.recordErr = err
			}
			failures = append(failures, errors.Join(p.recordErr, p.cause))
			continue
		}
		p.latched = true
	}
	return errors.Join(failures...)
}
