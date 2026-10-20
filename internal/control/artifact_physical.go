package control

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tmc/skygo/internal/artifact"
)

// artifactPhysical has a dedicated, short cache lock. No holder calls Store,
// Files, Local, service.mu, auth, a writer, or a waiting operation. The original
// maintenance worker alone performs physicalQuantum; pending prevents accidental
// concurrent dispatch. Registration survives every yielded quantum.
type artifactPhysical struct {
	mu                             sync.Mutex
	frame                          *physicalFrame
	cached                         *physicalObservation
	points                         physicalPoints
	pending, wantSample, wantClose bool
	serial                         uint64
}

type physicalFrame struct {
	serial      uint64
	scan        *artifact.PhysicalScan
	before      physicalCohort
	invalidated bool
	begun       time.Time
	uncertain   error
}

type physicalObservation struct {
	sample artifact.PhysicalSample
	cohort physicalCohort
}

type physicalPoints struct {
	databaseBytes, databaseFree, artifactFree  *uint64
	databaseAt, databaseFreeAt, artifactFreeAt time.Time
}

// physicalQuantumResult is a returned synchronous worker result, not evidence
// that a missing or timed-out worker joined. Registered and Held are independent:
// an ordinary yielded cursor owns descriptors without an active permit.
type physicalQuantumResult struct {
	Serial           uint64
	Registered, Held bool
	Complete         bool
	Err              error
}

func (c *Control) physicalChanged() {
	if c == nil || c.artifacts == nil {
		return
	}
	p := &c.artifacts.physical
	p.mu.Lock()
	if p.frame != nil {
		p.frame.invalidated = true
	}
	p.cached = nil
	p.wantSample = true
	p.mu.Unlock()
}

func (c *Control) requestPhysicalClose() {
	if c == nil || c.artifacts == nil {
		return
	}
	p := &c.artifacts.physical
	p.mu.Lock()
	p.wantClose = true
	p.wantSample = false
	p.cached = nil
	if p.frame != nil {
		p.frame.invalidated = true
	}
	p.mu.Unlock()
}

func (c *Control) physicalRegistered() bool {
	if c == nil || c.artifacts == nil {
		return false
	}
	p := &c.artifacts.physical
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.frame != nil || p.pending
}

func (c *Control) physicalWantSample() bool {
	if c == nil || c.artifacts == nil {
		return false
	}
	p := &c.artifacts.physical
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.pending && ((p.wantClose && p.frame != nil) || (!p.wantClose && (p.wantSample || p.frame != nil)))
}

// cachedLinkGrowthCheck is invoked under Candidate.mu -> Files.mu. It reads
// ONLY the dedicated cache. In particular it cannot refresh Store, Local, Files,
// auth or free space under that lock. It is admission, never a capacity lease.
func (c *Control) cachedLinkGrowthCheck() error {
	if c == nil || c.artifacts == nil {
		return ErrUnsupported
	}
	p := &c.artifacts.physical
	p.mu.Lock()
	defer p.mu.Unlock()
	reason := physicalGrowthCondition(p, time.Now())
	if reason != "" {
		p.wantSample = !p.wantClose
		return fmt.Errorf("%w: %s", artifact.ErrFilesBusy, reason)
	}
	return nil
}

// uploadLinkGrowthCheck is used ONLY at Files' actual new-link callback.
// Check and invalidation share the dedicated cache lock; it acquires no owner,
// Files, Store, Local or auth lock and performs no I/O. Ordinary readiness must
// not invalidate itself this way. This decision grants no future capacity lease.
func (c *Control) uploadLinkGrowthCheck() error {
	if c == nil || c.artifacts == nil {
		return ErrUnsupported
	}
	p := &c.artifacts.physical
	p.mu.Lock()
	defer p.mu.Unlock()
	if reason := physicalGrowthCondition(p, time.Now()); reason != "" {
		p.wantSample = !p.wantClose
		return fmt.Errorf("%w: %s", artifact.ErrFilesBusy, reason)
	}
	if p.frame != nil {
		p.frame.invalidated = true
	}
	p.cached = nil
	p.wantSample = !p.wantClose
	return nil
}

func physicalFresh(at, now time.Time) bool {
	return !at.IsZero() && !now.Before(at) && !now.UTC().Before(at.UTC()) && now.Sub(at) <= 5*time.Second && now.UTC().Sub(at.UTC()) <= 5*time.Second
}

func physicalGrowthCondition(p *artifactPhysical, now time.Time) string {
	if p.wantClose {
		return "artifact_closing"
	}
	q := p.points
	if q.databaseBytes == nil || q.databaseFree == nil || q.artifactFree == nil || !physicalFresh(q.databaseAt, now) || !physicalFresh(q.databaseFreeAt, now) || !physicalFresh(q.artifactFreeAt, now) {
		return "artifact_points_unavailable"
	}
	if *q.databaseBytes >= admissionDatabaseBytes || *q.databaseFree < admissionFreeBytes || *q.artifactFree < admissionFreeBytes {
		return "artifact_pressure"
	}
	o := p.cached
	if o == nil || o.sample.Status != artifact.PhysicalIntervalComplete || !physicalFresh(o.sample.EndedAt, now) || o.sample.StartedAt.IsZero() || o.sample.EndedAt.Before(o.sample.StartedAt) || o.sample.EndedAt.Sub(o.sample.StartedAt) > 2*time.Minute {
		return "artifact_inventory_unavailable"
	}
	if o.sample.AllocatedBytes == nil || o.sample.Inodes == nil || *o.sample.AllocatedBytes > o.cohort.allocated || *o.sample.Inodes > o.cohort.files {
		return "artifact_accounting_hold"
	}
	return ""
}

// tryPhysicalIO uses the same four original owners. Only an already registered
// scan can acquire its close quantum after external admission has stopped.
func (c *Control) tryPhysicalIO(closing bool) (*artifactIO, error) {
	s := c.artifacts
	if s == nil {
		return nil, ErrUnsupported
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing && !closing {
		return nil, ErrUnavailable
	}
	if len(s.calls)+len(s.workspaceIO) >= 4 {
		return nil, ErrArtifactBusy
	}
	owner := &artifactIO{service: s}
	s.workspaceIO[owner] = struct{}{}
	return owner, nil
}

// physicalQuantum executes ONE finite Begin/Step or original Close on the SAME
// maintenance worker. No goroutine, replacement cursor, queue, or retry is made.
// The worker must deliver this actual result before dispatching another quantum.
func (c *Control) physicalQuantum(ctx context.Context) (out physicalQuantumResult) {
	if c == nil || c.artifacts == nil {
		out.Err = ErrUnsupported
		return
	}
	s := c.artifacts
	p := &s.physical
	p.mu.Lock()
	if p.pending {
		p.mu.Unlock()
		out.Err = ErrArtifactBusy
		return
	}
	if p.wantClose && p.frame == nil {
		p.mu.Unlock()
		return
	}
	if p.frame != nil && p.frame.uncertain != nil {
		out.Serial = p.frame.serial
		out.Registered = true
		out.Held = true
		out.Err = p.frame.uncertain
		p.mu.Unlock()
		return
	}
	p.pending = true
	if p.frame == nil {
		p.serial++
		p.frame = &physicalFrame{serial: p.serial, begun: time.Now()}
	}
	frame := p.frame
	closing := p.wantClose || frame.invalidated || time.Since(frame.begun) > 2*time.Minute
	out.Serial = frame.serial
	p.mu.Unlock()
	// The frame is already registered BEFORE Store/Local cohort copies.
	defer func() { p.mu.Lock(); p.pending = false; out.Registered = p.frame != nil; p.mu.Unlock() }()
	if closing && frame.scan == nil {
		out.Err = c.closePhysicalFrame(frame)
		return
	}
	owner, err := c.tryPhysicalIO(closing && frame.scan != nil)
	if err != nil {
		out.Err = err
		return
	}
	var ioErr error
	defer func() { owner.finish(ioErr) }()
	c.samplePhysicalDatabase()
	if closing {
		out.Err = c.closePhysicalFrame(frame)
		ioErr = out.Err
		out.Held = errors.Is(out.Err, artifact.ErrFileCloseUncertain)
		c.samplePhysicalDatabase()
		return
	}
	// Independent DB points are obtained even if the census fails. Their actual
	// timestamps are never renewed by the inventory's EndedAt.
	before, err := c.physicalCohort()
	if err != nil {
		out.Err = err
		out.Err = errors.Join(out.Err, c.closePhysicalFrame(frame))
		ioErr = out.Err
		out.Held = errors.Is(out.Err, artifact.ErrFileCloseUncertain)
		c.samplePhysicalDatabase()
		return
	}
	if frame.scan == nil {
		frame.before = before
		frame.scan, err = s.files.BeginPhysicalSample()
		if err != nil {
			c.replaceArtifactPoint(artifact.PhysicalSample{})
			out.Err = errors.Join(err, c.closePhysicalFrame(frame))
			ioErr = out.Err
			out.Held = errors.Is(out.Err, artifact.ErrFileCloseUncertain)
			c.samplePhysicalDatabase()
			return
		}
	} else if before.digest != frame.before.digest {
		out.Err = errors.Join(ErrUnavailable, c.closePhysicalFrame(frame))
		ioErr = out.Err
		out.Held = errors.Is(out.Err, artifact.ErrFileCloseUncertain)
		c.samplePhysicalDatabase()
		return
	}
	// A concurrent pre-invalidation cannot be cleared by this worker.
	p.mu.Lock()
	invalid := frame.invalidated || p.wantClose
	p.mu.Unlock()
	if invalid {
		out.Err = c.closePhysicalFrame(frame)
		ioErr = out.Err
		out.Held = errors.Is(out.Err, artifact.ErrFileCloseUncertain)
		c.samplePhysicalDatabase()
		return
	}
	sample, err := frame.scan.Step(ctx)
	c.samplePhysicalDatabase()
	c.replaceArtifactPoint(sample)
	if errors.Is(err, artifact.ErrFileCloseUncertain) {
		p.mu.Lock()
		frame.uncertain = err
		p.cached = nil
		p.mu.Unlock()
		ioErr = err
		out.Err = err
		out.Held = true
		return
	}
	after, cohortErr := c.physicalCohort()
	if err == nil && cohortErr == nil && sample.Status == artifact.PhysicalIntervalComplete {
		cohortErr = physicalNamesMatch(sample, after)
	}
	terminal := err != nil || cohortErr != nil || sample.Status != artifact.PhysicalPartial || before.digest != after.digest
	if !terminal {
		return
	}
	// Keep the SAME registered frame until positive Close AND final promotion
	// are integrated atomically. Clearing it before promotion loses a concurrent
	// PhysicalChanged callback between close return and cache publication.
	closeErr := frame.scan.Close()
	out.Err = errors.Join(err, cohortErr, closeErr)
	ioErr = out.Err
	out.Held = errors.Is(out.Err, artifact.ErrFileCloseUncertain)
	now := time.Now()
	p.mu.Lock()
	if closeErr != nil {
		frame.uncertain = closeErr
		p.cached = nil
		p.mu.Unlock()
		return
	}
	if p.frame != frame {
		p.mu.Unlock()
		out.Err = ErrInvalid
		return
	}
	valid := out.Err == nil && sample.Status == artifact.PhysicalIntervalComplete && before.digest == after.digest && sample.Root == s.root && physicalFresh(sample.EndedAt, now) && now.Sub(frame.begun) >= 0 && now.Sub(frame.begun) <= 2*time.Minute && !now.UTC().Before(frame.begun.UTC()) && now.UTC().Sub(frame.begun.UTC()) <= 2*time.Minute
	if valid && !frame.invalidated && !p.wantClose {
		p.cached = &physicalObservation{sample: sample, cohort: after}
		p.wantSample = false
		out.Complete = true
	} else {
		p.cached = nil
		p.wantSample = !p.wantClose
	}
	p.frame = nil
	p.mu.Unlock()
	return
}

func physicalUint(n uint64) *uint64 { return &n }

func (c *Control) closePhysicalFrame(frame *physicalFrame) error {
	p := &c.artifacts.physical
	var err error
	if frame.scan != nil {
		err = frame.scan.Close()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.frame != frame {
		return ErrInvalid
	}
	if err != nil {
		p.cached = nil
		frame.uncertain = err
		return err
	}
	p.frame = nil
	p.cached = nil
	p.wantSample = !p.wantClose
	return nil
}

// Physical fence hashing uses copied values only. This helper is deliberately
// separate from Store's own purpose-separated digest, which remains authoritative
// for its validated metadata projection.
func physicalHash(value any) ([32]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(append([]byte("skygo-effective-artifact-cohort-v1\x00"), data...)), nil
}

// replaceArtifactPoint integrates EVERY actual returned component, including
// failure. An unavailable/wrong-root/stale new point cannot fall back to an old
// fresh value. Independent DB points and their timestamps remain untouched.
func (c *Control) replaceArtifactPoint(sample artifact.PhysicalSample) {
	now := time.Now()
	var free *uint64
	var at time.Time
	if sample.FreeBytes != nil && sample.Root == c.artifacts.root && physicalFresh(sample.FreeObservedAt, now) {
		free = physicalUint(*sample.FreeBytes)
		at = sample.FreeObservedAt
	}
	p := &c.artifacts.physical
	p.mu.Lock()
	p.points.artifactFree = free
	p.points.artifactFreeAt = at
	p.mu.Unlock()
}
