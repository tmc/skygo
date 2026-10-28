package control

import (
	"errors"
	"fmt"
	"time"
)

// Physical methods below adapt the installed engine to the ONE existing
// maintenance worker. Cache and service locks never nest. The worker may perform
// the engine's bounded read-only Store Views, never a Store mutation.
func (c *Control) physicalCloseWanted() bool {
	if c.artifacts == nil {
		return false
	}
	p := &c.artifacts.physical
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.wantClose
}

func physicalRequestShape(r maintenanceRequest) bool {
	t := r.target
	return r.readerRecovery == nil && r.downloadClose == nil && r.closeOwner == nil && r.artifactDelete == nil && r.workspaceRetirement == nil && r.physicalOwner != nil && r.record == nil && r.directory == "" && r.usage == nil && r.reusable == 0 && r.reservedBytes == 0 && r.reservedFiles == 0 &&
		t.Name == "" && t.Backend == nil && t.Kind == "" && t.Binding == "" && t.Advice == nil && t.Labels == nil && t.AcceleratorNames == nil &&
		!t.SupportsResourceLimits && !t.SupportsNodeSelector && !t.SupportsRetry && !t.SupportsPersistentEnvironments && !t.SupportsEnvironmentArtifacts && t.Retention == nil && t.DataPolicy == nil && !t.SupportsRemoteData && !t.SupportsDataMounts && !t.SupportsRemoteDataRetry && t.OutputStorage == 0 && t.OutputReservation == 0
}

func physicalReturnedHeld(q physicalQuantumResult) bool {
	return q.Held || q.Registered && q.Err != nil && !errors.Is(q.Err, ErrArtifactBusy)
}

func physicalMaintenanceHeld(m *maintenanceState) bool {
	return m.physicalResult != nil && m.physicalResult.result.physical != nil && physicalReturnedHeld(*m.physicalResult.result.physical)
}

func (c *Control) wantPhysicalMaintenance(m *maintenanceState, now time.Time) bool {
	if c.artifacts == nil || physicalMaintenanceHeld(m) || c.physicalCloseWanted() {
		return false
	}
	return c.physicalWantSample() || !now.Before(m.physicalDue)
}

func (c *Control) dispatchPhysical(m *maintenanceState) error {
	if c.artifacts == nil {
		return ErrUnsupported
	}
	if m.pending != nil || physicalMaintenanceHeld(m) {
		return ErrArtifactBusy
	}
	if m.serial == ^uint64(0) {
		return ErrUnavailable
	}
	closing := c.physicalCloseWanted()
	s := c.artifacts
	request := maintenanceRequest{sequence: m.serial + 1, physicalOwner: c, physicalClose: closing}
	s.mu.Lock()
	if s.terminalClosed || s.closing && !closing {
		s.mu.Unlock()
		return nil
	}
	if s.physicalWorkPending {
		s.mu.Unlock()
		return fmt.Errorf("%w: previous physical worker request remains held", ErrUnavailable)
	}
	m.pending = &request
	m.serial = request.sequence
	s.physicalWorkPending = true
	s.artifactChangedLocked()
	s.mu.Unlock()
	m.physicalDue = time.Now().Add(time.Second)
	m.input <- request
	return nil
}

func (c *Control) integratePhysical(m *maintenanceState, r maintenanceResult) error {
	if c.artifacts == nil || m.pending == nil || m.received == nil || m.received.request != m.pending || m.pending.sequence != r.request.sequence || m.pending.physicalOwner != c || r.request.physicalOwner != c || m.pending.physicalClose != r.request.physicalClose || !physicalRequestShape(*m.pending) || !physicalRequestShape(r.request) || r.readerRecovery != nil || r.downloadClose != nil || r.deletion != nil || r.retirement != nil || r.sample != nil || r.physical == nil || r.err != nil {
		return fmt.Errorf("%w: physical result has no matching original request", ErrUnavailable)
	}
	q := r.physical
	if (q.Registered || q.Complete || q.Err != nil) && q.Serial == 0 || q.Held && (!q.Registered || q.Err == nil) || q.Complete && (q.Registered || q.Held || q.Err != nil) || m.physicalSerial != 0 && q.Serial != m.physicalSerial {
		return fmt.Errorf("%w: physical result changed its original frame", ErrUnavailable)
	}
	// The original received pair moves here BEFORE pending is cleared. Failed
	// matching/serial checks leave the existing F1 received record untouched.
	m.physicalResult = m.received
	if r.request.physicalClose && q.Registered && errors.Is(q.Err, ErrArtifactBusy) && !physicalReturnedHeld(*q) {
		// A returned pre-I/O close refusal retries on the existing cadence,
		// measured from this actual return rather than an expired dispatch.
		m.physicalDue = time.Now().Add(time.Second)
	}
	if q.Registered {
		m.physicalSerial = q.Serial
	} else {
		m.physicalSerial = 0
	}
	s := c.artifacts
	s.mu.Lock()
	s.physicalFrameRegistered = q.Registered
	if physicalReturnedHeld(*q) {
		s.physicalHeld = q.Err
	}
	s.physicalWorkPending = false
	s.artifactChangedLocked()
	s.mu.Unlock()
	m.pending = nil
	return nil
}
