package control

import (
	"context"
	"errors"
	"fmt"

	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

// The original maintenance request owns the query and its common-four slot.
// A positive native result keeps that slot until the exact Store CAS succeeds.
type artifactReaderRecoveryRequest struct {
	control *Control
	current local.ServerLifetime
	closure store.ArtifactDownloadClosure
	owner   *artifactIO
}

type artifactReaderRecoveryResult struct {
	invoked bool
	stopped bool
}

func recoveryProducerKnown(p store.OriginalServerLifetime) bool {
	_, err := workspaceStoreProducer(workspaceLocalProducer(p))
	return err == nil && p != (store.OriginalServerLifetime{}) && p.NamespaceDevice != 0 && p.StartTicks != 0
}

func (c *Control) validArtifactReaderRecovery(w store.ArtifactDownloadClosure) bool {
	p := w.Producer
	if !c.recoverStoppedReaders || c.artifacts == nil || !recoveryProducerKnown(c.producer) || !recoveryProducerKnown(p) ||
		p == c.producer || p.Host != c.producer.Host || p.Boot != c.producer.Boot ||
		p.NamespaceDevice != c.producer.NamespaceDevice || p.NamespaceInode != c.producer.NamespaceInode ||
		!jobName.MatchString(w.LeaseID) || w.Revision == 0 || w.RootID != c.artifacts.root.ID ||
		w.File.Device != c.artifacts.root.Volume || w.File.Inode == 0 {
		return false
	}
	_, err := artifactPortableRef(w.Ref)
	return err == nil
}

// Called only under service.mu. A reserved or uncertain matching cached owner
// is never repaired by substituting a newly observed process identity.
func (c *Control) artifactReaderCachedLocked(w store.ArtifactDownloadClosure) bool {
	for d := range c.artifacts.downloads {
		if d.lease.ID == w.LeaseID || d.ref.ID == w.Ref.ID && (d.lease.ID == "" || d.uncertain) {
			return true
		}
	}
	return false
}

func (c *Control) tryArtifactReaderRecovery(w store.ArtifactDownloadClosure) (*artifactIO, error) {
	if !c.validArtifactReaderRecovery(w) {
		return nil, ErrInvalid
	}
	s := c.artifacts
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return nil, ErrArtifactBusy
	}
	if err := s.artifactUnresolvedLocked(); err != nil {
		return nil, err
	}
	if c.artifactReaderCachedLocked(w) || len(s.calls)+len(s.workspaceIO) >= 4 {
		return nil, ErrArtifactBusy
	}
	owner := &artifactIO{service: s}
	s.workspaceIO[owner] = struct{}{}
	return owner, nil
}

func (c *Control) dispatchArtifactReaderRecovery(m *maintenanceState) (bool, error) {
	if !c.recoverStoppedReaders || c.artifacts == nil || m.pending != nil {
		return false, nil
	}
	w, found, err := c.store.NextArtifactReaderRecovery(m.recoveryAfter)
	if err != nil || !found {
		return false, err
	}
	m.recoveryAfter = w.LeaseID
	if !c.validArtifactReaderRecovery(w) {
		// Unknown/cross-domain/current producers keep their original charge.
		return false, nil
	}
	if m.serial == ^uint64(0) {
		return false, ErrUnavailable
	}
	owner, err := c.tryArtifactReaderRecovery(w)
	if err != nil {
		if errors.Is(err, ErrArtifactBusy) {
			return false, nil
		}
		return false, err
	}
	request := maintenanceRequest{sequence: m.serial + 1, readerRecovery: &artifactReaderRecoveryRequest{
		control: c, current: workspaceLocalProducer(c.producer), closure: w, owner: owner,
	}}
	m.serial = request.sequence
	m.pending = &request
	m.input <- request
	return true, nil
}

func artifactReaderRecoveryShape(r maintenanceRequest) bool {
	q, t := r.readerRecovery, r.target
	return q != nil && q.control != nil && q.owner != nil && q.control.artifacts != nil &&
		q.owner.service == q.control.artifacts && q.current == workspaceLocalProducer(q.control.producer) &&
		q.control.validArtifactReaderRecovery(q.closure) && r.sequence != 0 &&
		r.downloadClose == nil && r.closeOwner == nil && r.artifactDelete == nil && r.workspaceRetirement == nil &&
		r.physicalOwner == nil && !r.physicalClose && r.record == nil && r.directory == "" && r.usage == nil &&
		r.reusable == 0 && r.reservedBytes == 0 && r.reservedFiles == 0 &&
		t.Name == "" && t.Backend == nil && t.Kind == "" && t.Binding == "" && t.Advice == nil && t.Labels == nil && t.AcceleratorNames == nil &&
		!t.SupportsResourceLimits && !t.SupportsNodeSelector && !t.SupportsRetry && t.Retention == nil && t.OutputStorage == 0 && t.OutputReservation == 0
}

func runArtifactReaderRecovery(ctx context.Context, r maintenanceRequest) (*artifactReaderRecoveryResult, error) {
	q := r.readerRecovery
	if !artifactReaderRecoveryShape(r) {
		if q != nil && q.owner != nil {
			q.owner.finish(nil)
		}
		return nil, ErrInvalid
	}
	out := &artifactReaderRecoveryResult{}
	s := q.control.artifacts
	s.mu.Lock()
	_, owned := s.workspaceIO[q.owner]
	valid := owned && !q.owner.finished && !q.control.artifactReaderCachedLocked(q.closure)
	s.mu.Unlock()
	err := ctx.Err()
	if err == nil && !valid {
		err = ErrUnavailable
	}
	if err == nil {
		out.invoked = true
		out.stopped, err = local.ObserveServerStopped(q.current, workspaceLocalProducer(q.closure.Producer))
	}
	if err != nil || !out.stopped {
		// Typed query AND self-capture Close errors retain this exact slot.
		// An actual positive result retains the slot for original integration.
		q.owner.finish(err)
	}
	return out, err
}

func (c *Control) integrateArtifactReaderRecovery(m *maintenanceState, r maintenanceResult) error {
	if m.pending == nil || m.received == nil || m.received.request != m.pending ||
		!artifactReaderRecoveryShape(*m.pending) || !artifactReaderRecoveryShape(r.request) ||
		m.pending.sequence != r.request.sequence || m.pending.readerRecovery != r.request.readerRecovery ||
		r.request.readerRecovery.control != c || r.downloadClose != nil || r.deletion != nil || r.retirement != nil || r.physical != nil || r.sample != nil {
		return fmt.Errorf("%w: reader recovery result has no matching original request", ErrUnavailable)
	}
	q := m.pending.readerRecovery
	out := r.readerRecovery
	if out == nil {
		return fmt.Errorf("%w: missing reader recovery result", ErrUnavailable)
	}
	if !out.invoked {
		if !out.stopped && errors.Is(r.err, context.Canceled) {
			m.pending = nil
			return nil
		}
		return fmt.Errorf("%w: reader recovery was not invoked: %w", ErrUnavailable, r.err)
	}
	if artifactCloseUncertain(r.err) {
		return r.err
	}
	if !out.stopped && (r.err == nil || errors.Is(r.err, local.ErrServerLifetimeUnavailable)) {
		m.pending = nil
		return nil
	}
	if r.err != nil {
		return r.err
	}
	if !out.stopped {
		return fmt.Errorf("%w: missing stopped reader observation", ErrUnavailable)
	}
	s := c.artifacts
	s.mu.Lock()
	_, owned := s.workspaceIO[q.owner]
	valid := owned && !q.owner.finished && !c.artifactReaderCachedLocked(q.closure)
	s.mu.Unlock()
	if !valid {
		return fmt.Errorf("%w: reader recovery original owner changed", ErrUnavailable)
	}
	// Distinct stopped settlement, never a fabricated original Reader.Close.
	// On any Store error F1 retains the original positive pair and permit.
	if err := c.store.SettleStoppedArtifactDownload(q.closure); err != nil {
		return err
	}
	q.owner.finish(nil)
	m.pending = nil
	return nil
}
