package control

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/store"
)

// One immutable request travels on the original maintenance worker. Existing
// pending/received slots retain the pair, including a failed Store Finish.
type artifactDeleteRequest struct {
	control *Control
	work    store.ArtifactDeleteWork
	owner   *artifactIO
}

type artifactDeleteResult struct{ invoked bool }

func artifactDeleteShape(r maintenanceRequest) bool {
	d, t := r.artifactDelete, r.target
	return d != nil && d.control != nil && d.owner != nil && d.control.artifacts != nil &&
		d.owner.service == d.control.artifacts && r.sequence != 0 &&
		r.readerRecovery == nil && r.downloadClose == nil && r.closeOwner == nil && r.workspaceRetirement == nil && r.physicalOwner == nil && !r.physicalClose &&
		r.record == nil && r.directory == "" && r.usage == nil && r.reusable == 0 && r.reservedBytes == 0 && r.reservedFiles == 0 &&
		t.Name == "" && t.Backend == nil && t.Kind == "" && t.Binding == "" && t.Advice == nil && t.Labels == nil && t.AcceleratorNames == nil &&
		!t.SupportsResourceLimits && !t.SupportsNodeSelector && !t.SupportsRetry && t.Retention == nil && t.OutputStorage == 0 && t.OutputReservation == 0 &&
		d.control.validArtifactDelete(d.work)
}

func (c *Control) validArtifactDelete(w store.ArtifactDeleteWork) bool {
	// Owner is a principal already validated by Store selection, not a job ID.
	// Preserve it verbatim in the complete immutable Work and digest below.
	if c.artifacts == nil || w.RootID != c.artifacts.root.ID || !jobName.MatchString(w.ID) ||
		w.Revision == 0 || w.Digest == ([32]byte{}) || w.File.Device != c.artifacts.root.Volume || w.File.Inode == 0 ||
		(w.Kind != "object" && w.Kind != "upload" && w.Kind != "finalizing") {
		return false
	}
	if _, err := artifactPortableRef(w.Ref); err != nil {
		return false
	}
	copy := w
	copy.Digest = [32]byte{}
	raw, err := json.Marshal(copy)
	return err == nil && sha256.Sum256(raw) == w.Digest
}

// Only this in-memory gate is under service.mu. The committed deleting state
// rejects a later upload/download before Files; earlier actual owners remain
// visible here through their original HTTP/descriptor completion.
func (c *Control) tryArtifactDeleteIO(w store.ArtifactDeleteWork) (*artifactIO, error) {
	if !c.validArtifactDelete(w) {
		return nil, ErrInvalid
	}
	s := c.artifacts
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return nil, ErrUnavailable
	}
	if err := s.artifactUnresolvedLocked(); err != nil {
		return nil, err
	}
	if s.uploads[w.Ref.ID] != nil {
		return nil, ErrArtifactBusy
	}
	for call := range s.calls {
		if call.uploadTransition {
			return nil, ErrArtifactBusy
		}
	}
	for d := range s.downloads {
		if d.ref.ID == w.Ref.ID {
			return nil, ErrArtifactBusy
		}
	}
	if len(s.calls)+len(s.workspaceIO) >= 4 {
		return nil, ErrArtifactBusy
	}
	owner := &artifactIO{service: s}
	s.workspaceIO[owner] = struct{}{}
	return owner, nil
}

func (c *Control) dispatchArtifactDelete(m *maintenanceState, work store.ArtifactDeleteWork) (bool, error) {
	if m.serial == ^uint64(0) {
		return false, ErrUnavailable
	}
	owner, err := c.tryArtifactDeleteIO(work)
	if err != nil {
		if errors.Is(err, ErrArtifactBusy) || errors.Is(err, ErrUnavailable) {
			return false, nil
		}
		return false, err
	}
	r := maintenanceRequest{sequence: m.serial + 1, artifactDelete: &artifactDeleteRequest{control: c, work: work, owner: owner}}
	m.serial = r.sequence
	m.pending = &r
	m.input <- r
	return true, nil
}

func runArtifactDelete(ctx context.Context, r maintenanceRequest) (*artifactDeleteResult, error) {
	d := r.artifactDelete
	if !artifactDeleteShape(r) {
		if d != nil && d.owner != nil {
			d.owner.finish(nil)
		}
		return nil, ErrInvalid
	}
	out := &artifactDeleteResult{}
	err := ctx.Err()
	if err == nil {
		out.invoked = true
		ref, _ := artifactPortableRef(d.work.Ref) // shape checked before entry
		id := artifact.FileID{Device: d.work.File.Device, Inode: d.work.File.Inode}
		if d.work.Kind != "object" {
			d.control.physicalChanged()
			err = d.control.artifacts.files.RemovePart(ref, id)
		}
		if err == nil && d.work.Kind != "upload" {
			d.control.physicalChanged()
			err = d.control.artifacts.files.RemoveObject(ref, id)
		}
	}
	d.owner.finish(err)
	return out, err
}

func (c *Control) integrateArtifactDelete(m *maintenanceState, r maintenanceResult) error {
	if m.pending == nil || m.received == nil || m.received.request != m.pending ||
		!artifactDeleteShape(*m.pending) || !artifactDeleteShape(r.request) ||
		m.pending.sequence != r.request.sequence || m.pending.artifactDelete != r.request.artifactDelete ||
		r.request.artifactDelete.control != c || r.readerRecovery != nil || r.downloadClose != nil || r.retirement != nil || r.physical != nil || r.sample != nil {
		return fmt.Errorf("%w: artifact delete result has no matching original request", ErrUnavailable)
	}
	if r.deletion == nil && r.err == nil {
		return fmt.Errorf("%w: artifact delete result missing", ErrUnavailable)
	}
	if errors.Is(r.err, artifact.ErrFileCloseUncertain) {
		return r.err
	}
	if r.err == nil {
		if !r.deletion.invoked {
			return fmt.Errorf("%w: artifact delete was not invoked", ErrUnavailable)
		}
		work := m.pending.artifactDelete.work
		// Keep BOTH original slots until the exact Store transaction succeeds.
		// An ambiguous error fails Run closed; ErrNotFound is never success.
		if _, err := c.store.FinishArtifactDelete(work, store.ArtifactDeleteReceipt{Work: work}); err != nil {
			return err
		}
		c.physicalChanged()
	}
	m.pending = nil
	return nil
}
