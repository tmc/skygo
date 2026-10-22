package control

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/store"
)

type artifactDownloadCloseResult struct{ invoked bool }

// Each turn checks at most one candidate from each bounded family and consumes
// at most one successful mark or original close dispatch. Busy/unknown entries
// advance the cursor without gaining any physical or accounting receipt.
func (c *Control) dispatchArtifactExpiry(m *maintenanceState, now time.Time) (bool, error) {
	if m.expiryHeld != nil {
		return false, m.expiryHeld.err
	}
	if c.artifacts == nil || m.pending != nil {
		return false, nil
	}
	s := c.artifacts
	s.mu.Lock()
	closing := s.closing
	s.mu.Unlock()
	if closing {
		return false, nil
	}
	families := uint8(3)
	if c.recoverStoppedReaders {
		families = 4
	}
	first := m.expiryFamily % families
	m.expiryFamily = (first + 1) % families
	for checked := uint8(0); checked < families; checked++ {
		var admitted bool
		var err error
		switch (first + checked) % families {
		case 0:
			admitted, err = c.markExpiredUpload(m)
		case 1:
			admitted, err = c.dispatchExpiredDownload(m, now)
		case 2:
			admitted, err = c.dispatchRemoteInputFinish(m, now)
		case 3:
			admitted, err = c.dispatchArtifactReaderRecovery(m)
		}
		if err != nil || admitted {
			return admitted, err
		}
	}
	return false, nil
}

func (c *Control) markExpiredUpload(m *maintenanceState) (bool, error) {
	candidate, found, err := c.store.NextArtifactUploadExpiry(m.expiryUploadAfter)
	if err != nil || !found {
		return false, err
	}
	m.expiryUploadAfter = candidate.Upload.Reference.ID
	if candidate.File == (store.ArtifactFile{}) {
		// Creating is unknown, not known absence. Its original charge remains.
		return false, nil
	}
	s := c.artifacts
	s.mu.Lock()
	busy := s.closing || s.uploads[candidate.Upload.Reference.ID] != nil
	unresolved := s.artifactUnresolvedLocked()
	for call := range s.calls {
		busy = busy || call.uploadTransition
	}
	s.mu.Unlock()
	if unresolved != nil {
		return false, unresolved
	}
	if busy {
		return false, nil
	}
	c.physicalChanged()
	_, err = c.store.ExpireArtifactUpload(candidate)
	if err != nil {
		// A real stale CAS is ordinary reselection, never deletion credit.
		if errors.Is(err, store.ErrArtifactConflict) || errors.Is(err, store.ErrArtifactExpired) || errors.Is(err, store.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	c.physicalChanged()
	return true, nil
}

// nextExpiredDownload copies only the original bounded cache. Zero/reserved
// leases are not expired, and an uncached durable row cannot enter this route.
func (c *Control) nextExpiredDownload(after string, now time.Time) *ArtifactDownload {
	s := c.artifacts
	s.mu.Lock()
	defer s.mu.Unlock()
	var choices []*ArtifactDownload
	for d := range s.downloads {
		if (d.remote == nil || d.remote.call == nil && d.remote.finish == nil) && d.lease.ID != "" && !d.lease.ExpiresAt.IsZero() && !now.Before(d.lease.ExpiresAt) && !d.finished && !d.settling {
			choices = append(choices, d)
		}
	}
	sort.Slice(choices, func(i, j int) bool { return choices[i].lease.ID < choices[j].lease.ID })
	for _, d := range choices {
		if d.lease.ID > after {
			return d
		}
	}
	if len(choices) != 0 {
		return choices[0]
	}
	return nil
}

func (c *Control) dispatchExpiredDownload(m *maintenanceState, now time.Time) (bool, error) {
	d := c.nextExpiredDownload(m.expiryDownloadAfter, now)
	if d == nil {
		return false, nil
	}
	s := c.artifacts
	s.mu.Lock()
	m.expiryDownloadAfter = d.lease.ID
	ref, refErr := artifactStoreRef(d.ref)
	_, present := s.downloads[d]
	valid := present && d.control == c && refErr == nil && ref == d.lease.Reference &&
		jobName.MatchString(d.lease.ID) && d.lease.Revision != 0 && d.lease.RootID == s.root.ID &&
		d.lease.File.Device == s.root.Volume && d.lease.File.Inode != 0 && d.lease.Producer == c.producer
	busy := s.closing || d.finished || d.settling || d.uncertain
	s.mu.Unlock()
	if !valid || busy {
		return false, nil
	}
	if m.serial == ^uint64(0) {
		return false, ErrUnavailable
	}
	frame, err := d.prepareOriginalClose()
	if err != nil {
		if errors.Is(err, ErrArtifactBusy) && (frame == nil || frame.err == nil) {
			if frame != nil {
				frame.release()
			}
			return false, nil
		}
		// Unlike an explicit caller's recoverable retry policy, a maintenance
		// Mark error is fail-stop. Keep its original claim/cause in runFrame
		// even though no worker request has yet been dispatched.
		if frame != nil {
			m.expiryHeld = frame
		}
		return false, err
	}
	if frame == nil {
		// A competing explicit close may have retired this exact original
		// after selection. Its already-finished result needs no worker call.
		return false, nil
	}
	s.mu.Lock()
	if s.closing || len(s.calls)+len(s.workspaceIO) >= 4 {
		s.mu.Unlock()
		frame.release()
		return false, nil
	}
	if err = s.artifactUnresolvedLocked(); err != nil {
		s.mu.Unlock()
		frame.err = err
		m.expiryHeld = frame
		return false, err
	}
	owner := &artifactIO{service: s}
	s.workspaceIO[owner] = struct{}{}
	s.mu.Unlock()
	request := maintenanceRequest{sequence: m.serial + 1, downloadClose: frame, closeOwner: owner}
	m.serial = request.sequence
	m.pending = &request
	m.input <- request
	return true, nil
}

func artifactDownloadCloseShape(r maintenanceRequest) bool {
	frame, t := r.downloadClose, r.target
	return frame != nil && frame.download != nil && frame.download.control != nil &&
		frame.download.control.artifacts != nil &&
		(frame.remote == nil && r.closeOwner != nil && r.closeOwner.service == frame.download.control.artifacts ||
			frame.remote != nil && r.closeOwner == nil && frame.remote.call != nil && frame.remote.call.service == frame.download.control.artifacts && frame.remote.close == frame && frame.remote.download == frame.download) && r.sequence != 0 &&
		frame.closure.LeaseID != "" && frame.closure.Revision != 0 &&
		(frame.reader != nil && !frame.neverOpened || frame.reader == nil && frame.neverOpened) &&
		r.readerRecovery == nil && r.artifactDelete == nil && r.workspaceRetirement == nil && r.physicalOwner == nil && !r.physicalClose &&
		r.record == nil && r.directory == "" && r.usage == nil && r.reusable == 0 && r.reservedBytes == 0 && r.reservedFiles == 0 &&
		t.Name == "" && t.Backend == nil && t.Kind == "" && t.Binding == "" && t.Advice == nil && t.Labels == nil && t.AcceleratorNames == nil &&
		!t.SupportsResourceLimits && !t.SupportsNodeSelector && !t.SupportsRetry && t.Retention == nil && t.DataPolicy == nil && !t.SupportsRemoteData && !t.SupportsDataMounts && !t.SupportsRemoteDataRetry && t.OutputStorage == 0 && t.OutputReservation == 0
}

// Called under service.mu, including before entry and before Store Finish.
func (frame *artifactDownloadClose) originalLocked() bool {
	d := frame.download
	c := d.control
	s := c.artifacts
	_, present := s.downloads[d]
	lease := d.lease
	return present && d.settling && d.closing && !d.finished && d.owners == 0 && d.reader == frame.reader &&
		frame.closure == (store.ArtifactDownloadClosure{Owner: d.owner, LeaseID: lease.ID, Revision: lease.Revision,
			Ref: lease.Reference, File: lease.File, RootID: lease.RootID, Producer: lease.Producer}) &&
		lease.Closing && lease.Producer == c.producer && lease.RootID == s.root.ID
}

func runArtifactDownloadClose(ctx context.Context, r maintenanceRequest) (*artifactDownloadCloseResult, error) {
	if !artifactDownloadCloseShape(r) {
		if r.closeOwner != nil {
			r.closeOwner.finish(nil)
		}
		return nil, ErrInvalid
	}
	out := &artifactDownloadCloseResult{}
	frame := r.downloadClose
	s := frame.download.control.artifacts
	s.mu.Lock()
	valid := frame.originalLocked() && !frame.download.uncertain
	if frame.remote == nil {
		_, owned := s.workspaceIO[r.closeOwner]
		valid = valid && owned && !r.closeOwner.finished
	} else {
		valid = valid && frame.remoteOriginalLocked()
	}
	s.mu.Unlock()
	err := ctx.Err()
	if err == nil && !valid {
		err = ErrUnavailable
	}
	if err == nil {
		out.invoked = true
		err = frame.invoke()
	}
	r.closeOwner.finish(err)
	return out, err
}

func (c *Control) integrateArtifactDownloadClose(m *maintenanceState, r maintenanceResult) error {
	if m.pending == nil || m.received == nil || m.received.request != m.pending ||
		!artifactDownloadCloseShape(*m.pending) || !artifactDownloadCloseShape(r.request) ||
		m.pending.sequence != r.request.sequence || m.pending.downloadClose != r.request.downloadClose ||
		m.pending.closeOwner != r.request.closeOwner || r.request.downloadClose.download.control != c ||
		r.readerRecovery != nil || r.deletion != nil || r.retirement != nil || r.physical != nil || r.sample != nil {
		return fmt.Errorf("%w: download close result has no matching original request", ErrUnavailable)
	}
	frame := m.pending.downloadClose
	if r.downloadClose == nil || !r.downloadClose.invoked {
		// Only an actual matched cancellation-before-entry can relinquish the
		// unused claim. Neither durable Finish nor map removal occurs here.
		if r.downloadClose != nil && !r.downloadClose.invoked && errors.Is(r.err, context.Canceled) {
			frame.release()
			if frame.remote != nil {
				s := c.artifacts
				s.mu.Lock()
				frame.remote.close = nil
				s.mu.Unlock()
			}
			m.pending = nil
			return nil
		}
		return fmt.Errorf("%w: download close was not invoked: %w", ErrUnavailable, r.err)
	}
	if r.err != nil {
		return r.err
	}
	s := c.artifacts
	s.mu.Lock()
	valid := frame.originalLocked() && !frame.download.uncertain
	if frame.remote != nil {
		valid = valid && frame.remoteOriginalLocked()
	}
	s.mu.Unlock()
	if !valid {
		return fmt.Errorf("%w: download close original changed", ErrUnavailable)
	}
	// A positive Close is retained in BOTH original slots until exact Finish.
	// ErrNotFound, timeout and transaction ambiguity never become credit.
	if err := frame.finish(); err != nil {
		if frame.remote == nil || !errors.Is(err, auth.ErrUnavailable) {
			return err
		}
		// Unknown authority keeps the SAME invoked frame and all its custody;
		// dispatchRemoteInputFinish retries only its Finish.
		s.mu.Lock()
		frame.remote.held = true
		s.mu.Unlock()
		m.pending = nil
		return nil
	}
	frame.release()
	m.pending = nil
	return nil
}

// Called under the original service lock. The retained common-four call is the
// remote close owner; an ordinary artifactIO must not coexist with it.
func (frame *artifactDownloadClose) remoteOriginalLocked() bool {
	f := frame.remote
	if f == nil || f.close != frame || f.download != frame.download || f.call == nil {
		return false
	}
	d := frame.download
	s := d.control.artifacts
	p := f.call.worker
	_, owned := s.calls[f.call]
	return owned && f.call.service == s && p != nil && p.call == f.call && p.input == f && p.pending && p.readerUseDetached && f.call.httpDone && !f.call.ioActive && f.call.download == nil &&
		d.remote != nil && d.remote.call == f.call && d.remote.finish == f && (!f.complete || frame.reader != nil && !frame.neverOpened)
}
