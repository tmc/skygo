package control

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/store"
)

// ArtifactDownload owns one exact admitted read descriptor. Its identity is not
// an authorization credential. Close is trusted original-owner cleanup only.
// Failed/uncertain opens and closes remain held rather than opening replacements.
type ArtifactDownload struct {
	remote     *remoteInputState
	control    *Control
	owner      string
	lease      store.ArtifactLease
	ref        artifact.Ref
	reader     *artifact.Reader
	owners     int
	closing    bool
	settling   bool
	finished   bool
	uncertain  bool
	closeError error
}

// ID returns the durable lease ID. It does not grant access to the download.
func (d *ArtifactDownload) ID() string { return d.lease.ID }

// Reference returns the immutable admitted reference.
func (d *ArtifactDownload) Reference() artifact.Ref { return d.ref }

// ExpiresAt returns the original durable lease deadline.
func (d *ArtifactDownload) ExpiresAt() time.Time { return d.lease.ExpiresAt }

// BeginArtifactDownload reserves one of 32 descriptor owners before opening the
// exact Store-admitted inode. A nonnil handle on error remains an original held
// owner; callers must preserve it, never infer successful descriptor closure.
func (c *Control) BeginArtifactDownload(ctx context.Context, actor auth.Actor, call *ArtifactCall, ref artifact.Ref) (*ArtifactDownload, error) {
	actor, err := c.artifactActor(ctx, actor, auth.Read)
	if err != nil {
		return nil, err
	}
	if c.artifacts == nil {
		return nil, ErrUnsupported
	}
	stored, err := artifactStoreRef(ref)
	if err != nil {
		return nil, err
	}
	end, err := c.beginArtifactCall(call, "")
	if err != nil {
		return nil, err
	}
	defer end()
	s := c.artifacts
	d, err := c.reserveArtifactDownload(actor.Principal(), ref, call)
	if err != nil {
		return nil, err
	}
	lease, err := c.store.BeginArtifactDownload(actor, stored, c.producer)
	if err != nil {
		s.mu.Lock()
		delete(s.downloads, d)
		s.artifactChangedLocked()
		s.mu.Unlock()
		return nil, err
	}
	s.mu.Lock()
	d.lease = lease
	d.owners++
	call.download = d
	s.mu.Unlock()
	if lease.Producer != c.producer || lease.Reference != stored || lease.RootID != s.root.ID || lease.File.Device != s.root.File.Device || lease.File.Inode == 0 {
		s.mu.Lock()
		d.uncertain = true
		s.mu.Unlock()
		return d, ErrUnavailable
	}
	actor, err = c.artifactActor(ctx, actor, auth.Read)
	if err != nil {
		return d, err
	}
	reader, err := s.files.OpenObject(ref, artifact.FileID{Device: lease.File.Device, Inode: lease.File.Inode})
	s.mu.Lock()
	d.reader = reader
	d.uncertain = errors.Is(err, artifact.ErrFileCloseUncertain)
	s.mu.Unlock()
	if err != nil {
		return d, c.artifactFileError(call, err)
	}
	if _, err = c.artifactActor(ctx, actor, auth.Read); err != nil {
		return d, err
	}
	return d, nil
}

// ReadArtifactChunk reauthorizes the owner and exact live lease before and after
// the bounded read. No bytes are returned on revocation, expiry, or read failure.
// Its descriptor ownership includes actual completion of the enclosing response.
func (c *Control) ReadArtifactChunk(ctx context.Context, actor auth.Actor, call *ArtifactCall, id string, offset int64, limit int) ([]byte, bool, error) {
	actor, err := c.artifactActor(ctx, actor, auth.Read)
	if err != nil {
		return nil, false, err
	}
	if c.artifacts == nil {
		return nil, false, ErrUnsupported
	}
	if !jobName.MatchString(id) {
		return nil, false, ErrInvalid
	}
	lease, err := c.store.GetArtifactDownload(actor, id)
	if err != nil {
		return nil, false, err
	}
	if offset < 0 || limit < 1 || limit > 64<<10 {
		return nil, false, ErrInvalid
	}
	end, err := c.beginArtifactCall(call, "")
	if err != nil {
		return nil, false, err
	}
	defer end()
	s := c.artifacts
	s.mu.Lock()
	var d *ArtifactDownload
	for candidate := range s.downloads {
		if candidate.remote == nil && candidate.lease.ID == id {
			d = candidate
			break
		}
	}
	if d == nil || d.closing || d.finished || d.uncertain || d.reader == nil || d.lease != lease {
		s.mu.Unlock()
		return nil, false, ErrUnavailable
	}
	if offset > d.ref.Size {
		s.mu.Unlock()
		return nil, false, ErrInvalid
	}
	d.owners++
	call.download = d
	reader, ref := d.reader, d.ref
	s.mu.Unlock()
	if _, err = c.artifactActor(ctx, actor, auth.Read); err != nil {
		return nil, false, err
	}
	size := int64(limit)
	if size > ref.Size-offset {
		size = ref.Size - offset
	}
	data := make([]byte, int(size))
	n, err := reader.ReadAt(data, offset)
	if err != nil && !(err == io.EOF && size == 0) {
		return nil, false, err
	}
	if n != len(data) {
		return nil, false, io.ErrUnexpectedEOF
	}
	actor, err = c.artifactActor(ctx, actor, auth.Read)
	if err != nil {
		return nil, false, err
	}
	current, err := c.store.GetArtifactDownload(actor, id)
	if err != nil {
		return nil, false, err
	}
	if current != lease {
		return nil, false, store.ErrArtifactConflict
	}
	if _, err = c.artifactActor(ctx, actor, auth.Read); err != nil {
		return nil, false, err
	}
	return data, offset+int64(n) == ref.Size, nil
}

// Close stops new reads and settles only this original handle. It needs no
// fabricated Actor after revocation. A busy result requires a later explicit
// retry after original I/O and HTTP owners join; it never spawns a waiter.
// call still requires Done after the close response actually completes.
func (d *ArtifactDownload) Close(call *ArtifactCall) error {
	c := d.control
	end, err := c.beginArtifactCall(call, "")
	if err != nil {
		return err
	}
	defer end()
	return d.closeOriginal(call)
}

// closeOriginal operates only on the cached original descriptor. Drain calls it
// after stopping and joining all request/internal I/O owners, without reopening
// admission or acquiring a replacement ArtifactCall.
func (d *ArtifactDownload) closeOriginal(call *ArtifactCall) error {
	s := d.control.artifacts
	s.mu.Lock()
	remoteBusy := d.remote != nil && (call != nil || d.remote.call != nil || d.remote.finish != nil)
	s.mu.Unlock()
	if remoteBusy {
		return ErrArtifactBusy
	}
	frame, err := d.prepareOriginalClose()
	if frame != nil {
		defer frame.release()
	}
	if err != nil || frame == nil {
		return err
	}
	if err = frame.invoke(); err != nil {
		return d.control.artifactFileError(call, err)
	}
	return frame.finish()
}

// artifactDownloadClose retains the SAME original descriptor and exact Store
// closing receipt. It lives on the explicit caller's stack or original worker
// request; it neither reopens a file nor registers a replacement owner.
type artifactDownloadClose struct {
	remote      *remoteInputFinish
	download    *ArtifactDownload
	closure     store.ArtifactDownloadClosure
	reader      *artifact.Reader
	neverOpened bool
	err         error
}

func (d *ArtifactDownload) prepareOriginalClose() (*artifactDownloadClose, error) {
	c := d.control
	s := c.artifacts
	s.mu.Lock()
	if _, ok := s.downloads[d]; !ok {
		done := d.finished
		s.mu.Unlock()
		if done {
			return nil, nil
		}
		return nil, ErrInvalid
	}
	if d.settling {
		s.mu.Unlock()
		return nil, ErrArtifactBusy
	}
	d.closing = true
	if d.uncertain {
		failure := d.closeError
		s.mu.Unlock()
		return nil, errors.Join(ErrArtifactBusy, failure)
	}
	d.settling = true
	lease := d.lease
	frame := &artifactDownloadClose{download: d, reader: d.reader, neverOpened: d.reader == nil,
		closure: store.ArtifactDownloadClosure{Owner: d.owner, LeaseID: lease.ID, Revision: lease.Revision, Ref: lease.Reference, File: lease.File, RootID: lease.RootID, Producer: lease.Producer}}
	s.mu.Unlock()
	closure, err := c.store.MarkArtifactDownloadClosing(frame.closure)
	if err != nil {
		frame.err = err
		return frame, err
	}
	frame.closure = closure
	s.mu.Lock()
	d.lease.Revision = closure.Revision
	d.lease.Closing = true
	held := d.owners != 0 || d.uncertain
	s.mu.Unlock()
	if held {
		return frame, ErrArtifactBusy
	}
	return frame, nil
}

func (frame *artifactDownloadClose) release() {
	s := frame.download.control.artifacts
	s.mu.Lock()
	frame.download.settling = false
	s.artifactChangedLocked()
	s.mu.Unlock()
}

// invoke performs no Store/auth call. A nil reader is usable only from the
// positively known, non-uncertain constructor path captured during prepare.
func (frame *artifactDownloadClose) invoke() error {
	if frame.reader == nil {
		if !frame.neverOpened {
			return ErrUnavailable
		}
		return nil
	}
	err := frame.reader.Close()
	if err != nil {
		s := frame.download.control.artifacts
		s.mu.Lock()
		frame.download.uncertain = true
		frame.download.closeError = err
		frame.err = err
		s.mu.Unlock()
	}
	return frame.download.control.artifactFileError(nil, err)
}

func (frame *artifactDownloadClose) finish() error {
	if frame.remote != nil {
		return frame.download.control.finishRemoteInput(frame)
	}
	d := frame.download
	c := d.control
	if err := c.store.FinishArtifactDownload(frame.closure, time.Now().UTC()); err != nil {
		return err
	}
	s := c.artifacts
	s.mu.Lock()
	d.finished = true
	delete(s.downloads, d)
	s.artifactChangedLocked()
	s.mu.Unlock()
	return nil
}

// CloseArtifactDownload closes only a cached original lease owned by the freshly
// checked principal. Admin does not gain another principal's cleanup identity.
// Restart leases without an original descriptor receipt remain held.
func (c *Control) CloseArtifactDownload(ctx context.Context, actor auth.Actor, call *ArtifactCall, id string) error {
	actor, err := c.artifactActor(ctx, actor, auth.Read)
	if err != nil {
		return err
	}
	if c.artifacts == nil {
		return ErrUnsupported
	}
	if !jobName.MatchString(id) {
		return ErrInvalid
	}
	s := c.artifacts
	s.mu.Lock()
	var selected *ArtifactDownload
	for d := range s.downloads {
		if d.remote == nil && d.lease.ID == id && d.owner == actor.Principal() {
			selected = d
			break
		}
	}
	s.mu.Unlock()
	if selected == nil {
		return store.ErrNotFound
	}
	if _, err = c.artifactActor(ctx, actor, auth.Read); err != nil {
		return err
	}
	return selected.Close(call)
}

func (c *Control) reserveArtifactDownload(owner string, ref artifact.Ref, call *ArtifactCall) (*ArtifactDownload, error) {
	s := c.artifacts
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.calls[call]; !ok || call.service != s || !call.used || !call.ioActive || call.download != nil {
		return nil, ErrInvalid
	}
	if len(s.downloads) >= 32 {
		return nil, ErrArtifactBusy
	}
	d := &ArtifactDownload{control: c, owner: owner, ref: ref}
	s.downloads[d] = struct{}{}
	return d, nil
}
