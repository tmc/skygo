package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/store"
)

// RemoteOperation selects a single fixed scoped worker operation. Zero is invalid.
type RemoteOperation uint8

const (
	RemoteReadInput RemoteOperation = iota + 1
	RemotePollRelease
	RemoteBeginOutput
	RemoteWriteOutput
	RemoteFinishOutput
)

type workerPermit struct {
	call                         *ArtifactCall
	operation                    RemoteOperation
	grant                        store.RemoteGrant
	hash                         [32]byte
	authority                    auth.Delegation
	response                     *remoteHTTP
	input                        *remoteInputFinish
	pending                      bool
	err                          error
	readRef                      artifact.Ref
	readOffset, readEnd          int64
	readArmed, readerUseDetached bool
}
type remoteInputState struct {
	key       store.RemoteKey
	published int64
	call      *ArtifactCall
	finish    *remoteInputFinish
}
type remoteInputFinish struct {
	call     *ArtifactCall
	download *ArtifactDownload
	grant    store.RemoteGrant
	close    *artifactDownloadClose
	complete bool
	held     bool // invoked close awaits its Finish while authority is unavailable
	err      error
}

func remoteTokenHash(kind store.RemoteGrantKind, raw []byte) [32]byte {
	h := sha256.New()
	h.Write([]byte{byte(kind)})
	h.Write(raw)
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum
}
func remoteToken(key store.RemoteKey, raw []byte) ([]byte, error) {
	if !jobName.MatchString(key.JobID) || !jobName.MatchString(key.AttemptID) || len(raw) != 32 {
		return nil, ErrInvalid
	}
	return []byte("sgd1." + key.JobID + "." + key.AttemptID + "." + base64.RawURLEncoding.EncodeToString(raw)), nil
}
func parseRemoteToken(token string) (store.RemoteKey, []byte, error) {
	if len(token) != 114 || token[:5] != "sgd1." || token[37] != '.' || token[70] != '.' {
		return store.RemoteKey{}, nil, ErrInvalid
	}
	key := store.RemoteKey{JobID: token[5:37], AttemptID: token[38:70]}
	if !jobName.MatchString(key.JobID) || !jobName.MatchString(key.AttemptID) {
		return store.RemoteKey{}, nil, ErrInvalid
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(token[71:])
	if err != nil || len(raw) != 32 || base64.RawURLEncoding.EncodeToString(raw) != token[71:] {
		return store.RemoteKey{}, nil, ErrInvalid
	}
	return key, raw, nil
}
func remotePurpose(op RemoteOperation) store.RemoteGrantKind {
	if op == RemoteReadInput {
		return store.RemoteInput
	}
	return store.RemoteOutput
}

func (c *Control) closeRemoteAuthority(key store.RemoteKey, hash [32]byte, kind store.RemoteGrantKind) {
	for i := 0; i < 2; i++ {
		row, err := c.store.GetRemoteAttempt(key)
		if err != nil || row.Active == nil {
			return
		}
		scope := row.Active.Output
		if kind == store.RemoteInput {
			scope = row.Active.Input
		}
		if scope == nil || subtle.ConstantTimeCompare(scope.Hash[:], hash[:]) != 1 || row.Active.ClosedReason != 0 {
			return
		}
		if _, err = c.store.RevokeRemote(key, row.Revision, store.RemoteAuthorityLost, time.Now().UTC()); !errors.Is(err, store.ErrArtifactConflict) {
			return
		}
	}
}

func (c *Control) remoteCredential(ctx context.Context, key store.RemoteKey, hash [32]byte, op RemoteOperation) (store.RemoteAttempt, store.RemoteGrant, error) {
	if c == nil || c.artifacts == nil || ctx.Err() != nil {
		return store.RemoteAttempt{}, store.RemoteGrant{}, ErrUnavailable
	}
	row, err := c.store.GetRemoteAttempt(key)
	if err != nil || row.Key != key || row.Active == nil {
		return store.RemoteAttempt{}, store.RemoteGrant{}, ErrUnavailable
	}
	kind := remotePurpose(op)
	scope := row.Active.Output
	if kind == store.RemoteInput {
		scope = row.Active.Input
	}
	if scope == nil || hash == ([32]byte{}) || subtle.ConstantTimeCompare(scope.Hash[:], hash[:]) != 1 {
		return store.RemoteAttempt{}, store.RemoteGrant{}, ErrUnavailable
	}
	if err = c.engine.CheckDelegation(row.Authority); err != nil {
		if errors.Is(err, auth.ErrUnavailable) {
			return store.RemoteAttempt{}, store.RemoteGrant{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
		}
		c.closeRemoteAuthority(key, hash, kind)
		return store.RemoteAttempt{}, store.RemoteGrant{}, ErrUnavailable
	}
	job, err := c.store.Get(key.JobID)
	if err != nil {
		return store.RemoteAttempt{}, store.RemoteGrant{}, ErrUnavailable
	}
	route, err := c.dataTargetFor(job)
	if err != nil || route.policy != row.Policy || job.CurrentAttemptID() != key.AttemptID {
		return store.RemoteAttempt{}, store.RemoteGrant{}, ErrUnavailable
	}
	grant := store.RemoteGrant{Key: key, Kind: kind, Epoch: scope.Epoch, Expires: scope.Expires}
	if op == RemotePollRelease {
		if _, err = c.store.PollRemoteRelease(key.AttemptID, hash, time.Now().UTC()); err != nil {
			return store.RemoteAttempt{}, store.RemoteGrant{}, ErrUnavailable
		}
	} else {
		grant, err = c.store.CheckRemoteGrant(key.AttemptID, kind, hash, time.Now().UTC())
		if err != nil || grant.Key != key {
			return store.RemoteAttempt{}, store.RemoteGrant{}, ErrUnavailable
		}
	}
	if err = c.recheckRemote(ctx, key, hash, kind, row.Authority); err != nil {
		return store.RemoteAttempt{}, store.RemoteGrant{}, err
	}
	return row, grant, nil
}

// recheckRemote repeats the fresh delegation check after the Store checks.
// Lost delegation closes the matching scope. A canceled request or
// unavailable authority is only refused: an HTTP disconnect, deadline or
// stale snapshot is not lost authority.
func (c *Control) recheckRemote(ctx context.Context, key store.RemoteKey, hash [32]byte, kind store.RemoteGrantKind, authority auth.Delegation) error {
	if err := c.engine.CheckDelegation(authority); err != nil {
		if errors.Is(err, auth.ErrUnavailable) {
			return fmt.Errorf("%w: %w", ErrUnavailable, err)
		}
		c.closeRemoteAuthority(key, hash, kind)
		return ErrUnavailable
	}
	if ctx.Err() != nil {
		return ErrUnavailable
	}
	return nil
}

// AuthorizeRemoteCall attaches one scoped permit only after exact credential,
// route and fresh delegation checks. Failure leaves the original call unattached.
func (c *Control) AuthorizeRemoteCall(ctx context.Context, call *ArtifactCall, token string, op RemoteOperation) error {
	if err := c.beginExternal(); err != nil {
		return err
	}
	defer c.endExternal()
	if call == nil || call.service != c.artifacts || op < RemoteReadInput || op > RemoteFinishOutput {
		return ErrInvalid
	}
	key, raw, err := parseRemoteToken(token)
	if err != nil {
		return ErrInvalid
	}
	hash := remoteTokenHash(remotePurpose(op), raw)
	clear(raw)
	row, grant, err := c.remoteCredential(ctx, key, hash, op)
	if err != nil {
		return err
	}
	p := &workerPermit{call: call, operation: op, grant: grant, hash: hash, authority: row.Authority, pending: true}
	s := c.artifacts
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.calls[call]; !ok || call.used || call.ioActive || call.httpDone || call.worker != nil || s.closing || c.failed.Load() || c.remoteCompletionFencedLocked(key) {
		return ErrUnavailable
	}
	select {
	case <-c.shutdown.request:
		return ErrUnavailable
	default:
	}
	call.worker = p
	return nil
}
func (c *Control) checkRemotePermit(ctx context.Context, p *workerPermit) (store.RemoteGrant, error) {
	if c == nil || c.artifacts == nil || p == nil || p.call == nil {
		return store.RemoteGrant{}, ErrUnavailable
	}
	s := c.artifacts
	s.mu.Lock()
	_, ok := s.calls[p.call]
	valid := ok && p.call.service == s && p.call.worker == p && p.pending && !p.call.httpDone && p.err == nil
	key, hash, op, epoch := p.grant.Key, p.hash, p.operation, p.grant.Epoch
	s.mu.Unlock()
	if !valid {
		return store.RemoteGrant{}, ErrUnavailable
	}
	row, grant, err := c.remoteCredential(ctx, key, hash, op)
	if err != nil || grant.Epoch != epoch || row.Authority != p.authority {
		return store.RemoteGrant{}, ErrUnavailable
	}
	return grant, nil
}
func (c *Control) beginRemoteIO(ctx context.Context, call *ArtifactCall, op RemoteOperation, upload string) (*workerPermit, func(), error) {
	if c == nil || c.artifacts == nil || call == nil || call.service != c.artifacts {
		return nil, nil, ErrInvalid
	}
	s := c.artifacts
	s.mu.Lock()
	p := call.worker
	if _, ok := s.calls[call]; !ok || p == nil || p.call != call || p.operation != op || !p.pending || call.used || call.httpDone || s.closing {
		s.mu.Unlock()
		return nil, nil, ErrInvalid
	}
	if upload != "" && s.uploads[upload] != nil {
		s.mu.Unlock()
		return nil, nil, ErrArtifactBusy
	}
	call.used = true
	call.ioActive = true
	if upload != "" {
		s.uploads[upload] = call
		call.upload = upload
	}
	s.mu.Unlock()
	end := func() {
		s.mu.Lock()
		call.ioActive = false
		call.releaseLocked()
		s.artifactChangedLocked()
		s.mu.Unlock()
	}
	grant, err := c.checkRemotePermit(ctx, p)
	if err != nil {
		end()
		return nil, nil, err
	}
	s.mu.Lock()
	p.grant = grant
	s.mu.Unlock()
	return p, end, nil
}

// ReadRemoteInput reads at most64KiB from the admitted original input descriptor.
// Returned bytes and their end offset are not complete-transfer/Close receipts.
func (c *Control) ReadRemoteInput(ctx context.Context, call *ArtifactCall, offset int64, limit int) (artifact.Ref, []byte, error) {
	if offset < 0 || limit < 1 || limit > 64<<10 {
		return artifact.Ref{}, nil, ErrInvalid
	}
	p, end, err := c.beginRemoteIO(ctx, call, RemoteReadInput, "")
	if err != nil {
		return artifact.Ref{}, nil, err
	}
	defer end()
	row, grant, err := c.remoteCredential(ctx, p.grant.Key, p.hash, p.operation)
	if err != nil {
		return artifact.Ref{}, nil, err
	}
	job, err := c.store.Get(row.Key.JobID)
	if err != nil || job.Task.Workdir == nil {
		return artifact.Ref{}, nil, ErrUnavailable
	}
	ref := *job.Task.Workdir
	s := c.artifacts
	s.mu.Lock()
	var d *ArtifactDownload
	for candidate := range s.downloads {
		if candidate.remote != nil && candidate.remote.key == p.grant.Key {
			d = candidate
			break
		}
	}
	if d != nil && (d.uncertain || d.closing || d.settling || d.finished || d.reader == nil || d.remote.call != nil || d.remote.finish != nil) {
		s.mu.Unlock()
		return artifact.Ref{}, nil, ErrArtifactBusy
	}
	if d == nil {
		if offset != 0 || len(s.downloads) >= 32 {
			s.mu.Unlock()
			return artifact.Ref{}, nil, ErrArtifactBusy
		}
		d = &ArtifactDownload{control: c, owner: row.Authority.Principal, ref: ref, remote: &remoteInputState{key: p.grant.Key}}
		s.downloads[d] = struct{}{}
	}
	if d.ref != ref || offset > d.ref.Size || offset > d.remote.published {
		s.mu.Unlock()
		return artifact.Ref{}, nil, ErrInvalid
	}
	d.owners++
	call.download = d
	d.remote.call = call
	reader := d.reader
	needOpen := d.lease.ID == ""
	s.mu.Unlock()
	if needOpen {
		lease, e := c.store.BeginRemoteInputRead(grant, c.producer, time.Now().UTC())
		if e != nil {
			s.mu.Lock()
			d.owners--
			call.download = nil
			delete(s.downloads, d)
			s.artifactChangedLocked()
			s.mu.Unlock()
			return artifact.Ref{}, nil, e
		}
		s.mu.Lock()
		d.lease = lease
		s.mu.Unlock()
		stored, e := artifactStoreRef(ref)
		if e != nil || lease.Reference != stored || lease.Producer != c.producer || lease.RootID != s.root.ID || lease.File.Device != s.root.Volume || lease.File.Inode == 0 {
			s.mu.Lock()
			d.uncertain = true
			s.mu.Unlock()
			return artifact.Ref{}, nil, ErrUnavailable
		}
		if _, e = c.checkRemotePermit(ctx, p); e != nil {
			return artifact.Ref{}, nil, e
		}
		reader, e = s.files.OpenObject(ref, artifact.FileID{Device: lease.File.Device, Inode: lease.File.Inode})
		s.mu.Lock()
		d.reader = reader
		d.uncertain = artifactCloseUncertain(e)
		s.mu.Unlock()
		if e != nil {
			return artifact.Ref{}, nil, c.artifactFileError(call, e)
		}
	}
	count := int64(limit)
	if count > ref.Size-offset {
		count = ref.Size - offset
	}
	s.mu.Lock()
	published := d.remote.published
	s.mu.Unlock()
	if offset < published && offset+count > published {
		return artifact.Ref{}, nil, ErrInvalid
	}
	if _, err = c.checkRemotePermit(ctx, p); err != nil {
		return artifact.Ref{}, nil, err
	}
	data := make([]byte, int(count))
	n, err := reader.ReadAt(data, offset)
	if err != nil && !(err == io.EOF && count == 0) {
		return artifact.Ref{}, nil, err
	}
	if n != len(data) {
		return artifact.Ref{}, nil, io.ErrUnexpectedEOF
	}
	fresh, err := c.checkRemotePermit(ctx, p)
	if err != nil {
		return artifact.Ref{}, nil, err
	}
	s.mu.Lock()
	p.grant = fresh
	p.readRef = ref
	p.readOffset = offset
	p.readEnd = offset + int64(n)
	p.readArmed = true
	s.mu.Unlock()
	return ref, data, nil
}

// PollRemoteRelease returns only the current scoped collection metadata.
func (c *Control) PollRemoteRelease(ctx context.Context, call *ArtifactCall) (store.RemoteRelease, error) {
	p, end, err := c.beginRemoteIO(ctx, call, RemotePollRelease, "")
	if err != nil {
		return store.RemoteRelease{}, err
	}
	defer end()
	out, err := c.store.PollRemoteRelease(p.grant.Key.AttemptID, p.hash, time.Now().UTC())
	if err != nil {
		return store.RemoteRelease{}, err
	}
	if _, err = c.checkRemotePermit(ctx, p); err != nil {
		return store.RemoteRelease{}, err
	}
	return out, nil
}

func (c *Control) remoteUploadIO(ctx context.Context, call *ArtifactCall, op RemoteOperation) (*workerPermit, store.ArtifactUpload, store.ArtifactFile, store.ArtifactManifest, func(), error) {
	if c == nil || call == nil || c.artifacts == nil {
		return nil, store.ArtifactUpload{}, store.ArtifactFile{}, store.ArtifactManifest{}, nil, ErrInvalid
	}
	s := c.artifacts
	s.mu.Lock()
	p := call.worker
	id := ""
	if p != nil {
		key := p.grant.Key
		s.mu.Unlock()
		row, err := c.store.GetRemoteAttempt(key)
		if err != nil || row.Active == nil {
			return nil, store.ArtifactUpload{}, store.ArtifactFile{}, store.ArtifactManifest{}, nil, ErrUnavailable
		}
		id = row.Active.ResultID
	} else {
		s.mu.Unlock()
		return nil, store.ArtifactUpload{}, store.ArtifactFile{}, store.ArtifactManifest{}, nil, ErrInvalid
	}
	p, end, err := c.beginRemoteIO(ctx, call, op, id)
	if err != nil {
		return nil, store.ArtifactUpload{}, store.ArtifactFile{}, store.ArtifactManifest{}, nil, err
	}
	u, file, manifest, err := c.store.GetRemoteOutput(p.grant, time.Now().UTC())
	if err != nil {
		end()
		return nil, store.ArtifactUpload{}, store.ArtifactFile{}, store.ArtifactManifest{}, nil, err
	}
	return p, u, file, manifest, end, nil
}

// BeginRemoteOutput reserves an output against its server-selected result ID.
// An existing unknown creating record is held, never adopted or recreated.
func (c *Control) BeginRemoteOutput(ctx context.Context, call *ArtifactCall, sha string, size int64) (store.ArtifactUpload, error) {
	if size < 1 || size > 32<<20 || len(sha) != 64 || strings.Trim(sha, "0123456789abcdef") != "" {
		return store.ArtifactUpload{}, ErrInvalid
	}
	if c == nil || call == nil || c.artifacts == nil {
		return store.ArtifactUpload{}, ErrInvalid
	}
	s := c.artifacts
	s.mu.Lock()
	p := call.worker
	s.mu.Unlock()
	if p == nil {
		return store.ArtifactUpload{}, ErrInvalid
	}
	row, err := c.store.GetRemoteAttempt(p.grant.Key)
	if err != nil || row.Active == nil {
		return store.ArtifactUpload{}, ErrUnavailable
	}
	p, end, err := c.beginRemoteIO(ctx, call, RemoteBeginOutput, row.Active.ResultID)
	if err != nil {
		return store.ArtifactUpload{}, err
	}
	defer end()
	existing, _, _, getErr := c.store.GetRemoteOutput(p.grant, time.Now().UTC())
	if getErr == nil {
		if existing.State == "creating" {
			return store.ArtifactUpload{}, ErrUnavailable
		}
		return c.store.BeginRemoteOutput(p.grant, sha, uint64(size), time.Now().UTC())
	}
	if !errors.Is(getErr, store.ErrNotFound) {
		return store.ArtifactUpload{}, getErr
	}
	if err = c.cachedLinkGrowthCheck(); err != nil {
		return store.ArtifactUpload{}, err
	}
	finish := c.beginUploadStructure(call)
	defer finish()
	upload, err := c.store.BeginRemoteOutput(p.grant, sha, uint64(size), time.Now().UTC())
	if err != nil {
		return store.ArtifactUpload{}, err
	}
	if upload.State != "creating" {
		return store.ArtifactUpload{}, ErrUnavailable
	}
	ref, err := artifactPortableRef(upload.Reference)
	if err != nil {
		return store.ArtifactUpload{}, err
	}
	if _, err = c.checkRemotePermit(ctx, p); err != nil {
		return store.ArtifactUpload{}, err
	}
	file, err := s.files.CreatePart(ref)
	if err != nil {
		return store.ArtifactUpload{}, c.artifactFileError(call, err)
	}
	if file.Device != s.root.Volume || file.Inode == 0 {
		return store.ArtifactUpload{}, ErrUnavailable
	}
	cursor := store.ArtifactCursor{ID: upload.Reference.ID, Revision: upload.Revision}
	observed := store.ArtifactFile{Device: file.Device, Inode: file.Inode}
	grant, checkErr := c.checkRemotePermit(ctx, p)
	if checkErr == nil {
		created, e := c.store.RemotePartCreated(grant, cursor, observed, time.Now().UTC())
		if e == nil {
			return created, nil
		}
		err = e
	} else {
		err = checkErr
	}
	// Only the nil-error Create return qualifies this exact original part handoff.
	_, settleErr := c.store.AbandonArtifactCreate(store.ArtifactCreateAbandonment{Owner: p.authority.Principal, Cursor: cursor, Ref: upload.Reference, File: observed, RootID: s.root.ID})
	return store.ArtifactUpload{}, errors.Join(err, settleErr)
}

// WriteRemoteOutput syncs a bounded new prefix or verifies an acknowledged range.
// Acknowledgement is metadata, never an enclosing response/descriptor receipt.
func (c *Control) WriteRemoteOutput(ctx context.Context, call *ArtifactCall, offset int64, data []byte) (uint64, error) {
	p, u, file, _, end, err := c.remoteUploadIO(ctx, call, RemoteWriteOutput)
	if err != nil {
		return 0, err
	}
	defer end()
	ref, err := artifactPortableRef(u.Reference)
	if err != nil {
		return 0, err
	}
	if u.State != "writable" || file.Inode == 0 || offset < 0 || len(data) < 1 || len(data) > 64<<10 || offset > ref.Size || int64(len(data)) > ref.Size-offset || uint64(offset) > u.Acknowledged || uint64(offset) < u.Acknowledged && uint64(len(data)) > u.Acknowledged-uint64(offset) {
		return 0, ErrInvalid
	}
	if uint64(offset) == u.Acknowledged {
		if err = c.cachedLinkGrowthCheck(); err != nil {
			return 0, err
		}
	}
	payload := bytes.Clone(data)
	identity := artifact.FileID{Device: file.Device, Inode: file.Inode}
	if err = c.artifacts.files.RecoverPart(ref, identity, int64(u.Acknowledged)); err != nil {
		return 0, c.artifactFileError(call, err)
	}
	if _, err = c.checkRemotePermit(ctx, p); err != nil {
		return 0, err
	}
	receipt, err := c.artifacts.files.WritePart(ref, identity, int64(u.Acknowledged), offset, payload)
	if err != nil {
		return 0, c.artifactFileError(call, err)
	}
	if receipt.File != identity || receipt.Offset != offset || receipt.Size != int64(len(payload)) {
		c.physicalChanged()
		return 0, ErrUnavailable
	}
	grant, err := c.checkRemotePermit(ctx, p)
	if err != nil {
		return 0, err
	}
	cursor := store.ArtifactCursor{ID: u.Reference.ID, Revision: u.Revision}
	if uint64(offset) < u.Acknowledged {
		latest, latestFile, _, e := c.store.GetRemoteOutput(grant, time.Now().UTC())
		if e != nil {
			return 0, e
		}
		if latestFile != file || latest.Reference != u.Reference || latest.Revision != u.Revision || latest.State != "writable" || latest.Acknowledged != u.Acknowledged {
			return 0, store.ErrArtifactConflict
		}
		return latest.Acknowledged, nil
	}
	updated, err := c.store.RemoteChunkSynced(grant, cursor, file, uint64(offset), uint64(len(payload)), receipt.SHA256, time.Now().UTC())
	if err != nil {
		return 0, err
	}
	return updated.Acknowledged, nil
}

// FinishRemoteOutput seals verified output privately. It does not attach a job
// result or grant owner access; original native completion remains independent.
func (c *Control) FinishRemoteOutput(ctx context.Context, call *ArtifactCall) (store.ArtifactUpload, error) {
	p, u, file, _, end, err := c.remoteUploadIO(ctx, call, RemoteFinishOutput)
	if err != nil {
		return store.ArtifactUpload{}, err
	}
	defer end()
	if u.State == "complete" {
		return u, nil
	}
	if file.Inode == 0 || u.Acknowledged != u.Reference.Size || u.State != "writable" && u.State != "finalizing" {
		return store.ArtifactUpload{}, store.ErrArtifactConflict
	}
	cursor := store.ArtifactCursor{ID: u.Reference.ID, Revision: u.Revision}
	grant, err := c.checkRemotePermit(ctx, p)
	if err != nil {
		return store.ArtifactUpload{}, err
	}
	if u.State == "writable" {
		if err = c.cachedLinkGrowthCheck(); err != nil {
			return store.ArtifactUpload{}, err
		}
		finish := c.beginUploadStructure(call)
		u, err = c.store.BeginRemoteSeal(grant, cursor, time.Now().UTC())
		finish()
		if err != nil {
			return store.ArtifactUpload{}, err
		}
		return u, nil // Actual publishing cohort must be observed before a new link.
	}
	ref, err := artifactPortableRef(u.Reference)
	if err != nil {
		return store.ArtifactUpload{}, err
	}
	u, err = c.store.BeginRemoteSeal(grant, cursor, time.Now().UTC())
	if err != nil {
		return store.ArtifactUpload{}, err
	}
	cursor.Revision = u.Revision
	sealed, err := c.artifacts.files.SealPart(ref, artifact.FileID{Device: file.Device, Inode: file.Inode}, c.uploadLinkGrowthCheck)
	if err != nil {
		if !errors.Is(err, artifact.ErrFilesBusy) || artifactCloseUncertain(err) {
			c.physicalChanged()
		}
		return store.ArtifactUpload{}, c.artifactFileError(call, err)
	}
	c.physicalChanged()
	if sealed.File != (artifact.FileID{Device: file.Device, Inode: file.Inode}) || sealed.Manifest.Reference != ref {
		return store.ArtifactUpload{}, ErrUnavailable
	}
	grant, err = c.checkRemotePermit(ctx, p)
	if err != nil {
		return store.ArtifactUpload{}, err
	}
	seal := store.RemoteOutputSeal{Reference: u.Reference, File: store.ArtifactFile{Device: sealed.File.Device, Inode: sealed.File.Inode}, Manifest: store.ArtifactManifest{Reference: u.Reference, ExpandedBytes: sealed.Manifest.ExpandedBytes, RegularFiles: sealed.Manifest.RegularFiles, Directories: sealed.Manifest.Directories}}
	finish := c.beginUploadStructure(call)
	err = c.store.SealRemoteOutput(grant, cursor, seal, time.Now().UTC())
	finish()
	if err != nil {
		return store.ArtifactUpload{}, err
	}
	complete, _, _, err := c.store.GetRemoteOutput(grant, time.Now().UTC())
	return complete, err
}

func (c *Control) finishRemoteResponse(h *remoteHTTP) error {
	if c == nil || c.artifacts == nil || h == nil || h.call == nil {
		return ErrUnavailable
	}
	s := c.artifacts
	s.mu.Lock()
	defer s.mu.Unlock()
	call := h.call
	p := call.worker
	if _, ok := s.calls[call]; !ok || call.service != s || p == nil || p.call != call || p.response != h || !p.pending || call.ioActive {
		return ErrUnavailable
	}
	if !h.bodyJoined || h.closeErr != nil || h.invoked && !h.returned {
		p.err = errors.Join(ErrUnavailable, h.closeErr)
		call.fileError = p.err
		s.artifactChangedLocked()
		return p.err
	}
	if call.fileError != nil {
		p.err = call.fileError
		return p.err
	}
	d := call.download
	if d == nil {
		if p.input != nil {
			return ErrUnavailable
		}
		p.pending = false
		call.releaseLocked()
		s.artifactChangedLocked()
		return nil
	}
	if _, ok := s.downloads[d]; !ok || d.remote == nil || d.remote.key != p.grant.Key || d.remote.call != call || d.remote.finish != nil || d.owners <= 0 || p.readerUseDetached || d.uncertain {
		return ErrUnavailable
	}
	published := remoteReadPublished(h, p)
	if published {
		if p.readOffset == d.remote.published {
			d.remote.published = p.readEnd
		} else if p.readEnd > d.remote.published {
			return ErrUnavailable
		}
	}
	d.owners--
	call.download = nil
	p.readerUseDetached = true
	if published && d.remote.published < d.ref.Size && !d.closing {
		d.remote.call = nil
		p.pending = false
		call.releaseLocked()
		s.artifactChangedLocked()
		return nil
	}
	f := &remoteInputFinish{call: call, download: d, grant: p.grant, complete: published && d.remote.published == d.ref.Size && d.reader != nil}
	p.input = f
	d.remote.finish = f
	s.artifactChangedLocked()
	return nil
}

// These bounded queries use the original existing frames. They grant no
// previous-process, native stop or physical closure authority.
func (c *Control) remoteCompletionFencedLocked(key store.RemoteKey) bool {
	c.runMu.Lock()
	defer c.runMu.Unlock()
	if c.run == nil {
		return false
	}
	for i := range c.run.state.workers {
		p := c.run.state.workers[i].dataCompletion
		if p != nil && p.key == key && p.fenced {
			return true
		}
	}
	return false
}
func (c *Control) remoteAttemptTransfersSettled(key store.RemoteKey) (bool, error) {
	if c.artifacts == nil {
		return false, ErrUnsupported
	}
	s := c.artifacts
	s.mu.Lock()
	defer s.mu.Unlock()
	for call := range s.calls {
		if p := call.worker; p != nil && p.grant.Key == key {
			if p.err != nil {
				return false, p.err
			}
			return false, nil
		}
	}
	for d := range s.downloads {
		if d.remote != nil && d.remote.key == key {
			if d.uncertain {
				return false, errors.Join(ErrUnavailable, d.closeError)
			}
			return false, nil
		}
	}
	// The SAME per-target completion frame is the admission fence. Attachment and
	// this transition share service.mu; no lookup/commit race creates a new owner.
	c.runMu.Lock()
	defer c.runMu.Unlock()
	if c.run == nil {
		return false, nil
	}
	for i := range c.run.state.workers {
		p := c.run.state.workers[i].dataCompletion
		if p != nil && p.key == key {
			p.fenced = true
			return true, nil
		}
	}
	return false, nil
}
func (c *Control) remoteTransfersPrepared() (bool, error) {
	if c.artifacts == nil {
		return true, nil
	}
	s := c.artifacts
	s.mu.Lock()
	defer s.mu.Unlock()
	for call := range s.calls {
		if p := call.worker; p != nil {
			if p.err != nil {
				return false, p.err
			}
			return false, nil
		}
	}
	for d := range s.downloads {
		if d.remote == nil {
			continue
		}
		r := d.remote
		if d.uncertain {
			return false, errors.Join(ErrUnavailable, d.closeError)
		}
		if d.reader == nil || d.owners != 0 || d.settling || d.closing || d.finished || r.call != nil || r.finish != nil || r.published >= d.ref.Size {
			return false, nil
		}
	}
	return true, nil
}

func (c *Control) dispatchRemoteInputFinish(m *maintenanceState, now time.Time) (bool, error) {
	if c.artifacts == nil || m.pending != nil || m.received != nil || m.expiryHeld != nil || now.Before(m.remoteInputDue) {
		return false, nil
	}
	m.remoteInputDue = now.Add(time.Second)
	s := c.artifacts
	s.mu.Lock()
	var held []*remoteInputFinish
	for call := range s.calls {
		if p := call.worker; p != nil && p.input != nil && p.input.held {
			held = append(held, p.input)
		}
	}
	s.mu.Unlock()
	for _, f := range held {
		// Retry only the SAME invoked frame's Store Finish; its Close never repeats.
		err := c.finishRemoteInput(f.close)
		if errors.Is(err, auth.ErrUnavailable) {
			continue
		}
		if err != nil {
			return false, err
		}
		f.close.release()
		s.mu.Lock()
		f.held = false
		s.mu.Unlock()
	}
	s.mu.Lock()
	var ready []*remoteInputFinish
	for call := range s.calls {
		p := call.worker
		if p == nil || p.input == nil {
			continue
		}
		f := p.input
		d := f.download
		if p.err != nil {
			s.mu.Unlock()
			return false, p.err
		}
		if f.err != nil {
			s.mu.Unlock()
			return false, f.err
		}
		if f.call == call && d != nil && d.remote != nil && d.remote.finish == f && d.remote.call == call && d.owners == 0 && p.readerUseDetached && !call.ioActive && call.httpDone && !d.uncertain && !d.finished && !d.settling {
			ready = append(ready, f)
		}
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i].download.lease.ID < ready[j].download.lease.ID })
	var f *remoteInputFinish
	for _, candidate := range ready {
		if candidate.download.lease.ID > m.remoteInputAfter {
			f = candidate
			break
		}
	}
	if f == nil && len(ready) != 0 {
		f = ready[0]
	}
	if f == nil {
		s.mu.Unlock()
		return false, nil
	}
	m.remoteInputAfter = f.download.lease.ID
	s.mu.Unlock()
	if m.serial == ^uint64(0) {
		return false, ErrUnavailable
	}
	frame, err := f.download.prepareOriginalClose()
	if frame != nil {
		s.mu.Lock()
		frame.remote = f
		f.close = frame
		s.mu.Unlock()
	}
	if err != nil {
		if errors.Is(err, ErrArtifactBusy) && (frame == nil || frame.err == nil) {
			if frame != nil {
				frame.release()
			}
			return false, nil
		}
		if frame != nil {
			m.expiryHeld = frame
		}
		s.mu.Lock()
		f.err = err
		s.mu.Unlock()
		return false, err
	}
	if frame == nil {
		return false, ErrUnavailable
	}
	if f.complete && frame.neverOpened {
		frame.err = ErrUnavailable
		m.expiryHeld = frame
		return false, ErrUnavailable
	}
	request := maintenanceRequest{sequence: m.serial + 1, downloadClose: frame}
	m.serial = request.sequence
	m.pending = &request
	m.input <- request
	return true, nil
}

func (c *Control) finishRemoteInput(frame *artifactDownloadClose) error {
	f := frame.remote
	d := frame.download
	if f == nil || f.close != frame || f.download != d || f.call == nil {
		return ErrUnavailable
	}
	s := c.artifacts
	s.mu.Lock()
	p := f.call.worker
	valid := p != nil && p.input == f && p.pending && p.readerUseDetached && d.remote != nil && d.remote.finish == f && d.remote.call == f.call && frame.originalLocked() && !d.uncertain
	s.mu.Unlock()
	if !valid {
		return ErrUnavailable
	}
	now := time.Now().UTC()
	if f.complete {
		grant, err := c.checkRemoteFinishGrant(p)
		if err == nil {
			s.mu.Lock()
			f.grant = grant
			s.mu.Unlock()
			err = c.store.FinishRemoteInput(grant, frame.closure, now)
			if err != nil {
				return err
			}
		} else {
			// Only a positively classified closed/expired admission may use ordinary
			// partial retirement; an ambiguous Store error retains the original pair.
			row, e := c.store.GetRemoteAttempt(p.grant.Key)
			if e != nil {
				return e
			}
			if row.Active != nil && row.Active.Input != nil && row.Active.Input.State == 4 {
				if e = c.store.FinishRemoteInput(f.grant, frame.closure, now); e != nil {
					return e
				}
			} else if row.Active != nil && (row.Active.ClosedReason != 0 || !now.Before(row.Active.ResourceDeadline) || row.Active.Input != nil && (row.Active.Input.State == 3 || !now.Before(row.Active.Input.Expires))) {
				if e = c.store.FinishArtifactDownload(frame.closure, now); e != nil {
					return e
				}
			} else {
				return err
			}
		}
	} else if err := c.store.FinishArtifactDownload(frame.closure, now); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !frame.originalLocked() {
		return ErrUnavailable
	}
	d.finished = true
	delete(s.downloads, d)
	d.remote.call = nil
	d.remote.finish = nil
	p.pending = false
	p.input = nil
	f.call.releaseLocked()
	s.artifactChangedLocked()
	return nil
}
func (c *Control) checkRemoteFinishGrant(p *workerPermit) (store.RemoteGrant, error) {
	// Done is expected here: original response ownership already joined. Do not
	// reopen ordinary call admission merely to refresh detached CAS metadata.
	row, grant, err := c.remoteCredential(context.Background(), p.grant.Key, p.hash, RemoteReadInput)
	if err != nil {
		return store.RemoteGrant{}, err
	}
	if row.Authority != p.authority || grant.Key != p.grant.Key || grant.Kind != store.RemoteInput || grant.Epoch != p.grant.Epoch {
		return store.RemoteGrant{}, ErrUnavailable
	}
	return grant, nil
}
