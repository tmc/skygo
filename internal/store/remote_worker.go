package store

import (
	"crypto/subtle"
	"time"

	bolt "go.etcd.io/bbolt"
)

func remoteScope(r remoteRecord, kind RemoteGrantKind) *remoteLease {
	if r.Active == nil {
		return nil
	}
	switch kind {
	case RemoteInput:
		return r.Active.Input
	case RemoteOutput:
		return r.Active.Output
	}
	return nil
}

func remoteGrantValue(id string, r remoteRecord, kind RemoteGrantKind, l *remoteLease) RemoteGrant {
	return RemoteGrant{Key: RemoteKey{JobID: r.Job, AttemptID: id}, Kind: kind, Epoch: l.Epoch, Expires: l.Expires}
}

// CheckRemoteGrant checks a purpose-separated secret without renewing it.
// The caller checks the original delegation outside this transaction.
func (s *Store) CheckRemoteGrant(attemptID string, kind RemoteGrantKind, hash [32]byte, now time.Time) (out RemoteGrant, err error) {
	if !remoteCheckTime(now) {
		return out, ErrArtifactConflict
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		r, ok := st.Remote[attemptID]
		if !ok {
			return ErrNotFound
		}
		job, e := get(tx, r.Job)
		if e != nil {
			return e
		}
		if job.CurrentAttemptID() != attemptID {
			return ErrNotFound
		}
		l := remoteScope(r, kind)
		if l == nil || hash == ([32]byte{}) || subtle.ConstantTimeCompare(l.Hash[:], hash[:]) != 1 {
			return ErrNotFound
		}
		g := remoteGrantValue(attemptID, r, kind, l)
		if _, _, e = remoteGrantRecord(st, g, now); e != nil {
			return e
		}
		out = g
		return nil
	})
	return
}

// PollRemoteRelease returns metadata to a historically activated helper only.
// A matching closed scope returns abort and conveys no byte or renewal grant.
func (s *Store) PollRemoteRelease(attemptID string, hash [32]byte, now time.Time) (out RemoteRelease, err error) {
	if !remoteCheckTime(now) {
		return out, ErrArtifactConflict
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		r, ok := st.Remote[attemptID]
		if !ok || r.Creation != 3 || r.Resources.Pod == nil {
			return ErrNotFound
		}
		l := remoteScope(r, RemoteOutput)
		if l == nil || hash == ([32]byte{}) || subtle.ConstantTimeCompare(l.Hash[:], hash[:]) != 1 {
			return ErrNotFound
		}
		out = RemoteRelease{State: 3, Revision: r.Revision}
		a := r.Active
		job, e := get(tx, r.Job)
		if e != nil {
			return e
		}
		if job.CurrentAttemptID() != attemptID {
			return nil
		}
		if r.State != 2 || a.ClosedReason != 0 || l.State != 2 || !now.Before(l.Expires) || !now.Before(a.ResourceDeadline) || a.Phase == 6 || a.NoLaunch != nil {
			return nil
		}
		if a.Phase <= 2 && !now.Before(a.StageDeadline) {
			return nil
		}
		if a.Stop == nil {
			if a.TaskDeadline != nil && !now.Before(*a.TaskDeadline) {
				return nil
			}
			out.State = 1
			return nil
		}
		deadline, e := remoteCollectionDeadline(a)
		if e != nil || a.ReleaseRevision == 0 || deadline == nil || !now.Before(*deadline) {
			return nil
		}
		if _, _, e = remoteGrantRecord(st, remoteGrantValue(attemptID, r, RemoteOutput, l), now); e != nil {
			return nil
		}
		code := a.Stop.Task.Code
		out = RemoteRelease{State: 2, Revision: a.ReleaseRevision, TaskContainerID: a.Stop.Task.ID, TaskExitCode: &code}
		return nil
	})
	return
}

// BeginRemoteInputRead admits a server-owned reader against the job's input pin.
// Its lifetime remains charged until the original I/O and HTTP owners join.
func (s *Store) BeginRemoteInputRead(grant RemoteGrant, producer OriginalServerLifetime, now time.Time) (out ArtifactLease, err error) {
	if grant.Kind != RemoteInput {
		return out, ErrArtifactConflict
	}
	p, e := artifactProducer(producer)
	if e != nil {
		return out, e
	}
	id, e := newID()
	if e != nil {
		return out, e
	}
	err = s.artifactUpdate(func(st *artifactState) error {
		r, _, e := remoteGrantRecord(st, grant, now)
		if e != nil {
			return e
		}
		owner, e := remoteOwner(st, r)
		if e != nil {
			return e
		}
		pin, ok := st.JobInputs[r.Job]
		if !ok || pin.Owner != owner {
			return ErrArtifactConflict
		}
		o, ok := st.Objects[pin.Ref.ID]
		if !ok || o.Owner != owner || o.Ref != pin.Ref || o.State != "complete" || !artifactFileValid(o.File) {
			return ErrArtifactConflict
		}
		if _, ok := st.Reads[id]; ok {
			return ErrArtifactConflict
		}
		expires := now.Add(5 * time.Minute)
		if grant.Expires.Before(expires) {
			expires = grant.Expires
		}
		read := artifactRead{RemoteAttemptID: grant.Key.AttemptID, Owner: owner, Ref: pin.Ref, Revision: 1, Expires: expires, Producer: p}
		st.Reads[id] = read
		out = artifactLeaseDTO(id, read, o.File, st.Root.ID)
		return nil
	})
	return
}

// FinishRemoteInput consumes the original complete-transfer closure only after
// actual response, reader and descriptor owners join. Ordinary partial closure
// uses FinishArtifactDownload and cannot close this input scope.
func (s *Store) FinishRemoteInput(grant RemoteGrant, original ArtifactDownloadClosure, now time.Time) error {
	if grant.Kind != RemoteInput {
		return ErrArtifactConflict
	}
	if !remoteCheckTime(now) {
		return ErrArtifactConflict
	}
	return s.artifactUpdate(func(st *artifactState) error {
		r, e := remoteGet(st, grant.Key)
		if e != nil {
			return e
		}
		read, e := artifactMatchDownload(st, original)
		if e != nil {
			return e
		}
		owner, e := remoteOwner(st, r)
		if e != nil {
			return e
		}
		l := remoteScope(r, RemoteInput)
		if read.RemoteAttemptID != grant.Key.AttemptID || read.Owner != owner || l == nil || l.Epoch != grant.Epoch || !l.Expires.Equal(grant.Expires) || !read.Closing || read.Revision != original.Revision {
			return ErrArtifactConflict
		}
		// An exact already-consumed closure is replay, never renewed admission.
		if l.State == 4 && read.Closed {
			return nil
		}
		if read.Closed {
			return ErrArtifactConflict
		} // partial closure cannot be upgraded to complete transfer.
		r, l, e = remoteGrantRecord(st, grant, now)
		if e != nil {
			return e
		}
		if e = artifactReaderClosed(st, owner, ArtifactCursor{ID: original.LeaseID, Revision: original.Revision}, now); e != nil {
			return e
		}
		l.State = 4
		if e = remoteBump(&r); e != nil {
			return e
		}
		st.Remote[grant.Key.AttemptID] = r
		return nil
	})
}

func remoteUpload(st *artifactState, grant RemoteGrant, cursor ArtifactCursor, now time.Time) (remoteRecord, artifactUploadRecord, error) {
	r, _, err := remoteGrantRecord(st, grant, now)
	if err != nil {
		return r, artifactUploadRecord{}, err
	}
	if grant.Kind != RemoteOutput || cursor.ID != remoteOutputID(r) {
		return r, artifactUploadRecord{}, ErrArtifactConflict
	}
	owner, err := remoteOwner(st, r)
	if err != nil {
		return r, artifactUploadRecord{}, err
	}
	u, ok := st.Uploads[cursor.ID]
	if !ok || u.Owner != owner || u.RemoteAttemptID != grant.Key.AttemptID || u.Ref.ID != cursor.ID || u.Revision != cursor.Revision || !now.Before(u.Expires) {
		return r, u, ErrArtifactConflict
	}
	return r, u, nil
}

// BeginRemoteOutput commits declared bytes against the original result ID.
// Replay compares content and never extends the original reservation lifetime.
func (s *Store) BeginRemoteOutput(grant RemoteGrant, sha string, size uint64, now time.Time) (out ArtifactUpload, err error) {
	if grant.Kind != RemoteOutput || !artifactHex(sha, 64) || size == 0 || size > artifactMaxSize {
		return out, ErrArtifactConflict
	}
	err = s.artifactUpdate(func(st *artifactState) error {
		r, _, e := remoteGrantRecord(st, grant, now)
		if e != nil {
			return e
		}
		owner, e := remoteOwner(st, r)
		if e != nil {
			return e
		}
		id := remoteOutputID(r)
		o, ok := st.Objects[id]
		if !ok || o.Owner != owner || o.Origin != "remote_result" || o.Source != grant.Key.AttemptID || o.JobID != r.Job {
			return ErrArtifactConflict
		}
		ref := ArtifactRef{ID: id, SHA256: sha, Size: size}
		if u, ok := st.Uploads[id]; ok {
			if u.Ref != ref || u.RemoteAttemptID != grant.Key.AttemptID || u.Owner != owner || u.State == "deleting" || !now.Before(u.Expires) {
				return ErrArtifactConflict
			}
			out = artifactUploadDTO(u)
			return nil
		}
		deadline, e := remoteCollectionDeadline(r.Active)
		if e != nil || o.State != "reserved" || deadline == nil {
			return ErrArtifactConflict
		}
		u := artifactUploadRecord{RemoteAttemptID: grant.Key.AttemptID, Owner: owner, Ref: ref, Revision: 1, State: "creating", Created: now, Expires: *deadline}
		o.Ref = ref
		st.Objects[id] = o
		st.Uploads[id] = u
		out = artifactUploadDTO(u)
		return nil
	})
	return
}

// RemotePartCreated accepts the original server's exclusively created part.
func (s *Store) RemotePartCreated(grant RemoteGrant, cursor ArtifactCursor, file ArtifactFile, now time.Time) (out ArtifactUpload, err error) {
	if !artifactFileValid(file) {
		return out, ErrArtifactConflict
	}
	err = s.artifactUpdate(func(st *artifactState) error {
		_, u, e := remoteUpload(st, grant, cursor, now)
		if e != nil {
			return e
		}
		if u.State != "creating" || file.Device != st.Root.Volume {
			return ErrArtifactConflict
		}
		u.Revision, e = artifactNext(u.Revision)
		if e != nil {
			return e
		}
		u.State = "writable"
		u.Part = file
		st.Uploads[cursor.ID] = u
		out = artifactUploadDTO(u)
		return nil
	})
	return
}

// RemoteChunkSynced acknowledges one original synced contiguous server chunk.
// Actual prefix equality and blocking I/O remain outside the transaction.
func (s *Store) RemoteChunkSynced(grant RemoteGrant, cursor ArtifactCursor, file ArtifactFile, offset, size uint64, sha string, now time.Time) (out ArtifactUpload, err error) {
	if size == 0 || size > artifactChunkSize || !artifactHex(sha, 64) {
		return out, ErrArtifactConflict
	}
	err = s.artifactUpdate(func(st *artifactState) error {
		_, u, e := remoteUpload(st, grant, cursor, now)
		if e != nil {
			return e
		}
		if u.State != "writable" || u.Part != file {
			return ErrArtifactConflict
		}
		if offset == u.LastOffset && size == u.LastSize && sha == u.LastSHA256 && offset <= u.Ack && size == u.Ack-offset {
			out = artifactUploadDTO(u)
			return nil
		}
		if offset != u.Ack || u.Ack > u.Ref.Size || size > u.Ref.Size-u.Ack {
			return ErrArtifactConflict
		}
		u.Revision, e = artifactNext(u.Revision)
		if e != nil {
			return e
		}
		u.LastOffset = offset
		u.LastSize = size
		u.LastSHA256 = sha
		u.Ack += size
		st.Uploads[cursor.ID] = u
		out = artifactUploadDTO(u)
		return nil
	})
	return
}

// BeginRemoteSeal reserves private publication after the full part is synced.
func (s *Store) BeginRemoteSeal(grant RemoteGrant, cursor ArtifactCursor, now time.Time) (out ArtifactUpload, err error) {
	err = s.artifactUpdate(func(st *artifactState) error {
		_, u, e := remoteUpload(st, grant, cursor, now)
		if e != nil {
			return e
		}
		o, ok := st.Objects[cursor.ID]
		if !ok || o.Origin != "remote_result" || o.Source != grant.Key.AttemptID || o.Ref != u.Ref || o.Owner != u.Owner {
			return ErrArtifactConflict
		}
		if u.State == "finalizing" && o.State == "publishing" {
			out = artifactUploadDTO(u)
			return nil
		}
		if u.State != "writable" || u.Ack != u.Ref.Size || o.State != "reserved" {
			return ErrArtifactConflict
		}
		u.Revision, e = artifactNext(u.Revision)
		if e != nil {
			return e
		}
		u.State = "finalizing"
		o.State = "publishing"
		st.Uploads[cursor.ID] = u
		st.Objects[cursor.ID] = o
		out = artifactUploadDTO(u)
		return nil
	})
	return
}

// SealRemoteOutput records a verified private candidate after original digest,
// archive, exclusive publication, part unlink and directory sync owners join.
// It publishes neither an owner grant nor a terminal job decision.
func (s *Store) SealRemoteOutput(grant RemoteGrant, cursor ArtifactCursor, seal RemoteOutputSeal, now time.Time) error {
	if !artifactFileValid(seal.File) || !artifactManifestValid(seal.Manifest, seal.Reference) {
		return ErrArtifactConflict
	}
	return s.artifactUpdate(func(st *artifactState) error {
		_, u, e := remoteUpload(st, grant, cursor, now)
		if e != nil {
			return e
		}
		o, ok := st.Objects[cursor.ID]
		if !ok || o.Origin != "remote_result" || o.Source != grant.Key.AttemptID || o.Ref != u.Ref || u.Ref != seal.Reference || o.Owner != u.Owner || seal.File.Device != st.Root.Volume {
			return ErrArtifactConflict
		}
		if u.State == "complete" && o.State == "complete" {
			if o.File != seal.File || o.Manifest != seal.Manifest {
				return ErrArtifactConflict
			}
			return nil
		}
		if u.State != "finalizing" || o.State != "publishing" {
			return ErrArtifactConflict
		}
		if _, ok := st.Grants[cursor.ID]; ok {
			return ErrArtifactConflict
		}
		u.Revision, e = artifactNext(u.Revision)
		if e != nil {
			return e
		}
		u.State = "complete"
		o.State = "complete"
		o.File = seal.File
		o.Manifest = seal.Manifest
		st.Uploads[cursor.ID] = u
		st.Objects[cursor.ID] = o
		return nil
	})
}

// GetRemoteOutput returns the current private output upload without mutation.
// For creating, writable and finalizing uploads, file is the original part
// (zero while creation is unknown) and manifest is zero. For a complete upload,
// file and manifest are the exact sealed object's receipts. These values confer
// no filesystem or owner-grant authority. The caller must check fresh delegation
// separately and retain the original file and HTTP owners for any subsequent IO.
func (s *Store) GetRemoteOutput(grant RemoteGrant, now time.Time) (upload ArtifactUpload, file ArtifactFile, manifest ArtifactManifest, err error) {
	if grant.Kind != RemoteOutput {
		return upload, file, manifest, ErrArtifactConflict
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		r, _, e := remoteGrantRecord(st, grant, now)
		if e != nil {
			return e
		}
		job, e := get(tx, r.Job)
		if e != nil {
			return e
		}
		if job.CurrentAttemptID() != grant.Key.AttemptID {
			return ErrNotFound
		}
		owner, e := remoteOwner(st, r)
		if e != nil {
			return e
		}
		id := remoteOutputID(r)
		u, ok := st.Uploads[id]
		if !ok {
			return ErrNotFound
		}
		if u.Owner != owner || u.RemoteAttemptID != grant.Key.AttemptID || u.Ref.ID != id || !now.Before(u.Expires) {
			return ErrArtifactConflict
		}
		o, ok := st.Objects[id]
		if !ok || o.Owner != owner || o.Origin != "remote_result" || o.Source != grant.Key.AttemptID || o.JobID != r.Job || o.Ref != u.Ref {
			return ErrArtifactConflict
		}
		switch u.State {
		case "creating", "writable":
			if o.State != "reserved" {
				return ErrArtifactConflict
			}
			file = u.Part
		case "finalizing":
			if o.State != "publishing" {
				return ErrArtifactConflict
			}
			file = u.Part
		case "complete":
			if o.State != "complete" || !artifactFileValid(o.File) || !artifactManifestValid(o.Manifest, u.Ref) {
				return ErrArtifactConflict
			}
			file, manifest = o.File, o.Manifest
		default:
			return ErrArtifactConflict
		}
		upload = artifactUploadDTO(u)
		return nil
	})
	if err != nil {
		return ArtifactUpload{}, ArtifactFile{}, ArtifactManifest{}, err
	}
	return
}
