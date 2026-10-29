package store

import (
	"errors"
	"math"
	"time"

	"github.com/tmc/skygo/internal/auth"
	bolt "go.etcd.io/bbolt"
)

func artifactNext(n uint64) (uint64, error) {
	if n == math.MaxUint64 {
		return 0, ErrArtifactConflict
	}
	return n + 1, nil
}

func artifactOwnedUpload(s *artifactState, owner string, c ArtifactCursor) (artifactUploadRecord, error) {
	u, ok := s.Uploads[c.ID]
	if !ok || u.Owner != owner || u.RemoteAttemptID != "" {
		return u, ErrNotFound
	}
	if u.Revision != c.Revision {
		return u, ErrArtifactConflict
	}
	return u, nil
}

func (s *Store) artifactUpdate(change func(*artifactState) error) error {
	return s.db.Update(func(tx *bolt.Tx) error { _, err := artifactChange(tx, 1<<20, change); return err })
}

// BeginArtifactUpload reserves the declared pending bytes before any part file
// is created. Control must check fresh actor authority and acquire its actual
// operation permit separately. Fixed expiration is never extended by replay.
func (s *Store) BeginArtifactUpload(actor auth.Actor, sha string, size uint64) (out ArtifactUpload, err error) {
	if err = s.authorize(actor, auth.Mutate); err != nil {
		return
	}
	if !artifactHex(sha, 64) || size == 0 || size > artifactMaxSize {
		return out, errors.New("invalid artifact size or digest")
	}
	id, e := newID()
	if e != nil {
		return out, e
	}
	now := s.now().UTC()
	err = s.artifactUpdate(func(st *artifactState) error {
		if _, ok := st.Uploads[id]; ok {
			return ErrArtifactConflict
		}
		if _, ok := st.Objects[id]; ok {
			return ErrArtifactConflict
		}
		if _, ok := st.BeginRequests[id]; ok {
			return ErrArtifactConflict
		}
		u := artifactUploadRecord{Owner: actor.Principal(), Ref: ArtifactRef{ID: id, SHA256: sha, Size: size}, Revision: 1, State: "creating", Created: now, Expires: now.Add(time.Hour)}
		st.Uploads[id] = u
		out = artifactUploadDTO(u)
		return nil
	})
	return
}

// GetArtifactUpload reads only the caller's durable cursor. Admin authority
// does not turn guessed foreign object identities into ownership grants.
func (s *Store) GetArtifactUpload(actor auth.Actor, id string) (out ArtifactUpload, err error) {
	if err = s.authorize(actor, auth.Read); err != nil {
		return
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		u, ok := st.Uploads[id]
		if !ok || u.Owner != actor.Principal() || u.RemoteAttemptID != "" {
			return ErrNotFound
		}
		out = artifactUploadDTO(u)
		return nil
	})
	return
}

// ArtifactPartCreated commits a synced, exclusively created part identity.
// The trusted caller must bind file and root identity to this exact cursor.
func (s *Store) ArtifactPartCreated(actor auth.Actor, c ArtifactCursor, file ArtifactFile) (out ArtifactUpload, err error) {
	if err = s.authorize(actor, auth.Mutate); err != nil {
		return
	}
	if !artifactFileValid(file) {
		return out, errors.New("invalid artifact file receipt")
	}
	err = s.artifactUpdate(func(st *artifactState) error {
		u, e := artifactOwnedUpload(st, actor.Principal(), c)
		if e != nil {
			return e
		}
		if u.State != "creating" || !s.now().Before(u.Expires) {
			return ErrArtifactConflict
		}
		u.Revision, e = artifactNext(u.Revision)
		if e != nil {
			return e
		}
		u.State = "writable"
		u.Part = file
		st.Uploads[c.ID] = u
		out = artifactUploadDTO(u)
		return nil
	})
	return
}

// ArtifactChunkSynced acknowledges one bounded contiguous chunk after the
// trusted adapter has synced it and checked actual bytes for retry equality.
// A stale CAS never claims a crash suffix durable; repair remains outside tx.
func (s *Store) ArtifactChunkSynced(actor auth.Actor, c ArtifactCursor, file ArtifactFile, offset, size uint64, sha string) (out ArtifactUpload, err error) {
	if err = s.authorize(actor, auth.Mutate); err != nil {
		return
	}
	if size == 0 || size > artifactChunkSize || !artifactHex(sha, 64) {
		return out, errors.New("invalid artifact chunk receipt")
	}
	err = s.artifactUpdate(func(st *artifactState) error {
		u, e := artifactOwnedUpload(st, actor.Principal(), c)
		if e != nil {
			return e
		}
		if u.State != "writable" || u.Part != file || !s.now().Before(u.Expires) {
			return ErrArtifactConflict
		}
		if offset == u.LastOffset && size == u.LastSize && sha == u.LastSHA256 && offset+size == u.Ack {
			out = artifactUploadDTO(u)
			return nil
		}
		if offset != u.Ack || size > u.Ref.Size-u.Ack {
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
		st.Uploads[c.ID] = u
		out = artifactUploadDTO(u)
		return nil
	})
	return
}

// BeginArtifactFinalize reserves completed capacity while the pending part is
// still charged. Hash/archive validation and exclusive publication follow
// outside the transaction. An incomplete finalization keeps both charges.
func (s *Store) BeginArtifactFinalize(actor auth.Actor, c ArtifactCursor) (out ArtifactUpload, err error) {
	if err = s.authorize(actor, auth.Mutate); err != nil {
		return
	}
	err = s.artifactUpdate(func(st *artifactState) error {
		u, e := artifactOwnedUpload(st, actor.Principal(), c)
		if e != nil {
			return e
		}
		if u.State == "finalizing" {
			out = artifactUploadDTO(u)
			return nil
		}
		if u.State != "writable" || u.Ack != u.Ref.Size || !s.now().Before(u.Expires) {
			return ErrArtifactConflict
		}
		if _, ok := st.Objects[c.ID]; ok {
			return ErrArtifactConflict
		}
		u.Revision, e = artifactNext(u.Revision)
		if e != nil {
			return e
		}
		u.State = "finalizing"
		st.Uploads[c.ID] = u
		expires := s.now().UTC().Add(24 * time.Hour)
		st.Objects[c.ID] = artifactObject{Owner: u.Owner, Ref: u.Ref, Origin: "upload", Source: c.ID, State: "publishing", Expires: expires, DeleteAfter: expires}
		out = artifactUploadDTO(u)
		return nil
	})
	return
}

// CompleteArtifactUpload publishes the owner's grant only after the trusted
// adapter confirms full digest/archive validation, exclusive link, part unlink
// and both directory syncs. The supplied file is the final opened identity.
// Completion is not permission to fabricate this receipt from disk presence.
func (s *Store) CompleteArtifactUpload(actor auth.Actor, c ArtifactCursor, ref ArtifactRef, file ArtifactFile, manifest ArtifactManifest) (out ArtifactRef, err error) {
	if err = s.authorize(actor, auth.Mutate); err != nil {
		return
	}
	if !artifactFileValid(file) || !artifactManifestValid(manifest, ref) {
		return out, ErrArtifactConflict
	}
	err = s.artifactUpdate(func(st *artifactState) error {
		u, e := artifactOwnedUpload(st, actor.Principal(), c)
		if e != nil {
			return e
		}
		if u.Ref != ref {
			return ErrArtifactConflict
		}
		if u.State == "complete" {
			o := st.Objects[c.ID]
			if o.File != file || o.Manifest != manifest {
				return ErrArtifactConflict
			}
			out = ref
			return nil
		}
		o, ok := st.Objects[c.ID]
		if u.State != "finalizing" || !ok || o.State != "publishing" || !s.now().Before(u.Expires) {
			return ErrArtifactConflict
		}
		u.Revision, e = artifactNext(u.Revision)
		if e != nil {
			return e
		}
		u.State = "complete"
		st.Uploads[c.ID] = u
		o.State = "complete"
		o.File = file
		o.Manifest = manifest
		o.Expires = s.now().UTC().Add(24 * time.Hour)
		o.DeleteAfter = o.Expires
		st.Objects[c.ID] = o
		st.Grants[c.ID] = artifactGrant{Owner: u.Owner, Ref: ref, Source: "upload", SourceID: c.ID, Expires: o.Expires}
		out = ref
		return nil
	})
	return
}

// CancelArtifactUpload persists a deletion intent and retains every charge.
// The existing sole worker must settle the original operation first, then
// remove the exact owned part/publication and sync before finishing cleanup.
func (s *Store) CancelArtifactUpload(actor auth.Actor, c ArtifactCursor) (id string, err error) {
	if err = s.authorize(actor, auth.Mutate); err != nil {
		return
	}
	id, err = newID()
	if err != nil {
		return
	}
	err = s.artifactUpdate(func(st *artifactState) error {
		u, e := artifactOwnedUpload(st, actor.Principal(), c)
		if e != nil {
			return e
		}
		return artifactMarkUploadDelete(st, id, u)
	})
	return
}

// artifactMarkUploadDelete is the common concrete Cancel/expiry transition.
// It retains every reservation; only the existing deletion receipt retires it.
func artifactMarkUploadDelete(st *artifactState, id string, u artifactUploadRecord) error {
	if u.State == "complete" {
		return ErrArtifactConflict
	}
	if u.State == "deleting" {
		return ErrArtifactConflict
	}
	if _, ok := st.Deletes[id]; ok {
		return ErrArtifactConflict
	}
	kind := "upload"
	if u.State == "finalizing" {
		kind = "finalizing"
	}
	revision, err := artifactNext(u.Revision)
	if err != nil {
		return err
	}
	u.Revision = revision
	u.State = "deleting"
	st.Uploads[u.Ref.ID] = u
	st.Deletes[id] = artifactDelete{Owner: u.Owner, Kind: kind, ObjectID: u.Ref.ID, File: u.Part, Revision: u.Revision}
	return nil
}
