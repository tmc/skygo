package store

import (
	"slices"
	"time"

	"github.com/tmc/skygo/internal/auth"
	bolt "go.etcd.io/bbolt"
)

// ArtifactResultFor reads an authorized attempt's result metadata. Expired is
// a complete typed witness with no Ref; cached raw Refs instead encounter the
// revoked grant and receive the ordinary nondisclosing not-found response.
func (s *Store) ArtifactResultFor(actor auth.Actor, job, attempt string) (out ArtifactAttemptResult, err error) {
	if _, err = s.GetFor(actor, job); err != nil {
		return
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		j, e := get(tx, job)
		if e != nil {
			return e
		}
		if !visible(tx, actor, job) || !slices.Contains(executionIDs(j), attempt) {
			return ErrNotFound
		}
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		out = ArtifactAttemptResult{State: "absent", AttemptID: attempt}
		if r, ok := st.Expired[attempt]; ok {
			if r.JobID != job {
				return artifactCorrupt()
			}
			out.State = "expired"
			return nil
		}
		if r, ok := st.Results[attempt]; ok {
			if r.JobID != job {
				return artifactCorrupt()
			}
			ref := r.Ref
			out.State = "available"
			out.Reference = &ref
			return nil
		}
		if w, ok := st.Workspaces[attempt]; ok && (!w.WriterClosed || w.Held) {
			out.State = "held"
		}
		return nil
	})
	return
}

func artifactGranted(st *artifactState, owner string, ref ArtifactRef, now time.Time) error {
	g, ok := st.Grants[ref.ID]
	if !ok || g.Owner != owner || g.Ref != ref {
		return ErrNotFound
	}
	if !now.Before(g.Expires) && !st.grantPinned(ref.ID) {
		return ErrArtifactExpired
	}
	o, ok := st.Objects[ref.ID]
	if !ok || o.State != "complete" || o.Ref != ref || o.Owner != owner {
		return ErrNotFound
	}
	return nil
}

// pinned reports whether id is required by any durable consumer, including an
// environment. It protects the object from deletion.
func (st *artifactState) pinned(id string) bool {
	for _, p := range st.EnvironmentPins {
		if p.Ref.ID == id {
			return true
		}
	}
	return st.grantPinned(id)
}

// grantPinned reports whether id is required by a job, workspace, remote
// attempt or workflow. Only these keep an expired grant usable; an environment
// pin is not read permission for its owner.
func (st *artifactState) grantPinned(id string) bool {
	for _, r := range st.JobInputs {
		if r.Ref.ID == id {
			return true
		}
	}
	for _, r := range st.ManualInputs {
		if r.Ref.ID == id {
			return true
		}
	}
	for _, r := range st.Results {
		if r.Ref.ID == id {
			return true
		}
	}
	for _, r := range st.Workspaces {
		if r.Input.ID == id || r.ResultID == id {
			return true
		}
	}
	for _, r := range st.Remote {
		if r.State == 2 && remoteOutputID(r) == id {
			return true
		}
	}
	for _, in := range st.WorkflowInputs {
		if in.Ref != nil && in.Ref.ID == id {
			return true
		}
	}
	return false
}

func (st *artifactState) reading(id string) bool {
	for _, r := range st.Reads {
		if !r.Closed && r.Ref.ID == id {
			return true
		}
	}
	return false
}

// BeginArtifactDownload creates a fixed five-minute owner-scoped lease.
// Control retains the actual operation/HTTP lifetime separately; expiry alone
// never releases the durable protection. The original self producer is immutable;
// zero permits live ownership but cannot qualify automatic stopped recovery.
func (s *Store) BeginArtifactDownload(actor auth.Actor, ref ArtifactRef, producer OriginalServerLifetime) (out ArtifactLease, err error) {
	if err = s.authorize(actor, auth.Read); err != nil {
		return
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
		if err := artifactGranted(st, actor.Principal(), ref, s.now()); err != nil {
			return err
		}
		if _, ok := st.Reads[id]; ok {
			return ErrArtifactConflict
		}
		r := artifactRead{Owner: actor.Principal(), Ref: ref, Revision: 1, Expires: s.now().UTC().Add(5 * time.Minute), Producer: p}
		st.Reads[id] = r
		out = artifactLeaseDTO(id, r, st.Objects[ref.ID].File, st.Root.ID)
		return nil
	})
	return
}

// GetArtifactDownload validates a lease before a bounded filesystem read.
// The real caller must check fresh authority again after blocking I/O.
func (s *Store) GetArtifactDownload(actor auth.Actor, id string) (out ArtifactLease, err error) {
	if err = s.authorize(actor, auth.Read); err != nil {
		return
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		r, ok := st.Reads[id]
		if !ok || r.Owner != actor.Principal() || !downloadRead(r) {
			return ErrNotFound
		}
		if r.Closing || !s.now().Before(r.Expires) {
			return ErrArtifactExpired
		}
		// In particular, an expired result relation revokes a cached Ref and
		// blocks new reads even while the old reader's close is outstanding.
		if err := artifactGranted(st, r.Owner, r.Ref, s.now()); err != nil {
			return err
		}
		out = artifactLeaseDTO(id, r, st.Objects[r.Ref.ID].File, st.Root.ID)
		return nil
	})
	return
}

// CloseArtifactDownload marks a lease closing. It does not release counters.
func (s *Store) CloseArtifactDownload(actor auth.Actor, c ArtifactCursor) (out ArtifactLease, err error) {
	if err = s.authorize(actor, auth.Read); err != nil {
		return
	}
	err = s.artifactUpdate(func(st *artifactState) error {
		r, ok := st.Reads[c.ID]
		if !ok || r.Owner != actor.Principal() || !downloadRead(r) {
			return ErrNotFound
		}
		if r.Revision != c.Revision {
			return ErrArtifactConflict
		}
		if !r.Closing {
			var e error
			r.Revision, e = artifactNext(r.Revision)
			if e != nil {
				return e
			}
			r.Closing = true
			st.Reads[c.ID] = r
		}
		out = artifactLeaseDTO(c.ID, r, st.Objects[r.Ref.ID].File, st.Root.ID)
		return nil
	})
	return
}

// artifactReaderClosed is called only by the original operation owner after
// cancellation, all actual I/O/HTTP writes and descriptor closure have joined.
// A timer, missing file or process restart is not such a receipt.
func artifactReaderClosed(st *artifactState, owner string, c ArtifactCursor, now time.Time) error {
	r, ok := st.Reads[c.ID]
	if !ok || r.Owner != owner || r.Revision != c.Revision || !r.Closing {
		return ErrArtifactConflict
	}
	if r.Closed {
		return nil
	}
	if now.IsZero() {
		return ErrArtifactConflict
	}
	r.Closed = true
	r.ClosedAt = now.UTC()
	st.Reads[c.ID] = r
	st.releaseGrace(r.Ref.ID, now)
	return nil
}

// artifactMarkObjectDelete is private maintenance work. The sole worker
// supplies a fresh operation ID and may dispatch only AFTER this commits.
func artifactMarkObjectDelete(st *artifactState, id, operation string, now time.Time) error {
	o, ok := st.Objects[id]
	if !ok || o.State != "complete" || !artifactHex(operation, 32) {
		return ErrArtifactConflict
	}
	if _, ok := st.Deletes[operation]; ok {
		return ErrArtifactConflict
	}
	if st.pinned(id) || st.reading(id) {
		return ErrArtifactConflict
	}
	if now.Before(o.DeleteAfter) {
		return ErrArtifactConflict
	}
	if g, ok := st.Grants[id]; ok && now.Before(g.Expires) {
		return ErrArtifactConflict
	}
	delete(st.Grants, id)
	o.State = "deleting"
	st.Objects[id] = o
	st.Deletes[operation] = artifactDelete{Owner: o.Owner, Kind: "object", ObjectID: id, File: o.File, Revision: 1}
	return nil
}

// artifactFinishDelete consumes the EXACT committed intent and original
// owned file receipt after unlink+parent sync. Already absent may confirm only
// this recorded intent; absence never creates authority or stopped proof.
func artifactFinishDelete(st *artifactState, operation string, expected artifactDelete) error {
	if remoteDeleteDeferred(st, expected) {
		return ErrArtifactConflict
	}
	d, ok := st.Deletes[operation]
	if !ok || d != expected {
		return ErrArtifactConflict
	}
	if d.Kind == "object" {
		o := st.Objects[d.ObjectID]
		if o.State != "deleting" || o.Owner != d.Owner || o.File != d.File || st.pinned(d.ObjectID) || st.reading(d.ObjectID) {
			return ErrArtifactConflict
		}
		// Closed receipts are bounded metadata, not active readers. Retire
		// them with their object's verified GC, never a mere close timeout.
		retired := 0
		for id, r := range st.Reads {
			if r.Ref.ID != d.ObjectID {
				continue
			}
			if !r.Closed {
				return ErrArtifactConflict
			}
			if retired == 64 {
				continue
			}
			delete(st.Reads, id)
			retired++
		}
		// Keep the exact intent/object charge until the bounded receipt
		// tail is gone. The sole worker reselects this same intent.
		for _, r := range st.Reads {
			if r.Ref.ID == d.ObjectID {
				return nil
			}
		}
		delete(st.Objects, d.ObjectID)
		if witness, ok := st.Expired[o.Source]; ok && witness.Collected && witness.ObjectID == d.ObjectID {
			delete(st.Expired, o.Source)
		}
		if u, ok := st.Uploads[d.ObjectID]; ok {
			if u.State != "complete" {
				return ErrArtifactConflict
			}
			delete(st.Uploads, d.ObjectID)
		}
	} else if d.Kind == "upload" || d.Kind == "finalizing" {
		u := st.Uploads[d.ObjectID]
		if u.State != "deleting" || u.Owner != d.Owner || u.Part != d.File || u.Revision != d.Revision {
			return ErrArtifactConflict
		}
		if d.Kind == "finalizing" {
			// This intent covers BOTH the exact part and any exclusive
			// publication of this Ref. The trusted original owner must
			// settle finalize and confirm both unlink+sync operations.
			o, ok := st.Objects[d.ObjectID]
			if !ok || o.State != "publishing" || o.Owner != u.Owner || o.Ref != u.Ref {
				return ErrArtifactConflict
			}
			delete(st.Objects, d.ObjectID)
		}
		delete(st.Uploads, d.ObjectID)
	} else {
		return ErrArtifactConflict
	}
	delete(st.Deletes, operation)
	return nil
}

// RemoveArtifact revokes an unconsumed owned grant and marks its exact object
// for deletion. It cannot bypass a job/manual pin, workspace, actual reader
// or durable deletion-not-before floor; it has no immediate-delete exception.
func (s *Store) RemoveArtifact(actor auth.Actor, ref ArtifactRef) (operation string, err error) {
	if err = s.authorize(actor, auth.Mutate); err != nil {
		return
	}
	operation, err = newID()
	if err != nil {
		return
	}
	err = s.artifactUpdate(func(st *artifactState) error {
		g, ok := st.Grants[ref.ID]
		if !ok || g.Owner != actor.Principal() || g.Ref != ref {
			return ErrNotFound
		}
		if st.pinned(ref.ID) || st.reading(ref.ID) {
			return ErrArtifactConflict
		}
		delete(st.Grants, ref.ID)
		return artifactMarkObjectDelete(st, ref.ID, operation, s.now())
	})
	return
}
