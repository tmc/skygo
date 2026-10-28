package store

import (
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
	"time"
)

// GetArtifactUploadFile returns the exact durable upload and part identity in
// one owner-authorized snapshot. A creating upload's zero File is UNKNOWN;
// callers must not repair that interval by adopting an existing path/inode.
func (s *Store) GetArtifactUploadFile(actor auth.Actor, c ArtifactCursor) (upload ArtifactUpload, file ArtifactFile, err error) {
	if err = s.authorize(actor, auth.Read); err != nil {
		return
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		u, e := artifactOwnedUpload(st, actor.Principal(), c)
		if e != nil {
			return e
		}
		upload = artifactUploadDTO(u)
		file = u.Part
		return nil
	})
	return
}

// ArtifactInstallation returns actual persisted artifact configuration. It
// does not certify a constructor's live root marker/device/ownership facts.
func (s *Store) ArtifactInstallation() (out ArtifactInstallation, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		var e error
		out, e = artifactInstallation(tx)
		return e
	})
	return
}

func artifactInstallation(tx *bolt.Tx) (ArtifactInstallation, error) {
	if tx.Bucket(artifactsBucket) == nil {
		return ArtifactInstallation{}, nil
	}
	st, err := artifactLoad(tx)
	if err != nil {
		return ArtifactInstallation{}, err
	}
	return ArtifactInstallation{Enabled: true, ID: st.Root.ID, Unit: st.Root.Unit, Volume: st.Root.Volume}, nil
}

func artifactMatchDownload(st *artifactState, c ArtifactDownloadClosure) (artifactRead, error) {
	r, ok := st.Reads[c.LeaseID]
	o, object := st.Objects[c.Ref.ID]
	if !ok || !object || r.Owner != c.Owner || r.Ref != c.Ref || o.Owner != c.Owner || o.Ref != c.Ref || o.File != c.File || c.RootID != st.Root.ID || artifactProducerValue(r.Producer) != c.Producer {
		return r, ErrArtifactConflict
	}
	return r, nil
}

// MarkArtifactDownloadClosing is cleanup-only for the original admitted
// Control+Files owner. It may stop admission before existing owners have joined;
// it never frees a reader or requires fabricated/reused actor authority.
func (s *Store) MarkArtifactDownloadClosing(c ArtifactDownloadClosure) (out ArtifactDownloadClosure, err error) {
	err = s.artifactUpdate(func(st *artifactState) error {
		r, e := artifactMatchDownload(st, c)
		if e != nil {
			return e
		}
		if r.Closing {
			if c.Revision != r.Revision && (r.Revision == 0 || c.Revision != r.Revision-1) {
				return ErrArtifactConflict
			}
		} else {
			if c.Revision != r.Revision {
				return ErrArtifactConflict
			}
			r.Revision, e = artifactNext(r.Revision)
			if e != nil {
				return e
			}
			r.Closing = true
			st.Reads[c.LeaseID] = r
		}
		out = c
		out.Revision = r.Revision
		return nil
	})
	return
}

// FinishArtifactDownload consumes the closing identity ONLY after the original
// Control+Files owner has joined all actual I/O and HTTP use and observed FD
// Close success. Fields/digests are identity fences, never proof of those facts.
// Exact duplicates do not credit twice or extend the deletion floor again.
func (s *Store) FinishArtifactDownload(c ArtifactDownloadClosure, now time.Time) error {
	return s.artifactUpdate(func(st *artifactState) error {
		if _, err := artifactMatchDownload(st, c); err != nil {
			return err
		}
		return artifactReaderClosed(st, c.Owner, ArtifactCursor{c.LeaseID, c.Revision}, now)
	})
}

func artifactAbandonMatch(st *artifactState, c ArtifactCreateAbandonment) (artifactUploadRecord, string, error) {
	u, ok := st.Uploads[c.Cursor.ID]
	if !ok {
		return u, "", ErrNotFound
	}
	if u.Owner != c.Owner || u.Ref != c.Ref || c.Ref.ID != c.Cursor.ID || c.RootID != st.Root.ID || !artifactFileValid(c.File) || c.File.Device != st.Root.Volume {
		return u, "", ErrArtifactConflict
	}
	if u.State == "deleting" {
		if u.Part != c.File || u.Revision == 0 || c.Cursor.Revision != u.Revision-1 {
			return u, "", ErrArtifactConflict
		}
		for id, d := range st.Deletes {
			if d.Kind == "upload" && d.Owner == c.Owner && d.ObjectID == c.Cursor.ID && d.File == c.File && d.Revision == u.Revision {
				return u, id, nil
			}
		}
		return u, "", ErrArtifactConflict
	}
	if u.State != "creating" || u.Part != (ArtifactFile{}) || u.Revision != c.Cursor.Revision {
		return u, "", ErrArtifactConflict
	}
	return u, "", nil
}

// AbandonArtifactCreate preserves a revoked/failed create's actually observed
// FileID for cleanup, without accepting or publishing it. Unknown FD closure
// or a crash without the original receipt remains charged Held/manual; EEXIST
// and copied fields alone never establish the required original ownership.
func (s *Store) AbandonArtifactCreate(c ArtifactCreateAbandonment) (operation string, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		_, operation, e = artifactAbandonMatch(st, c)
		return e
	})
	if err != nil || operation != "" {
		return
	}
	candidate, e := newID()
	if e != nil {
		return "", e
	}
	err = s.artifactUpdate(func(st *artifactState) error {
		u, id, e := artifactAbandonMatch(st, c)
		if e != nil {
			return e
		}
		if id != "" {
			operation = id
			return nil
		}
		if _, ok := st.Deletes[candidate]; ok {
			return ErrArtifactConflict
		}
		u.Revision, e = artifactNext(u.Revision)
		if e != nil {
			return e
		}
		if u.RemoteAttemptID != "" {
			r, ok := st.Remote[u.RemoteAttemptID]
			o, exists := st.Objects[c.Cursor.ID]
			owner, e := remoteOwner(st, r)
			if !ok || e != nil || r.State != 2 || remoteOutputID(r) != c.Cursor.ID || owner != u.Owner || !exists || o.State != "reserved" || o.Origin != "remote_result" || o.Source != u.RemoteAttemptID || o.JobID != r.Job || o.Owner != u.Owner || o.Ref != u.Ref || o.File != (ArtifactFile{}) || o.Manifest != (ArtifactManifest{}) {
				return ErrArtifactConflict
			}
			delete(st.Objects, c.Cursor.ID) // exact unmaterialized reservation; actual part remains charged below.
		}
		u.State = "deleting"
		u.Part = c.File
		st.Uploads[c.Cursor.ID] = u
		st.Deletes[candidate] = artifactDelete{Owner: c.Owner, Kind: "upload", ObjectID: c.Cursor.ID, File: c.File, Revision: u.Revision}
		operation = candidate
		return nil
	})
	return
}

// ArtifactWorkspaceReservation is the exact durable starting workspace intent.
// It conveys identity and charged ownership, never physical writer/stop proof.
// RootID identifies the artifact installation; the concrete local owner retains
// the distinct physical per-attempt workspace root identity separately.
type ArtifactWorkspaceReservation struct {
	Owner     string
	JobID     string
	AttemptID string
	Binding   string
	Instance  string
	RootID    string
	InputRef  ArtifactRef
	InputFile ArtifactFile
	Manifest  ArtifactManifest
	ResultID  string
}

// GetWorkspaceReservation reads one exact current Starting/Running intent for
// the trusted concrete coordinator. It is not an RPC or artifact-read grant.
// A stale attempt refuses rather than combining a current job with old files.
// Instance is empty before the actual ownership Observe, then durably pinned.
func (s *Store) GetWorkspaceReservation(jobID, expectedAttemptID string) (out ArtifactWorkspaceReservation, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		j, e := get(tx, jobID)
		if e != nil {
			return e
		}
		if j.CurrentAttemptID() != expectedAttemptID || j.Task.Workdir == nil || j.Status.State != backend.Starting && j.Status.State != backend.Running {
			return ErrArtifactConflict
		}
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		w, ok := st.Workspaces[expectedAttemptID]
		if !ok || w.JobID != jobID || w.AttemptID != expectedAttemptID || w.Binding != j.TargetBinding || w.Instance != j.Status.Instance {
			return ErrArtifactConflict
		}
		ref, e := artifactTaskRef(j.Task)
		if e != nil || ref != w.Input {
			return ErrArtifactConflict
		}
		o, ok := st.Objects[ref.ID]
		if !ok || o.State != "complete" || o.Ref != ref || o.Owner != w.Owner || !artifactFileValid(o.File) || !artifactManifestValid(o.Manifest, ref) {
			return ErrArtifactConflict
		}
		out = ArtifactWorkspaceReservation{Owner: w.Owner, JobID: jobID, AttemptID: expectedAttemptID, Binding: w.Binding, Instance: w.Instance, RootID: st.Root.ID, InputRef: ref, InputFile: o.File, Manifest: o.Manifest, ResultID: w.ResultID}
		return nil
	})
	return
}
