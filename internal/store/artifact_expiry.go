package store

import (
	"sort"

	bolt "go.etcd.io/bbolt"
)

// ArtifactUploadExpiry is a detached expired upload observation, not authority
// to touch a file. A creating upload has a zero File and must remain held until
// its original create owner supplies an actual qualified handoff.
type ArtifactUploadExpiry struct {
	Owner  string
	Upload ArtifactUpload
	File   ArtifactFile
	RootID string
}

// NextArtifactUploadExpiry selects one expired creating, writable or finalizing
// upload from the bounded validated installation, wrapping once after a lower-
// hex upload ID. Complete and deleting uploads are never selected. Selection
// changes no metadata or reservations and also works with retention disabled.
func (s *Store) NextArtifactUploadExpiry(after string) (out ArtifactUploadExpiry, found bool, err error) {
	if after != "" && !artifactHex(after, 32) {
		return out, false, ErrArtifactConflict
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		now := s.now().UTC()
		ids := make([]string, 0, len(st.Uploads))
		for id := range st.Uploads {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		start := sort.Search(len(ids), func(i int) bool { return ids[i] > after })
		for n := 0; n < len(ids); n++ {
			u := st.Uploads[ids[(start+n)%len(ids)]]
			if u.State != "creating" && u.State != "writable" && u.State != "finalizing" || now.Before(u.Expires) {
				continue
			}
			out = ArtifactUploadExpiry{Owner: u.Owner, Upload: artifactUploadDTO(u), File: u.Part, RootID: st.Root.ID}
			found = true
			return nil
		}
		return nil
	})
	return
}

// ExpireArtifactUpload marks an exactly matching known writable or finalizing
// upload after its fixed deadline. The trusted coordinator must separately
// fence original I/O and HTTP owners before the existing deletion worker acts.
// Creating uploads and stale observations refuse without mutation. Success
// returns committed deletion work and gives no physical receipt or quota credit.
func (s *Store) ExpireArtifactUpload(expected ArtifactUploadExpiry) (out ArtifactDeleteWork, err error) {
	id, err := newID()
	if err != nil {
		return out, err
	}
	err = s.artifactUpdate(func(st *artifactState) error {
		u, ok := st.Uploads[expected.Upload.Reference.ID]
		if !ok || st.Root.ID != expected.RootID || u.Owner != expected.Owner || artifactUploadDTO(u) != expected.Upload || u.Part != expected.File || !artifactFileValid(u.Part) || u.Part.Device != st.Root.Volume || u.State != "writable" && u.State != "finalizing" || s.now().Before(u.Expires) {
			return ErrArtifactConflict
		}
		if e := artifactMarkUploadDelete(st, id, u); e != nil {
			return e
		}
		var e error
		out, e = artifactDeleteWork(st, id, st.Deletes[id])
		return e
	})
	if err != nil {
		// A failed or uncertain transaction never exports provisional work.
		out = ArtifactDeleteWork{}
	}
	return
}
