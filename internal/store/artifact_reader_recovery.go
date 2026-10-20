package store

import (
	"sort"

	bolt "go.etcd.io/bbolt"
)

// NextArtifactReaderRecovery selects one unclosed reader in a validated,
// read-only snapshot, wrapping after a lower-hex lease cursor. Unknown producers
// are returned too, so a fair caller can advance without granting them authority.
// The returned identity is not evidence of producer cessation or descriptor close.
func (s *Store) NextArtifactReaderRecovery(after string) (out ArtifactDownloadClosure, found bool, err error) {
	if after != "" && !artifactHex(after, 32) {
		return out, false, ErrArtifactConflict
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		ids := make([]string, 0, len(st.Reads))
		for id, r := range st.Reads {
			if !r.Closed {
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		if len(ids) == 0 {
			return nil
		}
		i := sort.Search(len(ids), func(i int) bool { return ids[i] > after })
		if i == len(ids) {
			i = 0
		}
		id := ids[i]
		r := st.Reads[id]
		out = ArtifactDownloadClosure{Owner: r.Owner, LeaseID: id, Revision: r.Revision, Ref: r.Ref, File: st.Objects[r.Ref.ID].File, RootID: st.Root.ID, Producer: artifactProducerValue(r.Producer)}
		found = true
		return nil
	})
	return
}

// SettleStoppedArtifactDownload releases one exact old reader's protection.
// The trusted concrete coordinator must first qualify actual cessation of this
// producer, successful closure of every original observation descriptor, no
// cached owner, and the deployment's no-surviving-descriptor-or-use premise.
// Identity fields alone do not establish those facts. Unknown producers refuse.
// This is explicit stopped settlement, not a receipt for the old Reader.Close.
// Compatible closed replays neither credit twice nor extend deletion grace.
func (s *Store) SettleStoppedArtifactDownload(expected ArtifactDownloadClosure) error {
	return s.artifactUpdate(func(st *artifactState) error {
		p := expected.Producer
		if !p.valid() || p.NamespaceDevice == 0 || p.StartTicks == 0 || expected.Revision == 0 {
			return ErrArtifactConflict
		}
		r, err := artifactMatchDownload(st, expected)
		if err != nil {
			return err
		}
		if r.Closed {
			// The DTO does not identify which API caused an earlier Closing
			// increment. Accept only revision states compatible with at most
			// that one increment, without inferring its cause.
			if expected.Revision != r.Revision && expected.Revision != r.Revision-1 {
				return ErrArtifactConflict
			}
			return nil
		}
		if expected.Revision != r.Revision {
			return ErrArtifactConflict
		}
		now := s.now().UTC()
		if now.IsZero() {
			return ErrArtifactConflict
		}
		if !r.Closing {
			r.Revision, err = artifactNext(r.Revision)
			if err != nil {
				return err
			}
			r.Closing = true
		}
		r.Closed = true
		r.ClosedAt = now
		st.Reads[expected.LeaseID] = r
		st.releaseGrace(r.Ref.ID, now)
		return nil
	})
}
