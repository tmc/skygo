package store

import (
	"crypto/sha256"
	"encoding/json"
	"sort"

	bolt "go.etcd.io/bbolt"
)

// ArtifactReservations is a copied logical reservation cohort, not observed
// filesystem usage or permission to open/clean files. AllocatedBytes and Files
// are independently recomputed allowances, including fixed installation entries;
// do not add Subjects as another charge. When Installation.Enabled is false,
// these allowances are unavailable, not measured zero physical usage.
type ArtifactReservations struct {
	Installation   ArtifactInstallation
	AllocatedBytes uint64
	Files          uint64
	Subjects       []ArtifactPhysicalSubject
	// Digest is purpose-separated SHA256 of this canonical sorted snapshot
	// with Digest zero. Callers use this field, not a reconstructed Store hash.
	// It excludes Ack/revision/grant/lease noise. It is NOT Control's effective
	// Local+Files-root fence, a complete census or physical/cleanup proof.
	Digest [32]byte
}

// ArtifactPhysicalSubject is durable identity context for a bounded census.
// Kind is upload, object or workspace. File comes ONLY from upload.Part or
// object.File: zero means UNKNOWN for those kinds, never no physical allocation.
// Workspace File is inapplicable. Unmarked workspace Parent/Root/RootState are
// unknown zeros, not NeverCreated. Only a validated committed workspace GC row
// supplies Parent/Root/RootState/Candidate identity. Input File is NEVER Root.
type ArtifactPhysicalSubject struct {
	Kind          string
	ID            string
	File          ArtifactFile
	Parent        ArtifactFile
	Root          ArtifactFile
	RootState     WorkspaceRootState
	CandidateID   string
	CandidateFile ArtifactFile
}

// ArtifactReservations reads one bounded, validated metadata View. It returns
// no Task, grant, live transaction, path or filesystem authority. Ack/revision
// changes within the same prepaid identity/allowance keep Digest unchanged;
// structural subjects, identities, allowances and marked roots invalidate it.
// A sampler must separately join original Local states and verified Files root
// facts and compare BEFORE/AFTER cohorts. This method performs no filesystem,
// authentication, audit, epoch, ownership-adoption or counter mutation.
func (s *Store) ArtifactReservations() (out ArtifactReservations, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(artifactsBucket) == nil {
			return nil
		}
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		_, usage, _, e := st.encode()
		if e != nil {
			return e
		}
		out.Installation = ArtifactInstallation{Enabled: true, ID: st.Root.ID, Unit: st.Root.Unit, Volume: st.Root.Volume}
		out.AllocatedBytes = usage[aAllocatedBytes]
		out.Files = usage[aFiles]
		out.Subjects = make([]ArtifactPhysicalSubject, 0, len(st.Uploads)+len(st.Objects)+len(st.Workspaces))
		for id, u := range st.Uploads {
			out.Subjects = append(out.Subjects, ArtifactPhysicalSubject{Kind: "upload", ID: id, File: u.Part})
		}
		for id, o := range st.Objects {
			out.Subjects = append(out.Subjects, ArtifactPhysicalSubject{Kind: "object", ID: id, File: o.File})
		}
		// artifactLoad has already checked duplicate/orphan/kind relations.
		// Reuse the concrete validator/work projection for marked root facts;
		// no scan, input file or missing Local map can fill an unmarked Root.
		marked := make(map[string]ArtifactPhysicalSubject)
		for operation, d := range st.Deletes {
			if d.Kind != "workspace" {
				continue
			}
			work, e := artifactWorkspaceWork(st, operation, d)
			if e != nil {
				return e
			}
			o := work.Origin
			marked[d.ObjectID] = ArtifactPhysicalSubject{Kind: "workspace", ID: d.ObjectID, Parent: o.Parent, Root: o.Root, RootState: o.RootState, CandidateID: o.CandidateID, CandidateFile: o.CandidateFile}
		}
		for id := range st.Workspaces {
			subject, ok := marked[id]
			if !ok {
				subject = ArtifactPhysicalSubject{Kind: "workspace", ID: id}
			}
			out.Subjects = append(out.Subjects, subject)
		}
		// The existing actual row/byte cap applies before these copies. This
		// explicit count also bounds the returned cohort, not just map storage.
		if len(out.Subjects) > 4096 {
			return artifactCorrupt()
		}
		sort.Slice(out.Subjects, func(i, j int) bool {
			a, b := out.Subjects[i], out.Subjects[j]
			if a.Kind != b.Kind {
				return a.Kind < b.Kind
			}
			return a.ID < b.ID
		})
		raw, e := json.Marshal(out)
		if e != nil {
			return e
		}
		out.Digest = sha256.Sum256(append([]byte("skygo-artifact-reservations-v1\x00"), raw...))
		return nil
	})
	if err != nil {
		return ArtifactReservations{}, err
	}
	return
}
