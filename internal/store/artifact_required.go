package store

import (
	"errors"
	"sort"

	bolt "go.etcd.io/bbolt"
)

// ArtifactObjectIdentity is a trusted metadata handoff, not read permission or
// proof that the named physical bytes exist.
type ArtifactObjectIdentity struct {
	Reference ArtifactRef
	File      ArtifactFile
	Manifest  ArtifactManifest
}

// ArtifactRequiredObjects returns the complete objects pinned by current job
// inputs, manual inputs, results, workflow inputs and environments. It
// performs no filesystem I/O or grant check.
func (s *Store) ArtifactRequiredObjects() (out []ArtifactObjectIdentity, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		if err := validateSnapshot(tx); err != nil {
			return err
		}
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		var size uint64
		for id := range st.required() {
			o, ok := st.Objects[id]
			if !ok || o.State != "complete" {
				return artifactCorrupt()
			}
			if err := artifactAdd(&size, o.Ref.Size); err != nil {
				return err
			}
			out = append(out, ArtifactObjectIdentity{o.Ref, o.File, o.Manifest})
		}
		if len(out) > maxRequiredObjects || size > maxRequiredBytes {
			return errors.New("required artifact inventory exceeds limit")
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Reference.ID < out[j].Reference.ID })
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// The required inventory is verified in full before a server starts.
const (
	maxRequiredObjects = 128
	maxRequiredBytes   = 512 << 20
)

// required returns the IDs of the objects that ArtifactRequiredObjects
// reports.
func (st *artifactState) required() map[string]bool {
	ids := make(map[string]bool)
	for _, p := range st.JobInputs {
		ids[p.Ref.ID] = true
	}
	for _, p := range st.ManualInputs {
		ids[p.Ref.ID] = true
	}
	for _, r := range st.Results {
		ids[r.Ref.ID] = true
	}
	for _, in := range st.WorkflowInputs {
		if in.Ref != nil {
			ids[in.Ref.ID] = true
		}
	}
	for _, p := range st.EnvironmentPins {
		ids[p.Ref.ID] = true
	}
	return ids
}

// artifactRequiredSize counts the required inventory's objects and bytes.
func artifactRequiredSize(st *artifactState) (objects, size uint64, err error) {
	for id := range st.required() {
		o, ok := st.Objects[id]
		if !ok {
			return 0, 0, artifactCorrupt()
		}
		objects++
		if err = artifactAdd(&size, o.Ref.Size); err != nil {
			return
		}
	}
	return
}
