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
// inputs, manual inputs and results. It performs no filesystem I/O or grant check.
func (s *Store) ArtifactRequiredObjects() (out []ArtifactObjectIdentity, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		if err := validateSnapshot(tx); err != nil {
			return err
		}
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
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
		var size uint64
		for id := range ids {
			o, ok := st.Objects[id]
			if !ok || o.State != "complete" {
				return artifactCorrupt()
			}
			if err := artifactAdd(&size, o.Ref.Size); err != nil {
				return err
			}
			out = append(out, ArtifactObjectIdentity{o.Ref, o.File, o.Manifest})
		}
		if len(out) > 128 || size > 512<<20 {
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
