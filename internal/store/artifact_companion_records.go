package store

import (
	"encoding/json"
	"errors"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

func companionEligible(tx *bolt.Tx) (*artifactState, error) {
	if err := validateSnapshot(tx); err != nil {
		return nil, err
	}
	st, err := artifactLoad(tx)
	if err != nil {
		return nil, err
	}
	_, u, owners, err := st.encode()
	if err != nil {
		return nil, err
	}
	if err := artifactAdmission(artifactUsage{}, u, false); err != nil {
		return nil, err
	}
	for _, u := range owners {
		if err := artifactAdmission(artifactUsage{}, u, true); err != nil {
			return nil, err
		}
	}
	for _, r := range st.Remote {
		if r.State != 4 {
			return nil, errors.New("remote execution retirement is pending")
		}
	}
	if len(st.Workspaces) != 0 || len(st.Deletes) != 0 {
		return nil, errors.New("artifact cleanup must finish before companion backup")
	}
	for _, c := range st.Completions {
		if !c.RootRemoved {
			return nil, errors.New("artifact root retirement is pending")
		}
	}
	for _, o := range st.Objects {
		if o.State != "complete" {
			return nil, errors.New("artifact object is incomplete")
		}
	}
	for _, r := range st.Reads {
		if !r.Closed {
			return nil, errors.New("artifact reader is not closed")
		}
	}
	for id, r := range st.Uploads {
		o, ok := st.Objects[id]
		if r.State != "complete" || !ok || r.Ref != o.Ref || r.Part != (ArtifactFile{}) && r.Part != o.File {
			return nil, errors.New("artifact upload has outstanding or mismatched ownership")
		}
	}
	ret := retentionRoot(tx)
	if ret != nil {
		for _, name := range [][]byte{retainedOutputsBucket, retainedClosuresBucket, cleanupOperationsBucket, cleanupOperationIndexBucket} {
			b := retentionTable(tx, name)
			if b == nil {
				return nil, errors.New("missing companion retention table")
			}
			k, _ := b.Cursor().First()
			if k != nil {
				return nil, errors.New("output or native cleanup must finish before companion backup")
			}
		}
		u, err := usage(tx)
		if err != nil {
			return nil, err
		}
		if u[OutputBytesQuota] != 0 || u[OutputFilesQuota] != 0 {
			return nil, errors.New("local output reservations remain")
		}
		if err := retentionTable(tx, cleanupHeadersBucket).ForEach(func(k, v []byte) error {
			var h cleanupHeader
			if err := json.Unmarshal(v, &h); err != nil {
				return err
			}
			p, ok := st.ManualInputs[string(k)]
			if !ok || h.ID != string(k) || h.JobID != p.JobID || h.Principal != p.Owner || h.TaskDigest != p.TaskDigest || !h.NeedsManual || h.Task != nil || h.Remaining != 0 || h.OutputRemaining != 0 || h.NativeRemaining != 0 {
				return errors.New("unqualified manual cleanup owner")
			}
			for _, id := range h.ExecutionIDs {
				c, ok := st.Completions[id]
				if !ok || c.AttemptID != id || c.JobID != h.JobID || c.Owner != p.Owner || c.TaskDigest != p.TaskDigest || !c.RootRemoved {
					return errors.New("manual history lacks original workspace retirement")
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	if err := tx.Bucket(jobsBucket).ForEach(func(_, v []byte) error {
		var j backend.Job
		if err := json.Unmarshal(v, &j); err != nil {
			return err
		}
		var m retainedJob
		hasMeta := ret != nil
		if hasMeta {
			if err := loadJSON(retentionTable(tx, retainedJobsBucket), []byte(j.ID), &m); err != nil {
				return err
			}
		}
		never := hasMeta && !m.Legacy && !m.Started && m.OutputStart == 0 && j.Status.Instance == "" && len(j.Attempts) == 0
		if hasMeta && m.Storage == backend.UnspecifiedOutput && !never {
			return errors.New("executed output placement is unspecified")
		}
		if j.Status.State == backend.Queued && never && j.NextAttemptAt == nil && !j.CancelRequested {
			return nil
		}
		if j.Status.State != backend.Succeeded && j.Status.State != backend.Failed && j.Status.State != backend.Canceled {
			return errors.New("job has active or uncertain execution")
		}
		if j.NextAttemptAt != nil {
			return errors.New("job has pending retry intent")
		}
		if never || artifactWholeReady(st, j) || safeStatuses(j) {
			return nil
		}
		return errors.New("job lacks original stopped or workspace retirement evidence")
	}); err != nil {
		return nil, err
	}
	return st, nil
}

// This offline transaction is confined to an unpublished restored copy. It is
// deliberately not the runtime changed-row mutation API.
func companionRebind(tx *bolt.Tx, before *artifactState, unit, volume uint64, files map[string]ArtifactFile) error {
	st, err := companionEligible(tx)
	if err != nil {
		return err
	}
	if st.Root != before.Root || len(st.Objects) != len(files) {
		return ErrArtifactConflict
	}
	for id, old := range before.Objects {
		o, ok := st.Objects[id]
		f, exists := files[id]
		if !ok || !exists || o != old || !artifactFileValid(f) || f.Device != volume {
			return ErrArtifactConflict
		}
		o.File = f
		st.Objects[id] = o
		if r, ok := st.Uploads[id]; ok && r.Part != (ArtifactFile{}) {
			r.Part = f
			st.Uploads[id] = r
		}
	}
	st.Root.Unit, st.Root.Volume = unit, volume
	rows, u, _, err := st.encode()
	if err != nil {
		return err
	}
	b := tx.Bucket(artifactsBucket)
	raw, err := json.Marshal(st.Root)
	if err != nil {
		return err
	}
	if err := b.Put([]byte("state"), raw); err != nil {
		return err
	}
	if err := b.Put([]byte("usage"), artifactUsageBytes(u)); err != nil {
		return err
	}
	for _, table := range artifactTables {
		t := b.Bucket([]byte(table.name))
		// Membership is unchanged, except canonical owner counters. The existing
		// encoder emits exactly the same owner keys for this eligible cohort.
		for k, v := range rows[table.name] {
			if err := t.Put([]byte(k), v); err != nil {
				return err
			}
		}
	}
	return validateSnapshot(tx)
}
