package store

import (
	"bytes"
	"errors"
	"fmt"
	"maps"

	bolt "go.etcd.io/bbolt"
)

func (s *artifactState) clone() *artifactState {
	out := &artifactState{
		Root: s.Root, Uploads: maps.Clone(s.Uploads), Objects: maps.Clone(s.Objects),
		Grants: maps.Clone(s.Grants), Reads: maps.Clone(s.Reads), JobInputs: maps.Clone(s.JobInputs),
		ManualInputs: maps.Clone(s.ManualInputs), Results: maps.Clone(s.Results), Expired: maps.Clone(s.Expired),
		Workspaces: maps.Clone(s.Workspaces), Completions: maps.Clone(s.Completions), Deletes: maps.Clone(s.Deletes),
	}
	for id, r := range out.Reads {
		r.Producer = artifactCloneProducer(r.Producer)
		out.Reads[id] = r
	}
	for id, d := range out.Deletes {
		if d.Workspace != nil {
			payload := *d.Workspace
			payload.Producer = artifactCloneProducer(payload.Producer)
			d.Workspace = &payload
			out.Deletes[id] = d
		}
	}
	return out
}

var errArtifactMutationBudget = errors.New("artifact mutation budget exceeded")

type artifactWrite struct {
	table, key string
	value      []byte
}
type artifactPrepared struct {
	writes     []artifactWrite
	usage      []byte
	changed    uint64
	newRecords int
}

// prepareArtifactChange is a bounded read-only forecast. The optional concrete
// S03 plan supplies projected job/header rows; dry-run never writes temporary
// rows to make validation pass. Both admission and GC use this same arithmetic.
func prepareArtifactChange(tx *bolt.Tx, before, after *artifactState, core *retentionMutation, budget uint64) (out artifactPrepared, err error) {
	if before.Root != after.Root {
		return out, ErrArtifactConflict
	}
	if err := artifactValidateCorePlanned(tx, after, core); err != nil {
		return out, err
	}
	oldRows, oldUsage, oldOwners, err := before.encode()
	if err != nil {
		return out, err
	}
	rows, usage, owners, err := after.encode()
	if err != nil {
		return out, err
	}
	if err := artifactAdmission(oldUsage, usage, false); err != nil {
		return out, err
	}
	for owner, u := range owners {
		if err := artifactAdmission(oldOwners[owner], u, true); err != nil {
			return out, err
		}
	}
	for _, table := range artifactTables {
		keys := make(map[string]bool)
		for key := range oldRows[table.name] {
			keys[key] = true
		}
		for key := range rows[table.name] {
			keys[key] = true
		}
		for key := range keys {
			a, b := oldRows[table.name][key], rows[table.name][key]
			if bytes.Equal(a, b) {
				continue
			}
			if a == nil && b != nil {
				out.newRecords++
			}
			if err := artifactAdd(&out.changed, uint64(len(key)+len(a)+len(b))); err != nil {
				return out, err
			}
			out.writes = append(out.writes, artifactWrite{table.name, key, b})
		}
	}
	if oldUsage != usage {
		out.usage = artifactUsageBytes(usage)
		if err := artifactAdd(&out.changed, uint64(len("usage")+2*len(out.usage))); err != nil {
			return out, err
		}
	}
	if budget > maxChangeBytes || out.changed > budget || out.newRecords > 100 || len(out.writes) > 100 {
		return out, errArtifactMutationBudget
	}
	return out, nil
}

func (p artifactPrepared) apply(tx *bolt.Tx) error {
	b := tx.Bucket(artifactsBucket)
	for _, w := range p.writes {
		table := b.Bucket([]byte(w.table))
		var err error
		if w.value == nil {
			err = table.Delete([]byte(w.key))
		} else {
			err = table.Put([]byte(w.key), w.value)
		}
		if err != nil {
			return fmt.Errorf("write artifact %s: %w", w.table, err)
		}
	}
	if p.usage != nil {
		return b.Put([]byte("usage"), p.usage)
	}
	return nil
}
