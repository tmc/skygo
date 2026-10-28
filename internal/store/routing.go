package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

func validBinding(binding string) bool {
	return len(binding) > 0 && len(binding) <= 256 && utf8.ValidString(binding) && !strings.ContainsRune(binding, 0)
}

// validateRouting checks the routing fields of a decoded stored job. The
// caller has already refused, from its raw bytes, a task that requests
// workers; see decodeStoredJob.
func validateRouting(tx *bolt.Tx, job backend.Job) error {
	version := uint64(0)
	if raw := tx.Bucket(metadataBucket).Get(schemaKey); len(raw) == 8 {
		version = binary.BigEndian.Uint64(raw)
	}
	if version < 3 {
		t := job.Task
		if job.TargetBinding != "" || t.CPULimitMillis != 0 || t.MemoryLimitBytes != 0 || len(t.TargetLabels) != 0 || len(t.NodeSelector) != 0 {
			return errors.New("routing fields require schema 3")
		}
	} else if job.TargetBinding != "" && !validBinding(job.TargetBinding) {
		return errors.New("invalid stored target binding")
	}
	return nil
}

// BindLegacyLocal binds unbound historical local jobs to this installation.
// Only trusted server startup may call it, after constructing its own built-in
// local backend, with the same backend.LocalBinding used for that target.
// It performs no execution, remote route recovery or PID adoption.
func (s *Store) BindLegacyLocal(binding string) error {
	want, err := backend.LocalBinding(s.owner)
	if err != nil || binding != want {
		return errors.New("invalid built-in local binding")
	}
	return s.db.Update(func(tx *bolt.Tx) error { return bindLegacyLocal(tx, binding) })
}

func bindLegacyLocal(tx *bolt.Tx, binding string) error {
	if err := validateSnapshot(tx); err != nil {
		return err
	}
	jobs := tx.Bucket(jobsBucket)
	var ids []string
	if err := jobs.ForEach(func(id, _ []byte) error {
		ids = append(ids, string(id))
		return nil
	}); err != nil {
		return err
	}
	for _, id := range ids {
		data := jobs.Get([]byte(id))
		var job backend.Job
		if err := decodeStoredJob(data, &job); err != nil {
			return errors.New("invalid legacy job")
		}
		if job.Target != "local" || job.TargetBinding != "" {
			continue
		}
		job.TargetBinding = binding
		if retentionRoot(tx) != nil {
			if err := putLegacyLocalBinding(tx, job, binding); err != nil {
				return err
			}
			continue
		}
		data, err := json.Marshal(job)
		if err != nil {
			return err
		}
		if err := jobs.Put([]byte(id), data); err != nil {
			return err
		}
	}
	return nil
}

// putLegacyLocalBinding is the finite one-time legacy identity migration. It
// charges exact growth even for an already over-quota installation; no other
// mutation receives this allowance and a pinned binding cannot enter it again.
func putLegacyLocalBinding(tx *bolt.Tx, job backend.Job, binding string) error {
	before, e := get(tx, job.ID)
	if e != nil {
		return e
	}
	want, e := backend.LocalBinding(string(tx.Bucket(metadataBucket).Get([]byte("owner"))))
	if e != nil || want != binding || before.Target != "local" || before.TargetBinding != "" || job.TargetBinding != binding {
		return errors.New("invalid legacy local binding migration")
	}
	expected := before
	expected.TargetBinding = binding
	wantData, e := json.Marshal(expected)
	if e != nil {
		return e
	}
	gotData, e := json.Marshal(job)
	if e != nil {
		return e
	}
	if !bytes.Equal(wantData, gotData) {
		return errors.New("legacy binding changed accepted payload")
	}
	var m retainedJob
	if e = loadJSON(retentionTable(tx, retainedJobsBucket), []byte(job.ID), &m); e != nil {
		return e
	}
	if !m.Legacy {
		return errors.New("local binding migration requires legacy work")
	}
	n, e := jobOverhead(job)
	if e != nil {
		return e
	}
	old := m.Overhead
	oldRecord := retentionTable(tx, retainedJobsBucket).Get([]byte(job.ID))
	m.Overhead = n
	data, e := json.Marshal(m)
	if e != nil {
		return e
	}
	u, e := usage(tx)
	if e != nil {
		return e
	}
	for _, v := range []uint64{old, recordCharge(retainedJobsBucket, []byte(job.ID), oldRecord)} {
		if u[MetadataBytesQuota] < v {
			return errors.New("legacy binding metadata underflow")
		}
		u[MetadataBytesQuota] -= v
	}
	for _, v := range []uint64{n, recordCharge(retainedJobsBucket, []byte(job.ID), data)} {
		u[MetadataBytesQuota], e = addChecked(u[MetadataBytesQuota], v)
		if e != nil {
			return e
		}
	}
	if e = retentionTable(tx, retainedJobsBucket).Put([]byte(job.ID), data); e != nil {
		return e
	}
	if e = putJSON(retentionRoot(tx), retentionUsageKey, u); e != nil {
		return e
	}
	return putJSON(tx.Bucket(jobsBucket), []byte(job.ID), job)
}
