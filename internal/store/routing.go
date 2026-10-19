package store

import (
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

func validateRouting(tx *bolt.Tx, job backend.Job) error {
	version := uint64(0)
	if raw := tx.Bucket(metadataBucket).Get(schemaKey); len(raw) == 8 {
		version = binary.BigEndian.Uint64(raw)
	}
	if version < schemaVersion {
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
		if err := json.Unmarshal(data, &job); err != nil {
			return errors.New("invalid legacy job")
		}
		if job.Target != "local" || job.TargetBinding != "" {
			continue
		}
		job.TargetBinding = binding
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
