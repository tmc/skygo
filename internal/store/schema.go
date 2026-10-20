package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

const schemaVersion = 6

var schemaKey = []byte("schema_version")

// ErrUnsupportedSchema reports state whose schema is newer than this binary
// supports. Refusal does not migrate or write the database.
var ErrUnsupportedSchema = errors.New("unsupported database schema")

// inspect checks the refusal fence before inspecting any records. An empty
// database is initialized by Open; a partly missing layout is corruption.
func inspect(tx *bolt.Tx) error {
	// Fence before decoding ANY artifact or accepted job record. Schema5
	// never promised this root, even if the rows happen to look familiar.
	if schemaNumber(tx) <= 5 && tx.Bucket(artifactsBucket) != nil {
		return errors.New("artifact metadata requires schema 6")
	}
	if schemaNumber(tx) < 5 && retentionRoot(tx) != nil {
		return errors.New("retention metadata requires schema 5")
	}
	m := tx.Bucket(metadataBucket)
	if m != nil {
		if v := m.Get(schemaKey); v != nil {
			if len(v) != 8 {
				return errors.New("invalid database schema marker")
			}
			n := binary.BigEndian.Uint64(v)
			if n != 1 && n != 2 && n != 3 && n != 4 && n != 5 && n != schemaVersion {
				return fmt.Errorf("%w: version %d, supported %d", ErrUnsupportedSchema, n, schemaVersion)
			}
		}
	}
	count := 0
	for _, name := range [][]byte{jobsBucket, requestsBucket, sequenceBucket, metadataBucket} {
		if tx.Bucket(name) != nil {
			count++
		}
	}
	if count == 0 {
		c := tx.Cursor()
		if k, _ := c.First(); k != nil {
			return errors.New("invalid database bucket layout")
		}
		return nil
	}
	if count != 4 {
		return errors.New("invalid database bucket layout")
	}
	if data := m.Get(taskBytesKey); m.Bucket(taskBytesKey) != nil || data != nil && len(data) != 8 {
		return errors.New("invalid task byte counter")
	}
	if schemaNumber(tx) >= 3 && m.Get(taskBytesKey) == nil {
		return errors.New("missing task byte counter")
	}
	if len(m.Get([]byte("owner"))) == 0 && tx.Bucket(jobsBucket).Stats().KeyN != 0 {
		return errors.New("missing installation owner")
	}
	return nil
}

func validateExisting(tx *bolt.Tx) error {
	if err := inspect(tx); err != nil {
		return err
	}
	if tx.Bucket(jobsBucket) != nil {
		return validateSnapshot(tx)
	}
	return nil
}

func initialize(tx *bolt.Tx, owner string) error {
	if err := validateExisting(tx); err != nil {
		return err
	}
	for _, name := range [][]byte{jobsBucket, requestsBucket, sequenceBucket, metadataBucket} {
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return err
		}
	}
	m := tx.Bucket(metadataBucket)
	if data := m.Get(taskBytesKey); data == nil {
		var total uint64
		if err := tx.Bucket(jobsBucket).ForEach(func(_, data []byte) error {
			var j backend.Job
			if err := json.Unmarshal(data, &j); err != nil {
				return err
			}
			task, err := backend.TaskBytes(j.Task)
			if err != nil {
				return err
			}
			if uint64(len(task)) > ^uint64(0)-total {
				return errors.New("task byte counter overflow")
			}
			total += uint64(len(task))
			return nil
		}); err != nil {
			return err
		}
		if err := m.Put(taskBytesKey, sequenceKey(total)); err != nil {
			return err
		}
	}
	if len(m.Get([]byte("owner"))) == 0 {
		if err := m.Put([]byte("owner"), []byte(owner)); err != nil {
			return err
		}
	}
	if _, err := tx.CreateBucketIfNotExists(attemptJobsBucket); err != nil {
		return err
	}
	if m.Get(retryBytesKey) == nil {
		if err := m.Put(retryBytesKey, sequenceKey(0)); err != nil {
			return err
		}
	}
	if err := initializeRetention(tx); err != nil {
		return err
	}
	if schemaNumber(tx) == 5 {
		if err := migrateArtifactFence(tx); err != nil {
			return err
		}
	} else if !bytes.Equal(m.Get(schemaKey), sequenceKey(schemaVersion)) {
		if err := m.Put(schemaKey, sequenceKey(schemaVersion)); err != nil {
			return err
		}
	}
	return validateSnapshot(tx)
}

// migrateArtifactFence preserves the complete schema5 logical installation.
// Artifact storage stays dormant until the concrete bound companion root has
// been admitted; migration must not fabricate device/unit/inventory facts.
func migrateArtifactFence(tx *bolt.Tx) error {
	if schemaNumber(tx) != 5 || tx.Bucket(artifactsBucket) != nil {
		return ErrArtifactConflict
	}
	if err := validateRetention(tx); err != nil {
		return err
	}
	return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(schemaVersion))
}

// validateSnapshot checks references before publishing a restore. It does not
// normalize or rewrite accepted JSON, request bindings, or sequence numbers.
func validateSnapshot(tx *bolt.Tx) error {
	if err := inspect(tx); err != nil {
		return err
	}
	jobs, requests, sequence := tx.Bucket(jobsBucket), tx.Bucket(requestsBucket), tx.Bucket(sequenceBucket)
	if jobs == nil {
		return errors.New("snapshot contains no installation")
	}
	if len(tx.Bucket(metadataBucket).Get([]byte("owner"))) == 0 {
		return errors.New("snapshot contains no installation owner")
	}
	if err := jobs.ForEach(func(id, data []byte) error {
		var j backend.Job
		if err := json.Unmarshal(data, &j); err != nil {
			return fmt.Errorf("decode snapshot job: %w", err)
		}
		if string(id) != j.ID || j.Sequence == 0 || j.Sequence > sequence.Sequence() || string(sequence.Get(sequenceKey(j.Sequence))) != j.ID {
			return errors.New("invalid snapshot job sequence")
		}
		if err := validateRouting(tx, j); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if err := sequence.ForEach(func(key, id []byte) error {
		if len(key) != 8 {
			return errors.New("invalid snapshot sequence key")
		}
		j, err := get(tx, string(id))
		if err != nil {
			return err
		}
		if j.Sequence != binary.BigEndian.Uint64(key) {
			return errors.New("invalid snapshot sequence binding")
		}
		return nil
	}); err != nil {
		return err
	}
	if err := requests.ForEach(func(key, id []byte) error {
		if len(key) == 0 || jobs.Get(id) == nil {
			return errors.New("invalid snapshot request binding")
		}
		return nil
	}); err != nil {
		return err
	}
	if err := validateScoped(tx); err != nil {
		return err
	}
	if err := validateAttempts(tx); err != nil {
		return err
	}
	if schemaNumber(tx) >= 5 {
		if err := validateRetention(tx); err != nil {
			return err
		}
	}
	if tx.Bucket(artifactsBucket) != nil {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		return artifactValidateTaskRows(tx, st)
	}
	return artifactValidateTaskRows(tx, nil)
}
