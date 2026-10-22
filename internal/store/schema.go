package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

const schemaVersion = 8

var schemaKey = []byte("schema_version")

// ErrUnsupportedSchema reports state whose schema is newer than this binary
// supports. Refusal does not migrate or write the database.
var ErrUnsupportedSchema = errors.New("unsupported database schema")

// inspect checks the refusal fence before inspecting any records. An empty
// database is initialized by Open; a partly missing layout is corruption.
func inspect(tx *bolt.Tx) error {
	m := tx.Bucket(metadataBucket)
	if m != nil {
		if v := m.Get(schemaKey); v != nil {
			if len(v) != 8 {
				return errors.New("invalid database schema marker")
			}
			n := binary.BigEndian.Uint64(v)
			if n != 1 && n != 2 && n != 3 && n != 4 && n != 5 && n != 6 && n != 7 && n != schemaVersion {
				return fmt.Errorf("%w: version %d, supported %d", ErrUnsupportedSchema, n, schemaVersion)
			}
		}
	}
	// Fence before decoding ANY artifact or accepted job record. Schema5
	// never promised this root, even if the rows happen to look familiar.
	if schemaNumber(tx) <= 5 && tx.Bucket(artifactsBucket) != nil {
		return errors.New("artifact metadata requires schema 6")
	}
	if schemaNumber(tx) < 5 && retentionRoot(tx) != nil {
		return errors.New("retention metadata requires schema 5")
	}
	if schemaNumber(tx) < 8 && tx.Bucket(workflowsBucket) != nil {
		return errors.New("workflow metadata requires schema 8")
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
	if schemaNumber(tx) == 6 {
		if err := migrateRemoteFence(tx); err != nil {
			return err
		}
	} else if schemaNumber(tx) == 5 {
		if err := migrateArtifactFence(tx); err != nil {
			return err
		}
	} else if n := schemaNumber(tx); n != 7 && n != schemaVersion {
		if err := m.Put(schemaKey, sequenceKey(7)); err != nil {
			return err
		}
	}
	if schemaNumber(tx) == 7 {
		if err := migrateWorkflowFence(tx); err != nil {
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
	return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(7))
}

// migrateRemoteFence validates the complete old installation before changing
// either marker or adding the remote table. The caller owns the transaction.
func migrateRemoteFence(tx *bolt.Tx) error {
	if schemaNumber(tx) != 6 {
		return ErrArtifactConflict
	}
	if err := validateSnapshot(tx); err != nil {
		return err
	}
	if b := tx.Bucket(artifactsBucket); b != nil {
		var root artifactRoot
		if err := artifactDecode(b.Get([]byte("state")), &root); err != nil {
			return err
		}
		if root.SchemaFloor != 6 || b.Bucket([]byte("remote_attempts")) != nil {
			return artifactCorrupt()
		}
		if _, err := b.CreateBucket([]byte("remote_attempts")); err != nil {
			return err
		}
		root.SchemaFloor = 7
		raw, err := json.Marshal(root)
		if err != nil {
			return err
		}
		if err := b.Put([]byte("state"), raw); err != nil {
			return err
		}
	}
	return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(7))
}

// migrateWorkflowFence validates the genuine schema7 installation before
// installing the empty workflow bucket and, with installed artifacts, the
// empty input table and root floor8. The caller owns the transaction.
func migrateWorkflowFence(tx *bolt.Tx) error {
	if schemaNumber(tx) != 7 || tx.Bucket(workflowsBucket) != nil {
		return ErrArtifactConflict
	}
	if err := validateSnapshot(tx); err != nil {
		return err
	}
	if _, err := tx.CreateBucket(workflowsBucket); err != nil {
		return err
	}
	if b := tx.Bucket(artifactsBucket); b != nil {
		var root artifactRoot
		if err := artifactDecode(b.Get([]byte("state")), &root); err != nil {
			return err
		}
		if root.SchemaFloor != 7 || b.Bucket([]byte("workflow_inputs")) != nil {
			return artifactCorrupt()
		}
		if _, err := b.CreateBucket([]byte("workflow_inputs")); err != nil {
			return err
		}
		root.SchemaFloor = schemaVersion
		raw, err := json.Marshal(root)
		if err != nil {
			return err
		}
		if err := b.Put([]byte("state"), raw); err != nil {
			return err
		}
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
	var art *artifactState
	if tx.Bucket(artifactsBucket) != nil {
		var err error
		if art, err = artifactLoad(tx); err != nil {
			return err
		}
	}
	if err := artifactValidateTaskRows(tx, art); err != nil {
		return err
	}
	return validateWorkflows(tx, art)
}
