package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// workerSchema9 returns a closed MODEL of a schema9 installation: the named
// populated fixture with its empty worker group metadata, storage format tag
// and empty environment pin table removed and marker 9. Genuine schema9
// bytes come only from TestWorkerGroupMigrationGenuine's fixture.
func workerSchema9(t *testing.T, name string) string {
	t.Helper()
	var dir string
	switch name {
	case "plain":
		dir = t.TempDir()
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Submit(testActor(), "one", backend.Task{Argv: []string{"work"}}, "local", "test-binding"); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	case "environments":
		dir, _, _ = environmentPopulated(t)
	default:
		t.Fatalf("unknown fixture %q", name)
	}
	editFixture(t, dir, false, func(tx *bolt.Tx) error {
		b := tx.Bucket(workerGroupsBucket)
		if b == nil || b.Sequence() != 0 || b.Stats().KeyN != 0 {
			t.Fatal("fixture has worker group contents")
		}
		if err := tx.DeleteBucket(workerGroupsBucket); err != nil {
			return err
		}
		if err := environmentArtifactStripModel(tx); err != nil {
			return err
		}
		return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(9))
	})
	return dir
}

// workerMigrationState is the logical database without the format marker,
// the storage format tag, the environment pin table and the worker group
// bucket, which must be empty when present. It reports whether the worker
// group bucket was present.
func workerMigrationState(t *testing.T, db *bolt.DB) ([]byte, bool) {
	t.Helper()
	var buckets []fixtureBucket
	if err := json.Unmarshal(routingState(t, db), &buckets); err != nil {
		t.Fatal(err)
	}
	buckets, _ = formatStrip(t, buckets)
	var out []fixtureBucket
	found := false
	for _, b := range buckets {
		if bytes.Equal(b.Name, workerGroupsBucket) {
			if b.Sequence != 0 || len(b.Entries) != 0 || len(b.Buckets) != 0 {
				t.Fatal("migration added worker group contents or sequence")
			}
			found = true
			continue
		}
		out = append(out, b)
	}
	data, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	return data, found
}

func workerSchema9State(t *testing.T, dir string) []byte {
	t.Helper()
	db, err := bolt.Open(filepath.Join(dir, "jobs.db"), 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(tx *bolt.Tx) error {
		if schemaNumber(tx) != 9 {
			t.Fatalf("source marker %d, want 9", schemaNumber(tx))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	state, found := workerMigrationState(t, db)
	if found {
		t.Fatal("schema9 source has worker group metadata")
	}
	return state
}

// workerMigrated opens dir and checks that the migration added exactly the
// empty worker group bucket, the storage format tag, the empty environment
// pin table with artifacts, and the current marker to want.
func workerMigrated(t *testing.T, dir string, want []byte) {
	t.Helper()
	for range 2 {
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		got, found := workerMigrationState(t, s.db)
		if !found || !bytes.Equal(got, want) {
			s.Close()
			t.Fatalf("migration changed state or omitted the bucket (found %v)", found)
		}
		if err := s.db.View(func(tx *bolt.Tx) error {
			if schemaNumber(tx) != schemaVersion {
				t.Errorf("marker %d, schema %d", schemaNumber(tx), schemaVersion)
			}
			return validateSnapshot(tx)
		}); err != nil {
			s.Close()
			t.Fatal(err)
		}
		live, err := s.WorkerGroups(0, 1)
		if err != nil || len(live) != 0 {
			s.Close()
			t.Fatalf("migrated groups: %v %d", err, len(live))
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestWorkerGroupMigrationFromSchema9(t *testing.T) {
	for _, name := range []string{"plain", "environments"} {
		t.Run(name, func(t *testing.T) {
			dir := workerSchema9(t, name)
			workerMigrated(t, dir, workerSchema9State(t, dir))
		})
	}
}

// TestWorkerGroupMigrationGenuine migrates a schema9 jobs.db written by the
// shipping Store at e2d2116. It runs only when the SKYGO_SCHEMA9_DB pair
// names one.
func TestWorkerGroupMigrationGenuine(t *testing.T) {
	src, data := genuineFixture(t, "SKYGO_SCHEMA9_DB")
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "jobs.db"), data, 0600); err != nil {
		t.Fatal(err)
	}
	workerMigrated(t, dir, workerSchema9State(t, dir))
	if after, err := os.ReadFile(src); err != nil || !bytes.Equal(after, data) {
		t.Fatalf("source changed: %v", err)
	}
}

func TestWorkerGroupMigrationRollback(t *testing.T) {
	dir := workerSchema9(t, "plain")
	db, err := bolt.Open(filepath.Join(dir, "jobs.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	injected := errors.New("rollback worker group migration")
	if err := db.Update(func(tx *bolt.Tx) error {
		if err := initialize(tx, "unused"); err != nil {
			return err
		}
		if schemaNumber(tx) != schemaVersion || tx.Bucket(workerGroupsBucket) == nil {
			t.Error("worker group migration did not run")
		}
		return injected
	}); !errors.Is(err, injected) {
		t.Fatalf("migration: %v", err)
	}
	if err := db.View(func(tx *bolt.Tx) error {
		if schemaNumber(tx) != 9 {
			t.Error("marker escaped rollback")
		}
		if tx.Bucket(workerGroupsBucket) != nil {
			t.Error("worker group metadata escaped rollback")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerGroupMigrationRefusesWrongSource(t *testing.T) {
	s := openTest(t)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		if err := migrateWorkerGroupFence(tx); !errors.Is(err, ErrArtifactConflict) {
			t.Errorf("schema10 migration: %v", err)
		}
		if err := tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(9)); err != nil {
			return err
		}
		if err := migrateWorkerGroupFence(tx); !errors.Is(err, ErrArtifactConflict) {
			t.Errorf("schema9 migration over an existing bucket: %v", err)
		}
		if err := tx.DeleteBucket(workerGroupsBucket); err != nil {
			return err
		}
		if err := tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(8)); err != nil {
			return err
		}
		if err := migrateWorkerGroupFence(tx); !errors.Is(err, ErrArtifactConflict) {
			t.Errorf("schema8 migration: %v", err)
		}
		return errors.New("discard")
	}); err == nil {
		t.Fatal("test transaction committed")
	}
}

func TestWorkerGroupSchemaFences(t *testing.T) {
	for _, tt := range []struct {
		name, want string
		edit       func(*bolt.Tx) error
	}{
		{"nine with worker groups", "worker group metadata requires schema 10", lowerMarkerModel(9)},
		{"eight with worker groups", "requires schema", lowerMarkerModel(8)},
		{"ten without worker groups", "missing worker group metadata", func(tx *bolt.Tx) error {
			return tx.DeleteBucket(workerGroupsBucket)
		}},
		{"future before decode", ErrUnsupportedSchema.Error(), func(tx *bolt.Tx) error {
			if err := tx.Bucket(workerGroupsBucket).Put([]byte("g/x"), []byte("invalid")); err != nil {
				return err
			}
			return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(schemaVersion+1))
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir, _, _ := environmentPopulated(t)
			editFixture(t, dir, false, tt.edit)
			err := environmentRefused(t, dir, tt.want)
			if tt.name == "future before decode" && !errors.Is(err, ErrUnsupportedSchema) {
				t.Fatalf("future: %v", err)
			}
		})
	}
}

// TestWorkerGroupMigrationRefusesCorruptSource validates the schema9 source
// before installing the bucket: a corrupt source is refused unchanged.
func TestWorkerGroupMigrationRefusesCorruptSource(t *testing.T) {
	dir := workerSchema9(t, "environments")
	editFixture(t, dir, false, func(tx *bolt.Tx) error {
		return tx.Bucket(environmentsBucket).Put([]byte("x/1"), []byte("1"))
	})
	environmentRefused(t, dir, "")
	workerSchema9State(t, dir)
}
