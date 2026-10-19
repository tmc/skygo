package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

func legacyFixture(t *testing.T, dir string, unsynced bool) backend.Job {
	t.Helper()
	db, err := bolt.Open(filepath.Join(dir, "jobs.db"), 0600, &bolt.Options{NoFreelistSync: unsynced})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	j := backend.Job{ID: "0123456789abcdef0123456789abcdef", Task: backend.Task{Argv: []string{"work"}}, Target: "local", Status: backend.Status{State: backend.Succeeded}, Sequence: 1, CreatedAt: time.Unix(100, 0).UTC(), UpdatedAt: time.Unix(200, 0).UTC()}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{jobsBucket, requestsBucket, sequenceBucket, metadataBucket} {
			if _, err := tx.CreateBucket(name); err != nil {
				return err
			}
		}
		data, err := json.Marshal(j)
		if err != nil {
			return err
		}
		if err := tx.Bucket(jobsBucket).Put([]byte(j.ID), data); err != nil {
			return err
		}
		if err := tx.Bucket(requestsBucket).Put([]byte("original-request"), []byte(j.ID)); err != nil {
			return err
		}
		if err := tx.Bucket(sequenceBucket).Put(sequenceKey(1), []byte(j.ID)); err != nil {
			return err
		}
		if err := tx.Bucket(sequenceBucket).SetSequence(1); err != nil {
			return err
		}
		return tx.Bucket(metadataBucket).Put([]byte("owner"), []byte("legacy-owner"))
	})
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func editFixture(t *testing.T, dir string, unsynced bool, fn func(*bolt.Tx) error) {
	t.Helper()
	db, err := bolt.Open(filepath.Join(dir, "jobs.db"), 0600, &bolt.Options{NoFreelistSync: unsynced})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(fn); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyMigrationPreservesRecordsAndIdentity(t *testing.T) {
	dir := t.TempDir()
	first := legacyFixture(t, dir, false)
	second := first
	second.ID = "fedcba9876543210fedcba9876543210"
	second.Task = backend.Task{Argv: []string{"second"}, Env: map[string]string{"K": "V"}}
	second.Sequence = 2
	editFixture(t, dir, false, func(tx *bolt.Tx) error {
		raw, err := json.Marshal(second)
		if err != nil {
			return err
		}
		if err := tx.Bucket(jobsBucket).Put([]byte(second.ID), raw); err != nil {
			return err
		}
		if err := tx.Bucket(requestsBucket).Put([]byte("second-request"), []byte(second.ID)); err != nil {
			return err
		}
		if err := tx.Bucket(sequenceBucket).Put(sequenceKey(2), []byte(second.ID)); err != nil {
			return err
		}
		return tx.Bucket(sequenceBucket).SetSequence(2)
	})
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.Owner() != "legacy-owner" {
		t.Fatal("owner changed")
	}
	for _, binding := range []struct {
		request string
		job     backend.Job
	}{{"original-request", first}, {"second-request", second}} {
		task := binding.job.Task
		if len(task.Env) == 0 {
			task.Env = map[string]string{}
		}
		got, err := s.Submit(binding.request, task, "replacement")
		if err != nil || got.ID != binding.job.ID || got.Target != binding.job.Target || got.Sequence != binding.job.Sequence {
			t.Fatalf("retry: %+v %v", got, err)
		}
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		var total uint64
		for _, j := range []backend.Job{first, second} {
			raw, _ := json.Marshal(j)
			if !bytes.Equal(tx.Bucket(jobsBucket).Get([]byte(j.ID)), raw) {
				t.Error("migration rewrote accepted record")
			}
			taskRaw, _ := json.Marshal(j.Task)
			total += uint64(len(taskRaw))
		}
		m := tx.Bucket(metadataBucket)
		if !bytes.Equal(m.Get(schemaKey), sequenceKey(1)) {
			t.Error("missing schema fence")
		}
		if !bytes.Equal(m.Get(taskBytesKey), sequenceKey(total)) {
			t.Error("aggregate counter not derived")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.List(0, 2, 10)
	if err != nil || len(jobs) != 2 || jobs[0].ID != first.ID || jobs[1].ID != second.ID {
		t.Fatalf("pagination changed: %+v %v", jobs, err)
	}
	if n, err := s.LastSequence(); err != nil || n != 2 {
		t.Fatalf("sequence changed: %d %v", n, err)
	}
}

func TestSchemaRefusedWithoutWrites(t *testing.T) {
	for _, unsynced := range []bool{false, true} {
		for _, marker := range []struct {
			name   string
			value  []byte
			future bool
		}{{"future", sequenceKey(2), true}, {"malformed", []byte{1}, false}} {
			t.Run(marker.name+map[bool]string{false: "/synced", true: "/unsynced"}[unsynced], func(t *testing.T) {
				dir := t.TempDir()
				legacyFixture(t, dir, unsynced)
				editFixture(t, dir, unsynced, func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put(schemaKey, marker.value) })
				path := filepath.Join(dir, "jobs.db")
				before, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				s, err := Open(dir)
				if err == nil {
					s.Close()
					t.Fatal("unsupported schema opened")
				}
				if marker.future && !errors.Is(err, ErrUnsupportedSchema) {
					t.Fatalf("future error: %v", err)
				}
				after, readErr := os.ReadFile(path)
				if readErr != nil {
					t.Fatal(readErr)
				}
				if !bytes.Equal(before, after) {
					t.Fatalf("refused open changed database: %x -> %x", sha256.Sum256(before), sha256.Sum256(after))
				}
			})
		}
	}
}

func TestMigrationRollback(t *testing.T) {
	dir := t.TempDir()
	legacyFixture(t, dir, false)
	editFixture(t, dir, false, func(tx *bolt.Tx) error {
		return tx.Bucket(jobsBucket).Put([]byte("zz-invalid-last-record"), []byte("not JSON"))
	})
	db, err := openSnapshot(filepath.Join(dir, "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	var before []byte
	if err := db.View(func(tx *bolt.Tx) error {
		before = append(before, tx.Bucket(jobsBucket).Get([]byte("zz-invalid-last-record"))...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if s, err := Open(dir); err == nil {
		s.Close()
		t.Fatal("invalid migration succeeded")
	}
	db, err = openSnapshot(filepath.Join(dir, "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(tx *bolt.Tx) error {
		m := tx.Bucket(metadataBucket)
		if m.Get(schemaKey) != nil || m.Get(taskBytesKey) != nil || string(m.Get([]byte("owner"))) != "legacy-owner" {
			t.Error("failed migration partially committed")
		}
		if !bytes.Equal(before, tx.Bucket(jobsBucket).Get([]byte("zz-invalid-last-record"))) {
			t.Error("failed migration rewrote record")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyCanonicalTaskV1(t *testing.T) {
	for _, test := range []struct {
		name string
		task backend.Task
		want string
	}{
		{"zero", backend.Task{}, `{"argv":null}`},
		{"nil-env", backend.Task{Argv: []string{"work"}}, `{"argv":["work"]}`},
		{"empty-env", backend.Task{Argv: []string{"work"}, Env: map[string]string{}}, `{"argv":["work"]}`},
		{"sorted-env", backend.Task{Argv: []string{"work"}, Env: map[string]string{"Z": "2", "A": "1"}, CPUMillis: 1, TimeoutSeconds: 2}, `{"argv":["work"],"env":{"A":"1","Z":"2"},"cpu_millis":1,"timeout_seconds":2}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw, err := json.Marshal(test.task)
			if err != nil || string(raw) != test.want {
				t.Fatalf("canonical hash input changed: %s %v", raw, err)
			}
			if sha256.Sum256(raw) != sha256.Sum256([]byte(test.want)) {
				t.Fatal("hash changed")
			}
		})
	}
}

func TestLegacyMissingBucketRefused(t *testing.T) {
	dir := t.TempDir()
	legacyFixture(t, dir, false)
	editFixture(t, dir, false, func(tx *bolt.Tx) error { return tx.DeleteBucket(requestsBucket) })
	before, _ := os.ReadFile(filepath.Join(dir, "jobs.db"))
	if s, err := Open(dir); err == nil {
		s.Close()
		t.Fatal("partial store accepted")
	}
	after, _ := os.ReadFile(filepath.Join(dir, "jobs.db"))
	if !bytes.Equal(before, after) {
		t.Fatal("partial store mutated")
	}
}

func TestLegacyCorruptionRefusedWithExistingCounter(t *testing.T) {
	for _, kind := range []string{"job", "sequence", "request"} {
		for _, unsynced := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "/synced", true: "/unsynced"}[unsynced], func(t *testing.T) {
				dir := t.TempDir()
				j := legacyFixture(t, dir, unsynced)
				editFixture(t, dir, unsynced, func(tx *bolt.Tx) error {
					raw, _ := json.Marshal(j.Task)
					if err := tx.Bucket(metadataBucket).Put(taskBytesKey, sequenceKey(uint64(len(raw)))); err != nil {
						return err
					}
					switch kind {
					case "job":
						return tx.Bucket(jobsBucket).Put([]byte(j.ID), []byte("invalid JSON"))
					case "sequence":
						return tx.Bucket(sequenceBucket).Put(sequenceKey(2), []byte("orphan"))
					default:
						return tx.Bucket(requestsBucket).Put([]byte("orphan"), []byte("missing"))
					}
				})
				path := filepath.Join(dir, "jobs.db")
				before, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if s, err := Open(dir); err == nil {
					s.Close()
					t.Fatal("corrupt legacy store migrated")
				}
				after, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(before, after) {
					t.Fatal("refused legacy store changed bytes")
				}
			})
		}
	}
}

func TestMigrationTransactionRollback(t *testing.T) {
	dir := t.TempDir()
	legacyFixture(t, dir, false)
	db, err := bolt.Open(filepath.Join(dir, "jobs.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	injected := errors.New("injected after migration writes")
	if err := db.Update(func(tx *bolt.Tx) error {
		if err := initialize(tx, "unused-candidate"); err != nil {
			return err
		}
		return injected
	}); !errors.Is(err, injected) {
		t.Fatalf("migration failure: %v", err)
	}
	if err := db.View(func(tx *bolt.Tx) error {
		m := tx.Bucket(metadataBucket)
		if m.Get(schemaKey) != nil || m.Get(taskBytesKey) != nil || string(m.Get([]byte("owner"))) != "legacy-owner" {
			t.Error("failed migration committed partial metadata")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
