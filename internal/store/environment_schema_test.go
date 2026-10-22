package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/environment"
	bolt "go.etcd.io/bbolt"
)

// environmentState is the logical database without the format marker. The
// environment bucket must exist and be empty; it is then left out.
func environmentState(t *testing.T, db *bolt.DB) []byte {
	t.Helper()
	var buckets []fixtureBucket
	if err := json.Unmarshal(routingState(t, db), &buckets); err != nil {
		t.Fatal(err)
	}
	var out []fixtureBucket
	found := false
	for _, b := range buckets {
		if bytes.Equal(b.Name, environmentsBucket) {
			if b.Sequence != 0 || len(b.Entries) != 0 || len(b.Buckets) != 0 {
				t.Fatal("migration added environment contents or sequence")
			}
			found = true
			continue
		}
		out = append(out, b)
	}
	if !found {
		t.Fatal("migration omitted environment bucket")
	}
	data, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestEnvironmentMigrationFromPredecessorFixtures(t *testing.T) {
	for _, name := range []string{"schema1", "schema2"} {
		t.Run(name, func(t *testing.T) {
			dir := routingFixture(t, name)
			s, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			environmentState(t, s.db)
			if err := s.db.View(func(tx *bolt.Tx) error {
				if schemaNumber(tx) != 9 {
					t.Errorf("marker %d", schemaNumber(tx))
				}
				return validateSnapshot(tx)
			}); err != nil {
				t.Fatal(err)
			}
			page, err := s.Environments(0, 10)
			if err != nil || len(page.Environments) != 0 {
				t.Fatalf("migrated environments: %+v %v", page, err)
			}
		})
	}
}

func TestEnvironmentMigrationRollback(t *testing.T) {
	dir := routingFixture(t, "schema1")
	db, err := bolt.Open(filepath.Join(dir, "jobs.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var marker []byte
	if err := db.View(func(tx *bolt.Tx) error {
		marker = bytes.Clone(tx.Bucket(metadataBucket).Get(schemaKey))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("rollback environment migration")
	if err := db.Update(func(tx *bolt.Tx) error {
		if err := initialize(tx, "unused"); err != nil {
			return err
		}
		if schemaNumber(tx) != 9 || tx.Bucket(environmentsBucket) == nil {
			t.Error("environment migration did not run")
		}
		return injected
	}); !errors.Is(err, injected) {
		t.Fatalf("migration: %v", err)
	}
	if err := db.View(func(tx *bolt.Tx) error {
		if !bytes.Equal(tx.Bucket(metadataBucket).Get(schemaKey), marker) {
			t.Error("marker escaped rollback")
		}
		if tx.Bucket(environmentsBucket) != nil || tx.Bucket(workflowsBucket) != nil {
			t.Error("environment or workflow metadata escaped rollback")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestEnvironmentMigrationRefusesWrongSource(t *testing.T) {
	s := openTest(t)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		if err := migrateEnvironmentFence(tx); !errors.Is(err, ErrArtifactConflict) {
			t.Errorf("schema9 migration: %v", err)
		}
		if err := tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(8)); err != nil {
			return err
		}
		if err := migrateEnvironmentFence(tx); !errors.Is(err, ErrArtifactConflict) {
			t.Errorf("schema8 migration over an existing bucket: %v", err)
		}
		return errors.New("discard")
	}); err == nil {
		t.Fatal("test transaction committed")
	}
}

// environmentPopulated returns a closed scoped installation with two
// environments: alice's Ready "analysis" on alias "scratch", and bob's "b".
func environmentPopulated(t *testing.T) (dir string, alice, bob Environment) {
	t.Helper()
	f := newEnvironmentFixture(t)
	spec := environmentSpec("analysis")
	spec.StorageAlias = "scratch"
	alice = f.accept(t, "alice", "alice-1", spec)
	alice = f.commit(t, alice, environment.Present, environment.Ready)
	bob = f.accept(t, "bob", "bob-1", environmentSpec("b"))
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	return f.dir, alice, bob
}

// environmentRefused checks that Open refuses dir without changing a byte.
func environmentRefused(t *testing.T, dir, want string) error {
	t.Helper()
	path := filepath.Join(dir, "jobs.db")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(dir)
	if err == nil {
		s.Close()
		t.Fatal("refused state opened")
	}
	if want != "" && !strings.Contains(err.Error(), want) {
		t.Fatalf("open: %v, want %q", err, want)
	}
	after, readErr := os.ReadFile(path)
	if readErr != nil || !bytes.Equal(before, after) {
		t.Fatalf("refused open changed database: %v", readErr)
	}
	return err
}

func TestEnvironmentSchemaFences(t *testing.T) {
	for _, test := range []struct {
		name, want string
		edit       func(*bolt.Tx) error
	}{
		{"eight with environments", "environment metadata requires schema 9", func(tx *bolt.Tx) error {
			return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(8))
		}},
		{"seven with environments", "requires schema", func(tx *bolt.Tx) error {
			return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(7))
		}},
		{"nine without environments", "missing environment metadata", func(tx *bolt.Tx) error {
			return tx.DeleteBucket(environmentsBucket)
		}},
		{"future before decode", ErrUnsupportedSchema.Error(), func(tx *bolt.Tx) error {
			if err := tx.Bucket(environmentsBucket).Put([]byte("e/x"), []byte("invalid")); err != nil {
				return err
			}
			return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(schemaVersion+1))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir, _, _ := environmentPopulated(t)
			editFixture(t, dir, false, test.edit)
			err := environmentRefused(t, dir, test.want)
			if test.name == "future before decode" && !errors.Is(err, ErrUnsupportedSchema) {
				t.Fatalf("future: %v", err)
			}
		})
	}
}

func TestEnvironmentCorruptionRefusedWithoutWrites(t *testing.T) {
	record := func(b *bolt.Bucket, id string, edit func(*environmentRecord)) error {
		r, err := decodeEnvironmentRecord(b.Get(environmentRecordKey(id)))
		if err != nil {
			return err
		}
		edit(&r)
		raw, err := json.Marshal(r)
		if err != nil {
			return err
		}
		return b.Put(environmentRecordKey(id), raw)
	}
	for _, test := range []struct {
		name string
		edit func(b *bolt.Bucket, alice, bob Environment) error
	}{
		{"record JSON", func(b *bolt.Bucket, a, _ Environment) error {
			return b.Put(environmentRecordKey(a.Identity.ID), []byte("{"))
		}},
		{"record encoding", func(b *bolt.Bucket, a, _ Environment) error {
			return b.Put(environmentRecordKey(a.Identity.ID), append(bytes.Clone(b.Get(environmentRecordKey(a.Identity.ID))), ' '))
		}},
		{"record key", func(b *bolt.Bucket, a, o Environment) error {
			return b.Put(environmentRecordKey(o.Identity.ID), bytes.Clone(b.Get(environmentRecordKey(a.Identity.ID))))
		}},
		{"installation", func(b *bolt.Bucket, a, _ Environment) error {
			return record(b, a.Identity.ID, func(r *environmentRecord) { r.Identity.Installation = "other" })
		}},
		{"present deleted", func(b *bolt.Bucket, a, _ Environment) error {
			return record(b, a.Identity.ID, func(r *environmentRecord) { r.State.Phase = environment.Deleted })
		}},
		{"checkpoint", func(b *bolt.Bucket, a, _ Environment) error {
			return record(b, a.Identity.ID, func(r *environmentRecord) { r.Checkpoint.Data = make([]byte, environment.MaxCheckpointBytes+1) })
		}},
		{"spec bytes", func(b *bolt.Bucket, a, _ Environment) error {
			// Same name, target and alias: only the digest binds the image.
			spec := a.Spec
			spec.Image = "task:v2"
			raw, err := environment.Canonical(spec)
			if err != nil {
				return err
			}
			return b.Put(environmentSpecKey(a.Identity.ID), raw)
		}},
		{"spec encoding", func(b *bolt.Bucket, a, _ Environment) error {
			return b.Put(environmentSpecKey(a.Identity.ID), append(bytes.Clone(b.Get(environmentSpecKey(a.Identity.ID))), ' '))
		}},
		{"missing spec", func(b *bolt.Bucket, a, _ Environment) error {
			return b.Delete(environmentSpecKey(a.Identity.ID))
		}},
		{"missing request", func(b *bolt.Bucket, a, _ Environment) error {
			return b.Delete(environmentRequestKey("alice", "alice-1"))
		}},
		{"extra request", func(b *bolt.Bucket, a, _ Environment) error {
			return b.Put(environmentRequestKey("alice", "extra"), []byte(a.Identity.ID))
		}},
		{"crossed request", func(b *bolt.Bucket, a, o Environment) error {
			return b.Put(environmentRequestKey("alice", "alice-1"), []byte(o.Identity.ID))
		}},
		{"missing name", func(b *bolt.Bucket, _, _ Environment) error {
			return b.Delete(environmentNameKey("alice", "analysis"))
		}},
		{"extra name", func(b *bolt.Bucket, a, _ Environment) error {
			return b.Put(environmentNameKey("alice", "other"), []byte(a.Identity.ID))
		}},
		{"missing alias", func(b *bolt.Bucket, _, _ Environment) error {
			return b.Delete(environmentAliasKey("scratch", "kube"))
		}},
		{"extra alias", func(b *bolt.Bucket, o, _ Environment) error {
			return b.Put(environmentAliasKey("other", "kube"), []byte(o.Identity.ID))
		}},
		{"missing index", func(b *bolt.Bucket, _, _ Environment) error {
			return b.Delete(environmentIndexKey(1))
		}},
		{"index ordinal", func(b *bolt.Bucket, a, _ Environment) error {
			return b.Put(environmentIndexKey(3), []byte(a.Identity.ID))
		}},
		{"missing scope", func(b *bolt.Bucket, _, _ Environment) error {
			return b.Delete(environmentScopeKey("alice", 1))
		}},
		{"high-water", func(b *bolt.Bucket, _, _ Environment) error {
			return b.Put(environmentHighKey("alice"), sequenceKey(2))
		}},
		{"missing high-water", func(b *bolt.Bucket, _, _ Environment) error {
			return b.Delete(environmentHighKey("alice"))
		}},
		{"orphan high-water", func(b *bolt.Bucket, _, _ Environment) error {
			return b.Put(environmentHighKey("carol"), sequenceKey(1))
		}},
		{"sequence", func(b *bolt.Bucket, _, _ Environment) error {
			return b.SetSequence(3)
		}},
		{"unknown row", func(b *bolt.Bucket, _, _ Environment) error {
			return b.Put([]byte("x/1"), []byte("1"))
		}},
		{"nested bucket", func(b *bolt.Bucket, _, _ Environment) error {
			_, err := b.CreateBucket([]byte("sessions"))
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir, alice, bob := environmentPopulated(t)
			// Backup validates its source, so the snapshot is taken first and
			// then given the same corruption.
			snapshots := t.TempDir()
			if err := Backup(dir, filepath.Join(snapshots, "jobs.db")); err != nil {
				t.Fatal(err)
			}
			edit := func(tx *bolt.Tx) error { return test.edit(tx.Bucket(environmentsBucket), alice, bob) }
			editFixture(t, dir, false, edit)
			environmentRefused(t, dir, "")
			out := filepath.Join(t.TempDir(), "out.db")
			if err := Backup(dir, out); err == nil {
				t.Fatal("corrupt installation backed up")
			}
			if _, err := os.Stat(out); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("refused backup created %s: %v", out, err)
			}
			editFixture(t, snapshots, false, edit)
			restored := filepath.Join(t.TempDir(), "restored")
			if err := Restore(filepath.Join(snapshots, "jobs.db"), restored); err == nil {
				t.Fatal("corrupt snapshot restored")
			}
			if _, err := os.Stat(restored); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("refused restore created %s: %v", restored, err)
			}
		})
	}
}

func TestEnvironmentBackupRestorePreservesCheckpoint(t *testing.T) {
	f := newEnvironmentFixture(t)
	data := make([]byte, 256)
	for i := range data {
		data[i] = byte(i)
	}
	// An unknown kind is retained and copied, never interpreted.
	cp := environment.Checkpoint{Kind: "future-provider", Version: 9, Data: data}
	e, err := f.s.AcceptEnvironment(f.actors["alice"], "r", environmentSpec("a"), "test-binding", f.authority["alice"], cp)
	if err != nil {
		t.Fatal(err)
	}
	e = f.commit(t, e, environment.Present, environment.Unknown)
	var record, spec []byte
	if err := f.s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(environmentsBucket)
		record, spec = bytes.Clone(b.Get(environmentRecordKey(e.Identity.ID))), bytes.Clone(b.Get(environmentSpecKey(e.Identity.ID)))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(t.TempDir(), "snapshot.db")
	if err := Backup(f.dir, snapshot); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(t.TempDir(), "restored")
	if err := Restore(snapshot, restored); err != nil {
		t.Fatal(err)
	}
	s, err := Open(restored)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.Environment(e.Identity.ID)
	if err != nil || got.Revision != e.Revision || got.State != e.State || got.Checkpoint.Kind != cp.Kind || got.Checkpoint.Version != cp.Version || !bytes.Equal(got.Checkpoint.Data, data) {
		t.Fatalf("restored: %+v %v", got, err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(environmentsBucket)
		if !bytes.Equal(b.Get(environmentRecordKey(e.Identity.ID)), record) || !bytes.Equal(b.Get(environmentSpecKey(e.Identity.ID)), spec) {
			t.Error("restore changed environment rows")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestEnvironmentGenuineSchemaEightMigration migrates a populated schema8
// database written by the predecessor's own Store code. See the S10
// producer packet for the writer and the command that produces it.
func TestEnvironmentGenuineSchemaEightMigration(t *testing.T) {
	src := os.Getenv("SKYGO_SCHEMA8_DB")
	if src == "" {
		t.Skip("requires a populated schema8 database from the predecessor's Store")
	}
	raw, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if os.Getenv("SKYGO_SCHEMA8_DB_SHA256") != hex.EncodeToString(sum[:]) {
		t.Fatal("schema8 database hash mismatch")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "jobs.db")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	db, err := openSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *bolt.Tx) error {
		if schemaNumber(tx) != 8 || tx.Bucket(environmentsBucket) != nil {
			t.Fatalf("not a schema8 database: marker %d", schemaNumber(tx))
		}
		if tx.Bucket(jobsBucket).Stats().KeyN == 0 || tx.Bucket(workflowsBucket).Stats().KeyN == 0 || tx.Bucket(artifactsBucket) == nil {
			t.Fatal("schema8 database lacks jobs, workflows or artifacts")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var before []fixtureBucket
	if err := json.Unmarshal(routingState(t, db), &before); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	want, err := json.Marshal(before)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(environmentState(t, s.db), want) {
		t.Fatal("migration changed a schema8 row or sequence")
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		if schemaNumber(tx) != 9 || st.Root.SchemaFloor != artifactFormat {
			t.Errorf("migrated to %d floor %d", schemaNumber(tx), st.Root.SchemaFloor)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	migrated := workflowDatabase(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s, err = Open(dir); err != nil {
		t.Fatal(err)
	}
	workflowUnchanged(t, s, migrated)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if out := os.Getenv("SKYGO_SCHEMA9_OUT"); out != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		w, err := os.OpenFile(out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, err = w.Write(data)
		if closeErr := w.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}

// TestEnvironmentSchemaEightBinaryRefusesSchemaNine runs the accepted schema8
// binary against a schema9 installation. It must refuse before any write.
func TestEnvironmentSchemaEightBinaryRefusesSchemaNine(t *testing.T) {
	exe := os.Getenv("SKYGO_SCHEMA8_BINARY")
	if exe == "" {
		t.Skip("requires the accepted schema8 binary")
	}
	binary, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(binary)
	if os.Getenv("SKYGO_SCHEMA8_BINARY_SHA256") != hex.EncodeToString(sum[:]) {
		t.Fatal("schema8 binary hash mismatch")
	}
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "jobs.db")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "server", "--listen", "127.0.0.1:0", "--data-dir", dir)
	cmd.Env = append(os.Environ(), "SKYGO_TOKEN=")
	out, err := cmd.CombinedOutput()
	if err == nil || ctx.Err() != nil || !bytes.Contains(out, []byte("unsupported database schema")) {
		t.Fatalf("schema8 binary did not promptly refuse schema9: %v %s", err, out)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("schema8 binary modified schema9: %v", err)
	}
}
