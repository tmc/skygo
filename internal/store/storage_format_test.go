package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	bolt "go.etcd.io/bbolt"
)

// fixturePair resolves the predecessor fixture named by the environment pair
// name and name_SHA256. Both unset skips, unless require is set. One unset,
// or a digest that is not 64 lowercase hexadecimal digits, is an error.
func fixturePair(getenv func(string) string, name string) (path, sum string, skip bool, err error) {
	path, sum = getenv(name), getenv(name+"_SHA256")
	switch {
	case path == "" && sum == "":
		if getenv("SKYGO_REQUIRE_FIXTURES") == "1" {
			return "", "", false, fmt.Errorf("%s and %s_SHA256 are required", name, name)
		}
		return "", "", true, nil
	case path == "" || sum == "":
		return "", "", false, fmt.Errorf("%s and %s_SHA256 must be set together", name, name)
	}
	if b, e := hex.DecodeString(sum); e != nil || len(b) != sha256.Size || strings.ToLower(sum) != sum {
		return "", "", false, fmt.Errorf("%s_SHA256 is not a lowercase SHA-256 digest", name)
	}
	return path, sum, false, nil
}

// genuineFixture returns the path and bytes of a genuine predecessor fixture
// whose SHA-256 matches its pair.
func genuineFixture(t *testing.T, name string) (string, []byte) {
	t.Helper()
	path, sum, skip, err := fixturePair(os.Getenv, name)
	if err != nil {
		t.Fatal(err)
	}
	if skip {
		t.Skipf("%s and %s_SHA256 are not set", name, name)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got := sha256.Sum256(data)
	if hex.EncodeToString(got[:]) != sum {
		t.Fatalf("%s hash mismatch", name)
	}
	return path, data
}

func TestFixturePair(t *testing.T) {
	digest := strings.Repeat("ab", 32)
	for _, tt := range []struct {
		name    string
		env     map[string]string
		skip    bool
		wantErr bool
	}{
		{"unset", nil, true, false},
		{"unset required", map[string]string{"SKYGO_REQUIRE_FIXTURES": "1"}, false, true},
		{"path only", map[string]string{"F": "/x"}, false, true},
		{"digest only", map[string]string{"F_SHA256": digest}, false, true},
		{"short digest", map[string]string{"F": "/x", "F_SHA256": digest[:62]}, false, true},
		{"upper digest", map[string]string{"F": "/x", "F_SHA256": strings.ToUpper(digest)}, false, true},
		{"not hex", map[string]string{"F": "/x", "F_SHA256": strings.Repeat("zz", 32)}, false, true},
		{"pair", map[string]string{"F": "/x", "F_SHA256": digest}, false, false},
		{"pair required", map[string]string{"F": "/x", "F_SHA256": digest, "SKYGO_REQUIRE_FIXTURES": "1"}, false, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path, sum, skip, err := fixturePair(func(k string) string { return tt.env[k] }, "F")
			if skip != tt.skip || (err != nil) != tt.wantErr {
				t.Fatalf("skip %v err %v", skip, err)
			}
			if err == nil && !skip && (path != "/x" || sum != digest) {
				t.Fatalf("pair %q %q", path, sum)
			}
		})
	}
}

// formatStrip removes from an exported database the storage format tag and
// the empty environment pin table that the schema11 migration adds, and the
// serving storage format tag and empty serving_services family that the
// schema13 migration adds, and reports whether the environment tag was
// present. Each tag must have exactly its known value and each added table
// must be empty.
func formatStrip(t *testing.T, buckets []fixtureBucket) ([]fixtureBucket, bool) {
	t.Helper()
	tagged := false
	out := make([]fixtureBucket, 0, len(buckets))
	for _, b := range buckets {
		switch {
		case bytes.Equal(b.Name, servingServicesBucket):
			if b.Sequence != 0 || len(b.Entries) != 0 || len(b.Buckets) != 0 {
				t.Fatal("migration added serving contents or sequence")
			}
			continue
		case bytes.Equal(b.Name, metadataBucket):
			var entries []fixtureEntry
			for _, e := range b.Entries {
				if bytes.Equal(e.Key, artifactBeginStorageFormatKey) {
					if string(e.Value) != artifactBeginStorageFormat {
						t.Fatalf("artifact Begin storage format tag %q", e.Value)
					}
					continue
				}
				if bytes.Equal(e.Key, servingStorageFormatKey) {
					if string(e.Value) != servingStorageFormat {
						t.Fatalf("serving storage format tag %q", e.Value)
					}
					continue
				}
				if bytes.Equal(e.Key, environmentStorageFormatKey) {
					if string(e.Value) != environmentStorageFormat {
						t.Fatalf("storage format tag %q", e.Value)
					}
					tagged = true
					continue
				}
				entries = append(entries, e)
			}
			b.Entries = entries
		case bytes.Equal(b.Name, artifactsBucket):
			var tables []fixtureBucket
			for _, c := range b.Buckets {
				if bytes.Equal(c.Name, artifactBeginRequestsTable) {
					if c.Sequence != 0 || len(c.Entries) != 0 || len(c.Buckets) != 0 {
						t.Fatal("migration added artifact Begin requests")
					}
					continue
				}
				if bytes.Equal(c.Name, []byte("environment_pins")) {
					if c.Sequence != 0 || len(c.Entries) != 0 || len(c.Buckets) != 0 {
						t.Fatal("migration added environment pins")
					}
					continue
				}
				tables = append(tables, c)
			}
			b.Buckets = tables
		}
		out = append(out, b)
	}
	return out, tagged
}

// environmentArtifactStripModel is a MODEL of the schema10 format: it
// removes the storage format tag and, with artifacts, the empty environment
// pin table. It refuses a populated pin table. The caller sets the marker.
func environmentArtifactStripModel(tx *bolt.Tx) error {
	if err := servingStripModel(tx); err != nil {
		return err
	}
	if err := tx.Bucket(metadataBucket).Delete(environmentStorageFormatKey); err != nil {
		return err
	}
	b := tx.Bucket(artifactsBucket)
	if b == nil {
		return nil
	}
	pins := b.Bucket([]byte("environment_pins"))
	if pins == nil {
		return nil
	}
	if k, _ := pins.Cursor().First(); k != nil {
		return errors.New("model cannot strip populated environment pins")
	}
	return b.DeleteBucket([]byte("environment_pins"))
}

// servingStripModel is a MODEL of the schema12 format over a current
// database: it removes the serving storage format tag and the empty
// serving_services family. It refuses a populated family. The caller sets
// the marker.
func servingStripModel(tx *bolt.Tx) error {
	if err := artifactBeginStripModel(tx); err != nil {
		return err
	}
	if err := tx.Bucket(metadataBucket).Delete(servingStorageFormatKey); err != nil {
		return err
	}
	b := tx.Bucket(servingServicesBucket)
	if b == nil {
		return nil
	}
	if k, _ := b.Cursor().First(); k != nil || b.Sequence() != 0 {
		return errors.New("model cannot strip populated serving metadata")
	}
	return tx.DeleteBucket(servingServicesBucket)
}

// lowerMarkerModel is a MODEL corruption: it lowers the marker to n and
// removes the storage format tags and the empty serving family, so that the
// ordinal fence and not the format fence is the first refusal.
func lowerMarkerModel(n uint64) func(*bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		if err := servingStripModel(tx); err != nil {
			return err
		}
		m := tx.Bucket(metadataBucket)
		if err := m.Delete(environmentStorageFormatKey); err != nil {
			return err
		}
		return m.Put(schemaKey, sequenceKey(n))
	}
}

// formatArtifactPopulated returns a closed current installation with two
// environments and installed artifact metadata.
func formatArtifactPopulated(t *testing.T) string {
	t.Helper()
	dir, _, _ := environmentPopulated(t)
	editFixture(t, dir, false, func(tx *bolt.Tx) error {
		root := artifactRoot{Layout: 1, SchemaFloor: artifactFormat, Policy: "default-v1", ID: strings.Repeat("a", 32), Unit: 4096, Volume: 17}
		if err := initializeArtifacts(tx, root); err != nil {
			return err
		}
		return validateSnapshot(tx)
	})
	return dir
}

func formatTag(t *testing.T, path string) []byte {
	t.Helper()
	db, err := openSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var tag []byte
	if err := db.View(func(tx *bolt.Tx) error {
		if schemaNumber(tx) != schemaVersion {
			t.Errorf("marker %d", schemaNumber(tx))
		}
		tag = bytes.Clone(tx.Bucket(metadataBucket).Get(environmentStorageFormatKey))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return tag
}

func dirNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func fileSum(t *testing.T, path string) [sha256.Size]byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(data)
}

// readRow is an artifact read row with the given top-level JSON object.
func readRow(object string) func(*bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		return tx.Bucket(artifactsBucket).Bucket([]byte("reads")).Put([]byte(strings.Repeat("c", 32)), []byte(object))
	}
}

// TestStorageFormatRefusedEverywhere injects each foreign format fact into a
// real current Store database and checks that Open, Backup, Restore,
// snapshot and both physical readers refuse it without changing jobs.db or
// creating any output. Every injection is a MODEL of a foreign database.
func TestStorageFormatRefusedEverywhere(t *testing.T) {
	escaped := "{\"Serv" + string('\\') + "u0069ce\":\"x\"}"
	folded := "{\"" + string(rune(0x17f)) + "ervice\":\"x\"}"
	for _, tt := range []struct {
		name string
		edit func(*bolt.Tx) error
		// read reports a read-row fact, which a probe leaves unverified.
		read   bool
		reason string
	}{
		{"tag absent", func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Delete(environmentStorageFormatKey) }, false, "unsupported_storage_format"},
		{"tag empty", func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put(environmentStorageFormatKey, []byte{}) }, false, "unsupported_storage_format"},
		{"tag unknown", func(tx *bolt.Tx) error {
			return tx.Bucket(metadataBucket).Put(environmentStorageFormatKey, []byte("skygo-environment-store-v2"))
		}, false, "unsupported_storage_format"},
		{"tag trailing byte", func(tx *bolt.Tx) error {
			return tx.Bucket(metadataBucket).Put(environmentStorageFormatKey, []byte(environmentStorageFormat+"\x00"))
		}, false, "unsupported_storage_format"},
		{"tag bucket", func(tx *bolt.Tx) error {
			m := tx.Bucket(metadataBucket)
			if err := m.Delete(environmentStorageFormatKey); err != nil {
				return err
			}
			_, err := m.CreateBucket(environmentStorageFormatKey)
			return err
		}, false, "unsupported_storage_format"},
		{"tag at ten", func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(10)) }, false, "unsupported_storage_format"},
		{"services", func(tx *bolt.Tx) error { _, err := tx.CreateBucket([]byte("services")); return err }, false, "unsupported_storage_format"},
		{"compat_requests", func(tx *bolt.Tx) error { _, err := tx.CreateBucket([]byte("compat_requests")); return err }, false, "unsupported_storage_format"},
		{"service_pins table", func(tx *bolt.Tx) error {
			_, err := tx.Bucket(artifactsBucket).CreateBucket([]byte("service_pins"))
			return err
		}, false, "unsupported_storage_format"},
		{"service_pins scalar", func(tx *bolt.Tx) error {
			return tx.Bucket(artifactsBucket).Put([]byte("service_pins"), []byte("{}"))
		}, false, "unsupported_storage_format"},
		{"read Service", readRow(`{"Service":"web"}`), true, "unsupported_storage_format"},
		{"read ServiceRevision", readRow(`{"ServiceRevision":1}`), true, "unsupported_storage_format"},
		{"read ServicePod", readRow(`{"ServicePod":"pod"}`), true, "unsupported_storage_format"},
		{"read empty value", readRow(`{"Owner":"alice","Service":""}`), true, "unsupported_storage_format"},
		{"read lower case", readRow(`{"service":"web"}`), true, "unsupported_storage_format"},
		{"read mixed case", readRow(`{"sErViCePoD":"pod"}`), true, "unsupported_storage_format"},
		{"read escaped", readRow(escaped), true, "unsupported_storage_format"},
		{"read folded", readRow(folded), true, "unsupported_storage_format"},
		{"future", func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(schemaVersion+1)) }, false, "unsupported_schema"},
		{"future schema", func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(schemaVersion+1)) }, false, "unsupported_schema"},
		{"future max", func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(math.MaxUint64)) }, false, "unsupported_schema"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := formatArtifactPopulated(t)
			editFixture(t, dir, false, tt.edit)
			path := filepath.Join(dir, "jobs.db")
			before := fileSum(t, path)
			unchanged := func(op string) {
				t.Helper()
				if fileSum(t, path) != before {
					t.Fatalf("%s changed jobs.db", op)
				}
			}

			if s, err := Open(dir); !errors.Is(err, ErrUnsupportedSchema) {
				if s != nil {
					s.Close()
				}
				t.Fatalf("Open: %v", err)
			}
			unchanged("Open")

			out := t.TempDir()
			if err := Backup(dir, filepath.Join(out, "backup.db")); !errors.Is(err, ErrUnsupportedSchema) {
				t.Fatalf("Backup: %v", err)
			}
			if names := dirNames(t, out); len(names) != 0 {
				t.Fatalf("Backup left %v", names)
			}
			unchanged("Backup")

			parent := t.TempDir()
			if err := Restore(path, filepath.Join(parent, "restored")); !errors.Is(err, ErrUnsupportedSchema) {
				t.Fatalf("Restore: %v", err)
			}
			if names := dirNames(t, parent); len(names) != 0 {
				t.Fatalf("Restore left %v", names)
			}
			unchanged("Restore")

			db, err := openSnapshot(path)
			if err != nil {
				t.Fatal(err)
			}
			var buf bytes.Buffer
			err = snapshot(db, &buf)
			if closeErr := db.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
			if !errors.Is(err, ErrUnsupportedSchema) || buf.Len() != 0 {
				t.Fatalf("snapshot: %v, %d bytes", err, buf.Len())
			}
			unchanged("snapshot")

			got, err := InspectPhysicalState(context.Background(), dir)
			if err == nil || got.SchemaSupported == nil || *got.SchemaSupported || got.Reason != tt.reason {
				t.Fatalf("InspectPhysicalState: %v supported %v reason %q", err, got.SchemaSupported, got.Reason)
			}
			unchanged("InspectPhysicalState")

			got, err = ProbePhysicalState(context.Background(), dir)
			if tt.read {
				if err != nil || got.SchemaSupported != nil || !strings.Contains(strings.Join(got.Conditions, ","), "storage_format_unverified") {
					t.Fatalf("ProbePhysicalState: %v supported %v conditions %v", err, got.SchemaSupported, got.Conditions)
				}
			} else if err == nil || got.SchemaSupported == nil || *got.SchemaSupported || got.Reason != tt.reason {
				t.Fatalf("ProbePhysicalState: %v supported %v reason %q", err, got.SchemaSupported, got.Reason)
			}
			unchanged("ProbePhysicalState")
		})
	}
}

// TestStorageFormatReadControl checks that a MODEL read row whose unknown
// field names no foreign field is corruption that Open refuses, not a
// foreign format. Physical inspection decodes no record, so it reports the
// format supported.
func TestStorageFormatReadControl(t *testing.T) {
	dir := formatArtifactPopulated(t)
	editFixture(t, dir, false, readRow(`{"Servic":"web"}`))
	err := environmentRefused(t, dir, "")
	if errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("unknown non-foreign field reported as foreign: %v", err)
	}
	got, err := InspectPhysicalState(context.Background(), dir)
	if err != nil || got.SchemaSupported == nil || !*got.SchemaSupported {
		t.Fatalf("inspection: %v supported %v reason %q", err, got.SchemaSupported, got.Reason)
	}
}

// TestStorageFormatTagPreserved opens, reopens, backs up and restores a
// current installation and checks that the tag is kept exactly.
func TestStorageFormatTagPreserved(t *testing.T) {
	dir, _, _ := environmentPopulated(t)
	path := filepath.Join(dir, "jobs.db")
	if tag := formatTag(t, path); string(tag) != environmentStorageFormat {
		t.Fatalf("tag %q", tag)
	}
	for range 2 {
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if tag := formatTag(t, path); string(tag) != environmentStorageFormat {
			t.Fatalf("reopen tag %q", tag)
		}
	}
	snapshotPath := filepath.Join(t.TempDir(), "snapshot.db")
	if err := Backup(dir, snapshotPath); err != nil {
		t.Fatal(err)
	}
	if tag := formatTag(t, snapshotPath); string(tag) != environmentStorageFormat {
		t.Fatalf("backup tag %q", tag)
	}
	restored := filepath.Join(t.TempDir(), "restored")
	if err := Restore(snapshotPath, restored); err != nil {
		t.Fatal(err)
	}
	if tag := formatTag(t, filepath.Join(restored, "jobs.db")); string(tag) != environmentStorageFormat {
		t.Fatalf("restore tag %q", tag)
	}
	s, err := Open(restored)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for _, inspect := range []func(context.Context, string) (PhysicalInspection, error){InspectPhysicalState, ProbePhysicalState} {
		got, err := inspect(context.Background(), restored)
		if err != nil || got.SchemaSupported == nil || !*got.SchemaSupported {
			t.Fatalf("inspection: %v supported %v reason %q", err, got.SchemaSupported, got.Reason)
		}
	}
}

// TestStorageFormatArtifactInspection checks support of a current
// installation with installed artifacts and no read rows.
func TestStorageFormatArtifactInspection(t *testing.T) {
	dir := formatArtifactPopulated(t)
	for _, inspect := range []func(context.Context, string) (PhysicalInspection, error){InspectPhysicalState, ProbePhysicalState} {
		got, err := inspect(context.Background(), dir)
		if err != nil || got.SchemaSupported == nil || !*got.SchemaSupported {
			t.Fatalf("inspection: %v supported %v reason %q", err, got.SchemaSupported, got.Reason)
		}
	}
}

// TestStorageFormatMigrationTagsOnce migrates MODEL schema10 installations,
// with and without artifacts, and checks that the tag is written with
// marker11 and kept through the schema12 stage.
func TestStorageFormatMigrationTagsOnce(t *testing.T) {
	for _, artifacts := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "artifacts"}[artifacts], func(t *testing.T) {
			var dir string
			if artifacts {
				dir = formatArtifactPopulated(t)
			} else {
				dir, _, _ = environmentPopulated(t)
			}
			editFixture(t, dir, false, func(tx *bolt.Tx) error {
				if err := environmentArtifactStripModel(tx); err != nil {
					return err
				}
				return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(workerGroupSchema))
			})
			s, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.db.View(func(tx *bolt.Tx) error {
				if a := tx.Bucket(artifactsBucket); (a != nil && a.Bucket([]byte("environment_pins")) != nil) != artifacts {
					t.Error("environment pin table not installed with artifacts only")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if tag := formatTag(t, filepath.Join(dir, "jobs.db")); string(tag) != environmentStorageFormat {
				t.Fatalf("tag %q", tag)
			}
		})
	}
}

// TestStorageFormatNeverAutoTagged checks that an installation at 11 or 12
// without the tag is refused unchanged, never tagged.
func TestStorageFormatNeverAutoTagged(t *testing.T) {
	for _, n := range []uint64{environmentArtifactSchema, environmentRetirementSchema} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			dir, _, _ := environmentPopulated(t)
			editFixture(t, dir, false, func(tx *bolt.Tx) error {
				if err := servingStripModel(tx); err != nil {
					return err
				}
				m := tx.Bucket(metadataBucket)
				if err := m.Delete(environmentStorageFormatKey); err != nil {
					return err
				}
				return m.Put(schemaKey, sequenceKey(n))
			})
			if err := environmentRefused(t, dir, "missing storage format tag"); !errors.Is(err, ErrUnsupportedSchema) {
				t.Fatalf("open: %v", err)
			}
		})
	}
}

// TestStorageFormatElevenMigrates migrates a MODEL schema11 installation,
// whose tag is kept unchanged.
func TestStorageFormatElevenMigrates(t *testing.T) {
	dir, _, _ := environmentPopulated(t)
	editFixture(t, dir, false, func(tx *bolt.Tx) error {
		if err := servingStripModel(tx); err != nil {
			return err
		}
		return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(environmentArtifactSchema))
	})
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if tag := formatTag(t, filepath.Join(dir, "jobs.db")); string(tag) != environmentStorageFormat {
		t.Fatalf("tag %q", tag)
	}
}

// formatLogical is the logical database including the marker.
func formatLogical(t *testing.T, db *bolt.DB) []byte {
	t.Helper()
	var marker []byte
	if err := db.View(func(tx *bolt.Tx) error {
		marker = bytes.Clone(tx.Bucket(metadataBucket).Get(schemaKey))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return append(routingState(t, db), marker...)
}

// TestMigrationStagesAtomic runs the real migration chain from each
// supported source ordinal in one transaction and fails it after the last
// stage, and fails each later stage after the earlier ones have written. No
// stage's write survives.
func TestMigrationStagesAtomic(t *testing.T) {
	// MODEL sources: a current installation lowered by removing the later
	// additions.
	source := func(t *testing.T, n uint64) *bolt.DB {
		t.Helper()
		dir := formatArtifactPopulated(t)
		editFixture(t, dir, false, func(tx *bolt.Tx) error {
			if err := servingStripModel(tx); err != nil {
				return err
			}
			if n < environmentArtifactSchema {
				if err := environmentArtifactStripModel(tx); err != nil {
					return err
				}
			}
			if n < workerGroupSchema {
				if err := tx.DeleteBucket(workerGroupsBucket); err != nil {
					return err
				}
			}
			return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(n))
		})
		db, err := bolt.Open(filepath.Join(dir, "jobs.db"), 0600, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		return db
	}
	injected := errors.New("injected after migration")
	for _, n := range []uint64{9, workerGroupSchema, environmentArtifactSchema, environmentRetirementSchema} {
		t.Run(fmt.Sprintf("chain from %d", n), func(t *testing.T) {
			db := source(t, n)
			before := formatLogical(t, db)
			if err := db.Update(func(tx *bolt.Tx) error {
				if err := initialize(tx, "unused"); err != nil {
					return err
				}
				if schemaNumber(tx) != schemaVersion || string(tx.Bucket(metadataBucket).Get(environmentStorageFormatKey)) != environmentStorageFormat || tx.Bucket(workerGroupsBucket) == nil || string(tx.Bucket(metadataBucket).Get(servingStorageFormatKey)) != servingStorageFormat || tx.Bucket(servingServicesBucket) == nil {
					t.Error("migration did not run")
				}
				return injected
			}); !errors.Is(err, injected) {
				t.Fatalf("migration: %v", err)
			}
			if !bytes.Equal(before, formatLogical(t, db)) {
				t.Fatal("a migration write escaped rollback")
			}
		})
	}
	// MODEL corruptions placed between real stages: each makes the next
	// stage's own validation fail after earlier stages wrote.
	for _, tt := range []struct {
		name    string
		stages  []func(*bolt.Tx) error
		corrupt func(*bolt.Tx) error
		last    func(*bolt.Tx) error
	}{
		{"artifact stage", []func(*bolt.Tx) error{migrateWorkerGroupFence}, func(tx *bolt.Tx) error {
			_, err := tx.Bucket(artifactsBucket).CreateBucket([]byte("environment_pins"))
			return err
		}, migrateEnvironmentArtifactFence},
		{"serving stage", []func(*bolt.Tx) error{migrateWorkerGroupFence, migrateEnvironmentArtifactFence, migrateEnvironmentRetirement}, func(tx *bolt.Tx) error {
			return tx.Bucket(metadataBucket).Put(servingStorageFormatKey, []byte("unexpected-serving-format"))
		}, migrateServing},
		{"retirement stage", []func(*bolt.Tx) error{migrateWorkerGroupFence, migrateEnvironmentArtifactFence}, func(tx *bolt.Tx) error {
			return tx.Bucket(environmentsBucket).Put([]byte("x/alice"), sequenceKey(1))
		}, migrateEnvironmentRetirement},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db := source(t, 9)
			before := formatLogical(t, db)
			var last error
			if err := db.Update(func(tx *bolt.Tx) error {
				for _, stage := range tt.stages {
					if err := stage(tx); err != nil {
						return err
					}
				}
				if err := tt.corrupt(tx); err != nil {
					return err
				}
				last = tt.last(tx)
				return last
			}); err == nil || last == nil {
				t.Fatalf("failing stage committed: %v", err)
			}
			if !bytes.Equal(before, formatLogical(t, db)) {
				t.Fatal("an earlier stage's write escaped the failing stage")
			}
		})
	}
}

func TestMigrationStagesRefuseWrongSource(t *testing.T) {
	s := openTest(t)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		if err := migrateEnvironmentArtifactFence(tx); !errors.Is(err, ErrArtifactConflict) {
			t.Errorf("schema12 artifact stage: %v", err)
		}
		if err := migrateEnvironmentRetirement(tx); err == nil {
			t.Error("schema12 retirement stage ran")
		}
		if err := tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(9)); err != nil {
			return err
		}
		if err := migrateEnvironmentArtifactFence(tx); !errors.Is(err, ErrArtifactConflict) {
			t.Errorf("schema9 artifact stage: %v", err)
		}
		if err := migrateEnvironmentRetirement(tx); err == nil {
			t.Error("schema9 retirement stage ran")
		}
		return errors.New("discard")
	}); err == nil {
		t.Fatal("test transaction committed")
	}
}

// TestStorageFormatGenuineSchema10 migrates a schema10 jobs.db written by the
// frozen worker10 Store. It runs only when the SKYGO_SCHEMA10_DB pair names
// one.
func TestStorageFormatGenuineSchema10(t *testing.T) {
	src, data := genuineFixture(t, "SKYGO_SCHEMA10_DB")
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "jobs.db")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	db, err := openSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	var before []byte
	if err := db.View(func(tx *bolt.Tx) error {
		if schemaNumber(tx) != workerGroupSchema || tx.Bucket(workerGroupsBucket) == nil || tx.Bucket(metadataBucket).Get(environmentStorageFormatKey) != nil {
			t.Fatalf("not a schema10 database: marker %d", schemaNumber(tx))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before = routingState(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	var want []fixtureBucket
	if err := json.Unmarshal(before, &want); err != nil {
		t.Fatal(err)
	}
	wantState, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		var buckets []fixtureBucket
		if err := json.Unmarshal(routingState(t, s.db), &buckets); err != nil {
			s.Close()
			t.Fatal(err)
		}
		buckets, tagged := formatStrip(t, buckets)
		got, err := json.Marshal(buckets)
		if err != nil {
			s.Close()
			t.Fatal(err)
		}
		if !tagged || !bytes.Equal(got, wantState) {
			s.Close()
			t.Fatal("migration changed a schema10 row or omitted the tag")
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if after, err := os.ReadFile(src); err != nil || !bytes.Equal(after, data) {
		t.Fatalf("source changed: %v", err)
	}
}

// TestStorageFormatGenuineForeignRefused refuses a jobs.db written by a
// foreign storage line. It runs only when the SKYGO_FOREIGN_DB pair names
// one.
func TestStorageFormatGenuineForeignRefused(t *testing.T) {
	_, data := genuineFixture(t, "SKYGO_FOREIGN_DB")
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "jobs.db"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := environmentRefused(t, dir, ""); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("open: %v", err)
	}
	got, err := InspectPhysicalState(context.Background(), dir)
	if err == nil || got.SchemaSupported == nil || *got.SchemaSupported {
		t.Fatalf("inspection: %v supported %v reason %q", err, got.SchemaSupported, got.Reason)
	}
	if after, err := os.ReadFile(filepath.Join(dir, "jobs.db")); err != nil || !bytes.Equal(after, data) {
		t.Fatalf("inspection changed jobs.db: %v", err)
	}
}

// These corruptions are MODEL injections into actual Store databases, not
// genuine predecessor fixtures. Neither inspection nor a write-capable open
// may bless an incomplete format solely because its tag and ordinal match.
func TestStorageFormatIncompleteLayout(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*bolt.Tx) error
	}{
		{"read truncated", readRow(`{"Owner":`)},
		{"read missing close", readRow(`{"Owner":"alice"`)},
		{"read trailing", readRow(`{} {"Service":"web"}`)},
		{"read array", readRow(`[]`)},
		{"reads scalar", func(tx *bolt.Tx) error {
			a := tx.Bucket(artifactsBucket)
			if err := a.DeleteBucket([]byte("reads")); err != nil {
				return err
			}
			return a.Put([]byte("reads"), []byte("{}"))
		}},
		{"pins missing", func(tx *bolt.Tx) error {
			return tx.Bucket(artifactsBucket).DeleteBucket([]byte("environment_pins"))
		}},
		{"pins scalar", func(tx *bolt.Tx) error {
			a := tx.Bucket(artifactsBucket)
			if err := a.DeleteBucket([]byte("environment_pins")); err != nil {
				return err
			}
			return a.Put([]byte("environment_pins"), []byte("{}"))
		}},
		{"pins before eleven", func(tx *bolt.Tx) error {
			if err := servingStripModel(tx); err != nil {
				return err
			}
			m := tx.Bucket(metadataBucket)
			if err := m.Delete(environmentStorageFormatKey); err != nil {
				return err
			}
			return m.Put(schemaKey, sequenceKey(workerGroupSchema))
		}},
		{"schema bucket", func(tx *bolt.Tx) error {
			m := tx.Bucket(metadataBucket)
			if err := m.Delete(schemaKey); err != nil {
				return err
			}
			_, err := m.CreateBucket(schemaKey)
			return err
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := formatArtifactPopulated(t)
			editFixture(t, dir, false, tt.edit)
			path := filepath.Join(dir, "jobs.db")
			before := fileSum(t, path)
			if s, err := Open(dir); err == nil {
				s.Close()
				t.Fatal("Open accepted incomplete layout")
			}
			for _, inspect := range []func(context.Context, string) (PhysicalInspection, error){InspectPhysicalState, ProbePhysicalState} {
				got, _ := inspect(context.Background(), dir)
				if got.SchemaSupported != nil && *got.SchemaSupported {
					t.Fatalf("inspection reported support: %+v", got)
				}
			}
			out := t.TempDir()
			if err := Backup(dir, filepath.Join(out, "backup.db")); err == nil {
				t.Fatal("Backup accepted incomplete layout")
			}
			if err := Restore(path, filepath.Join(out, "restored")); err == nil {
				t.Fatal("Restore accepted incomplete layout")
			}
			if names := dirNames(t, out); len(names) != 0 {
				t.Fatalf("published outputs: %v", names)
			}
			if fileSum(t, path) != before {
				t.Fatal("refusal changed jobs.db")
			}
		})
	}
}

// TestStorageFormatFamilyMatrix includes plain databases: the storage tag
// cannot substitute for the required Environment and worker families. All
// corruptions are MODEL edits, not genuine old-writer fixtures.
func TestStorageFormatFamilyMatrix(t *testing.T) {
	// A bucket in place of the schema scalar is not a numeric marker, so its
	// support is unknown rather than false.
	const markerBucket = "schema marker bucket"
	cases := []struct {
		name     string
		schema13 bool // the serving-family fault needs a valid schema13 base
		edit     func(*bolt.Tx) error
	}{
		{"tag before nine", false, func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(8)) }},
		{"tag at nine", false, func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(9)) }},
		{"tag at ten", false, func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(10)) }},
		{"future schema", false, func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(schemaVersion+1)) }},
		{"future max", false, func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(math.MaxUint64)) }},
		{"serving tag absent", true, func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Delete(servingStorageFormatKey) }},
		{"serving tag unknown", true, func(tx *bolt.Tx) error {
			return tx.Bucket(metadataBucket).Put(servingStorageFormatKey, []byte("skygo-serving-store-v2"))
		}},
		{"serving tag bucket", true, func(tx *bolt.Tx) error {
			m := tx.Bucket(metadataBucket)
			if err := m.Delete(servingStorageFormatKey); err != nil {
				return err
			}
			_, err := m.CreateBucket(servingStorageFormatKey)
			return err
		}},
		{"serving family absent", true, func(tx *bolt.Tx) error { return tx.DeleteBucket(servingServicesBucket) }},
		{"serving at twelve", true, func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(12)) }},
		{markerBucket, false, func(tx *bolt.Tx) error {
			m := tx.Bucket(metadataBucket)
			if err := m.Delete(schemaKey); err != nil {
				return err
			}
			_, err := m.CreateBucket(schemaKey)
			return err
		}},
		{"environments absent", false, func(tx *bolt.Tx) error { return tx.DeleteBucket(environmentsBucket) }},
		{"workers absent", false, func(tx *bolt.Tx) error { return tx.DeleteBucket(workerGroupsBucket) }},
		{"both absent", false, func(tx *bolt.Tx) error {
			if err := tx.DeleteBucket(environmentsBucket); err != nil {
				return err
			}
			return tx.DeleteBucket(workerGroupsBucket)
		}},
		{"tag absent", false, func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Delete(environmentStorageFormatKey) }},
		{"tag empty", false, func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put(environmentStorageFormatKey, []byte{}) }},
		{"tag unknown", false, func(tx *bolt.Tx) error {
			return tx.Bucket(metadataBucket).Put(environmentStorageFormatKey, []byte("other"))
		}},
		{"tag trailing", false, func(tx *bolt.Tx) error {
			return tx.Bucket(metadataBucket).Put(environmentStorageFormatKey, []byte(environmentStorageFormat+"\x00"))
		}},
		{"tag bucket", false, func(tx *bolt.Tx) error {
			m := tx.Bucket(metadataBucket)
			if err := m.Delete(environmentStorageFormatKey); err != nil {
				return err
			}
			_, err := m.CreateBucket(environmentStorageFormatKey)
			return err
		}},
		{"services empty", false, func(tx *bolt.Tx) error { _, err := tx.CreateBucket([]byte("services")); return err }},
		{"compat empty", false, func(tx *bolt.Tx) error { _, err := tx.CreateBucket([]byte("compat_requests")); return err }},
		{"foreign family and missing environment", false, func(tx *bolt.Tx) error {
			if _, err := tx.CreateBucket([]byte("services")); err != nil {
				return err
			}
			return tx.DeleteBucket(environmentsBucket)
		}},
	}
	for _, artifacts := range []bool{false, true} {
		for _, n := range []uint64{environmentArtifactSchema, environmentRetirementSchema, servingSchema} {
			for _, tt := range cases {
				if tt.schema13 && n != servingSchema {
					continue
				}
				t.Run(fmt.Sprintf("artifacts=%v/schema=%d/%s", artifacts, n, tt.name), func(t *testing.T) {
					dir := formatFamilyFixture(t, artifacts)
					editFixture(t, dir, false, func(tx *bolt.Tx) error {
						if n < servingSchema {
							if err := servingStripModel(tx); err != nil {
								return err
							}
						}
						if err := tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(n)); err != nil {
							return err
						}
						return tt.edit(tx)
					})
					formatFamilyRefused(t, dir, tt.name == markerBucket)
				})
			}
		}
	}
}

func formatFamilyFixture(t *testing.T, artifacts bool) string {
	t.Helper()
	var dir string
	if artifacts {
		dir = formatArtifactPopulated(t)
	} else {
		dir, _, _ = environmentPopulated(t)
	}
	// MODEL: these format-family cases predate schema14. Remove only the
	// empty successor facts before projecting their requested older ordinal.
	editFixture(t, dir, false, func(tx *bolt.Tx) error {
		if err := artifactBeginStripModel(tx); err != nil {
			return err
		}
		return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(servingSchema))
	})
	return dir
}

// formatFamilyRefused checks that every reader refuses dir without writing.
// A known numeric schema reports false support; an unreadable schema marker
// (markerInvalid) fails physical inspection with support unknown.
func formatFamilyRefused(t *testing.T, dir string, markerInvalid bool) {
	t.Helper()
	path := filepath.Join(dir, "jobs.db")
	before := fileSum(t, path)
	if s, err := Open(dir); err == nil {
		s.Close()
		t.Fatal("Open accepted invalid family")
	}
	for _, tt := range []struct {
		name     string
		inspect  func(context.Context, string) (PhysicalInspection, error)
		geometry string
	}{
		{"InspectPhysicalState", InspectPhysicalState, "incomplete"},
		{"ProbePhysicalState", ProbePhysicalState, "not_requested"},
	} {
		got, err := tt.inspect(context.Background(), dir)
		if markerInvalid {
			if !errors.Is(err, errPhysicalInspection) || got.Reason != "invalid_schema_scalar" || got.SchemaVersion != nil || got.SchemaSupported != nil || got.ProbeComplete || got.GeometryStatus != tt.geometry {
				t.Fatalf("%s: invalid schema marker: %+v, %v", tt.name, got, err)
			}
		} else if err == nil || got.SchemaSupported == nil || *got.SchemaSupported {
			t.Fatalf("%s: inspection accepted family: %+v, %v", tt.name, got, err)
		}
		if fileSum(t, path) != before {
			t.Fatalf("%s changed jobs.db", tt.name)
		}
	}
	db, err := openSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	var data bytes.Buffer
	err = snapshot(db, &data)
	if cerr := db.Close(); cerr != nil {
		t.Fatal(cerr)
	}
	if err == nil || data.Len() != 0 {
		t.Fatalf("snapshot accepted invalid family: %v, %d bytes", err, data.Len())
	}
	out := t.TempDir()
	if err := Backup(dir, filepath.Join(out, "backup.db")); err == nil {
		t.Fatal("Backup accepted invalid family")
	}
	if err := Restore(path, filepath.Join(out, "restored")); err == nil {
		t.Fatal("Restore accepted invalid family")
	}
	if names := dirNames(t, out); len(names) != 0 {
		t.Fatalf("published outputs: %v", names)
	}
	if fileSum(t, path) != before {
		t.Fatal("refusal changed jobs.db")
	}
}

// TestStorageFormatFamilyControls checks the exact gates without opening or
// migrating the modeled legacy database. In particular published schema9
// needs environments but must not be required to have worker_groups; an
// entirely empty transaction is the valid uninitialized ordinal zero.
func TestStorageFormatFamilyControls(t *testing.T) {
	for _, artifacts := range []bool{false, true} {
		for _, n := range []uint64{8, 9, 10, 11, 12, servingSchema} {
			t.Run(fmt.Sprintf("artifacts=%v/schema=%d", artifacts, n), func(t *testing.T) {
				dir := formatFamilyFixture(t, artifacts)
				editFixture(t, dir, false, func(tx *bolt.Tx) error {
					if n < servingSchema {
						if err := servingStripModel(tx); err != nil {
							return err
						}
					}
					if n < environmentArtifactSchema {
						if err := environmentArtifactStripModel(tx); err != nil {
							return err
						}
					}
					if n < workerGroupSchema {
						if err := tx.DeleteBucket(workerGroupsBucket); err != nil {
							return err
						}
					}
					if n < 9 {
						if err := tx.DeleteBucket(environmentsBucket); err != nil {
							return err
						}
					}
					return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(n))
				})
				path := filepath.Join(dir, "jobs.db")
				before := fileSum(t, path)
				for _, inspect := range []func(context.Context, string) (PhysicalInspection, error){InspectPhysicalState, ProbePhysicalState} {
					got, err := inspect(context.Background(), dir)
					if err != nil || got.SchemaSupported == nil || !*got.SchemaSupported {
						t.Fatalf("valid family: %+v, %v", got, err)
					}
				}
				if fileSum(t, path) != before {
					t.Fatal("inspection changed jobs.db")
				}
			})
		}
	}
	path := filepath.Join(t.TempDir(), "empty.db")
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(tx *bolt.Tx) error { return checkFixedStorageFormat(tx, 0) }); err != nil {
		t.Fatalf("fresh database: %v", err)
	}
}

func TestStorageFormatEarlyFamilies(t *testing.T) {
	for _, artifacts := range []bool{false, true} {
		for _, tt := range []struct {
			name string
			n    uint64
		}{{"environment", 8}, {"worker", 9}} {
			t.Run(fmt.Sprintf("artifacts=%v/%s", artifacts, tt.name), func(t *testing.T) {
				dir := formatFamilyFixture(t, artifacts)
				editFixture(t, dir, false, func(tx *bolt.Tx) error {
					if err := environmentArtifactStripModel(tx); err != nil {
						return err
					}
					if tt.n == 8 {
						if err := tx.DeleteBucket(workerGroupsBucket); err != nil {
							return err
						}
					}
					return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(tt.n))
				})
				formatFamilyRefused(t, dir, false)
			})
		}
	}
}
