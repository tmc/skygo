package store

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type formatReader struct {
	name    string
	inspect func(context.Context, string) (PhysicalInspection, error)
}

// formatReaders returns the two physical readers, both of which must decide
// format support without writing.
func formatReaders() []formatReader {
	return []formatReader{
		{"InspectPhysicalState", InspectPhysicalState},
		{"ProbePhysicalState", ProbePhysicalState},
	}
}

// TestStorageFormatGenuineForeignRefusedEverywhere runs every reader against
// a private clone of a schema12 jobs.db whose bytes and schema were written
// by the genuine Store of the foreign storage line, not modeled here. It runs
// only when the SKYGO_FOREIGN_DB pair names one. Open, Backup, Restore and
// snapshot must refuse it as ErrUnsupportedSchema without output, both
// physical readers must report the format unsupported, and after each
// operation the clone and the source must equal the source bytes.
//
// Boundary: only the fixture's bytes, schema ordinal and record codecs are
// GENUINE old-Store output. Any artifact geometry or service demand the
// fixture's history implies is MODELED, not reproduced. The test makes no
// claim about native Kubernetes, services, compat14, ENV13 or read leases,
// nor about which foreign services, artifacts or reads the fixture holds.
func TestStorageFormatGenuineForeignRefusedEverywhere(t *testing.T) {
	src, data := genuineFixture(t, "SKYGO_FOREIGN_DB")
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "jobs.db")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	preserved := func(op string) {
		t.Helper()
		if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, data) {
			t.Fatalf("%s changed jobs.db: %v", op, err)
		}
		if got, err := os.ReadFile(src); err != nil || !bytes.Equal(got, data) {
			t.Fatalf("%s changed the fixture: %v", op, err)
		}
	}
	preserved("clone")

	s, err := Open(dir)
	if s != nil {
		if closeErr := s.Close(); closeErr != nil {
			t.Errorf("close accepted store: %v", closeErr)
		}
	}
	preserved("Open")
	if s != nil || !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("Open: store %v, %v", s != nil, err)
	}

	out := t.TempDir()
	err = Backup(dir, filepath.Join(out, "backup.db"))
	names := dirNames(t, out)
	preserved("Backup")
	if !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("Backup: %v", err)
	}
	if len(names) != 0 {
		t.Fatalf("Backup left %v", names)
	}

	parent := t.TempDir()
	err = Restore(path, filepath.Join(parent, "restored"))
	names = dirNames(t, parent)
	preserved("Restore")
	if !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("Restore: %v", err)
	}
	if len(names) != 0 {
		t.Fatalf("Restore left %v", names)
	}

	db, err := openSnapshot(path)
	if err != nil {
		preserved("openSnapshot")
		t.Fatalf("openSnapshot: %v", err)
	}
	var buf bytes.Buffer
	err = snapshot(db, &buf)
	closeErr := db.Close()
	preserved("snapshot")
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if !errors.Is(err, ErrUnsupportedSchema) || buf.Len() != 0 {
		t.Fatalf("snapshot: %v, %d bytes", err, buf.Len())
	}

	for _, r := range formatReaders() {
		got, err := r.inspect(context.Background(), dir)
		preserved(r.name)
		if err == nil || got.SchemaSupported == nil || *got.SchemaSupported || got.Reason != "unsupported_storage_format" {
			t.Fatalf("%s: %v supported %v reason %q", r.name, err, got.SchemaSupported, got.Reason)
		}
		if got.SchemaVersion == nil || *got.SchemaVersion != environmentRetirementSchema {
			t.Fatalf("%s: schema %v, want %d", r.name, got.SchemaVersion, environmentRetirementSchema)
		}
	}
}

// TestStorageFormatCurrentReadersPositive is the control for
// TestStorageFormatGenuineForeignRefusedEverywhere: both physical readers,
// snapshot and Backup accept a current Store installation, with and without
// installed artifacts, so the refusals are not blanket refusals. Restore
// publishes and reopens the plain installation with its identity. For the
// artifact-enabled installation, database-only Restore must refuse with its
// documented companion-recovery error, which is a distinct constraint on
// valid state and not ErrUnsupportedSchema.
//
// Boundary: environmentPopulated and formatArtifactPopulated build actual
// current Store databases, but their legacy environment records and
// artifact geometry are MODELED, not genuine old-Store output. No native
// Kubernetes, service, compat14, ENV13 or read-lease behavior is claimed.
func TestStorageFormatCurrentReadersPositive(t *testing.T) {
	for _, tt := range []struct {
		name      string
		fixture   func(*testing.T) string
		artifacts bool
	}{
		{"plain", func(t *testing.T) string { dir, _, _ := environmentPopulated(t); return dir }, false},
		{"artifacts", formatArtifactPopulated, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := tt.fixture(t)
			path := filepath.Join(dir, "jobs.db")
			owner, envs := formatIdentity(t, dir)
			if owner == "" || envs != 2 {
				t.Fatalf("source owner %q, %d environments", owner, envs)
			}

			before := fileSum(t, path)
			for _, r := range formatReaders() {
				got, err := r.inspect(context.Background(), dir)
				if err != nil || got.SchemaSupported == nil || !*got.SchemaSupported {
					t.Fatalf("%s: %v supported %v reason %q", r.name, err, got.SchemaSupported, got.Reason)
				}
				if got.SchemaVersion == nil || *got.SchemaVersion != schemaVersion {
					t.Fatalf("%s: schema %v", r.name, got.SchemaVersion)
				}
				if fileSum(t, path) != before {
					t.Fatalf("%s changed jobs.db", r.name)
				}
			}

			db, err := openSnapshot(path)
			if err != nil {
				t.Fatal(err)
			}
			var buf bytes.Buffer
			err = snapshot(db, &buf)
			if closeErr := db.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
			if err != nil || buf.Len() == 0 {
				t.Fatalf("snapshot: %v, %d bytes", err, buf.Len())
			}
			if fileSum(t, path) != before {
				t.Fatal("snapshot changed jobs.db")
			}
			direct := filepath.Join(t.TempDir(), "direct.db")
			if err := os.WriteFile(direct, buf.Bytes(), 0600); err != nil {
				t.Fatal(err)
			}
			formatSnapshotValid(t, direct)

			out := t.TempDir()
			backup := filepath.Join(out, "backup.db")
			if err := Backup(dir, backup); err != nil {
				t.Fatalf("Backup: %v", err)
			}
			if names := dirNames(t, out); len(names) != 1 || names[0] != "backup.db" {
				t.Fatalf("Backup left %v", names)
			}
			formatSnapshotValid(t, backup)

			backupData, err := os.ReadFile(backup)
			if err != nil {
				t.Fatal(err)
			}
			parent := t.TempDir()
			restored := filepath.Join(parent, "restored")
			err = Restore(backup, restored)
			names := dirNames(t, parent)
			if got, readErr := os.ReadFile(backup); readErr != nil || !bytes.Equal(got, backupData) {
				t.Fatalf("Restore changed its snapshot: %v", readErr)
			}
			if tt.artifacts {
				// A database-only restore cannot carry the companion artifact
				// root, so Restore refuses valid artifact-enabled state before
				// creating anything; recovery requires the companion.
				const want = "validate restore snapshot: database-only restore of artifact-enabled state is unsupported"
				if err == nil || err.Error() != want || errors.Is(err, ErrUnsupportedSchema) {
					t.Fatalf("Restore: %v, want %q", err, want)
				}
				if len(names) != 0 {
					t.Fatalf("Restore left %v", names)
				}
				return
			}
			if err != nil {
				t.Fatalf("Restore: %v", err)
			}
			if len(names) != 1 || names[0] != "restored" {
				t.Fatalf("Restore left %v", names)
			}
			if tag := formatTag(t, filepath.Join(restored, "jobs.db")); string(tag) != environmentStorageFormat {
				t.Fatalf("restored tag %q", tag)
			}
			for _, r := range formatReaders() {
				got, err := r.inspect(context.Background(), restored)
				if err != nil || got.SchemaSupported == nil || !*got.SchemaSupported {
					t.Fatalf("restored %s: %v supported %v reason %q", r.name, err, got.SchemaSupported, got.Reason)
				}
			}
			if gotOwner, gotEnvs := formatIdentity(t, restored); gotOwner != owner || gotEnvs != envs {
				t.Fatalf("restored owner %q, %d environments; want %q, %d", gotOwner, gotEnvs, owner, envs)
			}
		})
	}
}

// formatIdentity opens the installation in dir and returns its owner and
// number of environments.
func formatIdentity(t *testing.T, dir string) (owner string, environments int) {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	page, err := s.Environments(0, 10)
	owner = s.Owner()
	if closeErr := s.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil {
		t.Fatalf("environments: %v", err)
	}
	return owner, len(page.Environments)
}

// formatSnapshotValid checks that path is a current, tagged bbolt snapshot
// that passes snapshot validation.
func formatSnapshotValid(t *testing.T, path string) {
	t.Helper()
	if tag := formatTag(t, path); string(tag) != environmentStorageFormat {
		t.Fatalf("snapshot tag %q", tag)
	}
	db, err := openSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(validateSnapshot); err != nil {
		t.Fatalf("validate snapshot: %v", err)
	}
}
