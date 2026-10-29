package store

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func compactTemps(t *testing.T, dir string) []string {
	t.Helper()
	names, err := filepath.Glob(filepath.Join(dir, ".skygo-compact-*"))
	if err != nil {
		t.Fatal(err)
	}
	return names
}

func requireCompactSource(t *testing.T, path string, before []byte) {
	t.Helper()
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("source changed: %v", err)
	}
}

func TestCompactClosePositive(t *testing.T) {
	f := newSnapshotFixture(t)
	source := f.source(t)
	path := filepath.Join(source, "jobs.db")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(f.root, "compact.db")
	if err := Compact(source, out); err != nil {
		f.hold(err)
		t.Fatal(err)
	}
	requireCompactSource(t, path, before)
	if names := compactTemps(t, f.root); len(names) != 0 {
		t.Fatalf("temporary files after success: %v", names)
	}
	copy, err := openOwnedSnapshot(out, false)
	if err != nil {
		f.hold(err)
		t.Fatal(err)
	}
	c := newSnapshotCloseCalls()
	validationErr := copy.db.View(func(tx *bolt.Tx) error {
		if err := validateSnapshot(tx); err != nil {
			return err
		}
		if tx.Bucket(jobsBucket).Stats().KeyN != 1 {
			return errors.New("compacted snapshot lost its job")
		}
		return nil
	})
	if err := errors.Join(validationErr, copy.close(c.closes())); err != nil {
		f.hold(err)
		t.Fatal(err)
	}
	c.check(t)
	// Publication refuses an existing name and never replaces its bytes.
	published, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if err := Compact(source, out); !errors.Is(err, os.ErrExist) {
		f.hold(err)
		t.Fatalf("existing output = %v", err)
	}
	requireCompactSource(t, out, published)
	requireCompactSource(t, path, before)
}

// These faults are MODELED returns AFTER the actual original Close succeeded.
// They test retained owners, first-result propagation and I/O fences, not a
// kernel Close failure or a retained native lock.
func TestCompactModeledCloseFailure(t *testing.T) {
	for _, stage := range []string{"source", "destination", "validation", "temporary", "synced-file", "directory"} {
		t.Run(stage, func(t *testing.T) {
			f := newSnapshotFixture(t)
			source := f.source(t)
			path := filepath.Join(source, "jobs.db")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			out := filepath.Join(f.root, "compact.db")
			fault := errors.New("MODELED " + stage + " first Close failure")
			c := newSnapshotCloseCalls()
			var originalDB *bolt.DB
			var originalFile *os.File
			fileCalls := 0
			c.afterDB = func(db *bolt.DB, name string, readonly bool) error {
				if stage == "source" && name == path || stage == "destination" && !readonly || stage == "validation" && readonly && name != path {
					originalDB = db
					return fault
				}
				return nil
			}
			c.afterFile = func(file *os.File, _ string, directory bool) error {
				fileCalls++
				if stage == "temporary" && fileCalls == 1 || stage == "synced-file" && !directory && fileCalls == 2 || stage == "directory" && directory {
					originalFile = file
					return fault
				}
				return nil
			}
			err = compactWithOps(source, out, c.closes())
			f.hold(err)
			held := heldSnapshotError(t, err, fault)
			if originalDB != nil && held.db != originalDB || originalFile != nil && held.file != originalFile {
				t.Fatal("first Close error lost its actual owner")
			}
			c.check(t)
			requireCompactSource(t, path, before)
			wantDB, wantFiles := 3, 1
			switch stage {
			case "temporary":
				wantDB = 1
			case "source", "destination":
				wantDB = 2
			case "synced-file":
				wantFiles = 2
			case "directory":
				wantFiles = 3
			}
			if len(c.dbs) != wantDB || len(c.files) != wantFiles {
				t.Fatalf("operations crossed failed owner boundary: databases=%d files=%d, want %d/%d", len(c.dbs), len(c.files), wantDB, wantFiles)
			}
			if names := compactTemps(t, f.root); len(names) != 1 {
				t.Fatalf("uncertain original did not retain its temporary path: %v", names)
			}
			_, statErr := os.Stat(out)
			if stage == "directory" {
				if statErr != nil {
					t.Fatalf("late Close failure removed published output: %v", statErr)
				}
			} else if !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("output published after uncertain Close: %v", statErr)
			}
		})
	}
}

func TestCompactPrimaryAndCloseFailures(t *testing.T) {
	for _, stage := range []string{"source-validation", "compaction", "completed-validation"} {
		t.Run(stage, func(t *testing.T) {
			f := newSnapshotFixture(t)
			source := f.source(t)
			path := filepath.Join(source, "jobs.db")
			if stage == "source-validation" {
				if err := corruptSnapshotFormat(path); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			c := newSnapshotCloseCalls()
			sourceFault := errors.New("MODELED source Close failure")
			destFault := errors.New("MODELED destination Close failure")
			validationFault := errors.New("MODELED validation Close failure")
			c.afterDB = func(_ *bolt.DB, name string, readonly bool) error {
				if name == path && stage != "completed-validation" {
					return sourceFault
				}
				if !readonly {
					if stage == "compaction" {
						return destFault
					}
					if stage == "completed-validation" {
						return corruptSnapshotFormat(name)
					}
				}
				if readonly && name != path && stage == "completed-validation" {
					return validationFault
				}
				return nil
			}
			c.afterFile = func(_ *os.File, name string, _ bool) error {
				if stage != "compaction" || !strings.Contains(filepath.Base(name), ".skygo-compact-") {
					return nil
				}
				// Populate the closed temp with an existing root bucket. Real
				// bolt.Compact then refuses its duplicate, without a fake copier.
				db, err := bolt.Open(name, 0600, &bolt.Options{Timeout: time.Second})
				if err != nil {
					return err
				}
				err = db.Update(func(tx *bolt.Tx) error { _, err := tx.CreateBucket(metadataBucket); return err })
				return errors.Join(err, db.Close())
			}
			err = compactWithOps(source, filepath.Join(f.root, "compact.db"), c.closes())
			f.hold(err)
			switch stage {
			case "source-validation":
				heldSnapshotError(t, err, sourceFault)
				if !strings.Contains(err.Error(), "format") {
					t.Fatalf("lost validation cause: %v", err)
				}
			case "compaction":
				heldSnapshotError(t, err, sourceFault)
				if !errors.Is(err, destFault) || !errors.Is(err, bolt.ErrBucketExists) {
					t.Fatalf("lost compaction/Close causes: %v", err)
				}
			case "completed-validation":
				heldSnapshotError(t, err, validationFault)
				if !strings.Contains(err.Error(), "format") {
					t.Fatalf("lost completed validation cause: %v", err)
				}
			}
			c.check(t)
			requireCompactSource(t, path, before)
			if _, err := os.Stat(filepath.Join(f.root, "compact.db")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed compact published: %v", err)
			}
			wantTemps := 1
			if stage == "source-validation" {
				wantTemps = 0
			}
			if names := compactTemps(t, f.root); len(names) != wantTemps {
				t.Fatalf("retained temps = %v, want %d", names, wantTemps)
			}
		})
	}
}
