package store

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

func TestBackupRestorePreservesOwnerRequestAndSequence(t *testing.T) {
	base := t.TempDir()
	source := filepath.Join(base, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	want := legacyFixture(t, source, false)
	snapshot := filepath.Join(base, "snapshot.db")
	if err := Backup(source, snapshot); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(base, "restored")
	if err := Restore(snapshot, restored); err != nil {
		t.Fatal(err)
	}
	s, err := Open(restored)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.Owner() != "legacy-owner" {
		t.Fatal("owner changed")
	}
	got, err := s.Submit(testActor(), "original-request", want.Task, "new-target", "test-binding")
	if err != nil || got.ID != want.ID || got.Sequence != 1 || got.Target != want.Target {
		t.Fatalf("restored retry: %+v %v", got, err)
	}
	next, err := s.Submit(testActor(), "next", backend.Task{Argv: []string{"next"}}, "local", "test-binding")
	if err != nil || next.Sequence != 2 {
		t.Fatalf("sequence not preserved: %+v %v", next, err)
	}
	if info, err := os.Stat(snapshot); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("snapshot mode: %v %v", info, err)
	}
}

func TestSnapshotRefusesWriterAndExistingPaths(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "source")
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(base, "snapshot.db")
	if err := Backup(dir, snapshot); err == nil {
		t.Error("backup accepted live writer")
	}
	if _, err := os.Stat(snapshot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed backup created output: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := Backup(dir, snapshot); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(snapshot)
	if err := Backup(dir, snapshot); err == nil {
		t.Fatal("backup overwrote output")
	}
	after, _ := os.ReadFile(snapshot)
	if !bytes.Equal(before, after) {
		t.Fatal("existing backup changed")
	}
	if err := Restore(snapshot, dir); err == nil {
		t.Fatal("restore accepted existing directory")
	}
	entries, err := os.ReadDir(base)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if len(entry.Name()) > 0 && entry.Name()[0] == '.' {
			t.Errorf("temporary file leaked: %s", entry.Name())
		}
	}
}

func TestMissingSnapshotSourcesNotCreated(t *testing.T) {
	base := t.TempDir()
	missing := filepath.Join(base, "missing.db")
	dir := filepath.Join(base, "restored")
	if err := Restore(missing, dir); err == nil {
		t.Fatal("missing restore input accepted")
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("restore created input: %v", err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid restore created directory: %v", err)
	}
	source := filepath.Join(base, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := Backup(source, filepath.Join(base, "out.db")); err == nil {
		t.Fatal("missing source accepted")
	}
	if _, err := os.Stat(filepath.Join(source, "jobs.db")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("backup created source: %v", err)
	}
}

func TestActiveSnapshotPreservesIntent(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "source")
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	j, err := s.Submit(testActor(), "active", backend.Task{Argv: []string{"work"}}, "local", "test-binding")
	if err != nil {
		t.Fatal(err)
	}
	j, err = s.Update(j.ID, func(j *backend.Job) error { j.Status.State = backend.Running; j.CancelRequested = true; return nil })
	if err != nil {
		t.Fatal(err)
	}
	owner := s.Owner()
	s.Close()
	snap := filepath.Join(base, "snapshot.db")
	if err := Backup(dir, snap); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(base, "restored")
	if err := Restore(snap, restored); err != nil {
		t.Fatal(err)
	}
	s, err = Open(restored)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.Get(j.ID)
	if err != nil || got.Status.State != backend.Running || !got.CancelRequested || got.UpdatedAt != j.UpdatedAt || s.Owner() != owner {
		t.Fatalf("active intent changed: %+v %v", got, err)
	}
}

func TestRestoreRejectsMalformedSnapshot(t *testing.T) {
	base := t.TempDir()
	source := filepath.Join(base, "source")
	os.Mkdir(source, 0700)
	j := legacyFixture(t, source, false)
	editFixture(t, source, false, func(tx *bolt.Tx) error {
		return tx.Bucket(requestsBucket).Put([]byte("orphan"), []byte(j.ID+"missing"))
	})
	dest := filepath.Join(base, "restore")
	if err := Restore(filepath.Join(source, "jobs.db"), dest); err == nil {
		t.Fatal("orphan request restored")
	}
	if _, err := os.Stat(dest); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid snapshot created destination: %v", err)
	}
}
