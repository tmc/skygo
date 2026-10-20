package store

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// These source tests model trusted Files facts only. Concrete root marker and
// filesystem qualification belong to Files integration; no test here proves it.
func TestInstallArtifactsStopped(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	j, err := s.Submit(testActor(), "preserved", backend.Task{Argv: []string{"true"}}, "local", "test-binding")
	if err != nil {
		t.Fatal(err)
	}
	var row []byte
	if err := s.db.View(func(tx *bolt.Tx) error { row = bytes.Clone(tx.Bucket(jobsBucket).Get([]byte(j.ID))); return nil }); err != nil {
		t.Fatal(err)
	}
	facts := ArtifactInstallation{Enabled: true, ID: strings.Repeat("a", 32), Unit: 4096, Volume: 17}
	if _, err := InstallArtifacts(dir, facts); err == nil {
		t.Fatal("installed while server held exclusive lock")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		got, err := InstallArtifacts(dir, facts)
		if err != nil || got != facts {
			t.Fatalf("install %d: %v %v", i, got, err)
		}
	}
	wrong := facts
	wrong.ID = strings.Repeat("b", 32)
	if _, err := InstallArtifacts(dir, wrong); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("rebind: %v", err)
	}
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.ArtifactInstallation()
	if err != nil || got != facts {
		t.Fatalf("readback: %v %v", got, err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		if !bytes.Equal(row, tx.Bucket(jobsBucket).Get([]byte(j.ID))) {
			t.Fatal("accepted row changed")
		}
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		_, usage, _, e := st.encode()
		if e != nil {
			return e
		}
		if usage[aFiles] != 2 || usage[aAllocatedBytes] != 2*facts.Unit {
			t.Fatalf("root reservations: %v", usage)
		}
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}
}

func TestInstallArtifactsInvalidFactsDoNotCreate(t *testing.T) {
	base := ArtifactInstallation{Enabled: true, ID: strings.Repeat("a", 32), Unit: 4096, Volume: 17}
	for _, tt := range []struct {
		name   string
		change func(*ArtifactInstallation)
	}{
		{"disabled", func(f *ArtifactInstallation) { f.Enabled = false }},
		{"identity", func(f *ArtifactInstallation) { f.ID = "invalid" }},
		{"unit", func(f *ArtifactInstallation) { f.Unit = 513 }},
		{"volume", func(f *ArtifactInstallation) { f.Volume = 0 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			facts := base
			tt.change(&facts)
			dir := filepath.Join(t.TempDir(), "absent")
			if _, err := InstallArtifacts(dir, facts); err == nil {
				t.Fatal("invalid facts accepted")
			}
			if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid facts created directory: %v", err)
			}
		})
	}
}
