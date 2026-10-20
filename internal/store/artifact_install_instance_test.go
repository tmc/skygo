package store

import (
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/backend"
)

// These tests exercise the retained Store entrypoint with trusted metadata facts.
// They do not initialize a physical root or prove marker/writer ownership.
func TestStoreInstallArtifactsRetainsOpenStore(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	job, err := s.Submit(testActor(), "before-install", backend.Task{Argv: []string{"true"}}, "local", "test-binding")
	if err != nil {
		t.Fatal(err)
	}
	facts := ArtifactInstallation{Enabled: true, ID: strings.Repeat("a", 32), Unit: 4096, Volume: 17}
	for i := 0; i < 2; i++ {
		got, err := s.InstallArtifacts(facts)
		if err != nil || got != facts {
			t.Fatalf("install %d: got %v, error %v", i, got, err)
		}
	}
	got, err := s.ArtifactInstallation()
	if err != nil || got != facts {
		t.Fatalf("read installation: got %v, error %v", got, err)
	}
	current, err := s.Get(job.ID)
	if err != nil || !reflect.DeepEqual(current, job) {
		t.Fatalf("existing job changed: got %v, error %v", current, err)
	}
	// The instance method must leave this same Store usable, not close/reopen it.
	if _, err := s.Submit(testActor(), "after-install", backend.Task{Argv: []string{"true"}}, "local", "test-binding"); err != nil {
		t.Fatalf("submit through original Store after install: %v", err)
	}
}

func TestStoreInstallArtifactsRejectsRebind(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	facts := ArtifactInstallation{Enabled: true, ID: strings.Repeat("a", 32), Unit: 4096, Volume: 17}
	if _, err := s.InstallArtifacts(facts); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name   string
		change func(*ArtifactInstallation)
	}{
		{"identity", func(f *ArtifactInstallation) { f.ID = strings.Repeat("b", 32) }},
		{"unit", func(f *ArtifactInstallation) { f.Unit = 8192 }},
		{"volume", func(f *ArtifactInstallation) { f.Volume = 18 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			wrong := facts
			tt.change(&wrong)
			got, err := s.InstallArtifacts(wrong)
			if !errors.Is(err, ErrArtifactConflict) || got != (ArtifactInstallation{}) {
				t.Fatalf("rebind: got %v, error %v", got, err)
			}
			got, err = s.ArtifactInstallation()
			if err != nil || got != facts {
				t.Fatalf("rebind changed installation: got %v, error %v", got, err)
			}
		})
	}
}

func TestStoreInstallArtifactsInvalidPreservesDormantState(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	job, err := s.Submit(testActor(), "preserved", backend.Task{Argv: []string{"true"}}, "local", "test-binding")
	if err != nil {
		t.Fatal(err)
	}
	base := ArtifactInstallation{Enabled: true, ID: strings.Repeat("a", 32), Unit: 4096, Volume: 17}
	for _, tt := range []struct {
		name   string
		change func(*ArtifactInstallation)
	}{
		{"disabled", func(f *ArtifactInstallation) { f.Enabled = false }},
		{"identity", func(f *ArtifactInstallation) { f.ID = strings.Repeat("A", 32) }},
		{"short identity", func(f *ArtifactInstallation) { f.ID = strings.Repeat("a", 31) }},
		{"small unit", func(f *ArtifactInstallation) { f.Unit = 256 }},
		{"large unit", func(f *ArtifactInstallation) { f.Unit = 131072 }},
		{"unaligned unit", func(f *ArtifactInstallation) { f.Unit = 513 }},
		{"volume", func(f *ArtifactInstallation) { f.Volume = 0 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			facts := base
			tt.change(&facts)
			got, err := s.InstallArtifacts(facts)
			if err == nil || got != (ArtifactInstallation{}) {
				t.Fatalf("invalid install: got %v, error %v", got, err)
			}
			got, err = s.ArtifactInstallation()
			if err != nil || got != (ArtifactInstallation{}) {
				t.Fatalf("dormant installation changed: got %v, error %v", got, err)
			}
			current, err := s.Get(job.ID)
			if err != nil || !reflect.DeepEqual(current, job) {
				t.Fatalf("accepted job changed: got %v, error %v", current, err)
			}
		})
	}
}
