package store_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/server"
	"github.com/tmc/skygo/internal/store"
)

// The source has a real private marker and installed Store facts, not a raw DB
// fixture or invented device/inode. No upload, download or workload is started.
func TestRestoreRefusesArtifactEnabledSnapshot(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("artifact initialization requires Linux or Darwin")
	}
	for _, retained := range []bool{false, true} {
		t.Run(map[bool]string{false: "retention_disabled", true: "retention_enabled"}[retained], func(t *testing.T) {
			base := t.TempDir()
			source, root := filepath.Join(base, "source"), filepath.Join(base, "artifacts")
			var domain store.RequestDomain
			if retained {
				policy := store.DefaultRetentionPolicy()
				policy.LocalOutputEnabled = false
				info, err := store.EnableRetention(source, policy)
				if err != nil {
					t.Fatal(err)
				}
				domain = info.Domain
			}
			if err := os.Mkdir(root, 0700); err != nil {
				t.Fatal(err)
			}
			if err := server.InitializeArtifacts(source, root); err != nil {
				t.Fatal(err)
			}
			s, err := store.Open(source)
			if err != nil {
				t.Fatal(err)
			}
			owner := s.Owner()
			installed, readErr := s.ArtifactInstallation()
			closeErr := s.Close()
			if readErr != nil || closeErr != nil || !installed.Enabled {
				t.Fatalf("initialized source: %+v, read=%v close=%v", installed, readErr, closeErr)
			}
			database := filepath.Join(source, "jobs.db")
			marker := filepath.Join(root, ".skygo-artifact-root")
			beforeDB, beforeMarker := restoreFenceReadFile(t, database), restoreFenceReadFile(t, marker)
			snapshot := filepath.Join(base, "snapshot.db")
			if err := store.Backup(source, snapshot); err != nil {
				t.Fatal(err)
			}
			beforeSnapshot := restoreFenceReadFile(t, snapshot)
			destination := filepath.Join(base, "restored")
			err = store.Restore(snapshot, destination)
			if err == nil || !strings.Contains(err.Error(), "database-only restore of artifact-enabled state is unsupported") {
				t.Fatalf("artifact restore error=%v", err)
			}
			if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("refusal created destination: %v", err)
			}
			for path, want := range map[string][]byte{database: beforeDB, marker: beforeMarker, snapshot: beforeSnapshot} {
				if got := restoreFenceReadFile(t, path); !bytes.Equal(got, want) {
					t.Fatalf("refusal changed %s", filepath.Base(path))
				}
			}
			entries, err := os.ReadDir(base)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if strings.HasPrefix(entry.Name(), ".skygo-snapshot-") {
					t.Fatalf("refusal left temporary snapshot %s", entry.Name())
				}
			}
			// Open still accepts the actual installed source. The restore fence
			// must not become a generic schema, Open or Backup prohibition.
			s, err = store.Open(source)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			}()
			got, err := s.ArtifactInstallation()
			if err != nil || got != installed || s.Owner() != owner {
				t.Fatalf("source installation changed: %+v %v", got, err)
			}
			if retained {
				info, err := s.RetentionInfo(testActor())
				if err != nil || info.Domain != domain {
					t.Fatalf("refusal freshened source lineage: %+v %v", info, err)
				}
			}
		})
	}
}

func TestRestoreDormantArtifactState(t *testing.T) {
	for _, retained := range []bool{false, true} {
		t.Run(map[bool]string{false: "retention_disabled", true: "retention_enabled"}[retained], func(t *testing.T) {
			base := t.TempDir()
			source := filepath.Join(base, "source")
			var domain store.RequestDomain
			if retained {
				policy := store.DefaultRetentionPolicy()
				policy.LocalOutputEnabled = false
				info, err := store.EnableRetention(source, policy)
				if err != nil {
					t.Fatal(err)
				}
				domain = info.Domain
			}
			s, err := store.Open(source)
			if err != nil {
				t.Fatal(err)
			}
			owner := s.Owner()
			task := backend.Task{Argv: []string{"never-started"}}
			var accepted backend.Job
			if retained {
				accepted, err = s.SubmitRetained(testActor(), domain, "original", task, "remote", "test-binding", backend.RemoteOutput, 0)
			} else {
				accepted, err = s.Submit(testActor(), "original", task, "remote", "test-binding")
			}
			closeErr := s.Close()
			if err != nil || closeErr != nil {
				t.Fatalf("source admission: %v close=%v", err, closeErr)
			}
			snapshot := filepath.Join(base, "snapshot.db")
			if err := store.Backup(source, snapshot); err != nil {
				t.Fatal(err)
			}
			beforeSnapshot := restoreFenceReadFile(t, snapshot)
			destination := filepath.Join(base, "restored")
			if err := store.Restore(snapshot, destination); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(restoreFenceReadFile(t, snapshot), beforeSnapshot) {
				t.Fatal("restore changed source snapshot")
			}
			s, err = store.Open(destination)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			}()
			installed, err := s.ArtifactInstallation()
			if err != nil || installed.Enabled || s.Owner() != owner {
				t.Fatalf("dormant installation changed: %+v %v", installed, err)
			}
			got, err := s.Get(accepted.ID)
			if err != nil || !reflect.DeepEqual(got, accepted) {
				t.Fatalf("restored job changed: %+v %v", got, err)
			}
			if retained {
				info, err := s.RetentionInfo(testActor())
				if err != nil || info.Domain.Lineage == domain.Lineage || info.LegacyReplayOpen {
					t.Fatalf("restore did not freshen lineage: %+v %v", info, err)
				}
				_, _, err = s.LookupRetained(testActor(), domain, "original", task)
				var expired *store.ExpiryError
				if !errors.As(err, &expired) || expired.Reason != store.RequestLineageChanged {
					t.Fatalf("old lineage accepted after restore: %v", err)
				}
			} else {
				replayed, found, err := s.Lookup(testActor(), "original", task)
				if err != nil || !found || replayed.ID != accepted.ID {
					t.Fatalf("legacy replay changed: %+v %v %v", replayed, found, err)
				}
			}
		})
	}
}

func restoreFenceReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
