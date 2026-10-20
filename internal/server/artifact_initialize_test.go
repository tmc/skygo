//go:build linux || darwin

package server

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/store"
)

func initializerDirectories(t *testing.T) (string, string) {
	t.Helper()
	base := t.TempDir()
	root := filepath.Join(base, "artifacts")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(base, "state"), root
}

func initializerEmpty(t *testing.T, root string) {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("root changed: %v, %v", entries, err)
	}
}

func TestInitializeArtifactsReadback(t *testing.T) {
	data, name := initializerDirectories(t)
	if err := InitializeArtifacts(data, name); err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(data)
	if err != nil {
		t.Fatal("initializer retained lock after success", err)
	}
	installed, err := s.ArtifactInstallation()
	if err != nil || !installed.Enabled || len(installed.ID) != 32 {
		t.Fatal(installed, err)
	}
	root, err := os.OpenRoot(name)
	if err != nil {
		t.Fatal(err)
	}
	info, err := root.Lstat(".")
	if err != nil {
		t.Fatal(err)
	}
	id, err := artifactInitializerIdentity(info)
	if err != nil {
		t.Fatal(err)
	}
	files, err := artifact.Open(root, id)
	if err != nil {
		t.Fatal(err)
	}
	facts, err := files.RootFacts()
	if err != nil || facts.ID != installed.ID || facts.Unit != installed.Unit || facts.Volume != installed.Volume {
		t.Fatal(facts, installed, err)
	}
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestInitializeArtifactsPriorInstallationRefusesBeforeRoot(t *testing.T) {
	data, name := initializerDirectories(t)
	if err := InitializeArtifacts(data, name); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(name, ".skygo-artifact-root")
	before, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(filepath.Dir(name), "other")
	if err := os.Mkdir(other, 0700); err != nil {
		t.Fatal(err)
	}
	if err := InitializeArtifacts(data, other); !errors.Is(err, store.ErrArtifactConflict) {
		t.Fatal("existing installation adopted", err)
	}
	initializerEmpty(t, other)
	after, err := os.ReadFile(marker)
	if err != nil || string(before) != string(after) {
		t.Fatal("original marker changed", err)
	}
}

func TestInitializeArtifactsRootRefusals(t *testing.T) {
	for _, test := range []string{"nonempty", "marker", "public", "symlink", "missing"} {
		t.Run(test, func(t *testing.T) {
			data, root := initializerDirectories(t)
			actual := root
			switch test {
			case "nonempty":
				if err := os.WriteFile(filepath.Join(root, "original"), []byte("preserved"), 0600); err != nil {
					t.Fatal(err)
				}
			case "marker":
				if err := os.WriteFile(filepath.Join(root, ".skygo-artifact-root"), []byte("foreign marker"), 0600); err != nil {
					t.Fatal(err)
				}
			case "public":
				if err := os.Chmod(root, 0755); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				actual = root + "-link"
				if err := os.Symlink(root, actual); err != nil {
					t.Fatal(err)
				}
			case "missing":
				actual = root + "-absent"
			}
			err := InitializeArtifacts(data, actual)
			if err == nil || strings.Contains(err.Error(), root) || strings.Contains(err.Error(), data) {
				t.Fatalf("refusal or redaction failed: %v", err)
			}
			s, err := store.Open(data)
			if err != nil {
				t.Fatal("ordinary refusal leaked lock", err)
			}
			installed, err := s.ArtifactInstallation()
			if err != nil || installed.Enabled {
				t.Fatal("refusal installed metadata", installed, err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if test == "nonempty" {
				b, err := os.ReadFile(filepath.Join(root, "original"))
				if err != nil || string(b) != "preserved" {
					t.Fatal("original entry altered", err)
				}
			} else if test == "marker" {
				b, err := os.ReadFile(filepath.Join(root, ".skygo-artifact-root"))
				if err != nil || string(b) != "foreign marker" {
					t.Fatal("marker adopted/altered", err)
				}
			} else {
				initializerEmpty(t, root)
			}
			if test == "missing" {
				if _, err := os.Lstat(actual); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("missing root created", err)
				}
			}
		})
	}
}

func TestInitializeArtifactsRequiresOriginalStoreLock(t *testing.T) {
	data, root := initializerDirectories(t)
	original, err := store.Open(data)
	if err != nil {
		t.Fatal(err)
	}
	defer original.Close()
	if err := InitializeArtifacts(data, root); err == nil {
		t.Fatal("concurrent Store lock bypassed")
	}
	initializerEmpty(t, root)
	installed, err := original.ArtifactInstallation()
	if err != nil || installed.Enabled {
		t.Fatal(installed, err)
	}
}

func TestInitializeArtifactsMarkerPermissionFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses the actual directory write-permission refusal")
	}
	data, root := initializerDirectories(t)
	if err := os.Chmod(root, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(root, 0700) })
	err := InitializeArtifacts(data, root)
	if err == nil {
		t.Fatal("marker write unexpectedly succeeded")
	}
	initializerEmpty(t, root)
	s, err := store.Open(data)
	if err != nil {
		t.Fatal(err)
	}
	installed, err := s.ArtifactInstallation()
	if err != nil || installed.Enabled {
		t.Fatal(installed, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestInitializerBusyFilesRetainsOriginalDependencyFrame(t *testing.T) {
	data, name := initializerDirectories(t)
	if err := InitializeArtifacts(data, name); err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(data)
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(name)
	if err != nil {
		t.Fatal(err)
	}
	info, err := root.Lstat(".")
	if err != nil {
		t.Fatal(err)
	}
	id, err := artifactInitializerIdentity(info)
	if err != nil {
		t.Fatal(err)
	}
	files, err := artifact.Open(root, id)
	if err != nil {
		t.Fatal(err)
	}
	scan, err := files.BeginPhysicalSample()
	if err != nil {
		t.Fatal(err)
	}
	frame := &artifactInitialization{store: s, root: root, files: files}
	original := errors.New("readback failed")
	failure := frame.finish("readback", original)
	var held *ArtifactInitializationError
	if !errors.As(failure, &held) || held.frame != frame || !errors.Is(failure, original) || !errors.Is(failure, artifact.ErrFilesBusy) {
		t.Fatal("original frame/error lost", failure)
	}
	if frame.files != files || frame.root != root || frame.store != s {
		t.Fatal("closed a dependency beneath an original registered scan")
	}
	if err := frame.finish("second close", nil); err != failure {
		t.Fatal("held close boundary was retried", err)
	}
	if _, err := root.Lstat("."); err != nil {
		t.Fatal("borrowed root closed beneath Files", err)
	}
	other, err := store.Open(data)
	if err == nil {
		other.Close()
		t.Fatal("Store lock closed beneath Files")
	}
	// Explicit test-owner handoff: the real scan closes positively first. This
	// is registered-owner pressure, NOT a simulated uncertain kernel Close.
	if err := scan.Close(); err != nil {
		t.Fatal(err)
	}
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestInitializerInvalidStoreNeverOpensArtifactRoot(t *testing.T) {
	data, root := initializerDirectories(t)
	if err := os.Mkdir(data, 0700); err != nil {
		t.Fatal(err)
	}
	bad := []byte(strings.Repeat("invalid database", 1024))
	path := filepath.Join(data, "jobs.db")
	if err := os.WriteFile(path, bad, 0600); err != nil {
		t.Fatal(err)
	}
	if err := InitializeArtifacts(data, root); err == nil {
		t.Fatal("invalid database accepted")
	}
	initializerEmpty(t, root)
	after, err := os.ReadFile(path)
	if err != nil || string(after) != string(bad) {
		t.Fatal("invalid original database changed", err)
	}
}
