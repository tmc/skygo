//go:build linux || darwin

package store

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// existingEntry is one path's mode, inode and content digest.
type existingEntry struct {
	mode  fs.FileMode
	inode uint64
	sum   [32]byte
}

// existingTree records every path below root, so a refusal can be shown to
// leave the database, its directory and bystanders byte, mode and inode exact.
func existingTree(t *testing.T, root string) map[string]existingEntry {
	t.Helper()
	out := make(map[string]existingEntry)
	err := filepath.WalkDir(root, func(path string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		e := existingEntry{mode: info.Mode(), inode: uint64(info.Sys().(*syscall.Stat_t).Ino)}
		if info.Mode().IsRegular() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			e.sum = sha256.Sum256(data)
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		out[rel] = e
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// currentInstallation writes an actual current scoped installation with the
// ordinary Store in base/state, beside a bystander file.
func currentInstallation(t *testing.T) (base, dir, owner string) {
	t.Helper()
	base = t.TempDir()
	if err := os.Chmod(base, 0700); err != nil {
		t.Fatal(err)
	}
	dir = filepath.Join(base, "state")
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	owner = s.Owner()
	if err := s.ActivateScoped(); err != nil {
		s.Close()
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "bystander"), []byte("bystander\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return base, dir, owner
}

// modelEdit applies a MODELED corruption to an actual Store database. It is
// not a genuine database written by an earlier Store.
func modelEdit(t *testing.T, dir string, edit func(*bolt.Tx) error) {
	t.Helper()
	db, err := bolt.Open(filepath.Join(dir, "jobs.db"), 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(edit)
	if closeErr := db.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func modelMetadata(key []byte, value []byte) func(*bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		m := tx.Bucket(metadataBucket)
		if value == nil {
			return m.Delete(key)
		}
		return m.Put(key, value)
	}
}

func copyFile(t *testing.T, from, to string) {
	t.Helper()
	data, err := os.ReadFile(from)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(to, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func scopedTestActor(t *testing.T, owner string) auth.Actor {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	config, token := filepath.Join(dir, "auth.json"), filepath.Join(dir, "legacy.token")
	if _, err := auth.Initialize(config, token, owner); err != nil {
		t.Fatal(err)
	}
	e, err := auth.NewFile(config, owner)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(token)
	if err != nil {
		t.Fatal(err)
	}
	actor, err := e.Authenticate("Bearer " + strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	return actor
}

func TestExistingInstallationCurrent(t *testing.T) {
	base, dir, owner := currentInstallation(t)
	before := existingTree(t, base)
	installed, err := InspectExisting(dir)
	if err != nil {
		t.Fatal(err)
	}
	if installed.Owner() != owner || installed.ArtifactInstallation().Enabled {
		t.Fatalf("facts owner %q artifacts %+v", installed.Owner(), installed.ArtifactInstallation())
	}
	if !maps.Equal(before, existingTree(t, base)) {
		t.Fatal("InspectExisting changed the installation")
	}
	for range 2 {
		s, err := OpenExisting(dir, installed)
		if err != nil {
			t.Fatal(err)
		}
		if s.Owner() != owner || !s.Scoped() {
			s.Close()
			t.Fatalf("Store owner %q scoped %v", s.Owner(), s.Scoped())
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		// Neither acquisition nor Close initialized, migrated or wrote, so
		// the same facts still bind the unchanged database.
		if !maps.Equal(before, existingTree(t, base)) {
			t.Fatal("OpenExisting wrote the database")
		}
	}

	// The accepted Store is an ordinary writer; its commit is a legitimate
	// intervening write that makes the earlier facts stale.
	s, err := OpenExisting(dir, installed)
	if err != nil {
		t.Fatal(err)
	}
	job, err := s.Submit(scopedTestActor(t, owner), "existing", backend.Task{Argv: []string{"trainer"}}, "local", "test-binding")
	if closeErr := s.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenExisting(dir, installed); !errors.Is(err, ErrInstallationChanged) {
		t.Fatalf("OpenExisting with stale facts = %v, want ErrInstallationChanged", err)
	}
	refreshed, err := InspectExisting(dir)
	if err != nil {
		t.Fatal(err)
	}
	s, err = OpenExisting(dir, refreshed)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got, err := s.Get(job.ID); err != nil || got.ID != job.ID {
		t.Fatalf("Get = %v, %v", got.ID, err)
	}
}

func TestInspectExistingRefuses(t *testing.T) {
	for _, tt := range []struct {
		name string
		// setup prepares base and returns the directory to inspect.
		setup func(t *testing.T, base, dir string) string
		want  error
	}{
		{"missing database", func(t *testing.T, base, dir string) string {
			if err := os.Remove(filepath.Join(dir, "jobs.db")); err != nil {
				t.Fatal(err)
			}
			return dir
		}, fs.ErrNotExist},
		{"missing directory", func(t *testing.T, base, dir string) string {
			return filepath.Join(base, "absent")
		}, fs.ErrNotExist},
		{"zero-byte database", func(t *testing.T, base, dir string) string {
			if err := os.Truncate(filepath.Join(dir, "jobs.db"), 0); err != nil {
				t.Fatal(err)
			}
			return dir
		}, ErrNotCurrent},
		{"short database", func(t *testing.T, base, dir string) string {
			if err := os.Truncate(filepath.Join(dir, "jobs.db"), 100); err != nil {
				t.Fatal(err)
			}
			return dir
		}, ErrNotCurrent},
		{"database directory", func(t *testing.T, base, dir string) string {
			path := filepath.Join(dir, "jobs.db")
			if err := os.Rename(path, filepath.Join(base, "moved.db")); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(path, 0700); err != nil {
				t.Fatal(err)
			}
			return dir
		}, ErrNotCurrent},
		{"database symlink", func(t *testing.T, base, dir string) string {
			path := filepath.Join(dir, "jobs.db")
			if err := os.Rename(path, filepath.Join(dir, "real.db")); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("real.db", path); err != nil {
				t.Fatal(err)
			}
			return dir
		}, nil},
		{"directory symlink", func(t *testing.T, base, dir string) string {
			link := filepath.Join(base, "link")
			if err := os.Symlink("state", link); err != nil {
				t.Fatal(err)
			}
			return link
		}, ErrNotCurrent},
		{"unscoped", func(t *testing.T, base, dir string) string {
			// Actual ordinary Store output, not an edit: current, unscoped.
			other := filepath.Join(base, "unscoped")
			s, err := Open(other)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			return other
		}, ErrNotCurrent},
		{"modeled ownerless", func(t *testing.T, base, dir string) string {
			modelEdit(t, dir, modelMetadata([]byte("owner"), nil))
			return dir
		}, ErrNotCurrent},
		{"modeled missing retry counter", func(t *testing.T, base, dir string) string {
			modelEdit(t, dir, modelMetadata(retryBytesKey, nil))
			return dir
		}, ErrNotCurrent},
		{"modeled missing task counter", func(t *testing.T, base, dir string) string {
			modelEdit(t, dir, modelMetadata(taskBytesKey, nil))
			return dir
		}, nil},
		{"modeled missing retention table", func(t *testing.T, base, dir string) string {
			modelEdit(t, dir, func(tx *bolt.Tx) error { return retentionRoot(tx).DeleteBucket(retainedScopesBucket) })
			return dir
		}, ErrNotCurrent},
		{"modeled older marker", func(t *testing.T, base, dir string) string {
			var n [8]byte
			binary.BigEndian.PutUint64(n[:], schemaVersion-1)
			modelEdit(t, dir, modelMetadata(schemaKey, n[:]))
			return dir
		}, ErrNotCurrent},
		{"modeled newer marker", func(t *testing.T, base, dir string) string {
			var n [8]byte
			binary.BigEndian.PutUint64(n[:], schemaVersion+1)
			modelEdit(t, dir, modelMetadata(schemaKey, n[:]))
			return dir
		}, ErrUnsupportedSchema},
		{"writer holds lock", func(t *testing.T, base, dir string) string {
			s, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			return dir
		}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			base, dir, _ := currentInstallation(t)
			target := tt.setup(t, base, dir)
			before := existingTree(t, base)
			installed, err := InspectExisting(target)
			if err == nil || installed.Owner() != "" {
				t.Fatalf("InspectExisting accepted: owner %q", installed.Owner())
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("InspectExisting = %v, want %v", err, tt.want)
			}
			if errors.Is(err, ErrCloseUncertain) {
				t.Fatalf("positively closed refusal reported uncertainty: %v", err)
			}
			if _, err := OpenExisting(target, installed); err == nil {
				t.Fatal("OpenExisting accepted zero facts")
			}
			if !maps.Equal(before, existingTree(t, base)) {
				t.Fatal("refusal changed the installation, its directory or a bystander")
			}
		})
	}
}

func TestOpenExistingRefusesChange(t *testing.T) {
	for _, tt := range []struct {
		name string
		// change runs after InspectExisting; acquired runs under the writer lock.
		change, acquired func(t *testing.T, base, dir string)
		want             error
	}{
		{"intervening writer", func(t *testing.T, base, dir string) {
			s, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		}, nil, ErrInstallationChanged},
		{"replaced database", func(t *testing.T, base, dir string) {
			path := filepath.Join(dir, "jobs.db")
			copyFile(t, path, path+".new")
			if err := os.Rename(path+".new", path); err != nil {
				t.Fatal(err)
			}
		}, nil, ErrInstallationChanged},
		{"removed database", func(t *testing.T, base, dir string) {
			if err := os.Rename(filepath.Join(dir, "jobs.db"), filepath.Join(base, "moved.db")); err != nil {
				t.Fatal(err)
			}
		}, nil, fs.ErrNotExist},
		{"replaced directory", func(t *testing.T, base, dir string) {
			if err := os.Rename(dir, dir+".old"); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			copyFile(t, filepath.Join(dir+".old", "jobs.db"), filepath.Join(dir, "jobs.db"))
		}, nil, ErrInstallationChanged},
		{"replaced under writer lock", nil, func(t *testing.T, base, dir string) {
			path := filepath.Join(dir, "jobs.db")
			if err := os.Rename(path, filepath.Join(base, "moved.db")); err != nil {
				t.Fatal(err)
			}
			copyFile(t, filepath.Join(base, "moved.db"), path)
		}, ErrInstallationChanged},
		{"removed under writer lock", nil, func(t *testing.T, base, dir string) {
			if err := os.Rename(filepath.Join(dir, "jobs.db"), filepath.Join(base, "moved.db")); err != nil {
				t.Fatal(err)
			}
		}, ErrInstallationChanged},
	} {
		t.Run(tt.name, func(t *testing.T) {
			base, dir, _ := currentInstallation(t)
			installed, err := InspectExisting(dir)
			if err != nil {
				t.Fatal(err)
			}
			if tt.change != nil {
				tt.change(t, base, dir)
			}
			before := existingTree(t, base)
			acquisitions := 0
			t.Cleanup(func() { existingAcquiredHook = nil })
			existingAcquiredHook = func() {
				acquisitions++
				if tt.acquired != nil {
					// MODELED interleaving at the actual guard point.
					tt.acquired(t, base, dir)
					before = existingTree(t, base)
				}
			}
			s, err := OpenExisting(dir, installed)
			if err == nil {
				s.Close()
				t.Fatal("OpenExisting accepted a changed installation")
			}
			if !errors.Is(err, tt.want) || errors.Is(err, ErrCloseUncertain) {
				t.Fatalf("OpenExisting = %v, want %v", err, tt.want)
			}
			if acquisitions > 1 {
				t.Fatalf("writer acquired %d times", acquisitions)
			}
			if !maps.Equal(before, existingTree(t, base)) {
				t.Fatal("refusal created, initialized or wrote state")
			}
		})
	}
}

// TestOpenExistingRefusesHeldWriter refuses while an ordinary writer holds
// the lock. The hook owns the descriptor then, so its Close is positive.
func TestOpenExistingRefusesHeldWriter(t *testing.T) {
	base, dir, _ := currentInstallation(t)
	installed, err := InspectExisting(dir)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	before := existingTree(t, base)
	_, err = OpenExisting(dir, installed)
	if closeErr := s.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err == nil || errors.Is(err, ErrCloseUncertain) {
		t.Fatalf("OpenExisting under a held writer = %v", err)
	}
	if !maps.Equal(before, existingTree(t, base)) {
		t.Fatal("refusal changed the installation")
	}
}

// TestOpenExistingRefusesTornUnderLock models the database file being
// truncated, through the same inode, after OpenExisting's first descriptor
// check and while it holds the writer lock. bbolt would initialize a
// zero-byte file and reject a short one only after taking the descriptor, so
// the final check on the locked descriptor must refuse first. The current
// twin is the unmodified positive case.
func TestOpenExistingRefusesTornUnderLock(t *testing.T) {
	for _, tt := range []struct {
		name string
		size int64 // -1 leaves the database whole
	}{
		{"current twin", -1},
		{"zero under writer lock", 0},
		{"short under writer lock", 100},
		{"one byte short under writer lock", 2047},
	} {
		t.Run(tt.name, func(t *testing.T) {
			base, dir, owner := currentInstallation(t)
			installed, err := InspectExisting(dir)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "jobs.db")
			before := existingTree(t, base)
			inode := before[filepath.Join("state", "jobs.db")].inode
			acquisitions := 0
			t.Cleanup(func() { existingAcquiredHook = nil })
			existingAcquiredHook = func() {
				acquisitions++
				if tt.size < 0 {
					return
				}
				// MODELED transition: the locked descriptor's file is torn
				// before the final descriptor check.
				if err := os.Truncate(path, tt.size); err != nil {
					t.Fatal(err)
				}
				before = existingTree(t, base)
			}
			closed := modelDatabaseClose(t, nil)
			s, err := OpenExisting(dir, installed)
			if acquisitions != 1 {
				t.Fatalf("writer acquired %d times", acquisitions)
			}
			if tt.size < 0 {
				if err != nil {
					t.Fatal(err)
				}
				if s.Owner() != owner || !s.Scoped() {
					s.Close()
					t.Fatalf("Store owner %q scoped %v", s.Owner(), s.Scoped())
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				if s != nil {
					s.Close()
					t.Fatal("OpenExisting accepted a torn database")
				}
				if !errors.Is(err, ErrNotCurrent) || errors.Is(err, ErrCloseUncertain) {
					t.Fatalf("OpenExisting = %v, want ErrNotCurrent closed positively", err)
				}
				if len(*closed) != 0 {
					t.Fatal("a torn database reached bbolt")
				}
			}
			after := existingTree(t, base)
			if !maps.Equal(before, after) {
				t.Fatal("OpenExisting initialized or wrote the database")
			}
			if after[filepath.Join("state", "jobs.db")].inode != inode {
				t.Fatal("database inode changed")
			}
			// The original descriptor was closed, so its lock is free.
			fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer syscall.Close(fd)
			if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
				t.Fatalf("writer lock still held: %v", err)
			}
		})
	}
}

// modelDatabaseClose replaces closeExistingDB. Each call really closes the
// original database and records it, then returns fail if it is not nil. The
// returned error is MODELED: the native Close succeeded.
func modelDatabaseClose(t *testing.T, fail error) *[]*bolt.DB {
	t.Helper()
	var closed []*bolt.DB
	t.Cleanup(func() { closeExistingDB = (*bolt.DB).Close })
	closeExistingDB = func(db *bolt.DB) error {
		closed = append(closed, db)
		if err := db.Close(); err != nil {
			t.Errorf("native close: %v", err)
			return err
		}
		return fail
	}
	return &closed
}

func TestExistingCloseUncertain(t *testing.T) {
	modeled := errors.New("modeled close failure")
	t.Run("inspect", func(t *testing.T) {
		_, dir, _ := currentInstallation(t)
		closed := modelDatabaseClose(t, modeled)
		installed, err := InspectExisting(dir)
		var held *closeError
		if installed.Owner() != "" || !errors.Is(err, ErrCloseUncertain) || !errors.Is(err, modeled) || !errors.As(err, &held) {
			t.Fatalf("InspectExisting = %q, %v; want no facts and ErrCloseUncertain", installed.Owner(), err)
		}
		if len(*closed) != 1 || held.db != (*closed)[0] || held.file == nil {
			t.Fatalf("closes %d; error does not retain the original database", len(*closed))
		}
	})
	t.Run("refused writer", func(t *testing.T) {
		_, dir, _ := currentInstallation(t)
		installed, err := InspectExisting(dir)
		if err != nil {
			t.Fatal(err)
		}
		s, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		acquisitions := 0
		t.Cleanup(func() { existingAcquiredHook = nil })
		existingAcquiredHook = func() { acquisitions++ }
		closed := modelDatabaseClose(t, modeled)
		s, err = OpenExisting(dir, installed)
		var held *closeError
		if s != nil || !errors.Is(err, ErrCloseUncertain) || !errors.Is(err, ErrInstallationChanged) || !errors.As(err, &held) {
			t.Fatalf("OpenExisting = %v, %v; want no Store and both causes", s, err)
		}
		if acquisitions != 1 || len(*closed) != 1 || held.db != (*closed)[0] {
			t.Fatalf("acquisitions %d, closes %d; want one of each, retained", acquisitions, len(*closed))
		}
	})
	t.Run("accepted", func(t *testing.T) {
		_, dir, _ := currentInstallation(t)
		installed, err := InspectExisting(dir)
		if err != nil {
			t.Fatal(err)
		}
		closed := modelDatabaseClose(t, modeled)
		s, err := OpenExisting(dir, installed)
		if err != nil {
			t.Fatal(err)
		}
		if len(*closed) != 0 {
			t.Fatal("accepted Store was closed by OpenExisting")
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

// TestInspectExistingGenuinePredecessor refuses a supported genuine schema10
// database, which Open would migrate, and leaves it unchanged. Open still
// migrates it. It runs only when the SKYGO_SCHEMA10_DB pair names one.
func TestInspectExistingGenuinePredecessor(t *testing.T) {
	_, data := genuineFixture(t, "SKYGO_SCHEMA10_DB")
	base := t.TempDir()
	if err := os.Chmod(base, 0700); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(base, "state")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "jobs.db"), data, 0600); err != nil {
		t.Fatal(err)
	}
	before := existingTree(t, base)
	if _, err := InspectExisting(dir); !errors.Is(err, ErrNotCurrent) {
		t.Fatalf("InspectExisting = %v, want ErrNotCurrent", err)
	}
	if !maps.Equal(before, existingTree(t, base)) {
		t.Fatal("refusal changed the predecessor")
	}
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("ordinary Open no longer migrates: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}
