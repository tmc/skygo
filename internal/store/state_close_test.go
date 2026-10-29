package store

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// Snapshot fault fixtures are retained, including their actual errors and
// descriptors. The faults below are MODELED returns after successful native
// Close calls, not native close-failure or retained-lock evidence.
type snapshotFixture struct {
	root                string
	db                  *bolt.DB
	store               *Store
	storeCloseAttempted bool
	errors              []error
}

var heldSnapshotFixtures struct {
	sync.Mutex
	fixtures []*snapshotFixture
}

func newSnapshotFixture(t *testing.T) *snapshotFixture {
	t.Helper()
	dir, err := os.MkdirTemp("", "skygo-snapshot-close-")
	if err != nil {
		t.Fatal(err)
	}
	f := &snapshotFixture{root: dir}
	t.Cleanup(func() {
		if f.db != nil {
			if err := f.db.Close(); err != nil {
				f.errors = append(f.errors, err)
			}
		}
		if f.store != nil && !f.storeCloseAttempted {
			f.storeCloseAttempted = true
			if err := f.store.Close(); err != nil {
				f.errors = append(f.errors, err)
			}
		}
		if len(f.errors) != 0 {
			heldSnapshotFixtures.Lock()
			heldSnapshotFixtures.fixtures = append(heldSnapshotFixtures.fixtures, f)
			heldSnapshotFixtures.Unlock()
			t.Logf("retaining snapshot fault fixture: %s", f.root)
			return
		}
		if err := os.RemoveAll(f.root); err != nil {
			t.Error(err)
		}
	})
	return f
}

func (f *snapshotFixture) source(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(f.root, "source")
	s, err := Open(dir)
	f.store = s
	if err != nil {
		f.errors = append(f.errors, err)
		t.Fatal(err)
	}
	if _, err := s.Submit(testActor(), "snapshot", backend.Task{Argv: []string{"echo", "snapshot"}}, "local", "test-binding"); err != nil {
		t.Fatal(err)
	}
	f.storeCloseAttempted = true
	err = s.Close()
	f.store = nil // The original Close was attempted; never retry it.
	if err != nil {
		f.store = s
		f.errors = append(f.errors, err)
		t.Fatal(err)
	}
	return dir
}

func (f *snapshotFixture) hold(err error) { f.errors = append(f.errors, err) }

type snapshotCloseCalls struct {
	dbs    map[*bolt.DB]int
	files  map[*os.File]int
	native []error
	// Hooks run only after the same original Close returned nil. They may
	// model its reported failure, or an interleaving on the closed temp.
	afterDB   func(*bolt.DB, string, bool) error
	afterFile func(*os.File, string, bool) error
}

func newSnapshotCloseCalls() *snapshotCloseCalls {
	return &snapshotCloseCalls{dbs: make(map[*bolt.DB]int), files: make(map[*os.File]int)}
}

func (c *snapshotCloseCalls) closes() snapshotOps {
	return snapshotOps{
		link: os.Link,
		database: func(db *bolt.DB) error {
			path, readonly := db.Path(), db.IsReadOnly()
			c.dbs[db]++
			if err := db.Close(); err != nil {
				c.native = append(c.native, err)
				return err
			}
			if c.afterDB != nil {
				return c.afterDB(db, path, readonly)
			}
			return nil
		},
		file: func(f *os.File) error {
			c.files[f]++
			path := f.Name()
			info, err := f.Stat()
			if err != nil {
				return err
			}
			if err := f.Close(); err != nil {
				c.native = append(c.native, err)
				return err
			}
			if c.afterFile != nil {
				return c.afterFile(f, path, info.IsDir())
			}
			return nil
		},
	}
}

func (c *snapshotCloseCalls) check(t *testing.T) {
	t.Helper()
	for _, n := range c.dbs {
		if n != 1 {
			t.Errorf("database Close called %d times, want one", n)
		}
	}
	for _, n := range c.files {
		if n != 1 {
			t.Errorf("file Close called %d times, want one", n)
		}
	}
	if len(c.native) != 0 {
		t.Errorf("native Close failed, not the intended MODELED fault: %v", c.native)
	}
}

func heldSnapshotError(t *testing.T, err, cause error) *closeError {
	t.Helper()
	var held *closeError
	if !errors.Is(err, cause) || !errors.Is(err, ErrCloseUncertain) || !errors.As(err, &held) || held.file == nil {
		t.Fatalf("error = %v, want both causes and the actual original descriptor", err)
	}
	return held
}

// corruptSnapshotFormat is a MODELED mutation of a closed owned temp through
// real bbolt APIs. It changes a format tag, never the schema ordinal.
func corruptSnapshotFormat(path string) (result error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, db.Close()) }()
	return db.Update(func(tx *bolt.Tx) error {
		m := tx.Bucket(metadataBucket)
		if m == nil {
			return errors.New("snapshot mutation requires existing metadata")
		}
		return m.Put(servingStorageFormatKey, []byte("modeled-foreign-format"))
	})
}

func TestSnapshotClosePositive(t *testing.T) {
	f := newSnapshotFixture(t)
	source := f.source(t)
	out := filepath.Join(f.root, "backup.db")
	c := newSnapshotCloseCalls()
	if err := backup(source, out, c.closes()); err != nil {
		f.hold(err)
		t.Fatal(err)
	}
	c.check(t)
	c = newSnapshotCloseCalls()
	restored := filepath.Join(f.root, "restored")
	if err := restore(out, restored, c.closes()); err != nil {
		f.hold(err)
		t.Fatal(err)
	}
	c.check(t)
	s, err := Open(restored)
	f.store = s
	f.storeCloseAttempted = false
	if err != nil {
		f.hold(err)
		t.Fatal(err)
	}
	jobs, err := s.List(0, 1, 10)
	if err != nil || len(jobs) != 1 || len(jobs[0].Task.Argv) != 2 || jobs[0].Task.Argv[1] != "snapshot" {
		t.Fatalf("restored jobs = %v, %v", jobs, err)
	}
}

func TestSnapshotSourceCloseFailure(t *testing.T) {
	for _, restoring := range []bool{false, true} {
		t.Run(map[bool]string{false: "backup", true: "restore"}[restoring], func(t *testing.T) {
			f := newSnapshotFixture(t)
			source := f.source(t)
			input := filepath.Join(source, "jobs.db")
			fault := errors.New("MODELED source database Close failure")
			c := newSnapshotCloseCalls()
			var original *bolt.DB
			c.afterDB = func(db *bolt.DB, path string, _ bool) error {
				if path == input {
					original = db
					return fault
				}
				return nil
			}
			output := filepath.Join(f.root, "published")
			var err error
			if restoring {
				err = restore(input, output, c.closes())
			} else {
				err = backup(source, output, c.closes())
			}
			f.hold(err)
			held := heldSnapshotError(t, err, fault)
			if held.db != original || held.file.Name() != input {
				t.Fatal("original source owner lost")
			}
			if _, err := os.Stat(output); err != nil {
				t.Fatal("late Close error rolled back published output", err)
			}
			c.check(t)
		})
	}
}

func TestSnapshotPrimaryAndFileClose(t *testing.T) {
	for _, failing := range []bool{false, true} {
		t.Run(map[bool]string{false: "clean close", true: "failed close"}[failing], func(t *testing.T) {
			f := newSnapshotFixture(t)
			source := f.source(t)
			if err := corruptSnapshotFormat(filepath.Join(source, "jobs.db")); err != nil {
				t.Fatal(err)
			}
			db, err := bolt.Open(filepath.Join(source, "jobs.db"), 0600, &bolt.Options{ReadOnly: true})
			f.db = db
			if err != nil {
				f.hold(err)
				t.Fatal(err)
			}
			c := newSnapshotCloseCalls()
			fault := errors.New("MODELED temporary file Close failure")
			var original *os.File
			c.afterFile = func(file *os.File, _ string, _ bool) error {
				original = file
				if failing {
					return fault
				}
				return nil
			}
			name, err := prepareSnapshot(&snapshotDB{db: db}, f.root, c.closes())
			if failing {
				f.hold(err)
			}
			if !errors.Is(err, ErrUnsupportedSchema) || original == nil || name != "" {
				t.Fatalf("prepare = %q, %v", name, err)
			}
			if failing {
				if heldSnapshotError(t, err, fault).file != original {
					t.Fatal("original temp lost")
				}
				if _, err := os.Stat(original.Name()); err != nil {
					t.Fatal("failed Close temp was removed", err)
				}
			} else if _, err := os.Stat(original.Name()); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("ordinary failed temp not removed", err)
			}
			if len(c.dbs) != 0 {
				t.Fatal("failed temp was reopened")
			}
			c.check(t)
		})
	}
}

func TestSnapshotVerificationClose(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(map[bool]string{false: "valid copy", true: "invalid copy"}[corrupt], func(t *testing.T) {
			f := newSnapshotFixture(t)
			source := f.source(t)
			fault := errors.New("MODELED completed copy Close failure")
			c := newSnapshotCloseCalls()
			var temp string
			var original *bolt.DB
			c.afterFile = func(_ *os.File, path string, directory bool) error {
				if !directory {
					temp = path
					if corrupt {
						return corruptSnapshotFormat(path)
					}
				}
				return nil
			}
			c.afterDB = func(db *bolt.DB, path string, _ bool) error {
				if path == temp {
					original = db
					return fault
				}
				return nil
			}
			output := filepath.Join(f.root, "backup.db")
			err := backup(source, output, c.closes())
			f.hold(err)
			held := heldSnapshotError(t, err, fault)
			if held.db != original || held.file.Name() != temp || errors.Is(err, ErrUnsupportedSchema) != corrupt {
				t.Fatalf("verification owner or primary cause lost: %v", err)
			}
			if _, err := os.Stat(temp); err != nil {
				t.Fatal("verification Close failed but temp removed", err)
			}
			if _, err := os.Stat(output); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("verification Close failure published", err)
			}
			c.check(t)
		})
	}
}

func TestSnapshotFreshenClose(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(map[bool]string{false: "updated", true: "validation failure"}[corrupt], func(t *testing.T) {
			f := newSnapshotFixture(t)
			source := f.source(t)
			fault := errors.New("MODELED freshening database Close failure")
			c := newSnapshotCloseCalls()
			var temp string
			var original *bolt.DB
			c.afterDB = func(db *bolt.DB, path string, readonly bool) error {
				if strings.HasPrefix(filepath.Base(path), ".skygo-snapshot-") {
					temp = path
					if readonly && corrupt {
						return corruptSnapshotFormat(path)
					}
					if !readonly {
						original = db
						return fault
					}
				}
				return nil
			}
			output := filepath.Join(f.root, "restored")
			err := restore(filepath.Join(source, "jobs.db"), output, c.closes())
			f.hold(err)
			held := heldSnapshotError(t, err, fault)
			if held.db != original || held.file.Name() != temp || errors.Is(err, ErrUnsupportedSchema) != corrupt {
				t.Fatalf("freshening owner or primary cause lost: %v", err)
			}
			if _, err := os.Stat(temp); err != nil {
				t.Fatal("freshening temp removed", err)
			}
			if _, err := os.Stat(output); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("freshening Close failure published", err)
			}
			c.check(t)
		})
	}
}

func TestSnapshotFailedOpenRetainsFile(t *testing.T) {
	f := newSnapshotFixture(t)
	path := filepath.Join(f.root, "invalid.db")
	if err := os.WriteFile(path, make([]byte, 2*os.Getpagesize()), 0600); err != nil {
		t.Fatal(err)
	}
	owner, err := openOwnedSnapshot(path, false)
	f.hold(err)
	held := heldSnapshotError(t, err, bolt.ErrInvalid)
	if owner != nil || held.db != nil || held.file.Name() != path {
		t.Fatal("failed Open lost its handed-off file")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("failed Open removed source", err)
	}
	// No extra Close is attempted. bbolt's discarded internal Close result
	// remains UNKNOWN, even if a later observer finds a closed descriptor.
}

func TestSnapshotDirectoryClose(t *testing.T) {
	for _, restoring := range []bool{false, true} {
		t.Run(map[bool]string{false: "backup", true: "restore"}[restoring], func(t *testing.T) {
			f := newSnapshotFixture(t)
			source := f.source(t)
			fault := errors.New("MODELED directory Close failure")
			c := newSnapshotCloseCalls()
			var original *os.File
			c.afterFile = func(file *os.File, _ string, directory bool) error {
				if directory {
					original = file
					return fault
				}
				return nil
			}
			output := filepath.Join(f.root, "published")
			var err error
			if restoring {
				err = restore(filepath.Join(source, "jobs.db"), output, c.closes())
			} else {
				err = backup(source, output, c.closes())
			}
			f.hold(err)
			if heldSnapshotError(t, err, fault).file != original {
				t.Fatal("directory owner lost")
			}
			if _, err := os.Stat(output); err != nil {
				t.Fatal("published output rolled back", err)
			}
			temps, globErr := filepath.Glob(filepath.Join(f.root, ".skygo-snapshot-*"))
			if globErr != nil || len(temps) != 1 || !strings.Contains(err.Error(), temps[0]) {
				t.Fatalf("retained temporary name missing: %v, %v", temps, globErr)
			}
			c.check(t)
		})
	}
}

func TestSnapshotParallelClosePolicies(t *testing.T) {
	for _, name := range []string{"one", "two"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newSnapshotFixture(t)
			source := f.source(t)
			fault := errors.New("MODELED private " + name)
			c := newSnapshotCloseCalls()
			c.afterDB = func(_ *bolt.DB, _ string, _ bool) error { return fault }
			err := backup(source, filepath.Join(f.root, "backup.db"), c.closes())
			f.hold(err)
			heldSnapshotError(t, err, fault)
			c.check(t)
		})
	}
}

// snapshotFailWriter produces an actual Tx.WriteTo error. The write failure
// is MODELED; bbolt's reader Close executes normally but hides its result.
type snapshotFailWriter struct {
	err    error
	writes int
}

func (w *snapshotFailWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes <= 2 {
		return len(p), nil // Let bbolt write its two metadata pages first.
	}
	return 0, w.err
}

func TestSnapshotCopyReader(t *testing.T) {
	for _, failing := range []bool{false, true} {
		t.Run(map[bool]string{false: "successful write", true: "failed write"}[failing], func(t *testing.T) {
			f := newSnapshotFixture(t)
			source := f.source(t)
			owner, err := openOwnedSnapshot(filepath.Join(source, "jobs.db"), false)
			if err != nil {
				f.hold(err)
				t.Fatal(err)
			}
			fault := errors.New("MODELED snapshot writer failure")
			var writer io.Writer = io.Discard
			if failing {
				writer = &snapshotFailWriter{err: fault}
			}
			err = owner.snapshot(writer)
			c := newSnapshotCloseCalls()
			err = errors.Join(err, owner.close(c.closes()))
			if err != nil {
				f.hold(err)
			}
			if len(owner.readers) != 1 || owner.readers[0] == owner.file {
				t.Fatalf("copy readers = %v, want one distinct actual descriptor", owner.readers)
			}
			if failing {
				held := heldSnapshotError(t, err, fault)
				if held.file != owner.readers[0] || held.db != owner.db {
					t.Fatal("copy reader owner lost")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			// Observe without retrying Close. This shows the native reader
			// was closed on this host, not the hidden first Close return.
			if _, err := owner.readers[0].Stat(); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("copy reader observation = %v, want closed", err)
			}
			c.check(t)
		})
	}
}

// TestSnapshotRestoreLinkFailure uses a MODELED interleaving to create a
// bystander before the real os.Link. One twin removes only that test-owned
// bystander after the refusal, leaving an empty directory whose cleanup order
// is observable. The other preserves it through every failure path.
func TestSnapshotRestoreLinkFailure(t *testing.T) {
	for _, keepBystander := range []bool{false, true} {
		for _, failClose := range []bool{false, true} {
			t.Run(fmt.Sprintf("bystander=%v/close=%v", keepBystander, failClose), func(t *testing.T) {
				f := newSnapshotFixture(t)
				source := f.source(t)
				input := filepath.Join(source, "jobs.db")
				destination := filepath.Join(f.root, "restored")
				bystander := filepath.Join(destination, "jobs.db")
				c := newSnapshotCloseCalls()
				fault := errors.New("MODELED late input database Close failure")
				linkCalled, sourceClosed := false, false
				c.afterDB = func(_ *bolt.DB, path string, _ bool) error {
					if path != input {
						return nil
					}
					sourceClosed = true
					if !linkCalled {
						t.Error("source Close before actual Link refusal")
					}
					if _, err := os.Stat(destination); err != nil {
						t.Errorf("reserved directory removed before source Close: %v", err)
					}
					if failClose {
						return fault
					}
					return nil
				}
				ops := c.closes()
				ops.link = func(oldname, newname string) error {
					linkCalled = true
					if newname != bystander {
						t.Errorf("Link destination = %q", newname)
						return errors.New("unexpected snapshot Link destination")
					}
					if err := os.WriteFile(newname, []byte("bystander"), 0600); err != nil {
						return err
					}
					err := os.Link(oldname, newname)
					if !errors.Is(err, os.ErrExist) {
						t.Errorf("actual Link = %v, want exists", err)
					}
					if !keepBystander {
						err = errors.Join(err, os.Remove(newname))
					}
					return err
				}
				err := restore(input, destination, ops)
				if !linkCalled || !sourceClosed || !errors.Is(err, os.ErrExist) {
					f.hold(err)
					t.Fatalf("Link/Close/primary = %v/%v/%v", linkCalled, sourceClosed, err)
				}
				if failClose {
					f.hold(err)
					heldSnapshotError(t, err, fault)
					if !strings.Contains(err.Error(), destination) {
						t.Error("held restore directory path lost")
					}
					if _, err := os.Stat(destination); err != nil {
						t.Fatal("uncertain Close removed reserved directory", err)
					}
				} else if errors.Is(err, ErrCloseUncertain) {
					f.hold(err)
					t.Fatal("ordinary refusal became uncertain", err)
				}
				if keepBystander {
					data, readErr := os.ReadFile(bystander)
					if readErr != nil || string(data) != "bystander" {
						t.Fatalf("bystander = %q, %v", data, readErr)
					}
				} else if !failClose {
					if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
						t.Fatal("closed empty reserved directory not removed", err)
					}
				}
				c.check(t)
			})
		}
	}
}
