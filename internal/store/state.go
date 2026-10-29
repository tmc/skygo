package store

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Backup writes a consistent database snapshot from a stopped installation in
// dir to a new output file. It preserves identity and active job intent, but
// excludes local logs. It never overwrites output. The server must be stopped.
// An error may be returned after output was published. An ErrCloseUncertain
// error retains an original owner; keep the error and do not retry or clean up
// beneath it.
func Backup(dir, output string) error {
	if dir == "" || output == "" {
		return errors.New("backup requires a data directory and output file")
	}
	return backup(filepath.Clean(dir), filepath.Clean(output), snapshotOps{database: (*bolt.DB).Close, file: (*os.File).Close, link: os.Link})
}

// snapshotOps is passed by value through one backup or restore. Tests wrap
// the original Close and Link operations without changing another operation
// or replacing the snapshot algorithm.
type snapshotOps struct {
	database func(*bolt.DB) error
	file     func(*os.File) error
	link     func(string, string) error
}

func (c snapshotOps) closeFile(f *os.File) error {
	if err := c.file(f); err != nil {
		return &closeError{file: f, err: err}
	}
	return nil
}

// snapshotDB retains the descriptor handed to bbolt. bbolt discards its own
// reference even when Close fails, and a later Close cannot recover the result.
type snapshotDB struct {
	db      *bolt.DB
	file    *os.File
	readers []*os.File
}

func (s *snapshotDB) close(c snapshotOps) error {
	if err := c.database(s.db); err != nil {
		return &closeError{db: s.db, file: s.file, err: err}
	}
	return nil
}

// snapshot uses the same validation and Tx.WriteTo path as other callers.
// bbolt opens a separate copy reader through OpenFile. Its deferred Close
// result is returned only when WriteTo has no earlier error; otherwise that
// original reader must remain owned without another Close attempt.
func (s *snapshotDB) snapshot(w io.Writer) error {
	first := len(s.readers)
	err := snapshot(s.db, w)
	if err == nil {
		return nil
	}
	result := err
	for _, f := range s.readers[first:] {
		result = errors.Join(result, &closeError{db: s.db, file: f,
			err: fmt.Errorf("snapshot copy reader first close result unavailable: %w", err)})
	}
	return result
}

func openOwnedSnapshot(path string, writable bool) (*snapshotDB, error) {
	owner := new(snapshotDB)
	db, err := bolt.Open(path, 0600, &bolt.Options{
		Timeout: time.Second, ReadOnly: !writable, NoFreelistSync: writable,
		OpenFile: func(name string, flag int, mode os.FileMode) (*os.File, error) {
			f, err := os.OpenFile(name, flag&^os.O_CREATE, mode)
			if err == nil {
				// Capture each original before handing it to bbolt. The
				// first is the database; Tx.WriteTo opens its own reader.
				if owner.file == nil {
					owner.file = f
				} else {
					owner.readers = append(owner.readers, f)
				}
			}
			return f, err
		},
	})
	if err != nil {
		if owner.file != nil {
			// bbolt already attempted Close and discarded that result.
			// Retain the actual descriptor; never Close it a second time.
			return nil, &closeError{file: owner.file, err: err}
		}
		return nil, err
	}
	owner.db = db
	return owner, nil
}

// removeSnapshot removes only a closed, owned temporary name. Uncertain
// closure anywhere in the operation keeps that name and all original owners.
func removeSnapshot(name string, result *error) {
	if name == "" {
		return
	}
	if errors.Is(*result, ErrCloseUncertain) {
		*result = fmt.Errorf("snapshot temporary file %q retained: %w", name, *result)
		return
	}
	if err := os.Remove(name); err != nil {
		*result = errors.Join(*result, fmt.Errorf("remove snapshot temporary file: %w", err))
	}
}

func backup(dir, output string, closes snapshotOps) (result error) {
	source, err := openOwnedSnapshot(filepath.Join(dir, "jobs.db"), false)
	if err != nil {
		return fmt.Errorf("open backup source (stop the server first): %w", err)
	}
	var name string
	defer func() {
		result = errors.Join(result, source.close(closes))
		removeSnapshot(name, &result)
	}()
	// Validate before creating any file beside output, so a refused source,
	// including one of another storage format, leaves that directory untouched.
	if err := source.db.View(validateSnapshot); err != nil {
		return fmt.Errorf("validate backup source: %w", err)
	}
	name, err = prepareSnapshot(source, filepath.Dir(output), closes)
	if err != nil {
		return err
	}
	// Link atomically publishes the complete file without replacing output.
	if err := closes.link(name, output); err != nil {
		return fmt.Errorf("publish snapshot: %w", err)
	}
	if err := syncSnapshotDirectory(filepath.Dir(output), closes); err != nil {
		return fmt.Errorf("sync published snapshot parent: %w", err)
	}
	return nil
}

// Restore validates input and publishes its database in a new directory dir.
// It never overwrites state or runs workloads. Keep the original installation
// stopped before starting the restored copy: both have the same owner identity.
// Supported legacy state migrates only on the subsequent Open.
// Artifact-enabled snapshots require companion recovery; a database-only restore
// is unsupported. An error may be returned after publication. Keep an
// ErrCloseUncertain error and do not retry or clean up beneath its owner.
func Restore(input, dir string) error {
	if input == "" || dir == "" {
		return errors.New("restore requires an input file and new data directory")
	}
	return restore(filepath.Clean(input), filepath.Clean(dir), snapshotOps{database: (*bolt.DB).Close, file: (*os.File).Close, link: os.Link})
}

func restore(input, dir string, closes snapshotOps) (result error) {
	source, err := openOwnedSnapshot(input, false)
	if err != nil {
		return fmt.Errorf("open restore snapshot: %w", err)
	}
	var name, reservedDir string
	defer func() {
		result = errors.Join(result, source.close(closes))
		if reservedDir != "" {
			if errors.Is(result, ErrCloseUncertain) {
				result = fmt.Errorf("restore directory %q retained: %w", reservedDir, result)
			} else if err := os.Remove(reservedDir); err != nil {
				result = errors.Join(result, fmt.Errorf("remove empty restore directory: %w", err))
			}
		}
		removeSnapshot(name, &result)
	}()
	if err := source.db.View(func(tx *bolt.Tx) error {
		if err := validateSnapshot(tx); err != nil {
			return err
		}
		if tx.Bucket(artifactsBucket) != nil {
			return errors.New("database-only restore of artifact-enabled state is unsupported")
		}
		return nil
	}); err != nil {
		return fmt.Errorf("validate restore snapshot: %w", err)
	}
	name, err = prepareSnapshot(source, filepath.Dir(dir), closes)
	if err != nil {
		return err
	}
	if err := freshenRestored(name, closes); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		return fmt.Errorf("create restore directory: %w", err)
	}
	reservedDir = dir
	if err := closes.link(name, filepath.Join(dir, "jobs.db")); err != nil {
		return fmt.Errorf("publish restored database: %w", err)
	}
	reservedDir = "" // Publication is never rolled back by error cleanup.
	if err := syncSnapshotDirectory(dir, closes); err != nil {
		return fmt.Errorf("sync published restore directory: %w", err)
	}
	if err := syncSnapshotDirectory(filepath.Dir(dir), closes); err != nil {
		return fmt.Errorf("sync published restore parent: %w", err)
	}
	return nil
}

func openSnapshot(path string) (*bolt.DB, error) {
	return bolt.Open(path, 0600, &bolt.Options{
		Timeout: time.Second, ReadOnly: true,
		// bbolt supplies O_CREATE even for read-only opens. Strip it so a missing
		// source cannot be created, including a removal race after a prior stat.
		OpenFile: func(name string, flag int, mode os.FileMode) (*os.File, error) {
			return os.OpenFile(name, flag&^os.O_CREATE, mode)
		},
	})
}

func snapshot(db *bolt.DB, w io.Writer) error {
	return db.View(func(tx *bolt.Tx) error {
		if err := validateSnapshot(tx); err != nil {
			return err
		}
		_, err := tx.WriteTo(w)
		return err
	})
}

func prepareSnapshot(source *snapshotDB, dir string, closes snapshotOps) (name string, result error) {
	f, err := os.CreateTemp(dir, ".skygo-snapshot-*")
	if err != nil {
		return "", fmt.Errorf("create snapshot temporary file: %w", err)
	}
	name = f.Name()
	closeNeeded := true
	defer func() {
		if closeNeeded {
			result = errors.Join(result, closes.closeFile(f))
		}
		if result != nil {
			removeSnapshot(name, &result)
			// The helper has either removed the ordinary failed snapshot or
			// transferred its actual file/path through a retained closeError.
			// Its caller must not try removing this name a second time.
			name = ""
		}
	}()
	if err := source.snapshot(f); err != nil {
		return name, fmt.Errorf("write snapshot: %w", err)
	}
	if err := f.Sync(); err != nil {
		return name, fmt.Errorf("sync snapshot: %w", err)
	}
	closeNeeded = false // Set before the first Close; no deferred retry.
	if err := closes.closeFile(f); err != nil {
		return name, fmt.Errorf("close snapshot: %w", err)
	}
	copy, err := openOwnedSnapshot(name, false)
	if err != nil {
		return name, fmt.Errorf("open completed snapshot: %w", err)
	}
	validationErr := copy.db.View(validateSnapshot)
	closeErr := copy.close(closes)
	if validationErr != nil {
		result = fmt.Errorf("validate completed snapshot: %w", validationErr)
	}
	if closeErr != nil {
		result = errors.Join(result, fmt.Errorf("close completed snapshot: %w", closeErr))
	}
	return name, result
}

func syncSnapshotDirectory(dir string, closes snapshotOps) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := f.Sync()
	closeErr := closes.closeFile(f)
	return errors.Join(syncErr, closeErr)
}
