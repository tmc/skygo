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
func Backup(dir, output string) error {
	if dir == "" || output == "" {
		return errors.New("backup requires a data directory and output file")
	}
	return backup(filepath.Clean(dir), filepath.Clean(output))
}

func backup(dir, output string) error {
	db, err := openSnapshot(filepath.Join(dir, "jobs.db"))
	if err != nil {
		return fmt.Errorf("open backup source (stop the server first): %w", err)
	}
	defer db.Close()
	return writeSnapshot(db, output)
}

// Restore validates input and publishes its database in a new directory dir.
// It never overwrites state or runs workloads. Keep the original installation
// stopped before starting the restored copy: both have the same owner identity.
// Supported legacy state migrates only on the subsequent Open.
func Restore(input, dir string) error {
	if input == "" || dir == "" {
		return errors.New("restore requires an input file and new data directory")
	}
	return restore(filepath.Clean(input), filepath.Clean(dir))
}

func restore(input, dir string) error {
	db, err := openSnapshot(input)
	if err != nil {
		return fmt.Errorf("open restore snapshot: %w", err)
	}
	defer db.Close()
	// Validate before creating the destination. Mkdir, rather than MkdirAll,
	// reserves a new directory and refuses every existing installation.
	if err := db.View(validateSnapshot); err != nil {
		return fmt.Errorf("validate restore snapshot: %w", err)
	}
	name, err := prepareSnapshot(db, filepath.Dir(dir))
	if err != nil {
		return err
	}
	defer os.Remove(name)
	if err := freshenRestored(name); err != nil {
		return err
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		return fmt.Errorf("create restore directory: %w", err)
	}
	if err := os.Link(name, filepath.Join(dir, "jobs.db")); err != nil {
		// Remove only an empty directory. Another process may have created a file.
		_ = os.Remove(dir)
		return fmt.Errorf("publish restored database: %w", err)
	}
	if err := syncDirectory(dir); err != nil {
		return fmt.Errorf("sync published restore directory: %w", err)
	}
	if err := syncDirectory(filepath.Dir(dir)); err != nil {
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

func writeSnapshot(db *bolt.DB, output string) error {
	name, err := prepareSnapshot(db, filepath.Dir(output))
	if err != nil {
		return err
	}
	defer os.Remove(name)
	// Link atomically publishes the complete file and refuses an existing path.
	if err := os.Link(name, output); err != nil {
		return fmt.Errorf("publish snapshot: %w", err)
	}
	if err := syncDirectory(filepath.Dir(output)); err != nil {
		return fmt.Errorf("sync published snapshot parent: %w", err)
	}
	return nil
}

func prepareSnapshot(db *bolt.DB, dir string) (name string, result error) {
	f, err := os.CreateTemp(dir, ".skygo-snapshot-*")
	if err != nil {
		return "", fmt.Errorf("create snapshot temporary file: %w", err)
	}
	name = f.Name()
	defer f.Close()
	defer func() {
		if result != nil {
			os.Remove(name)
		}
	}()
	if err := snapshot(db, f); err != nil {
		return name, fmt.Errorf("write snapshot: %w", err)
	}
	if err := f.Sync(); err != nil {
		return name, fmt.Errorf("sync snapshot: %w", err)
	}
	if err := f.Close(); err != nil {
		return name, fmt.Errorf("close snapshot: %w", err)
	}
	copy, err := openSnapshot(name)
	if err != nil {
		return name, fmt.Errorf("open completed snapshot: %w", err)
	}
	err = copy.View(validateSnapshot)
	closeErr := copy.Close()
	if err != nil {
		return name, fmt.Errorf("validate completed snapshot: %w", err)
	}
	if closeErr != nil {
		return name, fmt.Errorf("close completed snapshot: %w", closeErr)
	}
	return name, nil
}
