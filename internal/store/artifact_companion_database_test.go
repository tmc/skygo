//go:build linux || darwin

package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// companionDatabaseLayout observes the actual committed artifact bucket through
// bbolt's supported page API. The original lock owner stays in the fixture until
// its one observed Close succeeds. No unsafe page cast or fabricated receipt is
// used to locate the later corruption.
type companionDatabaseLayout struct {
	logicalSize           int64
	pageSize              int
	statePage, statePages int
	state                 []byte
}

func inspectCompanionDatabase(t *testing.T, f *companionFixture, path string) companionDatabaseLayout {
	t.Helper()
	var layout companionDatabaseLayout
	var err error
	f.lockCloseAttempted = false
	f.lockedDB, err = bolt.Open(path, 0600, &bolt.Options{NoFreelistSync: true, Timeout: time.Second})
	if err != nil {
		f.fatal(t, err)
	}
	layout.pageSize = f.lockedDB.Info().PageSize
	viewErr := f.lockedDB.View(func(tx *bolt.Tx) error {
		if err := validateSnapshot(tx); err != nil {
			return err
		}
		layout.logicalSize = tx.Size()
		bucket := tx.Bucket(artifactsBucket)
		if bucket == nil || bucket.Root() == 0 {
			return errors.New("fixture has no physical artifact bucket root")
		}
		page, err := tx.Page(int(bucket.Root()))
		if err != nil {
			return err
		}
		if page == nil || page.Type != "leaf" {
			return errors.New("fixture artifact state does not occupy a leaf root")
		}
		layout.statePage, layout.statePages = page.ID, page.OverflowCount+1
		layout.state = bytes.Clone(bucket.Get([]byte("state")))
		if len(layout.state) == 0 {
			return errors.New("fixture artifact state is missing")
		}
		return nil
	})
	f.lockCloseAttempted = true
	closeErr := f.lockedDB.Close()
	if closeErr == nil {
		f.lockedDB = nil
	} else {
		f.hold(closeErr)
	}
	if err := errors.Join(viewErr, closeErr); err != nil {
		f.fatal(t, err)
	}
	return layout
}

func TestCompanionBackupPreservesPhysicalDatabase(t *testing.T) {
	f := newCompanionFixture(t, true)
	path := filepath.Join(f.data, "jobs.db")
	layout := inspectCompanionDatabase(t, f, path)
	original, err := os.ReadFile(path)
	if err != nil {
		f.fatal(t, err)
	}
	if int64(len(original)) < layout.logicalSize || layout.pageSize <= 0 {
		f.fatal(t, errors.New("fixture database is shorter than its committed size"))
	}
	// Add an allocation-tail canary outside the entire original physical file.
	// It is not reachable from either meta page and cannot alter committed rows.
	// A transactional WriteTo snapshot would omit it and rewrite meta pages.
	physical := append(bytes.Clone(original), make([]byte, layout.pageSize)...)
	copy(physical[len(original):], []byte("skygo companion physical allocation tail"))
	if err := os.WriteFile(path, physical, 0600); err != nil {
		f.fatal(t, err)
	}
	before := observeCompanionSource(t, f)
	base := filepath.Dir(f.data)
	bundle, restored := filepath.Join(base, "raw-backup"), filepath.Join(base, "raw-restored")
	// Exercise the public synchronous path. There is no writer callback, nested
	// transaction, detached copy goroutine or substitute source descriptor here.
	if err := f.operation(BackupCompanion(context.Background(), f.data, f.artifacts, bundle)); err != nil {
		f.fatal(t, err)
	}
	copied, err := os.ReadFile(filepath.Join(bundle, "jobs.db"))
	if err != nil {
		f.fatal(t, err)
	}
	if !bytes.Equal(copied, physical) || int64(len(copied)) <= layout.logicalSize {
		f.fatal(t, errors.New("backup did not preserve both meta pages and the full physical allocation tail"))
	}
	manifestBytes, err := os.ReadFile(filepath.Join(bundle, "manifest.json"))
	if err != nil {
		f.fatal(t, err)
	}
	var manifest companionManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		f.fatal(t, err)
	}
	hash := sha256.Sum256(physical)
	if manifest.DatabaseSize != uint64(len(physical)) || manifest.DatabaseSHA256 != hex.EncodeToString(hash[:]) {
		f.fatal(t, errors.New("manifest does not describe full physical database bytes"))
	}
	bundleBefore := observeCompanionPaths(t, bundle)
	if err := f.operation(RestoreCompanion(context.Background(), bundle, restored)); err != nil {
		f.fatal(t, err)
	}
	unchangedCompanionSource(t, f, before)
	unchangedCompanionPaths(t, bundleBefore, observeCompanionPaths(t, bundle))
	f.restored, err = Open(restored)
	if err != nil {
		f.fatal(t, err)
	}
	f.restoredCloseAttempted = false
	owner := f.restored.Owner()
	required, queryErr := f.restored.ArtifactRequiredObjects()
	closeErr := f.closeRestored()
	if err := errors.Join(queryErr, closeErr); err != nil {
		f.fatal(t, err)
	}
	if owner != f.owner || len(required) != 0 {
		f.fatal(t, errors.New("raw database backup did not restore the unconsumed-upload installation"))
	}
}

func TestCompanionRestoreRejectsDatabaseDigestBeforeParsing(t *testing.T) {
	f := newCompanionFixture(t, true)
	base := filepath.Dir(f.data)
	bundle, restored := filepath.Join(base, "digest-backup"), filepath.Join(base, "digest-restored")
	if err := f.operation(BackupCompanion(context.Background(), f.data, f.artifacts, bundle)); err != nil {
		f.fatal(t, err)
	}
	path := filepath.Join(bundle, "jobs.db")
	layout := inspectCompanionDatabase(t, f, path)
	original, err := os.ReadFile(path)
	if err != nil {
		f.fatal(t, err)
	}
	start, end := layout.statePage*layout.pageSize, (layout.statePage+layout.statePages)*layout.pageSize
	if start < 2*layout.pageSize || end > len(original) {
		f.fatal(t, errors.New("committed state page overlaps meta pages or exceeds database"))
	}
	page := original[start:end]
	if bytes.Count(page, layout.state) != 1 {
		f.fatal(t, errors.New("committed artifact state is not unique in its actual leaf page"))
	}
	var root artifactRoot
	if err := json.Unmarshal(layout.state, &root); err != nil {
		f.fatal(t, err)
	}
	marker := bytes.Index(layout.state, []byte(root.ID))
	if len(root.ID) != 32 || marker < 0 {
		f.fatal(t, errors.New("committed artifact root marker is missing"))
	}
	// Change one value byte in the actual reachable leaf, keeping the page
	// headers, both meta pages, JSON width, manifest and file length unchanged.
	tampered := bytes.Clone(original)
	offset := start + bytes.Index(page, layout.state) + marker
	tampered[offset] = 'd'
	if tampered[offset] == original[offset] {
		tampered[offset] = 'e'
	}
	if len(tampered) != len(original) || !bytes.Equal(tampered[:2*layout.pageSize], original[:2*layout.pageSize]) {
		f.fatal(t, errors.New("tamper fixture changed database size or meta pages"))
	}
	if err := os.WriteFile(path, tampered, 0600); err != nil {
		f.fatal(t, err)
	}
	before := observeCompanionPaths(t, base)
	err = f.operation(RestoreCompanion(context.Background(), bundle, restored))
	var failure *companionError
	if !errors.As(err, &failure) || failure.cause.Error() != "companion database digest differs" {
		f.fatal(t, errors.Join(errors.New("tampered database did not fail at the exact digest guard"), err))
	}
	op := failure.operation
	// These are constructor-transfer observations, not a native lock/FD proof:
	// successful pre-transfer file Close leaves the bbolt owner slot untouched.
	if op.sourceDB.db != nil || op.sourceDB.file != nil || op.sourceDB.err != nil || op.sourceDB.attempted || op.stagePath != "" || op.published {
		f.fatal(t, errors.Join(errors.New("tampered bytes reached bbolt ownership or stage creation"), err))
	}
	if _, err := os.Lstat(restored); !errors.Is(err, os.ErrNotExist) {
		f.fatal(t, errors.Join(errors.New("digest refusal published a destination"), err))
	}
	unchangedCompanionPaths(t, before, observeCompanionPaths(t, base))
}
