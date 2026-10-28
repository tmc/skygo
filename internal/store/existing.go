package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

// ErrNotCurrent reports a data directory that holds no existing, initialized,
// scoped installation at exactly the current schema. InspectExisting and
// OpenExisting never create, initialize or migrate one; Open does.
var ErrNotCurrent = errors.New("not an existing current installation")

// ErrInstallationChanged reports that the data directory or its database no
// longer matches the facts InspectExisting read.
var ErrInstallationChanged = errors.New("existing installation changed")

// ErrCloseUncertain means InspectExisting or OpenExisting attempted to close an
// original database and did not prove closure. This includes a failed bbolt
// Open after the descriptor was handed to bbolt, which closes it and discards
// the result. The caller must keep the error and stop; it must not reopen,
// retry or clean up beneath it. Use errors.Is.
var ErrCloseUncertain = errors.New("job database closure is uncertain")

// closeError retains the original database and descriptor whose Close did not
// prove closure. db is nil when bbolt closed the descriptor itself.
type closeError struct {
	db   *bolt.DB
	file *os.File
	err  error
}

func (e *closeError) Error() string        { return "close job database: " + e.err.Error() }
func (e *closeError) Unwrap() error        { return e.err }
func (e *closeError) Is(target error) bool { return target == ErrCloseUncertain }

// closeExistingDB closes a database opened by InspectExisting or OpenExisting.
// Tests replace it to model an operating system Close failure.
var closeExistingDB = (*bolt.DB).Close

// existingAcquiredHook, if set, runs after OpenExisting takes the writer lock
// on its descriptor, before the final descriptor check and before bbolt reads
// the file. Tests use it to model interleavings.
var existingAcquiredHook func()

// ExistingInstallation records one existing installation read by
// InspectExisting: the data directory and database file identities, the
// validated bbolt transaction, the owner and the installed artifact facts.
// The zero value is invalid.
type ExistingInstallation struct {
	dir, file os.FileInfo
	tx        int
	owner     string
	artifacts ArtifactInstallation
}

// Owner returns the persistent installation identity.
func (i ExistingInstallation) Owner() string { return i.owner }

// ArtifactInstallation returns the persisted artifact configuration. Like
// Store.ArtifactInstallation, it does not certify a live root marker.
func (i ExistingInstallation) ArtifactInstallation() ArtifactInstallation { return i.artifacts }

// read validates the current installation in tx and records its facts.
func (i *ExistingInstallation) read(tx *bolt.Tx) error {
	if err := validateCurrent(tx); err != nil {
		return err
	}
	artifacts, err := artifactInstallation(tx)
	if err != nil {
		return err
	}
	i.tx = tx.ID()
	i.owner = string(tx.Bucket(metadataBucket).Get([]byte("owner")))
	i.artifacts = artifacts
	return nil
}

func (i ExistingInstallation) same(o ExistingInstallation) bool {
	return os.SameFile(i.dir, o.dir) && os.SameFile(i.file, o.file) && i.tx == o.tx && i.owner == o.owner && i.artifacts == o.artifacts
}

// validateCurrent accepts only an installation that Open would neither
// initialize nor migrate: exactly the current schema, an owner, scoped
// authentication, and every root and counter initialize creates. The whole
// snapshot is then checked by validateSnapshot.
func validateCurrent(tx *bolt.Tx) error {
	if err := inspect(tx); err != nil {
		return err
	}
	m := tx.Bucket(metadataBucket)
	switch {
	case m == nil || schemaNumber(tx) != schemaVersion:
		return fmt.Errorf("%w: schema %d, current %d", ErrNotCurrent, schemaNumber(tx), schemaVersion)
	case len(m.Get([]byte("owner"))) == 0:
		return fmt.Errorf("%w: missing installation owner", ErrNotCurrent)
	case !isScoped(tx):
		return fmt.Errorf("%w: scoped authentication not active", ErrNotCurrent)
	case m.Get(taskBytesKey) == nil || m.Get(retryBytesKey) == nil || tx.Bucket(attemptJobsBucket) == nil:
		return fmt.Errorf("%w: missing counter", ErrNotCurrent)
	case retentionRoot(tx) == nil:
		return fmt.Errorf("%w: missing retention metadata", ErrNotCurrent)
	}
	for _, name := range retentionTables {
		if retentionTable(tx, name) == nil {
			return fmt.Errorf("%w: missing retention table", ErrNotCurrent)
		}
	}
	return validateSnapshot(tx)
}

// InspectExisting reads the installation in dir without creating,
// initializing, migrating or writing anything. dir must be a directory and
// dir/jobs.db an existing nonempty regular file, neither a symbolic link. The
// database must hold an initialized, scoped installation at exactly the
// current schema; one that Open would initialize or migrate is refused with
// ErrNotCurrent. Another writer holding the database is refused.
//
// The read-only database is closed before InspectExisting returns. An error
// matching ErrCloseUncertain retains it. Unsupported platforms refuse before
// opening the database.
func InspectExisting(dir string) (ExistingInstallation, error) {
	dirInfo, err := existingDirectory(dir)
	if err != nil {
		return ExistingInstallation{}, err
	}
	path := filepath.Join(dir, "jobs.db")
	o := &existingOpen{}
	db, err := bolt.Open(path, 0600, &bolt.Options{ReadOnly: true, Timeout: time.Second, OpenFile: o.open})
	if err != nil {
		return ExistingInstallation{}, o.failed(fmt.Errorf("open job database: %w", err))
	}
	out := ExistingInstallation{dir: dirInfo, file: o.info}
	err = db.View(out.read)
	if err == nil {
		err = existingPaths(dir, path, dirInfo, o.info)
	}
	if closeErr := closeExistingDB(db); closeErr != nil {
		return ExistingInstallation{}, &closeError{db: db, file: o.file, err: errors.Join(err, closeErr)}
	}
	if err != nil {
		return ExistingInstallation{}, err
	}
	return out, nil
}

// OpenExisting acquires the writer lock of the installation expected
// describes and returns its Store. It never creates, initializes, migrates or
// writes the database. Holding the lock, it repeats InspectExisting's
// validation and refuses with ErrInstallationChanged unless the directory,
// database file, validated transaction, owner and artifact facts all still
// match expected. A replaced or removed database, or any write committed
// since InspectExisting, is refused.
//
// On refusal the database is closed once; an error matching
// ErrCloseUncertain retains it. Writes that bypass bbolt's lock are outside
// this guarantee.
func OpenExisting(dir string, expected ExistingInstallation) (*Store, error) {
	if expected.dir == nil || expected.file == nil || expected.owner == "" {
		return nil, errors.New("invalid existing installation")
	}
	dirInfo, err := existingDirectory(dir)
	if err != nil {
		return nil, err
	}
	if !os.SameFile(dirInfo, expected.dir) {
		return nil, ErrInstallationChanged
	}
	path := filepath.Join(dir, "jobs.db")
	o := &existingOpen{writer: true}
	// NoFreelistSync keeps bbolt from writing a freelist during Open, before
	// the installation is accepted.
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second, NoFreelistSync: true, OpenFile: o.open})
	if err != nil {
		return nil, o.failed(fmt.Errorf("open job database: %w", err))
	}
	actual := ExistingInstallation{dir: dirInfo, file: o.info}
	err = db.View(actual.read)
	if err == nil && !actual.same(expected) {
		err = ErrInstallationChanged
	}
	if err == nil {
		err = existingPaths(dir, path, dirInfo, o.info)
	}
	if err != nil {
		if closeErr := closeExistingDB(db); closeErr != nil {
			return nil, &closeError{db: db, file: o.file, err: errors.Join(err, closeErr)}
		}
		return nil, err
	}
	db.NoFreelistSync = false
	return &Store{db: db, owner: actual.owner, scoped: true, now: func() time.Time { return time.Now().UTC() }}, nil
}

func existingDirectory(dir string) (os.FileInfo, error) {
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, fmt.Errorf("inspect data directory: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%w: data directory is not a directory", ErrNotCurrent)
	}
	return info, nil
}

// existingPaths reports whether dir and path still name the original
// directory and database file.
func existingPaths(dir, path string, dirInfo, file os.FileInfo) error {
	d, err := os.Lstat(dir)
	if err != nil || !d.IsDir() || !os.SameFile(d, dirInfo) {
		return ErrInstallationChanged
	}
	f, err := os.Lstat(path)
	if err != nil || !f.Mode().IsRegular() || !os.SameFile(f, file) {
		return ErrInstallationChanged
	}
	return nil
}

// existingOpen is the bbolt OpenFile hook for an existing database. It never
// creates the file: bbolt's O_CREATE is not forwarded. It owns its descriptor
// until it returns it; from then on bbolt alone closes it.
type existingOpen struct {
	writer bool
	file   *os.File
	info   os.FileInfo
}

func (o *existingOpen) open(name string, flag int, _ os.FileMode) (*os.File, error) {
	want := os.O_RDONLY
	if o.writer {
		want = os.O_RDWR
	}
	if o.file != nil || flag&(os.O_RDONLY|os.O_WRONLY|os.O_RDWR) != want {
		return nil, errors.New("unexpected job database open")
	}
	f, info, err := openExistingFile(name, o.writer, time.Second)
	if err != nil {
		return nil, err
	}
	o.file, o.info = f, info
	return f, nil
}

// failed classifies a bbolt Open failure. Once the hook has returned its
// descriptor, bbolt closed it and discarded that Close's result.
func (o *existingOpen) failed(err error) error {
	if o.file == nil {
		return err
	}
	return &closeError{file: o.file, err: err}
}
