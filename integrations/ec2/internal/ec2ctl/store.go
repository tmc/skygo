package ec2ctl

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// Names in a state directory.
const (
	lockName  = "lock"        // flock target; never removed
	stateName = "vm.json"     // the committed state
	tempName  = "vm.json.tmp" // an uncommitted save
	maxState  = 256 << 10     // bytes of an encoded state
)

// A store is an open, locked state directory.
//
// Every save writes a new temporary file, syncs it, renames it over the
// state file and syncs the directory. A save is one self-contained file
// operation: no file is left open for writing while the caller makes a
// provider call. A failed first Close keeps the temporary file, directory
// and lock; neither another save nor close acts on them again.
type store struct {
	dir  string
	d    *os.File // the directory, kept open for fsync
	lock *os.File // holds the exclusive flock

	// fault, if set, is called at each named stage of a save and fails
	// it by returning an error. It is set only by this package's tests;
	// the writes, syncs and renames around it are real.
	fault func(stage string) error

	// failed keeps the original temporary file whose first Close failed.
	// Its state is unknown: nothing saves again or releases the lock.
	failed *closeError

	closed   bool  // close was called
	closeErr error // the first close's result
}

// ErrUnsettled reports the failed first Close of a file this package
// opened, so that the file's state is unknown. The error keeps the file,
// or the store whose lock and directory it closed, reachable, and joins
// every independent cause; nothing closes the file again. The caller must
// keep the error and act on nothing the file guards.
var ErrUnsettled = errors.New("an original file close did not settle")

// A closeError is the failed first Close of f, or of s's lock or
// directory. It keeps f or s reachable while the error is.
type closeError struct {
	f   *os.File
	s   *store
	err error
}

func (e *closeError) Error() string   { return e.err.Error() }
func (e *closeError) Unwrap() []error { return []error{ErrUnsettled, e.err} }

// beforeClose, if set, is called with each file closeFile closes, just
// before its one Close. Only this package's tests set it: closing the file
// first is their MODEL of a failed Close.
var beforeClose func(*os.File)

// closeFile closes f once. A failed Close returns a *closeError keeping f.
func closeFile(f *os.File) error {
	if beforeClose != nil {
		beforeClose(f)
	}
	if err := f.Close(); err != nil {
		return &closeError{f: f, err: err}
	}
	return nil
}

// openStore opens the state directory dir and takes its lock without
// waiting. If create is set and dir does not exist, openStore creates it,
// and reports created. A lock held by another process is ErrBusy.
//
// Before it opens dir, openStore syncs dir's parent directory with
// syncParent, which is syncDir except in this package's tests, so that
// dir's entry is durable before anything is stored in it or any call acts
// on it. It does so on every open, not only when it creates dir: an
// earlier process may have created dir and failed or crashed before the
// sync. If the parent cannot be opened, synced or closed, openStore fails
// and takes neither the lock nor any other step.
func openStore(dir string, create bool, syncParent func(*os.File) error) (s *store, created bool, err error) {
	if dir == "" {
		return nil, false, errors.New("ec2ctl: no state directory")
	}
	dir = filepath.Clean(dir)
	if create {
		switch err := os.Mkdir(dir, 0o700); {
		case err == nil:
			created = true
		case !errors.Is(err, fs.ErrExist):
			return nil, false, fmt.Errorf("create state directory: %w", err)
		}
	}
	if _, err := os.Lstat(dir); errors.Is(err, fs.ErrNotExist) {
		return nil, false, fmt.Errorf("%w: %s does not exist", ErrNoState, dir)
	}
	if err := syncParentDir(filepath.Dir(dir), syncParent); err != nil {
		return nil, false, err
	}
	d, err := openDir(dir)
	if err != nil {
		return nil, false, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, lockName), os.O_RDWR|os.O_CREATE|openNoFollow, 0o600)
	if err != nil {
		return nil, false, errors.Join(fmt.Errorf("open state lock: %w", err), closeFile(d))
	}
	if err := checkOpen(lock, filepath.Join(dir, lockName), false); err != nil {
		return nil, false, errors.Join(err, closeFile(lock), closeFile(d))
	}
	if err := lockFile(lock); err != nil {
		return nil, false, errors.Join(err, closeFile(lock), closeFile(d))
	}
	s = &store{dir: dir, d: d, lock: lock}
	// A temporary file is a save that never committed: remove it.
	if err := os.Remove(s.path(tempName)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, false, errors.Join(fmt.Errorf("remove uncommitted state: %w", err), s.close())
	}
	return s, created, nil
}

// syncParentDir opens the directory parent, which holds the state
// directory's entry, syncs it with sync and closes it. The parent is not
// checked to be private: see the package documentation. A failed sync
// and a failed Close are both returned.
func syncParentDir(parent string, sync func(*os.File) error) error {
	p, err := os.OpenFile(parent, os.O_RDONLY|openDirectory, 0)
	if err != nil {
		return fmt.Errorf("sync parent directory: %w", err)
	}
	err = sync(p)
	if cerr := closeFile(p); cerr != nil {
		err = errors.Join(err, cerr)
	}
	if err != nil {
		return fmt.Errorf("sync parent directory %s: %w", parent, err)
	}
	return nil
}

// openDir opens the private directory dir, refusing a symbolic link.
func openDir(dir string) (*os.File, error) {
	d, err := os.OpenFile(dir, os.O_RDONLY|openDirectory|openNoFollow, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s does not exist", ErrNoState, dir)
	}
	if err != nil {
		return nil, fmt.Errorf("open state directory: %w", err)
	}
	if err := checkOpen(d, dir, true); err != nil {
		return nil, errors.Join(err, closeFile(d))
	}
	return d, nil
}

// checkOpen checks that the open file f, named name, is private.
func checkOpen(f *os.File, name string, dir bool) error {
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	return checkPrivate(name, fi, dir)
}

// readPrivate reads the private regular file name, which is at most max
// bytes. Errors never quote the content.
//
// The open does not wait: a FIFO with no writer opens at once instead of
// blocking, and like a directory, device or socket it is then refused
// before anything is read. A regular file ignores the nonblocking flag.
//
// The file is closed once. If that Close fails, readPrivate returns no
// data and an error wrapping [ErrUnsettled] that keeps the file, joined
// with any earlier error.
func readPrivate(name string, max int64) (data []byte, err error) {
	f, err := os.OpenFile(name, os.O_RDONLY|openNoFollow|openNonblock, 0)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := closeFile(f); cerr != nil {
			data, err = nil, errors.Join(err, cerr)
		}
	}()
	if err := checkOpen(f, name, false); err != nil {
		return nil, err
	}
	data, err = io.ReadAll(io.LimitReader(f, max+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", name, err)
	}
	if int64(len(data)) > max {
		return nil, fmt.Errorf("%s exceeds %d bytes", name, max)
	}
	return data, nil
}

// readState reads the state file of directory dir without its lock. A
// committed state file is only ever replaced by rename, so the bytes are
// one whole committed state. A failed Close of the directory is returned
// before anything is read.
func readState(dir string) ([]byte, error) {
	dir = filepath.Clean(dir)
	d, err := openDir(dir)
	if err != nil {
		return nil, err
	}
	if err := closeFile(d); err != nil {
		return nil, err
	}
	return readStateFile(filepath.Join(dir, stateName))
}

// readStateFile reads the state file name. A missing file is ErrNoState.
func readStateFile(name string) ([]byte, error) {
	data, err := readPrivate(name, maxState)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s does not exist", ErrNoState, name)
	}
	return data, err
}

func (s *store) path(name string) string { return filepath.Join(s.dir, name) }

func (s *store) load() ([]byte, error) {
	return readStateFile(s.path(stateName))
}

func (s *store) fail(stage string) error {
	if s.fault == nil {
		return nil
	}
	if err := s.fault(stage); err != nil {
		return fmt.Errorf("%s: %w", stage, err)
	}
	return nil
}

// save durably replaces the state file with data. When save fails after
// the rename, the new state may or may not survive a crash: the caller
// must save it again before acting on it.
func (s *store) save(data []byte) error {
	if s.failed != nil {
		return s.failed
	}
	if len(data) > maxState {
		return fmt.Errorf("save state: %d bytes exceeds %d", len(data), maxState)
	}
	tmp := s.path(tempName)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL|openNoFollow, 0o600)
	if err != nil {
		return fmt.Errorf("save state: %w", err)
	}
	err = s.fail("write")
	if err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = s.fail("sync")
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := closeFile(f); cerr != nil {
		s.failed = &closeError{f: f, s: s, err: errors.Join(err, cerr)}
		return fmt.Errorf("save state: %w", s.failed)
	}
	if err == nil {
		err = s.fail("rename")
	}
	if err == nil {
		err = os.Rename(tmp, s.path(stateName))
	}
	if err != nil {
		os.Remove(tmp)
		return fmt.Errorf("save state: %w", err)
	}
	if err := s.fail("sync directory"); err != nil {
		return fmt.Errorf("save state: %w", err)
	}
	if err := syncDir(s.d); err != nil {
		return fmt.Errorf("save state: sync directory: %w", err)
	}
	return nil
}

// close releases the lock and closes the directory, once: every call
// returns the first call's result. If either Close fails, the error wraps
// ErrUnsettled, joins both causes and keeps s, whose files are never
// closed again.
func (s *store) close() error {
	if !s.closed {
		s.closed = true
		if s.failed != nil {
			s.closeErr = s.failed
			return s.closeErr
		}
		if err := errors.Join(closeFile(s.lock), closeFile(s.d)); err != nil {
			s.closeErr = &closeError{s: s, err: err}
		}
	}
	return s.closeErr
}
