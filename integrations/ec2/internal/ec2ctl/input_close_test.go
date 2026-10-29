//go:build darwin || linux

package ec2ctl

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// These tests make a file's Close fail by closing the real file first,
// through beforeClose or the parent sync function, just before this
// package's one Close of it, which then returns the real error for a file
// already closed. That is a MODEL of a failed first Close: it is neither
// an uncertain native close nor a crash, and the descriptor is released.

// retainedErrors keeps every unsettled error a test made reachable for the
// life of the test process.
var retainedErrors []error

// keptDir returns a new directory that is never removed: an error a test
// keeps holds a file below it.
func keptDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "ec2ctl-unsettled-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("keeping %s: an unsettled error holds a file below it", dir)
	return dir
}

// precloseFiles sets beforeClose, until the test ends, to close each file
// whose name has suffix and record it in *got.
func precloseFiles(t *testing.T, suffix string, got *[]*os.File) {
	beforeClose = func(f *os.File) {
		if !strings.HasSuffix(f.Name(), suffix) {
			return
		}
		*got = append(*got, f)
		if err := f.Close(); err != nil {
			t.Errorf("native close of %s: %v", f.Name(), err)
		}
	}
	t.Cleanup(func() { beforeClose = nil })
}

// keeps reports whether some closeError in err's tree keeps f.
func keeps(err error, f *os.File) bool {
	switch e := err.(type) {
	case nil:
		return false
	case *closeError:
		return e.f == f || keeps(e.err, f)
	case interface{ Unwrap() []error }:
		for _, x := range e.Unwrap() {
			if keeps(x, f) {
				return true
			}
		}
	case interface{ Unwrap() error }:
		return keeps(e.Unwrap(), f)
	}
	return false
}

// TestLoadConfigCloseKept: the configuration file's Close fails. LoadConfig
// returns no configuration and an error wrapping ErrUnsettled that keeps
// the original file. The twin loads it.
func TestLoadConfigCloseKept(t *testing.T) {
	dir := keptDir(t)
	name := filepath.Join(dir, "config.json")
	must(t, os.WriteFile(name, []byte(goodConfig), 0o600))
	if _, err := LoadConfig(name); err != nil {
		t.Fatalf("LoadConfig = %v", err)
	}
	var got []*os.File
	precloseFiles(t, "config.json", &got)
	c, err := LoadConfig(name)
	if c != nil || len(got) != 1 || !errors.Is(err, ErrUnsettled) || !errors.Is(err, os.ErrClosed) || !keeps(err, got[0]) {
		t.Fatalf("LoadConfig = %v, %v; want ErrUnsettled keeping the file", c, err)
	}
	retainedErrors = append(retainedErrors, err)
}

// TestReadPrivateRefusalAndCloseKept: a file others can read is refused,
// and its Close fails too: both causes are returned. The twin, whose Close
// succeeds, is an ordinary refusal.
func TestReadPrivateRefusalAndCloseKept(t *testing.T) {
	dir := keptDir(t)
	name := filepath.Join(dir, "open.json")
	if err := os.WriteFile(name, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(name, 0o644); err != nil {
		t.Fatal(err)
	}
	_, plain := readPrivate(name, 1<<10)
	if plain == nil || errors.Is(plain, ErrUnsettled) {
		t.Fatalf("readPrivate = %v; want an ordinary refusal", plain)
	}
	var got []*os.File
	precloseFiles(t, "open.json", &got)
	data, err := readPrivate(name, 1<<10)
	if data != nil || len(got) != 1 || !errors.Is(err, ErrUnsettled) || !strings.Contains(err.Error(), plain.Error()) || !keeps(err, got[0]) {
		t.Fatalf("readPrivate = %q, %v; want the refusal and the kept Close", data, err)
	}
	retainedErrors = append(retainedErrors, err)
}

// TestReadStatusDirectoryCloseKept: the state directory's Close fails
// before the state is read. ReadStatus returns an error keeping it.
func TestReadStatusDirectoryCloseKept(t *testing.T) {
	e := newEnv(t)
	e.dir = filepath.Join(keptDir(t), "vm")
	e.create().Close()
	var got []*os.File
	precloseFiles(t, "vm", &got)
	_, err := ReadStatus(e.dir, e.cfg)
	if len(got) != 1 || !errors.Is(err, ErrUnsettled) || !keeps(err, got[0]) {
		t.Fatalf("ReadStatus = %v; want ErrUnsettled keeping the directory", err)
	}
	retainedErrors = append(retainedErrors, err)
}

// TestParentSyncAndCloseFail: the parent's sync and its Close both fail.
// Both causes are returned, the parent file is kept, and no lock is taken.
func TestParentSyncAndCloseFail(t *testing.T) {
	e := newEnv(t)
	e.dir = filepath.Join(keptDir(t), "vm")
	var parent *os.File
	fail := func(f *os.File) error {
		parent = f
		if err := f.Close(); err != nil {
			t.Errorf("native close: %v", err)
		}
		return errFault
	}
	m, err := createMachine(e.dir, e.cfg, fail)
	if m != nil || !errors.Is(err, errFault) || !errors.Is(err, ErrUnsettled) || !errors.Is(err, os.ErrClosed) || !keeps(err, parent) {
		t.Fatalf("createMachine = %v, %v; want both causes and the kept parent", m, err)
	}
	if _, err := os.Lstat(filepath.Join(e.dir, lockName)); err == nil {
		t.Fatal("the lock was taken")
	}
	if e.srv.Total() != 0 {
		t.Fatalf("%d requests", e.srv.Total())
	}
	retainedErrors = append(retainedErrors, err)
}

// TestOpenDirRefusalAndCloseKept: a state directory others can read is
// refused, and its Close fails too: Open returns both causes, keeps the
// directory and takes no lock. The twin is an ordinary refusal.
func TestOpenDirRefusalAndCloseKept(t *testing.T) {
	e := newEnv(t)
	e.dir = filepath.Join(keptDir(t), "vm")
	e.create().Close()
	if err := os.Chmod(e.dir, 0o755); err != nil {
		t.Fatal(err)
	}
	_, plain := Open(e.dir, e.cfg)
	if plain == nil || errors.Is(plain, ErrUnsettled) {
		t.Fatalf("Open = %v; want an ordinary refusal", plain)
	}
	var got []*os.File
	precloseFiles(t, "vm", &got)
	m, err := Open(e.dir, e.cfg)
	if m != nil || len(got) != 1 || !errors.Is(err, ErrUnsettled) || !strings.Contains(err.Error(), plain.Error()) || !keeps(err, got[0]) {
		t.Fatalf("Open = %v, %v; want the refusal and the kept directory", m, err)
	}
	retainedErrors = append(retainedErrors, err)
}
