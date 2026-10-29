//go:build darwin || linux

package ec2job

import (
	"errors"
	"io"
	"os"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
)

// These tests inject, through snapshotDir's private archive and closeRoot
// arguments, an archive error carrying artifact.ErrFileCloseUncertain and
// a failed first Root.Close. Both are MODELS: the archive error is not an
// uncertain native close of an archive file, and the Close callback returns
// an explicit failure without closing its real root. The root and workdir
// are real.

// errUncertain is a MODEL archive error carrying the core sentinel.
var errUncertain = errors.Join(errors.New("model: archive file close"), artifact.ErrFileCloseUncertain)

// TestSnapshotUncertainArchiveKeepsRoot: the archive's file Close is
// uncertain. Snapshot never closes the root: the error keeps the same
// open root and the archive's own error.
func TestSnapshotUncertainArchiveKeepsRoot(t *testing.T) {
	dir := keptDir(t) // the kept root stays open below it
	if err := os.WriteFile(dir+"/input.txt", []byte("input\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var got *os.Root
	closes := 0
	archive := func(r *os.Root, w io.Writer) error { got = r; return errUncertain }
	closeRoot := func(r *os.Root) error { closes++; return r.Close() }
	a, d, err := snapshotDir(dir, archive, closeRoot)
	var re *rootError
	switch {
	case a != nil || d != "":
		t.Fatal("an unsettled snapshot returned an archive")
	case !errors.Is(err, ErrUnsettled) || !errors.Is(err, artifact.ErrFileCloseUncertain) || !errors.As(err, &re):
		t.Fatalf("snapshotDir = %v; want ErrUnsettled with the archive's error", err)
	case re.root != got || closes != 0:
		t.Fatalf("root %p kept, %p archived, %d Closes; want the same root, never closed", re.root, got, closes)
	}
	if _, err := got.Lstat("input.txt"); err != nil {
		t.Fatalf("the kept root is not open: %v", err)
	}
	retain(err)
}

// TestSnapshotRootCloseFails: the archive fails and the root's first
// Close then fails too (MODEL). Both causes are returned, and the root is
// kept and never closed again.
func TestSnapshotRootCloseFails(t *testing.T) {
	dir := keptDir(t) // the error keeps the root
	if err := os.WriteFile(dir+"/input.txt", []byte("input\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	primary := errors.New("model: archive failed")
	closeErr := errors.New("model: root close failed")
	closes := 0
	var got *os.Root
	var err error
	// Retain the same returned owner even if an assertion ends the test.
	t.Cleanup(func() { retain(err) })
	closeRoot := func(r *os.Root) error {
		closes++
		got = r
		return closeErr // MODEL: the real root remains open and owned
	}
	_, _, err = snapshotDir(dir, func(*os.Root, io.Writer) error { return primary }, closeRoot)
	var re *rootError
	if !errors.Is(err, ErrUnsettled) || !errors.Is(err, primary) || !errors.Is(err, closeErr) || !errors.As(err, &re) || re.root == nil || re.root != got || closes != 1 {
		t.Fatalf("snapshotDir = %v, %d Closes; want every cause and the same root, closed once", err, closes)
	}
	if _, err := got.Lstat("input.txt"); err != nil {
		t.Fatalf("the kept root is not open: %v", err)
	}
}

// TestSnapshotOrdinaryFailure: the twin. The archive fails and the root's
// Close succeeds: the error is ordinary, keeps nothing and permits
// cleanup.
func TestSnapshotOrdinaryFailure(t *testing.T) {
	dir := newWorkdir(t, map[string]string{"input.txt": "input\n"})
	primary := errors.New("model: archive failed")
	_, _, err := snapshotDir(dir, func(*os.Root, io.Writer) error { return primary }, (*os.Root).Close)
	var re *rootError
	if !errors.Is(err, primary) || errors.Is(err, ErrUnsettled) || errors.As(err, &re) {
		t.Fatalf("snapshotDir = %v; want the ordinary archive error", err)
	}
	if _, _, err := snapshotDir(dir, artifact.Snapshot, (*os.Root).Close); err != nil {
		t.Fatalf("snapshotDir = %v", err)
	}
}
