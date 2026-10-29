//go:build darwin || linux

package ec2ctl

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// These tests close a store's real lock or directory first, so that the
// store's one Close of it fails (MODEL of a failed first Close, as
// input_close_test.go describes).

// TestMachineCloseKeepsStore: both the lock's and the directory's Close
// fail. Close returns both causes in an error wrapping ErrUnsettled that
// keeps the original store; the Machine keeps that error. A second Close
// is os.ErrClosed, as before, and closes nothing again.
func TestMachineCloseKeepsStore(t *testing.T) {
	e := newEnv(t)
	e.dir = filepath.Join(keptDir(t), "vm")
	m := e.create()
	s := m.s
	var got []*os.File
	beforeClose = func(f *os.File) {
		got = append(got, f)
		if err := f.Close(); err != nil {
			t.Errorf("native close: %v", err)
		}
	}
	t.Cleanup(func() { beforeClose = nil })
	err := m.Close()
	var ce *closeError
	switch {
	case !errors.Is(err, ErrUnsettled) || !errors.Is(err, os.ErrClosed) || !errors.As(err, &ce) || ce.s != s:
		t.Fatalf("Close = %v; want ErrUnsettled keeping the store", err)
	case len(got) != 2 || got[0] != s.lock || got[1] != s.d || !keeps(err, s.lock) || !keeps(ce.err, s.d):
		t.Fatalf("closed %v; want the lock and the directory, each kept", got)
	case m.closeErr != err || m.s != nil:
		t.Fatal("the Machine does not keep the first Close's error")
	}
	if err := m.Close(); err != os.ErrClosed || len(got) != 2 {
		t.Fatalf("second Close = %v, %d Closes; want os.ErrClosed and none", err, len(got))
	}
	if err := s.close(); err != m.closeErr || len(got) != 2 {
		t.Fatalf("store close again = %v; want the first result, no Close", err)
	}
	retainedErrors = append(retainedErrors, err)
}

// TestMachineCloseHealthy: the twin. A clean Close returns nil, keeps no
// error, and a second Close is os.ErrClosed.
func TestMachineCloseHealthy(t *testing.T) {
	e := newEnv(t)
	m := e.create()
	if err := m.Close(); err != nil || m.closeErr != nil {
		t.Fatalf("Close = %v", err)
	}
	if err := m.Close(); err != os.ErrClosed {
		t.Fatalf("second Close = %v, want os.ErrClosed", err)
	}
}

// TestOpenRefusalJoinsClose: Open refuses a state of another
// configuration (ErrMismatch), and the store's Close then fails. Open
// returns both, keeping the store; the twin is the ordinary refusal.
func TestOpenRefusalJoinsClose(t *testing.T) {
	e := newEnv(t)
	e.dir = filepath.Join(keptDir(t), "vm")
	e.create().Close()
	other := testConfig()
	other.Identity.Generation = 2
	if _, err := Open(e.dir, other); !errors.Is(err, ErrMismatch) || errors.Is(err, ErrUnsettled) {
		t.Fatalf("Open = %v; want an ordinary ErrMismatch", err)
	}
	var got []*os.File
	precloseFiles(t, lockName, &got)
	m, err := Open(e.dir, other)
	var ce *closeError
	if m != nil || len(got) != 1 || !errors.Is(err, ErrMismatch) || !errors.Is(err, ErrUnsettled) || !errors.As(err, &ce) || ce.s == nil || !keeps(err, got[0]) {
		t.Fatalf("Open = %v, %v; want the refusal and the kept store", m, err)
	}
	if e.srv.Total() != 0 {
		t.Fatalf("%d requests", e.srv.Total())
	}
	retainedErrors = append(retainedErrors, err)
}

// Create opens existing state too. Its refusal must keep a failed original
// lock Close just as Open's does, with both causes and the same store.
func TestCreateRefusalJoinsClose(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "mismatch", true: "missing state"}[missing], func(t *testing.T) {
			e := newEnv(t)
			e.dir = filepath.Join(keptDir(t), "vm")
			var primary error
			cfg := e.cfg
			if missing {
				if err := os.Mkdir(e.dir, 0o700); err != nil {
					t.Fatal(err)
				}
				primary = ErrNoState
			} else {
				if err := e.create().Close(); err != nil {
					t.Fatal(err)
				}
				cfg = testConfig()
				cfg.Identity.Generation = 2
				primary = ErrMismatch
			}
			if _, err := Create(e.dir, cfg); !errors.Is(err, primary) || errors.Is(err, ErrUnsettled) {
				t.Fatalf("clean Create refusal = %v", err)
			}
			var got []*os.File
			precloseFiles(t, lockName, &got)
			m, err := Create(e.dir, cfg)
			t.Cleanup(func() { retainedErrors = append(retainedErrors, err) })
			var ce *closeError
			if m != nil || len(got) != 1 || !errors.Is(err, primary) || !errors.Is(err, ErrUnsettled) || !errors.As(err, &ce) || ce.s == nil || !keeps(err, got[0]) {
				t.Fatalf("Create = %v, %v; want both causes and the original store", m, err)
			}
			if ce.s.close() != ce.s.closeErr || len(got) != 1 {
				t.Fatal("cleanup retried an original Close")
			}
			if e.srv.Total() != 0 {
				t.Fatalf("%d provider requests", e.srv.Total())
			}
		})
	}
}

// A failed original temporary-file Close must keep the file and its name,
// every cause and the same locked store, even after Machine.Close.
func TestSaveCloseKeepsStore(t *testing.T) {
	for _, stage := range []string{"write", "sync", ""} {
		name := stage
		if name == "" {
			name = "close only"
		}
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			e.dir = filepath.Join(keptDir(t), "vm")
			m := e.create()
			s := m.s
			before, err := os.ReadFile(s.path(stateName))
			if err != nil {
				t.Fatal(err)
			}
			if stage != "" {
				s.fault = func(got string) error {
					if got == stage {
						return errFault
					}
					return nil
				}
			}
			var files []*os.File
			precloseFiles(t, tempName, &files)
			err = m.SetGoal(GoalAbsent)
			held := err
			t.Cleanup(func() { retainedErrors = append(retainedErrors, held) })
			var ce *closeError
			if !errors.Is(err, ErrUnpersisted) || !errors.Is(err, ErrUnsettled) || !errors.Is(err, os.ErrClosed) || errors.Is(err, errFault) != (stage != "") || !errors.As(err, &ce) || ce.s != s || len(files) != 1 || ce.f != files[0] || s.failed != ce {
				t.Fatalf("SetGoal = %v; want every cause, the same file and store", err)
			}
			if _, err := os.Lstat(s.path(tempName)); err != nil {
				t.Fatalf("original temporary file removed: %v", err)
			}
			after, err := os.ReadFile(s.path(stateName))
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("failed Close replaced the state: %v", err)
			}
			for _, f := range []*os.File{s.lock, s.d} {
				if _, err := f.Stat(); err != nil {
					t.Fatalf("dependency closed before original settled: %v", err)
				}
			}
			for _, err := range []error{m.SetGoal(GoalRunning), s.save(before)} {
				if !errors.Is(err, ErrUnsettled) {
					t.Fatalf("later write = %v", err)
				}
			}
			if _, err := m.Drive(context.Background(), e.c, fast); !errors.Is(err, ErrUnsettled) {
				t.Fatalf("later Drive = %v", err)
			}
			closed := m.Close()
			if !errors.Is(closed, ErrUnsettled) || s.close() != closed || len(files) != 1 {
				t.Fatalf("Close = %v; original Close repeated or lost", closed)
			}
			if _, err := Open(e.dir, e.cfg); !errors.Is(err, ErrBusy) {
				t.Fatalf("the original lock was released: %v", err)
			}
			if _, err := os.Lstat(s.path(tempName)); err != nil || e.srv.Total() != 0 {
				t.Fatalf("later effect: original %v, requests %d", err, e.srv.Total())
			}
		})
	}
}
