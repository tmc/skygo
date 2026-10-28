package auth

import (
	"errors"
	"os"
	"testing"
)

// modelCloseFailure replaces closeConfigFile for the test. Each call really
// closes the original descriptor readConfig opened, records it, and returns
// fail if it is not nil. The returned error is MODELED: the native Close
// succeeded, and fail stands in for an operating system Close failure.
func modelCloseFailure(t *testing.T, fail error) *[]*os.File {
	t.Helper()
	var closed []*os.File
	t.Cleanup(func() { closeConfigFile = (*os.File).Close })
	closeConfigFile = func(f *os.File) error {
		closed = append(closed, f)
		if err := f.Close(); err != nil {
			t.Errorf("native close: %v", err)
			return err
		}
		return fail
	}
	return &closed
}

func TestReadConfigCloseUncertain(t *testing.T) {
	f := newFixture(t)
	modeled := errors.New("modeled close failure")
	closed := modelCloseFailure(t, modeled)
	c, data, info, err := readConfig(f.path)
	if c != nil || data != nil || info != nil {
		t.Fatal("accepted configuration from a read whose Close failed")
	}
	if !errors.Is(err, ErrCloseUncertain) || !errors.Is(err, modeled) {
		t.Fatalf("readConfig = %v, want ErrCloseUncertain wrapping the Close error", err)
	}
	var held *fileCloseError
	if !errors.As(err, &held) || len(*closed) != 1 || held.file != (*closed)[0] {
		t.Fatalf("error does not retain the one original descriptor: closes %d", len(*closed))
	}
	if err.Error() != "close authentication file" {
		t.Fatalf("error text %q exposes detail", err.Error())
	}
	if _, err := Tokens(f.path); !errors.Is(err, ErrCloseUncertain) {
		t.Fatalf("Tokens = %v, want ErrCloseUncertain", err)
	}
}

// TestReadConfigCloseSuccess is the control: a positive Close leaves the
// ordinary result unchanged and closes exactly once.
func TestReadConfigCloseSuccess(t *testing.T) {
	f := newFixture(t)
	closed := modelCloseFailure(t, nil)
	c, data, info, err := readConfig(f.path)
	if err != nil || c == nil || len(data) == 0 || info == nil || len(*closed) != 1 {
		t.Fatalf("readConfig = %v, closes %d", err, len(*closed))
	}
}

func TestNewFileCloseUncertain(t *testing.T) {
	f := newFixture(t)
	closed := modelCloseFailure(t, errors.New("modeled close failure"))
	e, err := NewFile(f.path, installation)
	if e != nil || !errors.Is(err, ErrCloseUncertain) || len(*closed) != 1 {
		t.Fatalf("NewFile = %v, %v, closes %d; want no Engine and ErrCloseUncertain", e, err, len(*closed))
	}
}

// TestEngineCloseUncertainSticky checks that an uncertain Close makes a
// serving Engine unavailable for good: later refreshes neither reopen the
// file nor publish a snapshot, even after the file reads cleanly again.
func TestEngineCloseUncertainSticky(t *testing.T) {
	f := newFixture(t)
	e, err := NewFile(f.path, installation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Authenticate("Bearer " + f.initial); err != nil || e.Err() != nil {
		t.Fatalf("Authenticate before failure: %v, Err %v", err, e.Err())
	}
	changed := e.Changed()
	modeled := errors.New("modeled close failure")
	closed := modelCloseFailure(t, modeled)
	first := e.refresh()
	if !errors.Is(first, ErrCloseUncertain) || !errors.Is(first, modeled) || e.Err() != first || len(*closed) != 1 {
		t.Fatalf("refresh = %v, Err %v, closes %d; want one Close and its retained error", first, e.Err(), len(*closed))
	}
	select {
	case <-changed:
	default:
		t.Fatal("availability change not notified")
	}
	if e.Ready() {
		t.Fatal("Ready after an uncertain Close")
	}
	if _, err := e.Authenticate("Bearer " + f.initial); err != ErrUnavailable {
		t.Fatalf("Authenticate = %v, want ErrUnavailable", err)
	}

	closeConfigFile = (*os.File).Close
	writeFixture(t, f.path, func(c *config) { c.Generation++ })
	closed = modelCloseFailure(t, nil)
	if err := e.refresh(); err != first {
		t.Fatalf("second refresh = %v, want the same retained error", err)
	}
	if len(*closed) != 0 {
		t.Fatalf("refresh reopened the file %d times after an uncertain Close", len(*closed))
	}
	if e.Ready() || e.Err() != first {
		t.Fatal("uncertain Close was not sticky")
	}
}
