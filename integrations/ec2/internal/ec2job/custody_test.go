//go:build darwin || linux

package ec2job

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
)

func TestLogsClosedJob(t *testing.T) {
	for _, j := range []*Job{nil, {}} {
		if _, err := j.Logs(context.Background(), 0, io.Discard); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("Logs = %v; want os.ErrClosed", err)
		}
	}
}

func TestStatusOwnsExitCode(t *testing.T) {
	j, _, dir := newJob(t)
	next := doubtful(j.st, RetentionDisabled, time.Time{})
	next.Submit, next.JobID = submitAccepted, strings.Repeat("1", 32)
	next.Observed, next.ObservedAt = "FAILED", j.now().UTC()
	code := int32(7)
	next.ExitCode = &code
	if err := next.check(); err != nil {
		t.Fatal(err)
	}
	if err := j.commit(next); err != nil {
		t.Fatal(err)
	}
	before := readJournal(t, dir)
	st := j.Status()
	if st.ExitCode == nil {
		t.Fatal("missing exit code")
	}
	*st.ExitCode = 99
	if got := j.Status(); got.ExitCode == nil || *got.ExitCode != 7 || !got.Durable {
		t.Fatalf("status mutated the job: %+v", got)
	}
	if !bytes.Equal(before, readJournal(t, dir)) {
		t.Fatal("obtaining status changed the journal")
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			retain(reopened)
			retain(err)
			t.Errorf("close reopened job: %v", err)
		}
	}()
	if got := reopened.Status(); got.ExitCode == nil || *got.ExitCode != 7 {
		t.Fatalf("reopened exit code = %v", got.ExitCode)
	}
}

// These tests use real files and the store's closing hook, as state_test.go
// describes: a test closes an original file itself just before the
// package's one Close of it, which then returns the real error for a file
// already closed. That is a MODEL of a failed first Close: it is neither
// an uncertain native close nor a crash, and the descriptor is in fact
// released.

// preclose returns a closing hook that closes, once, the first file whose
// name has suffix, and records it in *got.
func preclose(t *testing.T, suffix string, got **os.File) func(*os.File) {
	return func(f *os.File) {
		if *got != nil || !strings.HasSuffix(f.Name(), suffix) {
			return
		}
		*got = f
		if err := f.Close(); err != nil {
			t.Errorf("native close of %s: %v", f.Name(), err)
		}
	}
}

// TestLoadConfigCloseFails: the configuration file's Close fails (MODEL).
// LoadConfig returns no configuration and an error wrapping ErrUnsettled
// that keeps the original file; the twin without the failure loads it.
func TestLoadConfigCloseFails(t *testing.T) {
	dir := keptDir(t)
	name := filepath.Join(dir, "job-config.json")
	if err := os.WriteFile(name, encodeWire(t, testWire(dir, testInstanceID)), 0o600); err != nil {
		t.Fatal(err)
	}
	if c, err := loadConfig(name, nil); err != nil || c.InstanceID != testInstanceID {
		t.Fatalf("loadConfig = %v, %v", c, err)
	}
	var f *os.File
	c, err := loadConfig(name, preclose(t, "job-config.json", &f))
	var cerr *closeError
	if c != nil || !errors.Is(err, ErrUnsettled) || !errors.Is(err, os.ErrClosed) || !errors.As(err, &cerr) || f == nil || cerr.f != f {
		t.Fatalf("loadConfig = %v, %v; want ErrUnsettled keeping the original file", c, err)
	}
	retain(err)
}

// readerJob returns a job below parent whose retention is recorded, so
// that Resume's first effect is reading the frozen snapshot, attached to
// a counting peer that answers nothing.
func readerJob(t *testing.T, parent string) (*Job, string, func() int64) {
	t.Helper()
	j, c, dir := newJobIn(t, parent)
	next := j.st
	next.Retention = RetentionDisabled
	if err := j.commit(next); err != nil {
		t.Fatal(err)
	}
	peer, n := countingPeer(t)
	if err := j.Attach(peerSession{peer, sessionFor(c, testVM(c))}); err != nil {
		t.Fatal(err)
	}
	return j, dir, n.Load
}

// TestReaderCloseFails: the Close of the frozen snapshot the Job read
// fails (MODEL), after an independent refusal of the file's mode. Both
// causes are returned; the store latches the original file; nothing is
// sent, read or written afterwards; Close keeps the lock, and another
// Open of the job is refused as busy.
func TestReaderCloseFails(t *testing.T) {
	j, dir, requests := readerJob(t, keptDir(t))
	snapshot := filepath.Join(dir, workdirName)
	if err := os.Chmod(snapshot, 0o640); err != nil {
		t.Fatal(err)
	}
	before := readJournal(t, dir)
	var f *os.File
	j.s.closing = preclose(t, workdirName, &f)
	_, err := j.Resume(context.Background(), Wait{})
	if !errors.Is(err, ErrUnsettled) || !errors.Is(err, os.ErrClosed) || !strings.Contains(err.Error(), "group and other must have no access") {
		t.Fatalf("Resume = %v; want the mode refusal and the failed Close, both", err)
	}
	if f == nil || j.s.failed == nil || j.s.failed.f != f || j.s.failed.s != j.s {
		t.Fatalf("store keeps %v; want the original snapshot file", j.s.failed)
	}
	if err := os.Chmod(snapshot, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Resume(context.Background(), Wait{}); !errors.Is(err, ErrUnsettled) {
		t.Errorf("Resume after the failed Close = %v", err)
	}
	if err := j.RequestCancel(); !errors.Is(err, ErrUnsettled) {
		t.Errorf("RequestCancel after the failed Close = %v", err)
	}
	if _, err := j.snapshot(); !errors.Is(err, ErrUnsettled) {
		t.Errorf("a read after the failed Close = %v", err)
	}
	if requests() != 0 || !bytes.Equal(before, readJournal(t, dir)) {
		t.Fatal("a request was sent or the journal changed after the failed Close")
	}
	lock := j.s.lock
	err = j.Close()
	if !errors.Is(err, ErrUnsettled) || j.Close() != err || j.s.lock != lock {
		t.Fatalf("Close = %v; want a sticky ErrUnsettled keeping the lock", err)
	}
	if _, err := Open(dir); !errors.Is(err, ErrBusy) {
		t.Fatalf("Open while the reader is unsettled = %v, want ErrBusy", err)
	}
	retain(j)
}

// TestReaderCloseSettled is TestReaderCloseFails's twin without the
// failed Close: the mode refusal alone is returned, the store latches
// nothing, and once the mode is restored the read succeeds.
func TestReaderCloseSettled(t *testing.T) {
	j, dir, requests := readerJob(t, t.TempDir())
	snapshot := filepath.Join(dir, workdirName)
	if err := os.Chmod(snapshot, 0o640); err != nil {
		t.Fatal(err)
	}
	_, err := j.Resume(context.Background(), Wait{})
	if err == nil || errors.Is(err, ErrUnsettled) || j.s.failed != nil {
		t.Fatalf("Resume = %v, failed %v", err, j.s.failed)
	}
	if err := os.Chmod(snapshot, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := j.snapshot(); err != nil {
		t.Fatalf("snapshot = %v", err)
	}
	if requests() != 0 {
		t.Fatal("a request was sent")
	}
}

// dirState returns the job directory's mode and, for each entry, its
// mode and content digest.
func dirState(t *testing.T, dir string) map[string]string {
	t.Helper()
	fi, err := os.Lstat(dir)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]string{".": fi.Mode().String()}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		p := filepath.Join(dir, e.Name())
		fi, err := os.Lstat(p)
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		m[e.Name()] = fmt.Sprintf("%v %x", fi.Mode(), sha256.Sum256(b))
	}
	return m
}

// orphans writes a distinct temporary file for each name a writer of
// this format leaves, and removes the lock file, as a directory whose
// lock was never created has none.
func orphans(t *testing.T, dir string) {
	t.Helper()
	for _, name := range []string{stateName, workdirName, outputName} {
		if err := os.WriteFile(filepath.Join(dir, name+tempSuffix), []byte("sentinel "+name+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "notes"), []byte("not the format's\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, lockName)); err != nil {
		t.Fatal(err)
	}
}

// TestOpenRefusesBeforeEffects: a journal of an unknown format, a future
// version or corrupt JSON is refused by Open before it creates the lock
// file or removes any temporary file: the directory's membership, modes
// and bytes are unchanged.
func TestOpenRefusesBeforeEffects(t *testing.T) {
	for _, tt := range []struct {
		name        string
		mutate      func([]byte) []byte
		unsupported bool
	}{
		{"unknown format", func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"format": "`+stateFormat+`"`), []byte(`"format": "skygo-ec2-job-state-next"`), 1)
		}, true},
		{"future version", func(b []byte) []byte {
			return bytes.Replace(b, []byte(fmt.Sprintf(`"version": %d,`, stateVersion)), []byte(fmt.Sprintf(`"version": %d,`, stateVersion+1)), 1)
		}, true},
		{"corrupt JSON", func(b []byte) []byte { return b[:len(b)/2] }, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			j, _, dir := newJob(t)
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			bad := tt.mutate(readJournal(t, dir))
			if bytes.Equal(bad, readJournal(t, dir)) {
				t.Fatal("the mutation changed nothing")
			}
			if err := os.WriteFile(filepath.Join(dir, stateName), bad, 0o600); err != nil {
				t.Fatal(err)
			}
			orphans(t, dir)
			before := dirState(t, dir)
			j, err := Open(dir)
			if err == nil {
				j.Close()
				t.Fatal("Open accepted the journal")
			}
			if errors.Is(err, errUnsupportedState) != tt.unsupported {
				t.Errorf("Open = %v, unsupported want %v", err, tt.unsupported)
			}
			if after := dirState(t, dir); !maps.Equal(before, after) {
				t.Fatalf("Open changed the directory:\nbefore %v\nafter  %v", before, after)
			}
			if _, err := os.Lstat(filepath.Join(dir, lockName)); !errors.Is(err, fs.ErrNotExist) {
				t.Fatal("Open created the lock file")
			}
		})
	}
}

// TestOpenCleansAfterValidation is the twin with a current journal: Open
// creates the lock, removes exactly this format's temporary files, and
// leaves the journal, the frozen snapshot and any other file unchanged.
func TestOpenCleansAfterValidation(t *testing.T) {
	j, _, dir := newJob(t)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	orphans(t, dir)
	before := dirState(t, dir)
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	after := dirState(t, dir)
	for _, name := range []string{stateName, workdirName, outputName} {
		if _, ok := after[name+tempSuffix]; ok {
			t.Errorf("%s was not removed", name+tempSuffix)
		}
		delete(before, name+tempSuffix)
	}
	if _, ok := after[lockName]; !ok {
		t.Fatal("Open did not create the lock")
	}
	delete(after, lockName)
	if !maps.Equal(before, after) {
		t.Fatalf("Open changed other files:\nbefore %v\nafter  %v", before, after)
	}
}

// downloading returns st as an ended, accepted job whose result download
// is open under a known ID.
func downloading(st state) state {
	st = doubtful(st, RetentionDisabled, time.Time{})
	st.Submit, st.JobID = submitAccepted, strings.Repeat("1", 32)
	st.Observed, st.ObservedAt = "SUCCEEDED", time.Now().UTC()
	st.Download, st.DownloadID = downloadOpen, strings.Repeat("2", 32)
	st.DownloadRef = artifact.Ref{ID: strings.Repeat("4", 32), SHA256: st.Snapshot.SHA256, Size: st.Snapshot.Size}
	st.DownloadAttempt = strings.Repeat("3", 32)
	return st
}

// TestDownloadRecordVersion: a journal holding a result download is
// stored as version 3, which neither the version 1 nor the version 2
// encoding can hold, and this reader refuses its download record under a
// version 2 header. MODEL: that header is rewritten by this test to show
// this reader's own version 2 rule; it is not a version 2 writer's
// output. The genuine version 2 reader, whose fence refuses any version
// but 1 and 2, was not run.
func TestDownloadRecordVersion(t *testing.T) {
	j, _, dir := newJob(t)
	st := downloading(j.st)
	if err := j.commit(st); err != nil {
		t.Fatal(err)
	}
	data := readJournal(t, dir)
	if !bytes.Contains(data, []byte(`"version": 3,`)) || !bytes.Contains(data, []byte(`"download": {`)) {
		t.Fatalf("journal:\n%s", data)
	}
	if got, err := decodeState(data); err != nil || got.DownloadID != st.DownloadID || got.DownloadRef != st.DownloadRef {
		t.Fatalf("decodeState = %+v, %v", got, err)
	}
	for _, v := range []int{1, 2} {
		if _, err := encodeVersion(st, v); err == nil {
			t.Errorf("version %d encoded a result download", v)
		}
	}
	lowered := bytes.Replace(data, []byte(`"version": 3,`), []byte(`"version": 2,`), 1)
	if _, err := decodeState(lowered); err == nil {
		t.Fatal("a version 2 header with a download record was accepted")
	}
	held := st
	held.Download, held.DownloadID, held.Hold = downloadBegin, "", HoldDownloadBegin
	if _, err := encodeVersion(held, 2); err == nil {
		t.Fatal("version 2 encoded a download hold")
	}
}
