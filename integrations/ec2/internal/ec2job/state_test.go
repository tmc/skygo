//go:build darwin || linux

package ec2job

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/integrations/ssh/sshagent"
	"github.com/tmc/skygo/internal/artifact"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// These tests use real journal files under t.TempDir with fsync, rename
// and flock. Faults are injected only through store.fault between real
// writes; a fault is not a close. An actual os.File.Close failure is made
// through store.closing, which closes the original file itself just
// before the store's one Close of it: the store's Close then returns the
// real os.File error for a file already closed. That is a real Close
// failure frontier, not a device I/O failure. A "reopen" is a close and
// open by this process: it models a restart of the owner, not an
// operating system crash.

var errFault = errors.New("injected fault")

// retained keeps every unsettled original a test hands to retain
// reachable for the life of the test process, which is what owns them.
var retained struct {
	sync.Mutex
	owners []any
}

func retain(v any) {
	retained.Lock()
	retained.owners = append(retained.owners, v)
	retained.Unlock()
}

// keptDir returns a new directory for a test that leaves an original
// unsettled. It is not a t.TempDir: its cleanup would remove a directory
// below an owner that still holds it. The directory is kept and its path
// reported.
func keptDir(t *testing.T) string {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261028-ec2-job-target-phase-b-tests")
	if err := os.MkdirAll(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(parent, "unsettled-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("keeping %s: it holds an unsettled original", dir)
	return dir
}

// newWorkdir writes a small workdir and returns its path.
func newWorkdir(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "workdir")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, text := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

const testTaskJSON = `{"argv":["/bin/sh","-c","cat input.txt > out/result.txt; echo hello"],"setup":["/bin/sh","-c","mkdir out && echo setup-ran > out/setup.txt"],"outputs":["out"],"timeoutSeconds":"60"}`

// newJob creates a job directory below a new temporary directory.
func newJob(t *testing.T) (*Job, *Config, string) {
	t.Helper()
	return newJobIn(t, t.TempDir())
}

// newJobIn creates a job directory below parent.
func newJobIn(t *testing.T, parent string) (*Job, *Config, string) {
	t.Helper()
	c := testJobConfig(t, "/d", testInstanceID)
	archive, digest, err := Snapshot(newWorkdir(t, map[string]string{"input.txt": "input\n"}))
	if err != nil {
		t.Fatal(err)
	}
	task, err := c.Task(TaskInput{JSON: []byte(testTaskJSON)}, digest, int64(len(archive)), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(parent, "job")
	j, err := Create(dir, c, testVM(c), task, archive)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := j.Close(); errors.Is(err, ErrUnsettled) {
			retain(j)
		}
	})
	if err := j.Bind(c, testVM(c)); err != nil {
		t.Fatal(err)
	}
	return j, c, dir
}

func readJournal(t *testing.T, dir string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, stateName))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestCreateOpen(t *testing.T) {
	j, c, dir := newJob(t)
	st := j.Status()
	if !hexID32.MatchString(st.RequestID) || st.Method != Method || st.Desired != desiredRun || st.Upload != uploadNone || st.Submit != submitNone || !st.Durable || st.InstanceID != c.InstanceID || st.Address != "10.0.0.5" {
		t.Fatalf("new job status %+v", st)
	}
	before := readJournal(t, dir)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if j.Status().RequestID != st.RequestID {
		t.Fatal("reopened journal has another request ID")
	}
	if err := j.Bind(c, testVM(c)); err != nil {
		t.Fatal(err)
	}
	moved := testVM(c)
	moved.Address = moved.Address.Next()
	if err := j.Bind(c, moved); !errors.Is(err, ErrMismatch) {
		t.Errorf("Bind with a changed address = %v", err)
	}
	other := testJobConfig(t, "/e", testInstanceID)
	if err := j.Bind(other, testVM(other)); !errors.Is(err, ErrMismatch) {
		t.Errorf("Bind with another configuration = %v", err)
	}
	owner := testVM(c)
	owner.Generation++
	if err := j.Bind(c, owner); !errors.Is(err, ErrMismatch) {
		t.Errorf("Bind with another owner generation = %v", err)
	}
	if !bytes.Equal(before, readJournal(t, dir)) {
		t.Fatal("open and bind rewrote the journal")
	}
}

func TestDuplicateOwner(t *testing.T) {
	j, _, dir := newJob(t)
	if _, err := Open(dir); !errors.Is(err, ErrBusy) {
		t.Fatalf("second Open = %v, want ErrBusy", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open after Close = %v", err)
	}
	j2.Close()
}

// TestNoJournal: a directory without a journal, as a Create that failed
// after freezing the snapshot leaves, is refused by Open and by Create,
// and is never deleted.
func TestNoJournal(t *testing.T) {
	c := testJobConfig(t, "/d", testInstanceID)
	dir := filepath.Join(t.TempDir(), "job")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, workdirName), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); !errors.Is(err, ErrNoState) {
		t.Fatalf("Open = %v, want ErrNoState", err)
	}
	archive, digest, err := Snapshot(newWorkdir(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	task, err := c.Task(TaskInput{JSON: []byte(testTaskJSON)}, digest, int64(len(archive)), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Create(dir, c, testVM(c), task, archive); err == nil {
		t.Fatal("Create reused an existing directory")
	}
	if _, err := os.Stat(filepath.Join(dir, workdirName)); err != nil {
		t.Fatal("the directory's contents were removed")
	}
}

func TestCorruptJournal(t *testing.T) {
	edit := func(f func(*wireState)) func([]byte) []byte {
		return func(b []byte) []byte {
			var w wireState
			if err := json.Unmarshal(b, &w); err != nil {
				panic(err)
			}
			f(&w)
			out, err := json.MarshalIndent(w, "", "\t")
			if err != nil {
				panic(err)
			}
			return append(out, '\n')
		}
	}
	for _, tt := range []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"newer version", edit(func(w *wireState) { w.Version = stateVersion + 1 })},
		{"version 0", edit(func(w *wireState) { w.Version = 0 })},
		{"version 1 with a version 2 hold", edit(func(w *wireState) { w.Version, w.Hold = 1, HoldAcceptedReply })},
		{"log without a job", edit(func(w *wireState) { w.Log = &wireLog{Offset: 5} })},
		{"log past the limit", edit(func(w *wireState) { w.Log = &wireLog{Offset: MaxLogBytes + 1} })},
		{"empty log object", func(b []byte) []byte {
			return bytes.Replace(b, []byte("\n}\n"), []byte(",\n\t\"log\": {\n\t\t\"attempt\": \"\",\n\t\t\"offset\": 0,\n\t\t\"truncated\": false,\n\t\t\"complete\": false\n\t}\n}\n"), 1)
		}},
		{"download without a job", edit(func(w *wireState) {
			w.Download = &wireDownload{Stage: downloadBegin, Ref: wireRef{strings.Repeat("a", 32), strings.Repeat("b", 64), 10240}, Attempt: strings.Repeat("c", 32)}
		})},
		{"version 2 with a download hold", edit(func(w *wireState) { w.Version, w.Hold = 2, HoldDownloadBegin })},
		{"download hold without a download", edit(func(w *wireState) { w.Hold = HoldDownloadClose })},
		{"empty download object", func(b []byte) []byte {
			return bytes.Replace(b, []byte("\n}\n"), []byte(",\n\t\"download\": {\n\t\t\"stage\": \"\",\n\t\t\"id\": \"\",\n\t\t\"ref\": {\n\t\t\t\"id\": \"\",\n\t\t\t\"sha256\": \"\",\n\t\t\t\"size\": 0\n\t\t},\n\t\t\"attempt\": \"\"\n\t}\n}\n"), 1)
		}},
		{"cleanup of a job wanting run", edit(func(w *wireState) { w.Cleanup = cleanupSent })},
		{"unknown cleanup", edit(func(w *wireState) { w.Desired, w.Cleanup = desiredCancel, "deleted" })},
		{"task with an unknown field", edit(func(w *wireState) { w.Task = append(append([]byte{}, w.Task...), 0xc0, 0x3e, 0x01) })},
		{"format", edit(func(w *wireState) { w.Format = "skygo-ec2-vm-state" })},
		{"unknown field", func(b []byte) []byte { return bytes.Replace(b, []byte("{\n"), []byte("{\n\t\"token\": \"x\",\n"), 1) }},
		{"not canonical", func(b []byte) []byte { return bytes.ReplaceAll(b, []byte("\t"), []byte("  ")) }},
		{"trailing data", func(b []byte) []byte { return append(b, []byte("{}\n")...) }},
		{"method", edit(func(w *wireState) { w.Method = "SubmitJob" })},
		{"accepted without ID", edit(func(w *wireState) { w.Submit = submitAccepted })},
		{"ID without accept", edit(func(w *wireState) { w.JobID = strings.Repeat("b", 32) })},
		{"submit before upload", edit(func(w *wireState) { w.Retention, w.Submit = RetentionDisabled, submitSent })},
		{"upload before retention", edit(func(w *wireState) { w.Upload = uploadBeginSent })},
		{"upload ID without stage", edit(func(w *wireState) { w.UploadID = strings.Repeat("c", 32) })},
		{"domain without lineage", edit(func(w *wireState) { w.Retention = RetentionEnabled })},
		{"lineage when disabled", edit(func(w *wireState) { w.Retention, w.Lineage, w.Epoch = RetentionDisabled, strings.Repeat("d", 32), 1 })},
		{"cancel without ID", edit(func(w *wireState) { w.Desired, w.Cancel = desiredCancel, cancelSent })},
		{"unknown hold", edit(func(w *wireState) { w.Hold = "retry later" })},
		{"unknown observation", edit(func(w *wireState) { w.Observed, w.ObservedAt = "DONE", time.Now().UTC() })},
		{"request ID", edit(func(w *wireState) { w.RequestID = "req-1" })},
		{"architecture", edit(func(w *wireState) { w.VM.Architecture = "amd64" })},
		{"task", edit(func(w *wireState) { w.Task = []byte{0xff} })},
		{"truncated", func(b []byte) []byte { return b[:len(b)/2] }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			j, _, dir := newJob(t)
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			name := filepath.Join(dir, stateName)
			bad := tt.mutate(readJournal(t, dir))
			if err := os.WriteFile(name, bad, 0o600); err != nil {
				t.Fatal(err)
			}
			if j, err := Open(dir); err == nil {
				j.Close()
				t.Fatal("Open accepted a corrupt journal")
			}
			if !bytes.Equal(bad, readJournal(t, dir)) {
				t.Fatal("a refused journal was rewritten")
			}
			// The refusal released the lock.
			if s, err := openStore(dir, false); err != nil {
				t.Fatalf("lock not released: %v", err)
			} else {
				s.close()
			}
		})
	}
}

// countingPeer is a MODEL peer that counts requests and answers none; it
// shows that no request is sent.
func countingPeer(t *testing.T) (skyv1connect.ControlServiceClient, *atomic.Int64) {
	t.Helper()
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		http.Error(w, "no", http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)
	return skyv1connect.NewControlServiceClient(srv.Client(), srv.URL), &n
}

type peerSession struct {
	control skyv1connect.ControlServiceClient
	binding sshagent.Binding
}

func (s peerSession) Control() skyv1connect.ControlServiceClient { return s.control }
func (s peerSession) Binding() sshagent.Binding                  { return s.binding }

// sessionFor returns the Binding sshagent.Open reports for cfg's session
// to vm's address, with a fixed agent commit and binary digest.
func sessionFor(c *Config, vm VM) sshagent.Binding {
	s := c.Session(vm.Address, nil)
	return sshagent.Binding{
		Address: s.Address, User: s.User, KnownHostsFile: s.KnownHostsFile,
		KeyFile: s.KeyFile, Fingerprint: s.Fingerprint, AgentAddress: "127.0.0.1:47080",
		Target: s.Target, Commit: strings.Repeat("1", 40), Digest: strings.Repeat("2", 64),
		AgentDir: "skygo-test/agents/x", Installation: s.Installation,
	}
}

// TestUnpersistedStopsCalls: a save that fails leaves the state pending.
// No request is sent until a later call stores it.
func TestUnpersistedStopsCalls(t *testing.T) {
	j, c, dir := newJob(t)
	peer, n := countingPeer(t)
	if err := j.Attach(peerSession{peer, sessionFor(c, testVM(c))}); err != nil {
		t.Fatal(err)
	}
	before := readJournal(t, dir)
	j.s.fault = func(stage string) error {
		if stage == "rename" {
			return errFault
		}
		return nil
	}
	if err := j.RequestCancel(); !errors.Is(err, ErrUnpersisted) {
		t.Fatalf("RequestCancel = %v, want ErrUnpersisted", err)
	}
	if j.Status().Durable || !bytes.Equal(before, readJournal(t, dir)) {
		t.Fatal("a failed save is reported durable or changed the journal")
	}
	if _, err := j.Refresh(context.Background()); !errors.Is(err, ErrUnpersisted) {
		t.Fatalf("Refresh with a pending state = %v", err)
	}
	if n.Load() != 0 {
		t.Fatalf("%d requests sent with a pending state", n.Load())
	}
	j.s.fault = nil
	if err := j.RequestCancel(); err != nil {
		t.Fatal(err)
	}
	var w wireState
	if err := json.Unmarshal(readJournal(t, dir), &w); err != nil {
		t.Fatal(err)
	}
	if w.Desired != desiredCancel || !j.Status().Durable {
		t.Fatalf("the pending cancel was not stored: %+v", w)
	}
}

// TestSyncFailureUncertain: a failed directory sync happens after the
// rename. The new journal may or may not survive; the job keeps it
// pending and stores it again before acting.
func TestSyncFailureUncertain(t *testing.T) {
	j, _, dir := newJob(t)
	j.s.fault = func(stage string) error {
		if stage == "sync directory" {
			return errFault
		}
		return nil
	}
	if err := j.RequestCancel(); !errors.Is(err, ErrUnpersisted) {
		t.Fatalf("RequestCancel = %v", err)
	}
	if !strings.Contains(string(readJournal(t, dir)), `"desired": "cancel"`) || j.Status().Durable {
		t.Fatal("the renamed journal should be visible and still pending")
	}
	j.s.fault = nil
	if err := j.flush(); err != nil || !j.Status().Durable {
		t.Fatalf("flush = %v", err)
	}
}

// TestCloseSticky: MODEL fault reported before close's real closes, which
// still run and succeed. It shows that the first Close's result is sticky
// and that Close is not retried; it is not a failing os.File.Close (see
// TestLockCloseFails).
func TestCloseSticky(t *testing.T) {
	j, _, dir := newJob(t)
	j.s.fault = func(stage string) error {
		if stage == "before close" {
			return errFault
		}
		return nil
	}
	if err := j.Close(); !errors.Is(err, errFault) {
		t.Fatalf("Close = %v", err)
	}
	if err := j.Close(); !errors.Is(err, errFault) {
		t.Fatalf("second Close = %v, want the first Close's error", err)
	}
	if err := j.RequestCancel(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("RequestCancel after Close = %v", err)
	}
	if j.Status().RequestID == "" {
		t.Fatal("a closed job lost its status")
	}
	// The files were really closed: the lock is free.
	j2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	j2.Close()
}

func TestRequestCancelNeverSubmitted(t *testing.T) {
	j, _, dir := newJob(t)
	if err := j.RequestCancel(); err != nil {
		t.Fatal(err)
	}
	st := j.Status()
	if st.State != "NEVER_SUBMITTED" || !st.Ended() || st.Succeeded() || j.NeedsRemote() {
		t.Fatalf("status %+v", st)
	}
	j.Close()
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if j.Status().Desired != desiredCancel {
		t.Fatal("the cancel was not latched")
	}
}

// TestRequestCancelSubmitInDoubt: with a submit sent and no job ID, there
// is no read-only lookup, and a submit could create the job. The cancel
// holds it, and the hold is latched across a reopen.
func TestRequestCancelSubmitInDoubt(t *testing.T) {
	j, c, dir := newJob(t)
	// Construct the journal of a submit in doubt through the real commit
	// path: retention recorded, upload finalized, submit sent.
	next := j.st
	next.Retention = RetentionDisabled
	next.Upload, next.UploadID = uploadDone, strings.Repeat("e", 32)
	next.Workdir = artifact.Ref{ID: strings.Repeat("f", 32), SHA256: next.Snapshot.SHA256, Size: next.Snapshot.Size}
	next.Acknowledged = next.Snapshot.Size
	next.Submit = submitSent
	if err := j.commit(next); err != nil {
		t.Fatal(err)
	}
	if err := j.RequestCancel(); !errors.Is(err, ErrHeld) {
		t.Fatalf("RequestCancel = %v, want ErrHeld", err)
	}
	j.Close()
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if j.Status().Hold != HoldCanceledSubmit || j.NeedsRemote() {
		t.Fatalf("status %+v", j.Status())
	}
	if err := j.Bind(c, testVM(c)); err != nil {
		t.Fatal(err)
	}
	peer, n := countingPeer(t)
	if err := j.Attach(peerSession{peer, sessionFor(c, testVM(c))}); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Resume(context.Background(), Wait{}); !errors.Is(err, ErrHeld) {
		t.Fatalf("Resume of a held job = %v", err)
	}
	if n.Load() != 0 {
		t.Fatal("a held job sent a request")
	}
}

// TestAttachBinding: a session to another address, key, target or
// installation, or to a changed agent, is refused before any request.
func TestAttachBinding(t *testing.T) {
	j, c, _ := newJob(t)
	peer, n := countingPeer(t)
	good := sessionFor(c, testVM(c))
	for name, change := range map[string]func(*sshagent.Binding){
		"address":      func(b *sshagent.Binding) { b.Address = "10.0.0.6:22" },
		"user":         func(b *sshagent.Binding) { b.User = "root" },
		"known hosts":  func(b *sshagent.Binding) { b.KnownHostsFile = "/other" },
		"key":          func(b *sshagent.Binding) { b.KeyFile = "/other" },
		"target":       func(b *sshagent.Binding) { b.Target = "other" },
		"installation": func(b *sshagent.Binding) { b.Installation.DataDir = "/other" },
		"dirty":        func(b *sshagent.Binding) { b.Dirty = true },
	} {
		b := good
		change(&b)
		if err := j.Attach(peerSession{peer, b}); !errors.Is(err, ErrMismatch) {
			t.Errorf("%s: Attach = %v, want ErrMismatch", name, err)
		}
	}
	if err := j.Attach(peerSession{peer, good}); err != nil {
		t.Fatal(err)
	}
	changed := good
	changed.Commit = strings.Repeat("3", 40)
	if err := j.Attach(peerSession{peer, changed}); !errors.Is(err, ErrMismatch) {
		t.Errorf("a changed agent source was accepted: %v", err)
	}
	if n.Load() != 0 {
		t.Fatal("Attach sent a request")
	}
}

// TestTempCloseFails: the Close of a temporary journal write really fails
// (see the file comment), after a MODEL sync fault. The store keeps the
// same os.File, which was closed once and is never closed again; the
// temporary file is neither removed nor reused; the sync fault and the
// Close failure are both reported; nothing more is written or called;
// and the directory stays locked.
func TestTempCloseFails(t *testing.T) {
	j, _, dir := newJobIn(t, keptDir(t))
	before := readJournal(t, dir)
	var temp *os.File
	closes := map[*os.File]int{}
	j.s.fault = func(stage string) error {
		if stage == "sync" {
			return errFault
		}
		return nil
	}
	j.s.closing = func(f *os.File) {
		closes[f]++
		if temp == nil && strings.HasSuffix(f.Name(), tempSuffix) {
			temp = f
			if err := f.Close(); err != nil {
				t.Errorf("native close of the temporary file: %v", err)
			}
		}
	}
	err := j.RequestCancel()
	if !errors.Is(err, ErrUnpersisted) || !errors.Is(err, ErrUnsettled) || !errors.Is(err, os.ErrClosed) || !errors.Is(err, errFault) {
		t.Fatalf("RequestCancel = %v; want the sync fault and the failed Close, both", err)
	}
	if temp == nil || j.s.failed == nil || j.s.failed.f != temp || closes[temp] != 1 {
		t.Fatalf("store keeps %v; want the original temporary file, closed once", j.s.failed)
	}
	if _, err := os.Lstat(filepath.Join(dir, stateName+tempSuffix)); err != nil {
		t.Fatalf("the unsettled temporary file was removed: %v", err)
	}
	j.s.fault = nil
	if err := j.RequestCancel(); !errors.Is(err, ErrUnsettled) {
		t.Errorf("RequestCancel after the failed Close = %v, want ErrUnsettled", err)
	}
	if _, err := j.Refresh(context.Background()); !errors.Is(err, ErrUnsettled) {
		t.Errorf("Refresh after the failed Close = %v, want ErrUnsettled", err)
	}
	if err := j.CheckResume(); !errors.Is(err, ErrUnsettled) {
		t.Errorf("CheckResume after the failed Close = %v, want ErrUnsettled", err)
	}
	if !bytes.Equal(before, readJournal(t, dir)) {
		t.Error("the journal changed after the failed Close")
	}
	lock := j.s.lock
	err = j.Close()
	if !errors.Is(err, ErrUnsettled) || j.Close() != err {
		t.Fatalf("Close = %v, want a sticky ErrUnsettled", err)
	}
	if j.s.lock != lock || closes[lock] != 0 || closes[temp] != 1 {
		t.Fatal("Close released the lock or closed a file again")
	}
	if _, err := Open(dir); !errors.Is(err, ErrBusy) {
		t.Fatalf("Open while the temporary file is unsettled = %v, want ErrBusy", err)
	}
	retain(j)
}

// TestWriteFailureSettled is TestTempCloseFails's twin without the Close
// failure: the MODEL sync fault leaves a closed temporary file of the
// store's own, which is removed. The state is pending, not unsettled, and
// a later save stores it.
func TestWriteFailureSettled(t *testing.T) {
	j, _, dir := newJob(t)
	closes := 0
	j.s.closing = func(*os.File) { closes++ }
	j.s.fault = func(stage string) error {
		if stage == "sync" {
			return errFault
		}
		return nil
	}
	err := j.RequestCancel()
	if !errors.Is(err, ErrUnpersisted) || errors.Is(err, ErrUnsettled) {
		t.Fatalf("RequestCancel = %v, want ErrUnpersisted only", err)
	}
	if _, err := os.Lstat(filepath.Join(dir, stateName+tempSuffix)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the closed temporary file was kept: %v", err)
	}
	if closes != 1 || j.s.failed != nil {
		t.Fatalf("%d closes, failed %v", closes, j.s.failed)
	}
	j.s.fault = nil
	if err := j.flush(); err != nil || !j.Status().Durable {
		t.Fatalf("flush = %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestLockCloseFails: the store's Close of its lock really fails (see the
// file comment). Close reports it, still closes the directory, keeps the
// same files, closes each once and never again, and the Job refuses every
// later call.
func TestLockCloseFails(t *testing.T) {
	j, _, dir := newJob(t)
	before := readJournal(t, dir)
	lock, d := j.s.lock, j.s.d
	closes := map[*os.File]int{}
	j.s.closing = func(f *os.File) {
		closes[f]++
		if f == lock {
			if err := f.Close(); err != nil {
				t.Errorf("native close of the lock: %v", err)
			}
		}
	}
	err := j.Close()
	if !errors.Is(err, ErrUnsettled) || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Close = %v, want the lock's failed Close", err)
	}
	if j.Close() != err || closes[lock] != 1 || closes[d] != 1 || j.s.lock != lock || j.s.d != d {
		t.Fatalf("second Close or kept files changed: closes %v", closes)
	}
	if err := j.RequestCancel(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("RequestCancel after Close = %v", err)
	}
	if !bytes.Equal(before, readJournal(t, dir)) {
		t.Fatal("the journal changed")
	}
	// The lock's one native close released the flock.
	j2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	j2.Close()
}

// TestJournalUpgrade: a version 1 journal is read as written, and stored
// in the current version at its next save with the same task bytes,
// request ID and identities. MODEL: the version 1 bytes are this
// package's version 1 encoding, the format the version 1 writer produced;
// the genuine version 1 binary was not run.
func TestJournalUpgrade(t *testing.T) {
	j, _, dir := newJob(t)
	st := j.st
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	old, err := encodeVersion(st, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(old, []byte(`"version": 1,`)) || bytes.Contains(old, []byte(`"cleanup"`)) || bytes.Contains(old, []byte(`"log"`)) {
		t.Fatalf("version 1 encoding:\n%s", old)
	}
	if err := os.WriteFile(filepath.Join(dir, stateName), old, 0o600); err != nil {
		t.Fatal(err)
	}
	j, err = Open(dir)
	if err != nil {
		t.Fatalf("Open of a version 1 journal = %v", err)
	}
	defer j.Close()
	if !bytes.Equal(old, readJournal(t, dir)) {
		t.Fatal("Open rewrote the version 1 journal")
	}
	if !bytes.Equal(j.st.Task, st.Task) || j.st.RequestID != st.RequestID || j.st.VM != st.VM {
		t.Fatal("the version 1 journal's identities changed")
	}
	if err := j.RequestCancel(); err != nil {
		t.Fatal(err)
	}
	var w wireState
	if err := json.Unmarshal(readJournal(t, dir), &w); err != nil {
		t.Fatal(err)
	}
	if w.Version != stateVersion || !bytes.Equal(w.Task, st.Task) || w.RequestID != st.RequestID || w.Desired != desiredCancel {
		t.Fatalf("upgraded journal: version %d, request %s, desired %s", w.Version, w.RequestID, w.Desired)
	}
}

// TestJournalUpgradeVersion2: a version 2 journal with a log checkpoint
// and no result download is read as written, and stored as version 3 at
// its next save with the same task bytes, request ID, request domain and
// checkpoint. MODEL: the version 2 bytes are this package's version 2
// encoding; the genuine version 2 binary was not run.
func TestJournalUpgradeVersion2(t *testing.T) {
	j, _, dir := newJob(t)
	st := doubtful(j.st, RetentionEnabled, time.Now().Add(time.Hour))
	st.Submit, st.JobID = submitAccepted, strings.Repeat("1", 32)
	st.LogOffset = 12
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	old, err := encodeVersion(st, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(old, []byte(`"version": 2,`)) || bytes.Contains(old, []byte(`"download"`)) {
		t.Fatalf("version 2 encoding:\n%s", old)
	}
	if err := os.WriteFile(filepath.Join(dir, stateName), old, 0o600); err != nil {
		t.Fatal(err)
	}
	j, err = Open(dir)
	if err != nil {
		t.Fatalf("Open of a version 2 journal = %v", err)
	}
	defer j.Close()
	if !bytes.Equal(old, readJournal(t, dir)) {
		t.Fatal("Open rewrote the version 2 journal")
	}
	if err := j.RequestCancel(); err != nil {
		t.Fatal(err)
	}
	var w wireState
	if err := json.Unmarshal(readJournal(t, dir), &w); err != nil {
		t.Fatal(err)
	}
	if w.Version != 3 || !bytes.Equal(w.Task, st.Task) || w.RequestID != st.RequestID || w.Lineage != st.Lineage || w.Epoch != st.Epoch || w.Log == nil || w.Log.Offset != 12 || w.Download != nil {
		t.Fatalf("upgraded journal: %+v", w)
	}
}

// regroupFields returns the protobuf encoding b with its fields in the
// reverse order of their first appearance, keeping the order within each
// field, so that it decodes to the same message.
func regroupFields(t *testing.T, b []byte) []byte {
	t.Helper()
	var order []protowire.Number
	byNumber := map[protowire.Number][][]byte{}
	for len(b) > 0 {
		num, _, n := protowire.ConsumeField(b)
		if n < 0 {
			t.Fatal(protowire.ParseError(n))
		}
		if _, ok := byNumber[num]; !ok {
			order = append(order, num)
		}
		byNumber[num] = append(byNumber[num], b[:n])
		b = b[n:]
	}
	var out []byte
	for i := len(order) - 1; i >= 0; i-- {
		for _, f := range byNumber[order[i]] {
			out = append(out, f...)
		}
	}
	return out
}

// TestJournalTaskBytesKept: a stored task in another valid field order,
// as another protobuf encoder may write one, is accepted and kept byte for
// byte, never encoded again; it is the same task.
func TestJournalTaskBytesKept(t *testing.T) {
	j, _, dir := newJob(t)
	st := j.st
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	other := regroupFields(t, st.Task)
	if bytes.Equal(other, st.Task) {
		t.Fatal("regrouping did not change the encoding; the test would show nothing")
	}
	want, err := decodeTask(st.Task)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeTask(other)
	if err != nil || !proto.Equal(got, want) {
		t.Fatalf("regrouped task = %v, %v", got, err)
	}
	st.Task = other
	data, err := encodeState(st)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, stateName), data, 0o600); err != nil {
		t.Fatal(err)
	}
	j, err = Open(dir)
	if err != nil {
		t.Fatalf("Open of a journal with another task encoding = %v", err)
	}
	defer j.Close()
	if err := j.RequestCancel(); err != nil {
		t.Fatal(err)
	}
	var w wireState
	if err := json.Unmarshal(readJournal(t, dir), &w); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(w.Task, other) {
		t.Fatal("a save re-encoded the stored task")
	}
}

// doubtful returns st with the workdir finalized and a submit sent whose
// outcome is unknown, in retention mode retention, as the real commit
// path stores it.
func doubtful(st state, retention string, until time.Time) state {
	st.Retention = retention
	if retention == RetentionEnabled {
		st.Lineage, st.Epoch, st.RetryUntil = strings.Repeat("d", 32), 1, until.UTC()
	}
	st.Upload, st.UploadID = uploadDone, strings.Repeat("e", 32)
	st.Workdir = artifact.Ref{ID: strings.Repeat("f", 32), SHA256: st.Snapshot.SHA256, Size: st.Snapshot.Size}
	st.Acknowledged = st.Snapshot.Size
	st.Submit = submitSent
	return st
}

// TestCheckResume: each refusal the journal alone decides is made by a
// Job neither bound nor attached, so without any VM, SSH or job request;
// a journal that may continue passes.
func TestCheckResume(t *testing.T) {
	now := time.Now()
	for _, tt := range []struct {
		name   string
		mutate func(st *state)
		want   error
		hold   string
	}{
		{name: "new"},
		{name: "live submit in doubt", mutate: func(st *state) { *st = doubtful(*st, RetentionEnabled, now.Add(time.Hour)) }},
		{name: "held", mutate: func(st *state) { st.Hold = HoldAcceptedTask }, want: ErrHeld, hold: HoldAcceptedTask},
		{name: "canceled", mutate: func(st *state) { st.Desired = desiredCancel }, want: errCanceled},
		{name: "begin in doubt", mutate: func(st *state) { st.Retention, st.Upload = RetentionDisabled, uploadBeginSent }, want: ErrHeld, hold: HoldUploadBegin},
		{name: "expired submit in doubt", mutate: func(st *state) { *st = doubtful(*st, RetentionEnabled, now.Add(-time.Second)) }, want: ErrDomainExpired},
	} {
		t.Run(tt.name, func(t *testing.T) {
			j, _, dir := newJob(t)
			if tt.mutate != nil {
				next := j.st
				tt.mutate(&next)
				if err := j.commit(next); err != nil {
					t.Fatal(err)
				}
			}
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			j, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			err = j.CheckResume()
			if tt.want == nil && err != nil || tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("CheckResume = %v, want %v", err, tt.want)
			}
			if j.Status().Hold != tt.hold || !j.Status().Durable {
				t.Fatalf("status %+v, want hold %q stored", j.Status(), tt.hold)
			}
		})
	}
}

// TestRequestCancelBeginInDoubt: a canceled job whose Begin is in doubt
// was never submitted, but its upload ID is unknown: it is held, latched,
// and needs no remote call, because none can find or clean the upload.
func TestRequestCancelBeginInDoubt(t *testing.T) {
	j, _, _ := newJob(t)
	next := j.st
	next.Retention, next.Upload = RetentionDisabled, uploadBeginSent
	if err := j.commit(next); err != nil {
		t.Fatal(err)
	}
	if err := j.RequestCancel(); !errors.Is(err, ErrHeld) {
		t.Fatalf("RequestCancel = %v, want ErrHeld", err)
	}
	st := j.Status()
	if st.Hold != HoldUploadBegin || st.State != "NEVER_SUBMITTED" || st.Cleanup != "" || j.NeedsRemote() || j.Known() {
		t.Fatalf("status %+v", st)
	}
}

// TestRequestCancelKnownUpload: a canceled job never submitted whose
// upload ID is known ends, and needs a remote call only to clean up that
// upload; its twin with a submit in doubt is held and cleans up nothing.
func TestRequestCancelKnownUpload(t *testing.T) {
	j, _, _ := newJob(t)
	next := j.st
	next.Retention, next.Upload, next.UploadID = RetentionDisabled, uploadOpen, strings.Repeat("e", 32)
	next.Workdir = artifact.Ref{ID: strings.Repeat("f", 32), SHA256: next.Snapshot.SHA256, Size: next.Snapshot.Size}
	if err := j.commit(next); err != nil {
		t.Fatal(err)
	}
	if err := j.RequestCancel(); err != nil {
		t.Fatal(err)
	}
	if st := j.Status(); st.State != "NEVER_SUBMITTED" || st.Hold != "" || !j.NeedsRemote() || j.Known() || !j.needsCleanup() {
		t.Fatalf("status %+v", st)
	}

	d, _, _ := newJob(t)
	if err := d.commit(doubtful(d.st, RetentionDisabled, time.Time{})); err != nil {
		t.Fatal(err)
	}
	if err := d.RequestCancel(); !errors.Is(err, ErrHeld) || d.NeedsRemote() || d.needsCleanup() {
		t.Fatalf("submit in doubt: RequestCancel = %v, needs remote %v", err, d.NeedsRemote())
	}
}
