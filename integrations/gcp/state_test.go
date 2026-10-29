//go:build unix

package gce

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// These tests use the real filesystem. Faults of a file's first Close or
// a directory sync are injected through the fileClose and fsyncDir seams,
// which run the real call first where a file is involved. NOT_RUN by
// author.

// injectClose makes the first Close of files whose name ends in suffix
// return err after really closing them, for the rest of the test.
func injectClose(t *testing.T, suffix string, err error) {
	t.Cleanup(func() { fileClose = (*os.File).Close })
	fileClose = func(f *os.File) error {
		cerr := f.Close()
		if strings.HasSuffix(f.Name(), suffix) {
			return err
		}
		return cerr
	}
}

// injectSync makes directory syncs fail with err while *on is true.
func injectSync(t *testing.T, on *bool, err error) {
	t.Cleanup(func() { fsyncDir = syncDir })
	fsyncDir = func(d *os.File) error {
		if *on {
			return err
		}
		return syncDir(d)
	}
}

// newStateMachine creates a machine with no service in a test-owned root.
func newStateMachine(t *testing.T) (*Machine, string, *stateRoot) {
	t.Helper()
	sr := newStateRoot(t)
	dir := filepath.Join(sr.dir, "state")
	m, err := Create(dir, testConfig())
	sr.own(t, nil, m, err)
	if err != nil {
		t.Fatal(err)
	}
	return m, dir, sr
}

func TestCreateOpenRules(t *testing.T) {
	m, dir, sr := newStateMachine(t)
	m2, err := Open(dir, testConfig())
	sr.own(t, nil, m2, err)
	if err == nil {
		t.Error("second Open while locked succeeded")
	}
	name := m.rec.InstanceName
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	other := testConfig()
	other.DiskGiB = 30
	m2, err = Open(dir, other)
	sr.own(t, nil, m2, err)
	if err == nil {
		t.Error("Open with a changed config succeeded")
	}
	m2, err = Create(dir, testConfig())
	sr.own(t, nil, m2, err)
	if err != nil {
		t.Fatalf("Create of intact same-config state: %v", err)
	}
	if m2.rec.InstanceName != name {
		t.Error("Create reserved new names for an existing state")
	}
	if err := m2.Close(); err != nil {
		t.Fatal(err)
	}

	empty := filepath.Join(sr.dir, "empty")
	os.Mkdir(empty, 0o700)
	m3, err := Create(empty, testConfig())
	sr.own(t, nil, m3, err)
	if err == nil {
		t.Error("Create adopted an existing directory without state")
	}
	m3, err = Open(filepath.Join(sr.dir, "missing"), testConfig())
	sr.own(t, nil, m3, err)
	if err == nil {
		t.Error("Open created a state")
	}
	if _, err := os.Stat(filepath.Join(sr.dir, "missing")); err == nil {
		t.Error("Open created a directory")
	}
	bad := testConfig()
	bad.Zone = "nowhere"
	m3, err = Create(filepath.Join(sr.dir, "bad"), bad)
	sr.own(t, nil, m3, err)
	if err == nil {
		t.Error("Create accepted an invalid config")
	}
	if _, err := os.Stat(filepath.Join(sr.dir, "bad")); err == nil {
		t.Error("invalid config created a directory")
	}
}

func TestReadStatusStrict(t *testing.T) {
	_, dir, _ := newStateMachine(t)
	st, err := ReadStatus(dir) // no lock needed
	if err != nil || st.Desired != Running || st.InstanceName == "" || st.Reached {
		t.Fatalf("ReadStatus = %+v, %v", st, err)
	}
	path := filepath.Join(dir, stateName)
	good, _ := os.ReadFile(path)
	tests := []struct {
		name string
		edit func() error
	}{
		{"trailing", func() error { return os.WriteFile(path, append(append([]byte{}, good...), '{', '}'), 0o600) }},
		{"version", func() error {
			return os.WriteFile(path, []byte(strings.Replace(string(good), `"version":2`, `"version":3`, 1)), 0o600)
		}},
		{"unknown field", func() error {
			return os.WriteFile(path, []byte(strings.Replace(string(good), `{`, `{"token":"x",`, 1)), 0o600)
		}},
		{"case alias", func() error {
			return os.WriteFile(path, []byte(strings.Replace(string(good), `{`, `{"Phase":"created",`, 1)), 0o600)
		}},
		{"mode", func() error { return os.Chmod(path, 0o644) }},
		{"symlink", func() error {
			os.Remove(path)
			alt := filepath.Join(t.TempDir(), "alt")
			os.WriteFile(alt, good, 0o600)
			return os.Symlink(alt, path)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Remove(path)
			os.WriteFile(path, good, 0o600)
			if err := tt.edit(); err != nil {
				t.Fatal(err)
			}
			if _, err := ReadStatus(dir); err == nil {
				t.Error("ReadStatus accepted it")
			}
		})
	}
	os.Remove(path)
	os.WriteFile(path, good, 0o600)
}

func TestSetGoalLatchAndCommit(t *testing.T) {
	m, dir, _ := newStateMachine(t)
	if err := m.SetGoal("sideways"); err == nil {
		t.Error("unknown goal accepted")
	}
	if err := m.SetGoal(Absent); err != nil {
		t.Fatal(err)
	}
	if st, _ := ReadStatus(dir); st.Desired != Absent {
		t.Error("goal not committed before Drive")
	}
	for _, g := range []Goal{Running, Stopped, Restarted} {
		if err := m.SetGoal(g); err == nil {
			t.Errorf("SetGoal(%s) after absent succeeded", g)
		}
	}
}

// The new state directory's entry must be synced in its parent before
// Create returns a machine. If that fails, Create fails, keeps nothing
// locked, and the directory without state is never adopted.
func TestParentSyncFailure(t *testing.T) {
	on := true
	errSync := errors.New("injected parent sync failure")
	injectSync(t, &on, errSync)
	sr := newStateRoot(t)
	dir := filepath.Join(sr.dir, "state")
	m, err := Create(dir, testConfig())
	sr.own(t, nil, m, err)
	if !errors.Is(err, errSync) || m != nil {
		t.Fatalf("Create = %v, %v; want the sync failure", m, err)
	}
	on = false
	if _, err := os.Stat(filepath.Join(dir, stateName)); err == nil {
		t.Error("state committed before the parent sync")
	}
	m, err = Create(dir, testConfig())
	sr.own(t, nil, m, err)
	if err == nil {
		t.Fatal("Create adopted the directory left by a failed parent sync")
	}
	if strings.Contains(err.Error(), errBusy.Error()) {
		t.Error("the failed Create kept the lock")
	}
}

// A state commit whose rename succeeded but whose directory sync failed
// leaves the new goal possibly committed. The machine keeps it, refuses
// every later call and holds the lock; it never restores the old goal.
// Close commits the same state again and releases the lock only when that
// succeeds.
func TestPostRenameFailureFences(t *testing.T) {
	m, dir, sr := newStateMachine(t)
	on := true
	errSync := errors.New("injected directory sync failure")
	injectSync(t, &on, errSync)
	if err := m.SetGoal(Absent); !errors.Is(err, errSync) {
		t.Fatalf("SetGoal = %v", err)
	}
	if st, err := ReadStatus(dir); err != nil || st.Desired != Absent {
		t.Fatalf("on disk: %+v, %v; want the renamed absent goal", st, err)
	}
	if m.rec.Desired != Absent || !m.unsaved {
		t.Fatalf("in memory: %s, unsaved %v; want absent kept as possibly committed", m.rec.Desired, m.unsaved)
	}
	if err := m.SetGoal(Running); err == nil {
		t.Fatal("SetGoal(Running) admitted after an unconfirmed absent commit")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := m.Drive(ctx, testTok(), DriveOptions{Steps: 1, Interval: time.Second}); err == nil {
		t.Fatal("Drive admitted")
	}
	if _, err := m.Refresh(ctx, testTok()); err == nil {
		t.Fatal("Refresh admitted")
	}
	if err := m.Close(); !errors.Is(err, errSync) {
		t.Fatalf("Close = %v; want the commit failure", err)
	}
	m2, err := Open(dir, testConfig())
	sr.own(t, nil, m2, err)
	if err == nil {
		t.Fatal("lock released before the commit was confirmed")
	}
	on = false
	if err := m.Close(); err != nil {
		t.Fatalf("Close after the sync recovers = %v", err)
	}
	m2, err = Open(dir, testConfig())
	sr.own(t, nil, m2, err)
	if err != nil || m2.rec.Desired != Absent {
		t.Fatalf("reopened: %v; goal must stay absent", err)
	}
}

// A state temporary file whose first Close fails is kept by the machine,
// which keeps its lock and refuses all later calls. The old state stays
// committed.
func TestTempCloseFailureKeepsOwner(t *testing.T) {
	m, dir, sr := newStateMachine(t)
	errClose := errors.New("injected temp close failure")
	injectClose(t, stateTemp, errClose)
	if err := m.SetGoal(Stopped); !errors.Is(err, errClose) {
		t.Fatalf("SetGoal = %v", err)
	}
	var ce *closeError
	if m.heldFile == nil || !errors.As(m.fault, &ce) || ce.f == nil {
		t.Fatal("machine does not keep the file whose close failed")
	}
	for i := 0; i < 2; i++ {
		if err := m.Close(); !errors.Is(err, errClose) {
			t.Fatalf("Close = %v", err)
		}
	}
	m2, err := Open(dir, testConfig())
	sr.own(t, nil, m2, err)
	if err == nil {
		t.Fatal("lock released")
	}
	if st, err := ReadStatus(dir); err != nil || st.Desired != Running {
		t.Errorf("on disk: %+v, %v; want the old goal", st, err)
	}
}

// Constructor failures keep their owners. A state file whose first Close
// fails keeps the lock held by the returned error; an ordinary failure
// whose lock Close fails returns both causes and keeps the lock file. The
// fixture retains each such error, and its root, before any check: past
// the subtest, until the test process exits.
func TestConstructorCustody(t *testing.T) {
	var roots []*stateRoot
	t.Run("state close", func(t *testing.T) {
		m, dir, sr := newStateMachine(t)
		roots = append(roots, sr)
		if err := m.Close(); err != nil {
			t.Fatal(err)
		}
		errClose := errors.New("injected state close failure")
		injectClose(t, stateName, errClose)
		m2, err := Open(dir, testConfig())
		sr.own(t, nil, m2, err)
		var le *lockedError
		if !errors.Is(err, errClose) || !errors.As(err, &le) || le.lock == nil {
			t.Fatalf("Open = %v; want the close failure with the lock kept", err)
		}
		fileClose = (*os.File).Close
		m3, err := Open(dir, testConfig())
		sr.own(t, nil, m3, err)
		if !errors.Is(err, errBusy) {
			t.Errorf("Open after a kept lock = %v, want busy", err)
		}
	})
	t.Run("lock close", func(t *testing.T) {
		m, dir, sr := newStateMachine(t)
		roots = append(roots, sr)
		if err := m.Close(); err != nil {
			t.Fatal(err)
		}
		other := testConfig()
		other.DiskGiB = 30
		errClose := errors.New("injected lock close failure")
		injectClose(t, lockName, errClose)
		m2, err := Open(dir, other)
		sr.own(t, nil, m2, err)
		var ce *closeError
		if err == nil || !strings.Contains(err.Error(), "config differs") || !errors.As(err, &ce) || !strings.HasSuffix(ce.f.Name(), lockName) {
			t.Fatalf("Open = %v; want the config error and the lock close failure", err)
		}
	})
	for _, sr := range roots {
		checkRetained(t, sr)
	}
}

// checkRetained checks, after its test ended, that sr kept its directory
// and a constructor error owning a file or the lock, and that sr is held
// until the process exits.
func checkRetained(t *testing.T, sr *stateRoot) {
	t.Helper()
	if _, err := os.Stat(sr.dir); err != nil {
		t.Errorf("root %s retired with an owner unsettled: %v", sr.dir, err)
	}
	owned := false
	for _, err := range sr.errs {
		owned = owned || ownsFile(err)
	}
	if !owned {
		t.Errorf("root %s keeps no constructor owner", sr.dir)
	}
	unsettledMu.Lock()
	defer unsettledMu.Unlock()
	for _, u := range unsettled {
		if u == sr {
			return
		}
	}
	t.Errorf("root %s is not held past its test", sr.dir)
}

// tamper rewrites the stored state with edit applied to its decoded
// JSON form, keeping the file private.
func tamper(t *testing.T, dir string, edit func(map[string]any)) {
	t.Helper()
	path := filepath.Join(dir, stateName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var v map[string]any
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	edit(v)
	data, _ = json.Marshal(v)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// A reopened prepared operation must be exactly the request its kind
// implies for the stored config and reserved names, with evidence that
// fits its state. Any other is refused before any request.
func TestPreparedStateTampering(t *testing.T) {
	op := func(edit func(o map[string]any)) func(map[string]any) {
		return func(v map[string]any) { edit(v["op"].(map[string]any)) }
	}
	tests := []struct {
		name string
		edit func(map[string]any)
	}{
		{"method", op(func(o map[string]any) { o["method"] = "DELETE" })},
		{"path", op(func(o map[string]any) { o["path"] = "projects/another-project/zones/us-central1-a/instances/victim" })},
		{"target", op(func(o map[string]any) { o["target"] = "projects/skygo-test/zones/us-central1-a/disks/other" })},
		{"body", op(func(o map[string]any) {
			o["body"] = []byte(`{"name":"sg-disk-00000000000000000000","sizeGb":"65536"}`)
		})},
		{"query", op(func(o map[string]any) { o["query"] = "requestId=00000000-0000-4000-8000-000000000000" })},
		{"kind", op(func(o map[string]any) { o["kind"] = kindInstanceInsert })},
		{"done as current", op(func(o map[string]any) { o["state"] = opDone })},
		{"unknown state", op(func(o map[string]any) { o["state"] = "paused" })},
		{"sent without time", op(func(o map[string]any) { o["state"] = opSent })},
		{"named while prepared", op(func(o map[string]any) { o["name"], o["id"] = "operation-1", "77" })},
		{"history reuses id", func(v map[string]any) {
			h := map[string]any{}
			for k, x := range v["op"].(map[string]any) {
				h[k] = x
			}
			h["state"] = opWithdrawn
			v["history"] = []any{h}
		}},
		{"disk id without send", func(v map[string]any) { v["disk_sent"], v["disk_id"] = false, "9001" }},
		{"created without ids", func(v map[string]any) { v["phase"] = phaseCreated }},
		{"held reason", func(v map[string]any) { v["held"] = "anything" }},
		{"install", func(v map[string]any) { v["install"] = "00000000-0000-4000-8000-000000000000" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			m, dir := newTestMachine(t, f)
			crashAt(t, m, "prepared")
			if m.rec.Op == nil || m.rec.Op.State != opPrepared {
				t.Fatalf("no prepared operation: %+v", m.rec.Op)
			}
			before := len(f.requests())
			tamper(t, dir, tt.edit)
			m2, err := open(dir, testConfig(), f.client())
			f.root.own(t, f, m2, err)
			if err == nil {
				t.Fatal("Open accepted the tampered state")
			}
			if _, err := ReadStatus(dir); err == nil {
				t.Error("ReadStatus accepted the tampered state")
			}
			if n := len(f.requests()); n != before {
				t.Errorf("%d requests after tampering", n-before)
			}
		})
	}
	t.Run("untampered", func(t *testing.T) {
		f := newFake(t)
		m, dir := newTestMachine(t, f)
		crashAt(t, m, "prepared")
		m2 := reopen(t, f, dir)
		if st, err := drive(t, m2, 50); err != nil || !st.Reached {
			t.Fatalf("Drive = %+v, %v", st, err)
		}
		if n := f.count("POST", "/disks"); n != 1 {
			t.Errorf("%d disk inserts, want 1", n)
		}
	})
}
