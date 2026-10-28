package kube

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

const otherID = "abcdef0123456789abcdef0123456789"

// receiptJoinTimeout bounds every wait for an original operation or MODEL
// gate, so that a regression fails instead of hanging the test binary.
const receiptJoinTimeout = 30 * time.Second

// receiptFixture is a private state root outside t.TempDir, under
// $SKYGO_RECEIPT_TEST_DIR or $HOME/tmp/skygo. Testing never removes it
// automatically: its cleanup, registered as soon as it exists, removes it
// and every alias transferred to it with own only after every original
// operation started with run has positively returned. Otherwise the
// directory, its aliases and the operations are retained.
type receiptFixture struct {
	dir     string
	mu      sync.Mutex
	ops     []*receiptOp
	aliases []string
	kept    bool
}

// receiptOp is one original operation running in its own goroutine.
type receiptOp struct {
	name string
	done chan struct{}
	err  error
}

// retainedReceiptFixtures keeps fixtures with unjoined operations, and so
// their original references, reachable for the rest of the test binary.
// Tests are not parallel.
var retainedReceiptFixtures []*receiptFixture

func newReceiptFixture(t *testing.T) *receiptFixture {
	t.Helper()
	base := os.Getenv("SKYGO_RECEIPT_TEST_DIR")
	if base == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			t.Fatal(err)
		}
		base = filepath.Join(home, "tmp", "skygo", time.Now().UTC().Format("20060102")+"-execution-receipt-tests")
	}
	if err := os.MkdirAll(base, 0o700); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(base, "fixture-")
	if err != nil {
		t.Fatal(err)
	}
	f := &receiptFixture{dir: dir}
	t.Cleanup(func() { f.retire(t) })
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(resolved, 0o700); err != nil {
		t.Fatal(err)
	}
	f.dir = resolved
	return f
}

// run starts op as an original operation of the fixture.
func (f *receiptFixture) run(name string, op func() error) *receiptOp {
	o := &receiptOp{name: name, done: make(chan struct{})}
	f.mu.Lock()
	f.ops = append(f.ops, o)
	f.mu.Unlock()
	go func() {
		defer close(o.done)
		o.err = op()
	}()
	return o
}

// wait joins o within receiptJoinTimeout and returns its result.
func (o *receiptOp) wait(t *testing.T) error {
	t.Helper()
	select {
	case <-o.done:
		return o.err
	case <-time.After(receiptJoinTimeout):
		t.Fatalf("%s did not return", o.name)
	}
	return nil
}

// own transfers path, such as the fixture's root renamed aside, into the
// fixture's custody. Call it before the rename, so that the path is owned
// whenever it may exist.
func (f *receiptFixture) own(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.aliases = append(f.aliases, path)
}

// keep transfers the fixture into a MODELED process-lifetime HOLD: a
// receipt store whose operation permit was retained by an unsettled log body
// stays in use until the test binary exits, so retirement keeps every owned
// path and reports it instead of removing it.
func (f *receiptFixture) keep() {
	f.mu.Lock()
	f.kept = true
	f.mu.Unlock()
	if !slices.Contains(retainedReceiptFixtures, f) {
		retainedReceiptFixtures = append(retainedReceiptFixtures, f)
	}
}

// retire removes the fixture once every operation is joined. A cancel or
// timeout is not a join: an unjoined operation retains every owned path.
func (f *receiptFixture) retire(t *testing.T) {
	f.mu.Lock()
	kept := f.kept
	f.mu.Unlock()
	if kept {
		t.Logf("MODELED HOLD: retained %s until the test binary exits", strings.Join(f.paths(), ", "))
		return
	}
	held, err := f.release(receiptJoinTimeout)
	if held != "" {
		t.Errorf("%s did not return; retained %s", held, strings.Join(f.paths(), ", "))
	}
	if err != nil {
		t.Error(err)
	}
}

// paths returns the fixture directory and its aliases.
func (f *receiptFixture) paths() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string{f.dir}, f.aliases...)
}

// release waits up to timeout for every operation to return. If one does
// not, release removes nothing, retains f with its operations and paths,
// and returns that operation's name. Otherwise it removes every owned path
// and releases f from retention.
func (f *receiptFixture) release(timeout time.Duration) (held string, err error) {
	f.mu.Lock()
	ops := append([]*receiptOp(nil), f.ops...)
	f.mu.Unlock()
	deadline := time.After(timeout)
	for _, o := range ops {
		select {
		case <-o.done:
		case <-deadline:
			if !slices.Contains(retainedReceiptFixtures, f) {
				retainedReceiptFixtures = append(retainedReceiptFixtures, f)
			}
			return o.name, nil
		}
	}
	for _, path := range f.paths() {
		err = errors.Join(err, os.RemoveAll(path))
	}
	if err == nil {
		retainedReceiptFixtures = slices.DeleteFunc(retainedReceiptFixtures, func(r *receiptFixture) bool { return r == f })
	}
	return "", err
}

// receiptDir returns a private state directory without symlinks.
func receiptDir(t *testing.T) string {
	t.Helper()
	return newReceiptFixture(t).dir
}

// TestReceiptFixtureCustody checks that a root renamed aside stays in its
// fixture's custody: while an original operation that writes into the
// renamed root has not returned, retirement holds and keeps both the
// original and the renamed root; once it returned, both are removed.
func TestReceiptFixtureCustody(t *testing.T) {
	outer := newReceiptFixture(t)
	hold := newGate()
	t.Cleanup(hold.release) // runs before outer's retirement joins the writer
	f := &receiptFixture{dir: filepath.Join(outer.dir, "root")}
	if err := os.Mkdir(f.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	moved := f.dir + "-moved"
	f.own(moved)
	if err := os.Rename(f.dir, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(f.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	// The late writer is an original operation of both fixtures, so that
	// outer's retirement joins it even if this test fails first.
	writer := outer.run("late writer", func() error {
		<-hold.ch
		return os.WriteFile(filepath.Join(moved, "late"), []byte("x"), 0o600)
	})
	f.mu.Lock()
	f.ops = append(f.ops, writer)
	f.mu.Unlock()

	// HOLD: the writer cannot return before hold is released.
	if held, err := f.release(10 * time.Millisecond); held != "late writer" || err != nil {
		t.Fatalf("release while held: %q %v", held, err)
	}
	for _, path := range []string{f.dir, moved} {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("held fixture removed %s: %v", path, err)
		}
	}
	if !slices.Contains(retainedReceiptFixtures, f) {
		t.Fatal("held fixture not retained")
	}

	// Positive twin: after the original operation returned, both roots go.
	hold.release()
	if err := writer.wait(t); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(moved, "late")); err != nil {
		t.Fatalf("late write: %v", err)
	}
	if held, err := f.release(receiptJoinTimeout); held != "" || err != nil {
		t.Fatalf("release after join: %q %v", held, err)
	}
	for _, path := range []string{f.dir, moved} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("joined fixture kept %s: %v", path, err)
		}
	}
	if slices.Contains(retainedReceiptFixtures, f) {
		t.Fatal("joined fixture still retained")
	}
}

func testStore(t *testing.T) (*receiptStore, string) {
	t.Helper()
	root := receiptDir(t)
	s, err := openReceiptStore(root, "owner", "workloads", testBinding)
	if err != nil {
		t.Fatal(err)
	}
	return s, root
}

// restartedStore returns a store of the same pinned scope without generation
// memory, as after a restart.
func restartedStore(s *receiptStore) *receiptStore {
	return &receiptStore{root: s.root, scope: s.scope, dir: s.dir, rootID: s.rootID, scopeID: s.scopeID,
		owner: s.owner, namespace: s.namespace, binding: s.binding, limit: s.limit, seen: map[string]uint64{}}
}

func testReceipt(id string) executionReceipt {
	name, _ := jobName(id)
	return executionReceipt{Attempt: id, TaskSHA256: strings.Repeat("a", 64), Kind: "Job", Name: name,
		Nonce: strings.Repeat("b", 64), SpecSHA256: strings.Repeat("c", 64), Phase: phaseIntent}
}

func receiptFile(s *receiptStore, id string) string {
	return filepath.Join(s.dir, id+".json")
}

func pinTestReceipt(t *testing.T, s *receiptStore, r executionReceipt) {
	t.Helper()
	id := r.Attempt
	if err := s.create(r); err != nil {
		t.Fatal(err)
	}
	for _, f := range []func(*executionReceipt) error{
		func(x *executionReceipt) error { x.Phase = phaseSent; return nil },
		func(x *executionReceipt) error { x.Phase, x.UID = phasePinned, "job-uid"; return nil },
	} {
		if _, err := s.update(id, f); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReceiptStoreTransitions(t *testing.T) {
	s, root := testStore(t)
	if err := s.create(testReceipt(testID)); err != nil {
		t.Fatal(err)
	}
	if err := s.create(testReceipt(testID)); !errors.Is(err, errReceiptExists) {
		t.Fatalf("second create: %v", err)
	}
	r, err := s.update(testID, func(x *executionReceipt) error { x.Phase = phaseSent; return nil })
	if err != nil || r.Generation != 2 || r.Phase != phaseSent {
		t.Fatalf("sent: %+v %v", r, err)
	}
	// An update that changes nothing writes nothing.
	r, err = s.update(testID, func(*executionReceipt) error { return nil })
	if err != nil || r.Generation != 2 {
		t.Fatalf("unchanged: %+v %v", r, err)
	}
	data, err := os.ReadFile(receiptFile(s, testID))
	if err != nil {
		t.Fatal(err)
	}
	canonical, _ := json.Marshal(r)
	if !bytes.Equal(data, canonical) {
		t.Fatalf("stored body is not canonical:\n%s\n%s", data, canonical)
	}
	// A new store, as after a restart, reads the same record.
	reopened, err := openReceiptStore(root, "owner", "workloads", testBinding)
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopened.load(testID)
	if err != nil || got.Generation != 2 || got.Phase != phaseSent {
		t.Fatalf("reopened: %+v %v", got, err)
	}
	// A UID is recorded only with the pinned phase.
	if _, err := s.update(testID, func(x *executionReceipt) error { x.UID = "uid"; return nil }); !errors.Is(err, errReceiptCorrupt) {
		t.Fatalf("UID without pin accepted: %v", err)
	}
	// Only a pinned record retires.
	if err := s.retire(testID, func(*executionReceipt) error { return nil }); err == nil {
		t.Fatal("unpinned receipt retired")
	}
	if _, err := s.update(testID, func(x *executionReceipt) error { x.Phase, x.UID = phasePinned, "uid"; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := s.retire(testID, func(*executionReceipt) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if r, err := reopened.load(testID); err != nil || r.Phase != phaseRetired || r.UID != "uid" || r.Generation != 4 {
		t.Fatalf("retired: %+v %v", r, err)
	}
}

// TestReceiptStoreRetirementHold checks that a retired tombstone keeps its
// identity and counted slot, refuses every change, and holds a full store.
func TestReceiptStoreRetirementHold(t *testing.T) {
	s, root := testStore(t)
	s.limit = 2
	pinTestReceipt(t, s, testReceipt(testID))
	// Only retire retires.
	if _, err := s.update(testID, func(x *executionReceipt) error { x.Phase = phaseRetired; return nil }); !errors.Is(err, errReceiptCorrupt) {
		t.Fatalf("update retired a receipt: %v", err)
	}
	pinned, err := s.load(testID)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.retire(testID, func(*executionReceipt) error { return errors.New("refused") }); err == nil {
		t.Fatal("refused retirement retired")
	}
	if err := s.retire(testID, func(*executionReceipt) error { return nil }); err != nil {
		t.Fatal(err)
	}
	tomb, err := s.load(testID)
	want := pinned
	want.Phase, want.Generation = phaseRetired, pinned.Generation+1
	if err != nil || fmt.Sprint(tomb) != fmt.Sprint(want) {
		t.Fatalf("tombstone: %+v %v, want %+v", tomb, err, want)
	}
	before, err := os.ReadFile(receiptFile(s, testID))
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range []*receiptStore{s, restartedStore(s)} {
		if err := st.create(testReceipt(testID)); !errors.Is(err, errReceiptExists) {
			t.Fatalf("tombstone recreated: %v", err)
		}
		if _, err := st.update(testID, func(x *executionReceipt) error { x.Phase = phasePinned; return nil }); !errors.Is(err, errReceiptRetired) {
			t.Fatalf("tombstone updated: %v", err)
		}
		if err := st.retire(testID, func(*executionReceipt) error { return nil }); !errors.Is(err, errReceiptRetired) {
			t.Fatalf("tombstone retired again: %v", err)
		}
	}
	if after, _ := os.ReadFile(receiptFile(s, testID)); !bytes.Equal(before, after) {
		t.Fatal("tombstone changed")
	}
	// HOLD: the tombstone occupies its slot and is not reclaimed.
	s.limit = 1
	if err := s.create(testReceipt(otherID)); !errors.Is(err, errReceiptCapacity) {
		t.Fatalf("full store with tombstone: %v", err)
	}
	if after, _ := os.ReadFile(receiptFile(s, testID)); !bytes.Equal(before, after) {
		t.Fatal("capacity reclaimed the tombstone")
	}
	// Positive twin: with a free slot, another attempt is recorded.
	s.limit = 2
	if err := s.create(testReceipt(otherID)); err != nil {
		t.Fatal(err)
	}
	reopened, err := openReceiptStore(root, "owner", "workloads", testBinding)
	if err != nil {
		t.Fatal(err)
	}
	if r, err := reopened.load(testID); err != nil || r.Phase != phaseRetired {
		t.Fatalf("reopened tombstone: %+v %v", r, err)
	}
	// A tombstone with cancellation or dependents is corrupt.
	for _, edit := range []func(*executionReceipt){
		func(x *executionReceipt) { x.Cancel = stepIntent },
		func(x *executionReceipt) { x.UID = "" },
	} {
		x := tomb
		edit(&x)
		if err := s.check(&x, testID); !errors.Is(err, errReceiptCorrupt) {
			t.Errorf("invalid tombstone accepted: %+v", x)
		}
	}
}

// TestReceiptOperationPermits checks the scope operation lock: shared
// across nested calls and constructors, exclusive without waiting, and
// separate from the journal lock.
func TestReceiptOperationPermits(t *testing.T) {
	ctx := context.Background()
	s, root := testStore(t)
	if err := s.create(testReceipt(testID)); err != nil {
		t.Fatal(err)
	}
	other, err := openReceiptStore(root, "owner", "workloads", testBinding)
	if err != nil {
		t.Fatal(err)
	}
	outer, err := s.operate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	nested, err := s.operate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	shared, err := other.operate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range []*receiptStore{s, other} {
		if _, err := st.exclusive(); !errors.Is(err, errOperationBusy) {
			t.Fatalf("exclusive while shared: %v", err)
		}
	}
	outer()
	if _, err := other.exclusive(); !errors.Is(err, errOperationBusy) {
		t.Fatalf("exclusive while nested permit held: %v", err)
	}
	nested()
	if _, err := other.exclusive(); !errors.Is(err, errOperationBusy) {
		t.Fatalf("exclusive while other constructor holds a permit: %v", err)
	}
	shared()
	release, err := other.exclusive()
	if err != nil {
		t.Fatal(err)
	}
	// The journal lock is separate: records stay readable.
	if _, err := s.load(testID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.exclusive(); !errors.Is(err, errOperationBusy) {
		t.Fatalf("second exclusive: %v", err)
	}
	// A shared permit waits for cleanup only until its context is done.
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := s.operate(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shared during exclusive: %v", err)
	}
	release()
	again, err := s.operate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	again()
}

func TestReceiptStoreRefusesUnsafeRoot(t *testing.T) {
	root := receiptDir(t)
	link := filepath.Join(receiptDir(t), "link")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	open := filepath.Join(receiptDir(t), "open")
	if err := os.Mkdir(open, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(open, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{"", "state", root + "/x/..", link, open, filepath.Join(root, "missing")} {
		if _, err := openReceiptStore(dir, "owner", "workloads", testBinding); err == nil {
			t.Errorf("%q: unsafe state directory accepted", dir)
		}
	}
	if _, err := openReceiptStore(root, "owner", "workloads", ""); err == nil {
		t.Error("unbound target accepted")
	}
	// An unexpected entry in the scope directory refuses the store.
	s, err := openReceiptStore(root, "owner", "workloads", testBinding)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.dir, "notes.txt"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := openReceiptStore(root, "owner", "workloads", testBinding); err == nil {
		t.Fatal("unexpected entry accepted")
	}
	if err := s.create(testReceipt(testID)); err == nil {
		t.Fatal("create with unexpected entry accepted")
	}
}

func TestReceiptStoreRefusesTamperedRecords(t *testing.T) {
	for _, tt := range []struct {
		name   string
		tamper func(t *testing.T, s *receiptStore, path string, data []byte)
		want   error
	}{
		{"unknown-field", func(t *testing.T, _ *receiptStore, path string, data []byte) {
			writeReceiptBytes(t, path, append(bytes.TrimSuffix(data, []byte("}")), []byte(`,"extra":1}`)...))
		}, errReceiptCorrupt},
		{"unknown-version", func(t *testing.T, _ *receiptStore, path string, data []byte) {
			writeReceiptBytes(t, path, bytes.Replace(data, []byte("receipt-v1"), []byte("receipt-v2"), 1))
		}, errReceiptCorrupt},
		{"unknown-phase", func(t *testing.T, _ *receiptStore, path string, data []byte) {
			writeReceiptBytes(t, path, bytes.Replace(data, []byte(`"phase":"pinned"`), []byte(`"phase":"reaped"`), 1))
		}, errReceiptCorrupt},
		{"duplicate-key", func(t *testing.T, _ *receiptStore, path string, data []byte) {
			writeReceiptBytes(t, path, bytes.Replace(data, []byte("{"), []byte(`{"phase":"pinned",`), 1))
		}, errReceiptCorrupt},
		{"trailing-data", func(t *testing.T, _ *receiptStore, path string, data []byte) {
			writeReceiptBytes(t, path, append(data, '\n'))
		}, errReceiptCorrupt},
		{"partial-write", func(t *testing.T, _ *receiptStore, path string, data []byte) {
			writeReceiptBytes(t, path, data[:len(data)/2])
		}, errReceiptCorrupt},
		{"oversized", func(t *testing.T, _ *receiptStore, path string, data []byte) {
			writeReceiptBytes(t, path, append(data, bytes.Repeat([]byte(" "), maxReceiptBytes)...))
		}, errReceiptCorrupt},
		{"foreign-owner", func(t *testing.T, _ *receiptStore, path string, data []byte) {
			writeReceiptBytes(t, path, bytes.Replace(data, []byte(`"owner":"owner"`), []byte(`"owner":"other"`), 1))
		}, errReceiptForeign},
		{"foreign-attempt", func(t *testing.T, s *receiptStore, path string, data []byte) {
			writeReceiptBytes(t, receiptFile(s, otherID), data)
		}, errReceiptForeign},
		{"mode", func(t *testing.T, _ *receiptStore, path string, _ []byte) {
			if err := os.Chmod(path, 0o644); err != nil {
				t.Fatal(err)
			}
		}, nil},
		{"hard-link", func(t *testing.T, s *receiptStore, path string, _ []byte) {
			if err := os.Link(path, filepath.Join(s.dir, otherID+".tmp")); err != nil {
				t.Fatal(err)
			}
		}, nil},
		{"symlink", func(t *testing.T, s *receiptStore, path string, data []byte) {
			elsewhere := filepath.Join(receiptDir(t), "receipt")
			writeReceiptBytes(t, elsewhere, data)
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(elsewhere, path); err != nil {
				t.Fatal(err)
			}
		}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := testStore(t)
			pinTestReceipt(t, s, testReceipt(testID))
			path := receiptFile(s, testID)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			tt.tamper(t, s, path, data)
			id := testID
			if tt.name == "foreign-attempt" {
				id = otherID
			}
			fresh := restartedStore(s)
			_, err = fresh.load(id)
			if err == nil || tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("load: %v, want %v", err, tt.want)
			}
			if _, err := fresh.update(id, func(x *executionReceipt) error { x.Cancel = stepIntent; return nil }); err == nil {
				t.Fatal("tampered receipt updated")
			}
		})
	}
}

func writeReceiptBytes(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestReceiptStoreRollbackAndDisappearance(t *testing.T) {
	s, _ := testStore(t)
	if err := s.create(testReceipt(testID)); err != nil {
		t.Fatal(err)
	}
	old, err := os.ReadFile(receiptFile(s, testID))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.update(testID, func(x *executionReceipt) error { x.Phase = phaseSent; return nil }); err != nil {
		t.Fatal(err)
	}
	writeReceiptBytes(t, receiptFile(s, testID), old)
	if _, err := s.load(testID); !errors.Is(err, backend.ErrIdentityMismatch) || !strings.Contains(err.Error(), "rolled back") {
		t.Fatalf("rollback: %v", err)
	}
	if err := os.Remove(receiptFile(s, testID)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.load(testID); !errors.Is(err, backend.ErrIdentityMismatch) || !strings.Contains(err.Error(), "disappeared") {
		t.Fatalf("disappeared: %v", err)
	}
	if err := s.create(testReceipt(testID)); err == nil {
		t.Fatal("disappeared receipt recreated")
	}
}

func TestReceiptStoreCapacityAndPaths(t *testing.T) {
	s, _ := testStore(t)
	s.limit = 1
	if err := s.create(testReceipt(testID)); err != nil {
		t.Fatal(err)
	}
	if err := s.create(testReceipt(otherID)); !errors.Is(err, errReceiptCapacity) {
		t.Fatalf("capacity: %v", err)
	}
	if _, err := os.Stat(receiptFile(s, otherID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("refused receipt was written")
	}
	for _, id := range []string{"../../" + testID, testID + "/..", strings.ToUpper(testID), ""} {
		if _, err := s.load(id); err == nil || errors.Is(err, errReceiptMissing) {
			t.Errorf("%q: invalid attempt reached the file system: %v", id, err)
		}
	}
}

func TestReceiptStoreFaults(t *testing.T) {
	for _, tt := range []struct {
		step      string
		visible   bool
		uncertain bool
	}{{"sync", false, false}, {"rename", false, false}, {"dirsync", true, true}} {
		t.Run(tt.step, func(t *testing.T) {
			s, _ := testStore(t)
			s.fault = func(step string, _ *executionReceipt) error {
				if step == tt.step {
					return errors.New("injected fault")
				}
				return nil
			}
			err := s.create(testReceipt(testID))
			var uncertain receiptUncertain
			if err == nil || errors.As(err, &uncertain) != tt.uncertain {
				t.Fatalf("create: %v", err)
			}
			_, statErr := os.Stat(receiptFile(s, testID))
			if (statErr == nil) != tt.visible {
				t.Fatalf("visible: %v", statErr)
			}
			if _, err := os.Stat(filepath.Join(s.dir, testID+".tmp")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("temporary file left behind")
			}
		})
	}
}

// TestReceiptStoreModeledInventory stores MODELED dependent rows. No backend
// method creates them; the rows only exercise the bounded representation.
func TestReceiptStoreModeledInventory(t *testing.T) {
	spec := strings.Repeat("d", 64)
	valid := []receiptObject{
		{Kind: "ResourceClaimTemplate", Name: "skygo-" + testID + "-gpu", OwnerUID: "job-uid", SpecSHA256: spec, Phase: objectObserved, UID: "template-uid"},
		{Kind: "Pod", Name: "skygo-" + testID + "-a", OwnerUID: "job-uid", SpecSHA256: spec, Phase: objectObserved, UID: "pod-a"},
		{Kind: "Pod", Name: "skygo-" + testID + "-b", OwnerUID: "job-uid", SpecSHA256: spec, Phase: objectGone, UID: "pod-b"},
		{Kind: "ResourceClaim", Name: "skygo-" + testID + "-a-gpu", OwnerUID: "pod-a", SpecSHA256: spec, Phase: objectObserved, UID: "claim-a"},
		{Kind: "ResourceClaim", Name: "skygo-" + testID + "-b-gpu", OwnerUID: "pod-b", SpecSHA256: spec, Phase: objectIntent},
	}
	many := make([]receiptObject, maxReceiptObjects+1)
	for i := range many {
		many[i] = receiptObject{Kind: "ResourceClaimTemplate", Name: fmt.Sprintf("t%d", i), OwnerUID: "job-uid", SpecSHA256: spec, Phase: objectIntent}
	}
	// The parent UID of each receipt is "job-uid"; for a directly managed
	// Pod parent it is the Pod's own UID.
	podClaim := receiptObject{Kind: "ResourceClaim", Name: "skygo-" + testID + "-gpu", OwnerUID: "job-uid", SpecSHA256: spec, Phase: objectObserved, UID: "claim-p"}
	for _, tt := range []struct {
		name    string
		kind    string
		objects []receiptObject
		ok      bool
	}{
		{"valid", "Job", valid, true},
		{"claim-owned-by-job", "Job", []receiptObject{{Kind: "ResourceClaim", Name: "c", OwnerUID: "job-uid", SpecSHA256: spec, Phase: objectIntent}}, false},
		{"claim-owned-by-unrecorded-pod", "Job", []receiptObject{{Kind: "ResourceClaim", Name: "c", OwnerUID: "pod-x", SpecSHA256: spec, Phase: objectIntent}}, false},
		{"foreign-template", "Job", []receiptObject{{Kind: "ResourceClaimTemplate", Name: "t", OwnerUID: "other", SpecSHA256: spec, Phase: objectIntent}}, false},
		{"duplicate-key", "Job", []receiptObject{valid[0], valid[0]}, false},
		{"duplicate-uid", "Job", []receiptObject{valid[1], {Kind: "Pod", Name: "p", OwnerUID: "job-uid", SpecSHA256: spec, Phase: objectObserved, UID: "pod-a"}}, false},
		{"parent-uid-reused", "Job", []receiptObject{{Kind: "Pod", Name: "p", OwnerUID: "job-uid", SpecSHA256: spec, Phase: objectObserved, UID: "job-uid"}}, false},
		{"unsent-with-uid", "Job", []receiptObject{{Kind: "ResourceClaimTemplate", Name: "t", OwnerUID: "job-uid", SpecSHA256: spec, Phase: objectSent, UID: "u"}}, false},
		{"unknown-kind", "Job", []receiptObject{{Kind: "DeviceClass", Name: "gpu", OwnerUID: "job-uid", SpecSHA256: spec, Phase: objectIntent}}, false},
		{"too-many", "Job", many, false},
		{"pod-parent-claim", "Pod", []receiptObject{valid[0], podClaim}, true},
		{"pod-parent-claim-intent", "Pod", []receiptObject{{Kind: "ResourceClaim", Name: "c", OwnerUID: "job-uid", SpecSHA256: spec, Phase: objectIntent}}, true},
		{"pod-parent-claim-wrong-owner", "Pod", []receiptObject{{Kind: "ResourceClaim", Name: "c", OwnerUID: "pod-x", SpecSHA256: spec, Phase: objectIntent}}, false},
		{"pod-parent-claim-template-owner", "Pod", []receiptObject{valid[0], {Kind: "ResourceClaim", Name: "c", OwnerUID: "template-uid", SpecSHA256: spec, Phase: objectIntent}}, false},
		{"pod-parent-claim-no-owner", "Pod", []receiptObject{{Kind: "ResourceClaim", Name: "c", SpecSHA256: spec, Phase: objectIntent}}, false},
		{"pod-parent-pod-row", "Pod", valid[1:2], false},
		{"pod-parent-claim-of-pod-row", "Pod", valid[1:4], false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := testStore(t)
			r := testReceipt(testID)
			r.Kind = tt.kind
			pinTestReceipt(t, s, r)
			before, _ := os.ReadFile(receiptFile(s, testID))
			_, err := s.update(testID, func(x *executionReceipt) error { x.Objects = tt.objects; return nil })
			if (err == nil) != tt.ok {
				t.Fatalf("update: %v", err)
			}
			after, _ := os.ReadFile(receiptFile(s, testID))
			if !tt.ok && !bytes.Equal(before, after) {
				t.Fatal("refused inventory changed the receipt")
			}
			if tt.ok {
				r, err := s.load(testID)
				if err != nil || len(r.Objects) != len(tt.objects) {
					t.Fatalf("reload: %+v %v", r, err)
				}
			}
		})
	}
}
