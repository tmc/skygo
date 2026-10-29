//go:build unix

package gce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

// All tests in this file are MODELED (see compute_test.go) and NOT_RUN by
// their author.

func TestCreateRunning(t *testing.T) {
	f := newFake(t)
	m, dir := newTestMachine(t, f)
	st, err := drive(t, m, 50)
	if err != nil || !st.Reached {
		t.Fatalf("Drive = %+v, %v", st, err)
	}
	if f.count("POST", "/disks") != 1 || f.count("POST", "/instances") != 1 {
		t.Fatal("want exactly one disk insert and one instance insert")
	}
	ids := map[string]bool{}
	var sawDisk bool
	for _, r := range f.requests() {
		if r.Auth != "Bearer "+testToken {
			t.Errorf("%s %s: Authorization = %q", r.Method, r.Path, r.Auth)
		}
		if r.Method == "GET" {
			continue
		}
		id := strings.TrimPrefix(r.Query, "requestId=")
		if ids[id] || !uuidRE.MatchString(id) {
			t.Errorf("request id %q reused or malformed", id)
		}
		ids[id] = true
		if strings.HasSuffix(r.Path, "/disks") {
			sawDisk = true
		}
		if strings.HasSuffix(r.Path, "/instances") {
			if !sawDisk {
				t.Error("instance insert before disk insert")
			}
			var b map[string]any
			json.Unmarshal(r.Body, &b)
			d := b["disks"].([]any)[0].(map[string]any)
			if d["autoDelete"] != false || d["boot"] != true || !strings.HasSuffix(d["source"].(string), "/disks/"+m.rec.DiskName) {
				t.Errorf("instance disk = %v", d)
			}
			if sa, ok := b["serviceAccounts"].([]any); !ok || len(sa) != 0 {
				t.Errorf("serviceAccounts = %v, want []", b["serviceAccounts"])
			}
			if _, ok := d["initializeParams"]; ok {
				t.Error("instance insert initializes a disk inline")
			}
			md := b["metadata"].(map[string]any)["items"].([]any)
			if len(md) != 3 {
				t.Errorf("metadata = %v", md)
			}
		}
	}
	data, _ := os.ReadFile(filepath.Join(dir, stateName))
	if bytes.Contains(data, []byte(testToken)) {
		t.Error("state holds the token")
	}
	if st.InstanceID == "" || st.DiskID == "" || st.ImageID != "4242" || st.InstanceState != "RUNNING" {
		t.Errorf("status = %+v", st)
	}
}

// Counterexample 2: the image name now names another image id. The disk
// reveals it and no VM is inserted; only the owned disk may be deleted.
func TestImageReplacedBeforeVM(t *testing.T) {
	f := newFake(t)
	f.diskImageID = "5555"
	m, _ := newTestMachine(t, f)
	st, err := drive(t, m, 50)
	if err != nil || st.Reached || st.Held != heldImageChanged {
		t.Fatalf("Drive = %+v, %v", st, err)
	}
	if n := f.count("POST", "/instances"); n != 0 {
		t.Fatalf("%d instance inserts after image change", n)
	}
	if err := m.SetGoal(Absent); err != nil {
		t.Fatal(err)
	}
	if st, err := drive(t, m, 50); err != nil || !st.Reached || st.DiskState != stateAbsent {
		t.Fatalf("down = %+v, %v", st, err)
	}
	if len(f.disks) != 0 || f.count("POST", "/instances") != 0 {
		t.Error("owned disk not deleted or VM created")
	}
}

// Counterexample 3: a same-name disk exists; it is never adopted.
func TestReservedNameTaken(t *testing.T) {
	f := newFake(t)
	f.foreignDisk = true
	m, _ := newTestMachine(t, f)
	st, err := drive(t, m, 50)
	if err != nil || st.Held != heldNameTaken {
		t.Fatalf("Drive = %+v, %v", st, err)
	}
	if n := f.mutations(); n != 0 {
		t.Errorf("%d mutations sent", n)
	}
}

func TestPreflightRefusals(t *testing.T) {
	tests := []struct {
		name  string
		fault func(*fakeCompute)
	}{
		{"os login", func(f *fakeCompute) { f.osLogin = true }},
		{"image id", func(f *fakeCompute) { f.imageID = "1" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			tt.fault(f)
			m, _ := newTestMachine(t, f)
			if _, err := drive(t, m, 50); err == nil {
				t.Fatal("Drive succeeded")
			}
			if n := f.mutations(); n != 0 {
				t.Errorf("%d mutations sent", n)
			}
		})
	}
}

// A lost insert reply is recovered from the operation list; nothing is
// resent.
func TestLostReplyRecovered(t *testing.T) {
	for _, key := range []string{"POST disks", "POST instances"} {
		t.Run(key, func(t *testing.T) {
			f := newFake(t)
			f.dropReply[key] = true
			m, _ := newTestMachine(t, f)
			st, err := drive(t, m, 50)
			if err != nil || !st.Reached {
				t.Fatalf("Drive = %+v, %v", st, err)
			}
			if f.count("POST", "/disks") != 1 || f.count("POST", "/instances") != 1 {
				t.Error("an insert was resent")
			}
		})
	}
}

// Counterexample 4: the reply is lost and no operation evidence is
// visible. The machine holds, across reopen, and never resends.
func TestLostReplyUnresolvedHolds(t *testing.T) {
	tests := []struct {
		name string
		hide func(*fakeCompute)
		want string
	}{
		{"missing", func(f *fakeCompute) { f.hideOps = true }, heldUnresolved},
		{"truncated", func(f *fakeCompute) { f.hideOps, f.truncate = true, true }, heldTruncated},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			f.dropReply["POST disks"] = true
			tt.hide(f)
			m, dir := newTestMachine(t, f)
			st, err := drive(t, m, 50)
			if err != nil || st.Held != tt.want || st.PendingRequestID == "" {
				t.Fatalf("Drive = %+v, %v", st, err)
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			m = reopen(t, f, dir)
			st2, err := drive(t, m, 50)
			if err != nil || st2.Held != tt.want || st2.PendingRequestID != st.PendingRequestID {
				t.Fatalf("reopened Drive = %+v, %v", st2, err)
			}
			if n := f.count("POST", "/disks"); n != 1 {
				t.Errorf("%d disk inserts, want 1", n)
			}
		})
	}
}

// fillerOps returns n operations of other requests.
func fillerOps(n int) []*operationWire {
	var ops []*operationWire
	for i := 0; i < n; i++ {
		ops = append(ops, &operationWire{ID: "1", Name: "other", ClientOperationID: "00000000-0000-4000-8000-000000000000"})
	}
	return ops
}

// A match on an early page does not resolve a lost reply when the bounded
// enumeration ends with another page: an unvisited page may hold a
// conflicting original.
func TestRecoveryMatchThenTruncated(t *testing.T) {
	f := newFake(t)
	f.dropReply["POST disks"] = true
	f.pageSize = 1
	f.extraOps = fillerOps(maxListPages + 1)
	m, _ := newTestMachine(t, f)
	st, err := drive(t, m, 50)
	if err != nil || st.Held != heldTruncated || st.PendingRequestID == "" || st.PendingOperation != "" {
		t.Fatalf("Drive = %+v, %v", st, err)
	}
	if m.rec.Op == nil || m.rec.Op.State != opLost {
		t.Errorf("operation = %+v, want lost", m.rec.Op)
	}
	if f.mutations() != 1 {
		t.Error("a mutation was resent")
	}
}

// A second operation carrying the same request id on a later page refuses
// recovery after a first match.
func TestRecoveryLaterConflict(t *testing.T) {
	f := newFake(t)
	f.dropReply["POST disks"] = true
	f.pageSize = 1
	f.replyPatch = func(op *operationWire) {
		dup := *op
		dup.ID, dup.Name = "8888", "operation-twin"
		f.extraOps = append(fillerOps(1), &dup)
	}
	m, _ := newTestMachine(t, f)
	st, err := drive(t, m, 50)
	if err == nil || !strings.Contains(err.Error(), "two operations") || st.Reached {
		t.Fatalf("Drive = %+v, %v", st, err)
	}
	if f.mutations() != 1 {
		t.Error("a mutation was resent")
	}
}

// An operation status outside PENDING, RUNNING and DONE is an error and
// nothing more is sent, now or after reopen.
func TestUnknownOperationStatus(t *testing.T) {
	f := newFake(t)
	f.opStatus = "SUSPENDED"
	m, dir := newTestMachine(t, f)
	st, err := drive(t, m, 50)
	if err == nil || !strings.Contains(err.Error(), "not pinned") || st.PendingOperation == "" {
		t.Fatalf("Drive = %+v, %v", st, err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m = reopen(t, f, dir)
	if _, err := drive(t, m, 50); err == nil {
		t.Fatal("reopened Drive accepted the unknown status")
	}
	if f.mutations() != 1 {
		t.Errorf("%d mutations, want 1", f.mutations())
	}
}

// exitProcess models the end of m's process at a crash cut: its original
// returns, joined without a timeout, and is dropped unrecorded, and the
// kernel releases its lock.
func exitProcess(t *testing.T, m *Machine) {
	t.Helper()
	if c := m.pending; c != nil {
		<-c.done
	}
	m.pending, m.closed, m.lockShut = nil, true, true
	if err := m.st.lock.Close(); err != nil {
		t.Fatal(err)
	}
}

// crashAt runs m until the crash cut at point and exits its process.
func crashAt(t *testing.T, m *Machine, point string) {
	t.Helper()
	m.cut = func(p string) error {
		if p == point {
			return errors.New("crash")
		}
		return nil
	}
	if _, err := drive(t, m, 50); err == nil {
		t.Fatal("Drive survived the crash cut")
	}
	exitProcess(t, m)
}

// Crash cuts abandon a machine at a commit point, as a killed process
// would, and reopen the same directory.
func TestCrashCuts(t *testing.T) {
	tests := []struct {
		point     string
		wantPosts int
		wantHeld  string
	}{
		{"prepared", 1, ""},         // never sent: sent once after reopen
		{"sent", 0, heldUnresolved}, // may have been sent: hold, never send
		{"receipt", 1, ""},          // sent, reply not committed: recovered
	}
	for _, tt := range tests {
		t.Run(tt.point, func(t *testing.T) {
			f := newFake(t)
			m, dir := newTestMachine(t, f)
			crashAt(t, m, tt.point)
			m2 := reopen(t, f, dir)
			st, err := drive(t, m2, 50)
			if err != nil {
				t.Fatal(err)
			}
			if st.Held != tt.wantHeld || st.Reached != (tt.wantHeld == "") {
				t.Errorf("status = %+v", st)
			}
			if n := f.count("POST", "/disks"); n != tt.wantPosts {
				t.Errorf("%d disk inserts, want %d", n, tt.wantPosts)
			}
		})
	}
}

// A prepared, never sent insert is withdrawn by Absent: its reserved
// resource was never requested, so down is reached without a request.
func TestPreparedWithdrawnByAbsent(t *testing.T) {
	f := newFake(t)
	m, dir := newTestMachine(t, f)
	crashAt(t, m, "prepared")
	m2 := reopen(t, f, dir)
	if err := m2.SetGoal(Absent); err != nil {
		t.Fatal(err)
	}
	before := len(f.requests())
	st, err := drive(t, m2, 50)
	if err != nil || !st.Reached || st.PendingRequestID != "" {
		t.Fatalf("Drive = %+v, %v", st, err)
	}
	if len(f.requests()) != before || m2.rec.DiskSent {
		t.Errorf("withdrawn insert left requests or a sent disk")
	}
}

// Counterexample 5: stop is DONE without error but the VM still runs.
func TestStopDoneStillRunning(t *testing.T) {
	f := newFake(t)
	m, _ := newTestMachine(t, f)
	if st, err := drive(t, m, 50); err != nil || !st.Reached {
		t.Fatalf("start = %+v, %v", st, err)
	}
	f.stopNoEffect = true
	if err := m.SetGoal(Stopped); err != nil {
		t.Fatal(err)
	}
	st, err := drive(t, m, 3)
	if err != nil || st.Reached || st.Held != heldSteps || st.InstanceState != "RUNNING" {
		t.Fatalf("stop = %+v, %v", st, err)
	}
	if n := f.count("POST", "/stop"); n != 1 {
		t.Errorf("%d stops, want 1", n)
	}
}

func TestRestartKeepsIdentity(t *testing.T) {
	f := newFake(t)
	m, _ := newTestMachine(t, f)
	st1, err := drive(t, m, 50)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.SetGoal(Restarted); err != nil {
		t.Fatal(err)
	}
	st2, err := drive(t, m, 50)
	if err != nil || !st2.Reached || st2.InstanceState != "RUNNING" {
		t.Fatalf("restart = %+v, %v", st2, err)
	}
	if st1.InstanceID != st2.InstanceID || st1.DiskID != st2.DiskID {
		t.Error("restart changed identities")
	}
	if f.count("POST", "/stop") != 1 || f.count("POST", "/start") != 1 {
		t.Error("restart is not one stop then one start")
	}
}

// Counterexample 9: down deletes the VM, verifies it gone, then deletes the
// disk and verifies it gone. Absent is latched across reopen.
func TestDownOrderAndLatch(t *testing.T) {
	f := newFake(t)
	m, dir := newTestMachine(t, f)
	if _, err := drive(t, m, 50); err != nil {
		t.Fatal(err)
	}
	if err := m.SetGoal(Absent); err != nil {
		t.Fatal(err)
	}
	st, err := drive(t, m, 50)
	if err != nil || !st.Reached || st.InstanceState != stateAbsent || st.DiskState != stateAbsent {
		t.Fatalf("down = %+v, %v", st, err)
	}
	var order []string
	for _, r := range f.requests() {
		if r.Method == "DELETE" {
			order = append(order, r.Path[strings.LastIndex(r.Path[:strings.LastIndex(r.Path, "/")], "/")+1:])
		}
	}
	if len(order) != 2 || !strings.HasPrefix(order[0], "instances/") || !strings.HasPrefix(order[1], "disks/") {
		t.Errorf("deletes = %v", order)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m = reopen(t, f, dir)
	if err := m.SetGoal(Running); err == nil {
		t.Error("goal running accepted after absent")
	}
}

// Owned resources whose pinned ids vanish without an owned delete are
// reported as removed externally, never as this machine's delete.
func TestExternalRemovalLabeled(t *testing.T) {
	f := newFake(t)
	m, _ := newTestMachine(t, f)
	if _, err := drive(t, m, 50); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.vms, f.disks = map[string]*fakeVM{}, map[string]*fakeDisk{}
	f.mu.Unlock()
	if err := m.SetGoal(Absent); err != nil {
		t.Fatal(err)
	}
	st, err := drive(t, m, 50)
	if err != nil || !st.Reached || st.InstanceState != stateRemoved || st.DiskState != stateRemoved {
		t.Fatalf("down = %+v, %v", st, err)
	}
	if n := f.count("DELETE", ""); n != 0 {
		t.Errorf("%d deletes sent", n)
	}
}

// A create that failed or whose numeric id was never pinned is never
// inferred gone from a 404, even under Absent.
func TestUnpinnedCreateHolds(t *testing.T) {
	t.Run("failed", func(t *testing.T) {
		f := newFake(t)
		f.opError = "QUOTA_EXCEEDED"
		m, _ := newTestMachine(t, f)
		if _, err := drive(t, m, 50); err == nil {
			t.Fatal("failed insert not reported")
		}
		f.mu.Lock()
		f.disks = map[string]*fakeDisk{}
		f.mu.Unlock()
		if err := m.SetGoal(Absent); err != nil {
			t.Fatal(err)
		}
		st, err := drive(t, m, 50)
		if err != nil || st.Reached || st.Held != heldUnknownState || st.DiskState == stateAbsent {
			t.Fatalf("down = %+v, %v", st, err)
		}
	})
	t.Run("lost", func(t *testing.T) {
		f := newFake(t)
		f.dropReply["POST disks"] = true
		f.hideOps = true
		m, _ := newTestMachine(t, f)
		if _, err := drive(t, m, 50); err != nil {
			t.Fatal(err)
		}
		if err := m.SetGoal(Absent); err != nil {
			t.Fatal(err)
		}
		st, err := drive(t, m, 50)
		if err != nil || st.Reached || st.Held != heldUnresolved {
			t.Fatalf("down = %+v, %v", st, err)
		}
		if n := f.count("DELETE", ""); n != 0 {
			t.Errorf("%d deletes sent", n)
		}
	})
}

// The disk's numeric id or users change after the VM delete. The disk is
// not deleted.
func TestDiskDriftBeforeDelete(t *testing.T) {
	tests := []struct {
		name  string
		drift func(*fakeDisk)
		want  string
	}{
		{"id", func(d *fakeDisk) { d.ID = "31337" }, heldDrift},
		{"users", func(d *fakeDisk) { d.Users = []string{"projects/skygo-test/zones/us-central1-a/instances/intruder"} }, heldDrift},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			m, _ := newTestMachine(t, f)
			if _, err := drive(t, m, 50); err != nil {
				t.Fatal(err)
			}
			f.replyPatch = func(op *operationWire) {
				if op.OperationType == "delete" && strings.Contains(op.TargetLink, "/instances/") {
					for _, d := range f.disks {
						tt.drift(d) // f.mu is held by route
					}
				}
			}
			if err := m.SetGoal(Absent); err != nil {
				t.Fatal(err)
			}
			st, err := drive(t, m, 50)
			if err != nil || st.Reached || st.Held != tt.want || st.InstanceState != stateAbsent {
				t.Fatalf("down = %+v, %v", st, err)
			}
			if n := f.count("DELETE", "/disks/"+m.rec.DiskName); n != 0 {
				t.Errorf("%d disk deletes after drift", n)
			}
		})
	}
}

func TestOperationDoneWithError(t *testing.T) {
	f := newFake(t)
	f.opError = "QUOTA_EXCEEDED"
	m, _ := newTestMachine(t, f)
	st, err := drive(t, m, 50)
	if err == nil || st.Reached || !strings.Contains(err.Error(), "QUOTA_EXCEEDED") {
		t.Fatalf("Drive = %+v, %v", st, err)
	}
	if st2, _ := drive(t, m, 50); st2.Held != heldCreateFailed || f.count("POST", "/disks") != 1 {
		t.Errorf("after failure = %+v; disk inserts %d", st2, f.count("POST", "/disks"))
	}
}

func TestOperationCorrelation(t *testing.T) {
	tests := []struct {
		name  string
		patch func(*operationWire)
	}{
		{"client id", func(o *operationWire) { o.ClientOperationID = "00000000-0000-4000-8000-000000000000" }},
		{"target", func(o *operationWire) { o.TargetLink += "x" }},
		{"type", func(o *operationWire) { o.OperationType = "delete" }},
		{"zone", func(o *operationWire) { o.Zone = strings.Replace(o.Zone, "us-central1-a", "us-central1-b", 1) }},
		{"op id", func(o *operationWire) { o.ID = "0" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			f.replyPatch = tt.patch
			m, _ := newTestMachine(t, f)
			if st, err := drive(t, m, 50); err == nil || st.Reached {
				t.Fatalf("Drive = %+v, %v", st, err)
			}
			if f.count("POST", "/disks") != 1 || f.count("POST", "/instances") != 0 {
				t.Error("uncorrelated operation led to another mutation")
			}
		})
	}
}

// Counterexample 1, illustrated and NOT prevented: another writer replaces
// the VM between this machine's GET and its name-only DELETE. The delete
// acts on the replacement; the operation target id reveals it afterward.
func TestNameOnlyReplacementOutsideModel(t *testing.T) {
	f := newFake(t)
	m, _ := newTestMachine(t, f)
	if _, err := drive(t, m, 50); err != nil {
		t.Fatal(err)
	}
	f.afterGetVM = func(f *fakeCompute) {
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, vm := range f.vms {
			vm.ID = "31337" // deleted and recreated under the same name
		}
		f.afterGetVM = nil
	}
	if err := m.SetGoal(Absent); err != nil {
		t.Fatal(err)
	}
	_, err := drive(t, m, 50)
	if f.count("DELETE", "/instances/"+m.rec.InstanceName) != 1 {
		t.Fatal("the illustrated name-only delete was not sent")
	}
	if err == nil || !strings.Contains(err.Error(), "target id") {
		t.Errorf("Drive error = %v, want target id mismatch detected after the effect", err)
	}
}

// Counterexample 6: the original's reply is held past the budget. The
// machine keeps the original and its lock; Close refuses until it settles.
// The fixture's cleanup, registered before the first check, releases and
// joins the original if a check fails.
func TestPendingCustody(t *testing.T) {
	f := newFake(t)
	f.holdReplies()
	f.holdMutations = true
	m, dir := newTestMachine(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	st, err := m.Drive(ctx, testTok(), DriveOptions{Steps: 50, Interval: time.Second})
	if err != nil || st.Held != heldPending {
		t.Fatalf("Drive = %+v, %v", st, err)
	}
	c := m.pending
	if c == nil || c.settled() {
		t.Fatal("no pending original")
	}
	if err := m.Close(); err == nil {
		t.Fatal("Close released a pending original")
	}
	m2, err := Open(dir, testConfig())
	f.root.own(t, f, m2, err)
	if err == nil {
		t.Fatal("lock released while original pending")
	}
	if _, err := m.Drive(ctx, testTok(), DriveOptions{Steps: 1, Interval: time.Second}); err == nil {
		t.Fatal("closed machine admitted Drive")
	}
	f.release()
	<-c.done
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
	m2 = reopen(t, f, dir)
	if m2.rec.Op == nil || m2.rec.Op.State != opRunning {
		t.Errorf("settled reply not recorded: %+v", m2.rec.Op)
	}
	if f.count("POST", "/disks") != 1 {
		t.Error("original resent")
	}
}

func TestTokenRefusals(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name string
		tok  oauth2.Token
		held bool
	}{
		{"empty", oauth2.Token{TokenType: "Bearer", Expiry: now.Add(time.Hour)}, false},
		{"refresh", oauth2.Token{AccessToken: "a", RefreshToken: "r", Expiry: now.Add(time.Hour)}, false},
		{"no expiry", oauth2.Token{AccessToken: "a"}, false},
		{"type", oauth2.Token{AccessToken: "a", TokenType: "MAC", Expiry: now.Add(time.Hour)}, false},
		{"expiring", oauth2.Token{AccessToken: "a", Expiry: now.Add(time.Minute)}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			m, _ := newTestMachine(t, f)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			st, err := m.Drive(ctx, tt.tok, DriveOptions{Steps: 5, Interval: time.Second})
			if tt.held != (err == nil && st.Held == heldToken) {
				t.Errorf("Drive = %+v, %v", st, err)
			}
			if n := len(f.requests()); n != 0 {
				t.Errorf("%d requests sent", n)
			}
		})
	}
}

func TestDriveRefusesBeforeEffect(t *testing.T) {
	f := newFake(t)
	m, _ := newTestMachine(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	bad := []DriveOptions{{0, time.Second}, {1001, time.Second}, {1, 0}, {1, 6 * time.Minute}}
	for _, o := range bad {
		if _, err := m.Drive(ctx, testTok(), o); err == nil {
			t.Errorf("Drive accepted %+v", o)
		}
	}
	if _, err := m.Drive(context.Background(), testTok(), DriveOptions{1, time.Second}); err == nil {
		t.Error("Drive accepted a context without deadline")
	}
	if n := len(f.requests()); n != 0 {
		t.Errorf("%d requests sent", n)
	}
}

func TestZeroMachine(t *testing.T) {
	var nilM *Machine
	if err := nilM.Close(); err != nil {
		t.Error(err)
	}
	m := new(Machine)
	if err := m.SetGoal(Running); err == nil {
		t.Error("zero SetGoal succeeded")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := m.Drive(ctx, testTok(), DriveOptions{1, time.Second}); err == nil {
		t.Error("zero Drive succeeded")
	}
	if _, err := m.Refresh(ctx, testTok()); err == nil {
		t.Error("zero Refresh succeeded")
	}
	if err := m.Close(); err != nil {
		t.Error(err)
	}
}

// TestAmbientCredentialTripwire runs a lifecycle with every ambient
// credential source armed: a metadata server that counts requests, an
// application default credentials file, a gcloud on PATH that leaves a
// mark, and HOME with a gcloud configuration. None may be consulted; every
// request carries only the explicit token. It also refuses imports that
// could discover credentials or run processes.
func TestAmbientCredentialTripwire(t *testing.T) {
	var metadata atomic.Int32
	md := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		metadata.Add(1)
		http.Error(w, "tripwire", http.StatusTeapot)
	}))
	t.Cleanup(md.Close)
	home := t.TempDir()
	bin := t.TempDir()
	mark := filepath.Join(home, "gcloud-ran")
	adc := filepath.Join(home, "adc.json")
	os.WriteFile(adc, []byte(`{"type":"authorized_user","refresh_token":"tripwire"}`), 0o600)
	os.MkdirAll(filepath.Join(home, ".config", "gcloud"), 0o700)
	os.WriteFile(filepath.Join(bin, "gcloud"), []byte("#!/bin/sh\ntouch "+mark+"\necho ya29.ambient\n"), 0o700)
	t.Setenv("GCE_METADATA_HOST", strings.TrimPrefix(md.URL, "http://"))
	t.Setenv("GCE_METADATA_IP", strings.TrimPrefix(md.URL, "http://"))
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", adc)
	t.Setenv("CLOUDSDK_CONFIG", filepath.Join(home, ".config", "gcloud"))
	t.Setenv("HOME", home)
	t.Setenv("PATH", bin)

	f := newFake(t)
	m, _ := newTestMachine(t, f)
	if st, err := drive(t, m, 50); err != nil || !st.Reached {
		t.Fatalf("Drive = %+v, %v", st, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if st, err := m.Refresh(ctx, testTok()); err != nil || !st.Reached {
		t.Fatalf("Refresh = %+v, %v", st, err)
	}
	if n := metadata.Load(); n != 0 {
		t.Errorf("metadata server saw %d requests", n)
	}
	if _, err := os.Stat(mark); err == nil {
		t.Error("gcloud was run")
	}
	for _, r := range f.requests() {
		if r.Auth != "Bearer "+testToken {
			t.Errorf("%s %s used credential %q", r.Method, r.Path, r.Auth)
		}
	}

	files, _ := filepath.Glob("*.go")
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, bad := range []string{`"golang.org/x/oauth2/google"`, `"cloud.google.com/go`, `"os/exec"`, `TokenSource`, `"google.golang.org/api`} {
			if bytes.Contains(src, []byte(bad)) {
				t.Errorf("%s uses %s", name, bad)
			}
		}
	}
}
