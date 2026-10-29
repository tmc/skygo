//go:build unix

package gce

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Successful-create provenance (durable create witnesses, independent of
// the bounded history) and strict image binding of qualified
// observations. MODELED (see compute_test.go); NOT_RUN by the author.

// readRecord decodes the committed record in dir.
func readRecord(t *testing.T, dir string) *record {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, stateName))
	if err != nil {
		t.Fatal(err)
	}
	r := new(record)
	if err := json.Unmarshal(data, r); err != nil {
		t.Fatal(err)
	}
	return r
}

// startedMachine drives a new machine for f to Running and closes it, so
// that its directory can be edited and reopened.
func startedMachine(t *testing.T, f *fakeCompute) string {
	t.Helper()
	m, dir := newTestMachine(t, f)
	if st, err := drive(t, m, 50); err != nil || !st.Reached {
		t.Fatalf("start = %+v, %v", st, err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

// checkRefusedBeforeHTTP checks that Open and ReadStatus refuse dir and
// that no request was sent for it.
func checkRefusedBeforeHTTP(t *testing.T, f *fakeCompute, dir string) {
	t.Helper()
	before := len(f.requests())
	m, err := open(dir, testConfig(), f.client())
	f.root.own(t, f, m, err)
	if err == nil {
		t.Error("Open accepted the state")
	}
	if _, err := ReadStatus(dir); err == nil {
		t.Error("ReadStatus accepted the state")
	}
	if n := len(f.requests()) - before; n != 0 {
		t.Errorf("%d requests", n)
	}
}

// A genuine failed create, then only its canonical numeric pin injected
// into the stored record: Sent alone does not justify a pin. Both are
// refused before any request, so neither Refresh nor an Absent Drive can
// report the never-created resource gone.
func TestFabricatedPinRefused(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*fakeCompute)
		field string
		phase string
	}{
		{"disk", func(f *fakeCompute) { f.opError = "QUOTA_EXCEEDED" }, "disk_id", phaseImagePinned},
		{"instance", func(f *fakeCompute) {
			f.replyPatch = func(op *operationWire) {
				if op.OperationType == "insert" && strings.Contains(op.TargetLink, "/instances/") {
					op.Error = &operationError{Errors: []struct {
						Code string `json:"code"`
					}{{Code: "QUOTA_EXCEEDED"}}}
				}
			}
		}, "instance_id", phaseDiskVerified},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			tt.setup(f)
			m, dir := newTestMachine(t, f)
			if _, err := drive(t, m, 50); err == nil {
				t.Fatal("failed create not reported")
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			r := readRecord(t, dir)
			last := r.History[len(r.History)-1]
			if r.Phase != tt.phase || last.State != opFailed {
				t.Fatalf("not a settled failed create: phase %s, %+v", r.Phase, last)
			}
			m2 := reopen(t, f, dir) // the genuine record is accepted
			if err := m2.Close(); err != nil {
				t.Fatal(err)
			}
			tamper(t, dir, func(v map[string]any) { v[tt.field] = "9001" })
			checkRefusedBeforeHTTP(t, f, dir)
		})
	}
}

// Every field of a create witness binds; a missing witness is refused,
// and an old record without witnesses is never given one.
func TestCreateWitnessTampering(t *testing.T) {
	w := func(field string, val any) func(map[string]any) {
		return func(v map[string]any) { v["disk_create"].(map[string]any)[field] = val }
	}
	tests := []struct {
		name string
		edit func(map[string]any)
	}{
		{"missing disk witness", func(v map[string]any) { v["disk_create"] = nil }},
		{"missing instance witness", func(v map[string]any) { v["instance_create"] = nil }},
		{"old record", func(v map[string]any) { v["disk_create"], v["instance_create"], v["admission"] = nil, nil, nil }},
		{"request id", w("request_id", "00000000-0000-4000-8000-000000000000")},
		{"operation name", w("operation", "operation-99")},
		{"operation id", w("operation_id", "123")},
		{"kind", w("kind", kindInstanceInsert)},
		{"target", w("target", "projects/skygo-test/zones/us-central1-a/disks/other")},
		{"target id", w("target_id", "31337")},
		{"image", w("image_id", "9999")},
		{"spec", w("request", strings.Repeat("0", 64))},
		{"evicted then target id", func(v map[string]any) {
			v["history"] = []any{}
			v["disk_create"].(map[string]any)["target_id"] = "31337"
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			dir := startedMachine(t, f)
			tamper(t, dir, tt.edit)
			checkRefusedBeforeHTTP(t, f, dir)
		})
	}
}

// The witness, not the bounded history, carries provenance: with the
// history evicted, a settled create's resources found gone are removed
// externally, and an owned delete is still this machine's.
func TestWitnessOutlivesHistory(t *testing.T) {
	t.Run("external removal", func(t *testing.T) {
		f := newFake(t)
		dir := startedMachine(t, f)
		tamper(t, dir, func(v map[string]any) { v["history"] = []any{} })
		m := reopen(t, f, dir)
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
	})
	t.Run("owned delete", func(t *testing.T) {
		f := newFake(t)
		dir := startedMachine(t, f)
		tamper(t, dir, func(v map[string]any) { v["history"] = []any{} })
		m := reopen(t, f, dir)
		if err := m.SetGoal(Absent); err != nil {
			t.Fatal(err)
		}
		st, err := drive(t, m, 50)
		if err != nil || !st.Reached || st.InstanceState != stateAbsent || st.DiskState != stateAbsent {
			t.Fatalf("down = %+v, %v", st, err)
		}
		if f.count("DELETE", "/instances/"+m.rec.InstanceName) != 1 || f.count("DELETE", "/disks/"+m.rec.DiskName) != 1 {
			t.Error("want one owned delete of each")
		}
	})
}

// Each created resource has its witness, bound to its settled create.
func TestCreateWitnessRecorded(t *testing.T) {
	f := newFake(t)
	dir := startedMachine(t, f)
	r := readRecord(t, dir)
	for _, c := range []struct {
		w    *createWitness
		kind string
		id   string
	}{{r.DiskCreate, kindDiskInsert, r.DiskID}, {r.InstanceCreate, kindInstanceInsert, r.InstanceID}} {
		if c.w == nil || c.w.Kind != c.kind || c.w.TargetID != c.id || c.w.ImageID != "4242" {
			t.Errorf("%s witness = %+v", c.kind, c.w)
		}
		n := 0
		for _, o := range r.History {
			if o.Kind == c.kind && o.State == opDone && o.RequestID == c.w.RequestID && o.Name == c.w.Operation {
				n++
			}
		}
		if n != 1 {
			t.Errorf("%s witness matches %d done creates, want 1", c.kind, n)
		}
	}
}

// A same-id, same-spec disk that reports another source image does not
// qualify: Refresh holds, GET only, with the goal unchanged, and Drive
// sends nothing.
func TestQualifiedImageBinding(t *testing.T) {
	f := newFake(t)
	m, _ := newTestMachine(t, f)
	if st, err := drive(t, m, 50); err != nil || !st.Reached {
		t.Fatalf("start = %+v, %v", st, err)
	}
	f.mu.Lock()
	for _, d := range f.disks {
		d.ImageID = "9999"
	}
	f.mu.Unlock()
	mutations := f.mutations()
	op, phase := m.rec.Op, m.rec.Phase
	st, err := refreshGetOnly(t, f, m, testTok())
	if err != nil || st.Reached || st.Held != heldImageChanged {
		t.Fatalf("Refresh = %+v, %v", st, err)
	}
	if m.rec.Op != op || m.rec.Phase != phase {
		t.Error("Refresh changed the operation or phase")
	}
	if st, err := drive(t, m, 50); err != nil || st.Reached || st.Held != heldImageChanged {
		t.Fatalf("Drive = %+v, %v", st, err)
	}
	if f.mutations() != mutations {
		t.Error("a mutation followed the image mismatch")
	}
}

// The exactly owned disk found holding another image before any VM may
// still be deleted; it is never qualified and no VM is inserted. Refresh
// of that state holds, GET only.
func TestWrongImageDiskCleanup(t *testing.T) {
	f := newFake(t)
	f.diskImageID = "5555"
	m, _ := newTestMachine(t, f)
	if st, err := drive(t, m, 50); err != nil || st.Held != heldImageChanged {
		t.Fatalf("Drive = %+v, %v", st, err)
	}
	if st, err := refreshGetOnly(t, f, m, testTok()); err != nil || st.Reached || st.Held != heldImageChanged {
		t.Fatalf("Refresh = %+v, %v", st, err)
	}
	if err := m.SetGoal(Absent); err != nil {
		t.Fatal(err)
	}
	st, err := drive(t, m, 50)
	if err != nil || !st.Reached || st.DiskState != stateAbsent {
		t.Fatalf("down = %+v, %v", st, err)
	}
	if f.count("POST", "/instances") != 0 || f.count("DELETE", "/disks/"+m.rec.DiskName) != 1 {
		t.Error("want no VM insert and one owned disk delete")
	}
}
