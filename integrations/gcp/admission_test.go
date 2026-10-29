//go:build unix

package gce

import (
	"errors"
	"strings"
	"testing"
)

// Strict ARM64 admission through the configured fixture: one images.get
// and one machineTypes.get, both settled and their witness committed
// before any mutation. MODELED (see compute_test.go); NOT_RUN by the
// author.

const typePath = "projects/skygo-test/zones/us-central1-a/machineTypes/t2a-standard-1"

// The admission reads precede every mutation, the machine type is read
// exactly once on its exact path with no body, and the witness is
// committed before the first mutation is even prepared to be sent.
func TestAdmissionBeforeCharge(t *testing.T) {
	f := newFake(t)
	m, dir := newTestMachine(t, f)
	crashAt(t, m, "prepared")
	r := readRecord(t, dir)
	a := r.Admission
	if a == nil || r.Op == nil || r.Op.Kind != kindDiskInsert || r.Op.State != opPrepared {
		t.Fatalf("admission %+v, op %+v; want the witness with the disk insert only prepared", a, r.Op)
	}
	want := admission{admissionVersion, r.ConfigHash, "4242", testConfig().Image.Name,
		"projects/debian-cloud/global/images/debian-12-arm64-v20261001", archARM64,
		testTypeID, "t2a-standard-1", "us-central1-a", typePath, archARM64}
	if *a != want {
		t.Errorf("admission = %+v, want %+v", *a, want)
	}
	if n := f.mutations(); n != 0 {
		t.Errorf("%d mutations before the prepared one was sent", n)
	}
	n := 0
	for _, q := range f.requests() {
		if strings.HasSuffix(q.Path, "/machineTypes/t2a-standard-1") {
			n++
			if q.Method != "GET" || q.Path != typePath || q.Query != "" || len(q.Body) != 0 || q.Auth != "Bearer "+testToken {
				t.Errorf("machine type request = %+v", q)
			}
		}
	}
	if n != 1 {
		t.Errorf("%d machine type reads, want 1", n)
	}
	m2 := reopen(t, f, dir)
	if st, err := drive(t, m2, 50); err != nil || !st.Reached {
		t.Fatalf("Drive = %+v, %v", st, err)
	}
	for _, q := range f.requests() {
		if q.Method == "GET" && strings.Contains(q.Path, "/machineTypes/") && q.Path != typePath {
			t.Errorf("other machine type read: %s", q.Path)
		}
	}
}

// Any architecture other than exactly ARM64, and any identity drift, on
// either reply refuses with nothing sent and nothing admitted. A name
// prefix such as "arm64" in the image name is never evidence.
func TestAdmissionRefusals(t *testing.T) {
	set := func(k string, v any) func(map[string]any) { return func(m map[string]any) { m[k] = v } }
	del := func(k string) func(map[string]any) { return func(m map[string]any) { delete(m, k) } }
	link := "https://www.googleapis.com/compute/v1/"
	tests := []struct {
		name         string
		image, mtype func(map[string]any)
	}{
		{"image arch missing", del("architecture"), nil},
		{"image arch unspecified", set("architecture", "ARCHITECTURE_UNSPECIFIED"), nil},
		{"image arch x86", set("architecture", "X86_64"), nil},
		{"image arch unknown", set("architecture", "arm64"), nil},
		{"image name", set("name", "debian-12-arm64-v20261002"), nil},
		{"image id", set("id", "4243"), nil},
		{"image link project", set("selfLink", link+"projects/other-project/global/images/debian-12-arm64-v20261001"), nil},
		{"image link host", set("selfLink", "https://compute.example.com/compute/v1/projects/debian-cloud/global/images/debian-12-arm64-v20261001"), nil},
		{"image not ready", set("status", "PENDING"), nil},
		{"type arch missing", nil, del("architecture")},
		{"type arch unspecified", nil, set("architecture", "ARCHITECTURE_UNSPECIFIED")},
		{"type arch x86", nil, set("architecture", "X86_64")},
		{"type arch unknown", nil, set("architecture", "AARCH64")},
		{"type name", nil, set("name", "t2a-standard-2")},
		{"type zone", nil, set("zone", "us-central1-b")},
		{"type zone as link", nil, set("zone", link+"projects/skygo-test/zones/us-central1-a")},
		{"type link project", nil, set("selfLink", link+"projects/other-project/zones/us-central1-a/machineTypes/t2a-standard-1")},
		{"type link zone", nil, set("selfLink", link+"projects/skygo-test/zones/us-central1-b/machineTypes/t2a-standard-1")},
		{"type link host", nil, set("selfLink", "https://compute.example.com/compute/v1/"+typePath)},
		{"type id missing", nil, del("id")},
		{"type id zero", nil, set("id", "0")},
		{"type id not decimal", nil, set("id", "0777")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			f.imagePatch, f.typePatch = tt.image, tt.mtype
			m, dir := newTestMachine(t, f)
			if _, err := drive(t, m, 50); err == nil {
				t.Fatal("Drive admitted")
			}
			if n := f.mutations(); n != 0 {
				t.Errorf("%d mutations", n)
			}
			if m.rec.Admission != nil || m.rec.Phase != phaseNew {
				t.Errorf("admitted in memory: %+v", m.rec.Admission)
			}
			if r := readRecord(t, dir); r.Admission != nil || r.Phase != phaseNew {
				t.Errorf("admission committed: %+v", r.Admission)
			}
		})
	}
}

// A held Read, a held first Close, a held and failed first Close, or a
// failed first Close of either admission reply, and an unconfirmed commit
// of the admission, each leave the machine fenced or pending with nothing
// charged. The same machine keeps the original, and the fixture joins it
// before the root is retired.
func TestAdmissionCustody(t *testing.T) {
	errClose := errors.New("injected admission close failure")
	for _, suffix := range []string{"/global/images/debian-12-arm64-v20261001", "/machineTypes/t2a-standard-1"} {
		edges := []struct {
			name  string
			fault func(f *fakeCompute) *bodyFault
			held  bool
		}{
			{"held read", func(f *fakeCompute) *bodyFault { return &bodyFault{readHold: f.holdReplies()} }, true},
			{"held close", func(f *fakeCompute) *bodyFault { return &bodyFault{hold: f.holdReplies()} }, true},
			{"held failed close", func(f *fakeCompute) *bodyFault { return &bodyFault{hold: f.holdReplies(), err: errClose} }, true},
			{"failed close", func(f *fakeCompute) *bodyFault { return &bodyFault{err: errClose} }, false},
		}
		for _, e := range edges {
			t.Run(suffix+"/"+e.name, func(t *testing.T) {
				f := newFake(t)
				b := e.fault(f)
				b.method, b.suffix = "GET", suffix
				m, _ := newTestMachineWith(t, f, f.faultClient(b))
				st, err := briefDrive(m)
				if e.held {
					if err != nil || st.Held != heldPending || m.pending == nil {
						t.Fatalf("Drive = %+v, %v; want the admission read pending", st, err)
					}
					c := m.pending
					f.release()
					<-c.done
					if err := m.Close(); (err != nil) != (b.err != nil) {
						t.Errorf("Close after settle = %v", err)
					}
				} else if !errors.Is(err, errClose) || m.held == nil {
					t.Fatalf("Drive = %v; want the close failure kept", err)
				}
				if n := f.mutations(); n != 0 {
					t.Errorf("%d mutations", n)
				}
				if m.rec.Admission != nil {
					t.Error("admitted from an unsettled or unclosed reply")
				}
				if n := b.closes.Load(); n > 1 {
					t.Errorf("body closed %d times", n)
				}
			})
		}
	}
	t.Run("unconfirmed commit", func(t *testing.T) {
		on := false
		errSync := errors.New("injected admission sync failure")
		injectSync(t, &on, errSync)
		f := newFake(t)
		m, dir := newTestMachine(t, f)
		on = true // the admission's commit is the first in Drive
		if _, err := drive(t, m, 50); !errors.Is(err, errSync) {
			t.Fatalf("Drive = %v; want the commit failure", err)
		}
		if n := f.mutations(); n != 0 || !m.unsaved {
			t.Fatalf("%d mutations, unsaved %v", n, m.unsaved)
		}
		on = false
		if err := m.Close(); err != nil {
			t.Fatal(err)
		}
		m2 := reopen(t, f, dir)
		if m2.rec.Admission == nil {
			t.Fatal("the possibly committed admission was not kept")
		}
		if st, err := drive(t, m2, 50); err != nil || !st.Reached {
			t.Fatalf("Drive after reopen = %+v, %v", st, err)
		}
	})
}

// A record without its admission witness, such as one written before
// admission existed, is never admitted by Open, Drive or Refresh: it
// sends no create, start or stop and reports no goal but Absent reached.
// What it exactly owns can still be torn down under the identity guards.
func TestUnadmittedRecord(t *testing.T) {
	t.Run("prepared create", func(t *testing.T) {
		f := newFake(t)
		m, dir := newTestMachine(t, f)
		crashAt(t, m, "prepared")
		tamper(t, dir, func(v map[string]any) { v["admission"] = nil })
		m2 := reopen(t, f, dir)
		if st, err := drive(t, m2, 50); err != nil || st.Held != heldUnadmitted {
			t.Fatalf("Drive = %+v, %v", st, err)
		}
		if n := f.mutations(); n != 0 {
			t.Fatalf("%d mutations", n)
		}
		if err := m2.SetGoal(Absent); err != nil {
			t.Fatal(err)
		}
		if st, err := drive(t, m2, 50); err != nil || !st.Reached || f.mutations() != 0 {
			t.Fatalf("down = %+v, %v; want the unsent create withdrawn", st, err)
		}
	})
	t.Run("created", func(t *testing.T) {
		f := newFake(t)
		dir := startedMachine(t, f)
		tamper(t, dir, func(v map[string]any) { v["admission"] = nil })
		m := reopen(t, f, dir)
		if err := m.SetGoal(Stopped); err != nil {
			t.Fatal(err)
		}
		if st, err := drive(t, m, 50); err != nil || st.Held != heldUnadmitted {
			t.Fatalf("Drive = %+v, %v", st, err)
		}
		if st, err := refreshGetOnly(t, f, m, testTok()); err != nil || st.Reached || st.Held != heldUnadmitted {
			t.Fatalf("Refresh = %+v, %v", st, err)
		}
		if n := f.mutations(); n != 2 {
			t.Fatalf("%d mutations, want only the two original creates", n)
		}
		if m.rec.Admission != nil {
			t.Fatal("admission invented")
		}
		if err := m.SetGoal(Absent); err != nil {
			t.Fatal(err)
		}
		st, err := drive(t, m, 50)
		if err != nil || !st.Reached || st.InstanceState != stateAbsent || st.DiskState != stateAbsent {
			t.Fatalf("down = %+v, %v", st, err)
		}
	})
}

// A stored admission must bind exactly this config and ARM64; any edit
// is refused before any request.
func TestAdmissionTampering(t *testing.T) {
	a := func(field string, val any) func(map[string]any) {
		return func(v map[string]any) { v["admission"].(map[string]any)[field] = val }
	}
	tests := []struct {
		name string
		edit func(map[string]any)
	}{
		{"version", a("version", 2)},
		{"config hash", a("config_hash", strings.Repeat("0", 64))},
		{"image id", a("image_id", "4243")},
		{"image link", a("image_link", "projects/other-project/global/images/debian-12-arm64-v20261001")},
		{"image arch", a("image_architecture", "X86_64")},
		{"type id", a("machine_type_id", "0")},
		{"type name", a("machine_type_name", "t2a-standard-2")},
		{"type zone", a("machine_type_zone", "us-central1-b")},
		{"type link", a("machine_type_link", "projects/skygo-test/zones/us-central1-b/machineTypes/t2a-standard-1")},
		{"type arch", a("machine_type_architecture", "ARCHITECTURE_UNSPECIFIED")},
		{"admitted while new", func(v map[string]any) {
			v["phase"], v["image_id"] = phaseNew, ""
			v["disk_sent"], v["instance_sent"], v["disk_id"], v["instance_id"] = false, false, "", ""
			v["disk_create"], v["instance_create"], v["history"] = nil, nil, []any{}
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

// Refresh never admits: it reads neither the image nor the machine type.
func TestRefreshNeverAdmits(t *testing.T) {
	f := newFake(t)
	m, _ := newTestMachine(t, f)
	if st, err := drive(t, m, 50); err != nil || !st.Reached {
		t.Fatalf("start = %+v, %v", st, err)
	}
	before := len(f.requests())
	if st, err := refreshGetOnly(t, f, m, testTok()); err != nil || !st.Reached {
		t.Fatalf("Refresh = %+v, %v", st, err)
	}
	for _, q := range f.requests()[before:] {
		if strings.Contains(q.Path, "/global/images/") || strings.Contains(q.Path, "/machineTypes/") {
			t.Errorf("Refresh read %s", q.Path)
		}
	}
}
