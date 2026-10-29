//go:build unix

package gce

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// These tests drive the real journal and configured HTTP client against
// the local Compute model. A guest shutdown changes only the modeled VM's
// power state, never its numeric identity or the original operation.
func guestShutdown(t *testing.T, f *fakeCompute, name string) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	vm := f.vms[name]
	if vm == nil {
		t.Fatal("no VM to shut down")
	}
	vm.Status = "TERMINATED"
}

func requireReached(t *testing.T, m *Machine) {
	t.Helper()
	if st, err := drive(t, m, 50); err != nil || !st.Reached {
		t.Fatalf("Drive = %+v, %v", st, err)
	}
}

// All prepared mutations except deletes require admission, including
// those recovered from an older journal. Absent can withdraw the unsent
// mutation and delete only the two resources already exactly owned.
func TestPreparedPowerRequiresAdmission(t *testing.T) {
	for _, kind := range []string{kindStart, kindStop} {
		t.Run(kind, func(t *testing.T) {
			f := newFake(t)
			m, dir := newTestMachine(t, f)
			requireReached(t, m)
			goal := Stopped
			if kind == kindStart {
				if err := m.SetGoal(Stopped); err != nil {
					t.Fatal(err)
				}
				requireReached(t, m)
				goal = Running
			}
			if err := m.SetGoal(goal); err != nil {
				t.Fatal(err)
			}
			if _, err := m.prepare(kind); err != nil {
				t.Fatal(err)
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			tamper(t, dir, func(v map[string]any) { v["admission"] = nil })
			m = reopen(t, f, dir)
			before := f.mutations()
			if st, err := drive(t, m, 50); err != nil || st.Held != heldUnadmitted {
				t.Fatalf("unadmitted Drive = %+v, %v", st, err)
			}
			if f.mutations() != before || m.rec.Op == nil || m.rec.Op.State != opPrepared {
				t.Fatal("the unadmitted power operation was sent or discarded")
			}
			if err := m.SetGoal(Absent); err != nil {
				t.Fatal(err)
			}
			requireReached(t, m)
			if got := f.mutations() - before; got != 2 {
				t.Fatalf("%d teardown mutations, want only VM and disk deletes", got)
			}
		})
	}
}

func TestGoalAfterGuestShutdown(t *testing.T) {
	for _, historicalStart := range []bool{false, true} {
		for _, goal := range []Goal{Running, Restarted} {
			for _, refresh := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/previous-start=%t/refreshed=%t", goal, historicalStart, refresh), func(t *testing.T) {
					f := newFake(t)
					m, dir := newTestMachine(t, f)
					requireReached(t, m)
					if historicalStart {
						for _, g := range []Goal{Stopped, Running} {
							if err := m.SetGoal(g); err != nil {
								t.Fatal(err)
							}
							requireReached(t, m)
						}
					}
					guestShutdown(t, f, m.rec.InstanceName)
					if refresh {
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						st, err := m.Refresh(ctx, testTok())
						cancel()
						if err != nil || st.Reached {
							t.Fatalf("Refresh = %+v, %v", st, err)
						}
					}
					if err := m.Close(); err != nil {
						t.Fatal(err)
					}
					m = reopen(t, f, dir)
					before := f.mutations()
					if err := m.SetGoal(goal); err != nil {
						t.Fatal(err)
					}
					requireReached(t, m)
					if got := f.mutations() - before; got != 1 {
						t.Fatalf("%d new mutations, want one fresh start", got)
					}
					if m.rec.DiskCreate == nil || m.rec.InstanceCreate == nil {
						t.Fatal("the create witnesses were lost")
					}
				})
			}
		}
	}
}

func TestRepeatedPowerGoals(t *testing.T) {
	f := newFake(t)
	m, dir := newTestMachine(t, f)
	requireReached(t, m)
	seen := make(map[string]bool)
	for _, g := range []Goal{Stopped, Running, Stopped, Running, Restarted, Restarted} {
		if err := m.Close(); err != nil {
			t.Fatal(err)
		}
		m = reopen(t, f, dir)
		if err := m.SetGoal(g); err != nil {
			t.Fatal(err)
		}
		before := len(m.rec.History)
		requireReached(t, m)
		want := 1
		if g == Restarted {
			want = 2
		}
		if len(m.rec.History)-before != want {
			t.Fatalf("goal %s added %d operations, want %d", g, len(m.rec.History)-before, want)
		}
		for _, op := range m.rec.History[before:] {
			if seen[op.RequestID] {
				t.Fatalf("request ID reused: %s", op.RequestID)
			}
			seen[op.RequestID] = true
		}
	}
}

func TestUnresolvedPowerNeverResent(t *testing.T) {
	f := newFake(t)
	m, dir := newTestMachine(t, f)
	requireReached(t, m)
	if err := m.SetGoal(Stopped); err != nil {
		t.Fatal(err)
	}
	requireReached(t, m)
	f.dropReply["POST start"], f.hideOps = true, true
	if err := m.SetGoal(Running); err != nil {
		t.Fatal(err)
	}
	if st, err := drive(t, m, 50); err != nil || st.Held != heldUnresolved {
		t.Fatalf("lost start = %+v, %v", st, err)
	}
	id := m.rec.Op.RequestID
	before := f.mutations()
	for _, goal := range []Goal{Running, Stopped, Restarted, Absent} {
		if err := m.Close(); err != nil {
			t.Fatal(err)
		}
		m = reopen(t, f, dir)
		if err := m.SetGoal(goal); err != nil {
			t.Fatal(err)
		}
		if st, err := drive(t, m, 50); err != nil || st.Held != heldUnresolved {
			t.Fatalf("goal %s with lost start = %+v, %v", goal, st, err)
		}
		if f.mutations() != before || m.rec.Op == nil || m.rec.Op.RequestID != id {
			t.Fatal("the unresolved original was replaced or resent")
		}
	}
}

// Intent fields are mandatory in version 2; older encodings may not
// smuggle in fields that their writer could not have produced.
func TestIntentRecordRefusals(t *testing.T) {
	legacy := func(v map[string]any) {
		v["version"] = float64(1)
		delete(v, "intent")
		delete(v, "completed_intent")
		delete(v["op"].(map[string]any), "intent")
		for _, op := range v["history"].([]any) {
			delete(op.(map[string]any), "intent")
		}
	}
	for _, tt := range []struct {
		name string
		edit func(map[string]any)
	}{
		{"legacy intent alias", func(v map[string]any) { legacy(v); v["Intent"] = float64(0) }},
		{"legacy completion alias", func(v map[string]any) { legacy(v); v["Completed_Intent"] = float64(0) }},
		{"legacy operation alias", func(v map[string]any) { legacy(v); v["op"].(map[string]any)["Intent"] = float64(0) }},
		{"missing intent", func(v map[string]any) { delete(v, "intent") }},
		{"missing completed", func(v map[string]any) { delete(v, "completed_intent") }},
		{"zero intent", func(v map[string]any) { v["intent"] = float64(0) }},
		{"future completion", func(v map[string]any) { v["completed_intent"] = v["intent"].(float64) + 1 }},
		{"missing operation intent", func(v map[string]any) { delete(v["op"].(map[string]any), "intent") }},
		{"zero operation intent", func(v map[string]any) { v["op"].(map[string]any)["intent"] = float64(0) }},
		{"future operation intent", func(v map[string]any) { v["op"].(map[string]any)["intent"] = v["intent"].(float64) + 1 }},
		{"missing historical intent", func(v map[string]any) { delete(v["history"].([]any)[0].(map[string]any), "intent") }},
		{"legacy has current fields", func(v map[string]any) { v["version"] = float64(1) }},
		{"legacy has current zero fields", func(v map[string]any) {
			v["version"], v["intent"], v["completed_intent"] = float64(1), float64(0), float64(0)
		}},
		{"legacy has operation intent", func(v map[string]any) {
			v["version"] = float64(1)
			delete(v, "intent")
			delete(v, "completed_intent")
			for _, op := range v["history"].([]any) {
				delete(op.(map[string]any), "intent")
			}
			v["op"].(map[string]any)["intent"] = float64(0)
		}},
		{"legacy has historical intent", func(v map[string]any) {
			v["version"] = float64(1)
			delete(v, "intent")
			delete(v, "completed_intent")
			delete(v["op"].(map[string]any), "intent")
			for _, op := range v["history"].([]any) {
				delete(op.(map[string]any), "intent")
			}
			v["history"].([]any)[0].(map[string]any)["intent"] = float64(0)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			m, dir := newTestMachine(t, f)
			requireReached(t, m)
			if err := m.SetGoal(Stopped); err != nil {
				t.Fatal(err)
			}
			if _, err := m.prepare(kindStop); err != nil {
				t.Fatal(err)
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
			tamper(t, dir, tt.edit)
			checkRefusedBeforeHTTP(t, f, dir)
		})
	}
}

func TestGoalIntentExhaustion(t *testing.T) {
	f := newFake(t)
	m, _ := newTestMachine(t, f)
	m.rec.Intent, m.rec.CompletedIntent = ^uint64(0), ^uint64(0)
	if err := m.save(); err != nil {
		t.Fatal(err)
	}
	if err := m.SetGoal(Stopped); err == nil {
		t.Fatal("wrapped intent was accepted")
	}
	if m.rec.Intent != ^uint64(0) || m.rec.Desired != Running || f.mutations() != 0 {
		t.Fatal("refused intent changed the goal or sent a request")
	}
}

// Drive can discover drift while the stored status still says Reached.
// Preparing new work must clear that old status before saving its op.
func TestDriveAfterInitialGuestShutdown(t *testing.T) {
	f := newFake(t)
	m, _ := newTestMachine(t, f)
	requireReached(t, m)
	guestShutdown(t, f, m.rec.InstanceName)
	if !m.rec.Reached {
		t.Fatal("the trigger requires a previously reached status")
	}
	before := f.mutations()
	requireReached(t, m) // no SetGoal or Refresh to clear Reached first
	if f.mutations() != before+1 || m.rec.Op != nil || m.fault != nil {
		t.Fatal("guest shutdown did not reconverge with one fresh start")
	}
}
