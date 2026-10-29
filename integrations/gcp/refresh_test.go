//go:build unix

package gce

import (
	"context"
	"errors"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

// Every Refresh route sends only GET requests and changes nothing in
// Compute Engine. MODELED (see compute_test.go); NOT_RUN by the author.

// refreshGetOnly refreshes m with tok and checks that every request it
// sent was a GET and that the goal and restart step did not change.
func refreshGetOnly(t *testing.T, f *fakeCompute, m *Machine, tok oauth2.Token) (Status, error) {
	t.Helper()
	before := len(f.requests())
	goal, restart := m.rec.Desired, m.rec.Restart
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	st, err := m.Refresh(ctx, tok)
	for _, r := range f.requests()[before:] {
		if r.Method != "GET" {
			t.Errorf("Refresh sent %s %s", r.Method, r.Path)
		}
	}
	if m.rec.Desired != goal || m.rec.Restart != restart {
		t.Errorf("Refresh changed the goal to %s/%s", m.rec.Desired, m.rec.Restart)
	}
	return st, err
}

func TestRefreshGoals(t *testing.T) {
	tests := []struct {
		name      string
		goal      Goal
		wantState string
	}{
		{"running", Running, "RUNNING"},
		{"stopped", Stopped, "TERMINATED"},
		{"restarted", Restarted, "RUNNING"},
		{"absent", Absent, stateAbsent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			m, _ := newTestMachine(t, f)
			if st, err := drive(t, m, 50); err != nil || !st.Reached {
				t.Fatalf("start = %+v, %v", st, err)
			}
			if tt.goal != Running {
				if err := m.SetGoal(tt.goal); err != nil {
					t.Fatal(err)
				}
				if st, err := drive(t, m, 50); err != nil || !st.Reached {
					t.Fatalf("Drive(%s) = %+v, %v", tt.goal, st, err)
				}
			}
			mutations := f.mutations()
			st, err := refreshGetOnly(t, f, m, testTok())
			if err != nil || !st.Reached || st.InstanceState != tt.wantState {
				t.Fatalf("Refresh = %+v, %v", st, err)
			}
			if f.mutations() != mutations {
				t.Error("Refresh caused a mutation")
			}
		})
	}
}

// A record that never sent a create reports not-created with no request.
func TestRefreshUncreated(t *testing.T) {
	f := newFake(t)
	m, _ := newTestMachine(t, f)
	st, err := refreshGetOnly(t, f, m, testTok())
	if err != nil || st.Reached || st.Held != heldNotCreated {
		t.Fatalf("Refresh = %+v, %v", st, err)
	}
	if n := len(f.requests()); n != 0 {
		t.Errorf("%d requests", n)
	}
}

// A prepared, never sent operation is reported, not sent; a later Drive
// sends it exactly once.
func TestRefreshPrepared(t *testing.T) {
	f := newFake(t)
	m, dir := newTestMachine(t, f)
	crashAt(t, m, "prepared")
	m2 := reopen(t, f, dir)
	before := len(f.requests())
	st, err := refreshGetOnly(t, f, m2, testTok())
	if err != nil || st.Held != heldPrepared || st.PendingRequestID == "" {
		t.Fatalf("Refresh = %+v, %v", st, err)
	}
	if len(f.requests()) != before {
		t.Error("Refresh sent a request for a prepared operation")
	}
	if st, err := drive(t, m2, 50); err != nil || !st.Reached || f.count("POST", "/disks") != 1 {
		t.Fatalf("later Drive = %+v, %v; %d disk inserts", st, err, f.count("POST", "/disks"))
	}
}

// A sent or lost operation is only looked up. Missing evidence holds; once
// the operation is visible, Refresh records it and pins the disk id, and a
// later Drive goes on without resending.
func TestRefreshLostOperation(t *testing.T) {
	t.Run("sent", func(t *testing.T) {
		f := newFake(t)
		f.hideOps = true
		m, dir := newTestMachine(t, f)
		crashAt(t, m, "sent")
		m2 := reopen(t, f, dir)
		st, err := refreshGetOnly(t, f, m2, testTok())
		if err != nil || st.Held != heldUnresolved || f.mutations() != 0 {
			t.Fatalf("Refresh = %+v, %v; %d mutations", st, err, f.mutations())
		}
	})
	t.Run("lost then found", func(t *testing.T) {
		f := newFake(t)
		f.dropReply["POST disks"] = true
		f.hideOps = true
		m, _ := newTestMachine(t, f)
		if st, err := drive(t, m, 50); err != nil || st.Held != heldUnresolved {
			t.Fatalf("Drive = %+v, %v", st, err)
		}
		f.mu.Lock()
		f.hideOps = false
		f.mu.Unlock()
		st, err := refreshGetOnly(t, f, m, testTok())
		if err != nil || st.Held != heldNotCreated || st.DiskID == "" || st.PendingRequestID != "" {
			t.Fatalf("Refresh = %+v, %v", st, err)
		}
		if st, err := drive(t, m, 50); err != nil || !st.Reached {
			t.Fatalf("later Drive = %+v, %v", st, err)
		}
		if f.count("POST", "/disks") != 1 {
			t.Error("disk insert resent")
		}
	})
	t.Run("truncated", func(t *testing.T) {
		f := newFake(t)
		f.dropReply["POST disks"] = true
		f.pageSize = 1
		f.extraOps = fillerOps(maxListPages + 1)
		m, _ := newTestMachine(t, f)
		if _, err := drive(t, m, 50); err != nil {
			t.Fatal(err)
		}
		st, err := refreshGetOnly(t, f, m, testTok())
		if err != nil || st.Held != heldTruncated {
			t.Fatalf("Refresh = %+v, %v", st, err)
		}
	})
}

// An original left pending by Drive is waited for, never overlapped. If
// it is still held, Refresh holds without a request; once released,
// Refresh records it.
func TestRefreshPendingOriginal(t *testing.T) {
	f := newFake(t)
	f.holdReplies()
	f.holdMutations = true
	m, _ := newTestMachine(t, f)
	if st, _ := briefDrive(m); st.Held != heldPending {
		t.Fatalf("Drive = %+v", st)
	}
	before := len(f.requests())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	st, err := m.Refresh(ctx, testTok())
	if err != nil || st.Held != heldPending || len(f.requests()) != before {
		t.Fatalf("Refresh = %+v, %v", st, err)
	}
	f.release()
	st, err = refreshGetOnly(t, f, m, testTok())
	if err != nil || m.pending != nil || m.rec.Op != nil && m.rec.Op.State == opSent {
		t.Fatalf("Refresh after release = %+v, %v", st, err)
	}
	if f.count("POST", "/disks") != 1 {
		t.Error("original resent")
	}
}

// Refresh refuses before any request for unusable tokens, missing
// deadlines, a closed machine and a machine fenced by a failed close.
func TestRefreshRefusals(t *testing.T) {
	now := time.Now()
	t.Run("tokens", func(t *testing.T) {
		f := newFake(t)
		m, _ := newTestMachine(t, f)
		if _, err := drive(t, m, 50); err != nil {
			t.Fatal(err)
		}
		before := len(f.requests())
		for _, tok := range []oauth2.Token{
			{TokenType: "Bearer", Expiry: now.Add(time.Hour)},
			{AccessToken: "a", RefreshToken: "r", Expiry: now.Add(time.Hour)},
			{AccessToken: "a"},
		} {
			if _, err := refreshGetOnly(t, f, m, tok); err == nil {
				t.Errorf("Refresh accepted %+v", tok)
			}
		}
		st, err := refreshGetOnly(t, f, m, oauth2.Token{AccessToken: "a", Expiry: now.Add(-time.Hour)})
		if err != nil || st.Held != heldToken {
			t.Errorf("expired token: %+v, %v", st, err)
		}
		if _, err := m.Refresh(context.Background(), testTok()); err == nil {
			t.Error("Refresh accepted a context without deadline")
		}
		if n := len(f.requests()) - before; n != 0 {
			t.Errorf("%d requests", n)
		}
	})
	t.Run("closed", func(t *testing.T) {
		f := newFake(t)
		m, _ := newTestMachine(t, f)
		if err := m.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := refreshGetOnly(t, f, m, testTok()); !errors.Is(err, errClosed) || len(f.requests()) != 0 {
			t.Errorf("Refresh = %v", err)
		}
	})
	t.Run("failed close", func(t *testing.T) {
		f := newFake(t)
		b := &bodyFault{method: "GET", err: errBodyClose}
		m, _ := newTestMachineWith(t, f, f.faultClient(b))
		if _, err := drive(t, m, 50); err == nil {
			t.Fatal("Drive ignored the failed close")
		}
		before := len(f.requests())
		st, err := refreshGetOnly(t, f, m, testTok())
		if !errors.Is(err, errBodyClose) || len(f.requests()) != before || st.Reached {
			t.Errorf("Refresh = %+v, %v", st, err)
		}
		if m.held == nil {
			t.Error("Refresh cleared the kept original")
		}
	})
}

// Substituted ids, external removal and unpinned resources: Refresh
// reports, never deletes, and never infers an unpinned resource gone.
func TestRefreshIdentity(t *testing.T) {
	t.Run("substituted vm id", func(t *testing.T) {
		f := newFake(t)
		m, _ := newTestMachine(t, f)
		if _, err := drive(t, m, 50); err != nil {
			t.Fatal(err)
		}
		f.mu.Lock()
		for _, vm := range f.vms {
			vm.ID = "31337"
		}
		f.mu.Unlock()
		st, err := refreshGetOnly(t, f, m, testTok())
		if err != nil || st.Held != heldDrift || st.Reached {
			t.Fatalf("Refresh = %+v, %v", st, err)
		}
	})
	t.Run("pinned 404", func(t *testing.T) {
		f := newFake(t)
		m, _ := newTestMachine(t, f)
		if _, err := drive(t, m, 50); err != nil {
			t.Fatal(err)
		}
		f.mu.Lock()
		f.vms = map[string]*fakeVM{}
		for _, d := range f.disks {
			d.Users = nil
		}
		f.mu.Unlock()
		st, err := refreshGetOnly(t, f, m, testTok())
		if err != nil || st.Held != heldMissing || st.InstanceState != stateRemoved {
			t.Fatalf("Refresh = %+v, %v", st, err)
		}
	})
	t.Run("unpinned 404", func(t *testing.T) {
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
		st, err := refreshGetOnly(t, f, m, testTok())
		if err != nil || st.Reached || st.Held != heldUnknownState || st.DiskState == stateAbsent || st.DiskState == stateRemoved {
			t.Fatalf("Refresh = %+v, %v", st, err)
		}
	})
	t.Run("unpinned present", func(t *testing.T) {
		f := newFake(t)
		f.opError = "QUOTA_EXCEEDED"
		m, _ := newTestMachine(t, f)
		if _, err := drive(t, m, 50); err == nil {
			t.Fatal("failed insert not reported")
		}
		st, err := refreshGetOnly(t, f, m, testTok())
		if err != nil || st.Held != heldUnknownState {
			t.Fatalf("Refresh = %+v, %v", st, err)
		}
	})
}
