//go:build unix

package gce

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// Custody of originals: held body reads, held and failed first body
// Closes, and the cleanup that settles held fixtures when a test exits
// early. MODELED where a body's Close is faulted (see bodyFault). NOT_RUN
// by the author.

// briefDrive drives m with a two-second budget, short enough to leave a
// held original pending.
func briefDrive(m *Machine) (Status, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return m.Drive(ctx, testTok(), DriveOptions{Steps: 50, Interval: time.Second})
}

// checkFencedPending checks that m keeps an unsettled original and its
// lock and admits nothing, without settling it.
func checkFencedPending(t *testing.T, f *fakeCompute, m *Machine, dir string) {
	t.Helper()
	if m.pending == nil || m.pending.settled() {
		t.Fatal("no unsettled original")
	}
	before := len(f.requests())
	if err := m.Close(); !errors.Is(err, errPending) {
		t.Fatalf("Close = %v, want pending", err)
	}
	m2, err := Open(dir, testConfig())
	f.root.own(t, f, m2, err)
	if err == nil {
		t.Fatal("same-process Open succeeded while an original was pending")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := m.Refresh(ctx, testTok()); err == nil {
		t.Error("Refresh admitted after Close")
	}
	if len(f.requests()) != before {
		t.Error("a request was sent while an original was pending")
	}
}

// A read's response body is held mid-Read past the budget.
func TestHeldBodyRead(t *testing.T) {
	f := newFake(t)
	f.holdReplies()
	f.holdReadBody = true
	m, dir := newTestMachine(t, f)
	st, err := briefDrive(m)
	if err != nil || st.Held != heldPending {
		t.Fatalf("Drive = %+v, %v", st, err)
	}
	c := m.pending
	checkFencedPending(t, f, m, dir)
	f.release()
	<-c.done
	if c.err != nil || c.closeErr != nil || c.resp == nil {
		t.Fatalf("released read: err %v, close %v", c.err, c.closeErr)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close after settle = %v", err)
	}
	if n := len(f.requests()); n != 1 {
		t.Errorf("%d requests, want the one held read", n)
	}
}

// The first Close of a body is held past the budget, for a read and for a
// mutation. The body is closed exactly once, by its call.
func TestHeldFirstClose(t *testing.T) {
	for _, method := range []string{"GET", "POST"} {
		t.Run(method, func(t *testing.T) {
			f := newFake(t)
			b := &bodyFault{method: method, hold: f.holdReplies()}
			m, dir := newTestMachineWith(t, f, f.faultClient(b))
			st, err := briefDrive(m)
			if err != nil || st.Held != heldPending {
				t.Fatalf("Drive = %+v, %v", st, err)
			}
			c := m.pending
			checkFencedPending(t, f, m, dir)
			f.release()
			<-c.done
			if err := m.Close(); err != nil {
				t.Fatalf("Close after settle = %v", err)
			}
			if err := m.Close(); err != nil {
				t.Fatalf("second Close = %v", err)
			}
			if n := b.closes.Load(); n != 1 {
				t.Errorf("body closed %d times, want 1", n)
			}
			if method == "POST" {
				st, err := ReadStatus(dir)
				if err != nil || st.PendingOperation == "" {
					t.Errorf("mutation receipt not recorded: %+v, %v", st, err)
				}
			}
		})
	}
}

var errBodyClose = errors.New("injected body close failure")

// The first Close of a body fails, for a read and for a mutation. The
// machine keeps the call with its original error and its lock; every
// later call refuses and no body is closed again. A mutation's receipt is
// still recorded. The fixture's cleanup then cannot close the machine and
// keeps its state directory.
func TestFailedFirstClose(t *testing.T) {
	for _, method := range []string{"GET", "POST"} {
		t.Run(method, func(t *testing.T) {
			f := newFake(t)
			b := &bodyFault{method: method, err: errBodyClose}
			m, dir := newTestMachineWith(t, f, f.faultClient(b))
			_, err := drive(t, m, 50)
			if !errors.Is(err, errBodyClose) {
				t.Fatalf("Drive = %v, want the original close error", err)
			}
			if m.held == nil || m.held.closeErr != errBodyClose || m.held.resp == nil {
				t.Fatalf("machine does not keep the call: %+v", m.held)
			}
			sent := len(f.requests())
			for i := 0; i < 2; i++ {
				if err := m.Close(); !errors.Is(err, errBodyClose) {
					t.Fatalf("Close = %v, want the original close error", err)
				}
			}
			m2, err := Open(dir, testConfig())
			f.root.own(t, f, m2, err)
			if err == nil {
				t.Fatal("same-process Open succeeded after a failed close")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if _, err := m.Refresh(ctx, testTok()); err == nil {
				t.Error("Refresh admitted")
			}
			if err := m.SetGoal(Stopped); err == nil {
				t.Error("SetGoal admitted")
			}
			if len(f.requests()) != sent {
				t.Error("a request followed the failed close")
			}
			if n := b.closes.Load(); n != 1 {
				t.Errorf("body closed %d times, want 1", n)
			}
			if method == "POST" {
				st, err := ReadStatus(dir)
				if err != nil || st.PendingOperation == "" {
					t.Errorf("mutation receipt not recorded: %+v, %v", st, err)
				}
			}
		})
	}
}

// Each early exit of a test with held originals runs the fixture's
// cleanup: it releases the hold, joins the original without a timeout,
// closes the machine and only then removes the state directory. SkipNow
// leaves the subtest exactly as an early Fatal would, without failing it.
func TestEarlyExitCleanup(t *testing.T) {
	edges := []struct {
		name  string
		setup func(*fakeCompute) *computeClient
		drive bool
	}{
		{"before drive", func(f *fakeCompute) *computeClient { return f.client() }, false},
		{"held mutation", func(f *fakeCompute) *computeClient {
			f.holdReplies()
			f.holdMutations = true
			return f.client()
		}, true},
		{"held body read", func(f *fakeCompute) *computeClient {
			f.holdReplies()
			f.holdReadBody = true
			return f.client()
		}, true},
		{"held first close", func(f *fakeCompute) *computeClient {
			return f.faultClient(&bodyFault{method: "POST", hold: f.holdReplies()})
		}, true},
	}
	for _, e := range edges {
		var (
			m    *Machine
			c    *call
			root string
		)
		t.Run(e.name, func(t *testing.T) {
			f := newFake(t)
			m, _ = newTestMachineWith(t, f, e.setup(f))
			root = f.root.dir
			if e.drive {
				if st, _ := briefDrive(m); st.Held != heldPending {
					t.Fatalf("Drive = %+v, want a pending original", st)
				}
				c = m.pending
			}
			t.SkipNow() // as an early t.Fatal: only cleanups run
		})
		if m == nil {
			t.Fatalf("%s: no machine was made", e.name)
		}
		if c != nil && !c.settled() {
			t.Errorf("%s: original not joined", e.name)
		}
		if !m.lockShut || m.lockErr != nil {
			t.Errorf("%s: machine not closed: %v", e.name, m.lockErr)
		}
		if _, err := os.Stat(root); !os.IsNotExist(err) {
			t.Errorf("%s: state root %s not removed after a clean close", e.name, root)
		}
	}
}
