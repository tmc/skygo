//go:build unix

package gce

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// The local Compute model can return a successful start without changing
// the VM's observed power state. The real journal and configured HTTPS
// client must keep that unfinished work distinct from a completed goal.
func TestDirectDriftKeepsUnfinishedIntent(t *testing.T) {
	for _, priorStart := range []bool{false, true} {
		for _, goal := range []Goal{Running, Restarted} {
			for _, stage := range []string{"prepared", "pending", "done-unobserved"} {
				t.Run(fmt.Sprintf("%s/prior-start=%t/%s", goal, priorStart, stage), func(t *testing.T) {
					f := newFake(t)
					m, dir := newTestMachine(t, f)
					requireReached(t, m)
					if priorStart {
						for _, g := range []Goal{Stopped, Running} {
							if err := m.SetGoal(g); err != nil {
								t.Fatal(err)
							}
							requireReached(t, m)
						}
					}
					if goal == Restarted {
						if err := m.SetGoal(Restarted); err != nil {
							t.Fatal(err)
						}
						requireReached(t, m)
					}
					completed := m.rec.Intent
					guestShutdown(t, f, m.rec.InstanceName)
					before := f.count("POST", "/start")
					f.mu.Lock()
					f.startNoEffect = true
					f.mu.Unlock()

					if stage == "pending" {
						f.holdReplies()
						started := make(chan struct{}, 1)
						f.mu.Lock()
						f.holdMutations, f.mutationStarted = true, started
						f.mu.Unlock()
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						done := make(chan struct{})
						var st Status
						var err error
						// Before any assertion, own release and the actual Drive
						// join. The Machine fixture separately joins its call.
						t.Cleanup(func() { cancel(); f.release(); <-done })
						go func() {
							st, err = m.Drive(ctx, testTok(), DriveOptions{Steps: 10, Interval: time.Second})
							close(done)
						}()
						select {
						case <-started:
							cancel() // actual start entered; its reply remains held
						case <-done:
							t.Fatalf("Drive ended before its start entered: %+v, %v", st, err)
						case <-ctx.Done():
							t.Fatal("start did not enter before the request deadline")
						}
						<-done
						if err != nil || st.Held != heldPending || m.pending == nil || m.rec.Op == nil {
							t.Fatalf("pending direct Drive = %+v, %v", st, err)
						}
					} else {
						steps := 1 // prepare, without sending
						if stage == "done-unobserved" {
							steps = 3 // prepare, send, observe operation DONE
						}
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						st, err := m.Drive(ctx, testTok(), DriveOptions{Steps: steps, Interval: time.Second})
						cancel()
						if err != nil || st.Reached || st.Held != heldSteps {
							t.Fatalf("bounded direct Drive = %+v, %v", st, err)
						}
					}

					intent := m.rec.Intent
					if intent != completed+1 || m.rec.CompletedIntent != completed || m.rec.Reached {
						t.Fatalf("new work: intent %d completed %d reached %t", intent, m.rec.CompletedIntent, m.rec.Reached)
					}
					var original operation
					if m.rec.Op != nil {
						original = *m.rec.Op
					} else {
						original = m.rec.History[len(m.rec.History)-1]
					}
					call := m.pending
					if original.Kind != kindStart || original.Intent != intent {
						t.Fatalf("new original = %+v", original)
					}
					if err := m.SetGoal(goal); err != nil {
						t.Fatal(err)
					}
					if m.rec.Intent != intent || m.pending != call || m.rec.Op != nil && m.rec.Op.RequestID != original.RequestID {
						t.Fatal("same goal replaced an unfinished intent or original")
					}
					if goal == Restarted && m.rec.Restart != "done" {
						t.Fatal("same unfinished recovery started another restart cycle")
					}

					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					tok := testTok()
					if stage == "pending" {
						f.release()
						<-call.done
						if err := m.drain(ctx); err != nil {
							t.Fatal(err)
						}
					}
					// Continue the same prepared/running original to DONE;
					// do not run a new planning pass before the next checks.
					for i := 0; m.rec.Op != nil && i < 3; i++ {
						if _, err := m.step(ctx, &tok); err != nil {
							t.Fatal(err)
						}
					}
					if m.rec.Op != nil || m.pending != nil {
						t.Fatal("the original did not finish")
					}
					last := m.rec.History[len(m.rec.History)-1]
					if last.RequestID != original.RequestID || last.Intent != original.Intent || last.State != opDone {
						t.Fatal("the original identity changed on completion")
					}
					if err := m.Close(); err != nil {
						t.Fatal(err)
					}
					m = reopen(t, f, dir)
					if err := m.SetGoal(goal); err != nil {
						t.Fatal(err)
					}
					if m.rec.Intent != intent || m.rec.CompletedIntent != completed {
						t.Fatal("reopen or same goal completed unfinished work")
					}
					out, err := m.step(ctx, &tok) // real fresh VM/disk GETs
					if err != nil || out != stepWait || f.count("POST", "/start") != before+1 {
						t.Fatalf("unobserved DONE = %v, %v; %d starts", out, err, f.count("POST", "/start")-before)
					}
					f.mu.Lock()
					f.vms[m.rec.InstanceName].Status = "RUNNING"
					f.mu.Unlock()
					requireReached(t, m)
					if m.rec.CompletedIntent != intent || m.rec.Intent != intent || f.count("POST", "/start") != before+1 {
						t.Fatal("reaching the observed state sent another start")
					}
				})
			}
		}
	}
}

// Read-only observations and states that cannot yet admit a power
// mutation preserve the completed-intent witness without minting work.
func TestObservationDoesNotBeginIntent(t *testing.T) {
	for _, state := range []string{"refresh", "PROVISIONING", "STAGING", "STOPPING", "PAUSED"} {
		t.Run(state, func(t *testing.T) {
			f := newFake(t)
			m, _ := newTestMachine(t, f)
			requireReached(t, m)
			intent, completed, history := m.rec.Intent, m.rec.CompletedIntent, len(m.rec.History)
			before := f.mutations()
			power := state
			if state == "refresh" {
				power = "TERMINATED"
			}
			f.mu.Lock()
			f.vms[m.rec.InstanceName].Status = power
			f.mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			tok := testTok()
			if state == "refresh" {
				if st, err := m.Refresh(ctx, tok); err != nil || st.Reached {
					t.Fatalf("Refresh = %+v, %v", st, err)
				}
			} else {
				out, err := m.step(ctx, &tok)
				if state == "PAUSED" {
					var h holdError
					if !errors.As(err, &h) || h != holdError(heldUnknownState) {
						t.Fatalf("unknown power = %v, %v", out, err)
					}
				} else if err != nil || out != stepWait {
					t.Fatalf("transitional power = %v, %v", out, err)
				}
			}
			if m.rec.Intent != intent || m.rec.CompletedIntent != completed || m.rec.Op != nil || len(m.rec.History) != history || f.mutations() != before {
				t.Fatal("observation created an intent or power operation")
			}
		})
	}
}

func TestDirectDriftIntentOverflow(t *testing.T) {
	f := newFake(t)
	m, _ := newTestMachine(t, f)
	requireReached(t, m)
	m.rec.Intent, m.rec.CompletedIntent = ^uint64(0), ^uint64(0)
	if err := m.save(); err != nil {
		t.Fatal(err)
	}
	guestShutdown(t, f, m.rec.InstanceName)
	before := f.mutations()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tok := testTok()
	if _, err := m.step(ctx, &tok); err == nil {
		t.Fatal("wrapped drift intent was accepted")
	}
	if m.rec.Intent != ^uint64(0) || m.rec.CompletedIntent != ^uint64(0) || m.rec.Desired != Running || m.rec.Op != nil || f.mutations() != before {
		t.Fatal("overflow changed intent, goal or original, or sent a mutation")
	}
}

// The behavioral twin of the counter checks: an operation may be DONE
// before its expected power state is visible. Repeating the same goal
// must not cause a second start on the wire during that interval.
func TestDirectSameGoalNoSecondStart(t *testing.T) {
	f := newFake(t)
	m, _ := newTestMachine(t, f)
	requireReached(t, m)
	guestShutdown(t, f, m.rec.InstanceName)
	before := f.count("POST", "/start")
	f.mu.Lock()
	f.startNoEffect = true
	f.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := m.Drive(ctx, testTok(), DriveOptions{Steps: 3, Interval: time.Second})
	if err != nil || st.Reached || m.rec.Op != nil || f.count("POST", "/start") != before+1 {
		t.Fatalf("first direct start = %+v, %v", st, err)
	}
	if err := m.SetGoal(Running); err != nil {
		t.Fatal(err)
	}
	tok := testTok()
	for i := 0; i < 2; i++ {
		out, err := m.step(ctx, &tok)
		if err != nil {
			t.Fatal(err)
		}
		if out == stepWait {
			break
		}
	}
	if m.rec.Op != nil || f.count("POST", "/start") != before+1 {
		t.Fatalf("same goal sent %d starts before the expected power state was observed", f.count("POST", "/start")-before)
	}
}
