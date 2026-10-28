package environmentcontrol_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"

	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/environmentcontrol"
)

// The tests below pause a session admission at a Store read and start a
// Reconcile pass meanwhile. Admission holds the lifecycle pass there, so the
// pass waits and runs once admission returns. A Controller that let the pass
// run instead is driven through the race: the test waits for the pass to
// finish before resuming admission. No test waits for an interleaving that
// correct serialization forbids.

// pauseRead arms the next read through s. That read closes paused, then
// waits until resume is called.
func (s *storeFaults) pauseRead() (paused <-chan struct{}, resume func()) {
	p, r := make(chan struct{}), make(chan struct{})
	s.mu.Lock()
	defer s.mu.Unlock()
	s.paused, s.resume = p, r
	return p, sync.OnceFunc(func() { close(r) })
}

// durable returns environment id's committed Native and its count of
// recorded sessions.
func (f *fixture) durable(id string) (native string, sessions int, err error) {
	e, err := f.store.Environment(id)
	if err != nil {
		return "", 0, err
	}
	var l struct {
		Native   []byte            `json:"native"`
		Sessions []json.RawMessage `json:"sessions"`
	}
	if err := json.Unmarshal(e.Checkpoint.Data, &l); err != nil {
		return "", 0, err
	}
	return string(l.Native), len(l.Sessions), nil
}

// sessionRefs returns the Ref of every Exec and Forward call so far.
func (r *fakeRuntime) sessionRefs() []environmentcontrol.Ref {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append(slices.Clone(r.execRef), r.fwdRef...)
}

// calls returns the Native of every Ensure and every Delete call so far.
func (r *fakeRuntime) calls() (ensures, deletes []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ref := range r.ensures {
		ensures = append(ensures, string(ref.Native))
	}
	for _, ref := range r.deletes {
		deletes = append(deletes, string(ref.Native))
	}
	return ensures, deletes
}

type admissionCase struct {
	name    string
	forward bool // a forward session; an exec session otherwise
	absent  bool // Absent intent lands while admission is paused
}

var admissionCases = []admissionCase{
	{"exec", false, false},
	{"forward", true, false},
	{"exec_absent", false, true},
	{"forward_absent", true, true},
}

// session starts one session of tc's kind for alice. Its runtime call waits
// until release is called. The channel yields the session's error once it
// returns.
func (f *fixture) session(tc admissionCase, id string) (done <-chan error, release func()) {
	r := make(chan struct{})
	release = sync.OnceFunc(func() { close(r) })
	wait := func(environmentcontrol.Ref) { <-r }
	f.runtime.mu.Lock()
	f.runtime.onExec, f.runtime.onFwd = wait, wait
	f.runtime.mu.Unlock()
	errc := make(chan error, 1)
	go func() {
		var err error
		if tc.forward {
			_, err = f.c.Forward(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), f.actors["alice"], id)
		} else {
			_, err = f.c.Exec(context.Background(), f.actors["alice"], id, environmentcontrol.ExecRequest{Argv: []string{"true"}})
		}
		errc <- err
	}()
	return errc, release
}

// race runs a Reconcile pass against the admission paused at its read, and
// then lets both finish. A Controller whose admission does not hold the
// lifecycle pass fails the custody assertion what, and its pass finishes
// before admission resumes. race returns the pass's error and the session's.
func (f *fixture) race(t *testing.T, tc admissionCase, id, what string, paused <-chan struct{}, resume func(), admitted <-chan error, release func()) (passErr, sessionErr error) {
	t.Helper()
	<-paused
	busy := environmentcontrol.LifecycleBusy(f.c)
	if !busy {
		t.Error(what)
	}
	if tc.absent {
		// Absent intent stays latchable while admission waits.
		if _, err := f.c.Delete(f.actors["alice"], id); err != nil {
			t.Errorf("delete while admission waits: %v", err)
		}
	}
	passed := make(chan error, 1)
	go func() { passed <- f.c.Reconcile(context.Background()) }()
	if !busy {
		passErr = <-passed
	}
	resume()
	if busy {
		passErr = <-passed
	}
	release()
	return passErr, <-admitted
}

// TestAdmissionFlushSerializedWithPass pauses a session admission after it
// took a pending result pA and before it commits pA. Unserialized, a pass
// would commit pA itself, call the runtime again and commit its newer result
// pB; the paused flush would then commit pA over pB and admit a session on
// it.
func TestAdmissionFlushSerializedWithPass(t *testing.T) {
	for _, tc := range admissionCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			v := f.ready(t, "alice", testSpec("analysis"))
			old := "uid-" + v.ID
			const pA, pB = "uid-a-0004", "uid-b-0004"
			var mu sync.Mutex
			var recorded []int // sessions recorded at the call that returned pB
			result := func(r environmentcontrol.Ref, phase environment.Phase) (environmentcontrol.Observation, error) {
				switch string(r.Native) {
				case old:
					// pA is a late identity of a canceled call.
					return environmentcontrol.Observation{Native: []byte(pA)}, context.Canceled
				case pA:
					_, n, err := f.durable(r.Identity.ID)
					if err != nil {
						t.Error(err)
					}
					mu.Lock()
					recorded = append(recorded, n)
					mu.Unlock()
					return environmentcontrol.Observation{Phase: phase, Native: []byte(pB)}, nil
				}
				return environmentcontrol.Observation{Phase: phase}, nil
			}
			f.runtime.ensure = func(r environmentcontrol.Ref) (environmentcontrol.Observation, error) {
				return result(r, environment.Ready)
			}
			f.runtime.remove = func(r environmentcontrol.Ref) (environmentcontrol.Observation, error) {
				return result(r, environment.Deleting)
			}
			faults := f.faults(pA)
			faults.set(true, false, 0)
			if err := f.c.Reconcile(context.Background()); !errors.Is(err, context.Canceled) || !errors.Is(err, errFault) {
				t.Fatalf("first pass: %v, want the call's own error and the commit fault", err)
			}
			faults.set(false, false, 0)

			// Admission's flush reads the row to commit pA, and waits.
			paused, resume := faults.pauseRead()
			defer resume()
			admitted, release := f.session(tc, v.ID)
			defer release()
			passErr, err := f.race(t, tc, v.ID, "admission flushed a pending result outside the lifecycle pass", paused, resume, admitted, release)
			if passErr != nil {
				t.Errorf("pass: %v", passErr)
			}

			native, _, derr := f.durable(v.ID)
			if derr != nil {
				t.Fatal(derr)
			}
			if native != pB {
				t.Errorf("committed Native %q, want %q: an older result was committed over a newer one", native, pB)
			}
			refs := f.runtime.sessionRefs()
			ensures, deletes := f.runtime.calls()
			if tc.absent {
				if !errors.Is(err, environmentcontrol.ErrClosed) || len(refs) != 0 {
					t.Errorf("session after Absent: %v, %d runtime sessions", err, len(refs))
				}
				if !slices.Equal(ensures[1:], []string{old}) || !slices.Equal(deletes, []string{pA}) {
					t.Errorf("ensures %q, deletes %q, want one Ensure from %q and one Delete of pA", ensures[1:], deletes, old)
				}
				if g := f.get(t, "alice", v.ID, environment.Deleting); g.State.Desired != environment.Absent {
					t.Errorf("Absent intent lost: %+v", g.State)
				}
				return
			}
			if err != nil || len(refs) != 1 || string(refs[0].Native) != pA {
				t.Fatalf("session: %v, refs %+v, want one session on the committed pA", err, refs)
			}
			mu.Lock()
			defer mu.Unlock()
			if !slices.Equal(recorded, []int{1}) {
				t.Errorf("sessions recorded when the runtime returned pB: %v: a session was admitted on pA after pB was returned", recorded)
			}
			if !slices.Equal(ensures[1:], []string{old, pA}) || len(deletes) != 0 {
				t.Errorf("ensures %q, deletes %q, want one call for pA and one from it", ensures[1:], deletes)
			}
		})
	}
}

// TestAdmissionAfterEmptyFlushSerializedWithPass pauses a session admission
// after its flush found no pending result and before it commits the session.
// Unserialized, a pass would call the runtime, fail to commit its result pB
// and leave pB pending; admission would then commit a session on the older
// Native.
func TestAdmissionAfterEmptyFlushSerializedWithPass(t *testing.T) {
	for _, tc := range admissionCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			v := f.ready(t, "alice", testSpec("analysis"))
			old := "uid-" + v.ID
			const pB = "uid-b-0005"
			var mu sync.Mutex
			var recorded []int // sessions recorded at the call that returned pB
			result := func(r environmentcontrol.Ref, phase environment.Phase) (environmentcontrol.Observation, error) {
				if string(r.Native) != old {
					return environmentcontrol.Observation{Phase: phase}, nil
				}
				_, n, err := f.durable(r.Identity.ID)
				if err != nil {
					t.Error(err)
				}
				mu.Lock()
				recorded = append(recorded, n)
				mu.Unlock()
				return environmentcontrol.Observation{Phase: phase, Native: []byte(pB)}, nil
			}
			f.runtime.ensure = func(r environmentcontrol.Ref) (environmentcontrol.Observation, error) {
				return result(r, environment.Ready)
			}
			f.runtime.remove = func(r environmentcontrol.Ref) (environmentcontrol.Observation, error) {
				return result(r, environment.Deleting)
			}
			faults := f.faults(pB)
			faults.set(true, false, 0)

			// Nothing is pending; admission reads the row to commit its
			// session, and waits.
			paused, resume := faults.pauseRead()
			defer resume()
			admitted, release := f.session(tc, v.ID)
			defer release()
			passErr, err := f.race(t, tc, v.ID, "admission committed a session outside the lifecycle pass", paused, resume, admitted, release)
			if !errors.Is(passErr, errFault) {
				t.Errorf("pass: %v, want pB's commit refused", passErr)
			}

			refs := f.runtime.sessionRefs()
			if tc.absent {
				if !errors.Is(err, environmentcontrol.ErrClosed) || len(refs) != 0 {
					t.Errorf("session after Absent: %v, %d runtime sessions", err, len(refs))
				}
			} else {
				if err != nil || len(refs) != 1 || string(refs[0].Native) != old {
					t.Fatalf("session: %v, refs %+v, want one session on %q", err, refs, old)
				}
				mu.Lock()
				got := slices.Clone(recorded)
				mu.Unlock()
				if !slices.Equal(got, []int{1}) {
					t.Errorf("sessions recorded when the runtime returned pB: %v: a session was admitted while pB was uncommitted", got)
				}
			}

			// pB stays pending until its commit, and is never obtained
			// again.
			faults.set(false, false, 0)
			f.reconcile(t)
			native, _, derr := f.durable(v.ID)
			if derr != nil {
				t.Fatal(derr)
			}
			if native != pB {
				t.Errorf("committed Native %q, want the pending %q", native, pB)
			}
			ensures, deletes := f.runtime.calls()
			want := []string{old, pB}
			if tc.absent {
				ensures, deletes = deletes, ensures[1:]
			} else {
				ensures = ensures[1:]
			}
			if !slices.Equal(ensures, want) || len(deletes) != 0 {
				t.Errorf("runtime calls %q and %q, want %q: one call returned pB and the next carried it", ensures, deletes, want)
			}
		})
	}
}
