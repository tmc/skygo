package servingcontrol_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/serving"
	"github.com/tmc/skygo/internal/servingcontrol"
	"github.com/tmc/skygo/internal/store"
)

// Source-only; NOT_RUN. These tests use the real Store and file-backed auth
// engine of newFixture. A second Controller on the same Store and binding is
// a real concurrent writer; a replaced CommitService and a direct Store
// commit made inside Apply are MODELED faults and interleavings.

// secondController returns another Controller on f's Store, auth and
// binding, with its own model runtime, closed before the Store.
func secondController(t *testing.T, f *fixture) (*servingcontrol.Controller, *fakeRuntime) {
	t.Helper()
	r := &fakeRuntime{forwardStarted: make(chan struct{}, 1)}
	c, err := servingcontrol.New(servingcontrol.Config{
		Store: f.store, Auth: f.engine, Target: "kube", Binding: "kube/default", Runtime: r, Limits: testLimits,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := closeWithin(c, 10*time.Second); err != nil {
			t.Errorf("close second controller: %v", err)
		}
	})
	return c, r
}

// TestHeldResultNeverOverwritesAdvancedNative: the first Controller's
// create result is held after a failed commit; a second Controller on the
// same Store then advances the Native under the same operation. The held
// result is committed only from the Native its Apply was given, so it is
// never written over the newer one: reconcile and Close report
// ErrNativeAdvanced, with no runtime call, and the newer Native stays.
func TestHeldResultNeverOverwritesAdvancedNative(t *testing.T) {
	f := newFixture(t)
	f.held = true
	s := f.create(t, "alice", "req-1", "web")
	f.runtime.set(func(r *fakeRuntime) { r.applyNative = []serving.Native{"v1:first"} })
	fault := errors.New("modeled commit fault")
	servingcontrol.SetCommit(f.c, func(string, uint64, store.ServiceCommit) (store.Service, error) {
		return store.Service{}, fault
	})
	if err := f.reconcile(t); !errors.Is(err, fault) {
		t.Fatalf("reconcile = %v, want the commit fault", err)
	}
	servingcontrol.SetCommit(f.c, f.store.CommitService)

	// The second Controller sees the uncommitted record and advances it
	// under the same operation; its next Apply fails, so the operation
	// stays in flight.
	c2, r2 := secondController(t, f)
	r2.set(func(r *fakeRuntime) {
		r.applyNative = []serving.Native{"v1:second"}
		r.applyErr = errors.New("modeled target error")
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c2.Reconcile(ctx); err == nil {
		t.Fatal("second controller's failed apply returned nil")
	}
	advanced := f.read(t, s.ID)
	if advanced.Native != "v1:second" || advanced.Operation == nil || *advanced.Operation != *s.Operation {
		t.Fatalf("second controller left %q under %+v", advanced.Native, advanced.Operation)
	}

	applies, deletes := f.runtime.calls()
	for range 2 {
		if err := f.reconcile(t); !errors.Is(err, servingcontrol.ErrNativeAdvanced) {
			t.Fatalf("reconcile with the native advanced = %v, want ErrNativeAdvanced", err)
		}
	}
	if err := closeWithin(f.c, 10*time.Second); !errors.Is(err, servingcontrol.ErrNativeAdvanced) {
		t.Fatalf("Close with the native advanced = %v, want ErrNativeAdvanced", err)
	}
	got := f.read(t, s.ID)
	if got.Native != "v1:second" || got.Revision != advanced.Revision {
		t.Fatalf("held result written over the newer native: %q at revision %d", got.Native, got.Revision)
	}
	if a, d := f.runtime.calls(); a != applies || d != deletes {
		t.Fatalf("runtime called with a result held: %d applies, %d deletes", a-applies, d-deletes)
	}
}

// TestPersistNeverOverwritesAdvancedNative: another writer advances the
// Native while Apply runs (MODELED inside Apply). The result Apply returns
// is held and never committed over it.
func TestPersistNeverOverwritesAdvancedNative(t *testing.T) {
	f := newFixture(t)
	f.held = true
	s := f.create(t, "alice", "req-1", "web")
	f.runtime.set(func(r *fakeRuntime) {
		r.applyNative = []serving.Native{"v1:first"}
		r.onApply = func(ref serving.Ref) {
			cur, err := f.store.Service(s.ID)
			if err != nil {
				t.Error(err)
				return
			}
			cm := cur.Commit()
			cm.Native = "v1:other"
			if _, err := f.store.CommitService(s.ID, cur.Revision, cm); err != nil {
				t.Error(err)
			}
		}
	})
	if err := f.reconcile(t); !errors.Is(err, servingcontrol.ErrNativeAdvanced) {
		t.Fatalf("reconcile = %v, want ErrNativeAdvanced", err)
	}
	if got := f.read(t, s.ID); got.Native != "v1:other" || got.Operation == nil || *got.Operation != *s.Operation {
		t.Fatalf("record %q under %+v, want the advanced native under the original operation", got.Native, got.Operation)
	}
	applies, _ := f.runtime.calls()
	if err := f.reconcile(t); !errors.Is(err, servingcontrol.ErrNativeAdvanced) {
		t.Fatalf("second reconcile = %v, want ErrNativeAdvanced", err)
	}
	if a, _ := f.runtime.calls(); a != applies {
		t.Fatal("runtime called with a result held")
	}
}

// TestHeldResultAlreadyCommitted is the positive: a commit that was made
// but reported failed (MODELED) is found committed, the result is dropped,
// and the pass and Close proceed.
func TestHeldResultAlreadyCommitted(t *testing.T) {
	f := newFixture(t)
	s := f.create(t, "alice", "req-1", "web")
	f.runtime.set(func(r *fakeRuntime) {
		r.applyNative = []serving.Native{"v1:first"}
		r.observation = ready
	})
	fault := errors.New("modeled lost commit answer")
	servingcontrol.SetCommit(f.c, func(id string, rev uint64, next store.ServiceCommit) (store.Service, error) {
		if _, err := f.store.CommitService(id, rev, next); err != nil {
			return store.Service{}, err
		}
		return store.Service{}, fault
	})
	if err := f.reconcile(t); !errors.Is(err, fault) {
		t.Fatalf("reconcile = %v, want the lost answer", err)
	}
	servingcontrol.SetCommit(f.c, f.store.CommitService)
	if err := f.reconcile(t); err != nil {
		t.Fatalf("reconcile after the commit was made = %v", err)
	}
	got := f.read(t, s.ID)
	if got.Native != "v1:first" || got.Phase() != serving.Ready {
		t.Fatalf("native %q phase %v", got.Native, got.Phase())
	}
	if err := closeWithin(f.c, 10*time.Second); err != nil {
		t.Fatalf("Close = %v", err)
	}
}

// TestIdentityConflictHeld: a create the runtime refuses for good leaves
// the service visibly Held — an observation of the conflict committed, the
// operation, Native, history and charges kept — and it stays Held across
// passes and a new Controller, with no delete, no adoption and no forward.
func TestIdentityConflictHeld(t *testing.T) {
	f := newFixture(t)
	s := f.create(t, "alice", "req-1", "web")
	f.runtime.set(func(r *fakeRuntime) {
		r.applyNative = []serving.Native{"v1:refused"}
		r.applyErr = serving.ErrIdentityConflict
		r.observation = ready // never read: a conflict is not observed ready
	})
	if err := f.reconcile(t); !errors.Is(err, serving.ErrIdentityConflict) {
		t.Fatalf("reconcile = %v, want identity conflict", err)
	}
	held := f.read(t, s.ID)
	check := func(got store.Service) {
		t.Helper()
		if got.Phase() != serving.Held || got.Native != "v1:refused" ||
			got.Operation == nil || *got.Operation != *s.Operation ||
			got.Observation == nil || *got.Observation != (serving.Observation{Reason: serving.IdentityConflict}) ||
			got.Reserved == (store.ServiceReservation{}) {
			t.Fatalf("conflicted service: phase %v native %q op %+v observation %+v reserved %+v",
				got.Phase(), got.Native, got.Operation, got.Observation, got.Reserved)
		}
		unavailable := false
		for _, c := range got.Conditions {
			unavailable = unavailable || c.Code == store.ServiceTargetUnavailable
		}
		if !unavailable {
			t.Fatalf("conflicted service has no target-unavailable condition: %+v", got.Conditions)
		}
	}
	check(held)
	for range 3 {
		if err := f.reconcile(t); !errors.Is(err, serving.ErrIdentityConflict) {
			t.Fatalf("repeated reconcile = %v", err)
		}
	}
	again := f.read(t, s.ID)
	check(again)
	if again.Revision != held.Revision {
		t.Fatalf("repeated passes rewrote the record: revision %d -> %d", held.Revision, again.Revision)
	}
	if v, err := f.c.Get(f.actors["alice"], s.ID); err != nil || v.Phase() != serving.Held {
		t.Fatalf("Get = phase %v, %v; want held", v.Phase(), err)
	}
	if err := forwardStatus(f, "alice", s.ID); err == nil {
		t.Fatal("forward admitted to a held service")
	}
	if _, d := f.runtime.calls(); d != 0 {
		t.Fatalf("%d deletes of a conflicted service", d)
	}
	// A new Controller reconstructs the same Held state from the Store.
	c2, r2 := secondController(t, f)
	r2.set(func(r *fakeRuntime) { r.applyErr = serving.ErrIdentityConflict })
	if v, err := c2.Get(f.actors["alice"], s.ID); err != nil || v.Phase() != serving.Held {
		t.Fatalf("new controller Get = phase %v, %v; want held", v.Phase(), err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c2.Reconcile(ctx); !errors.Is(err, serving.ErrIdentityConflict) {
		t.Fatalf("new controller reconcile = %v", err)
	}
	check(f.read(t, s.ID))
	if _, d := r2.calls(); d != 0 {
		t.Fatal("new controller deleted a conflicted service")
	}
}

// waitDelegation waits, bounded, until the real Engine refresh gives d the
// verdict want (nil: authorized).
func waitDelegation(t *testing.T, f *fixture, d auth.Delegation, want error) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := f.engine.CheckDelegation(d)
		if want == nil && err == nil || want != nil && errors.Is(err, want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("delegation check = %v, want %v", err, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func hasCondition(s store.Service, code store.ServiceConditionCode) bool {
	for _, c := range s.Conditions {
		if c.Code == code {
			return true
		}
	}
	return false
}

// TestApplyRechecksAuthorityEachStage holds the first Apply of a pass, which
// returns a changed Native (the gate is MODELED inside the model runtime),
// while the real credential file changes and the real Engine refresh
// observes it. The Native the held Apply returns is committed and kept, and
// no later Apply of that pass, or of a later pass, starts until the
// authority is current again. Revocation and demotion are recorded as
// revoked; an unreadable file holds the operation as unavailable, never
// revoked. Restoring the authority continues from the committed Native,
// sending each remaining stage once; with no change the pass runs through.
func TestApplyRechecksAuthorityEachStage(t *testing.T) {
	for _, tt := range []struct {
		name    string
		change  func(t *testing.T, f *fixture, s store.Service)
		want    error
		code    store.ServiceConditionCode
		restore func(t *testing.T, f *fixture)
	}{
		{name: "healthy"},
		{
			name: "revoked",
			change: func(t *testing.T, f *fixture, s store.Service) {
				if err := auth.Revoke(f.config, s.Authority.TokenID); err != nil {
					t.Fatal(err)
				}
			},
			want: auth.ErrUnauthenticated,
			code: store.ServiceAuthorityRevoked,
		},
		{
			name: "demoted",
			change: func(t *testing.T, f *fixture, s store.Service) {
				if err := auth.SetRole(f.config, "alice", auth.Reader); err != nil {
					t.Fatal(err)
				}
			},
			want: auth.ErrPermissionDenied,
			code: store.ServiceAuthorityRevoked,
			restore: func(t *testing.T, f *fixture) {
				if err := auth.SetRole(f.config, "alice", auth.Operator); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			// A group-readable credential file is refused by every refresh,
			// so the snapshot goes stale: unavailable, not revoked.
			name: "unavailable",
			change: func(t *testing.T, f *fixture, s store.Service) {
				if err := os.Chmod(f.config, 0644); err != nil {
					t.Fatal(err)
				}
			},
			want: auth.ErrUnavailable,
			code: store.ServiceAuthorityUnavailable,
			restore: func(t *testing.T, f *fixture) {
				if err := os.Chmod(f.config, 0600); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			s := f.create(t, "alice", "req-1", "web")
			entered, release := make(chan struct{}), make(chan struct{})
			released := false
			open := func() {
				if !released {
					released = true
					close(release)
				}
			}
			defer open()
			f.runtime.set(func(r *fakeRuntime) {
				r.applyNative = []serving.Native{"v1:first", "v1:second", "v1:third"}
				r.observation = ready
				r.onApply = func(serving.Ref) { close(entered); <-release }
			})
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			errc := make(chan error, 1)
			go func() { errc <- f.c.Reconcile(ctx) }()
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("first apply not entered")
			}
			if tt.change != nil {
				tt.change(t, f, s)
				waitDelegation(t, f, s.Authority, tt.want)
			}
			open()
			var err error
			select {
			case err = <-errc:
			case <-time.After(15 * time.Second):
				t.Fatal("reconcile did not return after the held apply returned")
			}
			natives := func() []serving.Native {
				f.runtime.mu.Lock()
				defer f.runtime.mu.Unlock()
				var out []serving.Native
				for _, ref := range f.runtime.applies {
					out = append(out, ref.Native)
				}
				return out
			}
			settled := func(when string) {
				t.Helper()
				got, want := natives(), []serving.Native{s.Native, "v1:first", "v1:second", "v1:third"}
				if len(got) != len(want) {
					t.Fatalf("%s: applies given %q, want %q", when, got, want)
				}
				for i := range want {
					if got[i] != want[i] {
						t.Fatalf("%s: applies given %q, want %q", when, got, want)
					}
				}
				if cur := f.read(t, s.ID); cur.Native != "v1:third" || cur.Operation != nil || cur.Phase() != serving.Ready {
					t.Fatalf("%s: native %q op %+v phase %v", when, cur.Native, cur.Operation, cur.Phase())
				}
			}
			if tt.change == nil {
				if err != nil {
					t.Fatalf("healthy reconcile = %v", err)
				}
				settled("healthy")
				return
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("reconcile = %v, want %v", err, tt.want)
			}
			fenced := func(when string) {
				t.Helper()
				if a, _ := f.runtime.calls(); a != 1 {
					t.Fatalf("%s: %d applies, want only the held one", when, a)
				}
				cur := f.read(t, s.ID)
				if cur.Native != "v1:first" || cur.Operation == nil || *cur.Operation != *s.Operation {
					t.Fatalf("%s: native %q op %+v; want the held apply's native kept under the operation", when, cur.Native, cur.Operation)
				}
				if !hasCondition(cur, tt.code) {
					t.Fatalf("%s: conditions %+v, want %v", when, cur.Conditions, tt.code)
				}
				if tt.code == store.ServiceAuthorityUnavailable && hasCondition(cur, store.ServiceAuthorityRevoked) {
					t.Fatalf("%s: unavailable authority recorded as revoked", when)
				}
			}
			fenced("held pass")
			ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel2()
			if err := f.c.Reconcile(ctx2); !errors.Is(err, tt.want) {
				t.Fatalf("later pass = %v, want %v", err, tt.want)
			}
			fenced("later pass")
			if tt.restore == nil {
				return
			}
			tt.restore(t, f)
			waitDelegation(t, f, s.Authority, nil)
			if err := f.c.Reconcile(ctx2); err != nil {
				t.Fatalf("restored pass = %v", err)
			}
			settled("restored")
		})
	}
}
