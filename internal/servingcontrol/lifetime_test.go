package servingcontrol_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/serving"
	"github.com/tmc/skygo/internal/servingcontrol"
	"github.com/tmc/skygo/internal/store"
)

// Source-only; NOT_RUN. These tests use the real Store and file-backed auth
// engine of newFixture. A replaced CommitService is a MODELED commit fault;
// a direct Store commit made while a runtime call runs is a MODELED
// interleaving of the trusted Store writer. Every blocking call is bounded
// and every gate a test closes is released by a cleanup registered before
// any Fatal.

// snapshot returns every unretired service of f's Store with its record
// revision, in acceptance order.
func snapshot(t *testing.T, f *fixture) []store.Service {
	t.Helper()
	p, err := f.store.Services(0, 200)
	if err != nil {
		t.Fatal(err)
	}
	return p.Services
}

func sameSnapshot(a, b []store.Service) bool {
	return slices.EqualFunc(a, b, func(x, y store.Service) bool {
		return x.ID == y.ID && x.Revision == y.Revision && x.Native == y.Native && x.Desired == y.Desired
	})
}

// release returns a function that closes ch the first time it is called.
// Tests register it as a cleanup before blocking and use it to release.
func release(ch chan struct{}) func() {
	var once sync.Once
	return func() { once.Do(func() { close(ch) }) }
}

func closeWithin(c *servingcontrol.Controller, d time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return c.Close(ctx)
}

// drainRuntime is a fakeRuntime that keeps custody of its calls' originals
// (MODELED; no exchange runs): Drain repeats the first scripted offer of a
// service until Ack acknowledges its exact token and Native, then returns
// held[service] with token zero. onAck, if set, runs in each accepted Ack.
type drainRuntime struct {
	*fakeRuntime
	dmu    sync.Mutex
	offers map[string][]drainOffer
	held   map[string]error
	acks   []uint64
	onAck  func(ref serving.Ref)
}

type drainOffer struct {
	native serving.Native
	token  uint64
	err    error
}

func (r *drainRuntime) Drain(ref serving.Ref) (serving.Native, uint64, error) {
	r.dmu.Lock()
	defer r.dmu.Unlock()
	if o := r.offers[ref.Service]; len(o) > 0 {
		return o[0].native, o[0].token, o[0].err
	}
	return ref.Native, 0, r.held[ref.Service]
}

func (r *drainRuntime) Ack(ref serving.Ref, token uint64) error {
	r.dmu.Lock()
	o := r.offers[ref.Service]
	if len(o) == 0 || o[0].token != token || o[0].native != ref.Native {
		r.dmu.Unlock()
		return errors.New("modeled runtime: no such offer")
	}
	r.offers[ref.Service] = o[1:]
	r.acks = append(r.acks, token)
	hook := r.onAck
	r.dmu.Unlock()
	if hook != nil {
		hook(ref)
	}
	return nil
}

func (r *drainRuntime) script(f func(*drainRuntime)) {
	r.dmu.Lock()
	defer r.dmu.Unlock()
	f(r)
}

func (r *drainRuntime) acked() []uint64 {
	r.dmu.Lock()
	defer r.dmu.Unlock()
	return slices.Clone(r.acks)
}

// drainController returns a Controller on f's Store, auth and binding whose
// runtime is a drainRuntime. f's own Controller makes no call meanwhile:
// one live writer reconciles, as the supported premise requires.
func drainController(t *testing.T, f *fixture) (*servingcontrol.Controller, *drainRuntime) {
	t.Helper()
	r := &drainRuntime{fakeRuntime: &fakeRuntime{forwardStarted: make(chan struct{}, 1)},
		offers: map[string][]drainOffer{}, held: map[string]error{}}
	c, err := servingcontrol.New(servingcontrol.Config{
		Store: f.store, Auth: f.engine, Target: "kube", Binding: "kube/default", Runtime: r, Limits: testLimits,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeWithin(c, 10*time.Second) }) // tests check Close themselves
	return c, r
}

func reconcileWith(t *testing.T, c *servingcontrol.Controller) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return c.Reconcile(ctx)
}

var errModeledPending = errors.New("modeled original pending")

// pendingCreate reconciles a new service on c until its create was sent and
// its original is pending (MODELED): the committed Native is v1:sent.
func pendingCreate(t *testing.T, f *fixture, c *servingcontrol.Controller, r *drainRuntime, request, name string) store.Service {
	t.Helper()
	s, err := c.Create(f.actors["alice"], f.domain, request, name, "kube", testSpec())
	if err != nil {
		t.Fatal(err)
	}
	r.set(func(fr *fakeRuntime) {
		fr.applyNative = []serving.Native{"v1:sent"}
		fr.applyErr = errModeledPending
	})
	r.script(func(r *drainRuntime) { r.held[s.ID] = errModeledPending })
	if err := reconcileWith(t, c); !errors.Is(err, errModeledPending) {
		t.Fatalf("reconcile = %v, want the original pending", err)
	}
	if got := f.read(t, s.ID); got.Native != "v1:sent" || got.Operation == nil || *got.Operation != *s.Operation {
		t.Fatalf("record %q under %+v", got.Native, got.Operation)
	}
	r.set(func(fr *fakeRuntime) { fr.applyErr = nil })
	return s
}

// TestDrainHoldsOnlyItsService: an original that has not returned holds
// its service — no Apply, Observe or Delete, through repeated passes and a
// Delete — while a bystander on the same Controller progresses. Close does
// not wait for it and reports it held; once it has returned and its late
// result is offered, Close (after Stop) commits that Native under the
// original operation before acknowledging it, and returns nil only after
// the runtime reports done.
func TestDrainHoldsOnlyItsService(t *testing.T) {
	f := newFixture(t)
	c, r := drainController(t, f)
	a := pendingCreate(t, f, c, r, "req-a", "web")
	applies, deletes := r.calls()
	if _, err := c.Delete(f.actors["alice"], f.domain, "del-a", a.ID, f.read(t, a.ID).Revision); err != nil {
		t.Fatal(err)
	}
	b, err := c.Create(f.actors["alice"], f.domain, "req-b", "api", "kube", testSpec())
	if err != nil {
		t.Fatal(err)
	}
	r.set(func(fr *fakeRuntime) {
		fr.applyNative = []serving.Native{"v1:b"}
		fr.observation = ready
	})
	for range 2 {
		if err := reconcileWith(t, c); !errors.Is(err, errModeledPending) {
			t.Fatalf("reconcile with A held = %v", err)
		}
	}
	if got := f.read(t, b.ID); got.Native != "v1:b" || got.Phase() != serving.Ready {
		t.Fatalf("bystander %q %v, want it applied and ready", got.Native, got.Phase())
	}
	calledA := func() bool {
		r.fakeRuntime.mu.Lock()
		defer r.fakeRuntime.mu.Unlock()
		return slices.ContainsFunc(slices.Concat(r.applies[applies:], r.deletes[deletes:]), func(ref serving.Ref) bool { return ref.Service == a.ID })
	}
	if calledA() {
		t.Fatal("runtime called for the held service")
	}
	got := f.read(t, a.ID)
	if got.Desired != serving.Absent || got.Operation == nil || *got.Operation != *a.Operation || got.Native != "v1:sent" {
		t.Fatalf("held record %q %v under %+v; want the Apply operation kept", got.Native, got.Desired, got.Operation)
	}

	c.Stop()
	if err := closeWithin(c, 10*time.Second); !errors.Is(err, errModeledPending) {
		t.Fatalf("Close with A pending = %v", err)
	}
	r.script(func(r *drainRuntime) {
		r.offers[a.ID] = []drainOffer{{native: "v1:uid", token: 7}}
		r.held[a.ID] = nil
		r.onAck = func(ref serving.Ref) {
			if s, err := f.store.Service(ref.Service); err != nil || s.Native != "v1:uid" {
				t.Errorf("ack before commit: %q, %v", s.Native, err)
			}
		}
	})
	if err := closeWithin(c, 10*time.Second); err != nil {
		t.Fatalf("Close after the original returned = %v", err)
	}
	if got := f.read(t, a.ID); got.Native != "v1:uid" || got.Operation == nil || *got.Operation != *a.Operation {
		t.Fatalf("late native %q under %+v", got.Native, got.Operation)
	}
	if acks := r.acked(); !slices.Equal(acks, []uint64{7}) {
		t.Fatalf("acks %v, want [7]", acks)
	}
	if calledA() {
		t.Fatal("Close made a runtime call for the drained service")
	}
}

// replaceOperation ends the operation in flight of service id with a fresh
// observation, keeping its Native, and starts another Apply operation of
// the same rollout: the legal sequence of the trusted Store writer (MODELED
// interleaving). It returns the new operation.
func replaceOperation(t *testing.T, f *fixture, id string) store.ServiceOperation {
	t.Helper()
	cur := f.read(t, id)
	cm := cur.Commit()
	cm.Operation = nil
	cm.Observation = &serving.Observation{ObservedGeneration: 1}
	cm.ObservedAt = time.Now().UTC()
	cur, err := f.store.CommitService(id, cur.Revision, cm)
	if err != nil {
		t.Fatal(err)
	}
	op := store.ServiceOperation{ID: "0123456789abcdef0123456789abcdef", Kind: store.ServiceApply,
		Generation: cur.History.Current().Generation, Revision: cur.History.Current().Revision, Started: time.Now().UTC()}
	cm = cur.Commit()
	cm.Operation = &op
	if _, err := f.store.CommitService(id, cur.Revision, cm); err != nil {
		t.Fatal(err)
	}
	return op
}

// TestDrainNeverAcksByNative: an offer is acknowledged only after its
// Native is committed, or found committed, under the original operation
// over the last Native known committed. A record whose Native another
// writer advanced under the same operation (MODELED direct commit), whose
// operation ended with the Native coincidentally equal, or whose operation
// was replaced by another, non-nil one with the Native equal (C01), holds
// the offer unacknowledged, with no runtime call, through repeated passes
// and Close. The positive: the same Native under the same operation is
// acknowledged, and a lost commit answer (MODELED) is acknowledged only
// once the commit is found.
func TestDrainNeverAcksByNative(t *testing.T) {
	for _, tt := range []struct {
		name    string
		edit    func(cm *store.ServiceCommit)
		replace bool
		fault   bool
		ack     bool
	}{
		{"same op same native", nil, false, false, true},
		{"same op advanced", func(cm *store.ServiceCommit) { cm.Native = "v1:other" }, false, false, false},
		{"op ended equal native", func(cm *store.ServiceCommit) {
			// The Store clears an operation only with a fresh observation.
			cm.Operation = nil
			cm.Observation = &serving.Observation{ObservedGeneration: 1}
			cm.ObservedAt = time.Now().UTC()
		}, false, false, false},
		{"another op equal native", nil, true, false, false},
		{"lost commit answer", nil, false, true, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			c, r := drainController(t, f)
			s := pendingCreate(t, f, c, r, "req-1", "web")
			if tt.edit != nil {
				cur := f.read(t, s.ID)
				cm := cur.Commit()
				tt.edit(&cm)
				if _, err := f.store.CommitService(s.ID, cur.Revision, cm); err != nil {
					t.Fatal(err)
				}
			}
			var replaced store.ServiceOperation
			if tt.replace {
				replaced = replaceOperation(t, f, s.ID)
				if replaced == *s.Operation {
					t.Fatal("replacement operation equals the original")
				}
			}
			native := serving.Native("v1:sent")
			if tt.fault {
				native = "v1:uid"
				fault := errors.New("modeled lost commit answer")
				servingcontrol.SetCommit(c, func(id string, rev uint64, next store.ServiceCommit) (store.Service, error) {
					if _, err := f.store.CommitService(id, rev, next); err != nil {
						return store.Service{}, err
					}
					return store.Service{}, fault
				})
				r.script(func(r *drainRuntime) { r.offers[s.ID] = []drainOffer{{native: native, token: 3}}; r.held[s.ID] = nil })
				if err := reconcileWith(t, c); !errors.Is(err, fault) || len(r.acked()) != 0 {
					t.Fatalf("reconcile with the commit answer lost = %v, acks %v", err, r.acked())
				}
				servingcontrol.SetCommit(c, f.store.CommitService)
			} else {
				r.script(func(r *drainRuntime) { r.offers[s.ID] = []drainOffer{{native: native, token: 3}}; r.held[s.ID] = nil })
			}
			applies, deletes := r.calls()
			if !tt.ack {
				for range 2 {
					if err := reconcileWith(t, c); err == nil {
						t.Fatal("reconcile acknowledged a changed original's offer")
					}
				}
				if err := closeWithin(c, 10*time.Second); err == nil {
					t.Fatal("Close returned nil with the offer unacknowledged")
				}
				if acks := r.acked(); len(acks) != 0 {
					t.Fatalf("acks %v, want none", acks)
				}
				if a, d := r.calls(); a != applies || d != deletes {
					t.Fatalf("runtime called with the offer held: %d applies, %d deletes", a-applies, d-deletes)
				}
				if tt.replace {
					if got := f.read(t, s.ID); got.Native != "v1:sent" || got.Operation == nil || *got.Operation != replaced {
						t.Fatalf("record %q under %+v, want the replacement operation untouched", got.Native, got.Operation)
					}
				}
				return
			}
			r.set(func(fr *fakeRuntime) { fr.observation = ready })
			if err := reconcileWith(t, c); err != nil {
				t.Fatalf("reconcile = %v", err)
			}
			if acks := r.acked(); !slices.Equal(acks, []uint64{3}) {
				t.Fatalf("acks %v, want [3]", acks)
			}
			if got := f.read(t, s.ID); got.Native != native {
				t.Fatalf("native %q, want %q", got.Native, native)
			}
			if err := closeWithin(c, 10*time.Second); err != nil {
				t.Fatalf("Close = %v", err)
			}
		})
	}
}

// readyNoOperation reconciles a new service on c to Ready, with its Apply
// operation integrated, and then once more, which drains the Apply's
// original: no operation is in flight and no original remains.
func readyNoOperation(t *testing.T, f *fixture, c *servingcontrol.Controller, r *drainRuntime, request, name string) store.Service {
	t.Helper()
	s, err := c.Create(f.actors["alice"], f.domain, request, name, "kube", testSpec())
	if err != nil {
		t.Fatal(err)
	}
	r.set(func(fr *fakeRuntime) {
		fr.applyNative = []serving.Native{"v1:ready"}
		fr.observation = ready
	})
	for range 2 {
		if err := reconcileWith(t, c); err != nil {
			t.Fatalf("reconcile to ready = %v", err)
		}
	}
	got := f.read(t, s.ID)
	if got.Native != "v1:ready" || got.Operation != nil || got.Phase() != serving.Ready {
		t.Fatalf("ready record %q under %+v, phase %v", got.Native, got.Operation, got.Phase())
	}
	return got
}

// TestFirstDeleteDrainsAfterStop (V12, control lens): a Ready service with
// no operation in flight is deleted; the pass commits a Delete operation,
// records the original under it over the committed Native before the
// runtime Delete, and that Delete leaves its original pending (MODELED).
// Stop then Close, before any other pass, reports it held. Once the same
// original returns cleanly and offers its fact, Close (after Stop) commits
// it under that Delete operation, acknowledges its token and returns nil,
// with no runtime call. Its failed-Close twin stays held through repeated
// Closes, and nothing is acknowledged.
func TestFirstDeleteDrainsAfterStop(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "clean", true: "failed close"}[failed], func(t *testing.T) {
			f := newFixture(t)
			c, r := drainController(t, f)
			s := readyNoOperation(t, f, c, r, "req-1", "web")
			if _, err := c.Delete(f.actors["alice"], f.domain, "del-1", s.ID, s.Revision); err != nil {
				t.Fatal(err)
			}
			r.set(func(fr *fakeRuntime) { fr.deleteErr = errModeledPending })
			r.script(func(r *drainRuntime) { r.held[s.ID] = errModeledPending })
			if err := reconcileWith(t, c); !errors.Is(err, errModeledPending) {
				t.Fatalf("first delete pass = %v, want its original pending", err)
			}
			deleted := f.read(t, s.ID)
			if deleted.Operation == nil || deleted.Operation.Kind != store.ServiceDelete || deleted.Native != "v1:ready" {
				t.Fatalf("record %q under %+v, want a committed Delete operation", deleted.Native, deleted.Operation)
			}
			r.fakeRuntime.mu.Lock()
			refs := slices.Clone(r.deletes)
			r.fakeRuntime.mu.Unlock()
			if len(refs) != 1 || refs[0].Native != "v1:ready" {
				t.Fatalf("runtime deletes %+v, want one of the committed Native", refs)
			}
			applies, deletes := r.calls()

			c.Stop()
			if err := closeWithin(c, 10*time.Second); !errors.Is(err, errModeledPending) {
				t.Fatalf("Close with the first delete's original pending = %v", err)
			}
			if failed {
				r.script(func(r *drainRuntime) { r.held[s.ID] = errModeledClose })
				for range 2 {
					if err := closeWithin(c, 10*time.Second); !errors.Is(err, errModeledClose) {
						t.Fatalf("Close with the original's Close failed = %v", err)
					}
				}
				if acks := r.acked(); len(acks) != 0 {
					t.Fatalf("acks %v with the Close failed", acks)
				}
			} else {
				r.script(func(r *drainRuntime) {
					r.offers[s.ID] = []drainOffer{{native: "v1:ready", token: 9}}
					r.held[s.ID] = nil
				})
				if err := closeWithin(c, 10*time.Second); err != nil {
					t.Fatalf("Close after the original returned = %v", err)
				}
				if acks := r.acked(); !slices.Equal(acks, []uint64{9}) {
					t.Fatalf("acks %v, want [9]", acks)
				}
			}
			if a, d := r.calls(); a != applies || d != deletes {
				t.Fatalf("Close made %d applies and %d deletes", a-applies, d-deletes)
			}
			if got := f.read(t, s.ID); got.Operation == nil || *got.Operation != *deleted.Operation || got.Reserved == (store.ServiceReservation{}) {
				t.Fatalf("record after Close %+v; want the Delete operation and reservation kept", got)
			}
		})
	}
}

// errModeledClose is a failed original response Close (MODELED).
var errModeledClose = errors.New("modeled original close failed")

// TestDeleteOperationCommittedBeforeSend: the first Delete of a service
// with no operation in flight is sent only after its Delete operation is
// known committed. A failed commit (MODELED) sends nothing and records no
// original; a lost commit answer (MODELED) sends nothing in that pass, and
// the next pass deletes under the committed operation it reads.
func TestDeleteOperationCommittedBeforeSend(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "failed", true: "lost answer"}[lost], func(t *testing.T) {
			f := newFixture(t)
			c, r := drainController(t, f)
			s := readyNoOperation(t, f, c, r, "req-1", "web")
			if _, err := c.Delete(f.actors["alice"], f.domain, "del-1", s.ID, s.Revision); err != nil {
				t.Fatal(err)
			}
			fault := errors.New("modeled delete operation commit fault")
			servingcontrol.SetCommit(c, func(id string, rev uint64, next store.ServiceCommit) (store.Service, error) {
				if lost {
					if _, err := f.store.CommitService(id, rev, next); err != nil {
						return store.Service{}, err
					}
				}
				return store.Service{}, fault
			})
			if err := reconcileWith(t, c); !errors.Is(err, fault) {
				t.Fatalf("reconcile = %v, want the commit fault", err)
			}
			if _, d := r.calls(); d != 0 {
				t.Fatalf("%d runtime deletes before the operation was known committed", d)
			}
			got := f.read(t, s.ID)
			if !lost {
				if got.Operation != nil {
					t.Fatalf("failed commit left operation %+v", got.Operation)
				}
				servingcontrol.SetCommit(c, f.store.CommitService)
				if err := closeWithin(c, 10*time.Second); err != nil {
					t.Fatalf("Close with nothing sent = %v", err)
				}
				return
			}
			if got.Operation == nil || got.Operation.Kind != store.ServiceDelete {
				t.Fatalf("lost commit answer: operation %+v, want the committed Delete", got.Operation)
			}
			servingcontrol.SetCommit(c, f.store.CommitService)
			r.set(func(fr *fakeRuntime) { fr.deleted = serving.Observation{Absent: true} })
			if err := reconcileWith(t, c); err != nil {
				t.Fatalf("reconcile under the committed operation = %v", err)
			}
			if _, d := r.calls(); d != 1 {
				t.Fatalf("%d runtime deletes, want 1", d)
			}
			if _, err := f.store.Service(s.ID); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("service not retired: %v", err)
			}
		})
	}
}

// observeHook is a drainRuntime whose next Observe first runs onObserve:
// a MODELED interleaving of the trusted Store writer during the read.
type observeHook struct {
	*drainRuntime
	onObserve func(serving.Ref)
}

func (r *observeHook) Observe(ctx context.Context, ref serving.Ref) (serving.Observation, error) {
	r.fakeRuntime.mu.Lock()
	hook := r.onObserve
	r.onObserve = nil
	r.fakeRuntime.mu.Unlock()
	if hook != nil {
		hook(ref)
	}
	return r.fakeRuntime.Observe(ctx, ref)
}

// TestReleasedReservationObservesOnly (V14, control lens): a service
// deleted after an absence was observed while present has no operation in
// flight and keeps its reservation. Its cleanup only reads: a fresh absence
// of its committed Native releases the reservation and retires it; a read
// error, an observation other than absence, or a Native changed during the
// read (MODELED) holds it. No case deletes, applies or starts an operation.
func TestReleasedReservationObservesOnly(t *testing.T) {
	for _, tt := range []struct {
		name    string
		obs     serving.Observation
		obsErr  error
		advance bool
		retired bool
	}{
		{name: "fresh absence", obs: serving.Observation{Absent: true}, retired: true},
		{name: "read error", obs: serving.Observation{Absent: true}, obsErr: errors.New("modeled read error")},
		{name: "not absent", obs: ready},
		{name: "native advanced", obs: serving.Observation{Absent: true}, advance: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			r := &observeHook{drainRuntime: &drainRuntime{fakeRuntime: &fakeRuntime{forwardStarted: make(chan struct{}, 1)},
				offers: map[string][]drainOffer{}, held: map[string]error{}}}
			c, err := servingcontrol.New(servingcontrol.Config{
				Store: f.store, Auth: f.engine, Target: "kube", Binding: "kube/default", Runtime: r, Limits: testLimits,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { closeWithin(c, 10*time.Second) })
			s := readyNoOperation(t, f, c, r.drainRuntime, "req-1", "web")
			// Absence observed while present: the objects vanished (MODELED).
			r.set(func(fr *fakeRuntime) { fr.observation = serving.Observation{Absent: true} })
			if err := reconcileWith(t, c); err != nil {
				t.Fatal(err)
			}
			cur := f.read(t, s.ID)
			if _, err := c.Delete(f.actors["alice"], f.domain, "del-1", s.ID, cur.Revision); err != nil {
				t.Fatal(err)
			}
			held := f.read(t, s.ID)
			if held.Operation != nil || held.Observation == nil || !held.Observation.Absent || held.Reserved == (store.ServiceReservation{}) {
				t.Fatalf("record %+v, want released with its reservation held", held)
			}
			r.set(func(fr *fakeRuntime) { fr.observation, fr.observeErr = tt.obs, tt.obsErr })
			if tt.advance {
				r.onObserve = func(serving.Ref) {
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
			}
			r.fakeRuntime.mu.Lock()
			observes := r.observes
			r.fakeRuntime.mu.Unlock()
			applies, _ := r.calls()
			err = reconcileWith(t, c)
			r.fakeRuntime.mu.Lock()
			observed := r.observes - observes
			r.fakeRuntime.mu.Unlock()
			if a, del := r.calls(); del != 0 || a != applies {
				t.Fatalf("released cleanup made %d deletes and %d applies", del, a-applies)
			}
			if observed != 1 {
				t.Fatalf("%d observes, want the one read", observed)
			}
			got, gerr := f.store.Service(s.ID)
			if tt.retired {
				if err != nil || !errors.Is(gerr, store.ErrNotFound) {
					t.Fatalf("fresh absence: reconcile %v, record %v; want retired", err, gerr)
				}
				return
			}
			if err == nil || gerr != nil || got.Operation != nil {
				t.Fatalf("reconcile = %v, record %+v, %v; want held with no operation", err, got, gerr)
			}
			if tt.advance {
				return
			}
			if got.Reserved == (store.ServiceReservation{}) || got.Revision != held.Revision {
				t.Fatalf("record %+v committed or its reservation released without a fresh absence", got)
			}
			// A later pass still only reads: a commit of the released record
			// on the hold, such as a condition, would have released the
			// reservation and let this pass retire it unobserved.
			if err := reconcileWith(t, c); err == nil {
				t.Fatal("held released record not held again")
			}
			if again, err := f.store.Service(s.ID); err != nil || again.Reserved == (store.ServiceReservation{}) {
				t.Fatalf("released record after a second hold: %+v, %v", again, err)
			}
		})
	}
}

func TestClosedRefusesEveryCallWithoutEffect(t *testing.T) {
	f := newFixture(t)
	s := f.create(t, "alice", "req-1", "web")
	if err := closeWithin(f.c, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	before := snapshot(t, f)
	applies, deletes := f.runtime.calls()
	calls := map[string]func() error{
		"create": func() error {
			_, err := f.c.Create(f.actors["alice"], f.domain, "req-2", "api", "kube", testSpec())
			return err
		},
		"replayed create": func() error {
			_, err := f.c.Create(f.actors["alice"], f.domain, "req-1", "web", "kube", testSpec())
			return err
		},
		"get":  func() error { _, err := f.c.Get(f.actors["alice"], s.ID); return err },
		"list": func() error { _, err := f.c.List(f.actors["alice"], 0, 50); return err },
		"delete": func() error {
			_, err := f.c.Delete(f.actors["alice"], f.domain, "del-1", s.ID, s.Revision)
			return err
		},
		"reconcile": func() error { return f.reconcile(t) },
		"forward":   func() error { return forwardStatus(f, "alice", s.ID) },
	}
	for name, call := range calls {
		if err := call(); !errors.Is(err, servingcontrol.ErrClosed) {
			t.Errorf("%s after Close = %v, want ErrClosed", name, err)
		}
	}
	if after := snapshot(t, f); !sameSnapshot(before, after) {
		t.Fatalf("refused calls changed the store: %+v -> %+v", before, after)
	}
	if a, d := f.runtime.calls(); a != applies || d != deletes {
		t.Fatalf("refused calls reached the runtime: %d applies, %d deletes", a-applies, d-deletes)
	}
}

// TestCloseHoldsAdmittedCreate holds an admitted Create inside its call,
// after admission and before its Store write: Close reports the hold, and
// returns nil only after the original call made its write and returned.
func TestCloseHoldsAdmittedCreate(t *testing.T) {
	f := newFixture(t)
	entered := make(chan struct{}, 1)
	gate := make(chan struct{})
	free := release(gate)
	t.Cleanup(free)
	c, err := servingcontrol.New(servingcontrol.Config{
		Store: f.store, Auth: f.engine, Target: "kube", Binding: "kube/default", Runtime: f.runtime, Limits: testLimits,
		Admit: func(string, serving.Spec) error {
			entered <- struct{}{}
			<-gate
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	type created struct {
		s   store.Service
		err error
	}
	done := make(chan created, 1)
	go func() {
		s, err := c.Create(f.actors["alice"], f.domain, "req-held", "web", "kube", testSpec())
		done <- created{s, err}
		close(done)
	}()
	// The original call is joined before the fixture's Store closes.
	t.Cleanup(func() {
		free()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("held create never returned")
		}
	})
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("create was not admitted")
	}
	if err := closeWithin(c, 100*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close with an admitted create = %v, want a hold", err)
	}
	// A new call is refused while the held one runs.
	if _, err := c.List(f.actors["alice"], 0, 50); !errors.Is(err, servingcontrol.ErrClosed) {
		t.Fatalf("list while closing = %v", err)
	}
	free()
	var got created
	select {
	case got = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("released create did not return")
	}
	if got.err != nil {
		t.Fatalf("original create = %v", got.err)
	}
	if err := closeWithin(c, 10*time.Second); err != nil {
		t.Fatalf("Close after the original call returned = %v", err)
	}
	// The original call's write is the last one, and it is durable.
	if s := f.read(t, got.s.ID); s.Operation == nil || s.Name != "web" {
		t.Fatalf("original create not committed: %+v", s)
	}
}

// TestCloseCommitsHeldResult holds a result whose commit failed: Close
// reports it and does not return nil; once the MODELED fault clears, Close
// commits exactly that result, under its operation, with no runtime call.
func TestCloseCommitsHeldResult(t *testing.T) {
	f := newFixture(t)
	s := f.create(t, "alice", "req-1", "web")
	f.runtime.set(func(r *fakeRuntime) { r.applyNative = []serving.Native{"v1:acknowledged"} })
	fault := errors.New("modeled commit fault")
	servingcontrol.SetCommit(f.c, func(string, uint64, store.ServiceCommit) (store.Service, error) {
		return store.Service{}, fault
	})
	if err := f.reconcile(t); !errors.Is(err, fault) {
		t.Fatalf("reconcile = %v, want the commit fault", err)
	}
	applies, deletes := f.runtime.calls()
	err := closeWithin(f.c, 10*time.Second)
	if err == nil || errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, fault) {
		t.Fatalf("Close with a held result = %v, want the held commit's error", err)
	}
	if got := f.read(t, s.ID); got.Native != "" {
		t.Fatalf("native %q committed despite the fault", got.Native)
	}
	servingcontrol.SetCommit(f.c, f.store.CommitService)
	if err := closeWithin(f.c, 10*time.Second); err != nil {
		t.Fatalf("Close after the fault cleared = %v", err)
	}
	got := f.read(t, s.ID)
	if got.Native != "v1:acknowledged" || got.Operation == nil || *got.Operation != *s.Operation {
		t.Fatalf("held result committed as %q under %+v, want the original under %+v", got.Native, got.Operation, s.Operation)
	}
	if a, d := f.runtime.calls(); a != applies || d != deletes {
		t.Fatalf("Close made %d applies and %d deletes", a-applies, d-deletes)
	}
}

// TestHeldResultNeverOverwritesAnotherOperation commits, in a MODELED
// interleaving of the trusted Store writer, a newer operation while a
// result of the original one is held. The held result is never given to the
// newer operation, and Close keeps reporting it.
func TestHeldResultNeverOverwritesAnotherOperation(t *testing.T) {
	f := newFixture(t)
	f.held = true
	s := f.create(t, "alice", "req-1", "web")
	f.runtime.set(func(r *fakeRuntime) { r.applyNative = []serving.Native{"v1:acknowledged"} })
	servingcontrol.SetCommit(f.c, func(string, uint64, store.ServiceCommit) (store.Service, error) {
		return store.Service{}, errors.New("modeled commit fault")
	})
	_ = f.reconcile(t)
	servingcontrol.SetCommit(f.c, f.store.CommitService)
	cur := f.read(t, s.ID)
	if _, err := f.c.Delete(f.actors["alice"], f.domain, "del-1", s.ID, cur.Revision); err != nil {
		t.Fatal(err)
	}
	// The trusted writer integrates the original operation with a fresh
	// observation of the committed Native and starts a Delete operation.
	cur = f.read(t, s.ID)
	cm := cur.Commit()
	cm.Operation = nil
	cm.Observation = &serving.Observation{ObservedGeneration: 1}
	cm.ObservedAt = time.Now().UTC()
	cur, err := f.store.CommitService(s.ID, cur.Revision, cm)
	if err != nil {
		t.Fatal(err)
	}
	cm = cur.Commit()
	cm.Operation = &store.ServiceOperation{ID: "0123456789abcdef0123456789abcdef", Kind: store.ServiceDelete,
		Generation: 1, Revision: 1, Started: time.Now().UTC()}
	if cur, err = f.store.CommitService(s.ID, cur.Revision, cm); err != nil {
		t.Fatal(err)
	}
	applies, deletes := f.runtime.calls()
	if err := f.reconcile(t); err == nil {
		t.Fatal("reconcile committed a held result under another operation")
	}
	got := f.read(t, s.ID)
	if got.Native != "" || got.Operation == nil || *got.Operation != *cur.Operation {
		t.Fatalf("record %q under %+v, want the newer operation's untouched", got.Native, got.Operation)
	}
	if a, d := f.runtime.calls(); a != applies || d != deletes {
		t.Fatal("runtime called with a result held")
	}
	if err := closeWithin(f.c, 10*time.Second); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close = %v, want the held result reported", err)
	}
}

// TestDeleteWaitsForHeldForward holds a forward whose context ended but
// whose original exchange has not settled. Deletion latches, but nothing is
// deleted, cleared or retired, and Close holds, until the forward returns.
func TestDeleteWaitsForHeldForward(t *testing.T) {
	f := newFixture(t)
	s := f.create(t, "alice", "req-1", "web")
	f.runtime.set(func(r *fakeRuntime) { r.observation = ready })
	if err := f.reconcile(t); err != nil {
		t.Fatal(err)
	}
	hold := make(chan struct{})
	free := release(hold)
	t.Cleanup(free)
	f.runtime.set(func(r *fakeRuntime) { r.forwardBlock, r.forwardHold = true, hold })
	done := make(chan error, 1)
	go func() {
		done <- forwardStatus(f, "alice", s.ID)
		close(done)
	}()
	t.Cleanup(func() {
		free()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("held forward never returned")
		}
	})
	select {
	case <-f.runtime.forwardStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("forward did not start")
	}
	cur := f.read(t, s.ID)
	if _, err := f.c.Delete(f.actors["alice"], f.domain, "del-1", s.ID, cur.Revision); err != nil {
		t.Fatal(err)
	}
	f.runtime.set(func(r *fakeRuntime) { r.deleted = serving.Observation{Absent: true} })
	_, deletesBefore := f.runtime.calls()
	if err := f.reconcile(t); err == nil {
		t.Fatal("reconcile removed a service with a forward admitted")
	}
	if _, d := f.runtime.calls(); d != deletesBefore {
		t.Fatal("runtime delete ran with a forward admitted")
	}
	got := f.read(t, s.ID)
	if got.Desired != serving.Absent || got.Reserved == (store.ServiceReservation{}) {
		t.Fatalf("record released with a forward admitted: %+v", got)
	}
	select {
	case err := <-done:
		t.Fatalf("forward returned before its exchange settled: %v", err)
	default:
	}
	if err := closeWithin(f.c, 100*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close with a held forward = %v, want a hold", err)
	}
	free()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("forward = %v, want canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("released forward did not return")
	}
	if err := closeWithin(f.c, 10*time.Second); err != nil {
		t.Fatalf("Close after the forward returned = %v", err)
	}
}

// TestDeleteAfterForwardReturns is the positive twin: once the forward has
// returned, the deleted service is removed and retired.
func TestDeleteAfterForwardReturns(t *testing.T) {
	f := newFixture(t)
	s := f.create(t, "alice", "req-1", "web")
	f.runtime.set(func(r *fakeRuntime) { r.observation = ready })
	if err := f.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if err := forwardStatus(f, "alice", s.ID); err != nil {
		t.Fatal(err)
	}
	cur := f.read(t, s.ID)
	if _, err := f.c.Delete(f.actors["alice"], f.domain, "del-1", s.ID, cur.Revision); err != nil {
		t.Fatal(err)
	}
	f.runtime.set(func(r *fakeRuntime) { r.deleted = serving.Observation{Absent: true} })
	if err := f.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Service(s.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("service not retired: %v", err)
	}
}

// TestLateNativeVoidsAbsence commits, in a MODELED interleaving of the
// trusted Store writer during the runtime Delete, a late Native of the same
// operation. The absence that Delete observed is of the older Native and
// releases nothing; a fresh absence of the committed Native does.
func TestLateNativeVoidsAbsence(t *testing.T) {
	f := newFixture(t)
	s := f.create(t, "alice", "req-1", "web")
	f.runtime.set(func(r *fakeRuntime) {
		r.applyNative = []serving.Native{"v1:declared"}
		r.applyErr = errors.New("lost response")
	})
	_ = f.reconcile(t)
	cur := f.read(t, s.ID)
	if _, err := f.c.Delete(f.actors["alice"], f.domain, "del-1", s.ID, cur.Revision); err != nil {
		t.Fatal(err)
	}
	f.runtime.set(func(r *fakeRuntime) {
		r.applyErr = nil
		r.deleted = serving.Observation{Absent: true}
		r.onDelete = func(serving.Ref) {
			cur, err := f.store.Service(s.ID)
			if err != nil {
				t.Error(err)
				return
			}
			cm := cur.Commit()
			cm.Native = "v1:late"
			if _, err := f.store.CommitService(s.ID, cur.Revision, cm); err != nil {
				t.Error(err)
			}
		}
	})
	if err := f.reconcile(t); err == nil {
		t.Fatal("absence of an older Native accepted")
	}
	got := f.read(t, s.ID)
	if got.Operation == nil || *got.Operation != *s.Operation || got.Reserved == (store.ServiceReservation{}) {
		t.Fatalf("older absence released the service: %+v", got)
	}
	// The next pass deletes against the committed late Native and its
	// fresh absence releases and retires the service.
	if err := f.reconcile(t); err != nil {
		t.Fatal(err)
	}
	f.runtime.mu.Lock()
	last := f.runtime.deletes[len(f.runtime.deletes)-1]
	f.runtime.mu.Unlock()
	if last.Native != "v1:late" {
		t.Fatalf("fresh delete saw %q, want the late Native", last.Native)
	}
	if _, err := f.store.Service(s.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("service not retired after a fresh absence: %v", err)
	}
}

// setRole changes principal's role in f's credential file and waits,
// bounded, until the engine's refresh shows it.
func setRole(t *testing.T, f *fixture, principal string, role auth.Role) {
	t.Helper()
	if err := auth.SetRole(f.config, principal, role); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		a, err := f.engine.Check(f.actors[principal], auth.Read)
		if err == nil && a.Role() == role {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("role of %s not refreshed to %v: %v", principal, role, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestDemotedAdminReplayRefused deletes alice's service as the admin carol,
// demotes carol to operator in the credential file, and replays the exact
// delete: the replay is refused as NotFound with no Store write or runtime
// call, before and after retirement. A current admin's and the owner's own
// replays still succeed.
func TestDemotedAdminReplayRefused(t *testing.T) {
	f := newFixture(t)
	s := f.create(t, "alice", "req-1", "web")
	if _, err := f.c.Delete(f.actors["carol"], f.domain, "del-carol", s.ID, s.Revision); err != nil {
		t.Fatal(err)
	}
	v, err := f.c.Delete(f.actors["carol"], f.domain, "del-carol", s.ID, s.Revision)
	if err != nil || v.ID != s.ID {
		t.Fatalf("current admin replay = %+v, %v", v, err)
	}
	// alice's own service, deleted and replayed by alice.
	own := f.create(t, "alice", "req-own", "own")
	if _, err := f.c.Delete(f.actors["alice"], f.domain, "del-own", own.ID, own.Revision); err != nil {
		t.Fatal(err)
	}

	setRole(t, f, "carol", auth.Operator)
	refused := func(when string) {
		t.Helper()
		before := snapshot(t, f)
		applies, deletes := f.runtime.calls()
		if _, err := f.c.Delete(f.actors["carol"], f.domain, "del-carol", s.ID, s.Revision); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("%s: demoted replay = %v, want NotFound", when, err)
		}
		if _, err := f.c.Get(f.actors["carol"], s.ID); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("%s: demoted get = %v, want NotFound", when, err)
		}
		if after := snapshot(t, f); !sameSnapshot(before, after) {
			t.Fatalf("%s: refused replay wrote the store", when)
		}
		if a, d := f.runtime.calls(); a != applies || d != deletes {
			t.Fatalf("%s: refused replay reached the runtime", when)
		}
	}
	refused("unretired")
	if v, err := f.c.Delete(f.actors["alice"], f.domain, "del-own", own.ID, own.Revision); err != nil || v.ID != own.ID {
		t.Fatalf("owner replay = %+v, %v", v, err)
	}

	f.runtime.set(func(r *fakeRuntime) { r.deleted = serving.Observation{Absent: true} })
	if err := f.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Service(s.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("service not retired: %v", err)
	}
	refused("retired")

	setRole(t, f, "carol", auth.Admin)
	v, err = f.c.Delete(f.actors["carol"], f.domain, "del-carol", s.ID, s.Revision)
	if err != nil || !v.Retired || v.ID != s.ID {
		t.Fatalf("restored admin replay = %+v, %v; want the retired view", v, err)
	}
}
