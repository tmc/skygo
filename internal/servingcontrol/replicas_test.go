package servingcontrol_test

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/serving"
	"github.com/tmc/skygo/internal/servingcontrol"
	"github.com/tmc/skygo/internal/store"
)

// Runtime observations in these tests are modeled; admission and record
// changes use the actual Store, file authority and controller.
func TestFixedReplicaCounts(t *testing.T) {
	for _, count := range []int32{2, 8} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			f := newFixture(t)
			spec := testSpec()
			spec.Scaling.Replicas = count
			s, err := f.c.Create(f.actors["alice"], f.domain, "replicas", "web", "kube", spec)
			if err != nil {
				t.Fatal(err)
			}
			want := store.ServiceReservation{Replicas: uint64(count), CPUMillis: uint64(count) * 250, MemoryBytes: uint64(count) * (256 << 20)}
			if s.Reserved != want {
				t.Fatalf("reservation = %+v, want %+v", s.Reserved, want)
			}
			replayed, err := f.c.Create(f.actors["alice"], f.domain, "replicas", "web", "kube", spec)
			if err != nil || replayed.ID != s.ID {
				t.Fatalf("replay = %+v, %v", replayed, err)
			}
			changed := spec
			changed.Argv = []string{"/bin/other"}
			if _, err := f.c.Create(f.actors["alice"], f.domain, "replicas", "web", "kube", changed); !errors.Is(err, store.ErrConflict) {
				t.Fatalf("changed replay = %v", err)
			}
			if a, d := f.runtime.calls(); a != 0 || d != 0 {
				t.Fatalf("acceptance made runtime calls: %d/%d", a, d)
			}
			o := ready
			o.DesiredReplicas, o.UpdatedReplicas, o.ReadyReplicas, o.AvailableReplicas, o.ReadyEndpoints = count, count, count, count, count
			f.runtime.set(func(r *fakeRuntime) { r.observation = o })
			if err := f.reconcile(t); err != nil {
				t.Fatal(err)
			}
			if got := f.read(t, s.ID); got.Phase() != serving.Ready || got.Reserved != want {
				t.Fatalf("ready record = %+v", got)
			}
		})
	}
}

func TestFixedReplicaBudgetBeforeEffects(t *testing.T) {
	f := newFixture(t)
	if err := f.c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	var err error
	f.c, err = servingcontrol.New(servingcontrol.Config{Store: f.store, Auth: f.engine, Target: "kube", Binding: "kube/default", Runtime: f.runtime, Limits: store.ServiceLimits{Replicas: 2, CPUMillis: 500, MemoryBytes: 512 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	spec := testSpec()
	spec.Scaling.Replicas = 2
	first, err := f.c.Create(f.actors["alice"], f.domain, "first", "first", "kube", spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.c.Create(f.actors["alice"], f.domain, "overflow", "overflow", "kube", testSpec()); err == nil {
		t.Fatal("accepted beyond replica budget")
	}
	page, err := f.store.Services(0, 200)
	if err != nil || len(page.Services) != 1 || page.Services[0].Revision != first.Revision || page.Services[0].Reserved != first.Reserved {
		t.Fatalf("refusal changed records: %+v, %v", page, err)
	}
	if a, d := f.runtime.calls(); a != 0 || d != 0 {
		t.Fatalf("refusal made runtime calls: %d/%d", a, d)
	}
}

func TestFixedReplicaDeleteRetainsForwardCharge(t *testing.T) {
	f := newFixture(t)
	spec := testSpec()
	spec.Scaling.Replicas = 2
	s, err := f.c.Create(f.actors["alice"], f.domain, "create", "web", "kube", spec)
	if err != nil {
		t.Fatal(err)
	}
	o := ready
	o.DesiredReplicas, o.UpdatedReplicas, o.ReadyReplicas, o.AvailableReplicas, o.ReadyEndpoints = 2, 2, 2, 2, 2
	hold := make(chan struct{})
	f.runtime.set(func(r *fakeRuntime) {
		r.observation, r.deleted, r.forwardBlock, r.forwardHold = o, serving.Observation{Absent: true}, true, hold
	})
	if err := f.reconcile(t); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	done := make(chan error, 1)
	go func() {
		done <- f.c.Forward(f.actors["alice"], s.ID, httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil).WithContext(ctx))
	}()
	released := false
	t.Cleanup(func() {
		cancel()
		if !released {
			close(hold)
			<-done
		}
	})
	select {
	case <-f.runtime.forwardStarted:
	case <-ctx.Done():
		t.Fatal("forward did not start")
	}
	cur := f.read(t, s.ID)
	if _, err := f.c.Delete(f.actors["alice"], f.domain, "delete", s.ID, cur.Revision); err != nil {
		t.Fatal(err)
	}
	if err := f.reconcile(t); err == nil {
		t.Fatal("reconcile ignored admitted original forward")
	}
	if got := f.read(t, s.ID); got.Reserved != s.Reserved {
		t.Fatalf("charge released before original returned: %+v", got.Reserved)
	}
	if _, d := f.runtime.calls(); d != 0 {
		t.Fatalf("delete called with a forward admitted: %d", d)
	}
	close(hold)
	err = <-done
	released = true
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("forward = %v", err)
	}
	if err := f.reconcile(t); err != nil {
		t.Fatal(err)
	}
	got, err := f.c.Create(f.actors["alice"], f.domain, "create", "web", "kube", spec)
	if err != nil || !got.Retired || got.Reserved != (store.ServiceReservation{}) {
		t.Fatalf("deleted replay = %+v, %v", got, err)
	}
}
