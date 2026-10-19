package control

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

type routeBackend struct {
	checks, starts, observes, cancels, logs int
	refs                                    []backend.Reference
	check                                   func(backend.Task) error
	start                                   func(backend.Task)
}

func (b *routeBackend) Check(t backend.Task) error {
	b.checks++
	if b.check != nil {
		return b.check(t)
	}
	return nil
}
func (b *routeBackend) Start(_ context.Context, _ string, task backend.Task) error {
	b.starts++
	if b.start != nil {
		b.start(task)
	}
	return nil
}
func (b *routeBackend) Observe(_ context.Context, r backend.Reference) (backend.Status, error) {
	b.observes++
	b.refs = append(b.refs, r)
	return backend.Status{State: backend.Running}, nil
}
func (b *routeBackend) Cancel(_ context.Context, r backend.Reference) error {
	b.cancels++
	b.refs = append(b.refs, r)
	return nil
}
func (b *routeBackend) Logs(_ context.Context, r backend.Reference, _ int64, _ int) (backend.LogChunk, error) {
	b.logs++
	b.refs = append(b.refs, r)
	return backend.LogChunk{Data: []byte("owned")}, nil
}
func (b *routeBackend) ioCalls() int { return b.starts + b.observes + b.cancels + b.logs }
func routeStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func routeControl(t *testing.T, s *store.Store, targets ...backend.Target) *Control {
	t.Helper()
	c, err := New(s, targets, testEngine)
	if err != nil {
		t.Fatal(err)
	}
	if c.retention.enabled {
		primeStorage(c)
	}
	return c
}

func TestTargetMetadataFrozen(t *testing.T) {
	s := routeStore(t)
	b := &routeBackend{}
	targets := []backend.Target{{Name: "same", Binding: "original", Backend: b, Labels: map[string]string{"pool": "original"}, AcceleratorNames: []string{"z", "a"}}}
	c := routeControl(t, s, targets...)
	targets[0].Labels["pool"] = "changed"
	targets[0].AcceleratorNames[0] = "changed"
	targets[0].Binding = "changed"
	got, err := c.Targets(testActor())
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Labels["pool"] != "original" || got[0].Binding != "original" || !reflect.DeepEqual(got[0].AcceleratorNames, []string{"a", "z"}) {
		t.Fatal("caller changed frozen metadata")
	}
	got[0].Labels["pool"] = "changed"
	got[0].AcceleratorNames[0] = "changed"
	again, err := c.Targets(testActor())
	if err != nil {
		t.Fatal(err)
	}
	if again[0].Labels["pool"] != "original" || again[0].AcceleratorNames[0] != "a" {
		t.Fatal("returned metadata aliases control")
	}
	if _, err := c.Targets(auth.Actor{}); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal(err)
	}
}

func TestPlacementRequirements(t *testing.T) {
	for _, tc := range []struct {
		name              string
		task              backend.Task
		labels            map[string]string
		limits, selectors bool
		want              bool
	}{
		{name: "default", want: true},
		{name: "hard missing", task: backend.Task{Target: "other"}},
		{name: "labels", task: backend.Task{TargetLabels: map[string]string{"pool": "a"}}, labels: map[string]string{"pool": "a"}, want: true},
		{name: "labels mismatch", task: backend.Task{TargetLabels: map[string]string{"pool": "a"}}, labels: map[string]string{"pool": "b"}},
		{name: "hard intersects labels", task: backend.Task{Target: "same", TargetLabels: map[string]string{"pool": "a"}}, labels: map[string]string{"pool": "b"}},
		{name: "empty absent", task: backend.Task{TargetLabels: map[string]string{"pool": ""}}},
		{name: "empty present", task: backend.Task{TargetLabels: map[string]string{"pool": ""}}, labels: map[string]string{"pool": ""}, want: true},
		{name: "limit missing capability", task: backend.Task{CPUMillis: 100, CPULimitMillis: 200}},
		{name: "limit explicit capability", task: backend.Task{CPUMillis: 100, CPULimitMillis: 200}, limits: true, want: true},
		{name: "selector missing capability", task: backend.Task{NodeSelector: map[string]string{"pool": "a"}}},
		{name: "selector explicit capability", task: backend.Task{NodeSelector: map[string]string{"pool": "a"}}, selectors: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := routeStore(t)
			b := &routeBackend{}
			c := routeControl(t, s, backend.Target{Name: "same", Binding: "original", Backend: b, Labels: tc.labels, SupportsResourceLimits: tc.limits, SupportsNodeSelector: tc.selectors})
			tc.task.Argv = []string{"work"}
			name, _, err := c.Plan(testActor(), tc.task)
			if tc.want {
				if err != nil || name != "same" || b.checks != 1 {
					t.Fatalf("plan=%q,%v checks=%d", name, err, b.checks)
				}
			} else {
				if !errors.Is(err, ErrUnsupported) || b.checks != 0 {
					t.Fatalf("unsupported plan=%q,%v checks=%d", name, err, b.checks)
				}
			}
			if b.ioCalls() != 0 {
				t.Fatal("plan caused execution")
			}
		})
	}
}

func TestChecksCannotMutateAcceptedTask(t *testing.T) {
	s := routeStore(t)
	task := backend.Task{Argv: []string{"work"}, Env: map[string]string{"KEY": "value"}, TargetLabels: map[string]string{"pool": "a"}, NodeSelector: map[string]string{"node": "a"}}
	b1 := &routeBackend{check: func(t backend.Task) error {
		t.Argv[0] = "changed"
		t.Env["KEY"] = "changed"
		t.TargetLabels["pool"] = "changed"
		t.NodeSelector["node"] = "changed"
		return nil
	}}
	b2 := &routeBackend{check: func(got backend.Task) error {
		if !reflect.DeepEqual(got, task) {
			t.Fatal("previous backend mutated later check")
		}
		return nil
	}}
	c := routeControl(t, s, backend.Target{Name: "b", Binding: "b", Backend: b2, Labels: map[string]string{"pool": "a"}, SupportsNodeSelector: true}, backend.Target{Name: "a", Binding: "a", Backend: b1, Labels: map[string]string{"pool": "a"}, SupportsNodeSelector: true})
	j, err := c.Submit(testActor(), "one", task)
	if err != nil {
		t.Fatal(err)
	}
	if j.Target != "a" || j.TargetBinding != "a" || !reflect.DeepEqual(j.Task, task) || task.Argv[0] != "work" {
		t.Fatal("accepted input changed")
	}
	b1.check = func(backend.Task) error { return errors.New("actual policy rejected") }
	b2.check = b1.check
	if _, _, err := c.Plan(testActor(), task); !errors.Is(err, ErrUnsupported) {
		t.Fatal("capability bypassed backend policy")
	}
}

func TestReplayRetainsOriginalRoute(t *testing.T) {
	s := routeStore(t)
	task := backend.Task{Argv: []string{"work"}, TargetLabels: map[string]string{"pool": "a"}}
	original := routeControl(t, s, backend.Target{Name: "same", Binding: "original", Backend: &routeBackend{}, Labels: map[string]string{"pool": "a"}})
	j, err := original.Submit(testActor(), "one", task)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"same", "replacement"} {
		b := &routeBackend{check: func(backend.Task) error { return errors.New("policy removed") }}
		replacement := routeControl(t, s, backend.Target{Name: name, Binding: "new", Backend: b, Labels: map[string]string{"pool": "b"}})
		got, err := replacement.Submit(testActor(), "one", task)
		if err != nil || !reflect.DeepEqual(j, got) || b.checks != 0 || b.ioCalls() != 0 {
			t.Fatalf("replay moved execution: %+v %v", got, err)
		}
		changed := cloneTask(task)
		changed.TargetLabels["pool"] = "b"
		if _, err := replacement.Submit(testActor(), "one", changed); !errors.Is(err, store.ErrConflict) {
			t.Fatal(err)
		}
		if _, err := replacement.Submit(testActor(), "fresh", task); !errors.Is(err, ErrUnsupported) {
			t.Fatal(err)
		}
	}
}

func TestBindingFenceAndRestore(t *testing.T) {
	for _, state := range []backend.State{backend.Queued, backend.Starting, backend.Running} {
		for _, cancelRequested := range []bool{false, true} {
			t.Run(string(state)+map[bool]string{false: "", true: " cancel"}[cancelRequested], func(t *testing.T) {
				s := routeStore(t)
				task := backend.Task{Argv: []string{"work"}}
				j, err := s.Submit(testActor(), "one", task, "same", "original")
				if err != nil {
					t.Fatal(err)
				}
				_, err = s.Update(j.ID, func(j *backend.Job) error { j.Status.State = state; j.CancelRequested = cancelRequested; return nil })
				if err != nil {
					t.Fatal(err)
				}
				wrong := &routeBackend{}
				c := routeControl(t, s, backend.Target{Name: "same", Kind: "local", Binding: "changed", Backend: wrong})
				if err := reconcileStep(t, c); err != nil {
					t.Fatal(err)
				}
				got, err := s.Get(j.ID)
				if err != nil {
					t.Fatal(err)
				}
				wantState := state
				wantReason := changedTarget
				if state == backend.Queued && cancelRequested {
					wantState = backend.Canceled
					wantReason = "canceled before execution"
				}
				if got.Status.State != wantState || got.Status.Reason != wantReason || got.TargetBinding != "original" || wrong.ioCalls() != 0 || !c.Healthy() || !c.AuditHealthy() {
					t.Fatalf("wrong-route work: %+v calls=%d", got, wrong.ioCalls())
				}
				if err := reconcileStep(t, c); err != nil {
					t.Fatal(err)
				}
				again, _ := s.Get(j.ID)
				if !again.UpdatedAt.Equal(got.UpdatedAt) {
					t.Fatal("held reason rewrites every pass")
				}
				good := &routeBackend{}
				restored := routeControl(t, s, backend.Target{Name: "same", Binding: "original", Backend: good})
				if err := reconcileStep(t, restored); err != nil {
					t.Fatal(err)
				}
				got, _ = s.Get(j.ID)
				if wantState == backend.Canceled {
					if good.ioCalls() != 0 {
						t.Fatal("canceled queued work dispatched")
					}
					return
				}
				if got.Status.State != backend.Running || got.Status.Reason != "" {
					t.Fatalf("restored route held: %+v", got)
				}
				wantStarts := 0
				if state == backend.Queued {
					wantStarts = 1
				}
				if good.starts != wantStarts {
					t.Fatalf("starts=%d want%d", good.starts, wantStarts)
				}
				wantCancels := 0
				if cancelRequested {
					wantCancels = 1
				}
				if good.cancels != wantCancels {
					t.Fatalf("cancels=%d want%d", good.cancels, wantCancels)
				}
				for _, r := range good.refs {
					if r.ID != j.ID || r.Binding != "original" || !reflect.DeepEqual(r.Task, task) {
						t.Fatal("reference changed")
					}
				}
			})
		}
	}
}

func TestUnboundLocalClaimHasNoAuthority(t *testing.T) {
	s := routeStore(t)
	b := &routeBackend{}
	c := routeControl(t, s, backend.Target{Name: "local", Kind: "local", Binding: "declared", Backend: b})
	target, reason := c.boundTarget(backend.Job{Target: "local", Task: backend.Task{Argv: []string{"work"}}})
	if reason != unboundTarget || target.Backend != nil || b.ioCalls() != 0 {
		t.Fatal("kind/name inferred legacy authority")
	}
}

func TestLogsBindingFenceAndReference(t *testing.T) {
	s := routeStore(t)
	task := backend.Task{Argv: []string{"work"}}
	j, err := s.Submit(testActor(), "one", task, "same", "original")
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []backend.State{backend.Starting, backend.Running, backend.Succeeded} {
		_, err = s.Update(j.ID, func(j *backend.Job) error { j.Status.State = state; return nil })
		if err != nil {
			t.Fatal(err)
		}
		for _, binding := range []string{"changed", "original"} {
			b := &routeBackend{}
			c := routeControl(t, s, backend.Target{Name: "same", Binding: binding, Backend: b})
			chunk, err := c.Logs(context.Background(), testActor(), j.ID, j.ID, 0)
			if binding != "original" {
				if !errors.Is(err, ErrUnavailable) || b.logs != 0 {
					t.Fatal("foreign route read logs")
				}
			} else {
				if err != nil || string(chunk.Data) != "owned" || b.logs != 1 || b.refs[0].Binding != "original" || b.refs[0].ID != j.ID || !reflect.DeepEqual(b.refs[0].Task, task) {
					t.Fatalf("log reference %+v %v", b.refs, err)
				}
			}
		}
	}
}

func TestPortableBounds(t *testing.T) {
	for _, task := range []backend.Task{
		{CPULimitMillis: -1}, {MemoryLimitBytes: -1}, {CPULimitMillis: 1}, {MemoryLimitBytes: 1}, {CPUMillis: 2, CPULimitMillis: 1}, {MemoryBytes: 2, MemoryLimitBytes: 1},
		{TargetLabels: map[string]string{"": "v"}}, {TargetLabels: map[string]string{"x": strings.Repeat("v", 4097)}}, {NodeSelector: map[string]string{"x": "\x00"}}, {NodeSelector: map[string]string{string([]byte{255}): "v"}},
	} {
		task.Argv = []string{"work"}
		if !errors.Is(Validate(task), ErrInvalid) {
			t.Fatalf("invalid task accepted: %+v", task)
		}
	}
	labels := make(map[string]string)
	for i := 0; i < 17; i++ {
		labels[strings.Repeat("x", i+1)] = ""
	}
	if !errors.Is(Validate(backend.Task{Argv: []string{"work"}, TargetLabels: labels}), ErrInvalid) {
		t.Fatal("too many labels")
	}
	if err := Validate(backend.Task{Argv: []string{"work"}, CPUMillis: 1, CPULimitMillis: 1, MemoryBytes: 1, MemoryLimitBytes: 2, TargetLabels: map[string]string{"empty": ""}}); err != nil {
		t.Fatal(err)
	}
	s := routeStore(t)
	for _, binding := range []string{"", strings.Repeat("x", 257), "nul\x00", string([]byte{255})} {
		if _, err := New(s, []backend.Target{{Name: "same", Binding: binding, Backend: &routeBackend{}}}, testEngine); err == nil {
			t.Fatal("invalid binding accepted")
		}
	}
}

func TestFreshRequestUsesNewBindingWithoutMovingReplay(t *testing.T) {
	s := routeStore(t)
	task := backend.Task{Argv: []string{"work"}}
	b := &routeBackend{}
	c := routeControl(t, s, backend.Target{Name: "same", Binding: "original", Backend: b})
	first, err := c.Submit(testActor(), "one", task)
	if err != nil {
		t.Fatal(err)
	}
	replacement := routeControl(t, s, backend.Target{Name: "same", Binding: "new", Backend: b})
	fresh, err := replacement.Submit(testActor(), "two", task)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := replacement.Submit(testActor(), "one", task)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.ID == first.ID || fresh.TargetBinding != "new" || !reflect.DeepEqual(first, replay) {
		t.Fatal("new admission changed original binding")
	}
}

func TestStartCannotMutateLaterReference(t *testing.T) {
	s := routeStore(t)
	task := backend.Task{Argv: []string{"work"}, Env: map[string]string{"KEY": "value"}}
	b := &routeBackend{start: func(t backend.Task) { t.Argv[0] = "changed"; t.Env["KEY"] = "changed" }}
	c := routeControl(t, s, backend.Target{Name: "same", Binding: "original", Backend: b})
	j, err := c.Submit(testActor(), "one", task)
	if err != nil {
		t.Fatal(err)
	}
	if err := reconcileStep(t, c); err != nil {
		t.Fatal(err)
	}
	if len(b.refs) != 1 || !reflect.DeepEqual(b.refs[0].Task, task) {
		t.Fatal("Start mutated later execution reference")
	}
	got, err := s.Get(j.ID)
	if err != nil || !reflect.DeepEqual(got.Task, task) {
		t.Fatal("Start changed accepted task")
	}
}
