package kube

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// gangForeignUID is a valid UID that no gang fixture Pod was created with.
const gangForeignUID = types.UID("b3219392-688c-4e60-8ce1-680a8956a01f")

// gangInput is one call's arguments to gangReleasePlan.
type gangInput struct {
	plans []workerPodPlan
	uids  []types.UID
	got   []*corev1.Pod
}

// gangUID returns the original UID of rank in the gang fixtures.
func gangUID(rank uint32) types.UID {
	return types.UID(fmt.Sprintf("c0ffee00-0000-4000-8000-%012d", rank))
}

// gangPod returns the Pod of p as workerPod builds it, given only the original
// UID and a resourceVersion.
func gangPod(t *testing.T, b *Backend, p workerPodPlan) *corev1.Pod {
	t.Helper()
	pod := mustWorkerPod(t, b, p)
	pod.UID = gangUID(p.Rank)
	pod.ResourceVersion = strconv.FormatUint(uint64(100+p.Rank), 10)
	return pod
}

// gangFixture returns the plans, original UIDs and observed Pods of an
// n-rank gang that has not been released.
func gangFixture(t *testing.T, b *Backend, n uint32, mode workerPodMode) gangInput {
	t.Helper()
	var in gangInput
	for rank := range n {
		p := workerPlan(rank, n, mode)
		in.plans = append(in.plans, p)
		in.uids = append(in.uids, gangUID(rank))
		in.got = append(in.got, gangPod(t, b, p))
	}
	return in
}

func gangCopies(pods []*corev1.Pod) []*corev1.Pod {
	var out []*corev1.Pod
	for _, p := range pods {
		out = append(out, p.DeepCopy())
	}
	return out
}

func gangPlanCopies(plans []workerPodPlan) []workerPodPlan {
	var out []workerPodPlan
	for _, p := range plans {
		p.Task = backend.CloneTask(p.Task)
		p.Registry.PullSecrets = slices.Clone(p.Registry.PullSecrets)
		out = append(out, p)
	}
	return out
}

// checkGangArgs reports whether gangReleasePlan modified its arguments.
func checkGangArgs(t *testing.T, in, before gangInput) {
	t.Helper()
	if !equality.Semantic.DeepEqual(in.got, before.got) {
		t.Error("gangReleasePlan modified got")
	}
	if !equality.Semantic.DeepEqual(in.plans, before.plans) {
		t.Error("gangReleasePlan modified plans")
	}
	if !slices.Equal(in.uids, before.uids) {
		t.Error("gangReleasePlan modified uids")
	}
}

func TestGangReleasePlan(t *testing.T) {
	for _, tt := range []struct {
		name string
		n    uint32
		mode workerPodMode
	}{
		{"tcp 2", 2, workerTCP},
		{"tcp 3", 3, workerTCP},
		{"independent 3", 3, workerIndependent},
		{"tcp 16", 16, workerTCP},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := workerBackend()
			in := gangFixture(t, b, tt.n, tt.mode)
			before := gangInput{gangPlanCopies(in.plans), slices.Clone(in.uids), gangCopies(in.got)}
			out, err := b.gangReleasePlan(in.plans, in.got, in.uids)
			if err != nil {
				t.Fatal(err)
			}
			checkGangArgs(t, in, before)
			if len(out) != int(tt.n) {
				t.Fatalf("got %d updates, want %d", len(out), tt.n)
			}
			for i, c := range out {
				rank := uint32(i)
				if c == in.got[i] {
					t.Fatalf("rank %d: update aliases the observed Pod", i)
				}
				if c.Name != workerPodName(workerAttempt, rank) {
					t.Errorf("rank %d: update is for %s", i, c.Name)
				}
				if c.UID != gangUID(rank) || c.ResourceVersion != in.got[i].ResourceVersion {
					t.Errorf("rank %d: preconditions uid %q resourceVersion %q, want %q %q",
						i, c.UID, c.ResourceVersion, gangUID(rank), in.got[i].ResourceVersion)
				}
				if len(c.Spec.SchedulingGates) != 0 {
					t.Errorf("rank %d: gates %v remain", i, c.Spec.SchedulingGates)
				}
				want := in.got[i].DeepCopy()
				want.Spec.SchedulingGates = nil
				if !equality.Semantic.DeepEqual(c, want) {
					t.Errorf("rank %d: update changes more than the gate", i)
				}
			}
			// The updates are deep copies: changing one leaves got unchanged.
			for _, c := range out {
				c.Labels["example.com/x"] = "1"
				c.Annotations["example.com/x"] = "1"
				c.Spec.Containers[0].Env[0].Value = "changed"
			}
			if !equality.Semantic.DeepEqual(in.got, before.got) {
				t.Fatal("updates share state with got")
			}
		})
	}
}

// TestGangReleaseRefusals applies one deviation to a 3-rank gang. Every
// deviation refuses the whole gang with its typed error.
func TestGangReleaseRefusals(t *testing.T) {
	foreignGate := corev1.PodSchedulingGate{Name: "example.com/hold"}
	for _, tt := range []struct {
		name   string
		mutate func(*testing.T, *Backend, *gangInput)
		want   error
	}{
		{"missing rank", func(_ *testing.T, _ *Backend, in *gangInput) { in.got[1] = nil }, errGangMissing},
		{"missing last rank", func(_ *testing.T, _ *Backend, in *gangInput) { in.got[2] = nil }, errGangMissing},
		{"extra member", func(t *testing.T, b *Backend, in *gangInput) {
			in.got = append(in.got, gangPod(t, b, workerPlan(3, 4, workerTCP)))
		}, errGangShape},
		{"fewer plans", func(_ *testing.T, _ *Backend, in *gangInput) { in.plans = in.plans[:2] }, errGangShape},
		{"fewer uids", func(_ *testing.T, _ *Backend, in *gangInput) { in.uids = in.uids[:2] }, errGangShape},
		{"single member", func(_ *testing.T, _ *Backend, in *gangInput) {
			in.plans, in.uids, in.got = in.plans[:1], in.uids[:1], in.got[:1]
		}, errGangShape},
		{"empty", func(_ *testing.T, _ *Backend, in *gangInput) { *in = gangInput{} }, errGangShape},
		{"ranks out of order", func(_ *testing.T, _ *Backend, in *gangInput) {
			in.plans[0], in.plans[1] = in.plans[1], in.plans[0]
			in.uids[0], in.uids[1] = in.uids[1], in.uids[0]
			in.got[0], in.got[1] = in.got[1], in.got[0]
		}, errGangShape},
		{"count differs", func(_ *testing.T, _ *Backend, in *gangInput) { in.plans[2].Count = 4 }, errGangShape},
		{"attempt differs", func(_ *testing.T, _ *Backend, in *gangInput) {
			in.plans[1].AttemptID = workerJob
		}, errGangShape},
		{"missing resource version", func(_ *testing.T, _ *Backend, in *gangInput) {
			in.got[1].ResourceVersion = ""
		}, errGangShape},
		{"registry uid differs", func(_ *testing.T, _ *Backend, in *gangInput) {
			in.plans[1].Registry.ServiceAccountUID = gangForeignUID
		}, errGangShape},
		{"registry secrets differ", func(t *testing.T, b *Backend, in *gangInput) {
			in.plans[1].Registry.PullSecrets = []string{"another-secret"}
			in.got[1] = gangPod(t, b, in.plans[1])
		}, errGangShape},
		{"mode differs", func(_ *testing.T, _ *Backend, in *gangInput) { in.plans[1].Mode = workerIndependent }, errGangShape},
		// Rank 1 matches its own plan, but that plan is of another group.
		{"group digest differs", func(t *testing.T, b *Backend, in *gangInput) {
			p := in.plans[1]
			p.Task.Env = map[string]string{"A": "1", "B": "3"}
			in.plans[1], in.got[1] = p, gangPod(t, b, p)
		}, errGangShape},
		{"plan differs", func(_ *testing.T, _ *Backend, in *gangInput) {
			in.plans[1].Task.Env = map[string]string{"A": "1", "B": "3"}
		}, backend.ErrIdentityMismatch},
		{"foreign uid", func(_ *testing.T, _ *Backend, in *gangInput) { in.got[2].UID = gangForeignUID }, backend.ErrIdentityMismatch},
		{"original uid differs", func(_ *testing.T, _ *Backend, in *gangInput) { in.uids[0] = gangForeignUID }, backend.ErrIdentityMismatch},
		{"empty uids", func(_ *testing.T, _ *Backend, in *gangInput) {
			in.uids[1], in.got[1].UID = "", ""
		}, backend.ErrIdentityMismatch},
		{"ungated", func(_ *testing.T, _ *Backend, in *gangInput) { in.got[1].Spec.SchedulingGates = nil }, errGangGates},
		{"foreign gate after", func(_ *testing.T, _ *Backend, in *gangInput) {
			in.got[1].Spec.SchedulingGates = append(in.got[1].Spec.SchedulingGates, foreignGate)
		}, errGangGates},
		{"foreign gate before", func(_ *testing.T, _ *Backend, in *gangInput) {
			in.got[1].Spec.SchedulingGates = append([]corev1.PodSchedulingGate{foreignGate}, in.got[1].Spec.SchedulingGates...)
		}, errGangGates},
		{"foreign gate only", func(_ *testing.T, _ *Backend, in *gangInput) {
			in.got[1].Spec.SchedulingGates = []corev1.PodSchedulingGate{foreignGate}
		}, errGangGates},
		{"gate repeated", func(_ *testing.T, _ *Backend, in *gangInput) {
			in.got[1].Spec.SchedulingGates = append(in.got[1].Spec.SchedulingGates, in.got[1].Spec.SchedulingGates[0])
		}, errGangGates},
		{"terminating", func(_ *testing.T, _ *Backend, in *gangInput) {
			now := metav1.Unix(1792612228, 0)
			in.got[2].DeletionTimestamp = &now
		}, errGangTerminating},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := workerBackend()
			in := gangFixture(t, b, 3, workerTCP)
			tt.mutate(t, b, &in)
			before := gangInput{gangPlanCopies(in.plans), slices.Clone(in.uids), gangCopies(in.got)}
			out, err := b.gangReleasePlan(in.plans, in.got, in.uids)
			if out != nil || !errors.Is(err, tt.want) {
				t.Fatalf("gangReleasePlan = %d updates, %v; want none, %v", len(out), err, tt.want)
			}
			checkGangArgs(t, in, before)
		})
	}
}

func TestGangAssembly(t *testing.T) {
	released := time.Unix(1792612228, 0)
	limit := 5 * time.Minute
	unschedulable := func(p *corev1.Pod) {
		p.Status = corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{
			Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable}}}
	}
	for _, tt := range []struct {
		name      string
		bound     []int
		mutate    func(*gangInput)
		after     time.Duration
		want      gangState
		wantRanks []uint32
	}{
		{"none bound", nil, nil, 0, gangAssembling, []uint32{0, 1, 2}},
		{"one bound", []int{1}, nil, time.Minute, gangAssembling, []uint32{0, 2}},
		{"all bound", []int{0, 1, 2}, nil, time.Minute, gangAssembled, nil},
		{"all bound after limit", []int{0, 1, 2}, nil, time.Hour, gangAssembled, nil},
		{"unschedulable", []int{0, 2}, func(in *gangInput) { unschedulable(in.got[1]) }, time.Minute, gangAssembling, []uint32{1}},
		{"unschedulable after limit", []int{0, 2}, func(in *gangInput) { unschedulable(in.got[1]) }, time.Hour, gangAssemblyFailed, []uint32{1}},
		{"at limit", []int{0}, nil, limit, gangAssembling, []uint32{1, 2}},
		{"just after limit", []int{0}, nil, limit + time.Nanosecond, gangAssemblyFailed, []uint32{1, 2}},
		{"clock before release", nil, nil, -time.Hour, gangAssembling, []uint32{0, 1, 2}},
		{"foreign uid", []int{0, 1, 2}, func(in *gangInput) { in.got[2].UID = gangForeignUID }, time.Minute, gangHeld, []uint32{2}},
		{"foreign uid after limit", nil, func(in *gangInput) { in.got[2].UID = gangForeignUID }, time.Hour, gangHeld, []uint32{2}},
		{"missing", []int{1, 2}, func(in *gangInput) { in.got[0] = nil }, time.Minute, gangHeld, []uint32{0}},
		{"missing two", nil, func(in *gangInput) { in.got[2], in.got[0] = nil, nil }, time.Hour, gangHeld, []uint32{0, 2}},
		{"invalid original uid", []int{0, 1, 2}, func(in *gangInput) {
			in.uids[1], in.got[1].UID = "invalid\nuid", "invalid\nuid"
		}, time.Minute, gangHeld, []uint32{1}},
		{"too many ranks", nil, func(in *gangInput) {
			in.got = make([]*corev1.Pod, maxWorkers+1)
			in.uids = make([]types.UID, maxWorkers+1)
		}, time.Minute, gangHeld, nil},
		{"empty uids", []int{0, 1, 2}, func(in *gangInput) { in.uids[1], in.got[1].UID = "", "" }, time.Minute, gangHeld, []uint32{1}},
		{"fewer pods", []int{0, 1}, func(in *gangInput) { in.got = in.got[:2] }, time.Minute, gangHeld, nil},
		{"single member", []int{0}, func(in *gangInput) { in.got, in.uids = in.got[:1], in.uids[:1] }, time.Minute, gangHeld, nil},
		{"empty", nil, func(in *gangInput) { in.got, in.uids = nil, nil }, time.Minute, gangHeld, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			in := gangFixture(t, workerBackend(), 3, workerTCP)
			for _, rank := range tt.bound {
				in.got[rank].Spec.NodeName = "node-" + strconv.Itoa(rank)
			}
			if tt.mutate != nil {
				tt.mutate(&in)
			}
			state, ranks := gangAssembly(in.got, in.uids, released, released.Add(tt.after), limit)
			if state != tt.want || !slices.Equal(ranks, tt.wantRanks) {
				t.Fatalf("gangAssembly = %d %v, want %d %v", state, ranks, tt.want, tt.wantRanks)
			}
		})
	}
}
