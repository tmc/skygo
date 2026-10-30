package kube

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
	"github.com/tmc/skygo/internal/workeradmission"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

// Native facts below are MODELED. Every producer request uses the actual
// worker-only client and transport against the loopback API model; neither
// a fake clientset nor a replacement HTTP/ownership algorithm is used.
var (
	workerT0 = metav1.Unix(1792612228, 0)
	workerT1 = metav1.Unix(1792612230, 0)
	workerT2 = metav1.Unix(1792612290, 0)
)

const workerHelperSHA256 = "3333333333333333333333333333333333333333333333333333333333333333"

func newWorkerTestRuntime(t *testing.T) (*workerRuntime, *workerAPI) {
	t.Helper()
	return newWorkerHTTPRuntimeIn(t, workerHTTPJSON, nil, "jobs")
}
func workerTestGroup(t *testing.T, rt *workerRuntime, mode backend.WorkerMode, n uint8) backend.WorkerGroup {
	t.Helper()
	task := workerTask()
	raw, err := backend.TaskBytes(task)
	if err != nil {
		t.Fatal(err)
	}
	return backend.WorkerGroup{Execution: backend.Reference{ID: workerJob, Task: task, Binding: rt.b.binding}, AttemptID: workerAttempt, TaskDigest: sha256.Sum256(raw), Spec: backend.WorkerSpec{Count: n, Mode: mode}}
}
func workerCreateAll(t *testing.T, rt *workerRuntime, g backend.WorkerGroup) []backend.WorkerReference {
	t.Helper()
	refs := make([]backend.WorkerReference, g.Spec.Count)
	var svc backend.WorkerResource
	for rank := range g.Spec.Count {
		got, err := rt.CreateWorker(t.Context(), backend.WorkerCreate{Group: g, Rank: rank})
		if err != nil {
			t.Fatal(err)
		}
		if rank == 0 {
			svc = got.Service
		}
		refs[rank] = backend.WorkerReference{Group: g, Rank: rank, Pod: got.Pod, Service: svc}
	}
	return refs
}
func workerToken(t *testing.T, r backend.WorkerReference) [32]byte {
	t.Helper()
	v, err := backend.WorkerStartToken(r)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func workerEdit(a *workerAPI, name string, fn func(*corev1.Pod)) { a.editPod(name, fn) }
func workerStored(t *testing.T, a *workerAPI, name string) *corev1.Pod {
	t.Helper()
	o := a.get("pods", name)
	if o == nil {
		t.Fatal("missing modeled worker pod")
	}
	return o.(*corev1.Pod)
}
func mustWorkerTestPod(t *testing.T, rt *workerRuntime, g backend.WorkerGroup, rank uint8) *corev1.Pod {
	t.Helper()
	m, err := rt.plan(g, rank)
	if err != nil {
		t.Fatal(err)
	}
	return m.want
}
func workerPrepare(p *corev1.Pod) {
	id := "containerd://" + string(p.UID)
	p.Spec.NodeName = "node-a"
	p.Status.Phase = corev1.PodRunning
	p.Status.PodIP = workerSliceIP
	p.Status.PodIPs = []corev1.PodIP{{IP: workerSliceIP}}
	p.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "helper", ContainerID: id + "-helper", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ContainerID: id + "-helper", StartedAt: workerT0, FinishedAt: workerT0}}}}
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "task", ContainerID: id + "-task", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: workerT1}}}}
}
func workerExited(code int32) func(*corev1.Pod) {
	return func(p *corev1.Pod) {
		workerPrepare(p)
		p.Status.Phase = corev1.PodSucceeded
		if code != 0 {
			p.Status.Phase = corev1.PodFailed
		}
		s := &p.Status.ContainerStatuses[0]
		s.Ready = false
		s.State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ContainerID: s.ContainerID, ExitCode: code, StartedAt: workerT1, FinishedAt: workerT2}}
	}
}
func workerBarrierServerPod(want *corev1.Pod) *corev1.Pod {
	got := workerServerPod(want)
	for i := range got.Spec.InitContainers {
		got.Spec.InitContainers[i].TerminationMessagePath = corev1.TerminationMessagePathDefault
		got.Spec.InitContainers[i].TerminationMessagePolicy = corev1.TerminationMessageReadFile
	}
	return got
}

func TestNewWorkerRuntime(t *testing.T) {
	makeTarget := func() backend.Target {
		b := workerBackend()
		b.data = &dataProfile{helperImage: profileHelp, helperSHA256: workerHelperSHA256}
		return backend.Target{Backend: b, Binding: b.binding}
	}
	cfg := &rest.Config{Host: "https://127.0.0.1"}
	for _, tt := range []struct {
		name string
		edit func(*backend.Target, *workerRegistry)
		ok   bool
	}{
		{"static configuration", func(*backend.Target, *workerRegistry) {}, true},
		{"foreign binding", func(t *backend.Target, _ *workerRegistry) { t.Binding = "other" }, false},
		{"missing helper", func(t *backend.Target, _ *workerRegistry) { t.Backend.(*Backend).data = nil }, false},
		{"invalid registry", func(_ *backend.Target, r *workerRegistry) { r.ServiceAccountUID = "" }, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			target := makeTarget()
			r := workerRegistrySnapshot()
			tt.edit(&target, &r)
			rt, err := newWorkerRuntime(target, r, cfg)
			if (err == nil) != tt.ok {
				t.Fatalf("constructor error %v", err)
			}
			if !tt.ok {
				return
			}
			r.PullSecrets[0] = "changed"
			if rt.registry.PullSecrets[0] == "changed" {
				t.Fatal("registry aliases caller")
			}
		})
	}
}

func TestWorkerBarrierPod(t *testing.T) {
	rt, _ := newWorkerTestRuntime(t)
	for _, mode := range []workerPodMode{workerIndependent, workerTCP} {
		for rank := range uint32(2) {
			p := workerPlan(rank, 2, mode)
			before := backend.CloneTask(p.Task)
			got, err := rt.pod(p)
			if err != nil {
				t.Fatal(err)
			}
			again, _ := rt.pod(p)
			if !reflect.DeepEqual(got, again) || !reflect.DeepEqual(before, p.Task) {
				t.Fatal("builder not independent/deterministic")
			}
			if got.Annotations[workerProfileAnnotation] != workerBarrierProfile || !slices.Equal(got.Spec.Containers[0].Command, append([]string{helperCopy, "worker", "--"}, p.Task.Argv...)) {
				t.Fatal("helper protocol differs")
			}
			if len(got.Spec.InitContainers) != 1 || got.Spec.InitContainers[0].Command[1] != "install" || got.Spec.Containers[0].ReadinessProbe.Exec.Command[1] != "worker-ready" {
				t.Fatal("missing helper/readiness contract")
			}
			if _, ok := got.Annotations[workerStartAnnotation]; ok {
				t.Fatal("new pod already authorized")
			}
		}
	}
	for _, key := range []string{workerEnvMode, workerEnvTaskSHA256, workerEnvPodUID} {
		p := workerPlan(0, 2, workerIndependent)
		p.Task.Env[key] = "foreign"
		if _, err := rt.pod(p); err == nil {
			t.Fatal("accepted reserved worker env", key)
		}
	}
}

func TestWorkerRuntimeBarrier(t *testing.T) {
	rt, a := newWorkerTestRuntime(t)
	g := workerTestGroup(t, rt, backend.WorkerTCP, 2)
	refs := workerCreateAll(t, rt, g)
	observations := func() []backend.WorkerBarrier {
		t.Helper()
		var out []backend.WorkerBarrier
		for _, r := range refs {
			o, err := rt.ObserveBarrier(t.Context(), r)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, o)
		}
		return out
	}
	if backend.WorkerBarrierReady(refs, observations()) == nil {
		t.Fatal("gated group ready")
	}
	for _, r := range refs {
		if _, err := rt.ReleaseWorker(t.Context(), r); err != nil {
			t.Fatal(err)
		}
	}
	workerEdit(a, refs[0].Pod.Name, workerPrepare)
	a.putObject(workerSlice(refs[0].Service, types.UID(refs[0].Pod.UID), true))
	if backend.WorkerBarrierReady(refs, observations()) == nil {
		t.Fatal("partially prepared group ready")
	}
	if _, err := rt.StartWorker(t.Context(), refs[1], workerToken(t, refs[1])); err == nil {
		t.Fatal("unprepared member started")
	}
	workerEdit(a, refs[1].Pod.Name, workerPrepare)
	if err := backend.WorkerBarrierReady(refs, observations()); err != nil {
		t.Fatal(err)
	}
	for _, r := range refs {
		tok := workerToken(t, r)
		before := workerStored(t, a, r.Pod.Name)
		got, err := rt.StartWorker(t.Context(), r, tok)
		if err != nil || !got.Acknowledged {
			t.Fatal("start", got, err)
		}
		after := workerStored(t, a, r.Pod.Name)
		if after.UID != before.UID || after.Annotations[workerStartAnnotation] != hex.EncodeToString(tok[:]) {
			t.Fatal("token or uid differs")
		}
		mark := a.mark()
		if _, err := rt.StartWorker(t.Context(), r, tok); err != nil {
			t.Fatal(err)
		}
		if len(a.since(mark, "put", "pods")) != 0 {
			t.Fatal("same token delivery rewrote pod")
		}
	}
}

func TestWorkerRuntimeCancelAndRetire(t *testing.T) {
	for _, bound := range []bool{false, true} {
		t.Run(map[bool]string{false: "unbound", true: "bound"}[bound], func(t *testing.T) {
			rt, a := newWorkerTestRuntime(t)
			ref := workerCreateAll(t, rt, workerTestGroup(t, rt, backend.WorkerIndependent, 2))[0]
			if bound {
				if _, err := rt.ReleaseWorker(t.Context(), ref); err != nil {
					t.Fatal(err)
				}
				workerEdit(a, ref.Pod.Name, workerPrepare)
			}
			got, err := rt.CancelWorker(t.Context(), ref)
			if err != nil || !got.Acknowledged {
				t.Fatal("cancel", got, err)
			}
			o, err := rt.ObserveBarrier(t.Context(), ref)
			if err != nil {
				t.Fatal(err)
			}
			if bound {
				if o.Stop != backend.WorkerStopUnproven {
					t.Fatal("cancel acknowledgment became stop")
				}
				if _, err := rt.RemoveWorker(t.Context(), ref); !errors.Is(err, errWorkerStopUnproven) {
					t.Fatal("removed before stop", err)
				}
				workerEdit(a, ref.Pod.Name, workerExited(143))
				o, err = rt.ObserveBarrier(t.Context(), ref)
				if err != nil || o.Stop != backend.WorkerTerminated || o.Started {
					t.Fatal("stop", o, err)
				}
			} else if o.Stop != backend.WorkerNeverBound {
				t.Fatal("no never-bound fact", o)
			}
			rm, err := rt.RemoveWorker(t.Context(), ref)
			if err != nil || !rm.PodRemoved || rm.ServiceRemoved {
				t.Fatal("retirement", rm, err)
			}
			if a.get("pods", ref.Pod.Name) != nil {
				t.Fatal("pod remains")
			}
		})
	}
}

func TestWorkerRuntimeNativeUnboundRetirementDispatch(t *testing.T) {
	rt, a := newWorkerTestRuntime(t)
	ref := workerCreateAll(t, rt, workerTestGroup(t, rt, backend.WorkerIndependent, 2))[0]
	workerEdit(a, ref.Pod.Name, workerExited(0))
	rm, err := rt.RemoveWorker(t.Context(), ref)
	if err != nil || !rm.PodRemoved || !rm.PodDispatched || rm.ServiceDispatched {
		t.Fatal("acknowledged original delete", rm, err)
	}
	// No trace/intent/cancel acknowledgment set that bit; this call sent its
	// own UID+RV DELETE and consumed a typed successful, positively closed ack.
	if len(a.since(0, "delete", "pods")) != 1 {
		t.Fatal("not exactly one original delete")
	}
}

func TestWorkerRuntimeInvalidRecoverySendsNothing(t *testing.T) {
	rt, a := newWorkerTestRuntime(t)
	ref := workerCreateAll(t, rt, workerTestGroup(t, rt, backend.WorkerIndependent, 2))[0]
	for _, r := range []backend.WorkerRecovery{{Ref: ref}, {Ref: ref, Evidence: backend.WorkerTerminated}, {Ref: ref, Evidence: backend.WorkerNeverReleased, RemoveSent: true}} {
		mark := a.mark()
		if got, err := rt.RecoverWorkerRemoval(t.Context(), r); err == nil || got.PodRemoved {
			t.Fatal("invalid recovery", got, err)
		}
		if len(a.since(mark)) != 0 {
			t.Fatal("invalid recovery sent native requests")
		}
	}
}

func TestWorkerRuntimeTrustedNoPodRecovery(t *testing.T) {
	rt, a := newWorkerTestRuntime(t)
	g := workerTestGroup(t, rt, backend.WorkerTCP, 2)
	a.fail("pods", workerFault{status: 403})
	created, err := rt.CreateWorker(t.Context(), backend.WorkerCreate{Group: g, Rank: 0})
	if !errors.Is(err, backend.ErrWorkerCreateRefused) || created.Pod != (backend.WorkerResource{}) || created.Service.UID == "" {
		t.Fatalf("original NoPod refusal: %+v %v", created, err)
	}
	ref := backend.WorkerReference{Group: g, Service: created.Service}
	a.reply("GET services/"+ref.Service.Name, workerReply{n: 2, status: 503})
	partial, err := rt.RemoveWorker(t.Context(), ref)
	if err == nil || !partial.ServiceDispatched || partial.ServiceRemoved || partial.PodRemoved {
		t.Fatalf("original acknowledged service DELETE: %+v %v", partial, err)
	}
	// MODELED trusted Control/Store attestation: the actual original refusal
	// above proved no Pod; both HadPod flags are false, no Release exists and
	// the same original Service DELETE was acknowledged with clean closure.
	got, err := rt.RecoverWorkerRemoval(t.Context(), backend.WorkerRecovery{Ref: ref, Evidence: backend.WorkerNeverReleased, RemoveSent: true})
	if err != nil || !got.ServiceRemoved || got.PodRemoved || got.ServiceDispatched {
		t.Fatal("qualified no-pod recovery", got, err)
	}
}

func TestWorkerRuntimeMarkerPrecedence(t *testing.T) {
	owner := errors.Join(backend.ErrWorkerUnsettled, apierrors.NewForbidden(corev1.Resource("pods"), "p", errors.New("MODELED owner")))
	if errors.Is(workerCreateError(owner), backend.ErrWorkerCreateRefused) || !errors.Is(workerCreateError(owner), backend.ErrWorkerUnsettled) {
		t.Fatal("unsettled create classified refused")
	}
	missing := errors.Join(backend.ErrWorkerUnsettled, apierrors.NewNotFound(corev1.Resource("pods"), "p"))
	if errors.Is(workerVanished(missing), backend.ErrWorkerVanished) || !errors.Is(workerVanished(missing), backend.ErrWorkerUnsettled) {
		t.Fatal("unsettled read classified vanished")
	}
}

func TestWorkerStop(t *testing.T) {
	now := metav1.Now()
	for _, tt := range []struct {
		name string
		edit func(*corev1.Pod)
		stop backend.WorkerStop
		exit bool
	}{
		{"pending", func(*corev1.Pod) {}, backend.WorkerStopUnproven, false},
		{"deleted unbound", func(p *corev1.Pod) { p.DeletionTimestamp = &now }, backend.WorkerNeverBound, false},
		{"running", workerPrepare, backend.WorkerStopUnproven, false},
		{"native exit", workerExited(0), backend.WorkerTerminated, true},
		{"restarted", func(p *corev1.Pod) { workerExited(0)(p); p.Status.ContainerStatuses[0].RestartCount = 1 }, backend.WorkerStopUnproven, false},
		{"no container facts", func(p *corev1.Pod) { p.Status.Phase = corev1.PodFailed }, backend.WorkerStopUnproven, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: workerUID}}
			tt.edit(p)
			got, exit := workerStop(p)
			if got != tt.stop || (exit != nil) != tt.exit {
				t.Fatal("stop", got, exit)
			}
		})
	}
}

func TestWorkerRuntimeIdentityAndStartRefusals(t *testing.T) {
	rt, a := newWorkerTestRuntime(t)
	ref := workerCreateAll(t, rt, workerTestGroup(t, rt, backend.WorkerIndependent, 2))[0]
	workerEdit(a, ref.Pod.Name, func(p *corev1.Pod) { p.Spec.SchedulingGates = nil; workerPrepare(p) })
	token := workerToken(t, ref)
	wrong := token
	wrong[0] ^= 1
	mark := a.mark()
	if _, err := rt.StartWorker(t.Context(), ref, wrong); err == nil {
		t.Fatal("foreign token accepted")
	}
	if len(a.since(mark)) != 0 {
		t.Fatal("foreign token made a request")
	}
	workerEdit(a, ref.Pod.Name, func(p *corev1.Pod) { p.UID = "replacement" })
	if _, err := rt.ReleaseWorker(t.Context(), ref); !errors.Is(err, backend.ErrIdentityMismatch) || !errors.Is(err, backend.ErrWorkerVanished) {
		t.Fatal("replacement not fenced", err)
	}
	if o, err := rt.ObserveWorker(t.Context(), ref); err != nil || o.Status.State != backend.Unknown || o.Exit != nil {
		t.Fatal("vanished projected native stop", o, err)
	}
}

func TestWorkerRuntimeLogsWithoutTask(t *testing.T) {
	rt, _ := newWorkerTestRuntime(t)
	ref := workerCreateAll(t, rt, workerTestGroup(t, rt, backend.WorkerIndependent, 2))[0]
	if c, err := rt.WorkerLogs(context.Background(), ref, 0, 10); err != nil || len(c.Data) != 0 || c.Complete {
		t.Fatal("gated logs", c, err)
	}
	if _, err := rt.WorkerLogs(t.Context(), ref, 1, 10); err == nil || !strings.Contains(err.Error(), "offset") {
		t.Fatal("bad offset", err)
	}
}

// workerControlBackend exposes only the already-implemented interfaces to the
// real Control in tests. It is never a production registration or capability.
type workerControlBackend struct {
	*Backend
	backend.WorkerBarrierBackend
}

// TestWorkerCapacityRefusalSettles composes the actual file Store, scoped auth
// Engine, Control sampler/reducer and worker producer. The HTTP API and its
// capacity occupants are MODELED; no native workload or cluster is launched.
func TestWorkerCapacityRefusalSettles(t *testing.T) {
	rt, api := newWorkerTestRuntime(t)
	rt.http.mu.Lock()
	for i := 0; i < maxRetryReceipts; i++ {
		rt.http.members["worker/reserved/"+strconv.Itoa(i)] = false
	}
	rt.http.mu.Unlock()
	dir := t.TempDir()
	state := filepath.Join(dir, "state")
	info, err := store.EnableRetention(state, store.DefaultRetentionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(state)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	config, token := filepath.Join(dir, "auth.json"), filepath.Join(dir, "token")
	removeAuth, err := auth.Initialize(config, token, s.Owner())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := removeAuth(); err != nil {
			t.Error(err)
		}
	}()
	if err = s.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	engine, err := auth.NewFile(config, s.Owner())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(token)
	if err != nil {
		t.Fatal(err)
	}
	actor, err := engine.Authenticate("Bearer " + strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	authCtx, stopAuth := context.WithCancel(t.Context())
	authDone := make(chan struct{})
	go func() { defer close(authDone); engine.Run(authCtx) }()
	defer func() { stopAuth(); <-authDone }()
	target := backend.Target{Name: "workers", Binding: rt.b.binding, Kind: "modeled", Backend: workerControlBackend{rt.b, rt}, OutputStorage: backend.RemoteOutput, SupportsResourceLimits: true, SupportsNodeSelector: true}
	c, err := control.New(s, []backend.Target{target}, engine)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stopRun := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(runCtx) }()
	defer func() {
		stopRun()
		select {
		case err := <-runDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("original Control.Run did not join")
		}
	}()
	until := time.Now().Add(5 * time.Second)
	for !c.StorageReady() {
		if time.Now().After(until) {
			t.Fatal("actual Control sampler never ready")
		}
		time.Sleep(time.Millisecond)
	}
	w, err := control.NewWorkers(c, control.WorkerConfig{Budgets: map[string]workeradmission.Budget{"workers": {Ranks: 4, CPUMillis: 4000, MemoryBytes: 8 << 30}}})
	if err != nil {
		t.Fatal(err)
	}
	task := workerTask()
	task.Target = "workers"
	mark := api.mark()
	job, _, err := w.Submit(actor, info.Domain, "capacity", task, backend.WorkerSpec{Count: 2, Mode: backend.WorkerIndependent})
	if err != nil {
		t.Fatal(err)
	}
	var group store.WorkerGroup
	for i := 0; i < 32; i++ {
		if err := w.Step(t.Context()); err != nil {
			t.Fatal(err)
		}
		group, err = s.WorkerGroup(job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if group.Phase == store.WorkerGroupDone {
			break
		}
	}
	if group.Phase != store.WorkerGroupDone || group.Reserved || group.Job.Status.State != backend.Failed {
		t.Fatalf("capacity refusal did not settle and refund: %+v", group)
	}
	refused := 0
	for _, m := range group.Members {
		if m.Barrier.CreateRefused {
			refused++
		}
		if m.Pod != (backend.WorkerResource{}) || m.Barrier.LocalUnsettled {
			t.Fatalf("preflight invented effect/owner: %+v", m)
		}
	}
	if refused == 0 {
		t.Fatal("no original refusal retained")
	}
	before := group.Revision
	replay, _, err := w.Submit(actor, info.Domain, "capacity", task, backend.WorkerSpec{Count: 2, Mode: backend.WorkerIndependent})
	if err != nil || replay.ID != job.ID {
		t.Fatalf("replay = %+v, %v", replay, err)
	}
	if err := w.Step(t.Context()); err != nil {
		t.Fatal(err)
	}
	after, err := s.WorkerGroup(job.ID)
	if err != nil || after.Revision != before {
		t.Fatalf("settled replay changed original: %+v, %v", after, err)
	}
	if calls := api.since(mark); len(calls) != 0 {
		t.Fatalf("preflight/refund/replay sent native calls: %v", calls)
	}
}
