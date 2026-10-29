package kube

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func draRecordOf(t *testing.T, b *Backend, id string) draRecord {
	t.Helper()
	r, err := b.dra.store.load(id)
	if err != nil && !errors.Is(err, errReceiptRefused) {
		t.Fatal(err)
	}
	return r
}

// requestIndex returns the index of the first request equal to want at or
// after from, or -1.
func (a *draAPI) requestIndex(want string, from int) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	for i := from; i < len(a.requests); i++ {
		if a.requests[i] == want {
			return i
		}
	}
	return -1
}

func TestDRAStartClaimFirst(t *testing.T) {
	f := newDRAFixture(t)
	_, b := f.integration(t)
	ctx := context.Background()
	task := draTask(2)
	if err := b.Start(ctx, testID, task); err != nil {
		t.Fatal(err)
	}
	if f.api.count("resourceclaims") != 1 || f.api.count("pods") != 1 {
		t.Fatalf("creates: %d claims, %d pods", f.api.count("resourceclaims"), f.api.count("pods"))
	}
	requests := f.api.log()
	claimPost := slices.Index(requests, "POST "+draClaimsPath)
	podPost := slices.Index(requests, "POST "+podsPath)
	if claimPost < 0 || podPost < claimPost {
		t.Fatalf("pod create before claim create: %v", requests)
	}
	// The namespace and class are read again before each create, after the
	// previous one.
	for _, post := range []int{claimPost, podPost} {
		last := func(want string) int {
			for i := post - 1; i >= 0; i-- {
				if requests[i] == want {
					return i
				}
			}
			return -1
		}
		ns := last("GET /api/v1/namespaces/workloads")
		class := last("GET " + draClassesPath + "/gpu.example.com")
		prev := slices.Index(requests, "GET /apis/resource.k8s.io/v1") // construction discovery
		if post == podPost {
			prev = claimPost
		}
		if ns < prev || class < prev {
			t.Fatalf("no recheck before request %d: %v", post, requests)
		}
	}
	r := draRecordOf(t, b, testID)
	if r.Claim.Send != phasePinned || r.Pod.Send != phasePinned || r.Count != 2 {
		t.Fatalf("record = %+v", r)
	}
	var claimRef string
	f.api.pod(testID, func(p *corev1.Pod) { claimRef = *p.Spec.ResourceClaims[0].ResourceClaimName })
	if claimRef != r.Claim.Name {
		t.Fatalf("pod references %q, claim is %q", claimRef, r.Claim.Name)
	}
	ref := draRef(b, testID, task)
	if s, err := b.Observe(ctx, ref); err != nil || s.State != backend.Starting || s.Instance != r.Pod.UID {
		t.Fatalf("pending: %+v %v", s, err)
	}
	f.api.pod(testID, func(p *corev1.Pod) { p.Status.Phase = corev1.PodRunning })
	if s, err := b.Observe(ctx, ref); err != nil || s.State != backend.Running {
		t.Fatalf("running: %+v %v", s, err)
	}
	f.api.succeed(testID)
	s, err := b.Observe(ctx, ref)
	if err != nil || s.State != backend.Succeeded || s.ExitCode == nil || *s.ExitCode != 0 {
		t.Fatalf("succeeded: %+v %v", s, err)
	}
	if r := draRecordOf(t, b, testID); r.Execution != draExecSucceeded || r.Hold != "" || r.Pod.Delete != "" {
		t.Fatalf("witness = %+v", r)
	}
	chunk, err := b.Logs(ctx, ref, 0, 100)
	if err != nil || string(chunk.Data) != "output\n" || !chunk.Complete {
		t.Fatalf("logs: %+v %v", chunk, err)
	}
	// Observation never creates or deletes.
	if f.api.count("resourceclaims") != 1 || f.api.count("pods") != 1 || len(f.api.deleted()) != 0 {
		t.Fatal("observation changed native objects")
	}
}

func TestDRARefusedBeforeEffects(t *testing.T) {
	f := newDRAFixture(t)
	_, b := f.integration(t)
	retry := draTask(1)
	retry.Retry = &backend.RetryPolicy{MaxAttempts: 2}
	if p, err := backend.NormalizeRetry(retry.Retry); err == nil && p != nil {
		retry.Retry = p
	}
	outputs := draTask(1)
	outputs.Outputs = []string{"out"}
	before := len(f.api.log())
	for name, task := range map[string]backend.Task{"retry": retry, "outputs": outputs, "too many": draTask(9)} {
		if err := b.Start(context.Background(), testID, task); err == nil {
			t.Errorf("%s: started", name)
		}
	}
	if after := f.api.log(); len(after) != before {
		t.Fatalf("refusal sent requests: %v", after[before:])
	}
	if ids, err := b.dra.store.list(); err != nil || len(ids) != 0 {
		t.Fatalf("refusal recorded: %v %v", ids, err)
	}
}

func TestDRALostClaimReply(t *testing.T) {
	f := newDRAFixture(t)
	_, b := f.integration(t)
	ctx := context.Background()
	f.api.set(func() { f.api.drop["resourceclaims"] = true })
	if err := b.Start(ctx, testID, draTask(1)); err == nil {
		t.Fatal("lost reply reported success")
	}
	r := draRecordOf(t, b, testID)
	if r.Claim.Send != phaseSent || r.Pod.Send != draWithdrawn || f.api.count("pods") != 0 {
		t.Fatalf("record = %+v, pods %d", r, f.api.count("pods"))
	}
	// A restarted process recovers only the exact declared claim, and never
	// sends the Pod or another claim.
	f.api.set(func() { f.api.drop["resourceclaims"] = false })
	_, b2 := f.integration(t)
	s, err := b2.Observe(ctx, draRef(b2, testID, draTask(1)))
	if err != nil || s.State != backend.Failed || !strings.Contains(s.Reason, "never created") {
		t.Fatalf("status = %+v %v", s, err)
	}
	if r := draRecordOf(t, b2, testID); r.Claim.Send != phasePinned || r.Hold != "" {
		t.Fatalf("recovered = %+v", r)
	}
	if err := b2.Start(ctx, testID, draTask(1)); !errors.Is(err, errReceiptExists) {
		t.Fatalf("second start: %v", err)
	}
	if f.api.count("resourceclaims") != 1 || f.api.count("pods") != 0 {
		t.Fatal("resent after a lost reply")
	}
}

func TestDRALostPodReply(t *testing.T) {
	f := newDRAFixture(t)
	_, b := f.integration(t)
	ctx := context.Background()
	f.api.set(func() { f.api.drop["pods"] = true })
	if err := b.Start(ctx, testID, draTask(1)); err == nil {
		t.Fatal("lost reply reported success")
	}
	if r := draRecordOf(t, b, testID); r.Pod.Send != phaseSent {
		t.Fatalf("record = %+v", r)
	}
	_, b2 := f.integration(t)
	s, err := b2.Observe(ctx, draRef(b2, testID, draTask(1)))
	if err != nil || s.State != backend.Starting || s.Instance == "" {
		t.Fatalf("status = %+v %v", s, err)
	}
	if f.api.count("resourceclaims") != 1 || f.api.count("pods") != 1 {
		t.Fatal("resent after a lost reply")
	}
}

func TestDRAResponseRefusals(t *testing.T) {
	for _, tc := range []struct {
		name     string
		resource string
		set      func(a *draAPI)
	}{
		{"claim 200", "resourceclaims", func(a *draAPI) { a.notCreated["resourceclaims"] = true }},
		{"claim redirect", "resourceclaims", func(a *draAPI) { a.redirect["resourceclaims"] = true }},
		{"claim unknown field", "resourceclaims", func(a *draAPI) {
			a.reply["resourceclaims"] = func(m map[string]any) { m["spec"].(map[string]any)["future"] = true }
		}},
		{"claim other nonce", "resourceclaims", func(a *draAPI) {
			a.reply["resourceclaims"] = func(m map[string]any) {
				m["metadata"].(map[string]any)["annotations"].(map[string]any)[nonceAnnotation] = strings.Repeat("0", 64)
			}
		}},
		{"claim admission mutation", "resourceclaims", func(a *draAPI) {
			a.admit["resourceclaims"] = func(o any) {
				o.(*resourcev1.ResourceClaim).Spec.Devices.Requests[0].Exactly.AdminAccess = new(bool)
			}
		}},
		{"claim status in response", "resourceclaims", func(a *draAPI) {
			a.admit["resourceclaims"] = func(o any) {
				o.(*resourcev1.ResourceClaim).Status.ReservedFor = []resourcev1.ResourceClaimConsumerReference{{Resource: "pods", Name: "x", UID: "x"}}
			}
		}},
		{"pod 200", "pods", func(a *draAPI) { a.notCreated["pods"] = true }},
		{"pod redirect", "pods", func(a *draAPI) { a.redirect["pods"] = true }},
		{"pod pull secret", "pods", func(a *draAPI) {
			a.admit["pods"] = func(o any) { o.(*corev1.Pod).Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "x"}} }
		}},
		{"pod node name", "pods", func(a *draAPI) { a.admit["pods"] = func(o any) { o.(*corev1.Pod).Spec.NodeName = "node-a" } }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDRAFixture(t)
			_, b := f.integration(t)
			f.api.set(func() { tc.set(f.api) })
			if err := b.Start(context.Background(), testID, draTask(1)); err == nil {
				t.Fatal("refused response reported success")
			}
			o := draRecordOf(t, b, testID).Claim
			if tc.resource == "pods" {
				o = draRecordOf(t, b, testID).Pod
			}
			if o.Send != phaseRefused || o.Witness == nil {
				t.Fatalf("object = %+v", o)
			}
			if tc.resource == "resourceclaims" && f.api.count("pods") != 0 {
				t.Fatal("pod sent after a refused claim response")
			}
			// Neither this nor a restarted process adopts, resends or deletes.
			_, b2 := f.integration(t)
			if _, err := b2.Observe(context.Background(), draRef(b2, testID, draTask(1))); err != nil {
				t.Fatal(err)
			}
			if f.api.count(tc.resource) != 1 || len(f.api.deleted()) != 0 {
				t.Fatal("refused object resent or deleted")
			}
			if o := draRecordOf(t, b2, testID); o.Claim.Send == phasePinned && tc.resource == "resourceclaims" {
				t.Fatal("refused claim adopted")
			}
		})
	}
}

func TestDRADefiniteRejection(t *testing.T) {
	f := newDRAFixture(t)
	_, b := f.integration(t)
	f.api.set(func() {
		f.api.failCode["resourceclaims"], f.api.failReason["resourceclaims"] = 403, metav1.StatusReasonForbidden
	})
	if err := b.Start(context.Background(), testID, draTask(1)); err == nil {
		t.Fatal("rejection reported success")
	}
	if r := draRecordOf(t, b, testID); r.Claim.Send != phaseRejected || r.Pod.Send != draWithdrawn || f.api.count("pods") != 0 {
		t.Fatalf("record = %+v", r)
	}
	s, err := b.Observe(context.Background(), draRef(b, testID, draTask(1)))
	if err != nil || s.State != backend.Failed {
		t.Fatalf("status = %+v %v", s, err)
	}
}

func TestDRADrift(t *testing.T) {
	for name, drift := range map[string]func(a *draAPI){
		"namespace uid": func(a *draAPI) { a.nsUID = "ns-replaced" },
		"class uid":     func(a *draAPI) { a.classes["gpu.example.com"].UID = "class-replaced" },
		"class spec": func(a *draAPI) {
			a.classes["gpu.example.com"].Spec.Selectors = append(a.classes["gpu.example.com"].Spec.Selectors, a.classes["gpu.example.com"].Spec.Selectors[0])
		},
		"class missing": func(a *draAPI) { delete(a.classes, "gpu.example.com") },
	} {
		t.Run(name, func(t *testing.T) {
			f := newDRAFixture(t)
			_, b := f.integration(t)
			f.api.mu.Lock()
			drift(f.api)
			f.api.mu.Unlock()
			if err := b.Start(context.Background(), testID, draTask(1)); err == nil {
				t.Fatal("started after drift")
			}
			if r := draRecordOf(t, b, testID); r.Claim.Send != draWithdrawn || r.Pod.Send != draWithdrawn {
				t.Fatalf("record = %+v", r)
			}
			if f.api.count("resourceclaims") != 0 || f.api.count("pods") != 0 {
				t.Fatal("sent after drift")
			}
		})
	}
	t.Run("between sends", func(t *testing.T) {
		f := newDRAFixture(t)
		_, b := f.integration(t)
		// The class changes after the claim is stored: the Pod is never sent.
		// admit runs under the API's lock.
		f.api.set(func() {
			f.api.admit["resourceclaims"] = func(any) { f.api.classes["gpu.example.com"].UID = "class-replaced" }
		})
		if err := b.Start(context.Background(), testID, draTask(1)); err == nil {
			t.Fatal("pod started after drift")
		}
		if r := draRecordOf(t, b, testID); r.Claim.Send != phasePinned || r.Pod.Send != draWithdrawn || f.api.count("pods") != 0 {
			t.Fatalf("record = %+v", r)
		}
	})
}

func TestDRAWriteAndSendCuts(t *testing.T) {
	for _, step := range []string{"dirsync", "send"} {
		t.Run(step, func(t *testing.T) {
			f := newDRAFixture(t)
			_, b := f.integration(t)
			b.dra.store.fault = func(s string, r *draRecord) error {
				if s == step && r.Claim.Send == phaseSent {
					return errors.New("modeled " + step + " cut")
				}
				return nil
			}
			if err := b.Start(context.Background(), testID, draTask(1)); err == nil {
				t.Fatal("cut reported success")
			}
			if f.api.count("resourceclaims") != 0 || f.api.count("pods") != 0 {
				t.Fatal("sent after an uncertain grant write or send cut")
			}
			// The consumed grant is never consumed again after restart.
			_, b2 := f.integration(t)
			s, err := b2.Observe(context.Background(), draRef(b2, testID, draTask(1)))
			if err != nil || s.State != backend.Failed {
				t.Fatalf("status = %+v %v", s, err)
			}
			if r := draRecordOf(t, b2, testID); r.Claim.Send != phaseSent || r.Pod.Send != draWithdrawn {
				t.Fatalf("record = %+v", r)
			}
			if f.api.count("resourceclaims") != 0 {
				t.Fatal("claim sent after restart")
			}
		})
	}
}

// TestDRATwoSequentialBackends uses two integrations, in this one process,
// over the same roots, and Starts them one after the other. It checks the
// durable duplicate refusal only; it is not concurrent and not two OS
// processes (TestDRATwoOSProcesses is).
func TestDRATwoSequentialBackends(t *testing.T) {
	f := newDRAFixture(t)
	_, a := f.integration(t)
	_, b := f.integration(t)
	if err := a.Start(context.Background(), testID, draTask(1)); err != nil {
		t.Fatal(err)
	}
	if err := b.Start(context.Background(), testID, draTask(1)); !errors.Is(err, errReceiptExists) {
		t.Fatalf("second process: %v", err)
	}
	if f.api.count("resourceclaims") != 1 || f.api.count("pods") != 1 {
		t.Fatal("second process sent")
	}
}

func TestDRAObserveHolds(t *testing.T) {
	for _, tc := range []struct {
		name  string
		model func(a *draAPI)
		state backend.State
		err   bool
	}{
		{"foreign consumer", func(a *draAPI) {
			a.claim(testID, func(c *resourcev1.ResourceClaim) {
				c.Status.ReservedFor = []resourcev1.ResourceClaimConsumerReference{{Resource: "pods", Name: "intruder", UID: "x"}}
			})
		}, backend.Running, false},
		// MODELED PodGC: a synthesized Failed phase is not stopped evidence.
		{"podgc failed", func(a *draAPI) {
			a.pod(testID, func(p *corev1.Pod) {
				p.Status.Phase = corev1.PodFailed
				p.Status.Conditions = []corev1.PodCondition{{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue, Reason: "DeletionByPodGC"}}
			})
		}, backend.Failed, false},
		{"pod missing", func(a *draAPI) { a.set(func() { delete(a.pods, "skygo-"+testID) }) }, backend.Unknown, false},
		{"claim missing", func(a *draAPI) {
			a.set(func() {
				for name := range a.claims {
					delete(a.claims, name)
				}
			})
		}, backend.Running, false},
		{"claim replaced", func(a *draAPI) { a.claim(testID, func(c *resourcev1.ResourceClaim) { c.UID = "replaced" }) }, "", true},
		{"pod replaced", func(a *draAPI) { a.pod(testID, func(p *corev1.Pod) { p.UID = "replaced" }) }, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDRAFixture(t)
			_, b := f.integration(t)
			ctx := context.Background()
			if err := b.Start(ctx, testID, draTask(1)); err != nil {
				t.Fatal(err)
			}
			f.api.pod(testID, func(p *corev1.Pod) { p.Status.Phase = corev1.PodRunning })
			tc.model(f.api) // each model takes the API's lock itself
			s, err := b.Observe(ctx, draRef(b, testID, draTask(1)))
			if tc.err != (err != nil) || !tc.err && s.State != tc.state {
				t.Fatalf("status = %+v %v", s, err)
			}
			if r := draRecordOf(t, b, testID); r.Hold == "" {
				t.Fatalf("no hold: %+v", r)
			}
			if len(f.api.deleted()) != 0 {
				t.Fatal("hold deleted an object")
			}
		})
	}
}

func TestDRACancel(t *testing.T) {
	f := newDRAFixture(t)
	_, b := f.integration(t)
	ctx := context.Background()
	if err := b.Start(ctx, testID, draTask(1)); err != nil {
		t.Fatal(err)
	}
	f.api.pod(testID, func(p *corev1.Pod) { p.Status.Phase = corev1.PodRunning })
	ref := draRef(b, testID, draTask(1))
	if err := b.Cancel(ctx, ref); err != nil {
		t.Fatal(err)
	}
	r := draRecordOf(t, b, testID)
	deletes := f.api.deleted()
	if r.Cancel != stepDelivered || len(deletes) != 1 {
		t.Fatalf("record = %+v deletes %d", r, len(deletes))
	}
	opts := deletes[0]
	if opts.Preconditions == nil || opts.Preconditions.UID == nil || string(*opts.Preconditions.UID) != r.Pod.UID || opts.GracePeriodSeconds != nil {
		t.Fatalf("delete options = %+v", opts)
	}
	// The Pod is gone, but no terminal state was witnessed: a delivered
	// deletion proves neither stopped work nor release, so the missing
	// execution is Unknown, with the delivery reported beside it.
	s, err := b.Observe(ctx, ref)
	if err != nil || s.State != backend.Unknown || !strings.Contains(s.Reason, "cancellation was delivered") {
		t.Fatalf("status = %+v %v", s, err)
	}
	// The claim of a canceled attempt is kept in HOLD, never deleted.
	if r := draRecordOf(t, b, testID); r.Hold == "" || r.Claim.Delete != "" {
		t.Fatalf("record = %+v", r)
	}
	kept := false
	f.api.claim(testID, func(*resourcev1.ResourceClaim) { kept = true })
	if !kept {
		t.Fatal("claim deleted after cancel")
	}
}

// TestDRACancelWitnessed is TestDRACancel's positive twin: a terminal state
// witnessed from the exact Pod, while it still exists after the delivered
// deletion, is the attempt's outcome, reported as Canceled; the Pod's later
// absence does not change it, and the claim stays in HOLD.
func TestDRACancelWitnessed(t *testing.T) {
	f := newDRAFixture(t)
	_, b := f.integration(t)
	ctx := context.Background()
	if err := b.Start(ctx, testID, draTask(1)); err != nil {
		t.Fatal(err)
	}
	f.api.set(func() { f.api.keepDeleted["pods"] = true })
	f.api.pod(testID, func(p *corev1.Pod) { p.Status.Phase = corev1.PodRunning })
	ref := draRef(b, testID, draTask(1))
	if err := b.Cancel(ctx, ref); err != nil {
		t.Fatal(err)
	}
	// MODELED kubelet: the container stopped on its termination signal.
	f.api.pod(testID, func(p *corev1.Pod) {
		p.Status.Phase = corev1.PodFailed
		p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "task",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 143}}}}
	})
	for _, step := range []string{"witnessed", "then missing"} {
		if step == "then missing" {
			f.api.set(func() { delete(f.api.pods, "skygo-"+testID) })
		}
		s, err := b.Observe(ctx, ref)
		if err != nil || s.State != backend.Canceled {
			t.Fatalf("%s: status = %+v %v", step, s, err)
		}
	}
	if r := draRecordOf(t, b, testID); r.Execution != draExecFailed || r.Cancel != stepDelivered || r.Hold == "" || r.Claim.Delete != "" {
		t.Fatalf("record = %+v", r)
	}
}
