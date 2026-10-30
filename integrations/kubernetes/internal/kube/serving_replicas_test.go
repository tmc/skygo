package kube

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"connectrpc.com/connect"
	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/serving"
	"github.com/tmc/skygo/internal/store"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

// These tests use the existing MODELED Kubernetes API and scheduler. The
// vertical cases use the real integration, Store, auth and mounted RPC;
// none establishes actual Kubernetes replica or foreground-delete behavior.
func fixedServiceRevision(t *testing.T, replicas int32) serving.Revision {
	t.Helper()
	spec := testRevision(t).Spec
	spec.Scaling.Replicas = replicas
	h, err := serving.NewHistory(spec)
	if err != nil {
		t.Fatal(err)
	}
	rev, _ := h.Revision(1)
	return rev
}

func TestServiceFixedReplicaRendering(t *testing.T) {
	for _, replicas := range []int32{1, 2, 8} {
		t.Run(fmt.Sprint(replicas), func(t *testing.T) {
			rev := fixedServiceRevision(t, replicas)
			d := serviceDeployment("workloads", testRef(""), rev, 1, "declaration", testServiceConfig().profile)
			if d.Spec.Replicas == nil || *d.Spec.Replicas != replicas {
				t.Fatalf("replicas = %v, want %d", d.Spec.Replicas, replicas)
			}
			strategy := d.Spec.Strategy.RollingUpdate
			if d.Spec.Strategy.Type != appsv1.RollingUpdateDeploymentStrategyType || strategy == nil ||
				strategy.MaxSurge.IntValue() != 0 || strategy.MaxUnavailable.IntValue() != 1 {
				t.Fatalf("unbounded rollout strategy: %+v", d.Spec.Strategy)
			}
			other := d.DeepCopy()
			*other.Spec.Replicas = replicas + 1
			if deploymentIdentity(other).witness() == deploymentIdentity(d).witness() {
				t.Fatal("replica count drift kept the exact identity witness")
			}
		})
	}
}

func TestServiceFixedReplicaRuntimeRefusals(t *testing.T) {
	for _, replicas := range []int32{-1, 0, 9} {
		t.Run(fmt.Sprint(replicas), func(t *testing.T) {
			client := fake.NewSimpleClientset()
			writer := newModelWriter(client)
			rt := newTestServiceRuntime(t, client, writer)
			rev := testRevision(t)
			rev.Spec.Scaling.Replicas = replicas
			if err := rt.Check(rev.Spec); !errors.Is(err, serving.ErrUnsupported) {
				t.Fatalf("Check = %v, want unsupported", err)
			}
			native, err := rt.Apply(context.Background(), testRef(""), rev, 1)
			if native != "" || !errors.Is(err, serving.ErrUnsupported) {
				t.Fatalf("Apply = %q, %v, want empty and unsupported", native, err)
			}
			if writer.sends("deployment") != 0 || writer.sends("service") != 0 {
				t.Fatal("invalid replica count sent a native create")
			}
		})
	}
}

func applyFixedService(t *testing.T, rt *serviceRuntime, rev serving.Revision) serving.Native {
	t.Helper()
	var native serving.Native
	for range 8 {
		ctx, cancel := context.WithTimeout(context.Background(), testWait)
		out, err := rt.Apply(ctx, testRef(native), rev, 1)
		cancel()
		if out == "" || out == native {
			if err != nil {
				t.Fatal(err)
			}
			return native
		}
		// As in applyAll, this assignment MODELS the caller committing
		// each Native before the next Apply. The vertical below commits it.
		native = out
	}
	t.Fatal("fixed-replica apply did not settle")
	return ""
}

// addFixedServiceReady extends the existing one-Pod scheduler fixture with
// the exact count of owned Pods and ready endpoints, then records the
// Deployment controller's modeled counts. It makes no native API call.
func addFixedServiceReady(t *testing.T, client *fake.Clientset, id string, replicas, ready int32) {
	t.Helper()
	ctx := context.Background()
	addReadyAt(t, client, id, nil)
	d := getDeploymentOf(t, client, id)
	base, err := client.CoreV1().Pods("workloads").Get(ctx, d.Name+"-abc123-x", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	slice, err := client.DiscoveryV1().EndpointSlices("workloads").Get(ctx, d.Name+"-x", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := *slice.Endpoints[0].DeepCopy()
	slice.Endpoints = nil
	for i := int32(0); i < replicas; i++ {
		pod := base.DeepCopy()
		if i != 0 {
			pod.Name = fmt.Sprintf("%s-r%d", base.Name, i)
			pod.UID = types.UID(fmt.Sprintf("fixed-pod-%d", i))
		}
		state := corev1.ConditionFalse
		if i < ready {
			state = corev1.ConditionTrue
		}
		pod.Status.Conditions[1].Status = state
		if i == 0 {
			_, err = client.CoreV1().Pods("workloads").Update(ctx, pod, metav1.UpdateOptions{})
		} else {
			_, err = client.CoreV1().Pods("workloads").Create(ctx, pod, metav1.CreateOptions{})
		}
		if err != nil {
			t.Fatal(err)
		}
		e := *endpoint.DeepCopy()
		e.TargetRef.Name, e.TargetRef.UID = pod.Name, pod.UID
		isReady := i < ready
		e.Conditions.Ready = &isReady
		slice.Endpoints = append(slice.Endpoints, e)
	}
	if _, err := client.DiscoveryV1().EndpointSlices("workloads").Update(ctx, slice, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	d.Status = appsv1.DeploymentStatus{ObservedGeneration: d.Generation, Replicas: replicas,
		UpdatedReplicas: replicas, ReadyReplicas: ready, AvailableReplicas: ready}
	if _, err := client.AppsV1().Deployments("workloads").Update(ctx, d, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestServiceFixedReplicaReadiness(t *testing.T) {
	for _, tt := range []struct {
		name     string
		replicas int32
		ready    int32
		change   func(*testing.T, *fake.Clientset)
		want     serving.Phase
	}{
		{name: "two ready", replicas: 2, ready: 2, want: serving.Ready},
		{name: "eight ready", replicas: 8, ready: 8, want: serving.Ready},
		{name: "one of two", replicas: 2, ready: 1, want: serving.Progressing},
		{name: "foreign pod owner", replicas: 2, ready: 2, want: serving.Progressing, change: func(t *testing.T, client *fake.Clientset) {
			p, err := client.CoreV1().Pods("workloads").Get(context.Background(), serviceObjectName(testServiceID)+"-abc123-x-r1", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			p.OwnerReferences[0].UID = "foreign-replicaset"
			if _, err := client.CoreV1().Pods("workloads").Update(context.Background(), p, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "foreign endpoint uids", replicas: 2, ready: 2, want: serving.Unavailable, change: func(t *testing.T, client *fake.Clientset) {
			e, err := client.DiscoveryV1().EndpointSlices("workloads").Get(context.Background(), serviceObjectName(testServiceID)+"-x", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			for i := range e.Endpoints {
				e.Endpoints[i].TargetRef.UID = "foreign-pod"
			}
			if _, err := client.DiscoveryV1().EndpointSlices("workloads").Update(context.Background(), e, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "duplicate endpoint facts", replicas: 2, ready: 2, want: serving.Ready, change: func(t *testing.T, client *fake.Clientset) {
			e, err := client.DiscoveryV1().EndpointSlices("workloads").Get(context.Background(), serviceObjectName(testServiceID)+"-x", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			e.Name += "-duplicate"
			if _, err := client.DiscoveryV1().EndpointSlices("workloads").Create(context.Background(), e, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			writer := newModelWriter(client)
			rt := newTestServiceRuntime(t, client, writer)
			rev := fixedServiceRevision(t, tt.replicas)
			native := applyFixedService(t, rt, rev)
			addFixedServiceReady(t, client, testServiceID, tt.replicas, tt.ready)
			if tt.change != nil {
				tt.change(t, client)
			}
			o, err := observe(t, rt, native)
			if err != nil {
				t.Fatal(err)
			}
			h, err := serving.NewHistory(rev.Spec)
			if err != nil {
				t.Fatal(err)
			}
			if got := serving.PhaseOf(serving.Present, h, &o); got != tt.want {
				t.Fatalf("phase = %s, want %s; observation %+v", got, tt.want, o)
			}
			if o.DesiredReplicas != tt.replicas || o.ReadyEndpoints > o.ReadyReplicas {
				t.Fatalf("incorrect desired count or duplicate endpoints: %+v", o)
			}
			if d := getDeployment(t, client); d.Spec.Replicas == nil || *d.Spec.Replicas != tt.replicas {
				t.Fatalf("modeled Deployment did not run %d replicas", tt.replicas)
			}
		})
	}
}

func TestServiceFixedReplicaVertical(t *testing.T) {
	for _, replicas := range []int32{2, 8} {
		t.Run(fmt.Sprint(replicas), func(t *testing.T) {
			v := newServiceVerticalCreated(t)
			v.spec.Scaling.Replicas = replicas
			id := v.createService(t, "fixed")
			want := store.ServiceReservation{Replicas: uint64(replicas), CPUMillis: uint64(replicas) * 250, MemoryBytes: uint64(replicas) * (256 << 20)}
			if rec := v.record(t, id); rec.Reserved != want || rec.Native != "" {
				t.Fatalf("acceptance charge or pre-effect state: %+v", rec)
			}
			for range 4 {
				if v.hasObjects(id) {
					break
				}
				v.pass(t)
			}
			if !v.hasObjects(id) {
				t.Fatal("fixed-replica objects not created")
			}
			addFixedServiceReady(t, v.client, id, replicas, replicas)
			for range 4 {
				if err := v.pass(t); err != nil {
					t.Fatal(err)
				}
				if v.getOf(t, id).Status.GetPhase() == skyv1.ServicePhase_SERVICE_PHASE_READY {
					break
				}
			}
			s := v.getOf(t, id)
			if s.Status.GetPhase() != skyv1.ServicePhase_SERVICE_PHASE_READY || s.Status.Observation.DesiredReplicas != replicas || s.Status.Observation.ReadyReplicas != replicas {
				t.Fatalf("fixed service not ready: %+v", s.Status)
			}
			if got := v.record(t, id).Reserved; got != want {
				t.Fatalf("ready charge = %+v, want %+v", got, want)
			}
			v.deleteService(t, id)
			v.objects.mu.Lock()
			v.objects.fail = func(r *http.Request) error {
				if r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/deployments/"+serviceObjectName(id)) {
					return errors.New("modeled foreground deletion still pending")
				}
				return nil
			}
			v.objects.mu.Unlock()
			if err := v.pass(t); err == nil {
				t.Fatal("modeled deletion failure was ignored")
			}
			if got := v.record(t, id).Reserved; got != want {
				t.Fatalf("failed cleanup refunded %+v, want held %+v", got, want)
			}
			// Model the foreground collector finishing every owned Pod
			// before releasing the delete reply; no live-Pod absence is
			// inferred from this fake API's immediate object deletion.
			pods, err := v.client.CoreV1().Pods("workloads").List(context.Background(), metav1.ListOptions{})
			if err != nil {
				t.Fatal(err)
			}
			for _, p := range pods.Items {
				if strings.HasPrefix(p.Name, serviceObjectName(id)+"-") {
					if err := v.client.CoreV1().Pods("workloads").Delete(context.Background(), p.Name, metav1.DeleteOptions{}); err != nil {
						t.Fatal(err)
					}
				}
			}
			v.objects.mu.Lock()
			v.objects.fail = nil
			v.objects.mu.Unlock()
			for range 4 {
				if err := v.pass(t); err != nil {
					t.Fatal(err)
				}
				if _, err := v.o.s.ServiceFor(v.actor, id); errors.Is(err, store.ErrNotFound) {
					return
				}
			}
			t.Fatal("service not retired after modeled foreground absence")
		})
	}
}

func TestServiceFixedReplicaVerticalCapacityRefusal(t *testing.T) {
	v := newServiceVerticalCreated(t)
	v.spec.Scaling.Replicas = 8
	id := v.createService(t, "eight")
	before := v.record(t, id)
	for _, tt := range []struct {
		name string
		n    int32
		code connect.Code
	}{
		{"over-budget", 8, connect.CodeResourceExhausted},
		{"invalid-count", 9, connect.CodeInvalidArgument},
	} {
		t.Run(tt.name, func(t *testing.T) {
			v.spec.Scaling.Replicas = tt.n
			ctx, cancel := context.WithTimeout(context.Background(), testWait)
			defer cancel()
			_, err := v.rpc.CreateService(ctx, bearer(v.token, &skyv1.CreateServiceRequest{
				RequestId: "create-" + tt.name, Name: tt.name, Target: "kube", Spec: v.spec, Domain: v.domain,
			}))
			if connect.CodeOf(err) != tt.code {
				t.Fatalf("CreateService = %v, want %v", err, tt.code)
			}
			if rec := v.record(t, id); rec.Revision != before.Revision || rec.Reserved != before.Reserved || rec.Native != before.Native {
				t.Fatalf("refusal changed accepted state: %+v", rec)
			}
			if v.objects.sends("deployment") != 0 || v.objects.sends("service") != 0 {
				t.Fatal("refused or merely accepted service sent native creates")
			}
		})
	}
}

func TestServiceFixedReplicaNativeDrift(t *testing.T) {
	for _, field := range []string{"uid", "replicas"} {
		t.Run(field, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			writer := newModelWriter(client)
			rt := newTestServiceRuntime(t, client, writer)
			rev := fixedServiceRevision(t, 2)
			native := applyFixedService(t, rt, rev)
			d := getDeployment(t, client)
			switch field {
			case "uid":
				d.UID = "foreign-deployment"
			case "replicas":
				*d.Spec.Replicas = 3
			}
			if _, err := client.AppsV1().Deployments("workloads").Update(context.Background(), d, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			o, err := observe(t, rt, native)
			if err != nil || o.Reason != serving.IdentityConflict {
				t.Fatalf("Observe = %+v, %v, want identity conflict", o, err)
			}
			out, err := rt.Apply(context.Background(), testRef(native), rev, 1)
			if out != "" || !errors.Is(err, serving.ErrIdentityConflict) {
				t.Fatalf("Apply after drift = %q, %v", out, err)
			}
			if writer.sends("deployment") != 1 || writer.sends("service") != 1 {
				t.Fatal("identity conflict caused a repeated native create")
			}
		})
	}
}
