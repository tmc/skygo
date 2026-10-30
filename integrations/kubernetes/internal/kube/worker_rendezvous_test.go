package kube

import (
	"errors"
	"maps"
	"reflect"
	"slices"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// workerSliceIP is the original rank 0 Pod's IP in workerPrepare and the
// address of workerSlice's endpoint.
const workerSliceIP = "10.1.0.5"

// workerSlice MODELS the EndpointSlice controller's IPv4 slice for the
// original Service svc, with its managed-by label and controller owner
// reference, and one endpoint at workerSliceIP targeting the rank 0 Pod name
// with UID uid. The endpoint sets only Ready: Serving and Terminating are
// nil, as an older producer leaves them.
func workerSlice(svc backend.WorkerResource, uid types.UID, ready bool) *discoveryv1.EndpointSlice {
	name, port, proto := workerRendezvousPort, int32(workerMasterPort), corev1.ProtocolTCP
	yes := true
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{Name: svc.Name + "-x7k2p", Namespace: "jobs",
			Labels: map[string]string{discoveryv1.LabelServiceName: svc.Name, discoveryv1.LabelManagedBy: workerSliceController},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Service", Name: svc.Name,
				UID: types.UID(svc.UID), Controller: &yes, BlockOwnerDeletion: &yes}}},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{{Name: &name, Port: &port, Protocol: &proto}},
		Endpoints: []discoveryv1.Endpoint{{Addresses: []string{workerSliceIP},
			Conditions: discoveryv1.EndpointConditions{Ready: &ready},
			TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: "jobs", Name: "skygo-" + workerAttempt + "-0", UID: uid}}},
	}
}

func TestWorkerService(t *testing.T) {
	rt, _ := newWorkerTestRuntime(t)
	svc, err := rt.service(workerPlan(0, 3, workerTCP))
	if err != nil {
		t.Fatal(err)
	}
	pod, _ := rt.pod(workerPlan(0, 3, workerTCP))
	wantSelector := map[string]string{ownerLabel: "owner", workerJobLabel: workerJob,
		workerAttemptLabel: workerAttempt, workerRankLabel: "0"}
	if svc.Name != "skygo-"+workerAttempt || svc.Namespace != "jobs" || !maps.Equal(svc.Spec.Selector, wantSelector) {
		t.Fatalf("service %s/%s selector %v", svc.Namespace, svc.Name, svc.Spec.Selector)
	}
	for k, v := range wantSelector {
		if pod.Labels[k] != v {
			t.Errorf("rank 0 pod label %s = %q, want %q", k, pod.Labels[k], v)
		}
	}
	if !slices.Equal(svc.Finalizers, []string{workerFinalizer}) || svc.Spec.Type != corev1.ServiceTypeClusterIP ||
		svc.Spec.PublishNotReadyAddresses || len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != workerMasterPort {
		t.Fatalf("service %+v", svc)
	}
	if svc.Annotations[workerProfileAnnotation] != workerBarrierProfile ||
		svc.Annotations[workerGroupAnnotation] != pod.Annotations[workerGroupAnnotation] {
		t.Fatalf("annotations %v", svc.Annotations)
	}
	for _, p := range []workerPodPlan{workerPlan(1, 3, workerTCP), workerPlan(0, 3, workerIndependent)} {
		if _, err := rt.service(p); err == nil {
			t.Errorf("rank %d mode %d: service built", p.Rank, p.Mode)
		}
	}
}

func TestWorkerServiceMatches(t *testing.T) {
	rt, f := newWorkerTestRuntime(t)
	g := workerTestGroup(t, rt, backend.WorkerTCP, 2)
	refs := workerCreateAll(t, rt, g)
	stored := f.get("services", refs[0].Service.Name).(*corev1.Service)
	p := workerPlan(0, 2, workerTCP)
	uid := types.UID(refs[0].Service.UID)
	tests := []struct {
		name string
		edit func(*corev1.Service)
		ok   bool
	}{
		{"stored", func(*corev1.Service) {}, true},
		{"deleting", func(s *corev1.Service) { now := metav1.Now(); s.DeletionTimestamp = &now }, true},
		{"other uid", func(s *corev1.Service) { s.UID = "other" }, false},
		{"selector without attempt", func(s *corev1.Service) { delete(s.Spec.Selector, workerAttemptLabel) }, false},
		{"selector any rank", func(s *corev1.Service) { delete(s.Spec.Selector, workerRankLabel) }, false},
		{"node port", func(s *corev1.Service) { s.Spec.Type = corev1.ServiceTypeNodePort; s.Spec.Ports[0].NodePort = 30000 }, false},
		{"extra port", func(s *corev1.Service) { s.Spec.Ports = append(s.Spec.Ports, s.Spec.Ports[0]) }, false},
		{"not ready addresses", func(s *corev1.Service) { s.Spec.PublishNotReadyAddresses = true }, false},
		{"headless", func(s *corev1.Service) { s.Spec.ClusterIP = corev1.ClusterIPNone }, false},
		{"external ip", func(s *corev1.Service) { s.Spec.ExternalIPs = []string{"203.0.113.1"} }, false},
		{"finalizer removed live", func(s *corev1.Service) { s.Finalizers = nil }, false},
		{"stale attempt label", func(s *corev1.Service) { s.Labels[workerAttemptLabel] = workerJob }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := stored.DeepCopy()
			tt.edit(s)
			_, err := rt.serviceMatches(s, p, uid)
			if tt.ok != (err == nil) {
				t.Fatalf("serviceMatches = %v, want ok %v", err, tt.ok)
			}
			if !tt.ok && !errors.Is(err, backend.ErrIdentityMismatch) {
				t.Fatalf("serviceMatches = %v, want identity mismatch", err)
			}
		})
	}
}

// TestWorkerRouted changes one fact of an otherwise routed slice per row.
func TestWorkerRouted(t *testing.T) {
	const orig, stale = types.UID("orig-uid"), types.UID("stale-uid")
	svc := backend.WorkerResource{Name: "skygo-" + workerAttempt, UID: "svc-uid"}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "skygo-" + workerAttempt + "-0", Namespace: "jobs", UID: orig},
		Status: corev1.PodStatus{PodIP: workerSliceIP, PodIPs: []corev1.PodIP{{IP: workerSliceIP}, {IP: "fd00::5"}}}}
	with := func(edit func(*discoveryv1.EndpointSlice)) []discoveryv1.EndpointSlice {
		s := workerSlice(svc, orig, true)
		edit(s)
		return []discoveryv1.EndpointSlice{*s}
	}
	yes, no := true, false
	tests := []struct {
		name string
		list []discoveryv1.EndpointSlice
		pod  func(*corev1.Pod)
		want bool
	}{
		{"none", nil, nil, false},
		// Ready true with Serving and Terminating nil: the nil control.
		{"original ready", with(func(*discoveryv1.EndpointSlice) {}), nil, true},
		{"serving and not terminating", with(func(s *discoveryv1.EndpointSlice) {
			s.Endpoints[0].Conditions.Serving, s.Endpoints[0].Conditions.Terminating = &yes, &no
		}), nil, true},
		{"ipv6", with(func(s *discoveryv1.EndpointSlice) {
			s.AddressType, s.Endpoints[0].Addresses = discoveryv1.AddressTypeIPv6, []string{"fd00::5"}
		}), nil, true},
		{"empty valid extra slice", append(with(func(*discoveryv1.EndpointSlice) {}), func() discoveryv1.EndpointSlice {
			s := workerSlice(svc, orig, true)
			s.Name, s.Endpoints = s.Name+"-2", nil
			return *s
		}()), nil, true},

		{"original not ready", with(func(s *discoveryv1.EndpointSlice) { s.Endpoints[0].Conditions.Ready = &no }), nil, false},
		{"readiness unknown", with(func(s *discoveryv1.EndpointSlice) { s.Endpoints[0].Conditions.Ready = nil }), nil, false},
		{"not serving", with(func(s *discoveryv1.EndpointSlice) { s.Endpoints[0].Conditions.Serving = &no }), nil, false},
		{"terminating", with(func(s *discoveryv1.EndpointSlice) { s.Endpoints[0].Conditions.Terminating = &yes }), nil, false},
		{"stale attempt pod", []discoveryv1.EndpointSlice{*workerSlice(svc, stale, true)}, nil, false},
		{"foreign endpoint too", with(func(s *discoveryv1.EndpointSlice) {
			e := s.Endpoints[0]
			e.TargetRef = &corev1.ObjectReference{Kind: "Pod", Namespace: "jobs", Name: pod.Name, UID: stale}
			s.Endpoints = append(s.Endpoints, e)
		}), nil, false},
		{"foreign slice", append(with(func(*discoveryv1.EndpointSlice) {}), *workerSlice(svc, stale, false)), nil, false},
		{"no target", with(func(s *discoveryv1.EndpointSlice) { s.Endpoints[0].TargetRef = nil }), nil, false},
		{"other namespace", with(func(s *discoveryv1.EndpointSlice) { s.Endpoints[0].TargetRef.Namespace = "other" }), nil, false},
		{"other kind", with(func(s *discoveryv1.EndpointSlice) { s.Endpoints[0].TargetRef.Kind = "Node" }), nil, false},
		{"other target name", with(func(s *discoveryv1.EndpointSlice) { s.Endpoints[0].TargetRef.Name = "skygo-other-0" }), nil, false},
		{"no addresses", with(func(s *discoveryv1.EndpointSlice) { s.Endpoints[0].Addresses = nil }), nil, false},
		{"address not the pod's", with(func(s *discoveryv1.EndpointSlice) { s.Endpoints[0].Addresses = []string{"10.1.0.6"} }), nil, false},
		{"one address not the pod's", with(func(s *discoveryv1.EndpointSlice) {
			s.Endpoints[0].Addresses = append(s.Endpoints[0].Addresses, "10.1.0.6")
		}), nil, false},
		{"pod without ips", with(func(*discoveryv1.EndpointSlice) {}), func(p *corev1.Pod) { p.Status = corev1.PodStatus{} }, false},
		{"ipv6 slice ipv4 address", with(func(s *discoveryv1.EndpointSlice) { s.AddressType = discoveryv1.AddressTypeIPv6 }), nil, false},
		{"fqdn slice", with(func(s *discoveryv1.EndpointSlice) {
			s.AddressType, s.Endpoints[0].Addresses = discoveryv1.AddressTypeFQDN, []string{"fd00::5"}
		}), nil, false},
		{"no managed-by", with(func(s *discoveryv1.EndpointSlice) { delete(s.Labels, discoveryv1.LabelManagedBy) }), nil, false},
		{"foreign managed-by", with(func(s *discoveryv1.EndpointSlice) { s.Labels[discoveryv1.LabelManagedBy] = "example.com/mirror" }), nil, false},
		{"no owner", with(func(s *discoveryv1.EndpointSlice) { s.OwnerReferences = nil }), nil, false},
		{"replaced service owner", with(func(s *discoveryv1.EndpointSlice) { s.OwnerReferences[0].UID = "replacement" }), nil, false},
		{"other service owner", with(func(s *discoveryv1.EndpointSlice) { s.OwnerReferences[0].Name = "skygo-other" }), nil, false},
		{"owner kind", with(func(s *discoveryv1.EndpointSlice) { s.OwnerReferences[0].Kind = "Pod" }), nil, false},
		{"owner api version", with(func(s *discoveryv1.EndpointSlice) { s.OwnerReferences[0].APIVersion = "v2" }), nil, false},
		{"owner not controller", with(func(s *discoveryv1.EndpointSlice) { s.OwnerReferences[0].Controller = &no }), nil, false},
		{"owner controller unset", with(func(s *discoveryv1.EndpointSlice) { s.OwnerReferences[0].Controller = nil }), nil, false},
		{"extra owner", with(func(s *discoveryv1.EndpointSlice) {
			s.OwnerReferences = append(s.OwnerReferences, metav1.OwnerReference{APIVersion: "v1", Kind: "ConfigMap", Name: "x", UID: "x-uid"})
		}), nil, false},
		// An invalid slice is never skipped, even with no endpoints.
		{"empty invalid extra slice", append(with(func(*discoveryv1.EndpointSlice) {}), discoveryv1.EndpointSlice{}), nil, false},
		{"other port", with(func(s *discoveryv1.EndpointSlice) { p := int32(8080); s.Ports[0].Port = &p }), nil, false},
		{"udp", with(func(s *discoveryv1.EndpointSlice) { u := corev1.ProtocolUDP; s.Ports[0].Protocol = &u }), nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := pod.DeepCopy()
			if tt.pod != nil {
				tt.pod(p)
			}
			if got := workerRouted(tt.list, "jobs", svc, p); got != tt.want {
				t.Fatalf("workerRouted = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestWorkerRendezvousObserve checks rank 0's rendezvous through the
// runtime: the address is attempt-specific, a stale attempt's endpoint never
// routes, and a deleting or replaced Service never routes.
func TestWorkerRendezvousObserve(t *testing.T) {
	rt, f := newWorkerTestRuntime(t)
	g := workerTestGroup(t, rt, backend.WorkerTCP, 2)
	refs := workerCreateAll(t, rt, g)
	observe := func() (backend.WorkerRendezvous, error) {
		o, err := rt.ObserveBarrier(t.Context(), refs[0])
		return o.Rendezvous, err
	}
	r, err := observe()
	want := backend.WorkerRendezvous{Service: refs[0].Service, Host: "skygo-" + workerAttempt + ".jobs.svc", Port: workerMasterPort}
	if err != nil || r != want {
		t.Fatalf("rendezvous = %+v, %v; want %+v", r, err, want)
	}
	if o, err := rt.ObserveBarrier(t.Context(), refs[1]); err != nil || o.Rendezvous != (backend.WorkerRendezvous{}) {
		t.Fatalf("rank 1 rendezvous = %+v, %v", o.Rendezvous, err)
	}
	workerEdit(f, refs[0].Pod.Name, func(p *corev1.Pod) {
		p.Status.PodIP, p.Status.PodIPs = workerSliceIP, []corev1.PodIP{{IP: workerSliceIP}}
	})
	stale := f.putObject(workerSlice(refs[0].Service, "stale-uid", true)).(*discoveryv1.EndpointSlice)
	if r, err := observe(); err != nil || r.Routed {
		t.Fatalf("stale endpoint: %+v, %v", r, err)
	}
	f.remove("endpointslices", stale.Name)
	f.putObject(workerSlice(refs[0].Service, types.UID(refs[0].Pod.UID), true))
	// The Pod's IPs come from ObserveBarrier's own read: one Pod GET.
	mark := f.mark()
	if r, err := observe(); err != nil || !r.Routed {
		t.Fatalf("original endpoint: %+v, %v", r, err)
	}
	if n := len(f.since(mark, "get", "pods")); n != 1 {
		t.Fatalf("%d pod reads, want 1", n)
	}
	f.edit("services", refs[0].Service.Name, func(o runtime.Object) {
		now := metav1.Now()
		o.(*corev1.Service).DeletionTimestamp = &now
	})
	if r, err := observe(); err != nil || r.Routed {
		t.Fatalf("deleting service: %+v, %v", r, err)
	}
	f.edit("services", refs[0].Service.Name, func(o runtime.Object) { o.(*corev1.Service).UID = "replacement" })
	if _, err := observe(); !errors.Is(err, backend.ErrWorkerVanished) || !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("replaced service: %v, want vanished and identity mismatch", err)
	}
}

// workerReplaceService replaces the group Service of refs outside skygo by
// an object identical to it except for its UID and resourceVersion.
func workerReplaceService(t *testing.T, f *workerAPI, refs []backend.WorkerReference) {
	t.Helper()
	s := f.get("services", refs[0].Service.Name).(*corev1.Service)
	f.remove("services", s.Name)
	s.UID, s.ResourceVersion = "", ""
	if got := f.putObject(s).(*corev1.Service); got.UID == types.UID(refs[0].Service.UID) {
		t.Fatal("replacement kept the original uid")
	}
}

// TestWorkerRuntimeReplacedService: a consistent read that finds another
// UID at the original Service name has vanished, as for a Pod, before any
// profile comparison. No ObserveBarrier routes or prepares, and no
// ReleaseWorker or StartWorker writes. The same UID with an altered profile
// is an identity mismatch, not vanished.
func TestWorkerRuntimeReplacedService(t *testing.T) {
	vanished := func(t *testing.T, what string, err error) {
		t.Helper()
		if !errors.Is(err, backend.ErrWorkerVanished) || !errors.Is(err, backend.ErrIdentityMismatch) {
			t.Fatalf("%s = %v, want vanished and identity mismatch", what, err)
		}
	}
	t.Run("release", func(t *testing.T) {
		rt, f := newWorkerTestRuntime(t)
		g := workerTestGroup(t, rt, backend.WorkerTCP, 2)
		refs := workerCreateAll(t, rt, g)
		workerReplaceService(t, f, refs)
		mark := f.mark()
		for _, ref := range refs {
			got, err := rt.ReleaseWorker(t.Context(), ref)
			vanished(t, "ReleaseWorker", err)
			if got.Acknowledged {
				t.Fatalf("rank %d: release acknowledged", ref.Rank)
			}
			if p := workerStored(t, f, ref.Pod.Name); len(p.Spec.SchedulingGates) != 1 {
				t.Fatalf("rank %d: gate removed", ref.Rank)
			}
		}
		if n := len(f.since(mark, "put", "pods")); n != 0 {
			t.Fatalf("%d pod updates", n)
		}
	})
	t.Run("observe and start", func(t *testing.T) {
		rt, f := newWorkerTestRuntime(t)
		g := workerTestGroup(t, rt, backend.WorkerTCP, 2)
		refs := workerCreateAll(t, rt, g)
		for _, ref := range refs {
			if _, err := rt.ReleaseWorker(t.Context(), ref); err != nil {
				t.Fatal(err)
			}
			workerEdit(f, ref.Pod.Name, workerPrepare)
		}
		f.putObject(workerSlice(refs[0].Service, types.UID(refs[0].Pod.UID), true))
		// Control: the original Service routes and the group is ready.
		obs := make([]backend.WorkerBarrier, len(refs))
		for i, ref := range refs {
			o, err := rt.ObserveBarrier(t.Context(), ref)
			if err != nil {
				t.Fatal(err)
			}
			obs[i] = o
		}
		if !obs[0].Rendezvous.Routed || !obs[0].Prepared {
			t.Fatalf("original service: %+v", obs[0])
		}
		if err := backend.WorkerBarrierReady(refs, obs); err != nil {
			t.Fatalf("WorkerBarrierReady = %v", err)
		}
		workerReplaceService(t, f, refs)
		mark := f.mark()
		o, err := rt.ObserveBarrier(t.Context(), refs[0])
		vanished(t, "ObserveBarrier", err)
		if o != (backend.WorkerBarrier{}) {
			t.Fatalf("ObserveBarrier = %+v, want no observation", o)
		}
		for _, ref := range refs {
			got, err := rt.StartWorker(t.Context(), ref, workerToken(t, ref))
			vanished(t, "StartWorker", err)
			if got.Acknowledged {
				t.Fatalf("rank %d: start acknowledged", ref.Rank)
			}
			if _, ok := workerStored(t, f, ref.Pod.Name).Annotations[workerStartAnnotation]; ok {
				t.Fatalf("rank %d: start token written", ref.Rank)
			}
		}
		if n := len(f.since(mark, "put", "pods")); n != 0 {
			t.Fatalf("%d pod updates", n)
		}
	})
	t.Run("same uid altered profile", func(t *testing.T) {
		rt, f := newWorkerTestRuntime(t)
		g := workerTestGroup(t, rt, backend.WorkerTCP, 2)
		refs := workerCreateAll(t, rt, g)
		f.edit("services", refs[0].Service.Name, func(o runtime.Object) { delete(o.(*corev1.Service).Spec.Selector, workerRankLabel) })
		mark := f.mark()
		if _, err := rt.ObserveBarrier(t.Context(), refs[0]); !errors.Is(err, backend.ErrIdentityMismatch) || errors.Is(err, backend.ErrWorkerVanished) {
			t.Fatalf("ObserveBarrier = %v, want identity mismatch only", err)
		}
		if _, err := rt.ReleaseWorker(t.Context(), refs[1]); !errors.Is(err, backend.ErrIdentityMismatch) || errors.Is(err, backend.ErrWorkerVanished) {
			t.Fatalf("ReleaseWorker = %v, want identity mismatch only", err)
		}
		if n := len(f.since(mark, "put", "pods")); n != 0 {
			t.Fatalf("%d pod updates", n)
		}
	})
	t.Run("missing", func(t *testing.T) {
		rt, f := newWorkerTestRuntime(t)
		g := workerTestGroup(t, rt, backend.WorkerTCP, 2)
		refs := workerCreateAll(t, rt, g)
		f.remove("services", refs[0].Service.Name)
		if _, err := rt.ObserveBarrier(t.Context(), refs[0]); !errors.Is(err, backend.ErrWorkerVanished) {
			t.Fatalf("ObserveBarrier = %v, want vanished", err)
		}
		if _, err := rt.ReleaseWorker(t.Context(), refs[1]); !errors.Is(err, backend.ErrWorkerVanished) {
			t.Fatalf("ReleaseWorker = %v, want vanished", err)
		}
	})
	// Removal keeps its own rule: a replaced Service is retired only under a
	// full recovery attestation, and is never modified.
	t.Run("removal", func(t *testing.T) {
		rt, f := newWorkerTestRuntime(t)
		g := workerTestGroup(t, rt, backend.WorkerTCP, 2)
		refs := workerCreateAll(t, rt, g)
		ref := backend.WorkerReference{Group: g, Rank: 0, Service: refs[0].Service}
		workerReplaceService(t, f, refs)
		mark := f.mark()
		if rm, err := rt.RemoveWorker(t.Context(), ref); !errors.Is(err, backend.ErrWorkerVanished) || rm.ServiceRemoved {
			t.Fatalf("RemoveWorker = %+v, %v", rm, err)
		}
		r := backend.WorkerRecovery{Ref: ref, Evidence: backend.WorkerNeverBound, RemoveSent: true}
		if rm, err := rt.RecoverWorkerRemoval(t.Context(), r); err != nil || !rm.ServiceRemoved {
			t.Fatalf("RecoverWorkerRemoval = %+v, %v", rm, err)
		}
		if n := len(f.since(mark, "put", "services")) + len(f.since(mark, "delete", "services")); n != 0 {
			t.Fatalf("replacement modified %d times", n)
		}
		if f.get("services", refs[0].Service.Name) == nil {
			t.Fatal("replacement removed")
		}
	})
}

// TestWorkerRuntimeCleanup cancels and retires a whole WorkerTCP group,
// rank 0's Service last, and leaves another attempt's objects untouched.
func TestWorkerRuntimeCleanup(t *testing.T) {
	rt, f := newWorkerTestRuntime(t)
	old := workerTestGroup(t, rt, backend.WorkerTCP, 3)
	old.AttemptID = workerJob
	workerCreateAll(t, rt, old)
	before := map[workerObjectKey]runtime.Object{}
	for _, k := range f.keys() {
		before[k] = f.get(k.resource, k.name)
	}

	g := workerTestGroup(t, rt, backend.WorkerTCP, 3)
	refs := workerCreateAll(t, rt, g)
	for _, ref := range refs {
		if _, err := rt.CancelWorker(t.Context(), ref); err != nil {
			t.Fatal(err)
		}
		o, err := rt.ObserveBarrier(t.Context(), ref)
		if err != nil || o.Stop != backend.WorkerNeverBound {
			t.Fatalf("rank %d: %+v, %v", ref.Rank, o, err)
		}
	}
	// Rank 0 last: its RemoveWorker also retires the Service.
	for _, i := range []int{2, 1, 0} {
		rm, err := rt.RemoveWorker(t.Context(), refs[i])
		if err != nil || !rm.PodRemoved || rm.ServiceRemoved != (i == 0) {
			t.Fatalf("rank %d: RemoveWorker = %+v, %v", i, rm, err)
		}
		if err := backend.ValidateWorkerRemoval(refs[i], rm); err != nil {
			t.Fatalf("rank %d: %v", i, err)
		}
	}
	after := map[workerObjectKey]runtime.Object{}
	for _, k := range f.keys() {
		after[k] = f.get(k.resource, k.name)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("after cleanup %v, want only the other attempt's %v", f.keys(), slices.Collect(maps.Keys(before)))
	}
}

// TestWorkerRuntimeServiceOnlyCleanup uses a real partial Create: the original
// Service was acknowledged and the Pod POST was definitively refused. Control
// must retain precisely this original refusal before passing a service-only ref.
func TestWorkerRuntimeServiceOnlyCleanup(t *testing.T) {
	rt, f := newWorkerTestRuntime(t)
	g := workerTestGroup(t, rt, backend.WorkerTCP, 2)
	f.fail("pods", workerFault{status: 403})
	created, err := rt.CreateWorker(t.Context(), backend.WorkerCreate{Group: g, Rank: 0})
	if !errors.Is(err, backend.ErrWorkerCreateRefused) || created.Pod != (backend.WorkerResource{}) || created.Service.UID == "" {
		t.Fatalf("original partial refusal = %+v, %v", created, err)
	}
	ref := backend.WorkerReference{Group: g, Rank: 0, Service: created.Service}
	rm, err := rt.RemoveWorker(t.Context(), ref)
	if err != nil || rm.PodRemoved || !rm.ServiceRemoved || !rm.ServiceDispatched {
		t.Fatalf("RemoveWorker = %+v, %v", rm, err)
	}
	if f.get("services", ref.Service.Name) != nil {
		t.Fatal("service remains")
	}
	if _, err := rt.ObserveBarrier(t.Context(), ref); err == nil {
		t.Fatal("service-only reference observed")
	}
}

// TestWorkerRuntimeServiceMissing: a missing Service has vanished; it is
// retired only under a full recovery attestation.
func TestWorkerRuntimeServiceMissing(t *testing.T) {
	rt, f := newWorkerTestRuntime(t)
	g := workerTestGroup(t, rt, backend.WorkerTCP, 2)
	refs := workerCreateAll(t, rt, g)
	ref := backend.WorkerReference{Group: g, Rank: 0, Service: refs[0].Service}
	f.remove("services", ref.Service.Name)
	if rm, err := rt.RemoveWorker(t.Context(), ref); !errors.Is(err, backend.ErrWorkerVanished) || rm.ServiceRemoved {
		t.Fatalf("RemoveWorker = %+v, %v", rm, err)
	}
	mark := f.mark()
	if rm, err := rt.RecoverWorkerRemoval(t.Context(), backend.WorkerRecovery{Ref: ref, Evidence: backend.WorkerNeverBound}); err == nil || rm.ServiceRemoved || rm.PodRemoved || rm.ServiceDispatched || rm.PodDispatched || len(f.since(mark)) != 0 {
		t.Fatalf("unsent RecoverWorkerRemoval = %+v, %v", rm, err)
	}
	r := backend.WorkerRecovery{Ref: ref, Evidence: backend.WorkerNeverBound, RemoveSent: true}
	if rm, err := rt.RecoverWorkerRemoval(t.Context(), r); err != nil || !rm.ServiceRemoved {
		t.Fatalf("RecoverWorkerRemoval = %+v, %v", rm, err)
	}
}

// TestWorkerRuntimeStartRequiresService: a prepared member of a WorkerTCP
// group is not started once its group's Service is being deleted.
func TestWorkerRuntimeStartRequiresService(t *testing.T) {
	rt, f := newWorkerTestRuntime(t)
	g := workerTestGroup(t, rt, backend.WorkerTCP, 2)
	refs := workerCreateAll(t, rt, g)
	for _, ref := range refs {
		if _, err := rt.ReleaseWorker(t.Context(), ref); err != nil {
			t.Fatal(err)
		}
		workerEdit(f, ref.Pod.Name, workerPrepare)
	}
	f.edit("services", refs[0].Service.Name, func(o runtime.Object) {
		now := metav1.Now()
		o.(*corev1.Service).DeletionTimestamp = &now
	})
	mark := f.mark()
	if _, err := rt.StartWorker(t.Context(), refs[1], workerToken(t, refs[1])); err == nil {
		t.Fatal("started with a deleting Service")
	}
	if n := len(f.since(mark, "put", "pods")); n != 0 {
		t.Fatalf("%d updates", n)
	}
}
