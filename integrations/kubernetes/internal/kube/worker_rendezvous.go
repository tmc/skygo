package kube

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"slices"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/scheme"
)

// workerRendezvousPort is the name of the rendezvous Service port.
const workerRendezvousPort = "rendezvous"

// workerSliceController is the discoveryv1.LabelManagedBy value of the
// EndpointSlice controller in kube-controller-manager, the standard producer
// of a selector Service's slices.
const workerSliceController = "endpointslice-controller.k8s.io"

// service returns the rendezvous Service of the WorkerTCP group of rank 0's
// plan p. It is attempt-specific: its name, its labels and its selector
// carry the attempt ID, and the selector also requires rank 0. It carries
// the skygo finalizer, so it disappears only after RemoveWorker.
func (rt *workerRuntime) service(p workerPodPlan) (*corev1.Service, error) {
	pod, err := rt.pod(p)
	if err != nil {
		return nil, err
	}
	if p.Mode != workerTCP || p.Rank != 0 {
		return nil, errors.New("worker rendezvous belongs to rank 0 of a tcp group")
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: workerServiceName(p.AttemptID), Namespace: rt.b.namespace,
			Labels: map[string]string{
				ownerLabel:         rt.b.owner,
				workerJobLabel:     p.JobID,
				workerAttemptLabel: p.AttemptID,
				workerWorldLabel:   pod.Labels[workerWorldLabel],
			},
			Annotations: map[string]string{
				taskAnnotation:          pod.Annotations[taskAnnotation],
				bindingAnnotation:       rt.b.binding,
				workerGroupAnnotation:   pod.Annotations[workerGroupAnnotation],
				workerProfileAnnotation: workerBarrierProfile,
			},
			Finalizers: []string{workerFinalizer},
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Selector: map[string]string{
				ownerLabel:         rt.b.owner,
				workerJobLabel:     p.JobID,
				workerAttemptLabel: p.AttemptID,
				workerRankLabel:    "0",
			},
			Ports: []corev1.ServicePort{{Name: workerRendezvousPort, Protocol: corev1.ProtocolTCP,
				Port: workerMasterPort, TargetPort: intstr.FromInt32(workerMasterPort)}},
			SessionAffinity: corev1.ServiceAffinityNone,
		},
	}, nil
}

// workerServiceFacts are the deletion facts of an original Service.
type workerServiceFacts struct {
	deleting  bool
	finalized bool // being deleted with the skygo finalizer already removed
}

// serviceMatches reports the facts of got if it is the original rendezvous
// Service of p with UID uid. Identity metadata, the finalizer, the type, the
// selector and the one port must be exact; the server-assigned cluster IPs,
// IP families and traffic policies are not compared, but the Service must
// have a cluster IP and no external exposure.
func (rt *workerRuntime) serviceMatches(got *corev1.Service, p workerPodPlan, uid types.UID) (workerServiceFacts, error) {
	mismatch := fmt.Errorf("%w: kubernetes worker service differs", backend.ErrIdentityMismatch)
	want, err := rt.service(p)
	if err != nil {
		return workerServiceFacts{}, err
	}
	if uid == "" || !validInstance(string(uid)) || got == nil || got.UID != uid ||
		!profileMetaMatches(want.ObjectMeta, got.ObjectMeta) {
		return workerServiceFacts{}, mismatch
	}
	var f workerServiceFacts
	switch {
	case slices.Equal(got.Finalizers, want.Finalizers):
	case got.DeletionTimestamp != nil && len(got.Finalizers) == 0:
		f.finalized = true
	default:
		return workerServiceFacts{}, mismatch
	}
	f.deleting = got.DeletionTimestamp != nil
	g, w := &got.Spec, &want.Spec
	if g.Type != w.Type || !maps.Equal(g.Selector, w.Selector) || len(g.Ports) != 1 ||
		g.Ports[0].Name != w.Ports[0].Name || g.Ports[0].Protocol != w.Ports[0].Protocol ||
		g.Ports[0].Port != w.Ports[0].Port || g.Ports[0].TargetPort != w.Ports[0].TargetPort ||
		g.Ports[0].NodePort != 0 || g.ClusterIP == "" || g.ClusterIP == corev1.ClusterIPNone ||
		g.SessionAffinity != w.SessionAffinity || g.SessionAffinityConfig != nil ||
		g.PublishNotReadyAddresses || len(g.ExternalIPs) != 0 || g.ExternalName != "" ||
		g.LoadBalancerIP != "" || len(g.LoadBalancerSourceRanges) != 0 || g.LoadBalancerClass != nil ||
		g.ExternalTrafficPolicy != "" || g.HealthCheckNodePort != 0 || g.TrafficDistribution != nil {
		return workerServiceFacts{}, mismatch
	}
	return f, nil
}

// readService gets m's group Service by its original name, in a consistent
// read. A Service that is missing or has another UID has vanished, before
// any comparison with its expected Service.
func (rt *workerRuntime) readService(ctx context.Context, m workerMember) (*corev1.Service, error) {
	got, err := rt.getService(ctx, m.ref.Service.Name)
	if err != nil {
		return nil, fmt.Errorf("kubernetes worker service: %w", workerVanished(err))
	}
	if string(got.UID) != m.ref.Service.UID {
		return nil, fmt.Errorf("%w: %w: kubernetes worker service original uid absent", backend.ErrWorkerVanished, backend.ErrIdentityMismatch)
	}
	return got, nil
}

// serviceLive requires m's group Service to be the original and not deleting.
func (rt *workerRuntime) serviceLive(ctx context.Context, m workerMember) error {
	p := m.plan
	p.Rank = 0
	got, err := rt.readService(ctx, m)
	if err != nil {
		return err
	}
	f, err := rt.serviceMatches(got, p, types.UID(m.ref.Service.UID))
	if err != nil {
		return err
	}
	if f.deleting {
		return errors.New("kubernetes worker service is being deleted")
	}
	return nil
}

// rendezvous observes rank 0 member m's Service. pod is m's original Pod as
// the caller's read in the same call returned it. The address is the
// Service's namespace-qualified DNS name and workerMasterPort. It is Routed
// only when the Service is the original and live and workerRouted accepts
// every EndpointSlice labelled with its name. A stale attempt's Pod, a
// replacement Pod at the rank 0 name or a foreign endpoint or slice therefore
// never routes. Routed is an observation of the producer's slices, not of
// the network; see workerRouted.
func (rt *workerRuntime) rendezvous(ctx context.Context, m workerMember, pod *corev1.Pod) (backend.WorkerRendezvous, error) {
	r := backend.WorkerRendezvous{Service: m.ref.Service,
		Host: workerServiceDNS(m.plan.AttemptID, rt.b.namespace), Port: workerMasterPort}
	got, err := rt.readService(ctx, m)
	if err != nil {
		return backend.WorkerRendezvous{}, err
	}
	f, err := rt.serviceMatches(got, m.plan, types.UID(m.ref.Service.UID))
	if err != nil {
		return backend.WorkerRendezvous{}, err
	}
	if f.deleting {
		return r, nil
	}
	list, err := rt.listWorkerSlices(ctx, labels.Set{discoveryv1.LabelServiceName: got.Name}.String())
	if err != nil {
		return backend.WorkerRendezvous{}, err
	}
	r.Routed = workerRouted(list, rt.b.namespace, m.ref.Service, pod)
	return r, nil
}

// workerRouted reports whether list, every EndpointSlice labelled with the
// name of the original Service svc, routes only to the original rank 0 Pod
// pod in namespace and has at least one ready route. It applies the producer
// contract of the standard EndpointSlice controller for a selector Service:
//
//   - every slice is labelled managed-by workerSliceController, has the
//     address type IPv4 or IPv6 and has exactly one owner reference, the
//     controller reference to the Service with svc's name and original UID;
//   - every endpoint of every slice targets pod by kind, namespace, name and
//     UID, and has at least one address, each of the slice's family and one
//     of pod's IPs;
//   - a ready route is an endpoint in a slice with the TCP rendezvous port
//     whose Ready is true, whose Serving is not false and whose Terminating
//     is not true. Per discoveryv1.EndpointConditions a nil Serving defers to
//     Ready and a nil Terminating means not terminating; a nil Ready is not
//     ready.
//
// A slice or endpoint that fails is never skipped: it makes the result
// false. pod's IPs are those of the caller's earlier read; the reads are
// separate and no snapshot is claimed.
//
// This is a stricter observation profile, not an authenticated controller
// identity and not reachability of a listening socket: a namespace writer
// can make a slice with the same labels and owner reference, so the
// namespace's writers are trusted.
func workerRouted(list []discoveryv1.EndpointSlice, namespace string, svc backend.WorkerResource, pod *corev1.Pod) bool {
	ips := make(map[netip.Addr]bool)
	for _, ip := range append([]string{pod.Status.PodIP}, workerPodIPs(pod)...) {
		if a, err := netip.ParseAddr(ip); err == nil {
			ips[a] = true
		}
	}
	ready := false
	for _, s := range list {
		if !workerSliceOwned(s, svc) {
			return false
		}
		port := slices.ContainsFunc(s.Ports, func(p discoveryv1.EndpointPort) bool {
			return p.Port != nil && *p.Port == workerMasterPort && p.Protocol != nil && *p.Protocol == corev1.ProtocolTCP &&
				p.Name != nil && *p.Name == workerRendezvousPort
		})
		for _, e := range s.Endpoints {
			ref := e.TargetRef
			if ref == nil || ref.Kind != "Pod" || ref.Namespace != namespace || ref.Name != pod.Name || ref.UID != pod.UID ||
				len(e.Addresses) == 0 {
				return false
			}
			for _, x := range e.Addresses {
				a, err := netip.ParseAddr(x)
				if err != nil || a.Is4() != (s.AddressType == discoveryv1.AddressTypeIPv4) || !ips[a] {
					return false
				}
			}
			c := e.Conditions
			if port && c.Ready != nil && *c.Ready && (c.Serving == nil || *c.Serving) &&
				(c.Terminating == nil || !*c.Terminating) {
				ready = true
			}
		}
	}
	return ready
}

// workerPodIPs returns the IPs in p's status.podIPs.
func workerPodIPs(p *corev1.Pod) []string {
	ips := make([]string, len(p.Status.PodIPs))
	for i, ip := range p.Status.PodIPs {
		ips[i] = ip.IP
	}
	return ips
}

// workerSliceOwned reports whether s carries the standard producer's
// managed-by label, a supported address family and, as its only owner
// reference, the controller reference to the original Service svc.
func workerSliceOwned(s discoveryv1.EndpointSlice, svc backend.WorkerResource) bool {
	if s.Labels[discoveryv1.LabelManagedBy] != workerSliceController ||
		s.AddressType != discoveryv1.AddressTypeIPv4 && s.AddressType != discoveryv1.AddressTypeIPv6 ||
		len(s.OwnerReferences) != 1 {
		return false
	}
	o := s.OwnerReferences[0]
	return o.APIVersion == "v1" && o.Kind == "Service" && o.Name == svc.Name && o.UID == types.UID(svc.UID) &&
		o.Controller != nil && *o.Controller
}

// removeService retires rank 0 member m's Service, after its Pod; see
// RemoveWorker.
func (rt *workerRuntime) removeService(ctx context.Context, m workerMember, attested bool) (removed, dispatched bool, err error) {
	uid := types.UID(m.ref.Service.UID)
	gone := func(got *corev1.Service, err error) bool {
		return apierrors.IsNotFound(err) || err == nil && got.UID != uid
	}
	got, err := rt.getService(ctx, m.ref.Service.Name)
	if gone(got, err) && !errors.Is(err, backend.ErrWorkerUnsettled) {
		if attested {
			return true, false, nil
		}
		return false, false, fmt.Errorf("%w: kubernetes worker service", backend.ErrWorkerVanished)
	}
	if err != nil {
		return false, false, err
	}
	f, err := rt.serviceMatches(got, m.plan, uid)
	if err != nil {
		return false, false, err
	}
	if !f.deleting {
		rv := got.ResourceVersion
		dispatched, err = rt.deleteWorkerResource(ctx, "services", m.ref.Service, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}})
		if err != nil {
			return false, dispatched, err
		}
		got, err = rt.getService(ctx, m.ref.Service.Name)
		if err != nil {
			return false, dispatched, workerVanished(err)
		}
		f, err = rt.serviceMatches(got, m.plan, uid)
		if err != nil {
			return false, dispatched, err
		}
		if !f.deleting {
			return false, dispatched, errWorkerPodChanged
		}
	}
	if !f.finalized {
		u := got.DeepCopy()
		u.Finalizers = nil
		if _, err = rt.updateService(ctx, u); err != nil {
			return false, dispatched, err
		}
	}
	got, err = rt.getService(ctx, m.ref.Service.Name)
	if gone(got, err) && !errors.Is(err, backend.ErrWorkerUnsettled) {
		return true, dispatched, nil
	}
	if err != nil {
		return false, dispatched, err
	}
	return false, dispatched, errWorkerRemoving
}

// listWorkerSlices reads a bounded paginated consistent list. Each page has
// its own original request and closure proof before another page is requested.
func (rt *workerRuntime) listWorkerSlices(ctx context.Context, selector string) ([]discoveryv1.EndpointSlice, error) {
	options := metav1.ListOptions{LabelSelector: selector, Limit: 128}
	var out []discoveryv1.EndpointSlice
	for page := 0; page < 16; page++ {
		list := new(discoveryv1.EndpointSliceList)
		err := rt.http.client.DiscoveryV1().RESTClient().Get().Namespace(rt.b.namespace).Resource("endpointslices").VersionedParams(&options, scheme.ParameterCodec).MaxRetries(0).Do(rt.http.request(ctx)).Into(list)
		if err = errors.Join(err, rt.http.check(ctx)); err != nil {
			return nil, err
		}
		if len(out)+len(list.Items) > 1024 {
			return nil, errors.New("worker rendezvous slice limit reached")
		}
		out = append(out, list.Items...)
		if list.Continue == "" {
			return out, nil
		}
		if list.Continue == options.Continue {
			return nil, errors.New("worker rendezvous list did not advance")
		}
		options.Continue = list.Continue
	}
	return nil, errors.New("worker rendezvous page limit reached")
}
