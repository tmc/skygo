package kube

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/client-go/rest"
)

// draUnit returns a DRA Backend without an API server and the declared
// record, claim and Pod of attempt testID for draTask(3).
func draUnit(t *testing.T) (*Backend, draRecord, *resourcev1.ResourceClaim, *corev1.Pod) {
	t.Helper()
	target, _, err := fromRESTFrozen(context.Background(), TargetConfig{Name: "gpu", Namespace: "workloads",
		Resources: ResourcePolicy{MaxAccelerators: 8, Accelerators: map[string]AcceleratorPolicy{
			"h100": {DRA: &DRAPolicy{DeviceClass: "gpu.example.com", Selectors: []string{"s1", "s2"}}},
		}}}, "owner", &rest.Config{Host: "https://k8s.example"})
	if err != nil {
		t.Fatal(err)
	}
	b := target.Backend.(*Backend)
	b.dra = &draRuntime{namespaceUID: "ns-uid", classes: map[string]draClass{"gpu.example.com": {uid: "class-uid", spec: strings.Repeat("e", 64)}}, starting: map[string]int{}}
	r, claim, pod, err := b.draDeclare(testID, draTask(3))
	if err != nil {
		t.Fatal(err)
	}
	return b, r, claim, pod
}

func TestDRAClaimShape(t *testing.T) {
	_, r, c, _ := draUnit(t)
	if c.Name != "skygo-"+testID+"-"+r.Claim.Nonce[:16] || c.Namespace != "workloads" ||
		len(c.OwnerReferences) != 0 || len(c.Finalizers) != 0 || c.GenerateName != "" {
		t.Fatalf("claim metadata = %+v", c.ObjectMeta)
	}
	reqs := c.Spec.Devices.Requests
	if len(reqs) != 1 || len(c.Spec.Devices.Constraints) != 0 || len(c.Spec.Devices.Config) != 0 {
		t.Fatalf("claim devices = %+v", c.Spec.Devices)
	}
	q := reqs[0]
	e := q.Exactly
	if q.Name != "accelerator" || len(q.FirstAvailable) != 0 || e == nil ||
		e.DeviceClassName != "gpu.example.com" || e.AllocationMode != resourcev1.DeviceAllocationModeExactCount || e.Count != 3 ||
		e.AdminAccess != nil || len(e.Tolerations) != 0 || e.Capacity != nil || len(e.Selectors) != 2 ||
		e.Selectors[0].CEL.Expression != "s1" || e.Selectors[1].CEL.Expression != "s2" {
		t.Fatalf("claim request = %+v", q)
	}
	if r.Claim.Nonce == r.Pod.Nonce || r.Count != 3 || r.ClassUID != "class-uid" || r.NamespaceUID != "ns-uid" {
		t.Fatalf("record = %+v", r)
	}
}

func TestDRAPodShape(t *testing.T) {
	_, r, c, p := draUnit(t)
	s := p.Spec
	if p.Name != "skygo-"+testID || len(s.ResourceClaims) != 1 || s.ResourceClaims[0].Name != "accelerator" ||
		s.ResourceClaims[0].ResourceClaimName == nil || *s.ResourceClaims[0].ResourceClaimName != c.Name ||
		s.ResourceClaims[0].ResourceClaimTemplateName != nil {
		t.Fatalf("pod claims = %+v", s.ResourceClaims)
	}
	if len(s.Containers) != 1 || len(s.InitContainers) != 0 || len(s.EphemeralContainers) != 0 || s.Containers[0].Name != "task" {
		t.Fatalf("pod containers = %+v", s.Containers)
	}
	claims := s.Containers[0].Resources.Claims
	if len(claims) != 1 || claims[0].Name != "accelerator" || claims[0].Request != "accelerator" {
		t.Fatalf("container claims = %+v", claims)
	}
	if s.RestartPolicy != corev1.RestartPolicyNever || len(p.OwnerReferences) != 0 || r.Pod.Name != p.Name {
		t.Fatalf("pod = %+v", p.ObjectMeta)
	}
}

// draFixturePod is the admitted Pod of the independent v1.35.9 source fixture
// (K8S-SOURCE-FIXTURE.json expected_admitted_pod_spec), with the claim
// referenced by name instead of the fixture's template, the declared
// metadata of p, and spec fields added by mutate.
func draFixturePod(t *testing.T, p *corev1.Pod, mutate string) []byte {
	t.Helper()
	meta, err := json.Marshal(p.ObjectMeta)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	json.Unmarshal(meta, &m)
	m["uid"] = "pod-uid"
	meta, _ = json.Marshal(m)
	claim := *p.Spec.ResourceClaims[0].ResourceClaimName
	return []byte(`{"apiVersion":"v1","kind":"Pod","metadata":` + string(meta) + `,"spec":{
"automountServiceAccountToken":false,"restartPolicy":"Never",` + mutate + `
"containers":[{"name":"task","image":"example.invalid/task@sha256:1111111111111111111111111111111111111111111111111111111111111111",
 "command":["/task"],"resources":{"limits":{"cpu":"1"},"requests":{"cpu":"1"},"claims":[{"name":"accelerator","request":"accelerator"}]},
 "imagePullPolicy":"IfNotPresent","terminationMessagePath":"/dev/termination-log","terminationMessagePolicy":"File"}],
"resourceClaims":[{"name":"accelerator","resourceClaimName":"` + claim + `"}],
"dnsPolicy":"ClusterFirst","securityContext":{},"terminationGracePeriodSeconds":30,"schedulerName":"default-scheduler",
"enableServiceLinks":true,"serviceAccountName":"default","serviceAccount":"default","priority":0,"preemptionPolicy":"PreemptLowerPriority",
"tolerations":[{"key":"node.kubernetes.io/not-ready","operator":"Exists","effect":"NoExecute","tolerationSeconds":300},
 {"key":"node.kubernetes.io/unreachable","operator":"Exists","effect":"NoExecute","tolerationSeconds":300}]},"status":{}}`)
}

func TestDRAFixtureAdmittedPod(t *testing.T) {
	b, r, _, p := draUnit(t)
	match := func(raw []byte, read bool) error {
		obj, err := decodeDRA("Pod", raw)
		if err == nil {
			_, err = b.draMatch(obj, &r, &r.Pod, read)
		}
		return err
	}
	// Positive control: the independently derived admitted Pod matches the
	// declared Pod as a create response and as a read.
	if err := match(draFixturePod(t, p, ""), false); err != nil {
		t.Fatalf("fixture create response: %v", err)
	}
	if err := match(draFixturePod(t, p, ""), true); err != nil {
		t.Fatalf("fixture read: %v", err)
	}
	for name, mutate := range map[string]string{
		"image pull secrets":  `"imagePullSecrets":[{"name":"copied"}],`,
		"runtime class":       `"runtimeClassName":"gvisor",`,
		"node name on create": `"nodeName":"node-a",`,
		"host network":        `"hostNetwork":true,`,
		"unknown spec field":  `"futureField":true,`,
		"priority class":      `"priorityClassName":"high",`,
	} {
		if err := match(draFixturePod(t, p, mutate), false); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// A template reference in place of the named claim is refused.
	tmpl := strings.Replace(string(draFixturePod(t, p, "")), `"resourceClaimName"`, `"resourceClaimTemplateName"`, 1)
	if err := match([]byte(tmpl), true); err == nil {
		t.Error("template reference accepted")
	}
}

func TestDRAClaimMatch(t *testing.T) {
	b, r, c, _ := draUnit(t)
	encode := func(f func(*resourcev1.ResourceClaim), extra string) []byte {
		x := c.DeepCopy()
		x.APIVersion, x.Kind, x.UID = "resource.k8s.io/v1", "ResourceClaim", "claim-uid"
		if f != nil {
			f(x)
		}
		raw, _ := json.Marshal(x)
		if extra != "" {
			raw = []byte(strings.Replace(string(raw), `"spec":{`, `"spec":{`+extra, 1))
		}
		return raw
	}
	match := func(raw []byte) error {
		obj, err := decodeDRA("ResourceClaim", raw)
		if err == nil {
			_, err = b.draMatch(obj, &r, &r.Claim, true)
		}
		return err
	}
	if err := match(encode(nil, "")); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	// The built-in controller finalizer alone is server output, not identity.
	if err := match(encode(func(x *resourcev1.ResourceClaim) { x.Finalizers = []string{resourcev1.Finalizer} }, "")); err != nil {
		t.Fatalf("built-in finalizer: %v", err)
	}
	yes := true
	for name, raw := range map[string][]byte{
		"admin access": encode(func(x *resourcev1.ResourceClaim) { x.Spec.Devices.Requests[0].Exactly.AdminAccess = &yes }, ""),
		"added selector": encode(func(x *resourcev1.ResourceClaim) {
			x.Spec.Devices.Requests[0].Exactly.Selectors = append(x.Spec.Devices.Requests[0].Exactly.Selectors, x.Spec.Devices.Requests[0].Exactly.Selectors[0])
		}, ""),
		"count": encode(func(x *resourcev1.ResourceClaim) { x.Spec.Devices.Requests[0].Exactly.Count = 4 }, ""),
		"class": encode(func(x *resourcev1.ResourceClaim) { x.Spec.Devices.Requests[0].Exactly.DeviceClassName = "other" }, ""),
		"mode all": encode(func(x *resourcev1.ResourceClaim) {
			x.Spec.Devices.Requests[0].Exactly.AllocationMode = resourcev1.DeviceAllocationModeAll
		}, ""),
		"driver finalizer": encode(func(x *resourcev1.ResourceClaim) {
			x.Finalizers = []string{resourcev1.Finalizer, "driver.example.com/x"}
		}, ""),
		"renamed":       encode(func(x *resourcev1.ResourceClaim) { x.Name = x.Name + "x" }, ""),
		"other nonce":   encode(func(x *resourcev1.ResourceClaim) { x.Annotations[nonceAnnotation] = strings.Repeat("0", 64) }, ""),
		"no uid":        encode(func(x *resourcev1.ResourceClaim) { x.UID = "" }, ""),
		"unknown field": encode(nil, `"futureField":1,`),
	} {
		if err := match(raw); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// An unknown status field is refused, not dropped.
	if err := match([]byte(strings.Replace(string(encode(nil, "")), `"status":{`, `"status":{"futureStatus":1,`, 1))); err == nil {
		t.Error("unknown status field accepted")
	}
}

func TestDRAConsumers(t *testing.T) {
	_, r, c, _ := draUnit(t)
	r.Claim.Send, r.Claim.UID = phasePinned, "claim-uid"
	r.Pod.Send, r.Pod.UID = phasePinned, "pod-uid"
	ours := resourcev1.ResourceClaimConsumerReference{Resource: "pods", Name: r.Pod.Name, UID: "pod-uid"}
	for name, tc := range map[string]struct {
		refs         []resourcev1.ResourceClaimConsumerReference
		own, foreign bool
	}{
		"none":         {nil, false, false},
		"ours":         {[]resourcev1.ResourceClaimConsumerReference{ours}, true, false},
		"other pod":    {[]resourcev1.ResourceClaimConsumerReference{{Resource: "pods", Name: "x", UID: "x"}}, false, true},
		"replaced uid": {[]resourcev1.ResourceClaimConsumerReference{{Resource: "pods", Name: r.Pod.Name, UID: "other"}}, false, true},
		"other group":  {[]resourcev1.ResourceClaimConsumerReference{{APIGroup: "example.com", Resource: "pods", Name: r.Pod.Name, UID: "pod-uid"}}, false, true},
		"both":         {[]resourcev1.ResourceClaimConsumerReference{ours, {Resource: "pods", Name: "x", UID: "x"}}, true, true},
	} {
		x := c.DeepCopy()
		x.Status.ReservedFor = tc.refs
		own, foreign := draConsumers(x, &r)
		if own != tc.own || foreign != tc.foreign {
			t.Errorf("%s: own %v foreign %v", name, own, foreign)
		}
	}
}
