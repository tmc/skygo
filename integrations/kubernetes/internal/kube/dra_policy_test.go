package kube

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
)

// draConfigYAML returns a one-target configuration whose h100 accelerator
// block is accel, indented under accelerators.
func draConfigYAML(accel string, extra string) []byte {
	return []byte(`version: 1
targets:
- name: gpu
  in_cluster: true
  namespace: workloads
` + extra + `  resources:
    max_accelerators: 4
    accelerators:
      h100:
` + accel)
}

func TestDRAPolicyConfig(t *testing.T) {
	long := strings.Repeat("x", maxDRASelectorBytes+1)
	nine := "        dra:\n          device_class: gpu.example.com\n          selectors:\n"
	for i := range 9 {
		nine += "          - 'device.driver == \"d" + string(rune('a'+i)) + "\"'\n"
	}
	data := "  data:\n    helper_image: registry.example/h@sha256:" + strings.Repeat("a", 64) + "\n"
	for _, tc := range []struct {
		name, accel, extra, err string
	}{
		{"valid", "        dra:\n          device_class: gpu.example.com\n          selectors:\n          - 'device.driver == \"gpu.example.com\"'\n", "", ""},
		{"no selectors", "        dra:\n          device_class: gpu.example.com\n", "", ""},
		{"with resource name", "        resource_name: example.com/gpu\n        dra:\n          device_class: gpu.example.com\n", "", "dra accelerator conflicts"},
		{"with node selector", "        required_node_selector: {zone: a}\n        dra:\n          device_class: gpu.example.com\n", "", "dra accelerator conflicts"},
		{"bad class", "        dra:\n          device_class: Bad_Class\n", "", "invalid dra device class"},
		{"empty selector", "        dra:\n          device_class: gpu.example.com\n          selectors: ['']\n", "", "invalid dra selector"},
		{"long selector", "        dra:\n          device_class: gpu.example.com\n          selectors: ['" + long + "']\n", "", "invalid"},
		{"duplicate selector", "        dra:\n          device_class: gpu.example.com\n          selectors: ['a', 'a']\n", "", "duplicate dra selector"},
		{"nine selectors", nine, "", "dra selectors exceed 8"},
		// Task-irrelevant native fields are not configurable: admin access,
		// allocation mode or counts cannot be smuggled through policy.
		{"unknown dra field", "        dra:\n          device_class: gpu.example.com\n          admin_access: 'true'\n", "", "unknown field"},
		{"allocation mode field", "        dra:\n          device_class: gpu.example.com\n          allocation_mode: All\n", "", "unknown field"},
		{"with data block", "        dra:\n          device_class: gpu.example.com\n", data, "conflict with a data block"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := readConfig(draConfigYAML(tc.accel, tc.extra), "/", false)
			if tc.err == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("err = %v, want %q", err, tc.err)
			}
		})
	}
}

// TestDRAPolicyDataConflict checks the data/DRA conflict directly, since a
// complete data block is long: validateTarget refuses both together.
func TestDRAPolicyDataConflict(t *testing.T) {
	tc := TargetConfig{Name: "gpu", InCluster: true, Namespace: "workloads",
		Resources: ResourcePolicy{Accelerators: map[string]AcceleratorPolicy{"h100": {DRA: &DRAPolicy{DeviceClass: "gpu.example.com"}}}},
		Data:      &DataConfig{}}
	if err := validateTarget(tc); err == nil || !strings.Contains(err.Error(), "dra accelerators conflict with a data block") {
		t.Fatalf("err = %v", err)
	}
	tc.Data = nil
	if err := validateTarget(tc); err != nil {
		t.Fatalf("positive control: %v", err)
	}
}

func TestDRAPolicyClone(t *testing.T) {
	config, err := readConfig(draConfigYAML("        dra:\n          device_class: gpu.example.com\n          selectors: ['a', 'b']\n", ""), "/", false)
	if err != nil {
		t.Fatal(err)
	}
	first := config.Targets()
	p := first[0].Resources.Accelerators["h100"].DRA
	p.Selectors[0], p.DeviceClass = "mutated", "mutated"
	again := config.Targets()[0].Resources.Accelerators["h100"].DRA
	if again.DeviceClass != "gpu.example.com" || !slices.Equal(again.Selectors, []string{"a", "b"}) {
		t.Fatalf("snapshot shared state: %+v", again)
	}
	cloned := clonePolicy(config.Targets()[0].Resources)
	cloned.Accelerators["h100"].DRA.Selectors[1] = "mutated"
	if config.Targets()[0].Resources.Accelerators["h100"].DRA.Selectors[1] != "b" {
		t.Fatal("clonePolicy shared selectors")
	}
}

func TestDRABinding(t *testing.T) {
	rc := &rest.Config{Host: "https://k8s.example"}
	plain := TargetConfig{Name: "gpu", Namespace: "workloads",
		Resources: ResourcePolicy{Accelerators: map[string]AcceleratorPolicy{"nvidia": {ResourceName: "nvidia.com/gpu"}}}}
	legacy, err := bindingJSON(plain, rc, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Positive control: a target without DRA keeps the legacy format bytes.
	if !strings.HasPrefix(string(legacy), `{"format":"skygo-kubernetes-target-v1",`) || strings.Contains(string(legacy), `"dra"`) {
		t.Fatalf("legacy binding changed: %s", legacy)
	}
	with := func(class string, selectors ...string) string {
		t.Helper()
		tc := plain
		tc.Resources = clonePolicy(plain.Resources)
		tc.Resources.Accelerators["h100"] = AcceleratorPolicy{DRA: &DRAPolicy{DeviceClass: class, Selectors: selectors}}
		b, err := targetBinding(tc, rc, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	base := with("gpu.example.com", "a", "b")
	raw, _ := bindingJSON(TargetConfig{Name: "gpu", Namespace: "workloads", Resources: ResourcePolicy{Accelerators: map[string]AcceleratorPolicy{
		"h100": {DRA: &DRAPolicy{DeviceClass: "gpu.example.com", Selectors: []string{"a", "b"}}}}}}, rc, nil, nil)
	var decoded struct {
		Format string                  `json:"format"`
		DRA    map[string]bindingDRAV1 `json:"dra"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	want := bindingDRAV1{"gpu.example.com", []string{"a", "b"}, "ExactCount", "accelerator", "resourceClaimName"}
	if got := decoded.DRA["h100"]; decoded.Format != "skygo-kubernetes-dra-target-v1" || got.DeviceClass != want.DeviceClass ||
		!slices.Equal(got.Selectors, want.Selectors) || got.AllocationMode != want.AllocationMode || got.Request != want.Request || got.Reference != want.Reference {
		t.Fatalf("dra binding = %s", raw)
	}
	for name, other := range map[string]string{
		"class":            with("other.example.com", "a", "b"),
		"selector order":   with("gpu.example.com", "b", "a"),
		"added selector":   with("gpu.example.com", "a", "b", "c"),
		"removed selector": with("gpu.example.com", "a"),
	} {
		if other == base {
			t.Errorf("%s change kept the binding", name)
		}
	}
	if with("gpu.example.com", "a", "b") != base {
		t.Error("binding is not deterministic")
	}
}

func TestDRACheck(t *testing.T) {
	target, _, err := fromRESTFrozen(context.Background(), TargetConfig{Name: "gpu", Namespace: "workloads",
		Resources: ResourcePolicy{MaxAccelerators: 4, Accelerators: map[string]AcceleratorPolicy{
			"nvidia": {ResourceName: "nvidia.com/gpu"},
			"h100":   {DRA: &DRAPolicy{DeviceClass: "gpu.example.com"}},
		}}}, "owner", &rest.Config{Host: "https://k8s.example"})
	if err != nil {
		t.Fatal(err)
	}
	// Plain constructors never advertise or accept DRA accelerators.
	if !slices.Equal(target.AcceleratorNames, []string{"nvidia"}) {
		t.Fatalf("accelerator names = %v", target.AcceleratorNames)
	}
	b := target.Backend.(*Backend)
	if err := b.Check(draTask(1)); err != errDRAUnavailable {
		t.Fatalf("plain target: %v", err)
	}
	ordinary := draTask(1)
	ordinary.Accelerator = "nvidia"
	if err := b.Check(ordinary); err != nil {
		t.Fatalf("ordinary positive control: %v", err)
	}
	if q := b.podSpec(ordinary).Containers[0].Resources.Limits[corev1.ResourceName("nvidia.com/gpu")]; q.Value() != 1 {
		t.Fatalf("ordinary extended resource missing: %v", q)
	}
	b.dra = &draRuntime{}
	if err := b.Check(draTask(2)); err != nil {
		t.Fatalf("dra positive control: %v", err)
	}
	spec := b.podSpec(draTask(2))
	for name := range spec.Containers[0].Resources.Limits {
		if name != corev1.ResourceCPU {
			t.Fatalf("dra pod has extended resource %q", name)
		}
	}
	retry := draTask(1)
	retry.Retry = &backend.RetryPolicy{MaxAttempts: 2}
	if p, err := backend.NormalizeRetry(retry.Retry); err == nil && p != nil {
		retry.Retry = p
	}
	data := draTask(1)
	data.Outputs = []string{"out"}
	tooMany := draTask(5)
	for name, tc := range map[string]struct {
		task backend.Task
		err  string
	}{
		"retry":     {retry, "retry"},
		"outputs":   {data, "data"},
		"too many":  {tooMany, "exceeds target policy"},
		"zero":      {func() backend.Task { x := draTask(0); return x }(), "specified together"},
		"selectors": {func() backend.Task { x := draTask(1); x.NodeSelector = map[string]string{"zone": "a"}; return x }(), "node selector"},
	} {
		if err := b.Check(tc.task); err == nil || !strings.Contains(err.Error(), tc.err) {
			t.Errorf("%s: err = %v, want %q", name, err, tc.err)
		}
	}
}
