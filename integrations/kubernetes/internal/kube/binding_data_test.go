package kube

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"k8s.io/client-go/rest"
)

// bindingDataTarget is the target and connection used by the data vectors.
func bindingDataTarget() (TargetConfig, *rest.Config) {
	return TargetConfig{Name: "worker", Namespace: "jobs", Labels: map[string]string{"location": "lab"}},
		&rest.Config{Host: "https://cluster.example", TLSClientConfig: rest.TLSClientConfig{CAData: []byte("fixture CA")}}
}

func bindingDataProfile(t *testing.T, c *DataConfig) *dataProfile {
	t.Helper()
	d, err := newDataProfile(c)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestDataBindingLegacy(t *testing.T) {
	c := TargetConfig{Namespace: "skygo-a", Labels: map[string]string{"location": "lab"}, Resources: ResourcePolicy{Accelerators: map[string]AcceleratorPolicy{"nvidia": {ResourceName: "nvidia.com/gpu"}}}}
	cfg := &rest.Config{Host: "https://127.0.0.1:6443"}
	for _, q := range []*dataQualification{nil, {id: "qualified"}} {
		got, err := targetBinding(c, cfg, nil, q)
		if err != nil || got != "sha256:d8159a158839dfd18454c9df612443194f57a93950b30aa0ed234c52917acc93" {
			t.Fatalf("legacy binding changed: %s, %v", got, err)
		}
	}
}

func TestDataBindingVector(t *testing.T) {
	c, cfg := bindingDataTarget()
	d := bindingDataProfile(t, configDataFixture())
	got, err := targetBinding(c, cfg, d, &dataQualification{id: "qualification-fixture"})
	if err != nil || got != "sha256:e02456848ac8838220ef0d3f32f21b8b83cb894e1d7c996dc90dee3988fdaeed" {
		t.Fatalf("data binding vector: %s, %v", got, err)
	}
	legacy, err := targetBinding(c, cfg, nil, nil)
	if err != nil || legacy == got {
		t.Fatal("enabling data did not change the binding")
	}
}

func TestDataBindingEncoding(t *testing.T) {
	c, cfg := bindingDataTarget()
	d := bindingDataProfile(t, &DataConfig{
		HelperImage: configDataHelper, HelperSHA256: configDataDigest,
		Origin: "https://data.example.com", CAPEM: configDataPEM,
		TaskImages: []string{configDataImageA},
		References: []DataReference{{Name: "models", PVC: "model-cache", Destinations: []string{"/mnt/skygo/models"}}},
	})
	raw, err := bindingJSON(c, cfg, d, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	if strings.Contains(s, "null") || !strings.Contains(s, `"keys":[]`) || !strings.Contains(s, `"qualification":""`) {
		t.Fatalf("encoding normalizes empty values differently: %s", s)
	}
	if strings.Contains(s, "BEGIN") || strings.Contains(s, "fixture CA") {
		t.Fatal("binding contains certificate bytes")
	}
	for _, field := range []string{`{"format":"skygo-kubernetes-data-target-v1","kind":"kubernetes",`, `"policy":`, `"data":{"layout":"skygo-data-layout-v1","profile_version":"skygo-data-profile-v2","data_policy":{"stage_seconds":600,`, `"service_route":"/skygo.v1.RemoteDataService/","service_methods":["/skygo.v1.RemoteDataService/BeginOutput","/skygo.v1.RemoteDataService/FinishOutput","/skygo.v1.RemoteDataService/PollRelease","/skygo.v1.RemoteDataService/ReadInput","/skygo.v1.RemoteDataService/WriteOutput"],`, `"uid":65532,"gid":65532,"fs_group":65532,"seccomp":"RuntimeDefault","capabilities_drop":["ALL"]`, `"references":[{"name":"models","kind":"pvc","resource":"model-cache","keys":[],"destinations":["/mnt/skygo/models"],"allow_write":false}],"replacement_policy":"name-based-operator-trusted-v1"}}`} {
		if !strings.Contains(s, field) {
			t.Fatalf("missing %s in %s", field, s)
		}
	}
	if strings.Index(s, `"policy":`) > strings.Index(s, `"data":`) {
		t.Fatal("legacy fields do not precede data")
	}
	d.references = map[string]dataReference{}
	raw, err = json.Marshal(bindingData(d, nil))
	if err != nil || !strings.Contains(string(raw), `"references":[]`) {
		t.Fatalf("empty references: %s, %v", raw, err)
	}
}

func TestDataBindingChanges(t *testing.T) {
	c, cfg := bindingDataTarget()
	q := &dataQualification{id: "qualification-fixture"}
	want, err := targetBinding(c, cfg, bindingDataProfile(t, configDataFixture()), q)
	if err != nil {
		t.Fatal(err)
	}
	other := strings.Repeat("ef", 32)
	for _, test := range []struct {
		name   string
		change func(*DataConfig, *dataQualification)
		equal  bool
	}{
		{"task image order", func(d *DataConfig, q *dataQualification) { slices.Reverse(d.TaskImages) }, true},
		{"reference order", func(d *DataConfig, q *dataQualification) { slices.Reverse(d.References) }, true},
		{"key order", func(d *DataConfig, q *dataQualification) { slices.Reverse(d.References[1].Keys) }, true},
		{"destination order", func(d *DataConfig, q *dataQualification) { slices.Reverse(d.References[0].Destinations) }, true},
		{"helper image", func(d *DataConfig, q *dataQualification) {
			d.HelperImage = "registry.example.com/skygo/helper@sha256:" + other
		}, false},
		{"helper digest", func(d *DataConfig, q *dataQualification) { d.HelperSHA256 = other }, false},
		{"task image added", func(d *DataConfig, q *dataQualification) {
			d.TaskImages = append(d.TaskImages, "registry.example.com/team/new@sha256:"+other)
		}, false},
		{"task image removed", func(d *DataConfig, q *dataQualification) { d.TaskImages = d.TaskImages[:1] }, false},
		{"origin host", func(d *DataConfig, q *dataQualification) { d.Origin = "https://other.example.com:8443" }, false},
		{"origin port", func(d *DataConfig, q *dataQualification) { d.Origin = "https://data.example.com:9443" }, false},
		{"ca bytes", func(d *DataConfig, q *dataQualification) { d.CAPEM += "\n" }, false},
		{"reference write", func(d *DataConfig, q *dataQualification) { d.References[0].AllowWrite = false }, false},
		{"reference key", func(d *DataConfig, q *dataQualification) { d.References[1].Keys[0] = "other" }, false},
		{"reference key added", func(d *DataConfig, q *dataQualification) {
			d.References[1].Keys = append(d.References[1].Keys, "other")
		}, false},
		{"destination", func(d *DataConfig, q *dataQualification) { d.References[0].Destinations[0] = "/mnt/skygo/other" }, false},
		{"reference name", func(d *DataConfig, q *dataQualification) { d.References[0].Name = "other" }, false},
		{"reference resource", func(d *DataConfig, q *dataQualification) { d.References[0].PVC = "other" }, false},
		{"reference kind", func(d *DataConfig, q *dataQualification) {
			d.References[0] = DataReference{Name: "models", Secret: "model-cache", Keys: []string{"k"}, Destinations: []string{"/mnt/skygo/models", "/mnt/skygo/cache"}}
		}, false},
		{"reference removed", func(d *DataConfig, q *dataQualification) { d.References = d.References[:1] }, false},
		{"qualification", func(d *DataConfig, q *dataQualification) { q.id = "other" }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			d := configDataFixture()
			q := *q
			test.change(d, &q)
			got, err := targetBinding(c, cfg, bindingDataProfile(t, d), &q)
			if err != nil || (got == want) != test.equal {
				t.Fatalf("binding equality %v, want %v: %v", got == want, test.equal, err)
			}
		})
	}
	var defaults []string
	for _, origin := range []string{"https://data.example.com", "https://data.example.com:443"} {
		d := configDataFixture()
		d.Origin = origin
		got, err := targetBinding(c, cfg, bindingDataProfile(t, d), q)
		if err != nil {
			t.Fatal(err)
		}
		defaults = append(defaults, got)
	}
	if defaults[0] != defaults[1] || defaults[0] == want {
		t.Fatal("default origin port is not canonical")
	}
	unqualified, err := targetBinding(c, cfg, bindingDataProfile(t, configDataFixture()), nil)
	if err != nil || unqualified == want {
		t.Fatal("qualification identity is not bound")
	}
	legacyChange := TargetConfig{Name: c.Name, Namespace: "other", Labels: c.Labels}
	got, err := targetBinding(legacyChange, cfg, bindingDataProfile(t, configDataFixture()), q)
	if err != nil || got == want {
		t.Fatal("legacy field is not bound in data format")
	}
}
