package kube

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const targetDocument = `version: 1
targets:
- name: gpu-a
  namespace: workloads
  kubeconfig: ./credentials/kubeconfig
  context: chosen
  labels: {location: lab}
  resources:
    max_cpu_millis: 2000
    max_memory_bytes: 2147483648
    max_accelerators: 1
    node_selectors:
      skygo.test/pool: [gpu, cpu]
      skygo.test/model: [a100]
    accelerators:
      nvidia: {resource_name: nvidia.com/gpu}
      A100:
        resource_name: nvidia.com/gpu
        required_node_selector: {skygo.test/model: a100}
- name: cpu-b
  namespace: second
  in_cluster: true
`

func configFile(t *testing.T, text string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "targets.yaml")
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestReadConfig(t *testing.T) {
	path := configFile(t, targetDocument)
	c, err := ReadConfig(path, true)
	if err != nil {
		t.Fatal(err)
	}
	targets := c.Targets()
	if len(targets) != 2 {
		t.Fatalf("targets = %d", len(targets))
	}
	a, b := targets[0], targets[1]
	if a.Name != "gpu-a" || a.Namespace != "workloads" || a.Context != "chosen" || a.InCluster || a.Kubeconfig != filepath.Join(filepath.Dir(path), "credentials", "kubeconfig") {
		t.Fatal("explicit route differs")
	}
	if b.Name != "cpu-b" || !b.InCluster || b.Kubeconfig != "" || b.Context != "" {
		t.Fatal("in-cluster route differs")
	}
	if a.Resources.MaxCPUMillis != 2000 || a.Resources.MaxMemoryBytes != 2147483648 || a.Resources.MaxAccelerators != 1 || a.Resources.Accelerators["A100"].RequiredNodeSelector["skygo.test/model"] != "a100" {
		t.Fatal("resource policy differs")
	}
	// The nonexistent kubeconfig is intentional. Parsing must not load credentials
	// or contact a target, and only explicitly declared public labels are returned.
	if !reflect.DeepEqual(a.Labels, map[string]string{"location": "lab"}) {
		t.Fatal("connection settings entered public labels")
	}
}

func TestConfigSnapshots(t *testing.T) {
	c, err := ReadConfig(configFile(t, targetDocument), false)
	if err != nil {
		t.Fatal(err)
	}
	original := c.Targets()
	a, b := c.Targets(), c.Targets()
	a[0].Name = "changed"
	a[0].Labels["location"] = "changed"
	a[0].Resources.NodeSelectors["skygo.test/pool"][0] = "changed"
	a[0].Resources.NodeSelectors["new"] = []string{"changed"}
	a[0].Resources.Accelerators["A100"].RequiredNodeSelector["skygo.test/model"] = "changed"
	delete(a[0].Resources.Accelerators, "nvidia")
	if !reflect.DeepEqual(c.Targets(), original) || !reflect.DeepEqual(b, original) {
		t.Fatal("returned map or slice changed frozen configuration")
	}
	if got := (Config{}).Targets(); len(got) != 0 {
		t.Fatal("zero configuration has targets")
	}
}

func TestConfigExplicitConnection(t *testing.T) {
	for _, tc := range []struct {
		name, fields string
		ok           bool
	}{
		{"in cluster", "in_cluster: true", true},
		{"in cluster empty file fields", "in_cluster: true\n  kubeconfig: ''\n  context: ''", true},
		{"file and context", "kubeconfig: ./config\n  context: chosen", true},
		{"false with file", "in_cluster: false\n  kubeconfig: ./config\n  context: chosen", true},
		{"none", "", false},
		{"false only", "in_cluster: false", false},
		{"file only", "kubeconfig: ./config", false},
		{"context only", "context: chosen", false},
		{"cluster and file", "in_cluster: true\n  kubeconfig: ./config", false},
		{"cluster and context", "in_cluster: true\n  context: chosen", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			text := "version: 1\ntargets:\n- name: worker\n  namespace: jobs\n  " + tc.fields + "\n"
			_, err := ReadConfig(configFile(t, text), false)
			if (err == nil) != tc.ok {
				t.Fatalf("accepted = %v, want %v; %v", err == nil, tc.ok, err)
			}
		})
	}
}

func TestConfigRejectsAmbiguity(t *testing.T) {
	for _, tc := range []struct{ name, text string }{
		{"empty", ""},
		{"nonmapping", "[a,b]"},
		{"future version", strings.Replace(targetDocument, "version: 1", "version: 2", 1)},
		{"quoted version", strings.Replace(targetDocument, "version: 1", "version: '1'", 1)},
		{"duplicate top key", "version: 1\n" + targetDocument},
		{"duplicate policy key", strings.Replace(targetDocument, "max_cpu_millis: 2000", "max_cpu_millis: 2000\n    max_cpu_millis: 1000", 1)},
		{"duplicate label", strings.Replace(targetDocument, "{location: lab}", "{location: lab, location: other}", 1)},
		{"duplicate target", strings.Replace(targetDocument, "name: cpu-b", "name: gpu-a", 1)},
		{"unknown field", strings.Replace(targetDocument, "namespace: second", "namespace: second\n  unrecognized: secret-canary", 1)},
		{"unknown resource", strings.Replace(targetDocument, "max_cpu_millis: 2000", "max_cpu_millis: 2000\n    gpu_magic: secret-canary", 1)},
		{"alias", strings.Replace(targetDocument, "labels: {location: lab}", "labels: &labels {location: lab}\n  resources: *labels", 1)},
		{"unused anchor", strings.Replace(targetDocument, "context: chosen", "context: &unused chosen", 1)},
		{"key anchor", strings.Replace(targetDocument, "context: chosen", "&unused context: chosen", 1)},
		{"merge key", strings.Replace(targetDocument, "labels: {location: lab}", "labels: {<<: {location: lab}}", 1)},
		{"multiple documents", targetDocument + "---\n" + targetDocument},
		{"empty trailing document", targetDocument + "---\n"},
		{"custom tag", strings.Replace(targetDocument, "context: chosen", "context: !custom chosen", 1)},
		{"implicit string coercion", strings.Replace(targetDocument, "context: chosen", "context: 123", 1)},
		{"bool coercion", strings.Replace(targetDocument, "in_cluster: true", "in_cluster: 'true'", 1)},
		{"null map", strings.Replace(targetDocument, "labels: {location: lab}", "labels: null", 1)},
		{"reserved name", strings.Replace(targetDocument, "name: gpu-a", "name: local", 1)},
		{"invalid name", strings.Replace(targetDocument, "name: gpu-a", "name: UPPER", 1)},
		{"invalid namespace", strings.Replace(targetDocument, "namespace: workloads", "namespace: 'bad/name'", 1)},
		{"invalid label key", strings.Replace(targetDocument, "{location: lab}", "{'bad key': lab}", 1)},
		{"invalid label value", strings.Replace(targetDocument, "{location: lab}", "{location: 'bad value'}", 1)},
		{"negative maximum", strings.Replace(targetDocument, "max_cpu_millis: 2000", "max_cpu_millis: -1", 1)},
		{"overflow maximum", strings.Replace(targetDocument, "max_cpu_millis: 2000", "max_cpu_millis: 9223372036854775808", 1)},
		{"duplicate selector value", strings.Replace(targetDocument, "[gpu, cpu]", "[gpu, gpu]", 1)},
		{"empty selector values", strings.Replace(targetDocument, "[gpu, cpu]", "[]", 1)},
		{"invalid selector", strings.Replace(targetDocument, "skygo.test/pool:", "'bad selector':", 1)},
		{"native resource", strings.ReplaceAll(targetDocument, "nvidia.com/gpu", "cpu")},
		{"reserved resource domain", strings.ReplaceAll(targetDocument, "nvidia.com/gpu", "kubernetes.io/gpu")},
		{"quota resource", strings.ReplaceAll(targetDocument, "nvidia.com/gpu", "requests.nvidia.com/gpu")},
		{"missing model selector", strings.Replace(targetDocument, "required_node_selector: {skygo.test/model: a100}", "required_node_selector: {}", 1)},
		{"disallowed model selector", strings.Replace(targetDocument, "required_node_selector: {skygo.test/model: a100}", "required_node_selector: {skygo.test/model: h100}", 1)},
		{"deep input", "version: 1\ntargets: " + strings.Repeat("[", 17) + "x" + strings.Repeat("]", 17)},
		{"large scalar", strings.Replace(targetDocument, "context: chosen", "context: "+strings.Repeat("x", 4097), 1)},
		{"large context", strings.Replace(targetDocument, "context: chosen", "context: "+strings.Repeat("x", 1025), 1)},
		{"large key", strings.Replace(targetDocument, "location:", strings.Repeat("x", 257)+":", 1)},
		{"NUL", strings.Replace(targetDocument, "context: chosen", "context: \"\\0\"", 1)},
		{"oversize document", targetDocument + "#" + strings.Repeat("x", maxConfigBytes)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ReadConfig(configFile(t, tc.text), true)
			if err == nil {
				t.Fatal("invalid configuration accepted")
			}
			if strings.Contains(err.Error(), "secret-canary") || strings.Contains(err.Error(), "unrecognized") {
				t.Fatal("diagnostic exposed input")
			}
		})
	}
}

func TestConfigTargetBound(t *testing.T) {
	for _, tc := range []struct {
		count     int
		local, ok bool
	}{{0, false, false}, {31, true, true}, {32, true, false}, {32, false, true}, {33, false, false}} {
		t.Run(fmt.Sprintf("%d/local=%v", tc.count, tc.local), func(t *testing.T) {
			var text strings.Builder
			text.WriteString("version: 1\ntargets:\n")
			for i := 0; i < tc.count; i++ {
				fmt.Fprintf(&text, "- name: target-%d\n  namespace: jobs\n  in_cluster: true\n", i)
			}
			_, err := ReadConfig(configFile(t, text.String()), tc.local)
			if (err == nil) != tc.ok {
				t.Fatalf("accepted=%v, want %v; %v", err == nil, tc.ok, err)
			}
		})
	}
}

func TestConfigMapBounds(t *testing.T) {
	for _, field := range []string{"labels", "selectors", "values", "accelerators", "required"} {
		for _, count := range []int{32, 33} {
			t.Run(fmt.Sprintf("%s/%d", field, count), func(t *testing.T) {
				doc := struct {
					Version int            `yaml:"version"`
					Targets []TargetConfig `yaml:"targets"`
				}{Version: 1, Targets: []TargetConfig{{Name: "worker", Namespace: "jobs", InCluster: true}}}
				target := &doc.Targets[0]
				target.Labels = make(map[string]string)
				target.Resources.NodeSelectors = make(map[string][]string)
				target.Resources.Accelerators = make(map[string]AcceleratorPolicy)
				required := make(map[string]string)
				for i := 0; i < count; i++ {
					key := fmt.Sprintf("key-%d", i)
					switch field {
					case "labels":
						target.Labels[key] = "value"
					case "selectors":
						target.Resources.NodeSelectors[key] = []string{"value"}
					case "values":
						target.Resources.NodeSelectors["key"] = append(target.Resources.NodeSelectors["key"], key)
					case "accelerators":
						target.Resources.NodeSelectors["model"] = []string{"gpu"}
						target.Resources.Accelerators[key] = AcceleratorPolicy{ResourceName: "example.com/gpu", RequiredNodeSelector: map[string]string{"model": "gpu"}}
					case "required":
						required[key] = "value"
						target.Resources.NodeSelectors[key] = []string{"value"}
					}
				}
				if field == "required" {
					target.Resources.Accelerators["nvidia"] = AcceleratorPolicy{ResourceName: "nvidia.com/gpu", RequiredNodeSelector: required}
				}
				// Marshal typed values to exercise exactly the same public YAML schema.
				data, err := yaml.Marshal(doc)
				if err != nil {
					t.Fatal(err)
				}
				_, err = ReadConfig(configFile(t, string(data)), false)
				if (err == nil) != (count == 32) {
					t.Fatalf("accepted=%v, count=%d; %v", err == nil, count, err)
				}
			})
		}
	}
}

func TestConfigFileAndPath(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "secret-canary")
	for _, path := range []string{missing, filepath.Dir(missing)} {
		_, err := ReadConfig(path, false)
		if err == nil || strings.Contains(err.Error(), path) || strings.Contains(err.Error(), "secret-canary") {
			t.Fatalf("unsafe file diagnostic: %v", err)
		}
	}
	text := strings.Replace(targetDocument, "./credentials/kubeconfig", "$HOME/config", 1)
	path := configFile(t, text)
	c, err := ReadConfig(path, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Targets()[0].Kubeconfig; got != filepath.Join(filepath.Dir(path), "$HOME/config") {
		t.Fatal("environment variable was expanded")
	}
	absolute := filepath.Join(t.TempDir(), "absolute-config")
	c, err = ReadConfig(configFile(t, strings.Replace(targetDocument, "./credentials/kubeconfig", absolute, 1)), false)
	if err != nil {
		t.Fatal(err)
	}
	if c.Targets()[0].Kubeconfig != absolute {
		t.Fatal("absolute kubeconfig changed")
	}
	link := filepath.Join(filepath.Dir(path), "projected.yaml")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadConfig(link, false); err != nil {
		t.Fatal("regular projected configuration symlink rejected", err)
	}
}

func ExampleConfig_Targets() {
	var config Config
	fmt.Println(len(config.Targets()))
	// Output: 0
}

func ExampleReadConfig() {
	dir, err := os.MkdirTemp("", "skygo-targets-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "targets.yaml")
	data := "version: 1\ntargets:\n- name: batch\n  namespace: jobs\n  in_cluster: true\n"
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		panic(err)
	}
	config, err := ReadConfig(path, false)
	if err != nil {
		panic(err)
	}
	for _, target := range config.Targets() {
		fmt.Println(target.Name, target.Namespace)
	}
	// Output: batch jobs
}

// Direct constructors must not bypass the restrictions applied by ReadConfig.
func TestConfigDirectTarget(t *testing.T) {
	longKey := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + ".aa/" + strings.Repeat("d", 63)
	for _, tc := range []struct {
		name   string
		change func(*TargetConfig)
	}{
		{"reserved name", func(c *TargetConfig) { c.Name = "local" }},
		{"namespace", func(c *TargetConfig) { c.Namespace = "" }},
		{"relative path", func(c *TargetConfig) { c.InCluster = false; c.Kubeconfig = "config"; c.Context = "chosen" }},
		{"in cluster context", func(c *TargetConfig) { c.Context = "chosen" }},
		{"large context", func(c *TargetConfig) {
			c.InCluster = false
			c.Kubeconfig = "/config"
			c.Context = strings.Repeat("x", 1025)
		}},
		{"large path", func(c *TargetConfig) {
			c.InCluster = false
			c.Kubeconfig = "/" + strings.Repeat("x", 4096)
			c.Context = "chosen"
		}},
		{"NUL path", func(c *TargetConfig) { c.InCluster = false; c.Kubeconfig = "/config\x00"; c.Context = "chosen" }},
		{"label key bound", func(c *TargetConfig) { c.Labels = map[string]string{longKey: "value"} }},
		{"selector key bound", func(c *TargetConfig) { c.Resources.NodeSelectors = map[string][]string{longKey: {"value"}} }},
		{"required key bound", func(c *TargetConfig) {
			c.Resources.Accelerators = map[string]AcceleratorPolicy{"nvidia": {ResourceName: "nvidia.com/gpu", RequiredNodeSelector: map[string]string{longKey: "value"}}}
		}},
		{"negative maximum", func(c *TargetConfig) { c.Resources.MaxCPUMillis = -1 }},
		{"missing model selector", func(c *TargetConfig) {
			c.Resources.Accelerators = map[string]AcceleratorPolicy{"A100": {ResourceName: "nvidia.com/gpu"}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := TargetConfig{Name: "worker", Namespace: "jobs", InCluster: true}
			if err := validateTarget(c); err != nil {
				t.Fatal(err)
			}
			tc.change(&c)
			if err := validateTarget(c); err == nil {
				t.Fatal("invalid direct target accepted")
			}
		})
	}
	if err := validateTarget(TargetConfig{Name: "worker", Namespace: "jobs", Kubeconfig: "/config", Context: "chosen"}); err != nil {
		t.Fatal(err)
	}
}

func ExampleTargetConfig() {
	target := TargetConfig{Name: "batch", Namespace: "jobs", InCluster: true}
	fmt.Println(target.Name, target.Namespace, target.InCluster)
	// Output: batch jobs true
}

func ExampleResourcePolicy() {
	policy := ResourcePolicy{MaxCPUMillis: 2000, MaxMemoryBytes: 1 << 30}
	fmt.Println(policy.MaxCPUMillis, policy.MaxMemoryBytes)
	// Output: 2000 1073741824
}

func ExampleAcceleratorPolicy() {
	policy := AcceleratorPolicy{
		ResourceName:         "nvidia.com/gpu",
		RequiredNodeSelector: map[string]string{"skygo.test/model": "a100"},
	}
	fmt.Println(policy.ResourceName, policy.RequiredNodeSelector["skygo.test/model"])
	// Output: nvidia.com/gpu a100
}
