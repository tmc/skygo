package kube

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"k8s.io/client-go/rest"
)

func TestBindingVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/bindings.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors struct {
		Cases []struct {
			Name        string
			CompactJSON string `json:"compact_json"`
			Binding     string
		}
	}
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	for _, v := range vectors.Cases {
		if v.Name == "built-in-local" {
			continue
		} // The root backend owns this format.
		t.Run(v.Name, func(t *testing.T) {
			var value kubeBindingV1
			if err := json.Unmarshal([]byte(v.CompactJSON), &value); err != nil {
				t.Fatal(err)
			}
			got, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != v.CompactJSON || fmt.Sprintf("sha256:%x", sha256.Sum256(got)) != v.Binding {
				t.Fatalf("binding format changed: %s", got)
			}
		})
	}
	c := TargetConfig{Namespace: "skygo-a", Labels: map[string]string{"location": "lab"}, Resources: ResourcePolicy{Accelerators: map[string]AcceleratorPolicy{"nvidia": {ResourceName: "nvidia.com/gpu"}}}}
	got, err := targetBinding(c, &rest.Config{Host: "https://127.0.0.1:6443"}, nil, nil)
	if err != nil || got != "sha256:d8159a158839dfd18454c9df612443194f57a93950b30aa0ed234c52917acc93" {
		t.Fatalf("constructed system-root vector: %s, %v", got, err)
	}
}

func TestBindingChanges(t *testing.T) {
	base := TargetConfig{Name: "cpu", Namespace: "workloads", Labels: map[string]string{"location": "lab"}, Resources: ResourcePolicy{
		MaxCPUMillis: 1000, MaxMemoryBytes: 2048, MaxAccelerators: 2,
		NodeSelectors: map[string][]string{"model": {"A100", "B100"}},
		Accelerators:  map[string]AcceleratorPolicy{"A100": {ResourceName: "nvidia.com/gpu", RequiredNodeSelector: map[string]string{"model": "A100"}}},
	}}
	cfg := &rest.Config{Host: "https://cluster.example", TLSClientConfig: rest.TLSClientConfig{CAData: []byte("fixture CA")}}
	want, err := targetBinding(base, cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		change func(*TargetConfig, *rest.Config)
		equal  bool
	}{
		{"credentials", func(c *TargetConfig, r *rest.Config) {
			c.Kubeconfig = "other-path"
			c.Context = "other-context"
			r.BearerToken = "other-token"
			r.Username = "other-user"
			r.CertData = []byte("other-cert")
			r.KeyData = []byte("other-key")
		}, true},
		{"alias", func(c *TargetConfig, r *rest.Config) { c.Name = "alias" }, true},
		{"set-order", func(c *TargetConfig, r *rest.Config) { c.Resources.NodeSelectors["model"] = []string{"B100", "A100"} }, true},
		{"namespace", func(c *TargetConfig, r *rest.Config) { c.Namespace = "other" }, false},
		{"labels", func(c *TargetConfig, r *rest.Config) { c.Labels = map[string]string{"location": "other"} }, false},
		{"server", func(c *TargetConfig, r *rest.Config) { r.Host = "https://other.example" }, false},
		{"ca", func(c *TargetConfig, r *rest.Config) { r.CAData = []byte("other CA") }, false},
		{"tls-name", func(c *TargetConfig, r *rest.Config) { r.ServerName = "other.example" }, false},
		{"insecure", func(c *TargetConfig, r *rest.Config) { r.Insecure = true }, false},
		{"cpu", func(c *TargetConfig, r *rest.Config) { c.Resources.MaxCPUMillis++ }, false},
		{"memory", func(c *TargetConfig, r *rest.Config) { c.Resources.MaxMemoryBytes++ }, false},
		{"count", func(c *TargetConfig, r *rest.Config) { c.Resources.MaxAccelerators++ }, false},
		{"mapping", func(c *TargetConfig, r *rest.Config) {
			a := c.Resources.Accelerators["A100"]
			a.ResourceName = "other.example/gpu"
			c.Resources.Accelerators["A100"] = a
		}, false},
		{"required-selector", func(c *TargetConfig, r *rest.Config) {
			c.Resources.Accelerators["A100"].RequiredNodeSelector["model"] = "B100"
		}, false},
		{"allowlist", func(c *TargetConfig, r *rest.Config) { c.Resources.NodeSelectors["model"] = []string{"A100"} }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := base
			c.Resources = clonePolicy(base.Resources)
			r := rest.CopyConfig(cfg)
			test.change(&c, r)
			got, err := targetBinding(c, r, nil, nil)
			if err != nil || (got == want) != test.equal {
				t.Fatalf("binding equality %v, want %v: %v", got == want, test.equal, err)
			}
		})
	}
}

func TestCanonicalServer(t *testing.T) {
	for _, test := range []struct{ input, want string }{
		{"HTTPS://EXAMPLE.COM:443", "https://example.com"},
		{"http://EXAMPLE.COM:080/prefix", "http://example.com/prefix"},
		{"https://example.com:6443/a%20b", "https://example.com:6443/a%20b"},
		{"https://example.com/prefix/", "https://example.com/prefix"},
		{"https://[2001:0db8::1]:443/prefix", "https://[2001:db8::1]/prefix"},
		{"https://[::1]:6443", "https://[::1]:6443"},
	} {
		t.Run(test.input, func(t *testing.T) {
			got, err := canonicalServer(test.input)
			if err != nil || got != test.want {
				t.Fatalf("got %q,%v; want %q", got, err, test.want)
			}
		})
	}
	for _, input := range []string{"", "example.com", "ftp://example.com", "https://user:pass@example.com", "https://example.com?", "https://example.com?a=b", "https://example.com#", "https://example.com#part", "https://example.com/a/../b", "https://example.com/%2e%2e/x", "https://example.com/a%2F..%2Fb", "https://[fe80::1%25en0]", "https://example.com:", "https://example.com:65536", "https://example.com:0", "https://example.com/%00", "https://example.com/a%2Fb", "https://example.com/a//b", "https://example.com/a%3Bb", "https://example.com/%61", "https://éxample.com"} {
		t.Run(input, func(t *testing.T) {
			if _, err := canonicalServer(input); err == nil {
				t.Fatalf("accepted %q", input)
			}
		})
	}
}
