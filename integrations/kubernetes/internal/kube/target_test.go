package kube

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

func writeKubeconfig(t *testing.T, host string) string {
	t.Helper()
	data := map[string]any{
		"apiVersion": "v1", "kind": "Config", "current-context": "test",
		"clusters": []any{map[string]any{"name": "test", "cluster": map[string]any{"server": host}}},
		"contexts": []any{map[string]any{"name": "test", "context": map[string]any{"cluster": "test", "user": "test"}}},
		"users":    []any{map[string]any{"name": "test", "user": map[string]any{}}},
	}
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestConstructors(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer server.Close()
	path := writeKubeconfig(t, server.URL)
	named := TargetConfig{Name: "cpu-a", Namespace: "workloads", Kubeconfig: path, Context: "test",
		Labels:    map[string]string{"location": "lab"},
		Resources: ResourcePolicy{NodeSelectors: map[string][]string{"skygo.test/pool": {"s04"}}}}
	target, err := FromTarget(named, "owner")
	if err != nil {
		t.Fatal(err)
	}
	if target.Name != "cpu-a" || target.Kind != "kubernetes" || target.Binding == "" || !target.SupportsResourceLimits || !target.SupportsNodeSelector || len(target.AcceleratorNames) != 0 {
		t.Fatalf("named target metadata: %+v", target)
	}
	task := backend.Task{Argv: []string{"true"}, Image: "image", NodeSelector: map[string]string{"skygo.test/pool": "s04"}}
	if err := target.Backend.Check(task); err != nil {
		t.Fatal(err)
	}
	legacy, err := FromConfig(path, "", "workloads", "owner")
	if err != nil {
		t.Fatal(err)
	}
	if legacy.Name != "kubernetes" || legacy.Binding == "" || !legacy.SupportsResourceLimits || legacy.SupportsNodeSelector || len(legacy.Labels) != 0 || len(legacy.AcceleratorNames) != 1 || legacy.AcceleratorNames[0] != "nvidia" {
		t.Fatalf("legacy target metadata: %+v", legacy)
	}
	if err := legacy.Backend.Check(task); err == nil {
		t.Fatal("legacy selector unexpectedly admitted")
	}
	task.NodeSelector = nil
	task.Accelerator, task.AcceleratorCount = "nvidia", 1
	if err := legacy.Backend.Check(task); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("construction or Check made an API call")
	}
	for _, mutate := range []func(*TargetConfig){
		func(c *TargetConfig) { c.Context = "" },
		func(c *TargetConfig) { c.Kubeconfig = "relative" },
		func(c *TargetConfig) { c.InCluster = true },
		func(c *TargetConfig) { c.Namespace = "bad/value" },
		func(c *TargetConfig) { c.Resources.MaxCPUMillis = -1 },
	} {
		c := named
		mutate(&c)
		if _, err := FromTarget(c, "owner"); err == nil {
			t.Fatal("invalid direct constructor input accepted")
		}
	}
}

func TestTargetMutationIsolation(t *testing.T) {
	c := TargetConfig{Name: "cpu", Namespace: "workloads", Labels: map[string]string{"location": "lab"}, Resources: ResourcePolicy{
		MaxCPUMillis:  500,
		NodeSelectors: map[string][]string{"model": {"A100", "B100"}},
		Accelerators:  map[string]AcceleratorPolicy{"A100": {ResourceName: "nvidia.com/gpu", RequiredNodeSelector: map[string]string{"model": "A100"}}},
	}}
	cfg := &rest.Config{Host: "https://127.0.0.1:6443"}
	x, err := fromREST(c, "owner", cfg)
	if err != nil {
		t.Fatal(err)
	}
	original := x.Binding
	c.Labels["location"] = "other"
	c.Resources.NodeSelectors["model"][0] = "other"
	c.Resources.Accelerators["A100"].RequiredNodeSelector["model"] = "other"
	c.Resources.Accelerators["A100"] = AcceleratorPolicy{ResourceName: "other.example/gpu"}
	x.Labels["location"] = "mutated"
	x.AcceleratorNames[0] = "mutated"
	cfg.Host = "https://other.example"
	task := backend.Task{Argv: []string{"work"}, Image: "image", Accelerator: "A100", AcceleratorCount: 1, NodeSelector: map[string]string{"model": "A100"}}
	b := x.Backend.(*Backend)
	if err := b.Check(task); err != nil {
		t.Fatal(err)
	}
	if b.binding != original || b.policy.Accelerators["A100"].ResourceName != "nvidia.com/gpu" {
		t.Fatal("constructor policy mutated through input/output alias")
	}
}

func TestFrozenCAFile(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/version" {
			t.Errorf("unexpected request path %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"gitVersion":"v1.32.2"}`))
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	path := filepath.Join(t.TempDir(), "authority")
	if err := os.WriteFile(path, ca, 0600); err != nil {
		t.Fatal(err)
	}
	c := TargetConfig{Name: "cpu", Namespace: "workloads"}
	x, err := fromREST(c, "owner", &rest.Config{Host: server.URL, TLSClientConfig: rest.TLSClientConfig{CAFile: path}})
	if err != nil {
		t.Fatal(err)
	}
	y, err := fromREST(c, "owner", &rest.Config{Host: server.URL, TLSClientConfig: rest.TLSClientConfig{CAData: ca}})
	if err != nil {
		t.Fatal(err)
	}
	if x.Binding != y.Binding {
		t.Fatal("CAData and CAFile differ")
	}
	if calls.Load() != 0 {
		t.Fatal("constructor contacted API")
	}
	if err := os.WriteFile(path, []byte("invalid new authority"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := x.Backend.(*Backend).client.Discovery().ServerVersion(); err != nil {
		t.Fatalf("frozen CA no longer trusted: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatal("expected actual TLS request")
	}
	if _, err := fromREST(c, "owner", &rest.Config{Host: server.URL, TLSClientConfig: rest.TLSClientConfig{CAFile: path}}); err == nil {
		t.Fatal("invalid replacement CA accepted")
	}
}

func TestConstructorErrorsAreRedacted(t *testing.T) {
	canary := "private-context-or-token-canary"
	_, err := FromTarget(TargetConfig{Name: "cpu", Namespace: "workloads", Kubeconfig: filepath.Join(t.TempDir(), canary), Context: canary}, "owner")
	if err == nil || strings.Contains(err.Error(), canary) {
		t.Fatalf("loader error: %v", err)
	}
	_, err = fromREST(TargetConfig{Name: "cpu", Namespace: "workloads"}, "owner", &rest.Config{Host: "https://user:" + canary + "@example.com"})
	if err == nil || strings.Contains(err.Error(), canary) {
		t.Fatalf("URL error: %v", err)
	}
}

func TestNoDefaultResourceInjection(t *testing.T) {
	b, err := testBackend(fake.NewSimpleClientset(), "workloads", "owner")
	if err != nil {
		t.Fatal(err)
	}
	task := backend.Task{Argv: []string{"work"}, Image: "image"}
	if err := b.Start(context.Background(), testID, task); err != nil {
		t.Fatal(err)
	}
	j, err := b.get(context.Background(), backend.Reference{ID: testID, Task: task, Binding: b.binding})
	if err != nil {
		t.Fatal(err)
	}
	p := j.Spec.Template.Spec
	if len(p.NodeSelector) != 0 || len(p.Containers[0].Resources.Requests) != 0 || len(p.Containers[0].Resources.Limits) != 0 {
		t.Fatalf("injected defaults: %+v", p)
	}
}

func TestActualAPIPrefix(t *testing.T) {
	var got string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.URL.EscapedPath()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"JobList","apiVersion":"batch/v1","items":[]}`))
	}))
	defer server.Close()
	x, err := fromREST(TargetConfig{Name: "cpu", Namespace: "workloads"}, "owner", &rest.Config{Host: server.URL + "/prefix%20space/"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = x.Backend.(*Backend).client.BatchV1().Jobs("workloads").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got != "/prefix%20space/apis/batch/v1/namespaces/workloads/jobs" {
		t.Fatalf("actual escaped API path %q", got)
	}
	for _, path := range []string{"/prefix%2Ftenant", "/prefix//tenant"} {
		got = ""
		if _, err := fromREST(TargetConfig{Name: "cpu", Namespace: "workloads"}, "owner", &rest.Config{Host: server.URL + path}); err == nil {
			t.Fatal("accepted a path client-go would rewrite")
		}
		if got != "" {
			t.Fatal("rejected constructor made a request")
		}
	}
}
