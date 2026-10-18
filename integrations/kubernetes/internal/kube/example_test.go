package kube_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tmc/skygo/integrations/kubernetes/internal/kube"
	"github.com/tmc/skygo/internal/backend"
	"k8s.io/client-go/kubernetes/fake"
)

// The fake API stores native Job objects but runs no Pods or controllers.
func exampleKube() *kube.Backend {
	b, err := kube.New(fake.NewSimpleClientset(), "workloads", "example")
	if err != nil {
		panic(err)
	}
	return b
}

func Example() {
	b := exampleKube()
	ref := backend.Reference{ID: "0123456789abcdef0123456789abcdef", Task: backend.Task{Argv: []string{"trainer"}, Image: "trainer:v1"}}
	fmt.Println(b.Check(ref.Task))
	fmt.Println(b.Start(context.Background(), ref.ID, ref.Task))
	status, err := b.Observe(context.Background(), ref)
	fmt.Println(status.State, err)
	// Output:
	// <nil>
	// <nil>
	// starting <nil>
}

func ExampleBackend_Cancel() {
	b := exampleKube()
	ref := backend.Reference{ID: "0123456789abcdef0123456789abcdef", Task: backend.Task{Argv: []string{"trainer"}, Image: "trainer:v1"}}
	if err := b.Start(context.Background(), ref.ID, ref.Task); err != nil {
		panic(err)
	}
	fmt.Println(b.Cancel(context.Background(), ref))
	status, err := b.Observe(context.Background(), ref)
	// The fake API deletes synchronously; a live cluster may need more observations.
	fmt.Println(status.State, err)
	// Output:
	// <nil>
	// canceled <nil>
}

func ExampleBackend_Logs() {
	b := exampleKube()
	ref := backend.Reference{ID: "0123456789abcdef0123456789abcdef", Task: backend.Task{Argv: []string{"trainer"}, Image: "trainer:v1"}}
	if err := b.Start(context.Background(), ref.ID, ref.Task); err != nil {
		panic(err)
	}
	// A pending Job without a Pod has no output yet.
	chunk, err := b.Logs(context.Background(), ref, 0, backend.MaxChunkBytes)
	fmt.Println(len(chunk.Data), chunk.Complete, err)
	// Output: 0 false <nil>
}

func ExampleFromConfig() {
	dir, err := os.MkdirTemp("", "skygo-kubeconfig-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "config")
	config := `apiVersion: v1
kind: Config
clusters:
- name: test
  cluster: {server: "https://127.0.0.1:6443"}
contexts:
- name: test
  context: {cluster: test, user: test}
users:
- name: test
  user: {}
`
	if err := os.WriteFile(path, []byte(config), 0600); err != nil {
		panic(err)
	}
	b, err := kube.FromConfig(path, "test", "workloads", "example")
	if err != nil {
		panic(err)
	}
	// Construction and capability validation do not contact the API server.
	fmt.Println(b.Check(backend.Task{Argv: []string{"trainer"}, Image: "trainer:v1"}))
	// Output: <nil>
}
