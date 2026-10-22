package kube

import (
	"context"
	"fmt"

	"github.com/tmc/skygo/internal/backend"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

// exampleDataKube returns a data target as production constructs it: with a
// data block but no native qualification.
func exampleDataKube() *Backend {
	t, err := fromREST(TargetConfig{Name: "kube", Namespace: "workloads", Data: testDataConfig()},
		"example", &rest.Config{Host: "https://127.0.0.1:6443"})
	if err != nil {
		panic(err)
	}
	b := t.Backend.(*Backend)
	b.client = fake.NewSimpleClientset()
	return b
}

func ExampleBackend_StartData() {
	b := exampleDataKube()
	s := dataStart(b, stagedTask())
	// Without a native qualification Check refuses before any API request.
	c, err := b.StartData(context.Background(), s)
	fmt.Println(c.Instance == "", err)
	// Output: true kubernetes data profile is not qualified
}

func ExampleBackend_ObserveData() {
	b := exampleDataKube()
	ref := backend.Reference{ID: dataID, Task: stagedTask(), Binding: b.binding}
	// Only the original Create-return Pod UID authorizes a read.
	_, err := b.ObserveData(context.Background(), ref)
	fmt.Println(err)
	// Output: execution identity mismatch: data pod instance is unavailable
}

func ExampleBackend_RemoveData() {
	b := exampleDataKube()
	// Only the exact recorded names of this attempt are ever deleted; a
	// foreign name is refused before any API request.
	r := backend.DataResources{
		Execution:   backend.Reference{ID: dataID, Task: stagedTask(), Binding: b.binding},
		InputSecret: backend.DataResource{Name: "unrelated-secret", UID: "input-uid"},
	}
	out, err := b.RemoveData(context.Background(), r)
	fmt.Println(out.InputRemoved, err)
	// Output: false execution identity mismatch: data resource names differ
}
