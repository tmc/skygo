package kube

import (
	"errors"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
)

const testBinding = "sha256:392704915c32fe19e8529fee27e474b3dbe43b36fb0dac93e31dfb4294b0e60c"

// testBackend injects an API fixture after the real policy/binding constructor.
func testBackend(client kubernetes.Interface, namespace, owner string) (*Backend, error) {
	t, err := fromREST(TargetConfig{Name: "kube", Namespace: namespace,
		Resources: ResourcePolicy{Accelerators: map[string]AcceleratorPolicy{"nvidia": {ResourceName: "nvidia.com/gpu"}}}},
		owner, &rest.Config{Host: "https://127.0.0.1:6443"})
	if err != nil {
		return nil, err
	}
	if t.Binding != testBinding {
		return nil, errors.New("fixture binding differs")
	}
	b := t.Backend.(*Backend)
	b.client = client
	if c, ok := client.(*fake.Clientset); ok {
		c.PrependReactor("create", "jobs", func(a ktesting.Action) (bool, runtime.Object, error) {
			j := a.(ktesting.CreateAction).GetObject().(*batchv1.Job)
			if j.UID == "" {
				j.UID = types.UID("fixture-" + j.Name)
			}
			return false, nil, nil
		})
	}
	return b, nil
}
