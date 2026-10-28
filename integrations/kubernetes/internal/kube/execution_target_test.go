package kube

import (
	"context"
	"fmt"
	"testing"
)

func ExampleFromTargetWithState() {
	t := TargetConfig{Name: "training", Namespace: "workloads", Kubeconfig: "/etc/skygo/kubeconfig", Context: "training"}
	// Receipt state is server-owned private state; it is never guessed or
	// resolved against the working directory.
	_, err := FromTargetWithState(context.Background(), t, "example", "state")
	fmt.Println(err)
	// Output: kubernetes execution state directory must be absolute
}

func TestFromTargetWithStateRefusals(t *testing.T) {
	root := receiptDir(t)
	config := TargetConfig{Name: "training", Namespace: "workloads", Kubeconfig: "/nonexistent/kubeconfig", Context: "training"}
	data := config
	data.Data = &DataConfig{}
	if _, err := FromTargetWithState(context.Background(), data, "example", root); err == nil || err.Error() != "kubernetes execution receipts do not cover data targets" {
		t.Fatalf("data target: %v", err)
	}
	// The existing constructor's own refusals are unchanged.
	if _, err := FromTargetWithState(context.Background(), config, "example", root); err == nil {
		t.Fatal("missing kubeconfig accepted")
	}
	if _, err := FromTargetWithState(context.Background(), config, "example", root+"/missing"); err == nil {
		t.Fatal("missing state directory accepted")
	}
}
