package rpc

import (
	"testing"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"google.golang.org/protobuf/proto"
)

func TestTaskConversion(t *testing.T) {
	original := &v1.Task{Argv: []string{"program", "雪"}, Env: map[string]string{"A": "B"}, Directory: "/work", Image: "image", CpuMillis: 100, MemoryBytes: 1024, Accelerator: "gpu", AcceleratorCount: 2, Target: "chosen", TimeoutSeconds: 3, CpuLimitMillis: 200, MemoryLimitBytes: 2048, TargetLabels: map[string]string{"pool": "batch"}, NodeSelector: map[string]string{"zone": "west"}}
	saved := proto.Clone(original).(*v1.Task)
	core := task(original)
	roundtrip := wireTask(core)
	if !proto.Equal(roundtrip, original) {
		t.Fatalf("round trip = %v; want %v", roundtrip, original)
	}
	core.Argv[0] = "changed"
	core.Env["A"] = "changed"
	core.TargetLabels["pool"] = "changed"
	core.NodeSelector["zone"] = "changed"
	if !proto.Equal(original, saved) || !proto.Equal(roundtrip, saved) {
		t.Fatal("conversion retained mutable input aliases")
	}
	roundtrip.Argv[1] = "changed"
	roundtrip.Env["new"] = "changed"
	roundtrip.TargetLabels["new"] = "changed"
	roundtrip.NodeSelector["new"] = "changed"
	if len(core.Env) != 1 || len(core.TargetLabels) != 1 || len(core.NodeSelector) != 1 || core.Argv[1] != "雪" {
		t.Fatal("wire conversion retained mutable core aliases")
	}
	if got := wireTask(task(nil)); !proto.Equal(got, &v1.Task{}) {
		t.Fatalf("nil task = %v", got)
	}
}
