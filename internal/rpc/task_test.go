package rpc

import (
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"google.golang.org/protobuf/proto"
)

func TestTaskConversion(t *testing.T) {
	original := &v1.Task{Argv: []string{"program", "雪"}, Env: map[string]string{"A": "B"}, Directory: "/work", Image: "image", CpuMillis: 100, MemoryBytes: 1024, Accelerator: "gpu", AcceleratorCount: 2, Target: "chosen", TimeoutSeconds: 3, CpuLimitMillis: 200, MemoryLimitBytes: 2048, TargetLabels: map[string]string{"pool": "batch"}, NodeSelector: map[string]string{"zone": "west"}, Retry: &v1.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{7}}}
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
	core.Retry.MaxAttempts = 3
	core.Retry.ExitCodes[0] = 9
	if !proto.Equal(original, saved) || !proto.Equal(roundtrip, saved) {
		t.Fatal("conversion retained mutable input aliases")
	}
	roundtrip.Argv[1] = "changed"
	roundtrip.Env["new"] = "changed"
	roundtrip.TargetLabels["new"] = "changed"
	roundtrip.NodeSelector["new"] = "changed"
	roundtrip.Retry.MaxAttempts = 4
	roundtrip.Retry.ExitCodes[0] = 11
	if len(core.Env) != 1 || len(core.TargetLabels) != 1 || len(core.NodeSelector) != 1 || core.Argv[1] != "雪" || core.Retry.MaxAttempts != 3 || core.Retry.ExitCodes[0] != 9 {
		t.Fatal("wire conversion retained mutable core aliases")
	}
	if got := wireTask(task(nil)); !proto.Equal(got, &v1.Task{}) {
		t.Fatalf("nil task = %v", got)
	}
}

func TestJobConversionIndependent(t *testing.T) {
	code := int32(7)
	start := time.Date(2026, time.October, 18, 12, 0, 0, 0, time.UTC)
	finish := start.Add(time.Second)
	next := finish.Add(time.Second)
	core := backend.Job{
		ID:            "job",
		Task:          backend.Task{Argv: []string{"program"}, Retry: &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{7}}},
		Status:        backend.Status{State: backend.Retrying, ExitCode: &code},
		CreatedAt:     start,
		UpdatedAt:     finish,
		Attempts:      []backend.Attempt{{ID: "first", Status: backend.Status{State: backend.Failed, ExitCode: &code}, CreatedAt: start, StartingAt: &start, FinishedAt: &finish}},
		NextAttemptAt: &next,
	}
	wire := job(core)
	saved := proto.Clone(wire).(*v1.Job)
	code = 9
	start = start.Add(time.Hour)
	finish = finish.Add(time.Hour)
	next = next.Add(time.Hour)
	core.Task.Retry.ExitCodes[0] = 9
	core.Attempts[0].ID = "changed"
	if !proto.Equal(wire, saved) {
		t.Fatal("job conversion retained mutable core aliases")
	}
	*wire.ExitCode = 11
	*wire.Attempts[0].ExitCode = 13
	wire.Task.Retry.ExitCodes[0] = 11
	wire.Attempts[0].StartingAt.Seconds++
	wire.Attempts[0].FinishedAt.Seconds++
	wire.NextAttemptAt.Seconds++
	if code != 9 || core.Task.Retry.ExitCodes[0] != 9 || *core.Attempts[0].StartingAt != start || *core.Attempts[0].FinishedAt != finish || *core.NextAttemptAt != next {
		t.Fatal("job conversion retained mutable wire aliases")
	}
}
