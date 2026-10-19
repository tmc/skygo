package backend_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

func TestRetentionJobEncodingEnvelope(t *testing.T) {
	// This conservative encoding envelope combines field maxima; it is not a
	// claim that every lifecycle field can coexist in an accepted store row.
	task := backend.Task{Argv: []string{""}, Retry: &backend.RetryPolicy{MaxAttempts: 10, DelaySeconds: 3600}}
	for code := int32(1); code <= 32; code++ {
		task.Retry.ExitCodes = append(task.Retry.ExitCodes, code)
	}
	data, err := backend.TaskBytes(task)
	if err != nil {
		t.Fatal(err)
	}
	task.Argv[0] = strings.Repeat("x", (256<<10)-len(data))
	data, err = backend.TaskBytes(task)
	if err != nil || len(data) != 256<<10 {
		t.Fatalf("task envelope size = %d: %v", len(data), err)
	}
	now := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	code := int32(255)
	status := backend.Status{State: backend.Succeeded, Reason: strings.Repeat("\x01", 1024), ExitCode: &code, Termination: backend.StoppedWithExitCode, Instance: strings.Repeat("\"", 128)}
	job := backend.Job{ID: strings.Repeat("0", 32), Task: task, Target: strings.Repeat("\"", 128), Status: status, CreatedAt: now, UpdatedAt: now, Sequence: ^uint64(0), TargetBinding: strings.Repeat("\x01", 256), NextAttemptAt: &now}
	attemptBytes := 0
	for i := 0; i < 10; i++ {
		attempt := backend.Attempt{ID: strings.Repeat(string(rune('0'+i)), 32), Status: status, CreatedAt: now, StartingAt: &now, FinishedAt: &now}
		encoded, err := json.Marshal(attempt)
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) > 8192 {
			t.Fatalf("attempt envelope is %d bytes, exceeds reservation", len(encoded))
		}
		attemptBytes += len(encoded)
		job.Attempts = append(job.Attempts, attempt)
	}
	encoded, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	overhead := len(encoded) - len(data) - attemptBytes
	if overhead > 16384 {
		t.Fatalf("job envelope overhead is %d bytes, exceeds reservation", overhead)
	}
	t.Logf("job=%d task=%d attempts=%d overhead=%d", len(encoded), len(data), attemptBytes, overhead)
}
