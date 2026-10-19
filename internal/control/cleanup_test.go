package control

import (
	"context"
	"crypto/sha256"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

func TestCleanupWorkerCannotElevateInvalidNativeRecords(t *testing.T) {
	task := retryTask(1)
	data, err := backend.TaskBytes(task)
	if err != nil {
		t.Fatal(err)
	}
	id := "11111111111111111111111111111111"
	code := int32(7)
	original := store.CleanupRecord{ID: id, HeaderID: id, Sequence: 1, Target: "good", Binding: "binding-good", Operation: store.NativeCleanup, Stopped: true, Execution: backend.Reference{ID: id, Task: task, Binding: "binding-good", Instance: "owned"}, TaskDigest: sha256.Sum256(data), Proof: backend.Status{State: backend.Failed, ExitCode: &code, Termination: backend.StoppedWithExitCode, Instance: "owned"}}
	var calls atomic.Int32
	target := targetFor("good", &routeBackend{})
	target.Retention = &backend.Retention{RemoveExecution: func(context.Context, backend.Reference) error { calls.Add(1); return nil }}
	ctx, cancel := context.WithCancel(context.Background())
	input := make(chan maintenanceRequest, 1)
	results := make(chan maintenanceResult, 1)
	exited := make(chan struct{})
	go func() { defer close(exited); runMaintenance(ctx, input, results) }()
	defer func() { cancel(); waitValue(t, exited) }()
	cases := map[string]func(*store.CleanupRecord){
		"not-stopped":           func(r *store.CleanupRecord) { r.Stopped = false },
		"unknown":               func(r *store.CleanupRecord) { r.Proof.State = backend.Unknown },
		"no-proof":              func(r *store.CleanupRecord) { r.Proof.Termination = backend.UnknownTermination },
		"no-code":               func(r *store.CleanupRecord) { r.Proof.ExitCode = nil },
		"wrong-instance":        func(r *store.CleanupRecord) { r.Proof.Instance = "replacement" },
		"empty-instance":        func(r *store.CleanupRecord) { r.Execution.Instance = ""; r.Proof.Instance = "" },
		"disabled-policy-alias": func(r *store.CleanupRecord) { r.Execution.Task.Retry = &backend.RetryPolicy{MaxAttempts: 1} },
		"invalid-policy":        func(r *store.CleanupRecord) { r.Execution.Task.Retry.MaxAttempts = 11 },
		"unnormalized-delay":    func(r *store.CleanupRecord) { r.Execution.Task.Retry.DelaySeconds = 0 },
		"unsorted-codes":        func(r *store.CleanupRecord) { r.Execution.Task.Retry.ExitCodes = []int32{9, 7} },
		"duplicate-codes":       func(r *store.CleanupRecord) { r.Execution.Task.Retry.ExitCodes = []int32{7, 7} },
		"empty-argv":            func(r *store.CleanupRecord) { r.Execution.Task.Argv = nil },
		"oversized-task":        func(r *store.CleanupRecord) { r.Execution.Task.Argv = []string{strings.Repeat("x", 256<<10)} },
		"invalid-utf8":          func(r *store.CleanupRecord) { r.Execution.Task.Argv = []string{string([]byte{0xff})} },
		"wrong-task":            func(r *store.CleanupRecord) { r.Execution.Task.Argv = []string{"changed"} },
		"default":               func(r *store.CleanupRecord) { r.Execution.Task.Retry = nil },
		"wrong-route":           func(r *store.CleanupRecord) { r.Binding = "replacement" },
		"output-cannot-native":  func(r *store.CleanupRecord) { r.Operation = store.OutputCleanup },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			r := original
			r.Execution.Task = cloneTask(original.Execution.Task)
			change(&r)
			if name != "wrong-task" {
				data, err := backend.TaskBytes(r.Execution.Task)
				if err != nil {
					t.Fatal(err)
				}
				r.TaskDigest = sha256.Sum256(data)
			}
			input <- maintenanceRequest{record: &r, target: target}
			result := waitValue(t, results)
			if result.err == nil || calls.Load() != 0 {
				t.Fatal("invalid record invoked native callback", result.err)
			}
		})
	}
	input <- maintenanceRequest{record: &original, target: target}
	if result := waitValue(t, results); result.err != nil || calls.Load() != 1 {
		t.Fatal("valid durable dispatch did not reach exact callback", result.err)
	}
}

func TestStaleMaintenanceResultCannotClearNewerOperation(t *testing.T) {
	s := routeStore(t)
	c := routeControl(t, s, targetFor("good", &routeBackend{}))
	request := maintenanceRequest{sequence: 2, directory: s.Directory(), reusable: 4}
	m := &maintenanceState{pending: &request}
	stale := request
	stale.sequence = 1
	if err := c.integrateMaintenance(m, maintenanceResult{request: stale, sample: &RetentionAdmin{}}); err != nil {
		t.Fatal(err)
	}
	if m.pending != &request || c.retention.sample != nil {
		t.Fatal("old observation cleared/replaced current operation")
	}
}

func TestIdlePollRefreshesWithinTheExistingSampleSlot(t *testing.T) {
	s, _ := retainedStore(t)
	c := routeControl(t, s, retainedTarget(&routeBackend{}))
	c.retention.wantSample.Store(false)
	c.retention.sample.ObservedAt = time.Now().Add(-time.Second)
	m := &maintenanceState{input: make(chan maintenanceRequest, 1), results: make(chan maintenanceResult, 1)}
	if err := c.maintain(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if m.pending == nil || m.pending.record != nil || len(m.input) != 1 {
		t.Fatal("idle poll failed to request one observation")
	}
	for i := 0; i < 100; i++ {
		m.due = time.Time{}
		if err := c.maintain(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
	if len(m.input) != 1 || m.serial != 1 {
		t.Fatal("occupied idle sample slot was replaced")
	}
}
