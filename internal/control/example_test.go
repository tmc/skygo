package control_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

func exampleControl() (*control.Control, func()) {
	dir, err := os.MkdirTemp("", "skygo-control-example-")
	if err != nil {
		panic(err)
	}
	s, err := store.Open(dir)
	if err != nil {
		os.RemoveAll(dir)
		panic(err)
	}
	b, err := local.New(filepath.Join(dir, "logs"))
	if err != nil {
		s.Close()
		os.RemoveAll(dir)
		panic(err)
	}
	c, err := control.NewWithLocalOutput(s, []backend.Target{{Name: "local", Binding: "test-binding", Kind: "local", Backend: b}}, testEngine, control.LocalOutputConfig{})
	if err != nil {
		b.Close()
		s.Close()
		os.RemoveAll(dir)
		panic(err)
	}
	return c, func() {
		if err := errors.Join(b.Close(), s.Close()); err != nil {
			panic(err)
		}
		os.RemoveAll(dir)
	}
}

func Example() {
	c, close := exampleControl()
	defer close()
	target, rejected, err := c.Plan(testActor(), backend.Task{Argv: []string{"trainer"}})
	fmt.Println(target, len(rejected), err)
	targets, err := c.Targets(testActor())
	if err != nil {
		panic(err)
	}
	fmt.Println(targets[0].Kind, c.Healthy())
	// Output:
	// local 0 <nil>
	// local true
}

func ExampleValidate() {
	fmt.Println(control.Validate(backend.Task{Argv: []string{"trainer"}}))
	fmt.Println(errors.Is(control.Validate(backend.Task{}), control.ErrInvalid))
	// Output:
	// <nil>
	// true
}

func ExampleControl_Submit() {
	c, close := exampleControl()
	defer close()
	task := backend.Task{Argv: []string{"trainer"}}
	job, err := c.Submit(testActor(), "training-1", task)
	if err != nil {
		panic(err)
	}
	same, err := c.Submit(testActor(), "training-1", task)
	fmt.Println(job.ID == same.ID, same.Status.State, err)
	// Output: true queued <nil>
}

func ExampleControl_List() {
	c, close := exampleControl()
	defer close()
	job, err := c.Submit(testActor(), "training-1", backend.Task{Argv: []string{"trainer"}})
	if err != nil {
		panic(err)
	}
	got, err := c.Get(testActor(), job.ID)
	if err != nil {
		panic(err)
	}
	jobs, next, err := c.List(testActor(), 10, "")
	fmt.Println(got.Status.State, len(jobs), next == "", err)
	// Output: queued 1 true <nil>
}

func ExampleControl_Cancel() {
	c, close := exampleControl()
	defer close()
	job, err := c.Submit(testActor(), "training-1", backend.Task{Argv: []string{"trainer"}})
	if err != nil {
		panic(err)
	}
	job, err = c.Cancel(testActor(), job.ID)
	fmt.Println(job.CancelRequested, job.Status.State, err)
	// Output: true queued <nil>
}

func ExampleControl_Run() {
	c, close := exampleControl()
	defer close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fmt.Println(errors.Is(c.Run(ctx), context.Canceled))
	// Output: true
}

func ExampleControl_Logs() {
	c, close := exampleControl()
	defer close()
	job, err := c.Submit(testActor(), "training-1", backend.Task{Argv: []string{"trainer"}})
	if err != nil {
		panic(err)
	}
	// Acceptance alone does not start execution or produce output.
	chunk, err := c.Logs(context.Background(), testActor(), job.ID, job.ID, 0)
	fmt.Println(chunk.Complete, len(chunk.Data), err)
	// Output: false 0 <nil>
}

func ExampleControl_RecordAccess() {
	c, close := exampleControl()
	defer close()
	fmt.Println(c.RecordAccess(testActor(), "list", "", "allowed"))
	// Output: <nil>
}

func ExampleControl_AuditHealthy() {
	c, close := exampleControl()
	defer close()
	fmt.Println(c.AuditHealthy())
	// Output: true
}

func ExampleControl_LogAttempt() {
	c, close := exampleControl()
	defer close()
	job, err := c.Submit(testActor(), "training-1", backend.Task{Argv: []string{"trainer"}})
	if err != nil {
		panic(err)
	}
	selected, err := c.LogAttempt(testActor(), job.ID, 0)
	fmt.Println(selected == job.ID, err)
	// Output: true <nil>
}

func ExampleControl_QuoteTask() {
	c, close := exampleControl()
	defer close()
	plan, err := c.QuoteTask(testActor(), backend.Task{Argv: []string{"trainer"}}, "USD", backend.TargetHour, nil)
	fmt.Println(plan.Outcome, plan.SelectedTarget, len(plan.Candidates), err)
	// Output: no_comparable_quote  1 <nil>
}

func ExampleControl_RetentionInfo() {
	c, close := exampleControl()
	defer close()
	view, err := c.RetentionInfo(testActor(), 0, "")
	if err != nil {
		panic(err)
	}
	fmt.Println(view.Info.Enabled, len(view.Targets), view.Admin != nil)
	// Output: false 1 true
}

func ExampleControl_SubmitRetained() {
	c, close := exampleControl()
	defer close()
	// Explicit domains are accepted only after stopped-state retention enable.
	domain := store.RequestDomain{Lineage: "11111111111111111111111111111111", Epoch: 1}
	_, err := c.SubmitRetained(testActor(), domain, "training-1", backend.Task{Argv: []string{"trainer"}})
	fmt.Println(errors.Is(err, store.ErrRetentionDisabled))
	// Output: true
}

func ExampleControl_Prune() {
	c, close := exampleControl()
	defer close()
	_, _, err := c.Prune(context.Background(), testActor(), store.PruneSelection{Before: time.Now()}, false, "")
	fmt.Println(errors.Is(err, store.ErrRetentionDisabled))
	// Output: true
}

func ExampleControl_OpenLogs() {
	c, close := exampleControl()
	defer close()
	job, err := c.Submit(testActor(), "training-1", backend.Task{Argv: []string{"trainer"}})
	if err != nil {
		panic(err)
	}
	lease, err := c.OpenLogs(testActor(), job.ID, 0, "", 0)
	if err != nil {
		panic(err)
	}
	defer lease.Close() // Streaming transports wait for response completion first.
	fmt.Println(lease.AttemptID() == job.ID, lease.Offset(), lease.Check(testActor()))
	chunk, err := lease.Read(context.Background(), testActor(), lease.Offset())
	fmt.Println(len(chunk.Data), err)
	cursor, err := lease.Cursor(12)
	if err != nil {
		panic(err)
	}
	resumed, err := c.OpenLogs(testActor(), job.ID, 0, cursor, 0)
	if err != nil {
		panic(err)
	}
	defer resumed.Close()
	fmt.Println(resumed.AttemptID() == lease.AttemptID(), resumed.Offset())
	// Output:
	// true 0 <nil>
	// 0 <nil>
	// true 12
}

func ExampleControl_StorageReady() {
	c, close := exampleControl()
	defer close()
	// Disabled retention has no physical sampling prerequisite.
	fmt.Println(c.StorageReady())
	// Output: true
}

func ExampleNewWithLocalOutput() {
	c, close := exampleControl()
	defer close()
	// Zero configuration is sufficient when retention is disabled.
	view, err := c.RetentionInfo(testActor(), 0, "")
	fmt.Println(view.Info.Enabled, err)
	// Output: false <nil>
}
