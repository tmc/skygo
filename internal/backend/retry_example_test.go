package backend_test

import (
	"fmt"

	"github.com/tmc/skygo/internal/backend"
)

func ExampleNormalizeRetry() {
	policy, err := backend.NormalizeRetry(&backend.RetryPolicy{
		MaxAttempts: 3,
		ExitCodes:   []int32{9, 7, 9},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(policy.MaxAttempts, policy.DelaySeconds, policy.ExitCodes)
	// Output: 3 1 [7 9]
}

func ExampleRetryPolicy() {
	policy := backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 5, ExitCodes: []int32{7}}
	fmt.Println(policy.MaxAttempts, policy.DelaySeconds, policy.ExitCodes)
	// Output: 2 5 [7]
}

func ExampleJob_CurrentAttemptID() {
	job := backend.Job{ID: "logical"}
	fmt.Println(job.CurrentAttemptID())
	job.Attempts = []backend.Attempt{{ID: "logical"}, {ID: "second"}}
	fmt.Println(job.CurrentAttemptID())
	// Output:
	// logical
	// second
}

func ExampleAttempt() {
	attempt := backend.Attempt{ID: "first", Status: backend.Status{State: backend.Queued}}
	fmt.Println(attempt.ID, attempt.Status.State, attempt.StartingAt == nil)
	// Output: first queued true
}

func ExampleTermination() {
	// A failed status alone does not establish that execution has stopped.
	status := backend.Status{State: backend.Failed}
	fmt.Println(status.Termination == backend.UnknownTermination)
	// Output: true
}
