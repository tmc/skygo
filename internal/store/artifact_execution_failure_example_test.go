package store

import (
	"fmt"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

func ExampleWorkspaceOutcome_executionFailure() {
	status := backend.Status{State: backend.Failed, Reason: "start task: permission denied"}
	completion := WorkspaceCompletion{Outcome: WorkspaceExecutionFailure}
	// An actual exec-start/signal failure has no synthetic task exit code.
	// This DTO alone does not provide writer closure or terminal authority.
	fmt.Println(status.ExitCode == nil, completion.Outcome == WorkspaceExecutionFailure)
	// Output: true true
}

// Compile-only until the concrete qualified Local producer is composed. This
// does not manufacture a writer receipt or claim the example has executed.
func ExampleStore_CompleteWorkspaceAttempt_executionFailure() {
	settle := func(s *Store, job string, execution backend.Reference, joined WorkspaceWriterClosure, origin WorkspacePhysicalOrigin, reason string) (WorkspaceCommit, error) {
		// joined must come from actual original-owner joins and qualified
		// writer quiescence, never a status code, RPC or missing process map.
		status := backend.Status{State: backend.Failed, Instance: execution.Instance, Reason: reason}
		return s.CompleteWorkspaceAttempt(job, execution, status, WorkspaceCompletion{Outcome: WorkspaceExecutionFailure, Writer: &joined, Origin: &origin}, time.Now().UTC())
	}
	_ = settle
}
