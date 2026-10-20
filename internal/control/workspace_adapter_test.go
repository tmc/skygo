package control

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

type workspaceCheckBackend struct {
	configuredBackend
	checks int
}

func (b *workspaceCheckBackend) Check(backend.Task) error { b.checks++; return nil }

func TestWorkspacePlanAndQuoteRejectBeforeUnconfiguredBackend(t *testing.T) {
	b := &workspaceCheckBackend{}
	c := &Control{engine: testEngine, targets: []backend.Target{{Name: "remote", Binding: "remote", Backend: b}}}
	input := artifact.Ref{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Size: 1024}
	task := backend.Task{Argv: []string{"unused"}, Workdir: &input}
	if _, _, err := c.Plan(testActor(), task); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Plan unsupported workspace=%v", err)
	}
	quote, err := c.QuoteTask(testActor(), task, "USD", backend.TargetHour, nil)
	if err != nil || len(quote.Candidates) != 1 || quote.Candidates[0].Eligible || quote.Candidates[0].Rejection != "local workspace service is unsupported" {
		t.Fatalf("Quote workspace=%+v %v", quote, err)
	}
	if b.checks != 0 {
		t.Fatalf("workspace called unconfigured backend %d times", b.checks)
	}
	task.Workdir = nil
	if _, _, err := c.Plan(testActor(), task); err != nil {
		t.Fatal(err)
	}
	quote, err = c.QuoteTask(testActor(), task, "USD", backend.TargetHour, nil)
	if err != nil || !quote.Candidates[0].Eligible || b.checks != 2 {
		t.Fatalf("default plan/quote changed: checks=%d %+v %v", b.checks, quote, err)
	}
}

// This unit exercises acknowledgement ownership, not a fabricated Local writer
// or result producer. No Store Complete or native process is invoked here.
func TestWorkspaceAckErrorAndSuccessRetainRetirementBarrier(t *testing.T) {
	j := backend.Job{ID: strings.Repeat("a", 32), Target: "local", TargetBinding: "binding", Task: backend.Task{Argv: []string{"unused"}, Workdir: &artifact.Ref{ID: strings.Repeat("c", 32), SHA256: strings.Repeat("d", 64), Size: 1024}}, Status: backend.Status{State: backend.Running, Instance: strings.Repeat("b", 32)}}
	c := &Control{retention: retentionState{pending: map[string]struct{}{j.ID: {}}}}
	work := workspaceIntentVector(j)
	intent, err := workspaceLocalIntent(work)
	if err != nil {
		t.Fatal(err)
	}
	p := &workspacePending{job: cloneJob(j), durable: true, commitState: store.WorkspaceCommitMarked, cleanup: &work}
	op := operation{kind: workspaceCommitOperation, job: cloneJob(j), intent: &intent}
	w := targetWorker{target: backend.Target{Name: "local"}, pending: &op, completion: p, input: make(chan operation, 1)}
	state := &loopState{due: map[string]time.Time{j.ID: time.Now()}}
	original := errors.New("original Local acknowledgement held")
	if err := c.integrateWorkspace(state, &w, operationResult{operation: op, err: original}); err != nil {
		t.Fatal(err)
	}
	if w.completion != p || p.err != original || p.acked || w.pending != nil || len(c.retention.pending) != 1 {
		t.Fatal("ack error lost original completion")
	}
	w.pending = &op
	if err := c.integrateWorkspace(state, &w, operationResult{operation: op}); err != nil {
		t.Fatal(err)
	}
	if w.completion != p || !p.acked || w.pending != nil || len(c.retention.pending) != 1 {
		t.Fatal("ack fabricated native retirement")
	}
	state.workers = []targetWorker{w}
	if err := c.advanceWorkspaces(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	if len(state.workers[0].input) != 0 {
		t.Fatal("retirement-held completion dispatched replacement work")
	}
}

func TestWorkspaceMissingWriterCannotBecomeCompletion(t *testing.T) {
	c := &Control{}
	j := backend.Job{Task: backend.Task{Argv: []string{"unused"}}, Status: backend.Status{State: backend.Running, Instance: strings.Repeat("a", 32)}}
	for _, observation := range []local.WorkspaceObservation{
		{},
		{Ready: true, Outcome: local.WorkspaceTaskExit, Status: backend.Status{State: backend.Succeeded}},
		{Held: true, Outcome: local.WorkspaceUncertain, Status: backend.Status{State: backend.Unknown}},
	} {
		completion, err := c.workspaceCompletion(j, observation, local.WorkspacePhysicalOrigin{})
		if err == nil || completion.Writer != nil || completion.Candidate != nil {
			t.Fatalf("missing original writer elevated: %+v %v", completion, err)
		}
	}
}
