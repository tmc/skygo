package store_test

import (
	"fmt"
	"strings"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
	"github.com/tmc/skygo/internal/workflow"
)

// exampleAuthority stands in for the actor's file delegation, which a real
// caller obtains from auth.Engine.Delegate and checks before acceptance.
func exampleAuthority(actor auth.Actor) auth.Delegation {
	return auth.Delegation{Principal: actor.Principal(), TokenID: strings.Repeat("0", 32), Digest: [32]byte{1}}
}

func ExampleStore_SubmitWorkflow() {
	s, close := exampleStore()
	defer close()
	actor := testActor()
	g := workflow.Graph{Nodes: []workflow.Node{
		{Name: "build", Task: backend.Task{Argv: []string{"make"}}},
		{Name: "test", Task: backend.Task{Argv: []string{"make", "test"}}, DependsOn: []string{"build"}},
	}}
	w, err := s.SubmitWorkflow(actor, nil, "pipeline-1", g, exampleAuthority(actor))
	if err != nil {
		fmt.Println(err)
		return
	}
	same, err := s.SubmitWorkflow(actor, nil, "pipeline-1", g, exampleAuthority(actor))
	fmt.Println(same.ID == w.ID, err)
	for _, n := range w.Nodes {
		fmt.Println(n.Name, n.Disposition == store.WorkflowPending, n.JobID == "")
	}
	// Output:
	// true <nil>
	// build true true
	// test true true
}

func ExampleStore_AdmitWorkflowChild() {
	s, close := exampleStore()
	defer close()
	actor := testActor()
	g := workflow.Graph{Nodes: []workflow.Node{
		{Name: "build", Task: backend.Task{Argv: []string{"make"}}},
		{Name: "test", Task: backend.Task{Argv: []string{"make", "test"}}, DependsOn: []string{"build"}},
	}}
	w, err := s.SubmitWorkflow(actor, nil, "pipeline-1", g, exampleAuthority(actor))
	if err != nil {
		fmt.Println(err)
		return
	}
	candidates, err := s.WorkflowCandidates()
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, c := range candidates {
		j, err := s.AdmitWorkflowChild(store.WorkflowAdmission{Candidate: c, Target: "local", Binding: "test-binding"})
		fmt.Println(w.Graph.Nodes[c.Node].Name, j.Status.State, err)
	}
	// Output: build queued <nil>
}

func ExampleStore_CancelWorkflowFor() {
	s, close := exampleStore()
	defer close()
	actor := testActor()
	g := workflow.Graph{Nodes: []workflow.Node{{Name: "build", Task: backend.Task{Argv: []string{"make"}}}}}
	w, err := s.SubmitWorkflow(actor, nil, "pipeline-1", g, exampleAuthority(actor))
	if err != nil {
		fmt.Println(err)
		return
	}
	w, err = s.CancelWorkflowFor(actor, w.ID)
	fmt.Println(w.Canceled, w.Nodes[0].Disposition == store.WorkflowCanceled, err)
	// Output: true true <nil>
}
