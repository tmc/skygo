package control_test

import (
	"errors"
	"fmt"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
	"github.com/tmc/skygo/internal/workflow"
)

func ExampleControl_SubmitWorkflow() {
	c, close := exampleControl()
	defer close()
	g := workflow.Graph{Nodes: []workflow.Node{
		{Name: "build", Task: backend.Task{Argv: []string{"make"}}},
		{Name: "test", Task: backend.Task{Argv: []string{"make", "test"}}, DependsOn: []string{"build"}},
	}}
	// Each node's Task is validated before any lookup or acceptance.
	invalid := workflow.Graph{Nodes: []workflow.Node{{Name: "build"}}}
	_, err := c.SubmitWorkflow(testActor(), nil, "pipeline-1", invalid)
	fmt.Println(errors.Is(err, control.ErrInvalid))
	// A new workflow is admitted later under a file delegation, which a
	// legacy authentication engine cannot issue.
	_, err = c.SubmitWorkflow(testActor(), nil, "pipeline-1", g)
	fmt.Println(errors.Is(err, auth.ErrUnauthenticated))
	// Output:
	// true
	// true
}

func ExampleControl_ListWorkflows() {
	c, close := exampleControl()
	defer close()
	workflows, next, err := c.ListWorkflows(testActor(), 0, "")
	fmt.Println(len(workflows), next == "", err)
	_, _, err = c.ListWorkflows(testActor(), 0, "not-a-token")
	fmt.Println(errors.Is(err, control.ErrInvalid))
	// Output:
	// 0 true <nil>
	// true
}

func ExampleControl_GetWorkflow() {
	c, close := exampleControl()
	defer close()
	_, err := c.GetWorkflow(testActor(), "not-an-id")
	fmt.Println(errors.Is(err, control.ErrInvalid))
	_, err = c.CancelWorkflow(testActor(), "0123456789abcdef0123456789abcdef")
	fmt.Println(errors.Is(err, store.ErrNotFound))
	// Output:
	// true
	// true
}
