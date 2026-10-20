package control_test

import (
	"errors"
	"fmt"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"

	"github.com/tmc/skygo/internal/control"
)

func ExampleControl_ArtifactsEnabled() {
	var c *control.Control
	fmt.Println(c.ArtifactsEnabled())
	// Output: false
}

func ExampleControl_WorkspaceTargetEnabled() {
	var c *control.Control
	fmt.Println(c.WorkspaceTargetEnabled("local", "uninstalled"))
	// Output: false
}

func ExampleLocalWorkspaceConfig() {
	config := control.Config{}
	fmt.Println(config.LocalWorkspace == nil)
	// Output: true
}

func ExampleControl_SubmitWorkspace() {
	c, close := exampleControl()
	defer close()
	task := backend.Task{Argv: []string{"./run"}, Workdir: &artifact.Ref{
		ID: "0123456789abcdef0123456789abcdef", SHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", Size: 1024,
	}}
	_, err := c.SubmitWorkspace(testActor(), "workspace-1", task)
	// This daemon has no installed artifact service. It cannot accept workspace
	// submissions; the method never uploads a local directory implicitly.
	fmt.Println(errors.Is(err, control.ErrUnsupported))
	// Output: true
}

func ExampleControl_SubmitWorkspaceInDomain() {
	c, close := exampleControl()
	defer close()
	task := backend.Task{Argv: []string{"./run"}, Workdir: &artifact.Ref{
		ID: "0123456789abcdef0123456789abcdef", SHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", Size: 1024,
	}}
	domain := store.RequestDomain{Lineage: "11111111111111111111111111111111", Epoch: 1}
	_, err := c.SubmitWorkspaceInDomain(testActor(), domain, "workspace-1", task)
	fmt.Println(errors.Is(err, control.ErrUnsupported))
	// Output: true
}
