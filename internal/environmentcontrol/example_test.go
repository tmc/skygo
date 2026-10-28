package environmentcontrol_test

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/environmentcontrol"
)

func Example() {
	dir, err := os.MkdirTemp("", "environmentcontrol")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer os.RemoveAll(dir)
	f, err := setup(dir)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.stop()
	c, alice, ctx := f.c, f.actors["alice"], context.Background()

	spec := environment.Spec{Name: "analysis", Target: "kube", Image: "task:v1", CPUMillis: 500, MemoryBytes: 1 << 30}
	v, err := c.Create(alice, f.domain, "create-1", spec)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(v.State.Desired, v.State.Phase)

	// Sessions are admitted only once the runtime observed the environment.
	_, err = c.Exec(ctx, alice, v.ID, environmentcontrol.ExecRequest{Argv: []string{"echo", "hello"}})
	fmt.Println(errors.Is(err, environmentcontrol.ErrNotReady))
	if err := c.Reconcile(ctx); err != nil {
		fmt.Println(err)
		return
	}
	res, err := c.Exec(ctx, alice, v.ID, environmentcontrol.ExecRequest{Argv: []string{"echo", "hello"}})
	fmt.Printf("%s %d %v\n", res.Stdout, res.Code, err)

	// Deletion is recorded first; the runtime then removes owned resources.
	v, err = c.Delete(alice, v.ID)
	fmt.Println(v.State.Desired, v.State.Phase, err)
	if err := c.Reconcile(ctx); err != nil {
		fmt.Println(err)
		return
	}
	v, err = c.Get(alice, v.ID)
	fmt.Println(v.State.Phase, err)
	// Output:
	// present creating
	// true
	// hello 0 <nil>
	// absent deleting <nil>
	// deleted <nil>
}
