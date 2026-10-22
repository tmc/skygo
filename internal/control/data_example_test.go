package control_test

import (
	"errors"
	"fmt"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
)

func ExampleControl_SubmitData() {
	// NewWithConfig must supply the Store, current file authentication, installed
	// Files and a concrete configured data route before accepting submissions.
	var c *control.Control
	_, err := c.SubmitData(auth.Actor{}, "training", backend.Task{Argv: []string{"trainer"}})
	fmt.Println(errors.Is(err, control.ErrUnavailable))
	// Output: true
}

func ExampleControl_SubmitDataInDomain() {
	// Explicit domains select durable request replay; they cannot construct a
	// coordinator, authenticated Actor, installed root or native capability.
	var c *control.Control
	_, err := c.SubmitDataInDomain(auth.Actor{}, store.RequestDomain{}, "training", backend.Task{Argv: []string{"trainer"}})
	fmt.Println(errors.Is(err, control.ErrUnavailable))
	// Output: true
}
