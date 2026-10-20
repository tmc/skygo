package control

import (
	"context"
	"errors"
	"fmt"
)

// The zero Control has no original Run or lifecycle. Construct it with New
// before requesting preparation; a refusal creates no shutdown receipt.
func ExampleControl_BeginShutdown() {
	var c Control
	fmt.Println(errors.Is(c.BeginShutdown(), ErrUnavailable))
	// Output: true
}

// Wait cannot manufacture preparation for an unconstructed Control.
func ExampleControl_WaitShutdownPrepared() {
	var c Control
	fmt.Println(errors.Is(c.WaitShutdownPrepared(context.Background()), ErrUnavailable))
	// Output: true
}
