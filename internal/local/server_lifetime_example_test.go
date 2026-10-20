package local_test

import (
	"errors"
	"fmt"

	"github.com/tmc/skygo/internal/local"
)

func ExampleServerLifetime() {
	var value local.ServerLifetime
	fmt.Println(value == (local.ServerLifetime{}))
	// Output: true
}

func ExampleErrServerLifetimeUnavailable() {
	fmt.Println(errors.Is(local.ErrServerLifetimeUnavailable, local.ErrServerLifetimeUnavailable))
	// Output: true
}
