package local_test

import (
	"errors"
	"fmt"

	"github.com/tmc/skygo/internal/local"
)

func ExampleObserveServerStopped() {
	// An unknown historical producer cannot authorize settlement.
	stopped, err := local.ObserveServerStopped(local.ServerLifetime{}, local.ServerLifetime{})
	fmt.Println(stopped, errors.Is(err, local.ErrServerLifetimeUnavailable))
	// Output: false true
}

func ExampleServerStoppedCloseError() {
	// A close error needs custody; an ordinary unavailable result has none.
	var closeErr *local.ServerStoppedCloseError
	fmt.Println(errors.As(local.ErrServerLifetimeUnavailable, &closeErr))
	// Output: false
}
