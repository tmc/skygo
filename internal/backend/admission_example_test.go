package backend_test

import (
	"errors"
	"fmt"

	"github.com/tmc/skygo/internal/backend"
)

func ExampleErrResourcesUnavailable() {
	err := fmt.Errorf("admit job: %w", backend.ErrResourcesUnavailable)
	fmt.Println(errors.Is(err, backend.ErrResourcesUnavailable))
	// Output: true
}
