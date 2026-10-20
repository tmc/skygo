package control_test

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/tmc/skygo/internal/control"
)

func ExampleControl_StopArtifactAdmission() {
	c, close := exampleControl()
	defer close()
	// A default Control has no installed artifact admission to stop.
	c.StopArtifactAdmission()
	fmt.Println(c.StorageReady())
	// Output: true
}

func ExampleControl_DrainArtifacts() {
	c, close := exampleControl()
	defer close()
	// No installed artifact service means no original artifact owners to join.
	fmt.Println(c.DrainArtifacts(context.Background()))
	// Output: <nil>
}

func ExampleControl_ArtifactResultFor() {
	c, close := exampleControl()
	defer close()
	id := strings.Repeat("a", 32)
	_, err := c.ArtifactResultFor(testActor(), id, id)
	fmt.Println(errors.Is(err, control.ErrUnsupported))
	// Output: true
}
