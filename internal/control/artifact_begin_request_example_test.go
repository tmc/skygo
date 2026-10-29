package control_test

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
)

func ExampleControl_ArtifactBeginRequestsEnabled() {
	var c *control.Control
	fmt.Println(c.ArtifactBeginRequestsEnabled())
	// Output: false
}

func ExampleControl_BeginArtifactUploadRequest() {
	c, close := exampleControl()
	defer close()
	_, err := c.BeginArtifactUploadRequest(context.Background(), testActor(), nil,
		store.RequestDomain{Lineage: strings.Repeat("a", 32), Epoch: 1},
		"training-inputs", strings.Repeat("b", 64), 1024)
	fmt.Println(errors.Is(err, control.ErrUnsupported))
	// Output: true
}
