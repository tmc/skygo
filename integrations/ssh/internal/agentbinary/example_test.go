package agentbinary_test

import (
	"context"
	"fmt"
	"github.com/tmc/skygo/integrations/ssh/internal/agentbinary"
)

func ExampleSpec_Check() {
	fmt.Println((agentbinary.Spec{}).Check())
	// Output: agentbinary: binary must be a clean absolute path
}

func ExampleArtifact_IsZero() {
	var a agentbinary.Artifact
	fmt.Println(a.IsZero())
	// Output: true
}

func ExampleArtifact_Build() {
	var a agentbinary.Artifact
	_, err := a.Build()
	fmt.Println(err)
	// Output: agentbinary: zero artifact
}

func ExamplePrepare() {
	_, err := agentbinary.Prepare(context.Background(), "relative", agentbinary.Spec{})
	fmt.Println(err)
	// Output: agentbinary: binary must be a clean absolute path
}

func Example() {
	var binary agentbinary.Artifact
	fmt.Println(binary.IsZero())
	// Output: true
}
