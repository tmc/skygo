package auth_test

import (
	"fmt"

	"github.com/tmc/skygo/internal/auth"
)

func ExampleEngine_Err() {
	path, _, done := exampleFiles()
	defer done()
	engine, err := auth.NewFile(path, "0123456789abcdef0123456789abcdef")
	if err != nil {
		panic(err)
	}
	// A non-nil Err means a credential file Close failed: keep the error
	// and stop using the Engine.
	fmt.Println(engine.Err())
	// Output: <nil>
}
