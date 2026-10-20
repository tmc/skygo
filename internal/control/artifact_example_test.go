package control

import (
	"errors"
	"fmt"
)

func ExampleConfig() {
	config := Config{}
	fmt.Println(config.Artifacts == nil)
	// Output: true
}

func ExampleArtifactCall_Done() {
	// The zero value owns no I/O or HTTP response. Done is safe on that value.
	var call ArtifactCall
	call.Done()
	call.Done()
	fmt.Println("no owner released")
	// Output: no owner released
}

func ExampleIsArtifactBeginNotStarted() {
	// Generic Busy text is not a proof that Store.Begin was never entered.
	fmt.Println(IsArtifactBeginNotStarted(errors.New("artifact operation busy")))
	// Output: false
}
