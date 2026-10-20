package artifact_test

import (
	"fmt"

	"github.com/tmc/skygo/internal/artifact"
)

func Example() {
	ref := artifact.Ref{ID: "0123456789abcdef0123456789abcdef", SHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", Size: 1024}
	fmt.Println(ref.Validate())
	// Output: <nil>
}

func ExampleRef() {
	ref := artifact.Ref{ID: "0123456789abcdef0123456789abcdef", SHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", Size: 1024}
	fmt.Println(ref.ID, ref.Size)
	// Output: 0123456789abcdef0123456789abcdef 1024
}

func ExampleRef_Validate() {
	fmt.Println((artifact.Ref{}).Validate())
	// Output: invalid artifact id
}
