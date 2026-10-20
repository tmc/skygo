//go:build linux || darwin

package artifact

import "fmt"

func ExampleFiles_RemoveCandidateNames() {
	files, reserved, _, cleanup := exampleFiles()
	defer cleanup()
	candidate, err := files.CreateCandidate(reserved.ID)
	if err != nil {
		panic(err)
	}
	if _, err := candidate.Write([]byte("partial")); err != nil {
		panic(err)
	}
	if err := candidate.Close(); err != nil {
		panic(err)
	}
	// Files-only demonstration: real assembly first commits workspace cleanup
	// intent, joins original writer/collection owners and holds its I/O permit.
	prefix := candidate.Description()
	if err := files.RemoveCandidateNames(prefix); err != nil {
		panic(err)
	}
	fmt.Println(prefix.Size, "removed")
	// Output: 7 removed
}
