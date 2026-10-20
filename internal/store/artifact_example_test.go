package store

import "fmt"

func ExampleArtifactRef() {
	ref := ArtifactRef{ID: "0123456789abcdef0123456789abcdef", SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Size: 1024}
	// A reference describes bytes; a separate owner grant permits a read.
	fmt.Println(ref.ID, ref.Size)
	// Output: 0123456789abcdef0123456789abcdef 1024
}

func ExampleArtifactCapacityError() {
	err := &ArtifactCapacityError{Domain: "pending uploads", Used: 4, Limit: 4}
	fmt.Println(err)
	// Output: artifact pending uploads capacity: used 4, limit 4
}

func ExampleArtifactAttemptResult() {
	r := ArtifactAttemptResult{State: "expired", AttemptID: "0123456789abcdef0123456789abcdef"}
	fmt.Println(r.State, r.Reference == nil)
	// Output: expired true
}
