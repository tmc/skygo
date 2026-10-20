package store

import (
	"fmt"
	"os"
)

// This actual dormant-Store example is UNRUN. It does not install a fake root
// or confuse unavailable logical allowance with measured zero physical usage.
func ExampleStore_ArtifactReservations() {
	dir, err := os.MkdirTemp("", "skygo-reservations-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	s, err := Open(dir)
	if err != nil {
		panic(err)
	}
	defer s.Close()
	snapshot, err := s.ArtifactReservations()
	if err != nil {
		panic(err)
	}
	if !snapshot.Installation.Enabled {
		fmt.Println("artifact allowances unavailable")
	}
	// Output: artifact allowances unavailable
}

// Zero RootState on an unmarked workspace is explicitly unknown; only a
// validated committed GC intent can report positive NeverCreated or Owned.
func ExampleArtifactPhysicalSubject() {
	subject := ArtifactPhysicalSubject{Kind: "workspace", ID: "00000000000000000000000000000001"}
	fmt.Println(subject.Kind, subject.RootState == 0)
	// Output: workspace true
}
