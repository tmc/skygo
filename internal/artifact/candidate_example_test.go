//go:build linux || darwin

package artifact

import (
	"bytes"
	"fmt"
	"io"
)

func ExampleFiles_CreateCandidate() {
	files, ref, data, cleanup := exampleFiles()
	defer cleanup()
	candidate, err := files.CreateCandidate(ref.ID)
	if err != nil {
		panic(err)
	}
	// In a workspace the source is Collect, whose nil return is required before
	// Seal. Copying an already validated archive demonstrates the writer alone.
	if _, err := io.Copy(candidate, bytes.NewReader(data)); err != nil {
		candidate.Close()
		panic(err)
	}
	sealed, err := candidate.Seal(examplePublicationGrowth(true))
	if err != nil {
		panic(err)
	}
	fmt.Println(sealed.Manifest.Reference == ref, sealed.Manifest.RegularFiles)
	// Output: true 1
}

func ExampleCandidate_Description() {
	files, ref, _, cleanup := exampleFiles()
	defer cleanup()
	candidate, err := files.CreateCandidate(ref.ID)
	if err != nil {
		panic(err)
	}
	fmt.Println(candidate.Description().ID == ref.ID, candidate.Description().Size)
	if err := candidate.Close(); err != nil {
		panic(err)
	}
	// Output: true 0
}

func ExampleCandidate_Write() {
	files, ref, data, cleanup := exampleFiles()
	defer cleanup()
	candidate, err := files.CreateCandidate(ref.ID)
	if err != nil {
		panic(err)
	}
	if _, err := candidate.Write(data); err != nil {
		panic(err)
	}
	fmt.Println(candidate.Description().Size == ref.Size)
	if err := candidate.Close(); err != nil {
		panic(err)
	}
	// Output: true
}

func ExampleCandidate_Close() {
	files, ref, _, cleanup := exampleFiles()
	defer cleanup()
	candidate, err := files.CreateCandidate(ref.ID)
	if err != nil {
		panic(err)
	}
	if err := candidate.Close(); err != nil {
		panic(err)
	}
	_, err = candidate.Seal(examplePublicationGrowth(true))
	fmt.Println(err != nil)
	// Output: true
}

func ExampleCandidate_Seal() {
	files, ref, data, cleanup := exampleFiles()
	defer cleanup()
	candidate, err := files.CreateCandidate(ref.ID)
	if err != nil {
		panic(err)
	}
	if _, err := candidate.Write(data); err != nil {
		candidate.Close()
		panic(err)
	}
	sealed, err := candidate.Seal(examplePublicationGrowth(true))
	if err != nil {
		panic(err)
	}
	fmt.Println(sealed.Manifest.Reference == ref)
	// Output: true
}

func ExampleFiles_RemoveCandidate() {
	files, ref, _, cleanup := exampleFiles()
	defer cleanup()
	candidate, err := files.CreateCandidate(ref.ID)
	if err != nil {
		panic(err)
	}
	if err := candidate.Close(); err != nil {
		panic(err)
	}
	// Real assembly first commits cleanup intent and joins every original owner.
	if err := files.RemoveCandidate(candidate.Description()); err != nil {
		panic(err)
	}
	fmt.Println("removed")
	// Output: removed
}
