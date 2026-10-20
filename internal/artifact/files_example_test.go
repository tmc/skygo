//go:build linux || darwin

package artifact

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

func exampleFiles() (*Files, Ref, []byte, func()) {
	directory, err := os.MkdirTemp("", "artifact-files-example-")
	if err != nil {
		panic(err)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		panic(err)
	}
	info, err := root.Lstat(".")
	if err != nil {
		panic(err)
	}
	m, err := fileMetadata(info)
	if err != nil {
		panic(err)
	}
	// A real assembly binds this measured identity to its installation metadata.
	if _, err := Initialize(root, fileID(m), "abcdef0123456789abcdef0123456789"); err != nil {
		panic(err)
	}
	files, err := Open(root, fileID(m))
	if err != nil {
		panic(err)
	}
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	if err := tw.WriteHeader(&tar.Header{Name: "hello", Size: 5, Mode: 0600, Typeflag: tar.TypeReg, Format: tar.FormatUSTAR}); err != nil {
		panic(err)
	}
	if _, err := io.WriteString(tw, "hello"); err != nil {
		panic(err)
	}
	if err := tw.Close(); err != nil {
		panic(err)
	}
	data := archive.Bytes()
	digest := sha256.Sum256(data)
	ref := Ref{ID: "0123456789abcdef0123456789abcdef", SHA256: hex.EncodeToString(digest[:]), Size: int64(len(data))}
	cleanup := func() {
		if err := files.Close(); err != nil {
			panic(err)
		}
		if err := root.Close(); err != nil {
			panic(err)
		}
		if err := os.RemoveAll(directory); err != nil {
			panic(err)
		}
	}
	return files, ref, data, cleanup
}

func ExampleFiles() {
	files, ref, data, cleanup := exampleFiles()
	defer cleanup()
	id, err := files.CreatePart(ref)
	if err != nil {
		panic(err)
	}
	if _, err := files.WritePart(ref, id, 0, 0, data); err != nil {
		panic(err)
	}
	sealed, err := files.SealPart(ref, id, examplePublicationGrowth(true))
	if err != nil {
		panic(err)
	}
	reader, err := files.OpenObject(ref, sealed.File)
	if err != nil {
		panic(err)
	}
	var first [5]byte
	if _, err := reader.ReadAt(first[:], 512); err != nil {
		panic(err)
	}
	if err := reader.Close(); err != nil {
		panic(err)
	}
	fmt.Println(string(first[:]), sealed.Manifest.RegularFiles)
	// Output: hello 1
}

func ExampleFiles_WritePart() {
	files, ref, data, cleanup := exampleFiles()
	defer cleanup()
	id, err := files.CreatePart(ref)
	if err != nil {
		panic(err)
	}
	first, err := files.WritePart(ref, id, 0, 0, data)
	if err != nil {
		panic(err)
	}
	retry, err := files.WritePart(ref, id, int64(len(data)), 0, data)
	if err != nil {
		panic(err)
	}
	fmt.Println(first == retry)
	// Output: true
}

func ExampleFiles_RecoverPart() {
	files, ref, _, cleanup := exampleFiles()
	defer cleanup()
	id, err := files.CreatePart(ref)
	if err != nil {
		panic(err)
	}
	// Ack comes from the durable upload row; an observed suffix never advances it.
	if err := files.RecoverPart(ref, id, 0); err != nil {
		panic(err)
	}
	if err := files.RemovePart(ref, id); err != nil {
		panic(err)
	}
	fmt.Println("recovered")
	// Output: recovered
}

func ExampleFiles_RootFacts() {
	files, _, _, cleanup := exampleFiles()
	defer cleanup()
	facts, err := files.RootFacts()
	if err != nil {
		panic(err)
	}
	fmt.Println(len(facts.ID), facts.MetadataFiles, facts.MetadataAllowance == 2*facts.Unit)
	// Output: 32 2 true
}
