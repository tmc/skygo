//go:build linux || darwin

package artifact_test

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/tmc/skygo/internal/artifact"
)

func ExampleValidate() {
	var encoded bytes.Buffer
	tw := tar.NewWriter(&encoded)
	if err := tw.WriteHeader(&tar.Header{Name: "hello", Mode: 0600, Size: 5, Typeflag: tar.TypeReg, Format: tar.FormatUSTAR}); err != nil {
		panic(err)
	}
	if _, err := io.WriteString(tw, "hello"); err != nil {
		panic(err)
	}
	if err := tw.Close(); err != nil {
		panic(err)
	}
	digest, err := artifact.Validate(bytes.NewReader(encoded.Bytes()), int64(encoded.Len()))
	fmt.Println(len(digest), err)
	// Output: 64 <nil>
}

func ExampleExtract() {
	var encoded bytes.Buffer
	tw := tar.NewWriter(&encoded)
	if err := tw.WriteHeader(&tar.Header{Name: "hello", Mode: 0600, Size: 5, Typeflag: tar.TypeReg, Format: tar.FormatUSTAR}); err != nil {
		panic(err)
	}
	if _, err := io.WriteString(tw, "hello"); err != nil {
		panic(err)
	}
	if err := tw.Close(); err != nil {
		panic(err)
	}
	expected := sha256.Sum256(encoded.Bytes()) // Trusted immutable reference in a real caller.
	dir, err := os.MkdirTemp("", "artifact-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	root, err := os.OpenRoot(dir)
	if err != nil {
		panic(err)
	}
	defer root.Close()
	hash := sha256.New()
	if err := artifact.Extract(root, io.TeeReader(bytes.NewReader(encoded.Bytes()), hash), int64(encoded.Len())); err != nil {
		panic(err)
	}
	if hex.EncodeToString(hash.Sum(nil)) != hex.EncodeToString(expected[:]) {
		panic("artifact digest mismatch")
	}
	// Only now may the unpublished destination be handed to an execution owner.
	file, err := root.Open("hello")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))
	// Output: hello
}

func ExampleCollect() {
	dir, err := os.MkdirTemp("", "artifact-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	root, err := os.OpenRoot(dir)
	if err != nil {
		panic(err)
	}
	defer root.Close()
	file, err := root.OpenFile("result", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		panic(err)
	}
	if _, err := io.WriteString(file, "completed output"); err != nil {
		panic(err)
	}
	if err := file.Close(); err != nil {
		panic(err)
	}
	// The caller separately establishes that all output writers have stopped.
	var candidate bytes.Buffer
	if err := artifact.Collect(root, []string{"result"}, &candidate); err != nil {
		panic(err)
	}
	_, err = artifact.Validate(bytes.NewReader(candidate.Bytes()), int64(candidate.Len()))
	fmt.Println(err == nil)
	// Output: true
}
