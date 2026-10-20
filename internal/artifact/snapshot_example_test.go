//go:build linux || darwin

package artifact_test

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/tmc/skygo/internal/artifact"
)

func ExampleSnapshot() {
	dir, err := os.MkdirTemp("", "artifact-snapshot-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	root, err := os.OpenRoot(dir)
	if err != nil {
		panic(err)
	}
	defer root.Close()
	file, err := root.OpenFile("hello", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		panic(err)
	}
	if _, err := io.WriteString(file, "hello"); err != nil {
		panic(err)
	}
	if err := file.Close(); err != nil {
		panic(err)
	}
	// The caller separately ensures all input writers are quiescent.
	var candidate bytes.Buffer
	if err := artifact.Snapshot(root, &candidate); err != nil {
		panic(err)
	}
	digest, err := artifact.Validate(bytes.NewReader(candidate.Bytes()), int64(candidate.Len()))
	if err != nil {
		panic(err)
	}
	fmt.Println(len(digest))
	// Output: 64
}
