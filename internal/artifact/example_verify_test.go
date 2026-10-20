package artifact_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/tmc/skygo/internal/artifact"
)

func ExampleVerify() {
	data := make([]byte, 1024) // Two zero blocks encode an empty archive.
	digest := sha256.Sum256(data)
	ref := artifact.Ref{ID: "0123456789abcdef0123456789abcdef", SHA256: hex.EncodeToString(digest[:]), Size: int64(len(data))}
	manifest, err := artifact.Verify(ref, bytes.NewReader(data))
	fmt.Println(manifest.Reference == ref, manifest.RegularFiles, manifest.Directories, manifest.ExpandedBytes, err)
	// Output: true 0 0 0 <nil>
}
