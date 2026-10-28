package agentbuild_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tmc/skygo/integrations/ssh/internal/agentbuild"
)

// miniRoot creates a private miniature checkout holding only the two go.mod
// files FindRoot looks for. It is not buildable.
func miniRoot() (root string, cleanup func(), err error) {
	root, err = os.MkdirTemp("", "agentbuild-example-")
	if err != nil {
		return "", nil, err
	}
	cleanup = func() { os.RemoveAll(root) }
	files := map[string]string{
		"go.mod":                  "module github.com/tmc/skygo\n",
		"integrations/ssh/go.mod": "module github.com/tmc/skygo/integrations/ssh\n",
	}
	for name, data := range files {
		name = filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(name), 0o700); err != nil {
			cleanup()
			return "", nil, err
		}
		if err := os.WriteFile(name, []byte(data), 0o600); err != nil {
			cleanup()
			return "", nil, err
		}
	}
	return root, cleanup, nil
}

func Example() {
	root, cleanup, err := miniRoot()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer cleanup()
	nested := filepath.Join(root, "integrations", "ssh", "cmd", "skygo-ssh")
	if err := os.MkdirAll(nested, 0o700); err != nil {
		fmt.Println(err)
		return
	}
	found, err := agentbuild.FindRoot(nested)
	fmt.Println(found == filepath.Clean(root), err)

	for _, kv := range agentbuild.Env([]string{"HOME=/home/alice", "GOOS=darwin", "CGO_ENABLED=1"}) {
		fmt.Println(kv)
	}
	// Output:
	// true <nil>
	// HOME=/home/alice
	// GOOS=linux
	// GOARCH=arm64
	// CGO_ENABLED=0
	// GOWORK=off
	// GOTOOLCHAIN=local
}

func ExampleValidCommit() {
	sum := sha256.Sum256([]byte("agent binary"))
	fmt.Println(agentbuild.ValidCommit("4b825dc642cb6eb9a060e54bf8d69288fbee4904"))
	fmt.Println(agentbuild.ValidCommit("4b825dc"), agentbuild.ValidCommit("HEAD"))
	fmt.Println(agentbuild.ValidDigest(hex.EncodeToString(sum[:])))
	// Output:
	// true
	// false false
	// true
}

// This example shows only cancellation: the context is canceled before
// Build, so its real (*exec.Cmd).Run refuses to start the first command
// and no git or go process runs. It does not build or qualify an agent.
func ExampleBuilder_Build() {
	root, cleanup, err := miniRoot()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer cleanup()
	self, err := os.Executable() // absolute, so no PATH lookup is involved
	if err != nil {
		fmt.Println(err)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	b := &agentbuild.Builder{Git: self, Go: self}
	build, err := b.Build(ctx, root, filepath.Join(root, "cache"), false)
	fmt.Println(errors.Is(err, context.Canceled), build == agentbuild.Build{})
	fmt.Println(err)
	// Output:
	// true true
	// agentbuild: git rev-parse: context canceled
}
