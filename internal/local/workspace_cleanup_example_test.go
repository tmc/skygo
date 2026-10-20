package local

import (
	"context"
	"fmt"

	"github.com/tmc/skygo/internal/backend"
)

func ExampleBackend_PrepareWorkspaceCompletion() {
	// A new backend cannot prepare a prior server's original physical owners.
	var b Backend
	_, _, err := b.PrepareWorkspaceCompletion(backend.Reference{ID: "66666666666666666666666666666666"})
	fmt.Println(err != nil)
	// Output: true
}
func ExampleBackend_RemoveWorkspaceRoot() {
	var b Backend
	p, err := b.RemoveWorkspaceRoot(context.Background(), WorkspaceRootIntent{})
	fmt.Println(err != nil, p.Visited, p.Done)
	// Output: true 0 false
}
func ExampleBackend_HandoffWorkspaceCleanup() {
	var b Backend
	fmt.Println(b.HandoffWorkspaceCleanup(context.Background(), WorkspaceRootIntent{}) != nil)
	// Output: true
}
func ExampleWorkspaceCleanupIdentity() {
	var b Backend
	subject := WorkspaceCleanupIdentity{Owner: "example", AttemptID: "66666666666666666666666666666666"}
	fmt.Println(b.CommitWorkspace(backend.Reference{}, WorkspaceRootIntent{Origin: WorkspaceCleanupOrigin{Subject: subject}}) != nil)
	// Output: true
}
func ExampleWorkspaceCleanupOrigin() {
	var origin WorkspaceCleanupOrigin
	fmt.Println(origin.Parent.Inode, origin.Root.Inode)
	// Output: 0 0
}
func ExampleWorkspaceRootState() {
	var b Backend
	_, origin, err := b.PrepareWorkspaceCompletion(backend.Reference{})
	fmt.Println(err != nil, origin.RootState == WorkspaceRootNeverCreated)
	// Output: true false
}
func ExampleWorkspaceCandidateDisposition() {
	var origin WorkspaceCleanupOrigin
	fmt.Println(origin.Disposition == WorkspaceNoOutputs)
	// Output: false
}
func ExampleWorkspaceRootIntent() {
	var b Backend
	intent := WorkspaceRootIntent{ID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Revision: 1}
	_, err := b.RemoveWorkspaceRoot(context.Background(), intent)
	fmt.Println(err != nil)
	// Output: true
}
func ExampleWorkspaceRootProgress() {
	var b Backend
	progress, err := b.RemoveWorkspaceRoot(context.Background(), WorkspaceRootIntent{})
	fmt.Println(err != nil, progress.Done)
	// Output: true false
}
func ExampleWorkspacePhysicalOrigin() {
	var origin WorkspacePhysicalOrigin
	fmt.Println(origin.Producer == (ServerLifetime{}), origin.Prefix == nil)
	// Output: true true
}
func ExampleWorkspaceCandidatePrefix() {
	// Zero bytes are a possible actual closed prefix, never archive proof.
	var prefix WorkspaceCandidatePrefix
	fmt.Println(prefix.Size)
	// Output: 0
}
func ExampleWorkspacePhysicalCandidateState() {
	var state WorkspacePhysicalCandidateState
	fmt.Println(state == WorkspacePhysicalNoOutputs, state == WorkspacePhysicalNeverAttempted)
	// Output: false false
}
