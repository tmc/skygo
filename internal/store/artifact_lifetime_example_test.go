package store

import "fmt"

func ExampleOriginalServerLifetime() {
	// Unavailable identity preserves original-owned live operations. It does
	// not qualify automatic stopped recovery or replace actual FD closure.
	var producer OriginalServerLifetime
	fmt.Println(producer.PID)
	// Output: 0
}

func ExampleWorkspacePhysicalOrigin() {
	// Only the actual original owner may supply these identities and facts.
	origin := WorkspacePhysicalOrigin{Parent: ArtifactFile{17, 700}, RootState: WorkspaceRootNeverCreated, CandidateState: WorkspacePhysicalNeverAttempted}
	fmt.Println(origin.RootState == WorkspaceRootNeverCreated)
	// Output: true
}

func ExampleWorkspaceCandidatePrefix() {
	// A positively closed empty candidate is cleanup metadata, not a result.
	prefix := WorkspaceCandidatePrefix{ID: "00000000000000000000000000000001", File: ArtifactFile{17, 702}, SHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}
	fmt.Println(prefix.Size)
	// Output: 0
}

func ExampleWorkspacePhysicalCandidateState() {
	state := WorkspacePhysicalClosedPrefix
	fmt.Println(state == WorkspacePhysicalSealed)
	// Output: false
}

func ExampleWorkspaceCommitState() {
	var state WorkspaceCommitState
	fmt.Println(state == WorkspaceCommitHeld)
	// Output: true
}

func ExampleWorkspaceCommit() {
	var commit WorkspaceCommit
	// Held has no committed cleanup intent; neither field proves FD closure.
	fmt.Println(commit.State == WorkspaceCommitHeld, commit.Cleanup == nil)
	// Output: true true
}
