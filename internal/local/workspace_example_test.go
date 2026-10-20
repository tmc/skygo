package local

import (
	"fmt"
	"strings"

	"github.com/tmc/skygo/internal/artifact"

	"github.com/tmc/skygo/internal/backend"
)

func ExampleBackend_ConfigureWorkspace() {
	// Configuration needs actual borrowed Files/Root and their installation;
	// a zero config cannot advertise a workspace-capable backend.
	b := new(Backend)
	fmt.Println(b.ConfigureWorkspace(WorkspaceConfig{}) != nil)
	// Output: true
}

func ExampleBackend_BindWorkspace() {
	// A trusted caller supplies the actual committed reservation and acquired
	// materialization permit. An empty value never grants launch permission.
	b := new(Backend)
	fmt.Println(b.BindWorkspace(WorkspaceBinding{}, nil) != nil)
	// Output: true
}

func ExampleBackend_WorkspaceObservation() {
	// A new server cannot adopt a prior root or receipt from an execution ID.
	b := new(Backend)
	_, err := b.WorkspaceObservation(backend.Reference{ID: "0123456789abcdef0123456789abcdef"})
	fmt.Println(err != nil)
	// Output: true
}

func ExampleBackend_CommitWorkspace() {
	// An acknowledgement needs the current owned candidate and actual Instance.
	b := new(Backend)
	fmt.Println(b.CommitWorkspace(backend.Reference{}, WorkspaceRootIntent{}) != nil)
	// Output: true
}

func ExampleWorkspaceConfig() {
	// Missing actual borrowed owners cannot initialize or adopt an installation.
	var b Backend
	config := WorkspaceConfig{InstallationID: "11111111111111111111111111111111", RootID: "22222222222222222222222222222222"}
	fmt.Println(b.ConfigureWorkspace(config) != nil)
	// Output: true
}

func ExampleWorkspaceBinding() {
	// Even a nonzero descriptive task is not a committed physical reservation.
	// No persisted InputFile or installed service is supplied, so the original
	// caller retains its acquired permit after this actual Bind rejection.
	ref := artifact.Ref{ID: "33333333333333333333333333333333", SHA256: strings.Repeat("a", 64), Size: 1024}
	binding := WorkspaceBinding{Owner: "example-owner", JobID: strings.Repeat("5", 32), AttemptID: strings.Repeat("6", 32), Binding: "unverified-binding", RootID: strings.Repeat("2", 32), Input: ref, Task: backend.Task{Argv: []string{"program"}, Workdir: &ref}}
	var b Backend
	released := false
	release := func() { released = true }
	err := b.BindWorkspace(binding, release)
	fmt.Println(err != nil, released)
	// Rejection leaves the caller, rather than Local, responsible for release.
	if err != nil {
		release()
	}
	fmt.Println(released)
	// Output:
	// true false
	// true
}

func ExampleWorkspaceOutcome() {
	// An unsuccessful observation lookup has no completed outcome. In particular,
	// its zero value is not WorkspaceTaskExit and cannot drive Store completion.
	var b Backend
	o, err := b.WorkspaceObservation(backend.Reference{ID: strings.Repeat("6", 32)})
	fmt.Println(err != nil, o.Outcome == 0, o.Outcome == WorkspaceTaskExit)
	// Output: true true false
}

func ExampleWorkspaceWriterClosure() {
	// Only the actual sequencer can return its owned-writer receipt. A lookup
	// refusal returns none; neither an execution ID nor a DTO grants one.
	var b Backend
	o, err := b.WorkspaceObservation(backend.Reference{ID: strings.Repeat("6", 32)})
	var writer *WorkspaceWriterClosure = o.Writer
	fmt.Println(err != nil, writer == nil)
	// Output: true true
}

func ExampleWorkspaceObservation() {
	// Always check the lookup error before interpreting Ready/Held. This actual
	// unowned lookup cannot acknowledge completion or grant a result artifact.
	var b Backend
	ref := backend.Reference{ID: strings.Repeat("6", 32)}
	o, err := b.WorkspaceObservation(ref)
	fmt.Println(err != nil, o.Ready, o.Candidate == nil, b.CommitWorkspace(ref, WorkspaceRootIntent{}) != nil)
	// Output: true false true true
}

func ExampleBackend_SupportsWorkspace() {
	// Discovery cannot turn a missing installation into a workspace service.
	var absent *Backend
	var unconfigured Backend
	fmt.Println(absent.SupportsWorkspace(), unconfigured.SupportsWorkspace())
	// Output: false false
}

func ExampleBackend_ResumeWorkspacePublication() {
	var b Backend
	fmt.Println(b.ResumeWorkspacePublication(backend.Reference{}) != nil)
	// Output: true
}

func ExampleBackend_PhysicalState() {
	var b *Backend
	fmt.Println(len(b.PhysicalState()))
	// Output: 0
}

func ExampleWorkspacePhysical() {
	p := WorkspacePhysical{RootAttempted: true}
	// An attempted root with no observed inode is unknown, not absent.
	fmt.Println(p.RootAttempted, p.Root == (artifact.FileID{}))
	// Output: true true
}
