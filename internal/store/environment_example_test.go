package store_test

import (
	"errors"
	"fmt"

	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/store"
)

func ExampleStore_AcceptEnvironment() {
	s, close := exampleStore()
	defer close()
	// Environments require scoped authentication.
	if err := s.ActivateScoped(); err != nil {
		fmt.Println(err)
		return
	}
	actor := testActor()
	spec := environment.Spec{Name: "analysis", Target: "kube", Image: "task:v1", CPUMillis: 500, MemoryBytes: 1 << 30}
	cp := environment.Checkpoint{Kind: "example", Version: 1, Data: []byte("intent")}
	e, err := s.AcceptEnvironment(actor, "create-1", spec, "test-binding", exampleAuthority(actor), cp)
	if err != nil {
		fmt.Println(err)
		return
	}
	same, err := s.AcceptEnvironment(actor, "create-1", spec, "test-binding", exampleAuthority(actor), cp)
	fmt.Println(same.Identity == e.Identity, err)
	fmt.Println(e.Identity.Generation, e.Revision, e.State.Desired, e.State.Phase)
	spec.Image = "task:v2"
	_, err = s.AcceptEnvironment(actor, "create-1", spec, "test-binding", exampleAuthority(actor), cp)
	fmt.Println(errors.Is(err, store.ErrConflict))
	// Output:
	// true <nil>
	// 1 1 present creating
	// true
}

func ExampleStore_CommitEnvironment() {
	s, close := exampleStore()
	defer close()
	if err := s.ActivateScoped(); err != nil {
		fmt.Println(err)
		return
	}
	actor := testActor()
	spec := environment.Spec{Name: "analysis", Target: "kube", Image: "task:v1", CPUMillis: 500, MemoryBytes: 1 << 30}
	e, err := s.AcceptEnvironment(actor, "create-1", spec, "test-binding", exampleAuthority(actor), environment.Checkpoint{Kind: "example", Version: 1, Data: []byte("intent")})
	if err != nil {
		fmt.Println(err)
		return
	}
	// The provider observed its own resources; record that before acting on it.
	ready := environment.State{Desired: environment.Present, Phase: environment.Ready}
	next, err := s.CommitEnvironment(e.Identity.ID, e.Revision, store.EnvironmentCommit{State: ready, Checkpoint: environment.Checkpoint{Kind: "example", Version: 1, Data: []byte("observed")}})
	fmt.Println(next.Revision, next.State.Phase, err)
	// A second writer holding the old revision is refused.
	_, err = s.CommitEnvironment(e.Identity.ID, e.Revision, store.EnvironmentCommit{State: ready, Checkpoint: e.Checkpoint})
	fmt.Println(errors.Is(err, store.ErrEnvironmentStale))
	// Deletion commits Absent intent only; nothing is removed.
	absent := environment.State{Desired: environment.Absent, Phase: environment.Ready}
	gone, err := s.CommitEnvironment(next.Identity.ID, next.Revision, store.EnvironmentCommit{State: absent, Checkpoint: next.Checkpoint})
	fmt.Println(gone.Revision, gone.State.Desired, gone.State.Phase, err)
	// Output:
	// 2 ready <nil>
	// true
	// 3 absent ready <nil>
}
