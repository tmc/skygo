package store_test

import (
	"errors"
	"fmt"

	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/store"
)

func ExampleStore_ReclaimEnvironment() {
	dir, info, remove := exampleRetentionDirectory(store.DefaultRetentionPolicy())
	defer remove()
	s, err := store.Open(dir)
	if err != nil {
		panic(err)
	}
	defer s.Close()
	if err := s.ActivateScoped(); err != nil {
		panic(err)
	}
	actor := testActor()
	spec := environment.Spec{Name: "analysis", Target: "kube", Image: "task:v1", CPUMillis: 500, MemoryBytes: 1 << 30}
	cp := environment.Checkpoint{Kind: "example", Version: 1, Data: []byte("intent")}
	e, err := s.AcceptEnvironment(actor, info.Domain, "create-1", spec, "test-binding", exampleAuthority(actor), cp)
	if err != nil {
		panic(err)
	}
	// Only a Deleted environment is reclaimed.
	_, err = s.ReclaimEnvironment(e.Identity.ID, e.Revision)
	fmt.Println(err != nil)
	for _, p := range []environment.Phase{environment.Deleting, environment.Deleted} {
		if e, err = s.CommitEnvironment(e.Identity.ID, e.Revision, store.EnvironmentCommit{State: environment.State{Desired: environment.Absent, Phase: p}, Checkpoint: cp}); err != nil {
			panic(err)
		}
	}
	retired, err := s.ReclaimEnvironment(e.Identity.ID, e.Revision)
	fmt.Println(retired.Retired, retired.Identity == e.Identity, err)
	// The request replays the retired view. The Store kept only the spec
	// digest; the view carries the replayed spec, which matched it exactly.
	// The name is free again.
	same, found, err := s.EnvironmentRequest(actor, info.Domain, "create-1", spec)
	fmt.Println(found, same.Retired, same.State.Phase, same.Spec.Image, err)
	_, err = s.Environment(e.Identity.ID)
	fmt.Println(errors.Is(err, store.ErrNotFound))
	next, err := s.AcceptEnvironment(actor, info.Domain, "create-2", spec, "test-binding", exampleAuthority(actor), cp)
	fmt.Println(next.Identity.ID != e.Identity.ID, err)
	// Output:
	// true
	// true true <nil>
	// true true deleted task:v1 <nil>
	// true
	// true <nil>
}
