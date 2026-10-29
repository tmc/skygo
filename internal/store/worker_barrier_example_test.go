package store_test

import (
	"fmt"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
	"github.com/tmc/skygo/internal/workeradmission"
)

func exampleWorkerBarrier() (*store.Store, store.WorkerGroupRequest, store.WorkerGroup, func()) {
	p := store.DefaultRetentionPolicy()
	p.LocalOutputEnabled = false
	dir, info, remove := exampleRetentionDirectory(p)
	s, err := store.Open(dir)
	if err != nil {
		remove()
		panic(err)
	}
	r := store.WorkerGroupRequest{Domain: info.Domain, RequestID: "training", Task: backend.Task{Argv: []string{"train"}, CPUMillis: 1000, MemoryBytes: 1024}, Spec: backend.WorkerSpec{Count: 2, Mode: backend.WorkerIndependent}, Target: "worker", Binding: "worker-budget", Authority: exampleAuthority(testActor()), Demand: workeradmission.Demand{Ranks: 2, CPUMillis: 1000, MemoryBytes: 1024}}
	// exampleAuthority models the file delegation; no backend or native call runs.
	g, err := s.SubmitWorkerBarrierGroup(testActor(), r)
	if err != nil {
		s.Close()
		remove()
		panic(err)
	}
	finish := func() {
		if err := s.Close(); err != nil {
			panic(err)
		}
		remove()
	}
	return s, r, g, finish
}
func exampleWorkerIntent(s *store.Store, g store.WorkerGroup) (store.WorkerToken, store.WorkerGroup) {
	g, ok, err := s.AdmitWorkerGroup(g.Job.ID, g.Revision, workeradmission.Budget{Ranks: 2, CPUMillis: 2000, MemoryBytes: 2048}, time.Now())
	if err != nil || !ok {
		panic("worker admission failed")
	}
	tok, g, err := s.IntendWorkerBarrier(g.Job.ID, g.Revision, 0, store.WorkerCreate, time.Now())
	if err != nil {
		panic(err)
	}
	return tok, g
}

func ExampleStore_SubmitWorkerBarrierGroup() {
	s, r, g, finish := exampleWorkerBarrier()
	defer finish()
	again, err := s.SubmitWorkerBarrierGroup(testActor(), r)
	fmt.Println(g.Barrier.Version, again.Job.ID == g.Job.ID, err)
	// Output: 1 true <nil>
}
func ExampleStore_LookupWorkerGroup() {
	s, r, g, finish := exampleWorkerBarrier()
	defer finish()
	got, found, err := s.LookupWorkerGroup(testActor(), r.Domain, r.RequestID, r.Task, r.Spec)
	fmt.Println(found, got.Job.ID == g.Job.ID, err)
	// Output: true true <nil>
}
func ExampleStore_IntendWorkerBarrier() {
	s, _, g, finish := exampleWorkerBarrier()
	defer finish()
	tok, g := exampleWorkerIntent(s, g)
	fmt.Println(tok.Effect == store.WorkerCreate, g.Members[0].Create.State == store.WorkerIntended)
	// Output: true true
}
func ExampleStore_RecordWorkerBarrierEffect() {
	s, _, g, finish := exampleWorkerBarrier()
	defer finish()
	tok, _ := exampleWorkerIntent(s, g)
	// MODELED original receipt; a nil error does not mean a native member ran.
	g, err := s.RecordWorkerBarrierEffect(tok, store.WorkerBarrierReceipt{Created: backend.WorkerCreated{Pod: backend.WorkerResource{Name: "rank-0", UID: "original-uid"}}}, "", time.Now())
	fmt.Println(g.Members[0].Pod.UID, g.Members[0].Create.State == store.WorkerRecorded, err)
	// Output: original-uid true <nil>
}
func ExampleStore_RecordWorkerBarrier() {
	s, _, g, finish := exampleWorkerBarrier()
	defer finish()
	tok, _ := exampleWorkerIntent(s, g)
	pod := backend.WorkerResource{Name: "rank-0", UID: "original-uid"}
	g, err := s.RecordWorkerBarrierEffect(tok, store.WorkerBarrierReceipt{Created: backend.WorkerCreated{Pod: pod}}, "", time.Now())
	if err != nil {
		panic(err)
	}
	g, err = s.RecordWorkerBarrier(g.Job.ID, g.Revision, 0, backend.WorkerBarrier{Pod: pod, Revision: "1", Gated: true}, time.Now())
	fmt.Println(g.Members[0].Barrier.Observation.Gated, g.Barrier.Decision == store.WorkerBarrierUndecided, err)
	// Output: true true <nil>
}
func ExampleStore_RecordWorkerResolved() {
	s, _, g, finish := exampleWorkerBarrier()
	defer finish()
	tok, _ := exampleWorkerIntent(s, g)
	g, err := s.RecordWorkerBarrierEffect(tok, store.WorkerBarrierReceipt{}, "original timeout", time.Now())
	if err != nil {
		panic(err)
	}
	g, err = s.RecordWorkerResolved(g.Job.ID, g.Revision, 0, backend.WorkerCreated{}, store.WorkerResolveOutcome{Class: store.WorkerResolveClean, Refused: true}, time.Now())
	fmt.Println(g.Members[0].Barrier.Resolve.Class == store.WorkerResolveClean, g.Job.Status.State, err)
	// Output: true failed <nil>
}
func ExampleStore_RecordWorkerVanished() {
	s, _, g, finish := exampleWorkerBarrier()
	defer finish()
	tok, _ := exampleWorkerIntent(s, g)
	g, err := s.RecordWorkerBarrierEffect(tok, store.WorkerBarrierReceipt{Created: backend.WorkerCreated{Pod: backend.WorkerResource{Name: "rank-0", UID: "original-uid"}}}, "", time.Now())
	if err != nil {
		panic(err)
	}
	g, err = s.RecordWorkerVanished(g.Job.ID, g.Revision, 0, time.Now())
	fmt.Println(g.Members[0].Barrier.Vanished, g.Reserved, err)
	// Output: true true <nil>
}
func ExampleStore_DecideWorkerBarrier() {
	s, _, g, finish := exampleWorkerBarrier()
	defer finish()
	g, err := s.DecideWorkerBarrier(g.Job.ID, g.Revision, store.WorkerBarrierAbort, time.Now())
	fmt.Println(g.Barrier.Decision == store.WorkerBarrierAbort, g.Job.Status.State, err)
	// Output: true failed <nil>
}
func ExampleWorkerBarrierDecision() {
	fmt.Println(store.WorkerBarrierUndecided == 0) // Output: true
}
func ExampleWorkerResolveClass() {
	fmt.Println(store.WorkerResolveNone == 0) // Output: true
}
func ExampleWorkerResolveOutcome() {
	var o store.WorkerResolveOutcome
	fmt.Println(o.Class == store.WorkerResolveNone, o.At.IsZero()) // Output: true true
}
func ExampleWorkerBarrierGroup() {
	var g store.WorkerBarrierGroup
	fmt.Println(g.Decision == store.WorkerBarrierUndecided) // Output: true
}
func ExampleWorkerBarrierMember() {
	var m store.WorkerBarrierMember
	fmt.Println(m.Stop == backend.WorkerStopUnproven, m.PodRemoveSent) // Output: true false
}
func ExampleWorkerBarrierReceipt() {
	var r store.WorkerBarrierReceipt
	fmt.Println(r.Removed.PodDispatched, r.Removed.PodRemoved, r.Unsettled) // Output: false false false
}

func ExampleStore_LatchWorkerUnsettled() {
	s, _, g, finish := exampleWorkerBarrier()
	defer finish()
	tok, _ := exampleWorkerIntent(s, g)
	pod := backend.WorkerResource{Name: "rank-0", UID: "original-uid"}
	g, err := s.RecordWorkerBarrierEffect(tok, store.WorkerBarrierReceipt{Created: backend.WorkerCreated{Pod: pod}}, "", time.Now())
	if err != nil {
		panic(err)
	}
	// MODELED typed read failure. The real caller must separately retain its
	// original error and local owner; committing this fence never closes them.
	g, err = s.LatchWorkerUnsettled(g.Job.ID, g.Revision, 0, backend.WorkerReference{Group: g.Group, Rank: 0, Pod: pod}, time.Now())
	fmt.Println(g.Members[0].Barrier.LocalUnsettled, g.Reserved, err)
	// Output: true true <nil>
}
