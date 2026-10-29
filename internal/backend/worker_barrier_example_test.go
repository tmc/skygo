package backend_test

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/tmc/skygo/internal/backend"
)

func ExampleWorkerBarrierReady() {
	g := exampleWorkerGroup(2, backend.WorkerTCP)
	refs := []backend.WorkerReference{
		{Group: g, Rank: 0, Pod: examplePod0, Service: exampleService},
		{Group: g, Rank: 1, Pod: examplePod1, Service: exampleService},
	}
	routed := backend.WorkerRendezvous{Service: exampleService, Host: "skygo-fedcba9876543210fedcba9876543210.jobs.svc", Port: 29500, Routed: true}
	obs := []backend.WorkerBarrier{
		{Pod: examplePod0, Node: "node-a", Prepared: true, Rendezvous: routed},
		{Pod: examplePod1, Node: "node-b"},
	}
	fmt.Println(backend.WorkerBarrierReady(refs, obs))
	obs[1].Prepared = true
	fmt.Println(backend.WorkerBarrierReady(refs, obs))
	// An observation of only some members never permits the barrier.
	fmt.Println(backend.WorkerBarrierReady(refs[:1], obs[:1]))
	// Output:
	// worker rank 1 is not prepared
	// <nil>
	// worker barrier requires every member
}

func ExampleWorkerStartToken() {
	g := exampleWorkerGroup(2, backend.WorkerIndependent)
	t0, err0 := backend.WorkerStartToken(backend.WorkerReference{Group: g, Rank: 0, Pod: examplePod0})
	t1, err1 := backend.WorkerStartToken(backend.WorkerReference{Group: g, Rank: 1, Pod: examplePod1})
	fmt.Println(err0, err1, t0 != t1)
	// A Pod that replaced the original at its name has another token.
	replaced := examplePod0
	replaced.UID = "d3219392-688c-4e60-8ce1-680a8956a01f"
	t2, _ := backend.WorkerStartToken(backend.WorkerReference{Group: g, Rank: 0, Pod: replaced})
	fmt.Println(t0 != t2)
	_, err := backend.WorkerStartToken(backend.WorkerReference{Group: g, Rank: 0})
	fmt.Println(err)
	// Output:
	// <nil> <nil> true
	// true
	// missing worker pod
}

// A member helper computes its own token from its environment and compares
// it with the start file.
func ExampleWorkerStartDigest() {
	g := exampleWorkerGroup(2, backend.WorkerIndependent)
	want, _ := backend.WorkerStartToken(backend.WorkerReference{Group: g, Rank: 1, Pod: examplePod1})
	got := backend.WorkerStartDigest(g.Execution.ID, g.AttemptID, backend.WorkerIndependent, 1, 2, examplePod1.UID, g.TaskDigest)
	fmt.Println(got == want, len(hex.EncodeToString(got[:])))
	// Output:
	// true 64
}

func ExampleValidateWorkerRecovery() {
	ref := backend.WorkerReference{Group: exampleWorkerGroup(2, backend.WorkerIndependent), Rank: 0, Pod: examplePod0}
	proof := backend.WorkerRecovery{Ref: ref, Evidence: backend.WorkerNeverReleased}
	// Recorded intent alone does not establish actual removal dispatch.
	fmt.Println(backend.ValidateWorkerRecovery(proof))
	proof.RemoveSent = true // MODELED positive dispatch witness, not an intent.
	fmt.Println(backend.ValidateWorkerRecovery(proof))
	// Output:
	// worker recovery requires a sent removal
	// <nil>
}

func ExampleValidateWorkerBarrier() {
	ref := backend.WorkerReference{Group: exampleWorkerGroup(2, backend.WorkerIndependent), Rank: 0, Pod: examplePod0}
	observation := backend.WorkerBarrier{Pod: examplePod0, Stop: backend.WorkerNeverBound}
	fmt.Println(backend.ValidateWorkerBarrier(ref, observation))
	observation.Deleting = true
	fmt.Println(backend.ValidateWorkerBarrier(ref, observation))
	// Output:
	// invalid worker never-bound evidence
	// <nil>
}

func ExampleWorkerRemoval() {
	ref := backend.WorkerReference{Group: exampleWorkerGroup(2, backend.WorkerIndependent), Rank: 0, Pod: examplePod0}
	receipt := backend.WorkerRemoval{Pod: examplePod0, PodDispatched: true}
	// This MODELED dispatch witness proves neither application nor retirement.
	fmt.Println(backend.ValidateWorkerRemoval(ref, receipt), receipt.PodRemoved)
	// Output:
	// <nil> false
}

func ExampleWorkerStarted() {
	receipt := backend.WorkerStarted{Pod: examplePod0, Acknowledged: true, Revision: "42"}
	// Acknowledgment reports delivery; it does not certify main execution.
	fmt.Println(receipt.Pod == examplePod0, receipt.Acknowledged, receipt.Revision)
	// Output:
	// true true 42
}

func ExampleErrWorkerUnsettled() {
	// MODELED producer return: retain this chain with the producer's original
	// owner. Store acceptance of its partial result cannot settle that owner.
	cause := io.ErrUnexpectedEOF
	returned := fmt.Errorf("worker logs: %w", errors.Join(backend.ErrWorkerUnsettled, cause))
	fmt.Println(errors.Is(returned, backend.ErrWorkerUnsettled), errors.Is(returned, cause))
	// The same transport failure with positively settled local ownership is
	// native uncertainty, requiring resolution rather than a repeated effect.
	settled := fmt.Errorf("worker response: %w", cause)
	fmt.Println(errors.Is(settled, backend.ErrWorkerUnsettled))
	// Output:
	// true true
	// false
}
