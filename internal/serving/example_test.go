package serving_test

import (
	"fmt"
	"log"
	"strings"

	"github.com/tmc/skygo/internal/serving"
)

func echoSpec(mode string) serving.Spec {
	return serving.Spec{
		Image:     "registry.example.com/echo@sha256:" + strings.Repeat("a", 64),
		Argv:      []string{"/echo", "-listen", ":8080"},
		Env:       []serving.EnvVar{{Name: "MODE", Value: mode}},
		Port:      8080,
		Readiness: serving.Readiness{Path: "/healthz"},
		Resources: serving.Resources{CPUMillis: 250, MemoryBytes: 64 << 20},
		Scaling:   serving.Scaling{Replicas: 2},
	}
}

func Example() {
	h, err := serving.NewHistory(echoSpec("v1"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(serving.PhaseOf(serving.Present, h, nil), h.Current().Outcome)

	// A runtime observation, not the create, makes the service ready.
	o := serving.Observation{
		ObservedGeneration: 1, ReadyGeneration: 1,
		DesiredReplicas: 2, UpdatedReplicas: 2, ReadyReplicas: 2, AvailableReplicas: 2,
		ReadyEndpoints: 2,
	}
	if h, err = h.Settle(o); err != nil {
		log.Fatal(err)
	}
	fmt.Println(serving.PhaseOf(serving.Present, h, &o), h.Current().Outcome)
	// Output:
	// creating pending
	// ready complete
}

func ExampleSpec_Normalize() {
	s := echoSpec("v1")
	s.Image = "registry.example.com/echo:latest"
	_, err := s.Normalize()
	fmt.Println(err)
	// Output:
	// image must be a digest-pinned reference
}

func ExampleSpec_Digest() {
	a := echoSpec("v1")
	a.Env = []serving.EnvVar{{Name: "A", Value: "1"}, {Name: "B", Value: "2"}}
	b := echoSpec("v1")
	b.Env = []serving.EnvVar{{Name: "B", Value: "2"}, {Name: "A", Value: "1"}}
	b.Readiness = serving.Readiness{Path: "/healthz", PeriodSeconds: 10, TimeoutSeconds: 15, FailureThreshold: 3}
	da, err := a.Digest()
	if err != nil {
		log.Fatal(err)
	}
	db, err := b.Digest()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(da == db, len(da))
	// Output:
	// true 64
}

func ExampleHistory_Rollback() {
	h, _ := serving.NewHistory(echoSpec("v1"))
	h, _ = h.Settle(serving.Observation{ObservedGeneration: 1, ReadyGeneration: 1, DesiredReplicas: 2, UpdatedReplicas: 2, ReadyReplicas: 2, AvailableReplicas: 2, ReadyEndpoints: 2})
	h, _ = h.Update(echoSpec("v2"))

	// The new revision cannot pull its image; the old one keeps serving.
	o := serving.Observation{ObservedGeneration: 2, ReadyGeneration: 1, DesiredReplicas: 2, UpdatedReplicas: 1, ReadyReplicas: 2, AvailableReplicas: 2, ReadyEndpoints: 2, Reason: serving.ImagePull}
	h, _ = h.Settle(o)
	fmt.Println(serving.PhaseOf(serving.Present, h, &o))

	h, err := h.Rollback(1)
	if err != nil {
		log.Fatal(err)
	}
	c := h.Current()
	fmt.Println(c.Generation, c.Revision, c.Kind, c.Outcome)
	fmt.Println(serving.PhaseOf(serving.Present, h, &o), len(h.Revisions))
	// Output:
	// rollout-failed
	// 3 1 rollback pending
	// progressing 2
}

func ExampleHistory_Prune() {
	h, _ := serving.NewHistory(echoSpec("v0"))
	h, _ = h.Settle(serving.Observation{ObservedGeneration: 1, ReadyGeneration: 1, DesiredReplicas: 2, UpdatedReplicas: 2, ReadyReplicas: 2, AvailableReplicas: 2, ReadyEndpoints: 2})
	for i := 1; i <= 10; i++ {
		h, _ = h.Update(echoSpec(fmt.Sprintf("v%d", i)))
	}
	h, removed, err := h.Prune()
	if err != nil {
		log.Fatal(err)
	}
	var kept, gone []uint64
	for _, r := range h.Revisions {
		kept = append(kept, r.Number)
	}
	for _, r := range removed {
		gone = append(gone, r.Number)
	}
	fmt.Println(kept, gone)
	// Output:
	// [1 5 6 7 8 9 10 11] [2 3 4]
}
