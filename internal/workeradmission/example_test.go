package workeradmission_test

import (
	"fmt"

	"github.com/tmc/skygo/internal/workeradmission"
)

func ExampleAdmit() {
	budget := workeradmission.Budget{Ranks: 4, CPUMillis: 8000, MemoryBytes: 16 << 30}
	rank := workeradmission.Demand{Ranks: 3, CPUMillis: 2000, MemoryBytes: 4 << 30}
	held := []workeradmission.Held{{ID: "running", Demand: rank}}
	queue := []workeradmission.Queued{{ID: "next", Demand: rank}}
	admitted, err := workeradmission.Admit(budget, held, queue)
	fmt.Println(admitted, err)
	// Once "running" stops, "next" is admitted whole.
	admitted, err = workeradmission.Admit(budget, nil, queue)
	fmt.Println(admitted, err)
	// Output:
	// [] <nil>
	// [next] <nil>
}

func ExampleFits() {
	budget := workeradmission.Budget{Ranks: 4, CPUMillis: 8000}
	fmt.Println(workeradmission.Fits(budget, workeradmission.Demand{Ranks: 3, CPUMillis: 2000}))
	fmt.Println(workeradmission.Fits(budget, workeradmission.Demand{Ranks: 3, CPUMillis: 3000}))
	// A zero budget field is zero capacity.
	fmt.Println(workeradmission.Fits(budget, workeradmission.Demand{Ranks: 1, MemoryBytes: 1}))
	// Output:
	// <nil>
	// demand exceeds budget: cpu 9000m of 8000m
	// demand exceeds budget: memory 1 of 0 bytes
}
