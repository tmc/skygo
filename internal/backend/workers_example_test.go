package backend_test

import (
	"crypto/sha256"
	"fmt"

	"github.com/tmc/skygo/internal/backend"
)

var (
	examplePod0    = backend.WorkerResource{Name: "skygo-fedcba9876543210fedcba9876543210-0", UID: "a3219392-688c-4e60-8ce1-680a8956a01f"}
	examplePod1    = backend.WorkerResource{Name: "skygo-fedcba9876543210fedcba9876543210-1", UID: "c3219392-688c-4e60-8ce1-680a8956a01f"}
	exampleService = backend.WorkerResource{Name: "skygo-fedcba9876543210fedcba9876543210", UID: "b3219392-688c-4e60-8ce1-680a8956a01f"}
)

func exampleWorkerGroup(count uint8, mode backend.WorkerMode) backend.WorkerGroup {
	task := backend.Task{Argv: []string{"python", "train.py"}, Image: "trainer:v1"}
	raw, err := backend.TaskBytes(task)
	if err != nil {
		panic(err)
	}
	return backend.WorkerGroup{
		Execution:  backend.Reference{ID: "0123456789abcdef0123456789abcdef", Task: task, Binding: "sha256:binding"},
		AttemptID:  "fedcba9876543210fedcba9876543210",
		TaskDigest: sha256.Sum256(raw),
		Spec:       backend.WorkerSpec{Count: count, Mode: mode},
	}
}

func ExampleNormalizeWorkers() {
	w, err := backend.NormalizeWorkers(&backend.WorkerSpec{Count: 4, Mode: backend.WorkerTCP})
	fmt.Println(w.Count, w.Mode, err)
	// One independent worker is the existing one-worker behavior.
	w, err = backend.NormalizeWorkers(&backend.WorkerSpec{Count: 1, Mode: backend.WorkerIndependent})
	fmt.Println(w == nil, err)
	_, err = backend.NormalizeWorkers(&backend.WorkerSpec{Count: 1, Mode: backend.WorkerTCP})
	fmt.Println(err)
	// Output:
	// 4 tcp <nil>
	// true <nil>
	// tcp rendezvous requires at least 2 workers
}

func ExampleNewWorkerSpec() {
	w, err := backend.NewWorkerSpec(3, backend.WorkerIndependent)
	fmt.Println(w.Count, w.Mode, err)
	// Narrowed first, a wire count of 257 would become 1.
	_, err = backend.NewWorkerSpec(257, backend.WorkerIndependent)
	fmt.Println(err)
	// Output:
	// 3 independent <nil>
	// worker count must be between 1 and 16
}

func ExampleExecutionUnits() {
	fmt.Println(backend.ExecutionUnits(nil))
	fmt.Println(backend.ExecutionUnits(&backend.WorkerSpec{Count: 8, Mode: backend.WorkerTCP}))
	fmt.Println(backend.ExecutionUnits(&backend.WorkerSpec{Count: 8}))
	// Output:
	// 1 <nil>
	// 8 <nil>
	// 0 invalid worker mode
}

func ExampleValidateWorkerCreated() {
	c := backend.WorkerCreate{Group: exampleWorkerGroup(2, backend.WorkerTCP)}
	// Rank 0 created the group's Service, then its Pod Create failed.
	partial := backend.WorkerCreated{Service: exampleService}
	fmt.Println(backend.ValidateWorkerCreated(c, partial, true))
	fmt.Println(backend.ValidateWorkerCreated(c, partial, false))
	fmt.Println(backend.ValidateWorkerCreated(c, backend.WorkerCreated{Pod: examplePod0, Service: exampleService}, false))
	// Output:
	// <nil>
	// missing worker pod
	// <nil>
}

func ExampleValidateWorkerReference() {
	r := backend.WorkerReference{Group: exampleWorkerGroup(2, backend.WorkerTCP), Rank: 1, Pod: examplePod1}
	fmt.Println(backend.ValidateWorkerReference(r, false))
	r.Rank = 2
	fmt.Println(backend.ValidateWorkerReference(r, false))
	// Output:
	// <nil>
	// worker rank out of range
}

func ExampleValidateWorkerRelease() {
	r := backend.WorkerReference{Group: exampleWorkerGroup(2, backend.WorkerTCP), Rank: 1, Pod: examplePod1}
	fmt.Println(backend.ValidateWorkerRelease(r))
	r.Service = exampleService
	fmt.Println(backend.ValidateWorkerRelease(r))
	// Output:
	// worker release requires the rendezvous service
	// <nil>
}

func ExampleValidateWorkerRemoval() {
	// Rank 0's Pod was never created; its Service is a cleanup subject.
	r := backend.WorkerReference{Group: exampleWorkerGroup(2, backend.WorkerTCP), Service: exampleService}
	fmt.Println(backend.ValidateWorkerRemoval(r, backend.WorkerRemoval{Service: exampleService, ServiceRemoved: true}))
	fmt.Println(backend.ValidateWorkerRemoval(r, backend.WorkerRemoval{Service: exampleService, PodRemoved: true}))
	// Output:
	// <nil>
	// worker removal acknowledges unknown resource
}
