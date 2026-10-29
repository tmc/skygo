//go:build linux || darwin

package server

import (
	"context"
	"fmt"
	"os"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
	"github.com/tmc/skygo/internal/workeradmission"
)

// exampleWorkerProvider adds a Workers to an Integration that owns its targets
// and lifetime. BindControl in an installed integration constructs the Workers
// with the server's Control, rather than a second Store or Engine.
type exampleWorkerProvider struct {
	Integration
	workers *control.Workers
}

func (in exampleWorkerProvider) WorkerController() *control.Workers { return in.workers }

// exampleIdleWorkers is MODELED and refuses every worker effect. The example
// demonstrates interface composition, not native execution or server readiness.
type exampleIdleWorkers struct{ lifetimeRemote }

func (*exampleIdleWorkers) CreateWorker(context.Context, backend.WorkerCreate) (backend.WorkerCreated, error) {
	return backend.WorkerCreated{}, control.ErrUnsupported
}
func (*exampleIdleWorkers) ReleaseWorker(context.Context, backend.WorkerReference) (backend.WorkerReleased, error) {
	return backend.WorkerReleased{}, control.ErrUnsupported
}
func (*exampleIdleWorkers) ObserveWorker(context.Context, backend.WorkerReference) (backend.WorkerObservation, error) {
	return backend.WorkerObservation{}, control.ErrUnsupported
}
func (*exampleIdleWorkers) CancelWorker(context.Context, backend.WorkerReference) (backend.WorkerCanceled, error) {
	return backend.WorkerCanceled{}, control.ErrUnsupported
}
func (*exampleIdleWorkers) WorkerLogs(context.Context, backend.WorkerReference, int64, int) (backend.LogChunk, error) {
	return backend.LogChunk{}, control.ErrUnsupported
}
func (*exampleIdleWorkers) RemoveWorker(context.Context, backend.WorkerReference) (backend.WorkerRemoval, error) {
	return backend.WorkerRemoval{}, control.ErrUnsupported
}
func (*exampleIdleWorkers) ObserveBarrier(context.Context, backend.WorkerReference) (backend.WorkerBarrier, error) {
	return backend.WorkerBarrier{}, control.ErrUnsupported
}
func (*exampleIdleWorkers) StartWorker(context.Context, backend.WorkerReference, [32]byte) (backend.WorkerStarted, error) {
	return backend.WorkerStarted{}, control.ErrUnsupported
}
func (*exampleIdleWorkers) ResolveWorker(context.Context, backend.WorkerCreate) (backend.WorkerCreated, error) {
	return backend.WorkerCreated{}, control.ErrUnsupported
}
func (*exampleIdleWorkers) RecoverWorkerRemoval(context.Context, backend.WorkerRecovery) (backend.WorkerRemoval, error) {
	return backend.WorkerRemoval{}, control.ErrUnsupported
}

func ExampleWorkerProvider() {
	dir, err := os.MkdirTemp("", "skygo-worker-provider-")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer os.RemoveAll(dir)
	s, err := store.Open(dir)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer s.Close()
	target := backend.Target{Name: "gang", Binding: "example-gang", Backend: new(exampleIdleWorkers), OutputStorage: backend.RemoteOutput}
	c, err := control.New(s, []backend.Target{target}, auth.NewLegacy(""))
	if err != nil {
		fmt.Println(err)
		return
	}
	workers, err := control.NewWorkers(c, control.WorkerConfig{Budgets: map[string]workeradmission.Budget{
		"gang": {Ranks: 2, CPUMillis: 2000, MemoryBytes: 2 << 30},
	}})
	if err != nil {
		fmt.Println(err)
		return
	}
	base := newTestIntegration()
	base.targets = []backend.Target{target}
	var provider WorkerProvider = exampleWorkerProvider{Integration: base, workers: workers}
	// The adapter keeps the existing target and exposes exactly the Workers
	// constructed from this Control. No Run, listener or worker call starts.
	fmt.Println(provider.Targets()[0].Name, provider.WorkerController() == workers)
	// Output: gang true
}
