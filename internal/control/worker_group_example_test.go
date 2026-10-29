//go:build darwin || linux

package control

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
	"github.com/tmc/skygo/internal/workeradmission"
)

// exampleWorkerControl uses a real Store, scoped file Engine and the real
// Control.Run sampler. Only its native worker receipts are MODELED.
func exampleWorkerControl() (*Control, auth.Actor, store.RequestDomain, func()) {
	dir, err := os.MkdirTemp("", "skygo-worker-example-")
	if err != nil {
		panic(err)
	}
	info, err := store.EnableRetention(filepath.Join(dir, "state"), store.DefaultRetentionPolicy())
	if err != nil {
		panic(err)
	}
	s, err := store.Open(filepath.Join(dir, "state"))
	if err != nil {
		panic(err)
	}
	config, token := filepath.Join(dir, "auth.json"), filepath.Join(dir, "token")
	removeAuth, err := auth.Initialize(config, token, s.Owner())
	if err != nil {
		panic(err)
	}
	if err := s.ActivateScoped(); err != nil {
		panic(err)
	}
	engine, err := auth.NewFile(config, s.Owner())
	if err != nil {
		panic(err)
	}
	raw, err := os.ReadFile(token)
	if err != nil {
		panic(err)
	}
	actor, err := engine.Authenticate("Bearer " + strings.TrimSpace(string(raw)))
	if err != nil {
		panic(err)
	}
	authCtx, stopAuth := context.WithCancel(context.Background())
	authDone := make(chan struct{})
	go func() { defer close(authDone); engine.Run(authCtx) }()
	b := newG1Backend() // MODELED native operations, never a process or cluster.
	target := backend.Target{Name: "gang", Binding: "example-binding", Kind: "modeled", Backend: b, OutputStorage: backend.RemoteOutput, SupportsResourceLimits: true}
	c, err := New(s, []backend.Target{target}, engine)
	if err != nil {
		panic(err)
	}
	runCtx, stopRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(runCtx) }()
	until := time.Now().Add(5 * time.Second)
	for !c.StorageReady() {
		if time.Now().After(until) {
			panic("actual sampler did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	close := func() {
		stopRun()
		if err := <-runDone; err != nil && !errors.Is(err, context.Canceled) {
			panic(err)
		}
		stopAuth()
		<-authDone
		if err := s.Close(); err != nil {
			panic(err)
		}
		if err := removeAuth(); err != nil {
			panic(err)
		}
		if err := os.RemoveAll(dir); err != nil {
			panic(err)
		}
	}
	return c, actor, info.Domain, close
}

func exampleWorkerConfig() WorkerConfig {
	return WorkerConfig{Budgets: map[string]workeradmission.Budget{
		"gang": {Ranks: 4, CPUMillis: 4000, MemoryBytes: 4 << 30},
	}}
}
func exampleWorkers() (*Workers, auth.Actor, store.RequestDomain, func()) {
	c, actor, domain, close := exampleWorkerControl()
	w, err := NewWorkers(c, exampleWorkerConfig())
	if err != nil {
		panic(err)
	}
	return w, actor, domain, close
}
func exampleSubmitWorker(w *Workers, actor auth.Actor, domain store.RequestDomain) backend.Job {
	job, _, err := w.Submit(actor, domain, "training", backend.Task{Argv: []string{"train"}, Image: "trainer:v1", Target: "gang", CPUMillis: 1000, MemoryBytes: 1 << 30}, backend.WorkerSpec{Count: 2, Mode: backend.WorkerTCP})
	if err != nil {
		panic(err)
	}
	return job
}
func examplePrepareWorker(w *Workers) {
	for i := 0; i < 3; i++ {
		if err := w.Step(context.Background()); err != nil {
			panic(err)
		}
	}
}

func ExampleNewWorkers() {
	c, _, _, close := exampleWorkerControl()
	defer close()
	workers, err := NewWorkers(c, exampleWorkerConfig())
	fmt.Println(workers != nil, err)
	// Output: true <nil>
}

func ExampleWorkerConfig() {
	config := WorkerConfig{Budgets: map[string]workeradmission.Budget{
		"gang": {Ranks: 4, CPUMillis: 4000, MemoryBytes: 4 << 30},
	}}
	fmt.Println(config.Budgets["gang"].Ranks)
	// Output: 4
}

func ExampleWorkerRank() {
	rank := WorkerRank{Rank: 1, State: WorkerWaiting}
	fmt.Println(rank.Rank, rank.State == WorkerWaiting)
	// Output: 1 true
}

func ExampleWorkers_Submit() {
	w, actor, domain, close := exampleWorkers()
	defer close()
	task := backend.Task{Argv: []string{"train"}, Image: "trainer:v1", Target: "gang", CPUMillis: 1000, MemoryBytes: 1 << 30}
	job, spec, err := w.Submit(actor, domain, "training", task, backend.WorkerSpec{Count: 2, Mode: backend.WorkerTCP})
	if err != nil {
		panic(err)
	}
	fmt.Println(job.Status.State, spec.Count, err)
	// Output: queued 2 <nil>
}

func ExampleWorkers_Ranks() {
	w, actor, domain, close := exampleWorkers()
	defer close()
	job := exampleSubmitWorker(w, actor, domain)
	_, ranks, err := w.Ranks(actor, job.ID, job.ID)
	fmt.Println(len(ranks), ranks[0].State == WorkerWaiting, err)
	// Output: 2 true <nil>
}

func ExampleWorkers_Step() {
	w, actor, domain, close := exampleWorkers()
	defer close()
	job := exampleSubmitWorker(w, actor, domain)
	err := w.Step(context.Background()) // FIFO admission precedes any Create.
	group, readErr := w.c.store.WorkerGroup(job.ID)
	fmt.Println(group.Phase == store.WorkerGroupAdmitted, group.Reserved, err, readErr)
	// Output: true true <nil> <nil>
}

func ExampleWorkers_Logs() {
	w, actor, domain, close := exampleWorkers()
	defer close()
	job := exampleSubmitWorker(w, actor, domain)
	examplePrepareWorker(w)
	chunk, err := w.Logs(context.Background(), actor, job.ID, job.ID, 1, 0, 1024)
	fmt.Println(string(chunk.Data), chunk.NextOffset, err)
	// Output: log 3 <nil>
}

func ExampleWorkers_BeginLogs() {
	w, actor, domain, close := exampleWorkers()
	defer close()
	job := exampleSubmitWorker(w, actor, domain)
	examplePrepareWorker(w)
	release, err := w.BeginLogs(actor, job.ID, job.ID, 1)
	if err != nil {
		panic(err)
	}
	// An RPC server keeps this owner through its actual final response write.
	fmt.Println("admitted")
	release()
	// Output: admitted
}

func ExampleWorkerRankState() {
	var state WorkerRankState = WorkerWaiting
	fmt.Println(state == WorkerWaiting)
	// Output: true
}
