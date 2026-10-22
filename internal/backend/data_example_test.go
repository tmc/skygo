package backend_test

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

// These examples describe portable values and refusal/copy/arithmetic helpers.
// Constructed receipt fields never stand for native Create/stop/removal proof.
func ExampleDataBackend() {
	supportsData := func(b backend.Backend) bool { _, ok := b.(backend.DataBackend); return ok }
	fmt.Println(supportsData(nil))
	// Output: false
}

func ExampleDataMount() {
	b, err := json.Marshal(backend.DataMount{Reference: "cache", Destination: "/mnt/skygo/cache"})
	if err != nil {
		panic(err)
	}
	fmt.Println(string(b))
	// Output: {"reference":"cache","destination":"/mnt/skygo/cache","read_only":false}
}

func ExampleDataStart() {
	// Print selected non-secret fields, never the whole token-bearing value.
	s := backend.DataStart{JobID: "job", Budgets: backend.DataBudgets{StageSeconds: 600}}
	fmt.Println(s.JobID, s.Budgets.StageSeconds)
	// Output: job 600
}

func ExampleDataResource() {
	var unknown backend.DataResource
	fmt.Println(unknown.Name == "", unknown.UID == "")
	// Output: true true
}

func ExampleDataResources() {
	var unresolved backend.DataResources
	fmt.Println(unresolved.Pod.UID == "", unresolved.Execution.Instance == "")
	// Output: true true
}

func ExampleDataCreated() {
	var unresolved backend.DataCreated
	fmt.Println(unresolved.Instance == "", unresolved.Pod.UID == "")
	// Output: true true
}

func ExampleContainerStart() {
	var missing backend.ContainerStart
	fmt.Println(missing.ContainerID == "", missing.StartedAt.IsZero())
	// Output: true true
}

func ExampleContainerExit() {
	var missing backend.ContainerExit
	// A default exit code is not evidence that execution occurred or stopped.
	fmt.Println(missing.ExitCode, missing.ContainerID == "", missing.FinishedAt.IsZero())
	// Output: 0 true true
}

func ExampleDataTaskStop() {
	var missing backend.DataTaskStop
	fmt.Println(missing.Stage == nil, missing.Setup == nil, missing.Task.ContainerID == "")
	// Output: true true true
}

func ExampleDataInitPhase() {
	fmt.Println(backend.StageFailure, backend.SetupFailure)
	// Output: 1 2
}

func ExampleDataNotStarted() {
	var missing backend.DataNotStarted
	fmt.Println(missing.Name == "", missing.WaitingReason == "")
	// Output: true true
}

func ExampleDataNoLaunch() {
	var unknown backend.DataNoLaunch
	fmt.Println(unknown.FailedInit == 0, unknown.TaskBlocked == nil)
	// Output: true true
}

func ExampleDataPhase() {
	fmt.Println(backend.DataStarting, backend.DataStage, backend.DataTask, backend.DataCollect, backend.DataSettled, backend.DataHeld)
	// Output: 1 2 3 4 5 6
}

func ExampleDataObservation() {
	held := backend.DataObservation{Status: backend.Status{State: backend.Unknown}, Phase: backend.DataHeld}
	fmt.Println(held.Phase == backend.DataHeld, held.TaskStop == nil, held.NoLaunch == nil)
	// Output: true true true
}

func ExampleDataRemoval() {
	var unresolved backend.DataRemoval
	fmt.Println(unresolved.PodRemoved, unresolved.InputRemoved, unresolved.OutputRemoved)
	// Output: false false false
}

func ExampleDataBudgets() {
	b := backend.DataBudgets{StageSeconds: 600, TaskSeconds: 30, CollectionSeconds: 600, ShutdownSeconds: 120}
	total, err := b.TotalSeconds()
	fmt.Println(total, err)
	// Output: 1350 <nil>
}

func ExampleDataPolicy() {
	p := backend.DefaultDataPolicy()
	fmt.Println(p.StageSeconds, p.CollectionSeconds, p.DefaultTaskSeconds, p.MaxTaskSeconds, p.ShutdownSeconds)
	// Output: 600 600 86400 604800 120
}

func ExampleDefaultDataPolicy() {
	p := backend.DefaultDataPolicy()
	fmt.Println(p.Validate())
	// Output: <nil>
}

func ExampleDataPolicy_Validate() {
	fmt.Println((backend.DataPolicy{}).Validate() != nil)
	// Output: true
}

func ExampleDataPolicy_Budgets() {
	b, err := backend.DefaultDataPolicy().Budgets(0)
	fmt.Println(b.TaskSeconds, err)
	// Output: 86400 <nil>
}

func ExampleDataBudgets_TotalSeconds() {
	_, err := (backend.DataBudgets{}).TotalSeconds()
	fmt.Println(err != nil)
	// Output: true
}

func ExampleDataDeadline() {
	start := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	deadline, err := backend.DataDeadline(start, 600)
	if err != nil {
		panic(err)
	}
	fmt.Println(deadline.Format(time.RFC3339))
	// Output: 2026-10-20T12:10:00Z
}

func ExampleCloneDataPolicy() {
	p := backend.DefaultDataPolicy()
	copy := backend.CloneDataPolicy(&p)
	copy.StageSeconds = 1
	fmt.Println(p.StageSeconds, copy.StageSeconds)
	// Output: 600 1
}

func ExampleValidateDataTask() {
	task := backend.Task{Argv: []string{"run"}, DataMounts: []backend.DataMount{{Reference: "cache", Destination: "/mnt/skygo/cache", ReadOnly: true}}}
	// The actual target caller must supply its complete trusted protected set.
	err := backend.ValidateDataTask(task, []string{"/workspace", "/skygo-helper", "/run/skygo/credentials"})
	fmt.Println(err)
	// Output: <nil>
}

func ExampleValidateDataCreated() {
	task := backend.Task{Argv: []string{"run"}, DataMounts: []backend.DataMount{{Reference: "cache", Destination: "/mnt/skygo/cache"}}}
	fmt.Println(backend.ValidateDataCreated(task, backend.DataCreated{}, false) != nil)
	// Output: true
}

func ExampleValidateDataResources() {
	fmt.Println(backend.ValidateDataResources(backend.DataResources{}) != nil)
	// Output: true
}

func ExampleValidateDataRemoval() {
	fmt.Println(backend.ValidateDataRemoval(backend.DataResources{}, backend.DataRemoval{PodRemoved: true}) != nil)
	// Output: true
}

func ExampleValidateDataObservation() {
	fmt.Println(backend.ValidateDataObservation(backend.Reference{}, backend.DataObservation{}) != nil)
	// Output: true
}

func ExampleCloneDataObservation() {
	o := backend.DataObservation{TaskStart: &backend.ContainerStart{ContainerID: "original"}}
	copy := backend.CloneDataObservation(o)
	copy.TaskStart.ContainerID = "changed"
	fmt.Println(o.TaskStart.ContainerID, copy.TaskStart.ContainerID)
	// Output: original changed
}
