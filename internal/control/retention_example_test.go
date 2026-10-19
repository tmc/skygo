package control_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

func ExampleControl_SubmitRetained_replay() {
	dir, err := os.MkdirTemp("", "skygo-retained-control-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	info, err := store.EnableRetention(dir, store.DefaultRetentionPolicy())
	if err != nil {
		panic(err)
	}
	s, err := store.Open(dir)
	if err != nil {
		panic(err)
	}
	defer s.Close()
	p := info.Policy
	b, err := local.NewRetained(filepath.Join(dir, "logs"), local.RetentionConfig{Owner: s.Owner(), OutputUnit: p.OutputUnit, Volume: p.Volume, OutputBytes: p.OutputBytes, OutputDomains: p.OutputDomains, OutputFiles: p.OutputFiles})
	if err != nil {
		panic(err)
	}
	defer b.Close()
	binding, err := backend.LocalBinding(s.Owner())
	if err != nil {
		panic(err)
	}
	target := backend.Target{Name: "local", Kind: "local", Binding: binding, Backend: b, OutputStorage: backend.LocalOutput, OutputReservation: b.OutputReservation(), Retention: b.Retention()}
	config := control.LocalOutputConfig{Target: target.Name, Binding: binding, BindOutput: b.BindOutput, OutputUsage: func(ctx context.Context) (uint64, uint64, error) {
		usage, err := b.OutputUsage(ctx)
		return usage.AllocatedBytes, usage.Files, err
	}}
	c, err := control.NewWithLocalOutput(s, []backend.Target{target}, testEngine, config)
	if err != nil {
		panic(err)
	}
	// Store acceptance is real, but Run is never started and no program runs.
	task := backend.Task{Argv: []string{"trainer"}}
	accepted, err := s.SubmitRetained(testActor(), info.Domain, "training", task, target.Name, binding, target.OutputStorage, target.OutputReservation)
	if err != nil {
		panic(err)
	}
	// Commit the output reservation without dispatching Backend.Start.
	accepted, start, err := s.BeginAttempt(accepted.ID, accepted.ID, b.OutputReservation(), accepted.CreatedAt)
	if err != nil || !start {
		panic(fmt.Errorf("begin attempt: %v", err))
	}
	// Replay precedes placement and storage admission sampling.
	replayed, err := c.SubmitRetained(testActor(), info.Domain, "training", task)
	fmt.Println(replayed.ID == accepted.ID, err)
	view, err := c.RetentionInfo(testActor(), 0, "")
	if err != nil {
		panic(err)
	}
	fmt.Println(view.Info.Enabled, view.Admin != nil, view.Admin.OutputFileCount == nil)
	fmt.Println(c.StorageReady()) // Run has not produced a physical sample.
	// Even acknowledged metadata selection must retain an unclosed local output.
	// A terminal observation does not become output closure or stopped proof.
	_, err = s.Update(accepted.ID, func(j *backend.Job) error { j.Status.State = backend.Succeeded; return nil })
	if err != nil {
		panic(err)
	}
	selection := store.PruneSelection{Before: accepted.CreatedAt.Add(time.Minute), Mode: store.MetadataOnly, AcknowledgeUnverifiedTerminal: true}
	preview, next, err := c.Prune(context.Background(), testActor(), selection, false, "")
	fmt.Println(preview.Selected, preview.Marked, next == "", err)
	if err != nil || len(preview.Skips) != 1 {
		panic(fmt.Errorf("prune preview: %v", err))
	}
	fmt.Println(preview.Skips[0].Reason)
	replayed, err = c.SubmitRetained(testActor(), info.Domain, "training", task)
	fmt.Println(replayed.ID == accepted.ID, err)
	// Output:
	// true <nil>
	// true true true
	// false
	// 0 0 true <nil>
	// proof_required
	// true <nil>
}
