//go:build linux

package local

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
)

func assertWorkspaceNotEntered(t *testing.T, f *workspaceFixture, ref backend.Reference) {
	t.Helper()
	state := f.backend.PhysicalState()
	found := false
	for _, p := range state {
		if p.AttemptID == ref.ID {
			found = true
			if p.Transitioning || p.RootAttempted || p.CandidateAttempted || p.Root != (artifact.FileID{}) || p.Candidate != (artifact.FileID{}) {
				t.Fatalf("waiting fabricated physical facts: %+v", p)
			}
		}
	}
	if !found {
		t.Fatal("accepted binding mirror missing")
	}
	if _, err := f.root.Lstat("workspace-" + ref.ID); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("initial refusal entered root IO")
	}
	if _, err := os.Stat(filepath.Join(f.backend.directory, ref.ID+".log")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("initial refusal created output")
	}
}

func TestWorkspaceInitialGrowthWaitYieldsFourActualOwners(t *testing.T) {
	f := newWorkspaceFixture(t)
	f.backend.workspaces.config.CheckGrowth = func() error { return artifact.ErrFilesBusy }
	var starts atomic.Int32
	ops := f.backend.defaultOps
	f.backend.defaultOps.start = func(cmd *exec.Cmd) error { starts.Add(1); return ops.start(cmd) }
	var bindings []WorkspaceBinding
	for _, id := range []string{"6", "8", "9", "a"} {
		binding := f.binding
		binding.Task = backend.CloneTask(f.binding.Task)
		binding.Task.TimeoutSeconds = 0
		binding.AttemptID = strings.Repeat(id, 32)
		binding.ResultID = strings.Repeat(id, 32)
		f.slots <- struct{}{}
		if err := f.backend.BindWorkspace(binding, func() { <-f.slots; f.stageReleases.Add(1) }); err != nil {
			t.Fatal(err)
		}
		bindings = append(bindings, binding)
	}
	if len(f.slots) != 4 {
		t.Fatal("test did not transfer four actual initial owners")
	}
	var refs []backend.Reference
	for _, binding := range bindings {
		if err := f.backend.Start(context.Background(), binding.AttemptID, binding.Task); err != nil {
			t.Fatal(err)
		}
		s, err := f.backend.Observe(context.Background(), backend.Reference{ID: binding.AttemptID})
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, backend.Reference{ID: binding.AttemptID, Binding: binding.Binding, Instance: s.Instance, Task: backend.CloneTask(binding.Task)})
	}
	for _, ref := range refs {
		awaitWorkspacePhase(t, f, ref, "waiting-materialization")
		assertWorkspaceNotEntered(t, f, ref)
	}
	if f.stageReleases.Load() != 4 || len(f.slots) != 0 || starts.Load() != 0 {
		t.Fatalf("unused originals did not yield once: releases=%d slots=%d starts=%d", f.stageReleases.Load(), len(f.slots), starts.Load())
	}
	// The SAME ordinary common4 allocator now admits the sampler. This is an
	// actual Files interval walk, not a Control/Store cohort or fairness proof.
	release, ok := f.backend.workspaces.config.TryCollectionIO()
	if !ok {
		t.Fatal("yielded owners still occupy sampler slot")
	}
	scan, err := f.files.BeginPhysicalSample()
	if err != nil {
		t.Fatal(err)
	}
	var sample artifact.PhysicalSample
	for i := 0; i < 32; i++ {
		sample, err = scan.Step(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if sample.Status == artifact.PhysicalIntervalComplete {
			break
		}
	}
	if sample.Status != artifact.PhysicalIntervalComplete {
		t.Fatalf("bounded fixture did not complete interval: %+v", sample)
	}
	if err := scan.Close(); err != nil {
		t.Fatal(err)
	}
	var extra []func()
	for i := 0; i < 3; i++ {
		owner, ok := f.backend.workspaces.config.TryCollectionIO()
		if !ok {
			t.Fatal("existing four-slot capacity lost")
		}
		extra = append(extra, owner)
	}
	if fifth, ok := f.backend.workspaces.config.TryCollectionIO(); ok || fifth != nil {
		t.Fatal("a fifth common slot was invented")
	}
	for _, owner := range extra {
		owner()
	}
	release()
	for _, ref := range refs {
		if err := f.backend.Cancel(context.Background(), ref); err != nil {
			t.Fatal(err)
		}
		o := awaitWorkspace(t, f, ref)
		if !o.Ready || o.Held || o.Outcome != WorkspaceCanceled || o.Writer == nil || o.Status.Termination != backend.UnknownTermination {
			t.Fatalf("known no-call settlement=%+v", o)
		}
		assertWorkspaceNotEntered(t, f, ref)
	}
	if f.stageReleases.Load() != 4 || starts.Load() != 0 || len(f.slots) != 0 {
		t.Fatal("cancel re-released an unused owner or started a program")
	}
}

func TestWorkspaceInitialGrowthResumesWithoutEarlyIO(t *testing.T) {
	f := newWorkspaceFixture(t)
	var allow atomic.Bool
	f.backend.workspaces.config.CheckGrowth = func() error {
		if !allow.Load() {
			return artifact.ErrFilesBusy
		}
		return nil
	}
	ref := f.start(t)
	o := awaitWorkspacePhase(t, f, ref, "waiting-materialization")
	if o.Ready || o.Held || o.Writer != nil || f.stageReleases.Load() != 1 || len(f.slots) != 0 {
		t.Fatalf("initial pressure=%+v", o)
	}
	assertWorkspaceNotEntered(t, f, ref)
	allow.Store(true)
	if err := f.backend.ResumeWorkspacePublication(ref); err != nil {
		t.Fatal(err)
	}
	o = awaitWorkspace(t, f, ref)
	if !o.Ready || o.Held || o.Candidate == nil || o.Status.State != backend.Succeeded || f.stageReleases.Load() != 1 || f.collectionReleases.Load() != 2 {
		t.Fatalf("reacquired original stages=%+v releases=%d", o, f.collectionReleases.Load())
	}
}

func TestWorkspaceInitialTimeoutSettlesUnusedOwner(t *testing.T) {
	f := newWorkspaceFixture(t)
	f.binding.Task.TimeoutSeconds = 1
	f.backend.workspaces.config.CheckGrowth = func() error { return artifact.ErrFilesBusy }
	ref := f.start(t)
	awaitWorkspacePhase(t, f, ref, "waiting-materialization")
	o := awaitWorkspace(t, f, ref)
	if !o.Ready || o.Held || o.Outcome != WorkspaceTimeout || o.Status.Termination != backend.UnknownTermination || f.stageReleases.Load() != 1 || len(f.slots) != 0 {
		t.Fatalf("unstarted timeout settlement=%+v", o)
	}
	assertWorkspaceNotEntered(t, f, ref)
	if err := f.backend.Cancel(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	again, err := f.backend.WorkspaceObservation(ref)
	if err != nil || again.Outcome != WorkspaceTimeout {
		t.Fatal("later cancel overwrote first timeout intent")
	}
}

func TestWorkspaceCollectionGrowthRefusalIsNotFailure(t *testing.T) {
	f := newWorkspaceFixture(t)
	var allow atomic.Bool
	var checks atomic.Int32
	f.backend.workspaces.config.CheckGrowth = func() error {
		if checks.Add(1) == 1 {
			return nil
		}
		if !allow.Load() {
			return artifact.ErrFilesBusy
		}
		return nil
	}
	ref := f.start(t)
	o := awaitWorkspacePhase(t, f, ref, "waiting-collection")
	if o.Ready || o.Held || o.Writer != nil || len(f.slots) != 0 {
		t.Fatalf("collection capacity refusal=%+v", o)
	}
	state := f.backend.PhysicalState()
	if len(state) != 1 || state[0].Transitioning || !state[0].RootAttempted || state[0].CandidateAttempted {
		t.Fatalf("collection guard ran after transition/create: %+v", state)
	}
	if _, err := f.root.Lstat(f.binding.ResultID + ".part"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("denied collection created a candidate")
	}
	allow.Store(true)
	if err := f.backend.ResumeWorkspacePublication(ref); err != nil {
		t.Fatal(err)
	}
	o = awaitWorkspace(t, f, ref)
	if !o.Ready || o.Held || o.Candidate == nil || o.Status.State != backend.Succeeded {
		t.Fatalf("collection did not resume: %+v", o)
	}
}
