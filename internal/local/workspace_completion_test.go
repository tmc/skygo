//go:build linux

package local

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
)

// Literal identity tests DTO propagation/validation only. It is NOT a real self
// capture, original-owner death check or stopped cleanup qualification.
func fixtureServerLifetime() ServerLifetime {
	return ServerLifetime{Host: strings.Repeat("1", 32), Boot: strings.Repeat("2", 32), NamespaceInode: 7, PID: 42}
}

func TestWorkspacePrepareBeforeCommitFreezesOriginalFacts(t *testing.T) {
	for _, producer := range []ServerLifetime{{}, fixtureServerLifetime()} {
		t.Run(producer.Host, func(t *testing.T) {
			f := newWorkspaceProducerFixture(t, producer)
			ref := f.start(t)
			actual := awaitWorkspace(t, f, ref)
			release, ok := f.backend.workspaces.config.TryCollectionIO()
			if !ok {
				t.Fatal("preparer has no real owner")
			}
			o, p, err := f.backend.PrepareWorkspaceCompletion(ref)
			if err != nil {
				t.Fatal(err)
			}
			release()
			if !o.Ready || o.Writer == nil || p.Parent.Inode == 0 || p.Root.Inode == 0 || p.RootState != WorkspaceRootOwned || p.CandidateState != WorkspacePhysicalSealed || p.Prefix != nil || p.Producer != producer || o.Candidate == nil || *o.Candidate != *actual.Candidate {
				t.Fatalf("original prepare=%+v %+v", o, p)
			}
			f.backend.mu.Lock()
			w := f.backend.workspaces.bindings[ref.ID]
			c := w.cleanup
			if w.committed || c.marked || !c.prepared || !c.nativeRetired || c.root == nil || c.directory == nil || w.root != nil || w.directory != nil || len(c.observation.Writer.Execution.Task.Argv) != 0 {
				t.Fatal("precommit retired state or bounded payload wrong")
			}
			parent, root, directory := c.parent, c.root, c.directory
			f.backend.mu.Unlock()
			status, err := f.backend.Observe(context.Background(), ref)
			if err != nil || status.State != backend.Running {
				t.Fatal("preparation published terminal metadata")
			}
			// Returned pointer mutation cannot replace the retained original facts.
			o.Candidate.File = artifact.FileID{}
			o.Writer.RootID = "changed"
			*o.Status.ExitCode = 77
			p.Producer.Host = "changed"
			release, ok = f.backend.workspaces.config.TryCollectionIO()
			if !ok {
				t.Fatal("replay has no owner")
			}
			replayed, again, err := f.backend.PrepareWorkspaceCompletion(ref)
			if err != nil {
				t.Fatal(err)
			}
			release()
			if again.Producer != producer || again.Parent != p.Parent || again.Root != p.Root || replayed.Candidate.File != actual.Candidate.File || replayed.Writer.RootID != f.binding.RootID || *replayed.Status.ExitCode != 0 || c.parent != parent || c.root != root || c.directory != directory {
				t.Fatal("replay copied caller mutation or replaced original owner")
			}
			intent := workspaceCleanupIntent(fixtureWorkspaceOrigin(workspaceCleanupSubject(t, f, ref, replayed), again))
			if _, err := f.backend.RemoveWorkspaceRoot(context.Background(), intent); err == nil {
				t.Fatal("unbound future intent deleted before actual commit acknowledgement")
			}
			if err := f.backend.CommitWorkspace(ref, intent); err != nil {
				t.Fatal(err)
			}
			if err := f.backend.CommitWorkspace(ref, intent); err != nil {
				t.Fatal("exact intent ack was not idempotent")
			}
			if status, err := f.backend.Observe(context.Background(), ref); err != nil || status.State != backend.Succeeded {
				t.Fatal("bound completion did not project actual status")
			}
			if err := f.backend.HandoffWorkspaceCleanup(context.Background(), intent); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWorkspaceCommitRefusesForeignPreparedIntent(t *testing.T) {
	f := newWorkspaceProducerFixture(t, fixtureServerLifetime())
	ref := f.start(t)
	o := awaitWorkspace(t, f, ref)
	release, ok := f.backend.workspaces.config.TryCollectionIO()
	if !ok {
		t.Fatal("no prepare permit")
	}
	_, p, err := f.backend.PrepareWorkspaceCompletion(ref)
	if err != nil {
		t.Fatal(err)
	}
	release()
	original := workspaceCleanupIntent(fixtureWorkspaceOrigin(workspaceCleanupSubject(t, f, ref, o), p))
	cases := []struct {
		name   string
		change func(*WorkspaceRootIntent)
	}{
		{"owner", func(i *WorkspaceRootIntent) { i.Origin.Subject.Owner = "other" }},
		{"task", func(i *WorkspaceRootIntent) { i.Origin.Subject.TaskDigest[0] ^= 1 }},
		{"instance", func(i *WorkspaceRootIntent) { i.Origin.Subject.Instance = strings.Repeat("f", 32) }},
		{"root", func(i *WorkspaceRootIntent) { i.Origin.Root.Inode++ }},
		{"result", func(i *WorkspaceRootIntent) { i.Origin.Subject.ResultFile.Inode++ }},
		{"producer", func(i *WorkspaceRootIntent) { i.Origin.Producer.StartTicks++ }},
		{"partial producer", func(i *WorkspaceRootIntent) { i.Origin.Producer.Boot = "" }},
		{"completion", func(i *WorkspaceRootIntent) { i.Origin.Subject.CompletionDigest = [32]byte{} }},
		{"writer", func(i *WorkspaceRootIntent) { i.Origin.Subject.WriterReceipt = [32]byte{} }},
		{"disposition", func(i *WorkspaceRootIntent) { i.Origin.Disposition = WorkspaceCandidateNeverCreated }},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			bad := original
			tt.change(&bad)
			if err := f.backend.CommitWorkspace(ref, bad); !errors.Is(err, backend.ErrIdentityMismatch) {
				t.Fatalf("foreign ack=%v", err)
			}
			if f.backend.workspaces.bindings[ref.ID].committed {
				t.Fatal("refused ack mutated committed state")
			}
		})
	}
	if err := f.backend.CommitWorkspace(ref, original); err != nil {
		t.Fatal(err)
	}
	changed := original
	changed.Digest[0] ^= 1
	if err := f.backend.CommitWorkspace(ref, changed); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatal("bound replay changed intent digest")
	}
	if err := f.backend.HandoffWorkspaceCleanup(context.Background(), original); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceCanceledClosedPrefixPreparesWithoutFinalSubject(t *testing.T) {
	f := newWorkspaceFixture(t)
	var checks atomic.Int32
	f.backend.workspaces.config.CheckGrowth = func() error {
		if checks.Add(1) <= 2 {
			return nil
		}
		return artifact.ErrFilesBusy
	}
	ref := f.start(t)
	awaitWorkspacePhase(t, f, ref, "waiting-publication")
	if _, _, err := f.backend.PrepareWorkspaceCompletion(ref); err == nil {
		t.Fatal("waiting candidate supplied origin")
	}
	if err := f.backend.Cancel(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	o := awaitWorkspace(t, f, ref)
	release, ok := f.backend.workspaces.config.TryCollectionIO()
	if !ok {
		t.Fatal("no prepare owner")
	}
	_, p, err := f.backend.PrepareWorkspaceCompletion(ref)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if p.CandidateState != WorkspacePhysicalClosedPrefix || p.Prefix == nil || p.Prefix.ID != f.binding.ResultID || p.Prefix.File.Inode == 0 || o.Outcome != WorkspaceCanceled {
		t.Fatalf("closed-prefix frontier=%+v %+v", o, p)
	}
	prefix := *p.Prefix
	p.Prefix.ID = "foreign"
	p.Prefix.File = artifact.FileID{}
	release, ok = f.backend.workspaces.config.TryCollectionIO()
	if !ok {
		t.Fatal("no replay owner")
	}
	_, again, err := f.backend.PrepareWorkspaceCompletion(ref)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if again.Prefix == nil || *again.Prefix != prefix {
		t.Fatal("prefix replay retained caller mutation")
	}
	intent := workspaceCleanupIntent(fixtureWorkspaceOrigin(workspaceCleanupSubject(t, f, ref, o), again))
	bad := intent
	bad.Origin.CandidateSize++
	if err := f.backend.CommitWorkspace(ref, bad); err == nil {
		t.Fatal("different original prefix was acknowledged")
	}
	if err := f.backend.CommitWorkspace(ref, intent); err != nil {
		t.Fatal(err)
	}
	if err := f.backend.HandoffWorkspaceCleanup(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceConfigurationRejectsPartialProducer(t *testing.T) {
	f := newWorkspaceFixture(t)
	cases := []struct {
		name     string
		producer ServerLifetime
		valid    bool
	}{
		{"unavailable", ServerLifetime{}, true},
		{"known with zero device and ticks", fixtureServerLifetime(), true},
		{"host only", ServerLifetime{Host: strings.Repeat("1", 32)}, false},
		{"pid only", ServerLifetime{PID: 42}, false},
		{"newline host", ServerLifetime{Host: strings.Repeat("1", 32) + "\n", Boot: strings.Repeat("2", 32), NamespaceInode: 7, PID: 42}, false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			b, err := New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			config := f.backend.workspaces.config
			config.ServerLifetime = tt.producer
			err = b.ConfigureWorkspace(config)
			if (err == nil) != tt.valid {
				t.Fatalf("configured partial producer=%v", err)
			}
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
