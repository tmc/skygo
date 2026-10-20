package control

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

// These immutable intent values test mapping/ack ownership ONLY. They are not
// passed off as a real Store commit, producer, Writer or kernel-close receipt.
func workspaceIntentVector(j backend.Job) store.WorkspaceCleanupWork {
	input := store.ArtifactRef{ID: j.Task.Workdir.ID, SHA256: j.Task.Workdir.SHA256, Size: uint64(j.Task.Workdir.Size)}
	return store.WorkspaceCleanupWork{ID: strings.Repeat("f", 32), Revision: 7, Digest: [32]byte{3}, Origin: store.WorkspaceCleanupOrigin{
		Subject: store.ArtifactWorkspaceCleanupSubject{Owner: "legacy", JobID: j.ID, AttemptID: j.CurrentAttemptID(), Binding: j.TargetBinding, Instance: currentStatus(j).Instance, RootID: strings.Repeat("e", 32), TaskDigest: [32]byte{4}, CompletionDigest: [32]byte{5}, WriterReceipt: [32]byte{6}, InputRef: input, ResultState: "none"},
		Parent:  store.ArtifactFile{Device: 7, Inode: 1}, RootState: store.WorkspaceRootNeverCreated, Disposition: store.WorkspaceNoOutputs,
	}}
}

func TestWorkspaceAtomicProducerShapeAndRoundTrip(t *testing.T) {
	known := local.ServerLifetime{Host: strings.Repeat("1", 32), Boot: strings.Repeat("2", 32), NamespaceDevice: 0, NamespaceInode: 9, PID: 42, StartTicks: 0}
	other := known
	other.NamespaceDevice = 11
	other.StartTicks = 123
	for _, value := range []local.ServerLifetime{{}, known, other} {
		stored, err := workspaceStoreProducer(value)
		if err != nil || workspaceLocalProducer(stored) != value {
			t.Fatalf("known/zero roundtrip: %+v %v", stored, err)
		}
	}
	for _, value := range []local.ServerLifetime{{Host: known.Host}, {PID: 42}, {Host: known.Host, Boot: known.Boot, PID: 42}, {Host: strings.Repeat("0", 32), Boot: known.Boot, NamespaceInode: 9, PID: 42}, {Host: known.Host + "\n", Boot: known.Boot, NamespaceInode: 9, PID: 42}, {Host: known.Host, Boot: known.Boot, NamespaceInode: 9, PID: 1 << 31}} {
		if _, err := workspaceStoreProducer(value); !errors.Is(err, ErrInvalid) {
			t.Fatalf("partial/noncanonical original identity admitted: %+v %v", value, err)
		}
		// A nonnil Store/target is deliberately absent. Invalid identity must refuse
		// construction before root or Store access rather than become zero later.
		if _, err := NewWithConfig(&store.Store{}, []backend.Target{{}}, testEngine, Config{ServerLifetime: value}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("constructor did not reject malformed identity first: %v", err)
		}
	}
}

func TestWorkspaceAtomicOriginMapsAllOriginalStates(t *testing.T) {
	known := local.ServerLifetime{Host: strings.Repeat("1", 32), Boot: strings.Repeat("2", 32), NamespaceInode: 9, PID: 42}
	tests := []struct {
		name  string
		state local.WorkspacePhysicalCandidateState
		want  store.WorkspacePhysicalCandidateState
	}{
		{"no outputs", local.WorkspacePhysicalNoOutputs, store.WorkspacePhysicalNoOutputs},
		{"never attempted", local.WorkspacePhysicalNeverAttempted, store.WorkspacePhysicalNeverAttempted},
		{"closed prefix", local.WorkspacePhysicalClosedPrefix, store.WorkspacePhysicalClosedPrefix},
		{"sealed", local.WorkspacePhysicalSealed, store.WorkspacePhysicalSealed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			origin := local.WorkspacePhysicalOrigin{Parent: artifact.FileID{Device: 7, Inode: 1}, Root: artifact.FileID{Device: 7, Inode: 2}, RootState: local.WorkspaceRootOwned, CandidateState: tt.state, Producer: known}
			if tt.state == local.WorkspacePhysicalClosedPrefix {
				origin.Prefix = &local.WorkspaceCandidatePrefix{ID: strings.Repeat("c", 32), File: artifact.FileID{Device: 7, Inode: 3}, Size: 0, SHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}
			}
			mapped, err := workspaceStoreOrigin(origin)
			if err != nil {
				t.Fatal(err)
			}
			if mapped.Parent != physicalStoreFile(origin.Parent) || mapped.Root != physicalStoreFile(origin.Root) || mapped.RootState != store.WorkspaceRootOwned || mapped.CandidateState != tt.want || workspaceLocalProducer(mapped.Producer) != known {
				t.Fatal("physical origin field lost")
			}
			if origin.Prefix != nil {
				if mapped.Prefix == nil || mapped.Prefix.ID != origin.Prefix.ID || mapped.Prefix.File != physicalStoreFile(origin.Prefix.File) || mapped.Prefix.Size != 0 || mapped.Prefix.SHA256 != origin.Prefix.SHA256 {
					t.Fatal("empty/malformed-byte prefix became a Ref or lost identity")
				}
				origin.Prefix.Size = 17
				origin.Prefix.SHA256 = strings.Repeat("d", 64)
				nonempty, err := workspaceStoreOrigin(origin)
				if err != nil || nonempty.Prefix.Size != 17 || nonempty.Prefix.SHA256 != origin.Prefix.SHA256 {
					t.Fatal("nonempty prefix lost size/hash", err)
				}
				origin.Prefix.ID = "changed"
				if mapped.Prefix.ID == "changed" {
					t.Fatal("mapped prefix aliases original result")
				}
			} else if mapped.Prefix != nil {
				t.Fatal("non-prefix state fabricated prefix")
			}
		})
	}
	for _, origin := range []local.WorkspacePhysicalOrigin{{}, {RootState: local.WorkspaceRootOwned, CandidateState: 99}, {RootState: local.WorkspaceRootOwned, CandidateState: local.WorkspacePhysicalClosedPrefix}, {RootState: local.WorkspaceRootOwned, CandidateState: local.WorkspacePhysicalSealed, Prefix: &local.WorkspaceCandidatePrefix{}}} {
		if _, err := workspaceStoreOrigin(origin); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("unknown/frontier mismatch admitted: %v", err)
		}
	}
}

func TestWorkspaceAtomicCommitStatesRetainOriginalFrame(t *testing.T) {
	j := backend.Job{ID: strings.Repeat("a", 32), Target: "local", TargetBinding: "binding", Task: backend.Task{Argv: []string{"unused"}, Workdir: &artifact.Ref{ID: strings.Repeat("b", 32), SHA256: strings.Repeat("c", 64), Size: 1024}}, Status: backend.Status{State: backend.Running, Instance: "original"}}
	c := &Control{}
	held := &workspacePending{job: cloneJob(j), prepared: true}
	if err := c.acceptWorkspaceCommit(held, store.WorkspaceCommit{State: store.WorkspaceCommitHeld}); !errors.Is(err, ErrUnavailable) || held.durable || held.cleanup != nil || held.acked {
		t.Fatalf("Held became terminal/ack: %+v %v", held, err)
	}
	removed := &workspacePending{job: cloneJob(j), prepared: true}
	if err := c.acceptWorkspaceCommit(removed, store.WorkspaceCommit{State: store.WorkspaceCommitRemoved}); !errors.Is(err, ErrUnavailable) || !removed.held || removed.durable || removed.cleanup != nil || removed.acked {
		t.Fatalf("Removed fabricated zero intent/retirement: %+v %v", removed, err)
	}
	work := workspaceIntentVector(j)
	marked := &workspacePending{job: cloneJob(j), prepared: true}
	if err := c.acceptWorkspaceCommit(marked, store.WorkspaceCommit{State: store.WorkspaceCommitMarked, Cleanup: &work}); err != nil || !marked.durable || marked.cleanup == nil || *marked.cleanup != work || marked.acked {
		t.Fatalf("marked exact intent lost: %+v %v", marked, err)
	}
	work.Revision++
	if marked.cleanup.Revision == work.Revision {
		t.Fatal("caller mutation changed retained actual intent")
	}
}

func TestWorkspaceAtomicReceiveErrorAndCancellationKeepOriginalPrepare(t *testing.T) {
	j := backend.Job{ID: strings.Repeat("a", 32), Target: "local", TargetBinding: "binding", Task: backend.Task{Argv: []string{"unused"}, Workdir: &artifact.Ref{ID: strings.Repeat("b", 32), SHA256: strings.Repeat("c", 64), Size: 1024}}, Status: backend.Status{State: backend.Running, Instance: "original"}}
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "integration error", true: "cancelled"}[cancelled], func(t *testing.T) {
			c := artifactCallControl()
			c.retention.pending = map[string]struct{}{j.ID: {}}
			owner, err := c.tryArtifactIO()
			if err != nil {
				t.Fatal(err)
			}
			op := &operation{kind: workspacePrepareOperation, job: cloneJob(j), prepareOwner: owner}
			prepared := &workspacePrepared{} // deliberately INVALID, never producer proof
			result := operationResult{operation: *op, prepared: prepared}
			state := &loopState{workers: []targetWorker{{target: backend.Target{Name: "local"}, pending: op, completion: &workspacePending{job: cloneJob(j)}}}, due: make(map[string]time.Time)}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if cancelled {
				cancel()
			}
			if err := c.receiveOperation(ctx, state, result); err == nil {
				t.Fatal("invalid/missing original preparation integrated")
			}
			w := &state.workers[0]
			if w.pending != op || w.received == nil || w.received.request != op || w.received.result.prepared != prepared || w.received.request.prepareOwner != owner || w.completion.prepared {
				t.Fatal("F1 lost exact received request/result/owner")
			}
			// This unit never invoked IO; its test-owned unused admission can settle.
			owner.finish(nil)
		})
	}
}

func TestWorkspaceAtomicIntentCopiesActualSubjectAndOrigin(t *testing.T) {
	j := backend.Job{ID: strings.Repeat("a", 32), Target: "local", TargetBinding: "binding", Task: backend.Task{Argv: []string{"unused"}, Workdir: &artifact.Ref{ID: strings.Repeat("b", 32), SHA256: strings.Repeat("c", 64), Size: 1024}}, Status: backend.Status{State: backend.Running, Instance: "original"}}
	work := workspaceIntentVector(j)
	work.Origin.RootState = store.WorkspaceRootOwned
	work.Origin.Root = store.ArtifactFile{Device: 7, Inode: 2}
	work.Origin.CandidateID = strings.Repeat("d", 32)
	work.Origin.CandidateFile = store.ArtifactFile{Device: 7, Inode: 3}
	work.Origin.CandidateSize = 17
	work.Origin.CandidateSHA256 = strings.Repeat("e", 64)
	work.Origin.Producer = store.OriginalServerLifetime{Host: strings.Repeat("1", 32), Boot: strings.Repeat("2", 32), NamespaceDevice: 5, NamespaceInode: 9, PID: 42, StartTicks: 123}
	work.Origin.Subject.ResultID = strings.Repeat("d", 32)
	work.Origin.Subject.ResultState = "complete"
	work.Origin.Subject.ResultRef = store.ArtifactRef{ID: work.Origin.Subject.ResultID, SHA256: strings.Repeat("e", 64), Size: 1024}
	work.Origin.Subject.ResultFile = store.ArtifactFile{Device: 7, Inode: 4}
	states := []struct {
		in  store.WorkspaceCandidateDisposition
		out local.WorkspaceCandidateDisposition
	}{
		{store.WorkspaceNoOutputs, local.WorkspaceNoOutputs}, {store.WorkspaceCandidateNeverCreated, local.WorkspaceCandidateNeverCreated}, {store.WorkspaceCandidateClosedUnpublished, local.WorkspaceCandidateClosedUnpublished}, {store.WorkspaceResultPublishedIndependent, local.WorkspaceResultPublishedIndependent},
	}
	for _, state := range states {
		// Deliberately exhaustive plain-field mapping vectors. Local/Store validate
		// legal cross-field combinations independently; these are not commit proofs.
		work.Origin.Disposition = state.in
		mapped, err := workspaceLocalIntent(work)
		if err != nil {
			t.Fatal(err)
		}
		a, b := work.Origin, mapped.Origin
		x, y := a.Subject, b.Subject
		if mapped.ID != work.ID || mapped.Revision != work.Revision || mapped.Digest != work.Digest || b.Parent != (artifact.FileID{Device: a.Parent.Device, Inode: a.Parent.Inode}) || b.Root != (artifact.FileID{Device: a.Root.Device, Inode: a.Root.Inode}) || b.RootState != local.WorkspaceRootOwned || b.Disposition != state.out || b.CandidateID != a.CandidateID || physicalStoreFile(b.CandidateFile) != a.CandidateFile || b.CandidateSize != a.CandidateSize || b.CandidateSHA256 != a.CandidateSHA256 || workspaceLocalProducer(a.Producer) != b.Producer {
			t.Fatal("actual intent/origin field omitted")
		}
		if x.Owner != y.Owner || x.JobID != y.JobID || x.AttemptID != y.AttemptID || x.Binding != y.Binding || x.Instance != y.Instance || x.RootID != y.RootID || x.TaskDigest != y.TaskDigest || x.CompletionDigest != y.CompletionDigest || x.WriterReceipt != y.WriterReceipt || y.InputRef != *j.Task.Workdir || x.ResultID != y.ResultID || x.ResultState != y.ResultState || x.ResultRef.ID != y.ResultRef.ID || x.ResultRef.SHA256 != y.ResultRef.SHA256 || x.ResultRef.Size != uint64(y.ResultRef.Size) || x.ResultFile != physicalStoreFile(y.ResultFile) {
			t.Fatal("actual opaque subject field omitted/reconstructed")
		}
	}
}
