//go:build linux

package control

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

// actualWorkspaceInput uses actual public Store+Files parser/Sync/seal receipts.
// Its explicit small-fixture capacity callback is test-only. It intentionally
// does not qualify the separately unimplemented Control upload growth wrappers.
func actualWorkspaceInput(t *testing.T, c *Control) artifact.Ref {
	t.Helper()
	var encoded bytes.Buffer
	writer := tar.NewWriter(&encoded)
	if err := writer.WriteHeader(&tar.Header{Name: "input.txt", Mode: 0600, Size: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	data := encoded.Bytes()
	hash := sha256.Sum256(data)
	owner, err := c.tryArtifactIO()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { owner.finish(err) }()
	upload, err := c.store.BeginArtifactUpload(testActor(), hex.EncodeToString(hash[:]), uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	ref, err := artifactPortableRef(upload.Reference)
	if err != nil {
		t.Fatal(err)
	}
	file, err := c.artifacts.files.CreatePart(ref)
	if err != nil {
		t.Fatal(err)
	}
	storedFile := physicalStoreFile(file)
	upload, err = c.store.ArtifactPartCreated(testActor(), store.ArtifactCursor{ID: ref.ID, Revision: upload.Revision}, storedFile)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := c.artifacts.files.WritePart(ref, file, 0, 0, data)
	if err != nil {
		t.Fatal(err)
	}
	upload, err = c.store.ArtifactChunkSynced(testActor(), store.ArtifactCursor{ID: ref.ID, Revision: upload.Revision}, storedFile, 0, uint64(len(data)), chunk.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	upload, err = c.store.BeginArtifactFinalize(testActor(), store.ArtifactCursor{ID: ref.ID, Revision: upload.Revision})
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := c.artifacts.files.SealPart(ref, file, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	manifest := store.ArtifactManifest{Reference: upload.Reference, ExpandedBytes: sealed.Manifest.ExpandedBytes, RegularFiles: sealed.Manifest.RegularFiles, Directories: sealed.Manifest.Directories}
	_, err = c.store.CompleteArtifactUpload(testActor(), store.ArtifactCursor{ID: ref.ID, Revision: upload.Revision}, upload.Reference, storedFile, manifest)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

// This is a real original Local no-call cancellation, not a synthesized Writer.
// Missing actual physical cache makes CheckGrowth refuse BEFORE materialization.
// The original same sequencer yields its unused owner, receives Cancel, positively
// settles its explicit no-call frontier and prepares its own NeverCreated origin.
// No command/setup/Collect is entered; real native producer tests remain OPEN.
func TestWorkspaceAtomicOriginalNoCallCancelAndReadBirth(t *testing.T) {
	for _, known := range []bool{false, true} {
		t.Run(map[bool]string{false: "unavailable", true: "known"}[known], func(t *testing.T) {
			name := t.TempDir()
			if err := os.Chmod(name, 0700); err != nil {
				t.Fatal(err)
			}
			root, err := os.OpenRoot(name)
			if err != nil {
				t.Fatal(err)
			}
			info, err := root.Lstat(".")
			if err != nil {
				t.Fatal(err)
			}
			stat, ok := info.Sys().(*syscall.Stat_t)
			if !ok {
				t.Fatal("missing actual root identity")
			}
			identity := artifact.FileID{Device: uint64(stat.Dev), Inode: uint64(stat.Ino)}
			facts, err := artifact.Initialize(root, identity, strings.Repeat("a", 32))
			if err != nil {
				t.Fatal(err)
			}
			files, err := artifact.Open(root, identity)
			if err != nil {
				t.Fatal(err)
			}
			directory := t.TempDir()
			if err := os.Chmod(directory, 0700); err != nil {
				t.Fatal(err)
			}
			if _, err := store.InstallArtifacts(directory, store.ArtifactInstallation{Enabled: true, ID: facts.ID, Unit: facts.Unit, Volume: facts.Volume}); err != nil {
				t.Fatal(err)
			}
			s, err := store.Open(directory)
			if err != nil {
				t.Fatal(err)
			}
			b, err := local.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			binding, err := backend.LocalBinding(s.Owner())
			if err != nil {
				t.Fatal(err)
			}
			target := backend.Target{Name: "local", Binding: binding, Backend: b, OutputStorage: backend.RemoteOutput}
			var producer local.ServerLifetime
			// Literal identity tests exact forwarding/CAS, NOT self capture or death.
			if known {
				producer = local.ServerLifetime{Host: strings.Repeat("1", 32), Boot: strings.Repeat("2", 32), NamespaceInode: 9, PID: 42}
			}
			c, err := NewWithConfig(s, []backend.Target{target}, testEngine, Config{ServerLifetime: producer, Artifacts: files, LocalBackend: b, LocalWorkspace: &LocalWorkspaceConfig{Target: target.Name, Binding: binding, Backend: b, Roots: root}})
			if err != nil {
				t.Fatal(err)
			}
			var state *loopState
			var stop context.CancelFunc
			t.Cleanup(func() {
				if state != nil {
					stop()
					for i := range state.workers {
						close(state.workers[i].input)
					}
					for i := range state.workers {
						select {
						case <-state.workers[i].done:
						case <-time.After(5 * time.Second):
							t.Error("HOLD original target worker did not join; resource frame retained")
							return
						}
					}
				}
				if err := b.Close(); err != nil {
					t.Errorf("HOLD original Local owners: %v", err)
					return
				}
				if err := c.DrainArtifacts(context.Background()); err != nil {
					t.Errorf("HOLD original artifact owners: %v", err)
					return
				}
				if err := files.Close(); err != nil {
					t.Errorf("HOLD original Files: %v", err)
					return
				}
				if err := s.Close(); err != nil {
					t.Error(err)
					return
				}
				if err := root.Close(); err != nil {
					t.Error(err)
				}
			})
			if !c.WorkspaceTargetEnabled(target.Name, binding) {
				t.Fatal("HOLD actual Local ownership prerequisites unavailable")
			}
			input := actualWorkspaceInput(t, c)
			original, err := c.TryArtifactCall()
			if err != nil {
				t.Fatal(err)
			}
			download, err := c.BeginArtifactDownload(context.Background(), testActor(), original, input)
			original.Done()
			if err != nil {
				t.Fatal(err)
			}
			if workspaceLocalProducer(download.lease.Producer) != producer {
				t.Fatal("read birth changed original known/zero tuple")
			}
			closeCall, err := c.TryArtifactCall()
			if err != nil {
				t.Fatal(err)
			}
			err = download.Close(closeCall)
			closeCall.Done()
			if err != nil {
				t.Fatal(err)
			}
			task := backend.Task{Argv: []string{"must-not-start"}, Workdir: &input}
			accepted, err := c.SubmitWorkspace(testActor(), "original-cancel", task)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			stop = cancel
			state = c.newLoop(ctx)
			ticker := time.NewTicker(2 * time.Millisecond)
			defer ticker.Stop()
			cancelled := false
			for {
				if err := c.schedule(ctx, state); err != nil {
					t.Fatal(err)
				}
				current, err := s.Get(accepted.ID)
				if err != nil {
					t.Fatal(err)
				}
				if !cancelled && currentStatus(current).Instance != "" {
					if _, err := c.Cancel(testActor(), accepted.ID); err != nil {
						t.Fatal(err)
					}
					cancelled = true
				}
				if p := state.workers[0].completion; p != nil && p.acked {
					break
				}
				select {
				case result := <-state.results:
					if err := c.receiveOperation(ctx, state, result); err != nil {
						t.Fatal(err)
					}
				case <-ticker.C:
				case <-ctx.Done():
					t.Fatal("HOLD original atomic completion did not settle before observation deadline")
				}
			}
			p := state.workers[0].completion
			if !cancelled || !p.prepared || !p.durable || p.commitState != store.WorkspaceCommitMarked || p.cleanup == nil || p.origin.RootState != local.WorkspaceRootNeverCreated || p.origin.CandidateState != local.WorkspacePhysicalNoOutputs || p.origin.Producer != producer || p.observation.Writer == nil || len(p.observation.Writer.Execution.Task.Argv) != 0 {
				t.Fatalf("actual original facts lost: %+v", p)
			}
			terminal, err := s.Get(accepted.ID)
			if err != nil || terminal.Status.State != backend.Canceled {
				t.Fatalf("atomic terminal: %+v %v", terminal, err)
			}
			if _, err := root.Lstat("workspace-" + p.job.CurrentAttemptID()); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("capacity/cancel path entered root creation", err)
			}
			if _, ok := c.retention.pending[p.job.CurrentAttemptID()]; !ok {
				t.Fatal("ack released charged root barrier")
			}
			replay, err := s.CompleteWorkspaceAttempt(p.job.ID, reference(p.job), p.status, p.completion, time.Now().UTC())
			if err != nil || replay.State != store.WorkspaceCommitMarked || replay.Cleanup == nil || *replay.Cleanup != *p.cleanup {
				t.Fatalf("exact completion replay changed original intent: %+v %v", replay, err)
			}
			before, err := s.ArtifactReservations()
			if err != nil {
				t.Fatal(err)
			}
			intent, err := workspaceLocalIntent(*p.cleanup)
			if err != nil {
				t.Fatal(err)
			}
			// Positive original handle handoff closes owners only; it creates no unlink,
			// RootRemoved, Store Finish or counter credit. The durable intent remains.
			if err := b.HandoffWorkspaceCleanup(context.Background(), intent); err != nil {
				t.Fatal(err)
			}
			after, err := s.ArtifactReservations()
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("handoff fabricated reservation/root removal", err)
			}
			if err := c.advanceWorkspaces(ctx, state); err != nil {
				t.Fatal(err)
			}
			if state.workers[0].pending != nil || len(state.workers[0].input) != 0 {
				t.Fatal("acked original completion reran backend work")
			}
		})
	}
}
