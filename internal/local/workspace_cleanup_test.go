//go:build linux

package local

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
)

// Opaque CAS fences below are fixture values, not a Store transaction or writer
// proof. All native/root/candidate facts still come from the actual sequencer.
func workspaceCleanupSubject(t *testing.T, f *workspaceFixture, ref backend.Reference, o WorkspaceObservation) WorkspaceCleanupIdentity {
	t.Helper()
	data, err := backend.TaskBytes(ref.Task)
	if err != nil {
		t.Fatal(err)
	}
	s := WorkspaceCleanupIdentity{Owner: f.binding.Owner, JobID: f.binding.JobID, AttemptID: ref.ID, Binding: ref.Binding, Instance: ref.Instance, RootID: f.binding.RootID, TaskDigest: sha256.Sum256(data), CompletionDigest: sha256.Sum256([]byte("fixture Store completion CAS")), WriterReceipt: sha256.Sum256([]byte("fixture Store writer CAS")), InputRef: f.binding.Input, ResultID: f.binding.ResultID, ResultState: "none"}
	if s.ResultID != "" {
		s.ResultState = "reserved"
	}
	if o.Candidate != nil {
		s.ResultState = "complete"
		s.ResultRef = o.Candidate.Manifest.Reference
		s.ResultFile = o.Candidate.File
	}
	return s
}
func workspaceCleanupIntent(origin WorkspaceCleanupOrigin) WorkspaceRootIntent {
	return WorkspaceRootIntent{ID: strings.Repeat("a", 32), Revision: 1, Digest: sha256.Sum256([]byte("fixture committed intent CAS")), Origin: origin}
}

// These exact DTO fixtures exercise Local binding, NOT a real Store transaction.
// Original physical facts still come only from actual Local precommit prepare.
func fixturePrepareWorkspaceCleanup(b *Backend, ref backend.Reference, subject WorkspaceCleanupIdentity) (WorkspaceCleanupOrigin, error) {
	release, ok := b.workspaces.config.TryCollectionIO()
	if !ok || release == nil {
		return WorkspaceCleanupOrigin{}, artifact.ErrFilesBusy
	}
	_, p, err := b.PrepareWorkspaceCompletion(ref)
	if !errors.Is(err, artifact.ErrFileCloseUncertain) {
		release()
	} // actual positive return, never uncertainty credit
	if err != nil {
		return WorkspaceCleanupOrigin{}, err
	}
	origin := fixtureWorkspaceOrigin(subject, p)
	if err := b.CommitWorkspace(ref, workspaceCleanupIntent(origin)); err != nil {
		return WorkspaceCleanupOrigin{}, err
	}
	return origin, nil
}

func fixtureWorkspaceOrigin(subject WorkspaceCleanupIdentity, p WorkspacePhysicalOrigin) WorkspaceCleanupOrigin {
	origin := WorkspaceCleanupOrigin{Subject: subject, Parent: p.Parent, Root: p.Root, RootState: p.RootState, Producer: p.Producer}
	switch p.CandidateState {
	case WorkspacePhysicalNoOutputs:
		origin.Disposition = WorkspaceNoOutputs
	case WorkspacePhysicalNeverAttempted:
		origin.Disposition = WorkspaceCandidateNeverCreated
	case WorkspacePhysicalClosedPrefix:
		origin.Disposition = WorkspaceCandidateClosedUnpublished
		origin.CandidateID = p.Prefix.ID
		origin.CandidateFile = p.Prefix.File
		origin.CandidateSize = p.Prefix.Size
		origin.CandidateSHA256 = p.Prefix.SHA256
	case WorkspacePhysicalSealed:
		origin.Disposition = WorkspaceResultPublishedIndependent
	}
	return origin
}
func prepareWorkspaceCleanup(t *testing.T, f *workspaceFixture) (backend.Reference, WorkspaceRootIntent) {
	t.Helper()
	ref := f.start(t)
	o := awaitWorkspace(t, f, ref)
	if !o.Ready || o.Writer == nil {
		t.Fatalf("actual owned writer not closed: %+v", o)
	}
	subject := workspaceCleanupSubject(t, f, ref, o)
	origin, err := fixturePrepareWorkspaceCleanup(f.backend, ref, subject)
	if err != nil {
		t.Fatal(err)
	}
	return ref, workspaceCleanupIntent(origin)
}
func finishWorkspaceCleanup(t *testing.T, b *Backend, intent WorkspaceRootIntent) int {
	t.Helper()
	for n := 1; n <= 20; n++ {
		p, err := b.RemoveWorkspaceRoot(context.Background(), intent)
		if err != nil {
			t.Fatal(err)
		}
		if p.Visited > 100 {
			t.Fatal("unbounded quantum")
		}
		if p.Done {
			return n
		}
	}
	t.Fatal("bounded fixture did not finish")
	return 0
}

func TestWorkspaceCleanupQuantaKeepExternalResultAndLogs(t *testing.T) {
	f := newWorkspaceFixture(t)
	ref := f.start(t)
	o := awaitWorkspace(t, f, ref)
	w := f.backend.workspaces.bindings[ref.ID]
	for i := 0; i < 101; i++ {
		file, err := w.root.Create(fmt.Sprintf("entry-%03d", i))
		if err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	outside := filepath.Join(t.TempDir(), "sentinel")
	if err := os.WriteFile(outside, []byte("untouched"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, fmt.Sprintf("/proc/self/fd/%d/outside-link", w.directory.Fd())); err != nil {
		t.Fatal(err)
	}
	subject := workspaceCleanupSubject(t, f, ref, o)
	origin, err := fixturePrepareWorkspaceCleanup(f.backend, ref, subject)
	if err != nil {
		t.Fatal(err)
	}
	intent := workspaceCleanupIntent(origin)
	if w.parent.cmd != nil || w.parent.owned != nil || w.phase != nil || !w.cleanup.nativeRetired || w.cleanup.root == nil {
		t.Fatal("native/root ownership was not separated")
	}
	if rounds := finishWorkspaceCleanup(t, f.backend, intent); rounds < 2 {
		t.Fatal("101 entries completed in one quantum")
	}
	if _, err := f.root.Lstat("workspace-" + ref.ID); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("root remains: %v", err)
	}
	if _, err := f.root.Lstat(f.binding.ResultID + ".tar"); err != nil {
		t.Fatal("root cleanup touched independent published result")
	}
	data, err := os.ReadFile(outside)
	if err != nil || string(data) != "untouched" {
		t.Fatalf("outside sentinel changed: %q %v", data, err)
	}
	log, err := f.backend.Logs(context.Background(), ref, 0, backend.MaxChunkBytes)
	if err != nil || !strings.Contains(string(log.Data), "task output") {
		t.Fatalf("closed independent logs lost: %q %v", log.Data, err)
	}
	if err := f.backend.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceCleanupHandoffKeepsNamespacesAndExactFence(t *testing.T) {
	f := newWorkspaceFixture(t)
	ref, intent := prepareWorkspaceCleanup(t, f)
	if err := f.backend.HandoffWorkspaceCleanup(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	if err := f.backend.HandoffWorkspaceCleanup(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	wrong := intent
	wrong.Digest[0] ^= 1
	if err := f.backend.HandoffWorkspaceCleanup(context.Background(), wrong); err == nil {
		t.Fatal("conflicting intent accepted")
	}
	if _, err := f.root.Lstat("workspace-" + ref.ID); err != nil {
		t.Fatal("handoff removed root")
	}
	if _, err := f.root.Lstat(f.binding.ResultID + ".tar"); err != nil {
		t.Fatal("handoff removed external candidate")
	}
	if p, err := f.backend.RemoveWorkspaceRoot(context.Background(), intent); err == nil || p.Done {
		t.Fatal("handoff fabricated cleanup completion")
	}
	if err := f.backend.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.backend.RemoveWorkspaceRoot(context.Background(), intent); err == nil {
		t.Fatal("post-Close I/O admitted")
	}
}

func TestWorkspaceCleanupPartialHandoffAndExactRestart(t *testing.T) {
	f := newWorkspaceFixture(t)
	ref := f.start(t)
	o := awaitWorkspace(t, f, ref)
	w := f.backend.workspaces.bindings[ref.ID]
	for i := 0; i < 101; i++ {
		file, err := w.root.Create(fmt.Sprintf("entry-%03d", i))
		if err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	subject := workspaceCleanupSubject(t, f, ref, o)
	origin, err := fixturePrepareWorkspaceCleanup(f.backend, ref, subject)
	if err != nil {
		t.Fatal(err)
	}
	intent := workspaceCleanupIntent(origin)
	p, err := f.backend.RemoveWorkspaceRoot(context.Background(), intent)
	if err != nil || p.Done || p.Visited != 100 {
		t.Fatalf("first quantum=%+v %v", p, err)
	}
	if err := f.backend.HandoffWorkspaceCleanup(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	if err := f.backend.Close(); err != nil {
		t.Fatal(err)
	}
	// New concrete backend receives only the trusted exact already-marked work.
	// No binding/job/Task/Start/native receipt is recreated from disk presence.
	restarted, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	config := f.backend.workspaces.config
	config.PhysicalChanged = func() {
		state := restarted.PhysicalState()
		if len(state) != 1 || state[0].CandidateID != intent.Origin.Subject.ResultID {
			t.Errorf("restart result identity did not come from committed intent: %+v", state)
		}
	}
	if err := restarted.ConfigureWorkspace(config); err != nil {
		t.Fatal(err)
	}
	if _, _, err := restarted.PrepareWorkspaceCompletion(ref); err == nil {
		t.Fatal("premark original receipt adopted after restart")
	}
	finishWorkspaceCleanup(t, restarted, intent)
	if len(restarted.jobs) != 0 || len(restarted.workspaces.bindings) != 0 {
		t.Fatal("cleanup restart manufactured a job or launch binding")
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceCleanupCloseErrorRetainsOriginalOwner(t *testing.T) {
	for _, rootClose := range []bool{false, true} {
		t.Run(fmt.Sprint(rootClose), func(t *testing.T) {
			f := newWorkspaceFixture(t)
			_, intent := prepareWorkspaceCleanup(t, f)
			c := f.backend.workspaces.bindings[intent.Origin.Subject.AttemptID].cleanup
			originalFile, originalRoot := c.directory, c.root
			diagnostic := errors.New("diagnostic after actual original close")
			var closes atomic.Int32
			if rootClose {
				f.backend.workspaceCleanupOps.closeRoot = func(root *os.Root) error {
					if root != originalRoot {
						return root.Close()
					}
					closes.Add(1)
					if err := root.Close(); err != nil {
						return err
					}
					return diagnostic
				}
			} else {
				f.backend.workspaceCleanupOps.closeFile = func(file *os.File) error {
					if file != originalFile {
						return file.Close()
					}
					closes.Add(1)
					if err := file.Close(); err != nil {
						return err
					}
					return diagnostic
				}
			}
			f.expectHeld = true
			_, err := f.backend.RemoveWorkspaceRoot(context.Background(), intent)
			if !errors.Is(err, artifact.ErrFileCloseUncertain) || !errors.Is(err, diagnostic) || c.err != err {
				t.Fatalf("original Close outcome lost: %v", err)
			}
			if rootClose {
				if c.root != originalRoot {
					t.Fatal("original uncertain Root replaced")
				}
			} else if c.directory != originalFile {
				t.Fatal("original uncertain File replaced")
			}
			_, again := f.backend.RemoveWorkspaceRoot(context.Background(), intent)
			if again != err {
				t.Fatal("same original error not returned")
			}
			if err := f.backend.HandoffWorkspaceCleanup(context.Background(), intent); err != again {
				t.Fatal("handoff replaced uncertain original owner")
			}
			if closes.Load() != 1 {
				t.Fatal("uncertain raw Close retried")
			}
			// Fault diagnostic follows a real positive test-owned Close, not a kernel
			// failure. Test alone closes its other exact original fixture handles.
			t.Cleanup(func() {
				if c.root != nil && !rootClose {
					if err := c.root.Close(); err != nil {
						t.Error(err)
					}
				}
				if c.parentDir != nil {
					if err := c.parentDir.Close(); err != nil {
						t.Error(err)
					}
				}
				if c.parent != nil {
					if err := c.parent.Close(); err != nil {
						t.Error(err)
					}
				}
			})
		})
	}
}

func TestWorkspaceCleanupSyncFailureAndRootOnlyUnpublished(t *testing.T) {
	f := newWorkspaceFixture(t)
	var growthChecks atomic.Int32
	f.backend.workspaces.config.CheckGrowth = func() error {
		if growthChecks.Add(1) <= 2 {
			return nil
		}
		return artifact.ErrFilesBusy
	}
	ref := f.start(t)
	awaitWorkspacePhase(t, f, ref, "waiting-publication")
	if err := f.backend.Cancel(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	o := awaitWorkspace(t, f, ref)
	subject := workspaceCleanupSubject(t, f, ref, o)
	origin, err := fixturePrepareWorkspaceCleanup(f.backend, ref, subject)
	if err != nil {
		t.Fatal(err)
	}
	if origin.Disposition != WorkspaceCandidateClosedUnpublished || origin.CandidateFile.Inode == 0 {
		t.Fatal("actual unpublished candidate prefix lost")
	}
	intent := workspaceCleanupIntent(origin)
	failure := errors.New("parent sync fault")
	var syncs int
	f.backend.workspaceCleanupOps.sync = func(file *os.File) error {
		syncs++
		if syncs == 1 {
			return failure
		}
		return file.Sync()
	}
	p, err := f.backend.RemoveWorkspaceRoot(context.Background(), intent)
	if !errors.Is(err, failure) || p.Done {
		t.Fatalf("sync failure credited: %+v %v", p, err)
	}
	if _, err := f.root.Lstat(f.binding.ResultID + ".part"); err != nil {
		t.Fatal("external prefix disappeared without independent cleanup")
	}
	finishWorkspaceCleanup(t, f.backend, intent)
	if _, err := f.root.Lstat(f.binding.ResultID + ".part"); err != nil {
		t.Fatal("root-only completion credited external candidate")
	}
}

func TestWorkspaceCleanupRejectsChangedSubjectAndRoot(t *testing.T) {
	f := newWorkspaceFixture(t)
	ref := f.start(t)
	o := awaitWorkspace(t, f, ref)
	subject := workspaceCleanupSubject(t, f, ref, o)
	bad := subject
	bad.TaskDigest[0] ^= 1
	if _, err := fixturePrepareWorkspaceCleanup(f.backend, ref, bad); err == nil {
		t.Fatal("changed accepted task admitted")
	}
	origin, err := fixturePrepareWorkspaceCleanup(f.backend, ref, subject)
	if err != nil {
		t.Fatal(err)
	}
	intent := workspaceCleanupIntent(origin)
	name := "workspace-" + ref.ID
	c := f.backend.workspaces.bindings[ref.ID].cleanup
	parentPath := fmt.Sprintf("/proc/self/fd/%d", c.parentDir.Fd())
	if err := os.Rename(filepath.Join(parentPath, name), filepath.Join(parentPath, "original-workspace")); err != nil {
		t.Fatal(err)
	}
	if err := f.root.Mkdir(name, 0700); err != nil {
		t.Fatal(err)
	}
	if p, err := f.backend.RemoveWorkspaceRoot(context.Background(), intent); err == nil || p.Done {
		t.Fatal("replacement root accepted")
	}
	if err := f.root.Remove(name); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(parentPath, "original-workspace"), filepath.Join(parentPath, name)); err != nil {
		t.Fatal(err)
	}
	if err := f.backend.HandoffWorkspaceCleanup(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceCleanupDepthRefusalStillAllowsHandleHandoff(t *testing.T) {
	f := newWorkspaceFixture(t)
	ref := f.start(t)
	o := awaitWorkspace(t, f, ref)
	w := f.backend.workspaces.bindings[ref.ID]
	name := ""
	for range 65 {
		if name != "" {
			name += "/"
		}
		name += "deep"
		if err := w.root.Mkdir(name, 0700); err != nil {
			t.Fatal(err)
		}
	}
	subject := workspaceCleanupSubject(t, f, ref, o)
	origin, err := fixturePrepareWorkspaceCleanup(f.backend, ref, subject)
	if err != nil {
		t.Fatal(err)
	}
	intent := workspaceCleanupIntent(origin)
	p, err := f.backend.RemoveWorkspaceRoot(context.Background(), intent)
	if err == nil || !strings.Contains(err.Error(), "depth") || p.Done || p.Visited > 100 {
		t.Fatalf("deep tree=%+v %v", p, err)
	}
	if err := f.backend.HandoffWorkspaceCleanup(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	if _, err := f.root.Lstat("workspace-" + ref.ID); err != nil {
		t.Fatal("manual deep-tree refusal removed root")
	}
}

func TestWorkspaceCleanupAttemptedCreateIsNotNeverCreated(t *testing.T) {
	for _, which := range []string{"root", "candidate"} {
		t.Run(which, func(t *testing.T) {
			f := newWorkspaceFixture(t)
			f.expectHeld = true
			if which == "root" {
				if err := f.root.Mkdir("workspace-"+f.binding.AttemptID, 0700); err != nil {
					t.Fatal(err)
				}
			} else {
				file, err := f.root.Create(f.binding.ResultID + ".part")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := file.WriteString("foreign prefix"); err != nil {
					t.Fatal(err)
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
			}
			ref := f.start(t)
			o := awaitWorkspace(t, f, ref)
			w := f.backend.workspaces.bindings[ref.ID]
			if which == "root" {
				if !w.rootAttempted || w.created || !o.Held || o.Writer != nil {
					t.Fatalf("root EEXIST became no-create proof: %+v", o)
				}
				if _, _, err := f.backend.PrepareWorkspaceCompletion(ref); err == nil {
					t.Fatal("unknown root create prepared")
				}
			} else {
				if !w.candidateAttempted || !o.Held || o.Ready || o.Writer != nil || o.Candidate != nil {
					t.Fatalf("candidate EEXIST became completion proof: %+v", o)
				}
				if err := f.backend.CommitWorkspace(ref, WorkspaceRootIntent{}); err == nil {
					t.Fatal("unknown candidate creation was acknowledged")
				}
				if _, _, err := f.backend.PrepareWorkspaceCompletion(ref); err == nil {
					t.Fatal("candidate EEXIST became never-created proof")
				}
				if w.candidateFile.File != (artifact.FileID{}) {
					t.Fatal("foreign candidate inode was adopted")
				}
				file, err := f.root.Open(f.binding.ResultID + ".part")
				if err != nil {
					t.Fatal("foreign prefix changed without cleanup authority")
				}
				data, readErr := io.ReadAll(file)
				closeErr := file.Close()
				if readErr != nil || closeErr != nil || string(data) != "foreign prefix" {
					t.Fatalf("foreign prefix changed: %q %v %v", data, readErr, closeErr)
				}
			}
		})
	}
}

func TestWorkspaceCleanupPositiveNeverCreatedAndNoOutputs(t *testing.T) {
	for _, outputs := range []bool{false, true} {
		t.Run(fmt.Sprint(outputs), func(t *testing.T) {
			f := newWorkspaceFixture(t)
			if !outputs {
				f.binding.Task.Outputs = nil
				f.binding.ResultID = ""
			}
			// Actual exclusive log creation fails before any root or native phase is
			// attempted. The original sequencer still ends its actual synchronous I/O.
			file, err := os.Create(filepath.Join(f.backend.directory, f.binding.AttemptID+".log"))
			if err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			ref := f.start(t)
			o := awaitWorkspace(t, f, ref)
			if !o.Ready || o.Writer == nil || o.Outcome != WorkspaceMaterializationFailure {
				t.Fatalf("phase-free completion=%+v", o)
			}
			state := f.backend.PhysicalState()
			if len(state) != 1 || state[0].CandidateID != f.binding.ResultID {
				t.Fatalf("accepted result identity diverged: %+v", state)
			}
			subject := workspaceCleanupSubject(t, f, ref, o)
			origin, err := fixturePrepareWorkspaceCleanup(f.backend, ref, subject)
			if err != nil {
				t.Fatal(err)
			}
			want := WorkspaceNoOutputs
			if outputs {
				want = WorkspaceCandidateNeverCreated
			}
			if origin.RootState != WorkspaceRootNeverCreated || origin.Root != (artifact.FileID{}) || origin.Disposition != want {
				t.Fatalf("positive no-create origin=%+v", origin)
			}
			intent := workspaceCleanupIntent(origin)
			p, err := f.backend.RemoveWorkspaceRoot(context.Background(), intent)
			if err != nil || !p.Done || p.Visited != 0 {
				t.Fatalf("no-root cleanup=%+v %v", p, err)
			}
		})
	}
}

func TestWorkspaceCleanupActiveOwnerCannotPrepare(t *testing.T) {
	f := newWorkspaceFixture(t)
	f.binding.Task.Setup = workspaceProgram("wait")
	ref := f.start(t)
	if _, _, err := f.backend.PrepareWorkspaceCompletion(ref); err == nil {
		t.Fatal("active native owner prepared")
	}
	if err := f.backend.Cancel(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	awaitWorkspace(t, f, ref)
}

func TestWorkspaceCleanupFailedUnlinkKeepsExactPendingEntry(t *testing.T) {
	f := newWorkspaceFixture(t)
	_, intent := prepareWorkspaceCleanup(t, f)
	fault := errors.New("one actual unlink admission fault")
	failed := false
	f.backend.workspaceCleanupOps.remove = func(root *os.Root, name string) error {
		if !failed {
			failed = true
			return fault
		}
		return root.Remove(name)
	}
	p, err := f.backend.RemoveWorkspaceRoot(context.Background(), intent)
	if !errors.Is(err, fault) || p.Done {
		t.Fatalf("unlink failure=%+v %v", p, err)
	}
	finishWorkspaceCleanup(t, f.backend, intent)
}

func TestWorkspaceCleanupRestartCapacitySharesOriginalBound(t *testing.T) {
	f := newWorkspaceFixture(t)
	_, intent := prepareWorkspaceCleanup(t, f)
	if err := f.backend.HandoffWorkspaceCleanup(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	if err := f.backend.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.ConfigureWorkspace(f.backend.workspaces.config); err != nil {
		t.Fatal(err)
	}
	// Canceled dispatch reserves only the exact trusted intent frame, not root
	// I/O or native proof. All64 fences consume the SAME installation bound.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 64; i++ {
		one := intent
		one.ID = fmt.Sprintf("%032x", i+1)
		one.Origin.Subject.AttemptID = fmt.Sprintf("%032x", i+1000)
		if _, err := b.RemoveWorkspaceRoot(ctx, one); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
	other := intent
	other.Origin.Subject.AttemptID = strings.Repeat("b", 32)
	if _, err := b.RemoveWorkspaceRoot(ctx, other); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatal("cleanup frames exceeded combined64 bound")
	}
	if len(b.jobs) != 0 || len(b.workspaces.bindings) != 0 || len(b.workspaces.cleanupFrames) != 64 {
		t.Fatal("restart frames created native/launch records")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceCleanupPostmarkAbsentRootNeedsOriginalParentSync(t *testing.T) {
	f := newWorkspaceFixture(t)
	_, intent := prepareWorkspaceCleanup(t, f)
	finishWorkspaceCleanup(t, f.backend, intent)
	if err := f.backend.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.ConfigureWorkspace(f.backend.workspaces.config); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("postmark original parent sync failed")
	var calls int
	b.workspaceCleanupOps.sync = func(file *os.File) error {
		calls++
		if calls == 1 {
			return failure
		}
		return file.Sync()
	}
	p, err := b.RemoveWorkspaceRoot(context.Background(), intent)
	if p.Done || !errors.Is(err, failure) {
		t.Fatalf("absence became premature credit: %+v %v", p, err)
	}
	p, err = b.RemoveWorkspaceRoot(context.Background(), intent)
	if err != nil || !p.Done || calls != 2 {
		t.Fatalf("actual parent sync/close=%+v %v", p, err)
	}
	if len(b.jobs) != 0 || len(b.workspaces.bindings) != 0 {
		t.Fatal("absence minted a native owner")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceCleanupRootUnlinkFailureAfterPositiveHandleClose(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprint(after), func(t *testing.T) {
			f := newWorkspaceFixture(t)
			_, intent := prepareWorkspaceCleanup(t, f)
			failure := errors.New("root unlink fault")
			failed := false
			f.backend.workspaceCleanupOps.remove = func(root *os.Root, name string) error {
				if strings.HasPrefix(name, "workspace-") && !failed {
					failed = true
					if after {
						if err := root.Remove(name); err != nil {
							return err
						}
					}
					return failure
				}
				return root.Remove(name)
			}
			p, err := f.backend.RemoveWorkspaceRoot(context.Background(), intent)
			if !errors.Is(err, failure) || p.Done {
				t.Fatalf("root unlink=%+v %v", p, err)
			}
			c := f.backend.workspaces.bindings[intent.Origin.Subject.AttemptID].cleanup
			if !c.rootClosed || c.root != nil || c.directory != nil || c.err != nil {
				t.Fatal("ordinary unlink error misclassified original positive closures")
			}
			finishWorkspaceCleanup(t, f.backend, intent)
		})
	}
}

func TestWorkspaceCleanupSpecialFileRefusesWithoutOpening(t *testing.T) {
	f := newWorkspaceFixture(t)
	ref := f.start(t)
	o := awaitWorkspace(t, f, ref)
	w := f.backend.workspaces.bindings[ref.ID]
	if err := syscall.Mkfifo(fmt.Sprintf("/proc/self/fd/%d/fifo", w.directory.Fd()), 0600); err != nil {
		t.Fatal(err)
	}
	subject := workspaceCleanupSubject(t, f, ref, o)
	origin, err := fixturePrepareWorkspaceCleanup(f.backend, ref, subject)
	if err != nil {
		t.Fatal(err)
	}
	intent := workspaceCleanupIntent(origin)
	p, err := f.backend.RemoveWorkspaceRoot(context.Background(), intent)
	if err == nil || !strings.Contains(err.Error(), "special") || p.Done {
		t.Fatalf("special file=%+v %v", p, err)
	}
	if err := f.backend.HandoffWorkspaceCleanup(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceCleanupOrdinaryProgram(t *testing.T) {
	if os.Getenv("SKYGO_WORKSPACE_CLEANUP_ORDINARY") == "1" {
		os.Exit(9)
	}
}

func TestWorkspaceCleanupGatesGenuineRetryForget(t *testing.T) {
	f := newWorkspaceFixture(t)
	b := retainedBackend(t)
	config := f.backend.workspaces.config
	config.InstallationID = retentionOwner
	if err := b.ConfigureWorkspace(config); err != nil {
		t.Fatal(err)
	}
	f.backend = b
	f.binding.Binding = b.workspaces.binding
	f.binding.Task.Argv = workspaceProgram("run-fail")
	f.binding.Task.Retry = &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{9}}
	outref := backend.OutputReference{Owner: retentionOwner, JobID: f.binding.JobID, AttemptID: f.binding.AttemptID, Binding: f.binding.Binding, Generation: 1}
	if err := b.BindOutput(outref); err != nil {
		t.Fatal(err)
	}
	ref := f.start(t)
	o := awaitWorkspace(t, f, ref)
	if o.Status.Termination != backend.StoppedWithExitCode {
		t.Fatalf("actual genuine Retry exit proof missing: %+v", o)
	}
	if err := b.Retention().RemoveExecution(context.Background(), ref); err == nil {
		t.Fatal("live workspace erased through Retry-only forget")
	}
	subject := workspaceCleanupSubject(t, f, ref, o)
	origin, err := fixturePrepareWorkspaceCleanup(b, ref, subject)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Retention().RemoveExecution(context.Background(), ref); err == nil {
		t.Fatal("prepared but unhanded workspace erased")
	}
	intent := workspaceCleanupIntent(origin)
	if err := b.HandoffWorkspaceCleanup(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	if b.jobs[ref.ID].workspace != nil {
		t.Fatal("positive handoff left generic workspace retirement fenced forever")
	}
	if chunk, err := b.Logs(context.Background(), ref, 0, backend.MaxChunkBytes); err != nil || !strings.Contains(string(chunk.Data), "task output") {
		t.Fatalf("independent closed log lost: %q %v", chunk.Data, err)
	}
	if err := b.Retention().RemoveExecution(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	if b.jobs[ref.ID] != nil {
		t.Fatal("genuine stopped Retry record not forgotten")
	}
	if _, err := f.root.Lstat("workspace-" + ref.ID); err != nil {
		t.Fatal("generic forget credited namespace handoff")
	}
	// Ordinary retained Retry still uses its actual child exit and unchanged
	// digest/Instance/released-payload checks, without workspace preparation.
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ordinary := backend.Task{Argv: []string{exe, "-test.run=^TestWorkspaceCleanupOrdinaryProgram$"}, Env: map[string]string{"SKYGO_WORKSPACE_CLEANUP_ORDINARY": "1"}, Directory: t.TempDir(), Retry: &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{9}}}
	id := strings.Repeat("c", 32)
	startRetained(t, b, id, ordinary)
	status := completed(t, b, id)
	if status.Termination != backend.StoppedWithExitCode || status.ExitCode == nil || *status.ExitCode != 9 {
		t.Fatalf("ordinary Retry=%+v", status)
	}
	if err := b.Retention().RemoveExecution(context.Background(), backend.Reference{ID: id, Task: ordinary, Binding: b.retention.binding, Instance: status.Instance}); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceCleanupPartialReopenThenAbsenceJoinsOriginalHandles(t *testing.T) {
	for _, tt := range []struct {
		name      string
		directory bool
		fault     string
	}{
		{"root-only", false, ""},
		{"root-and-directory", true, ""},
		{"root-only-close-error", false, "root"},
		{"directory-close-error", true, "file"},
		{"root-close-error", true, "root"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newWorkspaceFixture(t)
			_, intent := prepareWorkspaceCleanup(t, f)
			if err := f.backend.HandoffWorkspaceCleanup(context.Background(), intent); err != nil {
				t.Fatal(err)
			}
			if err := f.backend.Close(); err != nil {
				t.Fatal(err)
			}
			b, err := New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := b.ConfigureWorkspace(f.backend.workspaces.config); err != nil {
				t.Fatal(err)
			}
			_, c, err := b.cleanupIntentAdmission(intent)
			if err != nil {
				t.Fatal(err)
			}
			// Stage the partial-reopen boundary with REAL original opened handles,
			// not an injected opener or claimed production race reproduction.
			// A real later identity error leaves initialized false; subsequent exact
			// committed-path absence must not discard any of these original owners.
			func() {
				c.mu.Lock()
				defer c.mu.Unlock()
				defer b.cleanupEnd(nil, c)
				c.parent, err = f.root.OpenRoot(".")
				if err != nil {
					t.Fatal(err)
				}
				c.parentDir, err = openWorkspaceDirectory(c.parent)
				if err != nil {
					t.Fatal(err)
				}
				name := "workspace-" + intent.Origin.Subject.AttemptID
				c.root, err = c.parent.OpenRoot(name)
				if err != nil {
					t.Fatal(err)
				}
				if tt.directory {
					c.directory, err = openWorkspaceDirectory(c.root)
					if err != nil {
						t.Fatal(err)
					}
				}
				parentPath := fmt.Sprintf("/proc/self/fd/%d", c.parentDir.Fd())
				if err := os.Rename(filepath.Join(parentPath, name), filepath.Join(parentPath, "retained-partial-root")); err != nil {
					t.Fatal(err)
				}
				if err := c.parent.Mkdir(name, 0700); err != nil {
					t.Fatal(err)
				}
				if err := c.checkRoot(); !errors.Is(err, backend.ErrIdentityMismatch) {
					t.Fatalf("actual later ordinary identity error=%v", err)
				}
				if c.initialized || c.err != nil {
					t.Fatal("ordinary failed reopen was mislabeled closed or initialized")
				}
				if _, err := c.root.Lstat("."); err != nil {
					t.Fatal("actual original root not retained", err)
				}
				if c.directory != nil {
					if _, err := c.directory.Stat(); err != nil {
						t.Fatal("actual original directory not retained", err)
					}
				}
				if err := c.parent.Remove(name); err != nil {
					t.Fatal(err)
				}
			}()
			originalRoot, originalFile := c.root, c.directory
			diagnostic := errors.New("diagnostic after actual partial-reopen original Close")
			var closes atomic.Int32
			if tt.fault == "file" {
				b.workspaceCleanupOps.closeFile = func(file *os.File) error {
					if file != originalFile {
						return file.Close()
					}
					closes.Add(1)
					if err := file.Close(); err != nil {
						return err
					}
					return diagnostic
				}
			}
			if tt.fault == "root" {
				b.workspaceCleanupOps.closeRoot = func(root *os.Root) error {
					if root != originalRoot {
						return root.Close()
					}
					closes.Add(1)
					if err := root.Close(); err != nil {
						return err
					}
					return diagnostic
				}
			}
			p, err := b.RemoveWorkspaceRoot(context.Background(), intent)
			if tt.fault == "" {
				if err != nil || !p.Done || !c.done || c.root != nil || c.directory != nil || c.parent != nil || c.parentDir != nil {
					t.Fatalf("original handles not joined before Done: %+v %v", p, err)
				}
				if _, err := originalRoot.Lstat("."); !errors.Is(err, os.ErrClosed) {
					t.Fatalf("original Root remains open: %v", err)
				}
				if originalFile != nil {
					if _, err := originalFile.Stat(); !errors.Is(err, os.ErrClosed) {
						t.Fatalf("original File remains open: %v", err)
					}
				}
				if b.workspaces.cleanupFrames[intent.Origin.Subject.AttemptID] != nil {
					t.Fatal("positive complete frame not retired")
				}
				if err := b.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				if p.Done || c.done || !errors.Is(err, artifact.ErrFileCloseUncertain) || !errors.Is(err, diagnostic) || c.err != err {
					t.Fatalf("unknown original Close credited: %+v %v", p, err)
				}
				if tt.fault == "root" && c.root != originalRoot || tt.fault == "file" && c.directory != originalFile {
					t.Fatal("original uncertain pointer lost")
				}
				if b.workspaces.cleanupFrames[intent.Origin.Subject.AttemptID] != c || !c.owned || !c.closeHeld {
					t.Fatal("held original frame retired")
				}
				if _, again := b.RemoveWorkspaceRoot(context.Background(), intent); again != err {
					t.Fatal("same original error replaced")
				}
				if again := b.HandoffWorkspaceCleanup(context.Background(), intent); again != err {
					t.Fatal("handoff replaced original uncertainty")
				}
				if closes.Load() != 1 {
					t.Fatal("original raw Close retried")
				}
				if err := b.Close(); err == nil {
					t.Fatal("uncertain original owner closed successfully")
				}
				if closes.Load() != 1 {
					t.Fatal("backend Close retried original")
				}
				// Hook diagnostic followed a real positive Close; the test knows
				// which exact remaining fixture handles it may close. No production
				// reconciliation, Store Finish or namespace credit is asserted.
				if c.directory != nil && tt.fault != "file" {
					if err := c.directory.Close(); err != nil {
						t.Fatal(err)
					}
				}
				if c.root != nil && tt.fault != "root" {
					if err := c.root.Close(); err != nil {
						t.Fatal(err)
					}
				}
				if c.parentDir != nil {
					if err := c.parentDir.Close(); err != nil {
						t.Fatal(err)
					}
				}
				if c.parent != nil {
					if err := c.parent.Close(); err != nil {
						t.Fatal(err)
					}
				}
			}
			// The deliberately moved original fixture tree still exists. This test
			// qualifies descriptor settlement only, NOT adversarial namespace cleanup.
			if _, err := f.root.Lstat("retained-partial-root"); err != nil {
				t.Fatal(err)
			}
			if len(b.jobs) != 0 || len(b.workspaces.bindings) != 0 {
				t.Fatal("cleanup proof manufactured native/launch records")
			}
		})
	}
}
