//go:build linux

package local

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
)

func awaitWorkspacePhase(t *testing.T, f *workspaceFixture, ref backend.Reference, phase string) WorkspaceObservation {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		o, err := f.backend.WorkspaceObservation(ref)
		if err != nil {
			t.Fatal(err)
		}
		if o.Phase == phase {
			return o
		}
		if o.Ready || o.Held {
			t.Fatalf("terminal instead of %s: %+v", phase, o)
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("did not reach %s", phase)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWorkspacePublicationResumesSameClosedCandidate(t *testing.T) {
	f := newWorkspaceFixture(t)
	var allow atomic.Bool
	var guardCalls atomic.Int32
	f.backend.workspaces.config.CheckGrowth = func() error {
		n := guardCalls.Add(1)
		if n <= 2 {
			return nil
		}
		if !allow.Load() {
			return artifact.ErrFilesBusy
		}
		return nil
	}
	ref := f.start(t)
	o := awaitWorkspacePhase(t, f, ref, "waiting-publication")
	if o.Ready || o.Held || o.Writer != nil || o.Candidate != nil || len(f.slots) != 0 {
		t.Fatalf("late refusal=%+v slots=%d", o, len(f.slots))
	}
	// Waiting publication is the sequencer's positive handoff; no hint has been
	// sent, so its original candidate cannot change while this test observes it.
	f.backend.mu.Lock()
	w := f.backend.jobs[ref.ID].workspace
	c := w.candidate
	f.backend.mu.Unlock()
	if c == nil {
		t.Fatal("same candidate owner lost")
	}
	before := c.Description()
	if _, err := c.Write([]byte("extra")); !errors.Is(err, artifact.ErrFilesClosed) {
		t.Fatalf("writer was not positively closed: %v", err)
	}
	if _, err := f.root.Lstat(f.binding.ResultID + ".tar"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("refused growth published an object")
	}
	allow.Store(true)
	if err := f.backend.ResumeWorkspacePublication(ref); err != nil {
		t.Fatal(err)
	}
	o = awaitWorkspace(t, f, ref)
	if !o.Ready || o.Held || o.Candidate == nil || o.Candidate.File != before.File || o.Candidate.Manifest.Reference.ID != before.ID || o.Candidate.Manifest.Reference.Size != before.Size || o.Candidate.Manifest.Reference.SHA256 != before.SHA256 {
		t.Fatalf("candidate changed: before=%+v after=%+v", before, o)
	}
	if guardCalls.Load() != 4 || f.collectionReleases.Load() != 2 {
		t.Fatalf("unexpected extra attempt calls=%d releases=%d", guardCalls.Load(), f.collectionReleases.Load())
	}
	if err := f.backend.ResumeWorkspacePublication(ref); err != nil {
		t.Fatal(err)
	}
	if guardCalls.Load() != 4 {
		t.Fatal("terminal hint repeated publication")
	}
	changed := ref
	changed.Instance = strings.Repeat("0", 32)
	if err := f.backend.ResumeWorkspacePublication(changed); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatal("stale Instance accepted")
	}
}

func TestWorkspaceCancelWaitsForCandidateSettlementPermit(t *testing.T) {
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
	for i := 0; i < 4; i++ {
		f.slots <- struct{}{}
	}
	defer func() {
		for len(f.slots) > 0 {
			<-f.slots
		}
	}()
	if err := f.backend.Cancel(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	o := awaitWorkspacePhase(t, f, ref, "settling-cancel")
	if o.Ready || o.Held || o.Writer != nil {
		t.Fatalf("cancel released an unsettled owner: %+v", o)
	}
	if err := f.files.Close(); !errors.Is(err, artifact.ErrFilesBusy) {
		t.Fatalf("registered candidate no longer owned: %v", err)
	}
	<-f.slots
	if err := f.backend.ResumeWorkspacePublication(ref); err != nil {
		t.Fatal(err)
	}
	o = awaitWorkspace(t, f, ref)
	if !o.Ready || o.Held || o.Outcome != WorkspaceCanceled || o.Writer == nil || o.Candidate != nil {
		t.Fatalf("settled cancel=%+v", o)
	}
	if _, err := f.root.Lstat(f.binding.ResultID + ".part"); err != nil {
		t.Fatal("cancel deleted charged prefix without committed cleanup")
	}
}

func TestWorkspacePhysicalInvalidationPrecedesCreation(t *testing.T) {
	f := newWorkspaceFixture(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	f.backend.workspaces.config.PhysicalChanged = func() {
		// Callback can take the copier's Backend lock: no Backend/Files/cleanup
		// owner lock may be held by its caller. The first transition is root IO.
		state := f.backend.PhysicalState()
		if len(state) == 1 && state[0].Transitioning && calls.Add(1) == 1 {
			if len(state) != 1 || !state[0].Transitioning || state[0].RootAttempted {
				t.Error("creation did not publish its transition first")
			}
			close(entered)
			<-resume
		}
	}
	defer func() {
		select {
		case <-resume:
		default:
			close(resume)
		}
	}()
	ref := f.start(t)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("pre-invalidation missing")
	}
	if _, err := f.root.Lstat("workspace-" + ref.ID); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("root created before invalidation returned")
	}
	state := f.backend.PhysicalState()
	state[0].RootAttempted = true
	if f.backend.PhysicalState()[0].RootAttempted {
		t.Fatal("copier returned mutable mirror storage")
	}
	close(resume)
	o := awaitWorkspace(t, f, ref)
	if !o.Ready || o.Held {
		t.Fatalf("completion=%+v", o)
	}
	state = f.backend.PhysicalState()
	if len(state) != 1 || state[0].Transitioning || state[0].Root.Inode == 0 || state[0].Candidate.Inode == 0 || state[0].Instance != ref.Instance || state[0].CandidateID != f.binding.ResultID || state[0].WriterActive {
		t.Fatalf("original physical identity lost: %+v", state)
	}
}

func TestWorkspaceWakeHintsAreCoalescedWithoutLostHint(t *testing.T) {
	b := &Backend{}
	w := &workspaceExecution{wake: make(chan struct{})}
	first := b.publicationAttempt(w)
	b.mu.Lock()
	wakeWorkspace(w)
	wakeWorkspace(w)
	current := w.wake
	b.mu.Unlock()
	select {
	case <-first:
	default:
		t.Fatal("captured waiter did not wake")
	}
	select {
	case <-current:
		t.Fatal("coalesced hint generated another event")
	default:
	}
	second := b.publicationAttempt(w)
	if second != current {
		t.Fatal("attempt replaced the pending channel")
	}
	b.mu.Lock()
	wakeWorkspace(w)
	b.mu.Unlock()
	select {
	case <-second:
	default:
		t.Fatal("hint after attempt snapshot lost")
	}
}

func TestWorkspaceTaskTimeoutAdmissionEndsBeforePublicationWait(t *testing.T) {
	f := newWorkspaceFixture(t)
	var allow atomic.Bool
	var growthChecks atomic.Int32
	f.backend.workspaces.config.CheckGrowth = func() error {
		if growthChecks.Add(1) <= 2 {
			return nil
		}
		if !allow.Load() {
			return artifact.ErrFilesBusy
		}
		return nil
	}
	ref := f.start(t)
	awaitWorkspacePhase(t, f, ref, "waiting-publication")
	f.backend.mu.Lock()
	w := f.backend.jobs[ref.ID].workspace
	open := w.taskDeadlineOpen
	f.backend.mu.Unlock()
	if open {
		t.Fatal("task deadline admission remained open during capacity waiting")
	}
	// Exercise the exact timer callback body after its real task phase ended.
	// This proves the admission fence, not a particular runtime timer schedule.
	f.backend.workspaceTimeout(w)
	o, err := f.backend.WorkspaceObservation(ref)
	if err != nil || o.Ready || o.Held || o.Writer != nil {
		t.Fatalf("late timeout changed waiting: %+v %v", o, err)
	}
	allow.Store(true)
	if err := f.backend.ResumeWorkspacePublication(ref); err != nil {
		t.Fatal(err)
	}
	o = awaitWorkspace(t, f, ref)
	if !o.Ready || o.Held || o.Status.State != backend.Succeeded || o.Outcome != WorkspaceTaskExit {
		t.Fatalf("late timer relabeled task: %+v", o)
	}
}

func TestWorkspaceUnknownCandidateCreationStaysHeld(t *testing.T) {
	f := newWorkspaceFixture(t)
	f.expectHeld = true
	foreign, err := f.root.OpenFile(f.binding.ResultID+".part", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := foreign.WriteString("foreign prefix"); err != nil {
		t.Fatal(err)
	}
	if err := foreign.Close(); err != nil {
		t.Fatal(err)
	}
	ref := f.start(t)
	o := awaitWorkspace(t, f, ref)
	if !o.Held || o.Ready || o.Writer != nil || o.Candidate != nil || len(f.slots) != 0 {
		t.Fatalf("unknown creation acquired proof: %+v slots=%d", o, len(f.slots))
	}
	state := f.backend.PhysicalState()
	if len(state) != 1 || !state[0].CandidateAttempted || !state[0].Uncertain || state[0].Candidate != (artifact.FileID{}) || state[0].CandidateID != f.binding.ResultID {
		t.Fatalf("unknown prefix was adopted or called absent: %+v", state)
	}
	reader, err := f.root.Open(f.binding.ResultID + ".part")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	closeErr := reader.Close()
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil || string(data) != "foreign prefix" {
		t.Fatalf("foreign data changed: %q %v", data, err)
	}
}
