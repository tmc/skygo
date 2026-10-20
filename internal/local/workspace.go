package local

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
)

// WorkspaceConfig installs only the concrete local artifact sequencer. Roots and
// Files are borrowed until all actual owners finish, including held cleanup.
// TryCollectionIO shares the service's four real artifact permits; it never queues.
// CheckGrowth reads only the dedicated capacity cache; PhysicalChanged only
// invalidates that cache/active sample outside all owner locks. Both are required.
type WorkspaceConfig struct {
	Files                  *artifact.Files
	Roots                  *os.Root
	InstallationID, RootID string
	TryCollectionIO        func() (release func(), ok bool)
	CheckGrowth            func() error
	PhysicalChanged        func()
	ServerLifetime         ServerLifetime
}

// WorkspaceBinding is the trusted committed reservation, never an inbound grant.
// Task must match the exact accepted Task. InputFile comes from the durable Store
// snapshot; disk presence never supplies it. ResultID is empty iff Outputs is empty.
type WorkspaceBinding struct {
	Owner, JobID, AttemptID, Binding, RootID string
	Task                                     backend.Task
	Input                                    artifact.Ref
	InputFile                                artifact.FileID
	ResultID                                 string
}

// WorkspaceOutcome classifies the owned sequence, independently of writer proof.
// A failed observation lookup returns zero, which is not a completion outcome.
type WorkspaceOutcome uint8

const (
	WorkspaceTaskExit WorkspaceOutcome = iota + 1
	WorkspaceSetupFailure
	WorkspaceMaterializationFailure
	WorkspaceCollectionFailure
	WorkspaceCanceled
	WorkspaceTimeout
	WorkspaceUncertain
	// WorkspaceExecutionFailure is appended and never supplies retry authority.
	WorkspaceExecutionFailure
)

// WorkspaceWriterClosure identifies a prior owned-writer handoff. It is distinct
// from native retry authority and never proves arbitrary escaped writers absent.
type WorkspaceWriterClosure struct {
	InstallationID, RootID, JobID string
	Execution                     backend.Reference
}

// WorkspaceObservation stays private to trusted completion assembly. Ready is a
// stable candidate, not a visible artifact grant or terminal Store transaction.
// Held retains reservations; only a real Writer receipt permits completion.
type WorkspaceObservation struct {
	Phase       string
	Ready, Held bool
	Outcome     WorkspaceOutcome
	Status      backend.Status
	Writer      *WorkspaceWriterClosure
	Candidate   *artifact.SealReceipt
}

type workspaceState struct {
	config        WorkspaceConfig
	root          artifact.FileID
	binding       string
	bindings      map[string]*workspaceExecution
	cleanupFrames map[string]*workspaceCleanup
}

type workspaceExecution struct {
	binding                                      WorkspaceBinding
	taskHash                                     [32]byte
	parent                                       *execution
	phase                                        *execution
	failedPhase                                  *execution
	observation                                  WorkspaceObservation
	stopState                                    backend.State
	stopReason                                   string
	intent, committed, startAttempted            bool
	wake                                         chan struct{}
	wakePending                                  bool
	taskDeadlineOpen                             bool
	logCloseAttempted                            bool
	logCloseErr                                  error
	materializationEntered, initialOwnerReleased bool
	noCallSettled                                bool // original unused callback returned before any sequencer entry
	physical                                     WorkspacePhysical
	// Single sequencer owns the following fields until parent.done.
	release                           func()
	collectionRelease                 func()
	root                              *os.Root
	directory                         *os.File
	rootInfo                          os.FileInfo
	rootName                          string
	created                           bool
	rootAttempted, candidateAttempted bool
	cleanup                           *workspaceCleanup
	cleanupBusy, cleanupOwned         bool
	candidate                         *artifact.Candidate
	candidateFile                     artifact.CandidateFile
	collectionErr                     error
	rootErr                           error
}

// workspacePhase is published only before phase.done. All real process access
// ends before closure is copied; this receipt does not survive a server restart.
type workspacePhase struct {
	writerClosed, taskRetry     bool
	startupWriter, startupInput *os.File
	startupErr                  error
}

// SupportsWorkspace reports whether the concrete workspace service is configured,
// open, and has this platform's existing process-ownership prerequisites. It does
// no I/O and grants no execution, writer closure, or native qualification. A nil
// or unconfigured Backend reports false.
func (b *Backend) SupportsWorkspace() bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.workspaces != nil && !b.closed && workspaceWriterPlatform() && b.retryAvailable
}

// ConfigureWorkspace verifies the physical root and fixed installation marker.
// Call once before starts. It does not initialize or adopt an existing root.
func (b *Backend) ConfigureWorkspace(config WorkspaceConfig) error {
	if config.Files == nil || config.Roots == nil || config.TryCollectionIO == nil || config.CheckGrowth == nil || config.PhysicalChanged == nil || !validWorkspaceProducer(config.ServerLifetime) || !validID.MatchString(config.InstallationID) || !validID.MatchString(config.RootID) {
		return errors.New("invalid local workspace configuration")
	}
	facts, err := config.Files.RootFacts()
	if err != nil {
		return err
	}
	info, err := config.Roots.Lstat(".")
	if err != nil {
		return err
	}
	id, err := workspaceFileID(info)
	if err != nil {
		return err
	}
	if facts.ID != config.RootID || facts.File != id {
		return backend.ErrIdentityMismatch
	}
	expectedBinding, err := backend.LocalBinding(config.InstallationID)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.workspaces != nil || len(b.jobs) != 0 {
		return errors.New("workspace configuration is not stopped")
	}
	if b.retention != nil && b.retention.binding != expectedBinding {
		return backend.ErrIdentityMismatch
	}
	config.InstallationID = strings.Clone(config.InstallationID)
	config.RootID = strings.Clone(config.RootID)
	config.ServerLifetime.Host = strings.Clone(config.ServerLifetime.Host)
	config.ServerLifetime.Boot = strings.Clone(config.ServerLifetime.Boot)
	b.workspaces = &workspaceState{config: config, root: id, binding: expectedBinding, bindings: make(map[string]*workspaceExecution), cleanupFrames: make(map[string]*workspaceCleanup)}
	return nil
}

// BindWorkspace transfers release only on nil return, before the single Start.
// Before any I/O the unused owner may yield to sampling and reacquire common4.
// Once materialization enters, only actual positive settlement releases it.
func (b *Backend) BindWorkspace(binding WorkspaceBinding, release func()) error {
	if release == nil || !validID.MatchString(binding.JobID) || !validID.MatchString(binding.AttemptID) || binding.Owner == "" || len(binding.Owner) > 64 || binding.Binding == "" || binding.Task.Workdir == nil || *binding.Task.Workdir != binding.Input || binding.InputFile.Device == 0 || binding.InputFile.Inode == 0 {
		return backend.ErrIdentityMismatch
	}
	if err := b.Check(binding.Task); err != nil {
		return err
	}
	if err := binding.Input.Validate(); err != nil {
		return err
	}
	if len(binding.Task.Outputs) == 0 && binding.ResultID != "" || len(binding.Task.Outputs) != 0 && !validID.MatchString(binding.ResultID) {
		return backend.ErrIdentityMismatch
	}
	data, err := backend.TaskBytes(binding.Task)
	if err != nil {
		return err
	}
	if len(data) > 256<<10 {
		return errors.New("workspace task exceeds 256 KiB")
	}
	binding.Task = backend.CloneTask(binding.Task)
	binding.Owner = strings.Clone(binding.Owner)
	binding.JobID = strings.Clone(binding.JobID)
	binding.AttemptID = strings.Clone(binding.AttemptID)
	binding.Binding = strings.Clone(binding.Binding)
	binding.RootID = strings.Clone(binding.RootID)
	binding.ResultID = strings.Clone(binding.ResultID)
	binding.Input.ID = strings.Clone(binding.Input.ID)
	binding.Input.SHA256 = strings.Clone(binding.Input.SHA256)
	b.mu.Lock()
	err = b.bindWorkspaceLocked(binding, data, release)
	b.mu.Unlock()
	if err != nil {
		return err
	}
	b.workspaces.config.PhysicalChanged()
	return nil
}

func (b *Backend) bindWorkspaceLocked(binding WorkspaceBinding, data []byte, release func()) error {
	if b.closed || b.workspaces == nil {
		return errors.New("local workspace service is unavailable")
	}
	if binding.RootID != b.workspaces.config.RootID || binding.Binding != b.workspaces.binding {
		return backend.ErrIdentityMismatch
	}
	if _, ok := b.workspaces.bindings[binding.AttemptID]; ok {
		return backend.ErrIdentityMismatch
	}
	if b.jobs[binding.AttemptID] != nil || b.workspaces.cleanupFrames[binding.AttemptID] != nil {
		return backend.ErrIdentityMismatch
	}
	if len(b.workspaces.bindings)+len(b.workspaces.cleanupFrames) >= 64 {
		return errors.New("local workspace reservation limit reached")
	}
	b.workspaces.bindings[binding.AttemptID] = &workspaceExecution{binding: binding, taskHash: sha256.Sum256(data), release: onceWorkspaceRelease(release), wake: make(chan struct{}), physical: WorkspacePhysical{AttemptID: binding.AttemptID, Binding: binding.Binding, RootID: binding.RootID, CandidateID: binding.ResultID, Parent: b.workspaces.root}}
	return nil
}

func workspaceMatch(w *workspaceExecution, ref backend.Reference) error {
	if ref.ID != w.binding.AttemptID || ref.Binding != w.binding.Binding || ref.Instance != w.parent.status.Instance {
		return backend.ErrIdentityMismatch
	}
	data, err := backend.TaskBytes(ref.Task)
	if err != nil {
		return err
	}
	if sha256.Sum256(data) != w.taskHash {
		return backend.ErrIdentityMismatch
	}
	return nil
}

// WorkspaceObservation returns bounded copies of the same owned candidate until
// trusted atomic completion is acknowledged. Unknown/restarted rows are not adopted.
func (b *Backend) WorkspaceObservation(ref backend.Reference) (WorkspaceObservation, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.jobs[ref.ID]
	if e == nil || e.workspace == nil {
		return WorkspaceObservation{}, errors.New("workspace is not owned by this server")
	}
	w := e.workspace
	if err := workspaceMatch(w, ref); err != nil {
		return WorkspaceObservation{}, err
	}
	o := w.observation
	o.Status.Instance = e.status.Instance
	if o.Status.ExitCode != nil {
		code := *o.Status.ExitCode
		o.Status.ExitCode = &code
	}
	if o.Writer != nil {
		writer := *o.Writer
		writer.Execution.Task = backend.CloneTask(ref.Task)
		o.Writer = &writer
	}
	if o.Candidate != nil {
		candidate := *o.Candidate
		o.Candidate = &candidate
	}
	return o, nil
}

func (b *Backend) startWorkspace(ctx context.Context, id string, t backend.Task) error {
	data, err := backend.TaskBytes(t)
	if err != nil {
		return err
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return err
	}
	b.mu.Lock()
	if b.closed || b.workspaces == nil {
		b.mu.Unlock()
		return errors.New("local workspace service is unavailable")
	}
	w := b.workspaces.bindings[id]
	if w == nil || w.parent != nil || w.startAttempted || w.taskHash != sha256.Sum256(data) {
		b.mu.Unlock()
		return backend.ErrIdentityMismatch
	}
	w.startAttempted = true
	if err := ctx.Err(); err != nil {
		// Only the original unused owner can settle this no-call frontier. A
		// detached nil callback or a failed Start is not a closure receipt.
		state := b.workspaces
		release := w.release
		if release == nil || !workspaceNoCallFrontier(w) || b.jobs[id] != nil {
			b.mu.Unlock()
			return err
		}
		w.release = nil
		b.mu.Unlock()
		release()
		b.mu.Lock()
		if !b.closed && b.workspaces == state && state.bindings[id] == w &&
			b.jobs[id] == nil && w.release == nil && w.startAttempted && workspaceNoCallFrontier(w) {
			w.initialOwnerReleased = true
			w.noCallSettled = true
		}
		b.mu.Unlock()
		return err
	}
	if _, ok := b.jobs[id]; ok {
		b.mu.Unlock()
		return errors.New("job already started")
	}
	e := &execution{status: backend.Status{State: backend.Running, Instance: hex.EncodeToString(token[:])}, done: make(chan struct{}), workspace: w}
	w.parent = e
	w.observation = WorkspaceObservation{Phase: "materializing", Status: e.status}
	w.physical.Instance = e.status.Instance
	b.jobs[id] = e
	b.mu.Unlock()
	b.workspaces.config.PhysicalChanged()
	go b.runWorkspace(w)
	return nil
}

// workspaceNoCallFrontier is read under b.mu only while no parent exists.
// It grants no writer, native, root, candidate, or durable completion authority.
func workspaceNoCallFrontier(w *workspaceExecution) bool {
	return w.parent == nil && w.phase == nil && w.failedPhase == nil &&
		!w.materializationEntered && !w.rootAttempted && !w.candidateAttempted &&
		!w.created && w.root == nil && w.directory == nil && w.rootInfo == nil &&
		w.rootName == "" && w.candidate == nil && w.collectionRelease == nil &&
		w.cleanup == nil && !w.cleanupBusy && !w.cleanupOwned &&
		!w.taskDeadlineOpen && !w.logCloseAttempted && w.logCloseErr == nil &&
		w.rootErr == nil && w.collectionErr == nil && !w.committed && !w.intent &&
		!w.observation.Ready && !w.observation.Held && w.observation.Writer == nil &&
		w.observation.Candidate == nil && w.physical.Instance == ""
}

// stopWorkspace holds b.mu. Pending intent wins before the next native phase.
// A phase starting outside the lock receives that intent on ownership install.
func (b *Backend) stopWorkspace(w *workspaceExecution, state backend.State, reason string) error {
	select {
	case <-w.parent.done:
		return nil
	default:
	}
	if !w.intent {
		w.intent = true
		w.stopState = state
		w.stopReason = reason
	}
	wakeWorkspace(w)
	if w.phase != nil && w.phase.cmd != nil && w.phase.cmd.Process != nil {
		return b.stopDefault(w.phase, state, reason)
	}
	return nil
}
func (b *Backend) workspaceStopped(w *workspaceExecution) (bool, backend.State, string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return w.intent, w.stopState, w.stopReason
}
func (b *Backend) workspacePhaseName(w *workspaceExecution, name string) {
	b.mu.Lock()
	w.observation.Phase = name
	b.mu.Unlock()
}

func (b *Backend) runWorkspace(w *workspaceExecution) {
	// Timer callback only enters the same intent/signal fence. Stop does not join
	// it under b.mu; after done no callback may dereference process owners.
	var timer *time.Timer
	b.mu.Lock()
	w.taskDeadlineOpen = true
	b.mu.Unlock()
	if w.binding.Task.TimeoutSeconds > 0 {
		timer = time.AfterFunc(time.Duration(w.binding.Task.TimeoutSeconds)*time.Second, func() { b.workspaceTimeout(w) })
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	if !b.admitWorkspaceMaterialization(w) {
		_, state, reason := b.workspaceStopped(w)
		closed := w.initialOwnerReleased && !w.materializationEntered && w.release == nil && w.collectionRelease == nil && !w.rootAttempted && !w.candidateAttempted && w.root == nil && w.directory == nil && w.parent.output == nil && w.phase == nil && w.failedPhase == nil
		b.finishWorkspace(w, workspaceStopOutcome(state), backend.Status{State: state, Reason: reason}, closed, nil)
		return
	}
	release := w.release
	w.materializationEntered = true
	err := b.materializeWorkspace(w)
	w.release = settleWorkspacePermit(release, err)
	release = nil // synchronous I/O has actually ended, including descriptor close
	if err != nil {
		b.finishWorkspace(w, WorkspaceMaterializationFailure, backend.Status{State: backend.Failed, Reason: "materialize workspace: " + err.Error()}, (!w.rootAttempted || w.created && w.root != nil) && w.rootErr == nil, nil)
		return
	}
	if stop, state, reason := b.workspaceStopped(w); stop {
		b.finishWorkspace(w, workspaceStopOutcome(state), backend.Status{State: state, Reason: reason}, true, nil)
		return
	}
	closed := true
	if len(w.binding.Task.Setup) != 0 {
		b.workspacePhaseName(w, "setup")
		status, proof, err := b.runWorkspacePhase(w, w.binding.Task.Setup, false)
		closed = proof
		if err != nil || !proof {
			b.finishWorkspace(w, WorkspaceUncertain, backend.Status{State: backend.Unknown, Reason: "setup ownership remains uncertain"}, false, nil)
			return
		}
		if stop, state, reason := b.workspaceStopped(w); stop {
			b.finishWorkspace(w, workspaceStopOutcome(state), backend.Status{State: state, Reason: reason}, closed, nil)
			return
		}
		if status.State != backend.Succeeded {
			status.Termination = backend.UnknownTermination
			if status.ExitCode != nil && *status.ExitCode < 0 {
				status.ExitCode = nil
			}
			b.finishWorkspace(w, WorkspaceSetupFailure, status, closed, nil)
			return
		}
	}
	b.workspacePhaseName(w, "running")
	status, proof, err := b.runWorkspacePhase(w, w.binding.Task.Argv, true)
	closed = closed && proof
	b.mu.Lock()
	w.taskDeadlineOpen = false
	b.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
	if err != nil || !closed {
		b.finishWorkspace(w, WorkspaceUncertain, backend.Status{State: backend.Unknown, Reason: "task ownership remains uncertain"}, false, nil)
		return
	}
	if stop, state, reason := b.workspaceStopped(w); stop {
		b.finishWorkspace(w, workspaceStopOutcome(state), backend.Status{State: state, Reason: reason}, closed, nil)
		return
	}
	if status.ExitCode == nil || *status.ExitCode < 0 {
		status.ExitCode = nil
		status.State = backend.Failed
		status.Termination = backend.UnknownTermination
		b.finishWorkspace(w, WorkspaceExecutionFailure, status, closed, nil)
		return
	}
	if status.State != backend.Succeeded || len(w.binding.Task.Outputs) == 0 {
		b.finishWorkspace(w, WorkspaceTaskExit, status, closed, nil)
		return
	}
	if err := b.closeWorkspaceLog(w); err != nil {
		b.finishWorkspace(w, WorkspaceUncertain, backend.Status{State: backend.Unknown, Reason: "workspace output closure is uncertain"}, false, nil)
		return
	}
	b.workspacePhaseName(w, "collecting")
	sealed, err := b.collectWorkspace(w)
	closed = closed && !errors.Is(err, artifact.ErrFileCloseUncertain) && !errors.Is(err, errWorkspaceCollectionHeld)
	if stop, state, reason := b.workspaceStopped(w); stop {
		b.finishWorkspace(w, workspaceStopOutcome(state), backend.Status{State: state, Reason: reason}, closed, nil)
		return
	}
	if err != nil {
		status.State = backend.Failed
		status.Termination = backend.UnknownTermination
		status.Reason = "collect workspace: " + err.Error()
		b.finishWorkspace(w, WorkspaceCollectionFailure, status, closed && !errors.Is(err, artifact.ErrFileCloseUncertain), nil)
		return
	}
	b.finishWorkspace(w, WorkspaceTaskExit, status, closed, &sealed)
}
func workspaceStopOutcome(state backend.State) WorkspaceOutcome {
	if state == backend.Canceled {
		return WorkspaceCanceled
	}
	return WorkspaceTimeout
}

// finishWorkspace runs after this sequencer's actual filesystem/native phases.
// It closes the common log outside b.mu, then installs the distinct writer receipt
// before retained-payload shedding. Root and candidate remain original owned state.
func (b *Backend) finishWorkspace(w *workspaceExecution, outcome WorkspaceOutcome, status backend.Status, closed bool, candidate *artifact.SealReceipt) {
	e := w.parent
	if err := b.closeWorkspaceLog(w); err != nil {
		closed = false
		status.State = backend.Unknown
		status.Reason = "workspace output closure is uncertain"
	}
	if !closed {
		b.mu.Lock()
		w.physical.Uncertain = true
		b.mu.Unlock()
		b.workspaces.config.PhysicalChanged()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	w.taskDeadlineOpen = false
	// Cancellation can arrive during the actual final log Close. Decide under
	// the same intent lock before closing done or exposing a stable candidate.
	if closed && w.intent {
		outcome = workspaceStopOutcome(w.stopState)
		status = backend.Status{State: w.stopState, Reason: w.stopReason}
		candidate = nil
	}
	status.Instance = e.status.Instance
	status.Reason = retryReason(status.Reason)
	o := WorkspaceObservation{Phase: "complete", Ready: closed, Held: !closed, Outcome: outcome, Status: status, Candidate: candidate}
	if closed {
		o.Writer = &WorkspaceWriterClosure{InstallationID: b.workspaces.config.InstallationID, RootID: w.binding.RootID, JobID: w.binding.JobID, Execution: backend.Reference{ID: w.binding.AttemptID, Binding: w.binding.Binding, Instance: status.Instance}}
	} else {
		o.Outcome = WorkspaceUncertain
		o.Candidate = nil
		o.Status.State = backend.Unknown
		o.Status.Termination = backend.UnknownTermination
	}
	w.observation = o
	// No phase or timer callback may use the accepted task after this point.
	w.binding.Task = backend.Task{}
	if e.output != nil {
		b.finishPayload(e)
	}
	close(e.done)
}

// Protect function-return callbacks against accidental duplicate release without
// spawning a waiter or confusing cancellation with actual filesystem completion.
func onceWorkspaceRelease(release func()) func() {
	var once sync.Once
	return func() { once.Do(release) }
}

// The classification is supplied by the actual Files/archive owner, not inferred
// from error text, timeout or the task state. Returning release retains the SAME
// acquired permit, never a replacement. No reservation/storage credit occurs.
func settleWorkspacePermit(release func(), err error) func() {
	if errors.Is(err, artifact.ErrFileCloseUncertain) {
		return release
	}
	release()
	return nil
}
