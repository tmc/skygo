//go:build linux || darwin

package store_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

// remoteExampleFrame keeps exactly this example's original Store, borrowed
// Root and constructor error alive if any original close remains uncertain.
// It is not a retry handle or a registry of reconstructed owners.
type remoteExampleFrame struct {
	base                                   string
	store                                  *store.Store
	root                                   *os.Root
	storeAttempted, rootAttempted          bool
	storeClosed, rootClosed                bool
	constructorErr, operationErr, closeErr error
	keeping                                bool
}

func (f *remoteExampleFrame) hold() {
	if f.keeping {
		return
	}
	f.keeping = true
	wait := make(chan struct{})
	go func() { <-wait; runtime.KeepAlive(f) }()
}

func (f *remoteExampleFrame) fail(err error) {
	f.operationErr = errors.Join(f.operationErr, err)
	if errors.Is(err, artifact.ErrFileCloseUncertain) {
		// Keep the actual typed chain: it privately retains the original Files/FD.
		f.constructorErr = errors.Join(f.constructorErr, err)
		f.hold()
		panic(fmt.Errorf("retain remote example at %s: %w", f.base, err))
	}
	panic(err)
}

func (f *remoteExampleFrame) finish() {
	// An uncertain Initialize close retains an internal Files owner borrowing
	// Root. Keep its exact constructor chain and BOTH Root and Store dependencies;
	// no downstream close or namespace removal is authorized in this case.
	if f.constructorErr != nil {
		f.hold()
		return
	}
	// Observe the exact original Root close first, at most once. A failed root
	// close retains the original pair: Store.Close must not even be attempted.
	if f.root != nil && !f.rootAttempted {
		f.rootAttempted = true
		err := f.root.Close()
		f.rootClosed = err == nil
		f.closeErr = errors.Join(f.closeErr, err)
	}
	if f.closeErr != nil || f.root != nil && !f.rootClosed {
		f.hold()
		panic(fmt.Errorf("retain remote example at %s: %w", f.base, errors.Join(f.operationErr, f.closeErr)))
	}
	// Store is the last original dependency to close, only after Root positively
	// closed (or no Root was acquired). Never retry or replace either owner.
	if f.store != nil && !f.storeAttempted {
		f.storeAttempted = true
		err := f.store.Close()
		f.storeClosed = err == nil
		f.closeErr = errors.Join(f.closeErr, err)
	}
	if f.closeErr != nil || f.store != nil && !f.storeClosed {
		f.hold()
		panic(fmt.Errorf("retain remote example at %s: %w", f.base, errors.Join(f.operationErr, f.closeErr)))
	}
	// Ordinary validation/init errors do not imply uncertain close. Their exact
	// error is retained by the original panic; removal still requires every actual
	// owned Root/Store close to have returned nil in dependency order above.
	if err := os.RemoveAll(f.base); err != nil {
		f.operationErr = errors.Join(f.operationErr, err)
		f.hold()
		panic(err)
	}
}

func newRemoteExampleStore() *remoteExampleFrame {
	home, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261020-remote-record-tests")
	if err = os.MkdirAll(parent, 0700); err != nil {
		panic(err)
	}
	base, err := os.MkdirTemp(parent, "example-")
	if err != nil {
		panic(err)
	}
	f := &remoteExampleFrame{base: base}
	f.store, err = store.Open(filepath.Join(base, "state"))
	// A failing Store constructor exposes no original owner handle. Retain its
	// exact error and path rather than guess that its internal close succeeded.
	if err != nil {
		f.operationErr = err
		f.hold()
		panic(fmt.Errorf("retain remote example at %s: %w", f.base, err))
	}
	return f
}

// This example installs actual measured companion metadata and authenticates
// through an actual file engine. It performs no native backend operation.
func ExampleStore_SubmitData() {
	f := newRemoteExampleStore()
	defer f.finish()
	s := f.store
	dir := filepath.Join(f.base, "objects")
	if err := os.Mkdir(dir, 0700); err != nil {
		f.fail(err)
	}
	var err error
	f.root, err = os.OpenRoot(dir)
	if err != nil {
		f.fail(err)
	}
	root := f.root
	info, err := root.Lstat(".")
	if err != nil {
		f.fail(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		f.fail(errors.New("missing root identity"))
	}
	id := "0123456789abcdef0123456789abcdef"
	facts, err := artifact.Initialize(root, artifact.FileID{Device: uint64(stat.Dev), Inode: uint64(stat.Ino)}, id)
	if err != nil {
		f.fail(err)
	}
	_, err = s.InstallArtifacts(store.ArtifactInstallation{Enabled: true, ID: id, Unit: facts.Unit, Volume: facts.Volume})
	if err != nil {
		f.fail(err)
	}
	// Keep credentials outside the companion root: the installation owns its
	// flat names and never adopts arbitrary application files.
	authDir := filepath.Join(f.base, "auth")
	if err = os.Mkdir(authDir, 0700); err != nil {
		f.fail(err)
	}
	config, tokenPath := filepath.Join(authDir, "auth.json"), filepath.Join(authDir, "token")
	if _, err = auth.Initialize(config, tokenPath, s.Owner()); err != nil {
		f.fail(err)
	}
	if err = s.ActivateScoped(); err != nil {
		f.fail(err)
	}
	engine, err := auth.NewFile(config, s.Owner())
	if err != nil {
		f.fail(err)
	}
	token, err := os.ReadFile(tokenPath)
	if err != nil {
		f.fail(err)
	}
	actor, err := engine.Authenticate("Bearer " + strings.TrimSpace(string(token)))
	if err != nil {
		f.fail(err)
	}
	authority, err := engine.Delegate(actor)
	if err != nil {
		f.fail(err)
	}
	if err = engine.CheckDelegation(authority); err != nil {
		f.fail(err)
	}
	task := backend.Task{Argv: []string{"trainer"}, DataMounts: []backend.DataMount{{Reference: "dataset", Destination: "/mnt/skygo/dataset", ReadOnly: true}}}
	// This is Store admission only. The real target caller also verifies the
	// configured alias, binding and protected paths before accepting work.
	if err = backend.ValidateDataTask(task, nil); err != nil {
		f.fail(err)
	}
	policy := backend.DefaultDataPolicy()
	now := time.Now().UTC()
	job, err := s.SubmitData(actor, nil, "training", task, "remote", "example-remote-binding", authority, policy, now)
	if err != nil {
		f.fail(err)
	}
	first, launch, err := s.BeginRemoteAttempt(job.ID, job.CurrentAttemptID(), store.RemoteHashes{}, policy, now)
	if err != nil {
		f.fail(err)
	}
	replay, again, err := s.BeginRemoteAttempt(job.ID, job.CurrentAttemptID(), store.RemoteHashes{}, policy, now)
	if err != nil {
		f.fail(err)
	}
	key := store.RemoteKey{JobID: job.ID, AttemptID: first.Job.CurrentAttemptID()}
	snapshot, err := s.GetRemoteAttempt(key)
	if err != nil {
		f.fail(err)
	}
	selected, found, err := s.NextRemoteAttempt("")
	if err != nil {
		f.fail(err)
	}
	// Revocation closes admission, but makes no claim that a Pod ever existed.
	closed, err := s.RevokeRemote(key, snapshot.Revision, store.RemoteCanceled, now)
	if err != nil {
		f.fail(err)
	}
	fmt.Println(launch, again, first.Revision == replay.Revision)
	fmt.Println(found, selected.Key == key, snapshot.Creation, closed.Creation)
	fmt.Println(closed.Active.ClosedReason == store.RemoteCanceled)
	// Output:
	// true false true
	// true true 0 0
	// true
}

func ExampleStore_GetRemoteAttempt() {
	f := newRemoteExampleStore()
	defer f.finish()
	s := f.store
	_, err := s.GetRemoteAttempt(store.RemoteKey{})
	fmt.Println(errors.Is(err, store.ErrArtifactsDisabled))
	// Output: true
}

func ExampleStore_NextRemoteAttempt() {
	f := newRemoteExampleStore()
	defer f.finish()
	s := f.store
	_, found, err := s.NextRemoteAttempt("")
	fmt.Println(found, errors.Is(err, store.ErrArtifactsDisabled))
	// Output: false true
}

func ExampleStore_BeginRemoteAttempt() {
	f := newRemoteExampleStore()
	defer f.finish()
	s := f.store
	// Unknown jobs cannot create remote launch intents.
	_, launch, err := s.BeginRemoteAttempt("missing", "missing", store.RemoteHashes{}, backend.DefaultDataPolicy(), time.Now().UTC())
	fmt.Println(launch, errors.Is(err, store.ErrNotFound))
	// Output: false true
}

func ExampleStore_RecordRemoteCreated() {
	f := newRemoteExampleStore()
	defer f.finish()
	s := f.store
	// An empty return value is not an original native resource receipt.
	_, err := s.RecordRemoteCreated(store.RemoteKey{}, 1, store.RemoteCreation{}, time.Now().UTC())
	fmt.Println(err != nil)
	// Output: true
}

func ExampleStore_ObserveRemote() {
	f := newRemoteExampleStore()
	defer f.finish()
	s := f.store
	_, err := s.ObserveRemote(store.RemoteKey{}, 1, backend.DataObservation{}, time.Now().UTC())
	fmt.Println(err != nil)
	// Output: true
}

func ExampleStore_RevokeRemote() {
	f := newRemoteExampleStore()
	defer f.finish()
	s := f.store
	_, err := s.RevokeRemote(store.RemoteKey{}, 1, store.RemoteCanceled, time.Now().UTC())
	fmt.Println(errors.Is(err, store.ErrArtifactsDisabled))
	// Output: true
}

func ExampleStore_CheckRemoteGrant() {
	f := newRemoteExampleStore()
	defer f.finish()
	s := f.store
	_, err := s.CheckRemoteGrant("missing", store.RemoteInput, [32]byte{}, time.Now().UTC())
	fmt.Println(errors.Is(err, store.ErrArtifactsDisabled))
	// Output: true
}

func ExampleStore_PollRemoteRelease() {
	f := newRemoteExampleStore()
	defer f.finish()
	s := f.store
	_, err := s.PollRemoteRelease("missing", [32]byte{}, time.Now().UTC())
	fmt.Println(errors.Is(err, store.ErrArtifactsDisabled))
	// Output: true
}

func ExampleStore_BeginRemoteInputRead() {
	f := newRemoteExampleStore()
	defer f.finish()
	s := f.store
	// A purpose mismatch is refused before opening an artifact reader.
	_, err := s.BeginRemoteInputRead(store.RemoteGrant{Kind: store.RemoteOutput}, store.OriginalServerLifetime{}, time.Now().UTC())
	fmt.Println(errors.Is(err, store.ErrArtifactConflict))
	// Output: true
}

func ExampleStore_FinishRemoteInput() {
	f := newRemoteExampleStore()
	defer f.finish()
	s := f.store
	// Missing original transfer ownership is not a complete input receipt.
	err := s.FinishRemoteInput(store.RemoteGrant{}, store.ArtifactDownloadClosure{}, time.Now().UTC())
	fmt.Println(errors.Is(err, store.ErrArtifactConflict))
	// Output: true
}

func ExampleStore_BeginRemoteOutput() {
	f := newRemoteExampleStore()
	defer f.finish()
	s := f.store
	_, err := s.BeginRemoteOutput(store.RemoteGrant{Kind: store.RemoteInput}, strings.Repeat("a", 64), 512, time.Now().UTC())
	fmt.Println(errors.Is(err, store.ErrArtifactConflict))
	// Output: true
}

func ExampleStore_RemotePartCreated() {
	f := newRemoteExampleStore()
	defer f.finish()
	s := f.store
	// A zero file identity cannot stand for an exclusively created part.
	_, err := s.RemotePartCreated(store.RemoteGrant{}, store.ArtifactCursor{}, store.ArtifactFile{}, time.Now().UTC())
	fmt.Println(errors.Is(err, store.ErrArtifactConflict))
	// Output: true
}

func ExampleStore_RemoteChunkSynced() {
	f := newRemoteExampleStore()
	defer f.finish()
	s := f.store
	// Empty chunks do not acknowledge durable bytes.
	_, err := s.RemoteChunkSynced(store.RemoteGrant{}, store.ArtifactCursor{}, store.ArtifactFile{}, 0, 0, "", time.Now().UTC())
	fmt.Println(errors.Is(err, store.ErrArtifactConflict))
	// Output: true
}

func ExampleStore_BeginRemoteSeal() {
	f := newRemoteExampleStore()
	defer f.finish()
	s := f.store
	_, err := s.BeginRemoteSeal(store.RemoteGrant{}, store.ArtifactCursor{}, time.Now().UTC())
	fmt.Println(errors.Is(err, store.ErrArtifactsDisabled))
	// Output: true
}

func ExampleStore_SealRemoteOutput() {
	f := newRemoteExampleStore()
	defer f.finish()
	s := f.store
	// Missing parser/file receipts cannot publish a private candidate.
	err := s.SealRemoteOutput(store.RemoteGrant{}, store.ArtifactCursor{}, store.RemoteOutputSeal{}, time.Now().UTC())
	fmt.Println(errors.Is(err, store.ErrArtifactConflict))
	// Output: true
}

func ExampleRemoteKey() {
	key := store.RemoteKey{JobID: "job", AttemptID: "attempt"}
	// Retry attempts retain the same job key and get a different attempt key.
	retry := store.RemoteKey{JobID: key.JobID, AttemptID: "retry"}
	fmt.Println(key.JobID == retry.JobID, key == retry)
	// Output: true false
}

func ExampleRemoteHashes() {
	// Empty hashes are appropriate for mount-only work with no byte scopes.
	var mountOnly store.RemoteHashes
	fmt.Println(mountOnly.Input == [32]byte{}, mountOnly.Output == [32]byte{})
	// Output: true true
}

func ExampleRemoteStart() {
	// No committed original launch exists in a zero start result.
	var start store.RemoteStart
	fmt.Println(start.Revision == 0)
	// Output: true
}

func ExampleRemoteCreation() {
	// Succeeded describes the original backend return, never grant activation.
	var original store.RemoteCreation
	fmt.Println(original.Succeeded, original.Resources.Pod.UID == "")
	// Output: false true
}

func ExampleRemoteOutputSeal() {
	// A reference alone cannot replace the original parser and file receipts.
	seal := store.RemoteOutputSeal{Reference: store.ArtifactRef{ID: "candidate"}}
	fmt.Println(seal.File == (store.ArtifactFile{}), seal.Manifest == (store.ArtifactManifest{}))
	// Output: true true
}

func ExampleRemoteCompletion() {
	// An empty handoff proves neither stop nor successful collection.
	var completion store.RemoteCompletion
	fmt.Println(completion.Stop == nil, completion.Collector == nil, completion.Candidate == nil)
	// Output: true true true
}

func ExampleRemoteGrantKind() {
	fmt.Println(store.RemoteInput == store.RemoteOutput)
	// Output: false
}

func ExampleRemoteCloseReason() {
	// Admission closure records its reason independently of native stop proof.
	reason := store.RemoteCanceled
	fmt.Println(reason == store.RemoteExpired)
	// Output: false
}

func ExampleDataOutcome() {
	// Main exit and output collection failure have different retry semantics.
	fmt.Println(store.DataMainExit == store.DataCollectionFailure)
	// Output: false
}

func ExampleRemoteRelease() {
	// A missing reply never means collect or abort has been authorized.
	var release store.RemoteRelease
	fmt.Println(release.State, release.TaskExitCode == nil)
	// Output: 0 true
}

func ExampleRemoteGrant() {
	// Only CheckRemoteGrant supplies a checked epoch; a zero value grants none.
	var grant store.RemoteGrant
	fmt.Println(grant.Epoch, grant.Expires.IsZero())
	// Output: 0 true
}

func ExampleRemoteScope() {
	// Scope snapshots contain a digest, not the bearer token.
	var scope store.RemoteScope
	fmt.Println(scope.Hash == [32]byte{}, scope.Epoch)
	// Output: true 0
}

func ExampleRemoteActive() {
	// No stop or no-launch receipt is inferred from a missing observation.
	var active store.RemoteActive
	fmt.Println(active.Stop == nil, active.NoLaunch == nil, active.Running == nil)
	// Output: true true true
}

func ExampleRemoteDecision() {
	// Result ID names metadata; it is not itself an artifact read grant.
	decision := store.RemoteDecision{ResultID: "candidate"}
	fmt.Println(decision.ResultID, decision.Stop == nil)
	// Output: candidate true
}

func ExampleRemoteAttempt() {
	// Detached attempt snapshots distinguish unknown creation from activation.
	var attempt store.RemoteAttempt
	fmt.Println(attempt.Creation, attempt.Active == nil, attempt.Done == nil)
	// Output: 0 true true
}

func ExampleStore_CompleteRemoteAttempt() {
	f := newRemoteExampleStore()
	defer f.finish()
	s := f.store
	// An absent original owner handoff cannot complete a remote attempt.
	_, err := s.CompleteRemoteAttempt(store.RemoteKey{}, store.RemoteCompletion{}, time.Now().UTC())
	fmt.Println(err != nil)
	// Output: true
}

func ExampleStore_AckRemoteRemoval() {
	f := newRemoteExampleStore()
	defer f.finish()
	s := f.store
	// Empty removal values never manufacture positive UID-qualified retirement.
	err := s.AckRemoteRemoval(store.RemoteKey{}, 1, backend.DataRemoval{}, time.Now().UTC())
	fmt.Println(err != nil)
	// Output: true
}

func ExampleStore_GetRemoteOutput() {
	f := newRemoteExampleStore()
	defer f.finish()
	// An input-purpose grant cannot inspect output metadata. Zero returned file
	// values on refusal provide no physical absence or ownership proof.
	upload, file, manifest, err := f.store.GetRemoteOutput(store.RemoteGrant{Kind: store.RemoteInput}, time.Now().UTC())
	fmt.Println(errors.Is(err, store.ErrArtifactConflict))
	fmt.Println(upload == (store.ArtifactUpload{}), file == (store.ArtifactFile{}), manifest == (store.ArtifactManifest{}))
	// Output:
	// true
	// true true true
}
