//go:build darwin || linux

package control

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

// Only Check models a configured integration's protected-layout qualification.
// Native methods refuse by default or invoke explicitly configured MODELED hooks.
// Every call is counted; acceptance is not
// native/image/helper/cluster qualification, and no Control.Run is started.
type submissionDataBackend struct {
	checks atomic.Int32
	native atomic.Int32
	check  func(backend.Task) error
	// Configure hooks before the original Run, never mutate a live call owner.
	// Their values are MODELED native effects, not backend qualification.
	startData   func(context.Context, backend.DataStart) (backend.DataCreated, error)
	observeData func(context.Context, backend.Reference) (backend.DataObservation, error)
	removeData  func(context.Context, backend.DataResources) (backend.DataRemoval, error)
	cancelData  func(context.Context, backend.Reference) error
}

func (b *submissionDataBackend) Check(task backend.Task) error {
	b.checks.Add(1)
	if b.check != nil {
		return b.check(task)
	}
	return backend.ValidateDataTask(task, nil)
}
func (b *submissionDataBackend) Start(context.Context, string, backend.Task) error {
	b.native.Add(1)
	return ErrUnsupported
}
func (b *submissionDataBackend) Observe(context.Context, backend.Reference) (backend.Status, error) {
	b.native.Add(1)
	return backend.Status{}, ErrUnsupported
}
func (b *submissionDataBackend) Cancel(ctx context.Context, ref backend.Reference) error {
	b.native.Add(1)
	if b.cancelData != nil {
		return b.cancelData(ctx, ref)
	}
	return ErrUnsupported
}
func (b *submissionDataBackend) Logs(context.Context, backend.Reference, int64, int) (backend.LogChunk, error) {
	b.native.Add(1)
	return backend.LogChunk{}, ErrUnsupported
}
func (b *submissionDataBackend) StartData(ctx context.Context, start backend.DataStart) (backend.DataCreated, error) {
	b.native.Add(1)
	if b.startData != nil {
		return b.startData(ctx, start)
	}
	return backend.DataCreated{}, ErrUnsupported
}
func (b *submissionDataBackend) ObserveData(ctx context.Context, ref backend.Reference) (backend.DataObservation, error) {
	b.native.Add(1)
	if b.observeData != nil {
		return b.observeData(ctx, ref)
	}
	return backend.DataObservation{}, ErrUnsupported
}
func (b *submissionDataBackend) RemoveData(ctx context.Context, resources backend.DataResources) (backend.DataRemoval, error) {
	b.native.Add(1)
	if b.removeData != nil {
		return b.removeData(ctx, resources)
	}
	return backend.DataRemoval{}, ErrUnsupported
}

type submissionOrdinaryBackend struct{ backend.Backend }

type submissionDataFixture struct {
	directory   string
	store       *store.Store
	originals   []*store.Store
	root        *os.Root
	files       *artifact.Files
	control     *Control
	backend     *submissionDataBackend
	engine      *auth.Engine
	actor       auth.Actor
	configPath  string
	authPath    string
	tokenPath   string
	authCleanup func() error
	stopAuth    context.CancelFunc
	authDone    chan struct{}
	domain      *store.RequestDomain
	cause       error
	holding     bool
}

func (f *submissionDataFixture) hold(err error) {
	f.cause = errors.Join(f.cause, err)
	if f.holding {
		return
	}
	f.holding = true
	wait := make(chan struct{})
	go func() { <-wait; runtime.KeepAlive(f) }()
}
func (f *submissionDataFixture) retire(t *testing.T) {
	t.Helper()
	if f.stopAuth != nil {
		f.stopAuth()
		select {
		case <-f.authDone:
		case <-time.After(5 * time.Second):
			f.hold(errors.New("original auth refresh did not join"))
			t.Errorf("retain %s: %v", f.directory, f.cause)
			return
		}
	}
	if f.cause != nil {
		f.hold(nil)
		t.Logf("retain original submission resources %s: %v", f.directory, f.cause)
		return
	}
	if f.control != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := f.control.DrainArtifacts(ctx); err != nil {
			f.hold(err)
			t.Errorf("retain %s: %v", f.directory, err)
			return
		}
	}
	if f.files != nil {
		if err := f.files.Close(); err != nil {
			f.hold(err)
			t.Errorf("retain Files/root/Store %s: %v", f.directory, err)
			return
		}
	}
	if f.root != nil {
		if err := f.root.Close(); err != nil {
			f.hold(err)
			t.Errorf("retain root/Store %s: %v", f.directory, err)
			return
		}
	}
	if f.store != nil {
		original := f.store
		f.store = nil
		if err := original.Close(); err != nil {
			f.hold(err)
			t.Errorf("retain Store %s: %v", f.directory, err)
			return
		}
	}
	if t.Failed() {
		f.hold(nil)
		t.Logf("retained failed submission fixture %s", f.directory)
		return
	}
	if f.authCleanup != nil {
		if err := f.authCleanup(); err != nil {
			f.hold(err)
			t.Errorf("retain credentials %s: %v", f.directory, err)
			return
		}
	}
	if err := os.RemoveAll(f.directory); err != nil {
		f.hold(err)
		t.Error(err)
	}
}
func newSubmissionDataFixture(t *testing.T, retained, installed bool) *submissionDataFixture {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261020-data-submission")
	if err = os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	directory, err := os.MkdirTemp(parent, "case-")
	if err != nil {
		t.Fatal(err)
	}
	f := &submissionDataFixture{directory: directory, backend: &submissionDataBackend{}}
	// Keep the complete original resource/error frame before any constructor.
	t.Cleanup(func() { f.retire(t) })
	state := filepath.Join(directory, "state")
	f.store, err = store.Open(state)
	if err != nil {
		f.hold(err)
		t.Fatal(err)
	}
	f.originals = append(f.originals, f.store)
	if retained {
		original := f.store
		f.store = nil
		if err = original.Close(); err != nil {
			f.hold(err)
			t.Fatal(err)
		}
		policy := store.DefaultRetentionPolicy()
		policy.LocalOutputEnabled = false
		info, err := store.EnableRetention(state, policy)
		if err != nil {
			f.hold(err)
			t.Fatal(err)
		}
		f.domain = &info.Domain
		f.store, err = store.Open(state)
		if err != nil {
			f.hold(err)
			t.Fatal(err)
		}
		f.originals = append(f.originals, f.store)
	}
	if installed {
		rootName := filepath.Join(directory, "artifacts")
		if err = os.Mkdir(rootName, 0700); err != nil {
			t.Fatal(err)
		}
		f.root, err = os.OpenRoot(rootName)
		if err != nil {
			f.hold(err)
			t.Fatal(err)
		}
		info, err := f.root.Stat(".")
		if err != nil {
			t.Fatal(err)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			t.Fatal("missing root identity")
		}
		identity := artifact.FileID{Device: uint64(stat.Dev), Inode: uint64(stat.Ino)}
		facts, err := artifact.Initialize(f.root, identity, strings.Repeat("a", 32))
		if err != nil {
			f.hold(err)
			t.Fatal(err)
		}
		f.files, err = artifact.Open(f.root, identity)
		if err != nil {
			f.hold(err)
			t.Fatal(err)
		}
		if _, err = f.store.InstallArtifacts(store.ArtifactInstallation{Enabled: true, ID: facts.ID, Unit: facts.Unit, Volume: facts.Volume}); err != nil {
			t.Fatal(err)
		}
	}
	f.configPath = filepath.Join(directory, "auth.json")
	f.authPath = f.configPath
	f.tokenPath = filepath.Join(directory, "token")
	tokenPath := f.tokenPath
	f.authCleanup, err = auth.Initialize(f.configPath, tokenPath, f.store.Owner())
	if err != nil {
		f.hold(err)
		t.Fatal(err)
	}
	f.engine, err = auth.NewFile(f.configPath, f.store.Owner())
	if err != nil {
		t.Fatal(err)
	}
	token, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	f.actor, err = f.engine.Authenticate("Bearer " + string(bytes.TrimSuffix(token, []byte("\n"))))
	if err != nil {
		t.Fatal(err)
	}
	if err = f.store.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	f.stopAuth = stop
	f.authDone = make(chan struct{})
	go func() { defer close(f.authDone); f.engine.Run(ctx) }()
	f.control, err = NewWithConfig(f.store, []backend.Target{f.target()}, f.engine, Config{Artifacts: f.files})
	if err != nil {
		f.hold(err)
		t.Fatal(err)
	}
	f.sample(t)
	return f
}
func (f *submissionDataFixture) target() backend.Target {
	policy := backend.DefaultDataPolicy()
	return backend.Target{Name: "data", Binding: "modeled-profile-v1", Kind: "modeled-data", Backend: f.backend, DataPolicy: &policy, SupportsRemoteData: true, SupportsDataMounts: true, SupportsRemoteDataRetry: true, SupportsRetry: true, SupportsResourceLimits: true, OutputStorage: backend.RemoteOutput}
}
func (f *submissionDataFixture) sample(t *testing.T) {
	t.Helper()
	if !f.control.retention.enabled {
		return
	}
	// Actual filesystem/DB observation; no synthetic free-space/verified-root
	// facts, cache success flag, native receipt or new sampling hook is installed.
	sample := sampleStorage(context.Background(), f.store.Directory(), 0, nil)
	f.control.retention.mu.Lock()
	f.control.retention.sample = sample
	f.control.retention.mu.Unlock()
	if err := f.control.storageGuard(false, false); err != nil {
		t.Skipf("actual storage admission unavailable: %v", err)
	}
}
func submissionDataTask() backend.Task {
	return backend.Task{Argv: []string{"trainer"}, Image: "modeled-image", DataMounts: []backend.DataMount{{Reference: "dataset", Destination: "/mnt/skygo/dataset", ReadOnly: true}}}
}
func (f *submissionDataFixture) submit(id string, task backend.Task) (backend.Job, error) {
	if f.domain != nil {
		return f.control.SubmitDataInDomain(f.actor, *f.domain, id, task)
	}
	return f.control.SubmitData(f.actor, id, task)
}

func TestDataSubmissionAcceptsWithoutStartingNativeWork(t *testing.T) {
	for _, retained := range []bool{false, true} {
		t.Run(fmt.Sprint(retained), func(t *testing.T) {
			f := newSubmissionDataFixture(t, retained, true)
			task := submissionDataTask()
			task.Env = map[string]string{}
			j, err := f.submit("accepted", task)
			if err != nil {
				t.Fatal(err)
			}
			if j.Status.State != backend.Queued || j.Target != "data" || j.TargetBinding != "modeled-profile-v1" || j.Task.Env != nil {
				t.Fatalf("accepted route/task: %#v", j)
			}
			row, err := f.store.GetRemoteAttempt(store.RemoteKey{JobID: j.ID, AttemptID: j.CurrentAttemptID()})
			if err != nil {
				t.Fatal(err)
			}
			delegation, err := f.engine.Delegate(f.actor)
			if err != nil {
				t.Fatal(err)
			}
			if row.State != 1 || row.Creation != 0 || row.Active != nil || row.Authority != delegation || row.Policy != backend.DefaultDataPolicy() {
				t.Fatalf("accepted remote intent: %#v", row)
			}
			if f.backend.checks.Load() != 1 || f.backend.native.Load() != 0 {
				t.Fatal("submission bypassed Check or invoked native work")
			}
			// Replay precedes changed/missing current route, marker service, admission,
			// planner and new delegation. These are current-process fault conditions;
			// durable replay must retain the actual previously authenticated acceptance.
			f.control.failed.Store(true)
			f.control.StopArtifactAdmission()
			f.control.dataTargets = nil
			same, err := func() (backend.Job, error) {
				service := f.control.artifacts
				f.control.artifacts = nil
				defer func() { f.control.artifacts = service }()
				return f.submit("accepted", task)
			}()
			if err != nil || !reflect.DeepEqual(same, j) || f.backend.checks.Load() != 1 {
				t.Fatalf("exact data replay repeated placement: %#v %v", same, err)
			}
			changed := backend.CloneTask(task)
			changed.Argv = []string{"different"}
			if _, err = f.submit("accepted", changed); err == nil || errors.Is(err, ErrUnavailable) || errors.Is(err, ErrUnsupported) {
				t.Fatalf("request conflict hidden by current placement: %v", err)
			}
			if _, err = f.submit("new-after-stop", task); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("new acceptance bypassed stopped admission: %v", err)
			}
		})
	}
}

func TestDataSubmissionRequiresArtifactsEvenForMountOnly(t *testing.T) {
	f := newSubmissionDataFixture(t, false, false)
	if _, err := f.submit("uninstalled", submissionDataTask()); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("mount-only bypassed installed marker gate: %v", err)
	}
	if f.backend.checks.Load() != 0 {
		t.Fatal("uninstalled request called backend Check")
	}
	sequence, err := f.store.LastSequenceFor(f.actor)
	if err != nil || sequence != 0 {
		t.Fatalf("uninstalled acceptance mutated jobs: %d %v", sequence, err)
	}
}

func TestDataConfigurationRequiresBothPolicyAndConcreteBackend(t *testing.T) {
	f := newSubmissionDataFixture(t, false, true)
	for _, name := range []string{"missing-interface", "invalid-policy"} {
		t.Run(name, func(t *testing.T) {
			target := f.target()
			if name == "missing-interface" {
				target.Backend = &submissionOrdinaryBackend{Backend: f.backend}
			} else {
				target.DataPolicy.StageSeconds++
			}
			if _, err := NewWithConfig(f.store, []backend.Target{target}, f.engine, Config{Artifacts: f.files}); err == nil {
				t.Fatal("malformed configured data route accepted")
			}
		})
	}
	target := f.target()
	target.DataPolicy = nil
	c, err := NewWithConfig(f.store, []backend.Target{target}, f.engine, Config{Artifacts: f.files})
	if err != nil {
		t.Fatal(err)
	}
	if len(c.dataTargets) != 0 {
		t.Fatal("interface presence alone advertised data route")
	}
	if _, err = c.SubmitData(f.actor, "missing-policy", submissionDataTask()); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("missing policy accepted data: %v", err)
	}
	target = f.target()
	original := *target.DataPolicy
	c, err = NewWithConfig(f.store, []backend.Target{target}, f.engine, Config{Artifacts: f.files})
	if err != nil {
		t.Fatal(err)
	}
	target.DataPolicy.StageSeconds++
	listed, err := c.Targets(f.actor)
	if err != nil {
		t.Fatal(err)
	}
	listed[0].DataPolicy.StageSeconds++
	route, err := c.dataTargetFor(backend.Job{Target: "data", TargetBinding: "modeled-profile-v1"})
	if err != nil || route.policy != original || *route.target.DataPolicy != original {
		t.Fatalf("policy alias escaped constructor/copy boundaries: %#v %v", route, err)
	}
	if _, err = c.dataTargetFor(backend.Job{Target: "data", TargetBinding: "different"}); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("route adopted changed binding: %v", err)
	}
}

func TestDataSubmissionRechecksActualFileAuthorityAfterCheck(t *testing.T) {
	f := newSubmissionDataFixture(t, false, true)
	f.prepareRevocation(t)
	f.backend.check = func(task backend.Task) error {
		changed := f.engine.Changed()
		if err := auth.Revoke(f.configPath, f.actor.TokenID()); err != nil {
			return err
		}
		select {
		case <-changed:
			return nil
		case <-time.After(5 * time.Second):
			return errors.New("original auth refresh did not observe revocation")
		}
	}
	if _, err := f.submit("revoked-during-check", submissionDataTask()); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("accepted after actual revocation during Backend.Check: %v", err)
	}
	jobs, err := f.store.List(0, 0, 10)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("revoked placement accepted job: %#v %v", jobs, err)
	}
	if f.backend.checks.Load() != 1 || f.backend.native.Load() != 0 {
		t.Fatal("revocation test did not reach actual Check exactly once")
	}
}

// Shared source-only test fixture contract for disjoint Control authors.
// Each invocation owns distinct actual Store/Files/root/auth resources; no
// Control.Run is launched. Callers must retain/join any additional original
// native/HTTP/worker owners they introduce before this fixture can retire.
type dataControlFixture = submissionDataFixture

func newDataControlFixture(t *testing.T) *dataControlFixture {
	return newSubmissionDataFixture(t, false, true)
}
func (f *submissionDataFixture) input(t *testing.T) (artifact.Ref, []byte) {
	t.Helper()
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	payload := []byte("data fixture input\n")
	if err := writer.WriteHeader(&tar.Header{Name: "input.txt", Mode: 0600, Size: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	data := bytes.Clone(archive.Bytes())
	digest := sha256.Sum256(data)
	u, err := f.store.BeginArtifactUpload(f.actor, hex.EncodeToString(digest[:]), uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	ref, err := artifactPortableRef(u.Reference)
	if err != nil {
		t.Fatal(err)
	}
	file, err := f.files.CreatePart(ref)
	if err != nil {
		f.hold(err)
		t.Fatal(err)
	}
	storedFile := store.ArtifactFile{Device: file.Device, Inode: file.Inode}
	u, err = f.store.ArtifactPartCreated(f.actor, store.ArtifactCursor{ID: u.Reference.ID, Revision: u.Revision}, storedFile)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := f.files.WritePart(ref, file, 0, 0, data)
	if err != nil {
		f.hold(err)
		t.Fatal(err)
	}
	u, err = f.store.ArtifactChunkSynced(f.actor, store.ArtifactCursor{ID: u.Reference.ID, Revision: u.Revision}, storedFile, uint64(chunk.Offset), uint64(chunk.Size), chunk.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	u, err = f.store.BeginArtifactFinalize(f.actor, store.ArtifactCursor{ID: u.Reference.ID, Revision: u.Revision})
	if err != nil {
		t.Fatal(err)
	}
	f.control.physicalChanged()
	uploadGrowthSample(t, f.control)
	sealed, err := f.files.SealPart(ref, file, f.control.uploadLinkGrowthCheck)
	if err != nil {
		f.hold(err)
		t.Fatal(err)
	}
	manifest := store.ArtifactManifest{Reference: u.Reference, ExpandedBytes: sealed.Manifest.ExpandedBytes, RegularFiles: sealed.Manifest.RegularFiles, Directories: sealed.Manifest.Directories}
	if _, err = f.store.CompleteArtifactUpload(f.actor, store.ArtifactCursor{ID: u.Reference.ID, Revision: u.Revision}, u.Reference, store.ArtifactFile{Device: sealed.File.Device, Inode: sealed.File.Inode}, manifest); err != nil {
		t.Fatal(err)
	}
	f.control.physicalChanged()
	return ref, data
}

func TestDataSubmissionWorkdirUsesActualPublishedInput(t *testing.T) {
	f := newDataControlFixture(t)
	ref, _ := f.input(t)
	task := backend.Task{Argv: []string{"trainer"}, Image: "modeled-image", Workdir: &ref, Outputs: []string{"result.txt"}}
	j, err := f.control.SubmitData(f.actor, "workdir-data", task)
	if err != nil {
		t.Fatal(err)
	}
	if j.Task.Workdir == nil || *j.Task.Workdir != ref || j.Target != "data" {
		t.Fatalf("remote workdir acceptance: %#v", j)
	}
	if f.backend.native.Load() != 0 {
		t.Fatal("workdir data acceptance ran native work")
	}
}

func TestDataSubmissionRequiresActualFileCredential(t *testing.T) {
	f := newDataControlFixture(t)
	if _, err := f.control.SubmitData(auth.Actor{}, "zero-actor", submissionDataTask()); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("zero Actor accepted: %v", err)
	}
	legacy := auth.NewLegacy("")
	actor, err := legacy.Authenticate("")
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewWithConfig(f.store, []backend.Target{f.target()}, legacy, Config{Artifacts: f.files})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.SubmitData(actor, "legacy-data", submissionDataTask()); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("legacy identity became delegated data authority: %v", err)
	}
	if f.backend.checks.Load() != 0 || f.backend.native.Load() != 0 {
		t.Fatal("invalid file authority reached placement or native work")
	}
}

func TestDataCapabilityFlagsDoNotFollowPolicy(t *testing.T) {
	f := newDataControlFixture(t)
	for _, name := range []string{"remote", "mounts", "retry"} {
		t.Run(name, func(t *testing.T) {
			target := f.target()
			task := submissionDataTask()
			switch name {
			case "remote":
				target.SupportsRemoteData = false
			case "mounts":
				target.SupportsDataMounts = false
			case "retry":
				target.SupportsRemoteDataRetry = false
				task.Retry = &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{75}}
			}
			c, err := NewWithConfig(f.store, []backend.Target{target}, f.engine, Config{Artifacts: f.files})
			if err != nil {
				t.Fatal(err)
			}
			before := f.backend.checks.Load()
			if _, err = c.SubmitData(f.actor, "unsupported-"+name, task); !errors.Is(err, ErrUnsupported) {
				t.Fatalf("capability %s inferred from policy: %v", name, err)
			}
			if f.backend.checks.Load() != before {
				t.Fatal("unqualified route called backend")
			}
		})
	}
	jobs, err := f.store.List(0, 0, 10)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("capability refusal accepted job: %#v %v", jobs, err)
	}
}

func TestDataCapabilitiesCannotHideInPhysicalRequest(t *testing.T) {
	original := maintenanceRequest{physicalOwner: &Control{}}
	if !physicalRequestShape(original) {
		t.Fatal("physical request fixture is not valid")
	}
	for _, name := range []string{"remote", "mounts", "retry"} {
		t.Run(name, func(t *testing.T) {
			changed := original
			switch name {
			case "remote":
				changed.target.SupportsRemoteData = true
			case "mounts":
				changed.target.SupportsDataMounts = true
			case "retry":
				changed.target.SupportsRemoteDataRetry = true
			}
			if physicalRequestShape(changed) {
				t.Fatalf("physical arm accepted %s target capability", name)
			}
		})
	}
}

// The initial administrative token is the last administrative credential.
// Preserve another real credential before revoking it. These deliberate file
// publications replace Initialize's original inode; after positive joins the
// fixture removes its private namespace instead of replaying stale cleanup.
func (f *submissionDataFixture) prepareRevocation(t *testing.T) {
	t.Helper()
	f.authCleanup = nil
	spare := filepath.Join(f.directory, "spare-token")
	if _, err := auth.Issue(f.configPath, "legacy", spare, time.Hour); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(spare)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		changed := f.engine.Changed()
		if _, err = f.engine.Authenticate("Bearer " + string(bytes.TrimSpace(raw))); err == nil {
			return
		}
		select {
		case <-changed:
		case <-deadline.C:
			t.Fatal("original auth refresh did not observe spare credential")
		}
	}
}

// These detached-value cases exercise the matcher contract, not a native
// directory census. The generated output test separately samples actual Files.
func TestDataUntouchedReservationNamesRemainUnexplained(t *testing.T) {
	marker := artifact.PhysicalEntry{Name: ".skygo-artifact-root", File: artifact.FileID{Device: 17, Inode: 1}, Type: artifact.PhysicalEntryRegular}
	reserved := store.ArtifactPhysicalSubject{Kind: "object", ID: "reserved", NotAttempted: true}
	for _, tc := range []struct {
		name   string
		change func(*artifact.PhysicalSample, *physicalCohort)
		want   bool
	}{
		{"untouched", func(*artifact.PhysicalSample, *physicalCohort) {}, true},
		{"same-part", func(s *artifact.PhysicalSample, _ *physicalCohort) {
			s.RootEntries = append(s.RootEntries, artifact.PhysicalEntry{Name: "reserved.part", File: artifact.FileID{Device: 17, Inode: 2}, Type: artifact.PhysicalEntryRegular})
		}, false},
		{"same-tar", func(s *artifact.PhysicalSample, _ *physicalCohort) {
			s.RootEntries = append(s.RootEntries, artifact.PhysicalEntry{Name: "reserved.tar", File: artifact.FileID{Device: 17, Inode: 2}, Type: artifact.PhysicalEntryRegular})
		}, false},
		{"special", func(s *artifact.PhysicalSample, _ *physicalCohort) {
			s.RootEntries = append(s.RootEntries, artifact.PhysicalEntry{Name: "reserved.part", File: artifact.FileID{Device: 17, Inode: 2}, Type: artifact.PhysicalEntryOther})
		}, false},
		{"unrelated", func(s *artifact.PhysicalSample, _ *physicalCohort) {
			s.RootEntries = append(s.RootEntries, artifact.PhysicalEntry{Name: "unexpected", File: artifact.FileID{Device: 17, Inode: 2}, Type: artifact.PhysicalEntryRegular})
		}, false},
		{"missing-marker", func(s *artifact.PhysicalSample, _ *physicalCohort) { s.RootEntries = nil }, false},
		{"partial", func(s *artifact.PhysicalSample, _ *physicalCohort) { s.Status = artifact.PhysicalPartial }, false},
		{"nonzero-file", func(_ *artifact.PhysicalSample, c *physicalCohort) {
			c.subjects[0].File = store.ArtifactFile{Device: 17, Inode: 2}
		}, false},
		{"partial-file", func(_ *artifact.PhysicalSample, c *physicalCohort) { c.subjects[0].File.Device = 17 }, false},
		{"upload-kind", func(_ *artifact.PhysicalSample, c *physicalCohort) { c.subjects[0].Kind = "upload" }, false},
		{"workspace-kind", func(_ *artifact.PhysicalSample, c *physicalCohort) {
			c.subjects[0].Kind = "workspace"
			c.subjects[0].RootState = store.WorkspaceRootNeverCreated
		}, false},
		{"ordinary-unknown", func(_ *artifact.PhysicalSample, c *physicalCohort) { c.subjects[0].NotAttempted = false }, false},
		{"contradictory-upload", func(_ *artifact.PhysicalSample, c *physicalCohort) {
			c.subjects = append(c.subjects, store.ArtifactPhysicalSubject{Kind: "upload", ID: "reserved", File: store.ArtifactFile{Device: 17, Inode: 2}})
		}, false},
		{"creating-unknown", func(_ *artifact.PhysicalSample, c *physicalCohort) {
			c.subjects[0].NotAttempted = false
			c.subjects = append(c.subjects, store.ArtifactPhysicalSubject{Kind: "upload", ID: "reserved"})
		}, false},
		{"actual-bound-part", func(s *artifact.PhysicalSample, c *physicalCohort) {
			c.subjects[0].NotAttempted = false
			c.subjects = append(c.subjects, store.ArtifactPhysicalSubject{Kind: "upload", ID: "reserved", File: store.ArtifactFile{Device: 17, Inode: 2}})
			s.RootEntries = append(s.RootEntries, artifact.PhysicalEntry{Name: "reserved.part", File: artifact.FileID{Device: 17, Inode: 2}, Type: artifact.PhysicalEntryRegular})
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sample := artifact.PhysicalSample{Status: artifact.PhysicalIntervalComplete, Root: artifact.RootFacts{Volume: 17}, RootEntries: []artifact.PhysicalEntry{marker}}
			cohort := physicalCohort{subjects: []store.ArtifactPhysicalSubject{reserved}}
			tc.change(&sample, &cohort)
			if err := physicalNamesMatch(sample, cohort); (err == nil) != tc.want {
				t.Fatalf("match error=%v wantMatch=%t", err, tc.want)
			}
		})
	}
}

func TestDataPlanExplicitTargetMatchesSubmissionWithoutMutation(t *testing.T) {
	f := newDataControlFixture(t)
	input, _ := f.input(t)
	for _, workdir := range []bool{false, true} {
		t.Run(fmt.Sprint(workdir), func(t *testing.T) {
			task := submissionDataTask()
			task.Target = "data"
			if workdir {
				task.DataMounts = nil
				task.Workdir = &input
			}
			before, err := os.ReadFile(filepath.Join(f.store.Directory(), "jobs.db"))
			if err != nil {
				t.Fatal(err)
			}
			selected, rejected, err := f.control.Plan(f.actor, task)
			if err != nil || selected != "data" || len(rejected) != 0 {
				t.Fatalf("explicit remote plan: %s %#v %v", selected, rejected, err)
			}
			after, err := os.ReadFile(filepath.Join(f.store.Directory(), "jobs.db"))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) || f.backend.native.Load() != 0 {
				t.Fatal("Plan mutated Store or invoked native work")
			}
			job, err := f.control.SubmitData(f.actor, "plan-parity-"+fmt.Sprint(workdir), task)
			if err != nil || job.Target != selected {
				t.Fatalf("plan/submission disagree: %#v %v", job, err)
			}
		})
	}
	for _, name := range []string{"remote", "mounts", "retry"} {
		t.Run(name, func(t *testing.T) {
			target := f.target()
			task := submissionDataTask()
			task.Target = target.Name
			switch name {
			case "remote":
				target.SupportsRemoteData = false
			case "mounts":
				target.SupportsDataMounts = false
			case "retry":
				target.SupportsRemoteDataRetry = false
				task.Retry = &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{75}}
			}
			c, err := NewWithConfig(f.store, []backend.Target{target}, f.engine, Config{Artifacts: f.files})
			if err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(filepath.Join(f.store.Directory(), "jobs.db"))
			if err != nil {
				t.Fatal(err)
			}
			checks := f.backend.checks.Load()
			if _, _, err = c.Plan(f.actor, task); !errors.Is(err, ErrUnsupported) {
				t.Fatalf("unqualified plan: %v", err)
			}
			after, err := os.ReadFile(filepath.Join(f.store.Directory(), "jobs.db"))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) || checks != f.backend.checks.Load() {
				t.Fatal("unqualified plan mutated Store or called Backend.Check")
			}
		})
	}
	if f.backend.native.Load() != 0 {
		t.Fatal("planning/acceptance invoked native work")
	}
}

// Native receipts and time arguments here are MODELED; real Store transitions
// establish the persisted queued/retry states. The retry-waiting input reaches
// State4 only through the actual generated full ReadInput, its original Close
// and Store FinishRemoteInput. This invokes the actual finite scheduler arm and
// does not qualify a remote task or a native clock.
func TestDataRetryCapabilityDriftCannotDispatchPersistedWork(t *testing.T) {
	for _, retryWaiting := range []bool{false, true} {
		t.Run(fmt.Sprint(retryWaiting), func(t *testing.T) {
			f := newDataControlFixture(t)
			input, archive := f.input(t)
			task := backend.Task{Argv: []string{"trainer"}, Image: "modeled-image", Workdir: &input, Retry: &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{75}}}
			job, err := f.control.SubmitData(f.actor, "retry-drift", task)
			if err != nil {
				t.Fatal(err)
			}
			key := store.RemoteKey{JobID: job.ID, AttemptID: job.CurrentAttemptID()}
			now := time.Now().UTC()
			if retryWaiting {
				secret := sha256.Sum256([]byte(key.AttemptID))
				start, began, e := f.store.BeginRemoteAttempt(job.ID, key.AttemptID, store.RemoteHashes{Input: remoteTokenHash(store.RemoteInput, secret[:])}, backend.DefaultDataPolicy(), now)
				if e != nil || !began {
					t.Fatalf("original Begin: %v %v", began, e)
				}
				created := backend.DataCreated{Instance: "modeled-pod", Pod: backend.DataResource{Name: "pod", UID: "modeled-pod"}, InputSecret: backend.DataResource{Name: "input", UID: "modeled-input"}}
				row, e := f.store.RecordRemoteCreated(key, start.Revision, store.RemoteCreation{Succeeded: true, Resources: created}, now)
				if e != nil {
					t.Fatal(e)
				}
				// The selected exit retries only after the complete original input:
				// the actual full ReadInput with the original scope token, its
				// original Close, and Store FinishRemoteInput.
				token, e := remoteToken(key, secret[:])
				clear(secret[:])
				if e != nil {
					t.Fatal(e)
				}
				finish := fullRemoteInput(t, f, string(token), input, archive)
				m := &maintenanceState{input: make(chan maintenanceRequest, 1), results: make(chan maintenanceResult, 1)}
				if admitted, e := f.control.dispatchRemoteInputFinish(m, time.Now()); e != nil || !admitted {
					t.Fatalf("original input close not dispatched: %v %v", admitted, e)
				}
				request := <-m.input
				result := maintenanceResult{request: request}
				result.downloadClose, result.err = runArtifactDownloadClose(context.Background(), request)
				if e = f.control.receiveMaintenance(context.Background(), m, result); e != nil {
					f.hold(e)
					t.Fatal(e)
				}
				if !waitLocked(t, f, 10*time.Second, func() bool { return finish.download.finished }) {
					t.Fatal("original input close did not finish")
				}
				if row, e = f.store.GetRemoteAttempt(key); e != nil || row.Active == nil || row.Active.Input == nil || row.Active.Input.State != 4 || row.Active.ClosedReason != 0 {
					t.Fatalf("actual full read did not reach input State4: %v", e)
				}
				finished := time.Now().UTC()
				code := int32(75)
				stop := &backend.DataTaskStop{Stage: &backend.ContainerExit{ContainerID: "modeled-stage", StartedAt: row.Active.Began, FinishedAt: row.Active.Began}, Task: backend.ContainerExit{ContainerID: "modeled-main", ExitCode: code, StartedAt: row.Active.Began, FinishedAt: finished}}
				if _, e = f.store.ObserveRemote(key, row.Revision, backend.DataObservation{Status: backend.Status{State: backend.Failed, Instance: created.Pod.UID, ExitCode: &code, Termination: backend.StoppedWithExitCode}, Phase: backend.DataSettled, TaskStop: stop}, finished); e != nil {
					t.Fatal(e)
				}
				job, e = f.store.CompleteRemoteAttempt(key, store.RemoteCompletion{Outcome: store.DataMainExit, Stop: stop}, finished)
				if e != nil {
					t.Fatal(e)
				}
				row, e = f.store.GetRemoteAttempt(key)
				if e != nil {
					t.Fatal(e)
				}
				if e = f.store.AckRemoteRemoval(key, row.Revision, backend.DataRemoval{Pod: created.Pod, InputSecret: created.InputSecret, PodRemoved: true, InputRemoved: true}, finished); e != nil {
					t.Fatal(e)
				}
				if job.Status.State != backend.Retrying || job.NextAttemptAt == nil {
					t.Fatal("fixture did not reach actual retry waiting")
				}
				now = job.NextAttemptAt.Add(time.Nanosecond)
			}
			target := f.target()
			target.SupportsRemoteDataRetry = false
			c, err := NewWithConfig(f.store, []backend.Target{target}, f.engine, Config{Artifacts: f.files})
			if err != nil {
				t.Fatal(err)
			}
			row, err := f.store.GetRemoteAttempt(key)
			if err != nil {
				t.Fatal(err)
			}
			beforeJob, err := f.store.Get(job.ID)
			if err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(filepath.Join(f.store.Directory(), "jobs.db"))
			if err != nil {
				t.Fatal(err)
			}
			worker := &targetWorker{target: target, input: make(chan operation, 1)}
			dispatched, err := c.dispatchData(context.Background(), &loopState{}, worker, beforeJob, row, now)
			if err != nil || dispatched || worker.pending != nil || len(worker.input) != 0 || f.backend.native.Load() != 0 {
				t.Fatalf("capability drift dispatched new original: %v %v", dispatched, err)
			}
			afterJob, err := f.store.Get(job.ID)
			if err != nil {
				t.Fatal(err)
			}
			afterRow, err := f.store.GetRemoteAttempt(key)
			if err != nil {
				t.Fatal(err)
			}
			after, err := os.ReadFile(filepath.Join(f.store.Directory(), "jobs.db"))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) || !reflect.DeepEqual(beforeJob, afterJob) || !reflect.DeepEqual(row, afterRow) {
				t.Fatal("capability drift rewrote retained intent or prior outcome")
			}
		})
	}
}
