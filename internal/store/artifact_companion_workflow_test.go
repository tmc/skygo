//go:build linux

package store_test

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

// UNRUN source. Opt in on a qualified Linux host. This exercises real Control.Run,
// Local workspace execution, result publication and production log retirement;
// it does not qualify HTTP, server assembly, a crash or remote writer death.
func TestArtifactCompanionCompletedWorkspace(t *testing.T) {
	if os.Getenv("SKYGO_COMPANION_NATIVE") != "1" {
		t.Skip("set SKYGO_COMPANION_NATIVE=1 on a qualified Linux host")
	}
	for _, manual := range []bool{false, true} {
		name := "retained-job"
		if manual {
			name = "manual-history"
		}
		t.Run(name, func(t *testing.T) { companionCompletedWorkspace(t, manual) })
	}
}

// The exact running test executable supplies a bounded, no-retry workload. It
// writes inside the extracted workspace; no completion or cleanup row is seeded.
func TestArtifactCompanionWorkload(t *testing.T) {
	if os.Getenv("SKYGO_COMPANION_WORKLOAD") != "1" {
		return
	}
	input, err := os.ReadFile("seed.txt")
	if err != nil || string(input) != "seed\n" {
		fmt.Fprintln(os.Stderr, "workspace seed:", err)
		os.Exit(91)
	}
	if err := os.WriteFile("result.txt", []byte("companion result\n"), 0600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(92)
	}
	fmt.Println("companion workspace log")
	os.Exit(0)
}

// No t.TempDir cleanup can remove a namespace beneath an unknown original owner.
// Successful flags record each sole observed Close; uncertain owners are retained
// as the same frame, following the existing persistent native fixture pattern.
type companionWorkflowFrame struct {
	path                                    string
	database                                *store.Store
	root                                    *os.Root
	files                                   *artifact.Files
	reader                                  *artifact.Reader
	local                                   *local.Backend
	control                                 *control.Control
	cancel                                  context.CancelFunc
	done                                    chan error
	stopped, localClosed                    bool
	filesClosed, rootClosed, databaseClosed bool
	held                                    error
	keeping                                 bool
}

func (f *companionWorkflowFrame) hold(t *testing.T, err error) {
	t.Helper()
	if f.held == nil {
		f.held = err
	}
	t.Errorf("HOLD original companion fixture %s: %v", f.path, f.held)
	if !f.keeping {
		f.keeping = true
		wait := make(chan struct{})
		go func() { <-wait; runtime.KeepAlive(f) }()
	}
}

func (f *companionWorkflowFrame) closeOriginal(ctx context.Context) error {
	if f.held != nil {
		return f.held
	}
	if f.control != nil && !f.stopped {
		if err := f.control.BeginShutdown(); err != nil {
			return err
		}
		if err := f.control.WaitShutdownPrepared(ctx); err != nil {
			return err
		}
		if err := f.control.DrainArtifacts(ctx); err != nil {
			return err
		}
		f.cancel()
		select {
		case err := <-f.done:
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
		case <-ctx.Done():
			return fmt.Errorf("original Run remains held: %w", ctx.Err())
		}
		if err := f.control.Join(ctx); err != nil {
			return err
		}
		if err := f.local.CheckShutdown(); err != nil {
			return err
		}
		f.stopped = true
	}
	if f.local != nil && !f.localClosed {
		if err := f.local.Close(); err != nil {
			return err
		}
		f.localClosed = true
	}
	if f.reader != nil {
		if err := f.reader.Close(); err != nil {
			return err
		}
		f.reader = nil
	}
	if f.files != nil && !f.filesClosed {
		if err := f.files.Close(); err != nil {
			return err
		}
		f.filesClosed = true
	}
	if f.root != nil && !f.rootClosed {
		if err := f.root.Close(); err != nil {
			return err
		}
		f.rootClosed = true
	}
	if f.database != nil && !f.databaseClosed {
		if err := f.database.Close(); err != nil {
			return err
		}
		f.databaseClosed = true
	}
	return nil
}

func companionCompletedWorkspace(t *testing.T, manual bool) {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261020-companion-workflow-tests")
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	path, err := os.MkdirTemp(parent, "original-")
	if err != nil {
		t.Fatal(err)
	}
	f := &companionWorkflowFrame{path: path}
	active := f
	fail := func(format string, args ...any) {
		t.Helper()
		err := fmt.Errorf(format, args...)
		active.hold(t, err)
		if active != f {
			f.hold(t, err)
		}
		t.FailNow()
	}
	t.Cleanup(func() {
		if f.held != nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := f.closeOriginal(ctx); err != nil {
			f.hold(t, err)
			return
		}
		if err := os.RemoveAll(f.path); err != nil {
			f.hold(t, err)
		}
	})
	directory, objectDirectory := filepath.Join(path, "state"), filepath.Join(path, "objects")
	if err := os.Mkdir(objectDirectory, 0700); err != nil {
		fail("private object directory: %w", err)
	}
	info, err := store.EnableRetention(directory, store.DefaultRetentionPolicy())
	if err != nil {
		fail("enable local retention: %w", err)
	}
	f.database, err = store.Open(directory)
	if err != nil {
		fail("open original Store: %w", err)
	}
	f.root, err = os.OpenRoot(objectDirectory)
	if err != nil {
		fail("open original root: %w", err)
	}
	identity := companionWorkflowIdentity(t, f.root)
	facts, err := artifact.Initialize(f.root, identity, strings.Repeat("a", 32))
	if err != nil {
		fail("initialize original root: %w", err)
	}
	f.files, err = artifact.Open(f.root, identity)
	if err != nil {
		fail("open original Files: %w", err)
	}
	if _, err := f.database.InstallArtifacts(store.ArtifactInstallation{Enabled: true, ID: facts.ID, Unit: facts.Unit, Volume: facts.Volume}); err != nil {
		fail("install actual original facts: %w", err)
	}
	policy := info.Policy
	f.local, err = local.NewRetained(filepath.Join(directory, "logs"), local.RetentionConfig{Owner: f.database.Owner(), OutputUnit: policy.OutputUnit, Volume: policy.Volume, OutputBytes: policy.OutputBytes, OutputDomains: policy.OutputDomains, OutputFiles: policy.OutputFiles})
	if err != nil {
		fail("open original Local: %w", err)
	}
	binding, err := backend.LocalBinding(f.database.Owner())
	if err != nil {
		fail("original binding: %w", err)
	}
	if err := f.database.BindOutputStorage("local", binding, backend.LocalOutput, f.local.OutputReservation()); err != nil {
		fail("bind original local output: %w", err)
	}
	engine := auth.NewLegacy("companion-workflow")
	actor, err := engine.Authenticate("Bearer companion-workflow")
	if err != nil {
		fail("authenticate original operator: %w", err)
	}
	producer, err := local.CaptureServerLifetime()
	if err != nil {
		fail("capture original producer: %w", err)
	}
	target := backend.Target{Name: "local", Kind: "local", Binding: binding, Backend: f.local, SupportsRetry: f.local.SupportsRetry(), OutputStorage: backend.LocalOutput, OutputReservation: f.local.OutputReservation(), Retention: f.local.Retention()}
	f.control, err = control.NewWithConfig(f.database, []backend.Target{target}, engine, control.Config{
		Artifacts: f.files, LocalBackend: f.local, ServerLifetime: producer,
		LocalWorkspace: &control.LocalWorkspaceConfig{Target: "local", Binding: binding, Backend: f.local, Roots: f.root},
		LocalOutput: control.LocalOutputConfig{Target: "local", Binding: binding, BindOutput: f.local.BindOutput, OutputUsage: func(ctx context.Context) (uint64, uint64, error) {
			u, err := f.local.OutputUsage(ctx)
			return u.AllocatedBytes, u.Files, err
		}},
	})
	if err != nil {
		fail("configure original Control: %w", err)
	}
	if !f.control.WorkspaceTargetEnabled("local", binding) {
		fail("original Linux workspace prerequisites unavailable")
	}
	runContext, cancel := context.WithCancel(context.Background())
	f.cancel, f.done = cancel, make(chan error, 1)
	go func() { f.done <- f.control.Run(runContext) }()
	ctx, end := context.WithTimeout(context.Background(), 60*time.Second)
	defer end()
	wait := func(label string, ready func() (bool, error)) {
		t.Helper()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			ok, err := ready()
			if err != nil {
				fail("%s: %w", label, err)
			}
			if ok {
				return
			}
			select {
			case <-ctx.Done():
				fail("%s: %w", label, ctx.Err())
			case err := <-f.done:
				// Preserve the original result; never consume its sole join receipt.
				f.done <- err
				fail("original Run returned during %s: %v", label, err)
			case <-ticker.C:
			}
		}
	}
	wait("actual storage readiness", func() (bool, error) { return f.control.StorageReady(), nil })
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	if err := writer.WriteHeader(&tar.Header{Name: "seed.txt", Mode: 0600, Size: 5, Format: tar.FormatUSTAR}); err != nil {
		fail("input header: %w", err)
	}
	if _, err := writer.Write([]byte("seed\n")); err != nil {
		fail("input bytes: %w", err)
	}
	if err := writer.Close(); err != nil {
		fail("input archive Close: %w", err)
	}
	sha, err := artifact.Validate(bytes.NewReader(archive.Bytes()), int64(archive.Len()))
	if err != nil {
		fail("validate original input: %w", err)
	}
	registrationBusy := errors.New("pre-call registration busy")
	withCall := func(operation func(*control.ArtifactCall) error) error {
		call, err := f.control.TryArtifactCall()
		if errors.Is(err, control.ErrArtifactBusy) {
			return registrationBusy
		}
		if err != nil {
			return err
		}
		defer call.Done()
		return operation(call)
	}
	retry := func(label string, operation func() error, safe func(error) bool) {
		t.Helper()
		wait(label, func() (bool, error) {
			err := operation()
			if err == nil {
				return true, nil
			}
			if safe(err) {
				return false, nil
			}
			return false, err
		})
	}
	var upload store.ArtifactUpload
	retry("begin actual input", func() error {
		return withCall(func(call *control.ArtifactCall) error {
			var err error
			upload, err = f.control.BeginArtifactUpload(ctx, actor, call, sha, int64(archive.Len()))
			return err
		})
	}, func(err error) bool {
		return errors.Is(err, registrationBusy) || control.IsArtifactBeginNotStarted(err)
	})
	safePart := func(err error) bool {
		return errors.Is(err, registrationBusy) || errors.Is(err, control.ErrArtifactBusy) || errors.Is(err, artifact.ErrFilesBusy)
	}
	retry("write same original input", func() error {
		return withCall(func(call *control.ArtifactCall) error {
			written, err := f.control.WriteArtifactChunk(ctx, actor, call, upload.Reference.ID, 0, archive.Bytes())
			if err == nil && written.Acknowledged != uint64(archive.Len()) {
				return errors.New("input acknowledgment differs")
			}
			return err
		})
	}, safePart)
	var input artifact.Ref
	retry("finalize same original input", func() error {
		return withCall(func(call *control.ArtifactCall) error {
			var err error
			input, err = f.control.FinalizeArtifactUpload(ctx, actor, call, upload.Reference.ID)
			return err
		})
	}, safePart)
	executable, err := os.Executable()
	if err != nil {
		fail("original workload executable: %w", err)
	}
	task := backend.Task{Argv: []string{executable, "-test.run=^TestArtifactCompanionWorkload$"}, Workdir: &input, Outputs: []string{"result.txt"}, TimeoutSeconds: 10, Env: map[string]string{"SKYGO_COMPANION_WORKLOAD": "1"}}
	accepted, err := f.control.SubmitWorkspaceInDomain(actor, info.Domain, "companion-completed", task)
	if err != nil {
		fail("submit original no-retry workspace: %w", err)
	}
	var completed backend.Job
	var result store.ArtifactAttemptResult
	wait("real completed result and root retirement", func() (bool, error) {
		var err error
		completed, err = f.database.Get(accepted.ID)
		if err != nil {
			return false, err
		}
		if !completed.Status.State.Terminal() {
			return false, nil
		}
		if completed.Status.State != backend.Succeeded || len(completed.Attempts) != 0 {
			return false, fmt.Errorf("unexpected no-retry completion: %+v", completed)
		}
		result, err = f.control.ArtifactResultFor(actor, completed.ID, completed.CurrentAttemptID())
		if err != nil {
			return false, err
		}
		if result.State != "available" || result.Reference == nil {
			return false, errors.New("completed result unavailable")
		}
		// This is the production pure shutdown predicate, not a fabricated receipt.
		return f.local.CheckShutdown() == nil, nil
	})
	// A successful public OutputOnly mark dispatches the actual production
	// RemoveOutput callback for logs. No raw unlink or synthetic Ack is used.
	page, next, err := f.control.Prune(ctx, actor, store.PruneSelection{Before: time.Now().UTC(), Mode: store.OutputOnly}, true, "")
	if err != nil || page.Marked != 1 || next != "" {
		fail("mark real log cleanup: %+v next=%q error=%v", page, next, err)
	}
	wait("positive production log cleanup", func() (bool, error) {
		reservedBytes, reservedFiles, err := f.database.OutputReservations()
		if err != nil {
			return false, err
		}
		_, pending, err := f.database.NextCleanup(0)
		if err != nil {
			return false, err
		}
		usage, err := f.local.OutputUsage(ctx)
		return reservedBytes == 0 && reservedFiles == 0 && !pending && usage.AllocatedBytes == 0 && usage.Files == 0, err
	})
	var manualBefore store.ObligationPage
	if manual {
		page, next, err = f.control.Prune(ctx, actor, store.PruneSelection{Before: time.Now().UTC(), Mode: store.MetadataOnly, AcknowledgeUnverifiedTerminal: true}, true, "")
		if err != nil || page.Marked != 1 || next != "" {
			fail("public metadata transfer: %+v next=%q error=%v", page, next, err)
		}
		manualBefore, err = f.database.ListObligations(actor, 0, 0, 100)
		if err != nil || manualBefore.More || len(manualBefore.Items) != 1 || !manualBefore.Items[0].NeedsManual || manualBefore.Items[0].OutputPending || manualBefore.Items[0].OutputBytes != 0 {
			fail("qualified manual history: %+v error=%v", manualBefore, err)
		}
	}
	before, err := f.database.ArtifactRequiredObjects()
	if err != nil || len(before) != 2 {
		fail("original required input and result: %+v error=%v", before, err)
	}
	owner := f.database.Owner()
	if err := f.closeOriginal(ctx); err != nil {
		fail("positive stopped original handoff: %w", err)
	}
	backup, restored := filepath.Join(path, "backup"), filepath.Join(path, "restored")
	if err := store.BackupCompanion(ctx, directory, objectDirectory, backup); err != nil {
		fail("stopped original backup: %w", err)
	}
	if err := store.RestoreCompanion(ctx, backup, restored); err != nil {
		fail("restore completed workspace: %w", err)
	}
	// Original owners remain closed. A distinct frame owns only the restored copy.
	r := &companionWorkflowFrame{path: path}
	active = r
	t.Cleanup(func() {
		if r.held != nil {
			return
		}
		if err := r.closeOriginal(context.Background()); err != nil {
			r.hold(t, err)
			f.hold(t, err)
		}
	})
	r.database, err = store.Open(restored)
	if err != nil {
		r.hold(t, err)
		fail("open restored Store: %w", err)
	}
	if r.database.Owner() != owner {
		fail("restored installation owner changed")
	}
	after, err := r.database.ArtifactRequiredObjects()
	if err != nil || len(after) != len(before) {
		fail("restored required objects: %+v error=%v", after, err)
	}
	for i := range before {
		if before[i].Reference != after[i].Reference || before[i].Manifest != after[i].Manifest {
			fail("immutable input/result identity changed")
		}
		if before[i].File == after[i].File {
			fail("restored object reused the original live inode")
		}
	}
	restoredInfo, err := r.database.RetentionInfo(actor)
	if err != nil || restoredInfo.Domain.Lineage == info.Domain.Lineage || restoredInfo.LegacyReplayOpen {
		fail("restore request lineage was not freshened: %+v error=%v", restoredInfo, err)
	}
	if _, _, err := r.database.LookupRetained(actor, info.Domain, "companion-completed", task); err != nil {
		var expiry *store.ExpiryError
		if !errors.As(err, &expiry) || expiry.Reason != store.RequestLineageChanged {
			fail("pre-restore domain did not receive lineage refusal: %v", err)
		}
	} else {
		fail("pre-restore request domain accepted")
	}
	if manual {
		got, err := r.database.ListObligations(actor, 0, 0, 100)
		if err != nil || !reflect.DeepEqual(got, manualBefore) {
			fail("manual logical history changed: %+v error=%v", got, err)
		}
	} else {
		got, err := r.database.Get(completed.ID)
		if err != nil || !reflect.DeepEqual(got, completed) {
			fail("completed job history changed: %+v error=%v", got, err)
		}
		gotResult, err := r.database.ArtifactResultFor(actor, completed.ID, completed.CurrentAttemptID())
		if err != nil || !reflect.DeepEqual(gotResult, result) {
			fail("completed result metadata changed: %+v error=%v", gotResult, err)
		}
	}
	r.root, err = os.OpenRoot(filepath.Join(restored, "artifacts"))
	if err != nil {
		fail("open restored root: %w", err)
	}
	r.files, err = artifact.Open(r.root, companionWorkflowIdentity(t, r.root))
	if err != nil {
		fail("open restored Files: %w", err)
	}
	restoredFacts, err := r.files.RootFacts()
	if err != nil || restoredFacts.ID != facts.ID {
		fail("restored root installation ID changed: %+v error=%v", restoredFacts, err)
	}
	for _, object := range after {
		ref := artifact.Ref{ID: object.Reference.ID, SHA256: object.Reference.SHA256, Size: int64(object.Reference.Size)}
		r.reader, err = r.files.OpenObject(ref, artifact.FileID{Device: object.File.Device, Inode: object.File.Inode})
		if err != nil {
			fail("verify actual restored object: %w", err)
		}
		if ref.ID == result.Reference.ID {
			if ref.Size > 64<<10 {
				fail("bounded fixture result unexpectedly exceeds one chunk")
			}
			data := make([]byte, int(ref.Size))
			if n, err := r.reader.ReadAt(data, 0); err != nil || n != len(data) {
				fail("read actual restored result: n=%d error=%v", n, err)
			}
			reader := tar.NewReader(bytes.NewReader(data))
			header, err := reader.Next()
			if err != nil || header.Name != "result.txt" {
				fail("actual restored result header: %+v error=%v", header, err)
			}
			payload, err := io.ReadAll(io.LimitReader(reader, 1024))
			if err != nil || string(payload) != "companion result\n" {
				fail("actual restored result payload: %q error=%v", payload, err)
			}
		}
		if err := r.reader.Close(); err != nil {
			r.hold(t, err)
			fail("original restored Reader Close: %w", err)
		}
		r.reader = nil
	}
	// Destination output geometry is actually probed by the production restore;
	// NewRetained must accept that empty root without a fake copied log inventory.
	p := restoredInfo.Policy
	r.local, err = local.NewRetained(filepath.Join(restored, "logs"), local.RetentionConfig{Owner: owner, OutputUnit: p.OutputUnit, Volume: p.Volume, OutputBytes: p.OutputBytes, OutputDomains: p.OutputDomains, OutputFiles: p.OutputFiles})
	if err != nil {
		fail("accept restored empty output geometry: %w", err)
	}
	if err := r.closeOriginal(ctx); err != nil {
		r.hold(t, err)
		fail("close original restored owners: %w", err)
	}
}

func companionWorkflowIdentity(t *testing.T, root *os.Root) artifact.FileID {
	t.Helper()
	info, err := root.Lstat(".")
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("missing actual root identity")
	}
	return artifact.FileID{Device: uint64(stat.Dev), Inode: uint64(stat.Ino)}
}
