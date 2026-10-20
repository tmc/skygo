//go:build linux

package control

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

// workspaceProgressHelperError retains the exact original helper descriptor and
// every actual operation cause when its sole Close did not return success.
// It offers no Close retry or reconstructed owner.
type workspaceProgressHelperError struct {
	original *os.File
	cause    error
}

func (e *workspaceProgressHelperError) Error() string { return e.cause.Error() }
func (e *workspaceProgressHelperError) Unwrap() error { return e.cause }

// This is a real Linux workload, not a loopback-only or synthetic owner test.
// The helper is the exact qualified CLI test binary's existing setup/main test.
// The existing persistent fixture retains the SAME Control/Local/Store/Files on failure.
// No HTTP server or server shutdown ordering is qualified by this test.
func TestControlWorkspacePublishesCompletedMain(t *testing.T) {
	helper, want := os.Getenv("SKYGO_NATIVE_WORKSPACE_HELPER"), os.Getenv("SKYGO_NATIVE_WORKSPACE_HELPER_SHA256")
	if helper == "" && want == "" {
		t.Skip("native workload requires explicitly qualified CLI test helper and SHA256")
	}
	f := newRetirementFixtureFrame(t)
	fail := func(format string, args ...any) {
		t.Helper()
		err := fmt.Errorf(format, args...)
		f.held = err
		t.Fatal(err)
	}
	if !filepath.IsAbs(helper) || len(want) != 64 {
		fail("qualified CLI test helper path and SHA256 required")
	}
	original, err := os.Open(helper)
	if err != nil {
		fail("open qualified CLI test helper: %w", err)
	}
	info, statErr := original.Stat()
	digest := sha256.New()
	_, readErr := io.Copy(digest, original)
	closeErr := original.Close()
	if closeErr != nil {
		fail("read qualified CLI test helper: %w", &workspaceProgressHelperError{original: original, cause: errors.Join(statErr, readErr, closeErr)})
	}
	if err := errors.Join(statErr, readErr); err != nil {
		fail("read qualified CLI test helper: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 || hex.EncodeToString(digest.Sum(nil)) != want {
		fail("CLI test helper executable/hash mismatch")
	}
	t.Logf("original fixture=%s helper=%s SHA256=%s", f.path, helper, want)
	name := filepath.Join(f.path, "artifacts")
	directory := filepath.Join(f.path, "store")
	localDirectory := filepath.Join(f.path, "local")
	for _, name := range []string{name, directory, localDirectory} {
		if err := os.Mkdir(name, 0700); err != nil {
			fail("create private fixture directory: %w", err)
		}
	}
	f.root, err = os.OpenRoot(name)
	if err != nil {
		fail("acquire original fixture: %w", err)
	}
	root := f.root
	info, err = root.Lstat(".")
	if err != nil {
		fail("acquire original fixture: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		fail("missing actual root identity")
	}
	identity := artifact.FileID{Device: uint64(stat.Dev), Inode: uint64(stat.Ino)}
	facts, err := artifact.Initialize(root, identity, strings.Repeat("a", 32))
	if err != nil {
		fail("acquire original fixture: %w", err)
	}
	f.files, err = artifact.Open(root, identity)
	if err != nil {
		fail("acquire original fixture: %w", err)
	}
	files := f.files
	f.store, err = store.Open(directory)
	if err != nil {
		fail("acquire original fixture: %w", err)
	}
	s := f.store
	if _, err = s.InstallArtifacts(store.ArtifactInstallation{Enabled: true, ID: facts.ID, Unit: facts.Unit, Volume: facts.Volume}); err != nil {
		fail("install original Store artifact facts: %w", err)
	}
	f.local, err = local.New(localDirectory)
	if err != nil {
		fail("acquire original fixture: %w", err)
	}
	b := f.local
	binding, err := backend.LocalBinding(s.Owner())
	if err != nil {
		fail("acquire original fixture: %w", err)
	}
	target := backend.Target{Name: "local", Binding: binding, Backend: b}
	producer, err := local.CaptureServerLifetime()
	if err != nil {
		fail("capture original producer: %w", err)
	}
	f.control, err = NewWithConfig(s, []backend.Target{target}, testEngine, Config{ServerLifetime: producer, Artifacts: files, LocalBackend: b, LocalWorkspace: &LocalWorkspaceConfig{Target: target.Name, Binding: binding, Backend: b, Roots: root}})
	if err != nil {
		fail("configure original Control: %w", err)
	}
	c := f.control
	if !c.WorkspaceTargetEnabled(target.Name, binding) {
		fail("original Local workspace prerequisites unavailable")
	}
	runCtx, runCancel := context.WithCancel(context.Background())
	f.cancel = runCancel
	runDone := make(chan struct{})
	var runErr error
	go func() { defer close(runDone); runErr = c.Run(runCtx) }()
	actor := testActor()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	// Actual bounded input archive; no Store rows, grants or physical cache seeded.
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	if err := writer.WriteHeader(&tar.Header{Name: "seed.txt", Mode: 0600, Size: 5, Format: tar.FormatUSTAR}); err != nil {
		fail("write input header: %w", err)
	}
	if _, err := writer.Write([]byte("seed\n")); err != nil {
		fail("write input bytes: %w", err)
	}
	if err := writer.Close(); err != nil {
		fail("close input archive: %w", err)
	}
	sha, err := artifact.Validate(bytes.NewReader(archive.Bytes()), int64(archive.Len()))
	if err != nil {
		fail("validate input archive: %w", err)
	}
	// This private sentinel is ONLY a returned pre-call registration refusal.
	// It never reclassifies a possibly started Begin as safe to repeat.
	registrationBusy := errors.New("diagnostic pre-call registration busy")
	withCall := func(operation func(*ArtifactCall) error) error {
		call, err := c.TryArtifactCall()
		if errors.Is(err, ErrArtifactBusy) {
			return registrationBusy
		}
		if err != nil {
			return err
		}
		defer call.Done() // direct caller: original synchronous function has returned
		return operation(call)
	}
	retry := func(stage string, operation func() error, safe func(error) bool) {
		t.Helper()
		for n := 0; ; n++ {
			err := operation()
			if err == nil {
				return
			}
			if n == 20 || !safe(err) {
				fail("%s: %w", stage, err)
			}
			timer := time.NewTimer(250 * time.Millisecond)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				fail("%s original wait: %w", stage, ctx.Err())
			}
		}
	}
	var upload store.ArtifactUpload
	retry("begin actual input", func() error {
		return withCall(func(call *ArtifactCall) error {
			var err error
			upload, err = c.BeginArtifactUpload(ctx, actor, call, sha, int64(archive.Len()))
			return err
		})
	}, func(err error) bool {
		return errors.Is(err, registrationBusy) || IsArtifactBeginNotStarted(err)
	})
	safePart := func(err error) bool {
		return errors.Is(err, registrationBusy) || errors.Is(err, ErrArtifactBusy) || errors.Is(err, artifact.ErrFilesBusy)
	}
	retry("write same actual input", func() error {
		return withCall(func(call *ArtifactCall) error {
			written, err := c.WriteArtifactChunk(ctx, actor, call, upload.Reference.ID, 0, archive.Bytes())
			if err == nil && written.Acknowledged != uint64(archive.Len()) {
				return fmt.Errorf("actual input acknowledgment %d", written.Acknowledged)
			}
			return err
		})
	}, safePart)
	var input artifact.Ref
	retry("finalize same actual input", func() error {
		return withCall(func(call *ArtifactCall) error {
			var err error
			input, err = c.FinalizeArtifactUpload(ctx, actor, call, upload.Reference.ID)
			return err
		})
	}, safePart)
	argv := []string{helper, "-test.run=^TestArtifactWorkload$"}
	task := backend.Task{Argv: argv, Setup: append(append([]string(nil), argv...), "--", "setup"), Workdir: &input, Outputs: []string{"out"}, TimeoutSeconds: 10, Env: map[string]string{"SKYGO_ARTIFACT_WORKLOAD": "workspace", "SKYGO_ARTIFACT_MARKER": filepath.Join(f.path, "native-marker")}}
	accepted, err := c.SubmitWorkspace(actor, "native-workspace-progress", task)
	if err != nil {
		fail("submit original workspace: %w", err)
	}
	t.Logf("accepted job=%s attempt=%s binding=%s", accepted.ID, accepted.CurrentAttemptID(), accepted.TargetBinding)
	deadline := time.Now().Add(30 * time.Second)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	last := ""
	for {
		current, getErr := s.Get(accepted.ID)
		if getErr != nil {
			fail("get original job: %w", getErr)
		}
		result, resultErr := c.ArtifactResultFor(actor, current.ID, current.CurrentAttemptID())
		if current.Status.State == backend.Succeeded && resultErr == nil && result.State == "available" && result.Reference != nil {
			t.Logf("original successful completion status=%+v result=%+v", current.Status, result)
			// Preserve the original normal shutdown ordering before fixture closure.
			stop, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer stopCancel()
			if err := c.BeginShutdown(); err != nil {
				fail("begin original shutdown: %w", err)
			}
			if err := c.WaitShutdownPrepared(stop); err != nil {
				fail("prepare original shutdown: %w", err)
			}
			if err := c.DrainArtifacts(stop); err != nil {
				fail("drain original artifacts: %w", err)
			}
			f.artifactsDrained = true
			runCancel()
			select {
			case <-runDone:
			case <-stop.Done():
				fail("original Run remains held: %w", stop.Err())
			}
			if err := c.Join(stop); err != nil {
				fail("original workers remain held: %w", err)
			}
			if runErr != nil && !errors.Is(runErr, context.Canceled) {
				fail("original Run: %w", runErr)
			}
			return
		}
		var observation local.WorkspaceObservation
		var observationErr error
		readObservation := current.Status.Instance != ""
		if readObservation {
			observation, observationErr = b.WorkspaceObservation(backend.Reference{ID: current.CurrentAttemptID(), Task: backend.CloneTask(current.Task), Binding: current.TargetBinding, Instance: current.Status.Instance})
		}
		// These are copied public snapshots, NOT mutable runFrame/worker fields.
		snapshot := fmt.Sprintf("job=%s status=%+v result=%+v result_error=%T(%v) observation_read=%v phase=%q ready=%v held=%v outcome=%d observation_status=%+v writer_present=%v candidate_present=%v observation_error=%T(%v) physical=%+v", current.ID, current.Status, result, resultErr, resultErr, readObservation, observation.Phase, observation.Ready, observation.Held, observation.Outcome, observation.Status, observation.Writer != nil, observation.Candidate != nil, observationErr, observationErr, b.PhysicalState())
		snapshot += " admission=" + workspaceProgressAdmission(c)
		if snapshot != last {
			t.Log(snapshot)
			last = snapshot
		}
		if current.Status.State.Terminal() {
			fail("original workspace terminal without successful result: %s", snapshot)
		}
		if !time.Now().Before(deadline) {
			fail("original completed-main publication did not finish: %s", snapshot)
		}
		select {
		case <-tick.C:
		case <-runDone:
			fail("original Control returned: %T(%v); %s", runErr, runErr, snapshot)
		case <-ctx.Done():
			fail("original diagnostic deadline: %w; %s", ctx.Err(), snapshot)
		}
	}
}

// Each snapshot uses the existing owner lock separately. These are observations,
// not a joint atomic decision or a license to enter an operation. No mutable
// Run/maintenance request/result pointer is inspected concurrently.
func workspaceProgressAdmission(c *Control) string {
	s := c.artifacts
	s.mu.Lock()
	owners := fmt.Sprintf("calls=%d io=%d closing=%v physical_pending=%v physical_registered=%v physical_held=%v", len(s.calls), len(s.workspaceIO), s.closing, s.physicalWorkPending, s.physicalFrameRegistered, s.physicalHeld)
	s.mu.Unlock()
	p := &s.physical
	p.mu.Lock()
	value := func(v *uint64) string {
		if v == nil {
			return "nil"
		}
		return fmt.Sprint(*v)
	}
	points := fmt.Sprintf("db=%s db_at=%s db_free=%s db_free_at=%s artifact_free=%s artifact_free_at=%s", value(p.points.databaseBytes), p.points.databaseAt, value(p.points.databaseFree), p.points.databaseFreeAt, value(p.points.artifactFree), p.points.artifactFreeAt)
	cache := "nil"
	if o := p.cached; o != nil {
		cache = fmt.Sprintf("status=%v allocated=%s inodes=%s prepaid_bytes=%d prepaid_files=%d start=%s end=%s conditions=%v", o.sample.Status, value(o.sample.AllocatedBytes), value(o.sample.Inodes), o.cohort.allocated, o.cohort.files, o.sample.StartedAt, o.sample.EndedAt, o.sample.Conditions)
	}
	frame := "nil"
	if f := p.frame; f != nil {
		frame = fmt.Sprintf("serial=%d begun=%s invalidated=%v uncertain=%v", f.serial, f.begun, f.invalidated, f.uncertain)
	}
	reason := physicalGrowthCondition(p, time.Now())
	physical := fmt.Sprintf("reason=%q pending=%v sample=%v close=%v points={%s} cache={%s} frame={%s}", reason, p.pending, p.wantSample, p.wantClose, points, cache, frame)
	p.mu.Unlock()
	reservations, err := c.store.ArtifactReservations()
	return fmt.Sprintf("{%s %s reservations=%+v reservations_error=%v}", owners, physical, reservations, err)
}
