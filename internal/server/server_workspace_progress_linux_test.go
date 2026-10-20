//go:build linux

package server

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
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

// workspaceHelperFileError retains the exact original helper descriptor and
// every actual operation cause when its sole Close did not return success.
// It offers no Close retry or reconstructed owner.
type workspaceHelperFileError struct {
	original *os.File
	cause    error
}

func (e *workspaceHelperFileError) Error() string { return e.cause.Error() }
func (e *workspaceHelperFileError) Unwrap() error { return e.cause }

// This is a real Linux workload, not a loopback-only or synthetic owner test.
// The helper is the exact qualified CLI test binary's existing setup/main test.
// On failure lifetimeFixture retains the SAME original server frame and path.
func TestServerWorkspacePublishesCompletedMain(t *testing.T) {
	helper, want := os.Getenv("SKYGO_NATIVE_WORKSPACE_HELPER"), os.Getenv("SKYGO_NATIVE_WORKSPACE_HELPER_SHA256")
	if helper == "" && want == "" {
		t.Skip("native workload requires explicitly qualified CLI test helper and SHA256")
	}
	f := lifetimeFixture(t)
	fail := func(format string, args ...any) {
		t.Helper()
		err := fmt.Errorf(format, args...)
		f.hold(t, err)
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
		fail("read qualified CLI test helper: %w", &workspaceHelperFileError{original: original, cause: errors.Join(statErr, readErr, closeErr)})
	}
	if err := errors.Join(statErr, readErr); err != nil {
		fail("read qualified CLI test helper: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 || hex.EncodeToString(digest.Sum(nil)) != want {
		fail("CLI test helper executable/hash mismatch")
	}
	t.Logf("original fixture=%s helper=%s SHA256=%s", f.directory, helper, want)
	root := filepath.Join(f.directory, "artifacts")
	if err := os.Mkdir(root, 0700); err != nil {
		fail("create private artifact root: %w", err)
	}
	if err := InitializeArtifacts(f.data, root); err != nil {
		fail("initialize original artifact root: %w", err)
	}
	frame := &serverRun{}
	f.frame = frame
	if err := frame.start(context.Background(), Config{DataDir: f.data, Listen: "127.0.0.1:0", ArtifactDir: root}); err != nil {
		fail("start original server: %w", err)
	}
	actor, err := frame.engine.Authenticate("")
	if err != nil {
		fail("authenticate original local owner: %w", err)
	}
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
	withCall := func(operation func(*control.ArtifactCall) error) error {
		call, err := frame.control.TryArtifactCall()
		if errors.Is(err, control.ErrArtifactBusy) {
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
		return withCall(func(call *control.ArtifactCall) error {
			var err error
			upload, err = frame.control.BeginArtifactUpload(ctx, actor, call, sha, int64(archive.Len()))
			return err
		})
	}, func(err error) bool {
		return errors.Is(err, registrationBusy) || control.IsArtifactBeginNotStarted(err)
	})
	safePart := func(err error) bool {
		return errors.Is(err, registrationBusy) || errors.Is(err, control.ErrArtifactBusy) || errors.Is(err, artifact.ErrFilesBusy)
	}
	retry("write same actual input", func() error {
		return withCall(func(call *control.ArtifactCall) error {
			written, err := frame.control.WriteArtifactChunk(ctx, actor, call, upload.Reference.ID, 0, archive.Bytes())
			if err == nil && written.Acknowledged != uint64(archive.Len()) {
				return fmt.Errorf("actual input acknowledgment %d", written.Acknowledged)
			}
			return err
		})
	}, safePart)
	var input artifact.Ref
	retry("finalize same actual input", func() error {
		return withCall(func(call *control.ArtifactCall) error {
			var err error
			input, err = frame.control.FinalizeArtifactUpload(ctx, actor, call, upload.Reference.ID)
			return err
		})
	}, safePart)
	argv := []string{helper, "-test.run=^TestArtifactWorkload$"}
	task := backend.Task{Argv: argv, Setup: append(append([]string(nil), argv...), "--", "setup"), Workdir: &input, Outputs: []string{"out"}, TimeoutSeconds: 10, Env: map[string]string{"SKYGO_ARTIFACT_WORKLOAD": "workspace", "SKYGO_ARTIFACT_MARKER": filepath.Join(f.directory, "native-marker")}}
	accepted, err := frame.control.SubmitWorkspace(actor, "native-workspace-progress", task)
	if err != nil {
		fail("submit original workspace: %w", err)
	}
	t.Logf("accepted job=%s attempt=%s binding=%s", accepted.ID, accepted.CurrentAttemptID(), accepted.TargetBinding)
	deadline := time.Now().Add(30 * time.Second)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	last := ""
	for {
		current, getErr := frame.store.Get(accepted.ID)
		if getErr != nil {
			fail("get original job: %w", getErr)
		}
		result, resultErr := frame.control.ArtifactResultFor(actor, current.ID, current.CurrentAttemptID())
		if current.Status.State == backend.Succeeded && resultErr == nil && result.State == "available" && result.Reference != nil {
			t.Logf("original successful completion status=%+v result=%+v", current.Status, result)
			return // existing fixture must still perform actual original server shutdown
		}
		var observation local.WorkspaceObservation
		var observationErr error
		readObservation := current.Status.Instance != ""
		if readObservation {
			observation, observationErr = frame.localBackend.WorkspaceObservation(backend.Reference{ID: current.CurrentAttemptID(), Task: backend.CloneTask(current.Task), Binding: current.TargetBinding, Instance: current.Status.Instance})
		}
		// These are copied public snapshots, NOT mutable runFrame/worker fields.
		snapshot := fmt.Sprintf("job=%s status=%+v result=%+v result_error=%T(%v) observation_read=%v phase=%q ready=%v held=%v outcome=%d observation_status=%+v writer_present=%v candidate_present=%v observation_error=%T(%v) physical=%+v", current.ID, current.Status, result, resultErr, resultErr, readObservation, observation.Phase, observation.Ready, observation.Held, observation.Outcome, observation.Status, observation.Writer != nil, observation.Candidate != nil, observationErr, observationErr, frame.localBackend.PhysicalState())
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
		case <-frame.runDone:
			fail("original Control returned: %T(%v); %s", frame.runErr, frame.runErr, snapshot)
		case <-ctx.Done():
			fail("original diagnostic deadline: %w; %s", ctx.Err(), snapshot)
		}
	}
}
