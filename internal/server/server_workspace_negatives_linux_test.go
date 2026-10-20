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
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

// This anchored helper runs only in a child of the SAME server test binary.
// The setup marker publishes actual entry and cwd, not a stopped-owner receipt.
func TestServerWorkspaceNegativeProgram(t *testing.T) {
	if os.Getenv("SKYGO_SERVER_WORKSPACE_NEGATIVE_CHILD") != "1" {
		return
	}
	mark := func(path string, data []byte) {
		if err := os.WriteFile(path+".tmp", data, 0600); err != nil {
			os.Exit(22)
		}
		if err := os.Rename(path+".tmp", path); err != nil {
			os.Exit(23)
		}
	}
	mode := os.Args[len(os.Args)-1]
	if data, err := os.ReadFile("seed.txt"); err != nil || string(data) != "seed\n" {
		os.Exit(20)
	}
	switch mode {
	case "setup":
		cwd, err := os.Getwd()
		if err != nil {
			os.Exit(21)
		}
		mark(os.Getenv("SKYGO_NEGATIVE_SETUP"), []byte(cwd))
		// The parent cancels only after observing the marker. Even a failed
		// parent leaves a finite owned program, not an unbounded helper.
		time.Sleep(15 * time.Second)
		os.Exit(0)
	case "main":
		mark(os.Getenv("SKYGO_NEGATIVE_MAIN"), []byte("main entered"))
	case "symlink":
		if err := os.Symlink(os.Getenv("SKYGO_NEGATIVE_OUTSIDE"), "out"); err != nil {
			os.Exit(25)
		}
		mark(os.Getenv("SKYGO_NEGATIVE_MAIN"), []byte("symlink created"))
	default:
		os.Exit(27)
	}
	os.Exit(0)
}

type workspaceNegativeServer struct {
	fixture  *lifetimeServerFixture
	frame    *serverRun
	client   skyv1connect.ControlServiceClient
	ctx      context.Context
	root     string
	input    *v1.ArtifactRef
	baseline store.ArtifactReservations
}

func (s *workspaceNegativeServer) fail(t *testing.T, format string, args ...any) {
	t.Helper()
	err := fmt.Errorf(format, args...)
	s.fixture.hold(t, err)
	t.Fatal(err)
}

func newWorkspaceNegativeServer(t *testing.T) *workspaceNegativeServer {
	t.Helper()
	f := lifetimeFixture(t)
	s := &workspaceNegativeServer{fixture: f, frame: &serverRun{}, root: filepath.Join(f.directory, "artifacts")}
	f.frame = s.frame
	if err := os.Mkdir(s.root, 0700); err != nil {
		s.fail(t, "create original artifact root: %w", err)
	}
	if err := InitializeArtifacts(f.data, s.root); err != nil {
		s.fail(t, "initialize original artifacts: %w", err)
	}
	if err := s.frame.start(context.Background(), Config{DataDir: f.data, Listen: "127.0.0.1:0", ArtifactDir: s.root}); err != nil {
		s.fail(t, "start original server: %w", err)
	}
	if !s.frame.localBackend.SupportsWorkspace() {
		t.Skip("actual Linux workspace process-ownership prerequisites unavailable")
	}
	f.client.Timeout = 5 * time.Second
	f.url = "http://" + s.frame.listener.Addr().String()
	s.client = skyv1connect.NewControlServiceClient(f.client, f.url)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	s.ctx = ctx
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	if err := writer.WriteHeader(&tar.Header{Name: "seed.txt", Mode: 0600, Size: 5, Format: tar.FormatUSTAR}); err != nil {
		s.fail(t, "write actual input header: %w", err)
	}
	if _, err := writer.Write([]byte("seed\n")); err != nil {
		s.fail(t, "write actual input: %w", err)
	}
	if err := writer.Close(); err != nil {
		s.fail(t, "close actual input archive: %w", err)
	}
	digest := sha256.Sum256(archive.Bytes())
	var begun *connect.Response[v1.BeginArtifactUploadResponse]
	s.retry(t, "begin input", func() error {
		var err error
		begun, err = s.client.BeginArtifactUpload(ctx, connect.NewRequest(&v1.BeginArtifactUploadRequest{Sha256: hex.EncodeToString(digest[:]), Size: int64(archive.Len())}))
		return err
	}, true)
	if begun == nil || begun.Msg.Upload == nil {
		s.fail(t, "begin input returned no original upload")
	}
	id := begun.Msg.Upload.Id
	s.retry(t, "write same input", func() error {
		written, err := s.client.WriteArtifactChunk(ctx, connect.NewRequest(&v1.WriteArtifactChunkRequest{Id: id, Data: archive.Bytes()}))
		if err == nil && (written.Msg.Upload == nil || written.Msg.Upload.Acknowledged != int64(archive.Len())) {
			return errors.New("input acknowledgment differs from exact archive size")
		}
		return err
	}, false)
	s.retry(t, "finalize same input", func() error {
		sealed, err := s.client.FinalizeArtifactUpload(ctx, connect.NewRequest(&v1.FinalizeArtifactUploadRequest{Id: id}))
		if err == nil {
			s.input = sealed.Msg.Reference
		}
		return err
	}, false)
	if s.input == nil || s.input.Id != id || s.input.Sha256 != hex.EncodeToString(digest[:]) || s.input.Size != int64(archive.Len()) {
		s.fail(t, "finalized input differs from original immutable bytes")
	}
	var err error
	s.baseline, err = s.frame.store.ArtifactReservations()
	if err != nil {
		s.fail(t, "read original input reservations: %w", err)
	}
	return s
}

// Begin repeats ONLY an explicit pre-call refusal. Chunk/finalize retry the
// same original identity and bytes; transport uncertainty never starts a Begin.
func (s *workspaceNegativeServer) retry(t *testing.T, stage string, call func() error, begin bool) {
	t.Helper()
	for {
		err := call()
		if err == nil {
			return
		}
		var rpcErr *connect.Error
		safe := connect.CodeOf(err) == connect.CodeResourceExhausted
		if begin {
			safe = errors.As(err, &rpcErr) && rpcErr.Meta().Get("Skygo-Artifact-Begin-Not-Started") == "v1"
		}
		if !safe {
			s.fail(t, "%s: %w", stage, err)
		}
		s.pause(t, stage)
	}
}

func (s *workspaceNegativeServer) pause(t *testing.T, stage string) {
	t.Helper()
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-s.frame.runDone:
		s.fail(t, "%s original Control returned: %v", stage, s.frame.runErr)
	case <-s.ctx.Done():
		s.fail(t, "%s: %w", stage, s.ctx.Err())
	}
}

func (s *workspaceNegativeServer) task(t *testing.T, mode string) *v1.Task {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		s.fail(t, "locate SAME server test executable: %w", err)
	}
	return &v1.Task{Argv: []string{exe, "-test.run=^TestServerWorkspaceNegativeProgram$", "--", mode}, Workdir: s.input, Outputs: []string{"out"}, TimeoutSeconds: 25, Env: map[string]string{
		"SKYGO_SERVER_WORKSPACE_NEGATIVE_CHILD": "1",
		"SKYGO_NEGATIVE_SETUP":                  filepath.Join(s.fixture.directory, "setup-entered"),
		"SKYGO_NEGATIVE_MAIN":                   filepath.Join(s.fixture.directory, "main-entered"),
		"SKYGO_NEGATIVE_OUTSIDE":                filepath.Join(s.fixture.directory, "outside-sentinel"),
	}}
}

func (s *workspaceNegativeServer) submit(t *testing.T, id string, task *v1.Task) *v1.Job {
	t.Helper()
	var job *v1.Job
	s.retry(t, "submit exact original request", func() error {
		accepted, err := s.client.SubmitWorkspaceJob(s.ctx, connect.NewRequest(&v1.SubmitWorkspaceJobRequest{RequestId: id, Task: task}))
		if err == nil {
			job = accepted.Msg.Job
		}
		return err
	}, false)
	if job == nil || job.Id == "" {
		s.fail(t, "accepted request has no job")
	}
	return job
}

func (s *workspaceNegativeServer) marker(t *testing.T, name string) []byte {
	t.Helper()
	for {
		data, err := os.ReadFile(filepath.Join(s.fixture.directory, name))
		if err == nil {
			return data
		}
		if !errors.Is(err, os.ErrNotExist) {
			s.fail(t, "read original marker %s: %w", name, err)
		}
		s.pause(t, "wait actual child marker "+name)
	}
}

func (s *workspaceNegativeServer) absent(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		s.fail(t, "unexpected path %s: %v", path, err)
	}
}

// This requires terminal integration AND exact released workspace/result
// allowances. An absent path alone never grants original owner retirement.
func (s *workspaceNegativeServer) completed(t *testing.T, id string, state backend.State) {
	t.Helper()
	for {
		job, err := s.frame.store.Get(id)
		if err != nil {
			s.fail(t, "read accepted original job: %w", err)
		}
		current, err := s.client.GetJob(s.ctx, connect.NewRequest(&v1.GetJobRequest{Id: id}))
		if err != nil {
			s.fail(t, "get original result over Connect: %w", err)
		}
		cohort, err := s.frame.store.ArtifactReservations()
		if err != nil {
			s.fail(t, "read actual cleanup allowances: %w", err)
		}
		if job.Status.State.Terminal() && job.Status.State != state {
			s.fail(t, "unexpected original outcome: %+v", job.Status)
		}
		if job.Status.State == state && current.Msg.Job != nil && current.Msg.Job.Result != nil && current.Msg.Job.Result.State == v1.ArtifactResultState_ARTIFACT_RESULT_STATE_ABSENT && current.Msg.Job.Result.Reference == nil && reflect.DeepEqual(cohort, s.baseline) {
			work, found, err := s.frame.store.NextArtifactCleanup("")
			if err != nil || found {
				s.fail(t, "workspace cleanup not actually credited: %+v %v %v", work, found, err)
			}
			s.absent(t, filepath.Join(s.root, "workspace-"+job.CurrentAttemptID()))
			return
		}
		s.pause(t, "wait actual terminal integration and cleanup credit")
	}
}

func (s *workspaceNegativeServer) finish(t *testing.T) {
	t.Helper()
	// Actual original shutdown joins Control, native/workspace owners, HTTP,
	// Files and root before Store. Failure retains the exact fixture/frame.
	if err := s.frame.shutdown(nil); err != nil {
		s.fail(t, "join original server and all dependencies: %w", err)
	}
	s.fixture.frame = nil
}

func TestServerWorkspaceConnectCancelDuringSetup(t *testing.T) {
	s := newWorkspaceNegativeServer(t)
	task := s.task(t, "main")
	task.Setup = append(append([]string(nil), task.Argv[:len(task.Argv)-1]...), "setup")
	job := s.submit(t, "cancel-during-setup", task)
	cwd := string(s.marker(t, "setup-entered"))
	original, err := s.frame.store.Get(job.Id)
	if err != nil || cwd != filepath.Join(s.root, "workspace-"+original.CurrentAttemptID()) {
		s.fail(t, "setup did not enter original private root: %q %+v %v", cwd, original, err)
	}
	for original.Status.Instance == "" {
		s.pause(t, "wait original setup Instance integration")
		original, err = s.frame.store.Get(job.Id)
		if err != nil || original.Status.State.Terminal() {
			s.fail(t, "original setup ended before cancellation: %+v %v", original, err)
		}
	}
	observation, err := s.frame.localBackend.WorkspaceObservation(backend.Reference{ID: original.CurrentAttemptID(), Task: backend.CloneTask(original.Task), Binding: original.TargetBinding, Instance: original.Status.Instance})
	if err != nil || observation.Phase != "setup" || observation.Ready || observation.Held {
		s.fail(t, "original setup is not active at cancellation barrier: %+v %v", observation, err)
	}
	s.absent(t, filepath.Join(s.fixture.directory, "main-entered"))
	if _, err := s.client.CancelJob(s.ctx, connect.NewRequest(&v1.CancelJobRequest{Id: job.Id})); err != nil {
		s.fail(t, "Connect cancel DURING actual setup: %w", err)
	}
	s.completed(t, job.Id, backend.Canceled)
	s.finish(t)
	s.absent(t, filepath.Join(s.fixture.directory, "main-entered"))
}

func TestServerWorkspaceConnectRefusesSymlinkOutput(t *testing.T) {
	s := newWorkspaceNegativeServer(t)
	outside := filepath.Join(s.fixture.directory, "outside-sentinel")
	payload := []byte("outside host sentinel must never become result bytes")
	if err := os.WriteFile(outside, payload, 0600); err != nil {
		s.fail(t, "write outside sentinel: %w", err)
	}
	job := s.submit(t, "malicious-output-symlink", s.task(t, "symlink"))
	if string(s.marker(t, "main-entered")) != "symlink created" {
		s.fail(t, "actual malicious workload did not create output symlink")
	}
	s.completed(t, job.Id, backend.Failed)
	failed, err := s.frame.store.Get(job.Id)
	if err != nil || failed.Status.ExitCode == nil || *failed.Status.ExitCode != 0 || !strings.HasPrefix(failed.Status.Reason, "collect workspace: ") {
		s.fail(t, "symlink rejection was not actual collection after task exit zero: %+v %v", failed.Status, err)
	}
	s.finish(t)
	if got, err := os.ReadFile(outside); err != nil || !bytes.Equal(got, payload) {
		s.fail(t, "outside sentinel changed: %q %v", got, err)
	}
}

func TestServerWorkspaceConnectRefusesNewMissingInput(t *testing.T) {
	s := newWorkspaceNegativeServer(t)
	task := s.task(t, "main")
	// New request identity: prior acceptance replay is a separate contract and
	// must continue to work when its original client input is unavailable.
	task.Workdir = &v1.ArtifactRef{Id: strings.Repeat("f", 32), Sha256: s.input.Sha256, Size: s.input.Size}
	s.refused(t, "new-missing-input", task, connect.CodeNotFound)
	s.finish(t)
	s.absent(t, filepath.Join(s.fixture.directory, "main-entered"))
}

func TestServerWorkspaceConnectRefusesEscapingOutputDeclaration(t *testing.T) {
	s := newWorkspaceNegativeServer(t)
	task := s.task(t, "main")
	task.Outputs = []string{"../outside-sentinel"}
	s.refused(t, "new-escaping-output", task, connect.CodeInvalidArgument)
	s.finish(t)
	s.absent(t, filepath.Join(s.fixture.directory, "main-entered"))
}

func (s *workspaceNegativeServer) refused(t *testing.T, id string, task *v1.Task, code connect.Code) {
	t.Helper()
	before, err := s.frame.store.LastSequence()
	if err != nil {
		s.fail(t, "read sequence before NEW refusal: %w", err)
	}
	var response *connect.Response[v1.SubmitWorkspaceJobResponse]
	var refusal error
	s.retry(t, "new request admission", func() error {
		response, refusal = s.client.SubmitWorkspaceJob(s.ctx, connect.NewRequest(&v1.SubmitWorkspaceJobRequest{RequestId: id, Task: task}))
		if connect.CodeOf(refusal) == code {
			return nil
		}
		if refusal == nil {
			return errors.New("new invalid request was accepted")
		}
		return refusal
	}, false)
	if response != nil || connect.CodeOf(refusal) != code {
		s.fail(t, "NEW refusal=%+v %v; want %v", response, refusal, code)
	}
	after, err := s.frame.store.LastSequence()
	if err != nil || before != 0 || after != before {
		s.fail(t, "NEW refusal mutated acceptance sequence: %d -> %d %v", before, after, err)
	}
	jobs, err := s.frame.store.List(0, ^uint64(0), 10)
	if err != nil || len(jobs) != 0 {
		s.fail(t, "NEW refusal retained an accepted job: %+v %v", jobs, err)
	}
	cohort, err := s.frame.store.ArtifactReservations()
	if err != nil || !reflect.DeepEqual(cohort, s.baseline) {
		s.fail(t, "NEW refusal mutated actual artifact reservations: %+v %v", cohort, err)
	}
	s.absent(t, filepath.Join(s.fixture.directory, "main-entered"))
}
