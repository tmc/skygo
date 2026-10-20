//go:build linux

package server

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
	"google.golang.org/protobuf/proto"
)

// This finite original child creates a real published result, using the SAME
// test executable. Neither metadata nor writer/cleanup receipts are seeded.
func TestArtifactRequiredPreflightProgram(t *testing.T) {
	if os.Getenv("SKYGO_PREFLIGHT_CHILD") != "1" {
		return
	}
	data, err := os.ReadFile("seed.txt")
	if err != nil || string(data) != "seed\n" {
		os.Exit(30)
	}
	if err := os.Mkdir("out", 0700); err != nil {
		os.Exit(31)
	}
	if err := os.WriteFile("out/result", data, 0600); err != nil {
		os.Exit(32)
	}
	if err := os.WriteFile(os.Getenv("SKYGO_NEGATIVE_MAIN"), []byte("original task ran"), 0600); err != nil {
		os.Exit(33)
	}
	os.Exit(0)
}

// Required inputs AND results come from a real Connect upload/workspace job.
// Only after original cleanup and server shutdown do the private fault vectors
// remove or corrupt a file. Startup must refuse both owned and borrowed routes.
func TestArtifactRequiredStartup(t *testing.T) {
	for _, borrowed := range []bool{false, true} {
		route := "owned"
		if borrowed {
			route = "borrowed"
		}
		for _, fault := range []string{"valid", "input_missing", "input_corrupt", "result_missing", "result_corrupt"} {
			t.Run(route+"/"+fault, func(t *testing.T) {
				s, job, identities := requiredArtifactJob(t)
				if fault != "valid" {
					id := s.input.Id
					if fault == "result_missing" || fault == "result_corrupt" {
						id = job.Result.Reference.Id
					}
					path := filepath.Join(s.root, id+".tar")
					if fault == "input_missing" || fault == "result_missing" {
						if err := os.Remove(path); err != nil {
							s.fail(t, "remove stopped private fault object: %w", err)
						}
					} else {
						corruptRequiredArtifact(t, s, path)
					}
				}
				cfg := Config{DataDir: s.fixture.data, Listen: "127.0.0.1:0", ArtifactDir: s.root}
				if borrowed {
					cfg.ArtifactDir = ""
					cfg.Artifacts = borrowRequiredArtifacts(t, s)
				}
				frame := &serverRun{}
				s.fixture.frame = frame
				startErr := frame.start(context.Background(), cfg)
				if fault != "valid" {
					if startErr == nil {
						s.fail(t, "startup accepted %s required artifact", fault)
					}
					// These actual producer fields precede native Start, Serve,
					// listening and dispatch; a missing marker alone is not proof.
					if frame.localBackend != nil || frame.engine != nil || frame.control != nil || frame.listener != nil || frame.runDone != nil || frame.serveDone != nil {
						s.fail(t, "required-object refusal followed startup/dispatch: %w", startErr)
					}
					var held *RunError
					if errors.As(startErr, &held) || errors.Is(startErr, artifact.ErrFileCloseUncertain) {
						s.fail(t, "original preflight owner remains unsettled: %w", startErr)
					}
					// Original ordinary failure must positively close its own
					// resources. Borrowed Files/root remain caller-owned below.
					if closed := frame.closeStartup(startErr); closed != startErr {
						s.fail(t, "startup failure lost original cause or closure: %w", closed)
					}
					s.fixture.frame = nil
					if borrowed {
						if _, err := cfg.Artifacts.RootFacts(); err != nil {
							s.fail(t, "refused startup closed borrowed Files: %w", err)
						}
					}
				} else {
					if startErr != nil {
						s.fail(t, "valid original input/result refused: %w", startErr)
					}
					if borrowed && frame.localBackend.SupportsWorkspace() {
						s.fail(t, "borrowed transfer Files gained workspace authority")
					}
					current, err := frame.store.ArtifactRequiredObjects()
					if err != nil || len(current) != len(identities) {
						s.fail(t, "required identities changed after valid startup: %+v %v", current, err)
					}
					for i := range current {
						if current[i] != identities[i] {
							s.fail(t, "startup rewrote required original identity: %+v", current[i])
						}
					}
					client := skyv1connect.NewControlServiceClient(s.fixture.client, "http://"+frame.listener.Addr().String())
					got, err := client.GetJob(s.ctx, connect.NewRequest(&v1.GetJobRequest{Id: job.Id}))
					if err != nil || got == nil || got.Msg == nil || got.Msg.Job == nil || got.Msg.Job.State != v1.JobState_JOB_STATE_SUCCEEDED || !proto.Equal(got.Msg.Job.Result, job.Result) {
						s.fail(t, "actual accepted result unavailable after preflight: %+v %v", got, err)
					}
					if err := frame.shutdown(nil); err != nil {
						s.fail(t, "join original restarted server: %w", err)
					}
					s.fixture.frame = nil
				}
				s.absent(t, filepath.Join(s.fixture.directory, "main-entered"))
			})
		}
	}
}

func requiredArtifactJob(t *testing.T) (*workspaceNegativeServer, *v1.Job, []store.ArtifactObjectIdentity) {
	t.Helper()
	s := newWorkspaceNegativeServer(t)
	task := s.task(t, "main")
	exe, err := os.Executable()
	if err != nil {
		s.fail(t, "locate SAME native test executable: %w", err)
	}
	task.Argv = []string{exe, "-test.run=^TestArtifactRequiredPreflightProgram$"}
	task.Env["SKYGO_PREFLIGHT_CHILD"] = "1"
	accepted := s.submit(t, "required-input-and-result", task)
	var job *v1.Job
	for {
		current, err := s.client.GetJob(s.ctx, connect.NewRequest(&v1.GetJobRequest{Id: accepted.Id}))
		if err != nil {
			s.fail(t, "read actual original result: %w", err)
		}
		if current == nil || current.Msg == nil || current.Msg.Job == nil {
			s.fail(t, "original result response contains no accepted job")
		}
		job = current.Msg.Job
		stored, err := s.frame.store.Get(accepted.Id)
		if err != nil {
			s.fail(t, "read actual original completion: %w", err)
		}
		if stored.Status.State.Terminal() && stored.Status.State != backend.Succeeded {
			s.fail(t, "original producer failed: %+v", stored.Status)
		}
		cohort, err := s.frame.store.ArtifactReservations()
		if err != nil {
			s.fail(t, "read real workspace retirement: %w", err)
		}
		workspace := false
		for _, subject := range cohort.Subjects {
			workspace = workspace || subject.Kind == "workspace"
		}
		if job != nil && job.State == v1.JobState_JOB_STATE_SUCCEEDED && job.Result != nil && job.Result.State == v1.ArtifactResultState_ARTIFACT_RESULT_STATE_AVAILABLE && job.Result.Reference != nil && !workspace {
			_, found, err := s.frame.store.NextArtifactCleanup("")
			if err != nil {
				s.fail(t, "read actual pending cleanup: %w", err)
			}
			if !found {
				break
			}
		}
		s.pause(t, "wait original publication and Store cleanup credit")
	}
	objects, err := s.frame.store.ArtifactRequiredObjects()
	if err != nil || len(objects) != 2 {
		s.fail(t, "real input/result required inventory: %+v %v", objects, err)
	}
	seenInput, seenResult := false, false
	for _, object := range objects {
		seenInput = seenInput || object.Reference.ID == s.input.Id
		seenResult = seenResult || object.Reference.ID == job.Result.Reference.Id
	}
	if !seenInput || !seenResult {
		s.fail(t, "required inventory omits actual input or published result: %+v", objects)
	}
	s.marker(t, "main-entered")
	s.finish(t) // positive original native/Control/HTTP/Files/root/Store joins FIRST
	if err := os.Remove(filepath.Join(s.fixture.directory, "main-entered")); err != nil {
		s.fail(t, "reset original completed marker before startup check: %w", err)
	}
	return s, job, objects
}

func borrowRequiredArtifacts(t *testing.T, s *workspaceNegativeServer) *artifact.Files {
	t.Helper()
	root, err := os.OpenRoot(s.root)
	if err != nil {
		s.fail(t, "open original borrowed root: %w", err)
	}
	s.fixture.borrowedRoot = root
	info, err := root.Lstat(".")
	if err != nil {
		s.fail(t, "read original borrowed root: %w", err)
	}
	id, err := artifactInitializerIdentity(info)
	if err != nil {
		s.fail(t, "identify original borrowed root: %w", err)
	}
	files, err := artifact.Open(root, id)
	if files != nil {
		s.fixture.borrowedFiles = files
	}
	if err != nil {
		s.fail(t, "open actual borrowed Files: %w", err)
	}
	return files
}

func corruptRequiredArtifact(t *testing.T, s *workspaceNegativeServer, path string) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		s.fail(t, "open stopped private corruption vector: %w", err)
	}
	_, writeErr := file.WriteAt([]byte("X"), 512)
	syncErr := file.Sync()
	closeErr := file.Close()
	if closeErr != nil {
		s.fail(t, "original corruption-vector Close: %w", &workspaceHelperFileError{original: file, cause: errors.Join(writeErr, syncErr, closeErr)})
	}
	if err := errors.Join(writeErr, syncErr); err != nil {
		s.fail(t, "write stopped private corruption vector: %w", err)
	}
}
