//go:build linux

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"rsc.io/script"
)

// TestArtifactResultOnlyPruneNative requires a source-qualified CGO0 daemon
// and a Linux host supporting the production Local native lifecycle. It uses
// real CLI/Connect requests and never manufactures Store or backend receipts.
func TestArtifactResultOnlyPruneNative(t *testing.T) {
	if os.Getenv("SKYGO_RESULT_ONLY_NATIVE") != "1" {
		t.Skip("set SKYGO_RESULT_ONLY_NATIVE=1 on the qualified Linux host")
	}
	exe := artifactScriptFixture(t, "SKYGO_ARTIFACT_BINARY")
	workload, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261020-result-only-native")
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	directory, err := os.MkdirTemp(parent, "fixture-")
	if err != nil {
		t.Fatal(err)
	}
	frame := &companionServerFixture{directory: directory}
	t.Logf("retained result-only native evidence: %s", directory)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	state, err := script.NewState(ctx, directory, os.Environ())
	if err != nil {
		t.Fatal(err)
	}
	frame.state = state
	t.Cleanup(func() {
		for _, process := range frame.servers {
			process.mu.Lock()
			running := process.cmd != nil
			process.mu.Unlock()
			if running {
				_, diag, err := process.stop(os.Interrupt)
				if err != nil {
					frame.causes = append(frame.causes, err)
					t.Errorf("cleanup original server: %v\n%s", err, diag)
				}
			}
		}
		if err := state.CloseAndWait(io.Discard); err != nil {
			frame.causes = append(frame.causes, err)
			t.Error(err)
		}
		if len(frame.causes) != 0 {
			heldCompanionServerFixtures.Lock()
			heldCompanionServerFixtures.frames = append(heldCompanionServerFixtures.frames, frame)
			heldCompanionServerFixtures.Unlock()
			t.Log("HOLD: original command handles, causes and private namespace retained")
		}
	})
	var env []string
	if mode := os.Getenv("SKYGO_ARTIFACT_BINARY_COVERMODE"); mode != "" {
		if mode != testing.CoverMode() || (mode != "set" && mode != "count" && mode != "atomic") {
			t.Fatal("artifact fixture coverage mode differs")
		}
		if dir := coverDir(t); dir != "" {
			env = append(env, "GOCOVERDIR="+dir)
		}
	}
	run := func(endpoint string, args ...string) string {
		t.Helper()
		callCtx, callCancel := context.WithTimeout(ctx, 60*time.Second)
		defer callCancel()
		cmd := exec.CommandContext(callCtx, exe, args...)
		cmd.Dir = directory
		cmd.Env = append(append(append([]string(nil), os.Environ()...), env...), "SKYGO_ENDPOINT="+endpoint, "SKYGO_TOKEN=coverage-token")
		cmd.WaitDelay = 2 * time.Second
		var out, diag bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &diag
		frame.commands = append(frame.commands, cmd)
		if err := cmd.Run(); err != nil {
			frame.causes = append(frame.causes, err)
			t.Fatalf("CLI %q: %v\n%s\n%s", args, err, &out, &diag)
		}
		return out.String()
	}
	data := filepath.Join(directory, "state")
	root := filepath.Join(data, "artifacts")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	run("", "state", "artifacts", "init", "--data-dir", data, "--artifact-dir", root, "--ack-writers-stopped")
	process := &serverProcess{exe: exe, address: freeAddress(t), data: data, args: []string{"--token", "coverage-token", "--artifact-dir", root}, env: append(append([]string(nil), env...), "SKYGO_TOKEN=")}
	frame.servers = append(frame.servers, process)
	err = process.start(state)
	process.mu.Lock()
	original := process.cmd
	frame.commands = append(frame.commands, original)
	process.mu.Unlock()
	if err != nil {
		frame.causes = append(frame.causes, err)
		t.Fatal(err)
	}
	endpoint := "http://" + process.address
	source, observer := filepath.Join(directory, "source"), filepath.Join(directory, "observer")
	for _, path := range []string{source, observer} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(source, "seed.txt"), []byte("selected retry seed\n"), 0600); err != nil {
		t.Fatal(err)
	}
	refPath := filepath.Join(directory, "input.json")
	if err := os.WriteFile(refPath, []byte(run(endpoint, "artifacts", "upload", source)), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := readArtifactRef(refPath)
	if err != nil {
		t.Fatal(err)
	}
	workenv := map[string]string{"SKYGO_SELECTED_RETRY_WORKLOAD": "main", "SKYGO_SELECTED_RETRY_OBSERVER": observer}
	if dir := coverDir(t); dir != "" {
		workenv["GOCOVERDIR"] = dir
	}
	// Both phases use the same executable; an explicit final argument selects
	// setup without changing the task's common environment.
	argv := []string{workload, "-test.run=^TestSelectedRetryWorkload$"}
	setup := append(append([]string(nil), argv...), "--", "setup")
	task := map[string]any{"argv": argv, "setup": setup, "env": workenv, "workdir": ref, "outputs": []string{"out"}, "retry": map[string]any{"max_attempts": 2, "exit_codes": []int{7}}}
	raw, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	taskPath := filepath.Join(directory, "task.json")
	if err := os.WriteFile(taskPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	var job v1.Job
	jobJSON := []byte(run(endpoint, "--json", "run", "-f", taskPath, "--request-id", "result-only-native", "--wait"))
	if err := os.WriteFile(filepath.Join(directory, "job.json"), jobJSON, 0600); err != nil {
		t.Fatal(err)
	}
	if err := protojson.Unmarshal(jobJSON, &job); err != nil {
		t.Fatal(err)
	}
	if job.State != v1.JobState_JOB_STATE_SUCCEEDED || len(job.Attempts) != 2 || job.NextAttemptAt != nil {
		t.Fatalf("two-attempt terminal history differs: %v", &job)
	}
	first, second := job.Attempts[0], job.Attempts[1]
	for i, a := range job.Attempts {
		if a.Id == "" || a.Ordinal != uint32(i+1) || a.StartingAt == nil || a.FinishedAt == nil {
			t.Fatalf("attempt identity/lifecycle missing: %v", a)
		}
	}
	if first.Id != job.Id || second.Id == first.Id || first.State != v1.JobState_JOB_STATE_FAILED || first.ExitCode == nil || *first.ExitCode != 7 || second.State != v1.JobState_JOB_STATE_SUCCEEDED || second.ExitCode == nil || *second.ExitCode != 0 {
		t.Fatalf("stopped failure/success history differs: %v", &job)
	}
	if first.Result == nil || first.Result.State != v1.ArtifactResultState_ARTIFACT_RESULT_STATE_ABSENT || first.Result.Reference != nil || first.Result.AttemptId != first.Id {
		t.Fatalf("failed attempt acquired result: %v", first.Result)
	}
	if job.Result == nil || job.Result.State != v1.ArtifactResultState_ARTIFACT_RESULT_STATE_AVAILABLE || job.Result.AttemptId != second.Id || !proto.Equal(job.Result, second.Result) {
		t.Fatalf("second result binding differs: %v", &job)
	}
	if _, err := checkedArtifactRef(job.Result.Reference); err != nil {
		t.Fatal(err)
	}
	t.Logf("original attempts: %s exit7 -> %s exit0; second result %s sha256=%s", first.Id, second.Id, job.Result.Reference.Id, job.Result.Reference.Sha256)
	resultRaw, err := protojson.Marshal(job.Result.Reference)
	if err != nil {
		t.Fatal(err)
	}
	resultRef := filepath.Join(directory, "result.json")
	if err := os.WriteFile(resultRef, resultRaw, 0600); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(directory, "downloaded")
	run(endpoint, "artifacts", "download", "--ref", resultRef, destination)
	got, err := os.ReadFile(filepath.Join(destination, "out", "result.txt"))
	if err != nil || string(got) != "selected retry seed\nsecond attempt\n" {
		t.Fatalf("second result bytes differ: %q %v", got, err)
	}
	if _, err := os.Lstat(filepath.Join(destination, "out", "failed-only.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("first attempt output mixed into result", err)
	}
	var workspaces []string
	for _, name := range []string{"first", "second"} {
		cwd, err := os.ReadFile(filepath.Join(observer, name))
		if err != nil {
			t.Fatal(err)
		}
		workspaces = append(workspaces, string(cwd))
	}
	if workspaces[0] == workspaces[1] || workspaces[0] != filepath.Join(root, "workspace-"+first.Id) || workspaces[1] != filepath.Join(root, "workspace-"+second.Id) {
		t.Fatalf("workspaces were not distinct original attempt roots: %q", workspaces)
	}
	entries, err := os.ReadDir(observer)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 4 {
		t.Fatalf("setup/main invocation count differs: %v", entries)
	}
	for _, cwd := range workspaces {
		prepared, err := os.ReadFile(filepath.Join(observer, "setup-"+filepath.Base(cwd)))
		if err != nil || string(prepared) != cwd {
			t.Fatalf("setup did not run once for original workspace %q: %v", cwd, err)
		}
	}
	var final v1.Job
	if err := protojson.Unmarshal([]byte(run(endpoint, "--json", "jobs", "get", job.Id)), &final); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(&job, &final) {
		t.Fatal("terminal attempt IDs/results/history changed after download")
	}
	// Terminal metadata precedes asynchronous physical retirement. Keep the
	// original server running so its ordinary maintenance worker can remove
	// both exact roots. Shutdown may instead positively hand off closed owners
	// with durable cleanup still pending; Wait success alone promises no unlink.
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		retired := true
		for _, cwd := range workspaces {
			if _, err := os.Lstat(cwd); err == nil {
				retired = false
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("observe original workspace retirement: %s: %v", cwd, err)
			}
		}
		if retired {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("ordinary production retirement did not remove both original workspace roots: %v", ctx.Err())
		case <-tick.C:
		}
	}
	// Both real attempt roots retired through ordinary production workers.
	client := skyv1connect.NewControlServiceClient(http.DefaultClient, endpoint)
	cutoff := time.Now().UTC().Add(time.Minute)
	prune := func(attempt uint32) *v1.PruneJobsResponse {
		t.Helper()
		r := connect.NewRequest(&v1.PruneJobsRequest{Mode: v1.PruneMode_PRUNE_MODE_RESULT_ONLY, Attempt: attempt, Before: timestamppb.New(cutoff)})
		r.Header().Set("Authorization", "Bearer coverage-token")
		response, err := client.PruneJobs(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		return response.Msg
	}
	if page := prune(1); page.Selected != 0 || page.Marked != 0 || page.Executed {
		t.Fatalf("fixed absent first result selected current second: %v", page)
	}
	// Unlink may precede integration of its original maintenance receipt.
	// Wait for the real Store selector, never turn missing paths into authority.
	for {
		page := prune(2)
		if page.Selected == 1 && page.Marked == 0 && !page.Executed {
			break
		}
		if page.Selected != 0 || page.Marked != 0 || page.Executed || len(page.Skips) != 1 || (page.Skips[0].Reason != "result_in_use" && page.Skips[0].Reason != "operation_pending") {
			t.Fatalf("actual generated-client result-only dry-run: %v", page)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("original result did not become eligible: %v; last page=%v", ctx.Err(), page)
		case <-tick.C:
		}
	}
	var dry v1.PruneJobsResponse
	if err := protojson.Unmarshal([]byte(run(endpoint, "--json", "jobs", "prune", "--result-only", "--attempt", "2", "--before", cutoff.Format(time.RFC3339Nano))), &dry); err != nil || dry.Selected != 1 || dry.Marked != 0 || dry.Executed {
		t.Fatalf("CLI dry-run=%v error=%v", &dry, err)
	}
	var unchanged v1.Job
	if err := protojson.Unmarshal([]byte(run(endpoint, "--json", "jobs", "get", job.Id)), &unchanged); err != nil || !proto.Equal(&job, &unchanged) {
		t.Fatalf("dry-run changed actual result or history: %v %v", &unchanged, err)
	}
	var executed v1.PruneJobsResponse
	if err := protojson.Unmarshal([]byte(run(endpoint, "--json", "jobs", "prune", "--result-only", "--attempt", "2", "--before", cutoff.Format(time.RFC3339Nano), "--yes")), &executed); err != nil || executed.Selected != 1 || executed.Marked != 1 || !executed.Executed {
		t.Fatalf("CLI execute=%v error=%v", &executed, err)
	}
	var expired v1.Job
	if err := protojson.Unmarshal([]byte(run(endpoint, "--json", "jobs", "get", job.Id)), &expired); err != nil {
		t.Fatal(err)
	}
	expected := proto.Clone(&job).(*v1.Job)
	expected.Result = &v1.ArtifactResult{State: v1.ArtifactResultState_ARTIFACT_RESULT_STATE_EXPIRED, AttemptId: second.Id}
	expected.Attempts[1].Result = proto.Clone(expected.Result).(*v1.ArtifactResult)
	if !proto.Equal(expected, &expired) {
		t.Fatalf("result-only expiry changed other job/input/attempt history: %v", &expired)
	}
	// The exact Store witness is projected; no result grant is recreated.
	download := connect.NewRequest(&v1.BeginArtifactDownloadRequest{Reference: job.Result.Reference})
	download.Header().Set("Authorization", "Bearer coverage-token")
	if _, err := client.BeginArtifactDownload(ctx, download); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expired result acquired a download: %v", err)
	}
	inputCopy := filepath.Join(directory, "input-after-expiry")
	run(endpoint, "artifacts", "download", "--ref", refPath, inputCopy)
	if data, err := os.ReadFile(filepath.Join(inputCopy, "seed.txt")); err != nil || string(data) != "selected retry seed\n" {
		t.Fatalf("independent result expiry damaged input: %q %v", data, err)
	}
	var replay v1.Job
	if err := protojson.Unmarshal([]byte(run(endpoint, "--json", "run", "-f", taskPath, "--request-id", "result-only-native")), &replay); err != nil || !proto.Equal(&expired, &replay) {
		t.Fatalf("request replay lost original expired-result history: %v %v", &replay, err)
	}
	if entries, err := os.ReadDir(observer); err != nil || len(entries) != 4 {
		t.Fatalf("request replay relaunched setup/main: %v %v", entries, err)
	}
	if page := prune(2); page.Selected != 0 || page.Marked != 0 {
		t.Fatalf("expired result selected again: %v", page)
	}
	t.Logf("result expiry preserved input and request history: job=%s attempt=%s", job.Id, second.Id)

	_, diag, err := process.stop(os.Interrupt)
	if err != nil {
		frame.causes = append(frame.causes, err)
		t.Fatalf("original server stop: %v\n%s", err, diag)
	}
	if original == nil || original.ProcessState == nil || !original.ProcessState.Success() || diag != "" {
		frame.causes = append(frame.causes, errors.New("original server join not positive"))
		t.Fatalf("original server join not positive: %s", diag)
	}
	// These observations follow original Wait and production drain. No path is
	// adopted, removed, or treated as authority to clean a native process.
	for _, cwd := range workspaces {
		if _, err := os.Lstat(cwd); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("original workspace root not retired: %s: %v", cwd, err)
		}
	}
}
