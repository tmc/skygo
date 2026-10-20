//go:build linux

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"rsc.io/script"
)

// This executable is the real setup/main program in both actual workspaces.
// Observer files are private to this test and never grant cleanup authority.
func TestSelectedRetryWorkload(t *testing.T) {
	mode := os.Getenv("SKYGO_SELECTED_RETRY_WORKLOAD")
	if mode == "" {
		return
	}
	if os.Args[len(os.Args)-1] == "setup" {
		mode = "setup"
	}
	fail := func(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(99) }
	cwd, err := os.Getwd()
	if err != nil {
		fail(err)
	}
	observer := os.Getenv("SKYGO_SELECTED_RETRY_OBSERVER")
	if !filepath.IsAbs(observer) {
		fail(errors.New("missing private observer directory"))
	}
	writeExclusive := func(path string, data []byte) {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			fail(err)
		}
		n, writeErr := f.Write(data)
		if writeErr == nil && n != len(data) {
			writeErr = io.ErrShortWrite
		}
		if err := errors.Join(writeErr, f.Close()); err != nil {
			fail(err)
		}
	}
	if mode == "setup" {
		writeExclusive("prepared", []byte(cwd))
		writeExclusive(filepath.Join(observer, "setup-"+filepath.Base(cwd)), []byte(cwd))
		os.Exit(0)
	}
	if mode != "main" {
		fail(errors.New("invalid workload phase"))
	}
	prepared, err := os.ReadFile("prepared")
	if err != nil || string(prepared) != cwd {
		fail(errors.Join(err, errors.New("setup did not prepare this exact workspace")))
	}
	seed, err := os.ReadFile("seed.txt")
	if err != nil || string(seed) != "selected retry seed\n" {
		fail(errors.Join(err, errors.New("input bytes differ")))
	}
	if err := os.Mkdir("out", 0700); err != nil {
		fail(err)
	}
	// Fail only the first actual main invocation. A third invocation refuses
	// both exclusive observer slots and cannot silently look successful.
	f, err := os.OpenFile(filepath.Join(observer, "first"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err == nil {
		_, writeErr := f.WriteString(cwd)
		if err := errors.Join(writeErr, f.Close()); err != nil {
			fail(err)
		}
		if err := os.WriteFile("out/failed-only.txt", []byte("first attempt must not escape\n"), 0600); err != nil {
			fail(err)
		}
		os.Exit(7)
	}
	if !errors.Is(err, os.ErrExist) {
		fail(err)
	}
	writeExclusive(filepath.Join(observer, "second"), []byte(cwd))
	if _, err := os.Lstat("out/failed-only.txt"); !errors.Is(err, os.ErrNotExist) {
		fail(errors.Join(err, errors.New("failed output leaked into second workspace")))
	}
	if err := os.WriteFile("out/result.txt", append(seed, []byte("second attempt\n")...), 0600); err != nil {
		fail(err)
	}
	os.Exit(0)
}

// TestArtifactSelectedTaskRetryNative requires a source-qualified CGO0 daemon
// and a Linux host supporting the production Local native lifecycle. It uses
// real CLI/Connect requests and never manufactures Store or backend receipts.
func TestArtifactSelectedTaskRetryNative(t *testing.T) {
	if os.Getenv("SKYGO_SELECTED_RETRY_NATIVE") != "1" {
		t.Skip("set SKYGO_SELECTED_RETRY_NATIVE=1 on the qualified Linux host")
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
	parent := filepath.Join(home, "tmp", "skypilot", "20261020-selected-retry-native")
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	directory, err := os.MkdirTemp(parent, "fixture-")
	if err != nil {
		t.Fatal(err)
	}
	frame := &companionServerFixture{directory: directory}
	t.Logf("retained selected retry native evidence: %s", directory)
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
	jobJSON := []byte(run(endpoint, "--json", "run", "-f", taskPath, "--request-id", "selected-retry-native", "--wait"))
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
