package cli

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"rsc.io/script"
)

// Keep original process handles and causes if a command or shutdown is uncertain.
// These private evidence directories are never automatically removed.
var heldCompanionServerFixtures struct {
	sync.Mutex
	frames []*companionServerFixture
}

type companionServerFixture struct {
	directory string
	state     *script.State
	servers   []*serverProcess
	commands  []*exec.Cmd
	causes    []error
}

// TestArtifactCompanionRestoredServer crosses the offline/online frontier using
// production CLI processes. The uploaded input has never belonged to a native
// workspace; this test does not qualify workspace execution or native shutdown.
func TestArtifactCompanionRestoredServer(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("artifact filesystem metadata unsupported")
	}
	exe := artifactScriptFixture(t, "SKYGO_ARTIFACT_BINARY")
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261020-companion-restored-server")
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	directory, err := os.MkdirTemp(parent, "fixture-")
	if err != nil {
		t.Fatal(err)
	}
	frame := &companionServerFixture{directory: directory}
	t.Logf("retained companion restored-server evidence: %s", directory)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	state, err := script.NewState(ctx, directory, os.Environ())
	if err != nil {
		t.Fatal(err)
	}
	frame.state = state
	// stop clears serverProcess.cmd even on error. Preserve the captured original
	// *exec.Cmd before stopping; never retry an uncertain stop or delete its root.
	t.Cleanup(func() {
		for _, process := range frame.servers {
			process.mu.Lock()
			running := process.cmd != nil
			process.mu.Unlock()
			if running {
				_, diag, err := process.stop(os.Interrupt)
				if err != nil {
					frame.causes = append(frame.causes, err)
					t.Errorf("cleanup server: %v\n%s", err, diag)
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
			t.Log("HOLD: original fixture handles, errors and namespace retained")
		}
	})
	var env []string
	if mode := os.Getenv("SKYGO_ARTIFACT_BINARY_COVERMODE"); mode != "" {
		if mode != testing.CoverMode() || (mode != "set" && mode != "count" && mode != "atomic") {
			t.Fatal("artifact fixture coverage mode does not match the instrumented test binary")
		}
		if dir := coverDir(t); dir != "" {
			env = append(env, "GOCOVERDIR="+dir)
		}
	}
	run := func(endpoint string, args ...string) (string, string) {
		t.Helper()
		callCtx, callCancel := context.WithTimeout(ctx, 30*time.Second)
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
			t.Fatalf("CLI %q: %v\nstdout: %s\nstderr: %s", args, err, &out, &diag)
		}
		return out.String(), diag.String()
	}
	start := func(data string) *serverProcess {
		t.Helper()
		process := &serverProcess{exe: exe, address: freeAddress(t), data: data,
			args: []string{"--token", "coverage-token", "--artifact-dir", filepath.Join(data, "artifacts")},
			env:  append(append([]string(nil), env...), "SKYGO_TOKEN=")}
		frame.servers = append(frame.servers, process)
		err := process.start(state)
		process.mu.Lock()
		frame.commands = append(frame.commands, process.cmd)
		process.mu.Unlock()
		if err != nil {
			frame.causes = append(frame.causes, err)
			t.Fatal(err)
		}
		return process
	}
	stop := func(process *serverProcess) {
		t.Helper()
		process.mu.Lock()
		original := process.cmd
		process.mu.Unlock()
		_, diag, err := process.stop(os.Interrupt)
		if err != nil {
			frame.causes = append(frame.causes, err)
			t.Fatalf("stop original server: %v\n%s", err, diag)
		}
		if original == nil || original.ProcessState == nil || !original.ProcessState.Success() || diag != "" {
			frame.causes = append(frame.causes, fmt.Errorf("server join/readback not positive: %q", diag))
			t.Fatalf("server join/readback not positive: %q", diag)
		}
	}
	data := filepath.Join(directory, "source-state")
	root := filepath.Join(data, "artifacts")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	out, diag := run("", "state", "artifacts", "init", "--data-dir", data, "--artifact-dir", root, "--ack-writers-stopped")
	if out != "enabled: true\npolicy: default-v1\nrestart_required: true\n" || diag != "" {
		t.Fatal("initializer readback", out, diag)
	}
	source := filepath.Join(directory, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("restored over actual HTTP\x00\xff\n"), 3000)
	if err := os.WriteFile(filepath.Join(source, "payload.bin"), want, 0600); err != nil {
		t.Fatal(err)
	}
	original := start(data)
	out, diag = run("http://"+original.address, "artifacts", "upload", source)
	refPath := filepath.Join(directory, "ref.json")
	if err := os.WriteFile(refPath, []byte(out), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := readArtifactRef(refPath)
	if err != nil {
		t.Fatal(err)
	}
	if diag != fmt.Sprintf("upload-id: %s\n", ref.ID) {
		t.Fatalf("upload identity/readback differs: %q", diag)
	}
	stop(original) // positive original Wait precedes acknowledgement/acquisition
	bundle := filepath.Join(directory, "bundle")
	out, diag = run("", "state", "artifacts", "backup", "--data-dir", data, "--artifact-dir", root, "--output", bundle, "--ack-writers-stopped")
	if out != "backup complete\n" || diag != "" {
		t.Fatal("backup readback", out, diag)
	}
	restoredData := filepath.Join(directory, "restored-state")
	out, diag = run("", "state", "artifacts", "restore", "--input", bundle, "--data-dir", restoredData, "--ack-writers-stopped")
	if out != "restore complete\n" || diag != "" {
		t.Fatal("restore readback", out, diag)
	}
	restored := start(restoredData)
	if restored == original || restored.data == original.data {
		t.Fatal("restored server reused original fixture")
	}
	destination := filepath.Join(directory, "downloaded")
	out, diag = run("http://"+restored.address, "artifacts", "download", "--ref", refPath, destination)
	if out != destination+"\n" || diag != "" {
		t.Fatal("restored HTTP download readback", out, diag)
	}
	got, err := os.ReadFile(filepath.Join(destination, "payload.bin"))
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("restored download bytes differ: %v (got %d, want %d)", err, len(got), len(want))
	}
	if after, err := readArtifactRef(refPath); err != nil || after != ref {
		t.Fatal("original reference changed", after, err)
	}
	stop(restored)
}
