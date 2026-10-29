//go:build darwin || linux

package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/server"
	"github.com/tmc/skygo/internal/store"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"rsc.io/script"
)

// TestWorkspaceJournalLockHelper opens one genuine journal owner. Its death
// releases an OS lock, but the incomplete durable intent remains incomplete.
func TestWorkspaceJournalLockHelper(t *testing.T) {
	if os.Getenv("SKYGO_WORKSPACE_LOCK_HELPER") != "1" {
		return
	}
	in := workspaceIntent{
		Endpoint: os.Getenv("SKYGO_ENDPOINT"), CredentialSHA256: workspaceHash([]byte("workspace-token")),
		YAML: []byte("workdir: input\nrun: echo frozen\n"), Shell: defaultShell,
		Workdir: os.Getenv("SKYGO_WORKSPACE_INPUT"),
	}
	run, err := openWorkspaceRun(os.Getenv("SKYGO_WORKSPACE_STATE"), in)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if _, err := fmt.Fprintln(os.Stdout, "workspace-lock-ready"); err != nil {
		fmt.Fprintln(os.Stderr, errors.Join(err, run.Close()))
		os.Exit(1)
	}
	var b [1]byte
	_, err = os.Stdin.Read(b[:])
	if !errors.Is(err, io.EOF) && err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := run.Close(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

type workspaceCapture struct {
	mu    sync.Mutex
	b     bytes.Buffer
	ready chan struct{}
	once  sync.Once
}

func (c *workspaceCapture) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, err := c.b.Write(b)
	if c.ready != nil && bytes.Contains(c.b.Bytes(), []byte("workspace-lock-ready\n")) {
		c.once.Do(func() { close(c.ready) })
	}
	return n, err
}

func (c *workspaceCapture) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.b.String()
}

// This concrete owner keeps the original Cmd until its one Wait completes.
// A timeout initiates a stop; it never supplies a synthetic Wait result.
type workspaceTestChild struct {
	cmd            *exec.Cmd
	out, diag      *workspaceCapture
	done           chan error
	joined, killed bool
	err            error
}

func workspaceStartChild(t *testing.T, cmd *exec.Cmd, ready chan struct{}) *workspaceTestChild {
	t.Helper()
	p := &workspaceTestChild{cmd: cmd, out: &workspaceCapture{ready: ready}, diag: new(workspaceCapture), done: make(chan error, 1)}
	cmd.Stdout, cmd.Stderr = p.out, p.diag
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.done <- cmd.Wait() }()
	t.Cleanup(func() {
		if !p.joined {
			p.kill(t)
			p.wait(t)
		}
	})
	return p
}

func (p *workspaceTestChild) kill(t *testing.T) {
	t.Helper()
	if p.killed || p.joined {
		return
	}
	p.killed = true
	if err := p.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Errorf("kill original CLI pid=%d: %v", p.cmd.Process.Pid, err)
	}
}

func (p *workspaceTestChild) wait(t *testing.T) (string, string, error) {
	t.Helper()
	if !p.joined {
		select {
		case p.err = <-p.done:
		case <-time.After(60 * time.Second):
			p.kill(t)
			// Retain this original until its actual Wait, including after Kill.
			p.err = <-p.done
			t.Errorf("CLI pid=%d exceeded the bounded operation", p.cmd.Process.Pid)
		}
		p.joined = true
		t.Logf("workspace original CLI pid=%d actual_wait=%v exit=%d stdout/stderr copy owners joined", p.cmd.Process.Pid, p.err, p.cmd.ProcessState.ExitCode())
	}
	return p.out.String(), p.diag.String(), p.err
}

func workspaceCLICommand(exe, directory, endpoint string, args ...string) *exec.Cmd {
	cmd := exec.Command(exe, append([]string{"--endpoint", endpoint, "--timeout", "45s"}, args...)...)
	cmd.Dir = directory
	cmd.Env = append(os.Environ(), "SKYGO_TOKEN=workspace-token", "SKYGO_TEST_HELPER=", "SKYGO_WORKSPACE_LOCK_HELPER=")
	return cmd
}

func workspaceTestCLICommand(t *testing.T, directory, endpoint string, args ...string) *exec.Cmd {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, append([]string{"-test.run=^TestCLIHelper$", "--", "--endpoint", endpoint}, args...)...)
	cmd.Dir = directory
	cmd.Env = append(os.Environ(), "SKYGO_TOKEN=workspace-token", "SKYGO_TEST_HELPER=1", "SKYGO_WORKSPACE_LOCK_HELPER=")
	return cmd
}

func TestWorkspaceProcessLockAndAbandonedStaging(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "unexpected request", http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	dir := workspaceTestDirectory(t)
	input, state := filepath.Join(dir, "input"), filepath.Join(dir, "state")
	if err := os.Mkdir(input, 0700); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^TestWorkspaceJournalLockHelper$")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "SKYGO_WORKSPACE_LOCK_HELPER=1", "SKYGO_ENDPOINT="+srv.URL, "SKYGO_WORKSPACE_INPUT="+input, "SKYGO_WORKSPACE_STATE="+state)
	// Cmd owns this parent's pipe until its original Wait. Keeping stdin
	// open makes the helper retain its lock until the explicit process death.
	if _, err := cmd.StdinPipe(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	owner := workspaceStartChild(t, cmd, ready)
	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		owner.kill(t)
		_, diag, err := owner.wait(t)
		t.Fatalf("actual lock owner did not become ready: %v %s", err, diag)
	}
	contender := workspaceStartChild(t, workspaceTestCLICommand(t, dir, srv.URL, "run", "--resume-workspace", state), nil)
	_, diag, err := contender.wait(t)
	if err == nil || !strings.Contains(diag, "workspace") || calls.Load() != 0 {
		t.Fatalf("second process passed a live lock: err=%v calls=%d stderr=%s", err, calls.Load(), diag)
	}
	owner.kill(t)
	if _, _, err := owner.wait(t); err == nil {
		t.Fatal("killed original unexpectedly succeeded")
	}
	// This actual released lock still cannot commit/adopt an interrupted file.
	staging := filepath.Join(state, "state.json.next")
	if err := os.WriteFile(staging, []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	before := workspaceTestFiles(t, state)
	resume := workspaceStartChild(t, workspaceTestCLICommand(t, dir, srv.URL, "run", "--resume-workspace", state), nil)
	_, diag, err = resume.wait(t)
	if err == nil || calls.Load() != 0 || !strings.Contains(diag, "workspace") {
		t.Fatalf("abandoned staging authorized network: err=%v calls=%d stderr=%s", err, calls.Load(), diag)
	}
	if after := workspaceTestFiles(t, state); !equalWorkspaceFiles(before, after) {
		t.Fatal("staging refusal changed private journal files")
	}
}

func equalWorkspaceFiles(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// These cases require an actual source-qualified Linux CLI/daemon fixture.
// Darwin runs the filesystem/refusal cases above; it does not substitute a
// modeled workspace placement or submission service for the production route.
func TestWorkspaceRealProcessRecovery(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("actual Local workspace submission/execution requires Linux; no modeled backend substitute")
	}
	exe := artifactScriptFixture(t, "SKYGO_ARTIFACT_BINARY")
	for _, tc := range []struct{ name, method, phase string }{
		{"begin_lost", "BeginArtifactUpload", "begin"},
		{"write_lost", "WriteArtifactChunk", "upload"},
		{"finalize_lost", "FinalizeArtifactUpload", "finalize"},
		{"submit_lost", "SubmitWorkspaceJob", "submit"},
		{"client_cancel", "GetJob", "accepted"},
	} {
		t.Run(tc.name, func(t *testing.T) { workspaceRealProcessRecovery(t, exe, tc.method, tc.phase) })
	}
}

type workspaceResponseLoss struct {
	mu                        sync.Mutex
	method                    string
	selected                  string
	phaseError                string
	body                      []byte
	requests                  map[string][][]byte
	effect, release, finished chan struct{}
	once                      sync.Once
}

func (f *workspaceResponseLoss) unblock() { f.once.Do(func() { close(f.release) }) }

func workspaceRealProcessRecovery(t *testing.T, exe, method, phase string) {
	t.Helper()
	dir, err := os.MkdirTemp(os.TempDir(), "workspace-real-")
	if err != nil {
		t.Fatal(err)
	}
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	data, root := filepath.Join(dir, "server"), filepath.Join(dir, "artifacts")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := server.InitializeArtifacts(data, root); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnableRetention(data, store.DefaultRetentionPolicy()); err != nil {
		t.Fatal(err)
	}
	p := &serverProcess{exe: exe, address: freeAddress(t), data: data, args: []string{"--token", "workspace-token", "--artifact-dir", root}, env: []string{"SKYGO_TOKEN="}}
	t.Cleanup(func() {
		if err := p.finish(false); err != nil {
			t.Errorf("retain real workspace fixture %s: %v", dir, err)
			return
		}
		if t.Failed() {
			t.Logf("retain failed workspace fixture after positive daemon join: %s", dir)
			return
		}
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	s, err := script.NewState(ctx, dir, os.Environ())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.start(s); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(dir, "intent")
	f := &workspaceResponseLoss{method: method, requests: make(map[string][][]byte), effect: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}
	target, err := url.Parse("http://" + p.address)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	direct := proxy.Director
	proxy.Director = func(r *http.Request) { direct(r); r.Header.Del("Accept-Encoding") }
	proxy.ModifyResponse = func(r *http.Response) error {
		if filepath.Base(r.Request.URL.Path) != method || r.StatusCode != http.StatusOK {
			return nil
		}
		f.mu.Lock()
		if f.selected != "" {
			f.mu.Unlock()
			return nil
		}
		f.selected = r.Request.Header.Get("X-Workspace-Test-Request")
		f.mu.Unlock()
		// This is a completed real server effect followed by a lost client
		// response. A later positive daemon Wait, not body EOF, joins all of
		// the actual daemon's original HTTP and file owners before restart.
		body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
		if err != nil {
			return err
		}
		if err := r.Body.Close(); err != nil {
			return err
		}
		f.mu.Lock()
		f.body = append([]byte(nil), body...)
		f.mu.Unlock()
		close(f.effect)
		<-f.release
		body = []byte(`{"code":"unavailable","message":"discarded completed workspace response"}`)
		r.StatusCode, r.Status = http.StatusServiceUnavailable, "503 Service Unavailable"
		r.Header.Set("Content-Type", "application/json")
		r.ContentLength = int64(len(body))
		r.Header.Set("Content-Length", fmt.Sprint(len(body)))
		r.Body = io.NopCloser(bytes.NewReader(body))
		return nil
	}
	var serial atomic.Uint64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := fmt.Sprint(serial.Add(1))
		r.Header.Set("X-Workspace-Test-Request", id)
		name := filepath.Base(r.URL.Path)
		body, err := io.ReadAll(io.LimitReader(r.Body, (8<<20)+1))
		if err != nil || len(body) > 8<<20 {
			http.Error(w, "invalid bounded request", http.StatusBadRequest)
			return
		}
		if err := r.Body.Close(); err != nil {
			http.Error(w, "request close failed", http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		f.mu.Lock()
		f.requests[name] = append(f.requests[name], append([]byte(nil), body...))
		f.mu.Unlock()
		if name == method {
			var beforeEffect workspaceState
			stateBytes, readErr := os.ReadFile(filepath.Join(statePath, "state.json"))
			decodeErr := json.Unmarshal(stateBytes, &beforeEffect)
			if readErr != nil || decodeErr != nil || beforeEffect.Phase != phase {
				f.mu.Lock()
				f.phaseError = fmt.Sprintf("phase before upstream %s=%q read=%v decode=%v", method, beforeEffect.Phase, readErr, decodeErr)
				f.mu.Unlock()
			}
		}
		proxy.ServeHTTP(w, r)
		f.mu.Lock()
		selected := f.selected == id
		f.mu.Unlock()
		if selected {
			close(f.finished)
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(f.unblock)
	input := filepath.Join(dir, "input")
	if err := os.Mkdir(input, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(input, "seed.txt"), []byte("frozen\n"), 0600); err != nil {
		t.Fatal(err)
	}
	yaml := "workdir: input\nsetup: touch prepared\nrun: test \"$EXPLICIT\" = frozen && test -f prepared && cat seed.txt > result.txt\n"
	releasePath := filepath.Join(dir, "release-job")
	if method == "GetJob" {
		yaml = "workdir: input\nsetup: touch prepared\nrun: while test ! -f " + releasePath + "; do sleep 0.05; done; test \"$EXPLICIT\" = frozen && test -f prepared && cat seed.txt > result.txt\n"
	}
	taskPath := filepath.Join(dir, "task.yaml")
	if err := os.WriteFile(taskPath, []byte(yaml), 0600); err != nil {
		t.Fatal(err)
	}
	first := workspaceStartChild(t, workspaceCLICommand(exe, dir, srv.URL, "--json", "run", taskPath, "--sync-workdir", "--workspace-state", statePath, "--env", "EXPLICIT=frozen", "--output", "result.txt", "--request-id", "workspace-recovery", "--wait"), nil)
	select {
	case <-f.effect:
	case <-ctx.Done():
		first.kill(t)
		out, diag, err := first.wait(t)
		t.Fatalf("real %s effect not reached: %v stdout=%s stderr=%s", method, err, out, diag)
	}
	f.mu.Lock()
	phaseError := f.phaseError
	f.mu.Unlock()
	if phaseError != "" {
		t.Fatal(phaseError)
	}
	var saved workspaceState
	stateBytes, err := os.ReadFile(filepath.Join(statePath, "state.json"))
	if err != nil || json.Unmarshal(stateBytes, &saved) != nil || saved.Phase != phase {
		t.Fatalf("phase before real %s effect = %+v, err=%v", method, saved, err)
	}
	before := serial.Load()
	contender := workspaceStartChild(t, workspaceCLICommand(exe, dir, srv.URL, "run", "--resume-workspace", statePath), nil)
	if _, diag, err := contender.wait(t); err == nil || serial.Load() != before || !strings.Contains(diag, "workspace") {
		t.Fatalf("concurrent actual CLI passed journal lock: %v stderr=%s", err, diag)
	}
	if method == "GetJob" {
		if err := first.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		if _, _, err := first.wait(t); err == nil || first.cmd.ProcessState.ExitCode() != 1 {
			t.Fatalf("actual SIGTERM did not cancel only the CLI: %v", err)
		}
		if err := os.WriteFile(releasePath, nil, 0600); err != nil {
			t.Fatal(err)
		}
	} else {
		first.kill(t)
		if _, _, err := first.wait(t); err == nil {
			t.Fatal("killed original CLI unexpectedly succeeded")
		}
	}
	f.unblock()
	select {
	case <-f.finished:
	case <-ctx.Done():
		t.Fatal("original proxy HTTP handler did not return")
	}
	if method == "SubmitWorkspaceJob" || method == "GetJob" {
		// Observe only: do not resume the journal or submit before the actual
		// old daemon stops. The lost-Submit identity must cross that restart.
		f.mu.Lock()
		completedResponse := append([]byte(nil), f.body...)
		f.mu.Unlock()
		workspaceWaitOriginalJob(t, ctx, "http://"+p.address, method, completedResponse)
	}
	p.mu.Lock()
	originalServerPID := p.cmd.Process.Pid
	p.mu.Unlock()
	if _, diag, err := p.stop(os.Interrupt); err != nil {
		t.Fatalf("join original daemon: %v stderr=%s", err, diag)
	}
	t.Logf("workspace original daemon pid=%d positive Wait0 before restart; original proxy handler returned", originalServerPID)
	if err := os.Remove(taskPath); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(input); err != nil {
		t.Fatal(err)
	}
	if err := p.start(s); err != nil {
		t.Fatal(err)
	}
	workspaceContinuityRefusals(t, exe, dir, srv.URL, statePath, &serial)
	resume := workspaceStartChild(t, workspaceCLICommand(exe, dir, srv.URL, "--json", "run", "--resume-workspace", statePath, "--wait"), nil)
	out, diag, err := resume.wait(t)
	if err != nil {
		t.Fatalf("resume frozen intent: %v stdout=%s stderr=%s", err, out, diag)
	}
	var job v1.Job
	if err := protojson.Unmarshal([]byte(out), &job); err != nil || job.State != v1.JobState_JOB_STATE_SUCCEEDED || job.Task == nil || job.Task.Workdir == nil || len(job.Task.Setup) == 0 {
		t.Fatalf("actual workspace did not succeed: %v job=%s", err, out)
	}
	if strings.Contains(out, "workspace-state:") || strings.Contains(out, "workspace-token") || strings.Contains(diag, "EXPLICIT=frozen") {
		t.Fatal("journal diagnostics or private values leaked to the wrong output")
	}
	workspaceContinuityRefusals(t, exe, dir, srv.URL, statePath, &serial)
	if job.Result == nil || job.Result.Reference == nil {
		t.Fatal("actual successful program has no collected result")
	}
	resultBytes, err := protojson.Marshal(job.Result.Reference)
	if err != nil {
		t.Fatal(err)
	}
	resultPath := filepath.Join(dir, "result-ref.json")
	if err := os.WriteFile(resultPath, resultBytes, 0600); err != nil {
		t.Fatal(err)
	}
	download := workspaceStartChild(t, workspaceCLICommand(exe, dir, srv.URL, "artifacts", "download", "--ref", resultPath, "restored"), nil)
	if out, diag, err := download.wait(t); err != nil {
		t.Fatalf("download actual program result: %v stdout=%s stderr=%s", err, out, diag)
	}
	if result, err := os.ReadFile(filepath.Join(dir, "restored", "result.txt")); err != nil || string(result) != "frozen\n" {
		t.Fatalf("program did not read frozen workdir/setup/env: %q %v", result, err)
	}
	// Accepted resume must get the same job, never Submit again or re-read YAML.
	f.mu.Lock()
	submits := len(f.requests["SubmitWorkspaceJob"])
	f.mu.Unlock()
	again := workspaceStartChild(t, workspaceCLICommand(exe, dir, srv.URL, "--json", "run", "--resume-workspace", statePath, "--wait"), nil)
	replay, diag, err := again.wait(t)
	var replayJob v1.Job
	if err != nil || protojson.Unmarshal([]byte(replay), &replayJob) != nil || !proto.Equal(&job, &replayJob) {
		t.Fatalf("accepted replay changed job: %v stdout=%s stderr=%s", err, replay, diag)
	}
	f.mu.Lock()
	if len(f.requests["SubmitWorkspaceJob"]) != submits {
		f.mu.Unlock()
		t.Fatal("accepted resume submitted another job")
	}
	archive, err := os.ReadFile(filepath.Join(statePath, "archive.tar"))
	if err != nil {
		f.mu.Unlock()
		t.Fatal(err)
	}
	workspaceCheckWireIdentities(t, f.requests, job.Task.Workdir, archive)
	f.mu.Unlock()
	workspacePrivateJournal(t, statePath)
	f.mu.Lock()
	cancels := len(f.requests["CancelJob"])
	f.mu.Unlock()
	if cancels != 0 {
		t.Fatal("client cancellation sent a workload cancellation")
	}
	beforeUnknown := serial.Load()
	if err := os.WriteFile(filepath.Join(statePath, "unknown-entry"), []byte("unknown"), 0600); err != nil {
		t.Fatal(err)
	}
	unknownFiles := workspaceTestFiles(t, statePath)
	unknown := workspaceStartChild(t, workspaceCLICommand(exe, dir, srv.URL, "run", "--resume-workspace", statePath), nil)
	if _, diag, err := unknown.wait(t); err == nil || serial.Load() != beforeUnknown {
		t.Fatalf("unknown entry authorized requests: %v stderr=%s", err, diag)
	}
	if !equalWorkspaceFiles(unknownFiles, workspaceTestFiles(t, statePath)) {
		t.Fatal("unknown-entry refusal mutated the existing journal")
	}
	if _, diag, err := p.stop(os.Interrupt); err != nil {
		t.Fatalf("stop before actual durable Store inspection: %v %s", err, diag)
	}
	// Ordinary Open may write. Record actual stopped fixture bytes and guest
	// stat facts before and after it; this is not a read-only API claim.
	inspection, err := os.MkdirTemp(os.TempDir(), "workspace-store-inspection-")
	if err != nil {
		t.Fatal(err)
	}
	beforeOpen, err := workspaceInspectionTree(data, root)
	if err != nil {
		t.Fatal(err)
	}
	if err := workspaceInspectionJSON(filepath.Join(inspection, "before.json"), beforeOpen); err != nil {
		t.Fatal(err)
	}
	inspectionOutcome := "before-open"
	defer func() {
		afterOpen, err := workspaceInspectionTree(data, root)
		if err != nil {
			t.Errorf("capture private after-Open inspection: %v", err)
			return
		}
		if err := workspaceInspectionJSON(filepath.Join(inspection, "after.json"), afterOpen); err != nil {
			t.Error(err)
			return
		}
		beforeDB, beforePresent := beforeOpen["data/jobs.db"]
		afterDB, afterPresent := afterOpen["data/jobs.db"]
		if !beforePresent || !afterPresent {
			t.Error("inspection did not capture both actual database files")
		}
		changed := beforeDB.SHA256 != afterDB.SHA256
		summary := struct {
			Test, Outcome, BeforeDBSHA256, AfterDBSHA256 string
			DBBytesChanged                               bool
		}{t.Name(), inspectionOutcome, beforeDB.SHA256, afterDB.SHA256, changed}
		if err := workspaceInspectionJSON(filepath.Join(inspection, "summary.json"), summary); err != nil {
			t.Error(err)
		}
		t.Logf("private Store.Open before/after evidence retained at %s; outcome=%s database_bytes_changed=%t (unchanged is not required)", inspection, inspectionOutcome, changed)
	}()
	// This is our stopped, known-current legacy-auth fixture. Scoped
	// InspectExisting/OpenExisting do not apply to this installation.
	database, err := store.Open(data)
	inspectionOutcome = "open-returned"
	if err != nil {
		t.Fatal(err)
	}
	jobs, listErr := database.List(0, ^uint64(0), 100)
	cohort, cohortErr := database.ArtifactReservations()
	closeErr := database.Close()
	if closeErr == nil {
		inspectionOutcome = "store-close-succeeded"
	} else {
		inspectionOutcome = "store-close-error"
	}
	if err := errors.Join(listErr, cohortErr, closeErr); err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].ID != job.Id || jobs[0].Task.Workdir == nil || jobs[0].Task.Workdir.ID != job.Task.Workdir.Id {
		t.Fatalf("actual durable Store contains duplicate/different jobs: %+v", jobs)
	}
	uploads, inputObjects := 0, 0
	for _, subject := range cohort.Subjects {
		if subject.Kind == "upload" {
			uploads++
			if subject.ID != job.Task.Workdir.Id {
				t.Fatal("durable cohort contains a second upload identity")
			}
		}
		if subject.Kind == "object" && subject.ID == job.Task.Workdir.Id {
			inputObjects++
			if subject.File.Device == 0 || subject.File.Inode == 0 {
				t.Fatal("completed input lost its real file identity")
			}
		}
	}
	if uploads != 1 || inputObjects != 1 {
		t.Fatalf("actual durable input cohort uploads=%d objects=%d", uploads, inputObjects)
	}
}

// workspaceWaitOriginalJob observes the exact already-accepted job from the
// retained successful server response. A timer only schedules another actual
// observation; neither elapsed time nor a journal replay establishes completion.
func workspaceWaitOriginalJob(t *testing.T, ctx context.Context, endpoint, method string, body []byte) {
	t.Helper()
	var job *v1.Job
	switch method {
	case "SubmitWorkspaceJob":
		var response v1.SubmitWorkspaceJobResponse
		if err := proto.Unmarshal(body, &response); err != nil {
			t.Fatal(err)
		}
		job = response.Job
	case "GetJob":
		var response v1.GetJobResponse
		if err := proto.Unmarshal(body, &response); err != nil {
			t.Fatal(err)
		}
		job = response.Job
	default:
		t.Fatal("terminal observation requires an accepted job response")
	}
	if job == nil || !validHexID(job.Id) {
		t.Fatal("actual successful response has no accepted job identity")
	}
	id := job.Id
	client := skyv1connect.NewControlServiceClient(http.DefaultClient, endpoint)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		request := connect.NewRequest(&v1.GetJobRequest{Id: id})
		request.Header().Set("Authorization", "Bearer workspace-token")
		response, err := client.GetJob(ctx, request)
		if err != nil {
			t.Fatalf("observe actual original job before daemon stop: %v", err)
		}
		if response == nil || response.Msg == nil || response.Msg.Job == nil || response.Msg.Job.Id != id {
			t.Fatal("original server returned a different accepted job")
		}
		job = response.Msg.Job
		if terminal(job.State) {
			if job.State != v1.JobState_JOB_STATE_SUCCEEDED {
				t.Fatalf("actual original job terminated without success before restart: %s", job.State)
			}
			t.Logf("actual original server observed job=%s SUCCEEDED before daemon stop; no journal resume or Submit used as a barrier", id)
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("actual original job has no successful terminal observation: %v", ctx.Err())
		case <-tick.C:
		}
	}
}

func workspaceContinuityRefusals(t *testing.T, exe, dir, endpoint, state string, calls *atomic.Uint64) {
	t.Helper()
	before := calls.Load()
	changed := workspaceCLICommand(exe, dir, endpoint, "run", "--resume-workspace", state)
	changed.Env = append(changed.Env, "SKYGO_TOKEN=rotated-secret-token")
	p := workspaceStartChild(t, changed, nil)
	_, diag, err := p.wait(t)
	if err == nil || calls.Load() != before || strings.Contains(diag, "rotated-secret-token") {
		t.Fatalf("changed effective credential made a request or leaked: %v stderr=%s", err, diag)
	}
	var foreign atomic.Int64
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		foreign.Add(1)
		http.Error(w, "unexpected", http.StatusServiceUnavailable)
	}))
	defer other.Close()
	p = workspaceStartChild(t, workspaceCLICommand(exe, dir, other.URL, "run", "--resume-workspace", state), nil)
	_, diag, err = p.wait(t)
	if err == nil || calls.Load() != before || foreign.Load() != 0 {
		t.Fatalf("changed endpoint made a network request: %v stderr=%s", err, diag)
	}
}

func workspaceCheckWireIdentities(t *testing.T, requests map[string][][]byte, ref *v1.ArtifactRef, archive []byte) {
	t.Helper()
	if int64(len(archive)) != ref.Size || workspaceHash(archive) != ref.Sha256 {
		t.Fatal("frozen journal archive differs from accepted input identity")
	}
	var begin *v1.BeginArtifactUploadRequest
	for _, b := range requests["BeginArtifactUpload"] {
		var r v1.BeginArtifactUploadRequest
		if err := proto.Unmarshal(b, &r); err != nil || r.RequestId == nil || r.Domain == nil || r.GetRequestId() != "workspace-recovery" || r.Sha256 != ref.Sha256 || r.Size != ref.Size {
			t.Fatalf("invalid canonical Begin replay: %v %s", err, &r)
		}
		if begin != nil && !proto.Equal(begin, &r) {
			t.Fatal("Begin key/domain/digest/size changed")
		}
		begin = proto.Clone(&r).(*v1.BeginArtifactUploadRequest)
	}
	if begin == nil {
		t.Fatal("no actual keyed Begin")
	}
	for _, b := range requests["WriteArtifactChunk"] {
		var r v1.WriteArtifactChunkRequest
		if err := proto.Unmarshal(b, &r); err != nil || r.Id != ref.Id || r.Offset < 0 || r.Offset > ref.Size || int64(len(r.Data)) > ref.Size-r.Offset {
			t.Fatalf("write changed upload/range: %v %s", err, &r)
		}
		if !bytes.Equal(r.Data, archive[r.Offset:r.Offset+int64(len(r.Data))]) {
			t.Fatal("write replay changed the frozen range bytes")
		}
	}
	for _, b := range requests["GetArtifactUpload"] {
		var r v1.GetArtifactUploadRequest
		if err := proto.Unmarshal(b, &r); err != nil || r.Id != ref.Id {
			t.Fatalf("known upload resume changed identity: %v %s", err, &r)
		}
	}
	for _, b := range requests["FinalizeArtifactUpload"] {
		var r v1.FinalizeArtifactUploadRequest
		if err := proto.Unmarshal(b, &r); err != nil || r.Id != ref.Id {
			t.Fatalf("Finalize changed upload: %v %s", err, &r)
		}
	}
	var first *v1.SubmitWorkspaceJobRequest
	for _, b := range requests["SubmitWorkspaceJob"] {
		var r v1.SubmitWorkspaceJobRequest
		if err := proto.Unmarshal(b, &r); err != nil || r.RequestId != begin.GetRequestId() || !proto.Equal(r.Domain, begin.Domain) || r.Task == nil || !proto.Equal(r.Task.Workdir, ref) {
			t.Fatalf("Submit changed request identity: %v %s", err, &r)
		}
		if first != nil && !proto.Equal(first, &r) {
			t.Fatal("Submit replay changed frozen task/domain/key")
		}
		first = proto.Clone(&r).(*v1.SubmitWorkspaceJobRequest)
	}
	if first == nil {
		t.Fatal("no real workspace Submit")
	}
}

func workspacePrivateJournal(t *testing.T, directory string) {
	t.Helper()
	if err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		want := os.FileMode(0600)
		if entry.IsDir() {
			want = 0700
		} else if !info.Mode().IsRegular() {
			return errors.New("journal contains a nonregular entry")
		}
		if info.Mode().Perm() != want {
			return fmt.Errorf("journal mode=%04o want=%04o", info.Mode().Perm(), want)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// These private evidence records contain hashes and actual guest metadata, not
// database, archive, authentication or task contents. The bounded walker never
// follows symlinks or opens a socket/device. Its directory lives outside the
// fixture so successful fixture cleanup cannot remove the inspection evidence.
type workspaceInspectionStat struct {
	SHA256        string `json:",omitempty"`
	Device, Inode uint64
	UID, Mode     uint32
	Size          int64
}

func workspaceInspectionTree(data, artifacts string) (map[string]workspaceInspectionStat, error) {
	out := make(map[string]workspaceInspectionStat)
	var total int64
	for label, directory := range map[string]string{"data": data, "artifacts": artifacts} {
		err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if len(out) >= 4096 {
				return errors.New("private inspection file count exceeded")
			}
			info, err := os.Lstat(path)
			if err != nil {
				return err
			}
			stat, ok := info.Sys().(*syscall.Stat_t)
			if !ok {
				return errors.New("private inspection guest stat unavailable")
			}
			record := workspaceInspectionStat{Device: uint64(stat.Dev), Inode: uint64(stat.Ino), UID: stat.Uid, Mode: uint32(stat.Mode), Size: info.Size()}
			if info.Mode().IsRegular() {
				if info.Size() < 0 || info.Size() > 64<<20 || total > (256<<20)-info.Size() {
					return errors.New("private inspection byte count exceeded")
				}
				file, err := os.Open(path)
				if err != nil {
					return err
				}
				hash := sha256.New()
				n, readErr := io.Copy(hash, io.LimitReader(file, (64<<20)+1))
				opened, statErr := file.Stat()
				closeErr := file.Close()
				if err := errors.Join(readErr, statErr, closeErr); err != nil {
					return err
				}
				if n != info.Size() || !os.SameFile(info, opened) || opened.Size() != info.Size() || opened.Mode() != info.Mode() {
					return errors.New("private inspection file changed while reading")
				}
				total += n
				record.SHA256 = hex.EncodeToString(hash.Sum(nil))
			}
			relative, err := filepath.Rel(directory, path)
			if err != nil {
				return err
			}
			name := label
			if relative != "." {
				name += "/" + filepath.ToSlash(relative)
			}
			out[name] = record
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func workspaceInspectionJSON(path string, value any) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	writeErr := json.NewEncoder(file).Encode(value)
	var syncErr error
	if writeErr == nil {
		syncErr = file.Sync()
	}
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
