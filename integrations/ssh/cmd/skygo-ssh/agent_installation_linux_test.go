package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/integrations/ssh/internal/bootstrap"
	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/cli"
	"github.com/tmc/skygo/internal/server"
	"github.com/tmc/skygo/internal/store"
	"golang.org/x/tools/txtar"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// A fixture owns one private directory holding every root a test serves
// or watches: the agent identity, the installations, the bystander and
// the client's files. None of them is registered with t.TempDir, whose
// cleanup would remove roots below a server that has not returned.
//
// The fixture counts the original server.Run calls it starts and settles
// each only by its actual return. retire removes the directory only if
// every Run returned, none left ownership unsettled and the test passed;
// otherwise it keeps the directory and reports its path. Once ownership
// is unsettled, no further Run uses the roots.
type fixture struct {
	t    *testing.T
	dir  string
	runs int   // original server.Run calls that have not returned
	held error // unsettled ownership, retained with its original frame
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261028-s11-ssh-installation-tests")
	if err := os.MkdirAll(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(parent, "case-")
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, dir: dir}
	t.Cleanup(f.retire)
	return f
}

func (f *fixture) retire() {
	switch {
	case f.held != nil:
		f.t.Errorf("HOLD: keeping %s: original server ownership unsettled: %v", f.dir, f.held)
	case f.runs != 0:
		f.t.Errorf("HOLD: keeping %s: %d original server.Run calls have not returned", f.dir, f.runs)
	case f.t.Failed():
		f.t.Logf("keeping failed case %s", f.dir)
	default:
		if err := os.RemoveAll(f.dir); err != nil {
			f.t.Errorf("remove %s: %v", f.dir, err)
		}
	}
}

// keep records unsettled ownership without stopping the caller, which
// may be a cleanup. The original error, and the frame it may retain, stay
// reachable for the life of the test process.
func (f *fixture) keep(err error) {
	if f.held != nil {
		return
	}
	f.held = err
	held, never := err, make(chan struct{})
	go func() { <-never; runtime.KeepAlive(held) }()
}

// hold records unsettled ownership and stops the test.
func (f *fixture) hold(err error) {
	f.t.Helper()
	f.keep(err)
	f.t.Fatalf("HOLD: keeping %s: original server ownership unsettled: %v", f.dir, f.held)
}

// mkdir makes the private directory name in the fixture.
func (f *fixture) mkdir(name string) string {
	f.t.Helper()
	p := filepath.Join(f.dir, name)
	if err := os.Mkdir(p, 0o700); err != nil {
		f.t.Fatal(err)
	}
	return p
}

// agentDir returns the fixture's agent directory holding a new identity
// and, as an installation agent requires, no legacy token.
func (f *fixture) agentDir() (string, identity) {
	f.t.Helper()
	id, err := loadIdentity(filepath.Join(f.dir, "agent"), agentName)
	if err != nil {
		f.t.Fatal(err)
	}
	remove(f.t, id.token)
	return filepath.Dir(id.cert), id
}

// serveAgent runs the agent in process with server.Run until the returned
// stop is called. stop cancels the original Run and waits for it to
// return, however long that takes: only its actual return settles it,
// and the go test timeout bounds one that never does. Any result but nil
// holds the fixture. A cleanup stops an agent the test did not.
func (f *fixture) serveAgent(args []string) (stop func()) {
	f.t.Helper()
	if f.held != nil {
		f.hold(f.held)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	f.runs++
	go func() { done <- runAgent(ctx, args, io.Discard, server.Run) }()
	joined := false
	var result error
	join := func() error {
		if !joined {
			cancel()
			result = <-done
			joined = true
			f.runs--
			if result != nil {
				f.keep(fmt.Errorf("agent server.Run returned: %w", result))
			}
		}
		return result
	}
	f.t.Cleanup(func() { join() })
	return func() {
		f.t.Helper()
		if join() != nil {
			f.hold(f.held)
		}
	}
}

// refused runs the agent with args, which the test expects to fail at
// startup, and returns its error. An ordinary refusal, after which
// server.Run settled everything it opened, is returned for the caller to
// check. A *server.RunError or an uncertain artifact or job database close
// holds the fixture at once, keeping the original error; nothing reopens
// or removes the roots beneath it.
func (f *fixture) refused(ctx context.Context, args []string) error {
	f.t.Helper()
	if f.held != nil {
		f.hold(f.held)
	}
	f.runs++
	err := runAgent(ctx, args, io.Discard, server.Run)
	f.runs--
	var held *server.RunError
	if errors.As(err, &held) || errors.Is(err, artifact.ErrFileCloseUncertain) || errors.Is(err, store.ErrCloseUncertain) {
		f.hold(fmt.Errorf("refused agent server.Run: %w", err))
	}
	return err
}

// An installation is a real installation initialized as an operator does
// on the host, with the canonical CLI on stopped state: scoped auth with
// an admin token, an operator bob and a reader carol, and an artifact
// root bound to the store.
type installation struct {
	bootstrap.Installation
	admin, bob, carol string // private token files
}

// operate runs a canonical local skygo command, as the operator on the
// host. A typed artifact initialization error holds the fixture.
func (f *fixture) operate(args ...string) string {
	f.t.Helper()
	root := cli.NewWithClient(nil, nil, cli.ClientOptions{})
	root.SetArgs(args)
	var out, errout bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errout)
	if err := root.ExecuteContext(context.Background()); err != nil {
		var initialization *server.ArtifactInitializationError
		if errors.As(err, &initialization) {
			f.hold(fmt.Errorf("skygo %q: %w", args, err))
		}
		f.t.Fatalf("skygo %q: %v\n%s", args, err, errout.String())
	}
	return out.String()
}

// newInstallation initializes an installation in the fixture directory
// name.
func (f *fixture) newInstallation(name string) installation {
	f.t.Helper()
	base := f.mkdir(name)
	in := installation{
		Installation: bootstrap.Installation{
			DataDir:     filepath.Join(base, "data"),
			AuthFile:    filepath.Join(base, "auth.json"),
			ArtifactDir: filepath.Join(base, "artifacts"),
		},
		admin: filepath.Join(base, "admin.token"),
		bob:   filepath.Join(base, "bob.token"),
		carol: filepath.Join(base, "carol.token"),
	}
	if !bootstrap.ValidPath(in.DataDir) || !bootstrap.ValidPath(in.AuthFile) || !bootstrap.ValidPath(in.ArtifactDir) {
		f.t.Skipf("fixture directory %s is not a valid installation path", base)
	}
	f.mkdir(filepath.Join(name, "data"))
	f.mkdir(filepath.Join(name, "artifacts"))
	f.operate("auth", "init", "--auth-file", in.AuthFile, "--data-dir", in.DataDir, "--token-output", in.admin)
	f.operate("auth", "principal", "add", "bob", "--role", "operator", "--auth-file", in.AuthFile)
	f.operate("auth", "token", "issue", "bob", "--output", in.bob, "--auth-file", in.AuthFile)
	f.operate("auth", "principal", "add", "carol", "--role", "reader", "--auth-file", in.AuthFile)
	f.operate("auth", "token", "issue", "carol", "--output", in.carol, "--auth-file", in.AuthFile)
	if out := f.operate("state", "artifacts", "init", "--data-dir", in.DataDir, "--artifact-dir", in.ArtifactDir, "--ack-writers-stopped"); out != "enabled: true\npolicy: default-v1\nrestart_required: true\n" {
		f.t.Fatalf("artifact initialization readback %q", out)
	}
	return in
}

// enableRetention enables bounded retention on in's stopped state with
// the canonical CLI, as an operator does after initialization and before
// any agent serves it: store.EnableRetention under the store's exclusive
// writer lock, with the default policy and local output kept. It checks
// the activation readback and returns its request domain.
func (f *fixture) enableRetention(in installation) string {
	f.t.Helper()
	if f.held != nil {
		f.hold(f.held)
	}
	if f.runs != 0 {
		f.t.Fatalf("enable retention on %s: %d original server.Run calls have not returned", in.DataDir, f.runs)
	}
	out := f.operate("state", "retention", "enable", "--data-dir", in.DataDir, "--json")
	var got struct {
		Path               string `json:"path"`
		Enabled            bool   `json:"enabled"`
		RequestDomain      string `json:"request_domain"`
		LegacyReplayOpen   bool   `json:"legacy_replay_open"`
		RequestWindow      string `json:"request_window"`
		RestartRequired    bool   `json:"restart_required"`
		LocalOutputEnabled bool   `json:"local_output_enabled"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		f.t.Fatalf("retention activation readback %q: %v", out, err)
	}
	window := store.DefaultRetentionPolicy().Window.String()
	if got.Path != in.DataDir || !got.Enabled || got.RequestDomain == "" || !got.LegacyReplayOpen || got.RequestWindow != window || !got.RestartRequired || !got.LocalOutputEnabled {
		f.t.Fatalf("retention activation readback %q, want enabled with local output, request window %s, restart required", out, window)
	}
	return got.RequestDomain
}

// expiryReason returns the typed retention expiry reason err carries, or
// zero if it carries none.
func expiryReason(err error) v1.RetentionExpiryReason {
	var rpc *connect.Error
	if !errors.As(err, &rpc) {
		return 0
	}
	for _, detail := range rpc.Details() {
		value, err := detail.Value()
		if err != nil {
			continue
		}
		if e, ok := value.(*v1.RetentionExpiry); ok {
			return e.Reason
		}
	}
	return 0
}

func freeAddress(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

// pinnedClient returns a client that, like the SSH transport, dials one
// fixed address and trusts only the agent's certificate. It is a MODEL of
// the SSH channel: requests do not cross agentssh, whose 1 MiB and 10s
// per-request bounds are tested in internal/agentssh.
func pinnedClient(t *testing.T, cert, address string) *http.Client {
	t.Helper()
	certPEM, err := os.ReadFile(cert)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(certPEM)
	dialer := &net.Dialer{}
	return &http.Client{Transport: identityTransport{&http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, address)
		},
		TLSClientConfig: &tls.Config{RootCAs: roots, ServerName: agentName, MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"}},
	}}}
}

func bearer(t *testing.T, file string) string {
	t.Helper()
	token, err := readToken(file)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func authorized[T any](token string, msg *T) *connect.Request[T] {
	req := connect.NewRequest(msg)
	req.Header().Set("Authorization", "Bearer "+token)
	return req
}

// agentClient runs the skygo arguments args as runClient does once the
// agent is ready: skygo-ssh's argument checks, then agentCommand through
// hc with the credential in tokenFile. Errors are also written to
// stderr, as main writes them.
func agentClient(ctx context.Context, hc *http.Client, tokenFile string, args []string, stdout, stderr io.Writer) error {
	err := func() error {
		if _, err := checkArgs(cli.NewWithClient(nil, nil, cli.ClientOptions{}), args); err != nil {
			return err
		}
		root, err := agentCommand(hc, "dgx", "dgx", tokenFile, args)
		if err != nil {
			return err
		}
		root.SetOut(stdout)
		root.SetErr(stderr)
		return root.ExecuteContext(ctx)
	}()
	if err != nil {
		fmt.Fprintln(stderr, err)
	}
	return err
}

// resultArgv is a workspace command that reads the uploaded seed and the
// file its setup made, and writes them as its result.
var resultArgv = []string{"/bin/sh", "-c", "mkdir out && cat seed.txt prepared >out/result.txt && cat out/result.txt"}

// workspaceTask returns a task running argv in the uploaded workdir
// input after a setup that makes the file prepared, keeping out.
func workspaceTask(input artifact.Ref, argv []string) ([]byte, error) {
	return json.Marshal(map[string]any{
		"argv":            argv,
		"setup":           []string{"/bin/sh", "-c", "printf 'prepared\\n' >prepared"},
		"workdir":         input,
		"outputs":         []string{"out"},
		"timeout_seconds": 120,
	})
}

// jobResult returns the result of job id, which must have succeeded on
// target dgx with its result available.
func jobResult(ctx context.Context, cc skyv1connect.ControlServiceClient, token, id string) (artifact.Ref, error) {
	resp, err := cc.GetJob(ctx, authorized(token, &v1.GetJobRequest{Id: id}))
	if err != nil {
		return artifact.Ref{}, err
	}
	job := resp.Msg.Job
	if job.Target != "dgx" || job.State != v1.JobState_JOB_STATE_SUCCEEDED {
		return artifact.Ref{}, fmt.Errorf("job = %v", job)
	}
	result := job.Result
	if result == nil || result.State != v1.ArtifactResultState_ARTIFACT_RESULT_STATE_AVAILABLE || result.Reference == nil {
		return artifact.Ref{}, fmt.Errorf("job result = %v", result)
	}
	ref := artifact.Ref{ID: result.Reference.Id, SHA256: result.Reference.Sha256, Size: result.Reference.Size}
	return ref, ref.Validate()
}

// awaitCredential calls an authenticated RPC with token until the agent
// accepts it or, if accepted is false, refuses it as unauthenticated. The
// agent's auth engine rereads its auth file once a second, so a
// credential issued or revoked on the host takes effect only at a later
// refresh. Until then only the opposite outcome is expected; any other
// error fails the test.
func awaitCredential(ctx context.Context, t *testing.T, cc skyv1connect.ControlServiceClient, token string, accepted bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		_, err := cc.ListJobs(ctx, authorized(token, &v1.ListJobsRequest{PageSize: 1}))
		unauthenticated := connect.CodeOf(err) == connect.CodeUnauthenticated
		if accepted && err == nil || !accepted && unauthenticated {
			return
		}
		if err != nil && !unauthenticated {
			t.Fatalf("credential check: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("credential accepted=%v after 30s, want %v: %v", err == nil, accepted, err)
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

// TestAgentInstallationCLI serves a real operator-initialized installation
// with the agent and server.Run, and drives it with the canonical CLI as
// skygo-ssh does: a workspace job uploads a small workdir, its setup makes
// a file the command reads, and the result downloads with exact bytes.
// Other principals, a new and a revoked credential, cancellation, a
// restart and mismatched installations follow.
//
// The installation keeps the initialized default: retention disabled, so
// jobs logs reads the legacy log stream.
func TestAgentInstallationCLI(t *testing.T) {
	testAgentInstallationCLI(t, false)
}

// TestAgentInstallationCLIRetained runs TestAgentInstallationCLI's body
// against an installation whose operator enabled retention on stopped
// state before the agent first served it, so jobs logs reads the
// retained log stream.
func TestAgentInstallationCLIRetained(t *testing.T) {
	testAgentInstallationCLI(t, true)
}

// testAgentInstallationCLI is the body of TestAgentInstallationCLI and
// TestAgentInstallationCLIRetained. Each runs in its own fixture, with
// its own installations, agent and request domain.
func testAgentInstallationCLI(t *testing.T, retained bool) {
	t.Setenv("SKYGO_TOKEN", "")
	f := newFixture(t)
	in := f.newInstallation("installation")
	activated := ""
	if retained {
		activated = f.enableRetention(in)
	}
	dir, id := f.agentDir()
	address := freeAddress(t)
	args := installationArgs(dir, address, in.Installation)
	stop := f.serveAgent(args)
	hc := pinnedClient(t, id.cert, address)
	endpoint := "https://" + agentName
	cc := skyv1connect.NewControlServiceClient(hc, endpoint)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	running := func(context.Context) (bootstrap.State, error) { return bootstrap.State{Agent: "running"}, nil }
	admin := bearer(t, in.admin)
	if err := waitReady(ctx, hc, endpoint, admin, "dgx", true, running); err != nil {
		t.Fatal(err)
	}
	// The agent serves the auth file, not the agent directory's legacy token.
	if err := checkReady(ctx, cc, id.bearer, "dgx", false); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf("legacy token = %v, want unauthenticated", err)
	}

	// An explicit request ID on a retained installation needs the explicit
	// request domain it was first given; the CLI never binds one to the
	// current epoch. The test reads the agent's domain once, before any
	// mutation, and every explicit request and replay reuses exactly that
	// text, even after the restart.
	var lineage, domain string
	if retained {
		r, err := cc.GetRetention(ctx, authorized(admin, &v1.GetRetentionRequest{}))
		if err != nil {
			t.Fatal(err)
		}
		d, until := r.Msg.Domain, r.Msg.AcceptUntil
		if !r.Msg.Enabled || d == nil || len(d.Lineage) != 32 || strings.Trim(d.Lineage, "0123456789abcdef") != "" || d.Epoch == 0 {
			t.Fatalf("GetRetention enabled=%v domain present=%v, want enabled with a lowercase hexadecimal lineage and positive epoch", r.Msg.Enabled, d != nil)
		}
		if until == nil || until.CheckValid() != nil || !time.Now().Before(until.AsTime()) {
			t.Fatalf("GetRetention accept-until = %v, want a valid future time", until)
		}
		lineage = d.Lineage
		domain = lineage + ":" + strconv.FormatUint(d.Epoch, 10)
		if l, _, _ := strings.Cut(activated, ":"); l != lineage {
			t.Fatal("served request lineage differs from the activation readback")
		}
	}

	skygo := func(tokenFile string, args ...string) (string, string, error) {
		root, err := agentCommand(hc, "dgx", "dgx", tokenFile, args)
		if err != nil {
			t.Fatal(err)
		}
		var out, errout bytes.Buffer
		root.SetOut(&out)
		root.SetErr(&errout)
		err = root.ExecuteContext(ctx)
		return out.String(), errout.String(), err
	}
	// runID runs an explicit request ID: on a retained installation with
	// the captured request domain, otherwise as given.
	runID := func(tokenFile string, args ...string) (string, string, error) {
		if retained {
			args = append(args[:len(args):len(args)], "--request-domain", domain)
		}
		return skygo(tokenFile, args...)
	}
	jobs := func(token string) int {
		t.Helper()
		resp, err := cc.ListJobs(ctx, authorized(token, &v1.ListJobsRequest{PageSize: 100}))
		if err != nil {
			t.Fatal(err)
		}
		return len(resp.Msg.Jobs)
	}
	getJob := func(id string) *v1.Job {
		t.Helper()
		resp, err := cc.GetJob(ctx, authorized(admin, &v1.GetJobRequest{Id: id}))
		if err != nil {
			t.Fatal(err)
		}
		return resp.Msg.Job
	}
	awaitJob := func(id string, done func(*v1.Job) bool) *v1.Job {
		t.Helper()
		deadline := time.Now().Add(60 * time.Second)
		for {
			job := getJob(id)
			if done(job) {
				return job
			}
			if time.Now().After(deadline) {
				t.Fatalf("job %s = %v", id, job)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Upload a small workdir: well below the 64 KiB canonical chunk and
	// the transport's 1 MiB request bound.
	work := f.mkdir("work")
	source := filepath.Join(work, "source")
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(source, "seed.txt"), "seed\n")
	out, errout, err := skygo(in.admin, "artifacts", "upload", source)
	if err != nil {
		t.Fatalf("upload: %v\n%s", err, errout)
	}
	var input artifact.Ref
	if err := json.Unmarshal([]byte(out), &input); err != nil || input.Validate() != nil {
		t.Fatalf("upload reference %q: %v", out, err)
	}
	if input.Size <= 0 || input.Size >= 64<<10 {
		t.Fatalf("workdir snapshot is %d bytes, want a small bounded fixture", input.Size)
	}
	uploadID := ""
	for _, line := range strings.Split(errout, "\n") {
		if s, ok := strings.CutPrefix(line, "upload-id: "); ok {
			uploadID = s
		}
	}
	if uploadID == "" {
		t.Fatalf("no upload ID in %q", errout)
	}

	writeTask := func(name string, argv []string) string {
		t.Helper()
		data, err := workspaceTask(input, argv)
		if err != nil {
			t.Fatal(err)
		}
		file := filepath.Join(work, name)
		write(t, file, string(data))
		return file
	}
	task := writeTask("task.json", resultArgv)
	if retained {
		// A new explicit request ID without its domain, or with another
		// lineage at the served epoch, is refused with its typed reason
		// before any job exists.
		before := jobs(admin)
		_, _, err := skygo(in.admin, "run", "-f", task, "--wait", "--request-id", "ssh-unbound")
		if connect.CodeOf(err) != connect.CodeFailedPrecondition || expiryReason(err) != v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_REQUEST_DOMAIN_REQUIRED {
			t.Fatalf("run without a request domain = %v (code %v, reason %v), want explicit request domain required", err, connect.CodeOf(err), expiryReason(err))
		}
		other := []byte(lineage)
		if other[0] == '0' {
			other[0] = '1'
		} else {
			other[0] = '0'
		}
		_, epoch, _ := strings.Cut(domain, ":")
		_, _, err = skygo(in.admin, "run", "-f", task, "--wait", "--request-id", "ssh-other-lineage", "--request-domain", string(other)+":"+epoch)
		if connect.CodeOf(err) != connect.CodeFailedPrecondition || expiryReason(err) != v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_REQUEST_LINEAGE_CHANGED {
			t.Fatalf("run with another request lineage = %v (code %v, reason %v), want request lineage changed", err, connect.CodeOf(err), expiryReason(err))
		}
		if n := jobs(admin); n != before {
			t.Fatalf("refused requests changed the job count from %d to %d", before, n)
		}
	}
	out, errout, err = runID(in.admin, "run", "-f", task, "--wait", "--request-id", "ssh-workspace")
	if err != nil || !strings.Contains(out, "\tdgx\tsucceeded") {
		t.Fatalf("run = %q, %v\n%s", out, err, errout)
	}
	ran := out
	jobID, _, _ := strings.Cut(out, "\t")
	output, err := jobResult(ctx, cc, admin, jobID)
	if err != nil {
		t.Fatal(err)
	}
	refData, err := json.Marshal(output)
	if err != nil {
		t.Fatal(err)
	}
	refFile := filepath.Join(work, "result.json")
	write(t, refFile, string(refData))
	download := func(tokenFile, dest string) error {
		_, errout, err := skygo(tokenFile, "artifacts", "download", "--ref", refFile, dest)
		if err != nil {
			return errors.New(err.Error() + "\n" + errout)
		}
		return nil
	}
	const want = "seed\nprepared\n"
	checkResult := func(dest string) {
		t.Helper()
		got, err := os.ReadFile(filepath.Join(dest, "out", "result.txt"))
		if err != nil || string(got) != want {
			t.Fatalf("downloaded result = %q, %v; want %q", got, err, want)
		}
	}
	if err := download(in.admin, filepath.Join(work, "result")); err != nil {
		t.Fatal(err)
	}
	checkResult(filepath.Join(work, "result"))
	if out, _, err := skygo(in.admin, "jobs", "logs", jobID); err != nil || out != want {
		t.Fatalf("logs = %q, %v", out, err)
	}
	// replay resubmits the retained workspace request with the same ID and
	// captured domain: the same job answers with the same line, its result
	// unchanged, and no job is added.
	replay := func(when string) {
		t.Helper()
		before := jobs(admin)
		out, errout, err := runID(in.admin, "run", "-f", task, "--wait", "--request-id", "ssh-workspace")
		if err != nil || out != ran {
			t.Fatalf("replay %s = %q, %v; want %q\n%s", when, out, err, ran, errout)
		}
		if j := getJob(jobID); j.State != v1.JobState_JOB_STATE_SUCCEEDED || j.Result == nil || j.Result.Reference.GetSha256() != output.SHA256 {
			t.Fatalf("job after replay %s = %v", when, j)
		}
		if n := jobs(admin); n != before {
			t.Fatalf("replay %s changed the job count from %d to %d", when, before, n)
		}
	}
	if retained {
		replay("before restart")
	}

	// Another principal cannot see the admin's upload, and a reader can
	// neither upload nor run.
	if _, _, err := skygo(in.admin, "artifacts", "status", uploadID); err != nil {
		t.Fatalf("owner upload status: %v", err)
	}
	if _, _, err := skygo(in.bob, "artifacts", "status", uploadID); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("another principal's upload status = %v, want not found", err)
	}
	adminJobs, carolJobs := jobs(admin), jobs(bearer(t, in.carol))
	if _, _, err := skygo(in.carol, "artifacts", "upload", source); err == nil {
		t.Fatal("reader uploaded")
	}
	if _, _, err := runID(in.carol, "run", "-f", task, "--request-id", "ssh-reader"); err == nil {
		t.Fatal("reader ran a job")
	} else if r := expiryReason(err); r != 0 {
		t.Fatalf("reader run refused as %v, want a refusal of the reader", r)
	}
	if jobs(admin) != adminJobs || jobs(bearer(t, in.carol)) != carolJobs {
		t.Fatal("refused reader created a job")
	}

	// A credential issued on the host works once the agent refreshes its
	// auth file. Revoked, it stops working at a later refresh, for
	// uploads, runs and downloads alike; others keep working.
	refreshed := filepath.Join(work, "bob2.token")
	out = f.operate("auth", "token", "issue", "bob", "--output", refreshed, "--auth-file", in.AuthFile)
	tokenID, _, _ := strings.Cut(out, "\t")
	issued := bearer(t, refreshed)
	awaitCredential(ctx, t, cc, issued, true)
	if _, _, err := skygo(refreshed, "targets", "list"); err != nil {
		t.Fatalf("new credential: %v", err)
	}
	f.operate("auth", "token", "revoke", tokenID, "--auth-file", in.AuthFile)
	awaitCredential(ctx, t, cc, issued, false)
	bob := bearer(t, in.bob)
	adminJobs, bobJobs := jobs(admin), jobs(bob)
	if _, _, err := skygo(refreshed, "artifacts", "upload", source); err == nil {
		t.Fatal("revoked credential uploaded")
	}
	if _, _, err := runID(refreshed, "run", "-f", task, "--request-id", "ssh-revoked"); err == nil {
		t.Fatal("revoked credential ran a job")
	} else if r := expiryReason(err); r != 0 {
		t.Fatalf("revoked credential run refused as %v, want a refusal of the credential", r)
	}
	revokedDest := filepath.Join(work, "revoked")
	if err := download(refreshed, revokedDest); err == nil {
		t.Fatal("revoked credential downloaded the result")
	}
	if _, err := os.Lstat(revokedDest); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("revoked download created %s: %v", revokedDest, err)
	}
	if jobs(admin) != adminJobs || jobs(bob) != bobJobs {
		t.Fatal("revoked credential created a job")
	}
	if _, _, err := skygo(in.bob, "targets", "list"); err != nil {
		t.Fatalf("unrevoked credential of the same principal: %v", err)
	}

	// Interrupting a client is not cancellation: the job keeps running
	// until canceled through the canonical API.
	long := writeTask("long.json", []string{"/bin/sh", "-c", "exec sleep 120"})
	out, errout, err = runID(in.admin, "run", "-f", long, "--request-id", "ssh-cancel")
	if err != nil {
		t.Fatalf("run = %q, %v\n%s", out, err, errout)
	}
	longID, _, _ := strings.Cut(out, "\t")
	awaitJob(longID, func(j *v1.Job) bool { return j.State == v1.JobState_JOB_STATE_RUNNING })
	// The interruption is a MODEL: a deadline on the client's context over
	// pinnedClient, not the teardown of an SSH connection. With --follow
	// the canonical CLI sets no timeout of its own.
	// The intended oracle: the call returns only with the follow deadline
	// as its error. The follow context's state is read as the call
	// returns, before anything cancels it, so a call that returns early is
	// reported with its own error, not a later cancellation.
	//
	// Connect sends that deadline as Connect-Timeout-Ms, and the agent's
	// handler starts its own timer from it, so the agent's copy of the
	// deadline can end the call before the local timer fires. The follow
	// alone runs over a copy of hc whose transport records each RPC's
	// procedure, request deadline and timeout header, which checkFollowTrace
	// and checkDeadlineSource hold to the canonical CLI's calls and to that
	// deadline before the test waits for the local timer.
	//
	// The installation's policy is the test's: the agent must report it,
	// and the follow must take the one stream readLogs selects for it.
	retention, err := cc.GetRetention(ctx, authorized(admin, &v1.GetRetentionRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if retention.Msg.Enabled != retained {
		t.Fatalf("GetRetention enabled = %v, want %v", retention.Msg.Enabled, retained)
	}
	stream := skyv1connect.ControlServiceStreamJobLogsProcedure
	if retained {
		stream = skyv1connect.ControlServiceStreamRetainedJobLogsProcedure
	}
	trace := &followTrace{}
	traced := *hc
	traced.Transport = traceTransport{hc.Transport, trace}
	follow, stopFollow := context.WithTimeout(ctx, time.Second)
	defer stopFollow()
	deadline, _ := follow.Deadline()
	root, err := agentCommand(&traced, "dgx", "dgx", in.admin, []string{"jobs", "logs", "--follow", longID})
	if err != nil {
		t.Fatal(err)
	}
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	start := time.Now()
	err = root.ExecuteContext(follow)
	followErr, followCause, ctxErr := follow.Err(), context.Cause(follow), ctx.Err()
	returned := time.Now()
	returnedAs := fmt.Sprintf("jobs logs --follow returned %v (code %v) at +%v; then follow context = %v (cause %v), test context = %v", err, connect.CodeOf(err), returned.Sub(start), followErr, followCause, ctxErr)
	calls := trace.calls()
	t.Logf("follow: deadline +%v from execute; retention enabled=%v (want %v), stream %s; %s", deadline.Sub(start), retention.Msg.Enabled, retained, stream, returnedAs)
	for i, c := range calls {
		t.Logf("follow RPC %d: %s", i, c.describe(start))
	}
	if e := followDeadline(err); e != nil || ctxErr != nil {
		t.Fatalf("interrupted jobs logs --follow: %v; %s", e, returnedAs)
	}
	if e := checkFollowTrace(calls, stream, deadline, start); e != nil {
		t.Fatalf("follow RPCs: %v; %s", e, returnedAs)
	}
	if e := checkDeadlineSource(followErr, followCause, returned, calls[len(calls)-1]); e != nil {
		t.Fatalf("follow deadline: %v; %s", e, returnedAs)
	}
	select {
	case <-follow.Done():
	case <-ctx.Done():
		t.Fatalf("follow deadline never fired: test context %v; %s", ctx.Err(), returnedAs)
	}
	if !errors.Is(follow.Err(), context.DeadlineExceeded) || !errors.Is(context.Cause(follow), context.DeadlineExceeded) || ctx.Err() != nil {
		t.Fatalf("follow context = %v (cause %v), test context = %v; want only the follow deadline; %s", follow.Err(), context.Cause(follow), ctx.Err(), returnedAs)
	}
	stopFollow()
	if j := getJob(longID); j.State != v1.JobState_JOB_STATE_RUNNING || j.CancelRequested {
		t.Fatalf("interrupted client changed the job: %v", j)
	}
	if _, _, err := skygo(in.admin, "jobs", "cancel", longID, "--yes"); err != nil {
		t.Fatal(err)
	}
	awaitJob(longID, func(j *v1.Job) bool { return j.State == v1.JobState_JOB_STATE_CANCELED })

	// A restarted agent serves the same installation: earlier jobs and the
	// retained result are unchanged.
	adminJobs = jobs(admin)
	stop()
	stop = f.serveAgent(args)
	if err := waitReady(ctx, hc, endpoint, admin, "dgx", true, running); err != nil {
		t.Fatal(err)
	}
	if again := getJob(jobID); again.State != v1.JobState_JOB_STATE_SUCCEEDED || again.Result == nil || again.Result.Reference.GetSha256() != output.SHA256 {
		t.Fatalf("job after restart = %v", again)
	}
	if again := getJob(longID); again.State != v1.JobState_JOB_STATE_CANCELED {
		t.Fatalf("canceled job after restart = %v", again)
	}
	if err := download(in.admin, filepath.Join(work, "result-again")); err != nil {
		t.Fatal(err)
	}
	checkResult(filepath.Join(work, "result-again"))
	if retained {
		replay("after restart")
	}
	if n := jobs(admin); n != adminJobs {
		t.Fatalf("restart changed the job count from %d to %d", adminJobs, n)
	}
	stop()

	// The server refuses another installation's auth file, artifact root or
	// state, an uninitialized root, or a zero-byte jobs.db, before serving;
	// nothing of either installation, the zero-byte state or the bystander
	// changes, including each jobs.db's bytes and identity, and no job
	// starts. Each refusal must be an ordinary one that server.Run settled;
	// refused holds the fixture otherwise, and no later Run follows.
	other := f.newInstallation("other")
	empty := f.mkdir("empty")
	bystander := f.mkdir("bystander")
	write(t, filepath.Join(bystander, "keep"), "bystander\n")
	// The zero-byte jobs.db is a MODELED fixture, not a genuine historical
	// writer's: it passes the agent's existence and privacy checks, and the
	// server's existing installation route must refuse it as not current
	// without initializing it.
	zero := f.mkdir("zero")
	zeroData := f.mkdir(filepath.Join("zero", "data"))
	write(t, filepath.Join(zeroData, "jobs.db"), "")
	mismatches := []struct {
		name string
		in   bootstrap.Installation
		want error // nil: any ordinary refusal
	}{
		{"other_auth", bootstrap.Installation{DataDir: in.DataDir, AuthFile: other.AuthFile, ArtifactDir: in.ArtifactDir}, nil},
		{"other_artifacts", bootstrap.Installation{DataDir: in.DataDir, AuthFile: in.AuthFile, ArtifactDir: other.ArtifactDir}, nil},
		{"uninitialized_artifacts", bootstrap.Installation{DataDir: in.DataDir, AuthFile: in.AuthFile, ArtifactDir: empty}, nil},
		{"other_data", bootstrap.Installation{DataDir: other.DataDir, AuthFile: in.AuthFile, ArtifactDir: in.ArtifactDir}, nil},
		{"zero_byte_jobs_db", bootstrap.Installation{DataDir: zeroData, AuthFile: in.AuthFile, ArtifactDir: in.ArtifactDir}, store.ErrNotCurrent},
	}
	// Each installation's whole directory: data (jobs.db and everything
	// the server keeps beside it), auth file, artifact root and tokens.
	watched := []string{filepath.Dir(in.DataDir), filepath.Dir(other.DataDir), zero, empty, bystander}
	for _, m := range mismatches {
		before := map[string]map[string]string{}
		for _, p := range watched {
			before[p] = identityTree(t, p)
		}
		auth, otherAuth := readFile(t, in.AuthFile), readFile(t, other.AuthFile)
		err := f.refused(ctx, installationArgs(dir, address, m.in))
		if err == nil {
			t.Fatalf("%s: agent served a mismatched installation", m.name)
		}
		if m.want != nil && !errors.Is(err, m.want) {
			t.Errorf("%s: refusal = %v, want %v", m.name, err, m.want)
		}
		for _, p := range watched {
			if after := identityTree(t, p); !maps.Equal(before[p], after) {
				t.Fatalf("%s: refusal changed %s from\n%v\nto\n%v", m.name, p, before[p], after)
			}
		}
		if readFile(t, in.AuthFile) != auth || readFile(t, other.AuthFile) != otherAuth {
			t.Fatalf("%s: refusal changed an auth file", m.name)
		}
	}
	stop = f.serveAgent(args)
	if err := waitReady(ctx, hc, endpoint, admin, "dgx", true, running); err != nil {
		t.Fatal(err)
	}
	if n := jobs(admin); n != adminJobs {
		t.Fatalf("refused starts changed the job count from %d to %d", adminJobs, n)
	}
	stop()
}

// TestAgentInstallationScript runs testdata/installation.txt against a
// real installation served by the agent and server.Run. Its skygo-ssh
// command runs skygo-ssh's own argument checks and agentCommand with a
// principal's scoped token; only the SSH channel is a MODEL (pinnedClient).
func TestAgentInstallationScript(t *testing.T) {
	t.Setenv("SKYGO_TOKEN", "")
	f := newFixture(t)
	in := f.newInstallation("installation")
	dir, id := f.agentDir()
	address := freeAddress(t)
	stop := f.serveAgent(installationArgs(dir, address, in.Installation))
	hc := pinnedClient(t, id.cert, address)
	endpoint := "https://" + agentName
	cc := skyv1connect.NewControlServiceClient(hc, endpoint)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	admin := bearer(t, in.admin)
	running := func(context.Context) (bootstrap.State, error) { return bootstrap.State{Agent: "running"}, nil }
	if err := waitReady(ctx, hc, endpoint, admin, "dgx", true, running); err != nil {
		t.Fatal(err)
	}

	tokens := map[string]string{"admin": in.admin, "bob": in.bob, "carol": in.carol}
	e := script.NewEngine()
	e.Cmds = scripttest.DefaultCmds()
	e.Conds = scripttest.DefaultConds()
	e.Cmds["skygo-ssh"] = script.Command(script.CmdUsage{Summary: "run skygo-ssh's client path against the agent with a principal's scoped token", Args: "admin|bob|carol skygo-args..."}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) < 1 || tokens[args[0]] == "" {
			return nil, script.ErrUsage
		}
		var out, errout bytes.Buffer
		err := agentClient(s.Context(), hc, tokens[args[0]], args[1:], &out, &errout)
		return func(*script.State) (string, string, error) { return out.String(), errout.String(), err }, nil
	})
	e.Cmds["task"] = script.Command(script.CmdUsage{Summary: "write a workspace task for an uploaded workdir reference", Args: "input.json task.json"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		data, err := os.ReadFile(s.Path(args[0]))
		if err != nil {
			return nil, err
		}
		var input artifact.Ref
		if err := json.Unmarshal(data, &input); err != nil {
			return nil, err
		}
		if err := input.Validate(); err != nil {
			return nil, err
		}
		task, err := workspaceTask(input, resultArgv)
		if err != nil {
			return nil, err
		}
		return nil, os.WriteFile(s.Path(args[1]), task, 0o600)
	})
	e.Cmds["job-result"] = script.Command(script.CmdUsage{Summary: "write the result reference of the job a run printed, which must have succeeded on dgx", Args: "run.txt result.json"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		data, err := os.ReadFile(s.Path(args[0]))
		if err != nil {
			return nil, err
		}
		jobID, _, _ := strings.Cut(string(data), "\t")
		ref, err := jobResult(s.Context(), cc, admin, jobID)
		if err != nil {
			return nil, err
		}
		b, err := json.Marshal(ref)
		if err != nil {
			return nil, err
		}
		return nil, os.WriteFile(s.Path(args[1]), b, 0o600)
	})
	e.Cmds["jobs"] = script.Command(script.CmdUsage{Summary: "check the number of jobs the admin sees", Args: "count"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		want, err := strconv.Atoi(args[0])
		if err != nil {
			return nil, err
		}
		resp, err := cc.ListJobs(s.Context(), authorized(admin, &v1.ListJobsRequest{PageSize: 100}))
		if err != nil {
			return nil, err
		}
		if got := len(resp.Msg.Jobs); got != want {
			return nil, fmt.Errorf("admin sees %d jobs, want %d", got, want)
		}
		return nil, nil
	})

	work := f.mkdir("work")
	s, err := script.NewState(ctx, work, []string{"WORK=" + work, "PATH=" + os.Getenv("PATH")})
	if err != nil {
		t.Fatal(err)
	}
	// As scripttest.Test does, the archive's files are extracted into the
	// state's working directory and only its comment runs as the script.
	file := filepath.Join("testdata", "installation.txt")
	archive, err := txtar.ParseFile(file)
	if err != nil {
		s.CloseAndWait(io.Discard)
		t.Fatal(err)
	}
	if err := s.ExtractFiles(archive); err != nil {
		s.CloseAndWait(io.Discard)
		t.Fatal(err)
	}
	scripttest.Run(t, e, s, file, bytes.NewReader(archive.Comment))
	stop()
}

// identityTree is tree with each path's device, inode and size as well as
// its mode and, for a regular file, content digest, so a refusal that
// replaces a file, even with the same bytes, is seen.
func identityTree(t *testing.T, root string) map[string]string {
	t.Helper()
	m := map[string]string{}
	err := filepath.WalkDir(root, func(p string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := os.Lstat(p)
		if err != nil {
			return err
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("%s: no stat", p)
		}
		rel, _ := filepath.Rel(root, p)
		e := fmt.Sprintf("%v dev=%d ino=%d size=%d", info.Mode(), st.Dev, st.Ino, info.Size())
		if info.Mode().IsRegular() {
			data, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			e += fmt.Sprintf(" sha256=%x", sha256.Sum256(data))
		}
		m[rel] = e
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func readFile(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// connectTimeoutHeader is the header in which a Connect client sends its
// context's remaining time, floored to whole milliseconds
// (connect@v1.18.1 protocol_connect.go NewConn), and from which the
// handler starts its own timer (SetTimeout).
const connectTimeoutHeader = "Connect-Timeout-Ms"

// An rpcCall is what traceTransport records of one request. It keeps no
// credential, other header, query, request or response bytes: only the
// procedure path, method, request context deadline, timeout header,
// RoundTrip times and status, and counts of response body reads.
type rpcCall struct {
	procedure   string // URL path
	method      string
	deadline    time.Time
	hasDeadline bool
	timeout     string // the Connect-Timeout-Ms value sent
	timeouts    int    // number of Connect-Timeout-Ms values
	start, end  time.Time
	failed      bool // RoundTrip returned an error
	status      int
	reads       int
	bytes       int64
	readEnd     string // how the last read that failed ended
	closed      bool
}

func (c rpcCall) describe(origin time.Time) string {
	return fmt.Sprintf("%s %s deadline=%v(+%v) %s=%q(n=%d) roundtrip=+%v..+%v failed=%v status=%d reads=%d bytes=%d readEnd=%q closed=%v",
		c.method, c.procedure, c.hasDeadline, c.deadline.Sub(origin), connectTimeoutHeader, c.timeout, c.timeouts,
		c.start.Sub(origin), c.end.Sub(origin), c.failed, c.status, c.reads, c.bytes, c.readEnd, c.closed)
}

// A followTrace collects the rpcCalls of one command. The HTTP client
// reads response bodies on its own goroutines, so every access holds mu.
type followTrace struct {
	mu   sync.Mutex
	list []*rpcCall
}

// calls returns a copy of the calls recorded so far, in RoundTrip order.
func (t *followTrace) calls() []rpcCall {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]rpcCall, len(t.list))
	for i, c := range t.list {
		out[i] = *c
	}
	return out
}

// A traceTransport records each request in trace and passes it to next
// unchanged. The response, its error and its body's reads and close are
// next's, only counted on the way.
type traceTransport struct {
	next  http.RoundTripper
	trace *followTrace
}

func (t traceTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	c := &rpcCall{procedure: req.URL.Path, method: req.Method, timeout: req.Header.Get(connectTimeoutHeader), timeouts: len(req.Header.Values(connectTimeoutHeader))}
	c.deadline, c.hasDeadline = req.Context().Deadline()
	c.start = time.Now()
	t.trace.mu.Lock()
	t.trace.list = append(t.trace.list, c)
	t.trace.mu.Unlock()
	resp, err := t.next.RoundTrip(req)
	t.trace.mu.Lock()
	c.end = time.Now()
	c.failed = err != nil
	if resp != nil {
		c.status = resp.StatusCode
	}
	t.trace.mu.Unlock()
	if resp != nil && resp.Body != nil {
		resp.Body = traceBody{resp.Body, t.trace, c}
	}
	return resp, err
}

// A traceBody counts the reads of a response body.
type traceBody struct {
	body  io.ReadCloser
	trace *followTrace
	call  *rpcCall
}

func (b traceBody) Read(p []byte) (int, error) {
	n, err := b.body.Read(p)
	b.trace.mu.Lock()
	b.call.reads++
	b.call.bytes += int64(n)
	switch {
	case err == nil:
	case err == io.EOF:
		b.call.readEnd = "EOF"
	case errors.Is(err, context.DeadlineExceeded):
		b.call.readEnd = "deadline"
	case errors.Is(err, context.Canceled):
		b.call.readEnd = "canceled"
	default:
		b.call.readEnd = "other"
	}
	b.trace.mu.Unlock()
	return n, err
}

func (b traceBody) Close() error {
	err := b.body.Close()
	b.trace.mu.Lock()
	b.call.closed = true
	b.trace.mu.Unlock()
	return err
}

// followDeadline reports whether err is a deadline: a context deadline or
// a Connect deadline_exceeded, never a cancellation or another failure.
func followDeadline(err error) error {
	switch {
	case err == nil:
		return errors.New("returned nil, want its deadline")
	case errors.Is(err, context.Canceled) || connect.CodeOf(err) == connect.CodeCanceled:
		return fmt.Errorf("canceled (%w), want its deadline", err)
	case errors.Is(err, context.DeadlineExceeded) || connect.CodeOf(err) == connect.CodeDeadlineExceeded:
		return nil
	}
	return fmt.Errorf("%w (code %v), want its deadline", err, connect.CodeOf(err))
}

// timeoutHeader returns the duration of c's Connect-Timeout-Ms, which
// must be one canonical positive decimal, as NewConn writes it.
func timeoutHeader(c rpcCall) (time.Duration, error) {
	ms, err := strconv.ParseInt(c.timeout, 10, 64)
	if c.timeouts != 1 || err != nil || ms <= 0 || len(c.timeout) > 10 || strconv.FormatInt(ms, 10) != c.timeout {
		return 0, fmt.Errorf("%s: %s %q (%d values), want one positive millisecond count", c.procedure, connectTimeoutHeader, c.timeout, c.timeouts)
	}
	return time.Duration(ms) * time.Millisecond, nil
}

// checkFollowTrace reports whether calls are exactly the RPCs readLogs
// (internal/cli/retention_logs.go) makes for jobs logs --follow with no
// cursor or attempt: GetRetention, whose context keeps the earlier
// follow deadline, and once it has returned, the one stream its answer
// selects. No other procedure and no retry may appear.
//
// Each call must carry deadline, the follow context's, in its request
// context, and a timeout header encoded from it. NewConn computes
// H = floor((deadline-T)/ms) at a time T after the command started at
// start and before the call's RoundTrip began, so H*ms <= deadline-start
// and deadline-RoundTrip start < (H+1)*ms. Neither bound assumes how
// long encoding or RoundTrip took.
func checkFollowTrace(calls []rpcCall, stream string, deadline, start time.Time) error {
	want := []string{skyv1connect.ControlServiceGetRetentionProcedure, stream}
	if len(calls) != len(want) {
		got := make([]string, len(calls))
		for i, c := range calls {
			got[i] = c.procedure
		}
		return fmt.Errorf("RPCs %q, want %q", got, want)
	}
	for i, c := range calls {
		if c.procedure != want[i] || c.method != http.MethodPost {
			return fmt.Errorf("RPC %d is %s %s, want POST %s", i, c.method, c.procedure, want[i])
		}
		if !c.hasDeadline || !c.deadline.Equal(deadline) {
			return fmt.Errorf("%s: request deadline %v (set %v), want the follow deadline %v", c.procedure, c.deadline, c.hasDeadline, deadline)
		}
		h, err := timeoutHeader(c)
		if err != nil {
			return err
		}
		if c.start.Before(start) {
			return fmt.Errorf("%s: RoundTrip began %v before the command", c.procedure, start.Sub(c.start))
		}
		if h > deadline.Sub(start) {
			return fmt.Errorf("%s: %s %v exceeds the %v the follow deadline left when the command started", c.procedure, connectTimeoutHeader, h, deadline.Sub(start))
		}
		if left := deadline.Sub(c.start); left >= h+time.Millisecond {
			return fmt.Errorf("%s: %s %v is shorter than the %v the follow deadline left at RoundTrip", c.procedure, connectTimeoutHeader, h, left)
		}
	}
	if r, s := calls[0], calls[1]; r.failed || r.status != http.StatusOK || r.end.After(s.start) {
		return fmt.Errorf("GetRetention failed=%v status=%d, ended +%v after the stream began; want it answered first", r.failed, r.status, r.end.Sub(s.start))
	}
	return nil
}

// checkStreamEntry reports whether the follow entered its stream: the
// stream's RoundTrip returned an HTTP 200 response after it began, and
// the command read at least one byte of its body. How the last read
// ended and whether the body was closed are diagnostics only.
func checkStreamEntry(c rpcCall) error {
	if c.failed || c.status != http.StatusOK || c.start.IsZero() || !c.end.After(c.start) || c.reads <= 0 || c.bytes <= 0 {
		return fmt.Errorf("%s: stream not entered: failed=%v status=%d roundtrip=%v reads=%d bytes=%d; want an HTTP 200 response and a read of at least one byte", c.procedure, c.failed, c.status, c.end.Sub(c.start), c.reads, c.bytes)
	}
	return nil
}

// checkDeadlineSource reports whether the deadline the follow returned
// with is one of the follow deadline's two copies, after the follow
// entered its stream (checkStreamEntry): a context that ended locally
// does not excuse a stream that never answered. If the local context
// had already ended, it must have ended by its deadline. Otherwise the
// agent's copy ended it: the handler starts its timer for the stream's
// Connect-Timeout-Ms only after the request reached it, after the
// stream's RoundTrip began, so the call cannot return before that start
// plus the header's duration, and the deadline must have come back in
// the stream's response.
func checkDeadlineSource(followErr, followCause error, returned time.Time, stream rpcCall) error {
	if followErr != nil && (!errors.Is(followErr, context.DeadlineExceeded) || !errors.Is(followCause, context.DeadlineExceeded)) {
		return fmt.Errorf("follow context ended with %v (cause %v), not its deadline", followErr, followCause)
	}
	if err := checkStreamEntry(stream); err != nil {
		return err
	}
	if followErr != nil {
		return nil
	}
	h, err := timeoutHeader(stream)
	if err != nil {
		return err
	}
	if early := stream.start.Add(h).Sub(returned); early > 0 {
		return fmt.Errorf("%s: agent deadline returned %v before its %s %v could expire", stream.procedure, early, connectTimeoutHeader, h)
	}
	return nil
}

// TestFollowTraceChecks holds followDeadline, checkFollowTrace and
// checkDeadlineSource to MODELED traces and errors, not to an actual
// wire: TestAgentInstallationCLI and TestAgentInstallationCLIRetained
// apply them to the RPCs they observe.
func TestFollowTraceChecks(t *testing.T) {
	start := time.Unix(1_000_000, 0)
	deadline := start.Add(time.Second)
	ms := func(f float64) time.Time { return start.Add(time.Duration(f * float64(time.Millisecond))) }
	legacy := skyv1connect.ControlServiceStreamJobLogsProcedure
	valid := func() []rpcCall {
		return []rpcCall{
			{procedure: skyv1connect.ControlServiceGetRetentionProcedure, method: http.MethodPost, deadline: deadline, hasDeadline: true, timeout: "998", timeouts: 1, start: ms(1.5), end: ms(3), status: http.StatusOK, reads: 2, bytes: 40, readEnd: "EOF", closed: true},
			{procedure: legacy, method: http.MethodPost, deadline: deadline, hasDeadline: true, timeout: "995", timeouts: 1, start: ms(4.5), end: ms(6), status: http.StatusOK, reads: 2, bytes: 30, readEnd: "EOF", closed: true},
		}
	}

	traces := []struct {
		name   string
		stream string
		edit   func([]rpcCall) []rpcCall
		ok     bool
	}{
		{"valid", legacy, func(c []rpcCall) []rpcCall { return c }, true},
		{"retained_twin", skyv1connect.ControlServiceStreamRetainedJobLogsProcedure, func(c []rpcCall) []rpcCall {
			c[1].procedure = skyv1connect.ControlServiceStreamRetainedJobLogsProcedure
			return c
		}, true},
		{"wrong_procedure", skyv1connect.ControlServiceStreamRetainedJobLogsProcedure, func(c []rpcCall) []rpcCall { return c }, false},
		{"no_discovery", legacy, func(c []rpcCall) []rpcCall { return c[1:] }, false},
		{"duplicate_stream", legacy, func(c []rpcCall) []rpcCall { return append(c, c[1]) }, false},
		{"get_method", legacy, func(c []rpcCall) []rpcCall { c[1].method = http.MethodGet; return c }, false},
		{"missing_header", legacy, func(c []rpcCall) []rpcCall { c[1].timeout, c[1].timeouts = "", 0; return c }, false},
		{"two_headers", legacy, func(c []rpcCall) []rpcCall { c[1].timeouts = 2; return c }, false},
		{"noncanonical_header", legacy, func(c []rpcCall) []rpcCall { c[1].timeout = "0995"; return c }, false},
		{"no_deadline", legacy, func(c []rpcCall) []rpcCall { c[1].hasDeadline = false; return c }, false},
		{"different_deadline", legacy, func(c []rpcCall) []rpcCall { c[1].deadline = deadline.Add(time.Millisecond); return c }, false},
		{"short_header", legacy, func(c []rpcCall) []rpcCall { c[1].timeout = "900"; return c }, false},
		{"long_header", legacy, func(c []rpcCall) []rpcCall { c[1].timeout = "1001"; return c }, false},
		{"before_command", legacy, func(c []rpcCall) []rpcCall { c[0].start = start.Add(-time.Millisecond); return c }, false},
		{"discovery_failed", legacy, func(c []rpcCall) []rpcCall { c[0].failed, c[0].status = true, 0; return c }, false},
		{"discovery_overlaps", legacy, func(c []rpcCall) []rpcCall { c[0].end = ms(5); return c }, false},
	}
	for _, tt := range traces {
		err := checkFollowTrace(tt.edit(valid()), tt.stream, deadline, start)
		if (err == nil) != tt.ok {
			t.Errorf("checkFollowTrace %s = %v, want ok %v", tt.name, err, tt.ok)
		}
	}

	sources := []struct {
		name      string
		followErr error
		cause     error
		returned  time.Time
		edit      func(*rpcCall)
		ok        bool
	}{
		{"local_deadline", context.DeadlineExceeded, context.DeadlineExceeded, ms(1000), func(*rpcCall) {}, true},
		{"local_canceled", context.Canceled, context.Canceled, ms(500), func(*rpcCall) {}, false},
		{"local_other_cause", context.DeadlineExceeded, errors.New("stop"), ms(1000), func(*rpcCall) {}, false},
		{"local_read_deadline_unclosed", context.DeadlineExceeded, context.DeadlineExceeded, ms(1000), func(c *rpcCall) { c.readEnd, c.closed = "deadline", false }, true},
		{"local_no_response", context.DeadlineExceeded, context.DeadlineExceeded, ms(1000), func(c *rpcCall) { c.failed, c.status, c.reads, c.bytes = true, 0, 0, 0 }, false},
		{"local_status_500", context.DeadlineExceeded, context.DeadlineExceeded, ms(1000), func(c *rpcCall) { c.status = http.StatusInternalServerError }, false},
		{"local_no_roundtrip_end", context.DeadlineExceeded, context.DeadlineExceeded, ms(1000), func(c *rpcCall) { c.end = c.start }, false},
		{"local_no_reads", context.DeadlineExceeded, context.DeadlineExceeded, ms(1000), func(c *rpcCall) { c.reads, c.bytes = 0, 0 }, false},
		{"local_empty_read", context.DeadlineExceeded, context.DeadlineExceeded, ms(1000), func(c *rpcCall) { c.reads, c.bytes, c.readEnd = 1, 0, "deadline" }, false},
		{"remote_deadline", nil, nil, ms(999.5), func(*rpcCall) {}, true},
		{"remote_read_deadline_unclosed", nil, nil, ms(999.5), func(c *rpcCall) { c.readEnd, c.closed = "deadline", false }, true},
		{"remote_early", nil, nil, ms(500), func(*rpcCall) {}, false},
		{"remote_just_early", nil, nil, ms(999.4), func(*rpcCall) {}, false},
		{"remote_no_response", nil, nil, ms(999.5), func(c *rpcCall) { c.failed, c.status = true, 0 }, false},
		{"remote_status_500", nil, nil, ms(999.5), func(c *rpcCall) { c.status = http.StatusInternalServerError }, false},
		{"remote_no_roundtrip_end", nil, nil, ms(999.5), func(c *rpcCall) { c.end = c.start }, false},
		{"remote_no_reads", nil, nil, ms(999.5), func(c *rpcCall) { c.reads, c.bytes = 0, 0 }, false},
		{"remote_no_bytes", nil, nil, ms(999.5), func(c *rpcCall) { c.bytes = 0 }, false},
		{"remote_no_header", nil, nil, ms(999.5), func(c *rpcCall) { c.timeout, c.timeouts = "", 0 }, false},
	}
	for _, tt := range sources {
		c := valid()[1]
		tt.edit(&c)
		err := checkDeadlineSource(tt.followErr, tt.cause, tt.returned, c)
		if (err == nil) != tt.ok {
			t.Errorf("checkDeadlineSource %s = %v, want ok %v", tt.name, err, tt.ok)
		}
	}

	errs := []struct {
		name string
		err  error
		ok   bool
	}{
		{"context_deadline", fmt.Errorf("read: %w", context.DeadlineExceeded), true},
		{"connect_deadline", connect.NewError(connect.CodeDeadlineExceeded, errors.New("deadline")), true},
		{"nil", nil, false},
		{"context_canceled", context.Canceled, false},
		{"connect_canceled", connect.NewError(connect.CodeCanceled, errors.New("canceled")), false},
		{"failed_precondition", connect.NewError(connect.CodeFailedPrecondition, errors.New("logs unavailable")), false},
		{"unimplemented", connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented")), false},
		{"generic", errors.New("connection reset"), false},
	}
	for _, tt := range errs {
		if err := followDeadline(tt.err); (err == nil) != tt.ok {
			t.Errorf("followDeadline %s = %v, want ok %v", tt.name, err, tt.ok)
		}
	}
}
