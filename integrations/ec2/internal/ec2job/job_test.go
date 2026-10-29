//go:build linux

package ec2job

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/cli"
	"github.com/tmc/skygo/internal/server"
	"google.golang.org/protobuf/proto"
)

// These tests run jobs against the real canonical server: an installation
// initialized with the canonical CLI and served by server.Run with its
// real Store, auth engine, Control, artifact root and local target, named
// as the configuration's agent target. The VM is verified with the real
// ec2ctl state and the ec2test signed model peer, which must see no
// mutation from any job operation.
//
// MODEL portions: the control channel is loopback HTTP carrying the
// installation's scoped bearer, as sshagent's Session.Control sends it; it
// does not cross SSH. Lost replies are made by that channel, after the
// server applied the request. A job "reopened" is closed and opened by
// this process, not recovered after an operating system crash.

// A fixture owns one private directory holding every root a test serves:
// the installation, the VM state and the job directories. None is
// registered with t.TempDir, whose cleanup would remove roots below a
// server that has not returned.
//
// The fixture counts the original server.Run calls it starts and settles
// each only by its actual return. retire removes the directory only if
// every Run returned, none left ownership unsettled and the test passed;
// otherwise it keeps the directory and reports its path.
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
	parent := filepath.Join(home, "tmp", "skypilot", "20261028-ec2-job-target-phase-b-tests")
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
// may be a cleanup. retain keeps the original error reachable for the
// life of the test process; no goroutine is left behind.
func (f *fixture) keep(err error) {
	if f.held != nil {
		return
	}
	f.held = err
	retain(err)
}

func (f *fixture) hold(err error) {
	f.t.Helper()
	f.keep(err)
	f.t.Fatalf("HOLD: keeping %s: original server ownership unsettled: %v", f.dir, f.held)
}

func (f *fixture) mkdir(name string) string {
	f.t.Helper()
	p := filepath.Join(f.dir, name)
	if err := os.Mkdir(p, 0o700); err != nil {
		f.t.Fatal(err)
	}
	return p
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

// An installation is a real installation initialized with the canonical
// CLI on stopped state: scoped auth with an admin token, an operator bob
// whose token the job uses, a reader carol, and an artifact root.
type installation struct {
	DataDir, AuthFile, ArtifactDir string
	admin, bob, carol              string // private token files
}

func (f *fixture) newInstallation(name string, retained bool) installation {
	f.t.Helper()
	base := f.mkdir(name)
	in := installation{
		DataDir:     filepath.Join(base, "data"),
		AuthFile:    filepath.Join(base, "auth.json"),
		ArtifactDir: filepath.Join(base, "artifacts"),
		admin:       filepath.Join(base, "admin.token"),
		bob:         filepath.Join(base, "bob.token"),
		carol:       filepath.Join(base, "carol.token"),
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
	if retained {
		out := f.operate("state", "retention", "enable", "--data-dir", in.DataDir, "--json")
		var got struct {
			Enabled bool `json:"enabled"`
		}
		if err := json.Unmarshal([]byte(out), &got); err != nil || !got.Enabled {
			f.t.Fatalf("retention activation readback %q: %v", out, err)
		}
	}
	return in
}

// serve runs server.Run on in until the test ends. Only Run's actual
// return settles it; any result but nil holds the fixture.
func (f *fixture) serve(in installation) string {
	f.t.Helper()
	if f.held != nil {
		f.hold(f.held)
	}
	address := freeAddress(f.t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	f.runs++
	go func() {
		done <- server.Run(ctx, server.Config{
			DataDir:              in.DataDir,
			AuthFile:             in.AuthFile,
			ArtifactDir:          in.ArtifactDir,
			ExistingInstallation: true,
			Listen:               address,
			LocalTargetName:      "ec2vm",
		})
	}()
	f.t.Cleanup(func() {
		cancel()
		err := <-done
		f.runs--
		// Any error, including a *server.RunError or an uncertain artifact
		// or store close, leaves ownership unsettled.
		if err != nil {
			f.keep(fmt.Errorf("server.Run returned: %w", err))
		}
	})
	return address
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

func readToken(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	token := strings.TrimSpace(string(b))
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		t.Fatalf("token file %s is not one token", name)
	}
	return token
}

var errLostReply = errors.New("model: reply lost after the server applied the request")

// A wire is the MODEL control channel. It separately counts raw attempts,
// typed refusals and decoded applied replies. It loses the selected applied
// reply (a selected chunk must end at its intended offset), after reading
// it in full: the server applied the request, and the client never learns
// its outcome. The lost reply's body is kept in lost, for the test alone.
//
// If before is set, it is called with each request's procedure before
// the request is sent; an error it returns fails the request, which the
// server never receives. If edits holds a procedure, every reply to it
// the server sent is rewritten, after the server applied the request: a
// model of a malformed or changed reply, not of anything the server does.
type wire struct {
	token string
	base  *http.Transport

	mu       sync.Mutex
	calls    map[string]int // raw attempts
	admitted map[string]int
	refused  map[string]int
	keys     map[string]map[string]bool
	lose     map[string]int
	lost     map[string][]byte
	edits    map[string]func([]byte) ([]byte, error)
	before   func(procedure string) error
}

func (w *wire) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+w.token)
	p := req.URL.Path
	method := strings.TrimPrefix(p, "/skygo.v1.ControlService/")
	w.mu.Lock()
	before := w.before
	w.mu.Unlock()
	if before != nil {
		if err := before(p); err != nil {
			return nil, err
		}
	}
	w.mu.Lock()
	w.calls[p]++
	armed := w.lose[p]
	edit := w.edits[p]
	w.mu.Unlock()
	tracked := method == "BeginArtifactUpload" || method == "WriteArtifactChunk" || method == "FinalizeArtifactUpload"
	if tracked || armed > 0 || edit != nil {
		req.Header.Set("Accept-Encoding", "identity")
		req.Header.Set("Connect-Accept-Encoding", "identity")
	}
	resp, err := w.base.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	if !tracked && armed == 0 && edit == nil {
		return resp, nil
	}
	body, err := io.ReadAll(resp.Body)
	err = errors.Join(err, resp.Body.Close())
	if err != nil {
		return nil, err
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	lost := false
	if resp.StatusCode == http.StatusOK {
		key, end, err := appliedReply(method, body)
		if err != nil {
			resp.Body.Close()
			return nil, err
		}
		w.mu.Lock()
		if w.admitted == nil {
			w.admitted = map[string]int{}
			if w.refused == nil {
				w.refused = map[string]int{}
			}
			w.keys = map[string]map[string]bool{}
		}
		if w.keys[p] == nil {
			w.keys[p] = map[string]bool{}
		}
		duplicate := w.keys[p][key] && (method == "BeginArtifactUpload" || method == "WriteArtifactChunk")
		w.keys[p][key] = true
		w.admitted[p]++
		lost = armed == w.admitted[p]
		if lost {
			w.lost[p] = bytes.Clone(body)
		}
		w.mu.Unlock()
		if duplicate {
			resp.Body.Close()
			return nil, errors.New("model: duplicate admitted upload identity/range")
		}
		if lost && method == "WriteArtifactChunk" && end != int64(armed)*ChunkSize && armed == 2 {
			resp.Body.Close()
			return nil, fmt.Errorf("model: second applied chunk ends at %d", end)
		}
	} else if tracked && busyReply(resp, body, method) {
		w.mu.Lock()
		if w.refused == nil {
			w.refused = map[string]int{}
		}
		w.refused[p]++
		w.mu.Unlock()
	}
	if lost {
		return nil, errors.Join(errLostReply, resp.Body.Close())
	}
	if edit != nil && resp.StatusCode == http.StatusOK {
		return rewrite(resp, edit)
	}
	return resp, nil
}

func (w *wire) accepted(procedure string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.admitted[procedure]
}

func (w *wire) checkUploadAttempts(t *testing.T, procedure string) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.calls[procedure] != w.admitted[procedure]+w.refused[procedure] || w.refused[procedure] > 20 {
		t.Fatalf("upload attempts raw/admitted/typed-refused = %d/%d/%d", w.calls[procedure], w.admitted[procedure], w.refused[procedure])
	}
}

// appliedReply classifies actual uncompressed protobuf replies, never HTTP200
// alone. The key identifies the admitted upload/range before a model loses it.
func appliedReply(method string, body []byte) (key string, end int64, err error) {
	var msg proto.Message
	switch method {
	case "BeginArtifactUpload", "WriteArtifactChunk":
		var u *v1.ArtifactUpload
		if method == "BeginArtifactUpload" {
			r := new(v1.BeginArtifactUploadResponse)
			err = proto.Unmarshal(body, r)
			u = r.Upload
		} else {
			r := new(v1.WriteArtifactChunkResponse)
			err = proto.Unmarshal(body, r)
			u = r.Upload
		}
		if err != nil {
			return "", 0, err
		}
		if u == nil || u.Reference == nil {
			return "", 0, errors.New("model: missing upload reply")
		}
		ref := artifact.Ref{ID: u.Reference.Id, SHA256: u.Reference.Sha256, Size: u.Reference.Size}
		if ref.Validate() != nil || u.Id != ref.ID || u.Acknowledged < 0 || u.Acknowledged > ref.Size || u.ExpiresAt == nil || u.ExpiresAt.CheckValid() != nil {
			return "", 0, errors.New("model: invalid upload reply")
		}
		if method == "WriteArtifactChunk" {
			return fmt.Sprintf("%s/%d", u.Id, u.Acknowledged), u.Acknowledged, nil
		}
		return u.Id, 0, nil
	case "FinalizeArtifactUpload":
		r := new(v1.FinalizeArtifactUploadResponse)
		err = proto.Unmarshal(body, r)
		if err != nil {
			return "", 0, err
		}
		if r.Reference == nil {
			return "", 0, errors.New("model: missing finalized reference")
		}
		ref := artifact.Ref{ID: r.Reference.Id, SHA256: r.Reference.Sha256, Size: r.Reference.Size}
		return ref.ID, ref.Size, ref.Validate()
	case "SubmitWorkspaceJob":
		r := new(v1.SubmitWorkspaceJobResponse)
		err = proto.Unmarshal(body, r)
		if err != nil {
			return "", 0, err
		}
		if r.Job == nil || r.Job.Id == "" {
			return "", 0, errors.New("model: missing submitted job")
		}
		return r.Job.Id, 0, nil
	case "GetJob":
		r := new(v1.GetJobResponse)
		err = proto.Unmarshal(body, r)
		if err != nil {
			return "", 0, err
		}
		if r.Job == nil || r.Job.Id == "" {
			return "", 0, errors.New("model: missing observed job")
		}
		return r.Job.Id, 0, nil
	case "BeginArtifactDownload":
		r := new(v1.BeginArtifactDownloadResponse)
		err = proto.Unmarshal(body, r)
		if err != nil {
			return "", 0, err
		}
		if len(r.Id) != 32 || r.Reference == nil {
			return "", 0, errors.New("model: missing download identity")
		}
		ref := artifact.Ref{ID: r.Reference.Id, SHA256: r.Reference.Sha256, Size: r.Reference.Size}
		return r.Id, 0, ref.Validate()
	case "CloseArtifactDownload":
		msg = new(v1.CloseArtifactDownloadResponse)
	case "CancelArtifactUpload":
		msg = new(v1.CancelArtifactUploadResponse)
	default:
		return "", 0, fmt.Errorf("model: no typed loss classifier for %s", method)
	}
	return method, 0, proto.Unmarshal(body, msg)
}

func busyReply(resp *http.Response, body []byte, method string) bool {
	var e struct {
		Code string `json:"code"`
	}
	if resp.StatusCode != http.StatusTooManyRequests || json.Unmarshal(body, &e) != nil || e.Code != "resource_exhausted" {
		return false
	}
	if method == "BeginArtifactUpload" {
		v := resp.Header.Values("Skygo-Artifact-Begin-Not-Started")
		return len(v) == 1 && v[0] == "v1"
	}
	return true
}

// rewrite replaces resp's reply with edit's change of it. The original
// body is read in full and closed once.
func rewrite(resp *http.Response, edit func([]byte) ([]byte, error)) (*http.Response, error) {
	body, err := io.ReadAll(resp.Body)
	err = errors.Join(err, resp.Body.Close())
	if err == nil && resp.Header.Get("Content-Encoding") != "" {
		err = errors.New("model: cannot rewrite a compressed reply")
	}
	if err == nil {
		body, err = edit(body)
	}
	if err != nil {
		return nil, err
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
	resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
	return resp, nil
}

// editReply returns a body rewrite that decodes a reply of type M, lets
// edit change it and encodes it again.
func editReply[M any, P interface {
	*M
	proto.Message
}](edit func(P)) func([]byte) ([]byte, error) {
	return func(body []byte) ([]byte, error) {
		m := P(new(M))
		if err := proto.Unmarshal(body, m); err != nil {
			return nil, err
		}
		edit(m)
		return proto.Marshal(m)
	}
}

// setReply arms edit for the wire's later replies to procedure.
func (w *wire) setReply(procedure string, edit func([]byte) ([]byte, error)) {
	w.mu.Lock()
	w.edits[procedure] = edit
	w.mu.Unlock()
}

// setEdit arms edit for the wire's later SubmitWorkspaceJob replies.
func (w *wire) setEdit(edit func(*v1.SubmitWorkspaceJobResponse)) {
	w.setReply(skyv1connect.ControlServiceSubmitWorkspaceJobProcedure, editReply(edit))
}

// setBefore arms before for the wire's later requests.
func (w *wire) setBefore(before func(procedure string) error) {
	w.mu.Lock()
	w.before = before
	w.mu.Unlock()
}

// lostReply returns the body of the last lost reply to procedure.
func (w *wire) lostReply(procedure string) []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lost[procedure]
}

func (w *wire) count(procedure string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.calls[procedure]
}

// A serverEnv is one served installation and one verified VM.
type serverEnv struct {
	t        *testing.T
	f        *fixture
	in       installation
	address  string
	vm       *vmEnv
	cfg      *Config
	verified VM
	admin    skyv1connect.ControlServiceClient
	ctx      context.Context
}

func newServerEnv(t *testing.T, retained bool) *serverEnv {
	t.Helper()
	f := newFixture(t)
	in := f.newInstallation("installation", retained)
	address := f.serve(in)
	vm := newVMEnv(t, f.mkdir("vm-root"))
	w := testWire(f.dir, vm.id)
	w.Installation.DataDir, w.Installation.AuthFile, w.Installation.ArtifactDir = in.DataDir, in.AuthFile, in.ArtifactDir
	w.Installation.ScopedTokenFile = in.bob
	// The deadline is beyond the default retention retry window, so that
	// the domain expiry test can move the clock past it.
	w.VM.SafetyDeadline = time.Now().UTC().Add(365 * 24 * time.Hour).Truncate(time.Second).Format(time.RFC3339)
	cfg, err := ParseConfig(bytes.NewReader(encodeWire(t, w)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	before := vm.mutations()
	verified, err := VerifyVM(ctx, vm.state, vm.vmCfg, vm.c, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if n := vm.mutations() - before; n != 0 {
			t.Errorf("job operations sent %d EC2 mutations", n)
		}
	})
	e := &serverEnv{t: t, f: f, in: in, address: address, vm: vm, cfg: cfg, verified: verified, ctx: ctx}
	e.admin = skyv1connect.NewControlServiceClient(e.client(readToken(t, in.admin), nil), "http://"+address)
	e.awaitReady()
	return e
}

func (e *serverEnv) client(token string, w **wire) *http.Client {
	tr := &http.Transport{}
	e.t.Cleanup(tr.CloseIdleConnections)
	c := &wire{token: token, base: tr, calls: map[string]int{}, lose: map[string]int{}, lost: map[string][]byte{}, edits: map[string]func([]byte) ([]byte, error){}}
	if w != nil {
		*w = c
	}
	return &http.Client{Transport: c}
}

func (e *serverEnv) awaitReady() {
	e.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, err := e.admin.GetServerInfo(e.ctx, connect.NewRequest(&v1.GetServerInfoRequest{}))
		if err == nil {
			return
		}
		if time.Now().After(deadline) || e.ctx.Err() != nil {
			e.t.Fatalf("server not ready: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// create snapshots a workdir holding files and creates a job running
// taskJSON in it.
func (e *serverEnv) create(name, taskJSON string, files map[string][]byte) *Job {
	e.t.Helper()
	src := e.f.mkdir(name + "-workdir")
	for file, data := range files {
		if err := os.WriteFile(filepath.Join(src, file), data, 0o600); err != nil {
			e.t.Fatal(err)
		}
	}
	archive, digest, err := Snapshot(src)
	if err != nil {
		e.t.Fatal(err)
	}
	task, err := e.cfg.Task(TaskInput{JSON: []byte(taskJSON)}, digest, int64(len(archive)), time.Now())
	if err != nil {
		e.t.Fatal(err)
	}
	j, err := Create(filepath.Join(e.f.dir, name), e.cfg, e.verified, task, archive)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() {
		if err := j.Close(); err != nil {
			e.t.Errorf("close job: %v", err)
		}
	})
	if err := j.Bind(e.cfg, e.verified); err != nil {
		e.t.Fatal(err)
	}
	return j
}

// attach gives j a session with the token in file, losing the replies
// lose names.
func (e *serverEnv) attach(j *Job, file string, lose map[string]int) *wire {
	e.t.Helper()
	var w *wire
	cc := skyv1connect.NewControlServiceClient(e.client(readToken(e.t, file), &w), "http://"+e.address)
	for p, n := range lose {
		w.lose[p] = n
	}
	if err := j.Attach(peerSession{cc, sessionFor(e.cfg, e.verified)}); err != nil {
		e.t.Fatal(err)
	}
	return w
}

// reopen closes j and opens its directory again, bound to the same VM.
func (e *serverEnv) reopen(j *Job) *Job {
	e.t.Helper()
	dir := j.dir
	if err := j.Close(); err != nil {
		e.t.Fatal(err)
	}
	j, err := Open(dir)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { j.Close() })
	if err := j.Bind(e.cfg, e.verified); err != nil {
		e.t.Fatal(err)
	}
	return j
}

// jobs returns the IDs of every job the admin sees.
func (e *serverEnv) jobs() []string {
	e.t.Helper()
	r, err := e.admin.ListJobs(e.ctx, connect.NewRequest(&v1.ListJobsRequest{PageSize: 100}))
	if err != nil {
		e.t.Fatal(err)
	}
	var ids []string
	for _, j := range r.Msg.Jobs {
		ids = append(ids, j.Id)
	}
	return ids
}

var poll = Wait{Poll: 100 * time.Millisecond}

// checkOutput checks the saved output.tar against its reference and
// that it holds what testTaskJSON's setup and command wrote.
func checkOutput(t *testing.T, st Status) {
	t.Helper()
	if !st.Succeeded() || st.Output != outputSaved || st.OutputRef == nil || st.OutputFile == "" {
		t.Fatalf("status %+v", st)
	}
	data, err := os.ReadFile(st.OutputFile)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := artifact.Verify(*st.OutputRef, bytes.NewReader(data)); err != nil {
		t.Fatalf("saved output does not verify: %v", err)
	}
	dest := filepath.Dir(st.OutputFile) + "-extracted"
	if err := os.Mkdir(dest, 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dest)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := artifact.Extract(root, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"result.txt": "input\n", "setup.txt": "setup-ran\n"} {
		got, err := os.ReadFile(filepath.Join(dest, "out", name))
		if err != nil || string(got) != want {
			t.Errorf("out/%s = %q, %v; want %q", name, got, err, want)
		}
	}
}

func TestRun(t *testing.T)         { testRun(t, false) }
func TestRunRetained(t *testing.T) { testRun(t, true) }

// testRun runs a workspace job with setup and outputs to success, reads
// its log, and shows that a reopened job is observed, never submitted.
func testRun(t *testing.T, retained bool) {
	e := newServerEnv(t, retained)
	j := e.create("job", testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
	w := e.attach(j, e.in.bob, nil)
	st, err := j.Start(e.ctx, poll)
	if err != nil {
		t.Fatalf("Start = %v, status %+v", err, st)
	}
	checkOutput(t, st)
	if retained != (st.Retention == RetentionEnabled) || retained != (st.RequestDomain != "") {
		t.Errorf("retention %q, domain %q", st.Retention, st.RequestDomain)
	}
	if ids := e.jobs(); len(ids) != 1 || ids[0] != st.JobID {
		t.Fatalf("server jobs %v, want [%s]", ids, st.JobID)
	}
	r, err := e.admin.GetJob(e.ctx, connect.NewRequest(&v1.GetJobRequest{Id: st.JobID}))
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Msg.Job.Task.Workdir; got == nil || got.Id != st.Workdir.ID || got.Sha256 != st.SnapshotSHA256 {
		t.Fatalf("server workdir %v, want %v", got, st.Workdir)
	}
	for _, p := range []string{skyv1connect.ControlServiceBeginArtifactUploadProcedure, skyv1connect.ControlServiceSubmitWorkspaceJobProcedure, skyv1connect.ControlServiceBeginArtifactDownloadProcedure, skyv1connect.ControlServiceCloseArtifactDownloadProcedure} {
		if n := w.count(p); n != 1 {
			t.Errorf("%s sent %d times, want 1", p, n)
		}
	}
	if st.Download != downloadClosed || !hexID32.MatchString(st.DownloadID) || st.Hold != "" {
		t.Errorf("download %q %q, hold %q; want a recorded release", st.Download, st.DownloadID, st.Hold)
	}

	var log bytes.Buffer
	page, err := j.Logs(e.ctx, 0, &log)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(log.String(), "hello\n") || !page.Complete || page.Bytes != int64(log.Len()) || retained && page.AttemptID == "" {
		t.Fatalf("log %q, page %+v", log.String(), page)
	}
	log.Reset()
	if again, err := j.Logs(e.ctx, page.NextOffset, &log); err != nil || log.Len() != 0 || again.NextOffset != page.NextOffset {
		t.Fatalf("log after the end = %q, %+v, %v", log.String(), again, err)
	}

	j = e.reopen(j)
	w = e.attach(j, e.in.bob, nil)
	again, err := j.Resume(e.ctx, poll)
	if err != nil || again.JobID != st.JobID || !again.Succeeded() {
		t.Fatalf("Resume of an ended job = %+v, %v", again, err)
	}
	if n := w.count(skyv1connect.ControlServiceSubmitWorkspaceJobProcedure) + w.count(skyv1connect.ControlServiceBeginArtifactUploadProcedure); n != 0 {
		t.Fatalf("a known job was uploaded or submitted again")
	}
	if n := w.count(skyv1connect.ControlServiceBeginArtifactDownloadProcedure); n != 0 {
		t.Fatalf("a saved output was downloaded again: %d Begins", n)
	}
	if len(e.jobs()) != 1 {
		t.Fatal("the server has another job")
	}

	// Privacy: neither the journal nor the status holds a credential.
	journal, err := os.ReadFile(filepath.Join(j.dir, stateName))
	if err != nil {
		t.Fatal(err)
	}
	report, _ := json.Marshal(again)
	for _, file := range []string{e.in.bob, e.in.admin} {
		token := readToken(t, file)
		if bytes.Contains(journal, []byte(token)) || bytes.Contains(report, []byte(token)) {
			t.Fatal("a token reached the journal or the status")
		}
	}
}

// TestLostSubmitReply: the server accepted the submit, but its reply was
// lost. Start does not submit again; Resume replays exactly the stored
// request, which the server deduplicates to the same job.
func TestLostSubmitReply(t *testing.T) {
	e := newServerEnv(t, false)
	j := e.create("job", testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
	submit := skyv1connect.ControlServiceSubmitWorkspaceJobProcedure
	w := e.attach(j, e.in.bob, map[string]int{submit: 1})
	st, err := j.Start(e.ctx, poll)
	if err == nil || st.Submit != submitSent || st.JobID != "" {
		t.Fatalf("Start = %+v, %v", st, err)
	}
	if strings.Contains(err.Error(), readToken(t, e.in.bob)) {
		t.Fatal("the error quotes the token")
	}
	if _, err := j.Start(e.ctx, poll); err == nil {
		t.Fatal("Start ran a job with progress")
	}
	if n := w.count(submit); n != 1 {
		t.Fatalf("submitted %d times before Resume", n)
	}
	ids := e.jobs()
	if len(ids) != 1 {
		t.Fatalf("server jobs %v, want the one applied", ids)
	}
	st, err = j.Resume(e.ctx, poll)
	if err != nil {
		t.Fatalf("Resume = %v", err)
	}
	checkOutput(t, st)
	if w.count(submit) != 2 || st.JobID != ids[0] {
		t.Fatalf("replay sent %d submits for %s, want 2 for %s", w.count(submit), st.JobID, ids[0])
	}
	if len(e.jobs()) != 1 {
		t.Fatal("the replay created another job")
	}
}

// TestLostBeginReply: a Begin whose reply was lost may have allocated an
// upload under an unknown ID. The job is held: no second Begin, no submit.
func TestLostBeginReply(t *testing.T) {
	e := newServerEnv(t, false)
	j := e.create("job", testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
	begin := skyv1connect.ControlServiceBeginArtifactUploadProcedure
	w := e.attach(j, e.in.bob, map[string]int{begin: 1})
	st, err := j.Start(e.ctx, poll)
	if !errors.Is(err, ErrHeld) || st.Hold != HoldUploadBegin {
		t.Fatalf("Start = %+v, %v", st, err)
	}
	rawBefore := w.count(begin)
	w.checkUploadAttempts(t, begin)
	j = e.reopen(j)
	w2 := e.attach(j, e.in.bob, nil)
	if _, err := j.Resume(e.ctx, poll); !errors.Is(err, ErrHeld) {
		t.Fatalf("Resume = %v, want ErrHeld", err)
	}
	if w.accepted(begin) != 1 || w2.count(begin) != 0 || w.count(begin) != rawBefore || w.count(skyv1connect.ControlServiceSubmitWorkspaceJobProcedure)+w2.count(skyv1connect.ControlServiceSubmitWorkspaceJobProcedure) != 0 {
		t.Fatal("a held upload was begun again or submitted")
	}
	if len(e.jobs()) != 0 {
		t.Fatal("a held job reached the server")
	}
}

// incompressible returns n deterministic bytes no archive compresses.
func incompressible(n int) []byte {
	var b []byte
	for i := uint64(0); len(b) < n; i++ {
		sum := sha256.Sum256(binary.BigEndian.AppendUint64(nil, i))
		b = append(b, sum[:]...)
	}
	return b[:n]
}

// TestLostChunkReply: the server applied the second chunk, but its reply
// was lost. Resume reconciles the offset with GetArtifactUpload and sends
// every chunk once.
func TestLostChunkReply(t *testing.T) {
	e := newServerEnv(t, false)
	j := e.create("job", testTaskJSON, map[string][]byte{"input.txt": []byte("input\n"), "blob": incompressible(150 << 10)})
	size := j.Status().SnapshotSize
	if size <= 2*ChunkSize {
		t.Fatalf("snapshot is %d bytes, want more than two chunks", size)
	}
	chunk := skyv1connect.ControlServiceWriteArtifactChunkProcedure
	w := e.attach(j, e.in.bob, map[string]int{chunk: 2})
	st, err := j.Start(e.ctx, poll)
	if err == nil || st.Upload != uploadOpen || st.Acknowledged != ChunkSize {
		t.Fatalf("Start = %+v, %v", st, err)
	}
	st, err = j.Resume(e.ctx, poll)
	if err != nil {
		t.Fatalf("Resume = %v", err)
	}
	checkOutput(t, st)
	chunks := int((size + ChunkSize - 1) / ChunkSize)
	w.checkUploadAttempts(t, chunk)
	w.checkUploadAttempts(t, skyv1connect.ControlServiceBeginArtifactUploadProcedure)
	if w.accepted(chunk) != chunks || w.count(skyv1connect.ControlServiceGetArtifactUploadProcedure) != 1 || w.accepted(skyv1connect.ControlServiceBeginArtifactUploadProcedure) != 1 {
		t.Fatalf("sent %d chunks for %d, %d reconciles, %d begins", w.count(chunk), chunks,
			w.count(skyv1connect.ControlServiceGetArtifactUploadProcedure), w.count(skyv1connect.ControlServiceBeginArtifactUploadProcedure))
	}
}

// TestCutBeforeSubmit: the save that would record the submit as sent
// fails. Nothing is submitted; the reopened job submits once.
func TestCutBeforeSubmit(t *testing.T) {
	e := newServerEnv(t, false)
	j := e.create("job", testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
	w := e.attach(j, e.in.bob, nil)
	renames := 0
	j.s.fault = func(stage string) error {
		if stage != "rename" || w.count(skyv1connect.ControlServiceFinalizeArtifactUploadProcedure) == 0 {
			return nil
		}
		renames++
		if renames == 2 { // the first stores the finalized upload
			return errFault
		}
		return nil
	}
	if _, err := j.Start(e.ctx, poll); !errors.Is(err, ErrUnpersisted) {
		t.Fatalf("Start = %v, want ErrUnpersisted", err)
	}
	if w.count(skyv1connect.ControlServiceSubmitWorkspaceJobProcedure) != 0 || len(e.jobs()) != 0 {
		t.Fatal("a submit not durably recorded was sent")
	}
	j = e.reopen(j)
	if st := j.Status(); st.Upload != uploadDone || st.Submit != submitNone {
		t.Fatalf("reopened status %+v", st)
	}
	w = e.attach(j, e.in.bob, nil)
	st, err := j.Resume(e.ctx, poll)
	if err != nil {
		t.Fatal(err)
	}
	checkOutput(t, st)
	if w.count(skyv1connect.ControlServiceSubmitWorkspaceJobProcedure) != 1 || len(e.jobs()) != 1 {
		t.Fatal("want exactly one submit and one job")
	}
}

// TestCancelRunning: a latched cancel of a known running job is sent
// once, and the job is canceled only when it is seen CANCELED.
func TestCancelRunning(t *testing.T) {
	e := newServerEnv(t, false)
	j := e.create("job", `{"argv":["/bin/sh","-c","sleep 120"],"timeoutSeconds":"300"}`, map[string][]byte{"input.txt": []byte("input\n")})
	w := e.attach(j, e.in.bob, nil)
	st, err := j.Start(e.ctx, Wait{})
	if err != nil || st.JobID == "" || st.Ended() {
		t.Fatalf("Start = %+v, %v", st, err)
	}
	if err := j.RequestCancel(); err != nil || !j.NeedsRemote() {
		t.Fatalf("RequestCancel = %v, needs remote %v", err, j.NeedsRemote())
	}
	if _, err := j.Resume(e.ctx, poll); err == nil {
		t.Fatal("Resume ran a job whose cancel was requested")
	}
	st, err = j.Cancel(e.ctx, poll)
	if err != nil || st.State != "CANCELED" || st.Cancel != cancelDelivered || st.Output != outputNotUsed {
		t.Fatalf("Cancel = %+v, %v", st, err)
	}
	cancel := skyv1connect.ControlServiceCancelJobProcedure
	if st, err := j.Cancel(e.ctx, poll); err != nil || st.State != "CANCELED" || w.count(cancel) != 1 {
		t.Fatalf("second Cancel = %+v, %v; %d cancels sent", st, err, w.count(cancel))
	}
	// A submitted job's upload may be the job's: it is never cleaned up.
	if n := w.count(skyv1connect.ControlServiceCancelArtifactUploadProcedure); n != 0 || j.Status().Cleanup != "" {
		t.Fatalf("a submitted job's upload was cleaned up: %d requests", n)
	}
}

const sleepTask = `{"argv":["/bin/sh","-c","sleep 120"],"timeoutSeconds":"300"}`

// TestAcceptedReplyHeld: the server accepted each job, but the MODEL
// reply the client reads names the valid job ID with an invalid state,
// no task, a task changed in one field, or another target. The ID is
// stored and the job held: it is observed and canceled by that ID, never
// run as the task or submitted again, and its outputs are not collected.
func TestAcceptedReplyHeld(t *testing.T) {
	e := newServerEnv(t, false)
	cases := []struct {
		name string
		edit func(*v1.SubmitWorkspaceJobResponse)
		hold string
	}{
		{"invalid-state", func(m *v1.SubmitWorkspaceJobResponse) { m.Job.State = v1.JobState(99) }, HoldAcceptedReply},
		{"unspecified-state", func(m *v1.SubmitWorkspaceJobResponse) { m.Job.State = v1.JobState_JOB_STATE_UNSPECIFIED }, HoldAcceptedReply},
		{"no-task", func(m *v1.SubmitWorkspaceJobResponse) { m.Job.Task = nil }, HoldAcceptedTask},
		{"other-image", func(m *v1.SubmitWorkspaceJobResponse) { m.Job.Task.Image = "busybox" }, HoldAcceptedTask},
		{"other-selector", func(m *v1.SubmitWorkspaceJobResponse) { m.Job.Task.NodeSelector = map[string]string{"a": "b"} }, HoldAcceptedTask},
		{"other-target", func(m *v1.SubmitWorkspaceJobResponse) { m.Job.Target = "other" }, HoldAcceptedTask},
	}
	submit := skyv1connect.ControlServiceSubmitWorkspaceJobProcedure
	for _, tt := range cases {
		j := e.create(tt.name, sleepTask, map[string][]byte{"input.txt": []byte("input\n")})
		w := e.attach(j, e.in.bob, nil)
		w.setEdit(tt.edit)
		st, err := j.Start(e.ctx, poll)
		if !errors.Is(err, ErrHeld) || st.Hold != tt.hold || !hexID32.MatchString(st.JobID) || st.Submit != submitAccepted || !st.Durable {
			t.Fatalf("%s: Start = %+v, %v", tt.name, st, err)
		}
		if !slices.Contains(e.jobs(), st.JobID) {
			t.Fatalf("%s: stored ID %s is not the server's job", tt.name, st.JobID)
		}
		id := st.JobID
		j = e.reopen(j)
		w = e.attach(j, e.in.bob, nil)
		if j.Status().JobID != id || j.Status().Hold != tt.hold {
			t.Fatalf("%s: reopened status %+v", tt.name, j.Status())
		}
		if _, err := j.Resume(e.ctx, poll); !errors.Is(err, ErrHeld) {
			t.Fatalf("%s: Resume = %v, want ErrHeld", tt.name, err)
		}
		if st, err := j.Refresh(e.ctx); err != nil || st.JobID != id || st.State == "" {
			t.Fatalf("%s: Refresh = %+v, %v", tt.name, st, err)
		}
		if err := j.RequestCancel(); err != nil {
			t.Fatalf("%s: RequestCancel = %v", tt.name, err)
		}
		st, err = j.Cancel(e.ctx, poll)
		if err != nil || st.State != "CANCELED" || st.JobID != id || st.Output != "" {
			t.Fatalf("%s: Cancel = %+v, %v", tt.name, st, err)
		}
		if w.count(submit) != 0 || w.count(skyv1connect.ControlServiceCancelJobProcedure) != 1 || w.count(skyv1connect.ControlServiceBeginArtifactDownloadProcedure) != 0 {
			t.Fatalf("%s: a held job was submitted again, canceled twice or collected", tt.name)
		}
	}
	if n := len(e.jobs()); n != len(cases) {
		t.Fatalf("server has %d jobs, want %d", n, len(cases))
	}
}

// TestAcceptedIDSaveFailure: the save of a returned ID, held because the
// MODEL reply's state is invalid, fails at the rename. The same ID stays
// pending in memory; each later call stores it before anything else and
// sends nothing until it is durable. Then the job is observed and
// canceled by that ID, and never submitted again.
func TestAcceptedIDSaveFailure(t *testing.T) {
	e := newServerEnv(t, false)
	j := e.create("job", sleepTask, map[string][]byte{"input.txt": []byte("input\n")})
	w := e.attach(j, e.in.bob, nil)
	w.setEdit(func(m *v1.SubmitWorkspaceJobResponse) { m.Job.State = v1.JobState(99) })
	submit := skyv1connect.ControlServiceSubmitWorkspaceJobProcedure
	get := skyv1connect.ControlServiceGetJobProcedure
	j.s.fault = func(stage string) error {
		if stage == "rename" && w.count(submit) == 1 {
			return errFault
		}
		return nil
	}
	st, err := j.Start(e.ctx, poll)
	if !errors.Is(err, ErrHeld) || !errors.Is(err, ErrUnpersisted) || !hexID32.MatchString(st.JobID) || st.Durable || st.Hold != HoldAcceptedReply {
		t.Fatalf("Start = %+v, %v", st, err)
	}
	id := st.JobID
	disk := func() wireState {
		t.Helper()
		var w wireState
		if err := json.Unmarshal(readJournal(t, j.dir), &w); err != nil {
			t.Fatal(err)
		}
		return w
	}
	if d := disk(); d.JobID != "" || d.Submit != submitSent {
		t.Fatalf("the failed save reached the journal: %+v", d)
	}
	if _, err := j.Refresh(e.ctx); !errors.Is(err, ErrUnpersisted) || w.count(get) != 0 {
		t.Fatalf("Refresh with the ID pending = %v, %d GetJob", err, w.count(get))
	}
	if err := j.RequestCancel(); !errors.Is(err, ErrUnpersisted) || j.Status().JobID != id {
		t.Fatalf("RequestCancel with the ID pending = %v", err)
	}
	j.s.fault = nil
	st, err = j.Refresh(e.ctx)
	if err != nil || st.JobID != id || !st.Durable || w.count(get) != 1 {
		t.Fatalf("Refresh = %+v, %v", st, err)
	}
	if d := disk(); d.JobID != id || d.Hold != HoldAcceptedReply {
		t.Fatalf("stored %+v, want ID %s held", d, id)
	}
	if err := j.RequestCancel(); err != nil {
		t.Fatal(err)
	}
	if st, err := j.Cancel(e.ctx, poll); err != nil || st.State != "CANCELED" {
		t.Fatalf("Cancel = %+v, %v", st, err)
	}
	if ids := e.jobs(); w.count(submit) != 1 || len(ids) != 1 || ids[0] != id {
		t.Fatalf("server jobs %v after %d submits, want [%s] after 1", ids, w.count(submit), id)
	}
}

func TestLogCheckpoint(t *testing.T)         { testLogCheckpoint(t, false) }
func TestLogCheckpointRetained(t *testing.T) { testLogCheckpoint(t, true) }

// testLogCheckpoint: NextLogs continues from the committed checkpoint,
// which a failed save leaves pending and a reopened job keeps. An
// explicit Logs seeks without moving it; a copy whose write fails cannot
// be committed, and a page commits once.
func testLogCheckpoint(t *testing.T, retained bool) {
	e := newServerEnv(t, retained)
	j := e.create("job", testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
	w := e.attach(j, e.in.bob, nil)
	if st, err := j.Start(e.ctx, poll); err != nil || !st.Succeeded() {
		t.Fatalf("Start = %+v, %v", st, err)
	}
	var first bytes.Buffer
	page, err := j.NextLogs(e.ctx, &first)
	if err != nil || page.Offset != 0 || !strings.Contains(first.String(), "hello\n") || !page.Complete || retained && page.AttemptID == "" {
		t.Fatalf("NextLogs = %+v, %v, %q", page, err, first.String())
	}
	if j.Status().LogOffset != 0 {
		t.Fatal("a copy moved the checkpoint before it was committed")
	}

	// A failed save leaves the checkpoint pending and stops calls.
	j.s.fault = func(stage string) error {
		if stage == "rename" {
			return errFault
		}
		return nil
	}
	if err := j.CommitLogs(page); !errors.Is(err, ErrUnpersisted) || j.Status().Durable {
		t.Fatalf("CommitLogs with a failing save = %v", err)
	}
	if bytes.Contains(readJournal(t, j.dir), []byte(`"log"`)) {
		t.Fatal("the failed checkpoint save reached the journal")
	}
	get := skyv1connect.ControlServiceGetJobProcedure
	before := w.count(get)
	if _, err := j.Refresh(e.ctx); !errors.Is(err, ErrUnpersisted) || w.count(get) != before {
		t.Fatalf("Refresh with a pending checkpoint = %v", err)
	}
	j.s.fault = nil
	if _, err := j.Refresh(e.ctx); err != nil || !j.Status().Durable || j.Status().LogOffset != page.NextOffset {
		t.Fatalf("Refresh = %v, status %+v", err, j.Status())
	}
	if err := j.CommitLogs(page); err == nil {
		t.Fatal("a page was committed twice")
	}

	j = e.reopen(j)
	e.attach(j, e.in.bob, nil)
	var again bytes.Buffer
	next, err := j.NextLogs(e.ctx, &again)
	if err != nil || again.Len() != 0 || next.Offset != page.NextOffset || next.NextOffset != page.NextOffset {
		t.Fatalf("NextLogs after reopen = %+v, %v, %q", next, err, again.String())
	}
	if err := j.CommitLogs(next); err != nil || j.Status().LogOffset != page.NextOffset {
		t.Fatalf("CommitLogs of an empty continuation = %v", err)
	}

	var all bytes.Buffer
	seek, err := j.Logs(e.ctx, 0, &all)
	if err != nil || all.String() != first.String() || j.Status().LogOffset != page.NextOffset {
		t.Fatalf("Logs from 0 = %+v, %v, %q", seek, err, all.String())
	}
	short, err := j.Logs(e.ctx, 0, &shortWriter{n: 1})
	if !errors.Is(err, io.ErrShortWrite) || short.NextOffset != 0 {
		t.Fatalf("Logs to a short writer = %+v, %v", short, err)
	}
	if err := j.CommitLogs(short); err == nil {
		t.Fatal("a copy whose write failed was committed")
	}
	if err := j.CommitLogs(seek); err == nil {
		t.Fatal("a page older than the last copy was committed")
	}
	if j.Status().LogOffset != page.NextOffset {
		t.Fatal("a refused commit moved the checkpoint")
	}
}

// TestCancelUploadCleanup: a canceled job never submitted whose upload ID
// is known has that upload deleted with CancelArtifactUpload, stored as
// sent first. The first request's reply is lost after the server applied
// it, so the cleanup is unknown. A later Cancel sends the same request
// for the same upload, and records it delivered only on a nil reply. The
// upload is no longer writable, and no Begin, submit or job follows.
func TestCancelUploadCleanup(t *testing.T) {
	e := newServerEnv(t, false)
	j := e.create("job", testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
	chunk := skyv1connect.ControlServiceWriteArtifactChunkProcedure
	cleanup := skyv1connect.ControlServiceCancelArtifactUploadProcedure
	w := e.attach(j, e.in.bob, map[string]int{chunk: 1})
	st, err := j.Start(e.ctx, poll)
	if err == nil || st.Upload != uploadOpen || st.UploadID == "" || st.Submit != submitNone {
		t.Fatalf("Start = %+v, %v", st, err)
	}
	upload := st.UploadID
	if err := j.RequestCancel(); err != nil || !j.NeedsRemote() || j.Known() {
		t.Fatalf("RequestCancel = %v, needs remote %v", err, j.NeedsRemote())
	}

	j = e.reopen(j)
	w1 := e.attach(j, e.in.bob, map[string]int{cleanup: 1})
	st, err = j.Cancel(e.ctx, poll)
	var re *RemoteError
	if !errors.As(err, &re) || st.Cleanup != cleanupSent || st.State != "NEVER_SUBMITTED" || w1.count(cleanup) != 1 {
		t.Fatalf("Cancel with a lost reply = %+v, %v", st, err)
	}
	bob := skyv1connect.NewControlServiceClient(e.client(readToken(t, e.in.bob), nil), "http://"+e.address)
	r, gerr := bob.GetArtifactUpload(e.ctx, connect.NewRequest(&v1.GetArtifactUploadRequest{Id: upload}))
	if gerr == nil && r.Msg.GetUpload().GetState() == v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_WRITABLE {
		t.Fatal("the server did not apply the lost CancelArtifactUpload")
	}

	j = e.reopen(j)
	w2 := e.attach(j, e.in.bob, nil)
	st, err = j.Cancel(e.ctx, poll)
	if w2.count(cleanup) != 1 || st.UploadID != upload {
		t.Fatalf("second Cancel sent %d requests for %s, want 1 for %s", w2.count(cleanup), st.UploadID, upload)
	}
	if err == nil && st.Cleanup != cleanupDelivered || err != nil && st.Cleanup != cleanupSent {
		t.Fatalf("second Cancel = %+v, %v: a cleanup recorded delivered without a nil reply", st, err)
	}
	begins := w.accepted(skyv1connect.ControlServiceBeginArtifactUploadProcedure) + w1.accepted(skyv1connect.ControlServiceBeginArtifactUploadProcedure) + w2.accepted(skyv1connect.ControlServiceBeginArtifactUploadProcedure)
	submits := w.count(skyv1connect.ControlServiceSubmitWorkspaceJobProcedure) + w1.count(skyv1connect.ControlServiceSubmitWorkspaceJobProcedure) + w2.count(skyv1connect.ControlServiceSubmitWorkspaceJobProcedure)
	if begins != 1 || submits != 0 || len(e.jobs()) != 0 {
		t.Fatalf("%d begins, %d submits, %d jobs; want 1, 0, 0", begins, submits, len(e.jobs()))
	}
}

// TestCancelUploadCleanupDelivered is TestCancelUploadCleanup's twin
// without the lost reply: one request, recorded delivered, after which
// the canceled job needs no remote call.
func TestCancelUploadCleanupDelivered(t *testing.T) {
	e := newServerEnv(t, false)
	j := e.create("job", testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
	cleanup := skyv1connect.ControlServiceCancelArtifactUploadProcedure
	e.attach(j, e.in.bob, map[string]int{skyv1connect.ControlServiceWriteArtifactChunkProcedure: 1})
	if st, err := j.Start(e.ctx, poll); err == nil || st.Upload != uploadOpen {
		t.Fatalf("Start = %+v, %v", st, err)
	}
	if err := j.RequestCancel(); err != nil {
		t.Fatal(err)
	}
	j = e.reopen(j)
	w := e.attach(j, e.in.bob, nil)
	st, err := j.Cancel(e.ctx, poll)
	if err != nil || st.Cleanup != cleanupDelivered || w.count(cleanup) != 1 || j.NeedsRemote() {
		t.Fatalf("Cancel = %+v, %v; %d requests", st, err, w.count(cleanup))
	}
	if len(e.jobs()) != 0 {
		t.Fatal("a canceled job reached the server")
	}
}

// TestDomainExpired: a submit in doubt is never replayed after its
// request domain's retry window, and canceling it holds the job.
func TestDomainExpired(t *testing.T) {
	e := newServerEnv(t, true)
	j := e.create("job", testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
	submit := skyv1connect.ControlServiceSubmitWorkspaceJobProcedure
	w := e.attach(j, e.in.bob, map[string]int{submit: 1})
	st, err := j.Start(e.ctx, poll)
	if err == nil || st.Retention != RetentionEnabled || st.RequestDomain == "" || st.Submit != submitSent {
		t.Fatalf("Start = %+v, %v", st, err)
	}
	j.now = func() time.Time { return j.st.RetryUntil }
	if _, err := j.Resume(e.ctx, poll); !errors.Is(err, ErrDomainExpired) {
		t.Fatalf("Resume after the retry window = %v, want ErrDomainExpired", err)
	}
	if w.count(submit) != 1 {
		t.Fatal("an expired domain was replayed")
	}
	if err := j.RequestCancel(); !errors.Is(err, ErrHeld) || j.Status().Hold != HoldCanceledSubmit {
		t.Fatalf("RequestCancel = %v, status %+v", err, j.Status())
	}
	j.now = time.Now
	if _, err := j.Resume(e.ctx, poll); !errors.Is(err, ErrHeld) || w.count(submit) != 1 {
		t.Fatalf("Resume of a held job = %v, %d submits", err, w.count(submit))
	}
}

// TestRefusedPrincipal: a reader, and an unknown token, cannot run a
// job. Neither reaches the server's jobs, and errors carry the Connect
// code without the server's message or the token.
func TestRefusedPrincipal(t *testing.T) {
	e := newServerEnv(t, false)
	unknown := filepath.Join(e.f.dir, "unknown.token")
	if err := os.WriteFile(unknown, []byte(strings.Repeat("z", 43)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name, file string
		code       connect.Code
	}{
		{"reader", e.in.carol, connect.CodePermissionDenied},
		{"unknown", unknown, connect.CodeUnauthenticated},
	} {
		j := e.create(tt.name, testTaskJSON, map[string][]byte{"input.txt": []byte("input\n")})
		w := e.attach(j, tt.file, nil)
		_, err := j.Start(e.ctx, poll)
		var re *RemoteError
		if !errors.As(err, &re) || re.Code != tt.code {
			t.Errorf("%s: Start = %v, want code %v", tt.name, err, tt.code)
		}
		if err != nil && strings.Contains(err.Error(), readToken(t, tt.file)) {
			t.Errorf("%s: the error quotes the token", tt.name)
		}
		if n := w.count(skyv1connect.ControlServiceSubmitWorkspaceJobProcedure); n != 0 {
			t.Errorf("%s: submitted %d times", tt.name, n)
		}
	}
	if len(e.jobs()) != 0 {
		t.Fatal("a refused principal created a job")
	}
}
