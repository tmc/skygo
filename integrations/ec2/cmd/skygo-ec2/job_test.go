//go:build linux

package main

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
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2job"
	"github.com/tmc/skygo/integrations/ec2/internal/ec2test"
	"github.com/tmc/skygo/integrations/ssh/sshagent"
	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/cli"
	"github.com/tmc/skygo/internal/server"
	"golang.org/x/tools/txtar"
	"google.golang.org/protobuf/proto"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// TestJobScripts runs testdata/job/*.txt. Each script has its own
// installation, initialized with the canonical CLI and served by the real
// server.Run (Store, auth, Control, artifact root, local target "ec2vm"),
// and its own ec2test model, which the lifecycle commands drive through
// the real ec2ctl caller and which job commands only describe.
//
// MODEL portions: the job commands' SSH session. Its opener checks the
// exact sshagent.Config the command passes, then returns a session whose
// Control is loopback HTTP to the real server with the scoped token from
// that Config's ScopedTokenFile, and whose Binding is what sshagent.Open
// reports for that Config. It never crosses SSH or builds an agent. Its
// failed Close and its open failure "whose cleanup did not settle" are
// models too: they hold no connection, which is why a script may settle
// such a command with release. The journal each command keeps locked is
// real.
//
// Scripts run one at a time: each subtest's fixture settles its server
// before the next starts.
func TestJobScripts(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "job", "*.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no job scripts")
	}
	for _, file := range files {
		t.Run(strings.TrimSuffix(filepath.Base(file), ".txt"), func(t *testing.T) {
			runJobScript(t, file)
		})
	}
}

// A jobFixture owns one private directory holding every root its script
// serves. It is never registered with t.TempDir. It counts the original
// server.Run calls it starts and removes the directory only if each
// returned nil and the test passed; otherwise it keeps it and reports it.
type jobFixture struct {
	t    *testing.T
	dir  string
	runs int
	held error
}

func newJobFixture(t *testing.T) *jobFixture {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261028-ec2-job-target-phase-b-tests")
	if err := os.MkdirAll(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(parent, "cmd-")
	if err != nil {
		t.Fatal(err)
	}
	f := &jobFixture{t: t, dir: dir}
	t.Cleanup(f.retire)
	return f
}

func (f *jobFixture) retire() {
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

func (f *jobFixture) keep(err error) {
	if f.held != nil {
		return
	}
	f.held = err
	retain(err)
}

// retained keeps every unsettled original a test hands to retain
// reachable for the life of the test process, which is what owns them.
var retained struct {
	sync.Mutex
	owners []any
}

func retain(v any) {
	retained.Lock()
	retained.owners = append(retained.owners, v)
	retained.Unlock()
}

func (f *jobFixture) mkdir(name string) string {
	f.t.Helper()
	p := filepath.Join(f.dir, name)
	if err := os.Mkdir(p, 0o700); err != nil {
		f.t.Fatal(err)
	}
	return p
}

func (f *jobFixture) write(name, text string, mode os.FileMode) string {
	f.t.Helper()
	p := filepath.Join(f.dir, name)
	if err := os.WriteFile(p, []byte(text), mode); err != nil {
		f.t.Fatal(err)
	}
	if err := os.Chmod(p, mode); err != nil {
		f.t.Fatal(err)
	}
	return p
}

func (f *jobFixture) operate(args ...string) string {
	f.t.Helper()
	root := cli.NewWithClient(nil, nil, cli.ClientOptions{})
	root.SetArgs(args)
	var out, errout bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errout)
	if err := root.ExecuteContext(context.Background()); err != nil {
		var initialization *server.ArtifactInitializationError
		if errors.As(err, &initialization) {
			f.keep(fmt.Errorf("skygo %q: %w", args, err))
		}
		f.t.Fatalf("skygo %q: %v\n%s", args, err, errout.String())
	}
	return out.String()
}

// serve initializes an installation in the fixture and serves it with
// server.Run until the test ends. It returns the installation's paths,
// the admin and bob token files and the listen address.
func (f *jobFixture) serve() (in sshagent.Installation, admin, bob, address string) {
	f.t.Helper()
	base := f.mkdir("installation")
	in = sshagent.Installation{
		DataDir:     filepath.Join(base, "data"),
		AuthFile:    filepath.Join(base, "auth.json"),
		ArtifactDir: filepath.Join(base, "artifacts"),
	}
	admin, bob = filepath.Join(base, "admin.token"), filepath.Join(base, "bob.token")
	f.mkdir(filepath.Join("installation", "data"))
	f.mkdir(filepath.Join("installation", "artifacts"))
	f.operate("auth", "init", "--auth-file", in.AuthFile, "--data-dir", in.DataDir, "--token-output", admin)
	f.operate("auth", "principal", "add", "bob", "--role", "operator", "--auth-file", in.AuthFile)
	f.operate("auth", "token", "issue", "bob", "--output", bob, "--auth-file", in.AuthFile)
	if out := f.operate("state", "artifacts", "init", "--data-dir", in.DataDir, "--artifact-dir", in.ArtifactDir, "--ack-writers-stopped"); out != "enabled: true\npolicy: default-v1\nrestart_required: true\n" {
		f.t.Fatalf("artifact initialization readback %q", out)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		f.t.Fatal(err)
	}
	address = ln.Addr().String()
	if err := ln.Close(); err != nil {
		f.t.Fatal(err)
	}
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
		if err != nil {
			f.keep(fmt.Errorf("server.Run returned: %w", err))
		}
	})
	return in, admin, bob, address
}

func readToken(name string) (string, error) {
	b, err := os.ReadFile(name)
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(string(b))
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return "", errors.New("token file holds no single token")
	}
	return token, nil
}

// jobVMConfig is main_test.go's lifecycle configuration with an arm64
// instance type, as a job's VM requires.
var jobVMConfig = strings.Replace(testConfig, `"t3.micro"`, `"t4g.small"`, 1)

// testInstance is the first instance the ec2test model launches.
const testInstance = "i-000000000000a0001"

// A jobHarness is one script's world.
type jobHarness struct {
	t       *testing.T
	f       *jobFixture
	srv     *ec2test.Server
	address string
	admin   skyv1connect.ControlServiceClient
	cfg     *ec2job.Config // the job configuration the scripts use
	env     map[string]string

	code int           // the last command's exit status
	mark int           // ec2test request total at ec2mark
	cmds []*jobCommand // every job command run, kept until settled

	mu              sync.Mutex
	calls           map[string]int // raw control requests, by method, in every session
	admitted        map[string]int
	refused         map[string]int
	keys            map[string]map[string]bool
	rawMarks        map[string]int
	opens           int
	closes          int
	lose            map[string]int // armed for the next session
	openFail        bool
	openUnsettled   bool
	closeFail       bool
	configUnsettled bool // armed for the next job configuration load
}

// A modelOwner stands, in a MODEL error, for the original file whose
// Close failed. It is not a file and holds nothing.
type modelOwner struct{ name string }

// A modelCloseError is the MODEL counterpart of the error
// ec2job.LoadConfig returns when the configuration file's Close fails:
// it wraps ec2job.ErrUnsettled and holds its owner.
type modelCloseError struct{ owner *modelOwner }

func (e *modelCloseError) Error() string {
	return "job config: model: close of " + e.owner.name + " did not settle"
}
func (e *modelCloseError) Unwrap() error { return ec2job.ErrUnsettled }

// loadConfig is the MODEL job configuration loader: ec2job.LoadConfig,
// unless a failure is armed, which it returns once instead.
func (h *jobHarness) loadConfig(name string) (*ec2job.Config, error) {
	h.mu.Lock()
	fail := h.configUnsettled
	h.configUnsettled = false
	h.mu.Unlock()
	if fail {
		return nil, &modelCloseError{&modelOwner{name: "the configuration file"}}
	}
	return ec2job.LoadConfig(name)
}

// errOpenUnsettled is the MODEL opener's failure whose cleanup did not
// settle, as an *sshagent.OpenError reports one. The model holds no
// connection.
var errOpenUnsettled = errors.New("model: session open failed and its cleanup did not settle")

// settled is the MODEL counterpart of openSettled.
func (h *jobHarness) settled(err error) bool { return !errors.Is(err, errOpenUnsettled) }

func newJobHarness(t *testing.T) *jobHarness {
	t.Helper()
	f := newJobFixture(t)
	in, admin, bob, address := f.serve()
	h := &jobHarness{
		t:        t,
		f:        f,
		srv:      ec2test.NewServer(t, ec2test.Options{Account: "123456789012", Region: "us-west-2", Creds: testCreds}),
		address:  address,
		calls:    map[string]int{},
		admitted: map[string]int{}, refused: map[string]int{}, keys: map[string]map[string]bool{}, rawMarks: map[string]int{},
		lose: map[string]int{},
	}
	creds, err := json.Marshal(map[string]string{
		"access_key_id":     testCreds.AccessKeyID,
		"secret_access_key": testCreds.SecretAccessKey,
		"session_token":     testCreds.SessionToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	jobConfig := func(instance, address, ip string) string {
		deadline := time.Now().UTC().Add(48 * time.Hour).Truncate(time.Second).Format(time.RFC3339)
		return fmt.Sprintf(`{
	"version": 1,
	"vm": {"instance_id": %q, "image_id": "ami-0123456789abcdef0", "instance_type": "t4g.small",
		"architecture": "arm64", "address": %q, "ip": %q, "cpu_millis": 2000, "memory_bytes": 2147483648,
		"safety_deadline": %q, "cleanup_reserve_seconds": 600},
	"ssh": {"port": 22, "user": "ec2-user", "known_hosts_file": %q, "key_file": %q},
	"agent": {"source": %q, "identity_dir": %q, "cache_dir": %q, "target": "ec2vm"},
	"installation": {"data_dir": %q, "auth_file": %q, "artifact_dir": %q, "scoped_token_file": %q},
	"task": {"shell": "/bin/sh", "infra": "aws/us-west-2", "max_timeout_seconds": 3600}
}`, instance, address, ip, deadline,
			filepath.Join(f.dir, "known_hosts"), filepath.Join(f.dir, "id_ed25519"),
			filepath.Join(f.dir, "src"), filepath.Join(f.dir, "identity"), filepath.Join(f.dir, "cache"),
			in.DataDir, in.AuthFile, in.ArtifactDir, bob)
	}
	h.env = map[string]string{
		"CONFIG":           f.write("config.json", jobVMConfig, 0o600),
		"CREDS":            f.write("credentials.json", string(creds), 0o600),
		"STATE":            filepath.Join(f.dir, "vm"),
		"JOBCONFIG":        f.write("job.json", jobConfig(testInstance, "private", "10.0.0.5"), 0o600),
		"JOBCONFIG_PUBLIC": f.write("job-public.json", jobConfig(testInstance, "public", "203.0.113.7"), 0o600),
		"JOBCONFIG_OTHER":  f.write("job-other.json", jobConfig("i-000000000000a0002", "private", "10.0.0.5"), 0o600),
		"JOBCONFIG_OPEN":   f.write("job-open.json", jobConfig(testInstance, "private", "10.0.0.5"), 0o644),
	}
	if h.cfg, err = ec2job.LoadConfig(h.env["JOBCONFIG"]); err != nil {
		t.Fatal(err)
	}
	// A command a script left unsettled holds the fixture: its journal
	// stays locked below the fixture's directory.
	t.Cleanup(func() {
		for _, c := range h.cmds {
			if c.unsettled != nil {
				retain(c)
				f.keep(fmt.Errorf("a job command was left unsettled: %w", c.unsettled))
			}
		}
	})
	token, err := readToken(admin)
	if err != nil {
		t.Fatal(err)
	}
	tr := &http.Transport{}
	t.Cleanup(tr.CloseIdleConnections)
	h.admin = skyv1connect.NewControlServiceClient(&http.Client{Transport: &jobWire{token: token, base: tr}}, "http://"+address)
	h.awaitReady()
	return h
}

func (h *jobHarness) awaitReady() {
	h.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, err := h.admin.GetServerInfo(context.Background(), connect.NewRequest(&v1.GetServerInfoRequest{}))
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("server not ready: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

var errLostReply = errors.New("model: reply lost after the server applied the request")

// A jobWire is the MODEL control channel: loopback HTTP with a bearer. It
// counts raw requests separately from decoded admitted replies and typed
// refusals. It loses the selected admitted reply of its session after reading
// it in full; ordinary Busy replies cannot move the selected frontier.
type jobWire struct {
	h     *jobHarness
	token string
	base  *http.Transport
	calls map[string]int // session admitted responses
	lose  map[string]int
}

func (w *jobWire) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+w.token)
	if w.h == nil {
		return w.base.RoundTrip(req)
	}
	method := strings.TrimPrefix(req.URL.Path, "/skygo.v1.ControlService/")
	w.h.mu.Lock()
	w.h.calls[method]++
	armed := w.lose[method]
	w.h.mu.Unlock()
	tracked := method == "BeginArtifactUpload" || method == "WriteArtifactChunk" || method == "FinalizeArtifactUpload"
	if tracked || armed > 0 {
		req.Header.Set("Accept-Encoding", "identity")
		req.Header.Set("Connect-Accept-Encoding", "identity")
	}
	resp, err := w.base.RoundTrip(req)
	if err != nil || !tracked && armed == 0 {
		return resp, err
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
		w.h.mu.Lock()
		if w.h.keys[method] == nil {
			w.h.keys[method] = map[string]bool{}
		}
		duplicate := w.h.keys[method][key] && (method == "BeginArtifactUpload" || method == "WriteArtifactChunk")
		w.h.keys[method][key] = true
		w.h.admitted[method]++
		w.calls[method]++
		lost = armed == w.calls[method]
		w.h.mu.Unlock()
		if duplicate {
			resp.Body.Close()
			return nil, errors.New("model: duplicate admitted upload identity/range")
		}
		if lost && method == "WriteArtifactChunk" && armed == 2 && end != 2*ec2job.ChunkSize {
			resp.Body.Close()
			return nil, fmt.Errorf("model: second applied chunk ends at %d", end)
		}
	} else if tracked && busyReply(resp, body, method) {
		w.h.mu.Lock()
		w.h.refused[method]++
		w.h.mu.Unlock()
	}
	if lost {
		return nil, errors.Join(errLostReply, resp.Body.Close())
	}
	return resp, nil
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

type modelSession struct {
	h       *jobHarness
	control skyv1connect.ControlServiceClient
	binding sshagent.Binding
	once    sync.Once
	fail    bool
	err     error
}

func (s *modelSession) Control() skyv1connect.ControlServiceClient { return s.control }
func (s *modelSession) Binding() sshagent.Binding                  { return s.binding }

// Close is sticky, as sshagent's is: an armed failure models a close
// whose outcome is unknown.
func (s *modelSession) Close() error {
	s.h.mu.Lock()
	s.h.closes++
	s.h.mu.Unlock()
	s.once.Do(func() {
		if s.fail {
			s.err = errors.New("model: session close outcome unknown")
		}
	})
	return s.err
}

// open is the MODEL sshagent.Open. It refuses any Config but the one the
// job configuration selects for the verified private address.
func (h *jobHarness) open(ctx context.Context, cfg sshagent.Config) (session, error) {
	h.mu.Lock()
	h.opens++
	fail, unsettled, closeFail, lose := h.openFail, h.openUnsettled, h.closeFail, h.lose
	h.openFail, h.openUnsettled, h.closeFail, h.lose = false, false, false, map[string]int{}
	h.mu.Unlock()
	want := h.cfg.Session(netip.MustParseAddr("10.0.0.5"), nil)
	if cfg.Address != "10.0.0.5:22" || cfg.Address != want.Address || cfg.User != want.User ||
		cfg.KnownHostsFile != want.KnownHostsFile || cfg.KeyFile != want.KeyFile || cfg.Target != want.Target ||
		cfg.Installation != want.Installation || cfg.ScopedTokenFile != want.ScopedTokenFile || cfg.Log == nil {
		h.t.Errorf("session opened with %+v, want %+v", cfg, want)
		return nil, errors.New("model: unexpected session configuration")
	}
	if fail {
		return nil, errors.New("model: session refused")
	}
	if unsettled {
		return nil, errOpenUnsettled
	}
	token, err := readToken(cfg.ScopedTokenFile)
	if err != nil {
		return nil, err
	}
	tr := &http.Transport{}
	h.t.Cleanup(tr.CloseIdleConnections)
	w := &jobWire{h: h, token: token, base: tr, calls: map[string]int{}, lose: lose}
	return &modelSession{
		h:       h,
		control: skyv1connect.NewControlServiceClient(&http.Client{Transport: w}, "http://"+h.address),
		binding: sshagent.Binding{
			Address: cfg.Address, User: cfg.User, KnownHostsFile: cfg.KnownHostsFile, KeyFile: cfg.KeyFile,
			Fingerprint: cfg.Fingerprint, AgentAddress: "127.0.0.1:47080", Target: cfg.Target,
			Commit: strings.Repeat("1", 40), Digest: strings.Repeat("2", 64),
			AgentDir: "skygo-test/agents/x", Installation: cfg.Installation,
		},
		fail: closeFail,
	}, nil
}

// exec runs skygo-ec2 with args: a job command through runJob with the
// MODEL opener, any other through run, as main does. The harness keeps
// every job command, as main does until it exits.
func (h *jobHarness) exec(ctx context.Context, args []string) (string, string, error) {
	var out, errb bytes.Buffer
	if len(args) > 0 && jobCommands[args[0]] {
		c := runJob(ctx, args[0], args[1:], jobEnv{&out, &errb, h.srv.NewClient, h.open, h.settled, h.loadConfig})
		h.cmds = append(h.cmds, c)
		h.code = c.code
	} else {
		h.code = run(ctx, args, env{&out, &errb, h.srv.NewClient})
	}
	var err error
	for _, s := range []string{out.String(), errb.String()} {
		for _, secret := range []string{testCreds.AccessKeyID, testCreds.SecretAccessKey, testCreds.SessionToken, ec2test.ProviderMessage} {
			if strings.Contains(s, secret) {
				h.t.Errorf("skygo-ec2 %q output holds a credential or provider message", args)
			}
		}
	}
	if h.code != 0 {
		err = fmt.Errorf("exit status %d", h.code)
	}
	return out.String(), errb.String(), err
}

func (h *jobHarness) mutations() int {
	n := 0
	for _, a := range []string{"RunInstances", "StartInstances", "StopInstances", "TerminateInstances", "CreateTags", "DeleteTags"} {
		n += h.srv.Count(a)
	}
	return n
}

func (h *jobHarness) engine() *script.Engine {
	e := script.NewEngine()
	e.Cmds = scripttest.DefaultCmds()
	e.Conds = scripttest.DefaultConds()
	cmd := func(summary, args string, f func(s *script.State, args ...string) error) script.Cmd {
		return script.Command(script.CmdUsage{Summary: summary, Args: args}, func(s *script.State, args ...string) (script.WaitFunc, error) {
			return nil, f(s, args...)
		})
	}
	count := func(what string, got func() int) func(*script.State, ...string) error {
		return func(_ *script.State, args ...string) error {
			if len(args) != 1 {
				return script.ErrUsage
			}
			want, err := strconv.Atoi(args[0])
			if err != nil {
				return script.ErrUsage
			}
			if n := got(); n != want {
				return fmt.Errorf("%s = %d, want %d", what, n, want)
			}
			return nil
		}
	}
	e.Cmds["skygo-ec2"] = script.Command(script.CmdUsage{Summary: "run skygo-ec2 with args", Args: "args..."}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		out, errb, err := h.exec(s.Context(), args)
		return func(*script.State) (string, string, error) { return out, errb, err }, nil
	})
	e.Cmds["job"] = script.Command(script.CmdUsage{Summary: "run a skygo-ec2 job command with the script's -config, -state, -credentials and -job-config", Args: "command args..."}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) == 0 || !jobCommands[args[0]] {
			return nil, script.ErrUsage
		}
		full := append([]string{args[0], "-config", h.env["CONFIG"], "-state", h.env["STATE"], "-credentials", h.env["CREDS"], "-job-config", h.env["JOBCONFIG"]}, args[1:]...)
		out, errb, err := h.exec(s.Context(), full)
		return func(*script.State) (string, string, error) { return out, errb, err }, nil
	})
	e.Cmds["code"] = cmd("check the last skygo-ec2 exit status", "n", count("exit status", func() int { return h.code }))
	e.Cmds["mutations"] = cmd("check the number of mutating EC2 requests", "n", count("EC2 mutations", h.mutations))
	e.Cmds["ec2mark"] = cmd("remember the EC2 request total", "", func(*script.State, ...string) error { h.mark = h.srv.Total(); return nil })
	e.Cmds["ec2since"] = cmd("check the EC2 requests since ec2mark", "n", count("EC2 requests since mark", func() int { return h.srv.Total() - h.mark }))
	e.Cmds["opens"] = cmd("check the number of sessions opened", "n", count("sessions opened", func() int { h.mu.Lock(); defer h.mu.Unlock(); return h.opens }))
	e.Cmds["closes"] = cmd("check the number of session Close calls", "n", count("session closes", func() int { h.mu.Lock(); defer h.mu.Unlock(); return h.closes }))
	e.Cmds["calls"] = cmd("check the number of control requests of a method", "method n", func(s *script.State, args ...string) error {
		if len(args) != 2 {
			return script.ErrUsage
		}
		return count(args[0]+" requests", func() int { h.mu.Lock(); defer h.mu.Unlock(); return h.calls[args[0]] })(s, args[1])
	})

	e.Cmds["accepted"] = cmd("check typed admitted upload responses and exact raw/refusal accounting", "method n", func(s *script.State, args ...string) error {
		if len(args) != 2 {
			return script.ErrUsage
		}
		method := args[0]
		h.mu.Lock()
		raw, ok, refused, opens := h.calls[method], h.admitted[method], h.refused[method], h.opens
		h.mu.Unlock()
		if raw != ok+refused || refused > 20*opens {
			return fmt.Errorf("%s raw/admitted/typed-refused = %d/%d/%d", method, raw, ok, refused)
		}
		return count(method+" typed admitted responses", func() int { return ok })(s, args[1])
	})
	e.Cmds["rawmark"] = cmd("record raw requests before unknown-outcome reopen", "method", func(_ *script.State, args ...string) error {
		if len(args) != 1 {
			return script.ErrUsage
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		h.rawMarks[args[0]] = h.calls[args[0]]
		return nil
	})
	e.Cmds["rawsince"] = cmd("check exact raw request delta after unknown frontier", "method n", func(s *script.State, args ...string) error {
		if len(args) != 2 {
			return script.ErrUsage
		}
		return count(args[0]+" raw requests after mark", func() int { h.mu.Lock(); defer h.mu.Unlock(); return h.calls[args[0]] - h.rawMarks[args[0]] })(s, args[1])
	})
	e.Cmds["jobs"] = cmd("check the number of jobs the admin sees", "n", func(s *script.State, args ...string) error {
		r, err := h.admin.ListJobs(s.Context(), connect.NewRequest(&v1.ListJobsRequest{PageSize: 100}))
		if err != nil {
			return fmt.Errorf("list jobs: %w", err)
		}
		return count("server jobs", func() int { return len(r.Msg.Jobs) })(s, args...)
	})
	e.Cmds["lose"] = cmd("lose the reply to the n'th request of a method in the next session", "method n", func(_ *script.State, args ...string) error {
		if len(args) != 2 {
			return script.ErrUsage
		}
		n, err := strconv.Atoi(args[1])
		if err != nil || n < 1 {
			return script.ErrUsage
		}
		h.mu.Lock()
		h.lose[args[0]] = n
		h.mu.Unlock()
		return nil
	})
	e.Cmds["openfail"] = cmd("refuse the next session open", "", func(*script.State, ...string) error {
		h.mu.Lock()
		h.openFail = true
		h.mu.Unlock()
		return nil
	})
	e.Cmds["closefail"] = cmd("make the next session's Close fail", "", func(*script.State, ...string) error {
		h.mu.Lock()
		h.closeFail = true
		h.mu.Unlock()
		return nil
	})
	e.Cmds["openunsettled"] = cmd("make the next session open fail with its cleanup unsettled", "", func(*script.State, ...string) error {
		h.mu.Lock()
		h.openUnsettled = true
		h.mu.Unlock()
		return nil
	})
	e.Cmds["configunsettled"] = cmd("make the next job configuration load fail with its file Close unsettled", "", func(*script.State, ...string) error {
		h.mu.Lock()
		h.configUnsettled = true
		h.mu.Unlock()
		return nil
	})
	e.Cmds["keptconfig"] = cmd("check the number of job commands keeping a configuration file's unsettled Close", "n", count("job commands keeping a configuration owner", func() int {
		n := 0
		for _, c := range h.cmds {
			var m *modelCloseError
			if errors.As(c.unsettled, &m) && m.owner != nil {
				n++
			}
		}
		return n
	}))
	e.Cmds["unsettled"] = cmd("check the number of job commands keeping an unsettled owner and their journal", "n", count("unsettled job commands", func() int {
		n := 0
		for _, c := range h.cmds {
			if c.unsettled != nil {
				n++
			}
		}
		return n
	}))
	e.Cmds["release"] = cmd("settle the job commands whose MODEL session owns nothing, closing their journals", "", func(*script.State, ...string) error {
		for _, c := range h.cmds {
			if c.unsettled == nil {
				continue
			}
			if err := c.release(); err != nil {
				return fmt.Errorf("release: %w", err)
			}
			c.unsettled, c.session = nil, nil
		}
		return nil
	})
	e.Cmds["blob"] = cmd("write n deterministic bytes no archive compresses", "file n", func(s *script.State, args ...string) error {
		if len(args) != 2 {
			return script.ErrUsage
		}
		n, err := strconv.Atoi(args[1])
		if err != nil || n < 0 {
			return script.ErrUsage
		}
		var b []byte
		for i := uint64(0); len(b) < n; i++ {
			sum := sha256.Sum256(binary.BigEndian.AppendUint64(nil, i))
			b = append(b, sum[:]...)
		}
		return os.WriteFile(s.Path(args[0]), b[:n], 0o600)
	})
	e.Cmds["ec2state"] = cmd("set the instance's state outside skygo", "state", func(_ *script.State, args ...string) error {
		if len(args) != 1 {
			return script.ErrUsage
		}
		h.srv.SetState(testInstance, args[0])
		return nil
	})
	e.Cmds["output"] = cmd("verify a job's saved output and check one extracted file", "jobdir file content", func(s *script.State, args ...string) error {
		if len(args) != 3 {
			return script.ErrUsage
		}
		j, err := ec2job.Open(s.Path(args[0]))
		if err != nil {
			return err
		}
		st := j.Status()
		if err := j.Close(); err != nil {
			return err
		}
		if st.OutputRef == nil {
			return errors.New("no saved output")
		}
		data, err := os.ReadFile(st.OutputFile)
		if err != nil {
			return err
		}
		if _, err := artifact.Verify(*st.OutputRef, bytes.NewReader(data)); err != nil {
			return err
		}
		dest, err := os.MkdirTemp(h.f.dir, "output-")
		if err != nil {
			return err
		}
		root, err := os.OpenRoot(dest)
		if err != nil {
			return err
		}
		defer root.Close()
		if err := artifact.Extract(root, bytes.NewReader(data), int64(len(data))); err != nil {
			return err
		}
		got, err := os.ReadFile(filepath.Join(dest, filepath.FromSlash(args[1])))
		if err != nil {
			return err
		}
		if string(got) != args[2]+"\n" {
			return fmt.Errorf("%s = %q, want %q", args[1], got, args[2]+"\n")
		}
		return nil
	})
	return e
}

func runJobScript(t *testing.T, file string) {
	h := newJobHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	work := h.f.mkdir("work")
	vars := []string{"WORK=" + work, "PATH=" + os.Getenv("PATH")}
	for k, v := range h.env {
		vars = append(vars, k+"="+v)
	}
	s, err := script.NewState(ctx, work, vars)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := txtar.ParseFile(file)
	if err != nil {
		s.CloseAndWait(io.Discard)
		t.Fatal(err)
	}
	if err := s.ExtractFiles(archive); err != nil {
		s.CloseAndWait(io.Discard)
		t.Fatal(err)
	}
	scripttest.Run(t, h.engine(), s, file, bytes.NewReader(archive.Comment))
}

// TestCommandKeepsEveryCause: an operation's primary error, a MODEL
// reader error whose Close did not settle, and a failed MODEL session
// Close are all returned and all kept by the one command; none replaces
// another. The models hold no file or connection.
func TestCommandKeepsEveryCause(t *testing.T) {
	primary := errors.New("model: operation failed")
	reader := &modelCloseError{&modelOwner{name: "workdir.tar"}}
	h := new(jobHarness)
	s := &modelSession{h: h, fail: true}
	c := new(jobCommand)
	err := c.closeSession(s, errors.Join(primary, reader))
	var m *modelCloseError
	switch {
	case !errors.Is(err, primary) || !errors.As(err, &m) || m != reader || !strings.Contains(err.Error(), "close SSH session"):
		t.Fatalf("closeSession = %v; want every cause", err)
	case !errors.Is(c.unsettled, primary) || !errors.As(c.unsettled, &m) || m != reader:
		t.Fatalf("command keeps %v; want the reader with its cause", c.unsettled)
	case c.session != s || !strings.Contains(c.unsettled.Error(), "session close outcome unknown"):
		t.Fatalf("command keeps session %v and %v; want the failed session too", c.session, c.unsettled)
	case h.closes != 1:
		t.Fatalf("session closed %d times, want 1", h.closes)
	}

	// The twin: a session that closes and a settled error keep nothing.
	c2 := new(jobCommand)
	if err := c2.closeSession(&modelSession{h: h}, primary); err != primary || c2.unsettled != nil || c2.session != nil {
		t.Fatalf("closeSession = %v, keeps %v", err, c2.unsettled)
	}
	retain(c)
}

// TestReadTaskCloseKept: run's task file is real; its Close fails because
// the test's hook closed it first (MODEL of a failed first Close). readTask
// returns no task and an error keeping the original file with both the
// read's and the Close's causes, and the one command keeps it whole. The
// twin, whose Close succeeds, returns an ordinary error the command does
// not keep. No EC2, SSH or job call is made: readTask is local.
func TestReadTaskCloseKept(t *testing.T) {
	// The directory is kept, never removed: the error keeps a file below it.
	dir, err := os.MkdirTemp("", "skygo-ec2-readtask-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("keeping %s: it holds a file an unsettled error keeps", dir)
	big := filepath.Join(dir, "big.json")
	if err := os.WriteFile(big, []byte(strings.Repeat(" ", maxTaskFile+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	var got *os.File
	preclose := func(file *os.File) {
		got = file
		if err := file.Close(); err != nil {
			t.Errorf("native close: %v", err)
		}
	}
	in, err := readTask(jobOptions{task: big}, preclose)
	var tce *taskCloseError
	switch {
	case in != nil:
		t.Fatal("readTask returned a task")
	case !errors.Is(err, ec2job.ErrUnsettled) || !errors.Is(err, os.ErrClosed) || !strings.Contains(err.Error(), "task exceeds 1 MiB"):
		t.Fatalf("readTask = %v; want the size refusal and the unsettled Close", err)
	case !errors.As(err, &tce) || tce.f != got || got == nil:
		t.Fatalf("readTask = %v; want the error to keep the original file", err)
	}
	c := new(jobCommand)
	c.keep(err)
	if c.unsettled != err {
		t.Fatalf("command keeps %v; want the whole error", c.unsettled)
	}
	retain(c)

	// The twin: the same refusal with a Close that succeeds is ordinary.
	in, err = readTask(jobOptions{task: big}, nil)
	c2 := new(jobCommand)
	c2.keep(err)
	if in != nil || err == nil || errors.Is(err, ec2job.ErrUnsettled) || errors.As(err, &tce) || c2.unsettled != nil {
		t.Fatalf("readTask = %v, %v; command keeps %v; want an ordinary refusal", in, err, c2.unsettled)
	}
}
