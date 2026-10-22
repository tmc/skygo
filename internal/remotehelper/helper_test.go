package remotehelper

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"

	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/artifact"
)

// These fixed slots retain only the three specifically owned error branches.
// They are test custody, not a supervisor or evidence of a live native FD.
var helperHeld struct {
	sync.Mutex
	publish                    *helperFileOwner
	originalExec, relativeExec *helperCommandOwner
	drop                       *helperDropOwner
}

type helperFileOwner struct {
	file  *os.File
	cause error
}
type helperCommandOwner struct {
	command *exec.Cmd
	cause   error
	output  []byte
}

func positiveHelperExit(cmd *exec.Cmd, err error, code int) bool {
	var exited *exec.ExitError
	return errors.As(err, &exited) && exited.ProcessState == cmd.ProcessState && cmd.ProcessState != nil && cmd.ProcessState.Exited() && cmd.ProcessState.ExitCode() == code
}

type helperService struct {
	skyv1connect.UnimplementedRemoteDataServiceHandler
	mu                sync.Mutex
	input             []byte
	ref               artifact.Ref
	output            []byte
	reads             int
	polls             int
	abort             bool
	lostWrite         bool
	writeOffsets      []int64
	sealed            bool
	finishCalls       int
	alternatingFinish bool
	finishFailure     connect.Code
	busyPolls         bool
	busyWrites        int
	writeBodies       [][32]byte
	candidate         string
	sealCount         int
	beginCalls        int
	verifyCandidate   bool
}

func (s *helperService) ReadInput(ctx context.Context, r *connect.Request[skyv1.ReadInputRequest]) (*connect.Response[skyv1.ReadInputResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	off := r.Msg.Offset
	end := off + int64(r.Msg.Limit)
	if off < 0 || off >= int64(len(s.input)) {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("range"))
	}
	if end > int64(len(s.input)) {
		end = int64(len(s.input))
	}
	return connect.NewResponse(&skyv1.ReadInputResponse{Data: s.input[off:end], NextOffset: end, Complete: end == int64(len(s.input)), Reference: &skyv1.ArtifactRef{Id: s.ref.ID, Sha256: s.ref.SHA256, Size: s.ref.Size}}), nil
}
func (s *helperService) PollRelease(context.Context, *connect.Request[skyv1.PollReleaseRequest]) (*connect.Response[skyv1.PollReleaseResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.polls++
	if s.busyPolls && s.polls < 8 {
		if s.polls%2 == 1 {
			return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("remote operation busy"))
		}
		return connect.NewResponse(&skyv1.PollReleaseResponse{State: skyv1.RemoteReleaseState_REMOTE_RELEASE_STATE_WAIT, Revision: 7}), nil
	}
	if s.abort {
		return connect.NewResponse(&skyv1.PollReleaseResponse{State: skyv1.RemoteReleaseState_REMOTE_RELEASE_STATE_ABORT}), nil
	}
	zero := int32(0)
	return connect.NewResponse(&skyv1.PollReleaseResponse{State: skyv1.RemoteReleaseState_REMOTE_RELEASE_STATE_COLLECT, Revision: 7, TaskContainerId: "container-original", TaskExitCode: &zero}), nil
}
func (s *helperService) BeginOutput(ctx context.Context, r *connect.Request[skyv1.BeginOutputRequest]) (*connect.Response[skyv1.BeginOutputResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.beginCalls++
	candidate := fmt.Sprintf("%s:%d", r.Msg.Sha256, r.Msg.Size)
	if s.candidate != "" && s.candidate != candidate {
		return nil, connect.NewError(connect.CodeAlreadyExists, errors.New("candidate changed"))
	}
	s.candidate = candidate
	return connect.NewResponse(&skyv1.BeginOutputResponse{State: skyv1.RemoteOutputState_REMOTE_OUTPUT_STATE_WRITABLE, Acknowledged: int64(len(s.output))}), nil
}
func (s *helperService) WriteOutput(ctx context.Context, r *connect.Request[skyv1.WriteOutputRequest]) (*connect.Response[skyv1.WriteOutputResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writeOffsets = append(s.writeOffsets, r.Msg.Offset)
	s.writeBodies = append(s.writeBodies, sha256.Sum256(r.Msg.Data))
	if s.busyWrites > 0 {
		s.busyWrites--
		return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("remote operation busy"))
	}
	off := r.Msg.Offset
	if off == int64(len(s.output)) {
		s.output = append(s.output, r.Msg.Data...)
	} else if off < 0 || off+int64(len(r.Msg.Data)) > int64(len(s.output)) || !bytes.Equal(s.output[off:off+int64(len(r.Msg.Data))], r.Msg.Data) {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("prefix"))
	}
	if s.lostWrite {
		s.lostWrite = false
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("lost original reply"))
	}
	return connect.NewResponse(&skyv1.WriteOutputResponse{Acknowledged: off + int64(len(r.Msg.Data))}), nil
}
func (s *helperService) FinishOutput(context.Context, *connect.Request[skyv1.FinishOutputRequest]) (*connect.Response[skyv1.FinishOutputResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finishCalls++
	if s.alternatingFinish {
		if s.finishCalls%2 == 1 {
			return nil, connect.NewError(s.finishFailure, errors.New("returned transient failure"))
		}
		return connect.NewResponse(&skyv1.FinishOutputResponse{State: skyv1.RemoteOutputSealState_REMOTE_OUTPUT_SEAL_STATE_SEALING}), nil
	}
	if s.verifyCandidate {
		hash := sha256.Sum256(s.output)
		if s.candidate != fmt.Sprintf("%x:%d", hash, len(s.output)) {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("candidate bytes mismatch"))
		}
	}
	if !s.sealed {
		s.sealCount++
	}
	s.sealed = true
	return connect.NewResponse(&skyv1.FinishOutputResponse{State: skyv1.RemoteOutputSealState_REMOTE_OUTPUT_SEAL_STATE_SEALED}), nil
}

func helperClient(t *testing.T, s *helperService) *client {
	t.Helper()
	_, handler := skyv1connect.NewRemoteDataServiceHandler(s)
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	// The production profile requires a DNS host. Its own httptest certificate
	// covers example.com; direct dialing remains scoped to this owned TLS server.
	origin := "https://example.com:" + strings.TrimPrefix(server.URL, "https://127.0.0.1:")
	token := "sgd1." + strings.Repeat("a", 32) + "." + strings.Repeat("b", 32) + "." + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{3}, 32))
	c, err := newClient(origin, string(ca), token)
	if err != nil {
		t.Fatal(err)
	}
	// Actual transport customization is test-only address routing, never TLS skip.
	c.transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, strings.TrimPrefix(server.URL, "https://"))
	}
	t.Cleanup(c.Close)
	return c
}

func testArchive(t *testing.T) []byte {
	t.Helper()
	var b bytes.Buffer
	w := tar.NewWriter(&b)
	data := bytes.Repeat([]byte("content"), 12000)
	if err := w.WriteHeader(&tar.Header{Name: "input.txt", Mode: 0600, Size: int64(len(data)), Format: tar.FormatUSTAR}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
func fixturePaths(t *testing.T) paths {
	t.Helper()
	parent := os.Getenv("SKYGO_ARTIFACT_TEST_ROOT")
	if parent == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			t.Fatal(err)
		}
		parent = filepath.Join(home, "tmp", "skypilot", "20261020-helper-tests")
	}
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(parent, "case-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("retaining failed helper fixture: %s", root)
			return
		}
		if err := os.RemoveAll(root); err != nil {
			t.Error(err)
		}
	})
	p := paths{workspace: filepath.Join(root, "workspace"), helper: filepath.Join(root, "helper"), self: filepath.Join(root, "self"), input: filepath.Join(root, "input"), output: filepath.Join(root, "output")}
	for _, d := range []string{p.workspace, p.helper, p.input, p.output} {
		if err := os.Mkdir(d, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(p.self, []byte("qualified helper copy fixture"), 0555); err != nil {
		t.Fatal(err)
	}
	return p
}
func TestStageActualArchive(t *testing.T) {
	data := testArchive(t)
	digest := sha256.Sum256(data)
	s := &helperService{input: data, ref: artifact.Ref{ID: strings.Repeat("a", 32), SHA256: hex.EncodeToString(digest[:]), Size: int64(len(data))}}
	c := helperClient(t, s)
	p := fixturePaths(t)
	self, _ := os.ReadFile(p.self)
	selfSHA := sha256.Sum256(self)
	env := map[string]string{"SKYGO_DATA_INPUT_ID": s.ref.ID, "SKYGO_DATA_INPUT_SHA256": s.ref.SHA256, "SKYGO_DATA_INPUT_SIZE": fmt.Sprint(len(data)), "SKYGO_DATA_HELPER_SHA256": hex.EncodeToString(selfSHA[:])}
	if err := stage(context.Background(), c, env, p); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(p.workspace, "work", "input.txt"))
	if err != nil || !bytes.Equal(got, bytes.Repeat([]byte("content"), 12000)) {
		t.Fatalf("materialized bytes: %v", err)
	}
	copied, err := os.ReadFile(filepath.Join(p.helper, "bin", "skygo-data"))
	if err != nil || !bytes.Equal(copied, self) {
		t.Fatalf("verified helper copy: %v", err)
	}
	if s.reads != 2 {
		t.Fatalf("reads=%d", s.reads)
	}
	if err := stage(context.Background(), c, env, p); err == nil {
		t.Fatal("accepted existing published work")
	}
}
func TestStageDigestRefusesPublication(t *testing.T) {
	data := testArchive(t)
	s := &helperService{input: data, ref: artifact.Ref{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Size: int64(len(data))}}
	p := fixturePaths(t)
	c := helperClient(t, s)
	env := map[string]string{"SKYGO_DATA_INPUT_ID": s.ref.ID, "SKYGO_DATA_INPUT_SHA256": s.ref.SHA256, "SKYGO_DATA_INPUT_SIZE": fmt.Sprint(len(data)), "SKYGO_DATA_HELPER_SHA256": strings.Repeat("c", 64)}
	if err := stage(context.Background(), c, env, p); err == nil {
		t.Fatal("accepted wrong digest")
	}
	if _, err := os.Lstat(filepath.Join(p.workspace, "work")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("published invalid input")
	}
}
func TestCollectReleaseAndReplay(t *testing.T) {
	s := &helperService{lostWrite: true}
	c := helperClient(t, s)
	p := fixturePaths(t)
	work := filepath.Join(p.workspace, "work")
	if err := os.Mkdir(work, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "out"), bytes.Repeat([]byte("x"), 70000), 0600); err != nil {
		t.Fatal(err)
	}
	if err := collect(context.Background(), c, []string{"out"}, p); err != nil {
		t.Fatal(err)
	}
	if !s.sealed || s.polls != 1 || len(s.writeOffsets) < 3 || s.writeOffsets[0] != s.writeOffsets[1] {
		t.Fatalf("lost-response replay not exercised: %+v", s.writeOffsets)
	}
	if _, err := artifact.Validate(bytes.NewReader(s.output), int64(len(s.output))); err != nil {
		t.Fatal(err)
	}
}
func TestAbortDoesNotReadWorkspace(t *testing.T) {
	s := &helperService{abort: true}
	c := helperClient(t, s)
	p := fixturePaths(t)
	if err := collect(context.Background(), c, []string{"nonexistent"}, p); err == nil || !strings.Contains(err.Error(), "aborted") {
		t.Fatalf("must abort before opening workspace: %v", err)
	}
	if s.sealed || len(s.output) != 0 {
		t.Fatal("aborted collector uploaded")
	}
}
func TestPublishNoReplace(t *testing.T) {
	p := fixturePaths(t)
	for _, n := range []string{".stage", "work"} {
		if err := os.Mkdir(filepath.Join(p.workspace, n), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(p.workspace, "work", "sentinel"), []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(p.workspace)
	if err != nil {
		t.Fatal(err)
	}
	owner := &helperFileOwner{file: f}
	err = publish(int(f.Fd()))
	owner.cause = f.Close() // SAME original, exactly one actual Close attempt.
	if owner.cause != nil {
		helperHeld.Lock()
		helperHeld.publish = owner
		helperHeld.Unlock()
		t.Fatal("original publication parent Close remains held")
	}
	if err == nil {
		t.Fatal("no-replace unexpectedly succeeded")
	}
	if b, err := os.ReadFile(filepath.Join(p.workspace, "work", "sentinel")); err != nil || string(b) != "keep" {
		t.Fatal("changed existing work")
	}
}
func TestExecOriginalProcess(t *testing.T) {
	if os.Getenv("SKYGO_HELPER_EXEC_CHILD") != "" {
		t.Skip("parent only")
	}
	directory := fixturePaths(t).workspace
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperExecChild$")
	cmd.Env = append(os.Environ(), "SKYGO_HELPER_EXEC_CHILD=wrapper", "SKYGO_HELPER_EXEC_DIR="+directory)
	frame := &helperCommandOwner{command: cmd}
	frame.output, frame.cause = cmd.CombinedOutput()
	if !positiveHelperExit(cmd, frame.cause, 17) {
		helperHeld.Lock()
		helperHeld.originalExec = frame
		helperHeld.Unlock()
		t.Fatal("unexpected original exec outcome retained")
	}
	if string(frame.output) != "original-exec-payload\n" {
		t.Fatalf("original exec output: %q", frame.output)
	}
}
func TestHelperExecChild(t *testing.T) {
	switch os.Getenv("SKYGO_HELPER_EXEC_CHILD") {
	case "":
		return
	case "payload":
		fmt.Println("original-exec-payload")
		os.Exit(17)
	case "relative":
		env := []string{}
		for _, v := range os.Environ() {
			if !strings.HasPrefix(v, "SKYGO_HELPER_EXEC_CHILD=") {
				env = append(env, v)
			}
		}
		env = append(env, "SKYGO_HELPER_EXEC_CHILD=payload")
		if err := execute(os.Getenv("SKYGO_HELPER_EXEC_DIR"), []string{"tool", "-test.run=^TestHelperExecChild$"}, env); err != nil {
			fmt.Println("relative lookup refused")
			os.Exit(23)
		}
		os.Exit(99)
	case "wrapper":
		env := []string{}
		for _, v := range os.Environ() {
			if !strings.HasPrefix(v, "SKYGO_HELPER_EXEC_CHILD=") {
				env = append(env, v)
			}
		}
		env = append(env, "SKYGO_HELPER_EXEC_CHILD=payload")
		if err := execute(os.Getenv("SKYGO_HELPER_EXEC_DIR"), []string{os.Args[0], "-test.run=^TestHelperExecChild$"}, env); err != nil {
			fmt.Fprintln(os.Stderr, "exec failed")
			os.Exit(99)
		}
	}
}

func TestStageSymlinkArchiveRefused(t *testing.T) {
	var archive bytes.Buffer
	w := tar.NewWriter(&archive)
	if err := w.WriteHeader(&tar.Header{Name: "escape", Typeflag: tar.TypeSymlink, Linkname: "../outside", Mode: 0777, Format: tar.FormatUSTAR}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(archive.Bytes())
	s := &helperService{input: archive.Bytes(), ref: artifact.Ref{ID: strings.Repeat("a", 32), SHA256: hex.EncodeToString(hash[:]), Size: int64(archive.Len())}}
	p := fixturePaths(t)
	c := helperClient(t, s)
	sentinel := filepath.Join(filepath.Dir(p.workspace), "outside")
	if err := os.WriteFile(sentinel, []byte("private sentinel"), 0600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{"SKYGO_DATA_INPUT_ID": s.ref.ID, "SKYGO_DATA_INPUT_SHA256": s.ref.SHA256, "SKYGO_DATA_INPUT_SIZE": fmt.Sprint(s.ref.Size), "SKYGO_DATA_HELPER_SHA256": strings.Repeat("a", 64)}
	if err := stage(context.Background(), c, env, p); err == nil {
		t.Fatal("accepted symlink archive")
	}
	if _, err := os.Lstat(filepath.Join(p.workspace, "work")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("published unsafe archive")
	}
	if got, err := os.ReadFile(sentinel); err != nil || string(got) != "private sentinel" {
		t.Fatal("outside sentinel changed")
	}
}
func TestCollectSymlinkRefused(t *testing.T) {
	s := &helperService{}
	c := helperClient(t, s)
	p := fixturePaths(t)
	work := filepath.Join(p.workspace, "work")
	if err := os.Mkdir(work, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(p.self, filepath.Join(work, "out")); err != nil {
		t.Fatal(err)
	}
	if err := collect(context.Background(), c, []string{"out"}, p); err == nil {
		t.Fatal("collected symlink")
	}
	if len(s.output) != 0 || s.sealed {
		t.Fatal("published unsafe output")
	}
}

func TestFinishRetryBudgetSurvivesSealing(t *testing.T) {
	for _, code := range []connect.Code{connect.CodeUnavailable, connect.CodeResourceExhausted} {
		t.Run(code.String(), func(t *testing.T) {
			s := &helperService{alternatingFinish: true, finishFailure: code}
			c := helperClient(t, s)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			err := upload(ctx, c, strings.Repeat("a", 64), []byte("x"))
			if connect.CodeOf(err) != code || s.finishCalls != 5 {
				t.Fatalf("cumulative three failures not enforced: calls=%d err=%v", s.finishCalls, err)
			}
		})
	}
}

// Ordinary WAIT replies between Busy refusals do not renew the failure budget.
func TestReleaseWaitDoesNotResetBudget(t *testing.T) {
	s := &helperService{busyPolls: true}
	c := helperClient(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := waitRelease(ctx, c)
	if connect.CodeOf(err) != connect.CodeResourceExhausted || !connect.IsWireError(err) || s.polls != 5 {
		t.Fatalf("WAIT reset the Busy budget: polls=%d err=%v", s.polls, err)
	}
}

// A Busy WriteOutput is repeated with the identical offset and body; a
// persistent Busy stops after three attempts, before any Finish.
func TestWriteBusyRetryIdenticalAndBounded(t *testing.T) {
	data := testArchive(t)
	hash := sha256.Sum256(data)
	first := sha256.Sum256(data[:64<<10])
	for _, tc := range []struct {
		name  string
		busy  int
		calls int // write calls on the first range
	}{{"release", 2, 3}, {"persistent", 99, 3}} {
		t.Run(tc.name, func(t *testing.T) {
			s := &helperService{busyWrites: tc.busy}
			c := helperClient(t, s)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			err := upload(ctx, c, hex.EncodeToString(hash[:]), data)
			s.mu.Lock()
			defer s.mu.Unlock()
			if len(s.writeOffsets) < tc.calls {
				t.Fatalf("write calls=%d err=%v", len(s.writeOffsets), err)
			}
			for i := range tc.calls {
				if s.writeOffsets[i] != 0 || s.writeBodies[i] != first {
					t.Fatalf("retry %d changed offset or body: offset=%d", i, s.writeOffsets[i])
				}
			}
			if tc.busy < tc.calls {
				if err != nil || !bytes.Equal(s.output, data) || !s.sealed || len(s.writeOffsets) != tc.calls+1 || s.writeOffsets[tc.calls] != 64<<10 {
					t.Fatalf("released Busy write: %v offsets=%v", err, s.writeOffsets)
				}
				return
			}
			if connect.CodeOf(err) != connect.CodeResourceExhausted || !connect.IsWireError(err) || len(s.writeOffsets) != 3 || s.finishCalls != 0 || len(s.output) != 0 {
				t.Fatalf("persistent Busy write not bounded: %v offsets=%v finish=%d", err, s.writeOffsets, s.finishCalls)
			}
		})
	}
}

// A ResourceExhausted produced by the local transport bound is policy, not
// server Busy: it is never retried.
func TestLocalResourceExhaustedNotRetried(t *testing.T) {
	var requests atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/proto")
		w.WriteHeader(http.StatusOK)
		// Two flushed writes force a chunked body past the 8KiB response bound.
		for range 2 {
			_, _ = w.Write(bytes.Repeat([]byte{0}, 5<<10))
			http.NewResponseController(w).Flush()
		}
	})
	server, origin, ca := transportTestTLS(t, "helper.example", handler)
	c := transportTestClient(t, server, origin, ca)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := waitRelease(ctx, c)
	if connect.CodeOf(err) != connect.CodeResourceExhausted || connect.IsWireError(err) || !errors.Is(err, errTransportPolicy) || requests.Load() != 1 {
		t.Fatalf("local transport policy retried: requests=%d err=%v", requests.Load(), err)
	}
}

// The transport now latches its own read-limit refusal, so retryable's wire
// check is a second guard; it is tested directly.
func TestRetryable(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"wire busy", connect.NewWireError(connect.CodeResourceExhausted, errors.New("remote operation busy")), true},
		{"local policy", connect.NewError(connect.CodeResourceExhausted, errTransportPolicy), false},
		{"unavailable", connect.NewError(connect.CodeUnavailable, errors.New("unavailable")), true},
		{"deadline", connect.NewError(connect.CodeDeadlineExceeded, errors.New("deadline")), true},
		{"precondition", connect.NewError(connect.CodeFailedPrecondition, errTransportPolicy), false},
		{"data loss", connect.NewWireError(connect.CodeDataLoss, errors.New("data loss")), false},
	} {
		if got := retryable(tc.err); got != tc.want {
			t.Errorf("%s: retryable = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestExecRelativePATHDoesNotSelectLaterProgram(t *testing.T) {
	p := fixturePaths(t)
	for _, name := range []string{"bin", "absolute"} {
		if err := os.Mkdir(filepath.Join(p.workspace, name), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(os.Args[0], filepath.Join(p.workspace, name, "tool")); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperExecChild$")
	cmd.Env = append(os.Environ(), "SKYGO_HELPER_EXEC_CHILD=relative", "SKYGO_HELPER_EXEC_DIR="+p.workspace, "PATH=bin:"+filepath.Join(p.workspace, "absolute"))
	frame := &helperCommandOwner{command: cmd}
	frame.output, frame.cause = cmd.CombinedOutput()
	if !positiveHelperExit(cmd, frame.cause, 23) {
		helperHeld.Lock()
		helperHeld.relativeExec = frame
		helperHeld.Unlock()
		t.Fatal("unexpected original PATH exec outcome retained")
	}
	if string(frame.output) != "relative lookup refused\n" {
		t.Fatalf("relative PATH output: %q", frame.output)
	}
}

// helperDropOwner owns the actual hijacked TLS connection through one Close.
// A failure keeps that exact connection/error and server frame, never retries.
type helperDropConnection struct {
	connection net.Conn
	cause      error
}
type helperDropOwner struct {
	mu          sync.Mutex
	connections [2]*helperDropConnection
	cause       error
	dropped     map[string]int
	server      *httptest.Server
}

func TestActualTLSWriteAndFinishReplyLoss(t *testing.T) {
	service := &helperService{verifyCandidate: true}
	_, generated := skyv1connect.NewRemoteDataServiceHandler(service)
	drop := &helperDropOwner{dropped: make(map[string]int)}
	wrapper := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorded := httptest.NewRecorder()
		generated.ServeHTTP(recorded, r)
		selected := r.URL.Path == skyv1connect.RemoteDataServiceWriteOutputProcedure || r.URL.Path == skyv1connect.RemoteDataServiceFinishOutputProcedure
		drop.mu.Lock()
		first := selected && drop.dropped[r.URL.Path] == 0
		if first {
			drop.dropped[r.URL.Path]++
		}
		drop.mu.Unlock()
		if first {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				drop.mu.Lock()
				drop.cause = errors.New("HTTP1 hijacker unavailable")
				drop.mu.Unlock()
				return
			}
			frame := &helperDropConnection{}
			index := 0
			if r.URL.Path == skyv1connect.RemoteDataServiceFinishOutputProcedure {
				index = 1
			}
			drop.mu.Lock()
			drop.connections[index] = frame
			drop.mu.Unlock()
			conn, _, err := hijacker.Hijack()
			drop.mu.Lock()
			frame.connection = conn
			frame.cause = err
			if err != nil && drop.cause == nil {
				drop.cause = err
			}
			drop.mu.Unlock()
			if err == nil {
				err = conn.Close()
				drop.mu.Lock()
				frame.cause = err
				if err != nil && drop.cause == nil {
					drop.cause = err
				}
				drop.mu.Unlock()
			}
			if err != nil {
				helperHeld.Lock()
				helperHeld.drop = drop
				helperHeld.Unlock()
			}
			return
		}
		for key, values := range recorded.Header() {
			for _, v := range values {
				w.Header().Add(key, v)
			}
		}
		w.WriteHeader(recorded.Code)
		_, _ = w.Write(recorded.Body.Bytes())
	})
	server := httptest.NewTLSServer(wrapper)
	drop.server = server
	t.Cleanup(func() {
		drop.mu.Lock()
		held := drop.cause != nil
		drop.mu.Unlock()
		if !held {
			server.Close()
		} else {
			helperHeld.Lock()
			helperHeld.drop = drop
			helperHeld.Unlock()
		}
	})
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	origin := "https://example.com:" + strings.TrimPrefix(server.URL, "https://127.0.0.1:")
	c, err := newClient(origin, string(ca), transportTestToken())
	if err != nil {
		t.Fatal(err)
	}
	c.transport.ForceAttemptHTTP2 = false
	c.transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, strings.TrimPrefix(server.URL, "https://"))
	}
	t.Cleanup(c.Close)
	data := testArchive(t)
	hash := sha256.Sum256(data)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := upload(ctx, c, hex.EncodeToString(hash[:]), data); err != nil {
		t.Fatal(err)
	}
	drop.mu.Lock()
	dropErr := drop.cause
	writeDrops := drop.dropped[skyv1connect.RemoteDataServiceWriteOutputProcedure]
	finishDrops := drop.dropped[skyv1connect.RemoteDataServiceFinishOutputProcedure]
	drop.mu.Unlock()
	if dropErr != nil {
		helperHeld.Lock()
		helperHeld.drop = drop
		helperHeld.Unlock()
		t.Fatal("original drop connection remains held")
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if writeDrops != 1 || finishDrops != 1 || service.beginCalls != 1 || service.finishCalls != 2 || service.sealCount != 1 || !bytes.Equal(service.output, data) || len(service.writeOffsets) != 3 || service.writeOffsets[0] != 0 || service.writeOffsets[1] != 0 || service.writeOffsets[2] != 64<<10 || service.candidate != fmt.Sprintf("%x:%d", hash, len(data)) {
		t.Fatalf("actual accepted-loss replay changed prefix/candidate: write=%v finish=%d seals=%d", service.writeOffsets, service.finishCalls, service.sealCount)
	}
}
