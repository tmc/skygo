package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/skycompat/tasklower"
)

func TestWorkspaceFlagRefusals(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "unexpected network", http.StatusInternalServerError)
	}))
	defer server.Close()
	for _, tt := range []struct {
		name  string
		args  []string
		input string
	}{
		{"false", []string{"run", "-", "--sync-workdir=false"}, "workdir: .\nrun: echo hi"},
		{"no-path", []string{"run", "--sync-workdir"}, ""},
		{"state-alone", []string{"run", "-", "--workspace-state", "private"}, "run: hi"},
		{"empty-state", []string{"run", "-", "--sync-workdir", "--workspace-state="}, "workdir: .\nrun: hi"},
		{"empty-resume", []string{"run", "--resume-workspace="}, ""},
		{"resume-path", []string{"run", "task.yaml", "--resume-workspace", "private"}, ""},
		{"resume-file-empty", []string{"run", "--resume-workspace", "private", "--file="}, ""},
		{"resume-sync-false", []string{"run", "--resume-workspace", "private", "--sync-workdir=false"}, ""},
		{"resume-state-empty", []string{"run", "--resume-workspace", "private", "--workspace-state="}, ""},
		{"resume-env-empty", []string{"run", "--resume-workspace", "private", "--env="}, ""},
		{"resume-output-empty", []string{"run", "--resume-workspace", "private", "--output="}, ""},
		{"resume-ref-empty", []string{"run", "--resume-workspace", "private", "--workdir-ref="}, ""},
		{"resume-shell-empty", []string{"run", "--resume-workspace", "private", "--shell="}, ""},
		{"resume-target-empty", []string{"run", "--resume-workspace", "private", "--target="}, ""},
		{"resume-id-empty", []string{"run", "--resume-workspace", "private", "--request-id="}, ""},
		{"resume-domain-empty", []string{"run", "--resume-workspace", "private", "--request-domain="}, ""},
		{"resume-remote-false", []string{"run", "--resume-workspace", "private", "--remote-data=false"}, ""},
		{"sync-ref-empty", []string{"run", "-", "--sync-workdir", "--workdir-ref="}, ""},
		{"sync-remote-false", []string{"run", "-", "--sync-workdir", "--remote-data=false"}, ""},
		{"native", []string{"run", "-", "--sync-workdir"}, "argv: [echo, hi]"},
		{"mixed", []string{"run", "-", "--sync-workdir"}, "argv: [echo, hi]\nrun: hi"},
		{"missing-workdir", []string{"run", "-", "--sync-workdir"}, "run: hi"},
		{"invalid-setup", []string{"run", "-", "--sync-workdir"}, "workdir: .\nsetup: [bad]\nrun: hi"},
		{"unresolved-env", []string{"run", "-", "--sync-workdir"}, "workdir: .\nenvs: {AMBIENT_WORKSPACE_SECRET: null}\nrun: hi"},
		{"duplicate-env", []string{"run", "-", "--sync-workdir", "--env", "A=one", "--env", "A=two"}, "workdir: .\nrun: hi"},
		{"empty-id", []string{"run", "-", "--sync-workdir", "--request-id="}, "workdir: .\nrun: hi"},
		{"empty-domain", []string{"run", "-", "--sync-workdir", "--request-domain="}, "workdir: .\nrun: hi"},
		{"plan", []string{"plan", "-", "--sync-workdir"}, "workdir: .\nrun: hi"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("SKYGO_TOKEN", "")
			t.Setenv("AMBIENT_WORKSPACE_SECRET", "secret-not-an-override")
			command := New(nil)
			command.SetArgs(append([]string{"--endpoint", server.URL}, tt.args...))
			command.SetIn(strings.NewReader(tt.input))
			command.SetOut(new(bytes.Buffer))
			command.SetErr(new(bytes.Buffer))
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := command.ExecuteContext(ctx); err == nil {
				t.Fatal("invalid workspace command accepted")
			}
			if got := requests.Load(); got != 0 {
				t.Fatalf("refusal made %d network requests", got)
			}
		})
	}
}

func TestWorkspaceIdentity(t *testing.T) {
	for _, tt := range []struct {
		endpoint, canonical string
	}{
		{"http://LOCALHOST:8080/", "http://localhost:8080"},
		{"https://example.com", "https://example.com"},
		{"ssh://host", ""},
		{"http://user@host", ""},
		{"http://host/path", ""},
		{"http://host?secret", ""},
		{"http://host#fragment", ""},
		{"http:///", ""},
		{"http://[fe80::1%25EthA]:8080", ""},
		{"http://[fe80::1%25etha]:8080", ""},
	} {
		t.Run(tt.endpoint, func(t *testing.T) {
			o := &options{endpoint: tt.endpoint, token: "resolved-effective-credential"}
			endpoint, credential, err := o.workspaceIdentity()
			if tt.canonical == "" {
				if err == nil {
					t.Fatal("invalid endpoint accepted")
				}
				return
			}
			digest := sha256.Sum256([]byte(o.token))
			if err != nil || endpoint != tt.canonical || credential != hex.EncodeToString(digest[:]) {
				t.Fatalf("identity=%q %q %v", endpoint, credential, err)
			}
			r := &workspaceRun{intent: workspaceIntent{Endpoint: endpoint, CredentialSHA256: credential}}
			if err := o.workspaceContinuity(r); err != nil {
				t.Fatal(err)
			}
			o.token = "rotated"
			if err := o.workspaceContinuity(r); err == nil {
				t.Fatal("credential rotation accepted")
			}
		})
	}
	for _, client := range []*http.Client{{}, {Transport: http.DefaultTransport}} {
		if _, _, err := (&options{endpoint: "http://localhost", http: client}).workspaceIdentity(); err == nil {
			t.Fatal("unverified selected client accepted")
		}
	}
}

func TestWorkspaceDocumentFrozenOptions(t *testing.T) {
	input := []byte("workdir: .\nenvs: {NEEDED: null}\nrun: echo run\n")
	_, selected, err := workspaceDocument(input, tasklower.Options{}, skypilotInputs{env: []string{"NEEDED=a,b=c"}, outputs: []string{"out"}})
	if err != nil || selected.Shell != defaultShell || len(selected.Outputs) != 1 {
		t.Fatalf("workspace options=%+v %v", selected, err)
	}
	intent := workspaceIntent{YAML: input, Shell: selected.Shell, Env: []string{"NEEDED=a,b=c"}, Outputs: selected.Outputs}
	doc, lower, err := frozenWorkspaceDocument(intent)
	if err != nil || doc.Task.Envs["NEEDED"] != "a,b=c" || lower.Workdir != nil {
		t.Fatalf("frozen document=%+v options=%+v %v", doc, lower, err)
	}
}

func TestWorkspaceSharedInputBound(t *testing.T) {
	for _, input := range []string{strings.Repeat("x", (1<<20)+1), ""} {
		c := New(nil)
		c.SetIn(strings.NewReader(input))
		if input == "" {
			if _, err := readTaskBytes(c, ""); err == nil {
				t.Fatal("missing input accepted")
			}
		} else if _, err := readTaskBytes(c, "-"); err == nil {
			t.Fatal("oversized input accepted")
		}
	}
	dir := t.TempDir()
	file := filepath.Join(dir, "task.yaml")
	if err := os.WriteFile(file, []byte("run: echo hi"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := readTaskBytes(New(nil), file)
	if err != nil || string(got) != "run: echo hi" {
		t.Fatalf("shared read=%q %v", got, err)
	}
}

func TestWorkspaceContinuityBeforeNetwork(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); http.Error(w, "unexpected", 500) }))
	defer server.Close()
	for _, phase := range []string{"prepared", "begin", "upload", "finalize", "sealed", "submit", "accepted"} {
		t.Run(phase, func(t *testing.T) {
			o := &options{endpoint: server.URL, token: "original"}
			endpoint, credential, err := o.workspaceIdentity()
			if err != nil {
				t.Fatal(err)
			}
			r := &workspaceRun{intent: workspaceIntent{Endpoint: endpoint, CredentialSHA256: credential}, state: workspaceState{Phase: phase, JobID: strings.Repeat("a", 32)}}
			o.token = "rotated"
			if _, err := o.continueWorkspace(context.Background(), r, new(bytes.Buffer)); err == nil {
				t.Fatal("rotated intent admitted")
			}
			if calls.Load() != 0 {
				t.Fatal("credential refusal made a network request")
			}
		})
	}
}

type workspaceBeginPeer struct {
	skyv1connect.UnimplementedControlServiceHandler
	mu       sync.Mutex
	requests []*v1.BeginArtifactUploadRequest
}

func (s *workspaceBeginPeer) BeginArtifactUpload(_ context.Context, r *connect.Request[v1.BeginArtifactUploadRequest]) (*connect.Response[v1.BeginArtifactUploadResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, proto.Clone(r.Msg).(*v1.BeginArtifactUploadRequest))
	if len(s.requests) == 1 {
		return nil, connect.NewError(connect.CodeUnavailable, context.DeadlineExceeded)
	}
	return connect.NewResponse(&v1.BeginArtifactUploadResponse{Upload: &v1.ArtifactUpload{Id: strings.Repeat("a", 32)}}), nil
}

// This tests the actual keyed helper against a Connect peer's transient error;
// genuine durable Store/Files response-loss coverage belongs to the script lane.
func TestWorkspaceBeginKeepsRequestIdentity(t *testing.T) {
	peer := new(workspaceBeginPeer)
	_, handler := skyv1connect.NewControlServiceHandler(peer)
	server := httptest.NewServer(handler)
	defer server.Close()
	o := &options{endpoint: server.URL}
	endpoint, credential, err := o.workspaceIdentity()
	if err != nil {
		t.Fatal(err)
	}
	r := &workspaceRun{intent: workspaceIntent{Endpoint: endpoint, CredentialSHA256: credential, RequestID: "stable"}, state: workspaceState{Phase: "begin", Domain: &v1.RequestDomain{Lineage: strings.Repeat("b", 32), Epoch: 4}}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := o.beginWorkspaceUpload(ctx, o.artifactClient(), r, strings.Repeat("c", 64), 1024, new(artifactUploadRetries)); err != nil {
		t.Fatal(err)
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if len(peer.requests) != 2 || !proto.Equal(peer.requests[0], peer.requests[1]) || peer.requests[1].RequestId == nil || peer.requests[1].GetRequestId() != "stable" || !proto.Equal(peer.requests[1].Domain, r.state.Domain) {
		t.Fatalf("changed keyed identity: %v", peer.requests)
	}
}

// These wrappers change the resolved options only after an actual HTTP peer
// returns. The next inner retry/status read must refuse before its transport.
type workspaceChangingClient struct {
	skyv1connect.ControlServiceClient
	options *options
}

func (c workspaceChangingClient) WriteArtifactChunk(ctx context.Context, r *connect.Request[v1.WriteArtifactChunkRequest]) (*connect.Response[v1.WriteArtifactChunkResponse], error) {
	out, err := c.ControlServiceClient.WriteArtifactChunk(ctx, r)
	c.options.token = "changed-after-response"
	return out, err
}
func (c workspaceChangingClient) FinalizeArtifactUpload(ctx context.Context, r *connect.Request[v1.FinalizeArtifactUploadRequest]) (*connect.Response[v1.FinalizeArtifactUploadResponse], error) {
	out, err := c.ControlServiceClient.FinalizeArtifactUpload(ctx, r)
	c.options.token = "changed-after-response"
	return out, err
}
func (c workspaceChangingClient) GetJob(ctx context.Context, r *connect.Request[v1.GetJobRequest]) (*connect.Response[v1.GetJobResponse], error) {
	out, err := c.ControlServiceClient.GetJob(ctx, r)
	c.options.token = "changed-after-response"
	return out, err
}

type workspaceRetryPeer struct {
	skyv1connect.UnimplementedControlServiceHandler
	mu    sync.Mutex
	calls map[string]int
}

func (s *workspaceRetryPeer) record(method string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[method]++
}
func (s *workspaceRetryPeer) WriteArtifactChunk(context.Context, *connect.Request[v1.WriteArtifactChunkRequest]) (*connect.Response[v1.WriteArtifactChunkResponse], error) {
	s.record("write")
	return nil, connect.NewError(connect.CodeUnavailable, context.DeadlineExceeded)
}
func (s *workspaceRetryPeer) FinalizeArtifactUpload(context.Context, *connect.Request[v1.FinalizeArtifactUploadRequest]) (*connect.Response[v1.FinalizeArtifactUploadResponse], error) {
	s.record("finalize")
	return nil, connect.NewError(connect.CodeUnavailable, context.DeadlineExceeded)
}
func (s *workspaceRetryPeer) GetArtifactUpload(context.Context, *connect.Request[v1.GetArtifactUploadRequest]) (*connect.Response[v1.GetArtifactUploadResponse], error) {
	s.record("get-upload")
	return nil, connect.NewError(connect.CodeUnavailable, context.DeadlineExceeded)
}
func (s *workspaceRetryPeer) GetJob(context.Context, *connect.Request[v1.GetJobRequest]) (*connect.Response[v1.GetJobResponse], error) {
	s.record("get-job")
	return connect.NewResponse(&v1.GetJobResponse{Job: &v1.Job{Id: strings.Repeat("a", 32), State: v1.JobState_JOB_STATE_QUEUED}}), nil
}

func TestWorkspaceInnerRetryContinuity(t *testing.T) {
	for _, method := range []string{"write", "finalize", "get-job"} {
		t.Run(method, func(t *testing.T) {
			peer := &workspaceRetryPeer{calls: make(map[string]int)}
			_, handler := skyv1connect.NewControlServiceHandler(peer)
			server := httptest.NewServer(handler)
			defer server.Close()
			o := &options{endpoint: server.URL, timeout: time.Second}
			endpoint, credential, err := o.workspaceIdentity()
			if err != nil {
				t.Fatal(err)
			}
			run := &workspaceRun{intent: workspaceIntent{Endpoint: endpoint, CredentialSHA256: credential}}
			client := workspaceChangingClient{ControlServiceClient: o.artifactClient(), options: o}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			switch method {
			case "write":
				_, err = o.writeArtifactChunkState(ctx, client, strings.Repeat("a", 32), artifact.Ref{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Size: 4}, 0, []byte("data"), new(artifactUploadRetries), run)
			case "finalize":
				_, err = o.finalizeArtifactUploadState(ctx, client, strings.Repeat("a", 32), new(artifactUploadRetries), run)
			case "get-job":
				_, err = o.waitState(ctx, client, &v1.Job{Id: strings.Repeat("a", 32), State: v1.JobState_JOB_STATE_QUEUED}, run)
			}
			if err == nil || !strings.Contains(err.Error(), "credential changed") {
				t.Fatalf("inner continuation did not refuse: %v", err)
			}
			peer.mu.Lock()
			defer peer.mu.Unlock()
			if len(peer.calls) != 1 || peer.calls[method] != 1 {
				t.Fatalf("additional RPC after changed credential: %v", peer.calls)
			}
		})
	}
}

func workspaceHTTPJournal(t *testing.T, o *options) *workspaceRun {
	t.Helper()
	dir := workspaceTestDirectory(t)
	source := filepath.Join(dir, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "input"), []byte("immutable input"), 0600); err != nil {
		t.Fatal(err)
	}
	intent := workspaceTestIntent(source)
	var err error
	intent.Endpoint, intent.CredentialSHA256, err = o.workspaceIdentity()
	if err != nil {
		t.Fatal(err)
	}
	r, err := openWorkspaceRun(filepath.Join(dir, "journal"), intent)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !r.closed {
			if err := r.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	archive, _, err := snapshotWorkspace(source, r.dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.saveArchive(archive); err != nil {
		t.Fatal(err)
	}
	next := r.state
	next.Phase, next.Domain = "begin", &v1.RequestDomain{Lineage: strings.Repeat("d", 32), Epoch: 1}
	if err := r.saveState(next); err != nil {
		t.Fatal(err)
	}
	o.workspaceOwner = r
	return r
}

func workspaceJournalUpload(t *testing.T, r *workspaceRun, phase string) {
	t.Helper()
	next := r.state
	next.Phase = "upload"
	next.Upload = &v1.ArtifactRef{Id: strings.Repeat("a", 32), Sha256: r.state.ArchiveSHA256, Size: r.state.ArchiveSize}
	if err := r.saveState(next); err != nil {
		t.Fatal(err)
	}
	if phase == "finalize" || phase == "submit" {
		next = r.state
		next.Phase = "finalize"
		if err := r.saveState(next); err != nil {
			t.Fatal(err)
		}
	}
	if phase == "submit" {
		task := &v1.Task{Argv: []string{"/bin/sh", "-c", "echo frozen"}, Workdir: proto.Clone(r.state.Upload).(*v1.ArtifactRef)}
		encoded, err := protojson.Marshal(task)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.saveTask(encoded); err != nil {
			t.Fatal(err)
		}
		next = r.state
		next.Phase = "submit"
		if err := r.saveState(next); err != nil {
			t.Fatal(err)
		}
	}
}

type workspaceCapabilityPeer struct {
	skyv1connect.UnimplementedControlServiceHandler
}

func (workspaceCapabilityPeer) GetServerInfo(context.Context, *connect.Request[v1.GetServerInfoRequest]) (*connect.Response[v1.GetServerInfoResponse], error) {
	return connect.NewResponse(&v1.GetServerInfoResponse{Capabilities: []v1.Capability{v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1, v1.Capability_CAPABILITY_ARTIFACT_BEGIN_REQUESTS_V1}}), nil
}

// Genuine A journal files and actual Connect/HTTP redirect responses are used.
// No remote admission or native crash claim is made by this negative test.
func TestWorkspaceRedirectRefusal(t *testing.T) {
	for _, status := range []int{http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		for _, method := range []string{"begin", "submit"} {
			t.Run(fmt.Sprintf("%d/%s", status, method), func(t *testing.T) {
				var destinationCalls, forwardedToken, sourceCalls atomic.Int32
				destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					destinationCalls.Add(1)
					if r.Header.Get("Authorization") != "" {
						forwardedToken.Add(1)
					}
					http.Error(w, "unexpected destination", 500)
				}))
				defer destination.Close()
				_, base := skyv1connect.NewControlServiceHandler(workspaceCapabilityPeer{})
				procedure := skyv1connect.ControlServiceBeginArtifactUploadProcedure
				if method == "submit" {
					procedure = skyv1connect.ControlServiceSubmitWorkspaceJobProcedure
				}
				source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != procedure {
						base.ServeHTTP(w, r)
						return
					}
					sourceCalls.Add(1)
					w.Header().Set("Location", destination.URL+r.URL.Path)
					w.WriteHeader(status)
				}))
				defer source.Close()
				original := &options{endpoint: source.URL, token: "private-credential", timeout: time.Second}
				o, err := original.workspaceOptions()
				if err != nil {
					t.Fatal(err)
				}
				if o.httpClient() == original.httpClient() || o.httpClient().Transport != original.httpClient().Transport || o.httpClient().Timeout != original.httpClient().Timeout || original.httpClient().CheckRedirect != nil {
					t.Fatal("workspace client changed original transport or redirect policy")
				}
				run := workspaceHTTPJournal(t, o)
				if method == "submit" {
					workspaceJournalUpload(t, run, "submit")
				}
				before := workspaceTestFiles(t, run.dir)
				state := run.state
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if method == "begin" {
					_, err = o.beginWorkspaceUpload(ctx, o.artifactClient(), run, run.state.ArchiveSHA256, run.state.ArchiveSize, new(artifactUploadRetries))
				} else {
					_, err = o.continueWorkspace(ctx, run, new(bytes.Buffer))
				}
				if err == nil || artifactBeginNotStarted(err) {
					t.Fatalf("redirect acquired successful/nonadmission result: %v", err)
				}
				if sourceCalls.Load() != 1 || destinationCalls.Load() != 0 || forwardedToken.Load() != 0 {
					t.Fatalf("redirect traffic: source=%d destination=%d credential=%d", sourceCalls.Load(), destinationCalls.Load(), forwardedToken.Load())
				}
				if !reflect.DeepEqual(state, run.state) || !reflect.DeepEqual(before, workspaceTestFiles(t, run.dir)) {
					t.Fatal("redirect advanced durable workspace intent")
				}
			})
		}
	}
}

type workspacePartialPeer struct {
	workspaceCapabilityPeer
	mu                sync.Mutex
	ref               *v1.ArtifactRef
	writes, finalizes atomic.Int32
}

func (s *workspacePartialPeer) GetArtifactUpload(context.Context, *connect.Request[v1.GetArtifactUploadRequest]) (*connect.Response[v1.GetArtifactUploadResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return connect.NewResponse(&v1.GetArtifactUploadResponse{Upload: &v1.ArtifactUpload{Id: s.ref.Id, Reference: proto.Clone(s.ref).(*v1.ArtifactRef), Acknowledged: s.ref.Size - 1, State: v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_WRITABLE, ExpiresAt: timestamppb.New(time.Now().Add(time.Hour))}}), nil
}
func (s *workspacePartialPeer) WriteArtifactChunk(context.Context, *connect.Request[v1.WriteArtifactChunkRequest]) (*connect.Response[v1.WriteArtifactChunkResponse], error) {
	s.writes.Add(1)
	return nil, connect.NewError(connect.CodeUnknown, context.Canceled)
}
func (s *workspacePartialPeer) FinalizeArtifactUpload(context.Context, *connect.Request[v1.FinalizeArtifactUploadRequest]) (*connect.Response[v1.FinalizeArtifactUploadResponse], error) {
	s.finalizes.Add(1)
	return nil, connect.NewError(connect.CodeUnknown, context.Canceled)
}

func TestWorkspaceFinalizeRejectsPartialAcknowledgment(t *testing.T) {
	peer := new(workspacePartialPeer)
	_, handler := skyv1connect.NewControlServiceHandler(peer)
	server := httptest.NewServer(handler)
	defer server.Close()
	o, err := (&options{endpoint: server.URL}).workspaceOptions()
	if err != nil {
		t.Fatal(err)
	}
	run := workspaceHTTPJournal(t, o)
	workspaceJournalUpload(t, run, "finalize")
	peer.mu.Lock()
	peer.ref = proto.Clone(run.state.Upload).(*v1.ArtifactRef)
	peer.mu.Unlock()
	before := workspaceTestFiles(t, run.dir)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := o.uploadWorkspaceSnapshot(ctx, run); err == nil || !strings.Contains(err.Error(), "contradicts") {
		t.Fatalf("partial acknowledged finalize resumed: %v", err)
	}
	if peer.writes.Load() != 0 || peer.finalizes.Load() != 0 || !reflect.DeepEqual(before, workspaceTestFiles(t, run.dir)) {
		t.Fatal("partial acknowledgment changed saved finalize intent")
	}
}

// The Do/Read/Close failures in these tests are MODELED. The generated Connect
// unary receive/error paths and concrete owner capture are real local code.
type modeledWorkspaceBody struct {
	reader            io.Reader
	readErr, closeErr error
	closes            atomic.Int32
}

func (b *modeledWorkspaceBody) Read(p []byte) (int, error) {
	if b.readErr != nil {
		return 0, b.readErr
	}
	return b.reader.Read(p)
}
func (b *modeledWorkspaceBody) Close() error { b.closes.Add(1); return b.closeErr }

type modeledWorkspaceDo struct {
	mu                sync.Mutex
	mode              string
	readErr, closeErr error
	calls             int
	response          *http.Response
	original          *modeledWorkspaceBody
}

func (d *modeledWorkspaceDo) Do(request *http.Request) (*http.Response, error) {
	if _, err := io.Copy(io.Discard, request.Body); err != nil {
		return nil, err
	}
	if err := request.Body.Close(); err != nil {
		return nil, err
	}
	encoded, err := proto.Marshal(&v1.GetServerInfoResponse{Capabilities: []v1.Capability{v1.Capability_CAPABILITY_FINITE_JOBS_V1}})
	if err != nil {
		return nil, err
	}
	body := &modeledWorkspaceBody{reader: bytes.NewReader(encoded), closeErr: d.closeErr}
	status := http.StatusOK
	if d.mode == "receive" {
		body.readErr = d.readErr
	}
	if d.mode == "redirect" {
		status = http.StatusTemporaryRedirect
	}
	response := &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/proto"}}, Body: body, Request: request, ProtoMajor: 1, ProtoMinor: 1}
	d.mu.Lock()
	d.calls++
	d.response = response
	d.original = body
	d.mu.Unlock()
	return response, nil
}

func TestWorkspaceResponseCloseCapture(t *testing.T) {
	for _, mode := range []string{"receive", "redirect", "positive"} {
		t.Run(mode, func(t *testing.T) {
			o, err := (&options{endpoint: "http://example.invalid"}).workspaceOptions()
			if err != nil {
				t.Fatal(err)
			}
			endpoint, credential, err := o.workspaceIdentity()
			if err != nil {
				t.Fatal(err)
			}
			run := &workspaceRun{intent: workspaceIntent{Endpoint: endpoint, CredentialSHA256: credential}}
			o.workspaceOwner = run
			readCause := errors.New("modeled-private-read-cause")
			closeCause := errors.New("modeled-private-close-cause")
			peer := &modeledWorkspaceDo{mode: mode, readErr: readCause, closeErr: closeCause}
			if mode == "positive" {
				peer.closeErr = nil
			}
			_, opts := o.workspaceClient()
			client := skyv1connect.NewControlServiceClient(workspaceHTTPDo{client: peer, options: o, run: run}, o.endpoint, opts...)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err = client.GetServerInfo(ctx, request(o, &v1.GetServerInfoRequest{}))
			peer.mu.Lock()
			response, original, calls := peer.response, peer.original, peer.calls
			peer.mu.Unlock()
			if calls != 1 || original == nil || original.closes.Load() != 1 {
				t.Fatalf("original body not closed once: %d %v", calls, original)
			}
			observed, ok := response.Body.(*workspaceResponseBody)
			if !ok || observed.original != original {
				t.Fatal("generator did not receive the observed original body")
			}
			if mode == "positive" {
				if err != nil || run.failed != nil {
					t.Fatalf("positive response poisoned: %v %v", err, run.failed)
				}
				if err := observed.Close(); err != nil || original.closes.Load() != 1 {
					t.Fatal("positive first Close was repeated")
				}
				return
			}
			if err == nil || err.Error() != "workspace response closure unresolved" || !errors.Is(err, closeCause) {
				t.Fatalf("first Close cause lost or exposed: %v", err)
			}
			if mode == "receive" && !errors.Is(err, readCause) {
				t.Fatalf("primary receive cause lost: %v", err)
			}
			if mode == "redirect" {
				var primary *connect.Error
				if !errors.As(err, &primary) {
					t.Fatal("primary Connect redirect error lost")
				}
			}
			var held *workspaceResponseCloseError
			if !errors.As(run.failed, &held) || held.body != observed || held.body.original != original || !errors.Is(held, closeCause) {
				t.Fatal("same concrete failed response owner was not retained")
			}
			if !errors.Is(observed.Close(), closeCause) || original.closes.Load() != 1 {
				t.Fatal("failed first Close was retried")
			}
			if _, nextErr := client.GetServerInfo(ctx, request(o, &v1.GetServerInfoRequest{})); !errors.Is(nextErr, closeCause) {
				t.Fatalf("poison did not stop next RPC: %v", nextErr)
			}
			peer.mu.Lock()
			after := peer.calls
			peer.mu.Unlock()
			if after != 1 || original.closes.Load() != 1 {
				t.Fatalf("poison allowed later Do/Close: %d %d", after, original.closes.Load())
			}
		})
	}
}

func TestWorkspaceZoneRefusesBeforeRPC(t *testing.T) {
	for _, zone := range []string{"EthA", "etha"} {
		t.Run(zone, func(t *testing.T) {
			o := &options{endpoint: "http://[fe80::1%25" + zone + "]:8080"}
			if _, _, err := o.workspaceIdentity(); err == nil {
				t.Fatal("zoned IPv6 identity admitted")
			}
			// Even the accepted-Job resume path must reject the identity before
			// it constructs/sends its GetJob request.
			run := &workspaceRun{state: workspaceState{Phase: "accepted", JobID: strings.Repeat("a", 32)}}
			if _, err := o.continueWorkspace(context.Background(), run, new(bytes.Buffer)); err == nil || !strings.Contains(err.Error(), "canonical HTTP") {
				t.Fatalf("zone reached a network path: %v", err)
			}
		})
	}
}

type workspaceMalformedUploadPeer struct {
	workspaceCapabilityPeer
	mode                            string
	mu                              sync.Mutex
	ref                             *v1.ArtifactRef
	begins, gets, writes, finalizes atomic.Int32
}

func (s *workspaceMalformedUploadPeer) BeginArtifactUpload(_ context.Context, r *connect.Request[v1.BeginArtifactUploadRequest]) (*connect.Response[v1.BeginArtifactUploadResponse], error) {
	s.begins.Add(1)
	return connect.NewResponse(&v1.BeginArtifactUploadResponse{Upload: &v1.ArtifactUpload{Id: strings.Repeat("a", 32), Reference: &v1.ArtifactRef{Id: strings.Repeat("b", 32), Sha256: r.Msg.Sha256, Size: r.Msg.Size}, State: v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_WRITABLE, ExpiresAt: timestamppb.New(time.Now().Add(time.Hour))}}), nil
}
func (s *workspaceMalformedUploadPeer) GetArtifactUpload(context.Context, *connect.Request[v1.GetArtifactUploadRequest]) (*connect.Response[v1.GetArtifactUploadResponse], error) {
	s.gets.Add(1)
	s.mu.Lock()
	ref := proto.Clone(s.ref).(*v1.ArtifactRef)
	s.mu.Unlock()
	id := ref.Id
	switch s.mode {
	case "get-id-mismatch":
		ref.Id = strings.Repeat("b", 32)
	case "get-other-upload":
		id = strings.Repeat("b", 32)
		ref.Id = id
	case "get-same-id-other-digest":
		ref.Sha256 = strings.Repeat("c", 64)
	}
	return connect.NewResponse(&v1.GetArtifactUploadResponse{Upload: &v1.ArtifactUpload{Id: id, Reference: ref, State: v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_WRITABLE, ExpiresAt: timestamppb.New(time.Now().Add(time.Hour))}}), nil
}
func (s *workspaceMalformedUploadPeer) WriteArtifactChunk(context.Context, *connect.Request[v1.WriteArtifactChunkRequest]) (*connect.Response[v1.WriteArtifactChunkResponse], error) {
	s.writes.Add(1)
	return nil, connect.NewError(connect.CodeUnknown, context.Canceled)
}
func (s *workspaceMalformedUploadPeer) FinalizeArtifactUpload(context.Context, *connect.Request[v1.FinalizeArtifactUploadRequest]) (*connect.Response[v1.FinalizeArtifactUploadResponse], error) {
	s.finalizes.Add(1)
	return nil, connect.NewError(connect.CodeUnknown, context.Canceled)
}

// The malformed replies are actual Connect/HTTP peer responses. Journal bytes
// are genuine A writer output; this is negative client validation, not a server
// admission or native data-file qualification claim.
func TestWorkspaceMalformedUploadRefusal(t *testing.T) {
	for _, mode := range []string{"begin-id-mismatch", "get-id-mismatch", "get-other-upload", "get-same-id-other-digest"} {
		t.Run(mode, func(t *testing.T) {
			peer := &workspaceMalformedUploadPeer{mode: mode}
			_, handler := skyv1connect.NewControlServiceHandler(peer)
			server := httptest.NewServer(handler)
			defer server.Close()
			o, err := (&options{endpoint: server.URL}).workspaceOptions()
			if err != nil {
				t.Fatal(err)
			}
			run := workspaceHTTPJournal(t, o)
			if mode != "begin-id-mismatch" {
				workspaceJournalUpload(t, run, "upload")
				peer.mu.Lock()
				peer.ref = proto.Clone(run.state.Upload).(*v1.ArtifactRef)
				peer.mu.Unlock()
			}
			before := workspaceTestFiles(t, run.dir)
			state := run.state
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if ref, err := o.uploadWorkspaceSnapshot(ctx, run); err == nil || ref.ID != "" {
				t.Fatalf("malformed upload admitted: %+v %v", ref, err)
			}
			begins, gets := int32(0), int32(1)
			if mode == "begin-id-mismatch" {
				begins, gets = 1, 0
			}
			if peer.begins.Load() != begins || peer.gets.Load() != gets || peer.writes.Load() != 0 || peer.finalizes.Load() != 0 {
				t.Fatalf("RPCs after malformed upload: begin=%d get=%d write=%d finalize=%d", peer.begins.Load(), peer.gets.Load(), peer.writes.Load(), peer.finalizes.Load())
			}
			if !reflect.DeepEqual(state, run.state) || !reflect.DeepEqual(before, workspaceTestFiles(t, run.dir)) {
				t.Fatal("malformed reply changed journal phase or bytes")
			}
		})
	}
}

// MODELED retryable receive and failed first Close are fed through the actual
// generated client and the actual Begin/Write/Finalize helper loops.
func TestWorkspaceHelperPoisonStopsRetry(t *testing.T) {
	for _, method := range []string{"begin", "write", "finalize"} {
		t.Run(method, func(t *testing.T) {
			o, err := (&options{endpoint: "http://example.invalid"}).workspaceOptions()
			if err != nil {
				t.Fatal(err)
			}
			endpoint, credential, err := o.workspaceIdentity()
			if err != nil {
				t.Fatal(err)
			}
			run := &workspaceRun{intent: workspaceIntent{Endpoint: endpoint, CredentialSHA256: credential, RequestID: "stable"}, state: workspaceState{Phase: "begin", Domain: &v1.RequestDomain{Lineage: strings.Repeat("a", 32), Epoch: 1}}}
			o.workspaceOwner = run
			primaryCause := errors.New("modeled-helper-private-primary")
			primary := connect.NewError(connect.CodeUnavailable, primaryCause)
			closeCause := errors.New("modeled-helper-private-first-close")
			peer := &modeledWorkspaceDo{mode: "receive", readErr: primary, closeErr: closeCause}
			_, opts := o.workspaceClient()
			client := skyv1connect.NewControlServiceClient(workspaceHTTPDo{client: peer, options: o, run: run}, o.endpoint, opts...)
			retries := new(artifactUploadRetries)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			invoke := func() error {
				switch method {
				case "begin":
					_, err := o.beginWorkspaceUpload(ctx, client, run, strings.Repeat("b", 64), 4, retries)
					return err
				case "write":
					_, err := o.writeArtifactChunkState(ctx, client, strings.Repeat("a", 32), artifact.Ref{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Size: 4}, 0, []byte("data"), retries, run)
					return err
				default:
					_, err := o.finalizeArtifactUploadState(ctx, client, strings.Repeat("a", 32), retries, run)
					return err
				}
			}
			err = invoke()
			if err == nil || !errors.Is(err, primaryCause) || !errors.Is(err, closeCause) || connect.CodeOf(err) != connect.CodeUnavailable {
				t.Fatalf("helper lost retryable primary or Close cause: %v", err)
			}
			if run.failed == nil || run.failed.Error() != "workspace response closure unresolved" {
				t.Fatal("poison owner exposes a private underlying cause")
			}
			if err.Error() != "workspace response closure unresolved" || retries.used != 0 {
				t.Fatalf("poison entered retry wait or exposed cause: used=%d err=%v", retries.used, err)
			}
			peer.mu.Lock()
			original, response, calls := peer.original, peer.response, peer.calls
			peer.mu.Unlock()
			var held *workspaceResponseCloseError
			if calls != 1 || original == nil || original.closes.Load() != 1 || !errors.As(run.failed, &held) || held.body != response.Body || held.body.original != original || !errors.Is(run.failed, primaryCause) || !errors.Is(run.failed, closeCause) {
				t.Fatal("poison did not retain original joined causes/body")
			}
			if nextErr := invoke(); !errors.Is(nextErr, primaryCause) || !errors.Is(nextErr, closeCause) {
				t.Fatalf("next helper lost retained causes: %v", nextErr)
			}
			if !errors.Is(held.body.Close(), closeCause) {
				t.Fatal("first Close result lost")
			}
			peer.mu.Lock()
			after := peer.calls
			peer.mu.Unlock()
			if after != 1 || original.closes.Load() != 1 || retries.used != 0 {
				t.Fatalf("poison retried Do/Close/wait: %d/%d/%d", after, original.closes.Load(), retries.used)
			}
		})
	}
}
