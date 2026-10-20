package cli

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/artifact"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestWorkspaceTaskYAML(t *testing.T) {
	ref := "{id: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, sha256: " + strings.Repeat("b", 64) + ", size: 2048}"
	for _, tc := range []struct{ name, fields, want string }{
		{"valid", "workdir: " + ref + "\nsetup: [prepare, '']\noutputs: [results]\n", ""},
		{"directory", "workdir: " + ref + "\ndirectory: /tmp\n", "empty directory"},
		{"setup_without_workdir", "setup: [prepare]\n", "require workdir"},
		{"outputs_without_workdir", "outputs: [results]\n", "require workdir"},
		{"overlap", "workdir: " + ref + "\noutputs: [results, results/sub]\n", "overlapping"},
		{"bad_ref", "workdir: {id: wrong, sha256: wrong, size: 0}\n", "workdir"},
		{"unknown_ref_field", "workdir: {id: wrong, uri: file:///tmp}\n", "field uri"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := New(nil)
			cmd.SetIn(strings.NewReader("argv: [run]\n" + tc.fields))
			task, err := readTask(cmd, "-")
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("got %v; want %q", err, tc.want)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if task.Workdir.GetSize() != 2048 || len(task.Setup) != 2 || task.Setup[1] != "" || len(task.Outputs) != 1 || task.Outputs[0] != "results" {
				t.Fatalf("snapshot fields lost: %v", task)
			}
		})
	}
}

func TestArtifactRefFile(t *testing.T) {
	id, sha := strings.Repeat("a", 32), strings.Repeat("b", 64)
	valid := fmt.Sprintf(`{"id":%q,"sha256":%q,"size":2048}`, id, sha)
	for _, tc := range []struct {
		name, text string
		valid      bool
	}{
		{"integer", valid, true},
		{"proto_integer", strings.Replace(valid, "2048", `"2048"`, 1), true},
		{"duplicate", strings.TrimSuffix(valid, "}") + `,"size":2048}`, false},
		{"unknown", strings.TrimSuffix(valid, "}") + `,"path":"/tmp"}`, false},
		{"trailing", valid + " {}", false},
		{"null", "null", false},
		{"fraction", strings.Replace(valid, "2048", "2.5", 1), false},
		{"huge", strings.Repeat(" ", 4097) + valid, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "ref.json")
			if err := os.WriteFile(p, []byte(tc.text), 0600); err != nil {
				t.Fatal(err)
			}
			got, err := readArtifactRef(p)
			if (err == nil) != tc.valid {
				t.Fatalf("got %+v, %v; valid=%v", got, err, tc.valid)
			}
			if tc.valid && got != (artifact.Ref{ID: id, SHA256: sha, Size: 2048}) {
				t.Fatalf("changed Ref: %+v", got)
			}
		})
	}
}

// This is a protocol peer, not a backend or filesystem implementation. Its
// counters prove the CLI's method selection, bounds and refusal behavior only.
type artifactPeer struct {
	skyv1connect.UnimplementedControlServiceHandler
	mu       sync.Mutex
	calls    map[string]int
	caps     []v1.Capability
	targets  []*v1.Target
	begin    func(int, *v1.BeginArtifactUploadRequest) (*v1.ArtifactUpload, error)
	upload   *v1.ArtifactUpload
	write    func(int, *v1.WriteArtifactChunkRequest) (*v1.ArtifactUpload, error)
	read     func(*v1.ReadArtifactChunkRequest) *v1.ReadArtifactChunkResponse
	ref      *v1.ArtifactRef
	finalize func(*v1.FinalizeArtifactUploadRequest) (*v1.ArtifactRef, error)
	task     *v1.Task
}

func (p *artifactPeer) count(name string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.calls == nil {
		p.calls = make(map[string]int)
	}
	p.calls[name]++
	return p.calls[name]
}
func (p *artifactPeer) GetServerInfo(context.Context, *connect.Request[v1.GetServerInfoRequest]) (*connect.Response[v1.GetServerInfoResponse], error) {
	p.count("GetServerInfo")
	return connect.NewResponse(&v1.GetServerInfoResponse{Capabilities: p.caps, Targets: p.targets}), nil
}
func (p *artifactPeer) SubmitWorkspaceJob(_ context.Context, r *connect.Request[v1.SubmitWorkspaceJobRequest]) (*connect.Response[v1.SubmitWorkspaceJobResponse], error) {
	p.count("SubmitWorkspaceJob")
	p.mu.Lock()
	p.task = proto.Clone(r.Msg.Task).(*v1.Task)
	p.mu.Unlock()
	return connect.NewResponse(&v1.SubmitWorkspaceJobResponse{Job: &v1.Job{Id: "replayed", Target: "removed-target", State: v1.JobState_JOB_STATE_SUCCEEDED}}), nil
}
func (p *artifactPeer) BeginArtifactUpload(_ context.Context, r *connect.Request[v1.BeginArtifactUploadRequest]) (*connect.Response[v1.BeginArtifactUploadResponse], error) {
	n := p.count("BeginArtifactUpload")
	u, err := p.begin(n, r.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&v1.BeginArtifactUploadResponse{Upload: u}), nil
}
func (p *artifactPeer) WriteArtifactChunk(_ context.Context, r *connect.Request[v1.WriteArtifactChunkRequest]) (*connect.Response[v1.WriteArtifactChunkResponse], error) {
	n := p.count("WriteArtifactChunk")
	u, err := p.write(n, r.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&v1.WriteArtifactChunkResponse{Upload: u}), nil
}
func (p *artifactPeer) GetArtifactUpload(context.Context, *connect.Request[v1.GetArtifactUploadRequest]) (*connect.Response[v1.GetArtifactUploadResponse], error) {
	p.count("GetArtifactUpload")
	return connect.NewResponse(&v1.GetArtifactUploadResponse{Upload: p.upload}), nil
}
func (p *artifactPeer) FinalizeArtifactUpload(_ context.Context, r *connect.Request[v1.FinalizeArtifactUploadRequest]) (*connect.Response[v1.FinalizeArtifactUploadResponse], error) {
	p.count("FinalizeArtifactUpload")
	ref, err := p.finalize(r.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&v1.FinalizeArtifactUploadResponse{Reference: ref}), nil
}

func (p *artifactPeer) BeginArtifactDownload(context.Context, *connect.Request[v1.BeginArtifactDownloadRequest]) (*connect.Response[v1.BeginArtifactDownloadResponse], error) {
	p.count("BeginArtifactDownload")
	return connect.NewResponse(&v1.BeginArtifactDownloadResponse{Id: strings.Repeat("d", 32), Reference: p.ref, ExpiresAt: timestamppb.New(time.Now().Add(time.Hour))}), nil
}
func (p *artifactPeer) ReadArtifactChunk(_ context.Context, r *connect.Request[v1.ReadArtifactChunkRequest]) (*connect.Response[v1.ReadArtifactChunkResponse], error) {
	p.count("ReadArtifactChunk")
	return connect.NewResponse(p.read(r.Msg)), nil
}
func (p *artifactPeer) CloseArtifactDownload(context.Context, *connect.Request[v1.CloseArtifactDownloadRequest]) (*connect.Response[v1.CloseArtifactDownloadResponse], error) {
	p.count("CloseArtifactDownload")
	return connect.NewResponse(&v1.CloseArtifactDownloadResponse{}), nil
}
func artifactPeerServer(t *testing.T, p *artifactPeer) *httptest.Server {
	t.Helper()
	_, handler := skyv1connect.NewControlServiceHandler(p)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.count("rpc:" + r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:])
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(s.Close)
	return s
}

func TestWorkspaceReplayMethod(t *testing.T) {
	for _, tc := range []struct {
		name string
		caps []v1.Capability
		want bool
	}{
		{"endpoint_without_global_target7", []v1.Capability{v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1}, true},
		{"target7_is_not_endpoint6", []v1.Capability{v1.Capability_CAPABILITY_WORKDIR_SNAPSHOT_V1}, false},
		{"absent", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer := &artifactPeer{caps: tc.caps}
			server := artifactPeerServer(t, peer)
			o := &options{endpoint: server.URL, timeout: time.Second}
			task := &v1.Task{Argv: []string{"run"}, Target: "removed-target", Workdir: &v1.ArtifactRef{Id: strings.Repeat("a", 32), Sha256: strings.Repeat("b", 64), Size: 2048}, Setup: []string{"prepare", ""}, Outputs: []string{"result"}}
			cmd := New(nil)
			cmd.SetOut(new(bytes.Buffer))
			cmd.SetErr(new(bytes.Buffer))
			job, err := o.submitTask(cmd, context.Background(), o.client(), task, "existing-request", strings.Repeat("a", 32)+":1", "original.yaml", true, true)
			if (err == nil) != tc.want {
				t.Fatalf("job=%v err=%v", job, err)
			}
			if tc.want && (!proto.Equal(peer.task, task) || job.Id != "replayed") {
				t.Fatalf("task changed or replay lost: %v %v", peer.task, job)
			}
			want := 0
			if tc.want {
				want = 1
			}
			if peer.calls["SubmitWorkspaceJob"] != want {
				t.Fatalf("calls=%v", peer.calls)
			}
			for _, method := range []string{"PlanTask", "SubmitJob", "SubmitRetryJob", "SubmitRetainedJob", "BeginArtifactUpload", "BeginArtifactDownload", "GetRetention"} {
				if peer.calls["rpc:"+method] != 0 {
					t.Fatalf("unexpected %s: %v", method, peer.calls)
				}
			}
			// Only discovery and the new submit method are implemented: a Plan,
			// old submit, artifact upload or target check makes success fail.
		})
	}
}

func TestArtifactChunkRecovery(t *testing.T) {
	ref := artifact.Ref{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Size: 2048}
	u := func(ack int64) *v1.ArtifactUpload {
		return &v1.ArtifactUpload{Id: ref.ID, Reference: wireArtifactRef(ref), Acknowledged: ack, State: v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_WRITABLE, ExpiresAt: timestamppb.New(time.Now().Add(time.Hour))}
	}
	for _, tc := range []struct {
		name      string
		recovered int64
		firstAck  int64
		transient bool
		want      bool
		writes    int
	}{
		{"acknowledged_lost_response", 4, 0, true, true, 2},
		{"exact_response", 0, 4, false, true, 1},
		{"unacknowledged_retry", 0, 0, true, true, 2},
		{"partial_recovery", 2, 0, true, false, 1},
		{"partial_response", 0, 2, false, false, 1},
		{"overshoot_response", 0, 5, false, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer := &artifactPeer{upload: u(tc.recovered)}
			peer.write = func(n int, r *v1.WriteArtifactChunkRequest) (*v1.ArtifactUpload, error) {
				if r.Offset != 0 || string(r.Data) != "test" {
					t.Errorf("chunk changed: %v", r)
				}
				if n == 1 && tc.transient {
					return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("lost response"))
				}
				if n == 1 {
					return u(tc.firstAck), nil
				}
				return u(4), nil
			}
			server := artifactPeerServer(t, peer)
			o := &options{endpoint: server.URL, timeout: time.Second}
			_, err := o.writeArtifactChunk(context.Background(), o.artifactClient(), ref.ID, ref, 0, []byte("test"), new(artifactUploadRetries))
			if (err == nil) != tc.want || peer.calls["WriteArtifactChunk"] != tc.writes {
				t.Fatalf("err=%v calls=%v", err, peer.calls)
			}
		})
	}
}

func TestArtifactDownloadVerification(t *testing.T) {
	for _, tc := range []struct {
		name                                 string
		path                                 string
		badOffset, badDigest, existing, want bool
	}{
		{"valid", "safe", false, false, false, true},
		{"traversal", "../escape", false, false, false, false},
		{"partial", "safe", true, false, false, false},
		{"digest", "safe", false, true, false, false},
		{"existing", "safe", false, false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var archive bytes.Buffer
			tw := tar.NewWriter(&archive)
			if err := tw.WriteHeader(&tar.Header{Name: tc.path, Mode: 0600, Size: 4, Format: tar.FormatUSTAR}); err != nil {
				t.Fatal(err)
			}
			if _, err := tw.Write([]byte("test")); err != nil {
				t.Fatal(err)
			}
			if err := tw.Close(); err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(archive.Bytes())
			digest := hex.EncodeToString(sum[:])
			if tc.badDigest {
				digest = strings.Repeat("b", 64)
			}
			ref := artifact.Ref{ID: strings.Repeat("a", 32), SHA256: digest, Size: int64(archive.Len())}
			peer := &artifactPeer{caps: []v1.Capability{v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1}, ref: wireArtifactRef(ref)}
			peer.read = func(r *v1.ReadArtifactChunkRequest) *v1.ReadArtifactChunkResponse {
				end := min(ref.Size, r.Offset+int64(r.Limit))
				next := end
				if tc.badOffset {
					next--
				}
				return &v1.ReadArtifactChunkResponse{Data: archive.Bytes()[r.Offset:end], NextOffset: next, Complete: end == ref.Size}
			}
			server := artifactPeerServer(t, peer)
			o := &options{endpoint: server.URL, timeout: time.Second}
			parent := t.TempDir()
			dst := filepath.Join(parent, "destination")
			if tc.existing {
				if err := os.Mkdir(dst, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dst, "keep"), []byte("unchanged"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			cmd := New(nil)
			cmd.SetContext(context.Background())
			cmd.SetOut(new(bytes.Buffer))
			cmd.SetErr(new(bytes.Buffer))
			err := o.downloadArtifact(cmd, ref, dst)
			if (err == nil) != tc.want {
				t.Fatalf("download err=%v want success=%v", err, tc.want)
			}
			if tc.want {
				b, err := os.ReadFile(filepath.Join(dst, "safe"))
				if err != nil || string(b) != "test" {
					t.Fatalf("verified content=%q err=%v", b, err)
				}
				if peer.calls["CloseArtifactDownload"] != 1 {
					t.Fatalf("close calls=%v", peer.calls)
				}
				return
			}
			if tc.existing {
				b, err := os.ReadFile(filepath.Join(dst, "keep"))
				if err != nil || string(b) != "unchanged" {
					t.Fatal("existing destination changed")
				}
				if peer.calls["BeginArtifactDownload"] != 0 {
					t.Fatal("existing destination reached HTTP")
				}
			} else if _, err := os.Lstat(dst); !os.IsNotExist(err) {
				t.Fatalf("destination created before verification: %v", err)
			}
			if _, err := os.Lstat(filepath.Join(parent, "escape")); !os.IsNotExist(err) {
				t.Fatal("traversal wrote outside destination")
			}
		})
	}
}

func TestArtifactCapabilityScopes(t *testing.T) {
	peer := &artifactPeer{
		caps:    []v1.Capability{v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1},
		targets: []*v1.Target{{Name: "workspace", Kind: "local", Capabilities: []v1.Capability{v1.Capability_CAPABILITY_WORKDIR_SNAPSHOT_V1}}},
	}
	server := artifactPeerServer(t, peer)
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"endpoint6", []string{"info", "--require", "artifact-snapshot-service-v1"}, ""},
		{"target7", []string{"info", "--target", "workspace", "--require", "workdir-snapshot-v1"}, ""},
		{"target7_requires_target", []string{"info", "--require", "workdir-snapshot-v1"}, "need --target"},
		{"old_target_rule", []string{"info", "--require", "resource-limits-v1"}, "need --target"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"--endpoint", server.URL}, tc.args...)
			_, _, err := execute(context.Background(), args...)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v; want %q", err, tc.want)
			}
		})
	}
}

// FINALIZING is a durable crash prefix, not a reason to allocate another upload.
// This protocol peer proves only the CLI recovery path, not filesystem recovery.
func TestArtifactResumeFinalizing(t *testing.T) {
	for _, tc := range []struct {
		name                               string
		state                              v1.ArtifactUploadState
		partialAck, wrongDigest, wrongSize bool
		want                               bool
	}{
		{"exact_finalizing", v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_FINALIZING, false, false, false, true},
		{"partial_finalizing", v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_FINALIZING, true, false, false, false},
		{"wrong_digest", v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_FINALIZING, false, true, false, false},
		{"wrong_size", v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_FINALIZING, false, false, true, false},
		{"creating", v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_CREATING, false, false, false, false},
		{"deleting", v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_DELETING, false, false, false, false},
		{"unspecified", v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_UNSPECIFIED, false, false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			directory := t.TempDir()
			if err := os.WriteFile(filepath.Join(directory, "seed.txt"), []byte("seed"), 0600); err != nil {
				t.Fatal(err)
			}
			root, err := os.OpenRoot(directory)
			if err != nil {
				t.Fatal(err)
			}
			var archive archiveBuffer
			err = artifact.Snapshot(root, &archive)
			closeErr := root.Close()
			if err != nil || closeErr != nil {
				t.Fatalf("snapshot: %v; close: %v", err, closeErr)
			}
			size := int64(archive.Len())
			digest, err := artifact.Validate(bytes.NewReader(archive.Bytes()), size)
			if err != nil {
				t.Fatal(err)
			}
			ref := artifact.Ref{ID: strings.Repeat("a", 32), SHA256: digest, Size: size}
			if tc.wrongDigest {
				ref.SHA256 = strings.Repeat("b", 64)
				if ref.SHA256 == digest {
					t.Fatal("negative fixture digest unexpectedly matches")
				}
			}
			if tc.wrongSize {
				ref.Size++
			}
			ack := ref.Size
			if tc.partialAck {
				ack--
			}
			peer := &artifactPeer{
				caps:   []v1.Capability{v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1},
				upload: &v1.ArtifactUpload{Id: ref.ID, Reference: wireArtifactRef(ref), Acknowledged: ack, State: tc.state, ExpiresAt: timestamppb.New(time.Now().Add(time.Hour))},
			}
			peer.finalize = func(r *v1.FinalizeArtifactUploadRequest) (*v1.ArtifactRef, error) {
				if r.Id != ref.ID {
					t.Errorf("finalize ID=%s; want original %s", r.Id, ref.ID)
				}
				return wireArtifactRef(ref), nil
			}
			server := artifactPeerServer(t, peer)
			o := &options{endpoint: server.URL, timeout: time.Second}
			cmd := New(nil)
			cmd.SetContext(context.Background())
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(new(bytes.Buffer))
			err = o.uploadArtifact(cmd, directory, ref.ID)
			if (err == nil) != tc.want {
				t.Fatalf("resume error=%v; want success=%v", err, tc.want)
			}
			peer.mu.Lock()
			calls := make(map[string]int, len(peer.calls))
			for method, count := range peer.calls {
				calls[method] = count
			}
			peer.mu.Unlock()
			wantFinalizes := 0
			if tc.want {
				wantFinalizes = 1
				var got artifact.Ref
				if err := json.Unmarshal(out.Bytes(), &got); err != nil || got != ref {
					t.Fatalf("finalized Ref=%+v error=%v; want %+v", got, err, ref)
				}
			} else if out.Len() != 0 {
				t.Fatalf("failed resume printed a Ref: %s", &out)
			}
			if calls["rpc:GetArtifactUpload"] != 1 || calls["rpc:FinalizeArtifactUpload"] != wantFinalizes || calls["rpc:BeginArtifactUpload"] != 0 || calls["rpc:WriteArtifactChunk"] != 0 {
				t.Fatalf("resume method frontier: %v", calls)
			}
		})
	}
}
