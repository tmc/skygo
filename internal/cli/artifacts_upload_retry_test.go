package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/artifact"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// These are real HTTP protocol peers, not production storage/owner proofs.
// No source-only review represents these unrun tests as execution evidence.
func TestArtifactBeginRetryWireMarker(t *testing.T) {
	for _, tc := range []struct {
		name   string
		code   connect.Code
		values []string
		want   bool
	}{
		{"exact", connect.CodeResourceExhausted, []string{"v1"}, true},
		{"missing", connect.CodeResourceExhausted, nil, false},
		{"duplicate", connect.CodeResourceExhausted, []string{"v1", "v1"}, false},
		{"unknown", connect.CodeResourceExhausted, []string{"v2"}, false},
		{"combined", connect.CodeResourceExhausted, []string{"v1, v1"}, false},
		{"wrong_code", connect.CodeUnavailable, []string{"v1"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer := new(artifactPeer)
			peer.begin = func(n int, r *v1.BeginArtifactUploadRequest) (*v1.ArtifactUpload, error) {
				if r.Sha256 != strings.Repeat("b", 64) || r.Size != 2048 {
					t.Errorf("Begin changed: %v", r)
				}
				if n == 1 {
					err := connect.NewError(tc.code, errors.New("guard refusal"))
					for _, value := range tc.values {
						err.Meta().Add("Skygo-Artifact-Begin-Not-Started", value)
					}
					return nil, err
				}
				return &v1.ArtifactUpload{Id: strings.Repeat("a", 32)}, nil
			}
			server := artifactPeerServer(t, peer)
			o := &options{endpoint: server.URL}
			retries := new(artifactUploadRetries)
			_, err := o.beginArtifactUpload(context.Background(), o.artifactClient(), strings.Repeat("b", 64), 2048, retries)
			wantCalls := 1
			if tc.want {
				wantCalls = 2
			}
			if (err == nil) != tc.want || peer.calls["BeginArtifactUpload"] != wantCalls {
				t.Fatalf("error=%v calls=%v", err, peer.calls)
			}
			if !tc.want && !strings.Contains(err.Error(), "outcome may be unknown") {
				t.Fatalf("missing conservative diagnostic: %v", err)
			}
		})
	}
	local := connect.NewError(connect.CodeResourceExhausted, errors.New("local error"))
	local.Meta().Set("Skygo-Artifact-Begin-Not-Started", "v1")
	if artifactBeginNotStarted(local) {
		t.Fatal("synthesized error accepted as wire evidence")
	}
}

func TestArtifactWriteRetryResponseRequired(t *testing.T) {
	ref := artifact.Ref{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Size: 2048}
	upload := func(ack int64) *v1.ArtifactUpload {
		return &v1.ArtifactUpload{Id: ref.ID, Reference: wireArtifactRef(ref), Acknowledged: ack, State: v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_WRITABLE, ExpiresAt: timestamppb.New(time.Now().Add(time.Hour))}
	}
	for _, tc := range []struct {
		name                                       string
		code                                       connect.Code
		badReplay, badRef, badID, badState, badAck bool
		want                                       bool
	}{
		{"busy", connect.CodeResourceExhausted, false, false, false, false, false, true},
		{"lost", connect.CodeUnavailable, false, false, false, false, false, true},
		{"deadline", connect.CodeDeadlineExceeded, false, false, false, false, false, true},
		{"status_is_not_write", connect.CodeUnavailable, true, false, false, false, false, false},
		{"changed_ref", connect.CodeUnavailable, false, true, false, false, false, false},
		{"changed_id", connect.CodeUnavailable, false, false, true, false, false, false},
		{"complete_status", connect.CodeUnavailable, false, false, false, true, false, false},
		{"overshoot_status", connect.CodeUnavailable, false, false, false, false, true, false},
		{"permission", connect.CodePermissionDenied, false, false, false, false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status := upload(4)
			if tc.badRef {
				status.Reference.Sha256 = strings.Repeat("c", 64)
			}
			if tc.badID {
				status.Id = strings.Repeat("c", 32)
			}
			if tc.badState {
				status.State = v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_COMPLETE
				status.Acknowledged = ref.Size
			}
			if tc.badAck {
				status.Acknowledged = 5
			}
			peer := &artifactPeer{upload: status}
			var first *v1.WriteArtifactChunkRequest
			peer.write = func(n int, r *v1.WriteArtifactChunkRequest) (*v1.ArtifactUpload, error) {
				if n == 1 {
					first = proto.Clone(r).(*v1.WriteArtifactChunkRequest)
					return nil, connect.NewError(tc.code, errors.New("first refusal"))
				}
				if !proto.Equal(first, r) {
					t.Errorf("retry changed: first=%v retry=%v", first, r)
				}
				if tc.badReplay {
					return upload(3), nil
				}
				return upload(4), nil
			}
			server := artifactPeerServer(t, peer)
			o := &options{endpoint: server.URL}
			_, err := o.writeArtifactChunk(context.Background(), o.artifactClient(), ref.ID, ref, 0, []byte("test"), new(artifactUploadRetries))
			if (err == nil) != tc.want {
				t.Fatalf("error=%v calls=%v", err, peer.calls)
			}
			wantWrites := 1
			if tc.want || tc.badReplay {
				wantWrites = 2
			}
			if peer.calls["WriteArtifactChunk"] != wantWrites {
				t.Fatalf("calls=%v", peer.calls)
			}
			wantGets := 1
			if tc.code == connect.CodeResourceExhausted || tc.code == connect.CodePermissionDenied {
				wantGets = 0
			}
			if peer.calls["GetArtifactUpload"] != wantGets {
				t.Fatalf("reconciliation calls=%v", peer.calls)
			}
		})
	}
}

func TestArtifactRetryBudgetShared(t *testing.T) {
	peer := new(artifactPeer)
	peer.begin = func(n int, r *v1.BeginArtifactUploadRequest) (*v1.ArtifactUpload, error) {
		if n == 1 {
			err := connect.NewError(connect.CodeResourceExhausted, errors.New("cold cache"))
			err.Meta().Set("Skygo-Artifact-Begin-Not-Started", "v1")
			return nil, err
		}
		return &v1.ArtifactUpload{Id: strings.Repeat("a", 32)}, nil
	}
	peer.finalize = func(r *v1.FinalizeArtifactUploadRequest) (*v1.ArtifactRef, error) {
		if r.Id != strings.Repeat("a", 32) {
			t.Errorf("changed finalize ID: %s", r.Id)
		}
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("lost finalize"))
	}
	server := artifactPeerServer(t, peer)
	o := &options{endpoint: server.URL}
	retries := &artifactUploadRetries{used: 19}
	if _, err := o.beginArtifactUpload(context.Background(), o.artifactClient(), strings.Repeat("b", 64), 2048, retries); err != nil {
		t.Fatal(err)
	}
	_, err := o.finalizeArtifactUpload(context.Background(), o.artifactClient(), strings.Repeat("a", 32), retries)
	if err == nil || !strings.Contains(err.Error(), "retry limit") || retries.used != 20 || peer.calls["FinalizeArtifactUpload"] != 1 {
		t.Fatalf("error=%v retries=%d calls=%v", err, retries.used, peer.calls)
	}
}

func TestArtifactRetryWaitCancellation(t *testing.T) {
	cause := connect.NewError(connect.CodeResourceExhausted, errors.New("busy"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	retries := new(artifactUploadRetries)
	if err := retries.wait(ctx, cause); !errors.Is(err, context.Canceled) || !errors.Is(err, cause) || retries.used != 0 {
		t.Fatalf("error=%v used=%d", err, retries.used)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := retries.wait(ctx, cause); !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, cause) || retries.used != 1 {
		t.Fatalf("error=%v used=%d", err, retries.used)
	}
}

func TestArtifactUploadAutomaticRetries(t *testing.T) {
	for _, tc := range []struct {
		name     string
		code     connect.Code
		wrongRef bool
		want     bool
	}{
		{"busy", connect.CodeResourceExhausted, false, true},
		{"lost", connect.CodeUnavailable, false, true},
		{"deadline", connect.CodeDeadlineExceeded, false, true},
		{"permission", connect.CodePermissionDenied, false, false},
		{"wrong_final_ref", connect.CodeResourceExhausted, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			directory := t.TempDir()
			if err := os.WriteFile(filepath.Join(directory, "seed"), []byte("seed"), 0600); err != nil {
				t.Fatal(err)
			}
			var ref artifact.Ref
			var upload *v1.ArtifactUpload
			peer := &artifactPeer{caps: []v1.Capability{v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1}}
			peer.begin = func(n int, r *v1.BeginArtifactUploadRequest) (*v1.ArtifactUpload, error) {
				if n == 1 {
					ref = artifact.Ref{ID: strings.Repeat("a", 32), SHA256: r.Sha256, Size: r.Size}
					err := connect.NewError(connect.CodeResourceExhausted, errors.New("not admitted"))
					err.Meta().Set("Skygo-Artifact-Begin-Not-Started", "v1")
					return nil, err
				}
				if r.Sha256 != ref.SHA256 || r.Size != ref.Size {
					t.Errorf("Begin changed: %v", r)
				}
				upload = &v1.ArtifactUpload{Id: ref.ID, Reference: wireArtifactRef(ref), State: v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_WRITABLE, ExpiresAt: timestamppb.New(time.Now().Add(time.Hour))}
				return upload, nil
			}
			var chunk *v1.WriteArtifactChunkRequest
			peer.write = func(n int, r *v1.WriteArtifactChunkRequest) (*v1.ArtifactUpload, error) {
				if r.Id != ref.ID || r.Offset != 0 || int64(len(r.Data)) != ref.Size {
					t.Errorf("wrong upload: %v", r)
				}
				if n == 1 {
					chunk = proto.Clone(r).(*v1.WriteArtifactChunkRequest)
					return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("sample pending"))
				}
				if !proto.Equal(chunk, r) {
					t.Errorf("chunk changed: %v", r)
				}
				upload.Acknowledged = ref.Size
				return upload, nil
			}
			finalizes := 0
			peer.finalize = func(r *v1.FinalizeArtifactUploadRequest) (*v1.ArtifactRef, error) {
				finalizes++
				if r.Id != ref.ID {
					t.Errorf("Finalize changed ID: %v", r)
				}
				if finalizes == 1 {
					return nil, connect.NewError(tc.code, errors.New("finalize refusal"))
				}
				result := wireArtifactRef(ref)
				if tc.wrongRef {
					result.Id = strings.Repeat("c", 32)
				}
				return result, nil
			}
			server := artifactPeerServer(t, peer)
			o := &options{endpoint: server.URL, timeout: 10 * time.Second}
			cmd := New(nil)
			cmd.SetContext(context.Background())
			var out, diagnostic bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&diagnostic)
			err := o.uploadArtifact(cmd, directory, "")
			if (err == nil) != tc.want {
				t.Fatalf("error=%v calls=%v", err, peer.calls)
			}
			if strings.Count(diagnostic.String(), "upload-id: "+ref.ID) != 1 {
				t.Fatalf("ID hint=%q", &diagnostic)
			}
			if tc.want {
				var got artifact.Ref
				if err := json.Unmarshal(out.Bytes(), &got); err != nil || got != ref {
					t.Fatalf("Ref=%v error=%v", got, err)
				}
			} else if out.Len() != 0 {
				t.Fatalf("failed upload printed Ref: %q", &out)
			}
			wantFinalizes := 2
			if tc.code == connect.CodePermissionDenied {
				wantFinalizes = 1
			}
			if peer.calls["BeginArtifactUpload"] != 2 || peer.calls["WriteArtifactChunk"] != 2 || peer.calls["GetArtifactUpload"] != 0 || peer.calls["FinalizeArtifactUpload"] != wantFinalizes {
				t.Fatalf("calls=%v", peer.calls)
			}
		})
	}
}

func TestArtifactBeginMarkedBudgetExhausted(t *testing.T) {
	peer := new(artifactPeer)
	peer.begin = func(n int, r *v1.BeginArtifactUploadRequest) (*v1.ArtifactUpload, error) {
		err := connect.NewError(connect.CodeResourceExhausted, errors.New("not started"))
		err.Meta().Set("Skygo-Artifact-Begin-Not-Started", "v1")
		return nil, err
	}
	server := artifactPeerServer(t, peer)
	o := &options{endpoint: server.URL}
	_, err := o.beginArtifactUpload(context.Background(), o.artifactClient(), strings.Repeat("b", 64), 2048, &artifactUploadRetries{used: 20})
	if err == nil || !strings.Contains(err.Error(), "no upload admitted by marked attempts") || peer.calls["BeginArtifactUpload"] != 1 {
		t.Fatalf("error=%v calls=%v", err, peer.calls)
	}
}

// This whole uploader consumes real waits: 4 Begin + 6 first-chunk + 6
// second-chunk + 4 Finalize = 20 retries, about five seconds before request time.
// A per-stage/per-chunk reset reaches the peer's sixth Finalize and wrongly
// succeeds. No seeded counter, replacement clock or retry hook is used here.
func TestArtifactUploadDistributedRetryLimit(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "seed"), bytes.Repeat([]byte("s"), artifactChunkSize), 0600); err != nil {
		t.Fatal(err)
	}
	var ref artifact.Ref
	var upload *v1.ArtifactUpload
	admitted := false
	peer := &artifactPeer{caps: []v1.Capability{v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1}}
	peer.begin = func(n int, r *v1.BeginArtifactUploadRequest) (*v1.ArtifactUpload, error) {
		if admitted {
			t.Error("Begin called after actual admission")
			return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("already admitted"))
		}
		if n == 1 {
			ref = artifact.Ref{ID: strings.Repeat("a", 32), SHA256: r.Sha256, Size: r.Size}
			if ref.Size <= artifactChunkSize || ref.Size > 2*artifactChunkSize {
				t.Errorf("fixture must span exactly two chunks: %d bytes", ref.Size)
				return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("wrong fixture size"))
			}
		}
		if r.Sha256 != ref.SHA256 || r.Size != ref.Size {
			t.Errorf("Begin changed immutable snapshot: %v", r)
		}
		if n <= 4 {
			err := connect.NewError(connect.CodeResourceExhausted, errors.New("not admitted"))
			err.Meta().Set("Skygo-Artifact-Begin-Not-Started", "v1")
			return nil, err
		}
		admitted = true
		upload = &v1.ArtifactUpload{Id: ref.ID, Reference: wireArtifactRef(ref), State: v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_WRITABLE, ExpiresAt: timestamppb.New(time.Now().Add(time.Hour))}
		peer.upload = upload
		return upload, nil
	}
	writes := make(map[int64]int)
	chunks := make(map[int64]*v1.WriteArtifactChunkRequest)
	peer.write = func(_ int, r *v1.WriteArtifactChunkRequest) (*v1.ArtifactUpload, error) {
		if !admitted || r.Id != ref.ID || (r.Offset != 0 && r.Offset != artifactChunkSize) {
			t.Errorf("write changed identity/range: %v", r)
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("wrong write"))
		}
		writes[r.Offset]++
		n := writes[r.Offset]
		if n == 1 {
			chunks[r.Offset] = proto.Clone(r).(*v1.WriteArtifactChunkRequest)
		} else if !proto.Equal(chunks[r.Offset], r) {
			t.Errorf("replay changed immutable chunk at %d", r.Offset)
		}
		if n == 1 && r.Offset == 0 {
			// The write happened, but its response was lost. Full status must
			// still be followed by an identical actual Write acknowledgment.
			upload.Acknowledged = int64(len(r.Data))
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("lost acknowledged response"))
		}
		if n == 1 {
			// This second chunk did not advance. Reconcile its original start.
			return nil, connect.NewError(connect.CodeDeadlineExceeded, errors.New("unacknowledged response"))
		}
		if n <= 6 {
			return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("pressure"))
		}
		upload.Acknowledged = r.Offset + int64(len(r.Data))
		return upload, nil
	}
	finalizes := 0
	peer.finalize = func(r *v1.FinalizeArtifactUploadRequest) (*v1.ArtifactRef, error) {
		finalizes++
		if r.Id != ref.ID || upload.Acknowledged != ref.Size {
			t.Errorf("Finalize changed identity or skipped bytes: %v", r)
		}
		if finalizes >= 6 {
			// Only an incorrectly reset budget can reach this actual success.
			return wireArtifactRef(ref), nil
		}
		codes := []connect.Code{connect.CodeResourceExhausted, connect.CodeUnavailable, connect.CodeDeadlineExceeded}
		return nil, connect.NewError(codes[(finalizes-1)%len(codes)], errors.New("finalize pending"))
	}
	server := artifactPeerServer(t, peer)
	o := &options{endpoint: server.URL, timeout: 15 * time.Second}
	cmd := New(nil)
	cmd.SetContext(context.Background())
	var out, diagnostic bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&diagnostic)
	started := time.Now()
	err := o.uploadArtifact(cmd, directory, "")
	if err == nil || !strings.Contains(err.Error(), "retry limit reached") || !strings.Contains(err.Error(), "--resume "+ref.ID) {
		t.Fatalf("want bounded failure with original resume ID: error=%v calls=%v", err, peer.calls)
	}
	if out.Len() != 0 || strings.Count(diagnostic.String(), "upload-id: "+ref.ID) != 1 {
		t.Fatalf("failure output=%q hint=%q", &out, &diagnostic)
	}
	if peer.calls["BeginArtifactUpload"] != 5 || writes[0] != 7 || writes[artifactChunkSize] != 7 || peer.calls["WriteArtifactChunk"] != 14 || peer.calls["GetArtifactUpload"] != 2 || finalizes != 5 {
		t.Fatalf("shared-budget request frontier: calls=%v writes=%v finalizes=%d", peer.calls, writes, finalizes)
	}
	// The request frontier accounts for exactly 20 retransmissions. The lower
	// wall-time bound also detects silently dropped/shortened production waits.
	if elapsed := time.Since(started); elapsed < 20*250*time.Millisecond {
		t.Fatalf("20 real waits completed too early: %v", elapsed)
	}
}
