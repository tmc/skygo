package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
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

func TestSnapshotDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "input"), []byte("input bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	a, digest, err := snapshotDirectory(dir)
	if err != nil {
		t.Fatal(err)
	}
	b, again, err := snapshotDirectory(dir)
	if err != nil || !bytes.Equal(a, b) || digest != again {
		t.Fatalf("snapshot changed: %q, %q, %v", digest, again, err)
	}
	if checked, err := artifact.Validate(bytes.NewReader(a), int64(len(a))); err != nil || checked != digest {
		t.Fatalf("digest=%q, error=%v", checked, err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := snapshotRoot(root); err != nil {
		t.Fatal(err)
	}
	if _, err := root.Lstat("."); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("consumed root still usable: %v", err)
	}
}

func TestSnapshotDirectoryRejectsPartial(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a-valid"), []byte("secret input"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("a-valid", filepath.Join(dir, "z-link")); err != nil {
		t.Fatal(err)
	}
	if archive, digest, err := snapshotDirectory(dir); err == nil || archive != nil || digest != "" {
		t.Fatalf("invalid snapshot published bytes=%d digest=%q err=%v", len(archive), digest, err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	if archive, digest, err := snapshotRoot(root); err == nil || archive != nil || digest != "" {
		t.Fatalf("invalid opened snapshot published bytes=%d digest=%q err=%v", len(archive), digest, err)
	}
	if _, err := root.Lstat("."); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("failed snapshot left its root open: %v", err)
	}
	if archive, digest, err := snapshotRoot(nil); err == nil || archive != nil || digest != "" {
		t.Fatal("nil root accepted")
	}
}

// This uses the existing actual Connect protocol peer. It measures client
// method/identity/byte behavior, not backend or filesystem publication.
func TestUploadSnapshotReturnsRef(t *testing.T) {
	for _, mode := range []string{"fresh", "resume", "finalizing", "complete", "nil-progress"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "input"), []byte("saved input"), 0600); err != nil {
				t.Fatal(err)
			}
			archive, digest, err := snapshotDirectory(dir)
			if err != nil {
				t.Fatal(err)
			}
			ref := artifact.Ref{ID: strings.Repeat("a", 32), SHA256: digest, Size: int64(len(archive))}
			upload := &v1.ArtifactUpload{Id: ref.ID, Reference: wireArtifactRef(ref), State: v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_WRITABLE, ExpiresAt: timestamppb.New(time.Now().Add(time.Hour))}
			resume := ""
			if mode == "resume" || mode == "finalizing" || mode == "complete" {
				resume = ref.ID
			}
			if mode == "finalizing" || mode == "complete" {
				upload.Acknowledged = ref.Size
				upload.State = v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_FINALIZING
				if mode == "complete" {
					upload.State = v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_COMPLETE
				}
			}
			peer := &artifactPeer{caps: []v1.Capability{v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1}, upload: upload}
			peer.begin = func(_ int, r *v1.BeginArtifactUploadRequest) (*v1.ArtifactUpload, error) {
				if r.Sha256 != ref.SHA256 || r.Size != ref.Size {
					t.Errorf("Begin changed archive identity: %v", r)
				}
				return proto.Clone(upload).(*v1.ArtifactUpload), nil
			}
			peer.write = func(_ int, r *v1.WriteArtifactChunkRequest) (*v1.ArtifactUpload, error) {
				if r.Id != ref.ID || r.Offset != 0 || !bytes.Equal(r.Data, archive) {
					t.Errorf("Write changed snapshot bytes or identity: %v", r)
				}
				upload.Acknowledged = ref.Size
				return proto.Clone(upload).(*v1.ArtifactUpload), nil
			}
			peer.finalize = func(r *v1.FinalizeArtifactUploadRequest) (*v1.ArtifactRef, error) {
				if r.Id != ref.ID {
					t.Errorf("Finalize changed identity: %v", r)
				}
				return wireArtifactRef(ref), nil
			}
			srv := artifactPeerServer(t, peer)
			rt := new(recordingTransport)
			o := &options{endpoint: srv.URL, http: &http.Client{Transport: rt}}
			var progress bytes.Buffer
			var w io.Writer = &progress
			if mode == "nil-progress" {
				w = nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			got, err := o.uploadSnapshot(ctx, archive, resume, w)
			if err != nil || got != ref {
				t.Fatalf("Ref=%+v error=%v, want %+v", got, err, ref)
			}
			if mode != "nil-progress" && progress.String() != fmt.Sprintf("upload-id: %s\n", ref.ID) {
				t.Fatalf("changed hint: %q", &progress)
			}
			peer.mu.Lock()
			calls := make(map[string]int, len(peer.calls))
			for k, v := range peer.calls {
				calls[k] = v
			}
			peer.mu.Unlock()
			if resume != "" && (calls["BeginArtifactUpload"] != 0 || calls["GetArtifactUpload"] != 1) {
				t.Fatalf("resume changed upload admission: %v", calls)
			}
			if (mode == "finalizing" || mode == "complete") && calls["WriteArtifactChunk"] != 0 {
				t.Fatalf("replayed acknowledged archive: %v", calls)
			}
			if calls["FinalizeArtifactUpload"] != 1 || calls["GetServerInfo"] != 1 || rt.count() != 2+calls["BeginArtifactUpload"]+calls["GetArtifactUpload"]+calls["WriteArtifactChunk"] {
				t.Fatalf("changed configured-client method selection: %v, transport=%d", calls, rt.count())
			}
		})
	}
}

func TestUploadSnapshotRejectsInvalidBeforeRPC(t *testing.T) {
	for _, archive := range [][]byte{nil, []byte("not an archive"), make([]byte, artifact.MaxSize+1)} {
		peer := &artifactPeer{caps: []v1.Capability{v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1}}
		srv := artifactPeerServer(t, peer)
		o := &options{endpoint: srv.URL}
		got, err := o.uploadSnapshot(context.Background(), archive, "", io.Discard)
		if err == nil || got != (artifact.Ref{}) {
			t.Fatalf("invalid archive returned Ref=%+v err=%v", got, err)
		}
		peer.mu.Lock()
		n := len(peer.calls)
		peer.mu.Unlock()
		if n != 0 {
			t.Fatal("invalid archive dispatched discovery or upload")
		}
	}
}

func TestUploadSnapshotUnknownBegin(t *testing.T) {
	archive, _, err := snapshotDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	peer := &artifactPeer{caps: []v1.Capability{v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1}}
	peer.begin = func(int, *v1.BeginArtifactUploadRequest) (*v1.ArtifactUpload, error) {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("lost Begin response"))
	}
	srv := artifactPeerServer(t, peer)
	o := &options{endpoint: srv.URL}
	got, err := o.uploadSnapshot(context.Background(), archive, "", nil)
	if err == nil || !strings.Contains(err.Error(), "outcome may be unknown") || got != (artifact.Ref{}) {
		t.Fatalf("unknown admission returned Ref=%+v err=%v", got, err)
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.calls["BeginArtifactUpload"] != 1 || peer.calls["rpc:WriteArtifactChunk"] != 0 || peer.calls["rpc:FinalizeArtifactUpload"] != 0 {
		t.Fatalf("unknown admission retried or advanced: %v", peer.calls)
	}
}
