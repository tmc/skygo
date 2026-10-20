//go:build linux

package rpc_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
)

// This is a real executable task if admission regresses. The parent additionally
// checks the durable sequence and concrete Local state; marker absence alone is
// not evidence that an operating-system process never existed.
func TestArtifactRejectedWorkspaceMarker(t *testing.T) {
	name := os.Getenv("SKYGO_REJECTED_WORKSPACE_MARKER")
	if name == "" {
		return
	}
	file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func rejectedArtifactArchive(t *testing.T, name, link string) []byte {
	t.Helper()
	var buf bytes.Buffer
	writer := tar.NewWriter(&buf)
	header := &tar.Header{Name: name, Mode: 0600, Format: tar.FormatUSTAR}
	if link != "" {
		header.Typeflag, header.Linkname = tar.TypeSymlink, link
	}
	if err := writer.WriteHeader(header); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestArtifactHTTPRejectedUploadNeverLaunchesWorkspace(t *testing.T) {
	program, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	valid, digest := artifactTar(t)
	for _, tt := range []struct {
		name   string
		data   []byte
		digest string
		extra  int64
		code   connect.Code
		text   string
	}{
		{"declared_size", valid, digest, 512, connect.CodeFailedPrecondition, "artifact operation no longer available"},
		{"digest", valid, strings.Repeat("0", 64), 0, connect.CodeFailedPrecondition, "artifact operation no longer available"},
		{"parent_path", rejectedArtifactArchive(t, "../escaped", ""), "", 0, connect.CodeInternal, "internal operation failed"},
		{"symlink", rejectedArtifactArchive(t, "input", "../escaped"), "", 0, connect.CodeInternal, "internal operation failed"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := artifactRPCConfigured(t, nil, true)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			info, err := f.client.GetServerInfo(ctx, authorized(f.tokens["reader"], &v1.GetServerInfoRequest{}))
			if err != nil || len(info.Msg.Targets) != 1 || !hasCapability(info.Msg.Targets[0].Capabilities, v1.Capability_CAPABILITY_WORKDIR_SNAPSHOT_V1) {
				t.Fatalf("concrete Local workspace route: %v %v", info, err)
			}
			before, err := f.store.LastSequence()
			if err != nil || before != 0 {
				t.Fatalf("initial sequence=%d error=%v", before, err)
			}
			if tt.digest == "" {
				sum := sha256.Sum256(tt.data)
				tt.digest = hex.EncodeToString(sum[:])
			}
			started, err := f.beginUpload(t, ctx, tt.digest, int64(len(tt.data))+tt.extra)
			if err != nil {
				t.Fatal(err)
			}
			id := started.Msg.Upload.Id
			ref := started.Msg.Upload.Reference
			if ref == nil || ref.Size != int64(len(tt.data))+tt.extra || ref.Sha256 != tt.digest {
				t.Fatalf("Begin reference=%v", ref)
			}
			for offset := 0; offset < len(tt.data); {
				end := min(offset+(64<<10), len(tt.data))
				written, err := f.writeUpload(t, ctx, id, int64(offset), tt.data[offset:end])
				if err != nil || written.Msg.Upload.Acknowledged != int64(end) {
					t.Fatalf("write=%v error=%v", written, err)
				}
				offset = end
			}
			sealed, err := f.finalizeUpload(t, ctx, id)
			var wireError *connect.Error
			if sealed != nil || connect.CodeOf(err) != tt.code || !errors.As(err, &wireError) || wireError.Message() != tt.text {
				t.Fatalf("Finalize=%v error=%v; want %s %q", sealed, err, tt.code, tt.text)
			}
			marker := filepath.Join(f.directory, "workload-started")
			for _, attempt := range []struct {
				name string
				ref  *v1.ArtifactRef
				code connect.Code
			}{
				{"unfinalized", ref, connect.CodeNotFound},
				{"invalid", &v1.ArtifactRef{Id: ref.Id, Sha256: ref.Sha256}, connect.CodeInvalidArgument},
			} {
				request := &v1.SubmitWorkspaceJobRequest{
					RequestId: "rejected-" + tt.name + "-" + attempt.name,
					Task: &v1.Task{
						Argv:    []string{program, "-test.run=^TestArtifactRejectedWorkspaceMarker$"},
						Env:     map[string]string{"SKYGO_REJECTED_WORKSPACE_MARKER": marker},
						Workdir: attempt.ref,
					},
				}
				accepted, err := f.client.SubmitWorkspaceJob(ctx, authorized(f.tokens["alice"], request))
				f.awaitReturned(t, skyv1connect.ControlServiceSubmitWorkspaceJobProcedure, 1)
				if accepted != nil || connect.CodeOf(err) != attempt.code {
					t.Fatalf("%s submission=%v error=%v; want %s", attempt.name, accepted, err, attempt.code)
				}
			}
			// Join the original HTTP/Run/maintenance owners before the final
			// observations. An arbitrary sleep would leave a late-dispatch race.
			if !f.settle(t) {
				t.Fatal("original fixture did not settle")
			}
			after, err := f.store.LastSequence()
			if err != nil || after != before {
				t.Fatalf("refused submissions changed sequence: %d -> %d: %v", before, after, err)
			}
			jobs, err := f.store.List(0, ^uint64(0), 1)
			if err != nil || len(jobs) != 0 {
				t.Fatalf("durable launch members=%v error=%v", jobs, err)
			}
			if states := f.localBackend.PhysicalState(); len(states) != 0 {
				t.Fatalf("unexpected actual Local workspace: %+v", states)
			}
			if _, err := os.Lstat(marker); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("workload marker exists or could not be excluded: %v", err)
			}
		})
	}
}
