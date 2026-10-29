package sshagent

import (
	"context"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
)

// artifactReadMaxBytes bounds each artifact response message Connect
// reads, before and after decompression, as the skygo command's artifact
// client does.
const artifactReadMaxBytes = 128 << 10

// A control is a session's control client. It sends each artifact method
// through artifact, a generated client that refuses a response message
// larger than artifactReadMaxBytes, and every other method through the embedded
// generated client, with Connect's default limits. Both share one ledger,
// recorder, HTTP client and bearer; see newControl.
type control struct {
	ordinary
	artifact skyv1connect.ControlServiceClient
}

// An ordinary is the generated client control embeds. It is a distinct
// unexported name only so that the embedded field is unexported.
type ordinary interface {
	skyv1connect.ControlServiceClient
}

var _ skyv1connect.ControlServiceClient = (*control)(nil)

func (c *control) BeginArtifactUpload(ctx context.Context, req *connect.Request[v1.BeginArtifactUploadRequest]) (*connect.Response[v1.BeginArtifactUploadResponse], error) {
	return c.artifact.BeginArtifactUpload(ctx, req)
}

func (c *control) GetArtifactUpload(ctx context.Context, req *connect.Request[v1.GetArtifactUploadRequest]) (*connect.Response[v1.GetArtifactUploadResponse], error) {
	return c.artifact.GetArtifactUpload(ctx, req)
}

func (c *control) WriteArtifactChunk(ctx context.Context, req *connect.Request[v1.WriteArtifactChunkRequest]) (*connect.Response[v1.WriteArtifactChunkResponse], error) {
	return c.artifact.WriteArtifactChunk(ctx, req)
}

func (c *control) FinalizeArtifactUpload(ctx context.Context, req *connect.Request[v1.FinalizeArtifactUploadRequest]) (*connect.Response[v1.FinalizeArtifactUploadResponse], error) {
	return c.artifact.FinalizeArtifactUpload(ctx, req)
}

func (c *control) CancelArtifactUpload(ctx context.Context, req *connect.Request[v1.CancelArtifactUploadRequest]) (*connect.Response[v1.CancelArtifactUploadResponse], error) {
	return c.artifact.CancelArtifactUpload(ctx, req)
}

func (c *control) BeginArtifactDownload(ctx context.Context, req *connect.Request[v1.BeginArtifactDownloadRequest]) (*connect.Response[v1.BeginArtifactDownloadResponse], error) {
	return c.artifact.BeginArtifactDownload(ctx, req)
}

func (c *control) ReadArtifactChunk(ctx context.Context, req *connect.Request[v1.ReadArtifactChunkRequest]) (*connect.Response[v1.ReadArtifactChunkResponse], error) {
	return c.artifact.ReadArtifactChunk(ctx, req)
}

func (c *control) CloseArtifactDownload(ctx context.Context, req *connect.Request[v1.CloseArtifactDownloadRequest]) (*connect.Response[v1.CloseArtifactDownloadResponse], error) {
	return c.artifact.CloseArtifactDownload(ctx, req)
}

func (c *control) RemoveArtifact(ctx context.Context, req *connect.Request[v1.RemoveArtifactRequest]) (*connect.Response[v1.RemoveArtifactResponse], error) {
	return c.artifact.RemoveArtifact(ctx, req)
}
