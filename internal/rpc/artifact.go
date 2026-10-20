package rpc

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func artifactReference(value *v1.ArtifactRef) (artifact.Ref, error) {
	if value == nil {
		return artifact.Ref{}, fmt.Errorf("%w: artifact reference required", control.ErrInvalid)
	}
	out := artifact.Ref{ID: value.Id, SHA256: value.Sha256, Size: value.Size}
	if err := out.Validate(); err != nil {
		return artifact.Ref{}, fmt.Errorf("%w: invalid artifact reference", control.ErrInvalid)
	}
	return out, nil
}

func wireArtifactReference(value artifact.Ref) (*v1.ArtifactRef, error) {
	if err := value.Validate(); err != nil {
		return nil, control.ErrUnavailable
	}
	return &v1.ArtifactRef{Id: value.ID, Sha256: value.SHA256, Size: value.Size}, nil
}

func wireArtifactUpload(value store.ArtifactUpload) (*v1.ArtifactUpload, error) {
	if value.Reference.Size < 1 || value.Reference.Size > 32<<20 || value.Acknowledged > value.Reference.Size {
		return nil, control.ErrUnavailable
	}
	ref, err := wireArtifactReference(artifact.Ref{ID: value.Reference.ID, SHA256: value.Reference.SHA256, Size: int64(value.Reference.Size)})
	if err != nil {
		return nil, err
	}
	states := map[string]v1.ArtifactUploadState{
		"creating":   v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_CREATING,
		"writable":   v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_WRITABLE,
		"finalizing": v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_FINALIZING,
		"complete":   v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_COMPLETE,
		"deleting":   v1.ArtifactUploadState_ARTIFACT_UPLOAD_STATE_DELETING,
	}
	state, ok := states[value.State]
	expires := timestamppb.New(value.ExpiresAt)
	if !ok || value.ExpiresAt.IsZero() || expires.CheckValid() != nil {
		return nil, control.ErrUnavailable
	}
	return &v1.ArtifactUpload{Id: ref.Id, Reference: ref, Acknowledged: int64(value.Acknowledged), ExpiresAt: expires, State: state}, nil
}

// artifactError exposes no physical paths, inode identities or internal closure
// records. Existing authorized job result expiry is a separate wire projection.
func artifactError(err error) error {
	if err == nil {
		return nil
	}
	var capacity *store.ArtifactCapacityError
	switch {
	case errors.As(err, &capacity):
		return connect.NewError(connect.CodeResourceExhausted, errors.New("artifact capacity reached"))
	case errors.Is(err, control.ErrArtifactBusy), errors.Is(err, artifact.ErrFilesBusy):
		return connect.NewError(connect.CodeResourceExhausted, errors.New("artifact operation busy"))
	case errors.Is(err, store.ErrArtifactConflict), errors.Is(err, artifact.ErrFileConflict), errors.Is(err, store.ErrArtifactExpired):
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("artifact operation no longer available"))
	case errors.Is(err, store.ErrArtifactsDisabled), errors.Is(err, artifact.ErrFilesClosed):
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("artifact service unavailable"))
	}
	return rpcError(err)
}

// artifactBeginError is the only RPCBeginMarkerOnly conversion. The private
// Control cause exists only before Store.Begin; no request header or generic
// Busy/transport/Store/Files error can manufacture this advice.
func artifactBeginError(err error) error {
	if control.IsArtifactBeginNotStarted(err) {
		refused := connect.NewError(connect.CodeResourceExhausted, errors.New("artifact operation busy"))
		refused.Meta().Set("Skygo-Artifact-Begin-Not-Started", "v1")
		return refused
	}
	return artifactError(err)
}

func (h *handler) BeginArtifactUpload(ctx context.Context, request *connect.Request[v1.BeginArtifactUploadRequest]) (*connect.Response[v1.BeginArtifactUploadResponse], error) {
	actor, err := h.artifactActor(ctx, auth.Mutate)
	if err != nil {
		return nil, artifactError(err)
	}
	if request.Msg == nil || request.Msg.Size < 1 || request.Msg.Size > 32<<20 || !artifactDigest(request.Msg.Sha256) {
		return nil, artifactError(control.ErrInvalid)
	}
	call, err := h.artifactCall(ctx, auth.Mutate)
	if err != nil {
		return nil, artifactError(err)
	}
	upload, err := h.control.BeginArtifactUpload(ctx, actor, call, request.Msg.Sha256, request.Msg.Size)
	if err != nil {
		return nil, artifactBeginError(err)
	}
	out, err := wireArtifactUpload(upload)
	if err != nil {
		return nil, artifactError(err)
	}
	if _, err := h.artifactActor(ctx, auth.Mutate); err != nil {
		return nil, artifactError(err)
	}
	return connect.NewResponse(&v1.BeginArtifactUploadResponse{Upload: out}), nil
}

func (h *handler) GetArtifactUpload(ctx context.Context, request *connect.Request[v1.GetArtifactUploadRequest]) (*connect.Response[v1.GetArtifactUploadResponse], error) {
	actor, err := h.artifactActor(ctx, auth.Read)
	if err != nil {
		return nil, artifactError(err)
	}
	if request.Msg == nil || !hexID(request.Msg.Id) {
		return nil, artifactError(control.ErrInvalid)
	}
	upload, err := h.control.GetArtifactUpload(ctx, actor, request.Msg.Id)
	if err != nil {
		return nil, artifactError(err)
	}
	out, err := wireArtifactUpload(upload)
	if err != nil {
		return nil, artifactError(err)
	}
	if _, err := h.artifactActor(ctx, auth.Read); err != nil {
		return nil, artifactError(err)
	}
	return connect.NewResponse(&v1.GetArtifactUploadResponse{Upload: out}), nil
}

func (h *handler) WriteArtifactChunk(ctx context.Context, request *connect.Request[v1.WriteArtifactChunkRequest]) (*connect.Response[v1.WriteArtifactChunkResponse], error) {
	actor, err := h.artifactActor(ctx, auth.Mutate)
	if err != nil {
		return nil, artifactError(err)
	}
	if request.Msg == nil || !hexID(request.Msg.Id) || request.Msg.Offset < 0 || request.Msg.Offset > 32<<20 || len(request.Msg.Data) < 1 || len(request.Msg.Data) > 64<<10 {
		return nil, artifactError(control.ErrInvalid)
	}
	call, err := h.artifactCall(ctx, auth.Mutate)
	if err != nil {
		return nil, artifactError(err)
	}
	upload, err := h.control.WriteArtifactChunk(ctx, actor, call, request.Msg.Id, request.Msg.Offset, request.Msg.Data)
	if err != nil {
		return nil, artifactError(err)
	}
	out, err := wireArtifactUpload(upload)
	if err != nil {
		return nil, artifactError(err)
	}
	if _, err := h.artifactActor(ctx, auth.Mutate); err != nil {
		return nil, artifactError(err)
	}
	return connect.NewResponse(&v1.WriteArtifactChunkResponse{Upload: out}), nil
}

func (h *handler) FinalizeArtifactUpload(ctx context.Context, request *connect.Request[v1.FinalizeArtifactUploadRequest]) (*connect.Response[v1.FinalizeArtifactUploadResponse], error) {
	actor, err := h.artifactActor(ctx, auth.Mutate)
	if err != nil {
		return nil, artifactError(err)
	}
	if request.Msg == nil || !hexID(request.Msg.Id) {
		return nil, artifactError(control.ErrInvalid)
	}
	call, err := h.artifactCall(ctx, auth.Mutate)
	if err != nil {
		return nil, artifactError(err)
	}
	ref, err := h.control.FinalizeArtifactUpload(ctx, actor, call, request.Msg.Id)
	if err != nil {
		return nil, artifactError(err)
	}
	out, err := wireArtifactReference(ref)
	if err != nil {
		return nil, artifactError(err)
	}
	if _, err := h.artifactActor(ctx, auth.Mutate); err != nil {
		return nil, artifactError(err)
	}
	return connect.NewResponse(&v1.FinalizeArtifactUploadResponse{Reference: out}), nil
}

func (h *handler) CancelArtifactUpload(ctx context.Context, request *connect.Request[v1.CancelArtifactUploadRequest]) (*connect.Response[v1.CancelArtifactUploadResponse], error) {
	actor, err := h.artifactActor(ctx, auth.Mutate)
	if err != nil {
		return nil, artifactError(err)
	}
	if request.Msg == nil || !hexID(request.Msg.Id) {
		return nil, artifactError(control.ErrInvalid)
	}
	call, err := h.artifactCall(ctx, auth.Mutate)
	if err != nil {
		return nil, artifactError(err)
	}
	if err := h.control.CancelArtifactUpload(ctx, actor, call, request.Msg.Id); err != nil {
		return nil, artifactError(err)
	}
	if _, err := h.artifactActor(ctx, auth.Mutate); err != nil {
		return nil, artifactError(err)
	}
	return connect.NewResponse(&v1.CancelArtifactUploadResponse{}), nil
}

func (h *handler) BeginArtifactDownload(ctx context.Context, request *connect.Request[v1.BeginArtifactDownloadRequest]) (*connect.Response[v1.BeginArtifactDownloadResponse], error) {
	actor, err := h.artifactActor(ctx, auth.Read)
	if err != nil {
		return nil, artifactError(err)
	}
	if request.Msg == nil {
		return nil, artifactError(control.ErrInvalid)
	}
	ref, err := artifactReference(request.Msg.Reference)
	if err != nil {
		return nil, artifactError(err)
	}
	call, err := h.artifactCall(ctx, auth.Read)
	if err != nil {
		return nil, artifactError(err)
	}
	download, err := h.control.BeginArtifactDownload(ctx, actor, call, ref)
	// Control retains every nonnil original handle, including one returned with
	// an error. RPC never drops that ownership or creates a replacement reader.
	if err != nil {
		return nil, artifactError(err)
	}
	if download == nil || !hexID(download.ID()) || download.ExpiresAt().IsZero() {
		return nil, artifactError(control.ErrUnavailable)
	}
	out, err := wireArtifactReference(download.Reference())
	if err != nil || download.Reference() != ref {
		return nil, artifactError(control.ErrUnavailable)
	}
	expires := timestamppb.New(download.ExpiresAt())
	if expires.CheckValid() != nil {
		return nil, artifactError(control.ErrUnavailable)
	}
	if _, err := h.artifactActor(ctx, auth.Read); err != nil {
		return nil, artifactError(err)
	}
	return connect.NewResponse(&v1.BeginArtifactDownloadResponse{Id: download.ID(), Reference: out, ExpiresAt: expires}), nil
}

func (h *handler) ReadArtifactChunk(ctx context.Context, request *connect.Request[v1.ReadArtifactChunkRequest]) (*connect.Response[v1.ReadArtifactChunkResponse], error) {
	actor, err := h.artifactActor(ctx, auth.Read)
	if err != nil {
		return nil, artifactError(err)
	}
	if request.Msg == nil || !hexID(request.Msg.DownloadId) || request.Msg.Offset < 0 || request.Msg.Offset > 32<<20 || request.Msg.Limit < 1 || request.Msg.Limit > 64<<10 {
		return nil, artifactError(control.ErrInvalid)
	}
	call, err := h.artifactCall(ctx, auth.Read)
	if err != nil {
		return nil, artifactError(err)
	}
	data, complete, err := h.control.ReadArtifactChunk(ctx, actor, call, request.Msg.DownloadId, request.Msg.Offset, int(request.Msg.Limit))
	if err != nil {
		return nil, artifactError(err)
	}
	if len(data) > int(request.Msg.Limit) || request.Msg.Offset+int64(len(data)) > 32<<20 {
		return nil, artifactError(control.ErrUnavailable)
	}
	if _, err := h.artifactActor(ctx, auth.Read); err != nil {
		return nil, artifactError(err)
	}
	return connect.NewResponse(&v1.ReadArtifactChunkResponse{Data: data, NextOffset: request.Msg.Offset + int64(len(data)), Complete: complete}), nil
}

func (h *handler) CloseArtifactDownload(ctx context.Context, request *connect.Request[v1.CloseArtifactDownloadRequest]) (*connect.Response[v1.CloseArtifactDownloadResponse], error) {
	actor, err := h.artifactActor(ctx, auth.Read)
	if err != nil {
		return nil, artifactError(err)
	}
	if request.Msg == nil || !hexID(request.Msg.DownloadId) {
		return nil, artifactError(control.ErrInvalid)
	}
	call, err := h.artifactCall(ctx, auth.Read)
	if err != nil {
		return nil, artifactError(err)
	}
	if err := h.control.CloseArtifactDownload(ctx, actor, call, request.Msg.DownloadId); err != nil {
		return nil, artifactError(err)
	}
	if _, err := h.artifactActor(ctx, auth.Read); err != nil {
		return nil, artifactError(err)
	}
	return connect.NewResponse(&v1.CloseArtifactDownloadResponse{}), nil
}

func (h *handler) RemoveArtifact(ctx context.Context, request *connect.Request[v1.RemoveArtifactRequest]) (*connect.Response[v1.RemoveArtifactResponse], error) {
	actor, err := h.artifactActor(ctx, auth.Mutate)
	if err != nil {
		return nil, artifactError(err)
	}
	if request.Msg == nil {
		return nil, artifactError(control.ErrInvalid)
	}
	ref, err := artifactReference(request.Msg.Reference)
	if err != nil {
		return nil, artifactError(err)
	}
	if err := h.control.RemoveArtifact(ctx, actor, ref); err != nil {
		return nil, artifactError(err)
	}
	if _, err := h.artifactActor(ctx, auth.Mutate); err != nil {
		return nil, artifactError(err)
	}
	return connect.NewResponse(&v1.RemoveArtifactResponse{}), nil
}

func artifactDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if c < '0' || c > '9' && c < 'a' || c > 'f' {
			return false
		}
	}
	return true
}
