package rpc

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
)

const remoteMaxSize = 32 << 20

type remoteCallKey struct{}

type remoteCallContext struct {
	response  *artifactResponse
	operation control.RemoteOperation
}

func remoteOperation(path string) control.RemoteOperation {
	switch path {
	case skyv1connect.RemoteDataServiceReadInputProcedure:
		return control.RemoteReadInput
	case skyv1connect.RemoteDataServicePollReleaseProcedure:
		return control.RemotePollRelease
	case skyv1connect.RemoteDataServiceBeginOutputProcedure:
		return control.RemoteBeginOutput
	case skyv1connect.RemoteDataServiceWriteOutputProcedure:
		return control.RemoteWriteOutput
	case skyv1connect.RemoteDataServiceFinishOutputProcedure:
		return control.RemoteFinishOutput
	}
	return 0
}

// serveRemoteHTTP precedes user authentication: a worker credential is never an
// Actor. Only the generated service retained by NewHandler is passed here.
func (h *handler) serveRemoteHTTP(lifetime *HTTPHandler, prefix string, service http.Handler, w http.ResponseWriter, r *http.Request) bool {
	if !strings.HasPrefix(r.URL.Path, prefix) {
		return false
	}
	op := remoteOperation(r.URL.Path)
	if op == 0 {
		http.Error(w, "remote procedure unavailable", http.StatusNotFound)
		return true
	}
	if !remoteRequestProfile(r) {
		http.Error(w, "remote protocol unavailable", http.StatusUnsupportedMediaType)
		return true
	}
	limit := int64(16 << 10)
	if op == control.RemoteWriteOutput {
		limit = 96 << 10
	}
	if r.ContentLength > limit {
		http.Error(w, "remote request too large", http.StatusRequestEntityTooLarge)
		return true
	}
	owner, err := lifetime.admit(h.control, r.Body, w)
	if err != nil {
		code := http.StatusServiceUnavailable
		if errors.Is(err, control.ErrArtifactBusy) {
			code = http.StatusTooManyRequests
		}
		http.Error(w, "remote admission unavailable", code)
		return true
	}
	r.Body = owner.body
	header, single := remoteProfileHeader(r.Header, "Authorization")
	if !single || len(header) != 121 || !strings.HasPrefix(header, "Bearer ") {
		http.Error(w, "remote authorization unavailable", http.StatusUnauthorized)
		owner.finish()
		return true
	}
	if err = h.control.AuthorizeRemoteCall(r.Context(), owner.call, header[len("Bearer "):], op); err != nil {
		code := http.StatusServiceUnavailable
		switch {
		case errors.Is(err, auth.ErrUnauthenticated), errors.Is(err, store.ErrNotFound):
			code = http.StatusUnauthorized
		case errors.Is(err, auth.ErrPermissionDenied):
			code = http.StatusForbidden
		case errors.Is(err, control.ErrArtifactBusy):
			code = http.StatusTooManyRequests
		}
		http.Error(w, "remote authorization unavailable", code)
		owner.finish()
		return true
	}
	ctx := context.WithValue(r.Context(), remoteCallKey{}, remoteCallContext{owner, op})
	// No deferred positive finish: a panic or non-returning original invoker
	// retains the same HTTP owner. The invoker owns its body and final flush.
	err = h.control.ServeRemoteHTTP(owner.call, w, r.WithContext(ctx), service)
	owner.finishRemote(err)
	return true
}

func remoteCall(ctx context.Context, operation control.RemoteOperation) (*control.ArtifactCall, error) {
	value, ok := ctx.Value(remoteCallKey{}).(remoteCallContext)
	if !ok || value.operation != operation || value.response == nil || value.response.call == nil || value.response.used {
		return nil, control.ErrUnavailable
	}
	value.response.used = true
	return value.response.call, nil
}

// remoteError exposes categories only. Even a recognized wrapped cause may
// contain a credential, route or local physical identity and is never returned.
func remoteError(err error) error {
	if err == nil {
		return nil
	}
	code, message := connect.CodeInternal, "remote operation failed"
	var capacity *store.ArtifactCapacityError
	switch {
	case errors.Is(err, auth.ErrUnauthenticated), errors.Is(err, store.ErrNotFound):
		code, message = connect.CodeUnauthenticated, "remote authorization unavailable"
	case errors.Is(err, auth.ErrPermissionDenied):
		code, message = connect.CodePermissionDenied, "remote authorization unavailable"
	case errors.Is(err, context.Canceled):
		code, message = connect.CodeCanceled, "remote request canceled"
	case errors.Is(err, context.DeadlineExceeded):
		code, message = connect.CodeDeadlineExceeded, "remote request deadline exceeded"
	case errors.As(err, &capacity), errors.Is(err, control.ErrArtifactBusy), errors.Is(err, artifact.ErrFilesBusy):
		code, message = connect.CodeResourceExhausted, "remote operation busy"
	case errors.Is(err, control.ErrInvalid):
		code, message = connect.CodeInvalidArgument, "invalid remote request"
	case errors.Is(err, store.ErrArtifactConflict), errors.Is(err, store.ErrArtifactExpired), errors.Is(err, artifact.ErrFileConflict), errors.Is(err, store.ErrArtifactsDisabled), errors.Is(err, control.ErrUnsupported):
		code, message = connect.CodeFailedPrecondition, "remote operation unavailable"
	case errors.Is(err, control.ErrUnavailable), errors.Is(err, auth.ErrUnavailable), errors.Is(err, artifact.ErrFilesClosed):
		code, message = connect.CodeUnavailable, "remote service unavailable"
	}
	return connect.NewError(code, errors.New(message))
}

func (h *handler) ReadInput(ctx context.Context, request *connect.Request[v1.ReadInputRequest]) (*connect.Response[v1.ReadInputResponse], error) {
	if request == nil || request.Msg == nil || request.Msg.Offset < 0 || request.Msg.Limit < 1 || request.Msg.Limit > 64<<10 {
		return nil, remoteError(control.ErrInvalid)
	}
	call, err := remoteCall(ctx, control.RemoteReadInput)
	if err != nil {
		return nil, remoteError(err)
	}
	ref, data, err := h.control.ReadRemoteInput(ctx, call, request.Msg.Offset, int(request.Msg.Limit))
	if err != nil {
		return nil, remoteError(err)
	}
	out, err := wireRemoteInput(ref, data, request.Msg.Offset, int(request.Msg.Limit))
	if err != nil {
		return nil, remoteError(err)
	}
	return connect.NewResponse(out), nil
}

func wireRemoteInput(ref artifact.Ref, data []byte, offset int64, limit int) (*v1.ReadInputResponse, error) {
	if ref.Validate() != nil || offset < 0 || offset > ref.Size || limit < 1 || limit > 64<<10 {
		return nil, control.ErrUnavailable
	}
	size := int64(limit)
	if size > ref.Size-offset {
		size = ref.Size - offset
	}
	if int64(len(data)) != size {
		return nil, control.ErrUnavailable
	}
	reference, err := wireArtifactReference(ref)
	if err != nil {
		return nil, err
	}
	next := offset + size
	return &v1.ReadInputResponse{Data: data, NextOffset: next, Complete: next == ref.Size, Reference: reference}, nil
}

func (h *handler) PollRelease(ctx context.Context, request *connect.Request[v1.PollReleaseRequest]) (*connect.Response[v1.PollReleaseResponse], error) {
	if request == nil || request.Msg == nil {
		return nil, remoteError(control.ErrInvalid)
	}
	call, err := remoteCall(ctx, control.RemotePollRelease)
	if err != nil {
		return nil, remoteError(err)
	}
	release, err := h.control.PollRemoteRelease(ctx, call)
	if err != nil {
		return nil, remoteError(err)
	}
	out, err := wireRemoteRelease(release)
	if err != nil {
		return nil, remoteError(err)
	}
	return connect.NewResponse(out), nil
}

func wireRemoteRelease(value store.RemoteRelease) (*v1.PollReleaseResponse, error) {
	if value.Revision == 0 || value.State < 1 || value.State > 3 {
		return nil, control.ErrUnavailable
	}
	out := &v1.PollReleaseResponse{State: v1.RemoteReleaseState(value.State), Revision: value.Revision}
	if value.State != 2 {
		if value.TaskContainerID != "" || value.TaskExitCode != nil {
			return nil, control.ErrUnavailable
		}
		return out, nil
	}
	if len(value.TaskContainerID) < 1 || len(value.TaskContainerID) > 128 || value.TaskExitCode == nil || *value.TaskExitCode != 0 {
		return nil, control.ErrUnavailable
	}
	for _, c := range []byte(value.TaskContainerID) {
		if c < 32 || c > 126 {
			return nil, control.ErrUnavailable
		}
	}
	code := *value.TaskExitCode
	out.TaskContainerId, out.TaskExitCode = value.TaskContainerID, &code
	return out, nil
}

func (h *handler) BeginOutput(ctx context.Context, request *connect.Request[v1.BeginOutputRequest]) (*connect.Response[v1.BeginOutputResponse], error) {
	if request == nil || request.Msg == nil || !artifactDigest(request.Msg.Sha256) || request.Msg.Size < 1 || request.Msg.Size > remoteMaxSize {
		return nil, remoteError(control.ErrInvalid)
	}
	call, err := remoteCall(ctx, control.RemoteBeginOutput)
	if err != nil {
		return nil, remoteError(err)
	}
	upload, err := h.control.BeginRemoteOutput(ctx, call, request.Msg.Sha256, request.Msg.Size)
	if err != nil {
		return nil, remoteError(err)
	}
	out, err := wireRemoteBegin(upload, request.Msg.Sha256, request.Msg.Size)
	if err != nil {
		return nil, remoteError(err)
	}
	return connect.NewResponse(out), nil
}

func remoteUploadValid(value store.ArtifactUpload) bool {
	return value.Reference.Size > 0 && value.Reference.Size <= remoteMaxSize && value.Acknowledged <= value.Reference.Size &&
		(artifact.Ref{ID: value.Reference.ID, SHA256: value.Reference.SHA256, Size: int64(value.Reference.Size)}).Validate() == nil
}

func wireRemoteBegin(value store.ArtifactUpload, sha string, size int64) (*v1.BeginOutputResponse, error) {
	if size < 1 || size > remoteMaxSize || !remoteUploadValid(value) || value.Reference.SHA256 != sha || value.Reference.Size != uint64(size) {
		return nil, control.ErrUnavailable
	}
	var state v1.RemoteOutputState
	switch value.State {
	case "writable":
		state = v1.RemoteOutputState_REMOTE_OUTPUT_STATE_WRITABLE
	case "finalizing":
		state = v1.RemoteOutputState_REMOTE_OUTPUT_STATE_SEALING
	case "complete":
		state = v1.RemoteOutputState_REMOTE_OUTPUT_STATE_SEALED
	default:
		return nil, control.ErrUnavailable
	}
	if value.State != "writable" && value.Acknowledged != value.Reference.Size {
		return nil, control.ErrUnavailable
	}
	return &v1.BeginOutputResponse{Acknowledged: int64(value.Acknowledged), State: state}, nil
}

func (h *handler) WriteOutput(ctx context.Context, request *connect.Request[v1.WriteOutputRequest]) (*connect.Response[v1.WriteOutputResponse], error) {
	if request == nil || request.Msg == nil || !remoteWriteValid(request.Msg.Offset, request.Msg.Data) {
		return nil, remoteError(control.ErrInvalid)
	}
	call, err := remoteCall(ctx, control.RemoteWriteOutput)
	if err != nil {
		return nil, remoteError(err)
	}
	ack, err := h.control.WriteRemoteOutput(ctx, call, request.Msg.Offset, request.Msg.Data)
	if err != nil {
		return nil, remoteError(err)
	}
	out, err := wireRemoteWrite(ack, request.Msg.Offset, len(request.Msg.Data))
	if err != nil {
		return nil, remoteError(err)
	}
	return connect.NewResponse(out), nil
}

func wireRemoteWrite(ack uint64, offset int64, size int) (*v1.WriteOutputResponse, error) {
	if offset < 0 || offset > remoteMaxSize || size < 1 || size > 64<<10 || int64(size) > remoteMaxSize-offset || ack > remoteMaxSize || ack < uint64(offset)+uint64(size) {
		return nil, control.ErrUnavailable
	}
	return &v1.WriteOutputResponse{Acknowledged: int64(ack)}, nil
}

func remoteWriteValid(offset int64, data []byte) bool {
	return offset >= 0 && offset <= remoteMaxSize && len(data) > 0 && len(data) <= 64<<10 && int64(len(data)) <= remoteMaxSize-offset
}

func (h *handler) FinishOutput(ctx context.Context, request *connect.Request[v1.FinishOutputRequest]) (*connect.Response[v1.FinishOutputResponse], error) {
	if request == nil || request.Msg == nil {
		return nil, remoteError(control.ErrInvalid)
	}
	call, err := remoteCall(ctx, control.RemoteFinishOutput)
	if err != nil {
		return nil, remoteError(err)
	}
	upload, err := h.control.FinishRemoteOutput(ctx, call)
	if err != nil {
		return nil, remoteError(err)
	}
	out, err := wireRemoteFinish(upload)
	if err != nil {
		return nil, remoteError(err)
	}
	return connect.NewResponse(out), nil
}

func wireRemoteFinish(value store.ArtifactUpload) (*v1.FinishOutputResponse, error) {
	if !remoteUploadValid(value) || value.Acknowledged != value.Reference.Size {
		return nil, control.ErrUnavailable
	}
	switch value.State {
	case "finalizing":
		return &v1.FinishOutputResponse{State: v1.RemoteOutputSealState_REMOTE_OUTPUT_SEAL_STATE_SEALING}, nil
	case "complete":
		return &v1.FinishOutputResponse{State: v1.RemoteOutputSealState_REMOTE_OUTPUT_SEAL_STATE_SEALED}, nil
	}
	return nil, control.ErrUnavailable
}
