package rpc

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
)

func (h *handler) GetRetention(ctx context.Context, request *connect.Request[v1.GetRetentionRequest]) (*connect.Response[v1.GetRetentionResponse], error) {
	actor, err := h.engine.Check(requestActor(ctx), auth.Read)
	if err != nil {
		return nil, rpcError(err)
	}
	if request.Msg == nil {
		return nil, rpcError(fmt.Errorf("%w: retention request required", control.ErrInvalid))
	}
	if err := retentionPage(request.Msg.PageSize, request.Msg.PageToken); err != nil {
		return nil, rpcError(err)
	}
	view, err := h.control.RetentionInfo(actor, int(request.Msg.PageSize), request.Msg.PageToken)
	if err != nil {
		return nil, rpcError(err)
	}
	out, err := wireRetention(view)
	if err != nil {
		return nil, rpcError(err)
	}
	// Discovery is read-only; h.record would enqueue a durable audit mutation.
	return connect.NewResponse(out), nil
}

func (h *handler) SubmitRetainedJob(ctx context.Context, request *connect.Request[v1.SubmitRetainedJobRequest]) (*connect.Response[v1.SubmitRetainedJobResponse], error) {
	actor, err := h.engine.Check(requestActor(ctx), auth.Mutate)
	if err != nil {
		return nil, rpcError(err)
	}
	if request.Msg == nil || !validTaskUTF8(request.Msg.Task) || !retentionRequestID(request.Msg.RequestId) {
		return nil, rpcError(fmt.Errorf("%w: task and valid request ID required", control.ErrInvalid))
	}
	if hasWorkspaceTask(request.Msg.Task) {
		return nil, rpcError(control.ErrUnsupported)
	}
	domain, err := requestDomain(request.Msg.Domain)
	if err != nil {
		return nil, rpcError(err)
	}
	accepted, err := h.control.SubmitRetained(actor, domain, request.Msg.RequestId, task(request.Msg.Task))
	h.record(ctx, "submit", accepted.ID, err)
	if err != nil {
		return nil, rpcError(err)
	}
	out, err := retainedJob(accepted)
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&v1.SubmitRetainedJobResponse{Job: out}), nil
}

func (h *handler) PruneJobs(ctx context.Context, request *connect.Request[v1.PruneJobsRequest]) (*connect.Response[v1.PruneJobsResponse], error) {
	actor, err := h.engine.Check(requestActor(ctx), auth.Mutate)
	if err != nil {
		return nil, rpcError(err)
	}
	if actor.Role() != auth.Admin {
		return nil, rpcError(auth.ErrPermissionDenied)
	}
	if request.Msg == nil {
		return nil, rpcError(fmt.Errorf("%w: prune request required", control.ErrInvalid))
	}
	selection, err := pruneSelection(request.Msg)
	if err != nil {
		return nil, rpcError(err)
	}
	page, next, err := h.control.Prune(ctx, actor, selection, request.Msg.Execute, request.Msg.PageToken)
	if err != nil {
		return nil, rpcError(err)
	}
	out, err := wirePrune(page, next, request.Msg.Execute)
	if err != nil {
		return nil, rpcError(err)
	}
	// Control audits only actual authorized mutations. In particular neither
	// dry-run nor a revoked first closure query may enqueue persistent audit.
	return connect.NewResponse(out), nil
}

func retainedJob(value backend.Job) (*v1.Job, error) {
	out := job(value)
	if out.CreatedAt.CheckValid() != nil || out.UpdatedAt.CheckValid() != nil {
		return nil, retentionUnavailable()
	}
	if out.NextAttemptAt != nil && out.NextAttemptAt.CheckValid() != nil {
		return nil, retentionUnavailable()
	}
	for _, attempt := range out.Attempts {
		if attempt.CreatedAt.CheckValid() != nil || (attempt.StartingAt != nil && attempt.StartingAt.CheckValid() != nil) || (attempt.FinishedAt != nil && attempt.FinishedAt.CheckValid() != nil) {
			return nil, retentionUnavailable()
		}
	}
	return out, nil
}
