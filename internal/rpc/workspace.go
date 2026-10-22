package rpc

import (
	"bytes"
	"context"
	"fmt"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
)

// SubmitWorkspaceJob preserves the caller's exact request domain. Control owns
// durable replay before placement, capacity or input-grant work on a MISS.
func (h *handler) SubmitWorkspaceJob(ctx context.Context, request *connect.Request[v1.SubmitWorkspaceJobRequest]) (*connect.Response[v1.SubmitWorkspaceJobResponse], error) {
	actor, err := h.engine.Check(requestActor(ctx), auth.Mutate)
	if err != nil {
		return nil, artifactError(err)
	}
	if request.Msg == nil || !validTaskUTF8(request.Msg.Task) || !retentionRequestID(request.Msg.RequestId) || request.Msg.Task.Workdir == nil || len(request.Msg.Task.DataMounts) != 0 {
		return nil, artifactError(fmt.Errorf("%w: workspace task and valid request ID required", control.ErrInvalid))
	}
	var accepted backend.Job
	if request.Msg.Domain == nil {
		accepted, err = h.control.SubmitWorkspace(actor, request.Msg.RequestId, task(request.Msg.Task))
	} else {
		domain, domainErr := requestDomain(request.Msg.Domain)
		if domainErr != nil {
			return nil, artifactError(domainErr)
		}
		accepted, err = h.control.SubmitWorkspaceInDomain(actor, domain, request.Msg.RequestId, task(request.Msg.Task))
	}
	h.record(ctx, "submit", accepted.ID, err)
	if err != nil {
		return nil, artifactError(err)
	}
	out, err := h.wireJob(ctx, accepted)
	if err != nil {
		return nil, artifactError(err)
	}
	return connect.NewResponse(&v1.SubmitWorkspaceJobResponse{Job: out}), nil
}

func wireArtifactResult(result store.ArtifactAttemptResult, attemptID string) (*v1.ArtifactResult, error) {
	if !hexID(attemptID) || result.AttemptID != attemptID {
		return nil, control.ErrUnavailable
	}
	out := &v1.ArtifactResult{AttemptId: attemptID}
	switch result.State {
	case "available":
		if result.Reference == nil || result.Reference.Size < 1 || result.Reference.Size > 32<<20 {
			return nil, control.ErrUnavailable
		}
		ref := result.Reference
		converted, err := wireArtifactReference(artifact.Ref{ID: ref.ID, SHA256: ref.SHA256, Size: int64(ref.Size)})
		if err != nil {
			return nil, err
		}
		out.State, out.Reference = v1.ArtifactResultState_ARTIFACT_RESULT_STATE_AVAILABLE, converted
	case "absent", "expired", "held":
		if result.Reference != nil {
			return nil, control.ErrUnavailable
		}
		switch result.State {
		case "absent":
			out.State = v1.ArtifactResultState_ARTIFACT_RESULT_STATE_ABSENT
		case "expired":
			out.State = v1.ArtifactResultState_ARTIFACT_RESULT_STATE_EXPIRED
		case "held":
			out.State = v1.ArtifactResultState_ARTIFACT_RESULT_STATE_HELD
		}
	default:
		return nil, control.ErrUnavailable
	}
	return out, nil
}

// wireJob adds authorized metadata without opening bytes or acquiring a grant.
// Result reads are separate snapshots. A final membership check refuses a job
// whose accepted execution set changed; it does not make all reads atomic.
func (h *handler) wireJob(ctx context.Context, accepted backend.Job) (*v1.Job, error) {
	out := job(accepted)
	if accepted.Task.Workdir == nil {
		return out, nil
	}
	if len(accepted.Attempts) > 10 {
		return nil, control.ErrUnavailable
	}
	actor, err := h.engine.Check(requestActor(ctx), auth.Read)
	if err != nil {
		return nil, err
	}
	current, err := h.control.Get(actor, accepted.ID)
	if err != nil {
		return nil, err
	}
	if !sameResultMembership(accepted, current) {
		return nil, control.ErrUnavailable
	}
	if len(accepted.Attempts) == 0 {
		result, err := h.control.ArtifactResultFor(actor, accepted.ID, accepted.ID)
		if err != nil {
			return nil, err
		}
		out.Result, err = wireArtifactResult(result, accepted.ID)
		if err != nil {
			return nil, err
		}
	} else {
		seen := make(map[string]bool, len(accepted.Attempts))
		for i, attempt := range accepted.Attempts {
			if !hexID(attempt.ID) || seen[attempt.ID] {
				return nil, control.ErrUnavailable
			}
			seen[attempt.ID] = true
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			result, err := h.control.ArtifactResultFor(actor, accepted.ID, attempt.ID)
			if err != nil {
				return nil, err
			}
			out.Attempts[i].Result, err = wireArtifactResult(result, attempt.ID)
			if err != nil {
				return nil, err
			}
		}
		last := out.Attempts[len(out.Attempts)-1].Result
		out.Result = &v1.ArtifactResult{State: last.State, AttemptId: last.AttemptId}
		if last.Reference != nil {
			out.Result.Reference = &v1.ArtifactRef{Id: last.Reference.Id, Sha256: last.Reference.Sha256, Size: last.Reference.Size}
		}
	}
	current, err = h.control.Get(actor, accepted.ID)
	if err != nil {
		return nil, err
	}
	if !sameResultMembership(accepted, current) {
		return nil, control.ErrUnavailable
	}
	if _, err = h.engine.Check(actor, auth.Read); err != nil {
		return nil, err
	}
	return out, nil
}

func sameResultMembership(a, b backend.Job) bool {
	if a.ID != b.ID || a.Target != b.Target || a.TargetBinding != b.TargetBinding || len(a.Attempts) != len(b.Attempts) {
		return false
	}
	for i := range a.Attempts {
		if a.Attempts[i].ID != b.Attempts[i].ID {
			return false
		}
	}
	x, e1 := backend.TaskBytes(a.Task)
	y, e2 := backend.TaskBytes(b.Task)
	return e1 == nil && e2 == nil && bytes.Equal(x, y)
}

func (h *handler) wireTarget(t backend.Target) *v1.Target {
	out := wireTarget(t)
	if h.control.WorkspaceTargetEnabled(t.Name, t.Binding) {
		out.Capabilities = append(out.Capabilities, v1.Capability_CAPABILITY_WORKDIR_SNAPSHOT_V1)
	}
	return out
}
