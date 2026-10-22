package rpc

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
)

// SubmitDataJob uses the dedicated durable remote route. Control owns semantic
// replay before new placement, delegation capture or input reservations.
func (h *handler) SubmitDataJob(ctx context.Context, request *connect.Request[v1.SubmitDataJobRequest]) (*connect.Response[v1.SubmitDataJobResponse], error) {
	actor, err := h.engine.Check(requestActor(ctx), auth.Mutate)
	if err != nil {
		return nil, dataError(err)
	}
	if actor.TokenID() == "" {
		return nil, dataError(control.ErrUnsupported)
	}
	if request == nil || request.Msg == nil || !validDataTaskUTF8(request.Msg.Task) || !retentionRequestID(request.Msg.RequestId) {
		return nil, dataError(control.ErrInvalid)
	}
	value := task(request.Msg.Task)
	if err = backend.ValidateDataTask(value, nil); err != nil {
		return nil, dataError(control.ErrInvalid)
	}
	var accepted backend.Job
	if request.Msg.Domain == nil {
		accepted, err = h.control.SubmitData(actor, request.Msg.RequestId, value)
	} else {
		domain, domainErr := requestDomain(request.Msg.Domain)
		if domainErr != nil {
			return nil, dataError(control.ErrInvalid)
		}
		accepted, err = h.control.SubmitDataInDomain(actor, domain, request.Msg.RequestId, value)
	}
	h.record(ctx, "submit", accepted.ID, err)
	if err != nil {
		return nil, dataError(err)
	}
	out, err := h.wireJob(ctx, accepted)
	if err != nil {
		return nil, dataError(err)
	}
	return connect.NewResponse(&v1.SubmitDataJobResponse{Job: out}), nil
}

// Keep the public retention details/codes while removing contextual causes.
func dataError(err error) error {
	if err == nil {
		return nil
	}
	if retained := retentionError(err); retained != nil {
		return retained
	}
	if code := connect.CodeOf(rpcError(err)); code != connect.CodeInternal {
		return connect.NewError(code, errors.New("data operation unavailable"))
	}
	return remoteError(err)
}

func taskDataMounts(values []*v1.DataMount) []backend.DataMount {
	if values == nil {
		return nil
	}
	out := make([]backend.DataMount, len(values))
	for i, value := range values {
		if value != nil {
			out[i] = backend.DataMount{Reference: value.Reference, Destination: value.Destination, ReadOnly: value.ReadOnly}
		}
	}
	return out
}

func wireDataMounts(values []backend.DataMount) []*v1.DataMount {
	if values == nil {
		return nil
	}
	out := make([]*v1.DataMount, len(values))
	for i, value := range values {
		out[i] = &v1.DataMount{Reference: value.Reference, Destination: value.Destination, ReadOnly: value.ReadOnly}
	}
	return out
}

func remoteTargetAvailable(t backend.Target) bool {
	if !t.SupportsRemoteData || t.DataPolicy == nil || t.DataPolicy.Validate() != nil {
		return false
	}
	_, ok := t.Backend.(backend.DataBackend)
	return ok
}

func remoteRouteAvailable(targets []backend.Target) bool {
	for _, target := range targets {
		if remoteTargetAvailable(target) {
			return true
		}
	}
	return false
}

func addRemoteTarget(t backend.Target, out *v1.Target) {
	if !remoteTargetAvailable(t) {
		return
	}
	p := *t.DataPolicy
	out.DataPolicy = &v1.DataPolicy{StageSeconds: p.StageSeconds, CollectionSeconds: p.CollectionSeconds, DefaultTaskSeconds: p.DefaultTaskSeconds, MaxTaskSeconds: p.MaxTaskSeconds, ShutdownSeconds: p.ShutdownSeconds}
	out.Capabilities = append(out.Capabilities, v1.Capability_CAPABILITY_REMOTE_DATA_V1)
	if t.SupportsDataMounts {
		out.Capabilities = append(out.Capabilities, v1.Capability_CAPABILITY_DATA_MOUNTS_V1)
	}
	if t.SupportsRetry && t.SupportsRemoteDataRetry {
		out.Capabilities = append(out.Capabilities, v1.Capability_CAPABILITY_REMOTE_DATA_RETRY_V1)
	}
}

// Planning may inspect a data task only for an explicitly selected, currently
// configured data target. The real Control.Plan still owns full placement.
func (h *handler) remotePlanTask(ctx context.Context, input *v1.Task) bool {
	if input.Target == "" || backend.ValidateDataTask(task(input), nil) != nil {
		return false
	}
	targets, err := h.control.Targets(requestActor(ctx))
	if err != nil {
		return false
	}
	for _, target := range targets {
		if target.Name == input.Target && remoteTargetAvailable(target) && (len(input.DataMounts) == 0 || target.SupportsDataMounts) {
			return true
		}
	}
	return false
}
