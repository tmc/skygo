package rpc

import (
	"context"
	"encoding/hex"
	"errors"
	"net/http"
	"strconv"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/environmentcontrol"
	"github.com/tmc/skygo/internal/environmentwire"
	"github.com/tmc/skygo/internal/store"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// environmentHandler serves EnvironmentService over one Controller, whose
// runtime decides which targets it accepts. It runs inside the handler's
// authenticated mux, so requestActor names the caller.
type environmentHandler struct {
	controller *environmentcontrol.Controller
}

// newEnvironmentService returns the EnvironmentService path and handler. An
// exec request holds at most 64 KiB of argv and 64 KiB of stdin.
func newEnvironmentService(c *environmentcontrol.Controller) (string, http.Handler) {
	return skyv1connect.NewEnvironmentServiceHandler(&environmentHandler{controller: c}, connect.WithReadMaxBytes(256<<10))
}

// PlanEnvironment checks a spec as CreateEnvironment would, with only the
// caller's read authority. It records nothing.
func (h *environmentHandler) PlanEnvironment(ctx context.Context, r *connect.Request[v1.PlanEnvironmentRequest]) (*connect.Response[v1.PlanEnvironmentResponse], error) {
	if err := refuseUnknown(r.Msg); err != nil {
		return nil, err
	}
	spec, err := environmentwire.SpecFromProto(r.Msg.Spec)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	p, err := h.controller.Plan(requestActor(ctx), spec)
	if err != nil {
		return nil, environmentError(err)
	}
	return connect.NewResponse(&v1.PlanEnvironmentResponse{
		Spec:       environmentwire.SpecToProto(p.Spec),
		SpecSha256: hex.EncodeToString(p.SpecDigest[:]),
		Support:    planSupport[p.Support],
		Artifacts:  planFact[p.Artifacts],
	}), nil
}

// CreateEnvironment accepts or replays an environment in the request's
// domain. A missing, zero or invalid domain is refused before the
// Controller.
func (h *environmentHandler) CreateEnvironment(ctx context.Context, r *connect.Request[v1.CreateEnvironmentRequest]) (*connect.Response[v1.CreateEnvironmentResponse], error) {
	if err := refuseUnknown(r.Msg, r.Msg.Domain); err != nil {
		return nil, err
	}
	d, err := environmentDomain(r.Msg.Domain)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if !retentionRequestID(r.Msg.RequestId) {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid request ID"))
	}
	spec, err := environmentwire.SpecFromProto(r.Msg.Spec)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	v, err := h.controller.Create(requestActor(ctx), d, r.Msg.RequestId, spec)
	if err != nil {
		return nil, environmentError(err)
	}
	e, err := wireEnvironment(v)
	if err != nil {
		return nil, environmentError(err)
	}
	return connect.NewResponse(&v1.CreateEnvironmentResponse{Environment: e}), nil
}

func (h *environmentHandler) GetEnvironment(ctx context.Context, r *connect.Request[v1.GetEnvironmentRequest]) (*connect.Response[v1.GetEnvironmentResponse], error) {
	if err := refuseUnknown(r.Msg); err != nil {
		return nil, err
	}
	v, err := h.controller.Get(requestActor(ctx), r.Msg.Id)
	if err != nil {
		return nil, environmentError(err)
	}
	e, err := wireEnvironment(v)
	if err != nil {
		return nil, environmentError(err)
	}
	return connect.NewResponse(&v1.GetEnvironmentResponse{Environment: e}), nil
}

// ListEnvironments pages in acceptance order. The page token is the
// Controller's cursor in decimal.
func (h *environmentHandler) ListEnvironments(ctx context.Context, r *connect.Request[v1.ListEnvironmentsRequest]) (*connect.Response[v1.ListEnvironmentsResponse], error) {
	if err := refuseUnknown(r.Msg); err != nil {
		return nil, err
	}
	var after uint64
	if r.Msg.PageToken != "" {
		n, err := strconv.ParseUint(r.Msg.PageToken, 10, 64)
		if err != nil || len(r.Msg.PageToken) > 256 {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid page token"))
		}
		after = n
	}
	p, err := h.controller.List(requestActor(ctx), after, int(r.Msg.PageSize))
	if err != nil {
		return nil, environmentError(err)
	}
	out := &v1.ListEnvironmentsResponse{}
	for _, v := range p.Environments {
		e, err := wireEnvironment(v)
		if err != nil {
			return nil, environmentError(err)
		}
		out.Environments = append(out.Environments, e)
	}
	if p.More {
		out.NextPageToken = strconv.FormatUint(p.After, 10)
	}
	return connect.NewResponse(out), nil
}

func (h *environmentHandler) DeleteEnvironment(ctx context.Context, r *connect.Request[v1.DeleteEnvironmentRequest]) (*connect.Response[v1.DeleteEnvironmentResponse], error) {
	if err := refuseUnknown(r.Msg); err != nil {
		return nil, err
	}
	v, err := h.controller.Delete(requestActor(ctx), r.Msg.Id)
	if err != nil {
		return nil, environmentError(err)
	}
	e, err := wireEnvironment(v)
	if err != nil {
		return nil, environmentError(err)
	}
	return connect.NewResponse(&v1.DeleteEnvironmentResponse{Environment: e}), nil
}

// ExecEnvironmentCommand runs one command. Controller.Exec returns an error
// only if no command started, and a refused result started none, so both
// are unavailable unless a more specific class applies. A started command
// is never an error: its exit code is absent when its outcome is unknown.
func (h *environmentHandler) ExecEnvironmentCommand(ctx context.Context, r *connect.Request[v1.ExecEnvironmentCommandRequest]) (*connect.Response[v1.ExecEnvironmentCommandResponse], error) {
	if err := refuseUnknown(r.Msg); err != nil {
		return nil, err
	}
	res, err := h.controller.Exec(ctx, requestActor(ctx), r.Msg.Id, environmentcontrol.ExecRequest{Argv: r.Msg.Argv, Stdin: r.Msg.Stdin})
	if err == nil && res.Refused && !res.Exited {
		err = errors.New("environment command refused")
	}
	if err != nil {
		if e := environmentClass(err); e != nil {
			return nil, e
		}
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("environment command not started"))
	}
	out := &v1.ExecEnvironmentCommandResponse{Stdout: res.Stdout, Stderr: res.Stderr, Truncated: res.Truncated}
	if res.Exited {
		code := int32(res.Code)
		out.ExitCode = &code
	}
	return connect.NewResponse(out), nil
}

// environmentDomain returns a CreateEnvironment request's domain. Unlike
// requestDomain, a missing or zero domain is invalid, never
// RequestDomainRequired, and epoch zero is invalid, never a legacy replay:
// no environment request is domain-free.
func environmentDomain(m *v1.RequestDomain) (store.RequestDomain, error) {
	if m == nil {
		return store.RequestDomain{}, errors.New("missing request domain")
	}
	if !hexID(m.Lineage) || m.Epoch == 0 {
		return store.RequestDomain{}, errors.New("invalid request domain")
	}
	return store.RequestDomain{Lineage: m.Lineage, Epoch: m.Epoch}, nil
}

// refuseUnknown refuses a request message carrying fields this server does
// not declare. environmentwire.SpecFromProto checks a spec at every depth;
// the other environment request messages hold no nested message but the
// domain, which the caller passes too.
func refuseUnknown(ms ...protoreflect.ProtoMessage) error {
	for _, m := range ms {
		if r := m.ProtoReflect(); r.IsValid() && len(r.GetUnknown()) != 0 {
			return connect.NewError(connect.CodeInvalidArgument, errors.New("request has unknown fields"))
		}
	}
	return nil
}

func wireEnvironment(v environmentcontrol.View) (*v1.Environment, error) {
	sum, err := environmentwire.SpecSHA256(v.Spec)
	if err != nil {
		return nil, err
	}
	e := &v1.Environment{
		Id:              v.ID,
		Spec:            environmentwire.SpecToProto(v.Spec),
		SpecSha256:      sum,
		Desired:         v1.EnvironmentDesired(v.State.Desired),
		Phase:           v1.EnvironmentPhase(v.State.Phase),
		CreatedAt:       timestamppb.New(v.CreatedAt),
		UpdatedAt:       timestamppb.New(v.UpdatedAt),
		OpenSessions:    uint32(v.Sessions),
		UnknownSessions: uint32(v.Unknown),
		Expired:         v.Expired,
	}
	if t, ok := v.ExpiresAt(); ok {
		e.ExpiresAt = timestamppb.New(t)
	}
	return e, nil
}

// environmentError maps an environment error to its code. An error of no
// known class, such as a runtime error, which may name configuration, or the
// Store's plain "unknown request epoch", is internal and carries no text.
// It is never passed through rpcError, which keeps the text of every code
// but internal.
func environmentError(err error) error {
	if e := environmentClass(err); e != nil {
		return e
	}
	return connect.NewError(connect.CodeInternal, errors.New("internal operation failed"))
}

// environmentClass returns the coded error of err's class, or nil. Retention
// errors are matched by type, with the same details jobs and services carry;
// every other class is a sentinel. Only ErrInvalid keeps its text, which
// describes the caller's own request.
func environmentClass(err error) *connect.Error {
	if e := retentionError(err); e != nil {
		return e
	}
	switch {
	case errors.Is(err, auth.ErrUnauthenticated):
		return connect.NewError(connect.CodeUnauthenticated, auth.ErrUnauthenticated)
	case errors.Is(err, auth.ErrPermissionDenied):
		return connect.NewError(connect.CodePermissionDenied, auth.ErrPermissionDenied)
	case errors.Is(err, auth.ErrUnavailable):
		return connect.NewError(connect.CodeUnavailable, auth.ErrUnavailable)
	case errors.Is(err, environmentcontrol.ErrInvalid):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, environmentcontrol.ErrUnsupported):
		return connect.NewError(connect.CodeInvalidArgument, errors.New("unsupported environment spec"))
	case errors.Is(err, environmentcontrol.ErrNotReady):
		return connect.NewError(connect.CodeFailedPrecondition, environmentcontrol.ErrNotReady)
	case errors.Is(err, environmentcontrol.ErrClosed):
		return connect.NewError(connect.CodeFailedPrecondition, environmentcontrol.ErrClosed)
	case errors.Is(err, environmentcontrol.ErrNoPort):
		return connect.NewError(connect.CodeFailedPrecondition, environmentcontrol.ErrNoPort)
	case errors.Is(err, environmentcontrol.ErrSessionLimit):
		return connect.NewError(connect.CodeResourceExhausted, environmentcontrol.ErrSessionLimit)
	case errors.Is(err, store.ErrNotFound):
		return connect.NewError(connect.CodeNotFound, store.ErrNotFound)
	case errors.Is(err, store.ErrConflict):
		return connect.NewError(connect.CodeAlreadyExists, errors.New("request ID names another spec"))
	case errors.Is(err, store.ErrEnvironmentReserved):
		return connect.NewError(connect.CodeAlreadyExists, store.ErrEnvironmentReserved)
	case errors.Is(err, store.ErrEnvironmentLimit):
		return connect.NewError(connect.CodeResourceExhausted, store.ErrEnvironmentLimit)
	case errors.Is(err, store.ErrEnvironmentArtifact):
		return connect.NewError(connect.CodeFailedPrecondition, store.ErrEnvironmentArtifact)
	case errors.Is(err, store.ErrRetentionDisabled):
		return connect.NewError(connect.CodeFailedPrecondition, store.ErrRetentionDisabled)
	}
	return nil
}

var planSupport = map[environmentcontrol.Support]v1.EnvironmentPlanSupport{
	environmentcontrol.Supported:   v1.EnvironmentPlanSupport_ENVIRONMENT_PLAN_SUPPORT_SUPPORTED,
	environmentcontrol.Unsupported: v1.EnvironmentPlanSupport_ENVIRONMENT_PLAN_SUPPORT_UNSUPPORTED,
}

var planFact = map[environmentcontrol.Fact]v1.EnvironmentPlanFact{
	environmentcontrol.FactNotApplicable: v1.EnvironmentPlanFact_ENVIRONMENT_PLAN_FACT_UNSPECIFIED,
	environmentcontrol.FactUnknown:       v1.EnvironmentPlanFact_ENVIRONMENT_PLAN_FACT_UNKNOWN,
	environmentcontrol.FactVerified:      v1.EnvironmentPlanFact_ENVIRONMENT_PLAN_FACT_VERIFIED,
	environmentcontrol.FactRefused:       v1.EnvironmentPlanFact_ENVIRONMENT_PLAN_FACT_REFUSED,
}
