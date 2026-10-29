package rpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"net/http"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/serving"
	"github.com/tmc/skygo/internal/servingcontrol"
	"github.com/tmc/skygo/internal/servingwire"
	"github.com/tmc/skygo/internal/store"
)

// servingHandler serves ServingService over one Controller. It runs inside
// the handler's authenticated mux, so requestActor names the caller.
type servingHandler struct {
	controller *servingcontrol.Controller
}

// newServingService returns the ServingService path and handler. A request
// holds at most one 64 KiB spec.
func newServingService(c *servingcontrol.Controller) (string, http.Handler) {
	return skyv1connect.NewServingServiceHandler(&servingHandler{controller: c}, connect.WithReadMaxBytes(128<<10))
}

// CreateService accepts or replays a service. The domain is required, and
// the spec is refused at any depth before the Controller if it carries an
// unknown field or a feature this endpoint does not run.
func (h *servingHandler) CreateService(ctx context.Context, r *connect.Request[v1.CreateServiceRequest]) (*connect.Response[v1.CreateServiceResponse], error) {
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
	spec, err := servingwire.SpecFromProto(r.Msg.Spec)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	s, err := h.controller.Create(requestActor(ctx), d, r.Msg.RequestId, r.Msg.Name, r.Msg.Target, spec)
	if err != nil {
		return nil, servingError(err)
	}
	return connect.NewResponse(&v1.CreateServiceResponse{Service: wireService(s)}), nil
}

func (h *servingHandler) GetService(ctx context.Context, r *connect.Request[v1.GetServiceRequest]) (*connect.Response[v1.GetServiceResponse], error) {
	if err := refuseUnknown(r.Msg); err != nil {
		return nil, err
	}
	s, err := h.controller.Get(requestActor(ctx), r.Msg.Id)
	if err != nil {
		return nil, servingError(err)
	}
	return connect.NewResponse(&v1.GetServiceResponse{Service: wireService(s)}), nil
}

// ListServices pages in acceptance order. A caller sees only its own
// services, or every service as an admin, whatever the token. The page token
// is opaque and bound to the principal it was issued to: another principal's
// token, a token that is not exactly one this server issues, and one longer
// than maxListCursor are invalid_argument. It is not secret and needs none:
// it carries only a listing position, and the Store decides visibility.
func (h *servingHandler) ListServices(ctx context.Context, r *connect.Request[v1.ListServicesRequest]) (*connect.Response[v1.ListServicesResponse], error) {
	if err := refuseUnknown(r.Msg); err != nil {
		return nil, err
	}
	size := int(r.Msg.PageSize)
	switch {
	case size == 0:
		size = 50
	case size < 0 || size > 200:
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("page size outside 0..200"))
	}
	actor := requestActor(ctx)
	var after uint64
	if t := r.Msg.PageToken; t != "" {
		n, ok := decodeListCursor(actor.Principal(), t)
		if !ok {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("invalid page token"))
		}
		after = n
	}
	p, err := h.controller.List(actor, after, size)
	if err != nil {
		return nil, servingError(err)
	}
	out := &v1.ListServicesResponse{}
	for _, s := range p.Services {
		out.Services = append(out.Services, wireServiceSummary(s))
	}
	if p.More {
		out.NextPageToken = encodeListCursor(actor.Principal(), p.After)
	}
	return connect.NewResponse(out), nil
}

// maxListCursor bounds a list page token; encodeListCursor's are 34 bytes.
const maxListCursor = 512

const listCursorVersion = 1

// listCursorBinding is the principal binding of a list cursor.
func listCursorBinding(principal string) []byte {
	sum := sha256.Sum256([]byte("skygo-serving-list-cursor-v1\x00" + principal))
	return sum[:16]
}

// encodeListCursor returns the page token of the listing position after,
// for principal: the unpadded base64url encoding of a version byte, the
// principal binding and after, big-endian.
func encodeListCursor(principal string, after uint64) string {
	b := make([]byte, 0, 25)
	b = append(b, listCursorVersion)
	b = append(b, listCursorBinding(principal)...)
	b = binary.BigEndian.AppendUint64(b, after)
	return base64.RawURLEncoding.EncodeToString(b)
}

// decodeListCursor returns the position of token, if it is exactly a token
// encodeListCursor returns for principal and a nonzero position.
func decodeListCursor(principal, token string) (uint64, bool) {
	if len(token) > maxListCursor {
		return 0, false
	}
	b, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || len(b) != 25 || b[0] != listCursorVersion || !bytes.Equal(b[1:17], listCursorBinding(principal)) {
		return 0, false
	}
	after := binary.BigEndian.Uint64(b[17:])
	if after == 0 || encodeListCursor(principal, after) != token {
		return 0, false
	}
	return after, true
}

func (h *servingHandler) DeleteService(ctx context.Context, r *connect.Request[v1.DeleteServiceRequest]) (*connect.Response[v1.DeleteServiceResponse], error) {
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
	s, err := h.controller.Delete(requestActor(ctx), d, r.Msg.RequestId, r.Msg.Id, r.Msg.ExpectedRecordRevision)
	if err != nil {
		if errors.Is(err, store.ErrServiceStale) {
			if cur, gerr := h.controller.Get(requestActor(ctx), r.Msg.Id); gerr == nil {
				return nil, serviceConflict(v1.ServiceConflictReason_SERVICE_CONFLICT_REASON_STALE_RECORD_REVISION, cur.Revision)
			}
		}
		return nil, servingError(err)
	}
	return connect.NewResponse(&v1.DeleteServiceResponse{Service: wireService(s)}), nil
}

// wireService projects s. It carries no binding, nonce, installation,
// authority or Native.
func wireService(s store.Service) *v1.Service {
	out := &v1.Service{
		Id:             s.ID,
		Name:           s.Name,
		Target:         s.Target,
		RecordRevision: s.Revision,
		Desired:        servingwire.DesiredToProto(s.Desired),
		Status:         wireServiceStatus(s),
		CreatedAt:      servingwire.Timestamp(s.CreatedAt),
		UpdatedAt:      servingwire.Timestamp(s.UpdatedAt),
		Retired:        s.Retired,
	}
	if !s.Retired {
		out.History = servingwire.HistoryToProto(s.History)
	} else {
		out.History = &v1.ServiceHistory{}
	}
	return out
}

func wireServiceSummary(s store.Service) *v1.ServiceSummary {
	cur := s.History.Current()
	out := &v1.ServiceSummary{
		Id:             s.ID,
		Name:           s.Name,
		Target:         s.Target,
		RecordRevision: s.Revision,
		Desired:        servingwire.DesiredToProto(s.Desired),
		Current:        servingwire.RolloutToProto(cur),
		Status:         wireServiceStatus(s),
		CreatedAt:      servingwire.Timestamp(s.CreatedAt),
		UpdatedAt:      servingwire.Timestamp(s.UpdatedAt),
	}
	if rev, ok := s.History.Revision(cur.Revision); ok {
		out.Sha256 = rev.SHA256
	}
	return out
}

func wireServiceStatus(s store.Service) *v1.ServiceStatus {
	out := &v1.ServiceStatus{
		Phase:             servingwire.PhaseToProto(s.Phase()),
		DesiredGeneration: s.History.Current().Generation,
		Observation:       servingwire.ObservationToProto(s.Observation),
		ObservedAt:        servingwire.Timestamp(s.ObservedAt),
	}
	for _, c := range s.Conditions {
		out.Conditions = append(out.Conditions, &v1.ServiceCondition{
			Code:       v1.ServiceConditionCode(c.Code),
			Generation: c.Generation,
			Since:      servingwire.Timestamp(c.Since),
		})
	}
	if s.Desired == serving.Absent && s.Phase() != serving.Deleted {
		out.Conditions = appendForwardDisabled(out.Conditions, s)
	}
	return out
}

// appendForwardDisabled reports that a deleting service admits no forward,
// unless a recorded condition already says so.
func appendForwardDisabled(cs []*v1.ServiceCondition, s store.Service) []*v1.ServiceCondition {
	for _, c := range cs {
		if c.Code == v1.ServiceConditionCode_SERVICE_CONDITION_CODE_FORWARD_DISABLED {
			return cs
		}
	}
	return append(cs, &v1.ServiceCondition{
		Code:       v1.ServiceConditionCode_SERVICE_CONDITION_CODE_FORWARD_DISABLED,
		Generation: s.History.Current().Generation,
		Since:      servingwire.Timestamp(s.UpdatedAt),
	})
}

func serviceConflict(reason v1.ServiceConflictReason, revision uint64) *connect.Error {
	e := connect.NewError(connect.CodeAborted, errors.New("service conflict"))
	if detail, err := connect.NewErrorDetail(&v1.ServiceConflict{Reason: reason, RecordRevision: revision}); err == nil {
		e.AddDetail(detail)
	}
	return e
}

// servingError maps a serving error to its code. An error of no known
// class, such as a runtime error, is internal and carries no text.
func servingError(err error) error {
	if e := retentionError(err); e != nil {
		return e
	}
	var capacity *store.ServiceCapacityError
	switch {
	case errors.Is(err, auth.ErrUnauthenticated):
		return connect.NewError(connect.CodeUnauthenticated, auth.ErrUnauthenticated)
	case errors.Is(err, auth.ErrPermissionDenied):
		return connect.NewError(connect.CodePermissionDenied, auth.ErrPermissionDenied)
	case errors.Is(err, auth.ErrUnavailable):
		return connect.NewError(connect.CodeUnavailable, auth.ErrUnavailable)
	case errors.Is(err, servingcontrol.ErrInvalid):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, servingcontrol.ErrUnsupported):
		return connect.NewError(connect.CodeInvalidArgument, errors.New("unsupported service spec"))
	case errors.Is(err, servingcontrol.ErrUnknownTarget):
		return connect.NewError(connect.CodeInvalidArgument, servingcontrol.ErrUnknownTarget)
	case errors.Is(err, store.ErrInvalidRequestDomain):
		return connect.NewError(connect.CodeInvalidArgument, store.ErrInvalidRequestDomain)
	case errors.Is(err, store.ErrNotFound):
		return connect.NewError(connect.CodeNotFound, errors.New("service not found"))
	case errors.Is(err, store.ErrConflict):
		return serviceConflict(v1.ServiceConflictReason_SERVICE_CONFLICT_REASON_REQUEST_REUSED, 0)
	case errors.Is(err, store.ErrServiceDeleted):
		return serviceConflict(v1.ServiceConflictReason_SERVICE_CONFLICT_REASON_DELETED, 0)
	case errors.Is(err, store.ErrServiceNameInUse):
		return serviceConflict(v1.ServiceConflictReason_SERVICE_CONFLICT_REASON_NAME_IN_USE, 0)
	case errors.Is(err, store.ErrServiceStale):
		return serviceConflict(v1.ServiceConflictReason_SERVICE_CONFLICT_REASON_STALE_RECORD_REVISION, 0)
	case errors.As(err, &capacity):
		e := connect.NewError(connect.CodeResourceExhausted, errors.New("service capacity reached"))
		if detail, derr := connect.NewErrorDetail(&v1.ServiceCapacity{
			Domain: v1.ServiceQuotaDomain(capacity.Domain), Used: capacity.Used, Requested: capacity.Requested, Limit: capacity.Limit,
		}); derr == nil {
			e.AddDetail(detail)
		}
		return e
	case errors.Is(err, servingcontrol.ErrClosed):
		return connect.NewError(connect.CodeUnavailable, servingcontrol.ErrClosed)
	}
	return connect.NewError(connect.CodeInternal, errors.New("internal operation failed"))
}
