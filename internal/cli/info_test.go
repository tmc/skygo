package cli

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"rsc.io/script/scripttest"
)

type infoService struct {
	skyv1connect.UnimplementedControlServiceHandler
	targets []*v1.Target
	err     error
	calls   atomic.Int32
}

func (s *infoService) GetServerInfo(_ context.Context, r *connect.Request[v1.GetServerInfoRequest]) (*connect.Response[v1.GetServerInfoResponse], error) {
	s.calls.Add(1)
	if r.Header().Get("Authorization") != "Bearer info-token" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid token"))
	}
	if s.err != nil {
		return nil, s.err
	}
	return connect.NewResponse(&v1.GetServerInfoResponse{Targets: s.targets}), nil
}

func serveInfo(t *testing.T, service *infoService) *httptest.Server {
	t.Helper()
	path, handler := skyv1connect.NewControlServiceHandler(service)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func infoTargets() []*v1.Target {
	return []*v1.Target{
		{Name: "supported", Kind: "local", Capabilities: []v1.Capability{v1.Capability_CAPABILITY_FINITE_JOBS_V1}},
		{Name: "limited", Kind: "fixture"},
		{Name: "unknown", Kind: "fixture", Capabilities: []v1.Capability{99}},
	}
}

func TestRequireCapabilities(t *testing.T) {
	finite := []v1.Capability{v1.Capability_CAPABILITY_FINITE_JOBS_V1}
	for _, tc := range []struct {
		name, target string
		required     []v1.Capability
		code         connect.Code
		calls        int32
	}{
		{name: "supported", target: "supported", required: finite, calls: 1},
		{name: "another target cannot authorize", target: "limited", required: finite, code: connect.CodeFailedPrecondition, calls: 1},
		{name: "unknown advertisement", target: "unknown", required: finite, code: connect.CodeFailedPrecondition, calls: 1},
		{name: "unknown requirement", target: "unknown", required: []v1.Capability{99}, code: connect.CodeInvalidArgument},
		{name: "unspecified requirement", target: "supported", required: []v1.Capability{0}, code: connect.CodeInvalidArgument},
		{name: "missing target", target: "absent", required: finite, code: connect.CodeNotFound, calls: 1},
		{name: "target required", required: finite, code: connect.CodeInvalidArgument},
		{name: "list", calls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := &infoService{targets: infoTargets()}
			srv := serveInfo(t, service)
			o := &options{endpoint: srv.URL, token: "info-token"}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := o.requireCapabilities(ctx, tc.target, tc.required...)
			if tc.code == 0 && err != nil || tc.code != 0 && (err == nil || connect.CodeOf(err) != tc.code) {
				t.Fatalf("error = %v; want code %v", err, tc.code)
			}
			if got := service.calls.Load(); got != tc.calls {
				t.Fatalf("discovery calls = %d; want %d", got, tc.calls)
			}
		})
	}
}

func TestCapabilityEndpoints(t *testing.T) {
	first := serveInfo(t, &infoService{targets: infoTargets()[:1]})
	second := serveInfo(t, &infoService{targets: []*v1.Target{{Name: "supported", Kind: "fixture"}}})
	o := &options{endpoint: first.URL, token: "info-token"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := o.requireCapabilities(ctx, "supported", v1.Capability_CAPABILITY_FINITE_JOBS_V1); err != nil {
		t.Fatal(err)
	}
	o.endpoint = second.URL
	if _, err := o.requireCapabilities(ctx, "supported", v1.Capability_CAPABILITY_FINITE_JOBS_V1); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("another endpoint inherited capability: %v", err)
	}
}

func TestDiscoveryErrors(t *testing.T) {
	for _, tc := range []struct {
		name, token string
		err         error
		code        connect.Code
		legacy      bool
	}{
		{"unimplemented", "info-token", connect.NewError(connect.CodeUnimplemented, errors.New("old server")), connect.CodeUnimplemented, true},
		{"unauthenticated", "wrong", nil, connect.CodeUnauthenticated, false},
		{"unavailable", "info-token", connect.NewError(connect.CodeUnavailable, errors.New("temporarily unavailable")), connect.CodeUnavailable, false},
		{"deadline", "info-token", connect.NewError(connect.CodeDeadlineExceeded, errors.New("deadline")), connect.CodeDeadlineExceeded, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := serveInfo(t, &infoService{err: tc.err})
			_, _, err := execute(context.Background(), "--endpoint", srv.URL, "--token", tc.token, "info", "--target", "supported", "--require", "finite-jobs-v1")
			if err == nil || connect.CodeOf(err) != tc.code {
				t.Fatalf("error = %v; want %v", err, tc.code)
			}
			if got := strings.Contains(err.Error(), "legacy task commands"); got != tc.legacy {
				t.Fatalf("legacy diagnostic = %v; error = %v", got, err)
			}
		})
	}
}

func TestInfoScripts(t *testing.T) {
	srv := serveInfo(t, &infoService{targets: infoTargets()})
	scripttest.Test(t, context.Background(), scriptEngine(t), []string{"SKYGO_ENDPOINT=" + srv.URL, "SKYGO_TOKEN=info-token"}, "testdata/info/*.txt")
}

func TestDuplicateTargetAdvertisement(t *testing.T) {
	srv := serveInfo(t, &infoService{targets: []*v1.Target{
		{Name: "same", Capabilities: []v1.Capability{v1.Capability_CAPABILITY_FINITE_JOBS_V1}},
		{Name: "same"},
	}})
	o := &options{endpoint: srv.URL, token: "info-token"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := o.requireCapabilities(ctx, "same", v1.Capability_CAPABILITY_FINITE_JOBS_V1); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("ambiguous target was accepted: %v", err)
	}
}

func TestDiscoveryTransportFailure(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close()
	o := &options{endpoint: srv.URL, token: "info-token"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := o.requireCapabilities(ctx, "local", v1.Capability_CAPABILITY_FINITE_JOBS_V1)
	if err == nil || connect.CodeOf(err) != connect.CodeUnavailable || strings.Contains(err.Error(), "legacy task commands") {
		t.Fatalf("closed endpoint misclassified: %v", err)
	}
	ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err = o.requireCapabilities(ctx, "local", v1.Capability_CAPABILITY_FINITE_JOBS_V1)
	if err == nil || connect.CodeOf(err) != connect.CodeDeadlineExceeded || strings.Contains(err.Error(), "legacy task commands") {
		t.Fatalf("expired deadline misclassified: %v", err)
	}
}
