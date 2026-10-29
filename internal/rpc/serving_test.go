package rpc

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/serving"
	"github.com/tmc/skygo/internal/servingcontrol"
	"github.com/tmc/skygo/internal/store"
	"google.golang.org/protobuf/proto"
)

const serviceID = "0123456789abcdef0123456789abcdef"

func TestServiceForwardTarget(t *testing.T) {
	tests := []struct {
		raw     string
		ok      bool
		app     string
		rawPath string
	}{
		{"/services/" + serviceID + "/forward/", true, "/", ""},
		{"/services/" + serviceID + "/forward/v1/x?q=1", true, "/v1/x", ""},
		{"/services/" + serviceID + "/forward/a%2Fb", true, "/a/b", "/a%2Fb"},
		{"/services/" + serviceID + "/forward", false, "", ""},
		{"/services/" + serviceID + "/forward/../x", false, "", ""},
		{"/services/" + serviceID + "/forward/%2e%2e/x", false, "", ""},
		{"/services/" + serviceID + "/forward/%00", false, "", ""},
		{"/services/ABCDEF/forward/", false, "", ""},
		{"/services/" + serviceID + "/exec/", false, "", ""},
		{"/environments/" + serviceID + "/forward/", false, "", ""},
	}
	for _, tt := range tests {
		u, err := url.Parse(tt.raw)
		if err != nil {
			t.Fatal(err)
		}
		id, app, ok := appTarget(u, servicePrefix, store.ValidServiceID)
		if ok != tt.ok {
			t.Errorf("%s: ok = %v, want %v", tt.raw, ok, tt.ok)
			continue
		}
		if !ok {
			continue
		}
		if id != serviceID || app.Path != tt.app || app.RawPath != tt.rawPath || app.Host != "" {
			t.Errorf("%s: id %q app %#v", tt.raw, id, app)
		}
		if app.RawQuery != u.RawQuery {
			t.Errorf("%s: query %q, want %q", tt.raw, app.RawQuery, u.RawQuery)
		}
	}
}

func TestRemoveHopHeaders(t *testing.T) {
	h := http.Header{}
	h.Set("Connection", "keep-alive, X-Secret")
	h.Set("X-Secret", "1")
	h.Set("Keep-Alive", "timeout=5")
	h.Set("Te", "trailers")
	h.Set("Accept", "text/plain")
	removeHopHeaders(h)
	for _, name := range []string{"Connection", "X-Secret", "Keep-Alive", "Te"} {
		if h.Get(name) != "" {
			t.Errorf("%s kept", name)
		}
	}
	if h.Get("Accept") == "" {
		t.Error("end-to-end header removed")
	}
}

// TestServiceForwardRefusesTunnels checks that an upgrade is refused before
// the Controller is consulted: a nil Controller would panic otherwise.
func TestServiceForwardRefusesTunnels(t *testing.T) {
	for _, set := range []func(*http.Request){
		func(r *http.Request) { r.Header.Set("Upgrade", "websocket") },
		func(r *http.Request) { r.Header.Set("Connection", "Upgrade") },
		func(r *http.Request) { r.Method = http.MethodConnect },
	} {
		r := httptest.NewRequest("GET", "/services/"+serviceID+"/forward/", nil)
		set(r)
		w := httptest.NewRecorder()
		serveServiceForward(nil, w, r)
		if w.Code != http.StatusBadRequest {
			t.Errorf("status %d, want 400", w.Code)
		}
	}
	w := httptest.NewRecorder()
	serveServiceForward(nil, w, httptest.NewRequest("GET", "/services/"+serviceID+"/forward/../", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("bad path status %d, want 404", w.Code)
	}
}

func TestServiceForwardError(t *testing.T) {
	tests := []struct {
		err  error
		code int
	}{
		{auth.ErrUnauthenticated, http.StatusUnauthorized},
		{auth.ErrPermissionDenied, http.StatusForbidden},
		{auth.ErrUnavailable, http.StatusServiceUnavailable},
		{store.ErrNotFound, http.StatusNotFound},
		{servingcontrol.ErrNotReady, http.StatusConflict},
		{servingcontrol.ErrForwardLimit, http.StatusTooManyRequests},
		{errors.New("pod 10.0.0.1 refused"), http.StatusBadGateway},
	}
	for _, tt := range tests {
		code, text := serviceForwardError(tt.err)
		if code != tt.code {
			t.Errorf("%v: code %d, want %d", tt.err, code, tt.code)
		}
		if code == http.StatusBadGateway && text != "service forward failed" {
			t.Errorf("unclassified error leaked %q", text)
		}
	}
}

func TestServingError(t *testing.T) {
	tests := []struct {
		err    error
		code   connect.Code
		reason v1.ServiceConflictReason
	}{
		{store.ErrNotFound, connect.CodeNotFound, 0},
		{store.ErrConflict, connect.CodeAborted, v1.ServiceConflictReason_SERVICE_CONFLICT_REASON_REQUEST_REUSED},
		{store.ErrServiceDeleted, connect.CodeAborted, v1.ServiceConflictReason_SERVICE_CONFLICT_REASON_DELETED},
		{store.ErrServiceNameInUse, connect.CodeAborted, v1.ServiceConflictReason_SERVICE_CONFLICT_REASON_NAME_IN_USE},
		{serving.ErrUnsupported, connect.CodeInvalidArgument, 0},
		{servingcontrol.ErrUnknownTarget, connect.CodeInvalidArgument, 0},
		{&store.ServiceCapacityError{Domain: store.ServiceCPUQuota, Used: 1, Requested: 2, Limit: 2}, connect.CodeResourceExhausted, 0},
		{errors.New("kube: secret xyz"), connect.CodeInternal, 0},
	}
	for _, tt := range tests {
		var ce *connect.Error
		if !errors.As(servingError(tt.err), &ce) {
			t.Fatalf("%v: not a connect error", tt.err)
		}
		if ce.Code() != tt.code {
			t.Errorf("%v: code %v, want %v", tt.err, ce.Code(), tt.code)
		}
		if tt.code == connect.CodeInternal && ce.Message() != "internal operation failed" {
			t.Errorf("internal error leaked %q", ce.Message())
		}
		if tt.reason == 0 {
			continue
		}
		var got v1.ServiceConflictReason
		for _, d := range ce.Details() {
			if m, err := d.Value(); err == nil {
				if c, ok := m.(*v1.ServiceConflict); ok {
					got = c.Reason
				}
			}
		}
		if got != tt.reason {
			t.Errorf("%v: conflict reason %v, want %v", tt.err, got, tt.reason)
		}
	}
}

// TestWireServiceWithholdsServerState checks that no response field carries
// the binding, nonce, installation, authority or Native.
func TestWireServiceWithholdsServerState(t *testing.T) {
	h, err := serving.NewHistory(serving.Spec{
		Image:     "registry.example.com/web@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Argv:      []string{"/bin/web"},
		Port:      8080,
		Readiness: serving.Readiness{Path: "/healthz"},
		Resources: serving.Resources{CPUMillis: 250, MemoryBytes: 1 << 28},
		Scaling:   serving.Scaling{Replicas: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	s := store.Service{
		ID: serviceID, Installation: "install-secret", Nonce: "nonce-secret", Principal: "alice",
		Name: "web", Target: "kube", Binding: "binding-secret", Native: "native-secret",
		CreatedAt: time.Now(), UpdatedAt: time.Now(), Revision: 3, Desired: serving.Present, History: h,
	}
	b, err := proto.Marshal(wireService(s))
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"install-secret", "nonce-secret", "binding-secret", "native-secret"} {
		if contains(b, secret) {
			t.Errorf("response carries %s", secret)
		}
	}
	m := wireService(s)
	if m.Status.Phase != v1.ServicePhase_SERVICE_PHASE_CREATING || m.RecordRevision != 3 {
		t.Fatalf("projection %v", m)
	}
}

func contains(b []byte, s string) bool {
	for i := 0; i+len(s) <= len(b); i++ {
		if string(b[i:i+len(s)]) == s {
			return true
		}
	}
	return false
}
