package rpc

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/environmentcontrol"
	"github.com/tmc/skygo/internal/store"
)

func TestForwardTarget(t *testing.T) {
	id := strings.Repeat("0f", 16)
	tests := []struct {
		name    string
		uri     string
		ok      bool
		path    string // the application URL's Path
		escaped string // its EscapedPath
		query   string
	}{
		{"root", "/environments/" + id + "/forward/", true, "/", "/", ""},
		{"path and query", "/environments/" + id + "/forward/a/b?x=1&y=%2F", true, "/a/b", "/a/b", "x=1&y=%2F"},
		{"escaped slash stays escaped", "/environments/" + id + "/forward/a%2Fb/c", true, "/a/b/c", "/a%2Fb/c", ""},
		{"escaped space", "/environments/" + id + "/forward/a%20b", true, "/a b", "/a%20b", ""},
		{"empty segment kept", "/environments/" + id + "/forward/a//b", true, "/a//b", "/a//b", ""},
		{"no trailing slash", "/environments/" + id + "/forward", false, "", "", ""},
		{"other suffix", "/environments/" + id + "/forwarded/", false, "", "", ""},
		{"upper id", "/environments/" + strings.ToUpper(id) + "/forward/", false, "", "", ""},
		{"short id", "/environments/" + id[:31] + "/forward/", false, "", "", ""},
		{"no id", "/environments//forward/", false, "", "", ""},
		{"dot", "/environments/" + id + "/forward/a/./b", false, "", "", ""},
		{"dot dot", "/environments/" + id + "/forward/../" + id, false, "", "", ""},
		{"escaped dot dot", "/environments/" + id + "/forward/%2e%2e/x", false, "", "", ""},
		{"escaped nul", "/environments/" + id + "/forward/a%00", false, "", "", ""},
		{"escaped prefix", "/%65nvironments/" + id + "/forward/", false, "", "", ""},
		{"other route", "/skygo.v1.ControlService/GetServerInfo", false, "", "", ""},
	}
	for _, test := range tests {
		u, err := url.ParseRequestURI(test.uri)
		if err != nil {
			t.Fatalf("%s: %v", test.name, err)
		}
		gotID, app, ok := forwardTarget(u)
		if ok != test.ok {
			t.Errorf("%s: ok = %v, want %v", test.name, ok, test.ok)
			continue
		}
		if !ok {
			continue
		}
		if gotID != id || app.Path != test.path || app.EscapedPath() != test.escaped || app.RawQuery != test.query || app.Host != "" || app.Scheme != "" {
			t.Errorf("%s: id %s, url %#v", test.name, gotID, app)
		}
	}
}

// TestForwardRefusesBeforeController checks refusals that must not reach the
// Controller, which is nil here.
func TestForwardRefusesBeforeController(t *testing.T) {
	route := "/environments/" + strings.Repeat("0f", 16) + "/forward/"
	tests := []struct {
		name   string
		method string
		target string
		header http.Header
		code   int
	}{
		{"connect", http.MethodConnect, route, nil, http.StatusBadRequest},
		{"upgrade", http.MethodGet, route, http.Header{"Upgrade": {"websocket"}}, http.StatusBadRequest},
		{"connection upgrade", http.MethodGet, route, http.Header{"Connection": {"keep-alive, Upgrade"}}, http.StatusBadRequest},
		{"malformed", http.MethodGet, "/environments/x/forward/", nil, http.StatusNotFound},
		{"dot dot", http.MethodGet, route + "..", nil, http.StatusNotFound},
	}
	for _, test := range tests {
		r := httptest.NewRequest(test.method, test.target, nil)
		for k, v := range test.header {
			r.Header[k] = v
		}
		w := httptest.NewRecorder()
		serveEnvironmentForward(nil, w, r)
		if w.Code != test.code || w.Header().Get("Location") != "" {
			t.Errorf("%s: status %d location %q, want %d and none", test.name, w.Code, w.Header().Get("Location"), test.code)
		}
	}
}

func TestForwardError(t *testing.T) {
	secret := "profile kube-secret-binding"
	tests := []struct {
		err  error
		code int
	}{
		{auth.ErrUnauthenticated, http.StatusUnauthorized},
		{auth.ErrPermissionDenied, http.StatusForbidden},
		{auth.ErrUnavailable, http.StatusServiceUnavailable},
		{store.ErrNotFound, http.StatusNotFound},
		{environmentcontrol.ErrInvalid, http.StatusNotFound},
		{environmentcontrol.ErrNotReady, http.StatusConflict},
		{errors.Join(environmentcontrol.ErrClosed, errors.New(secret)), http.StatusConflict},
		{environmentcontrol.ErrNoPort, http.StatusConflict},
		{environmentcontrol.ErrSessionLimit, http.StatusTooManyRequests},
		{errors.New(secret), http.StatusServiceUnavailable},
	}
	for _, test := range tests {
		code, text := forwardError(test.err)
		if code != test.code || strings.Contains(text, secret) {
			t.Errorf("forwardError(%v) = %d %q, want %d without the error's text", test.err, code, text, test.code)
		}
	}
}
