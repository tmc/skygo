package rpc

import (
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/environmentcontrol"
	"github.com/tmc/skygo/internal/store"
)

// forwardPrefix starts the route /environments/<id>/forward/<application-path>,
// the authenticated HTTP sibling of EnvironmentService.
const forwardPrefix = "/environments/"

// serveEnvironmentForward relays r to the declared port of the environment
// its path names, as r's checked actor, through c.Forward. It runs in the
// handler's authenticated lifetime, not its mux, so no path is cleaned or
// redirected. Only the route prefix is removed; the application path keeps
// its escaping and the query is kept as sent. The destination is never taken
// from r: Controller.Forward and its runtime use the environment's fixed
// Spec.Port.
func serveEnvironmentForward(c *environmentcontrol.Controller, w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect || r.Header.Get("Upgrade") != "" || headerHasToken(r.Header, "Connection", "upgrade") {
		http.Error(w, "environment forward refuses tunnels", http.StatusBadRequest)
		return
	}
	id, app, ok := forwardTarget(r.URL)
	if !ok {
		http.Error(w, "invalid environment forward path", http.StatusNotFound)
		return
	}
	out := r.Clone(r.Context())
	out.URL = app
	out.RequestURI = app.RequestURI()
	// The bearer authenticated this server, not the workload.
	out.Header.Del("Authorization")
	out.Header.Del("Proxy-Authorization")
	if _, err := c.Forward(w, out, requestActor(r.Context()), id); err != nil {
		code, text := forwardError(err)
		http.Error(w, text, code)
	}
}

// forwardTarget returns the environment ID and application URL of a forward
// route, or false for any other path. It reads the escaped path, so an
// escaped slash stays inside its segment. The ID is 32 lowercase hexadecimal
// digits and the route ends in "/forward/"; the application path is the rest
// with its leading slash, and "/" when empty. A segment that does not
// unescape, unescapes to "." or "..", or holds NUL is refused rather than
// cleaned.
func forwardTarget(u *url.URL) (string, *url.URL, bool) {
	rest, ok := strings.CutPrefix(u.EscapedPath(), forwardPrefix)
	if !ok {
		return "", nil, false
	}
	id, rest, ok := strings.Cut(rest, "/")
	if !ok || !environment.ValidID(id) {
		return "", nil, false
	}
	rest, ok = strings.CutPrefix(rest, "forward/")
	if !ok {
		return "", nil, false
	}
	escaped := "/" + rest
	for _, seg := range strings.Split(rest, "/") {
		s, err := url.PathUnescape(seg)
		if err != nil || s == "." || s == ".." || strings.IndexByte(s, 0) >= 0 {
			return "", nil, false
		}
	}
	path, err := url.PathUnescape(escaped)
	if err != nil {
		return "", nil, false
	}
	app := &url.URL{Path: path, RawQuery: u.RawQuery}
	if app.EscapedPath() != escaped {
		app.RawPath = escaped
	}
	return id, app, true
}

// headerHasToken reports whether a comma-separated header of h names token,
// ignoring case.
func headerHasToken(h http.Header, name, token string) bool {
	for _, v := range h.Values(name) {
		for _, t := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(t), token) {
				return true
			}
		}
	}
	return false
}

// forwardError maps an error of Controller.Forward, which wrote nothing, to
// its status and fixed text. An error of no known class relayed nothing.
func forwardError(err error) (int, string) {
	switch {
	case errors.Is(err, auth.ErrUnauthenticated):
		return http.StatusUnauthorized, "unauthenticated"
	case errors.Is(err, auth.ErrPermissionDenied):
		return http.StatusForbidden, "permission denied"
	case errors.Is(err, auth.ErrUnavailable):
		return http.StatusServiceUnavailable, "authentication unavailable"
	case errors.Is(err, environmentcontrol.ErrInvalid), errors.Is(err, store.ErrNotFound):
		return http.StatusNotFound, "environment not found"
	case errors.Is(err, environmentcontrol.ErrNotReady):
		return http.StatusConflict, environmentcontrol.ErrNotReady.Error()
	case errors.Is(err, environmentcontrol.ErrClosed):
		return http.StatusConflict, environmentcontrol.ErrClosed.Error()
	case errors.Is(err, environmentcontrol.ErrNoPort):
		return http.StatusConflict, environmentcontrol.ErrNoPort.Error()
	case errors.Is(err, environmentcontrol.ErrSessionLimit):
		return http.StatusTooManyRequests, environmentcontrol.ErrSessionLimit.Error()
	}
	return http.StatusServiceUnavailable, "environment forward not started"
}

// forwardRoute reports whether r's decoded path is under forwardPrefix. Such a
// request is served by serveEnvironmentForward, which refuses it unless its
// escaped path is a forward route too.
func forwardRoute(r *http.Request) bool {
	return strings.HasPrefix(r.URL.Path, forwardPrefix)
}
