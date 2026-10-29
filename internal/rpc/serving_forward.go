package rpc

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/serving"
	"github.com/tmc/skygo/internal/servingcontrol"
	"github.com/tmc/skygo/internal/store"
)

// servicePrefix starts the route /services/<id>/forward/<application-path>,
// the authenticated HTTP sibling of ServingService.
const servicePrefix = "/services/"

const (
	// maxServiceForwardBody bounds a forwarded request body.
	maxServiceForwardBody = 8 << 20
	// maxServiceForwardTime bounds one forwarded exchange.
	maxServiceForwardTime = 5 * time.Minute
)

// serviceRoute reports whether r's decoded path is under servicePrefix. Such
// a request is served by serveServiceForward, which refuses it unless its
// escaped path is a forward route too.
func serviceRoute(r *http.Request) bool {
	return strings.HasPrefix(r.URL.Path, servicePrefix)
}

// serveServiceForward relays r to the service its path names, as r's
// checked actor, through c.Forward. Like the environment route it runs in
// the handler's authenticated lifetime, outside the mux, and takes no
// destination from r. It removes the headers serving.ForwardHeader removes
// and the hop-by-hop headers, refuses
// upgrades, bounds the body and the exchange's time, and closes the request
// body once the runtime returns. Only the service's own principal reaches
// the runtime: Controller.Forward checks ownership and readiness first.
func serveServiceForward(c *servingcontrol.Controller, w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect || r.Header.Get("Upgrade") != "" || headerHasToken(r.Header, "Connection", "upgrade") {
		http.Error(w, "service forward refuses tunnels", http.StatusBadRequest)
		return
	}
	id, app, ok := appTarget(r.URL, servicePrefix, store.ValidServiceID)
	if !ok {
		http.Error(w, "invalid service forward path", http.StatusNotFound)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), maxServiceForwardTime)
	defer cancel()
	out := r.Clone(ctx)
	out.URL = app
	out.RequestURI = app.RequestURI()
	out.Host = ""
	// The bearer, cookies, proofs and impersonation fields belong to the
	// control plane, not the workload. The runtime removes them again as it
	// writes the workload's request.
	out.Header = serving.ForwardHeader(out.Header)
	removeHopHeaders(out.Header)
	body := &trackedBody{ReadCloser: http.MaxBytesReader(w, r.Body, maxServiceForwardBody)}
	out.Body = body
	tw := &trackedWriter{ResponseWriter: w}
	err := c.Forward(requestActor(r.Context()), id, tw, out)
	body.Close()
	if err != nil && !tw.wrote() {
		code, text := serviceForwardError(err)
		http.Error(w, text, code)
	}
}

// removeHopHeaders removes the hop-by-hop headers of h, including those
// named by Connection.
func removeHopHeaders(h http.Header) {
	for _, v := range h.Values("Connection") {
		for _, name := range strings.Split(v, ",") {
			if name = strings.TrimSpace(name); name != "" {
				h.Del(name)
			}
		}
	}
	for _, name := range []string{"Connection", "Keep-Alive", "Proxy-Connection", "TE", "Trailer", "Transfer-Encoding", "Upgrade"} {
		h.Del(name)
	}
}

// serviceForwardError maps an error of Controller.Forward that wrote no
// response to its status and fixed text.
func serviceForwardError(err error) (int, string) {
	switch {
	case errors.Is(err, auth.ErrUnauthenticated):
		return http.StatusUnauthorized, "unauthenticated"
	case errors.Is(err, auth.ErrPermissionDenied):
		return http.StatusForbidden, "permission denied"
	case errors.Is(err, auth.ErrUnavailable):
		return http.StatusServiceUnavailable, "authentication unavailable"
	case errors.Is(err, store.ErrNotFound):
		return http.StatusNotFound, "service not found"
	case errors.Is(err, servingcontrol.ErrNotReady):
		return http.StatusConflict, servingcontrol.ErrNotReady.Error()
	case errors.Is(err, servingcontrol.ErrClosed):
		return http.StatusServiceUnavailable, servingcontrol.ErrClosed.Error()
	case errors.Is(err, servingcontrol.ErrForwardLimit):
		return http.StatusTooManyRequests, servingcontrol.ErrForwardLimit.Error()
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout, "service forward timed out"
	}
	return http.StatusBadGateway, "service forward failed"
}

// appTarget returns the ID and application URL of a forward route under
// prefix, or false for any other path, by the rules of forwardTarget.
func appTarget(u *url.URL, prefix string, valid func(string) bool) (string, *url.URL, bool) {
	rest, ok := strings.CutPrefix(u.EscapedPath(), prefix)
	if !ok {
		return "", nil, false
	}
	id, rest, ok := strings.Cut(rest, "/")
	if !ok || !valid(id) {
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

// trackedBody closes its body exactly once, whether the runtime or the
// route closes it first.
type trackedBody struct {
	io.ReadCloser
	once sync.Once
	err  error
}

func (b *trackedBody) Close() error {
	b.once.Do(func() {
		b.err = b.ReadCloser.Close()
	})
	return b.err
}

// trackedWriter records whether a response was started.
type trackedWriter struct {
	http.ResponseWriter
	mu      sync.Mutex
	started bool
}

func (w *trackedWriter) WriteHeader(code int) {
	w.mark()
	w.ResponseWriter.WriteHeader(code)
}

func (w *trackedWriter) Write(p []byte) (int, error) {
	w.mark()
	return w.ResponseWriter.Write(p)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (w *trackedWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *trackedWriter) mark() {
	w.mu.Lock()
	w.started = true
	w.mu.Unlock()
}

func (w *trackedWriter) wrote() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.started
}
