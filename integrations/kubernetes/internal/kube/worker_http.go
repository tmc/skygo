package kube

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// workerHTTP owns only the worker client's exchanges. The borrowed Backend's
// bounded data frames remain the authority for member operation ownership.
// Its client never replaces or changes Backend.client.
type workerHTTP struct {
	b         *Backend
	client    *kubernetes.Clientset
	transport *workerHTTPTransport
	idle      *http.Transport

	mu      sync.Mutex
	stopped bool
	changed chan struct{}
	// Membership is reserved before any native effect, so capacity cannot
	// appear for the first time at cleanup. True closes new log admission.
	// Entries are never evicted or cleared during this client's lifetime.
	members map[string]bool
}

// newWorkerHTTP accepts only transports whose physical request path this
// profile controls. Opaque wrappers, credential plugins and file refresh are
// unqualified, not an excuse to fall back to the target's shared client.
func newWorkerHTTP(b *Backend, cfg *rest.Config) (*workerHTTP, error) {
	if b == nil || cfg == nil {
		return nil, errors.New("worker HTTP requires a backend and frozen configuration")
	}
	if cfg.Transport != nil || cfg.WrapTransport != nil || cfg.Dial != nil || cfg.Proxy != nil || cfg.AuthProvider != nil || cfg.ExecProvider != nil || cfg.BearerTokenFile != "" || cfg.CAFile != "" || cfg.CertFile != "" || cfg.KeyFile != "" {
		return nil, errors.New("worker HTTP transport or credential refresh is unqualified")
	}
	c := rest.CopyConfig(cfg)
	c.NextProtos = []string{"http/1.1"}
	tc, err := rest.TLSConfigFor(c)
	if err != nil {
		return nil, err
	}
	if tc != nil {
		tc = tc.Clone()
		tc.NextProtos = []string{"http/1.1"}
	}
	base := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: -1}).DialContext,
		TLSClientConfig:       tc,
		TLSNextProto:          make(map[string]func(string, *tls.Conn) http.RoundTripper),
		ForceAttemptHTTP2:     false,
		DisableKeepAlives:     true,
		DisableCompression:    true,
		MaxConnsPerHost:       2,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}
	wrapped, err := rest.HTTPWrappersForConfig(c, base)
	if err != nil {
		base.CloseIdleConnections()
		return nil, err
	}
	h := &workerHTTP{b: b, idle: base, changed: make(chan struct{}), members: make(map[string]bool)}
	h.transport = &workerHTTPTransport{h: h, base: wrapped}
	hc := &http.Client{Transport: h.transport, Timeout: c.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	h.client, err = kubernetes.NewForConfigAndClient(c, hc)
	if err != nil {
		base.CloseIdleConnections()
		return nil, err
	}
	return h, nil
}

// errWorkerHTTPAdmission means this new member acquired no native call.
// Only this definite pre-effect refusal can qualify a zero-result Create.
var errWorkerHTTPAdmission = errors.New("worker HTTP member capacity exhausted")

type workerHTTPCallKey struct{}
type workerHTTPAttemptKey struct{}

type workerHTTPCall struct {
	h   *workerHTTP
	key string
	f   *dataFrame

	mu       sync.Mutex
	cause    error
	io       int
	poisoned bool
	ended    bool
	finished sync.Once
	result   error
}

type workerHTTPAttempt struct {
	mu   sync.Mutex
	used bool
}

// begin uses the same immutable member key for logs and effects. Retirement
// takes an exclusive claim, excluding both active and held original readers.
func (h *workerHTTP) begin(ctx context.Context, key string, exclusive, logs bool) (context.Context, *workerHTTPCall, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stopped {
		return nil, nil, errors.New("worker HTTP is stopped")
	}
	if !strings.HasPrefix(key, "worker/") || len(key) > 256 {
		return nil, nil, errors.New("invalid worker HTTP member key")
	}
	retiring, member := h.members[key]
	if !member {
		if len(h.members) >= maxRetryReceipts {
			return nil, nil, errWorkerHTTPAdmission
		}
		if h.members == nil {
			h.members = make(map[string]bool)
		}
		h.members[key] = false
	}
	if logs && retiring {
		return nil, nil, errors.New("worker member logs are retired")
	}
	ctx, f, err := h.b.beginData(ctx, key, exclusive, logs)
	if err != nil {
		for _, old := range h.frames(key) {
			old.mu.Lock()
			held := old.held || len(old.failed) != 0
			old.mu.Unlock()
			if held {
				return nil, nil, h.ownerError(old, err)
			}
		}
		return nil, nil, err
	}
	if exclusive {
		// h.mu also guards log admission, and the exclusive claim has
		// already excluded every active or held original of this member.
		h.members[key] = true
	}
	call := &workerHTTPCall{h: h, key: key, f: f}
	f.mu.Lock()
	f.worker = call
	f.mu.Unlock()
	return context.WithValue(ctx, workerHTTPCallKey{}, call), call, nil
}

// request authorizes one physical transport call for one explicit REST
// request. SDK retries reuse this context and therefore cannot send again.
func (h *workerHTTP) request(ctx context.Context) context.Context {
	return context.WithValue(ctx, workerHTTPAttemptKey{}, new(workerHTTPAttempt))
}

func (h *workerHTTP) check(ctx context.Context) error {
	call, _ := ctx.Value(workerHTTPCallKey{}).(*workerHTTPCall)
	if call == nil || call.h != h {
		return errors.New("worker HTTP request has no original call")
	}
	call.mu.Lock()
	cause := call.cause
	poisoned := call.poisoned
	if call.io != 0 {
		call.poisoned, poisoned = true, true
		call.f.mu.Lock()
		call.f.held = true
		call.f.mu.Unlock()
	}
	call.mu.Unlock()
	call.f.mu.Lock()
	clean := !call.f.held && !call.f.ended && len(call.f.open) == 0 && len(call.f.failed) == 0
	call.f.mu.Unlock()
	if !clean || cause != nil || poisoned {
		return h.ownerError(call.f, nil)
	}
	for _, other := range h.frames(call.key) {
		if other == call.f {
			continue
		}
		other.mu.Lock()
		held := other.held || len(other.failed) != 0
		other.mu.Unlock()
		if held {
			return h.ownerError(other, nil)
		}
	}
	h.mu.Lock()
	stopped := h.stopped
	h.mu.Unlock()
	if stopped {
		return errors.New("worker HTTP is stopped")
	}
	return nil
}

// finish is called only after the original SDK operation returns. Uncertain
// bodies and transport errors retain the same frame and member claim.
func (c *workerHTTPCall) finish(err error) error {
	c.finished.Do(func() {
		// Serialize the clean end with raw fault publication and I/O admission.
		// A body whose inner Close returned may still have an original Read
		// or wrapper Close active; neither its frame nor claim is released.
		c.mu.Lock()
		c.f.mu.Lock()
		held := c.f.held
		c.f.mu.Unlock()
		clean := !c.poisoned && c.cause == nil && c.io == 0 && !held
		if clean {
			clean = c.h.b.endData(c.key, c.f)
		}
		c.ended = true
		if !clean {
			c.poisoned = true
			c.f.mu.Lock()
			c.f.ended, c.f.held = true, true
			c.f.mu.Unlock()
		}
		c.mu.Unlock()
		if clean {
			c.result = err
		} else {
			c.result = c.h.ownerError(c.f, err)
		}
		c.h.notify()
	})
	return c.result
}

// startIO counts an actual original before it can be read or closed. A clean
// completed call cannot admit new raw I/O. A held original may still finish;
// doing so never clears its poison or reopens the member.
func (c *workerHTTPCall) startIO() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ended && !c.poisoned {
		return errors.New("worker HTTP original already ended")
	}
	c.io++
	return nil
}

func (c *workerHTTPCall) endIO() {
	c.mu.Lock()
	c.io--
	c.mu.Unlock()
	c.h.notify()
}

// record latches the shared frame before returning a raw failure or waiting
// on a later Close. Lock order is call then frame; readers release frame.mu
// before acquiring the call to snapshot the original cause.
func (c *workerHTTPCall) record(err error) {
	if err == nil {
		return
	}
	c.mu.Lock()
	c.cause = errors.Join(c.cause, err)
	c.poisoned = true
	c.f.mu.Lock()
	c.f.held = true
	c.f.mu.Unlock()
	c.mu.Unlock()
	c.h.notify()
}

// ownerError snapshots only original raw causes, never call.result. In
// particular a different frame's admission failure must not make an error
// chain refer back to its caller through a stored result.
func (h *workerHTTP) ownerError(f *dataFrame, extra error) error {
	f.mu.Lock()
	original := f.worker
	f.mu.Unlock()
	var cause error
	if original != nil {
		original.mu.Lock()
		cause = original.cause
		original.mu.Unlock()
	}
	return &workerHTTPError{h: h, frame: f, call: original, cause: errors.Join(extra, cause)}
}

// workerHTTPError keeps actual original bodies, their first Close causes and
// the client/Backend alive. Its fixed text does not print API response bytes.
type workerHTTPError struct {
	h     *workerHTTP
	frame *dataFrame
	call  *workerHTTPCall
	cause error
}

func (e *workerHTTPError) Error() string { return "kubernetes worker HTTP ownership unsettled" }
func (e *workerHTTPError) Unwrap() []error {
	out := []error{backend.ErrWorkerUnsettled}
	if e.cause != nil {
		out = append(out, e.cause)
	}
	// A held request original may publish its raw failure after finish.
	// Follow only raw causes, never result, to retain it without a cycle.
	if e.call != nil {
		e.call.mu.Lock()
		cause := e.call.cause
		e.call.mu.Unlock()
		if cause != nil {
			out = append(out, cause)
		}
	}
	if e.frame != nil {
		e.frame.mu.Lock()
		for _, b := range e.frame.failed {
			out = append(out, b.err)
		}
		e.frame.mu.Unlock()
	}
	return out
}

type workerHTTPTransport struct {
	h    *workerHTTP
	base http.RoundTripper
}

// RoundTrip refuses hidden retries before the base transport. A fresh
// non-reusing HTTP/1 transport and one-shot request bodies also prevent the
// standard transport's connection-reuse/body replay path.
func (t *workerHTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	call, _ := req.Context().Value(workerHTTPCallKey{}).(*workerHTTPCall)
	attempt, _ := req.Context().Value(workerHTTPAttemptKey{}).(*workerHTTPAttempt)
	if call == nil || call.h != t.h || attempt == nil {
		return nil, errors.New("worker HTTP request has no one-shot authorization")
	}
	if err := t.h.check(req.Context()); err != nil {
		return nil, err
	}
	attempt.mu.Lock()
	used := attempt.used
	attempt.used = true
	attempt.mu.Unlock()
	if used {
		return nil, errors.New("worker HTTP request replay refused")
	}
	r := req.Clone(req.Context())
	r.GetBody = nil
	r.Close = true
	r.Header.Del("Idempotency-Key")
	r.Header.Del("X-Idempotency-Key")
	if r.Body != nil && r.Body != http.NoBody {
		r.Body = &workerResponseBody{ReadCloser: workerRegister(call.f, r.Body), call: call}
	}
	if err := call.startIO(); err != nil {
		return nil, err
	}
	defer call.endIO()
	resp, err := t.base.RoundTrip(r)
	if resp == nil && err == nil {
		err = errors.New("worker transport returned no response")
	}
	// Record before a response's original Close can block. A concurrent
	// same-member call must observe the failure immediately, not at finish.
	call.record(err)
	if resp != nil && resp.Body != nil {
		// A request body's Close may already have failed. Still retain the
		// same response original; the next request is fenced by check.
		body := workerRegister(call.f, resp.Body)
		resp.Body = &workerResponseBody{ReadCloser: &limitedBody{dataBody: body, n: backend.MaxLogBytes + 1}, call: call}
		if err != nil {
			// net/http does not hand a response returned alongside an error
			// to the SDK. This call therefore owns its one synchronous Close.
			err = errors.Join(err, resp.Body.Close())
			resp = nil
		}
	}
	return resp, err
}

type workerResponseBody struct {
	io.ReadCloser
	call *workerHTTPCall

	mu      sync.Mutex
	closing bool
	close   sync.Once
	err     error
}

func (b *workerResponseBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	if b.closing {
		b.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	err := b.call.startIO()
	b.mu.Unlock()
	if err != nil {
		return 0, err
	}
	defer b.call.endIO()
	n, err := b.ReadCloser.Read(p)
	if err != nil && err != io.EOF {
		b.call.record(err)
	}
	return n, err
}

func (b *workerResponseBody) Close() error {
	b.close.Do(func() {
		b.mu.Lock()
		b.closing = true
		err := b.call.startIO()
		b.mu.Unlock()
		if err != nil {
			b.err = err
			return
		}
		defer b.call.endIO()
		b.err = b.ReadCloser.Close()
		if b.err != nil {
			b.call.record(b.err)
		}
	})
	return b.err
}

// workerRegister also retains a response when an earlier request-body Close
// failed in this same original RoundTrip. No next request may enter that frame.
func workerRegister(f *dataFrame, body io.ReadCloser) *dataBody {
	f.mu.Lock()
	defer f.mu.Unlock()
	d := &dataBody{ReadCloser: body, frame: f, limit: f.limit}
	f.open = append(f.open, d)
	return d
}

func (h *workerHTTP) frames(key string) []*dataFrame {
	h.b.mu.Lock()
	defer h.b.mu.Unlock()
	var out []*dataFrame
	for k, frames := range h.b.dataFrames {
		if key != "" && k != key || key == "" && !strings.HasPrefix(k, "worker/") {
			continue
		}
		out = append(out, frames...)
	}
	return out
}

func (h *workerHTTP) notify() {
	h.mu.Lock()
	close(h.changed)
	h.changed = make(chan struct{})
	h.mu.Unlock()
}

func (h *workerHTTP) fence() {
	h.mu.Lock()
	h.stopped = true
	close(h.changed)
	h.changed = make(chan struct{})
	h.mu.Unlock()
}

// join never closes an original response body. Its Close belongs to the
// original SDK call, which must actually return before its frame disappears.
func (h *workerHTTP) join(ctx context.Context) error {
	for {
		h.mu.Lock()
		changed := h.changed
		h.mu.Unlock()
		frames := h.frames("")
		if len(frames) == 0 {
			if h.idle != nil {
				h.idle.CloseIdleConnections()
			}
			return nil
		}
		for _, f := range frames {
			f.mu.Lock()
			held := f.held || len(f.failed) != 0
			f.mu.Unlock()
			if held {
				return h.ownerError(f, nil)
			}
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return h.ownerError(frames[0], ctx.Err())
		}
	}
}
