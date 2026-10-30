package kube

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
)

// API bodies below are MODELED. The real client-go REST decoder, HTTP client,
// member ownership and original Read/Close calls are exercised in software.
type workerTestBody struct {
	io.Reader
	readErr, closeErr error
	entered, release  chan struct{}
	closes            atomic.Int32
}

func (b *workerTestBody) Read(p []byte) (int, error) {
	if b.readErr != nil {
		return 0, b.readErr
	}
	return b.Reader.Read(p)
}
func (b *workerTestBody) Close() error {
	b.closes.Add(1)
	if b.entered != nil {
		close(b.entered)
		<-b.release
	}
	return b.closeErr
}

type workerTestTransport struct {
	body         *workerTestBody
	status       int
	calls        atomic.Int32
	err          error
	withResponse bool
}

func (r *workerTestTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r.calls.Add(1)
	if req.GetBody != nil || !req.Close {
		return nil, errors.New("request permits replay or connection reuse")
	}
	if req.Body != nil {
		_, _ = io.Copy(io.Discard, req.Body)
		_ = req.Body.Close()
	}
	if r.err != nil && !r.withResponse {
		return nil, r.err
	}
	return &http.Response{StatusCode: r.status, Header: http.Header{"Content-Type": {"application/json"}, "Retry-After": {"0"}}, Body: r.body, Request: req}, r.err
}

func workerHTTPFixture(t *testing.T, body *workerTestBody) (*workerHTTP, *workerTestTransport) {
	t.Helper()
	h, err := newWorkerHTTP(new(Backend), &rest.Config{Host: "http://worker.invalid"})
	if err != nil {
		t.Fatal(err)
	}
	// Replace this instance's base before its first call. This is a controlled
	// software transport, not production admission of opaque custom wrappers.
	r := &workerTestTransport{body: body, status: http.StatusOK}
	h.transport.base = r
	return h, r
}

func workerHTTPPod() *workerTestBody {
	return &workerTestBody{Reader: strings.NewReader(`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"rank","uid":"original"}}`)}
}

func workerHTTPRead(h *workerHTTP, ctx context.Context) error {
	var pod corev1.Pod
	return h.client.CoreV1().RESTClient().Get().Resource("pods").Name("rank").MaxRetries(0).Do(h.request(ctx)).Into(&pod)
}

func TestWorkerHTTPOriginalClose(t *testing.T) {
	for _, readFailure := range []bool{false, true} {
		name := "close"
		if readFailure {
			name = "read_and_close"
		}
		t.Run(name, func(t *testing.T) {
			body := workerHTTPPod()
			body.closeErr = errors.New("original close failed")
			if readFailure {
				body.readErr = errors.New("original read failed")
			}
			h, transport := workerHTTPFixture(t, body)
			ctx, call, err := h.begin(context.Background(), "worker/job/attempt/0", false, false)
			if err != nil {
				t.Fatal(err)
			}
			nativeErr := workerHTTPRead(h, ctx)
			if !readFailure && nativeErr != nil {
				t.Fatal("decoder should discard Close failure", nativeErr)
			}
			check := h.check(ctx)
			err = call.finish(nativeErr)
			if !errors.Is(check, backend.ErrWorkerUnsettled) || !errors.Is(err, backend.ErrWorkerUnsettled) || !errors.Is(err, body.closeErr) || readFailure && !errors.Is(err, body.readErr) {
				t.Fatal("lost original cause or marker", check, err)
			}
			var owned *workerHTTPError
			if !errors.As(err, &owned) || owned.frame != call.f || owned.call != call || owned.h.b != h.b {
				t.Fatal("error lost original frame/backend")
			}
			if err := workerHTTPRead(h, ctx); !errors.Is(err, backend.ErrWorkerUnsettled) || transport.calls.Load() != 1 {
				t.Fatal("sticky failed Close allowed a later physical request", err)
			}
			if _, _, err := h.begin(context.Background(), "worker/job/attempt/0", true, false); !errors.Is(err, backend.ErrWorkerUnsettled) {
				t.Fatal("held reader admitted retirement", err)
			}
			h.fence()
			if err := h.join(context.Background()); !errors.Is(err, body.closeErr) {
				t.Fatal("join released failed original", err)
			}
			_ = call.finish(nil)
			if body.closes.Load() != 1 {
				t.Fatal("original Close repeated")
			}
		})
	}
}

func TestWorkerHTTPBlockedOriginalClose(t *testing.T) {
	body := workerHTTPPod()
	body.entered, body.release = make(chan struct{}), make(chan struct{})
	h, transport := workerHTTPFixture(t, body)
	ctx, call, err := h.begin(context.Background(), "worker/job/attempt/0", false, true)
	if err != nil {
		t.Fatal(err)
	}
	returned := make(chan struct{})
	var originalErr error
	go func() {
		originalErr = call.finish(workerHTTPRead(h, ctx))
		close(returned)
	}()
	var release sync.Once
	join := func() {
		release.Do(func() { close(body.release) })
		select {
		case <-returned:
		case <-time.After(10 * time.Second):
			t.Error("original did not return after its Close release")
		}
	}
	t.Cleanup(join)
	select {
	case <-body.entered:
	case <-returned:
		t.Fatal("original returned before its Close blocked")
	case <-time.After(10 * time.Second):
		t.Fatal("original Close did not enter")
	}
	if _, _, err := h.begin(context.Background(), "worker/job/attempt/0", true, false); err == nil || transport.calls.Load() != 1 {
		t.Fatal("active original log admitted retirement", err)
	}
	wait, cancel := context.WithCancel(context.Background())
	cancel()
	if err := h.join(wait); !errors.Is(err, backend.ErrWorkerUnsettled) || !errors.Is(err, context.Canceled) {
		t.Fatal("deadline reported a successful join", err)
	}
	join()
	if originalErr != nil || body.closes.Load() != 1 {
		t.Fatal("released original did not positively close once", originalErr)
	}
	h.fence()
	if err := h.join(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerHTTPRequestReplay(t *testing.T) {
	body := workerHTTPPod()
	h, transport := workerHTTPFixture(t, body)
	ctx, call, err := h.begin(context.Background(), "worker/job/attempt/0", false, false)
	if err != nil {
		t.Fatal(err)
	}
	request := h.request(ctx)
	var pod corev1.Pod
	get := func() error {
		return h.client.CoreV1().RESTClient().Get().Resource("pods").Name("rank").MaxRetries(0).Do(request).Into(&pod)
	}
	if err := get(); err != nil {
		t.Fatal(err)
	}
	if err := get(); err == nil || transport.calls.Load() != 1 {
		t.Fatal("same request context sent twice", err)
	}
	if err := call.finish(nil); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerHTTPTransportCause(t *testing.T) {
	h, transport := workerHTTPFixture(t, workerHTTPPod())
	transport.err = errors.New("original transport failed")
	ctx, call, err := h.begin(context.Background(), "worker/job/attempt/0", false, false)
	if err != nil {
		t.Fatal(err)
	}
	err = call.finish(workerHTTPRead(h, ctx))
	if !errors.Is(err, transport.err) || !errors.Is(err, backend.ErrWorkerUnsettled) {
		t.Fatal("transport cause/owner was released", err)
	}
}

func TestWorkerHTTPConstructorRefusesOpaque(t *testing.T) {
	for _, name := range []string{"transport", "wrapper", "token_file", "cert_file", "key_file"} {
		t.Run(name, func(t *testing.T) {
			cfg := &rest.Config{Host: "http://worker.invalid"}
			switch name {
			case "transport":
				cfg.Transport = new(workerTestTransport)
			case "wrapper":
				cfg.WrapTransport = func(r http.RoundTripper) http.RoundTripper { return r }
			case "token_file":
				cfg.BearerTokenFile = "/unread"
			case "cert_file":
				cfg.CertFile = "/unread"
			case "key_file":
				cfg.KeyFile = "/unread"
			}
			if _, err := newWorkerHTTP(new(Backend), cfg); err == nil {
				t.Fatal("unqualified transport was accepted")
			}
		})
	}
}

func TestWorkerHTTPRetryAfterCannotResend(t *testing.T) {
	h, transport := workerHTTPFixture(t, workerHTTPPod())
	transport.status = http.StatusServiceUnavailable
	ctx, call, err := h.begin(context.Background(), "worker/job/attempt/0", false, false)
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately permit an SDK retry. Its same authorization must still
	// permit only the first physical POST, never a later misleading409.
	err = h.client.CoreV1().RESTClient().Post().Resource("pods").Body(&corev1.Pod{}).MaxRetries(1).Do(h.request(ctx)).Error()
	if err == nil || transport.calls.Load() != 1 {
		t.Fatal("Retry-After sent a second original POST", err, transport.calls.Load())
	}
	_ = call.finish(err)
}

func TestWorkerHTTPBothBodyOwners(t *testing.T) {
	response := workerHTTPPod()
	response.closeErr = errors.New("response Close failed")
	h, transport := workerHTTPFixture(t, response)
	ctx, call, err := h.begin(context.Background(), "worker/job/attempt/0", false, false)
	if err != nil {
		t.Fatal(err)
	}
	request := &workerTestBody{Reader: strings.NewReader("body"), closeErr: errors.New("request Close failed")}
	req, err := http.NewRequestWithContext(h.request(ctx), http.MethodPost, "http://worker.invalid/pods", request)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := h.transport.RoundTrip(req)
	if err != nil || resp == nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	err = call.finish(nil)
	if !errors.Is(err, request.closeErr) || !errors.Is(err, response.closeErr) || !errors.Is(err, backend.ErrWorkerUnsettled) {
		t.Fatal("lost one of the original body owners", err)
	}
	if request.closes.Load() != 1 || response.closes.Load() != 1 || transport.calls.Load() != 1 {
		t.Fatal("body original repeated")
	}
}

func TestWorkerHTTPRedirectCannotResend(t *testing.T) {
	var original, destination atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/destination" {
			destination.Add(1)
			w.WriteHeader(http.StatusConflict)
			return
		}
		original.Add(1)
		w.Header().Set("Location", "/destination")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer api.Close()
	h, err := newWorkerHTTP(new(Backend), &rest.Config{Host: api.URL})
	if err != nil {
		t.Fatal(err)
	}
	ctx, call, err := h.begin(context.Background(), "worker/job/attempt/0", false, false)
	if err != nil {
		t.Fatal(err)
	}
	err = h.client.CoreV1().RESTClient().Post().Resource("pods").Body(&corev1.Pod{}).MaxRetries(0).Do(h.request(ctx)).Error()
	if err == nil || original.Load() != 1 || destination.Load() != 0 {
		t.Fatal("redirect replayed original POST", err, original.Load(), destination.Load())
	}
	if closeErr := h.check(ctx); closeErr != nil {
		t.Fatal("original redirect response was not closed", closeErr)
	}
	_ = call.finish(err)
	h.fence()
	if err := h.join(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerHTTPRetirementLogFence(t *testing.T) {
	h, transport := workerHTTPFixture(t, workerHTTPPod())
	const key = "worker/job/attempt/0"
	_, removal, err := h.begin(context.Background(), key, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.begin(context.Background(), key, false, true); err == nil {
		t.Fatal("new log slipped into active retirement")
	}
	if err := removal.finish(nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.begin(context.Background(), key, false, true); err == nil || transport.calls.Load() != 0 {
		t.Fatal("settled retirement readmitted a new log")
	}
	// Unknown native retirement is independent of local settlement. The
	// admission fence does not prohibit qualified later non-log recovery.
	_, recovery, err := h.begin(context.Background(), key, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovery.finish(nil); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerHTTPConcurrentLogRetirement(t *testing.T) {
	for range 32 {
		h, transport := workerHTTPFixture(t, workerHTTPPod())
		start := make(chan struct{})
		var log, removal *workerHTTPCall
		var logErr, removeErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, log, logErr = h.begin(context.Background(), "worker/job/attempt/0", false, true)
		}()
		go func() {
			defer wg.Done()
			<-start
			_, removal, removeErr = h.begin(context.Background(), "worker/job/attempt/0", true, false)
		}()
		close(start)
		wg.Wait()
		if (logErr == nil) == (removeErr == nil) {
			t.Fatal("log and exclusive retirement did not exclude each other", logErr, removeErr)
		}
		if log != nil {
			_ = log.finish(nil)
			_, laterLog, err := h.begin(context.Background(), "worker/job/attempt/0", false, true)
			if err != nil {
				t.Fatal("failed exclusive acquisition changed log admission", err)
			}
			_ = laterLog.finish(nil)
			_, removal, removeErr = h.begin(context.Background(), "worker/job/attempt/0", true, false)
			if removeErr != nil {
				t.Fatal("failed exclusive acquisition incorrectly latched logs", removeErr)
			}
		}
		_ = removal.finish(nil)
		if _, _, err := h.begin(context.Background(), "worker/job/attempt/0", false, true); err == nil || transport.calls.Load() != 0 {
			t.Fatal("retirement latch cleared after claim release", err)
		}
	}
}

func TestWorkerHTTPMembershipCapacityBeforeEffects(t *testing.T) {
	h, transport := workerHTTPFixture(t, workerHTTPPod())
	// MODELED member keys exercise real bounded admission, without any API.
	for i := range maxRetryReceipts {
		_, call, err := h.begin(context.Background(), fmt.Sprintf("worker/job/attempt/%d", i), false, false)
		if err != nil {
			t.Fatal(err)
		}
		if err := call.finish(nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := h.begin(context.Background(), "worker/new/attempt/0", false, false); !errors.Is(err, errWorkerHTTPAdmission) || transport.calls.Load() != 0 {
		t.Fatal("new member exceeded capacity or made a native call", err)
	}
	ctx, cleanup, err := h.begin(context.Background(), "worker/job/attempt/0", true, false)
	if err != nil {
		t.Fatal("capacity blocked an already reserved member's cleanup", err)
	}
	if err := cleanup.finish(workerHTTPRead(h, ctx)); err != nil || transport.calls.Load() != 1 {
		t.Fatal("reserved cleanup could not progress at capacity", err)
	}
	_, recovery, err := h.begin(context.Background(), "worker/job/attempt/0", true, false)
	if err != nil {
		t.Fatal("capacity blocked qualified reserved recovery", err)
	}
	_ = recovery.finish(nil)
}

func TestWorkerHTTPOtherOriginalFailureFencesNextRequest(t *testing.T) {
	body := workerHTTPPod()
	body.closeErr = errors.New("other original Close failed")
	h, transport := workerHTTPFixture(t, body)
	ctx, effect, err := h.begin(context.Background(), "worker/job/attempt/0", false, false)
	if err != nil {
		t.Fatal(err)
	}
	read, log, err := h.begin(context.Background(), "worker/job/attempt/0", false, true)
	if err != nil {
		t.Fatal(err)
	}
	_ = log.finish(workerHTTPRead(h, read))
	if err := effect.finish(workerHTTPRead(h, ctx)); !errors.Is(err, body.closeErr) || !errors.Is(err, backend.ErrWorkerUnsettled) || transport.calls.Load() != 1 {
		t.Fatal("existing operation sent after another original failed Close", err)
	}
}

func TestWorkerHTTPRawFaultBeforeFinish(t *testing.T) {
	for _, kind := range []string{"read", "round_trip"} {
		for _, faultIsLog := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/log_%v", kind, faultIsLog), func(t *testing.T) {
				body := workerHTTPPod()
				raw := errors.New("original raw " + kind + " failure")
				h, transport := workerHTTPFixture(t, body)
				if kind == "read" {
					body.readErr = raw
				} else {
					transport.err, transport.withResponse = raw, true
				}
				faultCtx, fault, err := h.begin(context.Background(), "worker/job/attempt/0", false, faultIsLog)
				if err != nil {
					t.Fatal(err)
				}
				otherCtx, other, err := h.begin(context.Background(), "worker/job/attempt/0", false, !faultIsLog)
				if err != nil {
					t.Fatal(err)
				}
				nativeErr := workerHTTPRead(h, faultCtx)
				if nativeErr == nil || body.closes.Load() != 1 {
					t.Fatal("fault did not return with its successful original Close", nativeErr)
				}
				// Deliberately pause before fault.finish. The other operation
				// was already admitted and must see the same exact owner now.
				otherErr := workerHTTPRead(h, otherCtx)
				var owner *workerHTTPError
				if !errors.Is(otherErr, raw) || !errors.Is(otherErr, backend.ErrWorkerUnsettled) || !errors.As(otherErr, &owner) || owner.call != fault || owner.frame != fault.f || transport.calls.Load() != 1 {
					t.Fatal("active peer lost the original owner or sent after failure", otherErr)
				}
				if err := other.finish(otherErr); !errors.Is(err, raw) {
					t.Fatal("other finish lost original cause", err)
				}
				if err := fault.finish(nativeErr); !errors.Is(err, raw) {
					t.Fatal("fault finish lost original cause", err)
				}
				h.fence()
				joined := h.join(context.Background())
				if !errors.Is(joined, raw) || !errors.Is(joined, backend.ErrWorkerUnsettled) || !errors.As(joined, &owner) || owner.call != fault || owner.frame != fault.f {
					t.Fatal("join lost raw cause or retained a different call", joined)
				}
				if body.closes.Load() != 1 || transport.calls.Load() != 1 {
					t.Fatal("original Close or transport repeated")
				}
			})
		}
	}
}

func TestWorkerHTTPRoundTripFaultBeforeBlockedClose(t *testing.T) {
	body := workerHTTPPod()
	body.entered, body.release = make(chan struct{}), make(chan struct{})
	h, transport := workerHTTPFixture(t, body)
	raw := errors.New("original RoundTrip returned response and failure")
	transport.err, transport.withResponse = raw, true
	ctx, fault, err := h.begin(context.Background(), "worker/job/attempt/0", false, true)
	if err != nil {
		t.Fatal(err)
	}
	otherCtx, other, err := h.begin(context.Background(), "worker/job/attempt/0", false, false)
	if err != nil {
		t.Fatal(err)
	}
	returned := make(chan struct{})
	var nativeErr error
	go func() {
		nativeErr = workerHTTPRead(h, ctx)
		close(returned)
	}()
	var release sync.Once
	joinOriginal := func() {
		release.Do(func() { close(body.release) })
		select {
		case <-returned:
		case <-time.After(10 * time.Second):
			t.Error("original did not return after response Close released")
		}
	}
	t.Cleanup(joinOriginal)
	select {
	case <-body.entered:
	case <-returned:
		t.Fatal("original returned before its response Close blocked")
	case <-time.After(10 * time.Second):
		t.Fatal("original response Close did not enter")
	}
	otherErr := workerHTTPRead(h, otherCtx)
	var owner *workerHTTPError
	if !errors.Is(otherErr, raw) || !errors.Is(otherErr, backend.ErrWorkerUnsettled) || !errors.As(otherErr, &owner) || owner.call != fault || transport.calls.Load() != 1 {
		t.Fatal("blocked Close hid the earlier raw fault from active peer", otherErr)
	}
	_ = other.finish(otherErr)
	joined := h.join(context.Background())
	if !errors.Is(joined, raw) || !errors.As(joined, &owner) || owner.call != fault {
		t.Fatal("join lost original while its response Close was blocked", joined)
	}
	joinOriginal()
	if err := fault.finish(nativeErr); !errors.Is(err, raw) {
		t.Fatal("released original lost raw cause", err)
	}
	if body.closes.Load() != 1 || transport.calls.Load() != 1 {
		t.Fatal("original Close or transport repeated")
	}
}

// workerLateRequestBody is an actual request Read that can return after a
// successful original Close. That software fault frontier models an HTTP
// transport's original writer, not a Kubernetes native effect.
type workerLateRequestBody struct {
	entered, release chan struct{}
	raw              error
	reads, closes    atomic.Int32
}

func (b *workerLateRequestBody) Read([]byte) (int, error) {
	b.reads.Add(1)
	close(b.entered)
	<-b.release
	return 0, b.raw
}
func (b *workerLateRequestBody) Close() error {
	b.closes.Add(1)
	return nil
}

type workerLateRequestTransport struct {
	original *workerLateRequestBody
	returned chan struct{}
	body     io.ReadCloser
	readErr  error
	calls    atomic.Int32
}

func (r *workerLateRequestTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r.calls.Add(1)
	r.body = req.Body
	go func() {
		_, r.readErr = req.Body.Read(make([]byte, 1))
		close(r.returned)
	}()
	<-r.original.entered
	_ = req.Body.Close() // positively returns while its original Read remains
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: workerHTTPPod(), Request: req}, nil
}

func TestWorkerHTTPLateRequestReadCannotReleaseOwner(t *testing.T) {
	for _, pauseFinish := range []bool{false, true} {
		t.Run(fmt.Sprintf("pause_finish_%v", pauseFinish), func(t *testing.T) {
			original := &workerLateRequestBody{entered: make(chan struct{}), release: make(chan struct{}), raw: errors.New("late original request Read failed")}
			h, err := newWorkerHTTP(new(Backend), &rest.Config{Host: "http://worker.invalid"})
			if err != nil {
				t.Fatal(err)
			}
			transport := &workerLateRequestTransport{original: original, returned: make(chan struct{})}
			h.transport.base = transport
			ctx, call, err := h.begin(context.Background(), "worker/job/attempt/0", false, false)
			if err != nil {
				t.Fatal(err)
			}
			var release sync.Once
			joinRead := func() {
				release.Do(func() { close(original.release) })
				select {
				case <-transport.returned:
				case <-time.After(10 * time.Second):
					t.Error("original request Read did not return after release")
				}
			}
			// Registered before the original request writer is launched.
			t.Cleanup(joinRead)
			var pod corev1.Pod
			if err := h.client.CoreV1().RESTClient().Post().Resource("pods").Body(original).MaxRetries(0).Do(h.request(ctx)).Into(&pod); err != nil {
				t.Fatal(err)
			}
			if original.closes.Load() != 1 {
				t.Fatal("original request Close did not positively return")
			}
			// The SDK returned, but the SAME actual request Read is still
			// pending. Its frame must not be clean-ended or detached.
			var finished error
			if !pauseFinish {
				finished = call.finish(nil)
				joinRead()
			} else {
				// Pause finish at its real call->frame critical section,
				// without a hook or synthetic cause/held mutation. Let the
				// actual Read fail while that atomic end is still pending.
				call.f.mu.Lock()
				unlocked := false
				t.Cleanup(func() {
					if !unlocked {
						call.f.mu.Unlock()
					}
				})
				done := make(chan struct{})
				go func() { finished = call.finish(nil); close(done) }()
				t.Cleanup(func() {
					release.Do(func() { close(original.release) })
					if !unlocked {
						call.f.mu.Unlock()
						unlocked = true
					}
					select {
					case <-done:
					case <-time.After(10 * time.Second):
						t.Error("original finish did not join during cleanup")
					}
				})
				deadline := time.Now().Add(10 * time.Second)
				for call.mu.TryLock() {
					call.mu.Unlock()
					if time.Now().After(deadline) {
						t.Fatal("finish did not acquire its original call lock")
					}
					time.Sleep(time.Millisecond)
				}
				release.Do(func() { close(original.release) })
				call.f.mu.Unlock()
				unlocked = true
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Fatal("original finish did not return after release")
				}
				joinRead()
			}
			var owner *workerHTTPError
			if !errors.Is(finished, backend.ErrWorkerUnsettled) || !errors.Is(finished, original.raw) || !errors.As(finished, &owner) || owner.call != call || owner.frame != call.f {
				t.Fatal("finish lost a pending or later-failed original", finished)
			}
			if !errors.Is(transport.readErr, original.raw) {
				t.Fatal("original request Read result was replaced", transport.readErr)
			}
			// No NEW raw Read may begin after Close, even when an earlier
			// admitted original Read returned later.
			if _, err := transport.body.Read(make([]byte, 1)); !errors.Is(err, io.ErrClosedPipe) || original.reads.Load() != 1 {
				t.Fatal("closed wrapper admitted another raw Read", err)
			}
			if _, _, err := h.begin(context.Background(), "worker/job/attempt/0", true, false); !errors.Is(err, original.raw) || !errors.Is(err, backend.ErrWorkerUnsettled) {
				t.Fatal("late request failure readmitted retirement", err)
			}
			h.fence()
			joined := h.join(context.Background())
			if !errors.Is(joined, original.raw) || !errors.Is(joined, backend.ErrWorkerUnsettled) || !errors.As(joined, &owner) || owner.call != call {
				t.Fatal("join erased late original fault after successful Close", joined)
			}
			_ = transport.body.Close()
			if original.closes.Load() != 1 || transport.calls.Load() != 1 {
				t.Fatal("original Close or transport was retried")
			}
		})
	}
}
