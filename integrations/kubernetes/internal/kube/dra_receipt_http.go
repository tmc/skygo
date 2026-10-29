package kube

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"

	"k8s.io/client-go/rest"
)

// Every native request of the DRA mode, from construction discovery and the
// namespace and class reads to creates, object reads and deletions, is one
// draCall in the draFrame of its operation. client-go reads a response body
// and closes it, but discards Close's error (rest/with_retry.go:349 in the
// pinned v0.35.9 sources), and a successful Result says nothing about it. So
// the receipt transport wraps the original body of a tagged request: it
// records the status, the bytes read and whether reading ended at io.EOF, and
// runs the original Close exactly once, on a goroutine counted in the frame
// and in the owner's draOriginals before it starts. The caller waits for that
// Close only until its context ends; returning then does not finish the
// original.
//
// Until its original Close has returned nil a call is not settled: what its
// response showed may be recorded as truth, but it authorizes no later send,
// pin or cleanup step. The frame keeps every permit of its operation, the
// Backend's operation registration and the store's operation permit, until
// the operation has returned and each original of its calls has finished, so
// the integration's Close joins the originals themselves. A failed original
// Close is sticky: that Close keeps reporting it, and it fences the whole
// attempt. The original's goroutine first publishes the failure, from when
// no native mutation of the attempt is admitted (draRuntime.admit), and
// then, before the frame releases anything, records it as the attempt's
// durable HOLD (draHoldClose), or keeps an earlier HOLD, the same fence, so
// that no later pin, create, cancellation, cleanup deletion or retirement
// continues the attempt, in this process or a replacement. If that record
// cannot be written, the frame keeps its permits for the process lifetime.
// A held original, or a caller's timeout, is not a failure.

var (
	errDRAHeld        = errors.New("kubernetes dra original response close remains held")
	errDRACloseFailed = errors.New("kubernetes dra original response close failed")
	errDRANoFrame     = errors.New("kubernetes dra request outside an operation")
)

// maxDRAResponse bounds the bytes a call keeps of one response body; a
// larger body is not a complete DRA object response.
const maxDRAResponse = 16 << 20

type (
	draFrameKey struct{}
	draCallKey  struct{}
)

// draOriginals is the registry of the original calls of one owner, a
// Backend's runtime or the integration's construction, whose response Close
// has not returned, and the sticky record of those whose Close failed.
type draOriginals struct {
	// persist, if set, durably records that an original Close of attempt
	// failed (draRuntime.holdClosed). It is set before any call.
	persist func(attempt string) error

	mu      sync.Mutex
	closed  bool
	open    int
	idle    chan struct{}  // closed when open returns to zero; nil while zero
	attempt map[string]int // open calls by attempt, "" for none
	failed  map[string]error
	first   error // the first failure, reported by join
}

func newDRAOriginals() *draOriginals {
	return &draOriginals{attempt: make(map[string]int), failed: make(map[string]error)}
}

// add registers one call of attempt, unless the owner was fenced or, for a
// native mutation, a failed original Close of attempt was published (fail).
func (o *draOriginals) add(attempt string, mutate bool) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return errDRAClosed
	}
	if failed := o.failed[attempt]; mutate && failed != nil {
		return fmt.Errorf("%w: %w", errDRAHold, failed)
	}
	if o.open == 0 {
		o.idle = make(chan struct{})
	}
	o.open++
	o.attempt[attempt]++
	return nil
}

// fail publishes that an original Close of attempt failed with err, before
// its HOLD is recorded and before the original finishes.
func (o *draOriginals) fail(attempt string, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.failed[attempt] == nil {
		o.failed[attempt] = err
	}
}

// finish records that a call's original has finished, with err if its Close
// failed.
func (o *draOriginals) finish(attempt string, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err != nil {
		if o.failed[attempt] == nil {
			o.failed[attempt] = err
		}
		if o.first == nil {
			o.first = err
		}
	}
	if o.attempt[attempt]--; o.attempt[attempt] == 0 {
		delete(o.attempt, attempt)
	}
	if o.open--; o.open == 0 {
		close(o.idle)
		o.idle = nil
	}
}

// state reports whether an original of attempt is still held, and the first
// failed Close of one.
func (o *draOriginals) state(attempt string) (held bool, failed error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.attempt[attempt] != 0, o.failed[attempt]
}

// record durably records a failed original Close of attempt. Construction
// reads, of no attempt, have no record: their partial integration keeps them.
func (o *draOriginals) record(attempt string) error {
	if o.persist == nil || attempt == "" {
		return nil
	}
	if err := o.persist(attempt); err != nil {
		return fmt.Errorf("kubernetes dra close failure not recorded, permits kept: %w", err)
	}
	return nil
}

// failure returns the first failed Close, if any.
func (o *draOriginals) failure() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.first
}

// fence refuses every later call. It is never undone.
func (o *draOriginals) fence() {
	o.mu.Lock()
	o.closed = true
	o.mu.Unlock()
}

// join waits until every original has finished, until ctx is done, and then
// reports the first failed Close, if any.
func (o *draOriginals) join(ctx context.Context) error {
	o.mu.Lock()
	idle := o.idle
	o.mu.Unlock()
	if idle != nil {
		select {
		case <-idle:
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", errDRAHeld, ctx.Err())
		}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.first
}

// draFrame is the custody of one operation: the permits it holds, released
// in reverse order once the operation has ended and the original of every
// call made in it has finished.
type draFrame struct {
	orig    *draOriginals
	attempt string

	mu   sync.Mutex
	refs int
	held []func()
}

// withDRAFrame returns a frame of attempt for calls registered in orig, and
// ctx carrying it. The caller calls end once the operation returns.
func withDRAFrame(ctx context.Context, orig *draOriginals, attempt string) (context.Context, *draFrame) {
	f := &draFrame{orig: orig, attempt: attempt, refs: 1}
	return context.WithValue(ctx, draFrameKey{}, f), f
}

// hold adds release to the permits the frame releases at its end.
func (f *draFrame) hold(release func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.held = append(f.held, release)
}

// child returns a frame of attempt for a part of f's operation, such as one
// record of a maintenance pass. f is not released before the child.
func (f *draFrame) child(ctx context.Context, attempt string) (context.Context, *draFrame) {
	f.ref()
	ctx, c := withDRAFrame(ctx, f.orig, attempt)
	c.hold(f.end)
	return ctx, c
}

func (f *draFrame) ref() {
	f.mu.Lock()
	f.refs++
	f.mu.Unlock()
}

// end drops one reference; the last one releases the held permits.
func (f *draFrame) end() {
	f.mu.Lock()
	f.refs--
	var held []func()
	if f.refs == 0 {
		held, f.held = f.held, nil
	}
	f.mu.Unlock()
	for i := len(held) - 1; i >= 0; i-- {
		held[i]()
	}
}

// draCall is the custody of one original native request.
type draCall struct {
	frame *draFrame
	wait  context.Context // bounds the caller's wait for the original Close
	once  sync.Once       // finishes the original in its frame

	mu       sync.Mutex
	sent     bool
	settled  bool
	status   int // of the response, 0 if none arrived
	body     []byte
	ended    bool  // reading the body reported io.EOF
	readErr  error // reading the body reported another error
	orig     io.ReadCloser
	closing  bool
	closed   chan struct{} // closed once the original Close returned
	closeErr error
}

// newDRACall admits one call in the frame of ctx, a native mutation if
// mutate, and returns the context that tags its request. The caller calls
// settle once the request returned. Mutations are admitted only by
// draRuntime.admit.
func newDRACall(ctx context.Context, mutate bool) (*draCall, context.Context, error) {
	f, _ := ctx.Value(draFrameKey{}).(*draFrame)
	if f == nil {
		return nil, nil, errDRANoFrame
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if err := f.orig.add(f.attempt, mutate); err != nil {
		return nil, nil, err
	}
	f.ref()
	c := &draCall{frame: f, wait: ctx, closed: make(chan struct{})}
	return c, context.WithValue(ctx, draCallKey{}, c), nil
}

// roundTrip sends the call's single request without GetBody, as a
// receiptExchange does, and wraps the original response body.
func (c *draCall) roundTrip(base http.RoundTripper, req *http.Request) (*http.Response, error) {
	c.mu.Lock()
	again := c.sent || c.settled
	c.sent = true
	c.mu.Unlock()
	if again {
		if req.Body != nil {
			req.Body.Close()
		}
		return nil, errSentOnce
	}
	one := *req
	one.GetBody = nil
	resp, err := base.RoundTrip(&one)
	if resp == nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status = resp.StatusCode
	if resp.Body == nil {
		resp.Body = http.NoBody
	}
	c.orig = resp.Body
	resp.Body = &draBody{c: c}
	return resp, err
}

// draBody is the body client-go reads and closes in place of the original.
type draBody struct{ c *draCall }

func (b *draBody) Read(p []byte) (int, error) {
	c := b.c
	c.mu.Lock()
	orig, closing := c.orig, c.closing
	c.mu.Unlock()
	if closing {
		return 0, errors.New("kubernetes dra response body read after close")
	}
	n, err := orig.Read(p)
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.body)+n > maxDRAResponse {
		if c.readErr == nil {
			c.readErr = errors.New("kubernetes dra response too large")
		}
	} else {
		c.body = append(c.body, p[:n]...)
	}
	switch {
	case err == io.EOF:
		c.ended = true
	case err != nil && c.readErr == nil:
		c.readErr = err
	}
	return n, err
}

func (b *draBody) Close() error { return b.c.close() }

// close starts the original Close once and waits for it until the caller's
// context ends.
func (c *draCall) close() error {
	c.mu.Lock()
	orig, start := c.orig, !c.closing && c.orig != nil
	c.closing = true
	c.mu.Unlock()
	if orig == nil {
		return nil
	}
	if start {
		// Counted in the frame and the originals since newDRACall.
		go func() {
			err := orig.Close()
			var kept error
			if err != nil {
				// Published first: from here no mutation of the attempt
				// is admitted. Then durable, while the frame still holds
				// every permit.
				c.frame.orig.fail(c.frame.attempt, fmt.Errorf("%w: %w", errDRACloseFailed, err))
				kept = c.frame.orig.record(c.frame.attempt)
			}
			err = errors.Join(err, kept)
			c.mu.Lock()
			c.closeErr = err
			c.mu.Unlock()
			c.finish(err, kept != nil)
			close(c.closed)
		}()
	}
	select {
	case <-c.closed:
	default:
		select {
		case <-c.closed:
		case <-c.wait.Done():
			return fmt.Errorf("%w: %w", errDRAHeld, c.wait.Err())
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closeErr != nil {
		return fmt.Errorf("%w: %w", errDRACloseFailed, c.closeErr)
	}
	return nil
}

// settle ends the call once its request returned. It returns nil only if no
// response arrived or the original Close of its body returned nil. Otherwise
// the original stays counted, and its frame held, until that Close returns.
func (c *draCall) settle() error {
	c.mu.Lock()
	c.settled = true
	orig := c.orig
	c.mu.Unlock()
	if orig == nil {
		c.finish(nil, false)
		return nil
	}
	return c.close()
}

// finish records the end of the call's original, once, with err if its
// Close failed. Unless keep, the call then releases its frame; a failure
// that could not be recorded durably keeps the frame, and so every permit
// of its operation, for the process lifetime.
func (c *draCall) finish(err error, keep bool) {
	c.once.Do(func() {
		if err != nil {
			err = fmt.Errorf("%w: %w", errDRACloseFailed, err)
		}
		c.frame.orig.finish(c.frame.attempt, err)
		if !keep {
			c.frame.end()
		}
	})
}

// response returns the status of the call's response, 0 if none arrived,
// the bytes read from its body, and whether they are the whole body: reading
// ended at io.EOF without another error. Headers alone are never complete.
func (c *draCall) response() (status int, body []byte, complete bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.status, c.body, c.status != 0 && c.ended && c.readErr == nil
}

// draDo sends req once as a call in the frame of ctx, without client-go
// retries, and returns its result only if its response, if any, was read
// completely and its original Close returned nil. A held or failed original
// is an error that is neither NotFound nor an identity difference: it proves
// nothing about the object.
func draDo(ctx context.Context, req *rest.Request) (rest.Result, error) {
	c, cctx, err := newDRACall(ctx, false)
	if err != nil {
		return rest.Result{}, err
	}
	res := req.MaxRetries(0).Do(cctx)
	if err := c.settle(); err != nil {
		return rest.Result{}, err
	}
	if status, _, complete := c.response(); status != 0 && !complete {
		return rest.Result{}, fmt.Errorf("kubernetes dra response status %d incomplete", status)
	}
	return res, nil
}

// draSend runs send, one typed client read, as a call in the frame of ctx,
// as draCall.send does.
func draSend(ctx context.Context, send func(context.Context) error) (delivered bool, err error) {
	c, cctx, err := newDRACall(ctx, false)
	if err != nil {
		return false, err
	}
	return c.send(cctx, send)
}

// send runs send, the call's one typed client request, with cctx, the
// context newDRACall or draRuntime.admit returned for it. It returns send's
// error, joined with any settle error, and whether a complete successful
// response was received: the truth of delivery, which a held or failed
// original Close does not undo but does not authorize past.
func (c *draCall) send(cctx context.Context, send func(context.Context) error) (delivered bool, err error) {
	err = send(cctx)
	serr := c.settle()
	status, _, complete := c.response()
	delivered = err == nil && complete && status >= 200 && status <= 299
	if serr != nil {
		return delivered, serr
	}
	if status != 0 && !complete {
		// Headers without the whole body prove nothing, not even NotFound:
		// err is reported, never wrapped.
		return false, fmt.Errorf("kubernetes dra response status %d incomplete: %v", status, err)
	}
	return delivered, err
}
