package kube

import (
	"context"
	"errors"
	"io"
	"net/http"
	"slices"
	"sync"
)

// client-go closes response bodies itself and discards the Close error
// (rest.readAndCloseResponseBody in v0.32.3), so a nil Result does not show
// that every original body closed. Each data operation therefore runs under
// one dataFrame. Its context tags the operation's requests; dataTransport
// registers every tagged response body in the frame before client-go sees it,
// and dataBody makes the single original Close and records its result. The
// operation keeps its ownership claim unless every registered body closed
// without error. Untagged requests pass through unchanged.

// maxDataFrames bounds the concurrent operation frames of one attempt. Attempts
// are bounded by the operation ownership limit, so frames are bounded too.
const maxDataFrames = 8

// errDataBodyHeld reports that an operation's response body closure is
// uncertain, or that another operation on the attempt has such a body.
// The original body and its Close error stay in the frame, never in the error.
var errDataBodyHeld = errors.New("kubernetes data response body closure is uncertain")

// errDataBodyLimit reports a response body longer than its frame's limit.
var errDataBodyLimit = errors.New("kubernetes data response exceeds its byte limit")

type dataFrameKey struct{}

// A dataFrame is the custody record of one data operation. A frame whose
// operation ended with any body open or failed is held, along with the
// operation's claim, for the Backend's lifetime. The frame keeps every
// original body whose Close failed, with its error, and every body whose Close
// has not returned. Bodies are registered only until the first failure, so the
// retained set is bounded by the bodies an operation holds open at once (at
// most two today: a log stream and its metadata check). No Close is retried, no
// further request of that operation is sent, and from the first failed Close
// no new operation on the attempt begins; operations already running continue
// under their own frames. A server restart releases a held frame only through
// the qualified lifetime handoff that the coordinator owns.
type dataFrame struct {
	worker *workerHTTPCall // optional exact worker owner; nil for existing data operations

	logs bool // the frame of the attempt's single log read

	mu     sync.Mutex
	limit  int64       // if positive, the byte limit of each body before decoding
	open   []*dataBody // registered bodies whose Close has not returned
	failed []*dataBody // registered bodies whose Close failed, in order
	ended  bool
	held   bool
}

// dataBody is a tagged response body. Its first Close makes the only Close of
// the original body; later calls return the recorded result.
type dataBody struct {
	io.ReadCloser
	frame *dataFrame
	limit int64 // the frame's limit when registered
	once  sync.Once
	err   error
}

func (d *dataBody) Close() error {
	d.once.Do(func() {
		err := d.ReadCloser.Close()
		f := d.frame
		f.mu.Lock()
		defer f.mu.Unlock()
		d.err = err
		f.open = slices.DeleteFunc(f.open, func(x *dataBody) bool { return x == d })
		if err != nil {
			f.failed = append(f.failed, d)
		}
	})
	return d.err
}

// register records an original response body. It refuses after a failed Close
// or after the operation ended.
func (f *dataFrame) register(body io.ReadCloser) (*dataBody, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.failed) != 0 || f.ended {
		return nil, errDataBodyHeld
	}
	d := &dataBody{ReadCloser: body, frame: f, limit: f.limit}
	f.open = append(f.open, d)
	return d, nil
}

// limitBodies sets the limit of bodies registered from now on. Zero, the
// default, adds none.
func (f *dataFrame) limitBodies(n int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.limit = n
}

func (f *dataFrame) bodyLimit() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.limit
}

// limitedBody delivers at most n more bytes of a registered body, then fails
// reads with errDataBodyLimit. Its Close is the registered body's Close.
type limitedBody struct {
	*dataBody
	n int64
}

func (l *limitedBody) Read(p []byte) (int, error) {
	if int64(len(p)) > l.n+1 {
		p = p[:l.n+1]
	}
	n, err := l.dataBody.Read(p)
	if int64(n) > l.n {
		n, l.n = int(l.n), 0
		return n, errDataBodyLimit
	}
	l.n -= int64(n)
	return n, err
}

// usable reports whether a new request may be sent in this frame.
func (f *dataFrame) usable() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.failed) == 0 && !f.ended
}

// clean reports whether every body registered so far closed without error.
func (f *dataFrame) clean() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.failed) == 0 && len(f.open) == 0
}

// end marks the operation finished and reports whether its bodies are clean.
// An unclean frame becomes held.
func (f *dataFrame) end() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ended = true
	f.held = len(f.failed) != 0 || len(f.open) != 0
	return !f.held
}

// dataTransport registers the bodies of tagged responses in their frame.
type dataTransport struct {
	base http.RoundTripper
}

func (t *dataTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	f, _ := req.Context().Value(dataFrameKey{}).(*dataFrame)
	if f == nil {
		return t.base.RoundTrip(req)
	}
	if !f.usable() {
		if req.Body != nil {
			req.Body.Close()
		}
		return nil, errDataBodyHeld
	}
	resp, err := t.base.RoundTrip(req)
	if resp == nil || resp.Body == nil {
		return resp, err
	}
	body, rerr := f.register(resp.Body)
	if rerr != nil {
		// The operation ended while the request was in flight; the body was
		// never registered and nothing in this frame reads it.
		resp.Body.Close()
		return nil, rerr
	}
	resp.Body = body
	if body.limit > 0 {
		resp.Body = &limitedBody{body, body.limit}
	}
	return resp, err
}

// beginData claims the attempt's operation ownership and opens a frame whose
// context tags the operation's requests. It refuses while the attempt retains
// a held frame, while another log read is open (for logs), or when the
// attempt has maxDataFrames frames.
func (b *Backend) beginData(ctx context.Context, id string, exclusive, logs bool) (context.Context, *dataFrame, error) {
	if err := b.claimRetry(id, exclusive); err != nil {
		return nil, nil, err
	}
	f := &dataFrame{logs: logs}
	b.mu.Lock()
	err := b.addFrameLocked(id, f)
	b.mu.Unlock()
	if err != nil {
		b.releaseRetry(id)
		return nil, nil, err
	}
	return context.WithValue(ctx, dataFrameKey{}, f), f, nil
}

func (b *Backend) addFrameLocked(id string, f *dataFrame) error {
	frames := b.dataFrames[id]
	for _, x := range frames {
		x.mu.Lock()
		held := x.held || len(x.failed) != 0
		x.mu.Unlock()
		if held || f.logs && x.logs {
			return errDataBodyHeld
		}
	}
	if len(frames) >= maxDataFrames {
		return errors.New("kubernetes data operation limit reached")
	}
	if b.dataFrames == nil {
		b.dataFrames = make(map[string][]*dataFrame)
	}
	b.dataFrames[id] = append(frames, f)
	return nil
}

// endData ends the operation and reports whether it released it. A clean
// frame is removed and its claim released. Otherwise the frame and the claim
// are retained.
func (b *Backend) endData(id string, f *dataFrame) bool {
	if !f.end() {
		return false
	}
	b.mu.Lock()
	b.dataFrames[id] = slices.DeleteFunc(b.dataFrames[id], func(x *dataFrame) bool { return x == f })
	if len(b.dataFrames[id]) == 0 {
		delete(b.dataFrames, id)
	}
	b.mu.Unlock()
	b.releaseRetry(id)
	return true
}

// withHeld adds errDataBodyHeld to err, keeping err's own failure.
func withHeld(err error) error {
	switch {
	case err == nil:
		return errDataBodyHeld
	case errors.Is(err, errDataBodyHeld):
		return err
	}
	return errors.Join(err, errDataBodyHeld)
}
