package control

import (
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"sync"
)

const remoteResponseLimit = 96 << 10

var errRemoteResponse = errors.New("remote response incomplete")

// remoteHTTP retains the original producer. Mutable observations are published
// under service.mu only after the synchronous invoker's operations return.
// A blocked or panicking invocation leaves this same frame attached.
type remoteHTTP struct {
	call                          *ArtifactCall
	request                       *http.Request
	service                       http.Handler
	body                          io.ReadCloser
	writer                        http.ResponseWriter
	invoked, returned, bodyJoined bool
	status                        int
	written                       uint64
	writeErr, flushErr, closeErr  error
	contentType                   string
	contentEncoding               string
}

type remoteRequestBody struct {
	original io.ReadCloser
	once     sync.Once
	err      error
}

func (b *remoteRequestBody) Read(p []byte) (int, error) {
	if b.original == nil {
		return 0, io.EOF
	}
	return b.original.Read(p)
}

func (b *remoteRequestBody) Close() error {
	b.once.Do(func() {
		// A recovered original panic must not make Once cache a nil result.
		b.err = errRemoteResponse
		if b.original != nil {
			b.err = b.original.Close()
		} else {
			b.err = nil
		}
	})
	return b.err
}

// remoteResponseWriter is owned by one synchronous generated ServeHTTP frame.
// It deliberately has no Unwrap: ResponseController must use its checked flush.
// No callback or returned value can manufacture a write or flush observation.
type remoteResponseWriter struct {
	original           http.ResponseWriter
	control            *Control
	permit             *workerPermit
	request            *http.Request
	status             int
	written            uint64
	writeErr, flushErr error
}

func (w *remoteResponseWriter) Header() http.Header { return w.original.Header() }

func (w *remoteResponseWriter) WriteHeader(status int) {
	if w.status != 0 || w.writeErr != nil {
		return
	}
	if status >= 100 && status < 200 {
		// The fixed unary profile needs no informational response or upgrade.
		if w.writeErr == nil {
			w.writeErr = errRemoteResponse
		}
		return
	}
	if status < 200 || status > 599 {
		if w.writeErr == nil {
			w.writeErr = errRemoteResponse
		}
		return
	}
	if _, err := w.control.checkRemotePermit(w.request.Context(), w.permit); err != nil {
		if w.writeErr == nil {
			w.writeErr = err
		}
		return
	}
	w.original.WriteHeader(status)
	w.status = status
}

func (w *remoteResponseWriter) Write(p []byte) (int, error) {
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	if uint64(len(p)) > remoteResponseLimit-w.written {
		w.writeErr = errRemoteResponse
		return 0, w.writeErr
	}
	if _, err := w.control.checkRemotePermit(w.request.Context(), w.permit); err != nil {
		w.writeErr = err
		return 0, err
	}
	n, err := w.original.Write(p)
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if n < 0 || n > len(p) {
		w.writeErr = errRemoteResponse
		return n, errRemoteResponse
	}
	w.written += uint64(n)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.writeErr = err
	}
	return n, err
}

func (w *remoteResponseWriter) FlushError() error {
	if w.flushErr != nil {
		return w.flushErr
	}
	if _, err := w.control.checkRemotePermit(w.request.Context(), w.permit); err != nil {
		if w.flushErr == nil {
			w.flushErr = err
		}
		return err
	}
	err := http.NewResponseController(w.original).Flush()
	if w.flushErr == nil {
		w.flushErr = err
	}
	return err
}

// ServeRemoteHTTP invokes the canonical generated remote service once using the
// already authorized original call. Its caller must pass the exact constructor
// handler, with the fixed uncompressed Connect unary profile. Nil means joined
// response I/O, not INPUT4 completion, peer receipt, or outer HTTP completion.
func (c *Control) ServeRemoteHTTP(call *ArtifactCall, w http.ResponseWriter, r *http.Request, service http.Handler) error {
	if c == nil || call == nil || call.service == nil || call.service != c.artifacts || w == nil || r == nil || service == nil {
		return ErrInvalid
	}
	s := call.service
	s.mu.Lock()
	p := call.worker
	_, mapped := s.calls[call]
	if !mapped || p == nil || p.call != call || !p.pending || p.response != nil {
		s.mu.Unlock()
		return ErrInvalid
	}
	body := &remoteRequestBody{original: r.Body}
	h := &remoteHTTP{call: call, request: r, service: service, body: body, writer: w}
	p.response = h
	s.mu.Unlock()

	// Attachment commits to this invoker even if authorization changed since
	// admission. All refusal I/O remains on this original response frame.
	tracked := &remoteResponseWriter{original: w, control: c, permit: p, request: r}
	request := r.Clone(r.Context())
	request.Body = body
	returned := false
	if _, err := c.checkRemotePermit(r.Context(), p); err != nil {
		tracked.writeErr = err
	} else {
		// No deferred success: a panic or blocked service retains h unchanged.
		s.mu.Lock()
		h.invoked = true
		s.mu.Unlock()
		service.ServeHTTP(tracked, request)
		returned = true
	}
	closeErr := body.Close()
	flushErr := tracked.FlushError()
	contentType := remoteResponseType(w.Header(), tracked.written)
	contentEncoding, encodingOK := remoteResponseEncoding(w.Header())
	if !encodingOK {
		contentType = ""
	}
	if contentType == "" && tracked.writeErr == nil {
		tracked.writeErr = errRemoteResponse
	}
	s.mu.Lock()
	h.returned = returned
	h.bodyJoined = closeErr == nil
	h.closeErr = closeErr
	h.status, h.written = tracked.status, tracked.written
	h.contentType = contentType
	h.contentEncoding = contentEncoding
	h.writeErr, h.flushErr = tracked.writeErr, tracked.flushErr
	if h.flushErr == nil {
		h.flushErr = flushErr
	}
	s.artifactChangedLocked()
	s.mu.Unlock()
	return c.finishRemoteResponse(h)
}

func remoteResponseType(h http.Header, written uint64) string {
	if len(h.Values("Content-Type")) != 1 || h.Get("Trailer") != "" || h.Get("Grpc-Status") != "" || h.Get("Connect-Error-Code") != "" {
		return ""
	}
	typ, params, err := mime.ParseMediaType(h.Get("Content-Type"))
	if err != nil || len(params) != 0 || typ != "application/proto" && typ != "application/json" {
		return ""
	}
	if values := h.Values("Content-Length"); len(values) != 0 {
		if len(values) != 1 {
			return ""
		}
		n, err := strconv.ParseUint(values[0], 10, 64)
		if err != nil || n != written {
			return ""
		}
	}
	return typ
}

// remoteReadPublished consumes actual generated-handler observations. The only
// admitted caller is the fixed identity Connect unary handler: its typed mapper
// returns the exact ref/range/data from ReadRemoteInput, with default codecs and
// no custom codec, interceptor or response transform. Those concrete wire
// preconditions require source qualification at the canonical RPC constructor;
// arbitrary HTTP200 handlers cannot satisfy this trusted producer contract.
// The caller holds service.mu after the original response operations joined.
func remoteReadPublished(h *remoteHTTP, p *workerPermit) bool {
	if h == nil || p == nil || h.call != p.call || p.response != h || p.operation != RemoteReadInput || !p.readArmed || !h.invoked || !h.returned || !h.bodyJoined || h.status != http.StatusOK || h.writeErr != nil || h.flushErr != nil || h.closeErr != nil || h.written == 0 || h.written > remoteResponseLimit {
		return false
	}
	if h.contentType != "application/proto" && h.contentType != "application/json" {
		return false
	}
	if h.contentEncoding != "" {
		return false
	}
	return p.readRef.Validate() == nil && p.readOffset >= 0 && p.readEnd >= p.readOffset && p.readEnd <= p.readRef.Size && p.readEnd-p.readOffset <= 64<<10
}

func remoteResponseEncoding(h http.Header) (string, bool) {
	values := h.Values("Content-Encoding")
	if len(values) > 1 {
		return "", false
	}
	encoding := h.Get("Content-Encoding")
	return encoding, encoding == ""
}
