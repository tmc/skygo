package kube

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
)

// The fixed-port gateway relays one client HTTP/1.1 exchange to the single
// accepted port of an environment Pod over its own port-forward connection.
// There is no local listener. The connection carries one error/data stream
// pair with the headers of client-go's PortForwarder.handleConnection
// (tools/portforward/portforward.go in v0.32.3); its listeners and unjoined
// copiers are not used.
//
// The gateway writes the request with Request.Write and reads the response
// with http.ReadResponse. It refuses CONNECT and protocol upgrades, removes
// the client's Authorization and hop-by-hop headers, never follows a redirect
// and bounds the request body, the response, the remote error message and
// the session's lifetime. The response header is flushed before the body is
// read, and body data as it arrives, so server-sent events and other
// streaming responses stream. The client-facing deadlines come from the ResponseWriter's
// own SetWriteDeadline and, for a request with a body, SetReadDeadline; a
// writer without them is refused before any connection is made. A request
// without a body gets no read deadline: net/http's background read owns that
// connection's reads, and a deadline firing there cancels the connection's
// base context. As its HTTP/1 close policy the gateway sets the literal
// response header Connection: close on every response, so net/http's HTTP/1
// server closes the client connection after that response and serves no later
// request on it. This is a no-reuse policy; the gateway makes no claim about
// connection reuse or about HTTP/2 clients. A client that leaves after a
// response with framed length was fully relayed does not fail the exchange. Ending the exchange
// closes this port-forward connection only. The limits on connections per
// environment and the environment's lifetime belong to the caller.

const (
	maxPortRequestBody = 1 << 20
	maxPortResponse    = 64 << 20 // header and body bytes read from the Pod
	maxPortError       = 64 << 10
	portLifetime       = 10 * time.Minute
	// portJoinBudget bounds how long the handler waits, after the exchange
	// ends, for the frame's other owners before it returns unsettled.
	portJoinBudget = 5 * time.Second
	// portRequestID pairs the error and data streams. It identifies the pair
	// on this one connection; it is not a replay identity.
	portRequestID = "0"
)

var (
	errPortLimit    = errors.New("kubernetes port gateway byte limit exceeded")
	errPortDeadline = errors.New("kubernetes port gateway requires response deadlines")
	errPortClient   = errors.New("kubernetes port gateway client went away")
	errPortExpired  = errors.New("kubernetes port gateway lifetime expired")
)

// portTarget is the single declared port of a checked environment Pod.
type portTarget struct {
	Pod  string
	Port int32
}

// portResult is a snapshot of a port frame.
type portResult struct {
	envState
	// Status is the status code written to the client; zero if none was.
	Status int
}

// portFrame is the frame of one gateway exchange.
type portFrame struct {
	*envFrame

	statusMu sync.Mutex // guards the fields below
	status   int
	ending   bool // the handler no longer watches the client
	gone     bool // the client left while it was watched
}

// clientGone records that the client left and aborts the frame, unless the
// handler already ended the exchange. The decision and the abort's close
// latch happen under statusMu, so end orders them against teardown.
func (p *portFrame) clientGone() {
	p.statusMu.Lock()
	defer p.statusMu.Unlock()
	if p.ending {
		return
	}
	p.gone = true
	p.abort(false) // spawns the Close; never waits
}

// expired cancels the frame for its lifetime, unless the handler already
// ended the exchange, and reports whether it did.
func (p *portFrame) expired() bool {
	p.statusMu.Lock()
	defer p.statusMu.Unlock()
	if p.ending {
		return false
	}
	p.cancel()
	return true
}

// end stops client watching and reports whether the client left before.
func (p *portFrame) end() (gone bool) {
	p.statusMu.Lock()
	defer p.statusMu.Unlock()
	p.ending = true
	return p.gone
}

func (p *portFrame) result() portResult {
	s := p.state()
	p.statusMu.Lock()
	defer p.statusMu.Unlock()
	return portResult{envState: s, Status: p.status}
}

// hopHeaders are removed in both directions, along with any header the
// Connection header names. Authorization and Proxy-Authorization are the
// control plane's and never reach the Pod; Expect is removed so that no
// interim response is solicited.
var hopHeaders = []string{
	"Connection", "Keep-Alive", "Proxy-Connection", "Proxy-Authenticate", "Proxy-Authorization",
	"Te", "Trailer", "Transfer-Encoding", "Upgrade",
}

func removeHopHeaders(h http.Header) {
	for _, v := range h["Connection"] {
		for _, name := range strings.Split(v, ",") {
			if name = textproto.TrimString(name); name != "" {
				h.Del(name)
			}
		}
	}
	for _, name := range hopHeaders {
		h.Del(name)
	}
}

// portRequest returns the request to send to the Pod, or a status and error
// refusing r.
func portRequest(r *http.Request, port int32, body io.Reader) (*http.Request, int, error) {
	if r.Method == http.MethodConnect {
		return nil, http.StatusMethodNotAllowed, errors.New("kubernetes port gateway refuses CONNECT")
	}
	if httpstream.IsUpgradeRequest(r) || r.Header.Get("Upgrade") != "" {
		return nil, http.StatusNotImplemented, errors.New("kubernetes port gateway refuses protocol upgrades")
	}
	if r.ContentLength > maxPortRequestBody {
		return nil, http.StatusRequestEntityTooLarge, errPortLimit
	}
	h := r.Header.Clone()
	removeHopHeaders(h)
	h.Del("Authorization")
	h.Del("Expect")
	out := &http.Request{
		Method:        r.Method,
		URL:           &url.URL{Path: r.URL.Path, RawPath: r.URL.RawPath, RawQuery: r.URL.RawQuery},
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        h,
		Host:          net.JoinHostPort("localhost", strconv.Itoa(int(port))),
		ContentLength: r.ContentLength,
		Close:         true,
	}
	if r.ContentLength != 0 {
		out.Body = io.NopCloser(body)
	}
	return out, 0, nil
}

// boundedReader returns at most n bytes of r. Once n bytes are returned it
// reads one byte ahead, which it never returns, to tell the end of r from
// excess: excess fails with errPortLimit and sets hit. A source of exactly n
// bytes ends with io.EOF, whether its last data came with io.EOF or not.
type boundedReader struct {
	r   io.Reader
	n   int64 // bytes still allowed
	hit bool
	eof bool // r returned io.EOF
}

func (b *boundedReader) Read(p []byte) (int, error) {
	switch {
	case b.hit:
		return 0, errPortLimit
	case b.eof:
		return 0, io.EOF
	case len(p) == 0:
		return 0, nil
	case b.n == 0:
		var ahead [1]byte
		for range 100 {
			n, err := b.r.Read(ahead[:])
			if n > 0 {
				b.hit = true
				return 0, errPortLimit
			}
			if err == io.EOF {
				b.eof = true
			}
			if err != nil {
				return 0, err
			}
		}
		return 0, io.ErrNoProgress
	}
	if int64(len(p)) > b.n {
		p = p[:b.n]
	}
	n, err := b.r.Read(p)
	b.n -= int64(n)
	if err == io.EOF {
		b.eof = true
	}
	return n, err
}

// handlerBody is the client's request body as the request writer reads it.
// After the handler ends it refuses reads, since net/http forbids reading a
// request body once its handler has returned.
type handlerBody struct {
	mu    sync.Mutex
	r     io.Reader
	ended bool
}

func (b *handlerBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ended {
		return 0, errors.New("kubernetes port gateway request body read after handler end")
	}
	return b.r.Read(p)
}

// end waits for a Read in progress, then refuses further reads.
func (b *handlerBody) end() {
	b.mu.Lock()
	b.ended = true
	b.mu.Unlock()
}

// servePort relays r to target's declared port and returns the exchange's
// frame. It is called from an http.Handler and returns once the response is
// written and the frame's other owners have returned, or after
// portJoinBudget with the frame unsettled. The caller keeps the frame.
func (t *envTransport) servePort(w http.ResponseWriter, r *http.Request, target portTarget) *portFrame {
	p := &portFrame{envFrame: newEnvFrame()}
	p.enter() // the handler
	var herr error
	defer func() { p.leave(herr) }()

	w.Header().Set("Connection", "close")
	refuse := func(code int, err error) *portFrame {
		p.statusMu.Lock()
		p.status = code
		p.statusMu.Unlock()
		http.Error(w, http.StatusText(code), code)
		herr = err
		return p
	}
	if target.Port < 1 || target.Port > 65535 {
		return refuse(http.StatusInternalServerError, errors.New("invalid kubernetes port"))
	}
	u, err := t.podURL(target.Pod, "portforward", nil)
	if err != nil {
		return refuse(http.StatusInternalServerError, err)
	}
	limited := &boundedReader{r: r.Body, n: maxPortRequestBody}
	body := &handlerBody{r: limited}
	defer body.end()
	out, code, err := portRequest(r, target.Port, body)
	if err != nil {
		return refuse(code, err)
	}
	rc := http.NewResponseController(w)
	deadline := time.Now().Add(portLifetime)
	hasBody := out.Body != nil
	if hasBody {
		if err := rc.SetReadDeadline(deadline); err != nil {
			return refuse(http.StatusInternalServerError, errors.Join(errPortDeadline, err))
		}
	}
	if err := rc.SetWriteDeadline(deadline); err != nil {
		return refuse(http.StatusInternalServerError, errors.Join(errPortDeadline, err))
	}

	var (
		owned  []<-chan struct{}
		wrote  <-chan struct{} // the request writer, once started
		framed bool            // the response body had a framed length
	)
	stop := make(chan struct{})
	defer func() {
		gone := p.end()
		close(stop)
		if hasBody && wrote != nil {
			select {
			case <-wrote:
			default:
				// End a body read still waiting on the client. The
				// connection closes after this response.
				rc.SetReadDeadline(time.Now())
			}
		}
		budget := time.NewTimer(portJoinBudget)
		defer budget.Stop()
	join:
		for _, c := range owned {
			select {
			case <-c:
			case <-budget.C:
				break join
			}
		}
		// A client that left after a framed response was fully relayed
		// did not interrupt the exchange.
		if gone && (herr != nil || !framed) {
			p.record(errPortClient)
			p.record(errEnvCanceled)
		}
	}()

	ctx := r.Context()
	expire := time.NewTimer(portLifetime)
	owned = append(owned, p.spawn(func() error {
		defer expire.Stop()
		select {
		case <-ctx.Done():
			p.clientGone()
			return nil
		case <-expire.C:
			if p.expired() {
				return errPortExpired
			}
			return nil
		case <-stop:
			return nil
		}
	}))

	var errStream, data *envStream
	opened := p.spawn(func() error {
		conn, err := p.dial(t, u, portProtocol)
		if err != nil {
			return err
		}
		h := http.Header{}
		h.Set(corev1.StreamType, corev1.StreamTypeError)
		h.Set(corev1.PortHeader, strconv.Itoa(int(target.Port)))
		h.Set(corev1.PortForwardRequestIDHeader, portRequestID)
		e, err := p.createStream(conn, "error", h)
		if err != nil {
			p.closeConn()
			return err
		}
		// Nothing is written on the error stream.
		if err := e.closeWrite(); err != nil {
			p.closeConn()
			return err
		}
		h.Set(corev1.StreamType, corev1.StreamTypeData)
		d, err := p.createStream(conn, "data", h)
		if err != nil {
			p.closeConn()
			return err
		}
		errStream, data = e, d
		return nil
	})
	owned = append(owned, opened)
	select {
	case <-opened:
	case <-p.ctx.Done():
	}
	ok := false
	select {
	case <-opened:
		ok = data != nil
	default:
	}
	if !ok {
		owned = append(owned, p.teardown())
		return refuse(http.StatusBadGateway, nil)
	}

	owned = append(owned, p.spawn(func() error {
		b, err := io.ReadAll(&boundedReader{r: errStream, n: maxPortError})
		switch {
		case err != nil:
			p.closeConn()
			return fmt.Errorf("read kubernetes port forward error stream: %w", err)
		case len(b) != 0:
			p.closeConn()
			return fmt.Errorf("kubernetes port forward error: %q", b)
		}
		return nil
	}))
	wrote = p.spawn(func() error {
		if err := out.Write(data); err != nil {
			p.closeConn()
			// Request.Write hides body read errors in an unexported type.
			if limited.hit {
				err = errors.Join(errPortLimit, err)
			}
			return fmt.Errorf("write kubernetes port gateway request: %w", err)
		}
		return nil
	})
	owned = append(owned, wrote)

	resp, err := http.ReadResponse(bufio.NewReader(&boundedReader{r: data, n: maxPortResponse}), out)
	if err != nil {
		p.closeConn()
		refuse(http.StatusBadGateway, fmt.Errorf("read kubernetes port gateway response: %w", err))
		owned = append(owned, p.teardown())
		return p
	}
	if resp.StatusCode < 200 {
		// Upgrades were refused and Expect removed; an interim or switching
		// response is not relayed.
		resp.Body.Close()
		p.closeConn()
		refuse(http.StatusBadGateway, fmt.Errorf("kubernetes port gateway refuses status %d", resp.StatusCode))
		owned = append(owned, p.teardown())
		return p
	}
	removeHopHeaders(resp.Header)
	// The Pod's Connection header was removed; the gateway's own stays.
	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	p.statusMu.Lock()
	p.status = resp.StatusCode
	p.statusMu.Unlock()
	w.WriteHeader(resp.StatusCode)
	// A framed body ends early with io.ErrUnexpectedEOF, never io.EOF, if
	// the data stream is cut; a body delimited by the stream's end does not.
	framed = resp.ContentLength >= 0 || len(resp.TransferEncoding) != 0 || resp.Body == http.NoBody
	// Send the header before the first body read, which may wait as long as
	// the Pod streams: a client sees a streaming response's header at once.
	if ferr := rc.Flush(); ferr != nil {
		herr = fmt.Errorf("flush kubernetes port gateway response header: %w", ferr)
	} else {
		herr = p.copyResponse(w, rc, resp.Body)
	}
	if herr != nil {
		// End any read still waiting on the data stream, so that the body's
		// Close, which reads the rest of the body, cannot wait on the Pod.
		p.closeConn()
	}
	if err := resp.Body.Close(); err != nil && herr == nil {
		herr = fmt.Errorf("close kubernetes port gateway response: %w", err)
	}
	// Stop watching the client before teardown, so that its explicit
	// stream Resets never race a disconnect's connection Close.
	p.end()
	if herr != nil {
		p.closeConn()
	}
	owned = append(owned, p.teardown())
	return p
}

// copyResponse copies body to w, flushing after each write.
func (p *portFrame) copyResponse(w http.ResponseWriter, rc *http.ResponseController, body io.Reader) error {
	buf := make([]byte, 32<<10)
	for {
		n, err := body.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return fmt.Errorf("write kubernetes port gateway response: %w", werr)
			}
			if ferr := rc.Flush(); ferr != nil {
				return fmt.Errorf("flush kubernetes port gateway response: %w", ferr)
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read kubernetes port gateway response: %w", err)
		}
	}
}

// teardown starts the frame's final owner: the explicit Reset of each
// stream, unless the connection is already closing, then the connection's
// single Close.
func (p *portFrame) teardown() <-chan struct{} {
	return p.spawn(func() error {
		if !p.isClosing() {
			p.resetAll()
		}
		if c := p.closeConn(); c != nil {
			<-c
		}
		return nil
	})
}
