package agentssh

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
)

// A responseBody is the body of a response from the agent. Reaching EOF
// does not release the request's connections; Close does.
type responseBody struct {
	frame *requestFrame
	body  io.ReadCloser
}

func (b *responseBody) Read(p []byte) (int, error) {
	n, err := b.body.Read(p)
	if err != nil && err != io.EOF {
		err = &requestError{op: "read", err: err, cause: b.frame.cancelCause(), frame: b.frame}
	}
	return n, err
}

// Close closes the request's TCP connection and joins the calls started
// for the request. Every call returns the same result: the connection's
// first Close error, or a *heldError if teardown has not finished within
// the close budget.
func (b *responseBody) Close() error { return b.frame.close() }

// A requestFrame owns everything one request opens. Each owner is
// published under mu before it can be abandoned, and mu is never held
// across a blocking call.
//
// Every connection rides on raw: closing raw ends the SSH connection,
// its channel, the TLS session and the response body. Cancellation and
// teardown therefore close raw first and then join what was started.
type requestFrame struct {
	deadline    time.Time // end of the request's budget
	closeBudget time.Duration

	// dialCtx carries the request's values and is canceled only after
	// cancel latches its cause, so a dial it unblocks is published late.
	dialCtx  context.Context
	stopDial context.CancelFunc

	dialed   chan struct{} // closed when the TCP dial returns
	canceled chan struct{} // closed when cause is set
	stop     chan struct{} // closed when teardown starts; ends watch
	watched  chan struct{} // closed when watch returns
	done     chan struct{} // closed when teardown finishes

	mu      sync.Mutex
	cause   error         // cancellation cause; once set, nothing new is adopted
	closing bool          // teardown started
	raw     *rawConn      // TCP connection to the SSH server
	conn    ssh.Conn      // authenticated SSH connection
	dialing chan struct{} // closed when the direct-tcpip call returns
	channel net.Conn      // direct-tcpip channel, set by that call
	dialErr error         // that call's error
	body    io.ReadCloser // parsed response body
	hostKey string        // SHA256 fingerprint of the verified host key

	// Set by teardown before done is closed.
	closeErr   error // first Close result of raw
	channelErr error
	bodyErr    error
	waitErr    error // ssh.Conn.Wait result
}

func newFrame(ctx context.Context, deadline time.Time, closeBudget time.Duration) *requestFrame {
	dialCtx, stopDial := context.WithCancel(context.WithoutCancel(ctx))
	return &requestFrame{
		deadline:    deadline,
		closeBudget: closeBudget,
		dialed:      make(chan struct{}),
		dialCtx:     dialCtx,
		stopDial:    stopDial,
		canceled:    make(chan struct{}),
		stop:        make(chan struct{}),
		watched:     make(chan struct{}),
		done:        make(chan struct{}),
	}
}

// watch cancels the request when ctx is done, including when ctx is
// already done as teardown starts, or with errDialTimeout if the TCP dial
// has not returned by the request's deadline. After the dial, the deadline
// is enforced on the connection itself. Teardown joins watch.
func (f *requestFrame) watch(ctx context.Context) {
	defer close(f.watched)
	timer := time.NewTimer(time.Until(f.deadline))
	defer timer.Stop()
	dialed, expired := f.dialed, timer.C
	for {
		select {
		case <-ctx.Done():
			f.cancel(context.Cause(ctx))
			return
		case <-dialed:
			dialed, expired = nil, nil
		case <-expired:
			select {
			case <-f.dialed:
				dialed, expired = nil, nil
				continue
			default:
			}
			f.cancel(errDialTimeout)
			return
		case <-f.stop:
			if ctx.Err() != nil {
				f.cancel(context.Cause(ctx))
			}
			return
		}
	}
}

var errDialTimeout = errors.New("agentssh: dial timed out")

// cancel latches cause and closes raw if it is already published.
// A raw connection published later is closed by publishRaw.
func (f *requestFrame) cancel(cause error) {
	f.mu.Lock()
	first := f.cause == nil
	if first {
		f.cause = cause
		close(f.canceled)
	}
	raw := f.raw
	f.mu.Unlock()
	if !first {
		return
	}
	f.stopDial()
	if raw != nil {
		raw.Close()
	}
}

func (f *requestFrame) cancelCause() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cause
}

// publishRaw makes c the frame's TCP connection. If the request was
// canceled while c was being dialed, publishRaw closes c and fails.
func (f *requestFrame) publishRaw(c net.Conn) (*rawConn, error) {
	raw := &rawConn{Conn: c, closed: make(chan struct{})}
	f.mu.Lock()
	f.raw = raw
	cause := f.cause
	f.mu.Unlock()
	if cause != nil {
		raw.Close()
		return nil, cause
	}
	return raw, nil
}

func (f *requestFrame) publishSSH(conn ssh.Conn) {
	f.mu.Lock()
	f.conn = conn
	f.mu.Unlock()
}

// publishBody records the response body, failing if the request has been
// canceled so that no response is returned after cancellation.
func (f *requestFrame) publishBody(body io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.body = body
	return f.cause
}

// dialAgent opens the direct-tcpip channel in a call the frame retains.
// ssh.Client.Dial can stay blocked if the SSH connection fails while the
// open is pending, so dialAgent stops waiting on cancellation or after
// timeout. Teardown joins the call and closes a channel that arrives late.
func (f *requestFrame) dialAgent(client *ssh.Client, address string, timeout time.Duration) (net.Conn, error) {
	done := make(chan struct{})
	f.mu.Lock()
	f.dialing = done
	f.mu.Unlock()
	go func() {
		defer close(done)
		ch, err := client.Dial("tcp", address)
		f.mu.Lock()
		f.channel, f.dialErr = ch, err
		f.mu.Unlock()
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.channel, f.dialErr
	case <-f.canceled:
		return nil, f.cancelCause()
	case <-timer.C:
		return nil, errors.New("timed out opening channel")
	}
}

// fail tears down the frame after a failed step and reports err with the
// frame's cancellation cause and close result.
func (f *requestFrame) fail(op string, err error) error {
	cerr := f.close()
	return &requestError{op: op, err: err, cause: f.cancelCause(), close: cerr, frame: f}
}

// close starts teardown once and waits for it for at most f.closeBudget.
func (f *requestFrame) close() error {
	f.mu.Lock()
	start := !f.closing
	f.closing = true
	f.mu.Unlock()
	if start {
		go f.teardown()
	}
	timer := time.NewTimer(f.closeBudget)
	defer timer.Stop()
	select {
	case <-f.done:
		return f.closeErr
	case <-timer.C:
		return &heldError{frame: f}
	}
}

// teardown closes raw first, then joins each call started for the request
// in order. A step that does not return holds every later step.
func (f *requestFrame) teardown() {
	defer close(f.done)
	close(f.stop)
	f.mu.Lock()
	raw, conn, dialing, body := f.raw, f.conn, f.dialing, f.body
	f.mu.Unlock()
	if raw != nil {
		f.closeErr = raw.Close()
	}
	if dialing != nil {
		<-dialing
		f.mu.Lock()
		ch := f.channel
		f.mu.Unlock()
		if ch != nil {
			f.channelErr = ch.Close()
		}
	}
	if body != nil {
		f.bodyErr = body.Close()
	}
	if conn != nil {
		f.waitErr = conn.Wait()
	}
	<-f.watched
	f.stopDial()
}

// wait joins teardown and returns raw's first Close result.
func (f *requestFrame) wait() error {
	<-f.done
	return f.closeErr
}

// A heldError reports that a request's teardown did not finish within the
// close budget. The frame still owns every original; its wait method joins
// the teardown.
type heldError struct {
	frame *requestFrame
}

func (e *heldError) Error() string { return "agentssh: connection teardown still running" }

// A rawConn is a request's TCP connection. Its first Close result is kept
// and returned to every caller, including x/crypto's own Close after a
// failed handshake; the underlying connection is closed once.
type rawConn struct {
	net.Conn

	mu      sync.Mutex
	closing bool
	closed  chan struct{} // closed when err is set
	err     error
}

func (c *rawConn) Close() error {
	c.mu.Lock()
	first := !c.closing
	c.closing = true
	c.mu.Unlock()
	if first {
		c.err = c.Conn.Close()
		close(c.closed)
	}
	<-c.closed
	return c.err
}
