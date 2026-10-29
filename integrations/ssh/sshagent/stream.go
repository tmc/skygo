package sshagent

import (
	"errors"
	"io"
	"net/http"

	"connectrpc.com/connect"
)

// A stream is an admitted streaming connection. Its call keeps Connect's
// original connection and records each original method frame until it
// returns, with Connect's concurrency: Send and CloseRequest may run
// alongside Receive, the header methods and CloseResponse. Receive's
// result is returned unchanged; io.EOF there ends no ownership.
//
// The first CloseResponse fences the stream at once: frames entered
// before it, and the body Reads and Closes they make, may finish, but no
// new original frame starts. Later Send, CloseRequest and Receive return a
// refusal wrapping no error; the header methods return the header their
// original returned before, or an empty one, without calling it; and
// later CloseResponse calls return the first one's outcome without
// calling the original again.
//
// The call is done once the original CloseResponse returned, which waits
// for Connect's request, possibly started by Send or CloseRequest in a
// goroutine of Connect's own, to finish; and released once every frame
// returned and each body closed cleanly.
//
// Once its session is closed or faulted, Send is refused and any HTTP
// request still to be sent is refused; Receive, the header methods,
// CloseRequest and CloseResponse remain, to finish the stream.
type stream struct {
	c *call
}

func (s *stream) Spec() connect.Spec { return s.c.conn.Spec() }

func (s *stream) Peer() connect.Peer { return s.c.conn.Peer() }

func (s *stream) RequestHeader() http.Header { return s.c.conn.RequestHeader() }

// A frame is a kind of original stream method frame.
type frame int

const (
	frameRead    frame = iota // Receive or a header method
	frameRequest              // CloseRequest, which may start the request
	frameSend                 // Send, which may start the request
)

// enter enters an original frame of f's kind, or returns why the stream
// refuses it: a closed or faulted session refuses Send, and a stream whose
// CloseResponse started refuses every frame.
func (s *stream) enter(f frame) error {
	c := s.c
	c.l.mu.Lock()
	defer c.l.mu.Unlock()
	if f == frameSend {
		if err := c.l.fenceLocked(); err != nil {
			return err
		}
	}
	if c.closing {
		return &CallError{kind: kindRefused, stages: stageStreamClosed, owner: c}
	}
	if f != frameRead {
		c.requested = true
	}
	c.enterLocked()
	return nil
}

func (s *stream) Send(msg any) error {
	if err := s.enter(frameSend); err != nil {
		return err
	}
	defer s.c.exit()
	return s.c.conn.Send(msg)
}

func (s *stream) CloseRequest() error {
	if err := s.enter(frameRequest); err != nil {
		return err
	}
	err := s.c.conn.CloseRequest()
	s.c.exit()
	if err != nil && s.Spec().StreamType == connect.StreamTypeServer {
		// Connect's CallServerStream drops the connection when
		// CloseRequest fails, without closing its response; no one
		// else can.
		s.CloseResponse()
	}
	return err
}

func (s *stream) Receive(msg any) error {
	if err := s.enter(frameRead); err != nil {
		return err
	}
	err := s.c.conn.Receive(msg)
	c := s.c
	c.l.mu.Lock()
	if err != nil && c.recv == nil && !errors.Is(err, io.EOF) {
		c.recv = err
	}
	c.exitLocked()
	c.l.mu.Unlock()
	return err
}

func (s *stream) ResponseHeader() http.Header { return s.header(false) }

func (s *stream) ResponseTrailer() http.Header { return s.header(true) }

// header returns the original response header or trailer, which waits for
// the response, keeping its first result. Once CloseResponse started, it
// returns that first result, or an empty header, without the original.
func (s *stream) header(trailer bool) http.Header {
	c := s.c
	c.l.mu.Lock()
	kept := &c.header
	if trailer {
		kept = &c.trailer
	}
	if c.closing {
		h := *kept
		c.l.mu.Unlock()
		if h == nil {
			h = make(http.Header)
		}
		return h
	}
	c.enterLocked()
	c.l.mu.Unlock()
	var h http.Header
	if trailer {
		h = c.conn.ResponseTrailer()
	} else {
		h = c.conn.ResponseHeader()
	}
	c.l.mu.Lock()
	if *kept == nil {
		*kept = h
	}
	c.exitLocked()
	c.l.mu.Unlock()
	return h
}

// CloseResponse fences the stream and closes the response through
// Connect, once, which waits for the request to finish and closes its
// body; it then marks the call done. If no request was started, it closes
// nothing and none will be.
//
// It returns the first original CloseResponse's result if every frame of
// the call returned cleanly and each body closed; a pending *CallError
// while the first original CloseResponse, or a frame entered before it,
// is still running or a body is not closed; and a *CallError faulting the
// session if a frame did not settle. A later call reports the same, as of
// then, without closing anything again.
func (s *stream) CloseResponse() error {
	c := s.c
	l := c.l
	l.mu.Lock()
	if c.closing {
		defer l.mu.Unlock()
		return c.closeResultLocked()
	}
	// The stream is pending from its first CloseResponse until it drains:
	// counted here, before the original runs, so no request is admitted
	// while it does.
	c.closing, c.pending = true, true
	l.pending++
	if !c.requested {
		c.done = true
		defer l.mu.Unlock()
		return c.closeResultLocked()
	}
	c.enterLocked()
	l.mu.Unlock()
	err := c.conn.CloseResponse()
	l.mu.Lock()
	defer l.mu.Unlock()
	c.done, c.result = true, err
	c.active--
	return c.closeResultLocked()
}

// A refusedConn is a stream its session refused. It sends nothing; Send,
// CloseRequest and Receive return the refusal, which wraps no error and so
// cannot match io.EOF.
type refusedConn struct {
	spec   connect.Spec
	header http.Header
	err    error
}

func (r *refusedConn) Spec() connect.Spec { return r.spec }

func (r *refusedConn) Peer() connect.Peer { return connect.Peer{} }

func (r *refusedConn) RequestHeader() http.Header { return r.header }

func (r *refusedConn) Send(any) error { return r.err }

func (r *refusedConn) CloseRequest() error { return r.err }

func (r *refusedConn) Receive(any) error { return r.err }

func (r *refusedConn) ResponseHeader() http.Header { return make(http.Header) }

func (r *refusedConn) ResponseTrailer() http.Header { return make(http.Header) }

func (r *refusedConn) CloseResponse() error { return nil }
