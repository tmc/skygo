package sshagent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"

	"connectrpc.com/connect"
	"github.com/tmc/skygo/integrations/ssh/internal/agentssh"
)

// A ledger owns a session's Control requests. It admits each new RPC and
// each HTTP request one of them sends, records every original frame those
// requests enter, and keeps each call until all of its frames returned and
// each response body's first Close returned nil. A failed body Close, or a
// unary or transport frame that did not settle, faults the ledger for
// good: it admits nothing more and keeps every call that did not settle.
//
// A stream is pending from the moment its first CloseResponse starts
// until that original and every frame it entered returned and its body
// closed cleanly. While a stream is pending, the ledger admits no new RPC
// or HTTP request; once it drains cleanly, the ledger admits them again
// unless it was closed. A pending stream that faults stays counted. Open
// streams do not hold other requests back.
//
// A ledger never holds its mutex across I/O, never starts a goroutine and
// never retries or cancels a request.
type ledger struct {
	signer     io.Closer // closed once, after a clean drain; nil with no SSH agent
	signerOnce sync.Once

	mu        sync.Mutex
	fenced    bool  // Close was called: admit nothing more
	faulted   bool  // an original frame did not settle: admit nothing more
	pending   int   // pending streams: admit nothing until 0
	signerErr error // the signer's failed first Close
	live      map[*call]struct{}
}

func newLedger(signer io.Closer) *ledger {
	return &ledger{signer: signer, live: make(map[*call]struct{})}
}

// fenceLocked returns the error refusing every new RPC, HTTP request and
// stream Send once l is faulted or closed, or nil.
func (l *ledger) fenceLocked() error {
	switch {
	case l.faulted:
		return &CallError{kind: kindRefused, stages: stageFaulted, owner: l}
	case l.fenced:
		return &CallError{kind: kindRefused, stages: stageClosed, owner: l}
	}
	return nil
}

// refusalLocked returns the error refusing a new RPC or HTTP request, or
// nil if l admits one. Its error never wraps another, so it cannot match
// io.EOF, which Connect would hide or replace.
func (l *ledger) refusalLocked() error {
	if err := l.fenceLocked(); err != nil {
		return err
	}
	if l.pending > 0 {
		return &CallError{kind: kindRefused, stages: stagePendingStream, owner: l}
	}
	return nil
}

// admit registers a new call, with its RPC or stream constructor frame
// entered, or returns why l refuses it.
func (l *ledger) admit(stream bool) (*call, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.refusalLocked(); err != nil {
		return nil, err
	}
	c := &call{l: l, stream: stream, active: 1}
	l.live[c] = struct{}{}
	return c, nil
}

// close fences l and, if every call settled and none faulted, closes the
// signer once. Otherwise it returns a *CallError and leaves the signer
// open. See Session.Close.
func (l *ledger) close() error {
	l.mu.Lock()
	l.fenced = true
	if l.faulted || len(l.live) > 0 {
		defer l.mu.Unlock()
		return l.errorLocked()
	}
	l.mu.Unlock()
	// Concurrent callers wait for the first Close.
	l.signerOnce.Do(func() {
		if l.signer == nil {
			return
		}
		err := l.signer.Close()
		l.mu.Lock()
		defer l.mu.Unlock()
		if err != nil {
			l.signerErr, l.faulted = err, true
		}
	})
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.signerErr != nil {
		return l.errorLocked()
	}
	return nil
}

// errorLocked returns a snapshot of every call l still holds and of the
// signer's failed Close. The snapshot keeps those calls, and so their
// connections and bodies.
func (l *ledger) errorLocked() *CallError {
	e := &CallError{kind: kindClose, owner: l}
	for c := range l.live {
		e.calls = append(e.calls, c)
		e.stages |= c.stagesLocked()
		e.errs = append(e.errs, c.errsLocked()...)
	}
	if l.signerErr != nil {
		e.stages |= stageSigner
		e.errs = append(e.errs, l.signerErr)
	}
	return e
}

// WrapUnary admits each unary RPC before Connect builds its request and
// settles the call once Connect returns.
func (l *ledger) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		c, err := l.admit(false)
		if err != nil {
			return nil, err
		}
		resp, err := next(context.WithValue(ctx, callKey{}, c), req)
		return c.settle(resp, err)
	}
}

// WrapStreamingClient admits each stream before Connect builds it and
// returns a connection recording each original method frame. The call
// keeps Connect's connection from before the constructor frame returns,
// so the ledger and every error holding the call reach it. A refused
// stream sends nothing; its methods return the refusal.
func (l *ledger) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		c, err := l.admit(true)
		if err != nil {
			return &refusedConn{spec: spec, header: make(http.Header), err: err}
		}
		conn := next(context.WithValue(ctx, callKey{}, c), spec)
		l.mu.Lock()
		c.conn = conn
		c.exitLocked()
		l.mu.Unlock()
		return &stream{c: c}
	}
}

func (l *ledger) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

// A callKey is the context key of the *call that owns an HTTP request.
type callKey struct{}

// A call is one admitted RPC or stream. All fields after stream are
// guarded by l.mu, except conn, which is set before the stream is
// returned and never changed.
type call struct {
	l                                *ledger
	stream                           bool
	command                          bool // one canonical command; typed Control calls leave false
	reserved                         int  // command response/error record slots reserved before dispatch
	refusals                         []*refusedRequest
	refusalPending, refusalHighWater int
	commandRefused                   bool // first local refusal permanently fences this command
	cancel                           context.CancelFunc

	conn       connect.StreamingClientConn // stream: Connect's original connection
	active     int                         // entered original frames that had not returned
	requested  bool                        // stream: Send or CloseRequest may have started the request
	closing    bool                        // stream: the first CloseResponse started; no new frame enters
	done       bool                        // unary: Connect returned; stream: the first CloseResponse returned
	pending    bool                        // stream: counted in l.pending
	held       bool                        // unary: Connect returned while a frame still ran
	resp       any                         // unary: Connect's response, kept with an unsettled call
	result     error                       // unary: Connect's result; stream: the first CloseResponse's
	recv       error                       // stream: the first Receive error not matching io.EOF
	roundTrips []error                     // non-nil original RoundTrip errors
	bodies     []*body

	header, trailer http.Header // stream: the first result of each header method
}

func (c *call) enterLocked() {
	c.active++
	c.l.live[c] = struct{}{}
}

func (c *call) exitLocked() {
	c.active--
	c.releaseLocked()
}

func (c *call) exit() {
	c.l.mu.Lock()
	defer c.l.mu.Unlock()
	c.exitLocked()
}

// faultyLocked reports whether c holds a frame that did not settle: a
// failed body Close, a transport receipt of an unsettled connection, or a
// frame that ran on after Connect returned the unary result.
// commandFaultLocked keeps every original command read/transport failure.
func (c *call) commandFaultLocked() bool {
	if !c.command {
		return false
	}
	if c.commandRefused {
		return true
	}
	for _, r := range c.refusals {
		if r.err != nil || r.readErr != nil {
			return true
		}
	}
	if len(c.roundTrips) > 0 {
		return true
	}
	for _, b := range c.bodies {
		if b.readErr != nil {
			return true
		}
	}
	return false
}

// retireBodyLocked drops only positively closed, error-free command IO.
// Escaped wrappers retain their answer but can never enter underlying IO again.
func (c *call) retireBodyLocked(b *body) {
	if !c.command || !b.closed || b.err != nil || b.readErr != nil || b.active != 0 {
		return
	}
	for i, v := range c.bodies {
		if v == b {
			c.bodies = slices.Delete(c.bodies, i, i+1)
			b.retired = true
			b.rc = nil
			return
		}
	}
}

func (c *call) faultyLocked() bool {
	return c.held || c.stagesLocked()&(stageClose|stageTransport) != 0 || c.commandFaultLocked()
}

// unfinishedLocked reports whether a frame c entered is still running or
// a response body of c was not closed.
func (c *call) unfinishedLocked() bool {
	for _, r := range c.refusals {
		if !r.closed || r.active > 0 {
			return true
		}
	}
	if c.active > 0 {
		return true
	}
	for _, b := range c.bodies {
		if !b.closed {
			return true
		}
	}
	return false
}

// releaseLocked forgets c if it is done and every frame it entered
// returned cleanly, and stops counting it as pending. A faulty call is
// kept for good.
func (c *call) releaseLocked() {
	if !c.done || c.unfinishedLocked() || c.faultyLocked() {
		return
	}
	if c.pending {
		c.pending = false
		c.l.pending--
	}
	delete(c.l.live, c)
}

func (c *call) stagesLocked() stage {
	var s stage
	for _, r := range c.refusals {
		if !r.closed {
			s |= stageCommandRefusalPending
		}
		if r.err != nil {
			s |= stageRequestClose
		}
	}
	if c.commandFaultLocked() {
		s |= stageFaulted
	}
	if c.active > 0 {
		s |= stageRunning
	}
	if c.held {
		s |= stageHeld
	}
	if c.stream && !c.done {
		s |= stageOpen
	}
	errs := append(slices.Clip(c.roundTrips), c.result, c.recv)
	for _, b := range c.bodies {
		if b.err != nil {
			s |= stageClose
		}
		if c.done && !b.closing {
			s |= stageBodyOpen
		}
		errs = append(errs, b.readErr)
	}
	for _, err := range errs {
		if agentssh.OwnershipUnsettled(err) {
			s |= stageTransport
		}
	}
	return s
}

func (c *call) errsLocked() []error {
	var errs []error
	for _, r := range c.refusals {
		if r.err != nil {
			errs = append(errs, r.err)
		}
		if r.readErr != nil {
			errs = append(errs, r.readErr)
		}
	}
	if c.result != nil {
		errs = append(errs, c.result)
	}
	if c.recv != nil {
		errs = append(errs, c.recv)
	}
	errs = append(errs, c.roundTrips...)
	for _, b := range c.bodies {
		if b.readErr != nil {
			errs = append(errs, b.readErr)
		}
		if b.err != nil {
			errs = append(errs, b.err)
		}
	}
	return errs
}

func (c *call) errorLocked() *CallError {
	return &CallError{kind: kindCall, stages: c.stagesLocked(), errs: c.errsLocked(), owner: c}
}

// closeResultLocked returns what the stream's CloseResponse reports once
// its first call started: a *CallError faulting l if a frame of c did not
// settle; a pending *CallError while the first original CloseResponse, or
// another frame c entered, is running or a body is not closed; and
// otherwise the first original CloseResponse's result. The first
// CloseResponse counted c pending in l; releaseLocked stops counting it.
func (c *call) closeResultLocked() error {
	l := c.l
	if c.faultyLocked() {
		l.faulted = true
		return c.errorLocked()
	}
	if !c.done || c.unfinishedLocked() {
		return &CallError{kind: kindPending, stages: c.stagesLocked(), errs: c.errsLocked(), owner: c}
	}
	c.releaseLocked()
	return c.result
}

// settle closes, once, each response body of a returned unary call that
// nobody closed, and returns Connect's result if every frame of the call
// returned cleanly. Otherwise it faults the ledger and returns a
// *CallError. It does not wait for a Close running elsewhere.
func (c *call) settle(resp connect.AnyResponse, err error) (connect.AnyResponse, error) {
	l := c.l
	l.mu.Lock()
	c.resp, c.result = resp, err
	bodies := slices.Clone(c.bodies)
	requests := slices.Clone(c.refusals)
	l.mu.Unlock()
	for _, r := range requests {
		if r.start() {
			r.run()
		}
	}
	for _, b := range bodies {
		if b.start() {
			b.run()
		}
	}
	l.mu.Lock()
	c.active-- // the RPC frame
	c.done = true
	if c.active > 0 {
		c.held = true
	}
	if c.faultyLocked() {
		l.faulted = true
		held := c.errorLocked()
		cancel := c.stopCommandLocked()
		l.mu.Unlock()
		if c.command && cancel != nil {
			cancel()
		}
		return nil, held
	}
	c.releaseLocked()
	l.mu.Unlock()
	return resp, err
}

// A recorder is the transport of a session's control client. It sends a
// request only for a call its ledger admitted and still admits, through
// next, the session's transport, unchanged. It records each original
// RoundTrip error and wraps each response body before Connect sees either.
type recorder struct {
	l    *ledger
	next http.RoundTripper
}

var (
	errNoCall      = errors.New("sshagent: request has no session call")
	errNoTransport = errors.New("sshagent: session has no transport")
)

func (r *recorder) RoundTrip(req *http.Request) (*http.Response, error) {
	c, _ := req.Context().Value(callKey{}).(*call)
	if err := r.admit(c); err != nil {
		if c != nil && c.l == r.l && c.command {
			return nil, c.refuse(req, err)
		}
		if req.Body != nil {
			req.Body.Close()
		}
		return nil, err
	}

	resp, err := r.next.RoundTrip(req)
	r.l.mu.Lock()
	if c.command {
		c.reserved -= 2
	}
	if err != nil {
		c.roundTrips = append(c.roundTrips, err)
		if c.command {
			r.l.faulted = true
		}
		if agentssh.OwnershipUnsettled(err) {
			r.l.faulted = true
		}
	}
	if resp != nil && resp.Body != nil {
		b := &body{c: c, rc: resp.Body, done: make(chan struct{})}
		c.bodies = append(c.bodies, b)
		resp.Body = b
	}
	c.exitLocked()
	if c.command && err != nil {
		err = c.errorLocked()
	}
	var cancel context.CancelFunc
	poisoned := c.command && r.l.faulted
	if poisoned {
		cancel = c.stopCommandLocked()
	}
	r.l.mu.Unlock()
	if poisoned && cancel != nil {
		cancel()
	}
	return resp, err
}

// admit enters a RoundTrip frame of c, or returns why r refuses it. A
// stream whose CloseResponse started sends nothing more, even a request
// Connect started before.
func (r *recorder) admit(c *call) error {
	if c == nil || c.l != r.l {
		return errNoCall
	}
	if r.next == nil {
		return errNoTransport
	}
	r.l.mu.Lock()
	defer r.l.mu.Unlock()
	if err := r.l.refusalLocked(); err != nil {
		return err
	}
	if c.done || c.closing {
		return &CallError{kind: kindRefused, stages: stageStreamClosed, owner: c}
	}
	if c.command {
		if c.refusalPending > 0 {
			return &CallError{kind: kindRefused, stages: stageCommandRefusalPending, owner: c}
		}
		if len(c.bodies)+len(c.roundTrips)+c.reserved+2 > commandRecordLimit {
			return &CallError{kind: kindRefused, stages: stageCommandLimit, owner: c}
		}
		c.reserved += 2
	}
	c.enterLocked()
	return nil
}

// A CallError reports a Control request, or a Session.Close, whose
// connections the session could not show were settled; a stream whose
// close is still pending; or a request the session refused.
//
// A request did not settle if a unary frame of it was still running when
// it returned, if the first Close of a response body failed, or if one of
// its errors holds a receipt of an unsettled connection. That faults the
// session for good: it refuses every later request and HTTP send, and
// Close leaves the SSH agent connection open.
//
// A stream's close is pending from the moment its first CloseResponse
// starts while that original, or a Send, CloseRequest, Receive, header
// read, HTTP request or body Read or Close it entered earlier, is still
// running, or its body is not closed. That is not a fault. Until the
// stream drains, the session refuses every new request and HTTP send;
// once every such frame returned and the body closed cleanly, it admits
// them again, unless it was closed.
//
// A session refuses requests once it is closed, too; Close returns a
// CallError while any admitted request, stream or response body has not
// returned and closed.
//
// Error and every fmt verb print only fixed words naming what did not
// settle, never a cause, which may quote remote output or a credential.
// Unwrap returns the original causes as of the moment the error was made,
// for errors.Is and errors.As: Connect's unary result or first stream
// CloseResponse result, a stream's first Receive error not matching
// io.EOF, each RoundTrip error, each response body Read error other than
// io.EOF, and each failed body or SSH agent connection Close. A refusal
// wraps none.
//
// The error keeps its requests and session reachable for as long as the
// caller holds it: each request's Connect streaming connection, response
// and response bodies with it. It closes and joins nothing.
type CallError struct {
	kind   kind
	stages stage
	errs   []error
	owner  any     // *call or *ledger
	calls  []*call // a Session.Close snapshot: every call the session held
}

func (e *CallError) Error() string {
	s := e.kind.String()
	if w := e.stages.String(); w != "" {
		s += ": " + w
	}
	return s
}

// Format prints the fixed text of Error for every verb.
func (e *CallError) Format(f fmt.State, verb rune) {
	s := e.Error()
	if verb == 'q' {
		s = strconv.Quote(s)
	}
	io.WriteString(f, s)
}

func (e *CallError) Unwrap() []error { return slices.Clone(e.errs) }

type kind int

const (
	kindCall kind = iota
	kindClose
	kindRefused
	kindPending
)

func (k kind) String() string {
	switch k {
	case kindClose:
		return "sshagent: session did not close"
	case kindRefused:
		return "sshagent: request refused"
	case kindPending:
		return "sshagent: stream close pending"
	}
	return "sshagent: request did not settle"
}

// A stage is a set of fixed reasons a CallError reports.
type stage uint

const (
	stageRunning       stage = 1 << iota // a request, read or close had not returned
	stageOpen                            // a stream's response was not closed
	stageHeld                            // a request returned while its connection was in use
	stageClose                           // a response body's first Close failed
	stageTransport                       // an error holds an unsettled connection
	stageSigner                          // the SSH agent connection's Close failed
	stageStreamClosed                    // the stream's response was closed
	stageClosed                          // the session was closed
	stageFaulted                         // the session holds a request that did not settle
	stagePendingStream                   // the session holds a pending stream
	stageBodyOpen                        // a closed request's response body was not closed
	stageCommandLimit                    // command retained record bound would be exceeded
	stageCommandRefusalPending
	stageRequestClose
)

var stageWords = []string{
	"request still running",
	"stream response not closed",
	"request returned with its connection in use",
	"close response body failed",
	"connection did not settle",
	"close ssh agent connection failed",
	"stream closed",
	"session closed",
	"session holds a request that did not settle",
	"closed stream still pending",
	"response body not closed",
	"command response record limit",
	"refused request body close pending",
	"close refused request body failed",
}

func (s stage) String() string {
	var w []string
	for i, word := range stageWords {
		if s&(1<<i) != 0 {
			w = append(w, word)
		}
	}
	return strings.Join(w, ", ")
}

// Each command bounds retained body/error records and in-flight reservations.
const commandRecordLimit = 64
