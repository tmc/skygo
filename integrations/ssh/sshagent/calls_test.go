package sshagent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
)

const (
	refusedClosed  = "sshagent: request refused: session closed"
	refusedFaulted = "sshagent: request refused: session holds a request that did not settle"
	refusedPending = "sshagent: request refused: closed stream still pending"
	refusedStream  = "sshagent: request refused: stream closed"
)

// asCallError returns the *CallError err holds, failing t if there is
// none, if its text is not want (unless want is empty), or if err's text
// holds the secret.
func asCallError(t *testing.T, err error, want string) *CallError {
	t.Helper()
	var ce *CallError
	if !errors.As(err, &ce) {
		t.Fatalf("err = %v, want a *CallError", err)
	}
	if want != "" && ce.Error() != want {
		t.Errorf("CallError = %q, want %q", ce.Error(), want)
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("err = %q holds the secret", err.Error())
	}
	return ce
}

// ownStream returns the session's stream under an actual generated server
// stream, failing t if there is none.
func ownStream(t *testing.T, st *connect.ServerStreamForClient[v1.StreamJobLogsResponse]) *stream {
	t.Helper()
	conn, err := st.Conn()
	if err != nil {
		t.Fatal(err)
	}
	ours, ok := conn.(*stream)
	if !ok {
		t.Fatalf("stream connection = %T, want the session's *stream", conn)
	}
	return ours
}

// TestUnarySettle checks each settlement of an actual generated unary
// call whose MODEL response body fails to read or decode or reads cleanly,
// and whose first Close fails or succeeds. Connect discards the Close
// result after a read or decode failure; the session keeps it, faults,
// and sends no later request. A clean Close leaves the session usable,
// whatever Connect's result.
func TestUnarySettle(t *testing.T) {
	info := infoServed(t)
	errRead := &secretError{"modeled body read failure", secret}
	tests := []struct {
		name     string
		data     []byte
		readErr  error
		closeErr bool
		wantErr  bool // Connect itself returns an error
	}{
		{name: "decode failure, close failure", data: []byte{0xff}, closeErr: true, wantErr: true},
		{name: "read failure, close failure", readErr: errRead, closeErr: true, wantErr: true},
		{name: "valid response, close failure", data: info.body, closeErr: true, wantErr: true},
		{name: "decode failure, clean close", data: []byte{0xff}, wantErr: true},
		{name: "read failure, clean close", readErr: errRead, wantErr: true},
		{name: "valid response, clean close", data: info.body},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := newModelBody(tt.data)
			body.readErr = tt.readErr
			var closeErr error
			if tt.closeErr {
				closeErr = &secretError{"modeled body close failure", secret}
				body.closeErr = closeErr
			}
			next := sequence(info.reply(body))
			signer := &modelSigner{}
			s := newTestSession(signer, next)

			resp, err := s.Control().GetServerInfo(context.Background(), infoRequest())
			if body.closeCount() != 1 || next.count() != 1 {
				t.Fatalf("body Closes %d, requests %d; want 1 and 1", body.closeCount(), next.count())
			}
			_, serr := s.Control().SubmitJob(context.Background(), connect.NewRequest(&v1.SubmitJobRequest{}))

			if !tt.closeErr {
				if (err != nil) != tt.wantErr || errors.As(err, new(*CallError)) {
					t.Fatalf("GetServerInfo = %v, want Connect's own result", err)
				}
				if err == nil && (len(resp.Msg.Targets) != 1 || resp.Msg.Targets[0].Name != "dgx") {
					t.Errorf("GetServerInfo = %v", resp.Msg)
				}
				if tt.readErr != nil && !errors.Is(err, tt.readErr) {
					t.Errorf("GetServerInfo = %v, want the read failure", err)
				}
				// The next request is sent, and fails in the model.
				if !errors.Is(serr, errNoReply) || next.count() != 2 {
					t.Errorf("SubmitJob = %v after %d requests; want it sent", serr, next.count())
				}
				if err := s.Close(); err != nil || signer.count() != 1 || liveCalls(s) != 0 {
					t.Errorf("Close = %v, signer Closes %d, calls held %d; want nil, 1 and 0", err, signer.count(), liveCalls(s))
				}
				return
			}

			ce := asCallError(t, err, "sshagent: request did not settle: close response body failed")
			if resp != nil {
				t.Errorf("GetServerInfo returned a response with %v", err)
			}
			if !errors.Is(err, closeErr) || !errors.As(err, new(*connect.Error)) {
				t.Errorf("GetServerInfo = %v; want both Connect's result and the Close failure", err)
			}
			if tt.readErr != nil && !errors.Is(err, tt.readErr) {
				t.Errorf("GetServerInfo = %v; want the read failure too", err)
			}
			for _, format := range []string{"%v", "%s", "%q", "%+v", "%#v", "%x", "%d"} {
				if got := fmt.Sprintf(format, err); strings.Contains(got, secret) || !strings.Contains(got, "did not settle") {
					t.Errorf("Sprintf(%q) = %q", format, got)
				}
			}

			// The fault refuses the next request before it is sent.
			asCallError(t, serr, refusedFaulted)
			if errors.Is(serr, closeErr) || next.count() != 1 {
				t.Errorf("SubmitJob = %v after %d requests; want a refusal wrapping nothing, and no request", serr, next.count())
			}
			for range 2 {
				cerr := s.Close()
				asCallError(t, cerr, "sshagent: session did not close: close response body failed")
				if !errors.Is(cerr, closeErr) {
					t.Errorf("Close = %v, want the Close failure", cerr)
				}
			}
			if signer.count() != 0 || body.closeCount() != 1 || liveCalls(s) != 1 {
				t.Errorf("signer Closes %d, body Closes %d, calls held %d; want 0, 1 and 1", signer.count(), body.closeCount(), liveCalls(s))
			}
			if ce.owner == nil {
				t.Error("CallError holds no owner")
			}
		})
	}
}

// TestTransportEOFRewrite checks that a transport error wrapping io.EOF,
// which actual Connect replaces with io.ErrUnexpectedEOF, is kept as
// returned by the session while its call is held, and that such an error
// holding no ownership receipt settles like any returned error.
//
// It is a plain modeled error: an agentssh receipt of an unsettled
// connection, which faults the session, cannot be made outside agentssh,
// and one request does not both fail in its transport and hold a body.
func TestTransportEOFRewrite(t *testing.T) {
	cause := &secretError{"modeled transport failure", secret}
	original := errors.Join(io.EOF, cause)
	fail := func(*http.Request) (*http.Response, error) { return nil, original }

	// Unary: Connect returns its replacement; the call settled.
	next := sequence(fail)
	s := newTestSession(nil, next)
	_, err := s.Control().GetServerInfo(context.Background(), infoRequest())
	if !errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) || errors.As(err, new(*secretError)) || errors.As(err, new(*CallError)) {
		t.Fatalf("GetServerInfo = %v; want Connect's io.ErrUnexpectedEOF in place of the original", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close after a returned transport error = %v", err)
	}

	// Stream: the call is held until its response is closed, and Close
	// reaches the original error.
	signer := &modelSigner{}
	next = sequence(fail)
	s = newTestSession(signer, next)
	st, err := s.Control().StreamJobLogs(context.Background(), logsRequest())
	if err != nil {
		t.Fatal(err)
	}
	if st.Receive() {
		t.Fatal("Receive = true")
	}
	if err := st.Err(); !errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) || errors.As(err, new(*secretError)) {
		t.Errorf("Err = %v; want Connect's io.ErrUnexpectedEOF", err)
	}
	cerr := s.Close()
	asCallError(t, cerr, "sshagent: session did not close: stream response not closed")
	var got *secretError
	if !errors.Is(cerr, io.EOF) || !errors.As(cerr, &got) || got != cause {
		t.Errorf("Close = %v; want the original transport error", cerr)
	}
	if err := st.Close(); err != nil {
		t.Errorf("stream Close = %v", err)
	}
	if err := s.Close(); err != nil || signer.count() != 1 {
		t.Errorf("Close = %v after %d signer Closes; want nil after 1", err, signer.count())
	}
	if next.count() != 1 {
		t.Errorf("requests %d, want 1", next.count())
	}
}

// TestStreamEOFHoldsSession checks that an actual generated stream read to
// its end is still the session's: Close fences new requests at once,
// through a client obtained before it, with a key file too, but closes no
// SSH agent connection until the stream's response is closed.
func TestStreamEOFHoldsSession(t *testing.T) {
	logs := logsServed(t)
	for _, keyFile := range []bool{true, false} {
		t.Run(fmt.Sprintf("keyFile=%v", keyFile), func(t *testing.T) {
			signer := &modelSigner{}
			var closer io.Closer
			if !keyFile {
				closer = signer
			}
			body := newModelBody(logs.body)
			next := sequence(logs.reply(body))
			s := newTestSession(closer, next)
			cc := s.Control()

			st, err := cc.StreamJobLogs(context.Background(), logsRequest())
			if err != nil {
				t.Fatal(err)
			}
			n := 0
			for st.Receive() {
				n++
			}
			if n != 1 || st.Err() != nil {
				t.Fatalf("received %d messages, Err = %v; want 1 and nil", n, st.Err())
			}

			asCallError(t, s.Close(), "sshagent: session did not close: stream response not closed")
			if body.closeCount() != 0 || signer.count() != 0 {
				t.Errorf("body Closes %d, signer Closes %d; want 0 and 0", body.closeCount(), signer.count())
			}
			_, err = cc.GetServerInfo(context.Background(), infoRequest())
			asCallError(t, err, refusedClosed)
			if _, err := cc.StreamJobLogs(context.Background(), logsRequest()); errors.Is(err, io.EOF) {
				t.Errorf("StreamJobLogs after Close = %v, which matches io.EOF", err)
			} else {
				asCallError(t, err, refusedClosed)
			}
			if next.count() != 1 {
				t.Errorf("requests %d after Close, want 1", next.count())
			}

			if err := st.Close(); err != nil || body.closeCount() != 1 {
				t.Fatalf("stream Close = %v after %d body Closes", err, body.closeCount())
			}
			for range 2 {
				if err := s.Close(); err != nil {
					t.Errorf("Close = %v", err)
				}
			}
			if want := map[bool]int{true: 0, false: 1}[keyFile]; signer.count() != want {
				t.Errorf("signer Closes %d, want %d", signer.count(), want)
			}
			if _, err := cc.GetServerInfo(context.Background(), infoRequest()); next.count() != 1 {
				t.Errorf("GetServerInfo after Close = %v, sent", err)
			}
		})
	}
}

// TestStreamPendingFence checks the close of an actual generated stream A
// whose Receive is still reading its MODEL body. A's Close fences it and
// returns a pending CallError wrapping no io.EOF, which is no fault, and
// so does a repeat, which closes nothing again. While A is pending the
// session refuses a new request unsent, though open stream B held none
// back. Once A's Receive returned, the session admits requests again,
// unless Close was called, which fences it for good; A's Close then
// returns the first original result, and the session's Close closes the
// signer once.
//
// C1's TestStreamReceiveHoldsClose wanted A's Close to return nil while
// its Receive still ran; root's close policy replaces that expectation.
func TestStreamPendingFence(t *testing.T) {
	info, logs := infoServed(t), logsServed(t)
	for _, closeSession := range []bool{false, true} {
		t.Run(fmt.Sprintf("closeSession=%v", closeSession), func(t *testing.T) {
			o := newOwner(t)
			bodyA, bodyB := newModelBody(logs.body), newModelBody(logs.body)
			gateA := o.gate()
			bodyA.readGate = gateA.c
			next := sequence(
				logs.reply(bodyB),
				info.reply(newModelBody(info.body)),
				logs.reply(bodyA),
				info.reply(newModelBody(info.body)),
			)
			signer := &modelSigner{}
			s := newTestSession(signer, next)
			cc := s.Control()
			ctx := context.Background()

			// B, read to its end and left open, holds no request back.
			stB, err := cc.StreamJobLogs(ctx, logsRequest())
			if err != nil {
				t.Fatal(err)
			}
			for stB.Receive() {
			}
			if err := stB.Err(); err != nil {
				t.Fatal(err)
			}
			if _, err := cc.GetServerInfo(ctx, infoRequest()); err != nil {
				t.Fatalf("GetServerInfo with stream B open = %v", err)
			}

			stA, err := cc.StreamJobLogs(ctx, logsRequest())
			if err != nil {
				t.Fatal(err)
			}
			r := o.goRun("A Receive", func() { stA.Receive() })
			await(t, bodyA.readIn, "A's first body Read")

			for range 2 {
				err := stA.Close()
				asCallError(t, err, "sshagent: stream close pending: request still running")
				if errors.Is(err, io.EOF) {
					t.Errorf("A Close = %v, which matches io.EOF", err)
				}
			}
			if bodyA.closeCount() != 1 {
				t.Errorf("A body Closes %d, want 1", bodyA.closeCount())
			}
			_, err = cc.GetServerInfo(ctx, infoRequest())
			asCallError(t, err, refusedPending)
			if errors.Is(err, io.EOF) || next.count() != 3 {
				t.Errorf("GetServerInfo = %v after %d requests; want a refusal not matching io.EOF, unsent", err, next.count())
			}
			if closeSession {
				asCallError(t, s.Close(), "sshagent: session did not close: request still running, stream response not closed")
				if signer.count() != 0 {
					t.Errorf("signer closed while A was pending")
				}
			}

			gateA.release()
			join(t, r)
			_, err = cc.GetServerInfo(ctx, infoRequest())
			if closeSession {
				asCallError(t, err, refusedClosed)
			} else if err != nil {
				t.Errorf("GetServerInfo after A drained = %v", err)
			}
			if err := stA.Close(); err != nil {
				t.Errorf("A Close after it drained = %v", err)
			}
			if err := stB.Close(); err != nil {
				t.Errorf("B Close = %v", err)
			}
			if err := s.Close(); err != nil || signer.count() != 1 || liveCalls(s) != 0 {
				t.Errorf("Close = %v, signer Closes %d, calls held %d; want nil, 1 and 0", err, signer.count(), liveCalls(s))
			}
			if want := map[bool]int{false: 4, true: 3}[closeSession]; next.count() != want || bodyA.closeCount() != 1 || bodyB.closeCount() != 1 {
				t.Errorf("requests %d, body Closes %d and %d; want %d, 1 and 1", next.count(), bodyA.closeCount(), bodyB.closeCount(), want)
			}
		})
	}
}

// TestStreamClosedNoReentry checks that an actual generated stream closed
// before it was read, in a session then closed cleanly, starts no
// original frame again: Receive returns a refusal not matching io.EOF,
// the header methods return what they returned before, or nothing,
// without reading, and Close closes nothing again and returns the first
// result. The session holds no call again and sends nothing more.
func TestStreamClosedNoReentry(t *testing.T) {
	logs := logsServed(t)
	body := newModelBody(logs.body)
	next := sequence(logs.reply(body))
	signer := &modelSigner{}
	s := newTestSession(signer, next)
	st, err := s.Control().StreamJobLogs(context.Background(), logsRequest())
	if err != nil {
		t.Fatal(err)
	}
	if h := st.ResponseHeader(); h.Get("Content-Type") != "application/connect+proto" {
		t.Fatalf("ResponseHeader = %v", h)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil || signer.count() != 1 {
		t.Fatalf("Close = %v after %d signer Closes; want nil after 1", err, signer.count())
	}
	reads := body.readCount()

	if st.Receive() {
		t.Error("Receive after Close = true")
	}
	if err := st.Err(); err == nil || errors.Is(err, io.EOF) {
		t.Errorf("Err = %v; want a refusal not matching io.EOF", err)
	} else {
		asCallError(t, err, refusedStream)
	}
	if h := st.ResponseHeader(); h.Get("Content-Type") != "application/connect+proto" {
		t.Errorf("ResponseHeader after Close = %v, want the one read before", h)
	}
	if h := st.ResponseTrailer(); len(h) != 0 {
		t.Errorf("ResponseTrailer after Close = %v, want none: none was read", h)
	}
	for range 2 {
		if err := st.Close(); err != nil {
			t.Errorf("repeated stream Close = %v", err)
		}
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close = %v", err)
	}
	if body.readCount() != reads || body.closeCount() != 1 || next.count() != 1 || signer.count() != 1 || liveCalls(s) != 0 {
		t.Errorf("body Reads %d (was %d), body Closes %d, requests %d, signer Closes %d, calls held %d; want %d, 1, 1, 1 and 0",
			body.readCount(), reads, body.closeCount(), next.count(), signer.count(), liveCalls(s), reads)
	}
}

// TestStreamFence checks, over a MODEL connection admitted through the
// actual interceptor, that the first CloseResponse fences every original
// method; that a header read entered before it finishes, keeping the
// close pending and new requests refused until it does; and that later
// header reads return the header it returned without the original.
func TestStreamFence(t *testing.T) {
	o := newOwner(t)
	l := newLedger(nil)
	g := o.gate()
	conn := &modelConn{
		spec:       connect.Spec{StreamType: connect.StreamTypeBidi},
		header:     http.Header{"X-Model": {"1"}},
		headerIn:   make(chan struct{}),
		headerGate: g.c,
	}
	st := modelStream(t, l, conn)
	if err := st.Send(nil); err != nil {
		t.Fatal(err)
	}
	var got http.Header
	r := o.goRun("ResponseHeader", func() { got = st.ResponseHeader() })
	await(t, conn.headerIn, "the header read")

	asCallError(t, st.CloseResponse(), "sshagent: stream close pending: request still running")
	if _, err := l.admit(false); err == nil {
		t.Error("admitted a call while a closed stream was pending")
	} else {
		asCallError(t, err, refusedPending)
	}
	for name, err := range map[string]error{
		"Send":         st.Send(nil),
		"CloseRequest": st.CloseRequest(),
		"Receive":      st.Receive(nil),
	} {
		if errors.Is(err, io.EOF) {
			t.Errorf("%s after CloseResponse = %v, which matches io.EOF", name, err)
		}
		asCallError(t, err, refusedStream)
	}
	if h := st.ResponseTrailer(); len(h) != 0 {
		t.Errorf("ResponseTrailer = %v, want none", h)
	}
	if h := st.ResponseHeader(); len(h) != 0 {
		t.Errorf("ResponseHeader while the first still runs = %v, want none", h)
	}

	g.release()
	join(t, r)
	if got.Get("X-Model") != "1" {
		t.Errorf("ResponseHeader entered before the fence = %v", got)
	}
	l.mu.Lock()
	live, pending := len(l.live), l.pending
	l.mu.Unlock()
	if live != 0 || pending != 0 {
		t.Errorf("calls held %d, pending %d after the stream drained; want 0 and 0", live, pending)
	}
	c, err := l.admit(false)
	if err != nil {
		t.Fatalf("admit after the stream drained = %v", err)
	}
	if _, err := c.settle(nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := st.CloseResponse(); err != nil {
		t.Errorf("CloseResponse after the drain = %v", err)
	}
	if h := st.ResponseHeader(); h.Get("X-Model") != "1" {
		t.Errorf("ResponseHeader after the drain = %v, want the one returned", h)
	}
	want := map[string]int{"Send": 1, "CloseRequest": 0, "Receive": 0, "ResponseHeader": 1, "ResponseTrailer": 0, "CloseResponse": 1}
	for method, n := range want {
		if conn.count(method) != n {
			t.Errorf("original %s calls %d, want %d", method, conn.count(method), n)
		}
	}
	if err := (&Session{calls: l}).Close(); err != nil {
		t.Errorf("Close = %v", err)
	}
}

// TestStreamKeepsConn checks that the ledger keeps the connection built
// under the actual interceptor, here a MODEL, before the stream is
// returned, unstarted or pending, and that Session.Close's and the
// pending CallError hold its call and so that connection. It inspects the
// object graph directly; it shows no closure and no join.
func TestStreamKeepsConn(t *testing.T) {
	o := newOwner(t)

	l := newLedger(nil)
	var inside *call
	conn := &modelConn{spec: connect.Spec{StreamType: connect.StreamTypeBidi}}
	sc := l.WrapStreamingClient(func(ctx context.Context, _ connect.Spec) connect.StreamingClientConn {
		inside, _ = ctx.Value(callKey{}).(*call)
		return conn
	})(context.Background(), conn.spec)
	st, ok := sc.(*stream)
	if !ok {
		t.Fatalf("WrapStreamingClient = %T, want an admitted *stream", sc)
	}
	if inside == nil || st.c != inside || st.c.conn != conn {
		t.Fatalf("stream call %p, constructor call %p; want one call keeping the model", st.c, inside)
	}
	l.mu.Lock()
	_, held := l.live[st.c]
	l.mu.Unlock()
	if !held {
		t.Error("ledger does not hold the unstarted stream's call")
	}
	ce := asCallError(t, (&Session{calls: l}).Close(), "sshagent: session did not close: stream response not closed")
	if len(ce.calls) != 1 || ce.calls[0] != st.c || ce.calls[0].conn != conn {
		t.Errorf("Close error holds %d calls; want the stream's, keeping the model", len(ce.calls))
	}

	l = newLedger(nil)
	g := o.gate()
	conn = &modelConn{spec: connect.Spec{StreamType: connect.StreamTypeBidi}, headerIn: make(chan struct{}), headerGate: g.c}
	st = modelStream(t, l, conn)
	if err := st.Send(nil); err != nil {
		t.Fatal(err)
	}
	r := o.goRun("ResponseHeader", func() { st.ResponseHeader() })
	await(t, conn.headerIn, "the header read")
	pe := asCallError(t, st.CloseResponse(), "sshagent: stream close pending: request still running")
	if c, _ := pe.owner.(*call); c != st.c || c.conn != conn {
		t.Errorf("pending error owner %T; want the stream's call, keeping the model", pe.owner)
	}
	ce = asCallError(t, (&Session{calls: l}).Close(), "sshagent: session did not close: request still running")
	if len(ce.calls) != 1 || ce.calls[0] != st.c || ce.calls[0].conn != conn {
		t.Errorf("Close error holds %d calls; want the pending stream's, keeping the model", len(ce.calls))
	}
	g.release()
	join(t, r)
}

// TestStreamCloseFaultSticky checks an actual generated stream whose MODEL
// body's first Close fails, run by the stream's Close at once or held
// while a repeat Close returns pending. The failure faults the session
// for good; every later Close of the stream reports it again without
// closing anything; and the errors and session keep the call with
// Connect's own connection.
func TestStreamCloseFaultSticky(t *testing.T) {
	logs := logsServed(t)
	for _, held := range []bool{false, true} {
		t.Run(fmt.Sprintf("held=%v", held), func(t *testing.T) {
			o := newOwner(t)
			closeErr := &secretError{"modeled stream body close failure", secret}
			body := newModelBody(logs.body)
			body.closeErr = closeErr
			g := o.gate()
			if held {
				body.gate = g.c
			}
			next := sequence(logs.reply(body))
			signer := &modelSigner{}
			s := newTestSession(signer, next)
			ctx := context.Background()
			st, err := s.Control().StreamJobLogs(ctx, logsRequest())
			if err != nil {
				t.Fatal(err)
			}
			for st.Receive() {
			}
			ours := ownStream(t, st)
			if _, own := ours.c.conn.(*stream); ours.c.conn == nil || own {
				t.Fatalf("call keeps connection %T, want Connect's", ours.c.conn)
			}

			var first error
			if held {
				r := o.goRun("first Close", func() { first = st.Close() })
				await(t, body.entered, "the body's first Close")
				asCallError(t, st.Close(), "sshagent: stream close pending: request still running, stream response not closed")
				g.release()
				join(t, r)
			} else {
				first = st.Close()
			}
			ce := asCallError(t, first, "sshagent: request did not settle: close response body failed")
			if !errors.Is(first, closeErr) || ce.owner != any(ours.c) {
				t.Errorf("Close = %v; want the body's Close failure, holding the stream's call", first)
			}
			for range 2 {
				err := st.Close()
				asCallError(t, err, "sshagent: request did not settle: close response body failed")
				if !errors.Is(err, closeErr) {
					t.Errorf("repeated Close = %v, want the first failure", err)
				}
			}
			_, err = s.Control().GetServerInfo(ctx, infoRequest())
			asCallError(t, err, refusedFaulted)
			cerr := s.Close()
			cce := asCallError(t, cerr, "sshagent: session did not close: close response body failed")
			if !errors.Is(cerr, closeErr) || len(cce.calls) != 1 || cce.calls[0] != ours.c {
				t.Errorf("Close = %v; want the failure, holding the stream's call", cerr)
			}
			if body.closeCount() != 1 || signer.count() != 0 || next.count() != 1 || liveCalls(s) != 1 {
				t.Errorf("body Closes %d, signer Closes %d, requests %d, calls held %d; want 1, 0, 1 and 1",
					body.closeCount(), signer.count(), next.count(), liveCalls(s))
			}
		})
	}
}

// TestStreamCloseFenceHeld checks an actual generated stream A whose
// MODEL body's first Close is held and then returns nil or fails. From the
// moment A's first Close starts, before it returns and before any repeat,
// the session refuses a new request, and a request it admitted earlier
// and paused in its cookie jar, each unsent with a refusal not matching
// io.EOF. A repeated Close reports A pending and closes nothing again. A
// clean drain admits requests again unless Session.Close was called while
// A was pending, which fences the session for good and closes the signer
// only after the drain. A failed Close keeps its cause and faults the
// session for good, leaving the signer open.
func TestStreamCloseFenceHeld(t *testing.T) {
	info, logs := infoServed(t), logsServed(t)
	tests := []struct {
		name         string
		closeFail    bool
		closeSession bool
	}{
		{"clean close", false, false},
		{"clean close, session closed while pending", false, true},
		{"failed close", true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := newOwner(t)
			closeErr := &secretError{"modeled held stream close failure", secret}
			bodyA := newModelBody(logs.body)
			gateA := o.gate()
			bodyA.gate = gateA.c
			if tt.closeFail {
				bodyA.closeErr = closeErr
			}
			jar := newPauseJar(o, skyv1connect.ControlServiceGetJobProcedure)
			next := sequence(logs.reply(bodyA), info.reply(newModelBody(info.body)))
			signer := &modelSigner{}
			s, _ := newJarSession(signer, next, jar)
			cc := s.Control()
			ctx := context.Background()

			stA, err := cc.StreamJobLogs(ctx, logsRequest())
			if err != nil {
				t.Fatal(err)
			}
			for stA.Receive() {
			}
			if err := stA.Err(); err != nil {
				t.Fatal(err)
			}
			ours := ownStream(t, stA)

			// C is admitted while A is open and paused before its send.
			var eC error
			rC := o.goRun("C", func() {
				_, eC = cc.GetJob(ctx, connect.NewRequest(&v1.GetJobRequest{}))
			})
			await(t, jar.entered, "C's request to pause")

			var first error
			rA := o.goRun("A first Close", func() { first = stA.Close() })
			await(t, bodyA.entered, "A's first body Close")

			// A's first Close is held: no repeat has run yet.
			_, err = cc.GetServerInfo(ctx, infoRequest())
			if errors.Is(err, io.EOF) {
				t.Errorf("GetServerInfo = %v, which matches io.EOF", err)
			}
			asCallError(t, err, refusedPending)
			jar.release.release()
			join(t, rC)
			if errors.Is(eC, io.EOF) {
				t.Errorf("GetJob = %v, which matches io.EOF", eC)
			}
			asCallError(t, eC, refusedPending)
			if next.count() != 1 {
				t.Errorf("requests %d while A's Close was held, want 1", next.count())
			}

			for range 2 {
				err := stA.Close()
				asCallError(t, err, "sshagent: stream close pending: request still running, stream response not closed")
				if errors.Is(err, io.EOF) {
					t.Errorf("repeated Close = %v, which matches io.EOF", err)
				}
			}
			if bodyA.closeCount() != 1 {
				t.Errorf("body Closes %d while the first was held, want 1", bodyA.closeCount())
			}
			if tt.closeSession {
				asCallError(t, s.Close(), "sshagent: session did not close: request still running, stream response not closed")
				if signer.count() != 0 {
					t.Errorf("signer closed while A was pending")
				}
			}

			gateA.release()
			join(t, rA)

			if !tt.closeFail {
				if first != nil {
					t.Errorf("A first Close = %v, want nil", first)
				}
				_, err = cc.GetServerInfo(ctx, infoRequest())
				if tt.closeSession {
					asCallError(t, err, refusedClosed)
				} else if err != nil {
					t.Errorf("GetServerInfo after A drained = %v", err)
				}
				if err := stA.Close(); err != nil {
					t.Errorf("A Close after it drained = %v", err)
				}
				if err := s.Close(); err != nil || signer.count() != 1 || liveCalls(s) != 0 {
					t.Errorf("Close = %v, signer Closes %d, calls held %d; want nil, 1 and 0", err, signer.count(), liveCalls(s))
				}
				if want := map[bool]int{false: 2, true: 1}[tt.closeSession]; next.count() != want || bodyA.closeCount() != 1 {
					t.Errorf("requests %d, body Closes %d; want %d and 1", next.count(), bodyA.closeCount(), want)
				}
				return
			}

			ce := asCallError(t, first, "sshagent: request did not settle: close response body failed")
			if !errors.Is(first, closeErr) || ce.owner != any(ours.c) {
				t.Errorf("A first Close = %v; want the held Close failure, holding A's call", first)
			}
			for range 2 {
				err := stA.Close()
				asCallError(t, err, "sshagent: request did not settle: close response body failed")
				if !errors.Is(err, closeErr) {
					t.Errorf("repeated Close = %v, want the first failure", err)
				}
			}
			_, err = cc.GetServerInfo(ctx, infoRequest())
			asCallError(t, err, refusedFaulted)
			for range 2 {
				cerr := s.Close()
				asCallError(t, cerr, "sshagent: session did not close: close response body failed")
				if !errors.Is(cerr, closeErr) {
					t.Errorf("Close = %v, want the held Close failure", cerr)
				}
			}
			if bodyA.closeCount() != 1 || signer.count() != 0 || next.count() != 1 || liveCalls(s) != 1 {
				t.Errorf("body Closes %d, signer Closes %d, requests %d, calls held %d; want 1, 0, 1 and 1",
					bodyA.closeCount(), signer.count(), next.count(), liveCalls(s))
			}
		})
	}
}

// TestClosedStreamsPending checks two actual generated streams A and B,
// each read to its end. While both are open they hold no request back.
// Once both first Closes are held over their MODEL bodies, the session
// refuses new requests; A's clean drain does not admit them while B is
// pending, nor close the signer. Once B drained too, the session admits
// requests again, unless Session.Close was called while they were
// pending; its Close then closes the signer once.
func TestClosedStreamsPending(t *testing.T) {
	info, logs := infoServed(t), logsServed(t)
	for _, closeSession := range []bool{false, true} {
		t.Run(fmt.Sprintf("closeSession=%v", closeSession), func(t *testing.T) {
			o := newOwner(t)
			bodyA, bodyB := newModelBody(logs.body), newModelBody(logs.body)
			gateA, gateB := o.gate(), o.gate()
			bodyA.gate, bodyB.gate = gateA.c, gateB.c
			next := sequence(
				logs.reply(bodyA),
				logs.reply(bodyB),
				info.reply(newModelBody(info.body)),
				info.reply(newModelBody(info.body)),
			)
			signer := &modelSigner{}
			s := newTestSession(signer, next)
			cc := s.Control()
			ctx := context.Background()

			var sts []*connect.ServerStreamForClient[v1.StreamJobLogsResponse]
			for range 2 {
				st, err := cc.StreamJobLogs(ctx, logsRequest())
				if err != nil {
					t.Fatal(err)
				}
				for st.Receive() {
				}
				if err := st.Err(); err != nil {
					t.Fatal(err)
				}
				sts = append(sts, st)
			}
			stA, stB := sts[0], sts[1]
			if _, err := cc.GetServerInfo(ctx, infoRequest()); err != nil {
				t.Fatalf("GetServerInfo with streams A and B open = %v", err)
			}

			var firstA, firstB error
			rA := o.goRun("A first Close", func() { firstA = stA.Close() })
			await(t, bodyA.entered, "A's first body Close")
			rB := o.goRun("B first Close", func() { firstB = stB.Close() })
			await(t, bodyB.entered, "B's first body Close")

			refused := refusedPending
			if closeSession {
				asCallError(t, s.Close(), "")
				refused = refusedClosed
			}
			_, err := cc.GetServerInfo(ctx, infoRequest())
			asCallError(t, err, refused)

			gateA.release()
			join(t, rA)
			if firstA != nil {
				t.Errorf("A first Close = %v, want nil", firstA)
			}
			_, err = cc.GetServerInfo(ctx, infoRequest())
			if errors.Is(err, io.EOF) {
				t.Errorf("GetServerInfo = %v, which matches io.EOF", err)
			}
			asCallError(t, err, refused)
			if closeSession {
				asCallError(t, s.Close(), "")
			}
			if signer.count() != 0 || liveCalls(s) != 1 {
				t.Errorf("signer Closes %d, calls held %d while B was pending; want 0 and 1", signer.count(), liveCalls(s))
			}

			gateB.release()
			join(t, rB)
			if firstB != nil {
				t.Errorf("B first Close = %v, want nil", firstB)
			}
			_, err = cc.GetServerInfo(ctx, infoRequest())
			if closeSession {
				asCallError(t, err, refusedClosed)
			} else if err != nil {
				t.Errorf("GetServerInfo after A and B drained = %v", err)
			}
			if err := s.Close(); err != nil || signer.count() != 1 || liveCalls(s) != 0 {
				t.Errorf("Close = %v, signer Closes %d, calls held %d; want nil, 1 and 0", err, signer.count(), liveCalls(s))
			}
			if want := map[bool]int{false: 4, true: 3}[closeSession]; next.count() != want || bodyA.closeCount() != 1 || bodyB.closeCount() != 1 {
				t.Errorf("requests %d, body Closes %d and %d; want %d, 1 and 1", next.count(), bodyA.closeCount(), bodyB.closeCount(), want)
			}
		})
	}
}

// TestUnrequestedStreamPending checks, over a MODEL connection admitted
// through the actual interceptor, a stream that sent nothing and whose
// header read entered before its first CloseResponse. That CloseResponse
// counts the stream pending once, at once; a repeat does not count it
// again; and new calls are refused until the header read returned, when
// the stream is released without any original CloseResponse.
func TestUnrequestedStreamPending(t *testing.T) {
	o := newOwner(t)
	l := newLedger(nil)
	g := o.gate()
	conn := &modelConn{
		spec:       connect.Spec{StreamType: connect.StreamTypeBidi},
		headerIn:   make(chan struct{}),
		headerGate: g.c,
	}
	st := modelStream(t, l, conn)
	r := o.goRun("ResponseHeader", func() { st.ResponseHeader() })
	await(t, conn.headerIn, "the header read")

	for range 2 {
		asCallError(t, st.CloseResponse(), "sshagent: stream close pending: request still running")
		l.mu.Lock()
		pending := l.pending
		l.mu.Unlock()
		if pending != 1 {
			t.Errorf("pending %d, want 1", pending)
		}
		if _, err := l.admit(false); err == nil {
			t.Error("admitted a call while a closed stream was pending")
		} else {
			asCallError(t, err, refusedPending)
		}
	}

	g.release()
	join(t, r)
	l.mu.Lock()
	live, pending := len(l.live), l.pending
	l.mu.Unlock()
	if live != 0 || pending != 0 {
		t.Errorf("calls held %d, pending %d after the stream drained; want 0 and 0", live, pending)
	}
	c, err := l.admit(false)
	if err != nil {
		t.Fatalf("admit after the stream drained = %v", err)
	}
	if _, err := c.settle(nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := st.CloseResponse(); err != nil {
		t.Errorf("CloseResponse after the drain = %v", err)
	}
	if conn.count("CloseResponse") != 0 || conn.count("Send") != 0 {
		t.Errorf("original CloseResponse %d, Send %d; want 0 and 0", conn.count("CloseResponse"), conn.count("Send"))
	}
	if err := (&Session{calls: l}).Close(); err != nil {
		t.Errorf("Close = %v", err)
	}
}

// TestStreamReceiveFailureKept checks an actual generated stream whose
// MODEL body fails after its first message, in place of its end, with a
// failed or clean first Close, and a clean twin. Receive and Err report
// Connect's own result unchanged; the stream's call keeps it, and the
// body's Read failure, so the stream's failed Close and the session's
// Close reach both originals. A clean Close releases the stream and
// leaves the session usable.
func TestStreamReceiveFailureKept(t *testing.T) {
	info, logs := infoServed(t), logsServed(t)
	tests := []struct {
		name                string
		readFail, closeFail bool
	}{
		{"read failure, close failure", true, true},
		{"read failure, clean close", true, false},
		{"clean read, clean close", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			readErr := &secretError{"modeled stream read failure", secret}
			closeErr := &secretError{"modeled stream close failure", secret}
			body := newModelBody(logs.body)
			if tt.readFail {
				body = newModelBody(firstMessage(t, logs.body))
				body.readErr = readErr
			}
			if tt.closeFail {
				body.closeErr = closeErr
			}
			next := sequence(logs.reply(body), info.reply(newModelBody(info.body)))
			signer := &modelSigner{}
			s := newTestSession(signer, next)
			ctx := context.Background()
			st, err := s.Control().StreamJobLogs(ctx, logsRequest())
			if err != nil {
				t.Fatal(err)
			}
			n := 0
			for st.Receive() {
				n++
			}
			ours := ownStream(t, st)
			ours.c.l.mu.Lock()
			kept := ours.c.recv
			ours.c.l.mu.Unlock()
			if n != 1 {
				t.Errorf("received %d messages, want 1", n)
			}
			if !tt.readFail {
				if st.Err() != nil || kept != nil {
					t.Errorf("Err = %v, kept %v; want nil and nil", st.Err(), kept)
				}
			} else if err := st.Err(); err == nil || errors.Is(err, io.EOF) || err != kept {
				t.Errorf("Err = %v, kept %v; want Connect's own failure, kept unchanged", err, kept)
			}

			cerr := st.Close()
			if tt.closeFail {
				asCallError(t, cerr, "sshagent: request did not settle: close response body failed")
				scerr := s.Close()
				asCallError(t, scerr, "sshagent: session did not close: close response body failed")
				for _, err := range []error{cerr, scerr} {
					if !errors.Is(err, closeErr) || !errors.Is(err, readErr) || !errors.Is(err, kept) {
						t.Errorf("Close = %v; want the Close failure, the body Read failure and Receive's result", err)
					}
				}
				if signer.count() != 0 {
					t.Errorf("signer Closes %d, want 0", signer.count())
				}
				return
			}
			if cerr != nil {
				t.Fatalf("stream Close = %v", cerr)
			}
			if _, err := s.Control().GetServerInfo(ctx, infoRequest()); err != nil {
				t.Errorf("GetServerInfo after a clean stream Close = %v", err)
			}
			if err := s.Close(); err != nil || signer.count() != 1 || liveCalls(s) != 0 {
				t.Errorf("Close = %v, signer Closes %d, calls held %d; want nil, 1 and 0", err, signer.count(), liveCalls(s))
			}
		})
	}
}

// TestConcurrentStreams checks that actual generated streams opened
// concurrently over MODEL bodies hold none another back: each of them
// receives its first message, its request sent and its response returned,
// before any is closed. They are then read to their end and closed
// concurrently, and each settles cleanly; the session then closes its
// signer once. A closed stream's pending close refuses new requests, so
// no stream closes until every one opened.
func TestConcurrentStreams(t *testing.T) {
	o := newOwner(t)
	logs := logsServed(t)
	var mu sync.Mutex
	var bodies []*modelBody
	next := &modelTransport{reply: func(_ int, req *http.Request) (*http.Response, error) {
		b := newModelBody(logs.body)
		mu.Lock()
		bodies = append(bodies, b)
		mu.Unlock()
		return logs.reply(b)(req)
	}}
	signer := &modelSigner{}
	s := newTestSession(signer, next)
	start, closing := o.gate(), o.gate()
	const streams = 8
	var errs [streams]error
	var counts [streams]int
	var opened [streams]chan struct{} // closed once stream i received or failed
	var runs []*run
	for i := range streams {
		opened[i] = make(chan struct{})
		runs = append(runs, o.goRun(fmt.Sprintf("stream %d", i), func() {
			var once sync.Once
			reached := func() { once.Do(func() { close(opened[i]) }) }
			defer reached()
			<-start.c
			st, err := s.Control().StreamJobLogs(context.Background(), logsRequest())
			if err != nil {
				errs[i] = err
				return
			}
			n := 0
			if st.Receive() {
				n++
			}
			reached()
			<-closing.c
			for st.Receive() {
				n++
			}
			counts[i] = n
			if err := errors.Join(st.Err(), st.Close()); err != nil {
				errs[i] = err
			}
		}))
	}
	start.release()

	// Every stream opened, with its response, before any closes. On a
	// failure here the owner releases closing and joins every stream.
	timer := time.NewTimer(ownerGuard)
	defer timer.Stop()
	for i := range streams {
		select {
		case <-opened[i]:
		case <-timer.C:
			t.Fatalf("waited %v for every stream to open", ownerGuard)
		}
	}
	for i := range streams {
		// Stream i writes errs[i] after opened[i] only once closing is
		// released.
		if errs[i] != nil {
			t.Fatalf("stream %d did not open: %v", i, errs[i])
		}
	}
	if n, held := next.count(), liveCalls(s); n != streams || held != streams {
		t.Fatalf("requests %d, calls held %d before any Close; want %d and %d", n, held, streams, streams)
	}
	s.calls.mu.Lock()
	pending := s.calls.pending
	s.calls.mu.Unlock()
	if pending != 0 {
		t.Fatalf("pending %d before any Close, want 0", pending)
	}

	closing.release()
	for _, r := range runs {
		join(t, r)
	}
	for i := range streams {
		if errs[i] != nil || counts[i] != 1 {
			t.Errorf("stream %d: %v after %d messages; want nil after 1", i, errs[i], counts[i])
		}
	}
	if err := s.Close(); err != nil || signer.count() != 1 || liveCalls(s) != 0 {
		t.Errorf("Close = %v, signer Closes %d, calls held %d; want nil, 1 and 0", err, signer.count(), liveCalls(s))
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != streams {
		t.Errorf("%d requests, want %d", len(bodies), streams)
	}
	for _, b := range bodies {
		if b.closeCount() != 1 {
			t.Errorf("body closed %d times, want 1", b.closeCount())
		}
	}
}

// TestStreamHeaderHoldsClose checks that a header read waiting for a
// stream's response, whose request Connect sends from a goroutine that
// CloseRequest started, holds the session, and that the request admitted
// before Close still completes.
func TestStreamHeaderHoldsClose(t *testing.T) {
	o := newOwner(t)
	logs := logsServed(t)
	gate := o.gate()
	sent := make(chan struct{})
	body := newModelBody(logs.body)
	next := sequence(func(req *http.Request) (*http.Response, error) {
		close(sent)
		<-gate.c
		return logs.replyProto(2, body)(req)
	})
	signer := &modelSigner{}
	s, hc := newJarSession(signer, next, nil)
	st := logsBidi(s, hc).CallBidiStream(context.Background())
	if err := st.CloseRequest(); err != nil {
		t.Fatal(err)
	}
	await(t, sent, "the request to reach the transport")
	var header http.Header
	r := o.goRun("ResponseHeader", func() { header = st.ResponseHeader() })
	asCallError(t, s.Close(), "")
	gate.release()
	join(t, r)
	if header.Get("Content-Type") != "application/connect+proto" {
		t.Errorf("ResponseHeader = %v", header)
	}
	if signer.count() != 0 {
		t.Errorf("signer closed while the stream was open")
	}
	if err := st.CloseResponse(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil || signer.count() != 1 || body.closeCount() != 1 {
		t.Errorf("Close = %v after %d signer and %d body Closes; want nil after 1 and 1", err, signer.count(), body.closeCount())
	}
}

// TestStreamLateRequestRefused checks that a stream whose CloseRequest
// started Connect's request in a goroutine holds the session even before
// that request reaches the transport, and that the request is refused
// there once the session is closed, with an error that does not match
// io.EOF.
func TestStreamLateRequestRefused(t *testing.T) {
	o := newOwner(t)
	jar := newPauseJar(o, skyv1connect.ControlServiceStreamJobLogsProcedure)
	next := sequence()
	signer := &modelSigner{}
	s, hc := newJarSession(signer, next, jar)
	st := logsBidi(s, hc).CallBidiStream(context.Background())
	if err := st.CloseRequest(); err != nil {
		t.Fatal(err)
	}
	await(t, jar.entered, "the request to pause")
	asCallError(t, s.Close(), "sshagent: session did not close: stream response not closed")
	jar.release.release()
	_, err := st.Receive()
	if errors.Is(err, io.EOF) {
		t.Fatalf("Receive = %v, which matches io.EOF", err)
	}
	asCallError(t, err, refusedClosed)
	if next.count() != 0 {
		t.Errorf("requests %d, want 0", next.count())
	}
	if err := st.CloseResponse(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil || signer.count() != 1 {
		t.Errorf("Close = %v after %d signer Closes; want nil after 1", err, signer.count())
	}
}

// TestStreamUnstarted checks that a stream that sent nothing holds the
// session until its response is closed, and that this CloseResponse,
// which in Connect alone would wait for a request never sent, returns at
// once.
func TestStreamUnstarted(t *testing.T) {
	next := sequence()
	s, hc := newJarSession(nil, next, nil)
	st := logsBidi(s, hc).CallBidiStream(context.Background())
	asCallError(t, s.Close(), "sshagent: session did not close: stream response not closed")
	if err := st.Send(nil); errors.Is(err, io.EOF) {
		t.Errorf("Send after Close = %v, which matches io.EOF", err)
	} else {
		asCallError(t, err, refusedClosed)
	}
	if err := st.CloseResponse(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil || next.count() != 0 {
		t.Errorf("Close = %v after %d requests; want nil after 0", err, next.count())
	}
}

// TestStreamFailedCloseRequest checks that a server stream whose
// CloseRequest failed, which Connect's CallServerStream then drops, has
// its response closed by the session; and that a bidirectional stream,
// which its caller still holds, is left to the caller. Actual Connect
// returns no such error; the connection is a MODEL, admitted through the
// actual interceptor.
func TestStreamFailedCloseRequest(t *testing.T) {
	errCloseRequest := errors.New("modeled close request failure")
	for _, typ := range []connect.StreamType{connect.StreamTypeServer, connect.StreamTypeBidi} {
		l := newLedger(nil)
		conn := &modelConn{spec: connect.Spec{StreamType: typ}, closeRequestErr: errCloseRequest}
		st := modelStream(t, l, conn)
		if err := st.CloseRequest(); err != errCloseRequest {
			t.Errorf("CloseRequest = %v, want %v", err, errCloseRequest)
		}
		l.mu.Lock()
		held := len(l.live)
		l.mu.Unlock()
		if typ == connect.StreamTypeServer && (conn.count("CloseResponse") != 1 || held != 0) {
			t.Errorf("server stream: CloseResponses %d, calls held %d; want 1 and 0", conn.count("CloseResponse"), held)
		}
		if typ == connect.StreamTypeBidi && (conn.count("CloseResponse") != 0 || held != 1) {
			t.Errorf("bidi stream: CloseResponses %d, calls held %d; want 0 and 1", conn.count("CloseResponse"), held)
		}
	}
}

// TestRefusalNotEOF checks that once a body Close failure wrapping io.EOF
// faulted the session, a stream refused when admitted and one admitted
// before the fault but paused before its request was sent each report a
// refusal that does not match io.EOF, so Connect neither hides nor
// replaces it; and that Close still reaches the original failure.
func TestRefusalNotEOF(t *testing.T) {
	o := newOwner(t)
	info := infoServed(t)
	closeErr := fmt.Errorf("modeled close failure: %w: %s", io.EOF, secret)
	body := newModelBody(info.body)
	body.closeErr = closeErr
	jar := newPauseJar(o, skyv1connect.ControlServiceStreamJobLogsProcedure)
	next := sequence(info.reply(body))
	s, _ := newJarSession(nil, next, jar)

	var st *connect.ServerStreamForClient[v1.StreamJobLogsResponse]
	var stErr error
	r := o.goRun("StreamJobLogs", func() {
		st, stErr = s.Control().StreamJobLogs(context.Background(), logsRequest())
	})
	await(t, jar.entered, "the stream's request to pause")
	_, err := s.Control().GetServerInfo(context.Background(), infoRequest())
	asCallError(t, err, "")
	jar.release.release()
	join(t, r)
	if stErr != nil || st == nil {
		t.Fatalf("StreamJobLogs = %v", stErr)
	}
	if st.Receive() {
		t.Fatal("Receive = true")
	}
	if err := st.Err(); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("Err = %v; want the refusal, not io.EOF", err)
	}
	asCallError(t, st.Err(), refusedFaulted)
	if err := st.Close(); err != nil {
		t.Errorf("stream Close = %v", err)
	}

	_, err = s.Control().StreamJobLogs(context.Background(), logsRequest())
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("StreamJobLogs = %v; want the refusal, not io.EOF", err)
	}
	asCallError(t, err, refusedFaulted)

	cerr := s.Close()
	asCallError(t, cerr, "sshagent: session did not close: close response body failed")
	if !errors.Is(cerr, io.EOF) || !errors.Is(cerr, closeErr) {
		t.Errorf("Close = %v; want the original Close failure", cerr)
	}
	if next.count() != 1 {
		t.Errorf("requests %d, want 1", next.count())
	}
}

// TestHeldCloseOverlap checks a body Close held while other calls run: a
// later call that settles does not release it; a request paused before
// its send is refused once the held Close fails; the first call's error
// is a snapshot that does not grow with another call's later error, while
// Close reports each call the session still holds.
func TestHeldCloseOverlap(t *testing.T) {
	o := newOwner(t)
	info := infoServed(t)
	errHeld := &secretError{"modeled held close failure", secret}
	errLate := errors.New("modeled late transport failure")

	bodyA := newModelBody(info.body)
	gateA := o.gate()
	bodyA.gate = gateA.c
	bodyA.closeErr = errHeld
	bodyB := newModelBody(info.body)
	gateD, sentD := o.gate(), make(chan struct{})
	next := sequence(
		info.reply(bodyA),
		info.reply(bodyB),
		func(*http.Request) (*http.Response, error) {
			close(sentD)
			<-gateD.c
			return nil, errLate
		},
	)
	jar := newPauseJar(o, skyv1connect.ControlServiceGetJobProcedure)
	signer := &modelSigner{}
	s, _ := newJarSession(signer, next, jar)
	cc := s.Control()
	ctx := context.Background()

	var eA error
	rA := o.goRun("A", func() { _, eA = cc.GetServerInfo(ctx, infoRequest()) })
	await(t, bodyA.entered, "A's body Close")

	if _, err := cc.GetServerInfo(ctx, infoRequest()); err != nil {
		t.Fatalf("GetServerInfo B = %v", err)
	}
	if n := liveCalls(s); n != 1 {
		t.Errorf("calls held %d after B settled, want 1: A", n)
	}

	var st *connect.ServerStreamForClient[v1.StreamJobLogsResponse]
	var stErr error
	rD := o.goRun("D", func() { st, stErr = cc.StreamJobLogs(ctx, logsRequest()) })
	await(t, sentD, "D's request")

	var eC error
	rC := o.goRun("C", func() { _, eC = cc.GetJob(ctx, connect.NewRequest(&v1.GetJobRequest{})) })
	await(t, jar.entered, "C's request to pause")

	gateA.release()
	join(t, rA)
	asCallError(t, eA, "sshagent: request did not settle: close response body failed")
	if !errors.Is(eA, errHeld) {
		t.Errorf("A = %v, want the held Close failure", eA)
	}

	jar.release.release()
	join(t, rC)
	asCallError(t, eC, refusedFaulted)

	gateD.release()
	join(t, rD)
	if stErr != nil || st == nil {
		t.Fatalf("StreamJobLogs D = %v", stErr)
	}
	if st.Receive() || !errors.Is(st.Err(), errLate) {
		t.Errorf("D Err = %v, want the late failure", st.Err())
	}
	if errors.Is(eA, errLate) {
		t.Errorf("A = %v; it reaches D's later error", eA)
	}

	eClose := s.Close()
	asCallError(t, eClose, "sshagent: session did not close: stream response not closed, close response body failed")
	if !errors.Is(eClose, errHeld) || !errors.Is(eClose, errLate) {
		t.Errorf("Close = %v; want A's Close failure and D's error", eClose)
	}
	if err := st.Close(); err != nil {
		t.Errorf("D Close = %v", err)
	}
	eClose2 := s.Close()
	asCallError(t, eClose2, "sshagent: session did not close: close response body failed")
	if !errors.Is(eClose2, errHeld) || errors.Is(eClose2, errLate) || !errors.Is(eClose, errLate) {
		t.Errorf("second Close = %v, first = %v; want a fresh snapshot each", eClose2, eClose)
	}
	if next.count() != 3 || bodyA.closeCount() != 1 || signer.count() != 0 || liveCalls(s) != 1 {
		t.Errorf("requests %d, A body Closes %d, signer Closes %d, calls held %d; want 3, 1, 0 and 1",
			next.count(), bodyA.closeCount(), signer.count(), liveCalls(s))
	}
}

// TestUnaryInFlightHoldsClose checks that Close returns at once, without
// closing the signer, while a unary request is still in its transport or
// reading its response body, and succeeds once it returned.
func TestUnaryInFlightHoldsClose(t *testing.T) {
	info := infoServed(t)
	for _, inBody := range []bool{false, true} {
		t.Run(fmt.Sprintf("inBody=%v", inBody), func(t *testing.T) {
			o := newOwner(t)
			gate, sent := o.gate(), make(chan struct{})
			body := newModelBody(info.body)
			reply := info.reply(body)
			if inBody {
				body.readGate = gate.c
			} else {
				inner := reply
				reply = func(req *http.Request) (*http.Response, error) {
					close(sent)
					<-gate.c
					return inner(req)
				}
			}
			signer := &modelSigner{}
			s := newTestSession(signer, sequence(reply))
			var err error
			r := o.goRun("GetServerInfo", func() {
				_, err = s.Control().GetServerInfo(context.Background(), infoRequest())
			})
			if inBody {
				await(t, body.readIn, "the body read")
			} else {
				await(t, sent, "the request to reach the transport")
			}
			asCallError(t, s.Close(), "sshagent: session did not close: request still running")
			gate.release()
			join(t, r)
			if err != nil {
				t.Errorf("GetServerInfo admitted before Close = %v", err)
			}
			if err := s.Close(); err != nil || signer.count() != 1 {
				t.Errorf("Close = %v after %d signer Closes; want nil after 1", err, signer.count())
			}
		})
	}
}

// TestCloseRaces runs Close alongside unary requests. Each request either
// succeeds, sent once, or is refused unsent; Close fails while one runs;
// the signer is closed once, after all returned.
func TestCloseRaces(t *testing.T) {
	o := newOwner(t)
	info := infoServed(t)
	var mu sync.Mutex
	var bodies []*modelBody
	next := &modelTransport{reply: func(_ int, req *http.Request) (*http.Response, error) {
		b := newModelBody(info.body)
		mu.Lock()
		bodies = append(bodies, b)
		mu.Unlock()
		return info.reply(b)(req)
	}}
	signer := &modelSigner{}
	s := newTestSession(signer, next)

	const calls, closes = 16, 4
	start := o.gate()
	var results [calls]error
	var closeErrs [closes]error
	var runs []*run
	for i := range calls {
		runs = append(runs, o.goRun(fmt.Sprintf("GetServerInfo %d", i), func() {
			<-start.c
			_, results[i] = s.Control().GetServerInfo(context.Background(), infoRequest())
		}))
	}
	for i := range closes {
		runs = append(runs, o.goRun(fmt.Sprintf("Close %d", i), func() {
			<-start.c
			closeErrs[i] = s.Close()
		}))
	}
	start.release()
	for _, r := range runs {
		join(t, r)
	}
	for _, err := range closeErrs {
		if err != nil && !errors.As(err, new(*CallError)) {
			t.Errorf("Close = %v, want nil or a *CallError", err)
		}
	}
	ok := 0
	for _, err := range results {
		if err == nil {
			ok++
			continue
		}
		var ce *CallError
		if !errors.As(err, &ce) || ce.stages&stageClosed == 0 {
			t.Errorf("GetServerInfo = %v, want success or a refusal", err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close after every request returned = %v", err)
	}
	if ok != next.count() || signer.count() != 1 {
		t.Errorf("%d succeeded, %d sent, %d signer Closes; want equal counts and 1", ok, next.count(), signer.count())
	}
	mu.Lock()
	defer mu.Unlock()
	for _, b := range bodies {
		if b.closeCount() != 1 {
			t.Errorf("body closed %d times, want 1", b.closeCount())
		}
	}
}

// TestSignerCloseFailure checks that concurrent Closes of a settled
// session close its SSH agent connection once, share its failure, keep it
// for every later Close, and never print it.
func TestSignerCloseFailure(t *testing.T) {
	o := newOwner(t)
	signerErr := &secretError{"modeled ssh agent close failure", secret}
	signer := &modelSigner{err: signerErr}
	s := newTestSession(signer, sequence())
	errs := make([]error, 4)
	var runs []*run
	for i := range errs {
		runs = append(runs, o.goRun(fmt.Sprintf("Close %d", i), func() { errs[i] = s.Close() }))
	}
	for _, r := range runs {
		join(t, r)
	}
	errs = append(errs, s.Close())
	for _, err := range errs {
		asCallError(t, err, "sshagent: session did not close: close ssh agent connection failed")
		var got *secretError
		if !errors.As(err, &got) || got != signerErr {
			t.Errorf("Close = %v, want the signer's failure", err)
		}
		for _, format := range []string{"%v", "%s", "%q", "%+v", "%#v", "%x"} {
			if got := fmt.Sprintf(format, err); strings.Contains(got, secret) {
				t.Errorf("Sprintf(%q) = %q", format, got)
			}
		}
		if got := errors.Join(err, errors.New("other")).Error(); strings.Contains(got, secret) {
			t.Errorf("errors.Join(...).Error() = %q", got)
		}
	}
	if signer.count() != 1 {
		t.Errorf("signer Closes %d, want 1", signer.count())
	}
}

// TestNoTransport checks that a session whose client has no transport
// refuses every request rather than use http.DefaultTransport.
func TestNoTransport(t *testing.T) {
	for _, hc := range []*http.Client{nil, {}} {
		l := newLedger(nil)
		cc := newControl(l, hc, testEndpoint, "session-token")
		_, err := cc.GetServerInfo(context.Background(), infoRequest())
		if !errors.Is(err, errNoTransport) {
			t.Errorf("GetServerInfo = %v, want %v", err, errNoTransport)
		}
		if hc != nil && hc.Transport != nil {
			t.Errorf("newControl modified the caller's client")
		}
		s := &Session{calls: l, control: cc}
		if err := s.Close(); err != nil {
			t.Errorf("Close = %v", err)
		}
	}
}

// TestOwnerRetire checks the owner the tests above rely on, with plain
// goroutines and no session. A goroutine held by an owned gate the test
// never released is released and joined by retire, and a second release
// does nothing. A goroutine held by a gate nobody released yet is
// reported, not joined, once the guard expires, and joins once released.
func TestOwnerRetire(t *testing.T) {
	o := newOwner(t)

	inner := new(testOwner)
	g := inner.gate()
	returned := false
	inner.goRun("held by an owned gate", func() {
		<-g.c
		returned = true
	})
	if left := inner.retire(ownerGuard); len(left) != 0 {
		strand(left)
		t.Fatalf("retire left %d runs, kept for the life of the process", len(left))
	}
	if !returned {
		t.Error("joined run did not return")
	}
	g.release()

	held := o.gate()
	r := o.goRun("held outside the guard", func() { <-held.c })
	if left := joinRuns([]*run{r}, time.Millisecond); len(left) != 1 || left[0] != r {
		t.Fatalf("joinRuns = %d runs, want the held one", len(left))
	}
	held.release()
	if left := joinRuns([]*run{r}, ownerGuard); len(left) != 0 {
		t.Fatalf("joinRuns after release left %d runs", len(left))
	}
}
