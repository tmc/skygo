package sshagent

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"connectrpc.com/connect"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/integrations/ssh/internal/agentidentity"
	"github.com/tmc/skygo/integrations/ssh/internal/agentsession"
)

// A Session is a ready skygo agent on an SSH host. Only Open returns a
// usable Session. A nil or zero Session owns nothing: its Control is nil,
// its Binding is zero and its Close returns nil.
//
// A Session owns each Control request it admits until every frame of it
// returned: the RPC or stream, each HTTP request, and each response body's
// Read and first Close. See Control and Close.
type Session struct {
	s       *agentsession.Session
	calls   *ledger
	control skyv1connect.ControlServiceClient
	binding Binding
}

// A Binding records what Open verified and started. It holds no
// credential or key. The server does not report an installation's
// identity, so an installation is bound by the agent directory's
// configuration record, not checked over RPC.
type Binding struct {
	Address        string // SSH server
	User           string // SSH user
	KnownHostsFile string
	KeyFile        string // client key file, "" with an SSH agent
	Fingerprint    string // pinned SSH agent key, "" with a key file
	AgentAddress   string // the agent's loopback address on the host
	Target         string
	Commit         string // full hex commit of the agent's source
	Digest         string // hex SHA-256 of the agent binary
	Dirty          bool   // the source had uncommitted changes
	AgentDir       string // agent directory, relative to the remote home
	Installation   Installation

	// PID is the agent process recorded when Open checked it. A process
	// ID does not identify a process later; never signal by it.
	PID int
}

// Open builds the agent from cfg.Source, installs and starts it on the
// host if needed, and waits until it answers as itself. It checks cfg and
// then reads the scoped token, if any, before anything else. The SSH
// connection Open uses for the bootstrap commands is closed, and joined,
// before it returns.
//
// If Open fails, it has closed every local connection it opened, once.
// It returns an ordinary error only if each of those Closes succeeded and
// each readiness request it sent showed that its connections were closed.
// Otherwise it returns an *OpenError, having sent no further request and
// run no further command once it saw the unsettled one. It returns no
// partial Session in either case.
func Open(ctx context.Context, cfg Config) (*Session, error) {
	if err := cfg.check(); err != nil {
		return nil, err
	}
	o := cfg.options()
	if !cfg.Installation.IsZero() {
		var err error
		if o.Bearer, err = agentidentity.ReadToken(o.TokenFile); err != nil {
			return nil, fmt.Errorf("sshagent: scoped token: %w", err)
		}
	}
	s, err := agentsession.Open(ctx, o)
	if err != nil {
		var u *agentsession.UnsettledError
		if errors.As(err, &u) {
			return nil, &OpenError{err: u}
		}
		return nil, err
	}
	b := s.Binding()
	calls := newLedger(s)
	return &Session{
		s:       s,
		calls:   calls,
		control: newControl(calls, s.HTTPClient(), s.Endpoint(), s.Bearer()),
		binding: Binding{
			Address:        b.SSHAddress,
			User:           b.User,
			KnownHostsFile: b.KnownHostsFile,
			KeyFile:        b.KeyFile,
			Fingerprint:    b.SignerKey,
			AgentAddress:   b.AgentAddress,
			Target:         b.Target,
			Commit:         b.Commit,
			Digest:         b.Digest,
			Dirty:          b.Dirty,
			AgentDir:       b.AgentDir,
			Installation:   Installation(b.Installation),
			PID:            b.PID,
		},
	}, nil
}

// Control returns a client of the agent's control service. Every request
// carries the session's token, replacing any Authorization header the
// caller set, offers no compression and follows no redirect. Each request
// uses its own SSH connection and is never retried; if one fails after it
// was sent, its outcome at the agent is unknown.
//
// The artifact methods, BeginArtifactUpload through RemoveArtifact, refuse
// a response message larger than 128 KiB, decompressed or not, with a
// Connect error of code ResourceExhausted, as the skygo command does. The
// limit is Connect's WithReadMaxBytes: it bounds each response message as
// received and as decompressed, not the bytes read from the connection,
// which Connect may read to its end past the limit, nor headers, requests
// or whole artifacts. Other methods read messages of any size. The
// session owns and settles an artifact request like any other.
//
// A unary request returns its result once each response body it received
// was closed once, through the session, with a nil result. If a frame of
// it was still running, a body's first Close failed, or its transport
// error holds a receipt of a connection that did not settle, it returns a
// *CallError instead, even if the agent answered; Connect alone would
// discard that Close result.
//
// A stream, and Connect's connection under it, is the session's from the
// moment Connect builds it until its first CloseResponse returned and
// every Send, CloseRequest, Receive, header read, HTTP request and body
// Read and Close it entered returned, with its body closed cleanly;
// reading to io.EOF ends nothing. The first CloseResponse fences the
// stream: frames already entered may finish, but none starts after it.
// Later Send, CloseRequest and Receive calls return a *CallError wrapping
// no cause; the header methods return what each returned before the
// fence, or an empty header; later CloseResponse calls close nothing
// again and report the first one's outcome as of then. CloseResponse
// returns the original result only once every frame of the stream
// returned and its body closed cleanly. While one has not, it returns a
// pending *CallError, which is no fault. From the moment the first
// CloseResponse starts, the session refuses new requests and HTTP sends
// until the stream drains; then it admits them again, unless it was
// closed. Open streams do not hold other requests back. Receive's result is Connect's own; the session keeps its first
// error not matching io.EOF for its CallErrors.
//
// The first request that does not settle faults the session for good:
// it refuses every later request, and every HTTP request not yet sent,
// with a *CallError wrapping no cause. So does a closed session, whatever
// client it was obtained from. Requests sent already may still be in
// flight; a refusal does not undo them.
func (s *Session) Control() skyv1connect.ControlServiceClient {
	if s == nil {
		return nil
	}
	return s.control
}

// Binding returns what Open verified and started.
func (s *Session) Binding() Binding {
	if s == nil {
		return Binding{}
	}
	return s.binding
}

// Close closes the session to new Control requests, at once and for good,
// with a key file too: each later request, stream Send and HTTP request
// is refused. Frames already entered may finish, and streams whose
// CloseResponse was not called can still be received from and closed.
// Unlike a stream's CloseResponse, which speaks for that stream alone,
// Close speaks for the whole session.
//
// If every request the session admitted has returned and settled, Close
// closes the local connection to the SSH agent that signs the session's
// SSH authentications, once; with a key file there is none. Concurrent
// calls wait for that first Close and return its result. A failed Close is
// never retried, and s keeps referencing the connection.
//
// Otherwise Close returns a *CallError and closes nothing. If a request,
// stream, read or body Close is still running or a stream is still open,
// Close neither waits for, cancels nor closes it; once the caller has
// finished each, a later Close may succeed. If a request did not settle,
// or the SSH agent connection's Close failed, every later Close fails too,
// and the session keeps each such request and the connection. A caller
// that gets such an error keeps s, and opens no further session to the
// host, until the cause is understood: nothing outside s stops another
// session or process from using the host.
//
// Close never stops or signals the remote agent or its jobs, which
// outlive the session.
func (s *Session) Close() error {
	if s == nil || s.calls == nil {
		return nil
	}
	return s.calls.close()
}

// An OpenError reports an Open that failed and whose local cleanup did not
// settle: the first Close of a connection Open opened returned an error,
// or a readiness request did not show that its connections were closed,
// so their state is unknown. Nothing is retried or reopened.
//
// Error names only the parts of the cleanup that did not settle, in fixed
// words, because a cause or Close error may quote remote output. Unwrap
// returns the original cause and each failed Close unchanged, for
// errors.Is and errors.As.
//
// The error keeps the original connections and readiness responses
// reachable only while the caller holds it. It closes none of them again
// and joins nothing beyond what Open already joined; once it is dropped,
// the Go runtime may finalize them. A caller that must not lose track of
// them keeps the error and opens no further session to the host until the
// cause is understood.
type OpenError struct {
	err *agentsession.UnsettledError
}

func (e *OpenError) Error() string {
	s := "sshagent: open failed and its cleanup did not settle"
	if stages := e.err.Summary(); stages != "" {
		s += ": " + stages
	}
	return s
}

func (e *OpenError) Unwrap() error { return e.err }

// newControl returns a control client for the agent at endpoint that
// sends token on every request it admits. It sends through a copy of hc
// whose transport records each request's frames in l and then uses hc's
// transport, unchanged; hc itself is not modified. If hc has no transport,
// every request is refused. Its artifact methods reject response messages
// exceeding artifactReadMaxBytes under Connect's decode bound; see control.
func newControl(l *ledger, hc *http.Client, endpoint, token string) skyv1connect.ControlServiceClient {
	var c http.Client
	if hc != nil {
		c = *hc
	}
	c.Transport = &recorder{l: l, next: c.Transport}
	intercept := connect.WithInterceptors(l, bearer(token))
	return &control{
		ordinary: skyv1connect.NewControlServiceClient(&c, endpoint, intercept),
		artifact: skyv1connect.NewControlServiceClient(&c, endpoint, intercept, connect.WithReadMaxBytes(artifactReadMaxBytes)),
	}
}

// A bearer sets the Authorization header of every request it intercepts.
type bearer string

func (b bearer) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		req.Header().Set("Authorization", "Bearer "+string(b))
		return next(ctx, req)
	}
}

func (b bearer) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		conn.RequestHeader().Set("Authorization", "Bearer "+string(b))
		return conn
	}
}

func (b bearer) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}
