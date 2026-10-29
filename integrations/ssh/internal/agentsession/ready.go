package agentsession

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sync"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/integrations/ssh/internal/agentssh"
	"github.com/tmc/skygo/integrations/ssh/internal/bootstrap"
)

const readyTimeout = time.Minute

// WaitReady waits until the agent at endpoint answers an authorized
// GetServerInfo naming target as a local target and, if workspace is set,
// advertising the artifact service and target's workdir snapshots. A
// response proves the agent holds the key of its pinned certificate and
// accepts the token; for a scoped token, that the agent serves an auth
// file that issued it. Between attempts it checks that the recorded
// process has not stopped.
//
// Each attempt is one request through hc. Before it acts on the result,
// WaitReady requires that the request returned, that the first Close of
// its response body returned nil, and that its error holds no unsettled
// agentssh receipt. Otherwise it returns a *ProbeError at once, even if
// the agent answered, and sends no further request and calls no status.
// WaitReady settles only its own requests; requests others make through
// hc own their connections.
func WaitReady(ctx context.Context, hc *http.Client, endpoint, token, target string, workspace bool, status func(context.Context) (bootstrap.State, error)) error {
	deadline := time.Now().Add(readyTimeout)
	delay := 100 * time.Millisecond
	for {
		p := newProbe(hc)
		err := CheckReady(ctx, skyv1connect.NewControlServiceClient(p.client(hc), endpoint), token, target, workspace)
		if perr := p.settle(err); perr != nil {
			return perr
		}
		if err == nil || errors.Is(err, ErrWrongAgent) {
			return err
		}
		st, serr := status(ctx)
		if serr != nil {
			return serr
		}
		if st.Agent != "running" && st.Agent != "unknown" {
			return fmt.Errorf("skygo-ssh: agent %s before it was ready: %w", st.Agent, err)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("skygo-ssh: agent not ready after %v: %w", readyTimeout, err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w; last attempt: %w", context.Cause(ctx), err)
		case <-time.After(delay):
		}
		delay = min(2*delay, time.Second)
	}
}

// A ProbeError reports a readiness request whose connections WaitReady
// could not show were closed: the request had not returned, its response
// body's first Close failed or had not returned, or its error holds an
// unsettled agentssh receipt. The error keeps the request's original
// response bodies and their Close results for as long as the caller
// holds it; it closes nothing again and retries nothing.
//
// Its text is fixed and holds none of its causes, which may carry
// secrets. errors.Is and errors.As reach CheckReady's result, the Close
// failure and each error a RoundTrip of the request had returned when
// WaitReady settled it. Connect may replace a transport error in its own
// result, so the RoundTrip errors are kept as returned.
type ProbeError struct {
	Err   error // CheckReady's result
	Close error // first failed Close of a response body

	roundTrips []error // non-nil errors returned by the request's RoundTrips
	probe      *probe
}

func (e *ProbeError) Error() string {
	if e.Close != nil {
		return "skygo-ssh: readiness request did not settle (close response body failed)"
	}
	return "skygo-ssh: readiness request did not settle"
}

func (e *ProbeError) Unwrap() []error {
	var errs []error
	for _, err := range append([]error{e.Err, e.Close}, e.roundTrips...) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

// A probe is the transport of one readiness request. It records whether
// each RoundTrip returned, its error, and its response body, whose
// original Close it runs at most once and whose result it keeps: Connect
// discards that result on some paths.
type probe struct {
	next http.RoundTripper

	mu       sync.Mutex
	started  int // RoundTrip calls
	returned int // RoundTrip calls that returned
	errs     []error
	bodies   []*probeBody
}

func newProbe(hc *http.Client) *probe {
	next := hc.Transport
	if next == nil {
		next = http.DefaultTransport
	}
	return &probe{next: next}
}

// client returns a copy of hc that sends its requests through p.
func (p *probe) client(hc *http.Client) *http.Client {
	c := *hc
	c.Transport = p
	return &c
}

func (p *probe) RoundTrip(req *http.Request) (*http.Response, error) {
	p.mu.Lock()
	p.started++
	p.mu.Unlock()
	resp, err := p.next.RoundTrip(req)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.returned++
	if err != nil {
		p.errs = append(p.errs, err)
	}
	if resp != nil && resp.Body != nil {
		b := &probeBody{body: resp.Body, done: make(chan struct{})}
		p.bodies = append(p.bodies, b)
		resp.Body = b
	}
	return resp, err
}

// settle returns a *ProbeError if the request whose result was err may
// still hold connections, and nil if it showed that it holds none. It
// closes a response body nobody closed, through the body's recorder, and
// waits for that Close. It does not wait for a Close or RoundTrip still
// running elsewhere: either makes the request unsettled.
func (p *probe) settle(err error) error {
	p.mu.Lock()
	unsettled := p.started != p.returned
	errs := slices.Clone(p.errs)
	bodies := slices.Clone(p.bodies)
	p.mu.Unlock()
	e := &ProbeError{Err: err, roundTrips: errs, probe: p}
	for _, rerr := range append(errs, err) {
		if agentssh.OwnershipUnsettled(rerr) {
			unsettled = true
		}
	}
	for _, b := range bodies {
		done, cerr := b.settled()
		if !done || cerr != nil {
			unsettled = true
		}
		if cerr != nil && e.Close == nil {
			e.Close = cerr
		}
	}
	if unsettled {
		return e
	}
	return nil
}

// A probeBody is a response body whose original Close runs once. Every
// Close waits for that call and returns its result.
type probeBody struct {
	body io.ReadCloser

	mu      sync.Mutex
	closing bool
	done    chan struct{} // closed once err is set
	err     error
}

func (b *probeBody) Read(p []byte) (int, error) { return b.body.Read(p) }

func (b *probeBody) Close() error {
	b.mu.Lock()
	first := !b.closing
	b.closing = true
	b.mu.Unlock()
	if first {
		b.err = b.body.Close()
		close(b.done)
	}
	<-b.done
	return b.err
}

// settled reports whether the body's Close has returned, and its result.
// If no Close has started, settled runs it and waits for it.
func (b *probeBody) settled() (bool, error) {
	b.mu.Lock()
	started := b.closing
	b.mu.Unlock()
	if !started {
		return true, b.Close()
	}
	select {
	case <-b.done:
		return true, b.err
	default:
		return false, nil
	}
}

// ErrWrongAgent reports an agent that answers but does not serve the
// expected target.
var ErrWrongAgent = errors.New("skygo-ssh: agent does not serve the expected target")

// CheckReady sends one authorized GetServerInfo through cc and checks that
// the agent serves target, with artifact workspaces if workspace is set.
func CheckReady(ctx context.Context, cc skyv1connect.ControlServiceClient, token, target string, workspace bool) error {
	req := connect.NewRequest(&v1.GetServerInfoRequest{})
	req.Header().Set("Authorization", "Bearer "+token)
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	resp, err := cc.GetServerInfo(ctx, req)
	if err != nil {
		return err
	}
	for _, t := range resp.Msg.Targets {
		if t.Name != target || t.Kind != "local" {
			continue
		}
		if !workspace {
			return nil
		}
		if slices.Contains(resp.Msg.Capabilities, v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1) &&
			slices.Contains(t.Capabilities, v1.Capability_CAPABILITY_WORKDIR_SNAPSHOT_V1) {
			return nil
		}
		return fmt.Errorf("%w %s with artifact workspaces", ErrWrongAgent, target)
	}
	return fmt.Errorf("%w %s", ErrWrongAgent, target)
}
