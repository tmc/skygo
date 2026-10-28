package kube

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/proxy"
	"k8s.io/apimachinery/pkg/util/httpstream"
	spdyapi "k8s.io/apimachinery/pkg/util/httpstream/spdy"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/third_party/forked/golang/netutil"
	"k8s.io/client-go/rest"
)

// This file is the private connection custody shared by environment exec and
// the fixed-port gateway. It is not wired to any endpoint or capability.
//
// One envFrame owns one admitted connection: the single upgrade request, the
// original network connection that request was written to, the response body,
// the httpstream.Connection made from an accepted upgrade, the streams created
// on it, and every goroutine this package starts for it. The frame records the
// first result of each original operation and never repeats one: the original
// network connection's Close, the SPDY connection's Close, a stream's FIN
// (Close) and Reset (separate operations), and the body's Close each run at
// most once. Its done channel closes only after every owned goroutine has
// recorded its last result and returned.
//
// The upgrade is made by the frame itself. Its envDialer makes the
// connections apimachinery v0.32.3's SpdyRoundTripper.Dial would (direct, an
// http or https proxy with CONNECT, or SOCKS5 through golang.org/x/net/proxy),
// with the TLS configuration from rest.TLSConfigFor, including any exec-plugin
// client certificate. The request goes through rest.HTTPWrappersForConfig, so
// authentication, impersonation, user agent and WrapTransport are the
// configured client's. The frame keeps the original TCP connection before
// anything is written to it, so cancellation closes it during a proxy
// handshake, CONNECT, TLS handshake, the request write and the response read
// alike; every layer above it closes it only through the frame's single
// Close. On an accepted upgrade the connection, with any bytes buffered past
// the response, goes to NewClientConnectionWithPings. A refused upgrade (any
// status other than 101 with SPDY/3.1, including a redirect, which is never
// followed) or a different selected protocol is read for at most
// maxUpgradeErrorBody bytes; then the frame closes the original connection
// once, which also ends any read still waiting on it, and only then closes the
// body once, so the body's Close cannot drain without bound. A proxy's CONNECT
// refusal is not read: the original connection is closed, then its body. A
// connection dialed after cancellation is closed as soon as it is kept. The
// request is sent at most once.
//
// After an accepted upgrade the SPDY connection owns the original connection;
// no handoff is made once close intent is latched. The frame's connection
// Close returns the result of spdystream v0.5.0's GOAWAY write; that library
// closes the network connection asynchronously, once no stream of it is open,
// and that Close is the original record's single Close. The owner that made
// the connection Close waits for that recorded Close to return, so the frame
// settles only after the original connection's Close returned, on every path.
// A connection the library never closes, because a stream of it stays open
// (one whose CreateStream failed waiting for the peer's reply, say, while the
// peer keeps the connection), holds the frame; it is never reported settled.
// The frame never starts its connection Close while a stream request is in
// flight, and requests no stream after close intent, so every stream it
// created is registered, and reset, before the GOAWAY (see createStream).
// CloseChan is not a join and is not used. None of these facts is proof of
// remote process exit, Pod state or the end of internal SPDY goroutines.

const (
	// execProtocol is the only exec subprotocol requested or accepted.
	execProtocol = "v4.channel.k8s.io"
	// portProtocol is the only port-forward subprotocol requested or accepted.
	portProtocol = "portforward.k8s.io"

	// maxUpgradeErrorBody bounds the bytes of a refused upgrade's response
	// body that are read.
	maxUpgradeErrorBody = 64 << 10
	// envPingPeriod is client-go's spdy.RoundTripperFor ping period.
	envPingPeriod = 5 * time.Second
)

var (
	// errEnvCanceled reports that the frame's owner canceled the connection.
	errEnvCanceled = errors.New("kubernetes environment connection canceled")
	// errEnvRefused reports an upgrade answered with anything but an
	// accepted SPDY/3.1 upgrade.
	errEnvRefused = errors.New("kubernetes environment upgrade refused")
	// errEnvRedirect reports a redirect answer to an upgrade; it is never
	// followed.
	errEnvRedirect = errors.New("kubernetes environment upgrade redirect refused")
	// errEnvProtocol reports a missing or different selected subprotocol.
	errEnvProtocol = errors.New("kubernetes environment upgrade selected an unexpected protocol")
	// errEnvReplay reports a second upgrade attempt on one frame.
	errEnvReplay = errors.New("kubernetes environment upgrade already sent")
	// errEnvProxyRefused reports a proxy that refused CONNECT.
	errEnvProxyRefused = errors.New("kubernetes environment proxy CONNECT refused")
	// errEnvProxyScheme reports a configured proxy whose scheme is not http,
	// https or socks5; it is refused, never bypassed.
	errEnvProxyScheme = errors.New("kubernetes environment proxy scheme not supported")
	// errEnvRawClosing reports, while a frame is unsettled, that the original
	// connection's single Close has started and not returned. The closer
	// remains an owner of the frame; that Close is never repeated.
	errEnvRawClosing = errors.New("kubernetes environment original connection Close not returned")
	// errEnvRawHeld reports, while a frame is unsettled, that its SPDY
	// connection's Close returned and the SPDY connection, which owns the
	// original connection, has not yet started its Close: a stream of it is
	// still open. The frame holds until that Close returns.
	errEnvRawHeld = errors.New("kubernetes environment original connection not yet closed by its SPDY connection")
)

// envTransport opens environment connections to one API server with its
// configured TLS and authentication. Its zero value is not usable; use
// newEnvTransport.
type envTransport struct {
	config    *rest.Config
	server    *url.URL
	namespace string

	// wrap, if set, is applied by tests to each SPDY connection before the
	// frame publishes it. wrapRaw, if set, is applied by tests to each
	// original TCP connection before the frame keeps it. Production
	// leaves both nil.
	wrap    func(*envFrame, httpstream.Connection) httpstream.Connection
	wrapRaw func(net.Conn) net.Conn
}

// newEnvTransport freezes a copy of config for environment connections in
// namespace. It refuses a configuration whose transport client-go's SPDY
// round tripper would silently ignore, a custom Transport or Dial, as this
// package's upgrade would too.
func newEnvTransport(config *rest.Config, namespace string) (*envTransport, error) {
	if config == nil {
		return nil, errors.New("kubernetes configuration is required")
	}
	if config.Transport != nil || config.Dial != nil {
		return nil, errors.New("kubernetes environment transport refuses a custom transport or dialer")
	}
	if len(validation.IsDNS1123Label(namespace)) != 0 {
		return nil, errors.New("invalid kubernetes namespace")
	}
	host, err := canonicalServer(config.Host)
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(host)
	if err != nil {
		return nil, errors.New("invalid kubernetes API server URL")
	}
	config = rest.CopyConfig(config)
	config.Host = host
	return &envTransport{config: config, server: u, namespace: namespace}, nil
}

// podURL returns the fixed subresource URL of pod in the transport's namespace.
func (t *envTransport) podURL(pod, subresource string, query url.Values) (*url.URL, error) {
	if len(validation.IsDNS1123Subdomain(pod)) != 0 {
		return nil, errors.New("invalid kubernetes pod name")
	}
	u := *t.server
	u.Path = strings.TrimSuffix(u.Path, "/") + "/api/v1/namespaces/" + t.namespace + "/pods/" + pod + "/" + subresource
	u.RawPath = ""
	u.RawQuery = query.Encode()
	return &u, nil
}

// envOp is the record of one original operation that runs at most once.
type envOp struct {
	started bool
	done    bool
	err     error
}

// envFrame is the custody record of one admitted environment connection.
type envFrame struct {
	ctx       context.Context // the upgrade request's context
	cancelCtx context.CancelFunc
	done      chan struct{} // closed when running returns to zero

	mu       sync.Mutex
	running  int                   // owned goroutines that have not returned
	closing  bool                  // close intent is latched
	canceled bool                  // the owner canceled
	dialed   bool                  // the upgrade request was sent
	raw      *envRaw               // the original network connection
	wire     *envBuffered          // the connection the upgrade was written to, above raw
	handed   bool                  // raw is owned by the SPDY connection
	conn     httpstream.Connection // the SPDY connection
	connErr  envOp                 // the SPDY connection's single Close
	connDone chan struct{}         // closes when that Close and the original's recorded Close have returned
	creating int                   // CreateStream calls that have not returned
	bodies   []*envBody
	streams  []*envStream
	errs     []error // recorded failures, in order
}

func newEnvFrame() *envFrame {
	ctx, cancel := context.WithCancel(context.Background())
	return &envFrame{ctx: ctx, cancelCtx: cancel, done: make(chan struct{})}
}

// enter registers an owned goroutine or call. It refuses once the frame is
// done, which happens only after every earlier owner returned.
func (f *envFrame) enter() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	select {
	case <-f.done:
		return false
	default:
	}
	f.running++
	return true
}

// leave records err, if any, and unregisters an owner. The last owner to
// leave closes done, after its result is recorded.
func (f *envFrame) leave(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err != nil {
		f.errs = append(f.errs, err)
	}
	f.running--
	if f.running == 0 {
		f.cancelCtx()
		close(f.done)
	}
}

// spawn runs fn in an owned goroutine. Its error is recorded before the
// goroutine leaves. The returned channel closes after that.
func (f *envFrame) spawn(fn func() error) <-chan struct{} {
	c := make(chan struct{})
	f.spawnTo(c, fn)
	return c
}

// spawnTo is spawn with a caller-supplied channel. A frame that is already
// done runs nothing: it records the refusal and closes c.
func (f *envFrame) spawnTo(c chan struct{}, fn func() error) {
	if !f.enter() {
		f.record(errors.New("kubernetes environment operation started after frame end"))
		close(c)
		return
	}
	go func() {
		var err error
		defer func() {
			f.leave(err)
			close(c)
		}()
		err = fn()
	}()
}

// record appends err to the frame's failures.
func (f *envFrame) record(err error) {
	if err == nil {
		return
	}
	f.mu.Lock()
	f.errs = append(f.errs, err)
	f.mu.Unlock()
}

// cancel records errEnvCanceled and aborts the frame. It does nothing to a
// settled frame.
func (f *envFrame) cancel() {
	f.abort(true)
}

// abort latches close intent, aborts the upgrade request and closes the
// SPDY connection, or before an accepted upgrade the original connection, if
// it has been kept. One kept later is closed when it is kept. A SPDY
// connection with a stream request in flight is closed when that request
// returns; see createStream. If record is
// set, abort also records errEnvCanceled once. It does nothing to a settled
// frame and never waits.
func (f *envFrame) abort(record bool) {
	f.mu.Lock()
	select {
	case <-f.done:
		f.mu.Unlock()
		return
	default:
	}
	if record && !f.canceled {
		f.canceled = true
		f.errs = append(f.errs, errEnvCanceled)
	}
	f.closing = true
	f.mu.Unlock()
	f.cancelCtx()
	f.closeConn()
	f.closeRaw()
}

func (f *envFrame) isClosing() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closing
}

// closeConn latches close intent and starts the connection's single Close in
// an owned goroutine, if a connection is published and Close has not started.
// While a CreateStream is in flight the Close is deferred: the creating call
// starts it when it returns, so the GOAWAY that Close writes never precedes a
// stream request. After Close returns, the same goroutine waits for the
// original connection's recorded Close, which the SPDY connection makes. The
// returned channel closes when both have returned; it is nil when no
// connection is published. Waiting on it never repeats either Close.
func (f *envFrame) closeConn() <-chan struct{} {
	f.mu.Lock()
	f.closing = true
	conn := f.conn
	if conn == nil || f.connErr.started {
		defer f.mu.Unlock()
		return f.connDone
	}
	if f.connDone == nil {
		f.connDone = make(chan struct{})
	}
	c := f.connDone
	if f.creating > 0 {
		f.mu.Unlock()
		return c
	}
	f.connErr.started = true
	raw := f.raw
	f.mu.Unlock()
	f.spawnTo(c, func() error {
		err := conn.Close()
		f.mu.Lock()
		f.connErr.done, f.connErr.err = true, err
		f.mu.Unlock()
		// The SPDY connection owns raw and closes it once no stream of it
		// is open. This owner holds the frame until that Close returns; it
		// never closes raw itself.
		if raw != nil {
			<-raw.done
		}
		if err != nil {
			return fmt.Errorf("close kubernetes environment connection: %w", err)
		}
		return nil
	})
	return c
}

// publish installs the SPDY connection made from the original connection.
// A frame whose close intent is already latched closes it at once.
func (f *envFrame) publish(conn httpstream.Connection) {
	f.mu.Lock()
	f.conn = conn
	closing := f.closing
	f.mu.Unlock()
	if closing {
		f.closeConn()
	}
}

// envRaw is the original network connection of a frame: the TCP connection
// to the API server or to the first proxy. The frame keeps it before any
// byte is written to it, so cancellation can end every later phase: proxy
// TLS, CONNECT, SOCKS5, target TLS, the request write and the response read.
// Every layer above it closes it only through Close. Its first Close is the
// only Close of the original, and a failure is recorded once; every later or
// concurrent call waits for and returns that result.
type envRaw struct {
	net.Conn
	frame *envFrame
	op    envOp // guarded by frame.mu
	done  chan struct{}
}

func (c *envRaw) Close() error {
	f := c.frame
	f.mu.Lock()
	if c.op.started {
		f.mu.Unlock()
		<-c.done
		f.mu.Lock()
		defer f.mu.Unlock()
		return c.op.err
	}
	c.op.started = true
	f.mu.Unlock()
	err := c.Conn.Close()
	f.mu.Lock()
	c.op.done, c.op.err = true, err
	if err != nil {
		f.errs = append(f.errs, fmt.Errorf("close kubernetes environment original connection: %w", err))
	}
	f.mu.Unlock()
	close(c.done)
	return err
}

// keepRaw installs the original connection. A frame whose close intent is
// already latched closes it at once.
func (f *envFrame) keepRaw(c *envRaw) {
	f.mu.Lock()
	f.raw = c
	closing := f.closing
	f.mu.Unlock()
	if closing {
		f.closeRaw()
	}
}

// closeRaw latches close intent and, unless the SPDY connection owns it,
// starts the original connection's single Close in an owned goroutine. The
// returned channel closes when that Close has returned; it is nil when no
// original connection is kept or the SPDY connection owns it. An owned
// original connection is closed by the SPDY connection, and the frame's
// connection Close waits for it (see closeConn); the handoff error path of
// dial waits for it too.
func (f *envFrame) closeRaw() <-chan struct{} {
	f.mu.Lock()
	f.closing = true
	c := f.raw
	if c == nil || f.handed {
		f.mu.Unlock()
		return nil
	}
	start := !c.op.started
	f.mu.Unlock()
	if start {
		f.spawn(func() error {
			c.Close()
			return nil
		})
	}
	return c.done
}

// releaseRaw closes the original connection, if the frame keeps it, and
// waits for that Close to return.
func (f *envFrame) releaseRaw() {
	if c := f.closeRaw(); c != nil {
		<-c
	}
}

// keepBody records rc, a response body read from the frame's connection,
// limiting its reads to maxUpgradeErrorBody bytes if limit is set.
func (f *envFrame) keepBody(rc io.ReadCloser, limit bool) *envBody {
	b := &envBody{rc: rc, r: rc, frame: f}
	if limit {
		b.r = io.LimitReader(rc, maxUpgradeErrorBody)
	}
	f.mu.Lock()
	f.bodies = append(f.bodies, b)
	f.mu.Unlock()
	return b
}

// envBuffered is a connection whose reads come first from the reader that
// parsed a response on it, so bytes buffered past that response are kept.
type envBuffered struct {
	net.Conn
	br *bufio.Reader
}

func (c *envBuffered) Read(p []byte) (int, error) { return c.br.Read(p) }

// envDialer establishes a frame's connection in place of
// SpdyRoundTripper.Dial, which in apimachinery v0.32.3 runs a proxy CONNECT
// and the request write before it returns a connection and abandons the
// socket on a CONNECT refusal. envDialer makes the same connections: direct
// TCP, then TLS for an https server; an http, https or schemeless proxy with
// CONNECT and Basic Proxy-Authorization from the proxy URL; or a SOCKS5 proxy
// through golang.org/x/net/proxy with the URL's user and password. Any other
// proxy scheme is refused before a connection is made. The API server hop
// uses the configuration from rest.TLSConfigFor with ServerName defaulted to
// the host dialed, as that Dial does. It differs in custody: the TCP
// connection is kept by the frame before anything is written to it, and no
// step closes it except through the frame's single Close. It also differs on
// an https proxy hop: that Dial reuses the cluster TLS configuration there,
// client certificate included; envDialer uses proxyTLS, which presents no
// client identity. A proxy receives no cluster credential, only the
// Proxy-Authorization from its own URL.
type envDialer struct {
	tls     *tls.Config // nil for an http server without TLS settings
	proxy   func(*http.Request) (*url.URL, error)
	frame   *envFrame
	wrapRaw func(net.Conn) net.Conn
}

// establish returns a connection to req.URL ready for the upgrade request.
// On failure the original connection, if one was made, is left to the
// frame, which closes it.
func (d *envDialer) establish(req *http.Request) (net.Conn, error) {
	ctx, target := req.Context(), req.URL
	targetAddr := netutil.CanonicalAddr(target)
	proxyURL, err := d.proxy(req)
	if err != nil {
		return nil, fmt.Errorf("kubernetes environment proxy: %w", err)
	}
	var conn net.Conn
	switch {
	case proxyURL == nil:
		conn, err = d.dialRaw(ctx, "tcp", targetAddr, 0)
	case proxyURL.Scheme == "socks5":
		conn, err = d.socks5(ctx, proxyURL, targetAddr)
	case proxyURL.Scheme == "http" || proxyURL.Scheme == "https" || proxyURL.Scheme == "":
		conn, err = d.connect(ctx, proxyURL, targetAddr)
	default:
		return nil, fmt.Errorf("%w: %q", errEnvProxyScheme, proxyURL.Scheme)
	}
	if err != nil {
		return nil, err
	}
	if target.Scheme == "https" {
		host, _, err := net.SplitHostPort(targetAddr)
		if err != nil {
			return nil, fmt.Errorf("kubernetes environment TLS address: %w", err)
		}
		return tlsClient(ctx, conn, targetAddr, d.targetTLS(host))
	}
	return conn, nil
}

// dialRaw makes the frame's original TCP connection to addr and keeps it in
// the frame before returning it.
func (d *envDialer) dialRaw(ctx context.Context, network, addr string, timeout time.Duration) (net.Conn, error) {
	nd := net.Dialer{Timeout: timeout}
	c, err := nd.DialContext(ctx, network, addr)
	if err != nil {
		return nil, fmt.Errorf("dial kubernetes environment connection: %w", err)
	}
	if d.wrapRaw != nil {
		c = d.wrapRaw(c)
	}
	raw := &envRaw{Conn: c, frame: d.frame, done: make(chan struct{})}
	d.frame.keepRaw(raw)
	return raw, nil
}

// targetTLS returns the TLS configuration for the API server hop: the
// transport's, including its roots, client certificate and exec-plugin
// callbacks, with ServerName defaulted to host as SpdyRoundTripper does.
func (d *envDialer) targetTLS(host string) *tls.Config {
	config := d.tls
	switch {
	case config == nil:
		config = &tls.Config{ServerName: host}
	case config.ServerName == "":
		config = config.Clone()
		config.ServerName = host
	}
	return config
}

// proxyTLS returns the TLS configuration for an https proxy hop. It is a
// clone of the transport's with every client identity removed, so the
// cluster client certificate is never presented to a proxy, ServerName set
// to the proxy's host, and certificate verification always on against the
// transport's roots (the system roots when it has none). A proxy that
// requires a client certificate is not supported: its handshake fails.
func (d *envDialer) proxyTLS(host string) *tls.Config {
	config := &tls.Config{}
	if d.tls != nil {
		config = d.tls.Clone()
	}
	config.Certificates = nil
	config.GetClientCertificate = nil
	config.InsecureSkipVerify = false
	config.ServerName = host
	return config
}

// tlsClient runs a TLS handshake with config on conn to addr. A failed
// handshake's connection is left to the frame.
func tlsClient(ctx context.Context, conn net.Conn, addr string, config *tls.Config) (net.Conn, error) {
	tc := tls.Client(conn, config)
	if err := tc.HandshakeContext(ctx); err != nil {
		return nil, fmt.Errorf("kubernetes environment TLS handshake with %s: %w", addr, err)
	}
	return tc, nil
}

// connect opens a tunnel to targetAddr through an HTTP or HTTPS proxy. A
// refusal closes the original connection before the response body.
func (d *envDialer) connect(ctx context.Context, proxyURL *url.URL, targetAddr string) (net.Conn, error) {
	f := d.frame
	proxyAddr := netutil.CanonicalAddr(proxyURL)
	conn, err := d.dialRaw(ctx, "tcp", proxyAddr, 0)
	if err != nil {
		return nil, err
	}
	if proxyURL.Scheme == "https" {
		if conn, err = tlsClient(ctx, conn, proxyAddr, d.proxyTLS(proxyURL.Hostname())); err != nil {
			return nil, err
		}
	}
	creq := &http.Request{Method: http.MethodConnect, URL: &url.URL{}, Host: targetAddr, Header: http.Header{}}
	if u := proxyURL.User; u != nil {
		password, _ := u.Password()
		creq.Header.Set("Proxy-Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(u.Username()+":"+password)))
	}
	if err := creq.Write(conn); err != nil {
		return nil, fmt.Errorf("write kubernetes environment proxy CONNECT: %w", err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, creq)
	if err != nil {
		return nil, fmt.Errorf("read kubernetes environment proxy CONNECT: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b := f.keepBody(resp.Body, true)
		err := fmt.Errorf("%w: %s: %s", errEnvProxyRefused, proxyURL.Redacted(), resp.Status)
		f.releaseRaw()
		if cerr := b.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("close kubernetes environment proxy response: %w", cerr))
		}
		return nil, err
	}
	// The tunnel follows the accepted response's header. net/http gives a
	// CONNECT response without Content-Length a body that reads to the end
	// of the connection, so that body is neither read nor closed; it holds
	// nothing but br.
	return &envBuffered{Conn: conn, br: br}, nil
}

// socks5 opens a connection to targetAddr through a SOCKS5 proxy. The
// forward dialer keeps the original connection before the SOCKS5 protocol
// starts; x/net closes it on a failed negotiation only through the frame's
// single Close.
func (d *envDialer) socks5(ctx context.Context, proxyURL *url.URL, targetAddr string) (net.Conn, error) {
	var auth *proxy.Auth
	if u := proxyURL.User; u != nil {
		password, _ := u.Password()
		auth = &proxy.Auth{User: u.Username(), Password: password}
	}
	pd, err := proxy.SOCKS5("tcp", netutil.CanonicalAddr(proxyURL), auth, envForward{d})
	if err != nil {
		return nil, fmt.Errorf("kubernetes environment SOCKS5 proxy: %w", err)
	}
	cd, ok := pd.(proxy.ContextDialer)
	if !ok {
		return nil, errors.New("kubernetes environment SOCKS5 proxy dialer does not take a context")
	}
	conn, err := cd.DialContext(ctx, "tcp", targetAddr)
	if err != nil {
		return nil, fmt.Errorf("kubernetes environment SOCKS5 proxy: %w", err)
	}
	return conn, nil
}

// envForward is the SOCKS5 forward dialer of a frame. Its timeout is
// apimachinery's SOCKS5 default.
type envForward struct{ d *envDialer }

func (w envForward) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return w.d.dialRaw(ctx, network, addr, 30*time.Second)
}

// Dial is never used: x/net prefers DialContext, which observes the frame's
// context.
func (w envForward) Dial(network, addr string) (net.Conn, error) {
	return nil, errors.New("kubernetes environment SOCKS5 dial without a context")
}

// envUpgrade is the innermost round tripper of a frame's upgrade request,
// wrapped by rest.HTTPWrappersForConfig. It establishes the connection with
// the frame's envDialer, writes the request, and reads the response. It sends
// at most one request per frame.
type envUpgrade struct {
	dialer *envDialer
	frame  *envFrame
}

func (u *envUpgrade) RoundTrip(req *http.Request) (*http.Response, error) {
	f := u.frame
	f.mu.Lock()
	dialed := f.dialed
	f.dialed = true
	f.mu.Unlock()
	if dialed {
		return nil, errEnvReplay
	}
	req = req.Clone(req.Context())
	req.Header.Add(httpstream.HeaderConnection, httpstream.HeaderUpgrade)
	req.Header.Add(httpstream.HeaderUpgrade, spdyapi.HeaderSpdy31)
	conn, err := u.dialer.establish(req)
	if err != nil {
		return nil, err
	}
	if err := req.Write(conn); err != nil {
		return nil, fmt.Errorf("write kubernetes environment upgrade request: %w", err)
	}
	wire := &envBuffered{Conn: conn, br: bufio.NewReader(conn)}
	f.mu.Lock()
	f.wire = wire
	f.mu.Unlock()
	resp, err := http.ReadResponse(wire.br, req)
	if err != nil {
		return nil, err
	}
	resp.Body = f.keepBody(resp.Body, !upgraded(resp))
	return resp, nil
}

// upgraded reports whether resp accepts a SPDY/3.1 upgrade, as
// SpdyRoundTripper.NewConnection decides.
func upgraded(resp *http.Response) bool {
	connection := strings.ToLower(resp.Header.Get(httpstream.HeaderConnection))
	upgrade := strings.ToLower(resp.Header.Get(httpstream.HeaderUpgrade))
	return resp.StatusCode == http.StatusSwitchingProtocols &&
		strings.Contains(connection, strings.ToLower(httpstream.HeaderUpgrade)) &&
		strings.Contains(upgrade, strings.ToLower(spdyapi.HeaderSpdy31))
}

// dial makes the frame's single upgrade request and returns the published
// SPDY connection. It must run in an owned goroutine. On every failure it
// returns only after the original connection, if one was kept, and the
// response body, if one was read, have each been closed once.
func (f *envFrame) dial(t *envTransport, u *url.URL, protocol string) (httpstream.Connection, error) {
	tlsConfig, err := rest.TLSConfigFor(t.config)
	if err != nil {
		return nil, fmt.Errorf("kubernetes environment transport: %w", err)
	}
	proxyFunc := http.ProxyFromEnvironment
	if t.config.Proxy != nil {
		proxyFunc = t.config.Proxy
	}
	dialer := &envDialer{tls: tlsConfig, proxy: proxyFunc, frame: f, wrapRaw: t.wrapRaw}
	rt, err := rest.HTTPWrappersForConfig(t.config, &envUpgrade{dialer: dialer, frame: f})
	if err != nil {
		return nil, fmt.Errorf("kubernetes environment transport: %w", err)
	}
	req, err := http.NewRequestWithContext(f.ctx, http.MethodPost, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("kubernetes environment request: %w", err)
	}
	req.Header.Set(httpstream.HeaderProtocolVersion, protocol)
	resp, err := rt.RoundTrip(req)
	if err != nil {
		f.releaseRaw()
		return nil, fmt.Errorf("kubernetes environment upgrade: %w", err)
	}
	// release ends a failed upgrade: the original connection is closed
	// first, which ends any read of it, then the body.
	release := func(err error) (httpstream.Connection, error) {
		f.releaseRaw()
		if cerr := resp.Body.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("close kubernetes environment upgrade response: %w", cerr))
		}
		return nil, err
	}
	if !upgraded(resp) {
		msg, rerr := io.ReadAll(resp.Body)
		err := fmt.Errorf("%w: %s: %q", errEnvRefused, resp.Status, bytesPrefix(msg, 512))
		if resp.StatusCode >= 300 && resp.StatusCode < 400 {
			err = errors.Join(errEnvRedirect, err)
		}
		if rerr != nil {
			err = errors.Join(err, fmt.Errorf("read kubernetes environment upgrade refusal: %w", rerr))
		}
		return release(err)
	}
	if got := resp.Header.Get(httpstream.HeaderProtocolVersion); got != protocol {
		return release(fmt.Errorf("%w: %q", errEnvProtocol, got))
	}
	if f.isClosing() {
		return release(errEnvCanceled)
	}
	if err := resp.Body.Close(); err != nil {
		return release(fmt.Errorf("close kubernetes environment upgrade response: %w", err))
	}
	// The handoff and close intent are decided under one lock: once intent
	// is latched the frame closes the original connection itself, and
	// after the handoff only the SPDY connection does.
	f.mu.Lock()
	if f.closing {
		f.mu.Unlock()
		return release(errEnvCanceled)
	}
	raw, wire := f.raw, f.wire
	f.handed = true
	f.mu.Unlock()
	// On error NewClientConnectionWithPings closes wire, which closes raw
	// through its single recorded Close.
	conn, err := spdyapi.NewClientConnectionWithPings(wire, envPingPeriod)
	if err != nil {
		<-raw.done
		return nil, fmt.Errorf("kubernetes environment SPDY connection: %w", err)
	}
	if t.wrap != nil {
		conn = t.wrap(f, conn)
	}
	f.publish(conn)
	if f.isClosing() {
		return nil, errEnvCanceled
	}
	return conn, nil
}

func bytesPrefix(b []byte, n int) []byte {
	if len(b) > n {
		return b[:n]
	}
	return b
}

// envBody is the response body of a frame's upgrade. Its first Close is the
// only Close of the original; later calls return the recorded result. A
// refused upgrade's body is limited to maxUpgradeErrorBody bytes. net/http's
// body Close reads the rest of an unfinished body, so dial closes the
// original connection before it closes a body it did not finish.
type envBody struct {
	rc    io.ReadCloser // the original
	r     io.Reader     // rc, or a limited reader of it
	frame *envFrame
	op    envOp // guarded by frame.mu
	once  sync.Once
}

func (b *envBody) Read(p []byte) (int, error) { return b.r.Read(p) }

func (b *envBody) Close() error {
	b.once.Do(func() {
		b.frame.mu.Lock()
		b.op.started = true
		b.frame.mu.Unlock()
		err := b.rc.Close()
		b.frame.mu.Lock()
		b.op.done, b.op.err = true, err
		b.frame.mu.Unlock()
	})
	b.frame.mu.Lock()
	defer b.frame.mu.Unlock()
	return b.op.err
}

// envStream is a stream created on a frame's connection. FIN and Reset are
// separate original operations; each runs at most once and keeps its first
// result. A FIN does not suppress a later Reset.
type envStream struct {
	stream httpstream.Stream
	name   string
	frame  *envFrame
	fin    envOp // guarded by frame.mu
	reset  envOp // guarded by frame.mu
}

// createStream creates one stream with headers on conn and records it in the
// frame before returning it. Once close intent is latched it requests no
// stream. A CreateStream in flight defers the connection's Close until it
// returns, and then starts it if close intent was latched meanwhile.
//
// The order matters because apimachinery v0.32.3's CreateStream waits up to
// 30 seconds for the peer's reply and registers the stream only after it, and
// connection Close resets only registered streams before spdystream v0.5.0
// writes GOAWAY. A peer stops reading at GOAWAY, so a stream requested after
// it is never answered, and losing the connection does not end that wait. In
// this order every created stream is registered before Close resets it, and
// no request follows the GOAWAY. A peer that never replies still holds the
// frame until CreateStream returns. spdystream keeps the stream of a
// CreateStream that failed waiting for its reply, and closes the original
// connection only once no stream is open, so such a peer then holds the
// frame until the library's read of the connection ends.
func (f *envFrame) createStream(conn httpstream.Connection, name string, headers http.Header) (*envStream, error) {
	f.mu.Lock()
	if f.closing {
		f.mu.Unlock()
		return nil, fmt.Errorf("create kubernetes %s stream: %w", name, errEnvCanceled)
	}
	f.creating++
	f.mu.Unlock()
	s, err := conn.CreateStream(headers)
	var es *envStream
	f.mu.Lock()
	f.creating--
	if err == nil {
		es = &envStream{stream: s, name: name, frame: f}
		f.streams = append(f.streams, es)
	}
	closing := f.closing
	f.mu.Unlock()
	if closing {
		f.closeConn()
	}
	if err != nil {
		return nil, fmt.Errorf("create kubernetes %s stream: %w", name, err)
	}
	return es, nil
}

// closeWrite makes the stream's single FIN and records its result.
func (s *envStream) closeWrite() error {
	return s.once(&s.fin, "close", s.stream.Close)
}

// resetStream makes the stream's single Reset and records its result.
func (s *envStream) resetStream() error {
	return s.once(&s.reset, "reset", s.stream.Reset)
}

func (s *envStream) Read(p []byte) (int, error)  { return s.stream.Read(p) }
func (s *envStream) Write(p []byte) (int, error) { return s.stream.Write(p) }

func (s *envStream) once(op *envOp, what string, fn func() error) error {
	f := s.frame
	f.mu.Lock()
	if op.started {
		err := op.err
		f.mu.Unlock()
		return err
	}
	op.started = true
	f.mu.Unlock()
	err := fn()
	if err != nil {
		err = fmt.Errorf("%s kubernetes %s stream: %w", what, s.name, err)
	}
	f.mu.Lock()
	op.done, op.err = true, err
	f.mu.Unlock()
	return err
}

// resetAll makes the explicit Reset of every created stream, in creation
// order, recording each result.
func (f *envFrame) resetAll() {
	f.mu.Lock()
	streams := append([]*envStream(nil), f.streams...)
	f.mu.Unlock()
	for _, s := range streams {
		if err := s.resetStream(); err != nil {
			f.record(err)
		}
	}
}

// envState is a snapshot of a frame's custody.
type envState struct {
	// Settled reports that every owned goroutine returned: the upgrade call,
	// each application worker, each stream operation started, and the
	// connection's Close if a connection was published, which returns only
	// after the original connection's Close, made by the SPDY connection,
	// returned.
	Settled bool
	// Err joins every recorded failure, including cancellation and a refused
	// upgrade. A settled frame with a nil Err closed everything it opened
	// without a reported error.
	Err error
}

func (f *envFrame) state() envState {
	select {
	case <-f.done:
	default:
		f.mu.Lock()
		defer f.mu.Unlock()
		errs := append([]error(nil), f.errs...)
		switch raw := f.raw; {
		case raw == nil:
		case raw.op.started && !raw.op.done:
			// Held: the original connection's single Close has not returned.
			errs = append(errs, errEnvRawClosing)
		case !raw.op.started && f.handed && f.connErr.done:
			// Held: the SPDY connection keeps the original open.
			errs = append(errs, errEnvRawHeld)
		}
		return envState{Err: errors.Join(errs...)}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	errs := append([]error(nil), f.errs...)
	for _, b := range f.bodies {
		if !b.op.done {
			// The client returned a body that nothing closed.
			errs = append(errs, errors.New("kubernetes environment response body not closed"))
		}
	}
	if f.conn != nil && !f.connErr.done {
		errs = append(errs, errors.New("kubernetes environment connection not closed"))
	}
	if f.raw != nil && !f.raw.op.done {
		// Before the handoff the frame closes the original connection, and
		// after it the connection's Close waits for it, so this is a lost
		// receipt, never a settled close.
		errs = append(errs, errors.New("kubernetes environment original connection not closed"))
	}
	return envState{Settled: true, Err: errors.Join(errs...)}
}
