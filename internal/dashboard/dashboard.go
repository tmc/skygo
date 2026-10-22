package dashboard

import (
	"bytes"
	"context"
	"crypto/rand"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"sync"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
)

// Reader is the read-only subset of the generated control client that the
// dashboard calls. It has no method that submits, cancels, prunes or
// transfers anything.
type Reader interface {
	GetServerInfo(context.Context, *connect.Request[v1.GetServerInfoRequest]) (*connect.Response[v1.GetServerInfoResponse], error)
	ListTargets(context.Context, *connect.Request[v1.ListTargetsRequest]) (*connect.Response[v1.ListTargetsResponse], error)
	GetRetention(context.Context, *connect.Request[v1.GetRetentionRequest]) (*connect.Response[v1.GetRetentionResponse], error)
	ListJobs(context.Context, *connect.Request[v1.ListJobsRequest]) (*connect.Response[v1.ListJobsResponse], error)
	GetJob(context.Context, *connect.Request[v1.GetJobRequest]) (*connect.Response[v1.GetJobResponse], error)
	StreamJobLogs(context.Context, *connect.Request[v1.StreamJobLogsRequest]) (*connect.ServerStreamForClient[v1.StreamJobLogsResponse], error)
	StreamRetainedJobLogs(context.Context, *connect.Request[v1.StreamRetainedJobLogsRequest]) (*connect.ServerStreamForClient[v1.StreamRetainedJobLogsResponse], error)
}

// Config configures Serve. Client must attach the caller's credential to
// every request; Timeout is the positive per-RPC deadline.
type Config struct {
	Client  Reader
	Timeout time.Duration
}

const (
	maxRequests = 16
	maxFollows  = 4
	maxBody     = 1 << 10 // bytes of any request body; only POST may have one
	bodyTimeout = 10 * time.Second
	followLimit = 10 * time.Minute
	csp         = "default-src 'none'; script-src 'self'; connect-src 'self'; style-src 'self'; img-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'"

	// proofHeader carries the session proof. Browsers send cookies to every
	// port of a host but send this header only when the dashboard's own
	// script adds it, so a cookie harvested by another local server
	// authorizes nothing.
	proofHeader = "X-Skygo-Session"
	// loginHeader marks a login made by the dashboard's script. A plain
	// form post cannot set it.
	loginHeader = "X-Skygo-Login"
)

var errLoopback = errors.New("dashboard listens only on a loopback address")

// Listen binds addr, which must be 127.0.0.1 or [::1] with a port. Port 0
// selects a free port. Host names and other addresses are refused unbound.
func Listen(addr string) (net.Listener, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, errLoopback
	}
	ip, err := netip.ParseAddr(host)
	if err != nil || !loopback(ip) {
		return nil, errLoopback
	}
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return nil, errors.New("invalid dashboard port")
	}
	return net.Listen("tcp", net.JoinHostPort(ip.String(), port))
}

func loopback(ip netip.Addr) bool {
	return ip == netip.AddrFrom4([4]byte{127, 0, 0, 1}) || ip == netip.IPv6Loopback()
}

// Serve runs the dashboard on ln until ctx is done, then closes ln. It first
// writes the dashboard URL and the one-time login code to log, which should
// be the terminal's standard error. Shutdown cancels open log streams, waits
// up to five seconds for responses to finish and then closes connections.
func Serve(ctx context.Context, ln net.Listener, cfg Config, log io.Writer) error {
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok || !loopback(addr.AddrPort().Addr().Unmap()) {
		ln.Close()
		return errLoopback
	}
	s, err := newServer(cfg, ln.Addr().String(), time.Now, rand.Reader)
	if err != nil {
		ln.Close()
		return err
	}
	if _, err := fmt.Fprintf(log, "dashboard: http://%s/\nlogin code: %s\n", s.host, s.code.value); err != nil {
		ln.Close()
		return err
	}
	return s.serve(ctx, ln)
}

// serve runs s on ln until ctx is done.
func (s *server) serve(ctx context.Context, ln net.Listener) error {
	base, stop := context.WithCancel(context.Background())
	defer stop()
	// ReadTimeout stays zero: net/http cancels a request's context when its
	// connection read deadline passes, which would end long log streams.
	// Instead, ServeHTTP refuses bodies on GET and bounds POST bodies in
	// size and time; headers have ReadHeaderTimeout.
	srv := &http.Server{
		Handler:           s,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
		// net/http answers "OPTIONS *" itself, copying up to 4 KiB of
		// body before ServeHTTP's checks and deadline; route it to them.
		DisableGeneralOptionsHandler: true,
		BaseContext:                  func(net.Listener) context.Context { return base },
	}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	select {
	case err := <-served:
		return err
	case <-ctx.Done():
	}
	stop()
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		srv.Close()
	}
	if err := <-served; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

type server struct {
	client   Reader
	timeout  time.Duration
	host     string // exact bound "IP:port"
	origin   string
	cookie   string // session cookie name
	now      func() time.Time
	random   io.Reader
	mux      *http.ServeMux
	pages    map[string]*template.Template
	requests chan struct{}
	follows  chan struct{}

	bodyTimeout time.Duration // POST body read deadline
	followLimit time.Duration // lifetime of one followed log stream

	mu      sync.Mutex
	code    loginCode
	session *session
}

//go:embed templates/*.html
var templateFiles embed.FS

//go:embed static/style.css
var style []byte

//go:embed static/app.js
var script []byte

var pageNames = []string{"login", "shell", "jobs", "job", "logs", "targets", "info", "message"}

func newServer(cfg Config, host string, now func() time.Time, random io.Reader) (*server, error) {
	if cfg.Client == nil || cfg.Timeout <= 0 {
		return nil, errors.New("dashboard requires a client and a positive timeout")
	}
	_, port, err := net.SplitHostPort(host)
	if err != nil {
		return nil, fmt.Errorf("dashboard address: %w", err)
	}
	code, err := randomHex(random, 16)
	if err != nil {
		return nil, fmt.Errorf("login code: %w", err)
	}
	s := &server{
		client:   cfg.Client,
		timeout:  cfg.Timeout,
		host:     host,
		origin:   "http://" + host,
		cookie:   cookiePrefix + port,
		now:      now,
		random:   random,
		mux:      http.NewServeMux(),
		pages:    make(map[string]*template.Template),
		requests: make(chan struct{}, maxRequests),
		follows:  make(chan struct{}, maxFollows),
		code:     loginCode{value: code, expires: now().Add(codeLifetime)},

		bodyTimeout: bodyTimeout,
		followLimit: followLimit,
	}
	for _, name := range pageNames {
		t, err := template.ParseFS(templateFiles, "templates/layout.html", "templates/"+name+".html")
		if err != nil {
			return nil, err
		}
		s.pages[name] = t
	}
	s.mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/jobs", http.StatusSeeOther) })
	s.mux.HandleFunc("GET /login", s.loginForm)
	s.mux.HandleFunc("POST /login", s.loginSubmit)
	s.mux.HandleFunc("POST /logout", s.authed(s.logout))
	s.mux.HandleFunc("GET /jobs", s.authed(s.jobs))
	s.mux.HandleFunc("GET /jobs/{id}", s.authed(s.job))
	s.mux.HandleFunc("GET /jobs/{id}/logs", s.authed(s.logs))
	s.mux.HandleFunc("GET /jobs/{id}/logs.txt", s.authed(s.logsText))
	s.mux.HandleFunc("GET /targets", s.authed(s.targets))
	s.mux.HandleFunc("GET /info", s.authed(s.info))
	s.mux.HandleFunc("GET /static/style.css", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
		w.Write(style)
	})
	s.mux.HandleFunc("GET /static/app.js", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
		w.Write(script)
	})
	return s, nil
}

// ServeHTTP applies the response headers, the exact Host check, the method
// check, the POST origin check, the body rules and the request bound before
// routing.
func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("Content-Security-Policy", csp)
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Referrer-Policy", "no-referrer")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Frame-Options", "DENY")
	// A request with a body gets a connection read deadline before any
	// response. net/http drains an unread body after the handler returns,
	// even for a refused request with Connection: close, so without the
	// deadline a body that stops arriving would hold the connection. The
	// deadline stays set through that drain. Bodyless requests, including
	// log follows, get none: an expired read deadline cancels the request.
	if hasBody(r) {
		_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(s.bodyTimeout))
	}
	if r.Host != s.host {
		refuse(w, http.StatusMisdirectedRequest, "misdirected request")
		return
	}
	// Only GET and POST are served. ServeMux would otherwise run GET
	// handlers, including log follows, for HEAD.
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		h.Set("Allow", "GET, POST")
		refuse(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if r.Method == http.MethodPost && !s.sameOrigin(r) {
		refuse(w, http.StatusForbidden, "cross-origin request refused")
		return
	}
	// The body is read before a request slot is taken, so bodies that stall
	// until the read deadline cannot hold the slots that reads need.
	if !s.body(w, r) {
		return
	}
	select {
	case s.requests <- struct{}{}:
		defer func() { <-s.requests }()
	default:
		http.Error(w, "dashboard busy", http.StatusServiceUnavailable)
		return
	}
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(3*s.timeout + 10*time.Second))
	s.mux.ServeHTTP(w, r)
}

// body enforces the request body rules: no Expect header, no body except on
// POST, and a POST body of at most maxBody bytes, which must arrive within
// the read deadline ServeHTTP set. It reads a POST body completely, so that
// handlers see it in memory.
func (s *server) body(w http.ResponseWriter, r *http.Request) bool {
	// Wrapping the body would hide net/http's 100-continue handling, and a
	// client waiting for 100 Continue would stall until the deadline. The
	// refused client was never asked for its body, so the drain after the
	// response ends at once rather than at the deadline.
	if r.Header.Get("Expect") != "" {
		_ = http.NewResponseController(w).SetReadDeadline(time.Now())
		refuse(w, http.StatusExpectationFailed, "expect not supported")
		return false
	}
	if r.Method != http.MethodPost {
		if hasBody(r) {
			refuse(w, http.StatusBadRequest, "request body not allowed")
			return false
		}
		return true
	}
	if r.ContentLength > maxBody {
		refuse(w, http.StatusRequestEntityTooLarge, "request body too large")
		return false
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			refuse(w, http.StatusRequestEntityTooLarge, "request body too large")
		} else {
			refuse(w, http.StatusBadRequest, "request body incomplete")
		}
		return false
	}
	r.Body = io.NopCloser(bytes.NewReader(data))
	return true
}

// hasBody reports whether r declares a request body.
func hasBody(r *http.Request) bool {
	return r.ContentLength != 0 || len(r.TransferEncoding) > 0
}

// refuse answers a request refused before routing and closes the connection
// after the response. Any unread body is drained only until the read
// deadline ServeHTTP set.
func refuse(w http.ResponseWriter, code int, text string) {
	w.Header().Set("Connection", "close")
	http.Error(w, text, code)
}

// sameOrigin requires one Origin equal to the dashboard origin or, without
// Origin, a browser's same-origin fetch metadata.
func (s *server) sameOrigin(r *http.Request) bool {
	switch origins := r.Header.Values("Origin"); len(origins) {
	case 0:
		return r.Header.Get("Sec-Fetch-Site") == "same-origin"
	case 1:
		return origins[0] == s.origin
	}
	return false
}

// authed runs next only for a request carrying both the session cookie and
// the session proof header. The cookie alone gets, for a GET, the static
// shell page, which holds no data; its script repeats the request with the
// proof. Nothing else is served on the cookie alone.
//
// A request with a proof header gets 401 only when the proof is not the live
// session's: the script then discards it. A live proof without the cookie
// gets 403, which leaves the proof in place, so another local page that
// disturbs the cookie cannot make the tab discard its session.
func (s *server) authed(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Vary", proofHeader)
		cookie, proof := s.authenticate(r)
		switch {
		case cookie && proof:
			next(w, r)
		case proof:
			http.Error(w, "session cookie missing; reload this page", http.StatusForbidden)
		case r.Header.Get(proofHeader) != "":
			http.Error(w, "session required", http.StatusUnauthorized)
		case r.Method != http.MethodGet:
			http.Error(w, "session required", http.StatusForbidden)
		case !cookie:
			http.Redirect(w, r, "/login", http.StatusSeeOther)
		default:
			s.render(w, http.StatusOK, "shell", "skygo", nil)
		}
	}
}

// view is the data of every page. Nav is set on the shell.
type view struct {
	Title string
	Nav   bool
	Data  any
}

// render writes a page. The login page and the shell are whole documents
// holding no job data; every other page is a fragment that the shell's
// script fetches with the session proof and inserts.
func (s *server) render(w http.ResponseWriter, status int, page string, title string, data any) {
	name := "fragment"
	if page == "login" || page == "shell" {
		name = "layout"
	}
	var buf bytes.Buffer
	if err := s.pages[page].ExecuteTemplate(&buf, name, view{Title: title, Nav: page == "shell", Data: data}); err != nil {
		http.Error(w, "render failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(10 * time.Second))
	w.WriteHeader(status)
	w.Write(buf.Bytes())
}

type message struct{ Text, Detail string }

func (s *server) message(w http.ResponseWriter, status int, text, detail string) {
	s.render(w, status, "message", text, message{text, detail})
}

// notFound is the single body for invalid, missing and foreign IDs.
func (s *server) notFound(w http.ResponseWriter) {
	s.message(w, http.StatusNotFound, "not found", "")
}

// failure renders an RPC or selection error. It never retries; a rejected
// credential is final for this page.
func (s *server) failure(w http.ResponseWriter, err error) {
	var rpc *connect.Error
	if !errors.As(err, &rpc) {
		s.message(w, http.StatusBadGateway, "request failed", err.Error())
		return
	}
	switch rpc.Code() {
	case connect.CodeUnauthenticated:
		s.message(w, http.StatusUnauthorized, "credential rejected", "")
	case connect.CodeNotFound:
		s.notFound(w)
	case connect.CodePermissionDenied:
		s.message(w, http.StatusForbidden, "permission denied", "")
	case connect.CodeUnavailable:
		s.message(w, http.StatusBadGateway, "server unavailable", "")
	default:
		if reason := expiry(err); reason != "" {
			s.message(w, http.StatusGone, reason, "")
			return
		}
		s.message(w, http.StatusBadGateway, "request failed", rpc.Code().String()+": "+rpc.Message())
	}
}

// expiry returns the retention expiry reason carried by err, worded as the
// CLI's retention diagnostic, or "".
func expiry(err error) string {
	var rpc *connect.Error
	if !errors.As(err, &rpc) {
		return ""
	}
	for _, detail := range rpc.Details() {
		value, err := detail.Value()
		if err != nil {
			continue
		}
		if e, ok := value.(*v1.RetentionExpiry); ok {
			switch e.Reason {
			case v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_RESULT_EXPIRED:
				return "result expired"
			case v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_REQUEST_EPOCH_EXPIRED:
				return "request epoch expired"
			case v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_REQUEST_DOMAIN_REQUIRED:
				return "explicit request domain required"
			case v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_REQUEST_LINEAGE_CHANGED:
				return "request lineage changed"
			case v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_LOGS_EXPIRED:
				return "logs expired"
			}
		}
	}
	return ""
}
