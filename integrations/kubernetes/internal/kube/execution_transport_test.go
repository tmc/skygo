package kube

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// The servers below are loopback endpoints. Redirect responses, partial
// bodies and HTTP/2 stream resets are MODELED stimuli for the client
// transport; none is a Kubernetes API server or native qualification.

// countingServer records every request it receives: method and path, POSTs
// whose body was read completely, credentials and protocol.
type countingServer struct {
	*httptest.Server
	reply func(w http.ResponseWriter, r *http.Request)

	mu       sync.Mutex
	requests []string
	posts    int
	auth     int
	protos   []string
}

// newCountingServer starts a counting server, over TLS with HTTP/2 enabled
// if secure. Its cleanup closes it, joining its handlers.
func newCountingServer(t *testing.T, secure bool, reply func(w http.ResponseWriter, r *http.Request)) *countingServer {
	t.Helper()
	s := &countingServer{reply: reply}
	s.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.Copy(io.Discard, r.Body)
		s.mu.Lock()
		s.requests = append(s.requests, r.Method+" "+r.URL.Path)
		if r.Method == "POST" && err == nil {
			s.posts++
		}
		if r.Header.Get("Authorization") != "" {
			s.auth++
		}
		s.protos = append(s.protos, r.Proto)
		reply := s.reply
		s.mu.Unlock()
		reply(w, r)
	}))
	if secure {
		s.EnableHTTP2 = true
		s.StartTLS()
	} else {
		s.Start()
	}
	t.Cleanup(s.Close)
	return s
}

func (s *countingServer) counts() (requests []string, posts, auth int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.requests), s.posts, s.auth
}

// pemCertificate returns the PEM encoding of the certificate of an
// httptest TLS server.
func pemCertificate(s *httptest.Server) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw})
}

func createPath(kind string) string {
	if kind == "Job" {
		return jobsPath
	}
	return podsPath
}

// TestReceiptCreateRedirectRefused sends receipted creates of a Job and a
// retry Pod to an origin that answers 307 or 308 with a Location on a
// destination server, over HTTP and over TLS with HTTP/2. The receipt client
// does not follow: the origin receives exactly one POST, the destination no
// request, body or credential. The complete redirect is refused durably,
// with a witness of its actual status and exact body, whether client-go can
// decode the body (a Status claiming AlreadyExists is not a definite
// rejection) or not (HTML). Neither the original nor a restarted Backend
// reads, creates or deletes afterwards.
func TestReceiptCreateRedirectRefused(t *testing.T) {
	html := []byte("<a href=\"/elsewhere\">Temporary Redirect</a>.\n")
	status := []byte(`{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"AlreadyExists","code":409}` + "\n")
	for _, secure := range []bool{false, true} {
		for _, kind := range []string{"Job", "Pod"} {
			for _, tt := range []struct {
				code  int
				ctype string
				body  []byte
			}{
				{307, "text/html; charset=utf-8", html},
				{308, "application/json", status},
			} {
				t.Run(fmt.Sprintf("tls=%v/%s/%d", secure, kind, tt.code), func(t *testing.T) {
					ctx := context.Background()
					root := receiptDir(t)
					dest := newCountingServer(t, secure, func(w http.ResponseWriter, r *http.Request) {
						w.WriteHeader(201)
					})
					origin := newCountingServer(t, secure, func(w http.ResponseWriter, r *http.Request) {
						w.Header().Set("Location", dest.URL+r.URL.Path)
						w.Header().Set("Content-Type", tt.ctype)
						w.WriteHeader(tt.code)
						w.Write(tt.body)
					})
					config := &rest.Config{Host: origin.URL, BearerToken: "secret-token"}
					if secure {
						config.TLSClientConfig.CAData = pemCertificate(origin.Server)
					}
					b := newReceiptBackendConfig(t, root, config)
					ref := receiptRef(b, kind)
					err := b.Start(ctx, ref.ID, ref.Task)
					if !errors.Is(err, backend.ErrIdentityMismatch) || !strings.Contains(err.Error(), "status "+strconv.Itoa(tt.code)) {
						t.Fatalf("start: %v", err)
					}
					requests, posts, auth := origin.counts()
					if !slices.Equal(requests, []string{"POST " + createPath(kind)}) || posts != 1 || auth != 1 {
						t.Fatalf("origin: %q, %d posts, %d with credentials", requests, posts, auth)
					}
					origin.mu.Lock()
					proto := origin.protos[0]
					origin.mu.Unlock()
					if secure && proto != "HTTP/2.0" {
						t.Fatalf("origin protocol %q, want HTTP/2.0", proto)
					}
					if requests, posts, auth := dest.counts(); len(requests) != 0 || posts != 0 || auth != 0 {
						t.Fatalf("destination reached: %q, %d posts, %d with credentials", requests, posts, auth)
					}
					restarted := newReceiptBackendConfig(t, root, config)
					r, err := restarted.receipts.load(testID)
					sum := sha256.Sum256(tt.body)
					want := receiptWitness{Status: tt.code, ResponseSHA256: fmt.Sprintf("%x", sum), ResponseBytes: int64(len(tt.body)), Decode: witnessUndecodable}
					if err != nil || r.Phase != phaseRefused || r.UID != "" || r.Witness == nil || *r.Witness != want {
						t.Fatalf("receipt: %+v %v, want witness %+v", r, err, want)
					}
					before := receiptBytes(t, b, testID)
					for i, c := range []*Backend{b, restarted} {
						if err := c.Start(ctx, ref.ID, ref.Task); !errors.Is(err, errReceiptExists) {
							t.Fatalf("backend %d start: %v", i, err)
						}
						for name, op := range refusedOps(ctx, c, ref, kind, "uid-redirected") {
							if err := op(); !errors.Is(err, errReceiptRefused) {
								t.Fatalf("backend %d %s: %v", i, name, err)
							}
						}
					}
					if requests, _, _ := origin.counts(); len(requests) != 1 {
						t.Fatalf("origin requests after refusal: %q", requests)
					}
					if requests, _, _ := dest.counts(); len(requests) != 0 {
						t.Fatalf("destination requests after refusal: %q", requests)
					}
					if !bytes.Equal(before, receiptBytes(t, b, testID)) {
						t.Fatal("refused receipt changed")
					}
				})
			}
		}
	}
}

// TestReceiptCreateRedirectIncomplete is the twin of an incomplete redirect:
// the 307 status line arrives with half of its declared body, then the
// connection or stream is aborted. That is not a known response; the receipt
// stays sent, without a witness, and nothing reached the destination.
func TestReceiptCreateRedirectIncomplete(t *testing.T) {
	body := []byte("<a href=\"/elsewhere\">Temporary Redirect</a>.\n")
	for _, secure := range []bool{false, true} {
		t.Run(fmt.Sprintf("tls=%v", secure), func(t *testing.T) {
			ctx := context.Background()
			root := receiptDir(t)
			dest := newCountingServer(t, secure, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(201) })
			origin := newCountingServer(t, secure, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", dest.URL+r.URL.Path)
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				w.Header().Set("Content-Length", strconv.Itoa(2*len(body)))
				w.WriteHeader(307)
				w.Write(body)
				w.(http.Flusher).Flush()
				panic(http.ErrAbortHandler)
			})
			config := &rest.Config{Host: origin.URL, BearerToken: "secret-token"}
			if secure {
				config.TLSClientConfig.CAData = pemCertificate(origin.Server)
			}
			b := newReceiptBackendConfig(t, root, config)
			ref := receiptRef(b, "Pod")
			if err := b.Start(ctx, ref.ID, ref.Task); err == nil || errors.Is(err, backend.ErrIdentityMismatch) {
				t.Fatalf("incomplete redirect: %v", err)
			}
			if r, err := newReceiptBackendConfig(t, root, config).receipts.load(testID); err != nil || r.Phase != phaseSent || r.Witness != nil {
				t.Fatalf("receipt: %+v %v", r, err)
			}
			if _, posts, _ := origin.counts(); posts != 1 {
				t.Fatalf("origin posts: %d", posts)
			}
			if requests, _, _ := dest.counts(); len(requests) != 0 {
				t.Fatalf("destination reached: %q", requests)
			}
		})
	}
}

// TestReceiptCreateRedirectControl is the positive control: the client built
// from the same frozen configuration as kubernetes.NewForConfig builds it,
// which FromTarget uses, follows the 307 and sends the POST, its body and the
// credential to the destination as well. The receipt client is what differs.
func TestReceiptCreateRedirectControl(t *testing.T) {
	for _, secure := range []bool{false, true} {
		t.Run(fmt.Sprintf("tls=%v", secure), func(t *testing.T) {
			dest := newCountingServer(t, secure, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(201)
				w.Write([]byte("{}"))
			})
			origin := newCountingServer(t, secure, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", dest.URL+r.URL.Path)
				w.WriteHeader(307)
			})
			config := &rest.Config{Host: origin.URL, BearerToken: "secret-token"}
			if secure {
				config.TLSClientConfig.CAData = pemCertificate(origin.Server)
			}
			_, frozen, err := fromRESTFrozen(context.Background(), TargetConfig{Name: "test", Namespace: "workloads"}, "owner", config)
			if err != nil {
				t.Fatal(err)
			}
			cs, err := kubernetes.NewForConfig(frozen)
			if err != nil {
				t.Fatal(err)
			}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "control", Namespace: "workloads"}}
			var code int
			res := cs.CoreV1().RESTClient().Post().Namespace("workloads").Resource("pods").Body(pod).MaxRetries(0).Do(context.Background()).StatusCode(&code)
			if err := res.Error(); err != nil || code != 201 {
				t.Fatalf("followed create: %d %v", code, err)
			}
			if _, posts, auth := origin.counts(); posts != 1 || auth != 1 {
				t.Fatalf("origin: %d posts, %d with credentials", posts, auth)
			}
			if _, posts, auth := dest.counts(); posts != 1 || auth != 1 {
				t.Fatalf("destination: %d posts, %d with credentials; the default client should follow", posts, auth)
			}
		})
	}
}

// retainedH2Peers keeps peers whose handlers did not return reachable for
// the rest of the test binary.
var retainedH2Peers []*h2Peer

// h2Peer is a MODELED HTTP/2 endpoint over TLS, written with the Framer and
// HPACK encoder of the module's x/net (selected v0.56.0). For each request it
// records the complete body, then, while fewer than resets bodies were
// received, resets the stream with code, a peer protocol stimulus sent only
// after the whole body arrived; otherwise it replies with reply. It is not a
// Kubernetes API server.
type h2Peer struct {
	ln      net.Listener
	code    http2.ErrCode
	resets  int
	reply   func(path string, body []byte) (int, []byte)
	fixture *receiptFixture

	mu     sync.Mutex
	closed bool
	conns  []net.Conn
	protos []string
	paths  []string
	types  []string
	bodies [][]byte
	errs   []error
	wg     sync.WaitGroup
}

// newH2Peer starts a peer in the custody of fixture f, which the caller
// created first, and returns the REST configuration that reaches it. The
// peer borrows the certificate of an httptest TLS server. The join of its
// accept loop and every connection handler is an original operation of f,
// registered before the first goroutine starts, so that f removes no path
// before every handler positively returned. The peer's cleanup, which runs
// before f's, closes the listener and every accepted connection and joins
// every goroutine; one that does not return is retained with f and fails
// the test.
func newH2Peer(t *testing.T, f *receiptFixture, code http2.ErrCode, resets int, reply func(path string, body []byte) (int, []byte)) (*h2Peer, *rest.Config) {
	t.Helper()
	if os.Getenv("DISABLE_HTTP2") != "" {
		t.Fatal("DISABLE_HTTP2 is set; this test requires the client's HTTP/2 transport")
	}
	ts := httptest.NewUnstartedServer(http.NotFoundHandler())
	ts.StartTLS()
	cert, ca := ts.TLS.Certificates[0], pemCertificate(ts)
	ts.Close()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"h2"}})
	if err != nil {
		t.Fatal(err)
	}
	p := &h2Peer{ln: ln, code: code, resets: resets, reply: reply, fixture: f}
	p.wg.Add(1)
	f.run("HTTP/2 peer handlers", func() error {
		p.wg.Wait()
		return nil
	})
	t.Cleanup(func() { p.close(t) })
	go p.accept()
	return p, &rest.Config{Host: "https://" + ln.Addr().String(), TLSClientConfig: rest.TLSClientConfig{CAData: ca}}
}

func (p *h2Peer) accept() {
	defer p.wg.Done()
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			c.Close()
			return
		}
		p.conns = append(p.conns, c)
		p.wg.Add(1)
		p.mu.Unlock()
		go p.serve(c)
	}
}

// close closes the listener and every accepted connection, then joins the
// accept loop and every connection handler within receiptJoinTimeout.
// Closing is not a join: if the handlers do not return, the peer stays
// retained, and its fixture, whose operation joins the same handlers,
// keeps every owned path.
func (p *h2Peer) close(t *testing.T) {
	t.Helper()
	p.mu.Lock()
	first := !p.closed
	p.closed = true
	conns := slices.Clone(p.conns)
	p.mu.Unlock()
	if first {
		p.ln.Close()
		for _, c := range conns {
			c.Close()
		}
	}
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(receiptJoinTimeout):
		if !slices.Contains(retainedH2Peers, p) {
			retainedH2Peers = append(retainedH2Peers, p)
		}
		t.Errorf("HTTP/2 peer handlers did not return; retained with %s", strings.Join(p.fixture.paths(), ", "))
	}
}

func (p *h2Peer) fail(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.errs = append(p.errs, err)
}

// serve owns the accepted connection c: every return, including a failed
// handshake, preface or frame, closes c before the handler is joined.
func (p *h2Peer) serve(c net.Conn) {
	defer p.wg.Done()
	defer c.Close()
	tc := c.(*tls.Conn)
	if err := tc.Handshake(); err != nil {
		p.fail(err)
		return
	}
	p.mu.Lock()
	p.protos = append(p.protos, tc.ConnectionState().NegotiatedProtocol)
	p.mu.Unlock()
	preface := make([]byte, len(http2.ClientPreface))
	if _, err := io.ReadFull(c, preface); err != nil || string(preface) != http2.ClientPreface {
		p.fail(fmt.Errorf("client preface %q: %v", preface, err))
		return
	}
	fr := http2.NewFramer(c, c)
	fr.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	var block bytes.Buffer
	enc := hpack.NewEncoder(&block)
	if err := fr.WriteSettings(); err != nil {
		return
	}
	type stream struct {
		path, ctype string
		body        []byte
	}
	streams := make(map[uint32]*stream)
	for {
		f, err := fr.ReadFrame()
		if err != nil {
			return
		}
		switch f := f.(type) {
		case *http2.SettingsFrame:
			if !f.IsAck() {
				fr.WriteSettingsAck()
			}
		case *http2.PingFrame:
			if !f.IsAck() {
				fr.WritePing(true, f.Data)
			}
		case *http2.MetaHeadersFrame:
			path, _, _ := strings.Cut(f.PseudoValue("path"), "?")
			s := &stream{path: path}
			for _, h := range f.RegularFields() {
				if h.Name == "content-type" {
					s.ctype = h.Value
				}
			}
			streams[f.StreamID] = s
			if f.StreamEnded() {
				p.respond(fr, enc, &block, f.StreamID, s.path, s.ctype, nil)
			}
		case *http2.DataFrame:
			s := streams[f.StreamID]
			if s == nil {
				p.fail(fmt.Errorf("data on unknown stream %d", f.StreamID))
				return
			}
			s.body = append(s.body, f.Data()...)
			if f.StreamEnded() {
				p.respond(fr, enc, &block, f.StreamID, s.path, s.ctype, s.body)
			}
		}
	}
}

func (p *h2Peer) respond(fr *http2.Framer, enc *hpack.Encoder, block *bytes.Buffer, id uint32, path, ctype string, body []byte) {
	p.mu.Lock()
	p.paths = append(p.paths, path)
	p.types = append(p.types, ctype)
	p.bodies = append(p.bodies, body)
	reset := len(p.bodies) <= p.resets
	p.mu.Unlock()
	if reset {
		fr.WriteRSTStream(id, p.code)
		return
	}
	code, out := p.reply(path, body)
	block.Reset()
	enc.WriteField(hpack.HeaderField{Name: ":status", Value: strconv.Itoa(code)})
	enc.WriteField(hpack.HeaderField{Name: "content-type", Value: "application/json"})
	enc.WriteField(hpack.HeaderField{Name: "content-length", Value: strconv.Itoa(len(out))})
	fr.WriteHeaders(http2.HeadersFrameParam{StreamID: id, BlockFragment: block.Bytes(), EndHeaders: true, EndStream: len(out) == 0})
	if len(out) > 0 {
		fr.WriteData(id, true, out)
	}
}

// received returns the complete bodies, paths and content types received,
// and the negotiated protocols and handler errors.
func (p *h2Peer) received() (bodies [][]byte, paths, types, protos []string, errs []error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.bodies), slices.Clone(p.paths), slices.Clone(p.types), slices.Clone(p.protos), slices.Clone(p.errs)
}

// requireJSONObject requires that body is the JSON encoding of an object of
// apiVersion and kind, sent with the JSON content type.
func requireJSONObject(t *testing.T, body []byte, ctype, apiVersion, kind string) {
	t.Helper()
	var tm metav1.TypeMeta
	if err := json.Unmarshal(body, &tm); err != nil || tm.APIVersion != apiVersion || tm.Kind != kind || ctype != "application/json" {
		t.Fatalf("posted %q %+v %v, want %s %s as application/json", ctype, tm, err, apiVersion, kind)
	}
}

// TestReceiptCreateHTTP2Reset resets the create's HTTP/2 stream after the
// peer received its complete body, with REFUSED_STREAM or a peer
// PROTOCOL_ERROR. REFUSED_STREAM is retried when x/net/http2 can obtain the
// body again (first traced at v0.30.0 transport.go:628-669; the module now
// selects v0.56.0, whose http2/transport_common.go was read only for the
// no-GetBody rule, so this control's replay is checked when it runs, not
// assumed from source). The rewindable control, the
// request kubernetes.NewForConfig's client builds for the same frozen
// configuration, is replayed: the peer receives the same body twice. The
// receipted create is not: the peer receives one body, Start fails with the
// transport's refusal to retry a written body, and the receipt stays sent.
// The created twin answers 201 over the same transport and pins it.
func TestReceiptCreateHTTP2Reset(t *testing.T) {
	created := func(path string, body []byte) (int, []byte) {
		api := &receiptAPI{jobs: make(map[string]*batchv1.Job), pods: make(map[string]*corev1.Pod)}
		rec := httptest.NewRecorder()
		api.create(rec, runtime.ContentTypeJSON, path == jobsPath, body)
		return rec.Code, rec.Body.Bytes()
	}
	t.Run("rewindable-control", func(t *testing.T) {
		f := newReceiptFixture(t)
		peer, config := newH2Peer(t, f, http2.ErrCodeRefusedStream, 1, func(string, []byte) (int, []byte) { return 201, []byte("{}") })
		_, frozen, err := fromRESTFrozen(context.Background(), TargetConfig{Name: "test", Namespace: "workloads"}, "owner", config)
		if err != nil {
			t.Fatal(err)
		}
		cs, err := kubernetes.NewForConfig(frozen)
		if err != nil {
			t.Fatal(err)
		}
		// The original call is bounded by ctx and is an original operation
		// of f. Cleanups run in reverse: cancel, close and join the peer,
		// then retire f once the call positively returned. A cancel is not
		// its completion.
		ctx, cancel := context.WithTimeout(context.Background(), receiptJoinTimeout)
		t.Cleanup(cancel)
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "control", Namespace: "workloads"}}
		var code int
		err = f.run("control create", func() error {
			return cs.CoreV1().RESTClient().Post().Namespace("workloads").Resource("pods").Body(pod).MaxRetries(0).Do(ctx).StatusCode(&code).Error()
		}).wait(t)
		if err != nil || code != 201 {
			t.Fatalf("control create: %d %v", code, err)
		}
		peer.close(t)
		bodies, _, _, protos, errs := peer.received()
		if len(errs) != 0 || len(protos) == 0 || protos[0] != "h2" {
			t.Fatalf("peer: protocols %q, errors %v", protos, errs)
		}
		if len(bodies) != 2 || !bytes.Equal(bodies[0], bodies[1]) || len(bodies[0]) == 0 {
			t.Fatalf("control bodies: %d, want the same body twice", len(bodies))
		}
	})
	for _, kind := range []string{"Job", "Pod"} {
		for _, code := range []http2.ErrCode{http2.ErrCodeRefusedStream, http2.ErrCodeProtocol} {
			t.Run(fmt.Sprintf("one-shot/%s/%v", kind, code), func(t *testing.T) {
				f := newReceiptFixture(t)
				peer, config := newH2Peer(t, f, code, 1, created)
				b := newReceiptBackendConfig(t, f.dir, config)
				ref := receiptRef(b, kind)
				err := f.run("start", func() error { return b.Start(context.Background(), ref.ID, ref.Task) }).wait(t)
				if err == nil || errors.Is(err, backend.ErrIdentityMismatch) {
					t.Fatalf("start: %v", err)
				}
				switch code {
				case http2.ErrCodeRefusedStream:
					if !strings.Contains(err.Error(), "after Request.Body was written") {
						t.Fatalf("start retry: %v", err)
					}
				case http2.ErrCodeProtocol:
					var reset http2.StreamError
					if !errors.As(err, &reset) || reset.Code != code || reset.StreamID == 0 {
						t.Fatalf("start reset: %v", err)
					}
				}
				peer.close(t)
				bodies, paths, types, protos, errs := peer.received()
				if len(errs) != 0 || len(protos) == 0 || protos[0] != "h2" {
					t.Fatalf("peer: protocols %q, errors %v", protos, errs)
				}
				if len(bodies) != 1 || paths[0] != createPath(kind) {
					t.Fatalf("bodies: %d at %q, want one", len(bodies), paths)
				}
				if kind == "Job" {
					requireJSONObject(t, bodies[0], types[0], "batch/v1", "Job")
				} else {
					requireJSONObject(t, bodies[0], types[0], "v1", "Pod")
				}
				r, err := newReceiptBackendConfig(t, f.dir, config).receipts.load(testID)
				if err != nil || r.Phase != phaseSent || r.Witness != nil || r.UID != "" {
					t.Fatalf("receipt: %+v %v", r, err)
				}
				if err := b.Start(context.Background(), ref.ID, ref.Task); !errors.Is(err, errReceiptExists) {
					t.Fatalf("second start: %v", err)
				}
			})
		}
		t.Run("created/"+kind, func(t *testing.T) {
			f := newReceiptFixture(t)
			peer, config := newH2Peer(t, f, 0, 0, created)
			b := newReceiptBackendConfig(t, f.dir, config)
			ref := receiptRef(b, kind)
			if err := f.run("start", func() error { return b.Start(context.Background(), ref.ID, ref.Task) }).wait(t); err != nil {
				t.Fatalf("start: %v", err)
			}
			peer.close(t)
			bodies, _, types, protos, errs := peer.received()
			if len(errs) != 0 || len(protos) == 0 || protos[0] != "h2" || len(bodies) != 1 {
				t.Fatalf("peer: %d bodies, protocols %q, errors %v", len(bodies), protos, errs)
			}
			if kind == "Job" {
				requireJSONObject(t, bodies[0], types[0], "batch/v1", "Job")
			} else {
				requireJSONObject(t, bodies[0], types[0], "v1", "Pod")
			}
			if r, err := b.receipts.load(testID); err != nil || r.Phase != phasePinned || r.UID != "uid-1" {
				t.Fatalf("receipt: %+v %v", r, err)
			}
		})
	}
}

// TestH2PeerClosesRejectedConnection checks the MODELED peer's harness
// failure path: a TLS client that sends a malformed client preface sees the
// peer close its connection while the peer is still open, because the
// handler that returned owns the connection. The peer records the failure,
// and its close joins the handler before its fixture retires.
func TestH2PeerClosesRejectedConnection(t *testing.T) {
	f := newReceiptFixture(t)
	peer, config := newH2Peer(t, f, 0, 0, func(string, []byte) (int, []byte) { return 500, nil })
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(config.TLSClientConfig.CAData) {
		t.Fatal("peer certificate")
	}
	c, err := tls.DialWithDialer(&net.Dialer{Timeout: receiptJoinTimeout}, "tcp", strings.TrimPrefix(config.Host, "https://"), &tls.Config{RootCAs: roots, NextProtos: []string{"h2"}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.SetDeadline(time.Now().Add(receiptJoinTimeout)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write([]byte(strings.Repeat("x", len(http2.ClientPreface)))); err != nil {
		t.Fatal(err)
	}
	if n, err := c.Read(make([]byte, 1)); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("read from rejected connection: %d bytes, %v; want it closed by the peer", n, err)
	}
	peer.close(t)
	if _, _, _, _, errs := peer.received(); len(errs) != 1 || !strings.Contains(errs[0].Error(), "client preface") {
		t.Fatalf("peer errors: %v", errs)
	}
}

// logCapHelperEnv selects the helper role of TestReceiptLogBodyCap. Its
// value is the process ID of the parent test process, so that a stray
// setting neither runs the helper in an ordinary test process nor spawns.
const logCapHelperEnv = "SKYGO_LOG_CAP_HELPER_PARENT"

// TestReceiptLogBodyCap fills every log custody of a process, maxLogBodies,
// and requires the next receipted Logs to be refused before any request,
// body or receipt change. A kept custody and its MODELED HOLD fixture last
// until their process exits, so the saturation runs in a separate process of
// this test binary and leaves the custodies of this process untouched. The
// helper's state lives under a fixture of this process. Its exit, which
// releases the kept bodies, connections and operation permits, is proved by
// a positive Wait, an original operation of the fixture: the fixture is
// removed only after Wait returned. A helper that does not exit within the
// bound is not killed; its PID and every owned path are reported and kept.
func TestReceiptLogBodyCap(t *testing.T) {
	if parent := os.Getenv(logCapHelperEnv); parent != "" {
		if parent != strconv.Itoa(os.Getppid()) {
			t.Fatalf("%s=%s is not this process's parent; refusing to run or spawn the helper", logCapHelperEnv, parent)
		}
		logBodyCapHelper(t)
		return
	}
	f := newReceiptFixture(t)
	var out bytes.Buffer
	cmd := exec.Command(os.Args[0], "-test.run=^TestReceiptLogBodyCap$", "-test.count=1", "-test.v",
		"-test.timeout="+(2*receiptJoinTimeout).String())
	cmd.Env = append(os.Environ(), logCapHelperEnv+"="+strconv.Itoa(os.Getpid()), "SKYGO_RECEIPT_TEST_DIR="+f.dir)
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	helper := f.run("log cap helper process", cmd.Wait)
	select {
	case <-helper.done:
	case <-time.After(3 * receiptJoinTimeout):
		t.Fatalf("log cap helper process %d did not exit; retained %s", cmd.Process.Pid, strings.Join(f.paths(), ", "))
	}
	if helper.err != nil || !strings.Contains(out.String(), "result: log custodies full, next Logs refused\n") ||
		!strings.Contains(out.String(), "--- PASS: TestReceiptLogBodyCap") {
		t.Fatalf("helper process %d: %v\n%s", cmd.Process.Pid, helper.err, out.String())
	}
}

// logBodyCapHelper runs in the helper process. Over a loopback API it reads
// one attempt's log cleanly, then keeps maxLogBodies custodies with a
// MODELED Close that closes the original body and fails, and requires the
// next Logs, of the clean attempt and of a new pinned one, to be refused.
func logBodyCapHelper(t *testing.T) {
	custodies := func() (reserved, kept int) {
		logCustodies.mu.Lock()
		defer logCustodies.mu.Unlock()
		return logCustodies.reserved, len(logCustodies.kept)
	}
	if reserved, kept := custodies(); reserved != 0 || kept != 0 {
		t.Fatalf("helper starts with %d reserved and %d kept log custodies", reserved, kept)
	}
	ctx := context.Background()
	// Only the saturating attempts, 000…01 to 000…11, match.
	m := newCloseModel(t, "skygo-000000", "error")
	m.hold.release()
	api, url, root := newReceiptAPI(t)
	api.fixture.keep()
	b := newReceiptBackendConfig(t, root, &rest.Config{Host: url, WrapTransport: m.wrap})
	start := func(id string) backend.Reference {
		t.Helper()
		ref := receiptRef(b, "Job")
		ref.ID = id
		if err := b.Start(ctx, ref.ID, ref.Task); err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
		api.addJobPod(api.job("skygo-"+id), "skygo-"+id+"-pod", corev1.PodRunning)
		return ref
	}

	// Positive twin before saturation: a clean Close releases its custody.
	healthy := start(otherID)
	if chunk, err := b.Logs(ctx, healthy, 0, 64); err != nil || string(chunk.Data) != "output\n" {
		t.Fatalf("healthy logs: %q %v", chunk.Data, err)
	}
	if reserved, kept := custodies(); reserved != 0 || kept != 0 || keptCustody(b.receipts, otherID) {
		t.Fatalf("after clean logs: %d reserved, %d kept", reserved, kept)
	}

	for i := 1; i <= maxLogBodies; i++ {
		id := fmt.Sprintf("%032x", i)
		ref := start(id)
		if chunk, err := b.Logs(ctx, ref, 0, 64); !errors.Is(err, errLogBodyHeld) || len(chunk.Data) != 0 {
			t.Fatalf("logs %d: %q %v", i, chunk.Data, err)
		}
		if !keptCustody(b.receipts, id) {
			t.Fatalf("custody %d not kept", i)
		}
	}
	if reserved, kept := custodies(); reserved != 0 || kept != maxLogBodies {
		t.Fatalf("saturated: %d reserved, %d kept, want %d kept", reserved, kept, maxLogBodies)
	}
	if calls, closes := m.counts(); calls != maxLogBodies || closes != maxLogBodies {
		t.Fatalf("close: %d calls, %d original closes, want %d", calls, closes, maxLogBodies)
	}

	// HOLD: every custody is kept. The next Logs is refused before its
	// first request, with no body and no receipt change.
	fresh := start(fmt.Sprintf("%032x", maxLogBodies+1))
	requests, writes := api.counts()
	before := [][]byte{receiptBytes(t, b, healthy.ID), receiptBytes(t, b, fresh.ID)}
	for _, ref := range []backend.Reference{healthy, fresh} {
		if chunk, err := b.Logs(ctx, ref, 0, 64); !errors.Is(err, errLogBodyLimit) || len(chunk.Data) != 0 {
			t.Fatalf("logs of %s with every custody kept: %q %v", ref.ID, chunk.Data, err)
		}
	}
	if n, w := api.counts(); n != requests || w != writes {
		t.Fatalf("refused logs reached the API: %d requests, %d writes, want %d and %d", n, w, requests, writes)
	}
	for i, ref := range []backend.Reference{healthy, fresh} {
		if !bytes.Equal(before[i], receiptBytes(t, b, ref.ID)) {
			t.Fatalf("refused logs changed the receipt of %s", ref.ID)
		}
	}
	if calls, _ := m.counts(); calls != maxLogBodies {
		t.Fatalf("close calls after refusal: %d", calls)
	}
	if reserved, kept := custodies(); reserved != 0 || kept != maxLogBodies {
		t.Fatalf("after refusal: %d reserved, %d kept", reserved, kept)
	}
	fmt.Println("result: log custodies full, next Logs refused")
}
