package agentssh

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

const (
	testAgentName = "agent.test"
	testUser      = "skygo"
	testLineage   = "0123456789abcdef0123456789abcdef"
	testEpoch     = 7
)

// A fixture is a loopback SSH server that forwards direct-tcpip channels
// to one allowed TLS agent serving the generated ControlService.
type fixture struct {
	t       *testing.T
	dir     string
	cfg     config
	token   string
	hostKey ssh.Signer
	userKey ssh.Signer
	caPEM   []byte
	service *service
	agent   *httptest.Server
	ln      net.Listener
	server  *ssh.ServerConfig
	wg      sync.WaitGroup // every server goroutine
	arrived chan string    // gate names of held HTTP requests
	holding chan struct{}  // receives when a channel open is held

	mu         sync.Mutex
	accepts    int
	handshakes int // completed SSH server handshakes
	conns      []net.Conn
	opens      []string // "type destination" of every channel open
	requests   []string // global and channel request types
	http       []recorded
	gates      map[string]chan struct{}
	hold       chan struct{} // if set, direct-tcpip opens wait for it to close
}

type recorded struct {
	method, uri, host string
	header            http.Header
	body              []byte
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{
		t:       t,
		dir:     t.TempDir(),
		token:   "test-bearer-" + randomHex(t, 16),
		hostKey: newSigner(t),
		userKey: newSigner(t),
		arrived: make(chan string, 16),
		holding: make(chan struct{}, 16),
		gates:   make(map[string]chan struct{}),
	}
	f.service = &service{fixture: f, domain: &v1.RequestDomain{Lineage: testLineage, Epoch: testEpoch}, accepted: make(chan struct{}, 4), release: make(chan struct{})}

	ca, caKey := newCA(t, "test agent CA")
	f.caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Raw})
	f.agent = httptest.NewUnstartedServer(f.handler())
	f.agent.TLS = &tls.Config{Certificates: []tls.Certificate{newLeaf(t, ca, caKey, testAgentName)}}
	f.agent.StartTLS()
	// Cleanups run last-registered first: the SSH server is joined, then
	// held handlers are released, then the agent is closed.
	t.Cleanup(f.agent.Close)
	t.Cleanup(f.releaseAll)

	f.server = &ssh.ServerConfig{
		PublicKeyCallback: func(m ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			if m.User() == testUser && bytes.Equal(key.Marshal(), f.userKey.PublicKey().Marshal()) {
				return nil, nil
			}
			return nil, errors.New("refused")
		},
	}
	f.server.AddHostKey(f.hostKey)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.ln = ln
	t.Cleanup(f.closeServer)
	f.wg.Add(1)
	go f.serve()

	f.cfg = config{
		sshAddress:     ln.Addr().String(),
		user:           testUser,
		keyFile:        f.writeKey("id_ed25519", f.userKey),
		knownHostsFile: f.write("known_hosts", knownhosts.Line([]string{ln.Addr().String()}, f.hostKey.PublicKey())+"\n", 0o644),
		agentAddress:   f.agent.Listener.Addr().String(),
		agentName:      testAgentName,
		agentCAFile:    f.write("ca.pem", string(f.caPEM), 0o644),
	}
	return f
}

func (f *fixture) closeServer() {
	f.ln.Close()
	f.mu.Lock()
	conns := f.conns
	f.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
	f.wg.Wait()
}

func (f *fixture) serve() {
	defer f.wg.Done()
	for {
		c, err := f.ln.Accept()
		if err != nil {
			return
		}
		f.mu.Lock()
		f.accepts++
		f.conns = append(f.conns, c)
		f.mu.Unlock()
		f.wg.Add(1)
		go f.handleSSH(c)
	}
}

func (f *fixture) handleSSH(c net.Conn) {
	defer f.wg.Done()
	defer c.Close()
	conn, chans, reqs, err := ssh.NewServerConn(c, f.server)
	if err != nil {
		return
	}
	f.mu.Lock()
	f.handshakes++
	f.mu.Unlock()
	defer conn.Close()
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		for r := range reqs {
			f.note(&f.requests, r.Type)
			r.Reply(false, nil)
		}
	}()
	for nc := range chans {
		var d struct {
			Host       string
			Port       uint32
			Origin     string
			OriginPort uint32
		}
		dest := "?"
		if ssh.Unmarshal(nc.ExtraData(), &d) == nil {
			dest = net.JoinHostPort(d.Host, strconv.Itoa(int(d.Port)))
		}
		f.note(&f.opens, nc.ChannelType()+" "+dest)
		if nc.ChannelType() != "direct-tcpip" || dest != f.agent.Listener.Addr().String() {
			nc.Reject(ssh.Prohibited, "refused")
			continue
		}
		f.wg.Add(1)
		go f.forward(nc)
	}
}

func (f *fixture) forward(nc ssh.NewChannel) {
	defer f.wg.Done()
	f.mu.Lock()
	hold := f.hold
	f.mu.Unlock()
	if hold != nil {
		f.holding <- struct{}{}
		<-hold
	}
	ch, reqs, err := nc.Accept()
	if err != nil {
		return
	}
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		for r := range reqs {
			f.note(&f.requests, r.Type)
			r.Reply(false, nil)
		}
	}()
	agent, err := net.Dial("tcp", f.agent.Listener.Addr().String())
	if err != nil {
		ch.Close()
		return
	}
	var copies sync.WaitGroup
	copies.Add(2)
	stopped := make(chan struct{}, 2)
	go func() { defer copies.Done(); io.Copy(agent, ch); stopped <- struct{}{} }()
	go func() { defer copies.Done(); io.Copy(ch, agent); stopped <- struct{}{} }()
	<-stopped
	ch.Close()
	agent.Close()
	copies.Wait()
}

func (f *fixture) handshakeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.handshakes
}

func (f *fixture) note(list *[]string, s string) {
	f.mu.Lock()
	*list = append(*list, s)
	f.mu.Unlock()
}

func (f *fixture) snapshot() (accepts int, opens, requests []string, http []recorded) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.accepts, append([]string(nil), f.opens...), append([]string(nil), f.requests...), append([]recorded(nil), f.http...)
}

func (f *fixture) handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle(skyv1connect.NewControlServiceHandler(f.service))
	mux.HandleFunc("/raw/echo/", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") })
	mux.HandleFunc("/raw/big", func(w http.ResponseWriter, r *http.Request) { w.Write(make([]byte, 2<<20)) })
	mux.HandleFunc("/raw/gzip", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Write([]byte{0x1f, 0x8b})
	})
	mux.HandleFunc("/raw/redirect", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/raw/echo/", http.StatusFound)
	})
	mux.HandleFunc("/raw/partial", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "hello")
		w.(http.Flusher).Flush()
		<-f.gate("partial")
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.http = append(f.http, recorded{r.Method, r.RequestURI, r.Host, r.Header.Clone(), body})
		f.mu.Unlock()
		r.Body = io.NopCloser(bytes.NewReader(body))
		if name := r.Header.Get("X-Test-Gate"); name != "" {
			f.arrived <- name
			<-f.gate(name)
		}
		mux.ServeHTTP(w, r)
	})
}

func (f *fixture) gate(name string) chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	g := f.gates[name]
	if g == nil {
		g = make(chan struct{})
		f.gates[name] = g
	}
	return g
}

func (f *fixture) release(name string) {
	g := f.gate(name)
	f.mu.Lock()
	defer f.mu.Unlock()
	select {
	case <-g:
	default:
		close(g)
	}
}

func (f *fixture) releaseAll() {
	f.mu.Lock()
	var names []string
	for name := range f.gates {
		names = append(names, name)
	}
	f.mu.Unlock()
	for _, name := range names {
		f.release(name)
	}
	f.service.releaseOnce.Do(func() { close(f.service.release) })
}

func (f *fixture) write(name, content string, mode os.FileMode) string {
	f.t.Helper()
	path := filepath.Join(f.dir, name)
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		f.t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		f.t.Fatal(err)
	}
	return path
}

func (f *fixture) writeKey(name string, s ssh.Signer) string {
	f.t.Helper()
	return f.write(name, string(privatePEM(f.t, s)), 0o600)
}

// client returns a client for f's agent after edit adjusts its config.
func (f *fixture) client(edit func(*config)) (*http.Client, *transport) {
	f.t.Helper()
	c := f.cfg
	if edit != nil {
		edit(&c)
	}
	client, err := newClient(c)
	if err != nil {
		f.t.Fatal(err)
	}
	return client, client.Transport.(*transport)
}

func (f *fixture) generated(client *http.Client) skyv1connect.ControlServiceClient {
	return skyv1connect.NewControlServiceClient(client, f.cfg.endpoint(), connect.WithAcceptCompression("gzip", nil, nil))
}

// noSecret fails if err's text contains the bearer token.
func (f *fixture) noSecret(err error) {
	f.t.Helper()
	if err != nil && strings.Contains(err.Error(), f.token) {
		f.t.Fatal("error text contains the bearer token")
	}
}

// signerKeys maps test signers to their private keys, which are written
// only to key files and never logged.
var signerKeys sync.Map // ssh.Signer -> ed25519.PrivateKey

func newSigner(t *testing.T) ssh.Signer {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	s, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	signerKeys.Store(s, priv)
	return s
}

func privatePEM(t *testing.T, s ssh.Signer) []byte {
	t.Helper()
	priv, _ := signerKeys.Load(s)
	block, err := ssh.MarshalPrivateKey(priv.(ed25519.PrivateKey), "")
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(block)
}

func randomHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", b)
}

func newCA(t *testing.T, name string) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert, key
}

func newLeaf(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, name string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: name},
		DNSNames:     []string{name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// A service is the generated ControlService behind the fixture's agent.
// It requires the fixture's bearer token.
type service struct {
	skyv1connect.UnimplementedControlServiceHandler
	fixture *fixture

	mu          sync.Mutex
	domain      *v1.RequestDomain
	noInfo      bool
	mutations   int
	accepted    chan struct{} // receives after a mutation is counted
	release     chan struct{} // closed to let mutations respond
	releaseOnce sync.Once
}

func (s *service) authorize(h http.Header) error {
	if h.Get("Authorization") != "Bearer "+s.fixture.token {
		return connect.NewError(connect.CodeUnauthenticated, errors.New("unauthenticated"))
	}
	return nil
}

func (s *service) GetRetention(_ context.Context, r *connect.Request[v1.GetRetentionRequest]) (*connect.Response[v1.GetRetentionResponse], error) {
	if err := s.authorize(r.Header()); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return connect.NewResponse(&v1.GetRetentionResponse{Enabled: s.domain != nil, Domain: s.domain}), nil
}

func (s *service) GetServerInfo(_ context.Context, r *connect.Request[v1.GetServerInfoRequest]) (*connect.Response[v1.GetServerInfoResponse], error) {
	if err := s.authorize(r.Header()); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.noInfo {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
	}
	return connect.NewResponse(&v1.GetServerInfoResponse{Targets: []*v1.Target{{Name: "local"}}}), nil
}

func (s *service) SubmitRetainedJob(_ context.Context, r *connect.Request[v1.SubmitRetainedJobRequest]) (*connect.Response[v1.SubmitRetainedJobResponse], error) {
	if err := s.authorize(r.Header()); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.mutations++
	s.mu.Unlock()
	s.accepted <- struct{}{}
	<-s.release
	return connect.NewResponse(&v1.SubmitRetainedJobResponse{Job: &v1.Job{Id: "job-1"}}), nil
}

func (s *service) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mutations
}

func identity() agentIdentity { return agentIdentity{testLineage, testEpoch} }

func TestCheckAgent(t *testing.T) {
	f := newFixture(t)
	client, _ := f.client(nil)
	if err := checkAgent(context.Background(), client, f.cfg, f.token, identity()); err != nil {
		t.Fatal(err)
	}
	accepts, opens, requests, got := f.snapshot()
	dest := "direct-tcpip " + f.agent.Listener.Addr().String()
	if accepts != 2 || len(opens) != 2 || opens[0] != dest || opens[1] != dest {
		t.Errorf("accepts %d, opens %q; want one connection and one %q per request", accepts, opens, dest)
	}
	if len(requests) != 0 {
		t.Errorf("SSH requests %q, want none", requests)
	}
	want := []string{"/skygo.v1.ControlService/GetRetention", "/skygo.v1.ControlService/GetServerInfo"}
	if len(got) != len(want) {
		t.Fatalf("agent saw %d requests, want %d", len(got), len(want))
	}
	for i, r := range got {
		if r.method != http.MethodPost || r.uri != want[i] || r.host != testAgentName {
			t.Errorf("request %d: %s %s Host %s, want POST %s Host %s", i, r.method, r.uri, r.host, want[i], testAgentName)
		}
		if r.header.Get("Authorization") != "Bearer "+f.token {
			t.Errorf("request %d: Authorization not passed through exactly", i)
		}
		if r.header.Get("Accept-Encoding") != "" || r.header.Get("Content-Encoding") != "" {
			t.Errorf("request %d: compression headers %q %q", i, r.header.Get("Accept-Encoding"), r.header.Get("Content-Encoding"))
		}
		if len(r.body) != 0 {
			t.Errorf("request %d: body %d bytes, want the empty request message", i, len(r.body))
		}
	}
	if n := f.service.count(); n != 0 {
		t.Errorf("mutations %d, want 0", n)
	}
}

func TestCheckAgentRefuses(t *testing.T) {
	tests := []struct {
		name   string
		domain *v1.RequestDomain
		noInfo bool
		want   agentIdentity
	}{
		{name: "lineage", domain: &v1.RequestDomain{Lineage: strings.Repeat("f", 32), Epoch: testEpoch}, want: identity()},
		{name: "epoch", domain: &v1.RequestDomain{Lineage: testLineage, Epoch: testEpoch + 1}, want: identity()},
		{name: "disabled", domain: nil, want: identity()},
		{name: "no server info", domain: &v1.RequestDomain{Lineage: testLineage, Epoch: testEpoch}, noInfo: true, want: identity()},
		{name: "invalid enrollment", domain: &v1.RequestDomain{Lineage: testLineage, Epoch: testEpoch}, want: agentIdentity{testLineage, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			f.service.mu.Lock()
			f.service.domain, f.service.noInfo = tt.domain, tt.noInfo
			f.service.mu.Unlock()
			client, _ := f.client(nil)
			err := checkAgent(context.Background(), client, f.cfg, f.token, tt.want)
			if err == nil {
				t.Fatal("checkAgent succeeded")
			}
			f.noSecret(err)
			if tt.noInfo && connect.CodeOf(err) != connect.CodeUnimplemented {
				t.Errorf("code %v, want unimplemented", connect.CodeOf(err))
			}
			if n := f.service.count(); n != 0 {
				t.Errorf("mutations %d, want 0", n)
			}
		})
	}
}

func TestHostKeyRefused(t *testing.T) {
	tests := []struct {
		name       string
		knownHosts func(f *fixture) string
		reason     string
	}{
		{"mismatched", func(f *fixture) string {
			return knownhosts.Line([]string{f.cfg.sshAddress}, newSigner(t).PublicKey())
		}, "mismatched"},
		{"unknown", func(f *fixture) string {
			return knownhosts.Line([]string{"[127.0.0.1]:1"}, f.hostKey.PublicKey())
		}, "unknown"},
		{"revoked", func(f *fixture) string {
			line := knownhosts.Line([]string{f.cfg.sshAddress}, f.hostKey.PublicKey())
			return line + "\n@revoked * " + strings.SplitN(line, " ", 2)[1]
		}, "revoked"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			client, _ := f.client(func(c *config) { c.knownHostsFile = f.write("known_hosts."+tt.name, tt.knownHosts(f)+"\n", 0o644) })
			err := checkAgent(context.Background(), client, f.cfg, f.token, identity())
			var hk *hostKeyError
			if !errors.As(err, &hk) || hk.reason != tt.reason {
				t.Fatalf("err = %v, want %s host key", err, tt.reason)
			}
			if hk.fingerprint != ssh.FingerprintSHA256(f.hostKey.PublicKey()) {
				t.Errorf("fingerprint %s, want the presented key's", hk.fingerprint)
			}
			f.noSecret(err)
			if _, opens, _, got := f.snapshot(); len(opens) != 0 || len(got) != 0 {
				t.Errorf("opens %q, agent requests %d; want none", opens, len(got))
			}
		})
	}
}

func TestClientKeyRefused(t *testing.T) {
	f := newFixture(t)
	client, _ := f.client(func(c *config) { c.keyFile = f.writeKey("other", newSigner(t)) })
	err := checkAgent(context.Background(), client, f.cfg, f.token, identity())
	if err == nil || !strings.Contains(err.Error(), "unable to authenticate") {
		t.Fatalf("err = %v, want authentication failure", err)
	}
	f.noSecret(err)
	if _, opens, _, got := f.snapshot(); len(opens) != 0 || len(got) != 0 {
		t.Errorf("opens %q, agent requests %d; want none", opens, len(got))
	}
}

func TestTLSRefused(t *testing.T) {
	tests := []struct {
		name string
		edit func(f *fixture, c *config)
	}{
		{"wrong CA", func(f *fixture, c *config) {
			ca, _ := newCA(t, "other CA")
			c.agentCAFile = f.write("other.pem", string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Raw})), 0o644)
		}},
		{"wrong name", func(f *fixture, c *config) { c.agentName = "other.test" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			var c config
			client, _ := f.client(func(cc *config) { tt.edit(f, cc); c = *cc })
			err := checkAgent(context.Background(), client, c, f.token, identity())
			var verr *tls.CertificateVerificationError
			if !errors.As(err, &verr) {
				t.Fatalf("err = %v, want certificate verification failure", err)
			}
			f.noSecret(err)
			if _, opens, _, got := f.snapshot(); len(opens) != 1 || len(got) != 0 {
				t.Errorf("opens %q, agent requests %d; want one channel and no request", opens, len(got))
			}
		})
	}
}

// A countingBody records how often it is closed.
type countingBody struct {
	io.Reader
	closes int
}

func (b *countingBody) Close() error { b.closes++; return nil }

func TestRefusedBeforeDial(t *testing.T) {
	f := newFixture(t)
	client, tr := f.client(nil)
	tests := []struct {
		name string
		edit func(r *http.Request)
	}{
		{"http scheme", func(r *http.Request) { r.URL.Scheme = "http" }},
		{"foreign host", func(r *http.Request) { r.URL.Host = "other.test" }},
		{"foreign Host header", func(r *http.Request) { r.Host = "other.test" }},
		{"agent port", func(r *http.Request) { r.URL.Host = testAgentName + ":443" }},
		{"user info", func(r *http.Request) { r.URL.User = url.UserPassword("u", "p") }},
		{"CONNECT", func(r *http.Request) { r.Method = http.MethodConnect }},
		{"DELETE", func(r *http.Request) { r.Method = http.MethodDelete }},
		{"upgrade", func(r *http.Request) { r.Header.Set("Upgrade", "websocket") }},
		{"connection", func(r *http.Request) { r.Header.Set("Connection", "keep-alive") }},
		{"expect", func(r *http.Request) { r.Header.Set("Expect", "100-continue") }},
		{"accept gzip", func(r *http.Request) { r.Header.Set("Accept-Encoding", "gzip") }},
		{"content gzip", func(r *http.Request) { r.Header.Set("Content-Encoding", "gzip") }},
		{"trailer", func(r *http.Request) { r.Trailer = http.Header{"X": nil} }},
		{"oversized body", func(r *http.Request) { r.Body = &countingBody{Reader: bytes.NewReader(make([]byte, maxRequest+1))} }},
		{"oversized header", func(r *http.Request) { r.Header.Set("X-Big", strings.Repeat("x", maxRequest)) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, f.cfg.endpoint()+"/raw/echo/", nil)
			if err != nil {
				t.Fatal(err)
			}
			body := &countingBody{Reader: strings.NewReader("x")}
			req.Body = body
			tt.edit(req)
			if b, ok := req.Body.(*countingBody); ok {
				body = b
			}
			resp, err := tr.RoundTrip(req)
			if err == nil {
				resp.Body.Close()
				t.Fatal("request was sent")
			}
			if body.closes != 1 {
				t.Errorf("request body closed %d times, want 1", body.closes)
			}
		})
	}
	t.Run("generated default compression", func(t *testing.T) {
		cc := skyv1connect.NewControlServiceClient(client, f.cfg.endpoint())
		_, err := cc.GetServerInfo(context.Background(), connect.NewRequest(&v1.GetServerInfoRequest{}))
		if err == nil || !strings.Contains(err.Error(), "Accept-Encoding") {
			t.Fatalf("err = %v, want Accept-Encoding refusal", err)
		}
	})
	if accepts, _, _, _ := f.snapshot(); accepts != 0 {
		t.Errorf("SSH server accepted %d connections, want 0", accepts)
	}
}

func TestExactRequest(t *testing.T) {
	f := newFixture(t)
	_, tr := f.client(nil)
	const uri = "/raw/echo/a%2Fb?q=x%20y&r=%2F&r=1"
	req, err := http.NewRequest(http.MethodPost, f.cfg.endpoint()+uri, strings.NewReader("payload\x00\xff"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+f.token)
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	if cerr := resp.Body.Close(); err != nil || cerr != nil {
		t.Fatalf("read %v, close %v", err, cerr)
	}
	if string(body) != "ok" || resp.StatusCode != http.StatusOK {
		t.Errorf("response %d %q", resp.StatusCode, body)
	}
	_, _, _, got := f.snapshot()
	if len(got) != 1 {
		t.Fatalf("agent saw %d requests, want 1", len(got))
	}
	r := got[0]
	if r.method != http.MethodPost || r.uri != uri || string(r.body) != "payload\x00\xff" || r.header.Get("Authorization") != "Bearer "+f.token {
		t.Errorf("agent saw %s %s body %q; want POST %s with exact body and Authorization", r.method, r.uri, r.body, uri)
	}
	if r.header.Get("Connection") != "close" {
		t.Errorf("Connection %q, want close", r.header.Get("Connection"))
	}
}

func TestResponseRefused(t *testing.T) {
	tests := []struct {
		path string
		want error // nil means any error from RoundTrip
	}{
		{"/raw/big", errResponseTooLarge},
		{"/raw/gzip", nil},
		{"/raw/redirect", nil},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			f := newFixture(t)
			client, _ := f.client(nil)
			resp, err := client.Get(f.cfg.endpoint() + tt.path)
			if err == nil {
				_, err = io.ReadAll(resp.Body)
				if cerr := resp.Body.Close(); cerr != nil {
					t.Errorf("close: %v", cerr)
				}
			}
			if err == nil || tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
			if _, _, _, got := f.snapshot(); len(got) != 1 {
				t.Errorf("agent saw %d requests, want 1 (no redirect or retry)", len(got))
			}
		})
	}
}

// lateDial makes tr dial the SSH server for real but return the
// connection only after the dial context is done, or after 5s so that a
// transport that never abandons the dial fails rather than hangs. It
// returns the late connection, how long after the request started the dial
// context was done (-1 if it never was), and the request's error.
func lateDial(t *testing.T, f *fixture, tr *transport, ctx context.Context, cancel func()) (net.Conn, time.Duration, error) {
	t.Helper()
	dialed := make(chan net.Conn, 1)
	released := time.Duration(-1)
	start := time.Now()
	tr.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		c, err := net.Dial(network, address)
		if err != nil {
			return nil, err
		}
		dialed <- c
		select {
		case <-ctx.Done():
			released = time.Since(start)
		case <-time.After(5 * time.Second):
		}
		return c, nil
	}
	errc := make(chan error, 1)
	go func() {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, f.cfg.endpoint()+"/raw/echo/", nil)
		_, err := tr.RoundTrip(req)
		errc <- err
	}()
	c := <-dialed
	if cancel != nil {
		cancel()
	}
	err := <-errc // RoundTrip has returned, so released is set
	return c, released, err
}

// checkLateConn checks that the late connection c was closed by the
// request before any SSH handshake completed on it, not adopted.
func checkLateConn(t *testing.T, f *fixture, err error, c net.Conn) {
	t.Helper()
	var re *requestError
	if !errors.As(err, &re) || re.op != "dial" {
		t.Fatalf("err = %v, want a request error from the dial step", err)
	}
	if re.frame.raw == nil || re.frame.raw.Conn != c {
		t.Error("late connection not retained by the request frame")
	}
	if _, werr := c.Write([]byte("x")); !errors.Is(werr, net.ErrClosed) {
		t.Errorf("late connection write: %v, want it closed by the request", werr)
	}
	f.closeServer() // joins every server goroutine before counting
	if n := f.handshakeCount(); n != 0 {
		t.Errorf("SSH server completed %d handshakes on the late connection, want 0", n)
	}
}

func TestCancelDuringDial(t *testing.T) {
	f := newFixture(t)
	_, tr := f.client(nil)
	ctx, cancel := context.WithCancel(context.Background())
	c, _, err := lateDial(t, f, tr, ctx, cancel)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want canceled", err)
	}
	checkLateConn(t, f, err, c)
}

// TestDialTimeout checks that a dial that does not return within the
// request budget is abandoned, even when the request has no deadline.
func TestDialTimeout(t *testing.T) {
	f := newFixture(t)
	_, tr := f.client(nil)
	tr.budget = 50 * time.Millisecond
	c, released, err := lateDial(t, f, tr, context.Background(), nil)
	if !errors.Is(err, errDialTimeout) {
		t.Fatalf("err = %v, want dial timeout", err)
	}
	// 10x the budget leaves room for a loaded machine and -race.
	if released < 0 || released > 500*time.Millisecond {
		t.Errorf("dial context done after %v (-1: never), want within 500ms of the 50ms budget", released)
	}
	checkLateConn(t, f, err, c)
}

func TestCancelDuringChannelOpen(t *testing.T) {
	f := newFixture(t)
	hold := make(chan struct{})
	defer close(hold)
	f.mu.Lock()
	f.hold = hold
	f.mu.Unlock()
	_, tr := f.client(nil)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, f.cfg.endpoint()+"/raw/echo/", nil)
		_, err := tr.RoundTrip(req)
		errc <- err
	}()
	<-f.holding
	cancel()
	err := <-errc
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want canceled", err)
	}
	var re *requestError
	var held *heldError
	if !errors.As(err, &re) || errors.As(err, &held) {
		t.Fatalf("err = %v, want a joined request error", err)
	}
	if re.op != "direct-tcpip" {
		t.Errorf("failed in %s, want direct-tcpip", re.op)
	}
	// Teardown joined the original Dial call: it returned, without a channel.
	re.frame.mu.Lock()
	ch, dialErr := re.frame.channel, re.frame.dialErr
	re.frame.mu.Unlock()
	if ch != nil || dialErr == nil {
		t.Errorf("dial call channel %v err %v, want the call joined with an error", ch, dialErr)
	}
	if _, _, _, got := f.snapshot(); len(got) != 0 {
		t.Errorf("agent saw %d requests, want 0", len(got))
	}
}

func TestSimultaneousCallers(t *testing.T) {
	f := newFixture(t)
	client, _ := f.client(nil)
	cc := f.generated(client)
	call := func(ctx context.Context, gate string) error {
		r := connect.NewRequest(&v1.GetServerInfoRequest{})
		r.Header().Set("Authorization", "Bearer "+f.token)
		r.Header().Set("X-Test-Gate", gate)
		_, err := cc.GetServerInfo(ctx, r)
		return err
	}
	ctxA, cancelA := context.WithCancel(context.Background())
	errA, errB := make(chan error, 1), make(chan error, 1)
	go func() { errA <- call(ctxA, "a") }()
	go func() { errB <- call(context.Background(), "b") }()
	arrived := map[string]bool{<-f.arrived: true, <-f.arrived: true}
	if !arrived["a"] || !arrived["b"] {
		t.Fatalf("arrived %v, want a and b", arrived)
	}
	cancelA()
	if err := <-errA; connect.CodeOf(err) != connect.CodeCanceled {
		t.Fatalf("canceled call: %v", err)
	}
	f.release("b")
	if err := <-errB; err != nil {
		t.Fatalf("other call: %v", err)
	}
	f.release("a")
}

func TestNoReplay(t *testing.T) {
	f := newFixture(t)
	client, _ := f.client(nil)
	cc := f.generated(client)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		r := connect.NewRequest(&v1.SubmitRetainedJobRequest{RequestId: "r1", Task: &v1.Task{}})
		r.Header().Set("Authorization", "Bearer "+f.token)
		_, err := cc.SubmitRetainedJob(ctx, r)
		errc <- err
	}()
	<-f.service.accepted
	cancel()
	err := <-errc
	if err == nil {
		t.Fatal("submit succeeded after its response was lost")
	}
	f.noSecret(err)
	f.service.releaseOnce.Do(func() { close(f.service.release) })
	info := connect.NewRequest(&v1.GetServerInfoRequest{})
	info.Header().Set("Authorization", "Bearer "+f.token)
	if _, err := cc.GetServerInfo(context.Background(), info); err != nil {
		t.Fatal(err)
	}
	if n := f.service.count(); n != 1 {
		t.Errorf("mutations %d, want exactly 1", n)
	}
	if _, _, _, got := f.snapshot(); len(got) != 2 {
		t.Errorf("agent saw %d requests, want the submit once and the read", len(got))
	}
}
