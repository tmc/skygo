package remotehelper

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
)

func transportTestToken() string {
	return "sgd1." + strings.Repeat("1", 32) + "." + strings.Repeat("2", 32) + "." + base64.RawURLEncoding.EncodeToString(make([]byte, 32))
}

type transportTestService struct {
	skyv1connect.UnimplementedRemoteDataServiceHandler
	data      []byte
	container string
}

func (s *transportTestService) ReadInput(context.Context, *connect.Request[v1.ReadInputRequest]) (*connect.Response[v1.ReadInputResponse], error) {
	return connect.NewResponse(&v1.ReadInputResponse{Data: s.data, NextOffset: int64(len(s.data))}), nil
}
func (s *transportTestService) PollRelease(context.Context, *connect.Request[v1.PollReleaseRequest]) (*connect.Response[v1.PollReleaseResponse], error) {
	return connect.NewResponse(&v1.PollReleaseResponse{State: v1.RemoteReleaseState_REMOTE_RELEASE_STATE_WAIT, TaskContainerId: s.container}), nil
}
func (s *transportTestService) BeginOutput(context.Context, *connect.Request[v1.BeginOutputRequest]) (*connect.Response[v1.BeginOutputResponse], error) {
	return connect.NewResponse(&v1.BeginOutputResponse{State: v1.RemoteOutputState_REMOTE_OUTPUT_STATE_WRITABLE}), nil
}
func (s *transportTestService) WriteOutput(context.Context, *connect.Request[v1.WriteOutputRequest]) (*connect.Response[v1.WriteOutputResponse], error) {
	return connect.NewResponse(&v1.WriteOutputResponse{Acknowledged: 1}), nil
}
func (s *transportTestService) FinishOutput(context.Context, *connect.Request[v1.FinishOutputRequest]) (*connect.Response[v1.FinishOutputResponse], error) {
	return connect.NewResponse(&v1.FinishOutputResponse{State: v1.RemoteOutputSealState_REMOTE_OUTPUT_SEAL_STATE_SEALED}), nil
}

// This fixture is a local TLS/Connect protocol oracle, not a cluster/native
// qualification. It uses one explicitly trusted certificate and DNS identity.
func transportTestTLS(t *testing.T, hostname string, handler http.Handler) (*httptest.Server, string, string) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	certificate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: hostname},
		DNSNames: []string{hostname}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, public, private)
	if err != nil {
		t.Fatal(err)
	}
	ca := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	server := httptest.NewUnstartedServer(handler)
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: private}}, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	t.Cleanup(server.Close)
	port := server.Listener.Addr().(*net.TCPAddr).Port
	return server, "https://" + hostname + ":" + strconv.Itoa(port), ca
}

var heldTransportTestClients struct {
	sync.Mutex
	clients []*client
}

func transportTestClient(t *testing.T, server *httptest.Server, origin, ca string) *client {
	t.Helper()
	c, err := newClient(origin, ca, transportTestToken())
	if err != nil {
		t.Fatal(err)
	}
	// A test-only DNS resolution mapping preserves real TLS verification against
	// the configured hostname, with no production dial callback/config addition.
	c.transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}
	t.Cleanup(func() {
		c.Close()
		if err := c.takeError(); err != nil && !policyRefusal(err) {
			heldTransportTestClients.Lock()
			heldTransportTestClients.clients = append(heldTransportTestClients.clients, c)
			heldTransportTestClients.Unlock()
			t.Errorf("original response Close uncertainty retained: %v", err)
		}
	})
	return c
}

// policyRefusal reports whether err is a latched transport policy refusal
// whose original response Close succeeded: it retains no original.
func policyRefusal(err error) bool {
	var closeErr *bodyCloseError
	var uncertain *responseUncertainError
	return errors.Is(err, errTransportPolicy) && !errors.As(err, &closeErr) && !errors.As(err, &uncertain)
}

func TestTransportGeneratedTLSAndIdentityEncoding(t *testing.T) {
	service := &transportTestService{data: bytes.Repeat([]byte("x"), 64<<10)}
	_, handler := skyv1connect.NewRemoteDataServiceHandler(service)
	var calls atomic.Int32
	server, origin, ca := transportTestTLS(t, "helper.example", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer "+transportTestToken() {
			t.Error("scope credential not supplied exactly")
		}
		if r.Header.Get("Accept-Encoding") != "identity" || strings.Contains(r.Header.Get("Connect-Accept-Encoding"), "gzip") {
			t.Error("compression negotiated")
		}
		handler.ServeHTTP(w, r)
	}))
	var proxyCalls atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { proxyCalls.Add(1); http.Error(w, "refused", 502) }))
	t.Cleanup(proxy.Close)
	t.Setenv("HTTPS_PROXY", proxy.URL)
	t.Setenv("HTTP_PROXY", proxy.URL)
	t.Setenv("ALL_PROXY", proxy.URL)
	c := transportTestClient(t, server, origin, ca)
	if c.transport.Proxy != nil || !c.transport.DisableCompression || c.httpClient.Timeout != 10*time.Second || c.transport.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Fatal("transport policy changed")
	}
	ctx := context.Background()
	input, err := c.remote.ReadInput(ctx, connect.NewRequest(&v1.ReadInputRequest{Limit: 64 << 10}))
	if err != nil || c.takeError() != nil || !bytes.Equal(input.Msg.Data, service.data) {
		t.Fatalf("real generated input: %v", err)
	}
	if _, err := c.remote.PollRelease(ctx, connect.NewRequest(&v1.PollReleaseRequest{})); err != nil || c.takeError() != nil {
		t.Fatalf("poll: %v", err)
	}
	if _, err := c.remote.BeginOutput(ctx, connect.NewRequest(&v1.BeginOutputRequest{})); err != nil || c.takeError() != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := c.remote.WriteOutput(ctx, connect.NewRequest(&v1.WriteOutputRequest{Data: []byte("x")})); err != nil || c.takeError() != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := c.remote.FinishOutput(ctx, connect.NewRequest(&v1.FinishOutputRequest{})); err != nil || c.takeError() != nil {
		t.Fatalf("finish: %v", err)
	}
	if calls.Load() != 5 || proxyCalls.Load() != 0 {
		t.Fatal("generated requests escaped fixed direct routes")
	}
}

func TestTransportOriginCAAndTokenPolicy(t *testing.T) {
	_, _, ca := transportTestTLS(t, "helper.example", http.NotFoundHandler())
	for _, origin := range []string{"http://helper.example", "https://127.0.0.1", "https://[::1]", "https://user@helper.example", "https://helper.example/", "https://helper.example?", "https://helper.example#", "https://Helper.example", "https://helper.example:0443", "https://helper.example:0", "https://helper.example:65536", "https://a..example", "https://a.example.", "https://0x7f.1", "https://a.example/path"} {
		if c, err := newClient(origin, ca, transportTestToken()); err == nil {
			c.Close()
			t.Error("invalid origin accepted")
		}
	}
	for _, badCA := range []string{"", ca + "garbage", "garbage" + ca, strings.Repeat(" ", 65537), "-----BEGIN PRIVATE KEY-----\nAA==\n-----END PRIVATE KEY-----\n", strings.Replace(ca, "CERTIFICATE-----", "CERTIFICATE-----\nHeader: value\n", 1)} {
		if c, err := newClient("https://helper.example", badCA, transportTestToken()); err == nil {
			c.Close()
			t.Error("partial or noncertificate CA accepted")
		}
	}
	token := transportTestToken()
	for _, bad := range []string{token + "\n", token[:113], "Bearer " + token, strings.Replace(token, "sgd1.", "sgd2.", 1), token[:5] + "A" + token[6:], token[:113] + "B", token[:113] + "=", token[:70] + "_" + token[71:]} {
		if validToken(bad) {
			t.Error("noncanonical scope token accepted")
		}
		if c, err := newClient("https://helper.example", ca, bad); err == nil {
			c.Close()
			t.Error("invalid token constructed client")
		}
	}
	if !validToken(token) {
		t.Fatal("canonical scope token refused")
	}
	if origin, _, ok := transportOrigin("https://helper.example:443"); !ok || origin != "https://helper.example" {
		t.Fatal("canonical default port changed")
	}
	for _, path := range []string{"/skygo.v1.ControlService/GetJob", "/skygo.v1.RemoteDataService/Other", skyv1connect.RemoteDataServiceReadInputProcedure + "/"} {
		if transportLimit(path) != 0 {
			t.Fatal("foreign route admitted")
		}
	}
}

func TestTransportTrustAndHostname(t *testing.T) {
	_, handler := skyv1connect.NewRemoteDataServiceHandler(&transportTestService{})
	server, origin, ca := transportTestTLS(t, "helper.example", handler)
	_, _, otherCA := transportTestTLS(t, "helper.example", handler)
	for _, test := range []struct{ name, origin, ca string }{{"wrong CA", origin, otherCA}, {"wrong hostname", strings.Replace(origin, "helper.example", "foreign.example", 1), ca}} {
		t.Run(test.name, func(t *testing.T) {
			c := transportTestClient(t, server, test.origin, test.ca)
			_, err := c.remote.PollRelease(context.Background(), connect.NewRequest(&v1.PollReleaseRequest{}))
			if err == nil || c.takeError() != nil {
				t.Fatal("TLS trust or hostname failure did not refuse cleanly")
			}
			if strings.Contains(err.Error(), transportTestToken()) {
				t.Fatal("transport error disclosed scope credential")
			}
		})
	}
}

func TestTransportRedirectsNeverForwardBearer(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		t.Run(strconv.FormatBool(foreign), func(t *testing.T) {
			var targetCalls, sourceCalls atomic.Int32
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targetCalls.Add(1) }))
			t.Cleanup(target.Close)
			location := target.URL + skyv1connect.RemoteDataServicePollReleaseProcedure
			server, origin, ca := transportTestTLS(t, "helper.example", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				sourceCalls.Add(1)
				if !foreign {
					location = "https://" + r.Host + skyv1connect.RemoteDataServiceFinishOutputProcedure
				}
				w.Header().Set("Location", location)
				w.WriteHeader(http.StatusTemporaryRedirect)
			}))
			c := transportTestClient(t, server, origin, ca)
			_, err := c.remote.PollRelease(context.Background(), connect.NewRequest(&v1.PollReleaseRequest{}))
			if err == nil || c.takeError() != nil || sourceCalls.Load() != 1 || targetCalls.Load() != 0 {
				t.Fatal("redirect forwarded or unresolved body")
			}
		})
	}
}

func TestTransportEncodingAndBodyBounds(t *testing.T) {
	for _, test := range []struct {
		name, encoding string
		input          bool
		size           int
		latched        bool // refused while reading, after headers
	}{
		{"gzip", "gzip", false, 0, false}, {"identity header", "identity", false, 0, false},
		{"metadata over bound", "", false, 8 << 10, true}, {"input over bound", "", true, 97 << 10, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := &transportTestService{}
			if test.input {
				service.data = bytes.Repeat([]byte("x"), test.size)
			} else {
				service.container = strings.Repeat("x", test.size)
			}
			_, handler := skyv1connect.NewRemoteDataServiceHandler(service)
			server, origin, ca := transportTestTLS(t, "helper.example", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if test.encoding != "" {
					w.Header().Set("Content-Encoding", test.encoding)
				}
				handler.ServeHTTP(w, r)
			}))
			c := transportTestClient(t, server, origin, ca)
			var err error
			if test.input {
				_, err = c.remote.ReadInput(context.Background(), connect.NewRequest(&v1.ReadInputRequest{Limit: 64 << 10}))
			} else {
				_, err = c.remote.PollRelease(context.Background(), connect.NewRequest(&v1.PollReleaseRequest{}))
			}
			if retained := c.takeError(); err == nil || test.latched != (retained != nil) || retained != nil && !policyRefusal(retained) {
				t.Fatalf("encoded/oversize response did not refuse with original Close: %v", retained)
			}
		})
	}
}

type transportPolicyBody struct {
	closes int
	err    error
}

func (*transportPolicyBody) Read([]byte) (int, error) { return 0, io.EOF }
func (b *transportPolicyBody) Close() error           { b.closes++; return b.err }

// An injected Close result is a custody policy oracle, not a native fault claim.
func TestTransportCloseErrorStickyPolicy(t *testing.T) {
	cause := errors.New("original policy close failure")
	original := &transportPolicyBody{err: cause}
	c := &client{active: true}
	body := &transportBody{owner: c, original: original, remaining: 8 << 10}
	first := body.Close()
	if first == nil || body.Close() != first || original.closes != 1 || c.takeError() != first {
		t.Fatal("original body Close retried or lost")
	}
	var retained *bodyCloseError
	if !errors.As(first, &retained) || retained.body != original || !errors.Is(first, cause) {
		t.Fatal("original body/cause not retained")
	}
	request, _ := http.NewRequest(http.MethodPost, "https://helper.example"+skyv1connect.RemoteDataServicePollReleaseProcedure, nil)
	if _, err := c.Do(request); err == nil || original.closes != 1 || c.takeError() != first {
		t.Fatal("uncertain original allowed another request")
	}
}

// This fixed, one-invocation keeper owns the composed policy fixture even when
// an assertion fails before the returned error can be inspected. The body fault
// is deliberately reported AFTER observing the real body's original Close; it
// is not a spontaneous kernel Close failure or a native qualification claim.
var heldComposedTransportClose struct {
	sync.Mutex
	frame *composedTransportCloseFrame
}

type composedTransportCloseFrame struct {
	mu             sync.Mutex
	client         *client
	server         *httptest.Server
	transport      *http.Transport
	response       *http.Response
	original       io.ReadCloser
	wrapper        *composedTransportCloseBody
	policy         error
	roundTripError error
	wireError      error
	wireCalls      int
	returned       error
	retained       *bodyCloseError
	roundTrips     int
}

type composedTransportCloseBody struct {
	mu            sync.Mutex
	original      io.ReadCloser
	policy        error
	calls         int
	originalCalls int
	originalError error
	returned      error
}

func (b *composedTransportCloseBody) Read(data []byte) (int, error) {
	return b.original.Read(data)
}

func (b *composedTransportCloseBody) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	if b.originalCalls != 0 {
		return b.returned
	}
	b.originalCalls++
	b.originalError = b.original.Close()
	if b.originalError != nil {
		b.returned = errors.Join(b.originalError, b.policy)
	} else {
		b.returned = b.policy
	}
	return b.returned
}

// The only substituted operation wraps the REAL TLS response returned by the
// unchanged production Transport. It neither supplies a synthetic response nor
// repeats a request, and retains the exact response/body before returning it.
type composedTransportCloseRoundTripper struct{ frame *composedTransportCloseFrame }

func (r *composedTransportCloseRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	f := r.frame
	f.mu.Lock()
	f.roundTrips++
	if f.roundTrips != 1 {
		f.mu.Unlock()
		return nil, errors.New("composed close fixture forbids replacement response")
	}
	f.mu.Unlock()
	response, err := f.transport.RoundTrip(request)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.response, f.roundTripError = response, err
	if response != nil {
		f.original = response.Body
		f.wrapper = &composedTransportCloseBody{original: response.Body, policy: f.policy}
		response.Body = f.wrapper
	}
	return response, err
}

// Observe the actual generated client's primary wire error without replacing
// any RPC behavior. Connect discards CloseResponse's error on this error branch;
// only the production helper's immediate takeError can preserve its precedence.
type composedTransportCloseRemote struct {
	skyv1connect.RemoteDataServiceClient
	frame *composedTransportCloseFrame
}

func (r *composedTransportCloseRemote) BeginOutput(ctx context.Context, request *connect.Request[v1.BeginOutputRequest]) (*connect.Response[v1.BeginOutputResponse], error) {
	response, err := r.RemoteDataServiceClient.BeginOutput(ctx, request)
	r.frame.mu.Lock()
	if r.frame.wireCalls == 0 {
		r.frame.wireError = err
	}
	r.frame.wireCalls++
	r.frame.mu.Unlock()
	return response, err
}

type composedTransportCloseService struct {
	transportTestService
	begins    atomic.Int32
	writes    atomic.Int32
	finishes  atomic.Int32
	polls     atomic.Int32
	wireCause error
}

func (s *composedTransportCloseService) BeginOutput(context.Context, *connect.Request[v1.BeginOutputRequest]) (*connect.Response[v1.BeginOutputResponse], error) {
	s.begins.Add(1)
	return nil, connect.NewError(connect.CodeUnavailable, s.wireCause)
}
func (s *composedTransportCloseService) WriteOutput(ctx context.Context, request *connect.Request[v1.WriteOutputRequest]) (*connect.Response[v1.WriteOutputResponse], error) {
	s.writes.Add(1)
	return s.transportTestService.WriteOutput(ctx, request)
}
func (s *composedTransportCloseService) FinishOutput(ctx context.Context, request *connect.Request[v1.FinishOutputRequest]) (*connect.Response[v1.FinishOutputResponse], error) {
	s.finishes.Add(1)
	return s.transportTestService.FinishOutput(ctx, request)
}
func (s *composedTransportCloseService) PollRelease(ctx context.Context, request *connect.Request[v1.PollReleaseRequest]) (*connect.Response[v1.PollReleaseResponse], error) {
	s.polls.Add(1)
	return s.transportTestService.PollRelease(ctx, request)
}

func TestUploadWireErrorClosePolicyStopsBeforeRetry(t *testing.T) {
	frame := &composedTransportCloseFrame{policy: errors.New("labeled policy fault after real response Close")}
	heldComposedTransportClose.Lock()
	if heldComposedTransportClose.frame != nil {
		heldComposedTransportClose.Unlock()
		t.Fatal("original composed close fixture already retained in this process")
	}
	heldComposedTransportClose.frame = frame
	heldComposedTransportClose.Unlock()
	service := &composedTransportCloseService{wireCause: errors.New("primary generated wire failure")}
	_, handler := skyv1connect.NewRemoteDataServiceHandler(service)
	server, origin, ca := transportTestTLS(t, "helper.example", handler)
	frame.server = server
	c, err := newClient(origin, ca, transportTestToken())
	if err != nil {
		t.Fatal(err)
	}
	frame.client, frame.transport = c, c.transport
	// Only DNS resolution is mapped to this owned TLS fixture. The production
	// custom CA/ServerName, ten-second timeout and response owner remain intact.
	c.transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}
	c.httpClient.Transport = &composedTransportCloseRoundTripper{frame: frame}
	c.remote = &composedTransportCloseRemote{RemoteDataServiceClient: c.remote, frame: frame}
	t.Cleanup(c.Close) // Idle pool only; never retry the retained original body.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = upload(ctx, c, strings.Repeat("a", 64), []byte("x"))
	frame.mu.Lock()
	frame.returned = err
	response, original, wrapper, wireErr := frame.response, frame.original, frame.wrapper, frame.wireError
	roundTrips, roundTripErr, wireCalls := frame.roundTrips, frame.roundTripError, frame.wireCalls
	frame.mu.Unlock()
	if roundTripErr != nil || response == nil || original == nil || wrapper == nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatal("actual generated TLS error response was not retained")
	}
	if connect.CodeOf(wireErr) != connect.CodeUnavailable || !strings.Contains(wireErr.Error(), service.wireCause.Error()) {
		t.Fatal("primary generated wire error was not observed")
	}
	var retained *bodyCloseError
	if !errors.As(err, &retained) || retained == nil {
		t.Fatal("helper lost original Close error behind primary wire error or retry")
	}
	frame.mu.Lock()
	frame.retained = retained
	frame.mu.Unlock()
	// net/http may insert its timeout body wrapper. The SAME response pointer
	// now names the production transportBody and its exact original client body.
	body, ok := response.Body.(*transportBody)
	if !ok || retained.body != body.original || body.closeErr != retained || retained.cause != frame.policy || c.takeError() != retained || err != retained {
		t.Fatal("exact production body/error/cause/client frame was not preserved")
	}
	wrapper.mu.Lock()
	calls, originalCalls, originalErr, result := wrapper.calls, wrapper.originalCalls, wrapper.originalError, wrapper.returned
	sameOriginal := wrapper.original == original
	wrapper.mu.Unlock()
	if !sameOriginal || calls != 1 || originalCalls != 1 || originalErr != nil || result != frame.policy {
		t.Fatal("real original Close was not observed exactly once before labeled policy fault")
	}
	if roundTrips != 1 || wireCalls != 1 || service.begins.Load() != 1 || service.writes.Load() != 0 || service.finishes.Load() != 0 || service.polls.Load() != 0 {
		t.Fatal("uncertain original permitted retry, output write or private seal")
	}
	// A future generated call must fail before any replacement HTTP request.
	if _, err := c.remote.PollRelease(ctx, connect.NewRequest(&v1.PollReleaseRequest{})); connect.CodeOf(err) != connect.CodeFailedPrecondition || c.takeError() != retained {
		t.Fatal("sticky original Close uncertainty allowed future call")
	}
	frame.mu.Lock()
	roundTrips = frame.roundTrips
	frame.mu.Unlock()
	wrapper.mu.Lock()
	originalCalls = wrapper.originalCalls
	wrapper.mu.Unlock()
	if roundTrips != 1 || originalCalls != 1 || service.begins.Load() != 1 || service.writes.Load() != 0 || service.finishes.Load() != 0 || service.polls.Load() != 0 {
		t.Fatal("future call retried original Close or reached application")
	}
}

// Frames of the oversized error body test stay reachable here for the life of
// the test process unless their case passed with a positively clean Close. The
// keeper is bounded; a case refuses to start rather than grow it further.
var heldPolicyCloseFrames struct {
	sync.Mutex
	frames []*composedTransportCloseFrame
}

type oversizedErrorService struct {
	skyv1connect.UnimplementedRemoteDataServiceHandler
	code  connect.Code
	polls atomic.Int32
}

func (s *oversizedErrorService) PollRelease(context.Context, *connect.Request[v1.PollReleaseRequest]) (*connect.Response[v1.PollReleaseResponse], error) {
	s.polls.Add(1)
	return nil, connect.NewError(s.code, errors.New(strings.Repeat("x", 9<<10)))
}

// A generated 429 or 503 whose JSON error body has unknown length and exceeds
// the 8KiB bound is refused while reading. Connect reports the status code
// (Unavailable) instead, so the refusal is latched and wins before any retry.
// The labeled Close fault is injected AFTER the real body's original Close: it
// is a custody policy oracle, not a native fault claim.
func TestTransportOversizedErrorBodyPolicy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		code   connect.Code
		status int
		fault  bool
	}{
		{"429", connect.CodeResourceExhausted, http.StatusTooManyRequests, false},
		{"503", connect.CodeUnavailable, http.StatusServiceUnavailable, false},
		{"429 close fault", connect.CodeResourceExhausted, http.StatusTooManyRequests, true},
		{"503 close fault", connect.CodeUnavailable, http.StatusServiceUnavailable, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frame := &composedTransportCloseFrame{}
			if tc.fault {
				frame.policy = errors.New("labeled policy fault after real response Close")
			}
			heldPolicyCloseFrames.Lock()
			if len(heldPolicyCloseFrames.frames) >= 8 {
				heldPolicyCloseFrames.Unlock()
				t.Fatal("policy close keeper full")
			}
			heldPolicyCloseFrames.frames = append(heldPolicyCloseFrames.frames, frame)
			heldPolicyCloseFrames.Unlock()
			t.Cleanup(func() {
				if tc.fault || t.Failed() {
					return
				}
				heldPolicyCloseFrames.Lock()
				defer heldPolicyCloseFrames.Unlock()
				frames := heldPolicyCloseFrames.frames
				for i := range frames {
					if frames[i] == frame {
						heldPolicyCloseFrames.frames = append(frames[:i], frames[i+1:]...)
						break
					}
				}
			})
			service := &oversizedErrorService{code: tc.code}
			_, handler := skyv1connect.NewRemoteDataServiceHandler(service)
			server, origin, ca := transportTestTLS(t, "helper.example", handler)
			frame.server = server
			c, err := newClient(origin, ca, transportTestToken())
			if err != nil {
				t.Fatal(err)
			}
			frame.client, frame.transport = c, c.transport
			c.transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
			}
			c.httpClient.Transport = &composedTransportCloseRoundTripper{frame: frame}
			t.Cleanup(c.Close) // Idle pool only; never retry the original body.
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err = waitRelease(ctx, c)
			frame.mu.Lock()
			frame.returned = err
			response, original, wrapper := frame.response, frame.original, frame.wrapper
			roundTrips, roundTripErr := frame.roundTrips, frame.roundTripError
			frame.mu.Unlock()
			if roundTripErr != nil || response == nil || original == nil || wrapper == nil {
				t.Fatal("actual generated TLS error response was not retained")
			}
			if response.StatusCode != tc.status || response.ContentLength != -1 {
				t.Fatalf("response status=%d length=%d, want %d of unknown length", response.StatusCode, response.ContentLength, tc.status)
			}
			wrapper.mu.Lock()
			calls, originalCalls, originalErr, result := wrapper.calls, wrapper.originalCalls, wrapper.originalError, wrapper.returned
			sameOriginal := wrapper.original == original
			wrapper.mu.Unlock()
			t.Logf("original Close receipt: calls=%d err=%v", originalCalls, originalErr)
			if !sameOriginal || calls != 1 || originalCalls != 1 || originalErr != nil || result != frame.policy {
				t.Fatal("real original Close was not observed exactly once")
			}
			body, ok := response.Body.(*transportBody)
			if !ok {
				t.Fatal("production body wrapper not retained")
			}
			body.mu.Lock()
			policyErr, closeErr, bodyOriginal := body.policyErr, body.closeErr, body.original
			body.mu.Unlock()
			if connect.CodeOf(policyErr) != connect.CodeResourceExhausted || connect.IsWireError(policyErr) || !errors.Is(policyErr, errTransportPolicy) {
				t.Fatalf("read bound refusal not latched: %v", policyErr)
			}
			if err != c.takeError() || !errors.Is(err, errTransportPolicy) || strings.Contains(err.Error(), "xxxx") {
				t.Fatalf("returned error is not the sticky policy refusal: %v", err)
			}
			var retained *bodyCloseError
			if tc.fault {
				frame.mu.Lock()
				if errors.As(err, &retained) {
					frame.retained = retained
				}
				frame.mu.Unlock()
				if retained == nil || err != closeErr || retained.body != bodyOriginal || !errors.Is(err, frame.policy) {
					t.Fatalf("Close uncertainty lost behind policy refusal: %v", err)
				}
			} else {
				c.mu.Lock()
				active := c.active
				c.mu.Unlock()
				if err != policyErr || closeErr != nil || !policyRefusal(err) || active {
					t.Fatalf("clean Close policy refusal retained ownership: %v", err)
				}
			}
			if roundTrips != 1 || service.polls.Load() != 1 {
				t.Fatalf("policy refusal retried: round trips=%d polls=%d", roundTrips, service.polls.Load())
			}
			// A later generated call fails before any replacement HTTP request.
			if _, err := c.remote.PollRelease(ctx, connect.NewRequest(&v1.PollReleaseRequest{})); connect.CodeOf(err) != connect.CodeFailedPrecondition || c.takeError() != policyOrClose(policyErr, retained) {
				t.Fatal("sticky refusal allowed a later call")
			}
			frame.mu.Lock()
			roundTrips = frame.roundTrips
			frame.mu.Unlock()
			if roundTrips != 1 || service.polls.Load() != 1 {
				t.Fatal("later call reached the server")
			}
		})
	}
}

func policyOrClose(policy error, retained *bodyCloseError) error {
	if retained != nil {
		return retained
	}
	return policy
}

// Repeated Reads after the bound return the first latched refusal without
// reading the original again; Read after Close is closed.
func TestTransportPolicyLatchOnce(t *testing.T) {
	original := &transportCountingBody{data: bytes.Repeat([]byte("x"), 32)}
	c := &client{active: true}
	body := &transportBody{owner: c, original: original, remaining: 8}
	data := make([]byte, 32)
	n, first := body.Read(data)
	if n != 8 || !errors.Is(first, errTransportPolicy) || c.takeError() != first {
		t.Fatalf("first overflow: n=%d err=%v", n, first)
	}
	reads := original.reads
	for range 3 {
		if n, err := body.Read(data); n != 0 || err != first {
			t.Fatalf("repeated read: n=%d err=%v", n, err)
		}
	}
	if original.reads != reads || c.takeError() != first {
		t.Fatal("repeated read reached original or replaced latch")
	}
	if err := body.Close(); err != nil || c.takeError() != first || c.active {
		t.Fatalf("clean close: %v", err)
	}
	if _, err := body.Read(data); err != io.ErrClosedPipe {
		t.Fatalf("read after close: %v", err)
	}
}

type transportCountingBody struct {
	data  []byte
	reads int
}

func (b *transportCountingBody) Read(data []byte) (int, error) {
	b.reads++
	n := copy(data, b.data)
	b.data = b.data[n:]
	return n, nil
}
func (*transportCountingBody) Close() error { return nil }
