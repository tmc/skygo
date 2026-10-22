package ec2api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func errorClass(t *testing.T, err error) *Error {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("error %v (%T) is not an *Error", err, err)
	}
	return e
}

func TestRunInstancesSigned(t *testing.T) {
	// The environment names credentials, a profile and a proxy that
	// must all be ignored; the fixture accepts only fixture credentials
	// sent directly to it.
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIDAMBIENT")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "ambient")
	t.Setenv("AWS_PROFILE", "ambient")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")

	f := newFixture(t)
	c := f.client(nil)
	req := testRequest(t)
	r, err := c.RunInstances(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Instances) != 1 || r.OwnerID != "123456789012" || r.Instances[0].ClientToken != req.ClientToken() {
		t.Fatalf("reservation = %+v", r)
	}
	if n := f.count("GetCallerIdentity"); n != 1 {
		t.Errorf("GetCallerIdentity requests = %d, want 1", n)
	}
	if got := f.requests[len(f.requests)-1].body; !bytes.Equal(got, req.Body()) {
		t.Errorf("sent body differs from the request body")
	}
	if n := f.connections(); n != 2 {
		t.Errorf("connections = %d, want one per request (2)", n)
	}
}

// TestRunInstancesReplyLost has the fixture apply a create and then drop
// the connection before replying. Reissuing the same request, from the
// same client and from a new client, recovers the one original instance.
// This is fixture token recovery, not a durable controller restart.
func TestRunInstancesReplyLost(t *testing.T) {
	f := newFixture(t)
	drop := true
	f.after = func(action string, w http.ResponseWriter, r *http.Request) bool {
		if action != "RunInstances" || !drop {
			return false
		}
		drop = false
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return true
		}
		conn.Close()
		return true
	}
	req := testRequest(t)
	c := f.client(nil)
	_, err := c.RunInstances(context.Background(), req)
	e := errorClass(t, err)
	if e.Class != ClassTransport || !e.Sent || e.Action != "RunInstances" {
		t.Fatalf("lost reply error = %+v, want sent transport failure", e)
	}
	if len(f.instances) != 1 {
		t.Fatalf("fixture instances after lost reply = %d, want 1", len(f.instances))
	}
	want := f.instances[0].id

	for i, c := range []*Client{c, f.client(nil)} {
		r, err := c.RunInstances(context.Background(), req)
		if err != nil {
			t.Fatalf("reissue %d: %v", i, err)
		}
		if got := r.Instances[0].ID; got != want {
			t.Fatalf("reissue %d instance = %s, want %s", i, got, want)
		}
	}
	tokens := map[string]bool{}
	for _, r := range f.requests {
		if r.action == "RunInstances" {
			tokens[r.form.Get("ClientToken")] = true
		}
	}
	if len(f.instances) != 1 || len(tokens) != 1 || f.count("RunInstances") != 3 {
		t.Fatalf("instances = %d, tokens = %d, RunInstances = %d; want 1, 1, 3",
			len(f.instances), len(tokens), f.count("RunInstances"))
	}
	inst, err := f.client(nil).FindOwned(context.Background(), req)
	if err != nil || inst.ID != want {
		t.Fatalf("FindOwned = %v, %v; want %s", inst, err, want)
	}
}

// TestRunInstancesServerErrorReissue answers two creates with 500 and
// checks that caller-driven reissues carry one token and one body.
func TestRunInstancesServerErrorReissue(t *testing.T) {
	f := newFixture(t)
	fails := 2
	f.before = func(action string, w http.ResponseWriter, r *http.Request) bool {
		if action != "RunInstances" || fails == 0 {
			return false
		}
		fails--
		writeError(w, "ec2", http.StatusInternalServerError, "InternalError")
		return true
	}
	c := f.client(nil)
	req := testRequest(t)
	var r *Reservation
	var err error
	for range 3 {
		r, err = c.RunInstances(context.Background(), req)
		if err == nil {
			break
		}
		if e := errorClass(t, err); e.Class != ClassServerError || !e.Retryable() || e.Code != "InternalError" {
			t.Fatalf("error = %+v", e)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	var bodies [][]byte
	for _, fr := range f.requests {
		if fr.action == "RunInstances" {
			bodies = append(bodies, fr.body)
		}
	}
	if len(bodies) != 3 {
		t.Fatalf("RunInstances requests = %d, want 3", len(bodies))
	}
	for _, b := range bodies[1:] {
		if !bytes.Equal(b, bodies[0]) {
			t.Fatal("reissued body differs")
		}
	}
	if len(r.Instances) != 1 || len(f.instances) != 1 {
		t.Fatalf("instances = %d", len(f.instances))
	}
}

func TestRunInstancesIdempotentParameterMismatch(t *testing.T) {
	f := newFixture(t)
	c := f.client(nil)
	if _, err := c.RunInstances(context.Background(), testRequest(t)); err != nil {
		t.Fatal(err)
	}
	other, err := NewCreateRequest(testProfile(), testIdentity(), []byte("#cloud-config\nother\n"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.RunInstances(context.Background(), other)
	if e := errorClass(t, err); e.Class != ClassIdempotentParameterMismatch || e.Retryable() {
		t.Fatalf("error = %+v", e)
	}
	if strings.Contains(err.Error(), "fixture IdempotentParameterMismatch") {
		t.Errorf("error text %q includes the provider message", err)
	}
	if len(f.instances) != 1 {
		t.Fatalf("instances = %d, want 1", len(f.instances))
	}
}

func TestAccountMismatch(t *testing.T) {
	f := newFixture(t)
	f.account = "210987654321"
	c := f.client(nil)
	req := testRequest(t)
	if _, err := c.RunInstances(context.Background(), req); !errors.Is(err, ErrAccountMismatch) {
		t.Fatalf("RunInstances error = %v, want ErrAccountMismatch", err)
	}
	if _, err := c.Terminate(context.Background(), req, "i-0123456789abcdef0"); !errors.Is(err, ErrAccountMismatch) {
		t.Fatalf("Terminate error = %v, want ErrAccountMismatch", err)
	}
	if n := f.ec2Requests(); n != 0 {
		t.Fatalf("EC2 requests = %d, want 0", n)
	}
}

func TestAccountCheckEveryGuardedOperation(t *testing.T) {
	f := newFixture(t)
	c := f.client(nil)
	req := testRequest(t)
	r, err := c.RunInstances(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.RunInstances(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if n := f.count("GetCallerIdentity"); n != 2 {
		t.Fatalf("GetCallerIdentity after two creates = %d, want 2", n)
	}
	id := r.Instances[0].ID
	for _, m := range []func(context.Context, *CreateRequest, string) (*StateChange, error){c.Stop, c.Start} {
		if _, err := m(context.Background(), req, id); err != nil {
			t.Fatal(err)
		}
	}
	if n := f.count("GetCallerIdentity"); n != 4 {
		t.Fatalf("GetCallerIdentity after stop and start = %d, want 4", n)
	}
}

// TestRotatingCredentials uses a provider that alternates between
// credentials A, in the profile's account, and B, in another account the
// fixture also accepts. Each operation retrieves one snapshot and signs its
// STS check, ownership description and mutation with it, so B's STS check
// fails and no EC2 request is ever signed with B.
func TestRotatingCredentials(t *testing.T) {
	f := newFixture(t)
	a := fixtureCreds
	b := aws.Credentials{AccessKeyID: "AKIDFIXTUREBBBB", SecretAccessKey: "fixture/secret/b", SessionToken: "fixture-session-b"}
	f.others[b.AccessKeyID] = fixtureIdentity{b, "210987654321"}
	retrievals := 0
	provider := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		retrievals++
		if retrievals%2 == 1 {
			return a, nil
		}
		return b, nil
	})
	c := f.clientWith(testProfile(), provider)
	req := testRequest(t)
	ctx := context.Background()

	r, err := c.RunInstances(ctx, req) // A
	if err != nil {
		t.Fatal(err)
	}
	id := r.Instances[0].ID
	if _, err := c.RunInstances(ctx, req); !errors.Is(err, ErrAccountMismatch) { // B
		t.Fatalf("create with B: %v, want ErrAccountMismatch", err)
	}
	if _, err := c.Stop(ctx, req, id); err != nil { // A
		t.Fatal(err)
	}
	if _, err := c.Terminate(ctx, req, id); !errors.Is(err, ErrAccountMismatch) { // B
		t.Fatalf("terminate with B: %v, want ErrAccountMismatch", err)
	}
	if _, err := c.Terminate(ctx, req, id); err != nil { // A
		t.Fatal(err)
	}
	if retrievals != 5 {
		t.Errorf("credential retrievals = %d, want one per operation (5)", retrievals)
	}
	A, B := a.AccessKeyID, b.AccessKeyID
	want := []string{
		"GetCallerIdentity " + A, "RunInstances " + A,
		"GetCallerIdentity " + B,
		"GetCallerIdentity " + A, "DescribeInstances " + A, "StopInstances " + A,
		"GetCallerIdentity " + B,
		"GetCallerIdentity " + A, "DescribeInstances " + A, "TerminateInstances " + A,
	}
	var got []string
	for _, fr := range f.requests {
		got = append(got, fr.action+" "+fr.key)
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("signed requests:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// TestMalformedErrorEnvelope answers with error documents that fail the
// structural bounds. None is trusted for its code: each is malformed,
// keeps its status, Sent and close cause, and prints no provider text.
func TestMalformedErrorEnvelope(t *testing.T) {
	const good = `<Response><Errors><Error><Code>RequestLimitExceeded</Code><Message>fixture secret</Message></Error></Errors><RequestID>r-1</RequestID></Response>`
	tests := []struct {
		name string
		body string
	}{
		{"duplicate code", `<Response><Errors><Error><Code>InvalidInstanceID.NotFound</Code><Code>RequestLimitExceeded</Code><Message>fixture secret</Message></Error></Errors></Response>`},
		{"STS envelope from EC2", `<ErrorResponse><Error><Code>RequestLimitExceeded</Code><Message>fixture secret</Message></Error></ErrorResponse>`},
		{"trailing", good + `<x/>`},
		{"deep", `<Response><Errors><Error><Code>RequestLimitExceeded</Code>` + strings.Repeat("<a>", maxXMLDepth) + strings.Repeat("</a>", maxXMLDepth) + `</Error></Errors></Response>`},
		{"no code", `<Response><Errors><Error><Message>fixture secret</Message></Error></Errors></Response>`},
		{"duplicate request ID", strings.Replace(good, "</Response>", "<RequestID>r-2</RequestID></Response>", 1)},
		{"not XML", `<html>fixture secret`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			c := f.client(nil)
			closes := 0
			c.http.Transport = closeFault{c.http.Transport, strings.TrimPrefix(f.ec2.URL, "https://"), &closes}
			f.before = func(action string, w http.ResponseWriter, r *http.Request) bool {
				w.WriteHeader(http.StatusServiceUnavailable)
				io.WriteString(w, tt.body)
				return true
			}
			_, err := c.FindOwned(context.Background(), testRequest(t))
			e := errorClass(t, err)
			if e.Class != ClassMalformed || e.StatusCode != http.StatusServiceUnavailable || !e.Sent || e.Code != "" || e.Retryable() {
				t.Fatalf("error = %+v", e)
			}
			if !errors.Is(err, errCloseFault) || closes != 1 {
				t.Fatalf("close cause lost: err = %v, closes = %d", err, closes)
			}
			if strings.Contains(err.Error(), "fixture secret") {
				t.Fatalf("error text %q includes provider text", err)
			}
		})
	}
}

// TestErrorEnvelopes classifies documented EC2 and STS error envelopes.
func TestErrorEnvelopes(t *testing.T) {
	tests := []struct {
		name    string
		service string
		status  int
		code    string
		action  string
		class   Class
	}{
		{"EC2 throttled", "ec2", http.StatusServiceUnavailable, "RequestLimitExceeded", "DescribeInstances", ClassThrottled},
		{"EC2 unauthorized", "ec2", http.StatusForbidden, "UnauthorizedOperation", "DescribeInstances", ClassUnauthorized},
		{"STS expired", "sts", http.StatusForbidden, "ExpiredToken", "GetCallerIdentity", ClassUnauthorized},
		{"STS server", "sts", http.StatusInternalServerError, "InternalFailure", "GetCallerIdentity", ClassServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			f.before = func(action string, w http.ResponseWriter, r *http.Request) bool {
				if action != tt.action {
					return false
				}
				writeError(w, tt.service, tt.status, tt.code)
				return true
			}
			_, err := f.client(nil).Stop(context.Background(), testRequest(t), "i-0123456789abcdef0")
			e := errorClass(t, err)
			if e.Action != tt.action || e.Class != tt.class || e.Code != tt.code || e.StatusCode != tt.status || e.RequestID != "err-1" {
				t.Fatalf("error = %+v", e)
			}
			if strings.Contains(err.Error(), "fixture "+tt.code) {
				t.Fatalf("error text %q includes the provider message", err)
			}
		})
	}
}

func TestLifecycle(t *testing.T) {
	f := newFixture(t)
	c := f.client(nil)
	req := testRequest(t)
	r, err := c.RunInstances(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	id := r.Instances[0].ID
	f.instances[0].state = "running"
	tests := []struct {
		name      string
		mutate    func(context.Context, *CreateRequest, string) (*StateChange, error)
		prev, cur string
	}{
		{"stop", c.Stop, "running", "stopping"},
		{"start", c.Start, "stopped", "pending"},
		{"terminate", c.Terminate, "running", "shutting-down"},
	}
	for _, tt := range tests {
		sc, err := tt.mutate(context.Background(), req, id)
		if err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}
		if *sc != (StateChange{id, tt.prev, tt.cur}) {
			t.Fatalf("%s: state change = %+v", tt.name, sc)
		}
	}
	for _, a := range []string{"StopInstances", "StartInstances", "TerminateInstances"} {
		if n := f.count(a); n != 1 {
			t.Errorf("%s requests = %d, want 1", a, n)
		}
	}
	if c, r := f.connections(), len(f.requests); c != r {
		t.Errorf("connections = %d, want one per request (%d)", c, r)
	}
}

// TestMutationRefusesForeign changes the observed instance and checks
// that no mutation is sent.
func TestMutationRefusesForeign(t *testing.T) {
	tests := []struct {
		name   string
		change func(*fixtureInstance)
	}{
		{"tag value", func(i *fixtureInstance) { i.tags[0].Value = "skygo-other" }},
		{"extra tag", func(i *fixtureInstance) { i.tags = append(i.tags, Tag{"team", "x"}) }},
		{"missing tag", func(i *fixtureInstance) { i.tags = i.tags[1:] }},
		{"token", func(i *fixtureInstance) { i.token = "skygo-other" }},
		{"account", func(i *fixtureInstance) { i.owner = "210987654321" }},
		{"region", func(i *fixtureInstance) { i.zone = "us-east-1a" }},
		{"image", func(i *fixtureInstance) { i.image = "ami-0fedcba9876543210" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			c := f.client(nil)
			req := testRequest(t)
			r, err := c.RunInstances(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			tt.change(f.instances[0])
			for _, m := range []func(context.Context, *CreateRequest, string) (*StateChange, error){c.Stop, c.Start, c.Terminate} {
				if _, err := m(context.Background(), req, r.Instances[0].ID); !errors.Is(err, ErrNotOwned) {
					t.Fatalf("error = %v, want ErrNotOwned", err)
				}
			}
			if _, err := c.ConsoleOutput(context.Background(), req, r.Instances[0].ID); !errors.Is(err, ErrNotOwned) {
				t.Fatalf("ConsoleOutput error = %v, want ErrNotOwned", err)
			}
			for _, a := range []string{"StopInstances", "StartInstances", "TerminateInstances", "GetConsoleOutput"} {
				if n := f.count(a); n != 0 {
					t.Fatalf("%s requests = %d, want 0", a, n)
				}
			}
		})
	}
}

func TestNotFoundIsNotTerminated(t *testing.T) {
	f := newFixture(t)
	c := f.client(nil)
	req := testRequest(t)
	_, err := c.Terminate(context.Background(), req, "i-0123456789abcdef0")
	e := errorClass(t, err)
	if e.Class != ClassInstanceNotFound || e.Action != "DescribeInstances" || e.Code != "InvalidInstanceID.NotFound" {
		t.Fatalf("error = %+v", e)
	}
	if n := f.count("TerminateInstances"); n != 0 {
		t.Fatalf("TerminateInstances requests = %d, want 0", n)
	}
}

// TestFindOwnedSecondPage puts an empty page with a NextToken before the
// page holding the owned instance.
func TestFindOwnedSecondPage(t *testing.T) {
	f := newFixture(t)
	c := f.client(nil)
	req := testRequest(t)
	if _, err := c.RunInstances(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	first := true
	f.before = func(action string, w http.ResponseWriter, r *http.Request) bool {
		if action != "DescribeInstances" || !first {
			return false
		}
		first = false
		fmt.Fprintf(w, `<DescribeInstancesResponse xmlns="%s"><requestId>p</requestId><reservationSet/><nextToken>page-0</nextToken></DescribeInstancesResponse>`, ec2Namespace)
		return true
	}
	inst, err := c.FindOwned(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if inst.ID != f.instances[0].id {
		t.Fatalf("instance = %s", inst.ID)
	}
	if n := f.count("DescribeInstances"); n != 2 {
		t.Fatalf("DescribeInstances requests = %d, want 2", n)
	}
	last := f.requests[len(f.requests)-1].form
	if last.Get("NextToken") != "page-0" || last.Get("Filter.1.Name") != "client-token" || last.Get("Filter.1.Value.1") != req.ClientToken() {
		t.Fatalf("second page request = %v", last)
	}
}

func TestFindOwnedNotObserved(t *testing.T) {
	f := newFixture(t)
	_, err := f.client(nil).FindOwned(context.Background(), testRequest(t))
	if !errors.Is(err, ErrNotObserved) {
		t.Fatalf("error = %v, want ErrNotObserved", err)
	}
}

func TestFindOwnedRefusesTwo(t *testing.T) {
	f := newFixture(t)
	c := f.client(nil)
	req := testRequest(t)
	if _, err := c.RunInstances(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	dup := *f.instances[0]
	dup.id = "i-0ffffffffffffffff"
	f.instances = append(f.instances, &dup)
	f.pageSize = 1
	if _, err := c.FindOwned(context.Background(), req); !errors.Is(err, ErrNotOwned) {
		t.Fatalf("error = %v, want ErrNotOwned", err)
	}
}

func TestDescribePagingBounds(t *testing.T) {
	tests := []struct {
		name string
		next func(n int) string
		max  int
	}{
		{"repeated token", func(int) string { return "same" }, 2},
		{"endless tokens", func(n int) string { return fmt.Sprintf("t%d", n) }, maxDescribePages},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			n := 0
			f.before = func(action string, w http.ResponseWriter, r *http.Request) bool {
				n++
				fmt.Fprintf(w, `<DescribeInstancesResponse xmlns="%s"><reservationSet/><nextToken>%s</nextToken></DescribeInstancesResponse>`, ec2Namespace, tt.next(n))
				return true
			}
			_, err := f.client(nil).FindOwned(context.Background(), testRequest(t))
			if e := errorClass(t, err); e.Class != ClassMalformed {
				t.Fatalf("error = %+v", e)
			}
			if errors.Is(err, ErrNotObserved) {
				t.Fatal("bounded search reported not observed")
			}
			if n != tt.max {
				t.Fatalf("requests = %d, want %d", n, tt.max)
			}
		})
	}
}

func TestResponseTooLarge(t *testing.T) {
	f := newFixture(t)
	f.before = func(action string, w http.ResponseWriter, r *http.Request) bool {
		fmt.Fprintf(w, `<DescribeInstancesResponse xmlns="%s"><requestId>%s</requestId></DescribeInstancesResponse>`,
			ec2Namespace, strings.Repeat("x", maxResponseBytes))
		return true
	}
	_, err := f.client(nil).FindOwned(context.Background(), testRequest(t))
	if e := errorClass(t, err); e.Class != ClassMalformed || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("error = %v", err)
	}
}

func TestRedirectNotFollowed(t *testing.T) {
	f := newFixture(t)
	f.before = func(action string, w http.ResponseWriter, r *http.Request) bool {
		http.Redirect(w, r, f.ec2.URL+"/elsewhere", http.StatusTemporaryRedirect)
		return true
	}
	_, err := f.client(nil).FindOwned(context.Background(), testRequest(t))
	if e := errorClass(t, err); e.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("error = %+v", e)
	}
	if n := f.count("DescribeInstances"); n != 1 {
		t.Fatalf("requests = %d, want 1", n)
	}
}

// TestCancelBoundsObservation cancels a create the fixture has received.
// The client returns promptly, reports a possible effect, and claims
// nothing about the fixture's state.
func TestCancelBoundsObservation(t *testing.T) {
	f := newFixture(t)
	got := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	f.before = func(action string, w http.ResponseWriter, r *http.Request) bool {
		if action != "RunInstances" {
			return false
		}
		close(got)
		select {
		case <-r.Context().Done():
		case <-release:
		}
		return true
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-got
		cancel()
	}()
	start := time.Now()
	_, err := f.client(nil).RunInstances(ctx, testRequest(t))
	e := errorClass(t, err)
	if e.Class != ClassTransport || !e.Sent || e.Retryable() || !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %+v (%v)", e, err)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("cancel took %v", d)
	}
	if n := f.count("RunInstances"); n != 1 {
		t.Fatalf("RunInstances requests = %d, want 1", n)
	}
}

// closeFault wraps a transport and fails every Close of a response body
// from host.
type closeFault struct {
	rt     http.RoundTripper
	host   string
	closes *int
}

func (c closeFault) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := c.rt.RoundTrip(r)
	if err == nil && r.URL.Host == c.host {
		resp.Body = &faultBody{resp.Body, c.closes}
	}
	return resp, err
}

type faultBody struct {
	io.ReadCloser
	closes *int
}

var errCloseFault = errors.New("injected close fault")

func (b *faultBody) Close() error {
	*b.closes++
	b.ReadCloser.Close()
	return errCloseFault
}

// TestCloseCustody injects a body Close failure as a policy fault, not a
// kernel fault. Each response body is closed exactly once, the failure is
// reported, and a decoded create result is retained beside it.
func TestCloseCustody(t *testing.T) {
	f := newFixture(t)
	c := f.client(nil)
	closes := 0
	c.http.Transport = closeFault{c.http.Transport, strings.TrimPrefix(f.ec2.URL, "https://"), &closes}
	req := testRequest(t)
	r, err := c.RunInstances(context.Background(), req)
	e := errorClass(t, err)
	if !errors.Is(err, errCloseFault) || e.Class != ClassTransport || e.Retryable() || closes != 1 {
		t.Fatalf("create: err = %v, closes = %d", err, closes)
	}
	if r == nil || r.Instances[0].ID != f.instances[0].id {
		t.Fatalf("create result not retained: %+v", r)
	}

	closes = 0
	_, err = c.Terminate(context.Background(), req, "i-0123456789abcdef0")
	if e := errorClass(t, err); !errors.Is(err, errCloseFault) || e.Class != ClassInstanceNotFound || closes != 1 {
		t.Fatalf("error response: err = %v, closes = %d", err, closes)
	}

	closes = 0
	_, err = c.Terminate(context.Background(), req, r.Instances[0].ID)
	if e := errorClass(t, err); !errors.Is(err, errCloseFault) || e.Action != "DescribeInstances" || closes != 1 {
		t.Fatalf("describe: err = %v, closes = %d", err, closes)
	}
	if n := f.count("TerminateInstances"); n != 0 {
		t.Fatalf("TerminateInstances requests = %d, want 0", n)
	}
}

func TestSessionTokenRequired(t *testing.T) {
	f := newFixture(t)
	c := f.client(nil)
	// Credentials without the session token the fixture expects are
	// rejected, so every successful test proves the token was signed.
	c.creds = credentialsFunc(func() (string, string, string) {
		return fixtureCreds.AccessKeyID, fixtureCreds.SecretAccessKey, ""
	})
	_, err := c.FindOwned(context.Background(), testRequest(t))
	if e := errorClass(t, err); e.Class != ClassUnauthorized || e.Code != "SignatureDoesNotMatch" {
		t.Fatalf("error = %+v", e)
	}
}

func TestConsoleOutput(t *testing.T) {
	f := newFixture(t)
	c := f.client(nil)
	req := testRequest(t)
	r, err := c.RunInstances(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	f.console = testConsole
	out, err := c.ConsoleOutput(context.Background(), req, r.Instances[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != testConsole {
		t.Fatalf("output = %q", out)
	}
	if _, err := ParseConsole(out); err != nil {
		t.Fatal(err)
	}
}

func TestClientRefusesOtherProfile(t *testing.T) {
	f := newFixture(t)
	p := testProfile()
	p.InstanceType = "t3.small"
	_, err := f.client(p).RunInstances(context.Background(), testRequest(t))
	if err == nil || errors.As(err, new(*Error)) {
		t.Fatalf("error = %v, want a local refusal", err)
	}
	if n := len(f.requests); n != 0 {
		t.Fatalf("requests = %d, want 0", n)
	}
}

func TestNewClient(t *testing.T) {
	p := testProfile()
	c, err := NewClient(p, credentialsFunc(func() (string, string, string) { return "a", "b", "" }))
	if err != nil {
		t.Fatal(err)
	}
	if c.ec2.url != "https://ec2.us-west-2.amazonaws.com/" || c.sts.url != "https://sts.us-west-2.amazonaws.com/" {
		t.Fatalf("endpoints = %s, %s", c.ec2.url, c.sts.url)
	}
	if _, err := newClient(p, c.creds, "http://127.0.0.1/", c.sts.url, nil); err == nil {
		t.Fatal("http endpoint accepted")
	}
	if _, err := NewClient(p, nil); err == nil {
		t.Fatal("nil credentials accepted")
	}
}

// launchCredit creates the creditProfile instance and marks it running.
func launchCredit(t *testing.T) (*fixture, *Client, *CreateRequest, string) {
	t.Helper()
	f := newFixture(t)
	c := f.client(creditProfile())
	req := creditRequest(t)
	r, err := c.RunInstances(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	f.instances[0].state = "running"
	return f, c, req, r.Instances[0].ID
}

func TestCheckCPUCredits(t *testing.T) {
	f, c, req, id := launchCredit(t)
	got, err := c.CheckCPUCredits(context.Background(), req, id)
	if err != nil || got != "standard" {
		t.Fatalf("CheckCPUCredits = %q, %v", got, err)
	}
	last := f.requests[len(f.requests)-1]
	if last.action != "DescribeInstanceCreditSpecifications" || len(last.form) != 3 || last.form.Get("InstanceId.1") != id {
		t.Fatalf("credit request = %v", last.form)
	}
	// The ownership check precedes the credit read.
	if prev := f.requests[len(f.requests)-2]; prev.action != "DescribeInstances" || prev.form.Get("InstanceId.1") != id {
		t.Fatalf("request before credit read = %s %v", prev.action, prev.form)
	}
}

// TestCreditMismatch checks the cleanup rule: a CPU credit mismatch on an
// owned instance blocks Start and is reported by CheckCPUCredits, but Stop
// and Terminate still proceed.
func TestCreditMismatch(t *testing.T) {
	f, c, req, id := launchCredit(t)
	f.instances[0].credits = "unlimited"
	got, err := c.CheckCPUCredits(context.Background(), req, id)
	var mismatch *CreditMismatchError
	if !errors.As(err, &mismatch) || got != "unlimited" || *mismatch != (CreditMismatchError{id, "standard", "unlimited"}) {
		t.Fatalf("CheckCPUCredits = %q, %v", got, err)
	}
	if _, err := c.Stop(context.Background(), req, id); err != nil {
		t.Fatalf("Stop refused: %v", err)
	}
	if _, err := c.Start(context.Background(), req, id); !errors.As(err, &mismatch) {
		t.Fatalf("Start error = %v, want CreditMismatchError", err)
	}
	if n := f.count("StartInstances"); n != 0 {
		t.Fatalf("StartInstances requests = %d, want 0", n)
	}
	if _, err := c.Terminate(context.Background(), req, id); err != nil {
		t.Fatalf("Terminate refused: %v", err)
	}
	if f.count("StopInstances") != 1 || f.count("TerminateInstances") != 1 {
		t.Fatal("cleanup mutations were not sent")
	}
}

// TestCreditUnavailable fails the credit read: Start refuses because an
// unavailable read is not evidence, while Stop and Terminate proceed.
func TestCreditUnavailable(t *testing.T) {
	f, c, req, id := launchCredit(t)
	f.instances[0].state = "stopped"
	f.before = func(action string, w http.ResponseWriter, r *http.Request) bool {
		if action != "DescribeInstanceCreditSpecifications" {
			return false
		}
		writeError(w, "ec2", http.StatusServiceUnavailable, "Unavailable")
		return true
	}
	_, err := c.Start(context.Background(), req, id)
	if e := errorClass(t, err); e.Action != "DescribeInstanceCreditSpecifications" || e.Class != ClassServerError {
		t.Fatalf("Start error = %+v", e)
	}
	if n := f.count("StartInstances"); n != 0 {
		t.Fatalf("StartInstances requests = %d, want 0", n)
	}
	if _, err := c.Stop(context.Background(), req, id); err != nil {
		t.Fatalf("Stop refused: %v", err)
	}
	if _, err := c.Terminate(context.Background(), req, id); err != nil {
		t.Fatalf("Terminate refused: %v", err)
	}
}

func TestStartWithMatchingCredits(t *testing.T) {
	f, c, req, id := launchCredit(t)
	f.instances[0].state = "stopped"
	if _, err := c.Start(context.Background(), req, id); err != nil {
		t.Fatal(err)
	}
	if f.count("DescribeInstanceCreditSpecifications") != 1 || f.count("StartInstances") != 1 {
		t.Fatal("Start did not read credits once and start once")
	}
}

func TestStartWithoutCreditOptionSkipsRead(t *testing.T) {
	f := newFixture(t)
	c := f.client(nil)
	req := testRequest(t)
	r, err := c.RunInstances(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Start(context.Background(), req, r.Instances[0].ID); err != nil {
		t.Fatal(err)
	}
	if n := f.count("DescribeInstanceCreditSpecifications"); n != 0 {
		t.Fatalf("credit reads = %d, want 0", n)
	}
}

// TestCreditCheckRequiresOwner makes the instance foreign: no credit read
// and no mutation is sent, including Stop and Terminate.
func TestCreditCheckRequiresOwner(t *testing.T) {
	f, c, req, id := launchCredit(t)
	f.instances[0].tags[0].Value = "skygo-other"
	if _, err := c.CheckCPUCredits(context.Background(), req, id); !errors.Is(err, ErrNotOwned) {
		t.Fatalf("CheckCPUCredits error = %v, want ErrNotOwned", err)
	}
	for _, m := range []func(context.Context, *CreateRequest, string) (*StateChange, error){c.Stop, c.Start, c.Terminate} {
		if _, err := m(context.Background(), req, id); !errors.Is(err, ErrNotOwned) {
			t.Fatalf("error = %v, want ErrNotOwned", err)
		}
	}
	for _, a := range []string{"DescribeInstanceCreditSpecifications", "StopInstances", "StartInstances", "TerminateInstances"} {
		if n := f.count(a); n != 0 {
			t.Fatalf("%s requests = %d, want 0", a, n)
		}
	}
}

// TestCreditResponseForOtherInstance answers the credit read for another
// instance: the response is refused, not attributed to the owned one.
func TestCreditResponseForOtherInstance(t *testing.T) {
	f, c, req, id := launchCredit(t)
	f.before = func(action string, w http.ResponseWriter, r *http.Request) bool {
		if action != "DescribeInstanceCreditSpecifications" {
			return false
		}
		fmt.Fprintf(w, `<DescribeInstanceCreditSpecificationsResponse xmlns="%s"><instanceCreditSpecificationSet><item><instanceId>i-0ffffffffffffffff</instanceId><cpuCredits>standard</cpuCredits></item></instanceCreditSpecificationSet></DescribeInstanceCreditSpecificationsResponse>`, ec2Namespace)
		return true
	}
	if _, err := c.CheckCPUCredits(context.Background(), req, id); errorClass(t, err).Class != ClassMalformed {
		t.Fatalf("error = %v", err)
	}
}
