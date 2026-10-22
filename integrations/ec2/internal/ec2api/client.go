package ec2api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

const (
	// requestTimeout bounds one HTTP exchange, including the response body.
	requestTimeout = 60 * time.Second

	// maxDescribePages bounds the pages one search follows.
	maxDescribePages = 20
)

// An endpoint is one signed Query service.
type endpoint struct {
	url       string
	service   string // SigV4 service name
	namespace string // XML namespace of successful responses
	errorRoot string // root element of error responses
}

// A Client calls EC2 and STS for one profile.
//
// Every call makes exactly one HTTP attempt on a new connection. The client
// never retries, never follows a redirect, ignores proxy environment
// variables and does not request compressed responses.
//
// Each public operation retrieves credentials from the provider once and
// signs all of its requests, including its STS account check and ownership
// description, with that one snapshot. A provider may return different
// credentials to each operation; each mutation is checked afresh.
//
// An error that is not an [*Error] was detected before any request was
// sent. An *Error describes the request named by its Action field, which
// may be the DescribeInstances or GetCallerIdentity check that precedes a
// mutation rather than the mutation itself.
type Client struct {
	profile Profile
	creds   aws.CredentialsProvider
	signer  *v4.Signer
	http    *http.Client
	ec2     endpoint
	sts     endpoint
	now     func() time.Time
}

// NewClient returns a client for profile p that signs requests with
// credentials from creds. It uses the regional endpoints
// https://ec2.REGION.amazonaws.com/ and https://sts.REGION.amazonaws.com/.
// NewClient reads no files or environment variables.
func NewClient(p *Profile, creds aws.CredentialsProvider) (*Client, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return newClient(p, creds,
		"https://ec2."+p.Region+".amazonaws.com/",
		"https://sts."+p.Region+".amazonaws.com/",
		nil)
}

// newClient returns a client using the given endpoints. A nil roots uses
// the system certificate pool.
func newClient(p *Profile, creds aws.CredentialsProvider, ec2URL, stsURL string, roots *x509.CertPool) (*Client, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	if creds == nil {
		return nil, errors.New("ec2api: no credentials provider")
	}
	for _, s := range []string{ec2URL, stsURL} {
		if u, err := url.Parse(s); err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" {
			return nil, fmt.Errorf("ec2api: endpoint %q is not an https URL", s)
		}
	}
	transport := &http.Transport{
		Proxy:                  nil,
		DialContext:            (&net.Dialer{Timeout: 30 * time.Second}).DialContext,
		TLSClientConfig:        &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots},
		TLSHandshakeTimeout:    10 * time.Second,
		TLSNextProto:           map[string]func(string, *tls.Conn) http.RoundTripper{},
		DisableKeepAlives:      true,
		DisableCompression:     true,
		MaxResponseHeaderBytes: 64 << 10,
	}
	profile := *p
	profile.SecurityGroupIDs = slices.Clone(p.SecurityGroupIDs)
	return &Client{
		profile: profile,
		creds:   creds,
		signer:  v4.NewSigner(),
		http: &http.Client{
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
			Timeout: requestTimeout,
		},
		ec2: endpoint{ec2URL, "ec2", ec2Namespace, "Response"},
		sts: endpoint{stsURL, "sts", stsNamespace, "ErrorResponse"},
		now: time.Now,
	}, nil
}

// RunInstances retrieves one credential snapshot, checks with STS that it
// belongs to the profile's account, and sends req once signed with that
// same snapshot.
//
// RunInstances never retries. If a sent create fails ambiguously, the
// instance may exist: reissue the same req, whose ClientToken and body are
// unchanged, or observe the exact owned identity with FindOwned and
// DescribeOwned. Absence from those observations is not deletion, and a
// new token or replacement request for the same identity is never safe.
// This package assumes no period for which EC2 retains a ClientToken.
//
// After a create, call CheckCPUCredits before continuing to use the
// instance.
//
// If the response decodes but violates the request (not exactly one
// instance, another account, another token), RunInstances returns the
// decoded reservation together with an error.
func (c *Client) RunInstances(ctx context.Context, req *CreateRequest) (*Reservation, error) {
	const action = "RunInstances"
	if err := req.matches(&c.profile); err != nil {
		return nil, err
	}
	creds, err := c.snapshot(ctx, action)
	if err != nil {
		return nil, err
	}
	if err := c.checkAccount(ctx, creds); err != nil {
		return nil, err
	}
	var out runResponse
	closeErr, err := c.exchange(ctx, creds, c.ec2, action, req.body, &out)
	if err != nil {
		return nil, err
	}
	r, err := out.reservation()
	if err != nil {
		return nil, finish(action, err, closeErr)
	}
	switch {
	case len(r.Instances) != 1:
		err = fmt.Errorf("reservation has %d instances, want 1", len(r.Instances))
	case r.OwnerID != c.profile.Account:
		err = errors.New("reservation belongs to a different account")
	case r.Instances[0].ClientToken != req.spec.token:
		err = errors.New("instance client token does not match the request")
	}
	return r, finish(action, err, closeErr)
}

// DescribeOwned describes exactly instance id and returns it if
// [VerifyOwner] accepts it for req.
// An InvalidInstanceID.NotFound error does not mean the instance is gone.
func (c *Client) DescribeOwned(ctx context.Context, req *CreateRequest, id string) (*Instance, error) {
	if err := req.matches(&c.profile); err != nil {
		return nil, err
	}
	creds, err := c.snapshot(ctx, "DescribeInstances")
	if err != nil {
		return nil, err
	}
	return c.describeOwned(ctx, creds, req, id)
}

// FindOwned searches for the instance req launched by its ClientToken and
// owner tags, following every page, and returns it if exactly one instance
// matches and [VerifyOwner] accepts it.
//
// If no instance matches, FindOwned returns an error wrapping
// ErrNotObserved. Searches are eventually consistent: shortly after a
// create, an empty result does not mean that no instance exists.
func (c *Client) FindOwned(ctx context.Context, req *CreateRequest) (*Instance, error) {
	if err := req.matches(&c.profile); err != nil {
		return nil, err
	}
	creds, err := c.snapshot(ctx, "DescribeInstances")
	if err != nil {
		return nil, err
	}
	rs, err := c.describe(ctx, creds, func(next string) []byte { return describeByTokenBody(req, next) })
	if err != nil {
		return nil, err
	}
	for _, r := range rs {
		if len(r.Instances) > 0 {
			return VerifyOwner(&c.profile, req, rs, r.Instances[0].ID)
		}
	}
	return nil, fmt.Errorf("ec2api: client token %s: %w", req.spec.token, ErrNotObserved)
}

// CheckCPUCredits verifies that req launched instance id and returns the
// CPU credit option EC2 reports for exactly that instance. If the profile
// sets CPUCredits and the reported option differs, it returns the option
// with a [*CreditMismatchError]. A caller must not continue a create (for
// example by bootstrapping the instance) until CheckCPUCredits succeeds; a
// failed read is not evidence that the option is right.
func (c *Client) CheckCPUCredits(ctx context.Context, req *CreateRequest, id string) (string, error) {
	if err := req.matches(&c.profile); err != nil {
		return "", err
	}
	creds, err := c.snapshot(ctx, "DescribeInstances")
	if err != nil {
		return "", err
	}
	if _, err := c.describeOwned(ctx, creds, req, id); err != nil {
		return "", err
	}
	return c.checkCredits(ctx, creds, id)
}

// Stop stops instance id after verifying that req launched it.
// A CPU credit mismatch does not prevent stopping an owned instance.
func (c *Client) Stop(ctx context.Context, req *CreateRequest, id string) (*StateChange, error) {
	return c.mutate(ctx, "StopInstances", req, id, false)
}

// Start starts instance id after verifying that req launched it and, if
// the profile sets CPUCredits, that EC2 reports that option for it. A
// mismatch returns a [*CreditMismatchError]; a failed read also refuses.
func (c *Client) Start(ctx context.Context, req *CreateRequest, id string) (*StateChange, error) {
	return c.mutate(ctx, "StartInstances", req, id, c.profile.CPUCredits != "")
}

// Terminate terminates instance id after verifying that req launched it.
// A CPU credit mismatch does not prevent terminating an owned instance.
// A reported state of shutting-down or terminated is not confirmation that
// the instance's volume and network interface are gone.
func (c *Client) Terminate(ctx context.Context, req *CreateRequest, id string) (*StateChange, error) {
	return c.mutate(ctx, "TerminateInstances", req, id, false)
}

// mutate sends one lifecycle action for exactly instance id. It retrieves
// one credential snapshot and signs, with that snapshot, the account check,
// an ownership check of a fresh description, the CPU credit check if
// credits is set, and the action.
func (c *Client) mutate(ctx context.Context, action string, req *CreateRequest, id string, credits bool) (*StateChange, error) {
	if err := req.matches(&c.profile); err != nil {
		return nil, err
	}
	creds, err := c.snapshot(ctx, action)
	if err != nil {
		return nil, err
	}
	if err := c.checkAccount(ctx, creds); err != nil {
		return nil, err
	}
	if _, err := c.describeOwned(ctx, creds, req, id); err != nil {
		return nil, err
	}
	if credits {
		if _, err := c.checkCredits(ctx, creds, id); err != nil {
			return nil, err
		}
	}
	var out stateChangeResponse
	closeErr, err := c.exchange(ctx, creds, c.ec2, action, queryBody(action, ec2Version, "InstanceId.1", id), &out)
	if err != nil {
		return nil, err
	}
	sc, err := out.stateChange(id)
	return sc, finish(action, err, closeErr)
}

// ConsoleOutput returns the console output of instance id after verifying
// that req launched it.
func (c *Client) ConsoleOutput(ctx context.Context, req *CreateRequest, id string) ([]byte, error) {
	const action = "GetConsoleOutput"
	if err := req.matches(&c.profile); err != nil {
		return nil, err
	}
	creds, err := c.snapshot(ctx, action)
	if err != nil {
		return nil, err
	}
	if _, err := c.describeOwned(ctx, creds, req, id); err != nil {
		return nil, err
	}
	var out consoleResponse
	closeErr, err := c.exchange(ctx, creds, c.ec2, action, queryBody(action, ec2Version, "InstanceId", id), &out)
	if err != nil {
		return nil, err
	}
	b, err := out.output(id)
	if err != nil {
		return nil, finish(action, err, closeErr)
	}
	return b, finish(action, nil, closeErr)
}

// snapshot retrieves the one credential value that signs every request of
// an operation. The provider is called once per operation and is never
// assumed to return the same credentials twice.
func (c *Client) snapshot(ctx context.Context, action string) (aws.Credentials, error) {
	if err := ctx.Err(); err != nil {
		return aws.Credentials{}, &Error{Action: action, Class: ClassTransport, cause: err}
	}
	creds, err := c.creds.Retrieve(ctx)
	if err != nil {
		return aws.Credentials{}, &Error{Action: action, Class: ClassUnauthorized, cause: fmt.Errorf("retrieve credentials: %w", err)}
	}
	if !creds.HasKeys() {
		return aws.Credentials{}, &Error{Action: action, Class: ClassUnauthorized, cause: errors.New("credentials have no keys")}
	}
	return creds, nil
}

// describeOwned describes exactly instance id with creds and verifies it.
func (c *Client) describeOwned(ctx context.Context, creds aws.Credentials, req *CreateRequest, id string) (*Instance, error) {
	if !instanceIDRE.MatchString(id) {
		return nil, fmt.Errorf("ec2api: instance ID %q is invalid", id)
	}
	rs, err := c.describe(ctx, creds, func(next string) []byte { return describeByIDBody(id, next) })
	if err != nil {
		return nil, err
	}
	return VerifyOwner(&c.profile, req, rs, id)
}

// checkCredits reads, with creds, the CPU credit option of exactly instance
// id, which the caller has verified as owned, and compares it with the
// profile's.
func (c *Client) checkCredits(ctx context.Context, creds aws.Credentials, id string) (string, error) {
	const action = "DescribeInstanceCreditSpecifications"
	var out creditResponse
	closeErr, err := c.exchange(ctx, creds, c.ec2, action, queryBody(action, ec2Version, "InstanceId.1", id), &out)
	if err != nil {
		return "", err
	}
	got, err := out.credits(id)
	if err := finish(action, err, closeErr); err != nil {
		return "", err
	}
	if want := c.profile.CPUCredits; want != "" && got != want {
		return got, &CreditMismatchError{InstanceID: id, Want: want, Got: got}
	}
	return got, nil
}

// checkAccount verifies with STS GetCallerIdentity, signed with creds, that
// creds belong to the profile's account. It is called for every guarded
// operation; a success approves only that operation's snapshot.
func (c *Client) checkAccount(ctx context.Context, creds aws.Credentials) error {
	const action = "GetCallerIdentity"
	var out callerIdentityResponse
	closeErr, err := c.exchange(ctx, creds, c.sts, action, queryBody(action, stsVersion), &out)
	if err != nil {
		return err
	}
	if !accountRE.MatchString(out.Account) {
		err = errors.New("caller identity has no valid account")
	}
	if err := finish(action, err, closeErr); err != nil {
		return err
	}
	if out.Account != c.profile.Account {
		return fmt.Errorf("ec2api: %w", ErrAccountMismatch)
	}
	return nil
}

// describe returns every reservation that DescribeInstances reports for the
// request bodies page returns, following NextToken within bounds.
func (c *Client) describe(ctx context.Context, creds aws.Credentials, page func(next string) []byte) ([]Reservation, error) {
	const action = "DescribeInstances"
	var all []Reservation
	instances := 0
	seen := make(map[string]bool)
	next := ""
	for range maxDescribePages {
		var out describeResponse
		closeErr, err := c.exchange(ctx, creds, c.ec2, action, page(next), &out)
		if err != nil {
			return nil, err
		}
		rs, err := out.reservations()
		if err := finish(action, err, closeErr); err != nil {
			return nil, err
		}
		for _, r := range rs {
			instances += len(r.Instances)
		}
		if all = append(all, rs...); len(all) > maxReservations || instances > maxInstances {
			return nil, finish(action, errors.New("search exceeds its instance bound"), nil)
		}
		if out.NextToken == "" {
			return all, nil
		}
		if seen[out.NextToken] {
			return nil, finish(action, errors.New("search repeats a next token"), nil)
		}
		seen[out.NextToken] = true
		next = out.NextToken
	}
	return nil, finish(action, fmt.Errorf("search exceeds %d pages", maxDescribePages), nil)
}

// exchange signs body with creds and sends it to ep in exactly one HTTP
// attempt. On a 200 response it decodes the XML document with root
// <action>Response into out and returns the error from closing the response
// body, if any, as closeErr. Otherwise it returns an *Error that retains the
// close error.
func (c *Client) exchange(ctx context.Context, creds aws.Credentials, ep endpoint, action string, body []byte, out any) (closeErr, err error) {
	if err := ctx.Err(); err != nil {
		return nil, &Error{Action: action, Class: ClassTransport, cause: err}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep.url, bytes.NewReader(body))
	if err != nil {
		return nil, &Error{Action: action, Class: ClassOther, cause: err}
	}
	req.GetBody = nil // the body is never replayed
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	sum := sha256.Sum256(body)
	if err := c.signer.SignHTTP(ctx, creds, req, hex.EncodeToString(sum[:]), ep.service, c.profile.Region, c.now()); err != nil {
		return nil, &Error{Action: action, Class: ClassOther, cause: fmt.Errorf("sign request: %w", err)}
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, &Error{Action: action, Class: ClassTransport, Sent: true, cause: err}
	}
	data, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err := resp.Body.Close(); err != nil {
		closeErr = fmt.Errorf("close response body: %w", err)
	}

	e := &Error{Action: action, StatusCode: resp.StatusCode, Sent: true}
	switch {
	case readErr != nil:
		e.Class, e.cause = ClassTransport, errors.Join(fmt.Errorf("read response: %w", readErr), closeErr)
		return nil, e
	case len(data) > maxResponseBytes:
		e.Class, e.cause = ClassMalformed, errors.Join(fmt.Errorf("response exceeds %d bytes", maxResponseBytes), closeErr)
		return nil, e
	case resp.StatusCode != http.StatusOK:
		code, message, requestID, err := decodeError(data, ep.errorRoot)
		if err != nil {
			// The decode error may quote provider bytes; report only
			// that the envelope was refused.
			e.Class, e.cause = ClassMalformed, errors.Join(errors.New("error response is malformed"), closeErr)
			return nil, e
		}
		e.Class, e.Code, e.RequestID, e.cause = classify(resp.StatusCode, code), code, requestID, closeErr
		e.provider = &providerError{code, message}
		return nil, e
	}
	root := xml.Name{Space: ep.namespace, Local: action + "Response"}
	if err := decodeXML(data, root, out); err != nil {
		e.Class, e.cause = ClassMalformed, errors.Join(err, closeErr)
		return nil, e
	}
	return closeErr, nil
}

// finish returns nil if neither err, a violation found in a decoded
// response, nor closeErr is set. Otherwise it returns an *Error retaining
// both.
func finish(action string, err, closeErr error) error {
	switch {
	case err != nil:
		return &Error{Action: action, Class: ClassMalformed, StatusCode: http.StatusOK, Sent: true, cause: errors.Join(err, closeErr)}
	case closeErr != nil:
		return &Error{Action: action, Class: ClassTransport, StatusCode: http.StatusOK, Sent: true, cause: closeErr}
	}
	return nil
}
