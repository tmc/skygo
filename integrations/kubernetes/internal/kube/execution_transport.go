package kube

import (
	"errors"
	"io"
	"net/http"
	"sync"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// The receipt client is the clientset of a Backend with execution receipts.
// newReceiptClient builds it from the target's frozen configuration as
// kubernetes.NewForConfig does, but with its own copy of the http.Client, so
// that the clients of other targets, http.DefaultClient and every shared or
// cached transport are unchanged. It differs in two ways.
//
// It follows no redirect. Its CheckRedirect returns http.ErrUseLastResponse,
// so the original 3xx response, its body unread and unclosed, is returned to
// client-go, which reads and closes it as it does any response. No request is
// sent to the Location: no method, body or credential reaches it. (Before
// CheckRedirect runs, net/http may build, but never sends, the next hop's
// request, calling the original request's GetBody for a 307 or 308.)
//
// The single create of startReceipted is tagged with a receiptExchange.
// receiptTransport passes it to the layers below with GetBody removed, so
// that no layer can obtain its body again, and records the status of a 3xx
// response and the exact bytes client-go reads from its body. Without
// GetBody, in the pinned sources (Go net/http, x/net/http2 v0.56.0,
// http2/transport_common.go):
//
//   - net/http's HTTP/1 transport retries a POST only after nothingWrittenError,
//     and then only with GetBody or an empty body; it never rewinds a body
//     already read.
//   - x/net/http2 RoundTripOpt retries a request with a body and no GetBody
//     only for errClientConnUnusable, returned before the stream's body was
//     read. A peer PROTOCOL_ERROR, REFUSED_STREAM or GOAWAY fails the request.
//
// So the transport may try again only a request whose body it never read,
// and never sends a body twice. A request so retried is the same request; no
// claim is made that none of its header frames was ever written. Credential
// wrappers below are the standard client-go and oauth2 ones, which delegate
// once; a custom WrapTransport is outside this statement.
//
// Only the create needs a one-shot body. The other receipted requests are
// reads without a body, which change nothing, and deletions preconditioned on
// the pinned UID, whose repetition can affect only the object a first
// delivery already deleted. Their outcomes are classified as before: a
// deletion's NotFound proves neither delivery nor release.

// newReceiptClient returns the receipt client for config, the frozen
// configuration of a target. config is not changed.
func newReceiptClient(config *rest.Config) (*kubernetes.Clientset, error) {
	c := *config
	if c.UserAgent == "" {
		c.UserAgent = rest.DefaultKubernetesUserAgent()
	}
	shared, err := rest.HTTPClientFor(&c)
	if err != nil {
		return nil, err
	}
	// shared may be http.DefaultClient; only the copy is changed.
	hc := *shared
	base := shared.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	hc.CheckRedirect = refuseRedirect
	hc.Transport = &receiptTransport{base: base}
	return kubernetes.NewForConfigAndClient(&c, &hc)
}

func refuseRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// receiptHTTP reports whether rc sends through the receipt client.
func receiptHTTP(rc rest.Interface) (*rest.RESTClient, error) {
	c, ok := rc.(*rest.RESTClient)
	if !ok || c == nil || c.Client == nil || c.Client.CheckRedirect == nil {
		return nil, errors.New("kubernetes execution receipts require the receipt client")
	}
	if _, ok := c.Client.Transport.(*receiptTransport); !ok {
		return nil, errors.New("kubernetes execution receipts require the receipt client")
	}
	return c, nil
}

// receiptTransport is the transport of the receipt client. Requests tagged
// with a draCall, a receiptExchange or a logCustody are admitted once each;
// untagged requests pass through unchanged.
type receiptTransport struct {
	base http.RoundTripper
}

type receiptExchangeKey struct{}

func (t *receiptTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// DRA requests only: see draCall (dra_receipt_http.go).
	if c, _ := req.Context().Value(draCallKey{}).(*draCall); c != nil {
		return c.roundTrip(t.base, req)
	}
	if x, _ := req.Context().Value(receiptExchangeKey{}).(*receiptExchange); x != nil {
		return x.roundTrip(t.base, req)
	}
	if c, _ := req.Context().Value(logBodyKey{}).(*logCustody); c != nil {
		return c.roundTrip(t.base, req)
	}
	return t.base.RoundTrip(req)
}

// errSentOnce reports a second request of a tagged exchange. client-go does
// not make one with MaxRetries(0) and the receipt client follows no
// redirect; the transport refuses it regardless, without sending it.
var errSentOnce = errors.New("kubernetes receipted request already sent")

// receiptExchange is the custody record of one receipted create request.
type receiptExchange struct {
	mu     sync.Mutex
	sent   bool
	status int    // the status of a 3xx response, 0 if none arrived
	body   []byte // the bytes read from that response's body
	ended  bool   // its body reported io.EOF
	failed bool   // its body reported another error
}

func (x *receiptExchange) roundTrip(base http.RoundTripper, req *http.Request) (*http.Response, error) {
	x.mu.Lock()
	sent := x.sent
	x.sent = true
	x.mu.Unlock()
	if sent {
		if req.Body != nil {
			req.Body.Close()
		}
		return nil, errSentOnce
	}
	one := *req
	one.GetBody = nil
	resp, err := base.RoundTrip(&one)
	if resp != nil && resp.StatusCode >= 300 && resp.StatusCode <= 399 && resp.Body != nil {
		x.mu.Lock()
		x.status = resp.StatusCode
		x.mu.Unlock()
		resp.Body = &exchangeBody{ReadCloser: resp.Body, x: x}
	}
	return resp, err
}

// redirect returns the status of a 3xx response, if one arrived, the bytes
// read from its body, and whether they are the whole body: reading ended at
// io.EOF without an error.
func (x *receiptExchange) redirect() (status int, body []byte, complete bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.status, x.body, x.ended && !x.failed
}

// exchangeBody records what is read from a 3xx response body. client-go reads
// and closes it; Close is the original body's.
type exchangeBody struct {
	io.ReadCloser
	x *receiptExchange
}

func (b *exchangeBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.x.mu.Lock()
	defer b.x.mu.Unlock()
	b.x.body = append(b.x.body, p[:n]...)
	switch {
	case err == io.EOF:
		b.x.ended = true
	case err != nil:
		b.x.failed = true
	}
	return n, err
}
