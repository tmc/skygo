package kube

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

// The service writer sends the service runtime's creates and deletes, each
// as exactly one HTTP request, with its own copy of the target's
// http.Client: other targets' clients, http.DefaultClient and every shared
// or cached transport are unchanged. The copy differs in two ways.
//
// It follows no redirect. CheckRedirect returns http.ErrUseLastResponse, so
// the original 3xx response is returned and read as the answer; no request
// is sent to its Location, and no method, body or credential reaches it.
//
// Each request body is a oneShotBody, which net/http and x/net/http2 cannot
// rewind: http.NewRequest sets GetBody only for its own reader types, and
// the writer sets it to nil regardless. In the pinned sources (Go net/http;
// golang.org/x/net/http2 v0.30.0, the version the module's go.mod already
// selects; transport.go shouldRetryRequest) a request whose body was read
// and that has no GetBody is not retried, even after a peer's
// REFUSED_STREAM, and net/http's HTTP/1 transport retries only a request
// with GetBody or no body. The credential wrappers below this client are
// client-go v0.32.3's: its bearer round tripper and exec plugin each call
// the next layer once, the latter refreshing credentials after a 401
// without resending. A custom WrapTransport is outside this statement; the
// runtime refuses configurations with one.
//
// Requests use the target's frozen configuration: its server, TLS,
// authentication, impersonation and user agent, and JSON encoding of the
// typed apps/v1 Deployment, v1 Service and v1 DeleteOptions. MaxRetries and
// rest.Request are not used.

// maxServiceResponse bounds the bytes read from one write's response.
const maxServiceResponse = 1 << 20

// serviceAnswer is what one write received. Status is 0 if no response
// arrived; then the request may or may not have been applied. Body holds
// the response bytes read, at most maxServiceResponse, and Complete reports
// that they are the whole body. Err is the request's or the body read's
// error, or errServiceAnswerPending if neither had returned when the write
// did. Close is the response body's original first Close, or nil if no
// response body was returned: complete bytes and a successful Close are
// separate facts, and the answer may return before that Close has.
//
// x is the write's original exchange, which keeps running after the write
// returns, and facts what of it this answer reflects. An answer made
// without an exchange has x nil.
type serviceAnswer struct {
	Status   int
	Body     []byte
	Complete bool
	Err      error
	Close    *bodyClose

	x     *serviceExchange
	facts exchangeFacts
}

// closeErr returns the cause of a's original Close: errServiceClosePending
// until it returns, then its error, if any, as errServiceCloseFailed.
func (a serviceAnswer) closeErr() error { return a.Close.cause() }

var (
	// errServiceClosePending reports a write whose original response body
	// Close has not returned. Only that Close returning ends it.
	errServiceClosePending = errors.New("kubernetes service answer close pending")
	// errServiceCloseFailed reports a write whose original response body
	// Close failed. It stays for the service in this process.
	errServiceCloseFailed = errors.New("kubernetes service answer close failed")
	// errServiceAnswerPending reports a write whose original Do, or its
	// body read, had not returned when the write did. Neither a deadline
	// nor cancellation ends it; only their return does.
	errServiceAnswerPending = errors.New("kubernetes service answer pending")
	// errServiceRequestClosePending reports a write whose request body the
	// transport had not closed when the write returned.
	errServiceRequestClosePending = errors.New("kubernetes service request body close pending")
)

// bodyClose is the one original Close of a response body. done is closed
// when that Close returns; err, its result, may be read only after done.
type bodyClose struct {
	done chan struct{}
	err  error
}

// startClose calls c.Close once, in its own goroutine.
func startClose(c io.Closer) *bodyClose {
	b := &bodyClose{done: make(chan struct{})}
	go func() {
		b.err = c.Close()
		close(b.done)
	}()
	return b
}

// cause returns nil once b returned nil, errServiceClosePending before it
// returns, and its error, as errServiceCloseFailed, after.
func (b *bodyClose) cause() error {
	if b == nil {
		return nil
	}
	select {
	case <-b.done:
		if b.err != nil {
			return fmt.Errorf("%w: %w", errServiceCloseFailed, b.err)
		}
		return nil
	default:
		return errServiceClosePending
	}
}

// wait waits until b returns or ctx ends.
func (b *bodyClose) wait(ctx context.Context) {
	if b == nil {
		return
	}
	select {
	case <-b.done:
	case <-ctx.Done():
	}
}

// returned is a channel that is always closed.
var returned = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

func isClosed(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

// exchangeFacts are the originals of one exchange that have returned. Each
// only ever becomes true.
type exchangeFacts struct {
	answered      bool // Do and the body read
	requestClosed bool // the request body's first Close
	closed        bool // the response body's first Close, or answered with none
}

func (f exchangeFacts) settled() bool { return f.answered && f.requestClosed && f.closed }

// A serviceExchange is one write's original request and response: the
// request and its one-shot body, the worker running Do and the body read,
// the bounded answer, and the response body's single first Close. It owns
// each of them from the write's start until they have all returned,
// whenever its caller stops waiting; nothing in it is ever restarted,
// closed again or reset.
type serviceExchange struct {
	req  *http.Request
	body *oneShotBody // nil for an exchange made of an answer alone
	done chan struct{}
	// Set before done is closed.
	resp *http.Response
	a    serviceAnswer
}

// answeredExchange returns the exchange of a, an answer made without one:
// its request is done and its answer has returned.
func answeredExchange(a serviceAnswer) *serviceExchange {
	a.x, a.facts = nil, exchangeFacts{}
	return &serviceExchange{done: returned, a: a}
}

// run sends x's request once with do, its writer's client's Do, and reads
// its answer.
func (x *serviceExchange) run(do func(*http.Request) (*http.Response, error)) {
	defer close(x.done)
	resp, err := do(x.req)
	if err != nil {
		// Do closes the request body on error; a response is never
		// returned with an error, but its body is closed if one is.
		x.a = serviceAnswer{Err: err}
		if resp != nil && resp.Body != nil {
			x.resp = resp
			x.a.Close = startClose(resp.Body)
		}
		return
	}
	x.resp = resp
	x.a = readAnswer(resp)
}

// facts returns the originals of x that have returned by now.
func (x *serviceExchange) facts() exchangeFacts {
	f := exchangeFacts{answered: isClosed(x.done)}
	f.requestClosed = x.body == nil || isClosed(x.body.closed)
	f.closed = f.answered && (x.a.Close == nil || isClosed(x.a.Close.done))
	return f
}

// closeDone returns a channel closed once x's response body Close returns;
// x must have answered.
func (x *serviceExchange) closeDone() <-chan struct{} {
	if x.a.Close == nil {
		return returned
	}
	return x.a.Close.done
}

func (x *serviceExchange) requestClosed() <-chan struct{} {
	if x.body == nil {
		return returned
	}
	return x.body.closed
}

// join waits until every original of x has returned.
func (x *serviceExchange) join() {
	<-x.done
	<-x.requestClosed()
	<-x.closeDone()
}

// answer returns x's answer as of facts f.
func (x *serviceExchange) answer(f exchangeFacts) serviceAnswer {
	if !f.answered {
		return serviceAnswer{Err: errServiceAnswerPending, x: x, facts: f}
	}
	a := x.a
	a.x, a.facts = x, f
	return a
}

// failed reports whether, as of f, x's response body Close has failed.
func (x *serviceExchange) failed(f exchangeFacts) bool {
	return f.closed && x.a.Close != nil && x.a.Close.err != nil
}

// pending returns, as of f, the causes of x's originals that have not
// returned and of a failed response body Close.
func (x *serviceExchange) pending(f exchangeFacts) error {
	var errs []error
	if !f.answered {
		errs = append(errs, errServiceAnswerPending)
	}
	if !f.requestClosed {
		errs = append(errs, errServiceRequestClosePending)
	}
	switch {
	case !f.answered || x.a.Close == nil:
	case !f.closed:
		errs = append(errs, errServiceClosePending)
	case x.a.Close.err != nil:
		errs = append(errs, fmt.Errorf("%w: %w", errServiceCloseFailed, x.a.Close.err))
	}
	return errors.Join(errs...)
}

// cause returns, as of f, every cause of x: the request's or the read's
// error and those of pending. The same facts give the same causes.
func (x *serviceExchange) cause(f exchangeFacts) error {
	if f.answered && x.a.Err != nil {
		return errors.Join(x.a.Err, x.pending(f))
	}
	return x.pending(f)
}

// refusal returns the bounded witness of a, a complete answer.
func (a serviceAnswer) refusal(decoded bool) *serviceRefusal {
	sum := sha256.Sum256(a.Body)
	return &serviceRefusal{Status: a.Status, Size: int64(len(a.Body)), SHA256: hex.EncodeToString(sum[:]), Decoded: decoded}
}

// serviceWriter makes the service runtime's writes. Each call sends at most
// one request and returns its answer.
type serviceWriter interface {
	create(ctx context.Context, k serviceKind, obj any) serviceAnswer
	delete(ctx context.Context, k serviceKind, name string, uid types.UID) serviceAnswer
}

// Default bounds of a write's caller waits; see serviceHTTP.await.
const (
	serviceAnswerWait = 30 * time.Second
	serviceSettleWait = 250 * time.Millisecond
)

type serviceHTTP struct {
	client *http.Client
	// do is client.Do, which sends each write's request once. Tests may
	// wrap it, before any write, with one that calls it exactly once.
	do        func(*http.Request) (*http.Response, error)
	server    string // canonical, without a trailing slash
	namespace string
	// answerWait bounds a caller's wait for its write's answer and
	// settleWait its further wait for that write's request and response
	// body Closes. Tests may shorten them while no write is in progress.
	answerWait, settleWait time.Duration
}

func refuseServiceRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// newServiceHTTP returns the service writer of namespace for config, the
// target's frozen configuration. config is not changed.
func newServiceHTTP(config *rest.Config, namespace string) (*serviceHTTP, error) {
	c := rest.CopyConfig(config)
	if c.WrapTransport != nil {
		return nil, errors.New("kubernetes service writer refuses a wrapped transport")
	}
	if c.UserAgent == "" {
		c.UserAgent = rest.DefaultKubernetesUserAgent()
	}
	server, err := canonicalServer(c.Host)
	if err != nil {
		return nil, err
	}
	shared, err := rest.HTTPClientFor(c)
	if err != nil {
		return nil, fmt.Errorf("kubernetes service client: %w", err)
	}
	// shared may be http.DefaultClient; only the copy is changed.
	hc := *shared
	hc.CheckRedirect = refuseServiceRedirect
	return &serviceHTTP{client: &hc, do: hc.Do, server: server, namespace: namespace,
		answerWait: serviceAnswerWait, settleWait: serviceSettleWait}, nil
}

func (c *serviceHTTP) collection(k serviceKind) string {
	if k == deploymentObject {
		return c.server + "/apis/apps/v1/namespaces/" + c.namespace + "/deployments"
	}
	return c.server + "/api/v1/namespaces/" + c.namespace + "/services"
}

func (c *serviceHTTP) create(ctx context.Context, k serviceKind, obj any) serviceAnswer {
	switch o := obj.(type) {
	case *appsv1.Deployment:
		o = o.DeepCopy()
		o.TypeMeta = metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"}
		obj = o
	case *corev1.Service:
		o = o.DeepCopy()
		o.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}
		obj = o
	default:
		return serviceAnswer{Err: errors.New("kubernetes service writer: unknown object")}
	}
	return c.send(ctx, http.MethodPost, c.collection(k), obj)
}

// delete deletes the object of uid at name only. The Deployment is deleted
// in the foreground, so that it stays visible until its Pods are gone. A
// lost answer may be followed by the same delete, with the same
// precondition: it can affect only the object a first delivery already
// deleted.
func (c *serviceHTTP) delete(ctx context.Context, k serviceKind, name string, uid types.UID) serviceAnswer {
	opts := &metav1.DeleteOptions{
		TypeMeta:      metav1.TypeMeta{APIVersion: "v1", Kind: "DeleteOptions"},
		Preconditions: &metav1.Preconditions{UID: &uid},
	}
	if k == deploymentObject {
		foreground := metav1.DeletePropagationForeground
		opts.PropagationPolicy = &foreground
	}
	return c.send(ctx, http.MethodDelete, c.collection(k)+"/"+name, opts)
}

// send sends one request with a one-shot JSON body and reads its answer, in
// the request's own exchange, started once. It returns the answer as of
// await's end, with that exchange: an original that has not returned by
// then stays the exchange's, and the caller must take custody of it.
func (c *serviceHTTP) send(ctx context.Context, method, url string, v any) serviceAnswer {
	raw, err := json.Marshal(v)
	if err != nil {
		return serviceAnswer{Err: err}
	}
	body := newOneShotBody(raw)
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return serviceAnswer{Err: err}
	}
	req.GetBody = nil
	req.ContentLength = int64(len(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	x := &serviceExchange{req: req, body: body, done: make(chan struct{})}
	go x.run(c.do)
	c.await(ctx, x)
	return x.answer(x.facts())
}

// await waits until x's Do and body read return, at most answerWait, and
// then until its request body's and response body's first Closes return,
// at most settleWait more. ctx ending ends either wait earlier, whether or
// not Do or the read honor it. Neither end is an original's return.
func (c *serviceHTTP) await(ctx context.Context, x *serviceExchange) {
	t := time.NewTimer(c.answerWait)
	defer t.Stop()
	select {
	case <-x.done:
	case <-t.C:
		return
	case <-ctx.Done():
		return
	}
	s := time.NewTimer(c.settleWait)
	defer s.Stop()
	for _, ch := range []<-chan struct{}{x.requestClosed(), x.closeDone()} {
		select {
		case <-ch:
		case <-s.C:
			return
		case <-ctx.Done():
			return
		}
	}
}

// readAnswer reads at most maxServiceResponse bytes of resp's body and
// starts its one original Close. The read's error and the Close are kept
// apart: neither replaces the other.
func readAnswer(resp *http.Response) serviceAnswer {
	a := serviceAnswer{Status: resp.StatusCode}
	var buf bytes.Buffer
	n, err := io.Copy(&buf, io.LimitReader(resp.Body, maxServiceResponse+1))
	switch {
	case err != nil:
		a.Err = err
	case n > maxServiceResponse:
		a.Err = errors.New("kubernetes service answer exceeds its bound")
	default:
		a.Complete = true
	}
	a.Body = buf.Bytes()
	if len(a.Body) > maxServiceResponse {
		a.Body = a.Body[:maxServiceResponse]
	}
	a.Close = startClose(resp.Body)
	return a
}

// oneShotBody is a request body that can be read once and has no way to
// start again. closed is closed by its first Close; later calls return the
// first's result.
type oneShotBody struct {
	r      *bytes.Reader
	once   sync.Once
	closed chan struct{}
}

func newOneShotBody(raw []byte) *oneShotBody {
	return &oneShotBody{r: bytes.NewReader(raw), closed: make(chan struct{})}
}

func (b *oneShotBody) Read(p []byte) (int, error) { return b.r.Read(p) }

func (b *oneShotBody) Close() error {
	b.once.Do(func() { close(b.closed) })
	return nil
}

// decodeAnswer decodes a complete 2xx answer as the object kind k and
// reports whether it decoded as that kind.
func decodeAnswer(k serviceKind, body []byte) (any, bool) {
	var (
		obj  any
		meta *metav1.TypeMeta
	)
	if k == deploymentObject {
		d := &appsv1.Deployment{}
		obj, meta = d, &d.TypeMeta
	} else {
		s := &corev1.Service{}
		obj, meta = s, &s.TypeMeta
	}
	if err := json.Unmarshal(body, obj); err != nil {
		return nil, false
	}
	want := metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"}
	if k != deploymentObject {
		want = metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}
	}
	if *meta != want {
		return nil, false
	}
	return obj, true
}

// statusBody reports whether body decodes as a metav1.Status.
func statusBody(body []byte) bool {
	var s metav1.Status
	return json.Unmarshal(body, &s) == nil && s.Kind == "Status"
}
