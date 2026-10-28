package kube

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
)

// Receipted log bodies.
//
// A receipted Logs call reserves a logCustody before its first request and
// sends its single log request tagged with it. receiptTransport registers the
// response body in the custody; its first Close makes the only Close of the
// original body and records the result, whoever calls it (client-go closes
// the body of a failed response itself and discards the error). Logs reads
// the body, closes it, settles the custody, and only then makes its final
// receipt check, so that no log byte is released, and no operation permit
// is released, before the original body positively closed.
//
// A custody settles clean if no body arrived or the original Close returned
// nil. Otherwise Logs returns errLogBodyHeld and no bytes, and the custody,
// with the original body, its Close error and a never-released reference to
// the shared operation permit of the receipt store, is kept in logCustodies
// until the process exits. Its consequences are exact and finite:
//
//   - Cleanup of the receipt scope fails with errOperationBusy in every
//     Backend and process while this process lives.
//   - Logs of the same attempt in the same scope are refused in this process.
//   - At most maxLogBodies custodies are reserved or kept per process; when
//     all are in use every receipted Logs call is refused, and a kept custody
//     is never dropped to make room.
//
// Nothing is closed again and nothing is canceled to settle a custody. A Close
// that does not return keeps its Logs call, and so its permit and
// reservation, waiting. The operator releases a kept custody only by stopping
// the process, which closes the original connection and the lock file.

// maxLogBodies bounds the log custodies of a process, reserved or kept.
const maxLogBodies = 16

// errLogBodyHeld reports a log response body whose closure is uncertain, or
// an attempt that has one in this process.
var errLogBodyHeld = errors.New("kubernetes log response body closure is uncertain")

// errLogBodyLimit reports that every log custody of the process is in use.
var errLogBodyLimit = errors.New("kubernetes log custody limit reached")

// logCustodies is the process-wide custody of receipted log bodies. kept
// holds every custody whose body did not close cleanly, and so its store and
// original body, for the life of the process.
var logCustodies struct {
	mu       sync.Mutex
	reserved int
	kept     []*logCustody
}

type logBodyKey struct{}

// logCustody is the custody record of one receipted Logs call.
type logCustody struct {
	store *receiptStore
	id    string

	mu       sync.Mutex
	sent     bool
	body     io.ReadCloser // the registered original body, nil if none arrived
	closed   bool          // the original Close returned
	closeErr error
	settled  bool
	clean    bool
}

// admitLogs reserves custody for one Logs call of attempt id in store s.
func admitLogs(s *receiptStore, id string) (*logCustody, error) {
	logCustodies.mu.Lock()
	defer logCustodies.mu.Unlock()
	for _, c := range logCustodies.kept {
		if c.store.dir == s.dir && c.id == id {
			return nil, errLogBodyHeld
		}
	}
	if logCustodies.reserved+len(logCustodies.kept) >= maxLogBodies {
		return nil, errLogBodyLimit
	}
	logCustodies.reserved++
	return &logCustody{store: s, id: id}, nil
}

func (c *logCustody) roundTrip(base http.RoundTripper, req *http.Request) (*http.Response, error) {
	c.mu.Lock()
	sent := c.sent
	c.sent = true
	c.mu.Unlock()
	if sent {
		if req.Body != nil {
			req.Body.Close()
		}
		return nil, errSentOnce
	}
	resp, err := base.RoundTrip(req)
	if resp != nil && resp.Body != nil {
		c.mu.Lock()
		c.body = resp.Body
		c.mu.Unlock()
		resp.Body = &logBody{ReadCloser: resp.Body, c: c}
	}
	return resp, err
}

// logBody is a registered log response body. Its first Close makes the only
// Close of the original body; later calls wait for it and return its result.
type logBody struct {
	io.ReadCloser
	c    *logCustody
	once sync.Once
}

func (b *logBody) Close() error {
	b.once.Do(func() {
		err := b.ReadCloser.Close()
		b.c.mu.Lock()
		b.c.closed, b.c.closeErr = true, err
		b.c.mu.Unlock()
	})
	b.c.mu.Lock()
	defer b.c.mu.Unlock()
	return b.c.closeErr
}

// read sends req, the custody's log request, and returns at most n bytes of
// its body. It closes the body and settles the custody before returning, so
// that the caller's final receipt check follows the original Close. A body
// that did not close cleanly returns errLogBodyHeld and no bytes.
func (c *logCustody) read(ctx context.Context, req *rest.Request, n int64) ([]byte, error) {
	stream, err := req.MaxRetries(0).Stream(context.WithValue(ctx, logBodyKey{}, c))
	var data []byte
	if err == nil {
		data, err = io.ReadAll(io.LimitReader(stream, n))
		cerr := stream.Close()
		if err == nil {
			err = cerr
		}
	}
	if !c.settle() {
		if err == nil || errors.Is(err, errLogBodyHeld) {
			return nil, errLogBodyHeld
		}
		return nil, errors.Join(err, errLogBodyHeld)
	}
	if err != nil {
		return nil, err
	}
	return data, nil
}

// settle ends the custody's reservation, once, and reports whether it is
// clean. A custody that is not clean, with a reference to the shared
// operation permit its caller holds, is kept for the life of the process.
// Callers defer settle as well, for paths that send no log request.
func (c *logCustody) settle() bool {
	c.mu.Lock()
	first := !c.settled
	if first {
		c.settled = true
		c.clean = c.body == nil || c.closed && c.closeErr == nil
	}
	clean := c.clean
	c.mu.Unlock()
	if !first {
		return clean
	}
	logCustodies.mu.Lock()
	defer logCustodies.mu.Unlock()
	logCustodies.reserved--
	if !clean {
		c.store.retain()
		logCustodies.kept = append(logCustodies.kept, c)
	}
	return clean
}

// admitReceiptedLogs is admitLogs for a Logs call of b, which must send
// through the receipt client.
func (b *Backend) admitReceiptedLogs(id string) (*logCustody, error) {
	if _, err := receiptHTTP(b.client.CoreV1().RESTClient()); err != nil {
		return nil, err
	}
	return admitLogs(b.receipts, id)
}

// receiptedLogs reads the task log of Pod name in custody c.
func (b *Backend) receiptedLogs(ctx context.Context, c *logCustody, name string) ([]byte, error) {
	return c.read(ctx, b.client.CoreV1().Pods(b.namespace).GetLogs(name, &corev1.PodLogOptions{Container: "task"}), backend.MaxLogBytes+1)
}
