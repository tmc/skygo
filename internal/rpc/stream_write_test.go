package rpc_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"google.golang.org/protobuf/proto"
)

// smallWriteListener limits the real TCP send buffer. The test still calls the
// operating system's Write: it does not replace a blocked write with a channel.
type smallWriteListener struct{ net.Listener }

func (l smallWriteListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if err := c.(*net.TCPConn).SetWriteBuffer(1024); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

type observedWrite struct{ done chan error }
type observedWriter struct {
	http.ResponseWriter
	writes chan<- observedWrite
}

func (w observedWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w observedWriter) Flush()                      { _ = http.NewResponseController(w.ResponseWriter).Flush() }
func (w observedWriter) Write(p []byte) (int, error) {
	if len(p) < 32<<10 {
		return w.ResponseWriter.Write(p)
	}
	call := observedWrite{done: make(chan error, 1)}
	w.writes <- call
	n, err := w.ResponseWriter.Write(p)
	call.done <- err
	return n, err
}

func TestRevocationAbortsBlockedNetworkWrite(t *testing.T) {
	f := scoped(t, &logBackend{read: func(_ context.Context, offset int64, size int) (backend.LogChunk, error) {
		n := min(int64(size), backend.MaxLogBytes-offset)
		return backend.LogChunk{Data: bytes.Repeat([]byte("x"), int(n)), NextOffset: offset + n, Complete: offset+n == backend.MaxLogBytes}, nil
	}})
	id := f.running(t, "alice")
	writes := make(chan observedWrite, 32)
	handlerDone := make(chan struct{})
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		f.handler.ServeHTTP(observedWriter{w, writes}, r)
	}))
	srv.Listener = smallWriteListener{srv.Listener}
	srv.Start()
	t.Cleanup(srv.Close)
	conn, err := net.DialTCP("tcp", nil, srv.Listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetReadBuffer(1024); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetDeadline(time.Now().Add(8 * time.Second)); err != nil {
		t.Fatal(err)
	}
	payload, err := proto.Marshal(&v1.StreamJobLogsRequest{Id: id, Follow: true})
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 5+len(payload))
	binary.BigEndian.PutUint32(body[1:5], uint32(len(payload)))
	copy(body[5:], payload)
	// Read just HTTP headers, then stop consuming the response body.
	_, err = fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer %s\r\nContent-Type: application/connect+proto\r\nConnect-Protocol-Version: 1\r\nContent-Length: %d\r\n\r\n", skyv1connect.ControlServiceStreamJobLogsProcedure, f.tokens["alice"], len(body))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(body); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("stream status %d", response.StatusCode)
	}
	var blocked observedWrite
	blockedAt := time.Now()
	for blocked.done == nil {
		var call observedWrite
		select {
		case call = <-writes:
		case <-time.After(2 * time.Second):
			t.Fatal("no network write observed")
		}
		select {
		case err := <-call.done:
			if err != nil {
				t.Fatal("write failed before revocation", err)
			}
		case <-time.After(100 * time.Millisecond):
			blocked = call
			blockedAt = time.Now()
		}
	}
	if err := auth.Revoke(f.path, f.ids["alice"]); err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(2 * time.Second)
	deadline := time.NewTimer(time.Until(expires))
	defer deadline.Stop()
	for {
		select {
		case err := <-blocked.done:
			if err != nil {
				netErr, ok := err.(net.Error)
				if !ok || !netErr.Timeout() {
					t.Fatalf("blocked write error = %v; want actual network timeout", err)
				}
				goto aborted
			}
			// TCP may reopen its window before the next auth refresh. Observe
			// the next real write rather than claiming cancellation of this one.
			select {
			case blocked = <-writes:
			case <-deadline.C:
				t.Fatal("no blocked network write was aborted")
			}
		case <-deadline.C:
			t.Fatal("revocation did not abort blocked network write")
		}
	}
aborted:
	select {
	case <-handlerDone:
	case <-deadline.C:
		t.Fatal("handler retained aborted stream past revocation deadline")
	}
	if time.Now().After(expires) {
		t.Fatal("write abort and handler exit exceeded two seconds")
	}
	t.Logf("actual TCP write blocked for >=100ms; revocation aborted it after %s", time.Since(blockedAt).Round(time.Millisecond))
}
