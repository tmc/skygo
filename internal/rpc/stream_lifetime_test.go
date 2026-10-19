package rpc_test

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"google.golang.org/protobuf/proto"
)

// paddedTrailer adds bounded metadata to Connect's real terminal envelope. The
// fault makes the trailer large enough to saturate the actual TCP send buffer;
// it never substitutes a channel wait or a fabricated network write error.
type paddedTrailer struct {
	http.ResponseWriter
	terminalSize uint32
	terminal     bool
	entered      chan<- struct{}
	wrote        chan<- error
}

func (w *paddedTrailer) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *paddedTrailer) Flush()                      { _ = http.NewResponseController(w.ResponseWriter).Flush() }
func (w *paddedTrailer) Write(p []byte) (int, error) {
	if len(p) == 5 && p[0] == 2 {
		w.terminal = true
		w.terminalSize = binary.BigEndian.Uint32(p[1:])
		return len(p), nil
	}
	if !w.terminal {
		return w.ResponseWriter.Write(p)
	}
	w.terminal = false
	if uint32(len(p)) != w.terminalSize || len(p) > 64<<10 {
		return 0, errors.New("invalid test terminal envelope")
	}
	var trailer map[string]json.RawMessage
	if err := json.Unmarshal(p, &trailer); err != nil {
		return 0, err
	}
	var metadata map[string][]string
	if data := trailer["metadata"]; len(data) > 0 {
		if err := json.Unmarshal(data, &metadata); err != nil {
			return 0, err
		}
	}
	if metadata == nil {
		metadata = make(map[string][]string)
	}
	metadata["Test-Padding"] = []string{strings.Repeat("x", 512<<10)}
	data, err := json.Marshal(metadata)
	if err != nil {
		return 0, err
	}
	trailer["metadata"] = data
	data, err = json.Marshal(trailer)
	if err != nil {
		return 0, err
	}
	header := make([]byte, 5)
	header[0] = 2
	binary.BigEndian.PutUint32(header[1:], uint32(len(data)))
	if _, err := w.ResponseWriter.Write(header); err != nil {
		return 0, err
	}
	w.entered <- struct{}{}
	_, err = w.ResponseWriter.Write(data)
	w.wrote <- err
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func TestRevocationAbortsTerminalNetworkWrite(t *testing.T) {
	f := scoped(t, &logBackend{read: func(context.Context, int64, int) (backend.LogChunk, error) {
		return backend.LogChunk{Complete: true}, nil
	}})
	id := f.running(t, "alice")
	entered, wrote, done := make(chan struct{}, 1), make(chan error, 1), make(chan struct{})
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		f.handler.ServeHTTP(&paddedTrailer{ResponseWriter: w, entered: entered, wrote: wrote}, r)
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
	binary.BigEndian.PutUint32(body[1:], uint32(len(payload)))
	copy(body[5:], payload)
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
	waitEvent(t, entered)
	select {
	case err := <-wrote:
		t.Fatalf("terminal TCP write did not block: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := auth.Revoke(f.path, f.ids["alice"]); err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(2 * time.Second)
	deadline := time.NewTimer(time.Until(expires))
	defer deadline.Stop()
	select {
	case err := <-wrote:
		netErr, ok := err.(net.Error)
		if !ok || !netErr.Timeout() {
			t.Fatalf("terminal write error = %v; want actual network timeout", err)
		}
	case <-deadline.C:
		t.Fatal("terminal TCP write survived revocation deadline")
	}
	select {
	case <-done:
	case <-deadline.C:
		t.Fatal("outer handler survived revocation deadline")
	}
	if time.Now().After(expires) {
		t.Fatal("terminal write and outer handler exceeded two seconds")
	}
	t.Logf("injected terminal metadata: actual TCP timeout and outer handler exit in %s", (2*time.Second - time.Until(expires)).Round(time.Millisecond))
}

// terminalBarrier is a deterministic completion-order fixture. Unlike the TCP
// test above, its blocked Write is simulated; this test qualifies permit
// ownership, not the network deadline implementation.
type terminalBarrier struct {
	http.ResponseWriter
	entered chan<- struct{}
	release <-chan struct{}
	abort   chan struct{}
	once    sync.Once
}

func (w *terminalBarrier) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *terminalBarrier) Flush()                      { _ = http.NewResponseController(w.ResponseWriter).Flush() }
func (w *terminalBarrier) SetWriteDeadline(deadline time.Time) error {
	if !deadline.IsZero() {
		w.once.Do(func() { close(w.abort) })
	}
	return http.NewResponseController(w.ResponseWriter).SetWriteDeadline(deadline)
}
func (w *terminalBarrier) Write(p []byte) (int, error) {
	if len(p) == 5 && p[0]&2 != 0 {
		w.entered <- struct{}{}
		select {
		case <-w.release:
		case <-w.abort:
			return 0, os.ErrDeadlineExceeded
		}
	}
	return w.ResponseWriter.Write(p)
}

func TestStreamPermitsIncludeTerminalTrailers(t *testing.T) {
	var calls atomic.Int32
	f := scoped(t, &logBackend{read: func(context.Context, int64, int) (backend.LogChunk, error) {
		calls.Add(1)
		return backend.LogChunk{Complete: true}, nil
	}})
	id := f.running(t, "alice")
	entered, outer := make(chan struct{}, 32), make(chan struct{}, 32)
	release := make(chan struct{})
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { outer <- struct{}{} }()
		f.handler.ServeHTTP(&terminalBarrier{ResponseWriter: w, entered: entered, release: release, abort: make(chan struct{})}, r)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	client := skyv1connect.NewControlServiceClient(srv.Client(), srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	finished := make(chan error, 32)
	for range 32 {
		go func() {
			stream, err := client.StreamJobLogs(ctx, authorized(f.tokens["alice"], &v1.StreamJobLogsRequest{Id: id, Follow: true}))
			if err == nil {
				for stream.Receive() {
				}
				err = stream.Err()
				stream.Close()
			}
			finished <- err
		}()
	}
	for range 32 {
		waitEvent(t, entered)
	}
	// The original server shares f.handler but has no barrier, so a rejection
	// trailer can arrive. An accidental 33rd admission reaches the backend.
	code(t, f.receive(ctx, "admin", id), connect.CodeResourceExhausted)
	if calls.Load() != 32 {
		t.Fatalf("backend calls = %d, want 32", calls.Load())
	}
	if err := auth.Revoke(f.path, f.ids["alice"]); err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(2 * time.Second)
	deadline := time.NewTimer(time.Until(expires))
	defer deadline.Stop()
	for range 32 {
		select {
		case err := <-finished:
			if err == nil {
				t.Fatal("blocked terminal stream succeeded")
			}
		case <-deadline.C:
			t.Fatal("client outlived revocation bound")
		}
	}
	for range 32 {
		select {
		case <-outer:
		case <-deadline.C:
			t.Fatal("outer handler retained terminal permit")
		}
	}
	if time.Now().After(expires) {
		t.Fatal("terminal completions exceeded two seconds")
	}
	once.Do(func() { close(release) })
	if err := f.receive(ctx, "admin", id); err != nil {
		t.Fatal("finished streams did not release permits", err)
	}
	if calls.Load() != 33 {
		t.Fatalf("recovered backend calls = %d, want 33", calls.Load())
	}
}

// A canceled stream may send its typed trailer and leave the HTTP/1 connection
// reusable. Its abort callback must neither leave nor later reapply a deadline
// to the next request on that connection.
func TestStreamDeadlineDoesNotReachNextRequest(t *testing.T) {
	f := scoped(t, &logBackend{read: func(ctx context.Context, offset int64, _ int) (backend.LogChunk, error) {
		if offset == 0 {
			return backend.LogChunk{Data: []byte("first"), NextOffset: 5}, nil
		}
		<-ctx.Done()
		return backend.LogChunk{}, ctx.Err()
	}})
	id := f.running(t, "alice")
	srv := httptest.NewServer(f.handler)
	t.Cleanup(srv.Close)
	transport := &http.Transport{MaxConnsPerHost: 1}
	t.Cleanup(transport.CloseIdleConnections)
	client := skyv1connect.NewControlServiceClient(&http.Client{Transport: transport}, srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.StreamJobLogs(ctx, authorized(f.tokens["alice"], &v1.StreamJobLogsRequest{Id: id, Follow: true}))
	if err != nil {
		t.Fatal(err)
	}
	if !stream.Receive() {
		t.Fatal("first chunk missing", stream.Err())
	}
	if err := auth.Revoke(f.path, f.ids["alice"]); err != nil {
		t.Fatal(err)
	}
	for stream.Receive() {
	}
	code(t, stream.Err(), connect.CodeUnauthenticated)
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	// Wait beyond the abort allowance before proving this same connection works.
	time.Sleep(150 * time.Millisecond)
	var reused bool
	next := httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused }})
	if _, err := client.GetJob(next, authorized(f.tokens["admin"], &v1.GetJobRequest{Id: id})); err != nil {
		t.Fatal(err)
	}
	if !reused {
		t.Fatal("transport did not exercise the next request on the same connection")
	}
}
