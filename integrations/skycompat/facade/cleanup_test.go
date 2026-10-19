package facade

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// cleanupWriter and cleanupBody expose the flush/close boundaries without
// opening a connection or asking a peer to withhold request bytes.
type cleanupWriter struct {
	*httptest.ResponseRecorder
	read, write time.Time
	flushing    chan struct{}
	release     <-chan struct{}
}

func (w *cleanupWriter) SetReadDeadline(t time.Time) error  { w.read = t; return nil }
func (w *cleanupWriter) SetWriteDeadline(t time.Time) error { w.write = t; return nil }
func (w *cleanupWriter) FlushError() error {
	w.ResponseRecorder.Flush()
	close(w.flushing)
	timer := time.NewTimer(time.Until(w.write))
	defer timer.Stop()
	select {
	case <-w.release:
		return nil
	case <-timer.C:
		return os.ErrDeadlineExceeded
	}
}

type cleanupBody struct {
	closing chan struct{}
	release <-chan struct{}
	calls   int
}

func (b *cleanupBody) Read([]byte) (int, error) { return 0, io.EOF }
func (b *cleanupBody) Close() error {
	b.calls++
	close(b.closing)
	<-b.release
	return nil
}

func TestBodyCloseBeforeDeadlineReset(t *testing.T) {
	for _, tc := range []struct {
		name, path string
		status     int
		permits    int
	}{
		{"unselected route", "/missing", http.StatusNotFound, 0},
		{"admitted rejection", "/validate", http.StatusBadRequest, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := &handler{admitted: make(chan struct{}, 32)}
			flushRelease, bodyRelease := make(chan struct{}), make(chan struct{})
			var flushOnce, bodyOnce sync.Once
			unflush := func() { flushOnce.Do(func() { close(flushRelease) }) }
			unclose := func() { bodyOnce.Do(func() { close(bodyRelease) }) }
			w := &cleanupWriter{ResponseRecorder: httptest.NewRecorder(), flushing: make(chan struct{}), release: flushRelease}
			body := &cleanupBody{closing: make(chan struct{}), release: bodyRelease}
			r := httptest.NewRequest(http.MethodPost, "http://facade"+tc.path, nil)
			r.Body = body
			done := make(chan struct{})
			go func() { h.ServeHTTP(w, r); close(done) }()
			t.Cleanup(func() {
				unflush()
				unclose()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("controlled cleanup did not finish")
				}
			})
			select {
			case <-w.flushing:
			case <-time.After(time.Second):
				t.Fatal("response did not reach flush")
			}
			if w.Code != tc.status || len(h.admitted) != tc.permits || w.read.IsZero() || w.write.IsZero() {
				t.Fatal("flush lost response, deadlines or admission state")
			}
			deadline := w.read
			if remaining := time.Until(deadline); remaining <= 0 || remaining > operationTimeout {
				t.Fatal("original five-second deadline was not installed")
			}
			unflush()
			select {
			case <-body.closing:
			case <-done:
				t.Fatal("handler returned without closing its request body")
			case <-time.After(time.Second):
				t.Fatal("body close was not reached")
			}
			if w.read != deadline || w.write != deadline || len(h.admitted) != tc.permits {
				t.Fatal("deadline or permit released before body close completed")
			}
			select {
			case <-done:
				t.Fatal("handler returned while body close was active")
			default:
			}
			unclose()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("completed cleanup retained the handler")
			}
			if body.calls != 1 || !w.read.IsZero() || !w.write.IsZero() || len(h.admitted) != 0 {
				t.Fatal("normal cleanup did not close once, reset deadlines and release its permit")
			}
		})
	}
}

func TestCompletedBodyConnectionReuse(t *testing.T) {
	// Complete ordinary requests exercise actual net/http cleanup and reuse.
	// Both routes reject before contacting the canonical service.
	h := &handler{admitted: make(chan struct{}, 32)}
	server := httptest.NewServer(h)
	defer server.Close()
	client := server.Client()
	client.Timeout = 2 * time.Second
	for i, path := range []string{"/missing", "/validate", "/missing"} {
		var reused bool
		r, err := http.NewRequest(http.MethodPost, server.URL+path, strings.NewReader("ordinary complete request body"))
		if err != nil {
			t.Fatal(err)
		}
		r = r.WithContext(httptrace.WithClientTrace(r.Context(), &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused }}))
		resp, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		_, readErr := io.Copy(io.Discard, resp.Body)
		closeErr := resp.Body.Close()
		want := http.StatusNotFound
		if path == "/validate" {
			want = http.StatusBadRequest
		}
		if readErr != nil || closeErr != nil || resp.StatusCode != want {
			t.Fatal("ordinary response did not complete")
		}
		if i > 0 && !reused {
			t.Fatal("completed request connection was not reused")
		}
	}
}
