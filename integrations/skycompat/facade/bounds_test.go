package facade

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCanonicalDeadlineAndAdmission(t *testing.T) {
	f := newFixture(t)
	endpoint, err := url.Parse(f.endpoint)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(endpoint)
	var observed atomic.Int32
	proxy.ModifyResponse = func(resp *http.Response) error {
		observed.Add(1)
		<-resp.Request.Context().Done()
		return resp.Request.Context().Err()
	}
	// Canonical authentication completes in the real process. The proxy then
	// withholds its response, exercising the facade's transport rather than a
	// mock-generated client or a fabricated authenticated identity.
	stalled := httptest.NewServer(proxy)
	t.Cleanup(stalled.Close)
	raw, err := NewHandler(Options{Endpoint: stalled.URL})
	if err != nil {
		t.Fatal(err)
	}
	h := raw.(*handler)
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	results := make(chan error, 32)
	start := time.Now()
	for range 32 {
		go func() {
			req, _ := http.NewRequest("GET", server.URL+"/api/health", nil)
			req.Header.Set("Authorization", "Bearer "+f.alice)
			resp, err := server.Client().Do(req)
			if err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 503 {
					err = &statusError{resp.StatusCode}
				}
			}
			results <- err
		}()
	}
	eventually(t, time.Second, func() bool { return observed.Load() == 32 })
	if len(h.admitted) != 32 {
		t.Fatalf("in-flight permits %d", len(h.admitted))
	}
	req, _ := http.NewRequest("GET", server.URL+"/api/health", nil)
	req.Header.Set("Authorization", "Bearer "+f.bob)
	rejectedAt := time.Now()
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 503 || time.Since(rejectedAt) > 500*time.Millisecond {
		t.Fatal("33rd operation was not promptly refused")
	}
	for range 32 {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}
	if elapsed := time.Since(start); elapsed < 1500*time.Millisecond || elapsed > 4*time.Second {
		t.Fatalf("canonical timeout envelope %s", elapsed)
	}
	eventually(t, time.Second, func() bool { return len(h.admitted) == 0 })
}

type statusError struct{ code int }

func (e *statusError) Error() string { return "unexpected HTTP status" }

type flushWriter struct {
	header   http.Header
	status   int
	deadline time.Time
	entered  chan struct{}
	release  <-chan struct{}
}

func (w *flushWriter) Header() http.Header                       { return w.header }
func (w *flushWriter) WriteHeader(status int)                    { w.status = status }
func (w *flushWriter) Write(p []byte) (int, error)               { return len(p), nil }
func (w *flushWriter) SetReadDeadline(time.Time) error           { return nil }
func (w *flushWriter) SetWriteDeadline(deadline time.Time) error { w.deadline = deadline; return nil }
func (w *flushWriter) FlushError() error {
	w.entered <- struct{}{}
	timer := time.NewTimer(time.Until(w.deadline))
	defer timer.Stop()
	select {
	case <-w.release:
		return nil
	case <-timer.C:
		return os.ErrDeadlineExceeded
	}
}

func TestPermitHeldThroughFlush(t *testing.T) {
	f := newFixture(t)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	completed := make(chan struct{}, 32)
	writers := make([]*flushWriter, 32)
	for i := range writers {
		w := &flushWriter{header: make(http.Header), entered: make(chan struct{}, 1), release: release}
		writers[i] = w
		req := httptest.NewRequest("GET", "http://facade/api/health", nil)
		req.Header.Set("Authorization", "Bearer "+f.alice)
		go func() { f.h.ServeHTTP(w, req); completed <- struct{}{} }()
	}
	for _, w := range writers {
		select {
		case <-w.entered:
			if w.status != 200 {
				t.Errorf("canonical health status %d", w.status)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("response did not reach flush")
		}
	}
	if len(f.h.admitted) != 32 {
		t.Fatalf("permits released before response flush: %d", len(f.h.admitted))
	}
	status, _, _ := f.request(t, "GET", "/api/health", f.bob, "", nil)
	if status != 503 {
		t.Fatalf("33rd operation status %d", status)
	}
	unblock()
	for range writers {
		select {
		case <-completed:
		case <-time.After(time.Second):
			t.Fatal("flush completion retained a permit")
		}
	}
	if len(f.h.admitted) != 0 {
		t.Fatal("response permits not released")
	}
	// This controlled writer models a blocked frontend flush and enforces the
	// response controller's actual deadline. It is not TCP backpressure evidence;
	// separate real TCP tests cover incomplete-body and disconnect behavior.
	timed := &flushWriter{header: make(http.Header), entered: make(chan struct{}, 1)}
	req := httptest.NewRequest("GET", "http://facade/api/health", nil)
	req.Header.Set("Authorization", "Bearer "+f.alice)
	done := make(chan struct{})
	start := time.Now()
	go func() { f.h.ServeHTTP(timed, req); close(done) }()
	select {
	case <-timed.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("timed flush not entered")
	}
	if len(f.h.admitted) != 1 {
		t.Fatal("timed flush lost its permit")
	}
	select {
	case <-done:
	case <-time.After(6 * time.Second):
		t.Fatal("flush deadline did not finish operation")
	}
	if elapsed := time.Since(start); elapsed < 4*time.Second || elapsed > 6500*time.Millisecond {
		t.Fatalf("flush timeout %s", elapsed)
	}
	if len(f.h.admitted) != 0 {
		t.Fatal("timed flush retained permit")
	}
}
