package kube

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

// faultBody is an original response body as client-go receives it from the
// network transport. It can fail Read or Close, or block Close, and counts
// Close calls.
type faultBody struct {
	io.ReadCloser
	readErr, closeErr error
	entered, release  chan struct{} // non-nil: Close blocks until release
	closes            atomic.Int32
}

func (f *faultBody) Read(p []byte) (int, error) {
	if f.readErr != nil {
		return 0, f.readErr
	}
	return f.ReadCloser.Read(p)
}

func (f *faultBody) Close() error {
	f.closes.Add(1)
	if f.entered != nil {
		close(f.entered)
		<-f.release
	}
	f.ReadCloser.Close()
	return f.closeErr
}

// faultTransport sits below the data transport and wraps every response body
// in a faultBody. After arm, the n-th request with the given method and path
// suffix gets the armed body. Several arms may be pending at once.
type faultTransport struct {
	rt     http.RoundTripper
	mu     sync.Mutex
	calls  int // every RoundTrip call, including refused or failed ones
	bodies []*faultBody

	arms []*faultArm
}

type faultArm struct {
	method, suffix string
	n              int
	body           *faultBody
}

func (f *faultTransport) wrap(rt http.RoundTripper) http.RoundTripper { f.rt = rt; return f }

func (f *faultTransport) arm(method, suffix string, n int, body *faultBody) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.arms = append(f.arms, &faultArm{method, suffix, n, body})
}

func (f *faultTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	resp, err := f.rt.RoundTrip(r)
	if err != nil {
		return resp, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	body := &faultBody{}
	for _, a := range f.arms {
		if a.body != nil && r.Method == a.method && strings.HasSuffix(r.URL.Path, a.suffix) {
			if a.n--; a.n == 0 {
				body, a.body = a.body, nil
			}
		}
	}
	body.ReadCloser = resp.Body
	resp.Body = body
	f.bodies = append(f.bodies, body)
	return resp, nil
}

// heldFrame returns the attempt's single retained frame and its claim count.
func heldFrame(b *Backend, id string) (*dataFrame, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	frames := b.dataFrames[id]
	if len(frames) != 1 {
		return nil, b.retryOwners[id]
	}
	return frames[0], b.retryOwners[id]
}

// checkHeld verifies that the frame retains exactly the failed original
// bodies, in order, each with its actual Close error and closed once, and
// that the frame and claim are held.
func checkHeld(t *testing.T, b *Backend, id string, err error, owners int, bodies ...*faultBody) {
	t.Helper()
	if !errors.Is(err, errDataBodyHeld) {
		t.Fatalf("err = %v, want fixed held category", err)
	}
	f, n := heldFrame(b, id)
	if f == nil || !f.held || len(f.failed) != len(bodies) {
		t.Fatalf("custody = %+v", f)
	}
	for i, body := range bodies {
		if errors.Is(err, body.closeErr) {
			t.Fatalf("err = %v carries a Close error", err)
		}
		if d := f.failed[i]; d.ReadCloser != io.ReadCloser(body) || d.err != body.closeErr {
			t.Fatalf("failed[%d] = %p %v, want %p %v", i, d.ReadCloser, d.err, body, body.closeErr)
		}
		if c := body.closes.Load(); c != 1 {
			t.Fatalf("original Close calls = %d, want 1", c)
		}
	}
	if n != owners {
		t.Fatalf("claim = %d, want %d", n, owners)
	}
}

var errClose = errors.New("close failed")

func TestStartDataBodyCloseFailure(t *testing.T) {
	tests := []struct {
		name, suffix string
		n            int
		posts        int
	}{
		{"input secret", "/secrets", 1, 1},
		{"output secret", "/secrets", 2, 2},
		{"pod", "/pods", 1, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ft := &faultTransport{}
			b, api := newDataAPIWrap(t, ft.wrap)
			s := dataStart(b, qualifiedTask())
			body := &faultBody{closeErr: errClose}
			ft.arm("POST", tt.suffix, tt.n, body)
			c, err := b.StartData(context.Background(), s)
			checkHeld(t, b, s.Execution.ID, err, 1, body)
			if err != errDataBodyHeld {
				t.Fatalf("err = %v, want exactly the held category", err)
			}
			if len(api.dataLog()) != tt.posts {
				t.Fatalf("requests = %v, want %d POSTs", api.log(), tt.posts)
			}
			// The decoded Create-return UIDs stay as receipts.
			got := []backend.DataResource{c.InputSecret, c.OutputSecret, c.Pod}
			for i, r := range got {
				if (i < tt.posts) != (r.UID != "") {
					t.Fatalf("receipts = %+v", c)
				}
			}
			if tt.posts == 3 && c.Instance != c.Pod.UID {
				t.Fatalf("instance = %q", c.Instance)
			}
			if _, err := b.StartData(context.Background(), s); err == nil {
				t.Fatal("second start accepted")
			}
			if len(api.dataLog()) != tt.posts || body.closes.Load() != 1 {
				t.Fatalf("held start retried: %v", api.log())
			}
		})
	}
}

func TestObserveDataBodyCloseFailure(t *testing.T) {
	for _, n := range []int{1, 2} {
		t.Run([]string{"", "first read", "second read"}[n], func(t *testing.T) {
			ft := &faultTransport{}
			b, api, ref, r := startedDataWrap(t, ft.wrap)
			api.setPod(settledTask)
			name, _ := jobName(ref.ID)
			body := &faultBody{closeErr: errClose}
			ft.arm("GET", "/pods/"+name, n, body)
			before := len(api.log())
			o, err := b.ObserveData(context.Background(), ref)
			if o.Phase != 0 || o.TaskStop != nil || o.Collector != nil {
				t.Fatalf("observation escaped: %+v", o)
			}
			checkHeld(t, b, ref.ID, err, 1, body)
			if err != errDataBodyHeld {
				t.Fatalf("err = %v, want exactly the held category", err)
			}
			if got := len(api.log()) - before; got != n {
				t.Fatalf("GETs = %d, want %d", got, n)
			}
			after := len(api.log())
			if _, err := b.ObserveData(context.Background(), ref); !errors.Is(err, errDataBodyHeld) {
				t.Fatalf("repeat observation: %v", err)
			}
			if out, err := b.RemoveData(context.Background(), r); err == nil || out.PodRemoved || out.InputRemoved || out.OutputRemoved {
				t.Fatalf("cleanup beside held body: %+v %v", out, err)
			}
			if len(api.log()) != after || body.closes.Load() != 1 {
				t.Fatalf("held owner sent %v", api.log()[after:])
			}
		})
	}
}

func TestRemoveDataBodyCloseFailure(t *testing.T) {
	tests := []struct {
		name, method, suffix string // suffix: pod, or the Secret role
		n                    int
	}{
		{"pod metadata read", "GET", "pod", 1},
		{"pod delete", "DELETE", "pod", 1},
		// The Pod's removal completed cleanly, but the call's acknowledgment
		// still waits on every body of the call.
		{"input secret absence read", "GET", "input", 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ft := &faultTransport{}
			b, api, ref, r := startedDataWrap(t, ft.wrap)
			body := &faultBody{closeErr: errClose}
			suffix := "/secrets/" + dataSecretName(ref.ID, tt.suffix)
			if tt.suffix == "pod" {
				suffix = "/pods/" + r.Pod.Name
			}
			ft.arm(tt.method, suffix, tt.n, body)
			out, err := b.RemoveData(context.Background(), r)
			if out.PodRemoved || out.InputRemoved || out.OutputRemoved {
				t.Fatalf("bits = %+v", out)
			}
			checkHeld(t, b, ref.ID, err, -1, body)
			if err := backend.ValidateDataRemoval(r, out); err != nil {
				t.Fatal(err)
			}
			if ft.bodies[len(ft.bodies)-1] != body {
				t.Fatalf("requests continued after the held body: %v", api.log())
			}
			n := len(api.log())
			if _, err := b.RemoveData(context.Background(), r); err == nil {
				t.Fatal("second removal accepted")
			}
			if len(api.log()) != n || body.closes.Load() != 1 {
				t.Fatal("held removal retried")
			}
		})
	}
}

// A Retry-After response is retried by client-go after it closes the body.
// If that Close fails, no further request is sent.
func TestDataRetryableStatusBodyClose(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "closed", true: "close failure"}[fail], func(t *testing.T) {
			ft := &faultTransport{}
			b, api, ref, _ := startedDataWrap(t, ft.wrap)
			api.setPod(runningTask)
			name, _ := jobName(ref.ID)
			api.mu.Lock()
			api.retryGet = api.gets + 1
			api.mu.Unlock()
			body := &faultBody{}
			if fail {
				body.closeErr = errClose
			}
			ft.arm("GET", "/pods/"+name, 1, body)
			before := len(api.log())
			o, err := b.ObserveData(context.Background(), ref)
			gets := len(api.log()) - before
			if !fail {
				if err != nil || o.Phase != backend.DataTask || gets != 2 {
					t.Fatalf("o=%+v err=%v gets=%d", o, err, gets)
				}
				if f, n := heldFrame(b, ref.ID); f != nil || n != 0 {
					t.Fatalf("frame=%+v claim=%d", f, n)
				}
				return
			}
			if gets != 1 {
				t.Fatalf("GETs = %d after uncertain close, want 1", gets)
			}
			// The configured wrappers, like authentication, sit inside the data
			// transport and never see the refused retry.
			if ft.calls != len(api.log()) {
				t.Fatalf("wrapper calls = %d, requests = %d", ft.calls, len(api.log()))
			}
			checkHeld(t, b, ref.ID, err, 1, body)
		})
	}
}

// A blocked original Close keeps the operation pending and owned until that
// same Close returns.
func TestDataBlockedBodyClose(t *testing.T) {
	for _, closeErr := range []error{nil, errClose} {
		t.Run(map[bool]string{true: "returns", false: "fails"}[closeErr == nil], func(t *testing.T) {
			ft := &faultTransport{}
			b, api, ref, r := startedDataWrap(t, ft.wrap)
			api.setPod(runningTask)
			name, _ := jobName(ref.ID)
			body := &faultBody{closeErr: closeErr, entered: make(chan struct{}), release: make(chan struct{})}
			ft.arm("GET", "/pods/"+name, 1, body)
			type result struct {
				o   backend.DataObservation
				err error
			}
			done := make(chan result)
			go func() {
				o, err := b.ObserveData(context.Background(), ref)
				done <- result{o, err}
			}()
			<-body.entered
			select {
			case res := <-done:
				t.Fatalf("returned during blocked Close: %+v", res)
			default:
			}
			if _, n := heldFrame(b, ref.ID); n != 1 {
				t.Fatalf("claim = %d during blocked Close", n)
			}
			if _, err := b.RemoveData(context.Background(), r); err == nil {
				t.Fatal("cleanup beside a blocked Close")
			}
			close(body.release)
			res := <-done
			if closeErr == nil {
				if res.err != nil || res.o.Phase != backend.DataTask {
					t.Fatalf("%+v", res)
				}
				if f, n := heldFrame(b, ref.ID); f != nil || n != 0 || body.closes.Load() != 1 {
					t.Fatalf("frame=%+v claim=%d", f, n)
				}
				return
			}
			if res.o.Phase != 0 {
				t.Fatalf("observation escaped: %+v", res.o)
			}
			checkHeld(t, b, ref.ID, res.err, 1, body)
		})
	}
}

func TestDataBodyReadAndCloseFailure(t *testing.T) {
	ft := &faultTransport{}
	b, api, ref, _ := startedDataWrap(t, ft.wrap)
	api.setPod(runningTask)
	name, _ := jobName(ref.ID)
	readErr := errors.New("read failed")
	body := &faultBody{readErr: readErr, closeErr: errClose}
	ft.arm("GET", "/pods/"+name, 1, body)
	_, err := b.ObserveData(context.Background(), ref)
	if !errors.Is(err, readErr) {
		t.Fatalf("read failure lost: %v", err)
	}
	checkHeld(t, b, ref.ID, err, 1, body)
}

// Logs holds its log stream open while it makes its second metadata GET.
// Every original body whose Close fails stays in the same frame with its
// exact error, whichever of the two fails.
func TestLogsDataDualCloseFailure(t *testing.T) {
	tests := []struct {
		name          string
		meta, logBody bool // whether each body's Close fails
	}{
		{"metadata and log", true, true},
		{"log only", false, true},
		{"metadata only", true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ft := &faultTransport{}
			b, api, ref, r := startedDataWrap(t, ft.wrap)
			api.setPod(runningTask)
			name, _ := jobName(ref.ID)
			meta := &faultBody{}
			if tt.meta {
				meta.closeErr = errors.New("metadata close failed")
			}
			logBody := &faultBody{}
			if tt.logBody {
				logBody.closeErr = errors.New("log close failed")
			}
			ft.arm("GET", "/pods/"+name, 2, meta)
			ft.arm("GET", "/pods/"+name+"/log", 1, logBody)
			before := len(api.log())
			chunk, err := b.Logs(context.Background(), ref, 0, 64)
			if chunk.Data != nil || chunk.NextOffset != 0 {
				t.Fatalf("chunk escaped: %+v", chunk)
			}
			if got := api.log()[before:]; len(got) != 3 {
				t.Fatalf("requests = %v, want GET, log, GET", got)
			}
			var failed []*faultBody
			if tt.meta {
				failed = append(failed, meta)
			}
			if tt.logBody {
				failed = append(failed, logBody)
			}
			checkHeld(t, b, ref.ID, err, 1, failed...)
			if meta.closes.Load() != 1 || logBody.closes.Load() != 1 {
				t.Fatalf("closes: metadata %d, log %d", meta.closes.Load(), logBody.closes.Load())
			}
			n := len(api.log())
			if _, err := b.Logs(context.Background(), ref, 0, 64); !errors.Is(err, errDataBodyHeld) {
				t.Fatalf("repeat logs: %v", err)
			}
			if _, err := b.ObserveData(context.Background(), ref); !errors.Is(err, errDataBodyHeld) {
				t.Fatalf("observe: %v", err)
			}
			if out, err := b.RemoveData(context.Background(), r); err == nil || out.PodRemoved || out.InputRemoved || out.OutputRemoved {
				t.Fatalf("cleanup beside held bodies: %+v %v", out, err)
			}
			if len(api.log()) != n || meta.closes.Load() != 1 || logBody.closes.Load() != 1 {
				t.Fatalf("held owner sent %v", api.log()[n:])
			}
			checkHeld(t, b, ref.ID, err, 1, failed...)
		})
	}
}

// After a failed Close, no new operation on the attempt begins, even before
// the failing operation ends.
func TestDataFrameAdmissionAfterFailure(t *testing.T) {
	b := &Backend{}
	_, f, err := b.beginData(context.Background(), "a", false, false)
	if err != nil {
		t.Fatal(err)
	}
	d, err := f.register(&faultBody{ReadCloser: io.NopCloser(strings.NewReader("")), closeErr: errClose})
	if err != nil {
		t.Fatal(err)
	}
	d.Close()
	if _, _, err := b.beginData(context.Background(), "a", false, false); err != errDataBodyHeld {
		t.Fatalf("sibling admitted: %v", err)
	}
	if b.endData("a", f) || b.retryOwners["a"] != 1 {
		t.Fatalf("frame released: owners=%v", b.retryOwners)
	}
}

// Repeated wrapper Close never closes the original again and never turns a
// failure into success.
func TestDataBodyCloseOnce(t *testing.T) {
	f := &dataFrame{}
	orig := &faultBody{ReadCloser: io.NopCloser(strings.NewReader("")), closeErr: errClose}
	d, err := f.register(orig)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := d.Close(); err != errClose {
			t.Fatalf("Close = %v", err)
		}
	}
	if orig.closes.Load() != 1 || len(f.failed) != 1 || f.failed[0] != d || f.end() {
		t.Fatalf("closes=%d failed=%v", orig.closes.Load(), f.failed)
	}
	if _, err := f.register(io.NopCloser(strings.NewReader(""))); err != errDataBodyHeld {
		t.Fatalf("register after failure: %v", err)
	}
}

// Ordinary operations close every original body exactly once and release
// their frames and claims.
func TestDataBodyOrdinaryClose(t *testing.T) {
	ft := &faultTransport{}
	b, api, ref, r := startedDataWrap(t, ft.wrap)
	api.setPod(settledTask)
	if _, err := b.ObserveData(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Logs(context.Background(), ref, 0, 64); err != nil {
		t.Fatal(err)
	}
	out, err := b.RemoveData(context.Background(), r)
	if err != nil || !out.PodRemoved || !out.InputRemoved || !out.OutputRemoved {
		t.Fatalf("%+v %v", out, err)
	}
	if len(ft.bodies) != len(api.log()) {
		t.Fatalf("bodies = %d, requests = %d", len(ft.bodies), len(api.log()))
	}
	for i, body := range ft.bodies {
		if c := body.closes.Load(); c != 1 {
			t.Fatalf("body %d (%s) closed %d times", i, api.log()[i], c)
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.dataFrames) != 0 || len(b.retryOwners) != 0 {
		t.Fatalf("frames=%v owners=%v", b.dataFrames, b.retryOwners)
	}
}

// Untagged requests are not tracked, and targets without a data profile do
// not install the data transport at all.
func TestDataTransportUntagged(t *testing.T) {
	ft := &faultTransport{}
	b, api, ref, _ := startedDataWrap(t, ft.wrap)
	api.setPod(runningTask)
	name, _ := jobName(ref.ID)
	body := &faultBody{closeErr: errClose}
	ft.arm("GET", "/pods/"+name, 1, body)
	if _, err := b.client.CoreV1().Pods(b.namespace).Get(context.Background(), name, metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	frames := len(b.dataFrames)
	b.mu.Unlock()
	if frames != 0 || body.closes.Load() != 1 {
		t.Fatalf("untagged request tracked: frames=%d", frames)
	}

	server := httptest.NewServer(http.HandlerFunc(api.serve))
	t.Cleanup(server.Close)
	legacy := &faultTransport{}
	target, err := fromREST(TargetConfig{Name: "legacy", Namespace: "workloads"}, "owner", &rest.Config{Host: server.URL, WrapTransport: legacy.wrap})
	if err != nil {
		t.Fatal(err)
	}
	lb := target.Backend.(*Backend)
	if _, ok := lb.client.CoreV1().RESTClient().(*rest.RESTClient).Client.Transport.(*dataTransport); ok {
		t.Fatal("legacy target installed the data transport")
	}
	if _, ok := b.client.CoreV1().RESTClient().(*rest.RESTClient).Client.Transport.(*dataTransport); !ok {
		t.Fatal("data transport is not outermost")
	}
	legacy.arm("GET", "/pods/"+name, 1, &faultBody{closeErr: errClose})
	f := &dataFrame{}
	ctx := context.WithValue(context.Background(), dataFrameKey{}, f)
	if _, err := lb.client.CoreV1().Pods(lb.namespace).Get(ctx, name, metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
	if len(legacy.bodies) != 1 || len(f.open) != 0 || len(f.failed) != 0 {
		t.Fatalf("legacy target tracked a body: %+v", f)
	}
}
