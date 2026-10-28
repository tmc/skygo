package kube

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
)

// closeModel wraps, in a rest.Config WrapTransport, the actual response
// body of every log request whose path contains match. Its Close is MODELED:
// it reports entry, waits for hold, then, by mode, closes the original body
// and returns the result ("pass"), closes it and returns an error ("error"),
// or returns an error without closing it ("unknown"), as a Close that was
// interrupted with an unknown outcome. It is not a Kubernetes behavior.
type closeModel struct {
	match, mode   string
	hold, entered *gate

	mu     sync.Mutex
	calls  int // modeled Close calls
	closes int // Close calls of the original body
}

func newCloseModel(t *testing.T, match, mode string) *closeModel {
	m := &closeModel{match: match, mode: mode, hold: newGate(), entered: newGate()}
	t.Cleanup(m.hold.release)
	return m
}

func (m *closeModel) wrap(rt http.RoundTripper) http.RoundTripper {
	return roundTripFunc(func(req *http.Request) (*http.Response, error) {
		resp, err := rt.RoundTrip(req)
		if resp != nil && resp.Body != nil && strings.HasSuffix(req.URL.Path, "/log") && strings.Contains(req.URL.Path, m.match) {
			resp.Body = &modeledBody{ReadCloser: resp.Body, m: m}
		}
		return resp, err
	})
}

func (m *closeModel) counts() (calls, closes int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls, m.closes
}

// modeledBody is a log response body with a MODELED Close.
type modeledBody struct {
	io.ReadCloser
	m *closeModel
}

func (b *modeledBody) Close() error {
	b.m.mu.Lock()
	b.m.calls++
	b.m.mu.Unlock()
	b.m.entered.release()
	<-b.m.hold.ch
	if b.m.mode == "unknown" {
		return errors.New("modeled close interrupted, outcome unknown")
	}
	b.m.mu.Lock()
	b.m.closes++
	b.m.mu.Unlock()
	err := b.ReadCloser.Close()
	if b.m.mode == "error" {
		return errors.Join(errors.New("modeled close error"), err)
	}
	return err
}

// keptCustody reports whether this process keeps a log custody of attempt
// id in the receipt directory of s.
func keptCustody(s *receiptStore, id string) bool {
	logCustodies.mu.Lock()
	defer logCustodies.mu.Unlock()
	return slices.ContainsFunc(logCustodies.kept, func(c *logCustody) bool { return c.store.dir == s.dir && c.id == id })
}

// TestReceiptHeldLogCloseAfterRefusal runs ordinary Job Logs and retry Pod
// Logs of a later Backend, which recovers and pins the stored object, while
// the original Start's response is held. Logs reads the whole log body and
// reaches the original body's Close, which is held. The original Start then
// receives a complete 200 acknowledgment and durably refuses the attempt.
// While Close is held, the reader keeps its operation permit: cleanup is
// busy, with no request. Once Close returns cleanly, the final receipt check
// releases no byte. The twin acknowledges 201: the same held Close is
// followed by the exact log. Either way the original Close is called once.
func TestReceiptHeldLogCloseAfterRefusal(t *testing.T) {
	podName := "skygo-" + testID + "-pod"
	jobLog := "GET " + podsPath + "/" + podName + "/log"
	podLog := parentGet("Pod") + "/log"
	list := "GET " + podsPath
	for _, tt := range []struct {
		kind       string
		notCreated bool
		want       []string
	}{
		{"Job", true, []string{parentGet("Job"), list, parentGet("Job"), list, jobLog}},
		{"Job", false, []string{parentGet("Job"), list, parentGet("Job"), list, jobLog}},
		{"Pod", true, []string{parentGet("Pod"), parentGet("Pod"), podLog}},
		{"Pod", false, []string{parentGet("Pod"), parentGet("Pod"), podLog, parentGet("Pod")}},
	} {
		t.Run(fmt.Sprintf("%s/not-created=%v", tt.kind, tt.notCreated), func(t *testing.T) {
			ctx := context.Background()
			h := startHeld(t, tt.kind, tt.notCreated)
			// Registered after the fixture, so that its release runs
			// before the fixture joins the held Logs.
			m := newCloseModel(t, testID, "pass")
			if tt.kind == "Job" {
				h.api.addJobPod(h.api.job("skygo-"+testID), podName, corev1.PodRunning)
			}
			reader := newReceiptBackendConfig(t, h.root, &rest.Config{Host: h.url, WrapTransport: m.wrap})
			requests, _ := h.api.counts()
			var chunk backend.LogChunk
			logs := h.api.fixture.run("held close", func() error {
				var err error
				chunk, err = reader.Logs(ctx, h.ref, 0, 64)
				return err
			})
			m.entered.wait(t, "close held")
			refused := h.release(t, tt.notCreated)
			// The reader's permit is held across the held Close.
			busy := newReceiptBackend(t, h.root, h.url)
			if release, err := busy.receipts.exclusive(); !errors.Is(err, errOperationBusy) {
				if err == nil {
					release()
				}
				t.Fatalf("exclusive while close held: %v", err)
			}
			if tt.kind == "Pod" {
				n, w := h.api.counts()
				ref := h.ref
				ref.Instance, _ = h.api.parent("Pod")
				if err := busy.removeExecution(ctx, ref); !errors.Is(err, errOperationBusy) {
					t.Fatalf("cleanup while close held: %v", err)
				}
				if n2, w2 := h.api.counts(); n2 != n || w2 != w {
					t.Fatal("busy cleanup reached the API")
				}
			}
			m.hold.release()
			err := logs.wait(t)
			if tt.notCreated {
				if !errors.Is(err, errReceiptRefused) || len(chunk.Data) != 0 {
					t.Fatalf("logs after refusal: %q %v", chunk.Data, err)
				}
			} else if err != nil || string(chunk.Data) != "output\n" {
				t.Fatalf("logs: %q %v", chunk.Data, err)
			}
			if calls, closes := m.counts(); calls != 1 || closes != 1 {
				t.Fatalf("close: %d calls, %d original closes, want 1", calls, closes)
			}
			if got := h.api.since(requests); !slices.Equal(got, tt.want) {
				t.Fatalf("requests: %q, want %q", got, tt.want)
			}
			if keptCustody(reader.receipts, testID) {
				t.Fatal("clean close kept its custody")
			}
			release, err := busy.receipts.exclusive()
			if err != nil {
				t.Fatalf("exclusive after close: %v", err)
			}
			release()
			if tt.notCreated {
				h.refusedState(t, tt.kind, refused)
				return
			}
			if r, err := reader.receipts.load(testID); err != nil || r.Phase != phasePinned {
				t.Fatalf("pinned: %+v %v", r, err)
			}
			if !bytes.Equal(h.byReceipt, receiptBytes(t, h.b, otherID)) || !bytes.Equal(h.byObject, storedJSON(h.api, tt.kind, otherID)) {
				t.Fatal("bystander changed")
			}
		})
	}
}

// TestReceiptLogCloseUncertain gives the attempt's log body a MODELED Close
// that fails, or returns without closing the original body. Logs returns
// errLogBodyHeld and no byte. The custody, with the original body and the
// operation permit, is kept for the life of the test process: cleanup of the
// scope is busy from any Backend, with no request; further Logs of the
// attempt in this process are refused with no request; Observe and the
// bystander's Logs proceed. The fixture is kept as a MODELED HOLD: its
// directory stays until the test binary exits, and is reported.
func TestReceiptLogCloseUncertain(t *testing.T) {
	for _, kind := range []string{"Job", "Pod"} {
		for _, mode := range []string{"error", "unknown"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				m := newCloseModel(t, testID, mode)
				m.hold.release()
				api, url, root := newReceiptAPI(t)
				api.fixture.keep()
				b := newReceiptBackendConfig(t, root, &rest.Config{Host: url, WrapTransport: m.wrap})
				ref := receiptRef(b, kind)
				by := receiptRef(b, kind)
				by.ID = otherID
				for _, r := range []backend.Reference{ref, by} {
					if err := b.Start(ctx, r.ID, r.Task); err != nil {
						t.Fatal(err)
					}
					if kind == "Job" {
						api.addJobPod(api.job("skygo-"+r.ID), "skygo-"+r.ID+"-pod", corev1.PodRunning)
					}
				}
				chunk, err := b.Logs(ctx, ref, 0, 64)
				if !errors.Is(err, errLogBodyHeld) || len(chunk.Data) != 0 {
					t.Fatalf("logs: %q %v", chunk.Data, err)
				}
				wantCloses := 1
				if mode == "unknown" {
					wantCloses = 0
				}
				if calls, closes := m.counts(); calls != 1 || closes != wantCloses {
					t.Fatalf("close: %d calls, %d original closes, want 1 and %d", calls, closes, wantCloses)
				}
				if !keptCustody(b.receipts, testID) {
					t.Fatal("custody not kept")
				}
				requests, writes := api.counts()
				before := receiptBytes(t, b, testID)
				other := newReceiptBackend(t, root, url)
				if release, err := other.receipts.exclusive(); !errors.Is(err, errOperationBusy) {
					if err == nil {
						release()
					}
					t.Fatalf("exclusive with a kept custody: %v", err)
				}
				if kind == "Pod" {
					pinned := ref
					pinned.Instance, _ = api.parent("Pod")
					if err := other.removeExecution(ctx, pinned); !errors.Is(err, errOperationBusy) {
						t.Fatalf("cleanup with a kept custody: %v", err)
					}
				}
				for i, c := range []*Backend{b, other} {
					if chunk, err := c.Logs(ctx, ref, 0, 64); !errors.Is(err, errLogBodyHeld) || len(chunk.Data) != 0 {
						t.Fatalf("backend %d logs again: %q %v", i, chunk.Data, err)
					}
				}
				if n, w := api.counts(); n != requests || w != writes || !bytes.Equal(before, receiptBytes(t, b, testID)) {
					t.Fatal("refused operations reached the API or changed the receipt")
				}
				if _, err := b.Observe(ctx, ref); err != nil {
					t.Fatalf("observe: %v", err)
				}
				if chunk, err := b.Logs(ctx, by, 0, 64); err != nil || string(chunk.Data) != "output\n" {
					t.Fatalf("bystander logs: %q %v", chunk.Data, err)
				}
				if calls, _ := m.counts(); calls != 1 {
					t.Fatalf("close calls: %d", calls)
				}
			})
		}
	}
}
