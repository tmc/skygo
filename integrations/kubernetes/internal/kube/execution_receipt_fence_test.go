package kube

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
)

// The interleavings below are MODELED with the loopback API's gates: a
// create response, a GET or a log body held after the server took its
// content. They show the order of receipt checks and requests in one
// process; they are not native Kubernetes or cross-host qualification.

// heldStart is an attempt whose original Start is held after the server
// stored its object, beside a started bystander attempt.
type heldStart struct {
	api       *receiptAPI
	b         *Backend // original Backend, its Start held
	start     *receiptOp
	ref, by   backend.Reference
	root, url string
	// state no refused operation may change
	byReceipt, byObject, object []byte
}

// startHeld starts the bystander otherID of kind, then starts testID with
// its create response held after the object is stored. The held response is
// the stored object acknowledged 200 if notCreated is set, which Start
// refuses, and 201 otherwise.
func startHeld(t *testing.T, kind string, notCreated bool) *heldStart {
	t.Helper()
	ctx := context.Background()
	api, url, root := newReceiptAPI(t)
	api.admit = admitParent
	b := newReceiptBackend(t, root, url)
	by := receiptRef(b, kind)
	by.ID = otherID
	if err := b.Start(ctx, by.ID, by.Task); err != nil {
		t.Fatal(err)
	}
	api.mu.Lock()
	api.holdStored, api.notCreated = true, notCreated
	api.mu.Unlock()
	api.hold(true)
	h := &heldStart{api: api, b: b, ref: receiptRef(b, kind), by: by, root: root, url: url}
	h.byReceipt, h.byObject = receiptBytes(t, b, otherID), storedJSON(api, kind, otherID)
	h.start = api.fixture.run("original start", func() error { return b.Start(ctx, h.ref.ID, h.ref.Task) })
	api.createEntered.wait(t, "create stored")
	h.object = storedJSON(api, kind, testID)
	return h
}

func storedJSON(api *receiptAPI, kind, id string) []byte {
	var data []byte
	if kind == "Job" {
		data, _ = json.Marshal(api.job("skygo-" + id))
	} else {
		data, _ = json.Marshal(api.pod("skygo-" + id))
	}
	return data
}

func parentGet(kind string) string {
	if kind == "Job" {
		return "GET " + jobsPath + "/skygo-" + testID
	}
	return "GET " + podsPath + "/skygo-" + testID
}

// release releases the held create response and joins the original Start.
// A 200 acknowledgment must be durably refused and its receipt is returned;
// a 201 must succeed.
func (h *heldStart) release(t *testing.T, notCreated bool) []byte {
	t.Helper()
	h.api.holdCreate.release()
	err := h.start.wait(t)
	h.api.createDone.wait(t, "create done")
	if !notCreated {
		if err != nil {
			t.Fatalf("original start: %v", err)
		}
		return nil
	}
	if !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("original start: %v", err)
	}
	r, err := newReceiptBackend(t, h.root, h.url).receipts.load(testID)
	if err != nil || r.Phase != phaseRefused || r.Witness == nil || r.Witness.Status != 200 || r.Witness.Decode != witnessDecoded {
		t.Fatalf("refusal: %+v %v", r, err)
	}
	return receiptBytes(t, h.b, testID)
}

// refusedState requires that, after the refusal, nothing was written but the
// two creates, the refused receipt, its object and the bystander are
// unchanged, and that the original and a restarted Backend refuse every
// operation without a request while the bystander remains observable.
func (h *heldStart) refusedState(t *testing.T, kind string, refused []byte) {
	t.Helper()
	ctx := context.Background()
	if !bytes.Equal(refused, receiptBytes(t, h.b, testID)) || !bytes.Equal(h.object, storedJSON(h.api, kind, testID)) {
		t.Fatal("refused attempt changed after its refusal")
	}
	if _, w := h.api.counts(); w != 2 {
		t.Fatalf("writes: %d, want 2", w)
	}
	if !bytes.Equal(h.byReceipt, receiptBytes(t, h.b, otherID)) || !bytes.Equal(h.byObject, storedJSON(h.api, kind, otherID)) {
		t.Fatal("bystander changed")
	}
	uid, _ := h.api.parent(kind)
	for i, c := range []*Backend{h.b, newReceiptBackend(t, h.root, h.url)} {
		requests, _ := h.api.counts()
		for name, op := range refusedOps(ctx, c, h.ref, kind, uid) {
			if err := op(); !errors.Is(err, errReceiptRefused) {
				t.Fatalf("backend %d %s: %v", i, name, err)
			}
		}
		if n, _ := h.api.counts(); n != requests {
			t.Fatalf("backend %d reached the API", i)
		}
	}
	if _, err := h.b.Observe(ctx, h.by); err != nil {
		t.Fatalf("bystander observe: %v", err)
	}
}

// TestReceiptHeldReadAfterRefusal holds a pinned Observe GET of a later
// Backend after the server took the stored, fully acceptable object. The
// original Start then receives a complete 200 acknowledgment of that object
// and durably refuses the attempt. The released GET returns the refusal, not
// a status, and nothing is pinned, written or requested further. The twin
// acknowledges 201 and the same held read reports the pinned instance.
func TestReceiptHeldReadAfterRefusal(t *testing.T) {
	for _, kind := range []string{"Job", "Pod"} {
		for _, notCreated := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/not-created=%v", kind, notCreated), func(t *testing.T) {
				ctx := context.Background()
				h := startHeld(t, kind, notCreated)
				uid, _ := h.api.parent(kind)
				// A recovery read of another Backend pins the stored object.
				if status, err := newReceiptBackend(t, h.root, h.url).Observe(ctx, h.ref); err != nil || status.Instance != uid {
					t.Fatalf("recovery: %+v %v", status, err)
				}
				reader := newReceiptBackend(t, h.root, h.url)
				h.api.holdRead(1)
				requests, _ := h.api.counts()
				var status backend.Status
				read := h.api.fixture.run("held read", func() error {
					var err error
					status, err = reader.Observe(ctx, h.ref)
					return err
				})
				h.api.getEntered.wait(t, "read taken")
				refused := h.release(t, notCreated)
				h.api.holdGet.release()
				err := read.wait(t)
				if notCreated {
					if !errors.Is(err, errReceiptRefused) || status != (backend.Status{}) {
						t.Fatalf("held read after refusal: %+v %v", status, err)
					}
				} else if err != nil || status.Instance != uid {
					t.Fatalf("held read: %+v %v", status, err)
				}
				// The receipt was loaded before the GET; no second read and,
				// after a refusal, no Pod list follows.
				want := []string{parentGet(kind)}
				if !notCreated && kind == "Job" {
					want = append(want, "GET "+podsPath)
				}
				if got := h.api.since(requests); !slices.Equal(got, want) {
					t.Fatalf("requests: %q, want %q", got, want)
				}
				if !notCreated {
					r, err := reader.receipts.load(testID)
					if err != nil || r.Phase != phasePinned || r.UID != uid || r.Witness != nil {
						t.Fatalf("pinned: %+v %v", r, err)
					}
					if _, w := h.api.counts(); w != 2 {
						t.Fatalf("writes: %d", w)
					}
					return
				}
				h.refusedState(t, kind, refused)
			})
		}
	}
}

// TestReceiptHeldReadGenerationAdvance is the benign twin: a pinned read is
// held while another Backend records and delivers a cancellation, which
// advances the receipt's generation without changing its identity, phase or
// UID. The read is reported under the current record.
func TestReceiptHeldReadGenerationAdvance(t *testing.T) {
	ctx := context.Background()
	api, b, ref, root, url := setupPinnedRetry(t, runRetryPod)
	before, err := b.receipts.load(testID)
	if err != nil {
		t.Fatal(err)
	}
	api.holdRead(1)
	var status backend.Status
	read := api.fixture.run("held read", func() error {
		var err error
		status, err = b.Observe(ctx, ref)
		return err
	})
	api.getEntered.wait(t, "read taken")
	if err := newReceiptBackend(t, root, url).Cancel(ctx, ref); err != nil {
		t.Fatal(err)
	}
	after, err := newReceiptBackend(t, root, url).receipts.load(testID)
	if err != nil || after.Generation <= before.Generation || after.Cancel != stepDelivered || after.UID != ref.Instance {
		t.Fatalf("cancel: %+v %v", after, err)
	}
	api.holdGet.release()
	if err := read.wait(t); err != nil || status.State != backend.Running || status.Instance != ref.Instance {
		t.Fatalf("held read: %+v %v", status, err)
	}
}

// TestReceiptHeldLogsAfterRefusal runs ordinary Job Logs and retry Pod Logs
// of a later Backend, which recovers and pins the stored object, while the
// original Start's response is held. The original Start then receives a
// complete 200 acknowledgment and durably refuses the attempt:
//
//   - body: while the log body is held. No byte is released.
//   - read: while the last read before the log request is held. No log
//     request is sent.
//
// The twin acknowledges 201 and the same held Logs release the bytes.
func TestReceiptHeldLogsAfterRefusal(t *testing.T) {
	podName := "skygo-" + testID + "-pod"
	jobLog := "GET " + podsPath + "/" + podName + "/log"
	podLog := parentGet("Pod") + "/log"
	list := "GET " + podsPath
	for _, tt := range []struct {
		kind, mode string
		notCreated bool
		want       []string
	}{
		{"Job", "body", true, []string{parentGet("Job"), list, parentGet("Job"), list, jobLog}},
		{"Job", "read", true, []string{parentGet("Job"), list, parentGet("Job")}},
		{"Job", "body", false, []string{parentGet("Job"), list, parentGet("Job"), list, jobLog}},
		{"Pod", "body", true, []string{parentGet("Pod"), parentGet("Pod"), podLog}},
		{"Pod", "read", true, []string{parentGet("Pod"), parentGet("Pod")}},
		{"Pod", "body", false, []string{parentGet("Pod"), parentGet("Pod"), podLog, parentGet("Pod")}},
	} {
		t.Run(fmt.Sprintf("%s/%s/not-created=%v", tt.kind, tt.mode, tt.notCreated), func(t *testing.T) {
			ctx := context.Background()
			h := startHeld(t, tt.kind, tt.notCreated)
			if tt.kind == "Job" {
				h.api.addJobPod(h.api.job("skygo-"+testID), podName, corev1.PodRunning)
			}
			reader := newReceiptBackend(t, h.root, h.url)
			var entered *gate
			if tt.mode == "body" {
				h.api.hold(false)
				entered = h.api.logEntered
			} else {
				h.api.holdRead(2)
				entered = h.api.getEntered
			}
			requests, _ := h.api.counts()
			var chunk backend.LogChunk
			logs := h.api.fixture.run("held logs", func() error {
				var err error
				chunk, err = reader.Logs(ctx, h.ref, 0, 64)
				return err
			})
			entered.wait(t, tt.mode+" held")
			refused := h.release(t, tt.notCreated)
			if tt.mode == "body" {
				h.api.holdLog.release()
			} else {
				h.api.holdGet.release()
			}
			err := logs.wait(t)
			if tt.notCreated {
				if !errors.Is(err, errReceiptRefused) || len(chunk.Data) != 0 {
					t.Fatalf("logs after refusal: %q %v", chunk.Data, err)
				}
			} else if err != nil || string(chunk.Data) != "output\n" {
				t.Fatalf("logs: %q %v", chunk.Data, err)
			}
			if got := h.api.since(requests); !slices.Equal(got, tt.want) {
				t.Fatalf("requests: %q, want %q", got, tt.want)
			}
			if tt.notCreated {
				h.refusedState(t, tt.kind, refused)
			} else if r, err := reader.receipts.load(testID); err != nil || r.Phase != phasePinned {
				t.Fatalf("pinned: %+v %v", r, err)
			}
		})
	}
}
