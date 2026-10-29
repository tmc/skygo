package kube

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// draWire wraps the receipt client's base transport, below the draCall, and
// changes the original response body of the first request matching each
// fault. It is MODELED: it stands for a connection whose body Close fails or
// blocks; the requests and responses themselves are the loopback API's.
type draWire struct {
	base   http.RoundTripper
	mu     sync.Mutex
	faults []*draFault
}

// draFault makes the original Close of one response wait for gate, if set,
// and return closeErr.
type draFault struct {
	method, path string // path prefix
	closeErr     error
	gate         chan struct{}
	started      chan struct{} // closed when the original Close starts
	returned     chan struct{} // closed when it returns
	used         bool
	once         sync.Once
}

// release opens the fault's gate, if any, once.
func (f *draFault) release() {
	if f.gate != nil {
		f.once.Do(func() { close(f.gate) })
	}
}

func newDRAFault(method, path string, closeErr error, gated bool) *draFault {
	f := &draFault{method: method, path: path, closeErr: closeErr, started: make(chan struct{}), returned: make(chan struct{})}
	if gated {
		f.gate = make(chan struct{})
	}
	return f
}

func (w *draWire) wrap(base http.RoundTripper) http.RoundTripper {
	w.base = base
	return w
}

func (w *draWire) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := w.base.RoundTrip(req)
	if resp == nil {
		return resp, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, f := range w.faults {
		if !f.used && req.Method == f.method && strings.HasPrefix(req.URL.Path, f.path) {
			f.used = true
			resp.Body = &draFaultBody{ReadCloser: resp.Body, f: f}
			break
		}
	}
	return resp, err
}

type draFaultBody struct {
	io.ReadCloser
	f *draFault
}

func (b *draFaultBody) Close() error {
	close(b.f.started)
	if b.f.gate != nil {
		<-b.f.gate
	}
	err := b.ReadCloser.Close()
	close(b.f.returned)
	if b.f.closeErr != nil {
		return b.f.closeErr
	}
	return err
}

// awaitDRA waits for c within the join bound.
func awaitDRA(t *testing.T, c <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(receiptJoinTimeout):
		t.Fatalf("%s did not happen", what)
	}
}

var errDRAModeledClose = errors.New("modeled original close failure")

// TestDRACompleteCreateErrors checks both objects: a complete error reply is
// a terminal refusal witness even when the API stored the object, and a
// later read never adopts it; an incomplete or lost reply of the same stored
// object is the positive twin, recovered exactly, without any resend.
func TestDRACompleteCreateErrors(t *testing.T) {
	type fault struct {
		name     string
		set      func(a *draAPI, resource string)
		terminal bool
	}
	faults := []fault{
		{"500", func(a *draAPI, r string) { a.storeFail[r], a.failReason[r] = 500, metav1.StatusReasonInternalError }, true},
		{"429", func(a *draAPI, r string) { a.storeFail[r], a.failReason[r] = 429, metav1.StatusReasonTooManyRequests }, true},
		{"409 conflict", func(a *draAPI, r string) { a.storeFail[r], a.failReason[r] = 409, metav1.StatusReasonConflict }, true},
		{"cut body", func(a *draAPI, r string) { a.cut[r] = true }, false},
		{"lost reply", func(a *draAPI, r string) { a.drop[r] = true }, false},
	}
	for _, resource := range []string{"resourceclaims", "pods"} {
		for _, fl := range faults {
			t.Run(resource+" "+fl.name, func(t *testing.T) {
				f := newDRAFixture(t)
				_, b := f.integration(t)
				ctx := context.Background()
				f.api.set(func() { fl.set(f.api, resource) })
				if err := b.Start(ctx, testID, draTask(1)); err == nil {
					t.Fatal("start reported success")
				}
				pod := resource == "pods"
				o := *draObjectOf(ptr(draRecordOf(t, b, testID)), pod)
				switch {
				case fl.terminal && (o.Send != phaseRefused || o.Witness == nil || o.Witness.Status == 0):
					t.Fatalf("complete error not refused: %+v", o)
				case !fl.terminal && o.Send != phaseSent:
					t.Fatalf("incomplete reply not left sent: %+v", o)
				}
				if !pod && f.api.count("pods") != 0 {
					t.Fatal("pod sent after the claim reply")
				}
				// A restarted process: the API holds the object in every case.
				f.api.set(func() { delete(f.api.storeFail, resource); delete(f.api.cut, resource); delete(f.api.drop, resource) })
				_, b2 := f.integration(t)
				s, err := b2.Observe(ctx, draRef(b2, testID, draTask(1)))
				if err != nil {
					t.Fatal(err)
				}
				o = *draObjectOf(ptr(draRecordOf(t, b2, testID)), pod)
				if fl.terminal {
					if o.Send == phasePinned || s.Instance != "" && pod {
						t.Fatalf("complete error adopted by a read: %+v %+v", o, s)
					}
				} else if o.Send != phasePinned || pod && s.Instance == "" {
					t.Fatalf("exact object not recovered: %+v %+v", o, s)
				}
				if f.api.count(resource) != 1 || len(f.api.deleted()) != 0 {
					t.Fatal("resent or deleted")
				}
			})
		}
	}
}

// TestDRACreateCloseFailed: an exact 201 whose original Close fails keeps its
// UID as seen truth, records a HOLD, sends nothing more, and keeps the
// integration's Close failing; the Pod's twin is the same.
func TestDRACreateCloseFailed(t *testing.T) {
	for _, tc := range []struct{ name, path string }{
		{"claim", draClaimsPath},
		{"pod", podsPath},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDRAFixture(t)
			w := &draWire{faults: []*draFault{newDRAFault("POST", tc.path, errDRAModeledClose, false)}}
			in, b := f.integrationWith(t, w.wrap)
			f.expect(in, errDRAModeledClose)
			err := b.Start(context.Background(), testID, draTask(1))
			if !errors.Is(err, errDRAModeledClose) || !errors.Is(err, errDRACloseFailed) {
				t.Fatalf("Start = %v", err)
			}
			r := draRecordOf(t, b, testID)
			o := draObjectOf(&r, tc.name == "pod")
			if o.Send != phaseSent || o.Seen == "" || o.UID != "" || r.Hold == "" {
				t.Fatalf("record = %+v", r)
			}
			if tc.name == "claim" && (f.api.count("pods") != 0 || r.Pod.Send != draWithdrawn) {
				t.Fatal("pod sent after the claim's close failed")
			}
			s, err := b.Observe(context.Background(), draRef(b, testID, draTask(1)))
			if err != nil || s.State != backend.Unknown {
				t.Fatalf("status = %+v %v", s, err)
			}
			for range 2 { // sticky
				if err := in.Close(context.Background()); !errors.Is(err, errDRAModeledClose) {
					t.Fatalf("Close = %v", err)
				}
			}
		})
	}
}

// TestDRAHeldOriginalClose holds the actual original Close of a production
// claim 201 across caller cancellation and the integration's Close budget:
// the same frame keeps the store permit, Close reports it held and fences
// new operations, and after release Close joins it. The released twin
// proceeds normally. The fixture owns every gate, context, original and
// integration before anything starts (draFixture.cleanup).
func TestDRAHeldOriginalClose(t *testing.T) {
	t.Run("held across cancel and close", func(t *testing.T) {
		f := newDRAFixture(t)
		fault := newDRAFault("POST", draClaimsPath, nil, true)
		in, b := f.integrationWith(t, f.wire(fault).wrap)
		ctx, cancel := f.context()
		start := f.run("start", func() error { return b.Start(ctx, testID, draTask(1)) })
		awaitDRA(t, fault.started, "original Close")
		cancel()
		if err := start.wait(t); !errors.Is(err, errDRAHeld) {
			t.Fatalf("Start = %v", err)
		}
		r := draRecordOf(t, b, testID)
		if r.Claim.Send != phaseSent || r.Claim.Seen == "" || r.Hold != "" || f.api.count("pods") != 0 {
			t.Fatalf("record = %+v pods %d", r, f.api.count("pods"))
		}
		// The original's frame keeps the scope's operation permit.
		if release, err := b.dra.store.s.exclusive(); !errors.Is(err, errOperationBusy) {
			if release != nil {
				release()
			}
			t.Fatalf("exclusive permit while the original is held: %v", err)
		}
		short, cancelShort := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancelShort()
		if err := in.Close(short); err == nil {
			t.Fatal("Close returned nil while the original Close is held")
		}
		if err := b.Start(context.Background(), otherID, draTask(1)); !errors.Is(err, errDRAClosed) {
			t.Fatalf("start after a held Close: %v", err)
		}
		fault.release()
		awaitDRA(t, fault.returned, "original Close return")
		if err := in.Close(context.Background()); err != nil {
			t.Fatalf("Close after release: %v", err)
		}
		release, err := b.dra.store.s.exclusive()
		if err != nil {
			t.Fatalf("permit not released after the join: %v", err)
		}
		release()
	})
	t.Run("released", func(t *testing.T) {
		f := newDRAFixture(t)
		fault := newDRAFault("POST", draClaimsPath, nil, true)
		in, b := f.integrationWith(t, f.wire(fault).wrap)
		start := f.run("start", func() error { return b.Start(context.Background(), testID, draTask(1)) })
		awaitDRA(t, fault.started, "original Close")
		fault.release()
		if err := start.wait(t); err != nil {
			t.Fatal(err)
		}
		if r := draRecordOf(t, b, testID); r.Pod.Send != phasePinned || f.api.count("pods") != 1 {
			t.Fatalf("record = %+v", r)
		}
		if err := in.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("read", func(t *testing.T) {
		f := newDRAFixture(t)
		fault := newDRAFault("GET", podsPath+"/", nil, true)
		w := f.wire(fault)
		in, b := f.integrationWith(t, w.wrap)
		w.mu.Lock()
		fault.used = true // not the Start's own reads
		w.mu.Unlock()
		if err := b.Start(context.Background(), testID, draTask(1)); err != nil {
			t.Fatal(err)
		}
		w.mu.Lock()
		fault.used = false
		w.mu.Unlock()
		ctx, cancel := f.context()
		observe := f.run("observe", func() error {
			_, err := b.Observe(ctx, draRef(b, testID, draTask(1)))
			return err
		})
		awaitDRA(t, fault.started, "original read Close")
		cancel()
		if err := observe.wait(t); !errors.Is(err, errDRAHeld) {
			t.Fatalf("Observe = %v", err)
		}
		short, cancelShort := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancelShort()
		if err := in.Close(short); err == nil {
			t.Fatal("Close returned nil while a read's original Close is held")
		}
		fault.release()
		awaitDRA(t, fault.returned, "original read Close return")
		if err := in.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

// TestDRACancelDeleteCloseFailed: the delivered deletion is recorded as
// truth; its failed original Close is returned and keeps Close failing.
func TestDRACancelDeleteCloseFailed(t *testing.T) {
	f := newDRAFixture(t)
	w := &draWire{faults: []*draFault{newDRAFault("DELETE", podsPath+"/", errDRAModeledClose, false)}}
	in, b := f.integrationWith(t, w.wrap)
	f.expect(in, errDRAModeledClose)
	ctx := context.Background()
	if err := b.Start(ctx, testID, draTask(1)); err != nil {
		t.Fatal(err)
	}
	f.api.pod(testID, func(p *corev1.Pod) { p.Status.Phase = corev1.PodRunning })
	if err := b.Cancel(ctx, draRef(b, testID, draTask(1))); !errors.Is(err, errDRAModeledClose) {
		t.Fatalf("Cancel = %v", err)
	}
	if r := draRecordOf(t, b, testID); r.Cancel != stepDelivered || r.Hold != draHoldClose {
		t.Fatalf("record = %+v", r)
	}
	if err := in.Close(ctx); !errors.Is(err, errDRAModeledClose) {
		t.Fatalf("Close = %v", err)
	}
}

var errModeledWrite = errors.New("modeled journal write failure")

// TestDRAFailedCloseFences: after a witnessed ordinary success, the original
// Close of a pinned claim or Pod read, or of the first cleanup Pod deletion,
// fails on the production request and body path. The failure is the
// attempt's durable HOLD: later clean maintenance passes, an Observe and a
// replacement process over the same roots send no further deletion and never
// retire the record, while the witnessed success, both pins and a delivered
// deletion stay recorded. Each twin has the same request with a clean
// original Close and completes API cleanup.
func TestDRAFailedCloseFences(t *testing.T) {
	for _, tc := range []struct {
		name, method, path string
		deletes            int // sent before the failed Close
	}{
		{"pinned claim read", "GET", draClaimsPath + "/", 0},
		{"pinned pod read", "GET", podsPath + "/", 0},
		{"first cleanup pod deletion", "DELETE", podsPath + "/", 1},
	} {
		for _, closeErr := range []error{errDRAModeledClose, nil} {
			name := tc.name + " failed"
			if closeErr == nil {
				name = tc.name + " clean"
			}
			t.Run(name, func(t *testing.T) {
				f := newDRAFixture(t)
				w := f.wire()
				in, b := f.integrationWith(t, w.wrap)
				if closeErr != nil {
					f.expect(in, closeErr)
				}
				ctx := context.Background()
				ref := draRef(b, testID, draTask(1))
				draSucceededAttempt(t, f, b)
				// Armed only now: the next matching request is maintenance's.
				w.mu.Lock()
				w.faults = append(w.faults, newDRAFault(tc.method, tc.path, closeErr, false))
				w.mu.Unlock()
				f.now = f.now.Add(2 * time.Hour)
				err := b.maintainDRA(ctx)
				if closeErr == nil {
					for i := 0; err == nil && i < 2 && draRecordOf(t, b, testID).State != draRetired; i++ {
						err = b.maintainDRA(ctx)
					}
					if r := draRecordOf(t, b, testID); err != nil || r.State != draRetired || len(f.api.deleted()) != 2 {
						t.Fatalf("clean twin: %v, %d deletes, %+v", err, len(f.api.deleted()), r)
					}
					return
				}
				r := draRecordOf(t, b, testID)
				if !errors.Is(err, errDRAModeledClose) || r.Hold != draHoldClose || r.Execution != draExecSucceeded ||
					r.Pod.Send != phasePinned || r.Claim.Send != phasePinned || len(f.api.deleted()) != tc.deletes {
					t.Fatalf("failed close: %v, %d deletes, %+v", err, len(f.api.deleted()), r)
				}
				if tc.method == "DELETE" && r.Pod.Delete != stepDelivered {
					t.Fatalf("delivered deletion not recorded: %+v", r)
				}
				// Later clean passes, in this process and a replacement.
				for range 2 {
					b.maintainDRA(ctx)
				}
				if s, err := b.Observe(ctx, ref); err != nil || s.State != backend.Succeeded {
					t.Fatalf("witnessed success lost: %+v %v", s, err)
				}
				_, b2 := f.integration(t)
				for range 2 {
					b2.maintainDRA(ctx)
				}
				r = draRecordOf(t, b2, testID)
				if r.State == draRetired || r.Hold != draHoldClose || r.Claim.Delete != "" || len(f.api.deleted()) != tc.deletes || !claimExists(f) {
					t.Fatalf("continued after a failed close: %d deletes, %+v", len(f.api.deleted()), r)
				}
				for range 2 { // sticky
					if err := in.Close(ctx); !errors.Is(err, errDRAModeledClose) {
						t.Fatalf("Close = %v", err)
					}
				}
			})
		}
	}
	// A failed read Close refuses a later cancellation, in this process and a
	// replacement; TestDRADriftBeforeEachMutation's unchanged case is the twin.
	t.Run("cancel after failed read", func(t *testing.T) {
		f := newDRAFixture(t)
		w := f.wire()
		in, b := f.integrationWith(t, w.wrap)
		f.expect(in, errDRAModeledClose)
		ctx := context.Background()
		if err := b.Start(ctx, testID, draTask(1)); err != nil {
			t.Fatal(err)
		}
		f.api.pod(testID, func(p *corev1.Pod) { p.Status.Phase = corev1.PodRunning })
		w.mu.Lock()
		w.faults = append(w.faults, newDRAFault("GET", podsPath+"/", errDRAModeledClose, false))
		w.mu.Unlock()
		if _, err := b.Observe(ctx, draRef(b, testID, draTask(1))); !errors.Is(err, errDRAModeledClose) {
			t.Fatalf("Observe = %v", err)
		}
		_, b2 := f.integration(t)
		for _, x := range []*Backend{b, b2} {
			if err := x.Cancel(ctx, draRef(x, testID, draTask(1))); !errors.Is(err, errDRACloseFailed) {
				t.Fatalf("Cancel = %v", err)
			}
		}
		if r := draRecordOf(t, b2, testID); len(f.api.deleted()) != 0 || r.Cancel != "" || r.Hold != draHoldClose {
			t.Fatalf("canceled after a failed close: %d deletes, %+v", len(f.api.deleted()), r)
		}
	})
}

// TestDRAPriorHoldFences: an attempt already in HOLD for another reason, a
// foreign claim consumer or class drift refusing a cancellation, then has an
// actual production Pod read whose original Close fails. The first reason
// stays, unchanged, and a replacement never reports it as a Close failure;
// yet the fence is not lost: this process and a replacement over the same
// roots refuse cancellation and send no deletion, and later maintenance
// witnesses the Pod's success but cleans up and retires nothing. Without the
// failed Close the same prior HOLD fences alike, since every HOLD of the
// first mode is left to the operator, with no Close-failure diagnosis at
// all. TestDRACancel, without any HOLD, is the twin that cancels.
func TestDRAPriorHoldFences(t *testing.T) {
	for _, prior := range []string{"foreign consumer", "class drift"} {
		for _, closeErr := range []error{errDRAModeledClose, nil} {
			name := prior + " then failed close"
			if closeErr == nil {
				name = prior + " only"
			}
			t.Run(name, func(t *testing.T) {
				f := newDRAFixture(t)
				w := f.wire()
				in, b := f.integrationWith(t, w.wrap)
				if closeErr != nil {
					f.expect(in, closeErr)
				}
				ctx := context.Background()
				ref := draRef(b, testID, draTask(1))
				if err := b.Start(ctx, testID, draTask(1)); err != nil {
					t.Fatal(err)
				}
				f.api.pod(testID, func(p *corev1.Pod) { p.Status.Phase = corev1.PodRunning })
				switch prior {
				case "foreign consumer":
					f.api.claim(testID, func(c *resourcev1.ResourceClaim) {
						c.Status.ReservedFor = []resourcev1.ResourceClaimConsumerReference{{Resource: "pods", Name: "foreign", UID: "foreign-uid"}}
					})
					if _, err := b.Observe(ctx, ref); err != nil {
						t.Fatal(err)
					}
					f.api.claim(testID, func(c *resourcev1.ResourceClaim) { c.Status.ReservedFor = nil })
				case "class drift":
					var selectors []resourcev1.DeviceSelector
					f.api.set(func() {
						c := f.api.classes["gpu.example.com"]
						selectors, c.Spec.Selectors = c.Spec.Selectors, nil
					})
					if err := b.Cancel(ctx, ref); err == nil {
						t.Fatal("cancellation after class drift")
					}
					f.api.set(func() { f.api.classes["gpu.example.com"].Spec.Selectors = selectors })
				}
				first := draRecordOf(t, b, testID)
				if first.Hold == "" || first.Hold == draHoldClose || first.Cancel != "" || len(f.api.deleted()) != 0 {
					t.Fatalf("prior hold: %+v", first)
				}
				if closeErr != nil {
					f.arm(w, newDRAFault("GET", podsPath+"/", closeErr, false))
					if _, err := b.Observe(ctx, ref); !errors.Is(err, closeErr) {
						t.Fatalf("Observe = %v", err)
					}
				}
				_, b2 := f.integration(t)
				for _, x := range []*Backend{b, b2} {
					err := x.Cancel(ctx, draRef(x, testID, draTask(1)))
					if !errors.Is(err, errDRAHold) {
						t.Fatalf("Cancel = %v", err)
					}
					// Only this process knows of its failed Close; the
					// record keeps the first reason, reported as itself.
					if (x == b && closeErr != nil) != errors.Is(err, errDRACloseFailed) {
						t.Fatalf("Cancel diagnosis = %v", err)
					}
				}
				f.api.succeed(testID)
				f.now = f.now.Add(2 * time.Hour)
				for range 2 {
					b.maintainDRA(ctx)
					b2.maintainDRA(ctx)
				}
				r := draRecordOf(t, b2, testID)
				if r.Hold != first.Hold || r.Cancel != "" || r.Execution != draExecSucceeded || r.State != draOpen ||
					r.Pod.Send != phasePinned || r.Pod.UID != first.Pod.UID || r.Pod.Delete != "" || r.Claim.Delete != "" ||
					len(f.api.deleted()) != 0 || !claimExists(f) {
					t.Fatalf("continued after a HOLD: %d deletes, %+v", len(f.api.deleted()), r)
				}
			})
		}
	}
}

// TestDRAHoldFencesRecoveryPin: a claim whose create reply was cut stays
// sent, and a replacement recovers the exact claim by a read. Under any
// HOLD, the literal known-Close-failure reason an earlier version wrote, or
// another reason (both written here as that version would, MODELED), the
// replacement pins nothing, sends nothing and reports and keeps the reason.
func TestDRAHoldFencesRecoveryPin(t *testing.T) {
	for _, hold := range []string{"", "kubernetes dra create response close failed", "kubernetes dra claim identity differs"} {
		name := hold
		if name == "" {
			name = "no hold"
		}
		t.Run(name, func(t *testing.T) {
			f := newDRAFixture(t)
			_, b := f.integration(t)
			ctx := context.Background()
			f.api.set(func() { f.api.cut["resourceclaims"] = true })
			if err := b.Start(ctx, testID, draTask(1)); err == nil {
				t.Fatal("start reported success")
			}
			f.api.set(func() { delete(f.api.cut, "resourceclaims") })
			if hold != "" {
				if err := b.dra.store.hold(testID, hold); err != nil {
					t.Fatal(err)
				}
			}
			_, b2 := f.integration(t)
			s, err := b2.Observe(ctx, draRef(b2, testID, draTask(1)))
			r := draRecordOf(t, b2, testID)
			switch {
			case hold == "":
				if err != nil || r.Claim.Send != phasePinned || r.Hold != "" {
					t.Fatalf("exact claim not recovered: %+v %v %+v", s, err, r)
				}
			case err != nil || s.State != backend.Unknown || s.Reason != hold ||
				r.Claim.Send != phaseSent || r.Claim.UID != "" || r.Hold != hold:
				t.Fatalf("pinned under a HOLD: %+v %v %+v", s, err, r)
			}
			if f.api.count("resourceclaims") != 1 || f.api.count("pods") != 0 || len(f.api.deleted()) != 0 {
				t.Fatal("resent, sent a pod or deleted")
			}
		})
	}
}

// TestDRACancelAdmission gates Cancel's class recheck, on the production
// request and body path, while a concurrent Observe of the same Backend
// completes: its Pod read's original Close fails, which is published and
// recorded as HOLD before Observe returns, or it records a foreign-consumer
// HOLD. Released, the recheck itself is clean, yet the cancellation is
// admitted against the current record and sends no deletion. The identical
// clean Observe is the positive twin: the deletion is sent and delivered.
func TestDRACancelAdmission(t *testing.T) {
	for _, tc := range []struct {
		name     string
		closeErr error
		foreign  bool
		deletes  int
	}{
		{"failed close", errDRAModeledClose, false, 0},
		{"foreign consumer", nil, true, 0},
		{"clean", nil, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDRAFixture(t)
			w := f.wire()
			in, b := f.integrationWith(t, w.wrap)
			if tc.closeErr != nil {
				f.expect(in, tc.closeErr)
			}
			ctx := context.Background()
			ref := draRef(b, testID, draTask(1))
			if err := b.Start(ctx, testID, draTask(1)); err != nil {
				t.Fatal(err)
			}
			f.api.pod(testID, func(p *corev1.Pod) { p.Status.Phase = corev1.PodRunning })
			recheck := newDRAFault("GET", draClassesPath+"/", nil, true)
			f.arm(w, recheck)
			cctx, stop := f.context()
			defer stop()
			cancel := f.run("cancel", func() error { return b.Cancel(cctx, ref) })
			awaitDRA(t, recheck.started, "cancellation's class recheck")
			if tc.foreign {
				f.api.claim(testID, func(c *resourcev1.ResourceClaim) {
					c.Status.ReservedFor = []resourcev1.ResourceClaimConsumerReference{{Resource: "pods", Name: "foreign", UID: "foreign-uid"}}
				})
			}
			f.arm(w, newDRAFault("GET", podsPath+"/", tc.closeErr, false))
			_, err := b.Observe(ctx, ref)
			if tc.closeErr != nil && !errors.Is(err, tc.closeErr) || tc.closeErr == nil && err != nil {
				t.Fatalf("Observe = %v", err)
			}
			before := draRecordOf(t, b, testID)
			if (before.Hold != "") != (tc.deletes == 0) || before.Cancel != "" || len(f.api.deleted()) != 0 {
				t.Fatalf("before admission: %+v", before)
			}
			recheck.release()
			err = cancel.wait(t)
			r := draRecordOf(t, b, testID)
			n := len(f.api.deleted())
			if tc.deletes == 0 {
				if !errors.Is(err, errDRAHold) || n != 0 || r.Cancel != "" || r.Hold != before.Hold {
					t.Fatalf("admitted after a HOLD: %v, %d deletes, %+v", err, n, r)
				}
				if tc.foreign && errors.Is(err, errDRACloseFailed) {
					t.Fatalf("foreign consumer diagnosed as a failed Close: %v", err)
				}
				return
			}
			if err != nil || n != 1 || r.Cancel != stepDelivered {
				t.Fatalf("clean twin: %v, %d deletes, %+v", err, n, r)
			}
		})
	}
}

// TestDRALateCloseFailure holds the original Close of a claim's exact 201
// past its caller's cancellation, which records the UID as seen and no HOLD:
// a pending original is not a failure. Then the Close fails. The failure is
// the attempt's durable HOLD before the original's frame releases the
// scope's permit, so a replacement process over the same roots pins nothing
// and sends nothing. A late clean Close is the positive twin: the
// replacement recovers the exact claim. If the HOLD cannot be written, the
// frame keeps the permit and Close reports both failures.
func TestDRALateCloseFailure(t *testing.T) {
	for _, tc := range []struct {
		name     string
		closeErr error
		write    error // of the HOLD's record
	}{
		{"late failure", errDRAModeledClose, nil},
		{"late clean close", nil, nil},
		{"hold not written", errDRAModeledClose, errModeledWrite},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDRAFixture(t)
			fault := newDRAFault("POST", draClaimsPath, tc.closeErr, true)
			in, b := f.integrationWith(t, f.wire(fault).wrap)
			if tc.closeErr != nil {
				f.expect(in, tc.closeErr)
			}
			if tc.write != nil {
				b.dra.store.fault = func(step string, r *draRecord) error {
					if step == "write" && r.Hold == draHoldClose {
						return tc.write
					}
					return nil
				}
			}
			ctx, cancel := f.context()
			start := f.run("start", func() error { return b.Start(ctx, testID, draTask(1)) })
			awaitDRA(t, fault.started, "original Close")
			cancel()
			if err := start.wait(t); !errors.Is(err, errDRAHeld) {
				t.Fatalf("Start = %v", err)
			}
			seen := draRecordOf(t, b, testID)
			if seen.Claim.Send != phaseSent || seen.Claim.Seen == "" || seen.Hold != "" {
				t.Fatalf("pending original recorded as a failure: %+v", seen)
			}
			fault.release()
			awaitDRA(t, fault.returned, "original Close return")
			// Close joins the original, its record write and its frame.
			bound := receiptJoinTimeout
			if tc.write != nil {
				bound = 200 * time.Millisecond // the frame is kept: Close cannot join it
			}
			closeCtx, cancelClose := context.WithTimeout(context.Background(), bound)
			defer cancelClose()
			cerr := in.Close(closeCtx)
			r := draRecordOf(t, b, testID)
			release, perr := b.dra.store.s.exclusive()
			if release != nil {
				release()
			}
			switch {
			case tc.write != nil:
				if !errors.Is(cerr, errDRAModeledClose) || !errors.Is(cerr, errModeledWrite) || r.Hold != "" || !errors.Is(perr, errOperationBusy) {
					t.Fatalf("unrecorded failure released its owner: Close %v, permit %v, %+v", cerr, perr, r)
				}
				f.retain(in) // its frame and permit stay for the process
				return
			case tc.closeErr != nil:
				if !errors.Is(cerr, errDRAModeledClose) || r.Hold != draHoldClose || r.Claim.Seen != seen.Claim.Seen || perr != nil {
					t.Fatalf("late failure: Close %v, permit %v, %+v", cerr, perr, r)
				}
			default:
				if cerr != nil || r.Hold != "" || perr != nil {
					t.Fatalf("late clean close: Close %v, permit %v, %+v", cerr, perr, r)
				}
			}
			_, b2 := f.integration(t)
			if _, err := b2.Observe(context.Background(), draRef(b2, testID, draTask(1))); err != nil {
				t.Fatal(err)
			}
			b2.maintainDRA(context.Background())
			r = draRecordOf(t, b2, testID)
			if tc.closeErr != nil {
				if r.Claim.Send != phaseSent || r.Claim.UID != "" || r.Hold != draHoldClose {
					t.Fatalf("replacement continued after a late failure: %+v", r)
				}
			} else if r.Claim.Send != phasePinned || r.Claim.UID != seen.Claim.Seen || r.Hold != "" {
				t.Fatalf("replacement did not recover the exact claim: %+v", r)
			}
			if f.api.count("resourceclaims") != 1 || f.api.count("pods") != 0 || len(f.api.deleted()) != 0 {
				t.Fatal("resent, sent a pod or deleted")
			}
		})
	}
}

// TestDRAFixtureOwnsOriginals is the control of draFixture's custody: an
// inner test that ends before it releases its gate (an early return stands
// for a failed assertion; the same cleanups run after either, but no Fatal
// is executed here: MODELED) still has that gate released once, its held
// Start and integration joined and, after a clean Close, its API closed and
// its roots and kubeconfig removed. With an original Close that failed, as
// the inner test declared (expect), the inner test passes, and the same
// integration, its live API, the kubeconfig and the roots are kept
// (draCheckKept). TestDRAFixtureOracle checks undeclared outcomes.
func TestDRAFixtureOwnsOriginals(t *testing.T) {
	for _, tc := range []struct {
		name     string
		closeErr error
		kept     bool
	}{
		{"released and joined", nil, false},
		{"failed close", errDRAModeledClose, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var f *draFixture
			var in *draIntegration
			var start *receiptOp
			fault := newDRAFault("POST", draClaimsPath, tc.closeErr, true)
			passed := t.Run("ends before release", func(t *testing.T) {
				f = newDRAFixture(t)
				var b *Backend
				in, b = f.integrationWith(t, f.wire(fault).wrap)
				if tc.closeErr != nil {
					f.expect(in, tc.closeErr)
				}
				start = f.run("start", func() error { return b.Start(context.Background(), testID, draTask(1)) })
				awaitDRA(t, fault.started, "original Close")
			})
			if start == nil {
				t.Fatal("inner test did not start")
			}
			if !passed {
				t.Fatal("inner test failed: a declared HOLD is not a failure")
			}
			for what, c := range map[string]<-chan struct{}{"Start": start.done, "original Close": fault.returned} {
				select {
				case <-c:
				default:
					t.Fatalf("%s not joined by the fixture's cleanup", what)
				}
			}
			draCheckKept(t, f, in, tc.kept, tc.closeErr)
		})
	}
}

// TestDRAFixtureOracle runs draFixture's cleanup oracle in a child process
// of this test binary, so that the child's failure is observed rather than
// failing this test: an owner whose Close fails without a declaration, one
// that fails other than declared, and a declared failure that does not
// happen must each mark the child's owner test FAILED. The first two must
// also keep the same integration, its live API, the kubeconfig and roots,
// which a second test of the same child inspects after that cleanup ran;
// the third closed positively and releases them. An actual early Fatal while
// a held Start's original Close is live is the owner's only failure: with a
// clean original Close, the cleanup releases them; with one declared to
// fail, it keeps them. The parent registers the child's kill and reap before
// it starts, waits for the original process within the bound, and owns its
// directories, retired only after it exited; it reads the child's output
// only after that Wait returned. TestDRAFixtureOwnsOriginals is the
// in-process twin of a declared failure and a clean Close.
func TestDRAFixtureOracle(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		mode, failure, inspected string
	}{
		{"unexpected", "integration not closed", "INSPECTED kept"},
		{"mismatch", "integration not closed", "INSPECTED kept"},
		{"missing", "did not happen", "INSPECTED released"},
		{"clean-original-close", draEarlyFatal, "INSPECTED released"},
		{"expected-failed-original-close", draEarlyFatal, "INSPECTED kept"},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			base := newReceiptFixture(t) // the child's directories
			c := exec.Command(exe, "-test.run=^TestDRAFixtureHelper(Owner|Inspect)$", "-test.count=1", "-test.v")
			c.Env = append(os.Environ(), "SKYGO_DRA_FIXTURE_HELPER="+tc.mode, "SKYGO_RECEIPT_TEST_DIR="+base.dir)
			out := new(strings.Builder)
			c.Stdout, c.Stderr = out, out
			waited := make(chan error, 1)
			reaped := false
			t.Cleanup(func() {
				if c.Process == nil || reaped {
					return
				}
				c.Process.Kill()
				select {
				case <-waited:
				case <-time.After(receiptJoinTimeout):
					t.Errorf("child %d not reaped; its directories retained", c.Process.Pid)
					base.keep()
				}
			})
			if err := c.Start(); err != nil {
				t.Fatal(err)
			}
			go func() { waited <- c.Wait() }()
			var werr error
			select {
			case werr = <-waited:
				reaped = true
			case <-time.After(receiptJoinTimeout):
				// The child may still write out: it is read only after its Wait.
				t.Fatal("child did not return within the bound; its output is not read before its Wait")
			}
			var exit *exec.ExitError
			if !errors.As(werr, &exit) || exit.ExitCode() != 1 {
				t.Fatalf("child = %v, want its failed test\n%s", werr, out)
			}
			for _, want := range []string{"--- FAIL: TestDRAFixtureHelperOwner", tc.failure, "--- PASS: TestDRAFixtureHelperInspect", tc.inspected} {
				if !strings.Contains(out.String(), want) {
					t.Fatalf("child output lacks %q:\n%s", want, out)
				}
			}
			if tc.failure != draEarlyFatal {
				return
			}
			// The deliberate Fatal is the owner's only failure: its cleanup
			// released, closed and joined as declared.
			for _, unwanted := range []string{"integration not closed:", "did not happen", "did not return"} {
				if strings.Contains(out.String(), unwanted) {
					t.Fatalf("child output has %q:\n%s", unwanted, out)
				}
			}
		})
	}
}

// draEarlyFatal marks the deliberate Fatal of TestDRAFixtureHelperOwner
// while its held Start and original Close are live.
const draEarlyFatal = "deliberate-early-fatal"

// draHelper is the fixture and owner of TestDRAFixtureHelperOwner, for
// TestDRAFixtureHelperInspect, which runs after it in the same child.
var draHelper struct {
	f     *draFixture
	in    *draIntegration
	start *receiptOp // the held Start of an early Fatal
	fault *draFault  // its gated original Close
}

// TestDRAFixtureHelperOwner is the child of TestDRAFixtureOracle. Its owner's
// Close outcome differs from the declared one, which its cleanup must turn
// into this test's failure. In the early-Fatal modes, it executes an actual
// Fatal while its Start is held in the gated original Close of the claim's
// response, clean or declared to fail; its cleanup must release the gate,
// close the integration and join the original, adding no failure.
func TestDRAFixtureHelperOwner(t *testing.T) {
	mode := os.Getenv("SKYGO_DRA_FIXTURE_HELPER")
	if mode == "" {
		return // only a child of TestDRAFixtureOracle runs it
	}
	f := newDRAFixture(t)
	if mode == "clean-original-close" || mode == "expected-failed-original-close" {
		var closeErr error
		if mode == "expected-failed-original-close" {
			closeErr = errDRAModeledClose
		}
		fault := newDRAFault("POST", draClaimsPath, closeErr, true)
		in, b := f.integrationWith(t, f.wire(fault).wrap)
		draHelper.f, draHelper.in, draHelper.fault = f, in, fault
		if closeErr != nil {
			f.expect(in, closeErr)
		}
		draHelper.start = f.run("start", func() error { return b.Start(context.Background(), testID, draTask(1)) })
		awaitDRA(t, fault.started, "original Close")
		t.Fatal(draEarlyFatal + ": Start held in its original Close, gate not released")
	}
	var faults []*draFault
	if mode != "missing" {
		faults = append(faults, newDRAFault("POST", draClaimsPath, errDRAModeledClose, false))
	}
	in, b := f.integrationWith(t, f.wire(faults...).wrap)
	draHelper.f, draHelper.in = f, in
	switch mode {
	case "mismatch":
		f.expect(in, errModeledWrite) // not the failure that happens
	case "missing":
		f.expect(in, errDRAModeledClose) // a failure that does not happen
	}
	err := b.Start(context.Background(), testID, draTask(1))
	if failed := errors.Is(err, errDRAModeledClose); failed != (mode != "missing") || !failed && err != nil {
		t.Fatalf("Start = %v", err)
	}
}

// TestDRAFixtureHelperInspect inspects, after its cleanup ran, the fixture
// of TestDRAFixtureHelperOwner in the same child.
func TestDRAFixtureHelperInspect(t *testing.T) {
	mode := os.Getenv("SKYGO_DRA_FIXTURE_HELPER")
	if mode == "" {
		return // only a child of TestDRAFixtureOracle runs it
	}
	if draHelper.f == nil {
		t.Fatal("no owner test ran")
	}
	switch mode {
	case "clean-original-close", "expected-failed-original-close":
		if draHelper.start == nil || draHelper.fault == nil {
			t.Fatal("owner did not start its held Start")
		}
		for what, c := range map[string]<-chan struct{}{"Start": draHelper.start.done, "original Close": draHelper.fault.returned} {
			select {
			case <-c:
			default:
				t.Fatalf("%s not joined by the fixture's cleanup", what)
			}
		}
		if mode == "clean-original-close" {
			draCheckKept(t, draHelper.f, draHelper.in, false, nil)
			fmt.Println("INSPECTED released")
			return
		}
		draCheckKept(t, draHelper.f, draHelper.in, true, errDRAModeledClose)
		fmt.Println("INSPECTED kept")
		return
	}
	if mode == "missing" {
		draCheckKept(t, draHelper.f, draHelper.in, false, nil)
		fmt.Println("INSPECTED released")
		return
	}
	draCheckKept(t, draHelper.f, draHelper.in, true, errDRAModeledClose)
	fmt.Println("INSPECTED kept")
}

// TestDRADiscoveryBounds checks construction's reads: an API that accepts
// discovery but sends no headers, the caller's own cancellation before any
// request, and a held original discovery Close, which the returned partial
// integration owns until its Close joins it.
func TestDRADiscoveryBounds(t *testing.T) {
	t.Run("no headers", func(t *testing.T) {
		f := newDRAFixture(t)
		stall := make(chan struct{})
		f.api.set(func() { f.api.stall = stall })
		defer close(stall)
		before := treeImage(t, f.root)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		start := time.Now()
		in, err := newDRAIntegration(ctx, f.config, "owner", DRAOptions{ExecutionState: f.exec, State: f.root}, nil)
		if err == nil || in != nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("in %v, err = %v", in, err)
		}
		if d := time.Since(start); d > receiptJoinTimeout {
			t.Fatalf("construction took %v", d)
		}
		awaitDRA(t, f.api.stalled, "stalled discovery")
		if treeImage(t, f.root) != before {
			t.Fatal("construction wrote the dra state")
		}
	})
	t.Run("canceled before", func(t *testing.T) {
		f := newDRAFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		in, err := newDRAIntegration(ctx, f.config, "owner", DRAOptions{ExecutionState: f.exec, State: f.root}, nil)
		if err == nil || in != nil {
			t.Fatalf("in %v, err = %v", in, err)
		}
		for _, r := range f.api.log() {
			if strings.Contains(r, "resource.k8s.io/v1") {
				t.Fatalf("canceled construction sent %s", r)
			}
		}
	})
	t.Run("held original close", func(t *testing.T) {
		f := newDRAFixture(t)
		fault := newDRAFault("GET", "/apis/resource.k8s.io/v1", nil, true)
		w := f.wire(fault)
		before := treeImage(t, f.root)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		in, err := newDRAIntegration(ctx, f.config, "owner", DRAOptions{ExecutionState: f.exec, State: f.root}, w.wrap)
		f.own(in) // the partial integration owns the held original
		if !errors.Is(err, errDRAHeld) || in == nil {
			t.Fatalf("in %v, err = %v", in, err)
		}
		if treeImage(t, f.root) != before {
			t.Fatal("construction wrote the dra state")
		}
		short, cancelShort := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancelShort()
		if err := in.Close(short); !errors.Is(err, errDRAHeld) {
			t.Fatalf("Close = %v", err)
		}
		fault.release()
		awaitDRA(t, fault.returned, "original discovery Close")
		if err := in.Close(context.Background()); err != nil {
			t.Fatalf("Close after release: %v", err)
		}
	})
}

// TestDRACloseFencesFirst checks that Close fences every owned Backend and
// stops its own Run before it joins anything: with a live external Run
// context, and with two DRA Backends of which the first holds an original.
func TestDRACloseFencesFirst(t *testing.T) {
	t.Run("live run", func(t *testing.T) {
		f := newDRAFixture(t)
		in, b := f.integration(t)
		in.interval = 10 * time.Millisecond
		// The external context is never canceled; Close must stop Run. Run
		// is an original of the fixture, registered before it is spawned:
		// on any exit the fixture's Close signals it before joining it.
		run := f.run("run", func() error { return in.Run(context.Background()) })
		for deadline := time.Now().Add(receiptJoinTimeout); ; time.Sleep(time.Millisecond) {
			in.mu.Lock()
			running := in.runDone != nil
			in.mu.Unlock()
			if running {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("Run did not start")
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), receiptJoinTimeout)
		defer cancel()
		if err := in.Close(ctx); err != nil {
			t.Fatalf("Close: %v", err)
		}
		in.mu.Lock()
		runDone := in.runDone
		in.mu.Unlock()
		select {
		case <-runDone:
		default:
			t.Fatal("Close returned before Run")
		}
		if err := run.wait(t); !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v", err)
		}
		if err := b.Start(context.Background(), testID, draTask(1)); !errors.Is(err, errDRAClosed) {
			t.Fatalf("start after Close: %v", err)
		}
	})
	t.Run("two backends", func(t *testing.T) {
		f := newDRAFixture(t)
		// A target's binding does not include its name: the second differs
		// in its policy, so that its scope is distinct.
		more := strings.NewReplacer("name: test", "name: second", "max_accelerators: 8", "max_accelerators: 4").
			Replace(strings.TrimPrefix(draFixtureTargets, "version: 1\ntargets:\n"))
		config, err := readConfig([]byte(draFixtureTargets+more), f.dir, false)
		if err != nil {
			t.Fatal(err)
		}
		f.config = config
		fault := newDRAFault("POST", draClaimsPath, nil, true)
		w := f.wire(fault)
		in, err := newDRAIntegration(context.Background(), f.config, "owner", DRAOptions{ExecutionState: f.exec, State: f.root, LogRetention: time.Hour}, w.wrap)
		f.own(in)
		if err != nil {
			t.Fatal(err)
		}
		if len(in.backends) != 2 {
			t.Fatalf("%d dra backends", len(in.backends))
		}
		first, second := in.backends[0], in.backends[1]
		targets := in.Targets() // cached references
		// A failure below, the property this test exists to detect, ends
		// the test before the release: the fixture then releases the gate
		// and joins this Start and the integration before the roots go.
		start := f.run("first start", func() error { return first.Start(context.Background(), testID, draTask(1)) })
		awaitDRA(t, fault.started, "first backend's original Close")
		short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if err := in.Close(short); err == nil {
			t.Fatal("Close returned nil with the first backend held")
		}
		claims := f.api.count("resourceclaims")
		if err := second.Start(context.Background(), otherID, draTask(1)); !errors.Is(err, errDRAClosed) {
			t.Fatalf("second backend admitted a start: %v", err)
		}
		ordinary := draTask(1)
		ordinary.Accelerator = "nvidia"
		for _, target := range targets {
			if err := target.Backend.Start(context.Background(), otherID, ordinary); !errors.Is(err, errDRAClosed) {
				t.Fatalf("cached target %s admitted a start: %v", target.Name, err)
			}
		}
		if f.api.count("resourceclaims") != claims {
			t.Fatal("a create was sent after Close fenced")
		}
		fault.release()
		if err := start.wait(t); err == nil || f.api.count("pods") != 0 {
			t.Fatalf("first Start = %v, pods %d: a pod was sent after the fence", err, f.api.count("pods"))
		}
		if err := in.Close(context.Background()); err != nil {
			t.Fatalf("Close after release: %v", err)
		}
	})
}

// TestDRADriftBeforeEachMutation changes the namespace or class after a
// normal success, immediately before the cancellation or each cleanup
// deletion: none is sent and the attempt holds. The unchanged twin sends.
func TestDRADriftBeforeEachMutation(t *testing.T) {
	drifts := map[string]func(a *draAPI){
		"namespace recreated": func(a *draAPI) { a.nsUID = "ns-replaced" },
		"class recreated":     func(a *draAPI) { a.classes["gpu.example.com"].UID = "class-replaced" },
		"class spec":          func(a *draAPI) { a.classes["gpu.example.com"].Spec.Selectors = nil },
		"class removed":       func(a *draAPI) { delete(a.classes, "gpu.example.com") },
		"unchanged":           nil,
	}
	for name, drift := range drifts {
		t.Run("cancel "+name, func(t *testing.T) {
			f := newDRAFixture(t)
			_, b := f.integration(t)
			ctx := context.Background()
			if err := b.Start(ctx, testID, draTask(1)); err != nil {
				t.Fatal(err)
			}
			f.api.pod(testID, func(p *corev1.Pod) { p.Status.Phase = corev1.PodRunning })
			if drift != nil {
				f.api.set(func() { drift(f.api) })
			}
			err := b.Cancel(ctx, draRef(b, testID, draTask(1)))
			r := draRecordOf(t, b, testID)
			if drift == nil {
				if err != nil || len(f.api.deleted()) != 1 || r.Cancel != stepDelivered {
					t.Fatalf("unchanged: %v, %d deletes, %+v", err, len(f.api.deleted()), r)
				}
				return
			}
			if err == nil || len(f.api.deleted()) != 0 || r.Hold == "" || r.Cancel != "" {
				t.Fatalf("drift: %v, %d deletes, %+v", err, len(f.api.deleted()), r)
			}
		})
		for _, step := range []string{"pod", "claim"} {
			t.Run(step+" delete "+name, func(t *testing.T) {
				f := newDRAFixture(t)
				_, b := f.integration(t)
				draSucceededAttempt(t, f, b)
				f.now = f.now.Add(2 * time.Hour)
				ctx := context.Background()
				want := 0 // deletions sent before the drift
				if step == "claim" {
					// The Pod deletion and its absence come first, unchanged;
					// the claim still reserved for the Pod (MODELED) waits.
					r := draRecordOf(t, b, testID)
					f.api.claim(testID, func(c *resourcev1.ResourceClaim) {
						c.Status.ReservedFor = []resourcev1.ResourceClaimConsumerReference{{Resource: "pods", Name: r.Pod.Name, UID: types.UID(r.Pod.UID)}}
					})
					if err := b.maintainDRA(ctx); err != nil {
						t.Fatal(err)
					}
					if r := draRecordOf(t, b, testID); !r.Pod.Gone || r.Claim.Delete != "" || len(f.api.deleted()) != 1 {
						t.Fatalf("before the claim deletion: %+v", r)
					}
					f.api.claim(testID, func(c *resourcev1.ResourceClaim) { c.Status.ReservedFor = nil })
					want = 1
				}
				if drift != nil {
					f.api.set(func() { drift(f.api) })
				}
				err := b.maintainDRA(ctx)
				r := draRecordOf(t, b, testID)
				n := len(f.api.deleted())
				if drift == nil {
					if err != nil || n < want+1 {
						t.Fatalf("unchanged: %v, %d deletes, %+v", err, n, r)
					}
					return
				}
				if n != want || r.Hold == "" || r.State == draRetired {
					t.Fatalf("drift: %v, %d deletes, %+v", err, n, r)
				}
			})
		}
	}
}

// TestDRADeleteFormats sends the production typed deletions, which prefer
// Kubernetes protobuf, and checks the fixture decoded each with the real
// serializer; JSON and an undecodable body are its controls.
func TestDRADeleteFormats(t *testing.T) {
	f := newDRAFixture(t)
	_, b := f.integration(t)
	draSucceededAttempt(t, f, b)
	f.now = f.now.Add(2 * time.Hour)
	if err := b.maintainDRA(context.Background()); err != nil {
		t.Fatal(err)
	}
	formats, bad := f.api.deleteFormats()
	if len(formats) == 0 || len(bad) != 0 {
		t.Fatalf("formats %q, decode errors %v", formats, bad)
	}
	for _, ct := range formats {
		if ct != "application/vnd.kubernetes.protobuf" {
			t.Fatalf("production deletion sent %q", ct)
		}
	}
	for _, tc := range []struct {
		ct, body string
		ok       bool
	}{
		{"application/json", `{"kind":"DeleteOptions","apiVersion":"v1","preconditions":{"uid":"x"}}`, true},
		{"application/json", `{"preconditions":`, false},
		{"application/vnd.kubernetes.protobuf", "k8s\x00garbage", false},
		{"text/plain", "x", false},
	} {
		req, _ := http.NewRequest("DELETE", "/", strings.NewReader(tc.body))
		req.Header.Set("Content-Type", tc.ct)
		opts, _, err := decodeDeleteOptions(req)
		if tc.ok != (err == nil) || tc.ok && (opts.Preconditions == nil || opts.Preconditions.UID == nil) {
			t.Errorf("%s %q: %+v %v", tc.ct, tc.body, opts, err)
		}
	}
}

// TestDRATwoOSProcesses runs two OS processes of this test binary over the
// same private roots and the same loopback API, released together by a
// barrier file. Exactly one sends the claim and the Pod. The parent waits
// for both original processes before it returns.
func TestDRATwoOSProcesses(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	f := newDRAFixture(t)
	// One construction first writes the marker and scope, as a deployed
	// server's first start does; the helpers then only preflight them.
	first, _ := f.integration(t)
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	barrier := filepath.Join(t.TempDir(), "barrier")
	var cmds []*exec.Cmd
	var outs []*strings.Builder
	t.Cleanup(func() {
		for _, c := range cmds {
			if c.ProcessState == nil {
				c.Process.Kill()
				c.Wait()
			}
		}
	})
	for i := range 2 {
		c := exec.Command(exe, "-test.run=^TestDRAProcessHelper$", "-test.count=1")
		c.Env = append(os.Environ(), "SKYGO_DRA_HELPER=1", "SKYGO_DRA_DIR="+f.dir, "SKYGO_DRA_EXEC="+f.exec,
			"SKYGO_DRA_ROOT="+f.root, "SKYGO_DRA_BARRIER="+barrier, fmt.Sprintf("SKYGO_DRA_READY=%s.%d", barrier, i))
		out := new(strings.Builder)
		c.Stdout, c.Stderr = out, out
		if err := c.Start(); err != nil {
			t.Fatal(err)
		}
		cmds, outs = append(cmds, c), append(outs, out)
	}
	deadline := time.Now().Add(receiptJoinTimeout)
	for i := range 2 {
		for {
			if _, err := os.Stat(fmt.Sprintf("%s.%d", barrier, i)); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("helper %d not ready", i)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	if err := os.WriteFile(barrier, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	started := 0
	for i, c := range cmds {
		waited := make(chan error, 1)
		go func() { waited <- c.Wait() }()
		select {
		case err := <-waited:
			if err != nil {
				t.Fatalf("helper %d: %v\n%s", i, err, outs[i])
			}
		case <-time.After(receiptJoinTimeout):
			c.Process.Kill()
			<-waited
			t.Fatalf("helper %d did not return\n%s", i, outs[i])
		}
		if strings.Contains(outs[i].String(), "STARTED") {
			started++
		}
	}
	if started != 1 || f.api.count("resourceclaims") != 1 || f.api.count("pods") != 1 {
		t.Fatalf("%d started, %d claims, %d pods", started, f.api.count("resourceclaims"), f.api.count("pods"))
	}
}

// TestDRAProcessHelper is one process of TestDRATwoOSProcesses.
func TestDRAProcessHelper(t *testing.T) {
	if os.Getenv("SKYGO_DRA_HELPER") == "" {
		return // only a process of TestDRATwoOSProcesses runs it
	}
	config, err := readConfig([]byte(draFixtureTargets), os.Getenv("SKYGO_DRA_DIR"), false)
	if err != nil {
		t.Fatal(err)
	}
	in, err := newDRAIntegration(context.Background(), config, "owner",
		DRAOptions{ExecutionState: os.Getenv("SKYGO_DRA_EXEC"), State: os.Getenv("SKYGO_DRA_ROOT")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := in.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	if err := os.WriteFile(os.Getenv("SKYGO_DRA_READY"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(receiptJoinTimeout); ; time.Sleep(time.Millisecond) {
		if _, err := os.Stat(os.Getenv("SKYGO_DRA_BARRIER")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("barrier not released")
		}
	}
	switch err := in.backends[0].Start(context.Background(), testID, draTask(1)); {
	case err == nil:
		fmt.Println("STARTED")
	case errors.Is(err, errReceiptExists), errors.Is(err, errOperationBusy):
		fmt.Println("REFUSED", err)
	default:
		t.Fatal(err)
	}
}
