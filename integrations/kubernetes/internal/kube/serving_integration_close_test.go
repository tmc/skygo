package kube

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/environmentcontrol"
	"github.com/tmc/skygo/internal/servingcontrol"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

// AUTHORED_NOT_RUN. These tests close an integration NewIntegration built
// with both environments and services while an exec of its environment
// Controller holds its original connection Close. The models are those of
// newCloseHarness: environmentAPIModel MODELS the API server and kubelet,
// fakeAPI runs the real SPDY upgrade and streams over loopback, and
// faultConn holds the original Close.

// newIntegratedCloseHarness is newCloseHarness with the integration built
// by NewIntegration with services as well as environments.
func newIntegratedCloseHarness(t *testing.T, protocol string) *closeHarness {
	t.Helper()
	o, token := closeStore(t)
	actor, err := o.engine.Authenticate("Bearer " + token)
	if err != nil {
		t.Fatal(err)
	}
	profiles := newProfileAPI(t)
	withPullSecret(profiles)
	api := newFakeAPI(t, protocol)
	h := &closeHarness{o: o, s: o.s, token: token, actor: actor, api: api,
		gate: make(chan struct{}), hold: make(chan struct{})}
	o.harness = h
	targets := profiles.targets(t)
	// As in newCloseHarness: registered before the factory runs, it
	// releases the modeled gates and then makes the owner's one attempt to
	// close the original integration.
	t.Cleanup(func() {
		h.release()
		o.closeIntegration()
	})
	got, err := NewIntegration(context.Background(), o.s, o.engine, targets, environmentTestConfig(), serviceTestConfig())
	if got != nil {
		in, ok := got.(*environmentIntegration)
		if !ok {
			o.held = fmt.Errorf("factory returned %T", got)
			t.Fatal(o.held)
		}
		o.in, o.closeIn = in, in.Close
	}
	if err != nil {
		t.Fatal(err)
	}
	if o.in == nil {
		t.Fatal("factory returned no Integration")
	}
	in := o.in
	h.in, h.ctl = in, in.EnvironmentController()
	if h.ctl == nil || in.svc == nil || in.svcCtl == nil {
		t.Fatal("integration lacks a family")
	}

	h.model = newEnvironmentAPIModel(t)
	profiles.mu.Lock()
	for _, b := range closeBorrowed {
		obj := profiles.objs[b.path].(runtime.Object).DeepCopyObject()
		m, err := meta.Accessor(obj)
		if err != nil {
			profiles.mu.Unlock()
			t.Fatal(err)
		}
		h.model.mu.Lock()
		h.model.assign(m)
		h.model.objs[environmentAPIKey{b.resource, b.name}] = obj
		h.model.mu.Unlock()
	}
	profiles.mu.Unlock()

	pods := h.model.client.CoreV1().Pods("workloads")
	in.rt.b.client = h.model.client
	in.rt.pods = &funcPods{
		createFunc: func(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
			return pods.Create(ctx, pod, metav1.CreateOptions{})
		},
		updateFunc: func(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
			return pods.Update(ctx, pod, metav1.UpdateOptions{})
		},
		removeFunc: func(ctx context.Context, name string, opts metav1.DeleteOptions) error {
			return pods.Delete(ctx, name, opts)
		},
	}
	tr := api.transport(t)
	h.fc = &faultConn{closeGate: h.gate}
	tr.wrap = wrapWith(h.fc)
	in.rt.port, in.rt.serve = tr, tr.servePort

	info, err := h.s.RetentionInfo(actor)
	if err != nil {
		t.Fatal(err)
	}
	v, err := h.ctl.Create(actor, info.Domain, "close-1", envRTSpec())
	if err != nil {
		t.Fatal(err)
	}
	h.id, h.pod = v.ID, "skygo-env-"+v.ID
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	var rerr error
	for range 4 {
		if h.model.get("pods", h.pod) != nil {
			break
		}
		rerr = h.ctl.Reconcile(ctx)
	}
	if h.model.get("pods", h.pod) == nil {
		t.Fatalf("no Pod created by Reconcile: %v", rerr)
	}
	// MODELED kubelet: the Pod is bound and its container running and ready.
	h.model.edit("pods", h.pod, func(o runtime.Object) {
		p := o.(*corev1.Pod)
		p.Spec.NodeName = "node1"
		p.Status = corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{
			Name: environmentContainer, Ready: true,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}
	})
	for range 4 {
		rerr = h.ctl.Reconcile(ctx)
		if v, err = h.ctl.Get(actor, h.id); err != nil {
			t.Fatal(err)
		}
		if v.State.Phase == environment.Ready {
			return h
		}
	}
	t.Fatalf("environment phase = %v, want Ready (%v)", v.State.Phase, rerr)
	return nil
}

// TestIntegrationCloseStopsBothBeforeJoin closes the combined integration
// while an exec's original connection Close is held. At the StopRuntime
// acknowledgment, before anything is joined, the serving Controller, the
// service runtime and the environment Controller's sessions all refuse, and
// the original exec frame is still held. Its public caller may return an
// unsettled result while settlement retains ownership. Close does not return nil while held, the
// Store and Engine stay usable, and once released Close returns nil and the
// exec is joined.
func TestIntegrationCloseStopsBothBeforeJoin(t *testing.T) {
	h := newIntegratedCloseHarness(t, execProtocol)
	h.api.serveExec(execScript{status: successStatus, hold: h.hold})
	frames := make(chan *envFrame, 16)
	wrap := h.in.rt.port.wrap
	h.in.rt.port.wrap = func(frame *envFrame, conn httpstream.Connection) httpstream.Connection {
		h.api.keep(frame)
		frames <- frame
		return wrap(frame, conn)
	}
	out := &struct {
		done chan struct{}
		res  environmentcontrol.ExecResult
		err  error
	}{done: make(chan struct{})}
	// Registered before the call starts, so it runs before the harness's
	// cleanup: it releases the gates and joins the call. A call that does
	// not return latches the owner's held error, so nothing is retired.
	t.Cleanup(func() {
		h.release()
		select {
		case <-out.done:
		case <-time.After(testWait):
			t.Errorf("original Exec did not return; fixture retained")
			if h.o.held == nil {
				h.o.held = errors.New("original Exec did not return")
			}
			retainCall(out, h)
		}
	})
	go func() {
		defer close(out.done)
		out.res, out.err = h.ctl.Exec(context.Background(), h.actor, h.id, environmentcontrol.ExecRequest{Argv: []string{"/bin/sleep", "1000"}})
	}()
	eventually(t, "exec streams", func() bool { _, n := h.fc.counts(); return n == 3 })
	frame := receive(t, "the actual exec frame", frames)

	// The hook runs inside Close, on this goroutine, after the StopRuntime
	// callback acknowledged and before either family is joined.
	acks := 0
	h.in.stopped = func() {
		acks++
		if acks != 1 {
			return
		}
		if _, err := h.in.svcCtl.Get(h.actor, testServiceID); !errors.Is(err, servingcontrol.ErrClosed) {
			t.Errorf("serving Controller admits at the stop ACK: %v", err)
		}
		if _, err := h.in.svc.begin(); !errors.Is(err, errServiceStopped) {
			t.Errorf("service runtime admits at the stop ACK: %v", err)
		}
		requests, calls := len(h.api.requestList()), h.callCount()
		if _, err := h.ctl.Exec(context.Background(), h.actor, h.id, environmentcontrol.ExecRequest{Argv: []string{"/bin/true"}}); err == nil {
			t.Error("environment Exec admitted at the stop ACK")
		}
		rec := httptest.NewRecorder()
		if _, err := h.ctl.Forward(rec, httptest.NewRequest(http.MethodGet, "/", nil), h.actor, h.id); err == nil || rec.Body.Len() != 0 {
			t.Errorf("environment Forward at the stop ACK = %v, wrote %q", err, rec.Body.String())
		}
		if n := len(h.api.requestList()); n != requests {
			t.Errorf("%d transport requests at the stop ACK", n-requests)
		}
		if n := h.callCount(); n != calls {
			t.Errorf("%d model calls at the stop ACK", n-calls)
		}
		// Exec may return Settled=false and hand ownership to settlement;
		// only the actual original frame proves that ownership has joined.
		select {
		case <-frame.done:
			t.Error("original exec frame settled before the stop ACK")
		default:
		}
		if n := h.in.rt.activeFrames(); n != 1 {
			t.Errorf("%d runtime frames at the stop ACK, want the original 1", n)
		}
		if _, err := h.ctl.Get(h.actor, h.id); err != nil {
			t.Errorf("Store unusable at the stop ACK: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	err := h.in.Close(ctx)
	cancel()
	if err == nil {
		h.o.held = errors.New("integration Close = nil while the original connection Close is held")
		t.Fatal(h.o.held)
	}
	if acks != 1 {
		t.Fatalf("stop ACK hook ran %d times in one Close, want 1", acks)
	}
	if _, err := h.ctl.Get(h.actor, h.id); err != nil {
		t.Fatalf("Store unusable while held: %v", err)
	}
	if _, err := h.o.engine.Authenticate("Bearer " + h.token); err != nil {
		t.Fatalf("auth Engine unusable while held: %v", err)
	}
	eventually(t, "the original connection Close", func() bool { n, _ := h.fc.counts(); return n == 1 })
	select {
	case <-frame.done:
		t.Fatal("original exec frame settled while its connection Close is held")
	default:
	}

	h.releaseGate()
	receive(t, "original connection shutdown", h.fc.Connection.CloseChan())
	h.releaseHold()
	ctx, cancel = context.WithTimeout(context.Background(), testWait)
	err = h.in.Close(ctx)
	cancel()
	if err != nil {
		h.o.held = fmt.Errorf("integration Close after release: %w", err)
		t.Fatal(h.o.held)
	}
	if n := h.in.rt.activeFrames(); n != 0 {
		h.o.held = fmt.Errorf("integration Close = nil with %d runtime frames", n)
		t.Fatal(h.o.held)
	}
	h.o.joined = true
	receive(t, "the original exec frame", frame.done)
	receive(t, "the original Exec caller", out.done)
	if out.err != nil || out.res.Exited {
		t.Fatalf("Exec = %+v, %v; want a started, unreported command", out.res, out.err)
	}
	if acks != 2 {
		t.Fatalf("stop ACK hook ran %d times in two Closes", acks)
	}
}

// TestIntegrationCloseStopsPartial closes integrations holding only some
// components. The StopRuntime acknowledgment still precedes every join, and
// each component present refuses by then.
func TestIntegrationCloseStopsPartial(t *testing.T) {
	for _, withEnv := range []bool{false, true} {
		t.Run(map[bool]string{false: "service runtime", true: "service and environment runtimes"}[withEnv], func(t *testing.T) {
			client := fake.NewSimpleClientset()
			svc := newTestServiceRuntime(t, client, newModelWriter(client))
			in := &environmentIntegration{svc: svc}
			var e *envRT
			if withEnv {
				e = newEnvRT(t, &rest.Config{Host: envRTHost})
				in.rt = e.rt
			}
			acks := 0
			in.stopped = func() {
				acks++
				if _, err := svc.begin(); !errors.Is(err, errServiceStopped) {
					t.Errorf("service runtime admits at the stop ACK: %v", err)
				}
				if e != nil {
					if _, err := e.rt.Exec(e.ref, environmentcontrol.ExecRequest{Argv: []string{"/bin/true"}}); !errors.Is(err, errEnvironmentStopped) {
						t.Errorf("environment runtime admits at the stop ACK: %v", err)
					}
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), testWait)
			defer cancel()
			if err := in.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if acks != 1 {
				t.Fatalf("stop ACK hook ran %d times", acks)
			}
		})
	}
}
