package backend_test

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

func TestWorkerStartTokenDistinct(t *testing.T) {
	base := backend.WorkerReference{Group: exampleWorkerGroup(2, backend.WorkerTCP), Rank: 1, Pod: examplePod1, Service: exampleService}
	edits := map[string]func(*backend.WorkerReference){
		"rank":    func(r *backend.WorkerReference) { r.Rank = 0; r.Pod = examplePod0 },
		"pod uid": func(r *backend.WorkerReference) { r.Pod.UID = "d3219392-688c-4e60-8ce1-680a8956a01f" },
		"attempt": func(r *backend.WorkerReference) { r.Group.AttemptID = "00000000000000000000000000000001" },
		"job":     func(r *backend.WorkerReference) { r.Group.Execution.ID = "00000000000000000000000000000002" },
		"count":   func(r *backend.WorkerReference) { r.Group.Spec.Count = 3 },
		"mode": func(r *backend.WorkerReference) {
			r.Group.Spec.Mode = backend.WorkerIndependent
			r.Service = backend.WorkerResource{}
		},
	}
	want, err := backend.WorkerStartToken(base)
	if err != nil {
		t.Fatal(err)
	}
	if again, _ := backend.WorkerStartToken(base); again != want {
		t.Fatal("token is not deterministic")
	}
	for name, edit := range edits {
		r := base
		edit(&r)
		got, err := backend.WorkerStartToken(r)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got == want {
			t.Errorf("%s: same token", name)
		}
	}
	noService := base
	noService.Service = backend.WorkerResource{}
	if _, err := backend.WorkerStartToken(noService); err == nil {
		t.Error("tcp token without the rendezvous service")
	}
}

func TestValidateWorkerBarrier(t *testing.T) {
	g := exampleWorkerGroup(2, backend.WorkerTCP)
	r0 := backend.WorkerReference{Group: g, Rank: 0, Pod: examplePod0, Service: exampleService}
	r1 := backend.WorkerReference{Group: g, Rank: 1, Pod: examplePod1, Service: exampleService}
	exit := &backend.ContainerExit{ContainerID: "containerd://x", ExitCode: 1, StartedAt: time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC), FinishedAt: time.Date(2026, 10, 29, 0, 1, 0, 0, time.UTC)}
	tests := []struct {
		name string
		ref  backend.WorkerReference
		got  backend.WorkerBarrier
		want string
	}{
		{"pending", r1, backend.WorkerBarrier{Pod: examplePod1, Gated: true}, ""},
		{"never bound", r1, backend.WorkerBarrier{Pod: examplePod1, Deleting: true, Stop: backend.WorkerNeverBound}, ""},
		{"never bound ungated", r1, backend.WorkerBarrier{Pod: examplePod1, Gated: false, Deleting: true, Stop: backend.WorkerNeverBound}, ""},
		{"terminated", r1, backend.WorkerBarrier{Pod: examplePod1, Node: "n", Stop: backend.WorkerTerminated, Exit: exit}, ""},
		{"rank 0 rendezvous", r0, backend.WorkerBarrier{Pod: examplePod0, Rendezvous: backend.WorkerRendezvous{Service: exampleService}}, ""},
		{"other pod", r1, backend.WorkerBarrier{Pod: examplePod0}, "identity mismatch"},
		{"rank 1 rendezvous", r1, backend.WorkerBarrier{Pod: examplePod1, Rendezvous: backend.WorkerRendezvous{Service: exampleService}}, "unexpected worker rendezvous"},
		{"other service", r0, backend.WorkerBarrier{Pod: examplePod0, Rendezvous: backend.WorkerRendezvous{Service: examplePod1}}, "service mismatch"},
		{"never bound with node", r1, backend.WorkerBarrier{Pod: examplePod1, Deleting: true, Node: "n", Stop: backend.WorkerNeverBound}, "never-bound"},
		{"never bound not deleting", r1, backend.WorkerBarrier{Pod: examplePod1, Stop: backend.WorkerNeverBound}, "never-bound"},
		{"exit without stop", r1, backend.WorkerBarrier{Pod: examplePod1, Exit: exit}, "exit without termination"},
		{"terminated prepared", r1, backend.WorkerBarrier{Pod: examplePod1, Prepared: true, Stop: backend.WorkerTerminated}, "reports prepared"},
		{"invalid native exit", r1, backend.WorkerBarrier{Pod: examplePod1, Stop: backend.WorkerTerminated, Exit: &backend.ContainerExit{ContainerID: "containerd://x"}}, "termination receipt"},
		{"unknown stop", r1, backend.WorkerBarrier{Pod: examplePod1, Stop: 9}, "invalid worker stop"},
		{"never released observed", r1, backend.WorkerBarrier{Pod: examplePod1, Stop: backend.WorkerNeverReleased}, "invalid worker stop"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := backend.ValidateWorkerBarrier(tt.ref, tt.got)
			if tt.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err %v, want %q", err, tt.want)
			}
		})
	}
}

func TestWorkerBarrierReady(t *testing.T) {
	g := exampleWorkerGroup(2, backend.WorkerTCP)
	refs := []backend.WorkerReference{
		{Group: g, Rank: 0, Pod: examplePod0, Service: exampleService},
		{Group: g, Rank: 1, Pod: examplePod1, Service: exampleService},
	}
	ready := func() []backend.WorkerBarrier {
		return []backend.WorkerBarrier{
			{Pod: examplePod0, Node: "a", Prepared: true, Rendezvous: backend.WorkerRendezvous{Service: exampleService, Routed: true}},
			{Pod: examplePod1, Node: "b", Prepared: true},
		}
	}
	tests := []struct {
		name string
		edit func([]backend.WorkerReference, []backend.WorkerBarrier) ([]backend.WorkerReference, []backend.WorkerBarrier)
		want string
	}{
		{"ready", nil, ""},
		{"started before decision", func(r []backend.WorkerReference, o []backend.WorkerBarrier) ([]backend.WorkerReference, []backend.WorkerBarrier) {
			o[0].Started, o[1].Started = true, true
			return r, o
		}, "already holds a start token"},
		{"missing member", func(r []backend.WorkerReference, o []backend.WorkerBarrier) ([]backend.WorkerReference, []backend.WorkerBarrier) {
			return r[:1], o[:1]
		}, "every member"},
		{"gated", func(r []backend.WorkerReference, o []backend.WorkerBarrier) ([]backend.WorkerReference, []backend.WorkerBarrier) {
			o[1].Gated = true
			return r, o
		}, "rank 1 is not bound"},
		{"unbound", func(r []backend.WorkerReference, o []backend.WorkerBarrier) ([]backend.WorkerReference, []backend.WorkerBarrier) {
			o[1].Node = ""
			return r, o
		}, "rank 1 is not bound"},
		{"not prepared", func(r []backend.WorkerReference, o []backend.WorkerBarrier) ([]backend.WorkerReference, []backend.WorkerBarrier) {
			o[0].Prepared = false
			return r, o
		}, "rank 0 is not prepared"},
		{"deleting", func(r []backend.WorkerReference, o []backend.WorkerBarrier) ([]backend.WorkerReference, []backend.WorkerBarrier) {
			o[1].Deleting = true
			return r, o
		}, "rank 1 is stopping"},
		{"not routed", func(r []backend.WorkerReference, o []backend.WorkerBarrier) ([]backend.WorkerReference, []backend.WorkerBarrier) {
			o[0].Rendezvous.Routed = false
			return r, o
		}, "not routed"},
		{"swapped ranks", func(r []backend.WorkerReference, o []backend.WorkerBarrier) ([]backend.WorkerReference, []backend.WorkerBarrier) {
			r[0], r[1] = r[1], r[0]
			return r, o
		}, "members differ"},
		{"stale observation", func(r []backend.WorkerReference, o []backend.WorkerBarrier) ([]backend.WorkerReference, []backend.WorkerBarrier) {
			o[1].Pod.UID = "d3219392-688c-4e60-8ce1-680a8956a01f"
			return r, o
		}, "identity mismatch"},
		{"other attempt", func(r []backend.WorkerReference, o []backend.WorkerBarrier) ([]backend.WorkerReference, []backend.WorkerBarrier) {
			r[1].Group.AttemptID = "00000000000000000000000000000001"
			return r, o
		}, "members differ"},
		// Each of the next two changes only one immutable identity of an
		// otherwise valid reference.
		{"other binding", func(r []backend.WorkerReference, o []backend.WorkerBarrier) ([]backend.WorkerReference, []backend.WorkerBarrier) {
			r[1].Group.Execution.Binding = "sha256:other"
			return r, o
		}, "members differ in binding"},
		{"other service uid", func(r []backend.WorkerReference, o []backend.WorkerBarrier) ([]backend.WorkerReference, []backend.WorkerBarrier) {
			r[1].Service.UID = "e3219392-688c-4e60-8ce1-680a8956a01f"
			return r, o
		}, "members differ in rendezvous service"},
		{"other service name", func(r []backend.WorkerReference, o []backend.WorkerBarrier) ([]backend.WorkerReference, []backend.WorkerBarrier) {
			r[1].Service.Name = "skygo-0123456789abcdef0123456789abcdef"
			return r, o
		}, "members differ in rendezvous service"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, o := append([]backend.WorkerReference(nil), refs...), ready()
			if tt.edit != nil {
				r, o = tt.edit(r, o)
			}
			err := backend.WorkerBarrierReady(r, o)
			if tt.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err %v, want %q", err, tt.want)
			}
		})
	}
}

func TestWorkerBarrierReadyIndependent(t *testing.T) {
	g := exampleWorkerGroup(2, backend.WorkerIndependent)
	refs := []backend.WorkerReference{
		{Group: g, Rank: 0, Pod: examplePod0},
		{Group: g, Rank: 1, Pod: examplePod1},
	}
	got := []backend.WorkerBarrier{
		{Pod: examplePod0, Node: "a", Prepared: true},
		{Pod: examplePod1, Node: "b", Prepared: true},
	}
	if err := backend.WorkerBarrierReady(refs, got); err != nil {
		t.Fatalf("ready: %v", err)
	}
	refs[1].Group.Execution.Binding = "sha256:other"
	if err := backend.WorkerBarrierReady(refs, got); err == nil || !strings.Contains(err.Error(), "members differ in binding") {
		t.Fatalf("other binding: %v", err)
	}
}

func TestWorkerStartDigest(t *testing.T) {
	r := backend.WorkerReference{Group: exampleWorkerGroup(2, backend.WorkerTCP), Rank: 1, Pod: examplePod1, Service: exampleService}
	tok, err := backend.WorkerStartToken(r)
	if err != nil {
		t.Fatal(err)
	}
	g := r.Group
	if d := backend.WorkerStartDigest(g.Execution.ID, g.AttemptID, g.Spec.Mode, r.Rank, g.Spec.Count, r.Pod.UID, g.TaskDigest); d != tok {
		t.Fatal("WorkerStartToken differs from WorkerStartDigest")
	}
	// Length prefixes keep field boundaries: moving a byte between the job
	// and the attempt changes the token.
	if backend.WorkerStartDigest("ab", "c", "tcp", 1, 2, "u", g.TaskDigest) == backend.WorkerStartDigest("a", "bc", "tcp", 1, 2, "u", g.TaskDigest) {
		t.Fatal("field boundaries are ambiguous")
	}
}

func TestValidateWorkerRecovery(t *testing.T) {
	ref := backend.WorkerReference{Group: exampleWorkerGroup(2, backend.WorkerIndependent), Rank: 1, Pod: examplePod1}
	tests := []struct {
		name string
		r    backend.WorkerRecovery
		ok   bool
	}{
		{"terminated", backend.WorkerRecovery{Ref: ref, Evidence: backend.WorkerTerminated, RemoveSent: true}, true},
		{"never bound", backend.WorkerRecovery{Ref: ref, Evidence: backend.WorkerNeverBound, RemoveSent: true}, true},
		{"never released", backend.WorkerRecovery{Ref: ref, Evidence: backend.WorkerNeverReleased, RemoveSent: true}, true},
		{"no evidence", backend.WorkerRecovery{Ref: ref, RemoveSent: true}, false},
		{"no removal sent", backend.WorkerRecovery{Ref: ref, Evidence: backend.WorkerTerminated}, false},
		{"invalid reference", backend.WorkerRecovery{Ref: backend.WorkerReference{Group: ref.Group}, Evidence: backend.WorkerTerminated, RemoveSent: true}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := backend.ValidateWorkerRecovery(tt.r); (err == nil) != tt.ok {
				t.Fatalf("ValidateWorkerRecovery = %v, want ok %v", err, tt.ok)
			}
		})
	}
}

// Dispatch and retirement are independent native facts. These flags are
// MODELED receipts here, not proof that any producer observed a dispatch.
func TestWorkerRemovalDispatchKinds(t *testing.T) {
	group := exampleWorkerGroup(2, backend.WorkerTCP)
	serviceOnly := backend.WorkerReference{Group: group, Service: exampleService}
	podOnly := backend.WorkerReference{Group: exampleWorkerGroup(2, backend.WorkerIndependent), Rank: 1, Pod: examplePod1}
	for _, tt := range []struct {
		name string
		ref  backend.WorkerReference
		got  backend.WorkerRemoval
		ok   bool
	}{
		{"neither dispatched", serviceOnly, backend.WorkerRemoval{Service: exampleService}, true},
		{"service dispatched", serviceOnly, backend.WorkerRemoval{Service: exampleService, ServiceDispatched: true}, true},
		{"unknown pod dispatch", serviceOnly, backend.WorkerRemoval{Service: exampleService, PodDispatched: true}, false},
		{"pod dispatched", podOnly, backend.WorkerRemoval{Pod: examplePod1, PodDispatched: true}, true},
		{"unknown service dispatch", podOnly, backend.WorkerRemoval{Pod: examplePod1, ServiceDispatched: true}, false},
		{"wrong dispatched uid", podOnly, backend.WorkerRemoval{Pod: examplePod0, PodDispatched: true}, false},
		{"positive retirement without dispatch observation", podOnly, backend.WorkerRemoval{Pod: examplePod1, PodRemoved: true}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := backend.ValidateWorkerRemoval(tt.ref, tt.got); (err == nil) != tt.ok {
				t.Fatalf("ValidateWorkerRemoval = %v, want accepted %v", err, tt.ok)
			}
		})
	}
}

func TestWorkerStartDigestVector(t *testing.T) {
	// Independently encoded field boundaries and domain, not another call to
	// WorkerStartToken (which delegates to WorkerStartDigest).
	var task [32]byte
	for i := range task {
		task[i] = byte(i)
	}
	got := backend.WorkerStartDigest("job", "attempt", backend.WorkerTCP, 1, 2, "pod-uid", task)
	const want = "896b0e19515565464ff469b0e0f80042f6c91883e41ab0fb83cacef9848078df"
	if fmt.Sprintf("%x", got) != want {
		t.Fatalf("digest = %x, want %s", got, want)
	}
}

// workerOwnerError is the original producer result in this MODELED custody
// test. The original resource is retained by the error, not a global registry.
type workerOwnerError struct {
	owner *os.File
	cause error
}

func (e *workerOwnerError) Error() string { return "MODELED first Close failure: " + e.cause.Error() }
func (e *workerOwnerError) Unwrap() error { return e.cause }

func TestWorkerUnsettledPreservesOwnerAndCause(t *testing.T) {
	owner, err := os.CreateTemp(t.TempDir(), "original-worker-reader")
	if err != nil {
		t.Fatal(err)
	}
	// Close the real test-owned descriptor once, then model its first Close
	// having returned an error. No retry of Close is used to infer settlement.
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	cause := errors.New("MODELED original Close error")
	original := &workerOwnerError{owner: owner, cause: cause}
	returned := fmt.Errorf("worker log return: %w", errors.Join(backend.ErrWorkerUnsettled, original))
	for _, wrapped := range []error{returned, fmt.Errorf("recorded original result: %w", returned)} {
		var held *workerOwnerError
		if !errors.Is(wrapped, backend.ErrWorkerUnsettled) || !errors.Is(wrapped, cause) || !errors.As(wrapped, &held) || held != original || held.owner != owner {
			t.Fatalf("original owner or cause lost: %v", wrapped)
		}
	}
	// Equal-looking text is not an ownership attestation. A settled transport
	// failure remains an unknown native outcome; its text cannot mark custody.
	if errors.Is(errors.New(backend.ErrWorkerUnsettled.Error()), backend.ErrWorkerUnsettled) {
		t.Fatal("error text classified as custody marker")
	}
	if errors.Is(fmt.Errorf("settled lost response: %w", io.ErrUnexpectedEOF), backend.ErrWorkerUnsettled) {
		t.Fatal("settled native uncertainty classified as unsettled local owner")
	}
}
