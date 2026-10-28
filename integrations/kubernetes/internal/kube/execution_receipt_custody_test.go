package kube

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

// The faults and orderings below are MODELED with the store's test hook and
// the loopback API. They are not power-loss, filesystem or native Kubernetes
// qualification.

var errInjectedSync = errors.New("injected root sync fault")

// syncHook is a store fault hook that records every step and, while fail is
// set, fails the constructor's root sync.
type syncHook struct {
	mu    sync.Mutex
	fail  bool
	steps []string
}

func (h *syncHook) fault(step string, _ *executionReceipt) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.steps = append(h.steps, step)
	if step == "rootsync" && h.fail {
		return errInjectedSync
	}
	return nil
}

// synced returns the number of root syncs attempted.
func (h *syncHook) synced() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, s := range h.steps {
		if s == "rootsync" {
			n++
		}
	}
	return n
}

// receiptBackendFault constructs a Backend as newReceiptBackend does, with
// fault set on its store from construction on.
func receiptBackendFault(t *testing.T, root, url string, fault func(string, *executionReceipt) error) (*Backend, error) {
	t.Helper()
	target, frozen, err := fromRESTFrozen(context.Background(), TargetConfig{Name: "test", Namespace: "workloads"}, "owner", &rest.Config{Host: url})
	if err != nil {
		t.Fatal(err)
	}
	client, err := newReceiptClient(frozen)
	if err != nil {
		t.Fatal(err)
	}
	b := target.Backend.(*Backend)
	s, err := openReceiptStoreFault(root, b.owner, b.namespace, b.binding, fault)
	if err != nil {
		return nil, err
	}
	b.client, b.receipts = client, s
	return b, nil
}

// TestReceiptConstructorSyncsRoot fails the root sync of the constructor
// that creates the scope directory, then of constructors that find it. Each
// refuses: no Backend exists, nothing is sent and nothing is recorded. A
// construction whose sync succeeds is accepted, also over a scope holding a
// completed create, whose receipt and grant stay unchanged.
func TestReceiptConstructorSyncsRoot(t *testing.T) {
	ctx := context.Background()
	api, url, root := newReceiptAPI(t)
	probe, err := fromREST(TargetConfig{Name: "test", Namespace: "workloads"}, "owner", &rest.Config{Host: url})
	if err != nil {
		t.Fatal(err)
	}
	pb := probe.Backend.(*Backend)
	scope := filepath.Join(root, receiptScope(pb.owner, pb.namespace, pb.binding))

	first := &syncHook{fail: true}
	if _, err := receiptBackendFault(t, root, url, first.fault); !errors.Is(err, errInjectedSync) || first.synced() != 1 {
		t.Fatalf("creating constructor: %v, %d syncs", err, first.synced())
	}
	if info, err := os.Lstat(scope); err != nil || !info.IsDir() {
		t.Fatalf("scope directory: %v", err)
	}
	// Each retry finds the scope directory and syncs root itself.
	for i := range 2 {
		retry := &syncHook{fail: true}
		if _, err := receiptBackendFault(t, root, url, retry.fault); !errors.Is(err, errInjectedSync) || retry.synced() != 1 {
			t.Fatalf("retry %d over the existing scope: %v, %d syncs", i, err, retry.synced())
		}
	}
	if entries, err := os.ReadDir(scope); err != nil || len(entries) != 0 {
		t.Fatalf("refused constructions wrote state: %v %v", entries, err)
	}
	if n, w := api.counts(); n != 0 || w != 0 {
		t.Fatalf("refused construction reached the API: %d requests, %d writes", n, w)
	}

	// Positive twin: a confirmed construction creates exactly once.
	ok := &syncHook{}
	b, err := receiptBackendFault(t, root, url, ok.fault)
	if err != nil || ok.synced() != 1 {
		t.Fatalf("confirmed construction: %v, %d syncs", err, ok.synced())
	}
	ref := receiptRef(b, "Pod")
	if err := b.Start(ctx, ref.ID, ref.Task); err != nil {
		t.Fatal(err)
	}
	if _, w := api.counts(); w != 1 {
		t.Fatalf("writes: %d", w)
	}
	pinned := receiptBytes(t, b, testID)

	// An existing scope with a completed create: a failed sync still
	// refuses, and neither outcome changes the receipt or grants a create.
	failing := &syncHook{fail: true}
	if _, err := receiptBackendFault(t, root, url, failing.fault); !errors.Is(err, errInjectedSync) || failing.synced() != 1 {
		t.Fatalf("existing scope, failed sync: %v, %d syncs", err, failing.synced())
	}
	again := &syncHook{}
	b2, err := receiptBackendFault(t, root, url, again.fault)
	if err != nil || again.synced() != 1 {
		t.Fatalf("existing scope, confirmed sync: %v, %d syncs", err, again.synced())
	}
	if err := b2.Start(ctx, ref.ID, ref.Task); !errors.Is(err, errReceiptExists) {
		t.Fatalf("restart start: %v", err)
	}
	if _, w := api.counts(); w != 1 || !bytes.Equal(pinned, receiptBytes(t, b2, testID)) {
		t.Fatalf("writes %d or receipt changed", w)
	}
}

// TestReceiptConstructorConcurrentScope holds the creating constructor
// between its mkdir and its root sync while a second constructor finds the
// scope. The second must sync root itself before it is accepted; the first
// then fails its own sync and refuses.
func TestReceiptConstructorConcurrentScope(t *testing.T) {
	f := newReceiptFixture(t)
	scope := filepath.Join(f.dir, receiptScope("owner", "workloads", testBinding))
	entered, hold := newGate(), newGate()
	t.Cleanup(hold.release) // before the fixture joins the first constructor
	first := f.run("creating constructor", func() error {
		_, err := openReceiptStoreFault(f.dir, "owner", "workloads", testBinding, func(step string, _ *executionReceipt) error {
			if step != "rootsync" {
				return nil
			}
			entered.release()
			<-hold.ch
			return errInjectedSync
		})
		return err
	})
	entered.wait(t, "creating constructor before its sync")
	if _, err := os.Lstat(scope); err != nil {
		t.Fatalf("scope directory before the sync: %v", err)
	}
	second := &syncHook{}
	s, err := openReceiptStoreFault(f.dir, "owner", "workloads", testBinding, second.fault)
	if err != nil || second.synced() != 1 {
		t.Fatalf("concurrent constructor: %v, %d syncs", err, second.synced())
	}
	hold.release()
	if err := first.wait(t); !errors.Is(err, errInjectedSync) {
		t.Fatalf("creating constructor accepted without its sync: %v", err)
	}
	if err := s.create(testReceipt(testID)); err != nil {
		t.Fatal(err)
	}
}

// TestReceiptStoreRefusedRecords checks the refused phase: only refuse
// writes it, only from sent or pinned with the receipt's nonce, with a valid
// witness; it is terminal; and the refusing store blocks the attempt even
// when nothing was written.
func TestReceiptStoreRefusedRecords(t *testing.T) {
	witness := receiptWitness{Status: 201, ResponseSHA256: strings.Repeat("e", 64), ResponseBytes: 512, Decode: witnessDecoded, UID: "forged-uid"}
	s, _ := testStore(t)
	if err := s.create(testReceipt(testID)); err != nil {
		t.Fatal(err)
	}
	for name, f := range map[string]func(*executionReceipt){
		"phase-by-update":   func(x *executionReceipt) { x.Phase, x.Witness = phaseRefused, &witness },
		"witness-when-sent": func(x *executionReceipt) { x.Phase, x.Witness = phaseSent, &witness },
	} {
		if _, err := s.update(testID, func(x *executionReceipt) error { f(x); return nil }); !errors.Is(err, errReceiptCorrupt) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if _, err := s.update(testID, func(x *executionReceipt) error { x.Phase = phaseSent; return nil }); err != nil {
		t.Fatal(err)
	}
	// A refusal of another nonce is not recorded, but still blocks this store.
	other := restartedStore(s)
	if err := other.refuse(testID, strings.Repeat("f", 64), witness); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("foreign nonce: %v", err)
	}
	if _, err := other.load(testID); !errors.Is(err, errReceiptRefused) {
		t.Fatalf("blocked load: %v", err)
	}
	if r, err := s.load(testID); err != nil || r.Phase != phaseSent {
		t.Fatalf("unrecorded refusal: %+v %v", r, err)
	}
	invalid := witness
	invalid.ResponseSHA256 = "x"
	if err := restartedStore(s).refuse(testID, strings.Repeat("b", 64), invalid); !errors.Is(err, errReceiptCorrupt) {
		t.Fatalf("invalid witness: %v", err)
	}
	if err := s.refuse(testID, strings.Repeat("b", 64), witness); err != nil {
		t.Fatal(err)
	}
	fresh := restartedStore(s)
	r, err := fresh.load(testID)
	if err != nil || r.Phase != phaseRefused || r.UID != "" || r.Witness == nil || *r.Witness != witness {
		t.Fatalf("refused: %+v %v", r, err)
	}
	if _, err := fresh.update(testID, func(x *executionReceipt) error { x.Cancel = stepIntent; return nil }); !errors.Is(err, errReceiptRefused) {
		t.Fatalf("refused updated: %v", err)
	}
	if err := fresh.retire(testID, func(*executionReceipt) error { return nil }); !errors.Is(err, errReceiptRefused) {
		t.Fatalf("refused retired: %v", err)
	}
	if err := fresh.refuse(testID, strings.Repeat("b", 64), witness); err == nil {
		t.Fatal("refused twice")
	}

	// A persisted witness of any other shape, canonically encoded, is
	// refused on load and authorizes nothing.
	path := receiptFile(s, testID)
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for name, f := range map[string]func(*receiptWitness){
		"no-status":            func(w *receiptWitness) { w.Status = 0 },
		"error-status":         func(w *receiptWitness) { w.Status = 500 },
		"client-error-status":  func(w *receiptWitness) { w.Status = 400 },
		"informational-status": func(w *receiptWitness) { w.Status = 101 },
		"bad-digest":           func(w *receiptWitness) { w.ResponseSHA256 = strings.Repeat("E", 64) },
		"negative-size":        func(w *receiptWitness) { w.ResponseBytes = -1 },
		"oversized":            func(w *receiptWitness) { w.ResponseBytes = maxWitnessBytes + 1 },
		"no-decode":            func(w *receiptWitness) { w.Decode = "" },
		"unknown-decode":       func(w *receiptWitness) { w.Decode = "partial" },
		"undecodable-with-uid": func(w *receiptWitness) { w.Decode = witnessUndecodable },
		"invalid-uid":          func(w *receiptWitness) { w.UID = "Not A UID" },
		"invalid-nonce":        func(w *receiptWitness) { w.Nonce = "nonce" },
	} {
		bad := r
		w := *r.Witness
		f(&w)
		bad.Witness = &w
		data, err := json.Marshal(&bad)
		if err != nil {
			t.Fatal(err)
		}
		writeReceiptBytes(t, path, data)
		if _, err := restartedStore(s).load(testID); !errors.Is(err, errReceiptCorrupt) {
			t.Fatalf("%s witness: %v", name, err)
		}
	}
	// Twins: undecodable witnesses without values, of an empty body and of
	// a complete redirection (C6).
	for _, status := range []int{200, 307} {
		twin := r
		twin.Witness = &receiptWitness{Status: status, ResponseSHA256: strings.Repeat("e", 64), Decode: witnessUndecodable}
		data, err := json.Marshal(&twin)
		if err != nil {
			t.Fatal(err)
		}
		writeReceiptBytes(t, path, data)
		if got, err := restartedStore(s).load(testID); err != nil || *got.Witness != *twin.Witness {
			t.Fatalf("undecodable witness %d: %+v %v", status, got, err)
		}
	}
	writeReceiptBytes(t, path, good)
}

// pinRetry starts retry attempt id and returns its reference with the
// pinned UID as instance.
func pinRetry(t *testing.T, b *Backend, id string) backend.Reference {
	t.Helper()
	ref := receiptRef(b, "Pod")
	ref.ID = id
	if err := b.Start(context.Background(), ref.ID, ref.Task); err != nil {
		t.Fatal(err)
	}
	r, err := b.receipts.load(id)
	if err != nil || r.Phase != phasePinned {
		t.Fatalf("pin: %+v %v", r, err)
	}
	ref.Instance = r.UID
	return ref
}

func runRetryPod(p *corev1.Pod) {
	schedulePod(p, "node-a")
	p.Status.Phase = corev1.PodRunning
}

// retryConsumers are the pinned retry consumers, each with a MODELED Pod
// state in which it would act on an accepted read: report a stopped
// termination, release log bytes, delete a running Pod, delete a stopped one.
var retryConsumers = []struct {
	name  string
	state func(*corev1.Pod)
	run   func(ctx context.Context, b *Backend, ref backend.Reference) error
}{
	{"observe", terminateScheduled, func(ctx context.Context, b *Backend, ref backend.Reference) error {
		_, err := b.Observe(ctx, ref)
		return err
	}},
	{"logs", terminateScheduled, func(ctx context.Context, b *Backend, ref backend.Reference) error {
		chunk, err := b.Logs(ctx, ref, 0, 64)
		if err == nil || len(chunk.Data) != 0 {
			return errors.Join(err, errors.New("log bytes released"))
		}
		return err
	}},
	{"cancel", runRetryPod, func(ctx context.Context, b *Backend, ref backend.Reference) error {
		return b.Cancel(ctx, ref)
	}},
	{"cleanup", terminateScheduled, func(ctx context.Context, b *Backend, ref backend.Reference) error {
		return b.removeExecution(ctx, ref)
	}},
}

// withField returns the encoding of p with field inserted after the first
// occurrence of at.
func withField(p *corev1.Pod, at, field string) []byte {
	data := encodeBody(p)
	i := bytes.Index(data, []byte(at))
	if i < 0 {
		panic("withField: no " + at)
	}
	i += len(at)
	return slices.Concat(data[:i], []byte(field), data[i:])
}

// pinnedChanges change what a GET of the pinned Pod returns, keeping its
// name, namespace, UID, owner and attempt labels, and task and binding
// annotations: everything the C2 metadata check compared.
var pinnedChanges = []struct {
	name  string
	reply func(p *corev1.Pod) any
}{
	{"nonce", func(p *corev1.Pod) any { p.Annotations[nonceAnnotation] = strings.Repeat("0", 64); return p }},
	{"nonce-removed", func(p *corev1.Pod) any { delete(p.Annotations, nonceAnnotation); return p }},
	{"spec-annotation", func(p *corev1.Pod) any { p.Annotations[specAnnotation] = strings.Repeat("0", 64); return p }},
	{"owner-reference", func(p *corev1.Pod) any {
		p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "ReplicaSet", Name: "rs", UID: "rs-uid"}}
		return p
	}},
	{"image", func(p *corev1.Pod) any { p.Spec.Containers[0].Image = "other"; return p }},
	{"args", func(p *corev1.Pod) any { p.Spec.Containers[0].Args = []string{"--exfiltrate"}; return p }},
	{"ephemeral-container", func(p *corev1.Pod) any {
		p.Spec.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug", Image: "busybox"}}}
		return p
	}},
	{"toleration", func(p *corev1.Pod) any {
		p.Spec.Tolerations = append(p.Spec.Tolerations, corev1.Toleration{Key: "dedicated", Operator: corev1.TolerationOpExists})
		return p
	}},
	{"unknown-spec-field", func(p *corev1.Pod) any { return withField(p, `"spec":{`, `"futureField":true,`) }},
}

// retrySnapshot is the state no refused consumer may change: both attempts'
// receipt bytes and stored Pods, the bystander's included, and the writes.
type retrySnapshot struct {
	receipts, pods [2][]byte
	writes         int
}

func snapshotRetry(t *testing.T, api *receiptAPI, b *Backend) retrySnapshot {
	t.Helper()
	var s retrySnapshot
	for i, id := range []string{testID, otherID} {
		s.receipts[i] = receiptBytes(t, b, id)
		s.pods[i], _ = json.Marshal(api.pod("skygo-" + id))
	}
	_, s.writes = api.counts()
	return s
}

func (s retrySnapshot) unchanged(t *testing.T, api *receiptAPI, b *Backend) {
	t.Helper()
	after := snapshotRetry(t, api, b)
	for i := range s.receipts {
		if !bytes.Equal(s.receipts[i], after.receipts[i]) || !bytes.Equal(s.pods[i], after.pods[i]) {
			t.Fatalf("attempt %d receipt or Pod changed", i)
		}
	}
	if after.writes != s.writes {
		t.Fatalf("writes: %d, want %d", after.writes, s.writes)
	}
}

// setupPinnedRetry pins testID and the bystander otherID with admitted Pods
// in state, then returns a Backend of a later process and testID's reference.
func setupPinnedRetry(t *testing.T, state func(*corev1.Pod)) (*receiptAPI, *Backend, backend.Reference, string, string) {
	t.Helper()
	api, url, root := newReceiptAPI(t)
	api.admit = admitParent
	b := newReceiptBackend(t, root, url)
	ref := pinRetry(t, b, testID)
	pinRetry(t, b, otherID)
	api.mu.Lock()
	state(api.pods["skygo-"+testID])
	state(api.pods["skygo-"+otherID])
	api.mu.Unlock()
	return api, newReceiptBackend(t, root, url), ref, root, url
}

// TestReceiptPinnedRetryReadsMatched changes the pinned Pod a GET returns
// and calls each consumer from a later process. Each refuses on its first
// read, before any log request, deletion or receipt write.
func TestReceiptPinnedRetryReadsMatched(t *testing.T) {
	name := "skygo-" + testID
	for _, change := range pinnedChanges {
		for _, c := range retryConsumers {
			t.Run(change.name+"/"+c.name, func(t *testing.T) {
				api, b, ref, _, _ := setupPinnedRetry(t, c.state)
				snap := snapshotRetry(t, api, b)
				api.replyWith(func(_ int, obj any) any {
					p := obj.(*corev1.Pod)
					if p.Name != name {
						return p
					}
					return change.reply(p)
				})
				requests, _ := api.counts()
				if err := c.run(context.Background(), b, ref); !errors.Is(err, backend.ErrIdentityMismatch) {
					t.Fatalf("changed pinned Pod accepted: %v", err)
				}
				if got := api.since(requests); !slices.Equal(got, []string{"GET " + podsPath + "/" + name}) {
					t.Fatalf("requests: %q", got)
				}
				snap.unchanged(t, api, b)
			})
		}
	}
}

// TestReceiptPinnedRetryRecheckFence returns the valid pinned Pod to the
// first GET and a Pod with a changed nonce, otherwise identical, to the
// next. Each recheck is matched by itself: the termination is not reported,
// the log bytes are not released and the stopped Pod is not deleted.
func TestReceiptPinnedRetryRecheckFence(t *testing.T) {
	name := "skygo-" + testID
	get := "GET " + podsPath + "/" + name
	for _, tt := range []struct {
		name string
		run  func(ctx context.Context, b *Backend, ref backend.Reference) error
		want []string
	}{
		{"observe-termination", retryConsumers[0].run, []string{get, get}},
		{"logs-release", retryConsumers[1].run, []string{get, get + "/log", get}},
		{"cleanup-stopped-proof", retryConsumers[3].run, []string{get, get}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			api, b, ref, _, _ := setupPinnedRetry(t, terminateScheduled)
			snap := snapshotRetry(t, api, b)
			api.replyWith(func(n int, obj any) any {
				p := obj.(*corev1.Pod)
				if p.Name == name && n >= 2 {
					p.Annotations[nonceAnnotation] = strings.Repeat("0", 64)
				}
				return p
			})
			requests, _ := api.counts()
			if err := tt.run(context.Background(), b, ref); !errors.Is(err, backend.ErrIdentityMismatch) {
				t.Fatalf("changed recheck accepted: %v", err)
			}
			if got := api.since(requests); !slices.Equal(got, tt.want) {
				t.Fatalf("requests: %q", got)
			}
			snap.unchanged(t, api, b)
		})
	}
}

// TestReceiptPinnedRetryTwins are the positive twins: the unchanged pinned
// Pod, also with an unknown status field, a recovered Pod, and a missing one.
func TestReceiptPinnedRetryTwins(t *testing.T) {
	ctx := context.Background()
	name := "skygo-" + testID
	t.Run("stopped", func(t *testing.T) {
		api, b, ref, _, _ := setupPinnedRetry(t, terminateScheduled)
		bystander := receiptBytes(t, b, otherID)
		api.replyWith(func(_ int, obj any) any { return withField(obj.(*corev1.Pod), `"status":{`, `"futureStatus":1,`) })
		status, err := b.Observe(ctx, ref)
		if err != nil || status.State != backend.Failed || status.Termination != backend.StoppedWithExitCode || status.Instance != ref.Instance {
			t.Fatalf("observe: %+v %v", status, err)
		}
		chunk, err := b.Logs(ctx, ref, 0, 64)
		if err != nil || string(chunk.Data) != "output\n" || !chunk.Complete {
			t.Fatalf("logs: %+v %v", chunk, err)
		}
		if err := b.removeExecution(ctx, ref); err != nil {
			t.Fatal(err)
		}
		if r, err := b.receipts.load(testID); err != nil || r.Phase != phaseRetired || r.UID != ref.Instance || api.pod(name) != nil {
			t.Fatalf("cleanup: %+v %v", r, err)
		}
		if !bytes.Equal(bystander, receiptBytes(t, b, otherID)) || api.pod("skygo-"+otherID) == nil {
			t.Fatal("bystander changed")
		}
	})
	t.Run("running-cancel", func(t *testing.T) {
		api, b, ref, _, _ := setupPinnedRetry(t, runRetryPod)
		if err := b.Cancel(ctx, ref); err != nil {
			t.Fatal(err)
		}
		if r, err := b.receipts.load(testID); err != nil || r.Cancel != stepDelivered || api.pod(name) != nil {
			t.Fatalf("cancel: %+v %v", r, err)
		}
	})
	t.Run("missing", func(t *testing.T) {
		api, b, ref, _, _ := setupPinnedRetry(t, terminateScheduled)
		api.mu.Lock()
		delete(api.pods, name)
		api.mu.Unlock()
		status, err := b.Observe(ctx, ref)
		if err != nil || status.State != backend.Unknown || status.Instance != ref.Instance || status.Reason != "kubernetes retry pod missing" {
			t.Fatalf("observe: %+v %v", status, err)
		}
		if _, err := b.Logs(ctx, ref, 0, 64); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("logs: %v", err)
		}
		if err := b.removeExecution(ctx, ref); err != nil {
			t.Fatal(err)
		}
		if r, err := b.receipts.load(testID); err != nil || r.Phase != phaseRetired {
			t.Fatalf("absence cleanup: %+v %v", r, err)
		}
	})
	t.Run("recovered", func(t *testing.T) {
		api, url, root := newReceiptAPI(t)
		api.admit, api.drop = admitParent, true
		ref := receiptRef(newReceiptBackend(t, root, url), "Pod")
		if err := newReceiptBackend(t, root, url).Start(ctx, ref.ID, ref.Task); err == nil {
			t.Fatal("lost response succeeded")
		}
		api.mu.Lock()
		terminateScheduled(api.pods[name])
		api.mu.Unlock()
		b := newReceiptBackend(t, root, url)
		status, err := b.Observe(ctx, ref)
		uid, _ := api.parent("Pod")
		if err != nil || status.Termination != backend.StoppedWithExitCode || status.Instance != uid {
			t.Fatalf("recovered observe: %+v %v", status, err)
		}
		ref.Instance = uid
		if chunk, err := b.Logs(ctx, ref, 0, 64); err != nil || string(chunk.Data) != "output\n" {
			t.Fatalf("recovered logs: %+v %v", chunk, err)
		}
	})
}

// parentJSON returns the encoding of the stored parent of kind.
func parentJSON(api *receiptAPI, kind string) []byte {
	var data []byte
	if kind == "Job" {
		data, _ = json.Marshal(api.job("skygo-" + testID))
	} else {
		data, _ = json.Marshal(api.pod("skygo-" + testID))
	}
	return data
}

// currentParent returns a copy of the stored parent of kind.
func currentParent(api *receiptAPI, kind string) runtime.Object {
	if kind == "Job" {
		return api.job("skygo-" + testID)
	}
	return api.pod("skygo-" + testID)
}

// refusedOps returns every operation on ref, with and without its stored
// UID as instance.
func refusedOps(ctx context.Context, b *Backend, ref backend.Reference, kind, uid string) map[string]func() error {
	named := ref
	named.Instance = uid
	ops := map[string]func() error{
		"observe":          func() error { _, err := b.Observe(ctx, ref); return err },
		"observe-instance": func() error { _, err := b.Observe(ctx, named); return err },
		"cancel":           func() error { return b.Cancel(ctx, named) },
		"logs":             func() error { _, err := b.Logs(ctx, named, 0, 64); return err },
	}
	if kind == "Pod" {
		ops["cleanup"] = func() error { return b.removeExecution(ctx, named) }
	}
	return ops
}

// TestReceiptRefusedResponseTerminal returns a create response that differs
// from the receipt while the stored object is, or later becomes, exactly
// acceptable to a recovery read. The receipt is durably refused with a
// witness of the actual response, and neither the original Backend nor a
// later one reads, logs, deletes or creates again.
func TestReceiptRefusedResponseTerminal(t *testing.T) {
	zeros := strings.Repeat("0", 64)
	same := func(obj any) any { return obj }
	for _, tt := range []struct {
		name, kind string
		reply      func(obj any) any
		uid        string // witness UID: "stored", a literal or ""
		nonce      string // witness nonce: "receipt", a literal or ""
		spec       string // witness spec digest: "same", "differs" or "" (undecodable)
		notCreated bool   // reply 200
	}{
		{"node-name", "Pod", func(obj any) any { obj.(*corev1.Pod).Spec.NodeName = "node-a"; return obj }, "stored", "receipt", "differs", false},
		{"nonce", "Job", func(obj any) any { receiptMeta(obj.(runtime.Object)).Annotations[nonceAnnotation] = zeros; return obj }, "stored", zeros, "same", false},
		{"nonce", "Pod", func(obj any) any { receiptMeta(obj.(runtime.Object)).Annotations[nonceAnnotation] = zeros; return obj }, "stored", zeros, "same", false},
		{"image", "Job", func(obj any) any { obj.(*batchv1.Job).Spec.Template.Spec.Containers[0].Image = "other:v1"; return obj }, "stored", "receipt", "differs", false},
		{"malformed", "Pod", func(any) any { return []byte(`{"apiVersion":"v1","kind":"Pod","metadata":`) }, "", "", "", false},
		// A strictly decoded body with an invalid UID and no nonce: both
		// are omitted, not synthesized.
		{"invalid-values", "Pod", func(obj any) any {
			m := receiptMeta(obj.(runtime.Object))
			m.UID = "Not A UID"
			delete(m.Annotations, nonceAnnotation)
			return obj
		}, "", "", "same", false},
		// The exact stored object with 200: known, complete, not created.
		{"not-created", "Job", same, "stored", "receipt", "same", true},
		{"not-created", "Pod", same, "stored", "receipt", "same", true},
		// An undecodable 200 is a known response as well.
		{"not-created-malformed", "Job", func(any) any { return []byte("not json") }, "", "", "", true},
	} {
		t.Run(tt.name+"/"+tt.kind, func(t *testing.T) {
			ctx := context.Background()
			api, url, root := newReceiptAPI(t)
			api.admit, api.createReply, api.notCreated = admitParent, tt.reply, tt.notCreated
			b := newReceiptBackend(t, root, url)
			ref := receiptRef(b, tt.kind)
			if err := b.Start(ctx, ref.ID, ref.Task); !errors.Is(err, backend.ErrIdentityMismatch) {
				t.Fatalf("start: %v", err)
			}
			uid, _ := api.parent(tt.kind)
			// Later the stored object reads as fully acceptable.
			api.mu.Lock()
			if p := api.pods["skygo-"+testID]; p != nil {
				schedulePod(p, "node-a")
			}
			replied := api.replied
			api.mu.Unlock()

			r, err := newReceiptBackend(t, root, url).receipts.load(testID)
			if err != nil || r.Phase != phaseRefused || r.UID != "" || r.Witness == nil {
				t.Fatalf("receipt: %+v %v", r, err)
			}
			w := *r.Witness
			sum := sha256.Sum256(replied)
			wantUID, wantNonce := tt.uid, tt.nonce
			if wantUID == "stored" {
				wantUID = uid
			}
			if wantNonce == "receipt" {
				wantNonce = r.Nonce
			}
			wantStatus, wantDecode := 201, witnessDecoded
			if tt.notCreated {
				wantStatus = 200
			}
			if tt.spec == "" {
				wantDecode = witnessUndecodable
			}
			if w.Status != wantStatus || w.ResponseSHA256 != hex.EncodeToString(sum[:]) || w.ResponseBytes != int64(len(replied)) ||
				w.Decode != wantDecode || w.UID != wantUID || w.Nonce != wantNonce || !w.valid() {
				t.Fatalf("witness: %+v", w)
			}
			switch tt.spec {
			case "same":
				if w.SpecSHA256 != r.SpecSHA256 || w.SpecAnnotation != r.SpecSHA256 {
					t.Fatalf("witness spec: %+v", w)
				}
			case "differs":
				if w.SpecSHA256 == "" || w.SpecSHA256 == r.SpecSHA256 || w.SpecAnnotation != r.SpecSHA256 {
					t.Fatalf("witness spec: %+v", w)
				}
			default:
				if w.SpecSHA256 != "" || w.SpecAnnotation != "" {
					t.Fatalf("witness of a malformed response: %+v", w)
				}
			}
			// Control: only the durable refusal stops adoption; the same
			// receipt still sent would accept the stored object.
			sent := r
			sent.Phase, sent.Witness = phaseSent, nil
			if got, err := b.matchReceipted(currentParent(api, tt.kind), sent, true); err != nil || got != uid {
				t.Fatalf("control read: %q %v", got, err)
			}

			before, stored := receiptBytes(t, b, testID), parentJSON(api, tt.kind)
			for i, c := range []*Backend{b, newReceiptBackend(t, root, url)} {
				requests, writes := api.counts()
				if err := c.Start(ctx, ref.ID, ref.Task); !errors.Is(err, errReceiptExists) {
					t.Fatalf("backend %d start: %v", i, err)
				}
				for name, op := range refusedOps(ctx, c, ref, tt.kind, uid) {
					if err := op(); !errors.Is(err, errReceiptRefused) {
						t.Fatalf("backend %d %s: %v", i, name, err)
					}
				}
				if n, w := api.counts(); n != requests || w != writes {
					t.Fatalf("backend %d reached the API", i)
				}
			}
			if !bytes.Equal(before, receiptBytes(t, b, testID)) || !bytes.Equal(stored, parentJSON(api, tt.kind)) {
				t.Fatal("refused attempt changed state")
			}
			if _, w := api.counts(); w != 1 {
				t.Fatalf("writes: %d", w)
			}
		})
	}
	t.Run("valid", func(t *testing.T) {
		api, url, root := newReceiptAPI(t)
		api.admit = admitParent
		b := newReceiptBackend(t, root, url)
		ref := pinRetry(t, b, testID)
		if r, err := b.receipts.load(testID); err != nil || r.Witness != nil || r.UID != ref.Instance {
			t.Fatalf("valid response: %+v %v", r, err)
		}
		if _, w := api.counts(); w != 1 {
			t.Fatalf("writes: %d", w)
		}
	})
	t.Run("definite-rejection", func(t *testing.T) {
		ctx := context.Background()
		api, url, root := newReceiptAPI(t)
		api.createCode, api.createReason = 422, metav1.StatusReasonInvalid
		b := newReceiptBackend(t, root, url)
		ref := receiptRef(b, "Pod")
		if err := b.Start(ctx, ref.ID, ref.Task); err == nil {
			t.Fatal("invalid create accepted")
		}
		if r, err := b.receipts.load(testID); err != nil || r.Phase != phaseRejected || r.Witness != nil {
			t.Fatalf("rejection: %+v %v", r, err)
		}
		_, err := b.Observe(ctx, ref)
		if !errors.Is(err, backend.ErrIdentityMismatch) || errors.Is(err, errReceiptRefused) {
			t.Fatalf("rejected observe: %v", err)
		}
	})
}

// TestReceiptLostResponseRecoversOnce is the twin of the node-name refusal:
// the create response is lost, the Pod is bound by the scheduler, and a later
// read recovers and pins it exactly once.
func TestReceiptLostResponseRecoversOnce(t *testing.T) {
	ctx := context.Background()
	api, url, root := newReceiptAPI(t)
	api.admit, api.drop = admitParent, true
	b := newReceiptBackend(t, root, url)
	ref := receiptRef(b, "Pod")
	if err := b.Start(ctx, ref.ID, ref.Task); err == nil || errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("lost response: %v", err)
	}
	if r, err := b.receipts.load(testID); err != nil || r.Phase != phaseSent || r.Witness != nil {
		t.Fatalf("receipt: %+v %v", r, err)
	}
	api.mu.Lock()
	schedulePod(api.pods["skygo-"+testID], "node-a")
	api.mu.Unlock()
	uid, _ := api.parent("Pod")
	restarted := newReceiptBackend(t, root, url)
	status, err := restarted.Observe(ctx, ref)
	if err != nil || status.Instance != uid {
		t.Fatalf("recovery: %+v %v", status, err)
	}
	r, err := restarted.receipts.load(testID)
	if err != nil || r.Phase != phasePinned || r.UID != uid {
		t.Fatalf("pinned: %+v %v", r, err)
	}
	if status, err := restarted.Observe(ctx, ref); err != nil || status.Instance != uid {
		t.Fatalf("pinned observe: %+v %v", status, err)
	}
	if again, err := restarted.receipts.load(testID); err != nil || again.Generation != r.Generation {
		t.Fatalf("pinned again: %+v %v", again, err)
	}
	if _, w := api.counts(); w != 1 {
		t.Fatalf("writes: %d", w)
	}
}

// TestReceiptRefusalPersistenceFaults fails the write of the refused record
// before and after its rename. Start returns the refusal with the storage
// error, and the original Backend authorizes nothing for the attempt. The
// stored record is the last durable one: refused only if the rename
// happened, uncertain if its directory sync failed, sent otherwise. No
// Backend resends.
func TestReceiptRefusalPersistenceFaults(t *testing.T) {
	ctx := context.Background()
	errInjected := errors.New("injected refusal write fault")
	for _, tt := range []struct {
		step      string
		renamed   bool
		uncertain bool
	}{{"sync", false, false}, {"rename", false, false}, {"dirsync", true, true}} {
		t.Run(tt.step, func(t *testing.T) {
			api, url, root := newReceiptAPI(t)
			api.admit = admitParent
			api.createReply = func(obj any) any { obj.(*corev1.Pod).Spec.NodeName = "node-a"; return obj }
			b := newReceiptBackend(t, root, url)
			b.receipts.fault = func(step string, r *executionReceipt) error {
				if step == tt.step && r != nil && r.Phase == phaseRefused {
					return errInjected
				}
				return nil
			}
			ref := receiptRef(b, "Pod")
			err := b.Start(ctx, ref.ID, ref.Task)
			var uncertain receiptUncertain
			if !errors.Is(err, backend.ErrIdentityMismatch) || !errors.Is(err, errInjected) || errors.As(err, &uncertain) != tt.uncertain {
				t.Fatalf("start: %v", err)
			}
			uid, _ := api.parent("Pod")
			api.mu.Lock()
			schedulePod(api.pods["skygo-"+testID], "node-a")
			api.mu.Unlock()
			if _, err := os.Stat(filepath.Join(b.receipts.dir, testID+".tmp")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("temporary file: %v", err)
			}

			// The original Backend is blocked whatever was written.
			requests, writes := api.counts()
			if _, err := b.receipts.load(testID); !errors.Is(err, errReceiptRefused) {
				t.Fatalf("original load: %v", err)
			}
			for name, op := range refusedOps(ctx, b, ref, "Pod", uid) {
				if err := op(); !errors.Is(err, errReceiptRefused) {
					t.Fatalf("original %s: %v", name, err)
				}
			}
			if n, w := api.counts(); n != requests || w != writes {
				t.Fatal("blocked attempt reached the API")
			}

			// Another process sees only the record. Without the rename the
			// refusal reached no record: it is not notified.
			restarted := newReceiptBackend(t, root, url)
			r, err := restarted.receipts.load(testID)
			if tt.renamed {
				if err != nil || r.Phase != phaseRefused || r.Witness == nil || r.Witness.UID != uid {
					t.Fatalf("renamed refusal: %+v %v", r, err)
				}
				if _, err := restarted.Observe(ctx, ref); !errors.Is(err, errReceiptRefused) {
					t.Fatalf("restarted observe: %v", err)
				}
				if n, _ := api.counts(); n != requests {
					t.Fatal("refused record reached the API")
				}
			} else if err != nil || r.Phase != phaseSent || r.Witness != nil {
				t.Fatalf("unrenamed refusal: %+v %v", r, err)
			}
			if err := restarted.Start(ctx, ref.ID, ref.Task); !errors.Is(err, errReceiptExists) {
				t.Fatalf("restart start: %v", err)
			}
			if _, w := api.counts(); w != 1 {
				t.Fatalf("writes: %d", w)
			}
		})
	}
}

// TestReceiptRefusalAfterConcurrentPin holds a create response after the
// object is stored. A recovery read of another Backend pins the valid stored
// object first; the held response then arrives, invalid by itself (a node
// name) or conflicting with the pinned UID, and still refuses the attempt,
// keeping the pinned UID beside the response's witness.
func TestReceiptRefusalAfterConcurrentPin(t *testing.T) {
	for _, tt := range []struct {
		name       string
		reply      func(obj any) any
		witnessUID string // "" for the stored UID
	}{
		{"node-name", func(obj any) any { obj.(*corev1.Pod).Spec.NodeName = "node-a"; return obj }, ""},
		{"conflicting-uid", func(obj any) any { obj.(*corev1.Pod).UID = "forged-uid"; return obj }, "forged-uid"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			api, url, root := newReceiptAPI(t)
			api.admit, api.holdStored, api.createReply = admitParent, true, tt.reply
			api.hold(true)
			b := newReceiptBackend(t, root, url)
			other := newReceiptBackend(t, root, url)
			ref := receiptRef(b, "Pod")
			start := api.fixture.run("start", func() error { return b.Start(ctx, ref.ID, ref.Task) })
			api.createEntered.wait(t, "create stored")
			uid, _ := api.parent("Pod")
			if status, err := other.Observe(ctx, ref); err != nil || status.Instance != uid {
				t.Fatalf("concurrent recovery: %+v %v", status, err)
			}
			if r, err := other.receipts.load(testID); err != nil || r.Phase != phasePinned || r.UID != uid {
				t.Fatalf("concurrent pin: %+v %v", r, err)
			}
			api.holdCreate.release()
			if err := start.wait(t); !errors.Is(err, backend.ErrIdentityMismatch) {
				t.Fatalf("start: %v", err)
			}
			api.createDone.wait(t, "create done")
			witnessUID := tt.witnessUID
			if witnessUID == "" {
				witnessUID = uid
			}
			r, err := other.receipts.load(testID)
			if err != nil || r.Phase != phaseRefused || r.UID != uid || r.Witness == nil || r.Witness.UID != witnessUID || r.Witness.Nonce != r.Nonce {
				t.Fatalf("refusal after pin: %+v %v", r, err)
			}
			requests, _ := api.counts()
			for name, op := range refusedOps(ctx, other, ref, "Pod", uid) {
				if err := op(); !errors.Is(err, errReceiptRefused) {
					t.Fatalf("%s after refusal: %v", name, err)
				}
			}
			if n, w := api.counts(); n != requests || w != 1 {
				t.Fatalf("requests %d (want %d), writes %d", n, requests, w)
			}
		})
	}
}
