package kube

import (
	"bytes"
	"context"
	"errors"
	"mime"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

// generatedUID has the form of the UIDs the API server generates.
const generatedUID = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"

func TestValidInstance(t *testing.T) {
	for _, tt := range []struct {
		name string
		uid  string
		want bool
	}{
		{"native-uuid", generatedUID, true},
		{"opaque", "uid-1", true},
		{"opaque-punctuation", "a.b_c:d/e~1", true},
		{"bound", strings.Repeat("a", 128), true},
		{"empty", "", false},
		{"over-bound", strings.Repeat("a", 129), false},
		{"inner-space", "Not A UID", false},
		{"leading-space", " " + generatedUID, false},
		{"trailing-space", generatedUID + " ", false},
		{"tab", "uid\t1", false},
		{"newline", "uid-1\n", false},
		{"nul", "uid\x001", false},
		{"del", "uid\x7f1", false},
		{"non-ascii", "uid-é", false},
		{"non-ascii-space", "uid 1", false},
	} {
		if got := validInstance(tt.uid); got != tt.want {
			t.Errorf("%s: validInstance(%q) = %v, want %v", tt.name, tt.uid, got, tt.want)
		}
	}
}

// protobufBody encodes obj, whose TypeMeta is set, as the protobuf serializer
// of client-go does.
func protobufBody(t *testing.T, obj runtime.Object) []byte {
	t.Helper()
	info, ok := runtime.SerializerInfoForMediaType(scheme.Codecs.SupportedMediaTypes(), runtime.ContentTypeProtobuf)
	if !ok {
		t.Fatal("no protobuf serializer")
	}
	var buf bytes.Buffer
	if err := info.Serializer.Encode(obj, &buf); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestReceiptAPINegotiatedRequests sends typed client-go writes to the
// receiptAPI fixture with the default configuration, which negotiates
// protobuf request bodies for built-in kinds, and with an explicit JSON
// configuration. In both, the fixture stores the posted spec and enforces
// the deletion's UID precondition as decoded from the request.
func TestReceiptAPINegotiatedRequests(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct {
		name, ctype string // ctype: the configured and expected media type
		want        string
	}{
		{"default", "", runtime.ContentTypeProtobuf},
		{"json", runtime.ContentTypeJSON, runtime.ContentTypeJSON},
	} {
		t.Run(tt.name, func(t *testing.T) {
			api, _, _ := newReceiptAPI(t)
			var mu sync.Mutex
			var sent []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "POST" || r.Method == "DELETE" {
					media, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
					mu.Lock()
					sent = append(sent, media)
					mu.Unlock()
				}
				api.serve(w, r)
			}))
			t.Cleanup(server.Close)
			config := &rest.Config{Host: server.URL}
			config.ContentType = tt.ctype
			cs, err := kubernetes.NewForConfig(config)
			if err != nil {
				t.Fatal(err)
			}
			pods := cs.CoreV1().Pods("workloads")
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "probe", Namespace: "workloads"},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "task", Image: "probe:v1", Args: []string{"a b"}}}}}
			created, err := pods.Create(ctx, pod, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			stored := api.pod("probe")
			if stored == nil || len(stored.Spec.Containers) != 1 || stored.Spec.Containers[0].Image != "probe:v1" ||
				!slices.Equal(stored.Spec.Containers[0].Args, []string{"a b"}) || created.UID != stored.UID {
				t.Fatalf("stored: %+v, created %+v", stored, created)
			}

			other := types.UID("other-uid")
			if err := pods.Delete(ctx, "probe", metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &other}}); !apierrors.IsConflict(err) {
				t.Fatalf("delete with another UID: %v", err)
			}
			if err := pods.Delete(ctx, "probe", metav1.DeleteOptions{}); !apierrors.IsBadRequest(err) {
				t.Fatalf("delete without precondition: %v", err)
			}
			if api.pod("probe") == nil {
				t.Fatal("refused delete removed the pod")
			}
			uid := stored.UID
			if err := pods.Delete(ctx, "probe", metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil {
				t.Fatalf("delete: %v", err)
			}
			if api.pod("probe") != nil {
				t.Fatal("pod not deleted")
			}
			mu.Lock()
			defer mu.Unlock()
			if len(sent) != 4 {
				t.Fatalf("writes: %q", sent)
			}
			for _, media := range sent {
				if media != tt.want {
					t.Fatalf("content types: %q, want %s", sent, tt.want)
				}
			}
		})
	}
}

// TestReceiptAPIRefusesMalformedRequests checks that the fixture refuses a
// protobuf write it cannot decode to the expected kind, and changes nothing.
func TestReceiptAPIRefusesMalformedRequests(t *testing.T) {
	api, url, _ := newReceiptAPI(t)
	send := func(method, path string, body []byte) int {
		t.Helper()
		req, err := http.NewRequest(method, url+path, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", runtime.ContentTypeProtobuf)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	pod := &corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{Name: "probe", Namespace: "workloads"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "task", Image: "probe:v1"}}}}
	podBody := protobufBody(t, pod)

	for name, body := range map[string][]byte{
		"not-protobuf": []byte("not protobuf"),
		"json":         []byte(`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"probe"}}`),
		"truncated":    podBody[:len(podBody)/2],
	} {
		if code := send("POST", podsPath, body); code != http.StatusBadRequest {
			t.Errorf("%s create: status %d", name, code)
		}
	}
	if code := send("POST", jobsPath, podBody); code != http.StatusBadRequest {
		t.Errorf("pod posted as job: status %d", code)
	}
	if api.pod("probe") != nil || api.job("probe") != nil {
		t.Fatal("malformed create stored an object")
	}

	// Positive twin: the same encoding of the expected kind is decoded.
	if code := send("POST", podsPath, podBody); code != http.StatusCreated {
		t.Fatalf("create: status %d", code)
	}
	stored := api.pod("probe")
	if stored == nil || stored.Spec.Containers[0].Image != "probe:v1" {
		t.Fatalf("stored: %+v", stored)
	}

	uid := stored.UID
	opts := &metav1.DeleteOptions{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "DeleteOptions"},
		Preconditions: &metav1.Preconditions{UID: &uid}}
	for name, body := range map[string][]byte{
		"not-protobuf": []byte("not protobuf"),
		"wrong-kind":   podBody,
	} {
		if code := send("DELETE", podsPath+"/probe", body); code != http.StatusBadRequest {
			t.Errorf("%s delete: status %d", name, code)
		}
	}
	if api.pod("probe") == nil {
		t.Fatal("malformed delete removed the pod")
	}
	if code := send("DELETE", podsPath+"/probe", protobufBody(t, opts)); code != http.StatusOK || api.pod("probe") != nil {
		t.Fatalf("delete: status %d", code)
	}
}

// TestReceiptInstanceUIDs creates receipted Pods whose stored UID is set by
// admission. Opaque and native UIDs are pinned and reported; a UID with
// whitespace or control bytes is refused with no witness UID; a stored UID
// later replaced under the same name authorizes nothing.
func TestReceiptInstanceUIDs(t *testing.T) {
	ctx := context.Background()
	setUID := func(uid string) func(any) {
		return func(obj any) { receiptMeta(obj.(runtime.Object)).UID = types.UID(uid) }
	}
	for _, uid := range []string{generatedUID, "uid-opaque_1.A"} {
		t.Run("accepted/"+uid, func(t *testing.T) {
			api, url, root := newReceiptAPI(t)
			api.admit = setUID(uid)
			b := newReceiptBackend(t, root, url)
			ref := receiptRef(b, "Pod")
			if err := b.Start(ctx, ref.ID, ref.Task); err != nil {
				t.Fatal(err)
			}
			if r, err := b.receipts.load(testID); err != nil || r.Phase != phasePinned || r.UID != uid {
				t.Fatalf("receipt: %+v %v", r, err)
			}
			status, err := newReceiptBackend(t, root, url).Observe(ctx, ref)
			if err != nil || status.Instance != uid {
				t.Fatalf("observe: %+v %v", status, err)
			}
		})
	}
	for _, uid := range []string{generatedUID + " ", "uid 1", "uid\t1", "uid-é"} {
		t.Run("refused/"+uid, func(t *testing.T) {
			api, url, root := newReceiptAPI(t)
			api.admit = setUID(uid)
			b := newReceiptBackend(t, root, url)
			ref := receiptRef(b, "Pod")
			if err := b.Start(ctx, ref.ID, ref.Task); !errors.Is(err, backend.ErrIdentityMismatch) {
				t.Fatalf("start: %v", err)
			}
			r, err := b.receipts.load(testID)
			if !errors.Is(err, errReceiptRefused) || r.Phase != phaseRefused || r.UID != "" || r.Witness == nil || r.Witness.UID != "" {
				t.Fatalf("receipt: %+v %v", r, err)
			}
		})
	}
	t.Run("replaced", func(t *testing.T) {
		api, url, root := newReceiptAPI(t)
		api.admit = setUID(generatedUID)
		b := newReceiptBackend(t, root, url)
		ref := receiptRef(b, "Pod")
		if err := b.Start(ctx, ref.ID, ref.Task); err != nil {
			t.Fatal(err)
		}
		ref.Instance = generatedUID
		name := "skygo-" + testID
		for _, replacement := range []string{"7ca7b810-9dad-11d1-80b4-00c04fd430c8", generatedUID + " ", " " + generatedUID} {
			api.meta("Pod", name, func(m *metav1.ObjectMeta) { m.UID = types.UID(replacement) })
			_, writes := api.counts()
			if _, err := b.Observe(ctx, ref); !errors.Is(err, backend.ErrIdentityMismatch) {
				t.Fatalf("%q observe: %v", replacement, err)
			}
			if err := b.Cancel(ctx, ref); err == nil {
				t.Fatalf("%q cancel accepted", replacement)
			}
			if _, w := api.counts(); w != writes || api.pod(name) == nil {
				t.Fatalf("%q reached a write", replacement)
			}
			if r, err := b.receipts.load(testID); err != nil || r.UID != generatedUID || r.Cancel != "" {
				t.Fatalf("%q receipt: %+v %v", replacement, r, err)
			}
		}
		// Positive twin: the pinned UID is accepted again.
		api.meta("Pod", name, func(m *metav1.ObjectMeta) { m.UID = generatedUID })
		if status, err := b.Observe(ctx, ref); err != nil || status.Instance != generatedUID {
			t.Fatalf("restored: %+v %v", status, err)
		}
	})
}
