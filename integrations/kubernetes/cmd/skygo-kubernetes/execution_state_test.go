//go:build linux || darwin

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/server"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
)

// Source-only; NOT_RUN. These tests build targets through the command's own
// server.Config.Targets, as server.Run does, against a loopback API server
// that stores what clients send and runs no controllers. API status and
// controller behavior are MODELED; no cluster or native object is involved.
// Every command and Backend call takes a context bounded by stateTimeout and
// is synchronous: it has returned before the test function returns. Each test
// defers closing its server, which waits for its handlers to return; the
// deferred close runs before t.Cleanup removes the temporary directories.

// stateTimeout bounds each test's context. Expiry fails the call; the test
// still waits for the call to return.
const stateTimeout = time.Minute

const (
	stateJobID   = "0123456789abcdef0123456789abcdef"
	stateRetryID = "abcdef0123456789abcdef0123456789"
	stateOwner   = "persistent-owner"
	stateNonce   = "skygo.tmc.dev/creation-nonce"
	stateSpec    = "skygo.tmc.dev/spec-sha256"
)

func stateJobTask() backend.Task {
	return backend.Task{Argv: []string{"trainer"}, Image: "trainer:v1"}
}

func stateRetryTask() backend.Task {
	return backend.Task{Argv: []string{"worker"}, Image: "image", Retry: &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7}}}
}

// stateAPI is a loopback API server for Jobs and Pods in any namespace.
type stateAPI struct {
	mu       sync.Mutex
	jobs     map[string]*batchv1.Job // by namespace/name
	pods     map[string]*corev1.Pod
	requests int
	posts    int
	uid      int

	srv    *httptest.Server
	closed sync.Once
}

// newStateAPI starts a loopback API server. The caller must defer close, so
// that the server and its handlers have returned before t.Cleanup removes
// any temporary directory of the test, whatever the order in which the
// directories were created. The Cleanup registered here covers only a caller
// that returns without its deferred close.
func newStateAPI(t *testing.T) (*stateAPI, string) {
	t.Helper()
	api := &stateAPI{jobs: make(map[string]*batchv1.Job), pods: make(map[string]*corev1.Pod)}
	api.srv = httptest.NewServer(http.HandlerFunc(api.serve))
	t.Cleanup(api.close)
	return api, api.srv.URL
}

// close closes the server once. httptest.Server.Close returns only after
// every outstanding request handler has returned.
func (a *stateAPI) close() {
	a.closed.Do(a.srv.Close)
}

func stateStatus(w http.ResponseWriter, code int, reason metav1.StatusReason) {
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Code: int32(code), Reason: reason})
}

func (a *stateAPI) serve(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.requests++
	w.Header().Set("Content-Type", "application/json")
	job := strings.HasPrefix(r.URL.Path, "/apis/batch/v1/namespaces/")
	rest := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, "/apis/batch/v1/namespaces/"), "/api/v1/namespaces/")
	parts := strings.Split(rest, "/")
	resource := "pods"
	if job {
		resource = "jobs"
	}
	if rest == r.URL.Path || len(parts) < 2 || len(parts) > 3 || parts[1] != resource {
		stateStatus(w, http.StatusNotFound, metav1.StatusReasonNotFound)
		return
	}
	key := parts[0] + "/"
	switch {
	case len(parts) == 2 && r.Method == "POST":
		a.posts++
		a.create(w, r, job, key)
	case len(parts) == 2 && r.Method == "GET" && !job:
		a.list(w, r, key)
	case len(parts) == 3 && r.Method == "GET":
		key += parts[2]
		if j := a.jobs[key]; job && j != nil {
			json.NewEncoder(w).Encode(j)
		} else if p := a.pods[key]; !job && p != nil {
			json.NewEncoder(w).Encode(p)
		} else {
			stateStatus(w, http.StatusNotFound, metav1.StatusReasonNotFound)
		}
	default:
		stateStatus(w, http.StatusMethodNotAllowed, metav1.StatusReasonMethodNotAllowed)
	}
}

// errStateMedia reports a create body of a media type the fixture does not
// decode.
var errStateMedia = errors.New("unsupported media type")

// stateDecode decodes raw, the body of a create sent with Content-Type
// ctype, into into, whose kind is want, with the client-go serializer for
// that media type. Typed client-go creates of built-in kinds are sent as
// protobuf unless the configuration sets a content type; receipted creates
// are sent as JSON. Another media type, a body that does not decode, and a
// body of another kind are refused.
func stateDecode(ctype string, raw []byte, want schema.GroupVersionKind, into runtime.Object) error {
	media, _, err := mime.ParseMediaType(ctype)
	if err != nil || media != runtime.ContentTypeJSON && media != runtime.ContentTypeProtobuf {
		return fmt.Errorf("%w %q", errStateMedia, ctype)
	}
	info, ok := runtime.SerializerInfoForMediaType(scheme.Codecs.SupportedMediaTypes(), media)
	if !ok {
		return fmt.Errorf("%w %q", errStateMedia, ctype)
	}
	obj, gvk, err := info.Serializer.Decode(raw, nil, into)
	if err != nil {
		return err
	}
	if obj != into || gvk == nil || *gvk != want {
		return fmt.Errorf("decoded %v %T, want %v %T", gvk, obj, want, into)
	}
	return nil
}

// stateRefuse replies to a create whose body stateDecode refused.
func stateRefuse(w http.ResponseWriter, err error) {
	if errors.Is(err, errStateMedia) {
		stateStatus(w, http.StatusUnsupportedMediaType, metav1.StatusReasonUnsupportedMediaType)
		return
	}
	stateStatus(w, http.StatusBadRequest, metav1.StatusReasonBadRequest)
}

// create stores the posted object with a new UID and returns it with 201.
// The body is decoded as its Content-Type states and must be of the kind
// the path names; a refused body stores nothing.
func (a *stateAPI) create(w http.ResponseWriter, r *http.Request, job bool, prefix string) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		stateStatus(w, http.StatusBadRequest, metav1.StatusReasonBadRequest)
		return
	}
	ctype := r.Header.Get("Content-Type")
	a.uid++
	uid := types.UID(fmt.Sprintf("uid-%d", a.uid))
	var obj any
	if job {
		j := new(batchv1.Job)
		if err := stateDecode(ctype, raw, batchv1.SchemeGroupVersion.WithKind("Job"), j); err != nil {
			stateRefuse(w, err)
			return
		}
		if a.jobs[prefix+j.Name] != nil {
			stateStatus(w, http.StatusConflict, metav1.StatusReasonAlreadyExists)
			return
		}
		j.TypeMeta = metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"}
		j.UID, j.ResourceVersion = uid, "1"
		a.jobs[prefix+j.Name], obj = j, j
	} else {
		p := new(corev1.Pod)
		if err := stateDecode(ctype, raw, corev1.SchemeGroupVersion.WithKind("Pod"), p); err != nil {
			stateRefuse(w, err)
			return
		}
		if a.pods[prefix+p.Name] != nil {
			stateStatus(w, http.StatusConflict, metav1.StatusReasonAlreadyExists)
			return
		}
		p.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}
		p.UID, p.ResourceVersion = uid, "1"
		a.pods[prefix+p.Name], obj = p, p
	}
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(obj)
}

func (a *stateAPI) list(w http.ResponseWriter, r *http.Request, prefix string) {
	sel, err := labels.Parse(r.URL.Query().Get("labelSelector"))
	if err != nil {
		stateStatus(w, http.StatusBadRequest, metav1.StatusReasonBadRequest)
		return
	}
	list := corev1.PodList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodList"}}
	var keys []string
	for key := range a.pods {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		if p := a.pods[key]; strings.HasPrefix(key, prefix) && sel.Matches(labels.Set(p.Labels)) {
			list.Items = append(list.Items, *p)
		}
	}
	json.NewEncoder(w).Encode(list)
}

func (a *stateAPI) counts() (requests, posts int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.requests, a.posts
}

// meta returns a copy of the metadata of the stored Job or Pod, or nil.
func (a *stateAPI) meta(kind, namespace, name string) *metav1.ObjectMeta {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := namespace + "/" + name
	if j := a.jobs[key]; kind == "Job" && j != nil {
		return j.ObjectMeta.DeepCopy()
	}
	if p := a.pods[key]; kind == "Pod" && p != nil {
		return p.ObjectMeta.DeepCopy()
	}
	return nil
}

// stateTargets writes a kubeconfig for url and a targets file listing
// entries, and returns the targets file.
func stateTargets(t *testing.T, url, entries string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "kubeconfig"), []byte(`apiVersion: v1
kind: Config
clusters:
- name: test
  cluster: {server: "`+url+`"}
contexts:
- name: chosen
  context: {cluster: test, user: test}
users:
- name: test
  user: {}
`), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "targets.yaml")
	if err := os.WriteFile(path, []byte("version: 1\ntargets:\n"+entries), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

// plainTarget is a targets file entry named after its namespace.
func plainTarget(namespace string) string {
	return fmt.Sprintf("- name: %[1]s\n  namespace: %[1]s\n  kubeconfig: ./kubeconfig\n  context: chosen\n", namespace)
}

// stateRoot returns an operator-prepared state directory: existing,
// absolute, without symlinks, mode 0700.
func stateRoot(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(dir, "state")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	return root
}

// serverTargets runs the server command with args under ctx and returns
// what the configuration's Targets builds for the persistent owner.
func serverTargets(t *testing.T, ctx context.Context, args ...string) ([]backend.Target, error) {
	t.Helper()
	var targets []backend.Target
	var err error
	called := false
	cmd := command(func(_ context.Context, cfg server.Config) error {
		called = true
		if cfg.Install != nil || cfg.Targets == nil {
			t.Fatal("command did not select finite targets")
		}
		targets, err = cfg.Targets(stateOwner)
		return nil
	})
	cmd.SetArgs(append([]string{"server", "--disable-local"}, args...))
	if err := cmd.ExecuteContext(ctx); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("server not called")
	}
	return targets, err
}

func serverTarget(t *testing.T, ctx context.Context, args ...string) backend.Target {
	t.Helper()
	targets, err := serverTargets(t, ctx, args...)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 1 {
		t.Fatalf("got %d targets", len(targets))
	}
	return targets[0]
}

type stateReceipt struct {
	Owner     string `json:"owner"`
	Namespace string `json:"namespace"`
	Binding   string `json:"binding"`
	Nonce     string `json:"nonce"`
	Phase     string `json:"phase"`
	UID       string `json:"uid"`
}

// stateReceipts returns the receipts of attempt id in every scope of root.
func stateReceipts(t *testing.T, root, id string) []stateReceipt {
	t.Helper()
	scopes, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	var receipts []stateReceipt
	for _, scope := range scopes {
		data, err := os.ReadFile(filepath.Join(root, scope.Name(), id+".json"))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		var r stateReceipt
		if err := json.Unmarshal(data, &r); err != nil {
			t.Fatal(err)
		}
		receipts = append(receipts, r)
	}
	return receipts
}

func emptyDir(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("%s: %d entries, %v", dir, len(entries), err)
	}
}

// TestExecutionStateCommand checks that --execution-state selects receipted
// targets: an ordinary Job and a retry Pod are each created once with a
// nonce whose receipt pins the returned UID, and a server rebuilt from the
// same flags recovers that UID without a second create.
func TestExecutionStateCommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), stateTimeout)
	defer cancel()
	api, url := newStateAPI(t)
	defer api.close()
	root := stateRoot(t)
	args := []string{"--targets", stateTargets(t, url, plainTarget("workloads")), "--execution-state", root}
	for _, tt := range []struct {
		kind, id string
		task     backend.Task
	}{
		{"Job", stateJobID, stateJobTask()},
		{"Pod", stateRetryID, stateRetryTask()},
	} {
		t.Run(tt.kind, func(t *testing.T) {
			first := serverTarget(t, ctx, args...)
			if err := first.Backend.Start(ctx, tt.id, tt.task); err != nil {
				t.Fatal(err)
			}
			_, posts := api.counts()
			meta := api.meta(tt.kind, "workloads", "skygo-"+tt.id)
			if meta == nil || len(meta.Annotations[stateNonce]) != 64 || meta.Annotations[stateSpec] == "" {
				t.Fatalf("receipted create: %+v", meta)
			}
			uid := string(meta.UID)
			r := stateReceipts(t, root, tt.id)
			if len(r) != 1 || r[0].Phase != "pinned" || r[0].UID != uid || r[0].Nonce != meta.Annotations[stateNonce] ||
				r[0].Owner != stateOwner || r[0].Namespace != "workloads" || r[0].Binding != first.Binding {
				t.Fatalf("receipt: %+v", r)
			}

			// A restarted server builds the same owner, binding and scope.
			restarted := serverTarget(t, ctx, args...)
			if restarted.Binding != first.Binding {
				t.Fatal("restarted binding differs")
			}
			ref := backend.Reference{ID: tt.id, Task: tt.task, Binding: restarted.Binding}
			status, err := restarted.Backend.Observe(ctx, ref)
			if err != nil || status.State != backend.Starting || status.Instance != uid {
				t.Fatalf("restart observe: %+v %v", status, err)
			}
			ref.Instance = status.Instance
			if status, err := restarted.Backend.Observe(ctx, ref); err != nil || status.Instance != uid {
				t.Fatalf("pinned observe: %+v %v", status, err)
			}
			if err := restarted.Backend.Start(ctx, tt.id, tt.task); err == nil {
				t.Fatal("second create accepted")
			}
			if _, n := api.counts(); n != posts {
				t.Fatal("restart sent a second create")
			}
		})
	}
}

// TestExecutionStateOmitted checks that without the flag the same targets
// file builds the existing stateless targets: creates carry no receipt
// annotations and no state is written, while target facts and binding are
// those of the receipted target.
func TestExecutionStateOmitted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), stateTimeout)
	defer cancel()
	api, url := newStateAPI(t)
	defer api.close()
	targets := stateTargets(t, url, plainTarget("workloads"))
	root := stateRoot(t)
	legacy := serverTarget(t, ctx, "--targets", targets)
	if err := legacy.Backend.Start(ctx, stateJobID, stateJobTask()); err != nil {
		t.Fatal(err)
	}
	meta := api.meta("Job", "workloads", "skygo-"+stateJobID)
	if meta == nil || meta.Annotations[stateNonce] != "" || meta.Annotations[stateSpec] != "" {
		t.Fatalf("legacy create: %+v", meta)
	}
	if _, err := legacy.Backend.Observe(ctx, backend.Reference{ID: stateJobID, Task: stateJobTask(), Binding: legacy.Binding}); err != nil {
		t.Fatal(err)
	}
	emptyDir(t, root)

	receipted := serverTarget(t, ctx, "--targets", targets, "--execution-state", root)
	if legacy.Name != receipted.Name || legacy.Kind != receipted.Kind || legacy.Binding != receipted.Binding ||
		!legacy.SupportsRetry || !receipted.SupportsRetry || legacy.SupportsRemoteData || receipted.SupportsRemoteData ||
		legacy.SupportsPersistentEnvironments || receipted.SupportsPersistentEnvironments ||
		legacy.Retention == nil || receipted.Retention == nil {
		t.Fatal("receipted target facts differ")
	}
}

// TestExecutionStateFlagRefused checks flag combinations that fail before
// the server runs and before any configuration file is read.
func TestExecutionStateFlagRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), stateTimeout)
	defer cancel()
	dir := t.TempDir()
	absent := filepath.Join(dir, "absent.yaml")
	for _, tt := range []struct {
		name string
		args []string
		want string
	}{
		{"without targets", []string{"--namespace", "tasks", "--execution-state", "secret-canary"}, "--execution-state requires --targets"},
		{"empty without targets", []string{"--namespace", "tasks", "--execution-state", ""}, "--execution-state requires --targets"},
		{"alone", []string{"--execution-state", dir}, "--execution-state requires --targets"},
		{"profiles", []string{"--targets", absent, "--execution-state", dir, "--environment-profiles", absent, "--auth-file", "private-auth.json"}, "conflicts with --environment-profiles"},
		{"empty profiles", []string{"--targets", absent, "--execution-state", dir, "--environment-profiles", "", "--auth-file", "private-auth.json"}, "conflicts with --environment-profiles"},
		{"empty both", []string{"--targets", absent, "--execution-state", "", "--environment-profiles", "secret-canary"}, "conflicts with --environment-profiles"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cmd := command(func(context.Context, server.Config) error {
				t.Fatal("server called for refused flags")
				return nil
			})
			cmd.SetArgs(append([]string{"server"}, tt.args...))
			err := cmd.ExecuteContext(ctx)
			if err == nil || !strings.Contains(err.Error(), tt.want) || strings.Contains(err.Error(), "secret-canary") || strings.Contains(err.Error(), dir) {
				t.Fatalf("error = %v, want %q", err, tt.want)
			}
		})
	}
}

// TestExecutionStateRootRefused checks that the receipt constructor refuses
// an unusable state directory when the server builds its targets, without
// an API request and without creating state.
func TestExecutionStateRootRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), stateTimeout)
	defer cancel()
	api, url := newStateAPI(t)
	defer api.close()
	targets := stateTargets(t, url, plainTarget("workloads"))
	root := stateRoot(t)
	dir := filepath.Dir(root)
	unsafe := filepath.Join(dir, "unsafe")
	if err := os.Mkdir(unsafe, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(unsafe, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(dir, "missing")
	for _, tt := range []struct{ name, state, want string }{
		{"empty", "", "must be absolute"},
		{"relative", "state", "must be absolute"},
		{"unclean", root + "/", "must be absolute"},
		{"missing", missing, "kubernetes execution state directory"},
		{"unsafe", unsafe, "unsafe kubernetes execution state permissions"},
		{"symlink", link, "must not contain symlinks"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := serverTargets(t, ctx, "--targets", targets, "--execution-state", tt.state)
			if err == nil || !strings.Contains(err.Error(), tt.want) || got != nil {
				t.Fatalf("error = %v, want %q", err, tt.want)
			}
		})
	}
	if n, _ := api.counts(); n != 0 {
		t.Fatalf("%d API requests", n)
	}
	emptyDir(t, root)
	emptyDir(t, unsafe)
	if _, err := os.Lstat(missing); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing state directory: %v", err)
	}
}

// stateDataPEM is a deterministic self-signed Ed25519 fixture CA.
const stateDataPEM = `-----BEGIN CERTIFICATE-----
MIIBMDCB46ADAgECAgEBMAUGAytlcDAgMR4wHAYDVQQDExVza3lnbyBkYXRhIGZp
eHR1cmUgQ0EwHhcNMjYwMTAxMDAwMDAwWhcNMzYwMTAxMDAwMDAwWjAgMR4wHAYD
VQQDExVza3lnbyBkYXRhIGZpeHR1cmUgQ0EwKjAFBgMrZXADIQB5tVYuj+ZU+UB4
sRLoqYunkB+FOuaVvtfg45ELrQSWZKNCMEAwDgYDVR0PAQH/BAQDAgIEMA8GA1Ud
EwEB/wQFMAMBAf8wHQYDVR0OBBYEFE4c8bsVIM0NmpnuH0rnUhZH3WpTMAUGAytl
cANBADPx7UmzZJAGEz6/Q9eTZTJyvSdMlDQTX+9alOm/VBXFxrNCIQaisjutys3P
wtVscj0xRSl9SQeWu2HNVN25cw0=
-----END CERTIFICATE-----
`

// TestExecutionStateDataRefused checks that a valid target with a data block
// is refused by the receipt constructor before any request.
func TestExecutionStateDataRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), stateTimeout)
	defer cancel()
	api, url := newStateAPI(t)
	defer api.close()
	var ca strings.Builder
	for _, line := range strings.SplitAfter(stateDataPEM, "\n") {
		if line != "" {
			ca.WriteString("      " + line)
		}
	}
	targets := stateTargets(t, url, "- name: worker\n  namespace: workloads\n  kubeconfig: ./kubeconfig\n  context: chosen\n  data:\n"+
		"    helper_image: registry.example.com/skygo/helper@sha256:"+strings.Repeat("ab", 32)+"\n"+
		"    helper_sha256: "+strings.Repeat("cd", 32)+"\n"+
		"    origin: https://data.example.com:443\n"+
		"    ca_pem: |\n"+ca.String()+
		"    task_images:\n    - registry.example.com/team/train@sha256:"+strings.Repeat("0a", 32)+"\n"+
		"    references:\n    - name: models\n      pvc: model-cache\n      destinations: [/mnt/skygo/models]\n")
	root := stateRoot(t)
	got, err := serverTargets(t, ctx, "--targets", targets, "--execution-state", root)
	if err == nil || err.Error() != "kubernetes execution receipts do not cover data targets" || got != nil {
		t.Fatalf("error = %v", err)
	}
	if n, _ := api.counts(); n != 0 {
		t.Fatalf("%d API requests", n)
	}
	emptyDir(t, root)
}

// TestExecutionStateLegacyAttempt checks that an attempt created without a
// receipt, here by a server started without the flag, is not adopted after
// a restart with it: Observe and Cancel refuse before any request, nothing
// is resent and the object remains.
func TestExecutionStateLegacyAttempt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), stateTimeout)
	defer cancel()
	api, url := newStateAPI(t)
	defer api.close()
	targets := stateTargets(t, url, plainTarget("workloads"))
	root := stateRoot(t)
	legacy := serverTarget(t, ctx, "--targets", targets)
	if err := legacy.Backend.Start(ctx, stateJobID, stateJobTask()); err != nil {
		t.Fatal(err)
	}
	receipted := serverTarget(t, ctx, "--targets", targets, "--execution-state", root)
	requests, _ := api.counts()
	ref := backend.Reference{ID: stateJobID, Task: stateJobTask(), Binding: receipted.Binding}
	if _, err := receipted.Backend.Observe(ctx, ref); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("observe: %v", err)
	}
	if err := receipted.Backend.Cancel(ctx, ref); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("cancel: %v", err)
	}
	if n, _ := api.counts(); n != requests {
		t.Fatal("unreceipted attempt reached the API")
	}
	if api.meta("Job", "workloads", "skygo-"+stateJobID) == nil {
		t.Fatal("legacy job removed")
	}
	if r := stateReceipts(t, root, stateJobID); len(r) != 0 {
		t.Fatalf("receipt synthesized: %+v", r)
	}
}

// TestExecutionStateSharedRoot checks that named targets share one state
// directory through separate scopes, one per namespace and binding.
func TestExecutionStateSharedRoot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), stateTimeout)
	defer cancel()
	api, url := newStateAPI(t)
	defer api.close()
	root := stateRoot(t)
	args := []string{"--targets", stateTargets(t, url, plainTarget("first")+plainTarget("second")), "--execution-state", root}
	targets, err := serverTargets(t, ctx, args...)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 2 || targets[0].Binding == targets[1].Binding {
		t.Fatal("named targets differ")
	}
	for _, target := range targets {
		if err := target.Backend.Start(ctx, stateJobID, stateJobTask()); err != nil {
			t.Fatal(err)
		}
	}
	if _, posts := api.counts(); posts != 2 {
		t.Fatalf("%d creates", posts)
	}
	if scopes, err := os.ReadDir(root); err != nil || len(scopes) != 2 {
		t.Fatalf("scopes: %d %v", len(scopes), err)
	}
	r := stateReceipts(t, root, stateJobID)
	if len(r) != 2 || r[0].Nonce == r[1].Nonce || r[0].UID == r[1].UID {
		t.Fatalf("receipts: %+v", r)
	}

	restarted, err := serverTargets(t, ctx, args...)
	if err != nil {
		t.Fatal(err)
	}
	for i, target := range restarted {
		meta := api.meta("Job", target.Name, "skygo-"+stateJobID)
		ref := backend.Reference{ID: stateJobID, Task: stateJobTask(), Binding: target.Binding}
		status, err := target.Backend.Observe(ctx, ref)
		if err != nil || meta == nil || status.Instance != string(meta.UID) {
			t.Fatalf("%s: %+v %v", target.Name, status, err)
		}
		// Another target's binding is refused before any request.
		requests, _ := api.counts()
		ref.Binding = restarted[1-i].Binding
		if _, err := target.Backend.Observe(ctx, ref); !errors.Is(err, backend.ErrIdentityMismatch) {
			t.Fatalf("%s foreign binding: %v", target.Name, err)
		}
		if n, _ := api.counts(); n != requests {
			t.Fatal("foreign binding reached the API")
		}
	}
}

// stateBody encodes obj, whose TypeMeta is set, with the client-go
// serializer for media.
func stateBody(t *testing.T, media string, obj runtime.Object) []byte {
	t.Helper()
	info, ok := runtime.SerializerInfoForMediaType(scheme.Codecs.SupportedMediaTypes(), media)
	if !ok {
		t.Fatalf("no serializer for %s", media)
	}
	var buf bytes.Buffer
	if err := info.Serializer.Encode(obj, &buf); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestExecutionStateAPICreate checks the fixture's create decoding apart from
// any backend: a Job or Pod sent as JSON or protobuf is stored as sent, and
// another media type, a body that does not decode as its stated media type,
// and a body of another kind or version are refused without storing.
func TestExecutionStateAPICreate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), stateTimeout)
	defer cancel()
	api, url := newStateAPI(t)
	defer api.close()
	const (
		jobs  = "/apis/batch/v1/namespaces/%s/jobs"
		pods  = "/api/v1/namespaces/%s/pods"
		js    = runtime.ContentTypeJSON
		pb    = runtime.ContentTypeProtobuf
		image = "registry.example.com/team/probe:v1"
	)
	spec := corev1.PodSpec{Containers: []corev1.Container{{Name: "task", Image: image, Args: []string{"a b"}}}}
	job := &batchv1.Job{TypeMeta: metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"}, ObjectMeta: metav1.ObjectMeta{Name: "probe"},
		Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: spec}}}
	pod := &corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}, ObjectMeta: metav1.ObjectMeta{Name: "probe"}, Spec: spec}
	jobPB := stateBody(t, pb, job)
	for _, tt := range []struct {
		name, path, ctype string
		body              []byte
		code              int
	}{
		{"job-json", jobs, js, stateBody(t, js, job), http.StatusCreated},
		{"job-protobuf", jobs, pb, jobPB, http.StatusCreated},
		{"pod-json-charset", pods, js + "; charset=utf-8", stateBody(t, js, pod), http.StatusCreated},
		{"pod-protobuf", pods, pb, stateBody(t, pb, pod), http.StatusCreated},
		{"pod-as-job-json", jobs, js, stateBody(t, js, pod), http.StatusBadRequest},
		{"pod-as-job-protobuf", jobs, pb, stateBody(t, pb, pod), http.StatusBadRequest},
		{"job-as-pod-protobuf", pods, pb, jobPB, http.StatusBadRequest},
		{"job-v1-json", jobs, js, []byte(`{"apiVersion":"v1","kind":"Job","metadata":{"name":"probe"}}`), http.StatusBadRequest},
		{"protobuf-as-json", jobs, js, jobPB, http.StatusBadRequest},
		{"json-as-protobuf", jobs, pb, stateBody(t, js, job), http.StatusBadRequest},
		{"truncated-protobuf", jobs, pb, jobPB[:len(jobPB)/2], http.StatusBadRequest},
		{"malformed-json", jobs, js, []byte(`{"apiVersion":"batch/v1","kind":"Job",`), http.StatusBadRequest},
		{"yaml", jobs, "application/yaml", []byte("apiVersion: batch/v1\nkind: Job\nmetadata: {name: probe}\n"), http.StatusUnsupportedMediaType},
		{"no-content-type", jobs, "", stateBody(t, js, job), http.StatusUnsupportedMediaType},
		{"bad-content-type", jobs, js + "; charset", stateBody(t, js, job), http.StatusUnsupportedMediaType},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(ctx, "POST", url+fmt.Sprintf(tt.path, tt.name), bytes.NewReader(tt.body))
			if err != nil {
				t.Fatal(err)
			}
			if tt.ctype != "" {
				req.Header.Set("Content-Type", tt.ctype)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != tt.code {
				t.Fatalf("status %d, want %d", resp.StatusCode, tt.code)
			}
			key := tt.name + "/probe"
			api.mu.Lock()
			j, p := api.jobs[key], api.pods[key]
			api.mu.Unlock()
			var got *corev1.PodSpec
			switch {
			case tt.code != http.StatusCreated:
				if j != nil || p != nil {
					t.Fatalf("refused create stored %v %v", j, p)
				}
				return
			case tt.path == jobs && j != nil && p == nil:
				got = &j.Spec.Template.Spec
			case tt.path == pods && p != nil && j == nil:
				got = &p.Spec
			default:
				t.Fatalf("stored %v %v", j, p)
			}
			if len(got.Containers) != 1 || got.Containers[0].Image != image || !slices.Equal(got.Containers[0].Args, []string{"a b"}) {
				t.Fatalf("stored spec %+v", got)
			}
		})
	}
	if requests, posts := api.counts(); requests != 15 || posts != 15 {
		t.Fatalf("%d requests, %d posts", requests, posts)
	}
}
