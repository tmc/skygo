package kube

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

const (
	dataID      = "0123456789abcdef0123456789abcdef"
	dataJob     = "fedcba9876543210fedcba9876543210"
	helperImage = "registry.example.com/skygo/helper@sha256:1111111111111111111111111111111111111111111111111111111111111111"
	taskImage   = "registry.example.com/team/train@sha256:2222222222222222222222222222222222222222222222222222222222222222"
)

var testCA = sync.OnceValue(func() string {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "skygo test"},
		NotBefore: time.Unix(0, 0), NotAfter: time.Unix(1<<32, 0), IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
})

func testDataConfig() *DataConfig {
	return &DataConfig{
		HelperImage: helperImage, HelperSHA256: strings.Repeat("3", 64),
		Origin: "https://skygo.example.com", CAPEM: testCA(), TaskImages: []string{taskImage},
		References: []DataReference{
			{Name: "datasets", PVC: "datasets", Destinations: []string{"/mnt/skygo/data"}, AllowWrite: true},
			{Name: "creds", Secret: "creds", Keys: []string{"config.json"}, Destinations: []string{"/mnt/skygo/creds"}},
		},
	}
}

// fixtureQualification is a labeled model fixture, not a native receipt. It
// validates a record written to match the model API, but copies invented
// implementation facts instead of loading a build receipt, and installs the
// result without startup reads, so the production StartData/ObserveData/
// RemoveData paths can be exercised against the model API. Only tests do this.
func fixtureQualification(t *testing.T, target TargetConfig, config *rest.Config) *dataQualification {
	t.Helper()
	r, err := validateQualificationCoverage(liveRecord(t, target, config, modelImplementation()), target, config, modelImplementation())
	if err != nil {
		t.Fatal(err)
	}
	id, err := qualificationRecordID(r)
	if err != nil {
		t.Fatal(err)
	}
	return &dataQualification{id: "model-fixture-not-native-" + id, record: r, premise: qualificationPremise{server: config.Host,
		caSHA256: r.Deployment.CASHA256, policy: "operator-policy-1", storage: map[string]string{"datasets": "storage-1"}}}
}

// dataAPI is a minimal namespaced Pod/Secret API that records every request.
type dataAPI struct {
	mu        sync.Mutex
	objects   map[string][]byte // "pods/name" -> JSON
	uids      map[string]string // resource -> UID for the next create
	requests  []string
	bodies    [][]byte
	createErr map[string]int  // resource -> HTTP status
	drop      map[string]bool // resource -> drop the create response
	inject    func(*corev1.Pod)
	keep      bool     // DELETE accepted but object remains (terminating)
	fullRead  []string // secret reads or deletes not limited to metadata
	onGet     func(n int, key string, obj []byte) []byte
	gets      int
	failGet   int // GET number that fails with HTTP 500
	retryGet  int // GET number answered 503 with Retry-After
	logStatus int // nonzero: status of log responses, with Retry-After
	logGets   int
	env       *qualificationAPI // qualification reads; logged, but not in gets
}

func newDataAPI(t *testing.T) (*Backend, *dataAPI) {
	t.Helper()
	return newDataAPIWrap(t, nil)
}

// newDataAPIWrap installs wrap as the client's transport wrapper, so a test
// can observe the actual response bodies that client-go hands to the backend.
func newDataAPIWrap(t *testing.T, wrap func(http.RoundTripper) http.RoundTripper) (*Backend, *dataAPI) {
	t.Helper()
	return newDataAPIHandler(t, wrap, nil)
}

// newDataAPIHandler is newDataAPIWrap with an optional handler around the
// model API.
func newDataAPIHandler(t *testing.T, wrap func(http.RoundTripper) http.RoundTripper, handler func(*dataAPI) http.Handler) (*Backend, *dataAPI) {
	t.Helper()
	api, c, config := newDataServer(t, wrap, handler)
	target, err := fromREST(c, "owner", config)
	if err != nil {
		t.Fatal(err)
	}
	if target.DataPolicy == nil || *target.DataPolicy != backend.DefaultDataPolicy() || target.SupportsRemoteData {
		t.Fatal("data target lacks fixed data policy or reports unqualified support")
	}
	b := target.Backend.(*Backend)
	b.dataQual = fixtureQualification(t, c, config)
	return b, api
}

// newDataServer starts the model API over HTTPS and returns the data target
// configuration and REST configuration that reach it through wrap.
func newDataServer(t *testing.T, wrap func(http.RoundTripper) http.RoundTripper, handler func(*dataAPI) http.Handler) (*dataAPI, TargetConfig, *rest.Config) {
	t.Helper()
	api := &dataAPI{objects: map[string][]byte{}, createErr: map[string]int{}, drop: map[string]bool{},
		uids: map[string]string{"pods": "pod-uid", "secrets": "secret-uid"}, env: newQualificationAPI()}
	h := http.Handler(http.HandlerFunc(api.serve))
	if handler != nil {
		h = handler(api)
	}
	server := httptest.NewTLSServer(h)
	t.Cleanup(server.Close)
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	target := TargetConfig{Name: "test", Namespace: "workloads", InCluster: true, Data: testDataConfig(),
		Resources: ResourcePolicy{NodeSelectors: map[string][]string{poolKey: {poolValue}}}}
	return api, target, &rest.Config{Host: server.URL, WrapTransport: wrap, TLSClientConfig: rest.TLSClientConfig{CAData: ca}}
}

func (a *dataAPI) serve(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	body, _ := io.ReadAll(r.Body)
	a.requests = append(a.requests, r.Method+" "+r.URL.Path)
	a.bodies = append(a.bodies, body)
	if a.env != nil && a.env.serve(w, r) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	failure := func(code int) {
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Code: int32(code), Reason: reasonFor(code)})
	}
	rest, ok := strings.CutPrefix(r.URL.Path, "/api/v1/namespaces/workloads/")
	if !ok {
		failure(400)
		return
	}
	resource, name, _ := strings.Cut(rest, "/")
	if r.Method == "POST" {
		var meta struct {
			Metadata metav1.ObjectMeta `json:"metadata"`
		}
		json.Unmarshal(body, &meta)
		name = meta.Metadata.Name
	}
	key := resource + "/" + name
	if r.Method == "GET" && strings.HasSuffix(name, "/log") {
		a.logGets++
		if r.URL.Query().Get("container") != "task" {
			failure(400)
			return
		}
		if a.logStatus != 0 {
			w.Header().Set("Retry-After", "1")
			failure(a.logStatus)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, "task output\n")
		return
	}
	metadata := strings.Contains(r.Header.Get("Accept"), "as=PartialObjectMetadata")
	if resource == "secrets" && r.Method != "POST" && !metadata {
		a.fullRead = append(a.fullRead, r.Method+" "+key)
	}
	switch r.Method {
	case "POST":
		if code := a.createErr[resource]; code != 0 {
			failure(code)
			return
		}
		var obj map[string]any
		json.Unmarshal(body, &obj)
		obj["apiVersion"], obj["kind"] = "v1", map[string]string{"pods": "Pod", "secrets": "Secret"}[resource]
		m := obj["metadata"].(map[string]any)
		m["uid"] = a.uids[resource] + "-" + name
		raw, _ := json.Marshal(obj)
		if resource == "pods" && a.inject != nil {
			p := &corev1.Pod{}
			json.Unmarshal(raw, p)
			a.inject(p)
			raw, _ = json.Marshal(p)
		}
		a.objects[key] = raw
		if a.drop[resource] {
			if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
				conn.Close()
			}
			return
		}
		w.WriteHeader(http.StatusCreated)
		w.Write(raw)
	case "GET":
		a.gets++
		if a.gets == a.failGet {
			failure(500)
			return
		}
		if a.gets == a.retryGet {
			w.Header().Set("Retry-After", "1")
			failure(503)
			return
		}
		obj, ok := a.objects[key]
		if a.onGet != nil && ok {
			obj = a.onGet(a.gets, key, obj)
			a.objects[key] = obj
		}
		if !ok {
			failure(404)
			return
		}
		if metadata {
			var m struct {
				Metadata metav1.ObjectMeta `json:"metadata"`
			}
			json.Unmarshal(obj, &m)
			obj, _ = json.Marshal(metav1.PartialObjectMetadata{TypeMeta: metav1.TypeMeta{APIVersion: "meta.k8s.io/v1", Kind: "PartialObjectMetadata"}, ObjectMeta: m.Metadata})
		}
		w.Write(obj)
	case "DELETE":
		var opts metav1.DeleteOptions
		if _, _, err := scheme.Codecs.UniversalDeserializer().Decode(body, nil, &opts); err != nil || opts.Preconditions == nil || opts.Preconditions.UID == nil {
			failure(400)
			return
		}
		obj, ok := a.objects[key]
		if !ok {
			failure(404)
			return
		}
		var m struct {
			Metadata metav1.ObjectMeta `json:"metadata"`
		}
		json.Unmarshal(obj, &m)
		if m.Metadata.UID != *opts.Preconditions.UID {
			failure(409)
			return
		}
		if !a.keep {
			delete(a.objects, key)
		}
		w.Write([]byte(`{"apiVersion":"v1","kind":"Status","status":"Success"}`))
	default:
		failure(405)
	}
}

func reasonFor(code int) metav1.StatusReason {
	switch code {
	case 404:
		return metav1.StatusReasonNotFound
	case 409:
		return metav1.StatusReasonConflict
	}
	return metav1.StatusReasonInternalError
}

func (a *dataAPI) log() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.requests...)
}

// dataLog returns the logged requests other than qualification reads.
func (a *dataAPI) dataLog() []string {
	return slices.DeleteFunc(a.log(), qualificationRead)
}

func (a *dataAPI) setPod(f func(*corev1.Pod)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	p := &corev1.Pod{}
	json.Unmarshal(a.objects["pods/skygo-"+dataID], p)
	f(p)
	a.objects["pods/skygo-"+dataID], _ = json.Marshal(p)
}

func stagedTask() backend.Task {
	return backend.Task{Argv: []string{"python", "train.py"}, Image: taskImage, Env: map[string]string{"MODE": "fast"},
		Workdir: &artifact.Ref{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Size: 1024},
		Setup:   []string{"pip", "install", "-r", "requirements.txt"}, Outputs: []string{"out"},
		DataMounts: []backend.DataMount{{Reference: "datasets", Destination: "/mnt/skygo/data", ReadOnly: true}}}
}

func dataStart(b *Backend, t backend.Task) backend.DataStart {
	budgets, err := backend.DefaultDataPolicy().Budgets(t.TimeoutSeconds)
	if err != nil {
		panic(err)
	}
	s := backend.DataStart{JobID: dataJob, Execution: backend.Reference{ID: dataID, Task: t, Binding: b.binding}, Budgets: budgets}
	if t.Workdir != nil {
		s.InputToken = []byte("sgd1.input-secret-token")
	}
	if len(t.Outputs) != 0 {
		s.OutputToken = []byte("sgd1.output-secret-token")
	}
	return s
}

func TestDataCheck(t *testing.T) {
	b, api := newDataAPI(t)
	if err := b.Check(qualifiedTask()); err != nil {
		t.Fatalf("qualified staged task: %v", err)
	}
	mountOnly := backend.Task{Argv: []string{"ls"}, Image: taskImage, NodeSelector: map[string]string{poolKey: poolValue},
		DataMounts: []backend.DataMount{{Reference: "creds", Destination: "/mnt/skygo/creds", ReadOnly: true}}}
	if err := b.Check(mountOnly); err != nil {
		t.Fatalf("qualified mount-only task: %v", err)
	}
	if err := b.Check(backend.Task{Argv: []string{"ls"}, Image: "alpine"}); err != nil {
		t.Fatalf("legacy task on data target: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*backend.Task)
	}{
		{"unapproved image", func(t *backend.Task) { t.Image = "alpine" }},
		{"unconfigured reference", func(t *backend.Task) { t.DataMounts[0].Reference = "other" }},
		{"destination not allowed", func(t *backend.Task) { t.DataMounts[0].Destination = "/mnt/skygo/other" }},
		{"writable secret", func(t *backend.Task) {
			t.DataMounts[0] = backend.DataMount{Reference: "creds", Destination: "/mnt/skygo/creds"}
		}},
		{"retry", func(t *backend.Task) { t.Retry = &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{1}} }},
		{"timeout above policy", func(t *backend.Task) { t.TimeoutSeconds = 604801 }},
		{"directory with workdir", func(t *backend.Task) { t.Directory = "/srv" }},
		{"protected destination", func(t *backend.Task) { t.DataMounts[0].Destination = "/workspace/x" }},
		{"setup without workdir", func(t *backend.Task) { t.Workdir = nil; t.Outputs = nil }},
		{"pool omitted", func(t *backend.Task) { t.NodeSelector = nil }},
		{"other pool", func(t *backend.Task) { t.NodeSelector[poolKey] = "other" }},
	}
	for _, tt := range tests {
		task := qualifiedTask()
		tt.mutate(&task)
		if err := b.Check(task); err == nil {
			t.Errorf("%s: Check accepted", tt.name)
		}
	}
	protectedDir := mountOnly
	protectedDir.Directory = "/run/skygo/x"
	if err := b.Check(protectedDir); err == nil {
		t.Error("mount-only protected directory accepted")
	}
	unqualifiedDir := mountOnly
	unqualifiedDir.Directory = "/opt/other"
	if err := b.Check(unqualifiedDir); err == nil {
		t.Error("unqualified explicit directory accepted")
	}
	b.dataQual = nil
	if err := b.Check(qualifiedTask()); err == nil || !strings.Contains(err.Error(), "not qualified") {
		t.Errorf("production target without qualification: %v", err)
	}
	if err := b.Start(context.Background(), dataID, qualifiedTask()); err == nil {
		t.Error("legacy Start accepted data task")
	}
	if n := len(api.log()); n != 0 {
		t.Errorf("Check/Start made %d API requests", n)
	}
}

func TestDataCheckWithoutProfile(t *testing.T) {
	b, err := testBackend(nil, "workloads", "owner")
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Check(stagedTask()); err == nil {
		t.Error("legacy target accepted data task")
	}
	if _, err := b.StartData(context.Background(), dataStart(b, stagedTask())); err == nil {
		t.Error("legacy target started data task")
	}
}

func TestStartDataReceipts(t *testing.T) {
	b, api := newDataAPI(t)
	task := qualifiedTask()
	c, err := b.StartData(context.Background(), dataStart(b, task))
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.ValidateDataCreated(task, c, false); err != nil {
		t.Fatal(err)
	}
	want := backend.DataCreated{Instance: "pod-uid-skygo-" + dataID,
		Pod:          backend.DataResource{Name: "skygo-" + dataID, UID: "pod-uid-skygo-" + dataID},
		InputSecret:  backend.DataResource{Name: "skygo-" + dataID + "-input", UID: "secret-uid-skygo-" + dataID + "-input"},
		OutputSecret: backend.DataResource{Name: "skygo-" + dataID + "-output", UID: "secret-uid-skygo-" + dataID + "-output"}}
	if c != want {
		t.Fatalf("receipts = %+v, want %+v", c, want)
	}
	got := strings.Join(api.log(), "\n")
	if got != "GET /api/v1/namespaces/workloads\nGET /version\nGET /api/v1/nodes\n"+
		"GET /api/v1/namespaces/workloads/persistentvolumeclaims/datasets\nGET /apis/storage.k8s.io/v1/storageclasses/local\n"+
		"POST /api/v1/namespaces/workloads/secrets\nPOST /api/v1/namespaces/workloads/secrets\nPOST /api/v1/namespaces/workloads/pods" {
		t.Fatalf("requests:\n%s", got)
	}
	// Token bytes appear only inside their own Secret bodies, base64 encoded.
	for i, body := range api.bodies[5:] {
		if bytes.Contains(body, []byte("secret-token")) {
			t.Errorf("request %d carries plaintext token", i)
		}
	}
	if !bytes.Contains(api.bodies[5], []byte(`"immutable":true`)) {
		t.Error("input secret is not immutable")
	}
	if _, err := b.StartData(context.Background(), dataStart(b, task)); err == nil {
		t.Error("second StartData for the same attempt accepted")
	}
	if n := len(api.log()); n != 8 {
		t.Errorf("repeated StartData made requests: %d", n)
	}
}

func TestStartDataPartialReceipts(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(*dataAPI)
		want   func(c backend.DataCreated) bool
		posted int
	}{
		{"pod create fails", func(a *dataAPI) { a.createErr["pods"] = 500 },
			func(c backend.DataCreated) bool {
				return c.Pod == (backend.DataResource{}) && c.Instance == "" && c.InputSecret.UID != "" && c.OutputSecret.UID != ""
			}, 3},
		{"pod response lost", func(a *dataAPI) { a.drop["pods"] = true },
			func(c backend.DataCreated) bool { return c.Pod == (backend.DataResource{}) && c.OutputSecret.UID != "" }, 3},
		{"pod profile injected", func(a *dataAPI) {
			a.inject = func(p *corev1.Pod) {
				p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: "sidecar", Image: "evil"})
			}
		}, func(c backend.DataCreated) bool {
			return c.Instance == "pod-uid-skygo-"+dataID && c.Pod.UID == c.Instance
		}, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, api := newDataAPI(t)
			tt.setup(api)
			task := qualifiedTask()
			c, err := b.StartData(context.Background(), dataStart(b, task))
			if err == nil {
				t.Fatal("StartData succeeded")
			}
			if !tt.want(c) {
				t.Fatalf("receipts = %+v", c)
			}
			if err := backend.ValidateDataCreated(task, c, true); err != nil {
				t.Fatalf("partial receipts invalid: %v", err)
			}
			if n := len(api.dataLog()); n != tt.posted {
				t.Fatalf("requests = %v", api.log())
			}
			for _, r := range api.dataLog() {
				if strings.HasPrefix(r, "GET") {
					t.Fatalf("StartData adopted by lookup: %v", api.log())
				}
			}
		})
	}
	t.Run("output secret fails", func(t *testing.T) {
		// Fail only the output Secret; the Pod must then never be posted.
		b, api := newDataAPIHandler(t, nil, func(api *dataAPI) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if r.Method == "POST" && bytes.Contains(body, []byte(`-output"`)) {
					api.mu.Lock()
					api.createErr["secrets"] = 500
					api.mu.Unlock()
					defer func() { api.mu.Lock(); api.createErr["secrets"] = 0; api.mu.Unlock() }()
				}
				r.Body = io.NopCloser(bytes.NewReader(body))
				api.serve(w, r)
			})
		})
		c, err := b.StartData(context.Background(), dataStart(b, qualifiedTask()))
		if err == nil || c.InputSecret.UID == "" || c.OutputSecret != (backend.DataResource{}) || c.Pod != (backend.DataResource{}) {
			t.Fatalf("receipts = %+v, err = %v", c, err)
		}
		if n := len(api.dataLog()); n != 2 {
			t.Fatalf("requests = %v", api.log())
		}
	})
}

func TestStartDataRejectsBeforeEffects(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*backend.DataStart)
	}{
		{"wrong binding", func(s *backend.DataStart) { s.Execution.Binding = "sha256:other" }},
		{"instance present", func(s *backend.DataStart) { s.Execution.Instance = "uid" }},
		{"invalid job", func(s *backend.DataStart) { s.JobID = "job" }},
		{"budget differs", func(s *backend.DataStart) { s.Budgets.TaskSeconds++ }},
		{"missing input token", func(s *backend.DataStart) { s.InputToken = nil }},
		{"unexpected output token", func(s *backend.DataStart) {
			s.Execution.Task.Outputs = nil
		}},
		{"oversize token", func(s *backend.DataStart) { s.InputToken = make([]byte, maxDataToken+1) }},
		{"legacy task", func(s *backend.DataStart) {
			s.Execution.Task = backend.Task{Argv: []string{"ls"}, Image: taskImage}
			s.InputToken, s.OutputToken = nil, nil
		}},
	}
	for _, tt := range tests {
		b, api := newDataAPI(t)
		s := dataStart(b, qualifiedTask())
		tt.mutate(&s)
		c, err := b.StartData(context.Background(), s)
		if err == nil || c != (backend.DataCreated{}) || len(api.log()) != 0 {
			t.Errorf("%s: c=%+v err=%v requests=%v", tt.name, c, err, api.log())
		}
	}
}

func startedData(t *testing.T) (*Backend, *dataAPI, backend.Reference, backend.DataResources) {
	t.Helper()
	b, api := newDataAPI(t)
	s := dataStart(b, qualifiedTask())
	c, err := b.StartData(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	ref := s.Execution
	ref.Instance = c.Instance
	return b, api, ref, backend.DataResources{Execution: ref, Pod: c.Pod, InputSecret: c.InputSecret, OutputSecret: c.OutputSecret}
}

var (
	t0 = time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
)

func terminated(id string, code int32, start, end time.Time) corev1.ContainerStatus {
	return corev1.ContainerStatus{ContainerID: "containerd://" + id, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
		ContainerID: "containerd://" + id, ExitCode: code, StartedAt: metav1.NewTime(start), FinishedAt: metav1.NewTime(end)}}}
}

func named(s corev1.ContainerStatus, name string) corev1.ContainerStatus { s.Name = name; return s }

func runningTask(p *corev1.Pod) {
	p.Status.Phase = corev1.PodRunning
	p.Status.InitContainerStatuses = []corev1.ContainerStatus{
		named(terminated("stage", 0, t0, t0.Add(time.Minute)), "stage"),
		named(terminated("setup", 0, t0.Add(time.Minute), t0.Add(2*time.Minute)), "setup"),
	}
	p.Status.ContainerStatuses = []corev1.ContainerStatus{
		{Name: "task", ContainerID: "containerd://task", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(t0.Add(3 * time.Minute))}}},
		{Name: "collector", ContainerID: "containerd://collector", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(t0.Add(3 * time.Minute))}}},
	}
}

func settledTask(p *corev1.Pod) {
	runningTask(p)
	p.Status.Phase = corev1.PodSucceeded
	p.Status.ContainerStatuses = []corev1.ContainerStatus{
		named(terminated("task", 0, t0.Add(3*time.Minute), t0.Add(time.Hour)), "task"),
		named(terminated("collector", 0, t0.Add(3*time.Minute), t0.Add(time.Hour+time.Minute)), "collector"),
	}
}

func TestObserveData(t *testing.T) {
	b, api, ref, _ := startedData(t)
	ctx := context.Background()
	o, err := b.ObserveData(ctx, ref)
	if err != nil || o.Phase != backend.DataStarting || o.Status.State != backend.Starting {
		t.Fatalf("pending: %+v %v", o, err)
	}
	api.setPod(runningTask)
	o, err = b.ObserveData(ctx, ref)
	if err != nil || o.Phase != backend.DataTask || o.TaskStart == nil || o.TaskStart.ContainerID != "containerd://task" {
		t.Fatalf("running: %+v %v", o, err)
	}
	if s, err := b.Observe(ctx, ref); err != nil || s.State != backend.Running || s.Instance != ref.Instance {
		t.Fatalf("Observe dispatch: %+v %v", s, err)
	}
	api.setPod(settledTask)
	before := api.gets
	o, err = b.ObserveData(ctx, ref)
	if err != nil || o.Phase != backend.DataSettled || o.TaskStop == nil || o.Collector == nil || o.Status.Termination != backend.StoppedWithExitCode {
		t.Fatalf("settled: %+v %v", o, err)
	}
	if api.gets-before != 2 {
		t.Errorf("stop receipt used %d reads, want 2", api.gets-before)
	}

	// A runtime identity change between the two reads is not a receipt.
	api.onGet = func(n int, key string, obj []byte) []byte {
		if n%2 == 0 {
			return obj
		}
		return bytes.ReplaceAll(obj, []byte("containerd://task"), []byte("containerd://task2"))
	}
	o, err = b.ObserveData(ctx, ref)
	if err != nil || o.Phase != backend.DataHeld || o.TaskStop != nil {
		t.Fatalf("changed termination: %+v %v", o, err)
	}
	api.onGet = nil

	// A mutated profile and a same-name replacement are identity errors.
	api.setPod(func(p *corev1.Pod) { p.Spec.Containers[0].Image = "evil" })
	if _, err := b.ObserveData(ctx, ref); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("mutated profile: %v", err)
	}
	api.setPod(func(p *corev1.Pod) { p.Spec.Containers[0].Image = taskImage; p.UID = "replacement" })
	if _, err := b.ObserveData(ctx, ref); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("replacement: %v", err)
	}
	api.mu.Lock()
	delete(api.objects, "pods/skygo-"+dataID)
	api.mu.Unlock()
	o, err = b.ObserveData(ctx, ref)
	if err != nil || o.Phase != backend.DataHeld || o.Status.State != backend.Unknown {
		t.Fatalf("missing: %+v %v", o, err)
	}
	noInstance := ref
	noInstance.Instance = ""
	if _, err := b.ObserveData(ctx, noInstance); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("unknown instance: %v", err)
	}
}

func TestObserveDataNoLaunch(t *testing.T) {
	b, api, ref, _ := startedData(t)
	api.setPod(func(p *corev1.Pod) {
		p.Status.Phase = corev1.PodFailed
		blocked := func(name string) corev1.ContainerStatus {
			return corev1.ContainerStatus{Name: name, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}}
		}
		p.Status.InitContainerStatuses = []corev1.ContainerStatus{named(terminated("stage", 3, t0, t0.Add(time.Minute)), "stage"), blocked("setup")}
		p.Status.ContainerStatuses = []corev1.ContainerStatus{blocked("task"), blocked("collector")}
	})
	before := api.gets
	o, err := b.ObserveData(context.Background(), ref)
	if err != nil || o.NoLaunch == nil || o.NoLaunch.FailedInit != backend.StageFailure || o.TaskStop != nil || o.Status.State != backend.Failed || o.Phase != backend.DataSettled {
		t.Fatalf("no-launch: %+v %v", o, err)
	}
	if api.gets-before != 2 {
		t.Errorf("no-launch used %d reads, want 2", api.gets-before)
	}
}

func TestCancelAndLogsData(t *testing.T) {
	b, api, ref, _ := startedData(t)
	ctx := context.Background()
	api.setPod(runningTask)
	chunk, err := b.Logs(ctx, ref, 0, 64)
	if err != nil || string(chunk.Data) != "task output\n" || chunk.Complete {
		t.Fatalf("logs: %+v %v", chunk, err)
	}
	if err := b.Cancel(ctx, ref); err != nil {
		t.Fatal(err)
	}
	log := api.log()
	if log[len(log)-1] != "DELETE /api/v1/namespaces/workloads/pods/skygo-"+dataID {
		t.Fatalf("cancel requests: %v", log)
	}
	for _, r := range log {
		if strings.Contains(r, "secrets/") && strings.HasPrefix(r, "DELETE") {
			t.Fatal("cancel deleted a secret")
		}
	}
	if err := b.Cancel(ctx, ref); err != nil {
		t.Fatalf("cancel after absence: %v", err)
	}
}

func TestRemoveData(t *testing.T) {
	ctx := context.Background()
	t.Run("all resources", func(t *testing.T) {
		b, api, _, r := startedData(t)
		out, err := b.RemoveData(ctx, r)
		if err != nil || !out.PodRemoved || !out.InputRemoved || !out.OutputRemoved {
			t.Fatalf("%+v %v", out, err)
		}
		if err := backend.ValidateDataRemoval(r, out); err != nil {
			t.Fatal(err)
		}
		if len(api.fullRead) != 0 {
			t.Fatalf("cleanup read secret data: %v", api.fullRead)
		}
		deletes := 0
		for _, req := range api.log() {
			if strings.HasPrefix(req, "DELETE") {
				deletes++
			}
		}
		if deletes != 3 {
			t.Fatalf("deletes = %d: %v", deletes, api.log())
		}
		// A later, new RemoveData call proves its own absence without deleting.
		again, err := (&Backend{client: b.client, namespace: b.namespace, owner: b.owner, binding: b.binding, data: b.data}).RemoveData(ctx, r)
		if err != nil || !again.PodRemoved || !again.InputRemoved || !again.OutputRemoved {
			t.Fatalf("restart removal: %+v %v", again, err)
		}
	})
	t.Run("replacement untouched", func(t *testing.T) {
		b, api, _, r := startedData(t)
		api.setPod(func(p *corev1.Pod) { p.UID = types.UID("replacement") })
		n := len(api.log())
		out, err := b.RemoveData(ctx, r)
		if !errors.Is(err, backend.ErrIdentityMismatch) || out.PodRemoved || !out.InputRemoved || !out.OutputRemoved {
			t.Fatalf("%+v %v", out, err)
		}
		for _, req := range api.log()[n:] {
			if req == "DELETE /api/v1/namespaces/workloads/pods/skygo-"+dataID {
				t.Fatal("replacement pod deleted")
			}
		}
	})
	t.Run("deletion pending", func(t *testing.T) {
		b, api, _, r := startedData(t)
		api.keep = true
		out, err := b.RemoveData(ctx, r)
		if err == nil || out.PodRemoved || out.InputRemoved || out.OutputRemoved {
			t.Fatalf("%+v %v", out, err)
		}
	})
	t.Run("known secret only", func(t *testing.T) {
		b, _, _, r := startedData(t)
		r.Pod, r.OutputSecret = backend.DataResource{}, backend.DataResource{}
		r.Execution.Instance = ""
		out, err := b.RemoveData(ctx, r)
		if err != nil || out.PodRemoved || !out.InputRemoved || out.OutputRemoved {
			t.Fatalf("%+v %v", out, err)
		}
	})
	t.Run("wrong names and binding", func(t *testing.T) {
		b, api, _, r := startedData(t)
		n := len(api.log())
		bad := r
		bad.InputSecret.Name = "skygo-other"
		if _, err := b.RemoveData(ctx, bad); err == nil {
			t.Error("foreign secret name accepted")
		}
		bad = r
		bad.Execution.Binding = "sha256:other"
		if _, err := b.RemoveData(ctx, bad); err == nil {
			t.Error("foreign binding accepted")
		}
		if len(api.log()) != n {
			t.Errorf("rejected removal made requests: %v", api.log()[n:])
		}
	})
	t.Run("live owner exclusive", func(t *testing.T) {
		b, api, _, r := startedData(t)
		if err := b.claimRetry(dataID, false); err != nil {
			t.Fatal(err)
		}
		n := len(api.log())
		if _, err := b.RemoveData(ctx, r); err == nil {
			t.Error("removal ran beside a live owner")
		}
		b.releaseRetry(dataID)
		if len(api.log()) != n {
			t.Error("pending removal made requests")
		}
	})
}

func TestDataPodMatchesReviewMutations(t *testing.T) {
	b, _ := newDataAPI(t)
	task := qualifiedTask()
	budgets, _ := backend.DefaultDataPolicy().Budgets(0)
	hash, _ := taskHash(task)
	want, err := b.dataPod(dataID, task, budgets, hash)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*corev1.Pod)
		ok     bool
	}{
		{"server default termination message", func(p *corev1.Pod) {
			p.Spec.Containers[0].TerminationMessagePath = corev1.TerminationMessagePathDefault
			p.Spec.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageReadFile
		}, true},
		{"termination message over token", func(p *corev1.Pod) {
			p.Spec.InitContainers[0].TerminationMessagePath = "/run/skygo/input/token"
		}, false},
		{"termination message from logs", func(p *corev1.Pod) {
			p.Spec.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageFallbackToLogsOnError
		}, false},
		{"unrelated annotation", func(p *corev1.Pod) { p.Annotations["example.com/note"] = "x" }, true},
		{"apparmor annotation", func(p *corev1.Pod) {
			p.Annotations["container.apparmor.security.beta.kubernetes.io/task"] = "unconfined"
		}, false},
		{"seccomp annotation", func(p *corev1.Pod) {
			p.Annotations["seccomp.security.alpha.kubernetes.io/pod"] = "unconfined"
		}, false},
	}
	for _, tt := range tests {
		got := want.DeepCopy()
		tt.mutate(got)
		if err := b.dataPodMatches(want, got); (err == nil) != tt.ok {
			t.Errorf("%s: err = %v", tt.name, err)
		}
	}
}

// A collector finishing more than CollectionSeconds after the main task is
// reported as its exact native exit. The collection deadline runs from the
// durable release, which only Store knows; the backend must not decide it.
func TestObserveDataLateCollectorFacts(t *testing.T) {
	b, api, ref, _ := startedData(t)
	taskEnd := t0.Add(time.Hour)
	collectorEnd := taskEnd.Add(time.Duration(backend.DefaultDataPolicy().CollectionSeconds+60) * time.Second)
	api.setPod(func(p *corev1.Pod) {
		settledTask(p)
		p.Status.ContainerStatuses[1] = named(terminated("collector", 0, t0.Add(3*time.Minute), collectorEnd), "collector")
	})
	o, err := b.ObserveData(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	want := backend.ContainerExit{ContainerID: "containerd://collector", StartedAt: t0.Add(3 * time.Minute), FinishedAt: collectorEnd}
	if o.Phase != backend.DataSettled || o.Status.Termination != backend.StoppedWithExitCode || o.Collector == nil || *o.Collector != want || o.TaskStop.Task.FinishedAt != taskEnd {
		t.Fatalf("late collector: %+v collector=%+v", o, o.Collector)
	}
}

func (a *dataAPI) setObject(key string, f func(*metav1.ObjectMeta)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var obj map[string]any
	json.Unmarshal(a.objects[key], &obj)
	raw, _ := json.Marshal(obj["metadata"])
	var m metav1.ObjectMeta
	json.Unmarshal(raw, &m)
	f(&m)
	raw, _ = json.Marshal(m)
	var meta map[string]any
	json.Unmarshal(raw, &meta)
	obj["metadata"] = meta
	a.objects[key], _ = json.Marshal(obj)
}

// A present object with the recorded UID but changed ownership metadata or
// Pod profile is left untouched with a false bit; the other recorded objects
// keep their independent positive bits alongside the error.
func TestRemoveDataOwnership(t *testing.T) {
	pod := "pods/skygo-" + dataID
	input := "secrets/skygo-" + dataID + "-input"
	tests := []struct {
		name   string
		key    string
		mutate func(a *dataAPI)
	}{
		{"pod spec", pod, func(a *dataAPI) { a.setPod(func(p *corev1.Pod) { p.Spec.Containers[0].Image = "evil" }) }},
		{"pod injected container", pod, func(a *dataAPI) {
			a.setPod(func(p *corev1.Pod) {
				p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: "x", Image: "x"})
			})
		}},
		{"pod owner reference", pod, func(a *dataAPI) {
			a.setObject(pod, func(m *metav1.ObjectMeta) {
				m.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: "x", UID: "x"}}
			})
		}},
		{"pod binding", pod, func(a *dataAPI) {
			a.setObject(pod, func(m *metav1.ObjectMeta) { m.Annotations[bindingAnnotation] = "sha256:other" })
		}},
		{"pod task", pod, func(a *dataAPI) {
			a.setObject(pod, func(m *metav1.ObjectMeta) { m.Annotations[taskAnnotation] = "other" })
		}},
		{"pod label", pod, func(a *dataAPI) { a.setObject(pod, func(m *metav1.ObjectMeta) { m.Labels["extra"] = "x" }) }},
		{"secret owner label", input, func(a *dataAPI) { a.setObject(input, func(m *metav1.ObjectMeta) { m.Labels[ownerLabel] = "other" }) }},
		{"secret id label", input, func(a *dataAPI) { a.setObject(input, func(m *metav1.ObjectMeta) { m.Labels[idLabel] = "other" }) }},
		{"secret layout", input, func(a *dataAPI) {
			a.setObject(input, func(m *metav1.ObjectMeta) { delete(m.Annotations, layoutAnnotation) })
		}},
		{"secret binding", input, func(a *dataAPI) {
			a.setObject(input, func(m *metav1.ObjectMeta) { m.Annotations[bindingAnnotation] = "sha256:other" })
		}},
		{"secret owner reference", input, func(a *dataAPI) {
			a.setObject(input, func(m *metav1.ObjectMeta) {
				m.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: "x", UID: "x"}}
			})
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, api, _, r := startedData(t)
			tt.mutate(api)
			n := len(api.log())
			out, err := b.RemoveData(context.Background(), r)
			if !errors.Is(err, backend.ErrIdentityMismatch) {
				t.Fatalf("err = %v", err)
			}
			for _, req := range api.log()[n:] {
				if req == "DELETE /api/v1/namespaces/workloads/"+tt.key {
					t.Fatalf("deleted mutated %s", tt.key)
				}
			}
			bits := map[string]bool{pod: out.PodRemoved, input: out.InputRemoved, "secrets/skygo-" + dataID + "-output": out.OutputRemoved}
			for key, removed := range bits {
				if removed == (key == tt.key) {
					t.Errorf("%s removed = %v", key, removed)
				}
			}
			if err := backend.ValidateDataRemoval(r, out); err != nil {
				t.Fatal(err)
			}
			if len(api.fullRead) != 0 {
				t.Fatalf("secret data read: %v", api.fullRead)
			}
		})
	}
}

// closeBody is the actual response body seen by logsData, wrapped so a test
// can fail its read or Close and count Close calls.
type closeBody struct {
	io.ReadCloser
	readErr  error
	closeErr error
	closes   int
}

func (c *closeBody) Read(p []byte) (int, error) {
	if c.readErr != nil {
		return 0, c.readErr
	}
	return c.ReadCloser.Read(p)
}

func (c *closeBody) Close() error {
	c.closes++
	c.ReadCloser.Close()
	return c.closeErr
}

type logTransport struct {
	rt   http.RoundTripper
	make func(io.ReadCloser) *closeBody
	body *closeBody
}

func (l *logTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := l.rt.RoundTrip(r)
	if err == nil && strings.HasSuffix(r.URL.Path, "/log") {
		l.body = l.make(resp.Body)
		resp.Body = l.body
	}
	return resp, err
}

func TestLogsDataCloseCustody(t *testing.T) {
	closeErr := errors.New("close failed")
	tests := []struct {
		name     string
		readErr  error
		closeErr error
		failGet  int // relative to the GETs before Logs
		status   int // log response status
		held     bool
	}{
		{"success", nil, nil, 0, 0, false},
		{"close failure", nil, closeErr, 0, 0, true},
		{"read and close failure", errors.New("read failed"), closeErr, 0, 0, true},
		{"second check and close failure", nil, closeErr, 2, 0, true},
		{"retryable status and close failure", nil, closeErr, 0, 503, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lt := &logTransport{make: func(rc io.ReadCloser) *closeBody {
				return &closeBody{ReadCloser: rc, readErr: tt.readErr, closeErr: tt.closeErr}
			}}
			b, api, ref, r := startedDataWrap(t, func(rt http.RoundTripper) http.RoundTripper { lt.rt = rt; return lt })
			api.setPod(runningTask)
			api.mu.Lock()
			if tt.failGet != 0 {
				api.failGet = api.gets + tt.failGet
			}
			api.logStatus = tt.status
			api.mu.Unlock()
			_, err := b.Logs(context.Background(), ref, 0, 64)
			if api.logGets != 1 {
				t.Fatalf("log requests = %d, want exactly one", api.logGets)
			}
			if lt.body == nil || lt.body.closes != 1 {
				t.Fatalf("log body closes = %+v", lt.body)
			}
			s, owners := heldFrame(b, ref.ID)
			if !tt.held {
				if err != nil || s != nil || owners != 0 {
					t.Fatalf("err=%v held=%v owners=%d", err, s, owners)
				}
				if _, err := b.RemoveData(context.Background(), r); err != nil {
					t.Fatalf("cleanup after closed stream: %v", err)
				}
				return
			}
			if errors.Is(err, closeErr) || !errors.Is(err, errDataBodyHeld) {
				t.Fatalf("err = %v, want fixed held category without the Close error", err)
			}
			if tt.failGet != 0 && !apierrors.IsInternalError(err) {
				t.Fatalf("second metadata check did not fail: %v", err)
			}
			if tt.readErr != nil && !errors.Is(err, tt.readErr) {
				t.Fatalf("read failure lost: %v", err)
			}
			if s == nil || !s.logs || len(s.failed) != 1 || s.failed[0].ReadCloser != io.ReadCloser(lt.body) || s.failed[0].err != closeErr || owners != 1 {
				t.Fatalf("custody = %+v owners=%d", s, owners)
			}
			n := len(api.log())
			if _, err := b.RemoveData(context.Background(), r); err == nil {
				t.Fatal("cleanup ran beside an uncertain log body")
			}
			if _, err := b.Logs(context.Background(), ref, 0, 64); !errors.Is(err, errDataBodyHeld) {
				t.Fatalf("second log read: %v", err)
			}
			if len(api.log()) != n || lt.body.closes != 1 {
				t.Fatalf("held owner retried: requests %v closes %d", api.log()[n:], lt.body.closes)
			}
		})
	}
}

func startedDataWrap(t *testing.T, wrap func(http.RoundTripper) http.RoundTripper) (*Backend, *dataAPI, backend.Reference, backend.DataResources) {
	t.Helper()
	b, api := newDataAPIWrap(t, wrap)
	s := dataStart(b, qualifiedTask())
	c, err := b.StartData(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	ref := s.Execution
	ref.Instance = c.Instance
	return b, api, ref, backend.DataResources{Execution: ref, Pod: c.Pod, InputSecret: c.InputSecret, OutputSecret: c.OutputSecret}
}

// gateBody blocks its first Read until released, holding a log read open.
type gateBody struct {
	io.ReadCloser
	opened, gate chan struct{}
}

func (g *gateBody) Read(p []byte) (int, error) {
	if g.opened != nil {
		close(g.opened)
		g.opened = nil
		<-g.gate
	}
	return g.ReadCloser.Read(p)
}

// A second log read for the same attempt is refused while one is open, so
// two failed Closes can never compete for the attempt's retained frame.
func TestLogsDataSingleFrame(t *testing.T) {
	opened, gate := make(chan struct{}), make(chan struct{})
	var bodies int
	var mu sync.Mutex
	wrap := func(rt http.RoundTripper) http.RoundTripper {
		return roundTripFunc(func(r *http.Request) (*http.Response, error) {
			resp, err := rt.RoundTrip(r)
			if err == nil && strings.HasSuffix(r.URL.Path, "/log") {
				mu.Lock()
				bodies++
				mu.Unlock()
				resp.Body = &gateBody{ReadCloser: resp.Body, opened: opened, gate: gate}
			}
			return resp, err
		})
	}
	b, api, ref, _ := startedDataWrap(t, wrap)
	api.setPod(runningTask)
	done := make(chan error)
	go func() {
		_, err := b.Logs(context.Background(), ref, 0, 64)
		done <- err
	}()
	<-opened
	if _, err := b.Logs(context.Background(), ref, 0, 64); !errors.Is(err, errDataBodyHeld) {
		t.Errorf("concurrent log read: %v", err)
	}
	close(gate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	frames, owners := len(b.dataFrames), b.retryOwners[ref.ID]
	b.mu.Unlock()
	if bodies != 1 || frames != 0 || owners != 0 {
		t.Fatalf("bodies=%d frames=%d owners=%d", bodies, frames, owners)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
