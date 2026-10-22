package kube

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/rest"
)

const (
	poolKey   = "skygo.example.com/pool"
	poolValue = "qualified"
)

// qualificationAPI serves the model deployment facts a qualified target
// reads: one namespace, a server version, pool nodes, PVCs and storage
// classes. It is a fixture for actual client-go requests over HTTPS. Its
// facts are invented, not observations of any cluster.
type qualificationAPI struct {
	mu           sync.Mutex
	namespaceUID string
	version      string
	nodes        []corev1.Node
	pvcs         map[string]corev1.PersistentVolumeClaim
	classes      map[string]storagev1.StorageClass
	cont         string // continue token added to node lists
	pad          int    // bytes of trailing whitespace added to node lists
	namespace    string // namespace name returned, if not the requested one
	versionBody  []byte // /version body, if not the encoded version
	unfiltered   bool   // node lists ignore the label selector
}

func modelNode(name, uid string) corev1.Node {
	return corev1.Node{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Node"},
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(uid), Labels: map[string]string{poolKey: poolValue}},
		Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{KubeletVersion: "v1.32.3", ContainerRuntimeVersion: "containerd://2.0.0",
			KernelVersion: "6.8.0", OperatingSystem: "linux", Architecture: "arm64"}}}
}

func newQualificationAPI() *qualificationAPI {
	class := "local"
	other := modelNode("node-other", "other-uid")
	other.Labels = map[string]string{poolKey: "unqualified"}
	return &qualificationAPI{namespaceUID: "namespace-uid", version: "v1.32.3",
		nodes: []corev1.Node{modelNode("node-1", "node-uid-1"), other},
		pvcs: map[string]corev1.PersistentVolumeClaim{"datasets": {TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
			ObjectMeta: metav1.ObjectMeta{Name: "datasets", Namespace: "workloads", UID: "pvc-uid"},
			Spec:       corev1.PersistentVolumeClaimSpec{StorageClassName: &class}}},
		classes: map[string]storagev1.StorageClass{"local": {TypeMeta: metav1.TypeMeta{APIVersion: "storage.k8s.io/v1", Kind: "StorageClass"},
			ObjectMeta: metav1.ObjectMeta{Name: "local"}, Provisioner: "example.com/csi"}},
	}
}

func (q *qualificationAPI) set(f func(*qualificationAPI)) {
	q.mu.Lock()
	defer q.mu.Unlock()
	f(q)
}

// qualificationRead reports whether a logged request is a qualification read.
func qualificationRead(request string) bool {
	path, ok := strings.CutPrefix(request, "GET ")
	return ok && (path == "/version" || path == "/api/v1/nodes" || path == "/api/v1/namespaces/workloads" ||
		strings.HasPrefix(path, "/api/v1/namespaces/workloads/persistentvolumeclaims/") ||
		strings.HasPrefix(path, "/apis/storage.k8s.io/v1/storageclasses/"))
}

// serve answers r if it is a qualification read and reports whether it was.
func (q *qualificationAPI) serve(w http.ResponseWriter, r *http.Request) bool {
	if !qualificationRead(r.Method + " " + r.URL.Path) {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	var obj any
	p := r.URL.Path
	switch {
	case p == "/version" && q.versionBody != nil:
		w.Write(q.versionBody)
		return true
	case p == "/version":
		obj = version.Info{Major: "1", Minor: "32", GitVersion: q.version}
	case p == "/api/v1/namespaces/workloads":
		name := "workloads"
		if q.namespace != "" {
			name = q.namespace
		}
		obj = corev1.Namespace{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
			ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(q.namespaceUID)}}
	case p == "/api/v1/nodes":
		selector, err := labels.Parse(r.URL.Query().Get("labelSelector"))
		if err != nil {
			w.WriteHeader(400)
			return true
		}
		list := corev1.NodeList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "NodeList"}, ListMeta: metav1.ListMeta{Continue: q.cont}}
		for _, n := range q.nodes {
			if q.unfiltered || selector.Matches(labels.Set(n.Labels)) {
				list.Items = append(list.Items, n)
			}
		}
		obj = list
	case strings.HasPrefix(p, "/api/v1/namespaces/workloads/persistentvolumeclaims/"):
		pvc, ok := q.pvcs[strings.TrimPrefix(p, "/api/v1/namespaces/workloads/persistentvolumeclaims/")]
		if !ok {
			notFound(w)
			return true
		}
		obj = pvc
	default:
		class, ok := q.classes[strings.TrimPrefix(p, "/apis/storage.k8s.io/v1/storageclasses/")]
		if !ok {
			notFound(w)
			return true
		}
		obj = class
	}
	raw, _ := json.Marshal(obj)
	if p == "/api/v1/nodes" && q.pad > 0 {
		raw = append(raw[:len(raw)-1], append(bytes.Repeat([]byte(" "), q.pad), '}')...)
	}
	w.Write(raw)
	return true
}

func notFound(w http.ResponseWriter) {
	w.WriteHeader(404)
	json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Code: 404, Reason: metav1.StatusReasonNotFound})
}

// modelImplementation is invented implementation identity for model fixtures
// that install a record without loading a build receipt.
func modelImplementation() qualificationImplementation {
	return qualificationImplementation{ExecutableSHA256: strings.Repeat("e", 64), BuildReceiptSHA256: strings.Repeat("f", 64),
		HelperImage: helperImage, HelperSHA256: strings.Repeat("3", 64), HelperPlatformDigest: strings.Repeat("4", 64),
		HelperOS: "linux", HelperArchitecture: "arm64", OS: runtime.GOOS, Architecture: runtime.GOARCH}
}

// liveRecord returns a record matching the model API facts, target and REST
// configuration, for implementation impl. It is test input written to match
// the model; the record producer and its native evidence are separate and
// unrun, and these receipt names prove nothing.
func liveRecord(t *testing.T, target TargetConfig, config *rest.Config, impl qualificationImplementation) *qualificationRecord {
	t.Helper()
	d, err := newDataProfile(target.Data)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := bindingJSON(target, config, d, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := strings.Repeat("a", 64)
	r := &qualificationRecord{Version: qualificationRecordVersion, Composition: qualificationComposition,
		Implementation: impl, TargetSHA256: fmt.Sprintf("%x", sha256.Sum256(raw)),
		Deployment: qualificationDeployment{APIServer: config.Host, CASHA256: fmt.Sprintf("%x", sha256.Sum256(config.CAData)),
			Namespace: "workloads", NamespaceUID: "namespace-uid", KubernetesVersion: "v1.32.3",
			Pool: qualificationPool{Key: poolKey, Value: poolValue}, PolicyRevision: "operator-policy-1",
			Nodes:   []qualificationNode{{Name: "node-1", UID: "node-uid-1", Kubelet: "v1.32.3", Runtime: "containerd://2.0.0", Kernel: "6.8.0", OS: "linux", Architecture: "arm64"}},
			Storage: []qualificationStorage{{Reference: "datasets", PVCUID: "pvc-uid", StorageClass: "local", Driver: "example.com/csi", PolicyRevision: "storage-1"}}},
		Images: []qualificationImage{{Image: taskImage, PlatformDigest: h, OS: "linux", Architecture: "arm64",
			Profiles: []string{"mount-only", "workdir", "workdir-output", "workdir-setup", "workdir-setup-output"}, FixedPathsSHA256: qualificationPathsID(),
			DefaultDirectory: true, Directories: []string{"/srv"},
			Mounts: []qualificationMount{{Reference: "creds", Destination: "/mnt/skygo/creds", ReadOnly: true},
				{Reference: "datasets", Destination: "/mnt/skygo/data"}, {Reference: "datasets", Destination: "/mnt/skygo/data", ReadOnly: true}}}},
		Evidence: qualificationEvidence{RunID: strings.Repeat("b", 32), CompletedAt: time.Date(2026, 10, 20, 1, 2, 3, 0, time.UTC), ReceiptSHA256: h},
	}
	for _, name := range []string{"input-output-digests", "long-task-renewal", "main-failure", "model-fixture-not-native", "mount-only", "normal-retirement", "paths-security", "pvc-persistence", "secret-missing-key", "secret-projection", "setup-before-main", "setup-no-launch", "stage-no-launch", "worker-tls-credentials"} {
		r.Evidence.Cases = append(r.Evidence.Cases, qualificationCase{Name: name, ReceiptSHA256: h})
	}
	return r
}

// testExecutable is the digest of this test binary, computed independently
// of executableSHA256.
var testExecutable = sync.OnceValues(func() (string, error) {
	name, err := os.Executable()
	if err != nil {
		return "", err
	}
	b, err := os.ReadFile(name)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
})

// liveFixture is a data target whose record and build receipt are private
// files, constructed through the production path against the model API.
type liveFixture struct {
	api     *dataAPI
	target  TargetConfig
	config  *rest.Config
	record  *qualificationRecord
	receipt *qualificationBuild
	dir     string
}

func newLiveFixture(t *testing.T, wrap func(http.RoundTripper) http.RoundTripper) *liveFixture {
	t.Helper()
	exe, err := testExecutable()
	if err != nil {
		t.Fatal(err)
	}
	api, target, config := newDataServer(t, wrap, nil)
	l := &liveFixture{api: api, target: target, config: config, dir: t.TempDir()}
	impl := modelImplementation()
	l.receipt = &qualificationBuild{Version: qualificationBuildVersion, ExecutableSHA256: exe, HelperImage: impl.HelperImage,
		HelperSHA256: impl.HelperSHA256, HelperPlatformDigest: impl.HelperPlatformDigest, HelperOS: impl.HelperOS, HelperArchitecture: impl.HelperArchitecture}
	l.target.Data.Qualification = &DataQualification{Record: filepath.Join(l.dir, "record.json"), BuildReceipt: filepath.Join(l.dir, "build.json"),
		PolicyRevision: "operator-policy-1", StoragePolicy: map[string]string{"datasets": "storage-1"}}
	l.writeReceipt(t)
	l.record = liveRecord(t, l.target, config, l.implementation(t))
	l.writeRecord(t)
	return l
}

func (l *liveFixture) receiptBytes(t *testing.T) []byte {
	t.Helper()
	b, err := json.Marshal(l.receipt)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// implementation is what loadQualificationBuild must return for the fixture.
func (l *liveFixture) implementation(t *testing.T) qualificationImplementation {
	sum := sha256.Sum256(l.receiptBytes(t))
	impl := modelImplementation()
	impl.ExecutableSHA256 = l.receipt.ExecutableSHA256
	impl.BuildReceiptSHA256 = hex.EncodeToString(sum[:])
	return impl
}

func writePrivate(t *testing.T, name string, b []byte) {
	t.Helper()
	os.Remove(name)
	if err := os.WriteFile(name, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func (l *liveFixture) writeReceipt(t *testing.T) {
	writePrivate(t, l.target.Data.Qualification.BuildReceipt, l.receiptBytes(t))
}

func (l *liveFixture) writeRecord(t *testing.T) {
	t.Helper()
	b, err := encodeQualificationRecord(l.record)
	if err != nil {
		t.Fatal(err)
	}
	writePrivate(t, l.target.Data.Qualification.Record, b)
}

func (l *liveFixture) construct() (backend.Target, error) {
	return fromRESTContext(context.Background(), l.target, "owner", l.config)
}

// reads returns the qualification reads in the logged requests.
func (a *dataAPI) reads() []string {
	return slices.DeleteFunc(a.log(), func(r string) bool { return !qualificationRead(r) })
}

func qualifiedTask() backend.Task {
	t := stagedTask()
	t.NodeSelector = map[string]string{poolKey: poolValue}
	return t
}

func TestQualifiedTargetStartup(t *testing.T) {
	l := newLiveFixture(t, nil)
	target, err := l.construct()
	if err != nil {
		t.Fatal(err)
	}
	if !target.SupportsRemoteData || !target.SupportsDataMounts || target.SupportsRemoteDataRetry || target.DataPolicy == nil {
		t.Fatalf("capabilities = %v %v %v", target.SupportsRemoteData, target.SupportsDataMounts, target.SupportsRemoteDataRetry)
	}
	want := []string{"GET /api/v1/namespaces/workloads", "GET /version", "GET /api/v1/nodes",
		"GET /api/v1/namespaces/workloads/persistentvolumeclaims/datasets", "GET /apis/storage.k8s.io/v1/storageclasses/local"}
	if got := l.api.log(); !slices.Equal(got, want) {
		t.Fatalf("startup requests = %v", got)
	}
	b := target.Backend.(*Backend)
	id, err := qualificationRecordID(l.record)
	if err != nil {
		t.Fatal(err)
	}
	if b.dataQual == nil || b.dataQual.id != id || b.dataQual.record == l.record {
		t.Fatal("installed record is not the detached validated record")
	}
	d, err := newDataProfile(l.target.Data)
	if err != nil {
		t.Fatal(err)
	}
	pre, err := targetBinding(l.target, l.config, d, nil)
	if err != nil {
		t.Fatal(err)
	}
	final, err := targetBinding(l.target, l.config, d, &dataQualification{id: id})
	if err != nil || target.Binding != final || final == pre || b.binding != final {
		t.Fatalf("binding = %s, want %s (pre-q %s)", target.Binding, final, pre)
	}
	// File names are input policy, not target identity.
	other := t.TempDir()
	for _, f := range []*string{&l.target.Data.Qualification.Record, &l.target.Data.Qualification.BuildReceipt} {
		b, err := os.ReadFile(*f)
		if err != nil {
			t.Fatal(err)
		}
		*f = filepath.Join(other, filepath.Base(*f))
		writePrivate(t, *f, b)
	}
	moved, err := l.construct()
	if err != nil || moved.Binding != target.Binding {
		t.Fatalf("moved files changed the binding: %v", err)
	}
	if len(l.api.fullRead) != 0 {
		t.Fatalf("startup read secrets: %v", l.api.fullRead)
	}
	n := len(l.api.log())
	if err := b.Check(qualifiedTask()); err != nil {
		t.Fatal(err)
	}
	if err := b.Check(stagedTask()); err == nil || !strings.Contains(err.Error(), "not qualified") {
		t.Fatalf("task without the pool selector: %v", err)
	}
	retry := qualifiedTask()
	retry.Retry = &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{1}}
	if err := b.Check(retry); err == nil {
		t.Fatal("staged retry accepted")
	}
	if len(l.api.log()) != n {
		t.Fatal("Check made requests")
	}
}

// A record without mount coverage installs remote data support but not data
// mounts, and selects no storage reads.
func TestQualifiedTargetWithoutMounts(t *testing.T) {
	l := newLiveFixture(t, nil)
	l.record.Images[0].Mounts = nil
	l.record.Deployment.Storage = nil
	l.writeRecord(t)
	target, err := l.construct()
	if err != nil {
		t.Fatal(err)
	}
	if !target.SupportsRemoteData || target.SupportsDataMounts || target.SupportsRemoteDataRetry {
		t.Fatalf("capabilities = %v %v %v", target.SupportsRemoteData, target.SupportsDataMounts, target.SupportsRemoteDataRetry)
	}
	if got := l.api.log(); len(got) != 3 {
		t.Fatalf("requests = %v", got)
	}
	if err := target.Backend.Check(qualifiedTask()); err == nil {
		t.Fatal("uncovered mount accepted")
	}
}

// Without a configured record a data target makes no request and reports no
// data support, whatever its data policy.
func TestQualificationAbsentRecord(t *testing.T) {
	l := newLiveFixture(t, nil)
	if _, err := fromREST(l.target, "owner", l.config); err == nil {
		t.Fatal("context-free constructor accepted a qualification")
	}
	l.target.Data.Qualification = nil
	for _, construct := range []func() (backend.Target, error){
		l.construct,
		func() (backend.Target, error) { return fromREST(l.target, "owner", l.config) },
	} {
		target, err := construct()
		if err != nil {
			t.Fatal(err)
		}
		b := target.Backend.(*Backend)
		if b.dataQual != nil || target.SupportsRemoteData || target.SupportsDataMounts || target.SupportsRemoteDataRetry || target.DataPolicy == nil {
			t.Fatalf("unqualified target: %+v", target)
		}
		if err := b.Check(qualifiedTask()); err == nil {
			t.Fatal("unqualified target accepted a data task")
		}
	}
	if n := len(l.api.log()); n != 0 {
		t.Fatalf("requests = %v", l.api.log())
	}
}

// A configured record that is absent, malformed, unsafe or for another build
// or target refuses the target before any API request.
func TestQualificationRefusedBeforeReads(t *testing.T) {
	tests := []struct {
		name   string
		change func(*testing.T, *liveFixture)
	}{
		{"missing record", func(t *testing.T, l *liveFixture) { os.Remove(l.target.Data.Qualification.Record) }},
		{"noncanonical record", func(t *testing.T, l *liveFixture) {
			b, _ := os.ReadFile(l.target.Data.Qualification.Record)
			writePrivate(t, l.target.Data.Qualification.Record, append(b, '\n'))
		}},
		{"shared record", func(t *testing.T, l *liveFixture) { os.Chmod(l.target.Data.Qualification.Record, 0o644) }},
		{"record symlink", func(t *testing.T, l *liveFixture) {
			name := l.target.Data.Qualification.Record
			os.Rename(name, name+".real")
			os.Symlink(name+".real", name)
		}},
		{"missing receipt", func(t *testing.T, l *liveFixture) { os.Remove(l.target.Data.Qualification.BuildReceipt) }},
		{"other executable", func(t *testing.T, l *liveFixture) {
			l.receipt.ExecutableSHA256 = strings.Repeat("0", 64)
			l.writeReceipt(t)
		}},
		{"receipt helper", func(t *testing.T, l *liveFixture) {
			l.receipt.HelperPlatformDigest = strings.Repeat("5", 64)
			l.writeReceipt(t)
		}},
		{"record executable", func(t *testing.T, l *liveFixture) {
			l.record.Implementation.ExecutableSHA256 = strings.Repeat("0", 64)
			l.writeRecord(t)
		}},
		{"record platform", func(t *testing.T, l *liveFixture) {
			l.record.Implementation.Architecture = "other"
			l.writeRecord(t)
		}},
		{"target binding", func(t *testing.T, l *liveFixture) { l.target.Labels = map[string]string{"changed": "binding"} }},
		{"pool policy", func(t *testing.T, l *liveFixture) { l.target.Resources.NodeSelectors = nil }},
		{"uncovered layout", func(t *testing.T, l *liveFixture) {
			l.record.Images[0].FixedPathsSHA256 = strings.Repeat("0", 64)
			l.writeRecord(t)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := newLiveFixture(t, nil)
			tt.change(t, l)
			target, err := l.construct()
			if err == nil || target.Backend != nil {
				t.Fatal("target constructed")
			}
			if n := len(l.api.log()); n != 0 {
				t.Fatalf("requests = %v", l.api.log())
			}
		})
	}
}

// Observed or operator-asserted deployment facts that differ from the record
// refuse the target after the startup reads.
func TestQualificationStartupDrift(t *testing.T) {
	tests := []struct {
		name   string
		change func(*liveFixture)
	}{
		{"namespace uid", func(l *liveFixture) { l.api.env.namespaceUID = "replacement" }},
		{"server version", func(l *liveFixture) { l.api.env.version = "v1.32.4" }},
		{"extra pool node", func(l *liveFixture) { l.api.env.nodes = append(l.api.env.nodes, modelNode("node-2", "node-uid-2")) }},
		{"replaced node", func(l *liveFixture) { l.api.env.nodes[0].UID = "replacement" }},
		{"node runtime", func(l *liveFixture) {
			l.api.env.nodes[0].Status.NodeInfo.ContainerRuntimeVersion = "containerd://2.1.0"
		}},
		{"node kernel", func(l *liveFixture) { l.api.env.nodes[0].Status.NodeInfo.KernelVersion = "6.9.0" }},
		{"node kubelet", func(l *liveFixture) { l.api.env.nodes[0].Status.NodeInfo.KubeletVersion = "v1.32.4" }},
		{"node architecture", func(l *liveFixture) { l.api.env.nodes[0].Status.NodeInfo.Architecture = "amd64" }},
		{"empty pool", func(l *liveFixture) { l.api.env.nodes = l.api.env.nodes[1:] }},
		{"paginated pool", func(l *liveFixture) { l.api.env.cont = "more" }},
		{"oversize pool", func(l *liveFixture) {
			for i := range maxQualificationNodes {
				l.api.env.nodes = append(l.api.env.nodes, modelNode(fmt.Sprintf("node-x%02d", i), fmt.Sprintf("uid-%d", i)))
			}
		}},
		{"replaced pvc", func(l *liveFixture) {
			pvc := l.api.env.pvcs["datasets"]
			pvc.UID = "replacement"
			l.api.env.pvcs["datasets"] = pvc
		}},
		{"storage class", func(l *liveFixture) {
			pvc := l.api.env.pvcs["datasets"]
			class := "other"
			pvc.Spec.StorageClassName = &class
			l.api.env.pvcs["datasets"] = pvc
			l.api.env.classes["other"] = storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "other"}, Provisioner: "example.com/csi"}
		}},
		{"provisioner", func(l *liveFixture) {
			sc := l.api.env.classes["local"]
			sc.Provisioner = "other.example.com/csi"
			l.api.env.classes["local"] = sc
		}},
		{"missing pvc", func(l *liveFixture) { delete(l.api.env.pvcs, "datasets") }},
		{"namespace name", func(l *liveFixture) { l.api.env.namespace = "other" }},
		{"version body", func(l *liveFixture) { l.api.env.versionBody = []byte("v1.32.3") }},
		{"unlabeled node", func(l *liveFixture) { l.api.env.unfiltered = true }},
		{"pvc name", func(l *liveFixture) {
			pvc := l.api.env.pvcs["datasets"]
			pvc.Name = "other"
			l.api.env.pvcs["datasets"] = pvc
		}},
		{"no storage class", func(l *liveFixture) {
			pvc := l.api.env.pvcs["datasets"]
			pvc.Spec.StorageClassName = nil
			l.api.env.pvcs["datasets"] = pvc
		}},
		{"storage class name", func(l *liveFixture) {
			sc := l.api.env.classes["local"]
			sc.Name = "other"
			l.api.env.classes["local"] = sc
		}},
		{"operator policy", func(l *liveFixture) { l.target.Data.Qualification.PolicyRevision = "operator-policy-2" }},
		{"storage policy", func(l *liveFixture) { l.target.Data.Qualification.StoragePolicy["datasets"] = "storage-2" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := newLiveFixture(t, nil)
			l.api.env.set(func(*qualificationAPI) { tt.change(l) })
			target, err := l.construct()
			if err == nil || target.Backend != nil {
				t.Fatal("drifted deployment accepted")
			}
			// A missing PVC fails its read; every other change is drift.
			read := tt.name == "missing pvc"
			var re *qualificationReadError
			if errors.As(err, &re) != read || !read && !errors.Is(err, errQualificationCoverage) {
				t.Fatalf("err = %T %v, want read failure %v", err, err, read)
			}
			if len(l.api.log()) == 0 || len(l.api.reads()) != len(l.api.log()) {
				t.Fatalf("requests = %v", l.api.log())
			}
		})
	}
}

// Every startup response body is limited before decoding.
func TestQualificationStartupByteLimit(t *testing.T) {
	for _, tt := range []struct {
		name string
		pad  int
		ok   bool
	}{
		{"within limit", maxQualificationResponse - 4096, true},
		{"over limit", maxQualificationResponse, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ft := &faultTransport{}
			l := newLiveFixture(t, ft.wrap)
			l.api.env.set(func(q *qualificationAPI) { q.pad = tt.pad })
			_, err := l.construct()
			if tt.ok {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var re *qualificationReadError
			if !errors.As(err, &re) || !errors.Is(err, errDataBodyLimit) || errors.Is(err, errDataBodyHeld) {
				t.Fatalf("err = %v", err)
			}
			// The limited body was still closed exactly once, cleanly, and
			// nothing was read after it.
			if got := l.api.log(); len(got) != 3 || got[2] != "GET /api/v1/nodes" || len(ft.bodies) != 3 {
				t.Fatalf("requests = %v", got)
			}
			for _, b := range ft.bodies {
				if b.closes.Load() != 1 {
					t.Fatal("body not closed once")
				}
			}
			if !re.frame.ended || re.frame.held || len(re.frame.failed) != 0 {
				t.Fatalf("frame = %+v", re.frame)
			}
		})
	}
}

// A startup body whose Close fails refuses the target with a typed error
// that retains the frame, the original body and its exact Close error.
func TestQualificationStartupBodyCloseFailure(t *testing.T) {
	readErr := errors.New("read failed")
	for _, tt := range []struct {
		name   string
		suffix string
		reads  int
		read   error
	}{
		{"namespace", "/namespaces/workloads", 1, nil},
		{"version", "/version", 2, nil},
		{"nodes read and close", "/nodes", 3, readErr},
		{"storage class", "/storageclasses/local", 5, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ft := &faultTransport{}
			l := newLiveFixture(t, ft.wrap)
			body := &faultBody{readErr: tt.read, closeErr: errClose}
			ft.arm("GET", tt.suffix, 1, body)
			target, err := l.construct()
			if target.Backend != nil {
				t.Fatal("target constructed")
			}
			var re *qualificationReadError
			if !errors.As(err, &re) || !errors.Is(err, errDataBodyHeld) || err.Error() != errDataBodyHeld.Error() {
				t.Fatalf("err = %v", err)
			}
			if tt.read != nil && !errors.Is(err, tt.read) {
				t.Fatalf("read failure lost: %v", err)
			}
			f := re.frame
			if !f.ended || !f.held || len(f.failed) != 1 || f.failed[0].ReadCloser != io.ReadCloser(body) || f.failed[0].err != errClose || len(f.open) != 0 {
				t.Fatalf("custody = %+v", f)
			}
			if body.closes.Load() != 1 || len(l.api.log()) != tt.reads || ft.bodies[len(ft.bodies)-1] != body {
				t.Fatalf("requests after the held body: %v", l.api.log())
			}
		})
	}
}

// Construction waits for the original Close of a startup body; a deadline
// does not stand in for it.
func TestQualificationStartupBlockedClose(t *testing.T) {
	ft := &faultTransport{}
	l := newLiveFixture(t, ft.wrap)
	body := &faultBody{entered: make(chan struct{}), release: make(chan struct{})}
	ft.arm("GET", "/version", 1, body)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() {
		_, err := fromRESTContext(ctx, l.target, "owner", l.config)
		done <- err
	}()
	<-body.entered
	cancel()
	select {
	case err := <-done:
		t.Fatalf("returned during blocked Close: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(body.release)
	if err := <-done; err == nil {
		t.Fatal("canceled context accepted")
	}
	if body.closes.Load() != 1 {
		t.Fatal("body not closed once")
	}
}

// startedLive constructs a qualified target and starts one attempt on it.
func startedLive(t *testing.T, wrap func(http.RoundTripper) http.RoundTripper) (*liveFixture, *Backend, backend.Reference, backend.DataResources) {
	t.Helper()
	l := newLiveFixture(t, wrap)
	target, err := l.construct()
	if err != nil {
		t.Fatal(err)
	}
	b := target.Backend.(*Backend)
	s := dataStart(b, qualifiedTask())
	c, err := b.StartData(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	ref := s.Execution
	ref.Instance = c.Instance
	return l, b, ref, backend.DataResources{Execution: ref, Pod: c.Pod, InputSecret: c.InputSecret, OutputSecret: c.OutputSecret}
}

const secondID = "00112233445566778899aabbccddeeff"

func secondStart(b *Backend) backend.DataStart {
	s := dataStart(b, qualifiedTask())
	s.Execution.ID = secondID
	return s
}

// Only an observed difference with no held body is drift; anything else
// remains a read failure that retains its frame.
func TestObservedDrift(t *testing.T) {
	for _, tt := range []struct {
		err  error
		want bool
	}{
		{nil, false},
		{errQualificationCoverage, true},
		{errors.New("read failed"), false},
		{errDataBodyHeld, false},
		{errors.Join(errQualificationCoverage, errDataBodyHeld), false},
		{withHeld(errQualificationCoverage), false},
	} {
		if got := observedDrift(tt.err); got != tt.want {
			t.Errorf("observedDrift(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

// A deployment that the reads cannot match to any record is drift at
// StartData, not a failed read.
func TestStartDataStructuralDrift(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*qualificationAPI)
	}{
		{"paginated pool", func(q *qualificationAPI) { q.cont = "more" }},
		{"namespace name", func(q *qualificationAPI) { q.namespace = "other" }},
		{"version body", func(q *qualificationAPI) { q.versionBody = []byte("v1.32.3") }},
		{"unlabeled node", func(q *qualificationAPI) { q.unfiltered = true }},
		{"no storage class", func(q *qualificationAPI) {
			pvc := q.pvcs["datasets"]
			pvc.Spec.StorageClassName = nil
			q.pvcs["datasets"] = pvc
		}},
		{"oversize pool", func(q *qualificationAPI) {
			for i := range maxQualificationNodes {
				q.nodes = append(q.nodes, modelNode(fmt.Sprintf("node-x%02d", i), fmt.Sprintf("uid-%d", i)))
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l, b, _, _ := startedLive(t, nil)
			l.api.env.set(tt.change)
			n := len(l.api.log())
			c, err := b.StartData(context.Background(), secondStart(b))
			var re *qualificationReadError
			if err == nil || !errors.Is(err, errQualificationCoverage) || errors.As(err, &re) || c != (backend.DataCreated{}) {
				t.Fatalf("drifted start: %+v %T %v", c, err, err)
			}
			if !strings.Contains(err.Error(), "deployment changed") {
				t.Fatalf("err = %v", err)
			}
			if posts(l.api.log()[n:]) != 0 {
				t.Fatalf("drifted start requests = %v", l.api.log()[n:])
			}
		})
	}
}

func posts(log []string) int {
	n := 0
	for _, r := range log {
		if strings.HasPrefix(r, "POST") {
			n++
		}
	}
	return n
}

// StartData repeats the qualification reads in its own frame before any
// create. Drift refuses the new attempt with no create, while the earlier
// attempt stays observable and removable.
func TestStartDataDriftBeforePost(t *testing.T) {
	l, b, ref, r := startedLive(t, nil)
	log := l.api.log()
	if len(log) != 13 || !slices.Equal(log[5:10], log[:5]) || posts(log[:10]) != 0 || posts(log[10:]) != 3 {
		t.Fatalf("requests = %v", log)
	}
	l.api.env.set(func(q *qualificationAPI) { q.nodes = append(q.nodes, modelNode("node-2", "node-uid-2")) })
	n := len(l.api.log())
	c, err := b.StartData(context.Background(), secondStart(b))
	if err == nil || !errors.Is(err, errQualificationCoverage) || c != (backend.DataCreated{}) {
		t.Fatalf("drifted start: %+v %v", c, err)
	}
	if got := l.api.log()[n:]; len(got) != 5 || posts(got) != 0 {
		t.Fatalf("drifted start requests = %v", got)
	}
	if f, owners := heldFrame(b, secondID); f != nil || owners != 0 {
		t.Fatal("drift left the operation owned")
	}
	// The record and the binding are unchanged; earlier resources remain
	// under their original identity.
	l.api.setPod(settledTask)
	if o, err := b.ObserveData(context.Background(), ref); err != nil || o.Phase != backend.DataSettled {
		t.Fatalf("observe after drift: %+v %v", o, err)
	}
	out, err := b.RemoveData(context.Background(), r)
	if err != nil || !out.PodRemoved || !out.InputRemoved || !out.OutputRemoved {
		t.Fatalf("remove after drift: %+v %v", out, err)
	}
	if got := l.api.log()[n+5:]; slices.ContainsFunc(got, qualificationRead) {
		t.Fatalf("observation or removal reread qualification: %v", got)
	}
}

// A drift read whose body Close fails holds StartData's own frame and claim,
// with that exact body, and nothing is created.
func TestStartDataDriftBodyCloseFailure(t *testing.T) {
	ft := &faultTransport{}
	l := newLiveFixture(t, ft.wrap)
	target, err := l.construct()
	if err != nil {
		t.Fatal(err)
	}
	b := target.Backend.(*Backend)
	body := &faultBody{closeErr: errClose}
	ft.arm("GET", "/nodes", 1, body)
	s := dataStart(b, qualifiedTask())
	c, err := b.StartData(context.Background(), s)
	if c != (backend.DataCreated{}) {
		t.Fatalf("receipts = %+v", c)
	}
	checkHeld(t, b, s.Execution.ID, err, 1, body)
	var re *qualificationReadError
	if !errors.As(err, &re) || re.frame == nil || re.frame.failed[0].ReadCloser != io.ReadCloser(body) {
		t.Fatalf("err = %v", err)
	}
	if got := l.api.log()[5:]; len(got) != 3 || posts(got) != 0 {
		t.Fatalf("requests = %v", got)
	}
	n := len(l.api.log())
	if _, err := b.StartData(context.Background(), s); err == nil || len(l.api.log()) != n || body.closes.Load() != 1 {
		t.Fatal("held start retried")
	}
}

// StartData limits each of its response bodies before decoding.
func TestStartDataDriftByteLimit(t *testing.T) {
	l := newLiveFixture(t, nil)
	target, err := l.construct()
	if err != nil {
		t.Fatal(err)
	}
	b := target.Backend.(*Backend)
	l.api.env.set(func(q *qualificationAPI) { q.pad = maxQualificationResponse })
	c, err := b.StartData(context.Background(), dataStart(b, qualifiedTask()))
	if !errors.Is(err, errDataBodyLimit) || c != (backend.DataCreated{}) || posts(l.api.log()) != 0 {
		t.Fatalf("receipts = %+v err = %v requests = %v", c, err, l.api.log())
	}
	if f, owners := heldFrame(b, dataID); f != nil || owners != 0 {
		t.Fatal("limited start left the operation owned")
	}
}

// The constructed target keeps its own copies: later changes to the caller's
// configuration or files change neither its binding nor its premise.
func TestQualificationMutationIsolation(t *testing.T) {
	l := newLiveFixture(t, nil)
	target, err := l.construct()
	if err != nil {
		t.Fatal(err)
	}
	b := target.Backend.(*Backend)
	q := l.target.Data.Qualification
	q.PolicyRevision = "changed"
	q.StoragePolicy["datasets"] = "changed"
	l.target.Data.References[0].Destinations[0] = "/mnt/skygo/changed"
	l.target.Resources.NodeSelectors[poolKey][0] = "changed"
	l.record.Deployment.Nodes[0].UID = "changed"
	l.writeRecord(t)
	os.Remove(q.BuildReceipt)
	if _, err := b.StartData(context.Background(), dataStart(b, qualifiedTask())); err != nil {
		t.Fatal(err)
	}
	if b.dataQual.premise.policy != "operator-policy-1" || b.dataQual.premise.storage["datasets"] != "storage-1" || b.dataQual.record.Deployment.Nodes[0].UID != "node-uid-1" {
		t.Fatal("caller mutation reached the installed qualification")
	}
}

func TestQualificationBuildReceipt(t *testing.T) {
	l := newLiveFixture(t, nil)
	got, err := loadQualificationBuild(l.target.Data.Qualification.BuildReceipt)
	if err != nil || got != l.implementation(t) {
		t.Fatalf("implementation = %+v, %v", got, err)
	}
	if got.OS != runtime.GOOS || got.Architecture != runtime.GOARCH {
		t.Fatal("runtime platform not observed")
	}
	raw := l.receiptBytes(t)
	for name, b := range map[string][]byte{
		"empty": nil, "newline": append(bytes.Clone(raw), '\n'), "space": append([]byte(" "), raw...),
		"unknown":   append([]byte(`{"extra":0,`), raw[1:]...),
		"duplicate": append([]byte(`{"version":"ignored",`), raw[1:]...),
		"trailing":  append(bytes.Clone(raw), []byte("{}")...),
		"version":   bytes.Replace(raw, []byte(qualificationBuildVersion), []byte("skygo-kubernetes-build-receipt-v2"), 1),
		"oversize":  bytes.Repeat([]byte(" "), maxQualificationBuild+1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeQualificationBuild(b); !errors.Is(err, errQualificationBuild) {
				t.Fatalf("err = %v", err)
			}
		})
	}
	// A file that cannot be read still has its single Close made, and that
	// failure is retained with the original handle. The file is in a
	// persistent fixture: if its first Close fails, the fixture keeps the
	// original file, cause and namespace, and nothing is probed or removed.
	fixture := newQualificationRecordFixture(t, 1)
	previous := qualificationRecordFixtures[1]
	f, err := os.CreateTemp(fixture.directory, "receipt")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		fixture.retain(f, err)
		t.Fatal("first original close failed; fixture retained")
	}
	_, err = readQualificationFile(f, maxQualificationBuild)
	if err != nil {
		fixture.retain(f, err)
	}
	if fixture.previous != previous {
		t.Fatal("previous original fixture custody lost")
	}
	var fe *qualificationFileError
	if !errors.As(err, &fe) || fe.file != f || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("err = %v", err)
	}
}

func TestQualificationConfig(t *testing.T) {
	q := &DataQualification{Record: "/etc/skygo/record.json", BuildReceipt: "/etc/skygo/build.json", PolicyRevision: "policy-1", StoragePolicy: map[string]string{"datasets": "storage-1"}}
	refs := map[string]dataReference{"datasets": {pvc: "datasets"}, "creds": {secret: "creds"}}
	if err := configQualification(q, refs); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name   string
		change func(*DataQualification)
	}{
		{"relative record", func(q *DataQualification) { q.Record = "record.json" }},
		{"empty receipt", func(q *DataQualification) { q.BuildReceipt = "" }},
		{"empty policy", func(q *DataQualification) { q.PolicyRevision = "" }},
		{"control policy", func(q *DataQualification) { q.PolicyRevision = "a\nb" }},
		{"secret storage", func(q *DataQualification) { q.StoragePolicy = map[string]string{"creds": "x"} }},
		{"unknown storage", func(q *DataQualification) { q.StoragePolicy = map[string]string{"other": "x"} }},
		{"empty storage revision", func(q *DataQualification) { q.StoragePolicy = map[string]string{"datasets": ""} }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := *q
			c.StoragePolicy = map[string]string{"datasets": "storage-1"}
			tt.change(&c)
			if configQualification(&c, refs) == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func ExampleDataQualification() {
	dir, err := os.MkdirTemp("", "skygo-example")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	kubeconfig := filepath.Join(dir, "kubeconfig")
	if err := os.WriteFile(kubeconfig, []byte(`{"apiVersion":"v1","kind":"Config","clusters":[{"name":"c","cluster":{"server":"https://127.0.0.1:1"}}],"contexts":[{"name":"c","context":{"cluster":"c","user":"u"}}],"users":[{"name":"u","user":{}}]}`), 0o600); err != nil {
		panic(err)
	}
	targets := filepath.Join(dir, "targets.yaml")
	if err := os.WriteFile(targets, []byte(`version: 1
targets:
  - name: data
    kubeconfig: ./kubeconfig
    context: c
    namespace: workloads
    resources:
      node_selectors:
        skygo.example.com/pool: [qualified]
    data:
      helper_image: registry.example.com/skygo/helper@sha256:`+strings.Repeat("1", 64)+`
      helper_sha256: "`+strings.Repeat("3", 64)+`"
      origin: https://skygo.example.com
      ca_pem: |
`+indent(testCA(), "        ")+`
      task_images: [registry.example.com/team/train@sha256:`+strings.Repeat("2", 64)+`]
      qualification:
        record: ./qualification.json
        build_receipt: ./build-receipt.json
        policy_revision: operator-policy-1
`), 0o600); err != nil {
		panic(err)
	}
	c, err := ReadConfig(targets, false)
	if err != nil {
		panic(err)
	}
	t := c.Targets()[0]
	fmt.Println(t.Data.Qualification.Record == filepath.Join(dir, "qualification.json"))
	// The record is read before the API server is contacted; without it the
	// target is refused.
	_, err = FromTarget(context.Background(), t, "example")
	fmt.Println(err)
	// Output:
	// true
	// open kubernetes qualification file: no such file or directory
}

func indent(s, prefix string) string {
	return prefix + strings.ReplaceAll(strings.TrimSuffix(s, "\n"), "\n", "\n"+prefix)
}
