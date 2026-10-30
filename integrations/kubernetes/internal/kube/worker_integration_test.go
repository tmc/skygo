package kube

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
	"github.com/tmc/skygo/internal/workeradmission"
	bolt "go.etcd.io/bbolt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// Kubernetes objects, scheduler and container status below are MODELED.
// Persistence, file authentication, Control, Workers, Connect and typed REST
// transport are real. No test here launches a task, helper, Pod or kernel.
func integrationWorkerProfile() WorkerConfig {
	return WorkerConfig{target: "kube", namespaceUID: "namespace-uid-1",
		registry:    workerRegistry{ServiceAccountUID: "service-account-uid-1", PullSecrets: []string{"registry-b", "registry-a"}},
		helperImage: profileHelp, helperSHA256: workerHelperSHA256,
		budget: workeradmission.Budget{Ranks: 4, CPUMillis: 4000, MemoryBytes: 4 << 30}}
}

func integrationWorkerAPI(t *testing.T) *profileAPI {
	t.Helper()
	a := newProfileAPI(t)
	for _, name := range []string{"registry-a", "registry-b"} {
		a.objs["/api/v1/namespaces/workloads/secrets/"+name] = &corev1.Secret{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "workloads", UID: types.UID("uid-" + name)},
			Type:       corev1.SecretTypeDockerConfigJson}
	}
	return a
}

func integrationWorkerTargets(t *testing.T, a *profileAPI) Config {
	t.Helper()
	c := a.targets(t)
	c.targets = c.targets[:1]
	c.targets[0].Resources = ResourcePolicy{}
	return c
}

func newTestWorkerIntegration(t *testing.T) (*workerIntegration, *profileAPI, *store.Store, *auth.Engine) {
	t.Helper()
	a := integrationWorkerAPI(t)
	s, engine := environmentStore(t, true, true)
	got, err := NewWorkerIntegration(context.Background(), s, engine, integrationWorkerTargets(t, a), integrationWorkerProfile())
	if err != nil {
		t.Fatal(err)
	}
	return got.(*workerIntegration), a, s, engine
}

func TestWorkerIntegrationOriginalTarget(t *testing.T) {
	in, a, _, _ := newTestWorkerIntegration(t)
	defer closeIntegration(t, in)
	targets := in.Targets()
	if len(targets) != 1 {
		t.Fatalf("targets: %v", targets)
	}
	b := targets[0].Backend.(*workerTargetBackend)
	if b.Backend != in.rt.b || b.workerRuntime != in.rt || in.rt.b.client == in.rt.http.client || targets[0].Binding != in.rt.b.binding {
		t.Fatal("runtime does not share the original bound Backend")
	}
	if targets[0].SupportsRetry || targets[0].SupportsRemoteData || targets[0].SupportsDataMounts || targets[0].SupportsPersistentEnvironments || targets[0].DataPolicy != nil || len(targets[0].AcceleratorNames) != 0 {
		t.Fatal("worker profile inherited another execution capability")
	}
	if a.requests() != 4 || len(a.writes()) != 0 {
		t.Fatalf("startup reads=%d writes=%v", a.requests(), a.writes())
	}
	profile := integrationWorkerProfile()
	base, err := workerProfileBinding("original", "workloads", profile)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*WorkerConfig){
		func(p *WorkerConfig) { p.namespaceUID = "different" },
		func(p *WorkerConfig) { p.registry.ServiceAccountUID = "different" },
		func(p *WorkerConfig) {
			p.registry.PullSecrets[0], p.registry.PullSecrets[1] = p.registry.PullSecrets[1], p.registry.PullSecrets[0]
		},
		func(p *WorkerConfig) { p.helperSHA256 = strings.Repeat("4", 64) },
		func(p *WorkerConfig) { p.budget.CPUMillis++ },
	} {
		p := profile.clone()
		change(&p)
		got, err := workerProfileBinding("original", "workloads", p)
		if err != nil || got == base {
			t.Fatalf("profile change did not alter binding: %s %v", got, err)
		}
	}
	targets[0].Labels = map[string]string{"changed": "true"}
	if in.Targets()[0].Labels["changed"] != "" {
		t.Fatal("Targets aliases caller metadata")
	}
}

func TestWorkerIntegrationRefusesBeforeLoad(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*Config, *WorkerConfig)
	}{
		{"empty profile", func(_ *Config, p *WorkerConfig) { *p = WorkerConfig{} }},
		{"no target", func(c *Config, _ *WorkerConfig) { c.targets = nil }},
		{"other target", func(c *Config, _ *WorkerConfig) { c.targets[0].Name = "different" }},
		{"two targets", func(c *Config, _ *WorkerConfig) { c.targets = append(c.targets, c.targets[0]) }},
		{"data", func(c *Config, _ *WorkerConfig) { c.targets[0].Data = &DataConfig{} }},
		{"accelerator", func(c *Config, _ *WorkerConfig) {
			c.targets[0].Resources.Accelerators = map[string]AcceleratorPolicy{"gpu": {ResourceName: "example.org/gpu"}}
		}},
		{"dra", func(c *Config, _ *WorkerConfig) {
			c.targets[0].Resources.Accelerators = map[string]AcceleratorPolicy{"gpu": {DRA: &DRAPolicy{}}}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, engine := environmentStore(t, true, true)
			c := Config{targets: []TargetConfig{{Name: "kube", Namespace: "workloads", Kubeconfig: "/absent/never-read", Context: "test"}}}
			p := integrationWorkerProfile()
			tt.edit(&c, &p)
			got, err := NewWorkerIntegration(context.Background(), s, engine, c, p)
			if err == nil || got != nil || strings.Contains(err.Error(), "load kubernetes") {
				t.Fatalf("must refuse before loading external configuration: %v %v", got, err)
			}
		})
	}
	for _, scoped := range []bool{false, true} {
		t.Run(fmt.Sprintf("retention scoped=%v", scoped), func(t *testing.T) {
			s, engine := environmentStore(t, scoped, false)
			c := Config{targets: []TargetConfig{{Name: "kube", Namespace: "workloads", Kubeconfig: "/absent/never-read", Context: "test"}}}
			got, err := NewWorkerIntegration(context.Background(), s, engine, c, integrationWorkerProfile())
			if err == nil || got != nil || strings.Contains(err.Error(), "load kubernetes") {
				t.Fatalf("must require retained scoped authority before credentials: %v %v", got, err)
			}
		})
	}
}

func TestWorkerIntegrationBorrowedMismatch(t *testing.T) {
	for _, edit := range []struct {
		name, path string
		change     func(any)
	}{
		{"namespace", "/api/v1/namespaces/workloads", func(x any) { x.(*corev1.Namespace).UID = "other" }},
		{"account", "/api/v1/namespaces/workloads/serviceaccounts/default", func(x any) { x.(*corev1.ServiceAccount).UID = "other" }},
		{"secret order", "/api/v1/namespaces/workloads/serviceaccounts/default", func(x any) { x.(*corev1.ServiceAccount).ImagePullSecrets[0].Name = "registry-a" }},
		{"secret type", "/api/v1/namespaces/workloads/secrets/registry-a", func(x any) { x.(*corev1.Secret).Type = corev1.SecretTypeOpaque }},
	} {
		t.Run(edit.name, func(t *testing.T) {
			a := integrationWorkerAPI(t)
			a.edit(edit.path, edit.change)
			s, engine := environmentStore(t, true, true)
			got, err := NewWorkerIntegration(context.Background(), s, engine, integrationWorkerTargets(t, a), integrationWorkerProfile())
			if err == nil || got == nil {
				t.Fatalf("mismatch needs actual owner: %v %v", got, err)
			}
			closeIntegration(t, got)
			if len(a.writes()) != 0 {
				t.Fatal("startup changed borrowed objects")
			}
		})
	}
}

func TestWorkerIntegrationConstructorReadFault(t *testing.T) {
	a := integrationWorkerAPI(t)
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "1000")
		io.WriteString(w, `{"kind":"Namespace"}`)
	}))
	defer broken.Close()
	c := integrationWorkerTargets(t, a)
	c.targets[0].Kubeconfig = writeKubeconfig(t, broken.URL)
	s, engine := environmentStore(t, true, true)
	got, err := NewWorkerIntegration(context.Background(), s, engine, c, integrationWorkerProfile())
	if got == nil || !errors.Is(err, backend.ErrWorkerUnsettled) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("original read fault not retained: %v %v", got, err)
	}
	first := got.Close(context.Background())
	if !errors.Is(first, backend.ErrWorkerUnsettled) || !errors.Is(first, io.ErrUnexpectedEOF) {
		t.Fatalf("Close lost original owner/cause: %v", first)
	}
	if again := got.Close(context.Background()); again != first {
		t.Fatal("failed original Close retried")
	}
}

func bindTestWorkerIntegration(t *testing.T, in *workerIntegration, s *store.Store, engine *auth.Engine) *control.Control {
	t.Helper()
	c, err := control.New(s, in.Targets(), engine)
	if err != nil {
		t.Fatal(err)
	}
	if err := in.BindControl(c); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestWorkerIntegrationCloseBeforeRun(t *testing.T) {
	in, _, s, engine := newTestWorkerIntegration(t)
	bindTestWorkerIntegration(t, in, s, engine)
	if err := in.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := in.Run(context.Background()); err == nil {
		t.Fatal("Run started after Close")
	}
	if err := in.BindControl(nil); err == nil {
		t.Fatal("second bind accepted")
	}
}

func TestWorkerIntegrationRunJoin(t *testing.T) {
	in, _, s, engine := newTestWorkerIntegration(t)
	bindTestWorkerIntegration(t, in, s, engine)
	done := make(chan error, 1)
	go func() { done <- in.Run(context.Background()) }()
	workerIntegrationWait(t, func() bool { in.mu.Lock(); defer in.mu.Unlock(); return in.started })
	if err := in.Run(context.Background()); err == nil {
		t.Fatal("second Run accepted")
	}
	in.StopEffects()
	if err := in.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("original Run did not return after Close")
	}
}

func TestWorkerIntegrationSettledStepErrorCloses(t *testing.T) {
	in, _, s, engine := newTestWorkerIntegration(t)
	bindTestWorkerIntegration(t, in, s, engine)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- in.Run(context.Background()) }()
	workerIntegrationWait(t, func() bool { in.mu.Lock(); defer in.mu.Unlock(); return in.stepErr != nil })
	in.StopEffects()
	if err := <-done; err != nil {
		t.Fatalf("settled list failure made shutdown fail: %v", err)
	}
	if err := in.workers.PendingResults(); err != nil {
		t.Fatalf("Store-list failure fabricated a pending native result: %v", err)
	}
	if err := in.Close(context.Background()); err != nil {
		t.Fatalf("settled Store-list failure retained false ownership: %v", err)
	}
}

func TestWorkerIntegrationOriginalCloseFault(t *testing.T) {
	in, _, _, _ := newTestWorkerIntegration(t)
	fault := new(faultTransport)
	fault.rt = in.rt.http.transport.base
	in.rt.http.transport.base = fault
	cause := errors.New("original startup close failed")
	body := &faultBody{closeErr: cause}
	fault.arm(http.MethodGet, "/namespaces/workloads", 1, body)
	err := in.verifyBorrowed(context.Background())
	if !errors.Is(err, backend.ErrWorkerUnsettled) || !errors.Is(err, cause) || body.closes.Load() != 1 {
		t.Fatalf("startup fault: %v closecount=%d", err, body.closes.Load())
	}
	first := in.Close(context.Background())
	if !errors.Is(first, cause) || !errors.Is(first, backend.ErrWorkerUnsettled) {
		t.Fatalf("Close lost raw original: %v", first)
	}
	if in.Close(context.Background()) != first || body.closes.Load() != 1 {
		t.Fatal("failed original response Close repeated")
	}
}

func TestWorkerIntegrationCloseDeadlineSticky(t *testing.T) {
	in, _, _, _ := newTestWorkerIntegration(t)
	// MODELED active original call; production begin/frame/finish/join own it.
	_, call, err := in.rt.http.begin(context.Background(), "worker/held-original", false, false)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	first := in.Close(ctx)
	if !errors.Is(first, context.Canceled) || !errors.Is(first, backend.ErrWorkerUnsettled) {
		t.Fatalf("Close claimed active original joined: %v", first)
	}
	if err := call.finish(nil); err != nil {
		t.Fatal(err)
	}
	if second := in.Close(context.Background()); second != first {
		t.Fatal("later completion changed first Close result")
	}
}

func workerIntegrationWait(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !ready() {
		select {
		case <-deadline.C:
			t.Fatal("original integration did not reach expected state")
		case <-tick.C:
		}
	}
}

// ExampleNewWorkerIntegration shows refusal before credentials or API access.
func ExampleNewWorkerIntegration() {
	in, err := NewWorkerIntegration(context.Background(), nil, nil, Config{}, WorkerConfig{})
	fmt.Println(in == nil, err != nil)
	// Output: true true
}

func integrationWorkerAuthority(t *testing.T) (*store.Store, *auth.Engine, string, store.RequestDomain, string) {
	t.Helper()
	dir := t.TempDir()
	state := filepath.Join(dir, "state")
	info, err := store.EnableRetention(state, store.DefaultRetentionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(state)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	file, tokenPath := filepath.Join(dir, "auth.json"), filepath.Join(dir, "admin.token")
	if _, err := auth.Initialize(file, tokenPath, s.Owner()); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	engine, err := auth.NewFile(file, s.Owner())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); engine.Run(ctx) }()
	t.Cleanup(func() { stop(); <-done })
	return s, engine, strings.TrimSpace(string(raw)), info.Domain, file
}

func TestWorkerIntegrationMountedSoftware(t *testing.T) {
	profileAPI := integrationWorkerAPI(t)
	workersAPI := &workerAPI{t: t, objs: map[string]runtime.Object{}, faults: map[string]workerFault{}, logs: map[string]string{}, logStatus: map[string]int{}}
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/pods") || strings.Contains(r.URL.Path, "/services/") || strings.HasSuffix(r.URL.Path, "/services") {
			workersAPI.serve(w, r)
		} else {
			profileAPI.serve(w, r)
		}
	}))
	defer api.Close()
	config := integrationWorkerTargets(t, profileAPI)
	config.targets[0].Kubeconfig = writeWorkerTLSKubeconfig(t, api)
	s, engine, token, domain, _ := integrationWorkerAuthority(t)
	got, err := NewWorkerIntegration(context.Background(), s, engine, config, integrationWorkerProfile())
	if err != nil {
		t.Fatal(err)
	}
	in := got.(*workerIntegration)
	defer closeIntegration(t, in)
	c := bindTestWorkerIntegration(t, in, s, engine)
	ctx, stop := context.WithCancel(context.Background())
	controlDone := make(chan error, 1)
	go func() { controlDone <- c.Run(ctx) }()
	defer func() {
		stop()
		if err := <-controlDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	workerIntegrationWait(t, c.StorageReady)
	server := httptest.NewServer(rpc.NewWorkerHandler(c, engine, in.WorkerController()))
	defer server.Close()
	controlClient := skyv1connect.NewControlServiceClient(server.Client(), server.URL)
	workers := skyv1connect.NewWorkerGroupServiceClient(server.Client(), server.URL)
	infoReq := connect.NewRequest(&v1.GetServerInfoRequest{})
	infoReq.Header().Set("Authorization", "Bearer "+token)
	info, err := controlClient.GetServerInfo(context.Background(), infoReq)
	if err != nil {
		t.Fatal(err)
	}
	for _, cap := range info.Msg.Capabilities {
		if cap == v1.Capability_CAPABILITY_INDEXED_WORKERS_V1 || cap == v1.Capability_CAPABILITY_TCP_RENDEZVOUS_V1 {
			t.Fatal("unqualified profile advertised worker support")
		}
	}
	submit := func(id string) (*v1.Job, error) {
		req := connect.NewRequest(&v1.SubmitWorkerJobRequest{Task: &v1.Task{Argv: []string{"train"}, Image: taskImage, Target: "kube", CpuMillis: 1000, MemoryBytes: 1 << 30}, RequestId: id,
			Domain: &v1.RequestDomain{Lineage: domain.Lineage, Epoch: domain.Epoch}, Workers: &v1.WorkerSpec{Count: 2, Mode: v1.WorkerMode_WORKER_MODE_INDEPENDENT}})
		req.Header().Set("Authorization", "Bearer "+token)
		res, err := workers.SubmitWorkerJob(context.Background(), req)
		if err != nil {
			return nil, err
		}
		return res.Msg.Job, nil
	}
	job, err := submit("one")
	if err != nil {
		t.Fatal(err)
	}
	replay, err := submit("one")
	if err != nil || replay.Id != job.Id {
		t.Fatalf("canonical replay: %v %v", replay, err)
	}
	second, err := submit("two")
	if err != nil {
		t.Fatal(err)
	}
	third, err := submit("three")
	if err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if err := in.workers.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	firstGroup, err := s.WorkerGroup(job.Id)
	if err != nil {
		t.Fatal(err)
	}
	secondGroup, err := s.WorkerGroup(second.Id)
	if err != nil {
		t.Fatal(err)
	}
	thirdGroup, err := s.WorkerGroup(third.Id)
	if err != nil {
		t.Fatal(err)
	}
	if !firstGroup.Reserved || !secondGroup.Reserved || thirdGroup.Reserved || thirdGroup.Phase != store.WorkerGroupQueued {
		t.Fatalf("whole-group budget or FIFO changed: first=%+v second=%+v third=%+v", firstGroup, secondGroup, thirdGroup)
	}
	workersAPI.mu.Lock()
	pods := 0
	for _, obj := range workersAPI.objs {
		if p, ok := obj.(*corev1.Pod); ok {
			pods++
			if p.Annotations[workerStartAnnotation] != "" {
				t.Fatal("token delivered before allPrepared")
			}
		}
	}
	workersAPI.mu.Unlock()
	if pods != 4 {
		t.Fatalf("native API model pods=%d want4 admitted ranks", pods)
	}
	ranksReq := connect.NewRequest(&v1.GetWorkerRanksRequest{Id: job.Id, AttemptId: job.Id})
	ranksReq.Header().Set("Authorization", "Bearer "+token)
	ranks, err := workers.GetWorkerRanks(context.Background(), ranksReq)
	if err != nil || len(ranks.Msg.Ranks) != 2 {
		t.Fatalf("mounted rank projection: %v %v", ranks, err)
	}
	// Native Prepared/Started are deliberately not fabricated here. The
	// first software composition stops at durable admission and gated Pods.
}

func TestWorkerIntegrationBlockedOriginalClose(t *testing.T) {
	in, _, _, _ := newTestWorkerIntegration(t)
	fault := &faultTransport{rt: in.rt.http.transport.base}
	in.rt.http.transport.base = fault
	cause := errors.New("blocked original close failed")
	body := &faultBody{closeErr: cause, entered: make(chan struct{}), release: make(chan struct{})}
	fault.arm(http.MethodGet, "/namespaces/workloads", 1, body)
	returned := make(chan error, 1)
	go func() { returned <- in.verifyBorrowed(context.Background()) }()
	select {
	case <-body.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("original Close did not start")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	first := in.Close(ctx)
	if !errors.Is(first, backend.ErrWorkerUnsettled) || !errors.Is(first, context.Canceled) {
		t.Fatalf("blocked original falsely acknowledged: %v", first)
	}
	close(body.release)
	select {
	case err := <-returned:
		if !errors.Is(err, cause) || !errors.Is(err, backend.ErrWorkerUnsettled) {
			t.Fatalf("original did not retain failure: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("released original did not return")
	}
	if second := in.Close(context.Background()); second != first || body.closes.Load() != 1 {
		t.Fatal("first failed Close was retried or original closed twice")
	}
}

// TestWorkerIntegrationOrdinaryRestart is separate from the worker-barrier
// proof. It retains the ordinary native Job path, including its absence of
// execution receipts; only the Job API and controller-owned Pod are MODELED.
func TestWorkerIntegrationOrdinaryRestart(t *testing.T) {
	profiles := integrationWorkerAPI(t)
	jobs, _, _ := newReceiptAPI(t)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/jobs") || strings.Contains(r.URL.Path, "/pods") {
			jobs.serve(w, r)
		} else {
			profiles.serve(w, r)
		}
	}))
	defer api.Close()
	config := integrationWorkerTargets(t, profiles)
	config.targets[0].Kubeconfig = writeKubeconfig(t, api.URL)
	dir := t.TempDir()
	state, authPath := filepath.Join(dir, "state"), filepath.Join(dir, "auth.json")
	retention, err := store.EnableRetention(state, store.DefaultRetentionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(state)
	if err != nil {
		t.Fatal(err)
	}
	tokenPath := filepath.Join(dir, "admin.token")
	if _, err := auth.Initialize(authPath, tokenPath, s.Owner()); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	tokenBytes, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	token := strings.TrimSpace(string(tokenBytes))
	var in *workerIntegration
	var c *control.Control
	var endpoint *httptest.Server
	var client skyv1connect.ControlServiceClient
	var workers skyv1connect.WorkerGroupServiceClient
	var stopControl, stopAuth context.CancelFunc
	var controlDone chan error
	var authDone chan struct{}
	closeEpoch := func() {
		if endpoint != nil {
			endpoint.Close()
			endpoint = nil
		}
		if stopControl != nil {
			// Drain the original ordinary Control results before cancellation;
			// Join alone would not integrate a late original result.
			joinCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := c.BeginShutdown(); err != nil {
				t.Error(err)
			}
			if err := c.WaitShutdownPrepared(joinCtx); err != nil {
				t.Error(err)
			}
			stopControl()
			if err := <-controlDone; err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
			if err := c.Join(joinCtx); err != nil {
				t.Error(err)
			}
			cancel()
			stopControl = nil
		}
		if in != nil {
			if err := in.Close(context.Background()); err != nil {
				t.Error(err)
			}
			in = nil
		}
		if stopAuth != nil {
			stopAuth()
			<-authDone
			stopAuth = nil
		}
	}
	t.Cleanup(func() {
		closeEpoch()
		if s != nil {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	openEpoch := func() {
		engine, err := auth.NewFile(authPath, s.Owner())
		if err != nil {
			t.Fatal(err)
		}
		authCtx, cancel := context.WithCancel(context.Background())
		stopAuth, authDone = cancel, make(chan struct{})
		go func() { defer close(authDone); engine.Run(authCtx) }()
		got, err := NewWorkerIntegration(context.Background(), s, engine, config, integrationWorkerProfile())
		if err != nil {
			t.Fatal(err)
		}
		in = got.(*workerIntegration)
		c = bindTestWorkerIntegration(t, in, s, engine)
		runCtx, stop := context.WithCancel(context.Background())
		stopControl, controlDone = stop, make(chan error, 1)
		go func() { controlDone <- c.Run(runCtx) }()
		workerIntegrationWait(t, c.StorageReady)
		endpoint = httptest.NewServer(rpc.NewWorkerHandler(c, engine, in.WorkerController()))
		client = skyv1connect.NewControlServiceClient(endpoint.Client(), endpoint.URL)
		workers = skyv1connect.NewWorkerGroupServiceClient(endpoint.Client(), endpoint.URL)
	}
	openEpoch()
	request := func(m *v1.GetJobRequest) *connect.Request[v1.GetJobRequest] {
		r := connect.NewRequest(m)
		r.Header().Set("Authorization", "Bearer "+token)
		return r
	}
	submit := connect.NewRequest(&v1.SubmitRetainedJobRequest{Task: &v1.Task{Argv: []string{"train"}, Image: taskImage, Target: "kube", CpuMillis: 250, MemoryBytes: 1 << 20}, RequestId: "ordinary",
		Domain: &v1.RequestDomain{Lineage: retention.Domain.Lineage, Epoch: retention.Domain.Epoch}})
	submit.Header().Set("Authorization", "Bearer "+token)
	first, err := client.SubmitRetainedJob(context.Background(), submit)
	if err != nil {
		t.Fatal(err)
	}
	if first.Msg.Job.Workers != nil {
		t.Fatal("ordinary Control RPC created a worker group")
	}
	id := first.Msg.Job.Id
	name, err := jobName(id)
	if err != nil {
		t.Fatal(err)
	}
	workerIntegrationWait(t, func() bool { return jobs.job(name) != nil })
	original := jobs.job(name)
	jobs.addJobPod(original, "ordinary-pod", corev1.PodRunning)
	workerIntegrationWait(t, func() bool {
		got, err := client.GetJob(context.Background(), request(&v1.GetJobRequest{Id: id}))
		return err == nil && got.Msg.Job.State == v1.JobState_JOB_STATE_RUNNING && got.Msg.Job.ExitCode == nil
	})
	logReq := connect.NewRequest(&v1.StreamJobLogsRequest{Id: id})
	logReq.Header().Set("Authorization", "Bearer "+token)
	stream, err := client.StreamJobLogs(context.Background(), logReq)
	if err != nil {
		t.Fatal(err)
	}
	var logs []byte
	for stream.Receive() {
		logs = append(logs, stream.Msg().Data...)
	}
	if stream.Err() != nil || string(logs) != "output\n" {
		t.Fatalf("ordinary logs %q %v", logs, stream.Err())
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	accepted, err := s.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	binding, owner := accepted.TargetBinding, s.Owner()
	if _, err := in.rt.b.Observe(context.Background(), backend.Reference{ID: id, Task: accepted.Task, Binding: binding, Instance: "invented"}); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("ordinary execution invented an instance identity: %v", err)
	}
	if binding != in.rt.b.binding || len(accepted.Attempts) != 0 || in.rt.b.receipts != nil {
		t.Fatal("ordinary job gained fake durable execution identity")
	}
	closeEpoch()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = nil
	s, err = store.Open(state)
	if err != nil {
		t.Fatal(err)
	}
	if s.Owner() != owner {
		t.Fatal("actual Store reopen changed owner")
	}
	openEpoch()
	if in.rt.b.binding != binding || in.rt.b.receipts != nil {
		t.Fatal("restart changed binding or invented execution receipts")
	}
	got, err := client.GetJob(context.Background(), request(&v1.GetJobRequest{Id: id}))
	if err != nil || got.Msg.Job.Id != id || got.Msg.Job.Workers != nil || got.Msg.Job.ExitCode != nil {
		t.Fatalf("actual reopen lost ordinary identity: %v %v", got, err)
	}
	// Lose the native object independently: absence is Unknown, not exit0.
	jobs.mu.Lock()
	delete(jobs.jobs, name)
	jobs.mu.Unlock()
	workerIntegrationWait(t, func() bool {
		got, err := client.GetJob(context.Background(), request(&v1.GetJobRequest{Id: id}))
		return err == nil && got.Msg.Job.State == v1.JobState_JOB_STATE_UNKNOWN && got.Msg.Job.ExitCode == nil
	})
	// Count1 independent uses the exact ordinary path, with nil wire spec.
	oneReq := connect.NewRequest(&v1.SubmitWorkerJobRequest{Task: &v1.Task{Argv: []string{"train"}, Image: taskImage, Target: "kube", CpuMillis: 250, MemoryBytes: 1 << 20}, RequestId: "count1",
		Domain: &v1.RequestDomain{Lineage: retention.Domain.Lineage, Epoch: retention.Domain.Epoch}, Workers: &v1.WorkerSpec{Count: 1, Mode: v1.WorkerMode_WORKER_MODE_INDEPENDENT}})
	oneReq.Header().Set("Authorization", "Bearer "+token)
	one, err := workers.SubmitWorkerJob(context.Background(), oneReq)
	if err != nil || one.Msg.Job.Workers != nil {
		t.Fatalf("count1 did not normalize to ordinary: %v %v", one, err)
	}
	oneName, err := jobName(one.Msg.Job.Id)
	if err != nil {
		t.Fatal(err)
	}
	workerIntegrationWait(t, func() bool { return jobs.job(oneName) != nil })
	cancelReq := connect.NewRequest(&v1.CancelJobRequest{Id: one.Msg.Job.Id})
	cancelReq.Header().Set("Authorization", "Bearer "+token)
	if _, err := client.CancelJob(context.Background(), cancelReq); err != nil {
		t.Fatal(err)
	}
	workerIntegrationWait(t, func() bool {
		got, err := client.GetJob(context.Background(), request(&v1.GetJobRequest{Id: one.Msg.Job.Id}))
		return err == nil && got.Msg.Job.State == v1.JobState_JOB_STATE_CANCELED && got.Msg.Job.ExitCode == nil
	})
}

// closeWorkerStoreAfterResponse introduces a real Store write failure only
// after the original typed HTTP response is available. No receipt is supplied
// by this transport: the production decoder/producer still observes it.
type closeWorkerStoreAfterResponse struct {
	base  http.RoundTripper
	store *store.Store
	once  sync.Once
}

func (tr *closeWorkerStoreAfterResponse) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := tr.base.RoundTrip(r)
	if err == nil && r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/pods/") {
		tr.once.Do(func() { err = tr.store.Close() })
	}
	return resp, err
}

func TestWorkerIntegrationPendingObservationHolds(t *testing.T) {
	profiles := integrationWorkerAPI(t)
	model := &workerAPI{t: t, objs: map[string]runtime.Object{}, faults: map[string]workerFault{}, logs: map[string]string{}, logStatus: map[string]int{}}
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/pods") {
			model.serve(w, r)
		} else {
			profiles.serve(w, r)
		}
	}))
	defer api.Close()
	config := integrationWorkerTargets(t, profiles)
	config.targets[0].Kubeconfig = writeWorkerTLSKubeconfig(t, api)
	s, engine, token, domain, _ := integrationWorkerAuthority(t)
	got, err := NewWorkerIntegration(context.Background(), s, engine, config, integrationWorkerProfile())
	if err != nil {
		t.Fatal(err)
	}
	in := got.(*workerIntegration)
	c := bindTestWorkerIntegration(t, in, s, engine)
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	defer func() {
		stop()
		// Store failure is deliberate. Whatever Run returns, positively
		// join its original workers before fixture cleanup.
		<-done
		join, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := c.Join(join); err != nil {
			t.Error(err)
		}
	}()
	workerIntegrationWait(t, c.StorageReady)
	actor, err := engine.Authenticate("Bearer " + token)
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := in.workers.Submit(actor, domain, "pending-observation", backend.Task{Argv: []string{"train"}, Image: taskImage, Target: "kube", CPUMillis: 1000, MemoryBytes: 1 << 30}, backend.WorkerSpec{Count: 2, Mode: backend.WorkerIndependent})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := in.workers.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	model.mu.Lock()
	pods := 0
	for _, obj := range model.objs {
		if pod, ok := obj.(*corev1.Pod); ok {
			workerExited(7)(pod)
			pods++
		}
	}
	model.mu.Unlock()
	if pods != 2 {
		t.Fatalf("MODELED positive stop needs both original Pods, got%d", pods)
	}
	in.rt.http.transport.base = &closeWorkerStoreAfterResponse{base: in.rt.http.transport.base, store: s}
	if err := in.workers.Step(context.Background()); err == nil {
		t.Fatal("closed Store accepted original observation")
	}
	pending := in.workers.PendingResults()
	if pending == nil || !errors.Is(pending, bolt.ErrDatabaseNotOpen) {
		t.Fatalf("actual original observation/cause lost: %v", pending)
	}
	if errors.Is(pending, backend.ErrWorkerUnsettled) {
		t.Fatal("settled native observation invented an unsettled backend marker")
	}
	if len(in.rt.http.frames("")) != 0 {
		t.Fatal("observation fault was not a clean HTTP/failed Store crossing")
	}
	first := in.Close(context.Background())
	var retained *workerIntegrationError
	if first == nil || !errors.As(first, &retained) || retained.in != in || !errors.Is(first, bolt.ErrDatabaseNotOpen) {
		t.Fatalf("Close dropped local Store-pending original for%s: %v", job.ID, first)
	}
	if in.Close(context.Background()) != first {
		t.Fatal("Close retried/discarded original pending observation")
	}
}

func integrationWorkerModelConfig(t *testing.T, model *workerAPI) Config {
	t.Helper()
	profiles := integrationWorkerAPI(t)
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/pods") || strings.Contains(r.URL.Path, "/services") {
			model.serve(w, r)
		} else {
			profiles.serve(w, r)
		}
	}))
	t.Cleanup(api.Close)
	config := integrationWorkerTargets(t, profiles)
	config.targets[0].Kubeconfig = writeWorkerTLSKubeconfig(t, api)
	return config
}

func integrationWorkerStartControl(t *testing.T, c *control.Control) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	t.Cleanup(func() {
		join, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := c.BeginShutdown(); err != nil {
			t.Error(err)
		}
		if err := c.WaitShutdownPrepared(join); err != nil {
			t.Error(err)
		}
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
		if err := c.Join(join); err != nil {
			t.Error(err)
		}
	})
	workerIntegrationWait(t, c.StorageReady)
}

func TestWorkerIntegrationUnavailableAuthCloses(t *testing.T) {
	a := integrationWorkerAPI(t)
	s, engine, token, domain, authPath := integrationWorkerAuthority(t)
	got, err := NewWorkerIntegration(context.Background(), s, engine, integrationWorkerTargets(t, a), integrationWorkerProfile())
	if err != nil {
		t.Fatal(err)
	}
	in := got.(*workerIntegration)
	c := bindTestWorkerIntegration(t, in, s, engine)
	integrationWorkerStartControl(t, c)
	actor, err := engine.Authenticate("Bearer " + token)
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := in.workers.Submit(actor, domain, "stale-auth", backend.Task{Argv: []string{"train"}, Image: taskImage, Target: "kube", CPUMillis: 1000, MemoryBytes: 1 << 30}, backend.WorkerSpec{Count: 2, Mode: backend.WorkerIndependent})
	if err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(authPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.WriteFile(authPath, original, 0600); err != nil {
			t.Error(err)
		}
	})
	if err := os.WriteFile(authPath, []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	// Wait for the real refresh loop's bounded stale snapshot, not a fake
	// time hook or synthesized delegation refusal.
	workerIntegrationWait(t, func() bool { _, err := engine.Check(actor, auth.Mutate); return errors.Is(err, auth.ErrUnavailable) })
	if err := in.workers.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	g, err := s.WorkerGroup(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if g.Reserved || g.Revoked || g.Phase != store.WorkerGroupQueued {
		t.Fatalf("auth unavailability became native ownership/revocation: %+v", g)
	}
	if err := in.workers.PendingResults(); err != nil {
		t.Fatalf("unavailable authority fabricated local original: %v", err)
	}
	if err := in.Close(context.Background()); err != nil {
		t.Fatalf("unavailable authority forced false Close HOLD: %v", err)
	}
	if len(a.writes()) != 0 {
		t.Fatal("unavailable authority dispatched native work")
	}
}

func TestWorkerIntegrationDurableUnknownCloses(t *testing.T) {
	model := &workerAPI{t: t, objs: map[string]runtime.Object{}, faults: map[string]workerFault{"pods": {apply: false, status: http.StatusServiceUnavailable}}, logs: map[string]string{}, logStatus: map[string]int{}}
	s, engine, token, domain, _ := integrationWorkerAuthority(t)
	got, err := NewWorkerIntegration(context.Background(), s, engine, integrationWorkerModelConfig(t, model), integrationWorkerProfile())
	if err != nil {
		t.Fatal(err)
	}
	in := got.(*workerIntegration)
	c := bindTestWorkerIntegration(t, in, s, engine)
	integrationWorkerStartControl(t, c)
	actor, err := engine.Authenticate("Bearer " + token)
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := in.workers.Submit(actor, domain, "durable-unknown", backend.Task{Argv: []string{"train"}, Image: taskImage, Target: "kube", CPUMillis: 1000, MemoryBytes: 1 << 30}, backend.WorkerSpec{Count: 2, Mode: backend.WorkerIndependent})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := in.workers.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	g, err := s.WorkerGroup(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !g.Reserved || g.Phase != store.WorkerGroupHeld {
		t.Fatalf("ambiguous original did not remain durably charged/Held: %+v", g)
	}
	if err := in.workers.PendingResults(); err != nil {
		t.Fatalf("durably recorded native ambiguity fabricated local custody: %v", err)
	}
	if len(in.rt.http.frames("")) != 0 {
		t.Fatal("ambiguous response did not positively settle its HTTP body")
	}
	if err := in.Close(context.Background()); err != nil {
		t.Fatalf("durable unknown forced local Close HOLD: %v", err)
	}
	current, err := s.WorkerGroup(job.ID)
	if err != nil || !current.Reserved || current.Phase != store.WorkerGroupHeld {
		t.Fatalf("clean local Close fabricated native cleanup/refund: %+v %v", current, err)
	}
}

// writeWorkerTLSKubeconfig embeds the fixture's actual CA and static token.
// client-go deliberately omits user credentials for cleartext API endpoints.
func writeWorkerTLSKubeconfig(t *testing.T, api *httptest.Server) string {
	t.Helper()
	certificate := api.Certificate()
	if certificate == nil {
		t.Fatal("worker credential fixture requires TLS")
	}
	file := writeKubeconfig(t, api.URL)
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw})
	doc["clusters"].([]any)[0].(map[string]any)["cluster"].(map[string]any)["certificate-authority-data"] = base64.StdEncoding.EncodeToString(ca)
	doc["users"].([]any)[0].(map[string]any)["user"].(map[string]any)["token"] = workerHTTPToken
	raw, err = json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return file
}
