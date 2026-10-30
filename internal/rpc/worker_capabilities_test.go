package rpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
	"github.com/tmc/skygo/internal/workeradmission"
)

// These are software tests: the Store, authentication, Control and mounted
// Connect service are real. mountedWorkerBackend models native resources;
// no worker Step or task execution is started by this fixture.
type workerCapabilityFixture struct {
	store    *store.Store
	engine   *auth.Engine
	authPath string
	control  *control.Control
	workers  *control.Workers
	server   *httptest.Server
	backends map[string]*mountedWorkerBackend
	tokens   map[string]string
	domain   *v1.RequestDomain

	directory                        string // manually owned; never removed while an original is held
	stopAuth                         context.CancelFunc
	authDone                         chan struct{}
	stopRun                          context.CancelFunc
	runDone                          chan struct{}
	runErr                           error // published before runDone closes; never consumed from a channel
	held                             error
	joined                           bool
	authJoined, storeClosed, removed bool
}

func workerCapabilityBudgets() map[string]workeradmission.Budget {
	return map[string]workeradmission.Budget{
		"a": {Ranks: 4, CPUMillis: 4000, MemoryBytes: 4 << 30},
		"b": {Ranks: 4, CPUMillis: 4000, MemoryBytes: 4 << 30},
	}
}

func newWorkerCapabilityFixture(t *testing.T, modes map[string][]backend.WorkerMode, legacy bool) *workerCapabilityFixture {
	t.Helper()
	dir := workerCapabilityDirectory(t)
	f := &workerCapabilityFixture{directory: dir, tokens: make(map[string]string), backends: make(map[string]*mountedWorkerBackend)}
	// One cleanup owns the entire lifetime, including partially built fixtures.
	t.Cleanup(func() { f.cleanup(t) })
	state := filepath.Join(dir, "state")
	info, err := store.EnableRetention(state, store.DefaultRetentionPolicy())
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	s, err := store.Open(state)
	f.store = s
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	f.domain = &v1.RequestDomain{Lineage: info.Domain.Lineage, Epoch: info.Domain.Epoch}
	config := filepath.Join(dir, "auth.json")
	f.authPath = config
	if _, err := auth.Initialize(config, filepath.Join(dir, "admin.token"), s.Owner()); err != nil {
		f.held = err
		t.Fatal(err)
	}
	for _, principal := range []struct {
		name string
		role auth.Role
	}{{"alice", auth.Operator}, {"reader", auth.Reader}} {
		if err := auth.AddPrincipal(config, principal.name, principal.role); err != nil {
			f.held = err
			t.Fatal(err)
		}
		path := filepath.Join(dir, principal.name+".token")
		if _, err := auth.Issue(config, principal.name, path, time.Hour); err != nil {
			f.held = err
			t.Fatal(err)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			f.held = err
			t.Fatal(err)
		}
		f.tokens[principal.name] = strings.TrimSpace(string(raw))
	}
	if err := s.ActivateScoped(); err != nil {
		f.held = err
		t.Fatal(err)
	}
	if legacy {
		f.tokens["legacy"] = "MODELED legacy capability credential"
		f.engine = auth.NewLegacy(f.tokens["legacy"])
	} else {
		oidcPath, token := workerCapabilityOIDC(t, dir, s.Owner())
		f.tokens["oidc"] = token
		f.engine, err = auth.NewFileOIDC(config, s.Owner(), oidcPath, "")
		if err != nil {
			f.held = err
			t.Fatal(err)
		}
	}
	ctx, stop := context.WithCancel(context.Background())
	f.stopAuth, f.authDone = stop, make(chan struct{})
	go func() { defer close(f.authDone); f.engine.Run(ctx) }()
	if !legacy {
		deadline := time.NewTimer(10 * time.Second)
		defer deadline.Stop()
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			actor, err := f.engine.Authenticate("Bearer " + f.tokens["oidc"])
			if err == nil {
				if actor.TokenID() != "" || actor.Role() != auth.Reader {
					t.Fatal("OIDC fixture did not mint a non-file Reader")
				}
				break
			}
			if !errors.Is(err, auth.ErrUnavailable) {
				t.Fatalf("authenticate actual signed OIDC token: %v", err)
			}
			select {
			case <-deadline.C:
				t.Fatal("actual OIDC file refresh did not publish")
			case <-tick.C:
			}
		}
	}
	var targets []backend.Target
	for _, name := range []string{"a", "b", "ordinary"} {
		b := &mountedWorkerBackend{read: mountedRankLog}
		f.backends[name] = b
		targets = append(targets, backend.Target{Name: name, Kind: "MODELED", Binding: name + "-binding", Backend: b, SupportsResourceLimits: true, OutputStorage: backend.RemoteOutput})
	}
	f.control, err = control.New(s, targets, f.engine)
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	if f.control.ArtifactsEnabled() {
		t.Fatal("CPU capability fixture accidentally installed artifacts")
	}
	runCtx, stopRun := context.WithCancel(context.Background())
	f.stopRun, f.runDone = stopRun, make(chan struct{})
	go func() {
		f.runErr = f.control.Run(runCtx)
		close(f.runDone)
	}()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for !f.control.StorageReady() {
		select {
		case <-f.runDone:
			t.Fatalf("Control returned before storage sampler: %v", f.runErr)
		case <-deadline.C:
			t.Fatal("actual storage sampler did not become ready")
		case <-tick.C:
		}
	}
	f.workers, err = control.NewWorkers(f.control, control.WorkerConfig{Budgets: workerCapabilityBudgets(), Modes: modes})
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	f.server = httptest.NewServer(NewWorkerHandler(f.control, f.engine, f.workers))
	return f
}

// The concrete fixture owns this private persistent child. testing.TempDir
// must not delete a namespace whose Control or first Close remains held.
func workerCapabilityDirectory(t *testing.T) string {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(home, "tmp", "skypilot", time.Now().Format("20060102")+"-worker-capability-fixtures")
	if err := os.MkdirAll(base, 0700); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(base)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		t.Fatal("worker capability fixture parent must be a private real directory")
	}
	dir, err := os.MkdirTemp(base, "rpc-")
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func workerCapabilityWait(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	default:
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *workerCapabilityFixture) cleanup(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if f.held == nil && f.runDone != nil {
		f.held = f.control.BeginShutdown()
	}
	if f.held == nil && f.server != nil {
		if err := f.server.Config.Shutdown(ctx); err != nil {
			f.held = err
		} else {
			// Shutdown and Close join this actual server, including its callers.
			f.server.Close()
		}
	}
	if f.held == nil && f.runDone != nil {
		// Preparation still needs the original Run. Cancel it only after the
		// receipt, then separately join all target/maintenance originals.
		f.held = f.control.WaitShutdownPrepared(ctx)
		f.stopRun()
		if err := workerCapabilityWait(ctx, f.runDone); err != nil {
			f.held = errors.Join(f.held, err)
		} else {
			if f.runErr != nil && !errors.Is(f.runErr, context.Canceled) {
				f.held = errors.Join(f.held, f.runErr)
			}
			if err := f.control.Join(ctx); err != nil {
				f.held = errors.Join(f.held, err)
			} else {
				f.joined = true
			}
		}
	}
	if f.held == nil {
		f.held = f.engine.Err()
	}
	if f.held == nil && f.stopAuth != nil {
		f.stopAuth()
		if err := workerCapabilityWait(ctx, f.authDone); err != nil {
			f.held = err
		} else {
			f.authJoined = true
			// An original reader can fail its first Close while Run stops.
			f.held = f.engine.Err()
		}
	}
	if f.held == nil && f.store != nil {
		if err := f.store.Close(); err != nil {
			f.held = err
		} else {
			f.storeClosed = true
		}
	}
	if f.held == nil {
		if err := os.RemoveAll(f.directory); err != nil {
			f.held = err
		} else {
			f.removed = true
		}
	}
	if f.held != nil {
		t.Errorf("retaining actual worker capability fixture at %s: %v", f.directory, f.held)
		// Same actual owners/cause/path stay reachable for the process lifetime.
		// No retry of a first failed Close, replacement owner or global registry.
		lifetime := make(chan struct{})
		go func(original *workerCapabilityFixture) { <-lifetime; runtime.KeepAlive(original) }(f)
	}
}

func TestWorkerCapabilitiesFixtureCleanup(t *testing.T) {
	var f *workerCapabilityFixture
	// Registered before the fixture's cleanup, so this observes its real joins.
	t.Cleanup(func() {
		if f == nil || f.held != nil {
			return
		}
		if !f.joined || !f.authJoined || !f.storeClosed || !f.removed {
			t.Fatal("fixture cleanup omitted an original join or closed too little")
		}
		if _, err := os.Stat(f.directory); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("positively settled fixture namespace remains: %v", err)
		}
	})
	f = newWorkerCapabilityFixture(t, nil, false)
}

// workerCapabilityOIDC creates a real, locally signed ID token and local JWKS.
// It configures no issuer discovery or external network request.
func workerCapabilityOIDC(t *testing.T, dir, owner string) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	encode := base64.RawURLEncoding.EncodeToString
	write := func(name string, value any) string {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	write("worker-keys.json", map[string]any{"keys": []any{map[string]any{
		"kty": "EC", "crv": "P-256", "kid": "worker-cap", "alg": "ES256", "use": "sig",
		"x": encode(key.X.FillBytes(make([]byte, 32))), "y": encode(key.Y.FillBytes(make([]byte, 32))),
	}}})
	path := write("worker-oidc.json", map[string]any{
		"version": 1, "installation": owner, "generation": 1,
		"issuer": "https://issuer.example.invalid/worker", "audience": "skygo", "jwks_file": "worker-keys.json",
		"identities": []any{map[string]string{"subject": "worker-reader", "principal": "reader"}},
	})
	header, err := json.Marshal(map[string]string{"alg": "ES256", "kid": "worker-cap", "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	claims, err := json.Marshal(map[string]any{"iss": "https://issuer.example.invalid/worker", "aud": "skygo", "sub": "worker-reader", "iat": now, "nbf": now, "exp": now + 120})
	if err != nil {
		t.Fatal(err)
	}
	input := encode(header) + "." + encode(claims)
	digest := sha256.Sum256([]byte(input))
	r, s, err := ecdsa.Sign(rand.Reader, key, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	signature := append(r.FillBytes(make([]byte, 32)), s.FillBytes(make([]byte, 32))...)
	return path, input + "." + encode(signature)
}

func capabilityRequest[T any](f *workerCapabilityFixture, principal string, msg *T) *connect.Request[T] {
	r := connect.NewRequest(msg)
	r.Header().Set("Authorization", "Bearer "+f.tokens[principal])
	return r
}

func workerCapabilityValues(values []v1.Capability) []v1.Capability {
	var out []v1.Capability
	for _, value := range values {
		if value == v1.Capability_CAPABILITY_INDEXED_WORKERS_V1 || value == v1.Capability_CAPABILITY_TCP_RENDEZVOUS_V1 {
			out = append(out, value)
		}
	}
	return out
}

func checkWorkerCapabilityInfo(t *testing.T, client skyv1connect.ControlServiceClient, f *workerCapabilityFixture, principal string, endpoint []v1.Capability, targets map[string][]v1.Capability) {
	t.Helper()
	info, err := client.GetServerInfo(context.Background(), capabilityRequest(f, principal, &v1.GetServerInfoRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if got := workerCapabilityValues(info.Msg.Capabilities); !reflect.DeepEqual(got, endpoint) {
		t.Fatalf("endpoint worker capabilities = %v, want %v", got, endpoint)
	}
	if len(info.Msg.Targets) != 3 {
		t.Fatalf("target count = %d, want3", len(info.Msg.Targets))
	}
	for _, target := range info.Msg.Targets {
		if got := workerCapabilityValues(target.Capabilities); !reflect.DeepEqual(got, targets[target.Name]) {
			t.Fatalf("target %q worker capabilities = %v, want %v", target.Name, got, targets[target.Name])
		}
	}
	listed, err := client.ListTargets(context.Background(), capabilityRequest(f, principal, &v1.ListTargetsRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(listed.Msg.Targets, info.Msg.Targets) {
		t.Fatal("GetServerInfo and ListTargets disagree on exact target projection")
	}
	for name, b := range f.backends {
		if b.checks.Load() != 0 {
			t.Fatalf("discovery called backend %s", name)
		}
	}
}

func TestWorkerCapabilitiesProjection(t *testing.T) {
	ind := v1.Capability_CAPABILITY_INDEXED_WORKERS_V1
	tcp := v1.Capability_CAPABILITY_TCP_RENDEZVOUS_V1
	for _, tt := range []struct {
		name     string
		modes    map[string][]backend.WorkerMode
		endpoint []v1.Capability
		targets  map[string][]v1.Capability
	}{
		{"nil legacy", nil, nil, nil},
		{"empty denial", map[string][]backend.WorkerMode{}, nil, nil},
		{"target denial", map[string][]backend.WorkerMode{"a": {}, "b": nil}, nil, nil},
		{"independent only", map[string][]backend.WorkerMode{"a": {backend.WorkerIndependent}}, []v1.Capability{ind}, map[string][]v1.Capability{"a": {ind}}},
		{"mixed", map[string][]backend.WorkerMode{"a": {backend.WorkerIndependent}, "b": {backend.WorkerIndependent, backend.WorkerTCP}}, []v1.Capability{ind, tcp}, map[string][]v1.Capability{"a": {ind}, "b": {ind, tcp}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newWorkerCapabilityFixture(t, tt.modes, false)
			c := skyv1connect.NewControlServiceClient(f.server.Client(), f.server.URL)
			checkWorkerCapabilityInfo(t, c, f, "reader", tt.endpoint, tt.targets)
			_, err := skyv1connect.NewWorkerGroupServiceClient(f.server.Client(), f.server.URL).SubmitWorkerJob(context.Background(), capabilityRequest(f, "reader", workerCapabilitySubmit("reader-denied", "a", v1.WorkerMode_WORKER_MODE_INDEPENDENT)))
			if connect.CodeOf(err) != connect.CodePermissionDenied {
				t.Fatalf("Reader mutated through advertised support: %v", err)
			}
			groups, err := f.store.WorkerGroups(0, 16)
			if err != nil || len(groups) != 0 {
				t.Fatalf("Reader created worker rows: %v %v", groups, err)
			}
		})
	}
}

func TestWorkerCapabilitiesDetachment(t *testing.T) {
	modes := map[string][]backend.WorkerMode{"a": {backend.WorkerIndependent}, "b": {backend.WorkerIndependent, backend.WorkerTCP}}
	f := newWorkerCapabilityFixture(t, modes, false)
	modes["a"][0] = backend.WorkerTCP
	delete(modes, "b")
	returned := f.workers.Modes("a")
	returned[0] = backend.WorkerTCP
	ind := v1.Capability_CAPABILITY_INDEXED_WORKERS_V1
	tcp := v1.Capability_CAPABILITY_TCP_RENDEZVOUS_V1
	checkWorkerCapabilityInfo(t, skyv1connect.NewControlServiceClient(f.server.Client(), f.server.URL), f, "reader", []v1.Capability{ind, tcp}, map[string][]v1.Capability{"a": {ind}, "b": {ind, tcp}})
}

func TestWorkerCapabilitiesOrdinaryHandler(t *testing.T) {
	f := newWorkerCapabilityFixture(t, map[string][]backend.WorkerMode{"a": {backend.WorkerIndependent, backend.WorkerTCP}}, false)
	for _, tt := range []struct {
		name string
		h    *HTTPHandler
	}{
		{"ordinary", NewHandler(f.control, f.engine)},
		{"nil environment", NewEnvironmentHandler(f.control, f.engine, nil)},
		{"nil integrated", NewIntegratedHandler(f.control, f.engine, nil, nil)},
		{"nil workers", NewWorkerHandler(f.control, f.engine, nil)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := httptest.NewServer(tt.h)
			defer s.Close()
			checkWorkerCapabilityInfo(t, skyv1connect.NewControlServiceClient(s.Client(), s.URL), f, "reader", nil, nil)
			_, err := skyv1connect.NewWorkerGroupServiceClient(s.Client(), s.URL).GetWorkerRanks(context.Background(), capabilityRequest(f, "reader", &v1.GetWorkerRanksRequest{Id: strings.Repeat("a", 32), AttemptId: strings.Repeat("a", 32)}))
			if connect.CodeOf(err) != connect.CodeUnimplemented {
				t.Fatalf("ordinary handler unexpectedly mounts worker RPC: %v", err)
			}
		})
	}
}

func TestWorkerCapabilitiesNonFileAuthority(t *testing.T) {
	modes := map[string][]backend.WorkerMode{"a": {backend.WorkerIndependent, backend.WorkerTCP}}
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "OIDC", true: "legacy"}[legacy], func(t *testing.T) {
			f := newWorkerCapabilityFixture(t, modes, legacy)
			principal := "oidc"
			if legacy {
				principal = "legacy"
			}
			checkWorkerCapabilityInfo(t, skyv1connect.NewControlServiceClient(f.server.Client(), f.server.URL), f, principal, nil, nil)
		})
	}
}

func workerCapabilitySubmit(id, target string, mode v1.WorkerMode) *v1.SubmitWorkerJobRequest {
	return &v1.SubmitWorkerJobRequest{RequestId: id,
		Task:    &v1.Task{Argv: []string{"train"}, Image: "trainer:v1", Target: target, CpuMillis: 1000, MemoryBytes: 1 << 30},
		Workers: &v1.WorkerSpec{Count: 2, Mode: mode}}
}

func TestWorkerCapabilitiesMountedSelectionAndReplay(t *testing.T) {
	f := newWorkerCapabilityFixture(t, map[string][]backend.WorkerMode{"a": {backend.WorkerIndependent}, "b": {backend.WorkerIndependent, backend.WorkerTCP}}, false)
	client := skyv1connect.NewWorkerGroupServiceClient(f.server.Client(), f.server.URL)
	submit := func(id, target string, mode v1.WorkerMode) (*v1.SubmitWorkerJobResponse, error) {
		msg := workerCapabilitySubmit(id, target, mode)
		msg.Domain = f.domain
		out, err := client.SubmitWorkerJob(context.Background(), capabilityRequest(f, "alice", msg))
		if err != nil {
			return nil, err
		}
		return out.Msg, nil
	}
	accepted, err := submit("tcp-on-second", "", v1.WorkerMode_WORKER_MODE_TCP_RENDEZVOUS)
	if err != nil || accepted == nil || accepted.Job == nil || accepted.Job.Workers == nil || accepted.Job.Target != "b" || accepted.Job.Workers.Mode != v1.WorkerMode_WORKER_MODE_TCP_RENDEZVOUS {
		t.Fatalf("mounted mode-aware placement: %v %v", accepted, err)
	}
	if f.backends["a"].checks.Load() != 0 || f.backends["b"].checks.Load() != 1 || f.backends["ordinary"].checks.Load() != 0 {
		t.Fatal("mode filter called an ineligible backend")
	}
	before, err := f.store.WorkerGroups(0, 16)
	if err != nil {
		t.Fatal(err)
	}
	checks := f.backends["b"].checks.Load()
	if _, err := submit("wrong-target", "a", v1.WorkerMode_WORKER_MODE_TCP_RENDEZVOUS); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("TCP accepted on independent-only target: %v", err)
	}
	after, err := f.store.WorkerGroups(0, 16)
	if err != nil || !reflect.DeepEqual(before, after) || f.backends["a"].checks.Load() != 0 || f.backends["b"].checks.Load() != checks {
		t.Fatal("refused placement changed group rows/sequence or checked a backend")
	}
	// Replace only an idle Workers after joining the original HTTP server.
	// There is no worker Step loop, original effect or read to transfer.
	f.server.Close()
	f.workers, err = control.NewWorkers(f.control, control.WorkerConfig{Budgets: workerCapabilityBudgets(), Modes: map[string][]backend.WorkerMode{}})
	if err != nil {
		t.Fatal(err)
	}
	f.server = httptest.NewServer(NewWorkerHandler(f.control, f.engine, f.workers))
	client = skyv1connect.NewWorkerGroupServiceClient(f.server.Client(), f.server.URL)
	replayed, err := submit("tcp-on-second", "", v1.WorkerMode_WORKER_MODE_TCP_RENDEZVOUS)
	if err != nil || replayed == nil || replayed.Job == nil || replayed.Job.Workers == nil || replayed.Job.Id != accepted.Job.Id || replayed.Job.Target != "b" || replayed.Job.Workers.Mode != v1.WorkerMode_WORKER_MODE_TCP_RENDEZVOUS {
		t.Fatalf("current denial lost accepted canonical replay: %v %v", replayed, err)
	}
	changed := workerCapabilitySubmit("tcp-on-second", "", v1.WorkerMode_WORKER_MODE_TCP_RENDEZVOUS)
	changed.Domain, changed.Workers.Count = f.domain, 3
	if _, err := client.SubmitWorkerJob(context.Background(), capabilityRequest(f, "alice", changed)); connect.CodeOf(err) != connect.CodeAlreadyExists {
		t.Fatalf("current denial masked canonical conflict: %v", err)
	}
	if _, err := submit("fresh-denied", "b", v1.WorkerMode_WORKER_MODE_TCP_RENDEZVOUS); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("explicit empty table fell back to legacy: %v", err)
	}
	after, err = f.store.WorkerGroups(0, 16)
	if err != nil || !reflect.DeepEqual(before, after) || f.backends["a"].checks.Load() != 0 || f.backends["b"].checks.Load() != checks {
		t.Fatal("replay or fresh denied request changed original group rows or replanned")
	}
}

// A real failed file refresh and the existing freshness bound, rather than a
// mocked Actor or clock, prevent stale discovery from publishing support.
func TestWorkerCapabilitiesUnavailable(t *testing.T) {
	f := newWorkerCapabilityFixture(t, map[string][]backend.WorkerMode{"a": {backend.WorkerIndependent}}, false)
	if err := os.WriteFile(f.authPath, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		_, err := f.engine.Authenticate("Bearer " + f.tokens["reader"])
		if errors.Is(err, auth.ErrUnavailable) {
			break
		}
		if err != nil {
			t.Fatalf("unexpected refresh refusal: %v", err)
		}
		select {
		case <-deadline.C:
			t.Fatal("actual unavailable authentication bound did not expire")
		case <-tick.C:
		}
	}
	_, err := skyv1connect.NewControlServiceClient(f.server.Client(), f.server.URL).GetServerInfo(context.Background(), capabilityRequest(f, "reader", &v1.GetServerInfoRequest{}))
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("stale authority advertised support: %v", err)
	}
	for _, b := range f.backends {
		if b.checks.Load() != 0 {
			t.Fatal("unavailable discovery called a backend")
		}
	}
}
