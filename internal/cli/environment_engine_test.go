package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/environmentcontrol"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// TestEnvironmentEngineScripts runs testdata/environment/engine_*.txt
// against a real Store, auth Engine, environment Controller and
// EnvironmentService handler, with a MODELED runtime (engineRuntime) and a
// MODELED GetServerInfo (engineInfo). Each script gets its own server. The
// scripts call skygo with --token-file=$TOKEN_FILE; no token is in the
// environment. Besides the standard commands they have:
//
//	env-reconcile        run one Controller.Reconcile pass
//	env-restart          restart the server on the same state and runtime
//	env-advance DURATION advance the Controller's clock
//	env-records FILE     write the caller's environments, one per line
//	env-domain VAR       set VAR to the current request domain, LINEAGE:EPOCH
//	env-id VAR NAME      set VAR to the ID of the caller's environment NAME
//	env-lose-reply       serve the next CreateEnvironment, then answer 503
//	env-retire           retire the current request epoch
//	env-withdraw         withdraw the modeled target's environment support
func TestEnvironmentEngineScripts(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("real retention filesystem sampling requires Linux or Darwin")
	}
	files, err := filepath.Glob("testdata/environment/engine_*.txt")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		t.Run(strings.TrimSuffix(filepath.Base(file), ".txt"), func(t *testing.T) {
			f := newEnvironmentEngine(t)
			e := scriptEngine(t)
			f.addCommands(e)
			scripttest.Test(t, context.Background(), e, []string{"SKYGO_ENDPOINT=" + f.server.URL, "TOKEN_FILE=" + f.tokenFile}, file)
		})
	}
}

// engineTarget is the only target engineRuntime accepts.
const engineTarget = "modeled"

// environmentEngine is one server of a real Store, Engine, Controller and
// handler. Its listener, state directory and runtime survive env-restart.
type environmentEngine struct {
	t         *testing.T
	state     string
	config    string
	tokenFile string
	runtime   engineRuntime
	server    *httptest.Server
	// withdrawn is set by env-withdraw: the target no longer advertises
	// persistent environments and the runtime's Plan refuses every spec.
	withdrawn atomic.Bool
	// loseReply is set by env-lose-reply until the next CreateEnvironment.
	loseReply atomic.Bool

	mu    sync.Mutex
	now   time.Time
	stack *engineStack
}

// engineStack is the part of the server that env-restart replaces.
type engineStack struct {
	store      *store.Store
	stopAuth   func()
	actor      auth.Actor // the operator, for env-records and env-id
	controller *environmentcontrol.Controller
	handler    http.Handler
}

func newEnvironmentEngine(t *testing.T) *environmentEngine {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	f := &environmentEngine{
		t:         t,
		state:     filepath.Join(dir, "state"),
		config:    filepath.Join(dir, "auth.json"),
		tokenFile: filepath.Join(dir, "operator.token"),
		now:       time.Now(),
	}
	f.runtime = engineRuntime{withdrawn: &f.withdrawn}
	s, err := store.Open(f.state)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := auth.Initialize(f.config, filepath.Join(dir, "admin.token"), s.Owner()); err != nil {
		t.Fatal(err)
	}
	if err := auth.AddPrincipal(f.config, "operator", auth.Operator); err != nil {
		t.Fatal(err)
	}
	if _, err := auth.Issue(f.config, "operator", f.tokenFile, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	policy := store.DefaultRetentionPolicy()
	policy.LocalOutputEnabled = false
	if _, err := store.EnableRetention(f.state, policy); err != nil {
		t.Fatal(err)
	}
	if err := f.start(); err != nil {
		t.Fatal(err)
	}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		h := f.stack.handler
		f.mu.Unlock()
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(func() {
		f.server.Close()
		f.stop()
	})
	return f
}

// start opens the server's stack. The caller holds no lock.
func (f *environmentEngine) start() error {
	s, err := store.Open(f.state)
	if err != nil {
		return err
	}
	st := &engineStack{store: s}
	fail := func(err error) error {
		if st.stopAuth != nil {
			st.stopAuth()
		}
		return errors.Join(err, s.Close())
	}
	engine, err := auth.NewFile(f.config, s.Owner())
	if err != nil {
		return fail(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); engine.Run(ctx) }()
	st.stopAuth = func() { cancel(); <-done }
	token, err := os.ReadFile(f.tokenFile)
	if err != nil {
		return fail(err)
	}
	if st.actor, err = engine.Authenticate("Bearer " + strings.TrimSuffix(string(token), "\n")); err != nil {
		return fail(err)
	}
	c, err := control.New(s, []backend.Target{{
		Name: "jobs", Kind: "test", Binding: "test-binding", Backend: noJobs{},
		OutputStorage: backend.RemoteOutput,
		Retention:     &backend.Retention{RemoveExecution: func(context.Context, backend.Reference) error { return errNoJobs }},
	}}, engine)
	if err != nil {
		return fail(err)
	}
	st.controller, err = environmentcontrol.New(environmentcontrol.Config{Store: s, Auth: engine, Runtime: f.runtime, Now: f.clock})
	if err != nil {
		return fail(err)
	}
	served := rpc.NewEnvironmentHandler(c, engine, st.controller)
	_, info := skyv1connect.NewControlServiceHandler(engineInfo{withdrawn: &f.withdrawn})
	st.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == skyv1connect.ControlServiceGetServerInfoProcedure:
			info.ServeHTTP(w, r)
		case r.URL.Path == skyv1connect.EnvironmentServiceCreateEnvironmentProcedure && f.loseReply.CompareAndSwap(true, false):
			// The real handler runs to completion; only its reply is lost.
			served.ServeHTTP(httptest.NewRecorder(), r)
			http.Error(w, "reply lost", http.StatusServiceUnavailable)
		default:
			served.ServeHTTP(w, r)
		}
	})
	f.mu.Lock()
	f.stack = st
	f.mu.Unlock()
	return nil
}

// stop closes the server's stack. Requests that arrive before the next
// start fail.
func (f *environmentEngine) stop() {
	f.mu.Lock()
	st := f.stack
	f.stack = &engineStack{handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server restarting", http.StatusServiceUnavailable)
	})}
	f.mu.Unlock()
	if st == nil || st.store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := st.controller.Close(ctx); err != nil {
		f.t.Error(err)
	}
	st.stopAuth()
	if err := st.store.Close(); err != nil {
		f.t.Error(err)
	}
}

func (f *environmentEngine) clock() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *environmentEngine) current() *engineStack {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stack
}

// environments returns the operator's environments, in List order.
func (st *engineStack) environments() ([]environmentcontrol.View, error) {
	var vs []environmentcontrol.View
	var after uint64
	for {
		p, err := st.controller.List(st.actor, after, 200)
		if err != nil {
			return nil, err
		}
		vs = append(vs, p.Environments...)
		if !p.More {
			return vs, nil
		}
		after = p.After
	}
}

func (f *environmentEngine) addCommands(e *script.Engine) {
	e.Cmds["env-reconcile"] = script.Command(script.CmdUsage{Summary: "run one environment reconcile pass"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 0 {
			return nil, script.ErrUsage
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return nil, f.current().controller.Reconcile(ctx)
	})
	e.Cmds["env-restart"] = script.Command(script.CmdUsage{Summary: "restart the server on the same state and runtime"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 0 {
			return nil, script.ErrUsage
		}
		f.stop()
		return nil, f.start()
	})
	e.Cmds["env-advance"] = script.Command(script.CmdUsage{Summary: "advance the environment clock", Args: "duration"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		d, err := time.ParseDuration(args[0])
		if err != nil || d < 0 {
			return nil, fmt.Errorf("invalid duration %q", args[0])
		}
		f.mu.Lock()
		f.now = f.now.Add(d)
		f.mu.Unlock()
		return nil, nil
	})
	e.Cmds["env-records"] = script.Command(script.CmdUsage{Summary: "write the caller's environments, one per line", Args: "file"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		vs, err := f.current().environments()
		if err != nil {
			return nil, err
		}
		var b strings.Builder
		for _, v := range vs {
			fmt.Fprintf(&b, "%s %v/%v sessions=%d unknown=%d held=%v expired=%v updated=%s\n",
				v.ID, v.State.Desired, v.State.Phase, v.Sessions, v.Unknown, v.Held, v.Expired, v.UpdatedAt.UTC().Format(time.RFC3339Nano))
		}
		return nil, os.WriteFile(s.Path(args[0]), []byte(b.String()), 0600)
	})
	// env-id gives scripts the server-generated ID of an environment, which
	// they cannot capture from skygo's output.
	e.Cmds["env-id"] = script.Command(script.CmdUsage{Summary: "set a variable to the ID of the caller's environment with a name", Args: "var name"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		vs, err := f.current().environments()
		if err != nil {
			return nil, err
		}
		id := ""
		for _, v := range vs {
			if v.Spec.Name != args[1] {
				continue
			}
			if id != "" {
				return nil, fmt.Errorf("more than one environment named %q", args[1])
			}
			id = v.ID
		}
		if id == "" {
			return nil, fmt.Errorf("no environment named %q", args[1])
		}
		return nil, s.Setenv(args[0], id)
	})
	e.Cmds["env-domain"] = script.Command(script.CmdUsage{Summary: "set a variable to the current request domain", Args: "var"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		st := f.current()
		info, err := st.store.RetentionInfo(st.actor)
		if err != nil {
			return nil, err
		}
		return nil, s.Setenv(args[0], fmt.Sprintf("%s:%d", info.Domain.Lineage, info.Domain.Epoch))
	})
	e.Cmds["env-lose-reply"] = script.Command(script.CmdUsage{Summary: "serve the next CreateEnvironment, then answer 503"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 0 {
			return nil, script.ErrUsage
		}
		f.loseReply.Store(true)
		return nil, nil
	})
	// env-retire advances retention to the current epoch's retry deadline,
	// which retires that epoch for good; it runs in this, the only writing
	// process.
	e.Cmds["env-retire"] = script.Command(script.CmdUsage{Summary: "retire the current request epoch"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 0 {
			return nil, script.ErrUsage
		}
		st := f.current()
		info, err := st.store.RetentionInfo(st.actor)
		if err != nil {
			return nil, err
		}
		return nil, st.store.MaintainRetention(info.RetryUntil)
	})
	e.Cmds["env-withdraw"] = script.Command(script.CmdUsage{Summary: "withdraw the modeled target's environment support"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 0 {
			return nil, script.ErrUsage
		}
		f.withdrawn.Store(true)
		return nil, nil
	})
}

// engineInfo is a MODELED GetServerInfo. The real handler advertises no
// environment capability, and none is ratified; it reports
// ENVIRONMENTS_V1 and target engineTarget with PERSISTENT_ENVIRONMENTS_V1
// so that the CLI's capability checks pass. Once withdrawn is set the
// target is still listed, without PERSISTENT_ENVIRONMENTS_V1.
type engineInfo struct {
	skyv1connect.UnimplementedControlServiceHandler
	withdrawn *atomic.Bool
}

func (in engineInfo) GetServerInfo(context.Context, *connect.Request[v1.GetServerInfoRequest]) (*connect.Response[v1.GetServerInfoResponse], error) {
	target := &v1.Target{Name: engineTarget, Kind: "modeled"}
	if !in.withdrawn.Load() {
		target.Capabilities = []v1.Capability{v1.Capability_CAPABILITY_PERSISTENT_ENVIRONMENTS_V1}
	}
	return connect.NewResponse(&v1.GetServerInfoResponse{
		Capabilities: []v1.Capability{v1.Capability_CAPABILITY_ENVIRONMENTS_V1},
		Targets:      []*v1.Target{target},
	}), nil
}

// engineRuntime is a MODELED environmentcontrol.Runtime of target
// engineTarget, without artifacts. It sends nothing and owns no resource:
// Ensure observes Ready and Delete observes Deleted. Exec relays argv as
// stdout and stdin as stderr, with exit status zero; argv[0] "lost" relays
// no exit status, and argv[0] "refused" starts nothing. Once withdrawn is
// set, Plan refuses every spec. Forward answers
// 200 with the request it received: method, escaped path, query, and
// whether an Authorization header reached it.
type engineRuntime struct {
	withdrawn *atomic.Bool
}

func (engineRuntime) Kind() string { return "modeled" }

func (rt engineRuntime) Plan(principal string, spec environment.Spec, digest [32]byte) (string, []byte, error) {
	if rt.withdrawn.Load() || spec.Target != engineTarget || len(spec.Artifacts) != 0 {
		return "", nil, environmentcontrol.ErrUnsupported
	}
	return "modeled-binding", []byte("modeled"), nil
}

func (engineRuntime) Ensure(ctx context.Context, r environmentcontrol.Ref) (environmentcontrol.Observation, error) {
	return environmentcontrol.Observation{Phase: environment.Ready}, nil
}

func (engineRuntime) Delete(ctx context.Context, r environmentcontrol.Ref) (environmentcontrol.Observation, error) {
	return environmentcontrol.Observation{Phase: environment.Deleted}, nil
}

func (engineRuntime) Exec(r environmentcontrol.Ref, req environmentcontrol.ExecRequest) (environmentcontrol.Exec, error) {
	res := environmentcontrol.ExecResult{Settled: true}
	switch req.Argv[0] {
	case "refused":
		res.Refused = true
	case "lost":
		res.Err = errors.New("modeled exit status lost")
	default:
		res.Exited = true
		res.Stdout = []byte(strings.Join(req.Argv, " ") + "\n")
		res.Stderr = req.Stdin
	}
	return engineFrame{res}, nil
}

func (engineRuntime) Forward(w http.ResponseWriter, req *http.Request, r environmentcontrol.Ref) environmentcontrol.Forward {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "%s %s?%s port=%d authorization=%v\n", req.Method, req.URL.EscapedPath(), req.URL.RawQuery, r.Spec.Port, req.Header.Get("Authorization") != "")
	return engineForward{}
}

// engineDone is the Done channel of every engineRuntime frame.
var engineDone = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// engineFrame is an exec frame that settled before it was returned.
type engineFrame struct {
	res environmentcontrol.ExecResult
}

func (engineFrame) Done() <-chan struct{}                   { return engineDone }
func (engineFrame) Cancel()                                 {}
func (x engineFrame) Result() environmentcontrol.ExecResult { return x.res }

type engineForward struct{}

func (engineForward) Done() <-chan struct{} { return engineDone }
func (engineForward) Cancel()               {}
func (engineForward) Result() environmentcontrol.ForwardResult {
	return environmentcontrol.ForwardResult{Settled: true, Status: http.StatusOK}
}

var errNoJobs = errors.New("environment engine runs no jobs")

// noJobs is the job backend of the engine's only job target. It accepts no
// task.
type noJobs struct{}

func (noJobs) Check(backend.Task) error                          { return errNoJobs }
func (noJobs) Start(context.Context, string, backend.Task) error { return errNoJobs }
func (noJobs) Observe(context.Context, backend.Reference) (backend.Status, error) {
	return backend.Status{}, errNoJobs
}
func (noJobs) Cancel(context.Context, backend.Reference) error { return errNoJobs }
func (noJobs) Logs(context.Context, backend.Reference, int64, int) (backend.LogChunk, error) {
	return backend.LogChunk{}, errNoJobs
}

// TestEnvironmentForwardEngine relays one request through the CLI's own
// forward listener, the real forward route and Controller.Forward to the
// modeled runtime. Only the route prefix is removed, the escaping and query
// are kept, the environment's declared port is used, and neither the local
// client's nor the CLI's credentials reach the workload.
func TestEnvironmentForwardEngine(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("real retention filesystem sampling requires Linux or Darwin")
	}
	f := newEnvironmentEngine(t)
	st := f.current()
	info, err := st.store.RetentionInfo(st.actor)
	if err != nil {
		t.Fatal(err)
	}
	spec := environment.Spec{Name: "web", Target: engineTarget, Image: "task:v1", CPUMillis: 100, MemoryBytes: 1 << 20, Port: environment.Port}
	v, err := st.controller.Create(st.actor, info.Domain, "forward-1", spec)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := st.controller.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if v, err = st.controller.Get(st.actor, v.ID); err != nil || v.State.Phase != environment.Ready {
		t.Fatalf("environment = %v, %v; want Ready", v.State, err)
	}
	token, err := os.ReadFile(f.tokenFile)
	if err != nil {
		t.Fatal(err)
	}
	target, err := forwardURL(f.server.URL, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, stop := context.WithCancel(ctx)
	served := make(chan error, 1)
	go func() { served <- serveForward(serveCtx, ln, target, strings.TrimSpace(string(token)), io.Discard) }()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+ln.Addr().String()+"/a%2Fb/c?x=1&y=%2F", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer local-client-credential")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("GET /a%%2Fb/c?x=1&y=%%2F port=%d authorization=false\n", environment.Port)
	if resp.StatusCode != http.StatusOK || string(body) != want {
		t.Fatalf("forward = %d %q, want 200 %q", resp.StatusCode, body, want)
	}
	stop()
	if err := <-served; err != nil {
		t.Fatalf("serveForward = %v", err)
	}
}
