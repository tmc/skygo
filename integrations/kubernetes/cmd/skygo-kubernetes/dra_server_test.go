//go:build linux || darwin

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/server"
	"github.com/tmc/skygo/internal/store"
)

// draNative is the MODELED native API of TestDRAServerRun for namespace
// skygo-dra: construction reads, one create per object, echoed with a UID,
// exact reads and Pod logs. It runs no scheduler, kubelet or DRA driver: the
// test publishes the Pod's success itself, standing for the trusted kubelet
// of docs/dra.md. Any other request, a deletion included, is refused and
// recorded; with zero log retention none is expected.
type draNative struct {
	mu      sync.Mutex
	class   bool
	objects map[string]map[string]any // by object path
	creates []string                  // collection path of each create, in order
	other   []string                  // requests that are neither reads nor creates
	uid     int
}

const (
	draNativeClaims = "/apis/resource.k8s.io/v1/namespaces/skygo-dra/resourceclaims"
	draNativePods   = "/api/v1/namespaces/skygo-dra/pods"
)

func (n *draNative) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply := func(code int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(v)
	}
	status := func(code int, reason string) {
		reply(code, map[string]any{"kind": "Status", "apiVersion": "v1", "status": "Failure", "reason": reason, "code": code})
	}
	p := r.URL.Path
	switch {
	case r.Method == "GET" && p == "/apis/resource.k8s.io/v1":
		reply(200, map[string]any{"kind": "APIResourceList", "apiVersion": "v1", "groupVersion": "resource.k8s.io/v1", "resources": []any{
			map[string]any{"name": "resourceclaims", "singularName": "", "namespaced": true, "kind": "ResourceClaim", "verbs": []string{"create", "delete", "get"}},
			map[string]any{"name": "deviceclasses", "singularName": "", "namespaced": false, "kind": "DeviceClass", "verbs": []string{"get"}},
		}})
	case r.Method == "GET" && p == "/api/v1/namespaces/skygo-dra":
		reply(200, map[string]any{"kind": "Namespace", "apiVersion": "v1", "metadata": map[string]any{"name": "skygo-dra", "uid": "ns-uid"}, "status": map[string]any{"phase": "Active"}})
	case r.Method == "GET" && p == "/apis/resource.k8s.io/v1/deviceclasses/gpu.example.com" && n.class:
		reply(200, map[string]any{"kind": "DeviceClass", "apiVersion": "resource.k8s.io/v1", "metadata": map[string]any{"name": "gpu.example.com", "uid": "class-uid"}, "spec": map[string]any{}})
	case r.Method == "POST" && (p == draNativeClaims || p == draNativePods):
		n.creates = append(n.creates, p)
		var obj map[string]any
		if err := json.NewDecoder(r.Body).Decode(&obj); err != nil {
			status(400, "BadRequest")
			return
		}
		meta, _ := obj["metadata"].(map[string]any)
		name, _ := meta["name"].(string)
		if name == "" || n.objects[p+"/"+name] != nil {
			status(409, "AlreadyExists")
			return
		}
		n.uid++
		meta["uid"] = fmt.Sprintf("native-uid-%d", n.uid)
		meta["namespace"], meta["resourceVersion"] = "skygo-dra", "1"
		meta["creationTimestamp"] = time.Now().UTC().Format(time.RFC3339)
		obj["apiVersion"], obj["kind"] = "resource.k8s.io/v1", "ResourceClaim"
		if p == draNativePods {
			obj["apiVersion"], obj["kind"] = "v1", "Pod"
		}
		n.objects[p+"/"+name] = obj
		reply(201, obj)
	case r.Method == "GET" && strings.HasPrefix(p, draNativePods+"/") && strings.HasSuffix(p, "/log"):
		if n.objects[strings.TrimSuffix(p, "/log")] == nil {
			status(404, "NotFound")
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, "dra output\n")
	case r.Method == "GET":
		if obj := n.objects[p]; obj != nil {
			reply(200, obj)
			return
		}
		status(404, "NotFound")
	default:
		n.other = append(n.other, r.Method+" "+p)
		status(405, "MethodNotAllowed")
	}
}

// succeed publishes an ordinary success of the one created Pod, as the
// trusted kubelet would after its container stopped and its claim was
// unprepared. It reports whether a Pod exists.
func (n *draNative) succeed() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	for path, obj := range n.objects {
		if !strings.HasPrefix(path, draNativePods+"/") {
			continue
		}
		now := time.Now().UTC()
		terminated := map[string]any{"exitCode": 0, "containerID": "containerd://dra",
			"startedAt": now.Add(-time.Second).Format(time.RFC3339), "finishedAt": now.Format(time.RFC3339)}
		obj["status"] = map[string]any{"phase": "Succeeded", "containerStatuses": []any{map[string]any{
			"name": "task", "containerID": "containerd://dra", "image": "example.invalid/task", "imageID": "",
			"ready": false, "restartCount": 0, "state": map[string]any{"terminated": terminated},
		}}}
		return true
	}
	return false
}

func (n *draNative) record() (creates, other []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]string(nil), n.creates...), append([]string(nil), n.other...)
}

const draServerTargets = `version: 1
targets:
- name: gpu
  kubeconfig: kubeconfig
  context: test
  namespace: skygo-dra
  resources:
    max_accelerators: 8
    accelerators:
      h100:
        dra:
          device_class: gpu.example.com
`

// draServer is one run of the production command with the actual
// server.Run, over private directories, against a draNative served by api.
type draServer struct {
	addr                      string
	api                       *httptest.Server
	targets, state, dra, data string
	before                    string   // image of dra before the server started
	dirs                      []string // every privateDir of the server
	cancel                    context.CancelFunc
	done                      chan error
	joined                    bool
	err                       error
	closed                    bool  // a reopen showed the Store was closed
	expect                    error // the server error of a HOLD the test expects
	retained                  bool  // held: the API and directories stay
}

// retainedDRAServers keeps every server whose cleanup held it, and every
// owner a test transfers into that HOLD, such as a Store a modeled server
// kept open, strongly reachable for the test binary's lifetime: no finalizer
// may close the Store's lock while its directories are held.
var retainedDRAServers struct {
	sync.Mutex
	owners []any
}

func retainDRA(owner any) {
	retainedDRAServers.Lock()
	defer retainedDRAServers.Unlock()
	retainedDRAServers.owners = append(retainedDRAServers.owners, owner)
}

// startDRAServer starts the command with the actual server.Run. Its
// cancellation and join are registered before the server goroutine is
// spawned. A server that is not joined within the bound keeps every
// directory and the API, and so does one that returned, with or without an
// error, without positive proof that its Store, and so the integration, was
// closed (reopen): the core keeps the Integration and the Store it depends
// on, and returns an error, when the integration's Close fails, so a
// returned server proves nothing. Such a HOLD fails the test unless the test
// declared it, by the server error it expects (expect).
func startDRAServer(t *testing.T, n *draNative) *draServer {
	t.Helper()
	return startDRACommand(t, n, server.Run)
}

// startDRACommand is startDRAServer with run in place of server.Run.
func startDRACommand(t *testing.T, n *draNative, run func(context.Context, server.Config) error) *draServer {
	t.Helper()
	s := &draServer{done: make(chan error, 1), api: httptest.NewServer(n)}
	// Registered first, so it runs after the server's cleanup decided: the
	// API goes only with everything that uses it.
	t.Cleanup(func() {
		if !s.retained {
			s.api.Close()
		}
	})
	dir := privateDir(t)
	kubeconfig := fmt.Sprintf("apiVersion: v1\nkind: Config\nclusters: [{name: c, cluster: {server: %q}}]\ncontexts: [{name: test, context: {cluster: c, user: u}}]\nusers: [{name: u, user: {}}]\ncurrent-context: test\n", s.api.URL)
	if err := os.WriteFile(filepath.Join(dir, "kubeconfig"), []byte(kubeconfig), 0o600); err != nil {
		t.Fatal(err)
	}
	s.targets, s.state, s.dra, s.data = filepath.Join(dir, "targets.yaml"), privateDir(t), privateDir(t), filepath.Join(privateDir(t), "data")
	s.dirs = []string{dir, s.state, s.dra, filepath.Dir(s.data)}
	if err := os.WriteFile(s.targets, []byte(draServerTargets), 0o600); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s.addr = l.Addr().String()
	l.Close()
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	t.Cleanup(func() {
		switch {
		case !s.join(t):
			s.hold(t, errors.New("HOLD: server.Run original not joined"))
		case !s.closed && !s.reopened():
			s.hold(t, fmt.Errorf("HOLD: server returned %v without positive Store closure; its integration and Store may be retained", s.err))
		case s.expect != nil:
			t.Errorf("server closed its Store, but its expected HOLD (%v) did not happen", s.expect)
		}
	})
	cmd := command(run)
	cmd.SetArgs([]string{"server", "--targets", s.targets, "--execution-state", s.state, "--dra-state", s.dra,
		"--data-dir", s.data, "--listen", s.addr, "--disable-local"})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	s.before = dirImage(t, s.dra)
	go func() { s.done <- cmd.ExecuteContext(ctx) }()
	return s
}

// join cancels the server and waits for its original Run, within the bound.
func (s *draServer) join(t *testing.T) bool {
	t.Helper()
	s.cancel()
	return s.wait(t)
}

// hold keeps s, its API and its directories for reason: a failure of t,
// unless the joined server returned the error the test expects.
func (s *draServer) hold(t *testing.T, reason error) {
	s.retained = true
	retainDRA(s)
	if s.joined && s.expect != nil && errors.Is(s.err, s.expect) {
		keepDirs(t, reason)
		return
	}
	holdDirs(t, reason)
}

// reopened is reopen for the cleanup: it reports, without failing t,
// whether the joined server's Store can be opened and closed again.
func (s *draServer) reopened() bool {
	st, err := store.Open(s.data)
	if err != nil {
		return false
	}
	if st.Close() != nil {
		return false
	}
	s.closed = true
	return true
}

// reopen opens and closes the returned server's Store. Only its success is
// positive proof that the server closed the Store, and so, since the core
// closes the Store only after the integration's Close returned nil, the
// integration too; a returned server, with or without an error, is not.
func (s *draServer) reopen(t *testing.T) {
	t.Helper()
	st, err := store.Open(s.data)
	if err != nil {
		t.Fatalf("store retained: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	s.closed = true
}

// wait waits for the original Run to return by itself, within the bound. A
// returned Run is joined; it is not proof of what it closed (reopen).
func (s *draServer) wait(t *testing.T) bool {
	t.Helper()
	if s.joined {
		return true
	}
	select {
	case s.err = <-s.done:
		s.joined = true
	case <-time.After(stateTimeout):
	}
	return s.joined
}

// ready waits until the server reports ready, or fails if it returned.
func (s *draServer) ready(t *testing.T) {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	for deadline := time.Now().Add(stateTimeout); ; time.Sleep(10 * time.Millisecond) {
		select {
		case s.err = <-s.done:
			s.joined = true
			t.Fatalf("server returned before ready: %v", s.err)
		default:
		}
		if r, err := client.Get("http://" + s.addr + "/ready"); err == nil {
			r.Body.Close()
			if r.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("server not ready")
		}
	}
}

// draRecords returns the record files in the scopes of DRA state root.
func draRecords(t *testing.T, root string) []string {
	t.Helper()
	var records []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() && filepath.Dir(path) != root && strings.HasSuffix(path, ".json") {
			records = append(records, path)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return records
}

// TestDRAServerRun runs the production command with the actual server.Run,
// a real Store and the generated Connect client against a MODELED native
// API: Install constructs the DRA integration, a submitted DRA job creates
// exactly one claim and then one Pod, the Pod's published success makes the
// job succeed, and cancellation shuts the server down through the
// integration's Run and Close. Its twin is a startup failure, which writes no
// DRA state and releases the Store. It is not native qualification.
func TestDRAServerRun(t *testing.T) {
	t.Run("job succeeds", func(t *testing.T) {
		n := &draNative{class: true, objects: map[string]map[string]any{}}
		s := startDRAServer(t, n)
		s.ready(t)
		ctx, cancel := context.WithTimeout(context.Background(), stateTimeout)
		defer cancel()
		api := skyv1connect.NewControlServiceClient(&http.Client{Timeout: 10 * time.Second}, "http://"+s.addr)
		accepted, err := api.SubmitJob(ctx, connect.NewRequest(&v1.SubmitJobRequest{RequestId: "dra-vertical", Task: &v1.Task{
			Target: "gpu", Argv: []string{"/task"}, Image: "example.invalid/task@sha256:" + strings.Repeat("1", 64),
			Accelerator: "h100", AcceleratorCount: 1, CpuMillis: 1000, CpuLimitMillis: 1000,
		}}))
		if err != nil {
			t.Fatal(err)
		}
		id := accepted.Msg.Job.Id
		for !n.succeed() {
			if ctx.Err() != nil {
				t.Fatal("no Pod was created")
			}
			time.Sleep(10 * time.Millisecond)
		}
		for {
			got, err := api.GetJob(ctx, connect.NewRequest(&v1.GetJobRequest{Id: id}))
			if err != nil {
				t.Fatal(err)
			}
			state := got.Msg.Job.GetState()
			if state == v1.JobState_JOB_STATE_SUCCEEDED {
				break
			}
			if state == v1.JobState_JOB_STATE_FAILED || state == v1.JobState_JOB_STATE_UNKNOWN || state == v1.JobState_JOB_STATE_CANCELED {
				t.Fatalf("job state %v", state)
			}
			if ctx.Err() != nil {
				t.Fatalf("job did not succeed: %v", state)
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !s.join(t) {
			holdDirs(t, errors.New("HOLD: server.Run original not joined"))
			t.FailNow()
		}
		if s.err != nil {
			// Joined, but not closed: the cleanup keeps the directories.
			t.Fatalf("server shutdown: %v", s.err)
		}
		creates, other := n.record()
		if strings.Join(creates, " ") != draNativeClaims+" "+draNativePods || len(other) != 0 {
			t.Fatalf("native requests: creates %v, other %v", creates, other)
		}
		if _, err := os.Stat(filepath.Join(s.dra, "skygo-dra-state.json")); err != nil {
			t.Fatal(err)
		}
		if records := draRecords(t, s.dra); len(records) != 1 {
			t.Fatalf("dra records %v", records)
		}
		// The Store lock was released by the joined shutdown.
		s.reopen(t)
	})
	t.Run("startup failure", func(t *testing.T) {
		n := &draNative{class: false, objects: map[string]map[string]any{}}
		s := startDRAServer(t, n)
		// Not canceled: construction fails by itself.
		if !s.wait(t) {
			holdDirs(t, errors.New("HOLD: server.Run original not joined"))
			t.FailNow()
		}
		if s.err == nil || !strings.Contains(s.err.Error(), "device class is missing") {
			t.Fatalf("server = %v", s.err)
		}
		if after := dirImage(t, s.dra); after != s.before {
			t.Fatalf("failed startup wrote dra state:\n%s", after)
		}
		if creates, other := n.record(); len(creates) != 0 || len(other) != 0 {
			t.Fatalf("native requests: creates %v, other %v", creates, other)
		}
		// The expected error is not proof of closure; the reopen is. Any
		// failure above keeps the directories.
		s.reopen(t)
	})
}

// TestDRAServerReturnedError is the control of startDRAServer's custody. A
// server that returns an error while it still holds its Store, as the core
// does after the integration's Close failed (MODELED by the callback: the
// unchanged core's failure cannot be injected here), has joined, yet the
// same API stays served and its directories stay until that retained owner
// is positively closed; the test declared that HOLD, so it does not fail.
// The twin returns the same error after closing its Store, which a reopen
// shows: its API is closed and its directories are removed.
func TestDRAServerReturnedError(t *testing.T) {
	errClosure := errors.New("modeled integration closure remains held")
	for _, holds := range []bool{true, false} {
		t.Run(fmt.Sprintf("store held %v", holds), func(t *testing.T) {
			var s *draServer
			retained := make(chan *store.Store, 1)
			passed := t.Run("server", func(t *testing.T) {
				n := &draNative{class: true, objects: map[string]map[string]any{}}
				s = startDRACommand(t, n, func(_ context.Context, cfg server.Config) error {
					st, err := store.Open(cfg.DataDir)
					if err != nil {
						return err
					}
					if holds {
						retainDRA(st) // strongly, before its last reference may go
						retained <- st
						return errClosure
					}
					return errors.Join(errClosure, st.Close())
				})
				if holds {
					s.expect = errClosure
				}
				if !s.wait(t) || !errors.Is(s.err, errClosure) {
					t.Fatalf("server = %v", s.err)
				}
				if !holds {
					s.reopen(t)
				}
			})
			if s == nil {
				t.Fatal("server did not start")
			}
			if !passed {
				t.Fatal("server test failed: a declared HOLD is not a failure")
			}
			for _, dir := range s.dirs {
				if _, err := os.Lstat(dir); (err == nil) != holds {
					t.Fatalf("%s kept %v, want %v (%v)", dir, err == nil, holds, err)
				}
			}
			resp, err := (&http.Client{Timeout: 10 * time.Second}).Get(s.api.URL + "/api/v1/namespaces/skygo-dra")
			if err == nil {
				resp.Body.Close()
			}
			if served := err == nil && resp.StatusCode == http.StatusOK; served != holds || s.retained != holds {
				t.Fatalf("API served %v (%v), retained %v; want %v", served, err, s.retained, holds)
			}
			if !holds {
				return
			}
			// Only the retained owner's positive Close lets them go.
			var st *store.Store
			select {
			case st = <-retained:
			default:
				t.Fatal("no retained store")
			}
			if err := st.Close(); err != nil {
				t.Fatalf("retained store: %v; directories kept", err)
			}
			s.api.Close()
			for _, dir := range s.dirs {
				if err := os.RemoveAll(dir); err != nil {
					t.Error(err)
				}
			}
		})
	}
}

// TestDRAServerUnexpectedHold runs one server test in a child process of
// this test binary, so that its failure is observed rather than failing
// this test: a server that returns while it still holds its Store, without
// a declared HOLD, must mark that test FAILED, while its API stays served
// and its directories stay, which a second test of the same child inspects
// after the cleanup ran. The parent registers the child's kill and reap
// before it starts, waits for it within the bound, and owns its temporary
// directories, removed only after it exited.
func TestDRAServerUnexpectedHold(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	base := privateDir(t) // the child's directories
	c := exec.Command(exe, "-test.run=^TestDRAServerHelper(Hold|Inspect)$", "-test.count=1", "-test.v")
	c.Env = append(os.Environ(), "SKYGO_DRA_SERVER_HELPER=1", "TMPDIR="+base)
	out := new(strings.Builder)
	c.Stdout, c.Stderr = out, out
	waited := make(chan error, 1)
	reaped := false
	t.Cleanup(func() {
		if c.Process == nil || reaped {
			return
		}
		c.Process.Kill()
		select {
		case <-waited:
		case <-time.After(stateTimeout):
			holdDirs(t, fmt.Errorf("HOLD: child %d not reaped", c.Process.Pid))
		}
	})
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { waited <- c.Wait() }()
	var werr error
	select {
	case werr = <-waited:
		reaped = true
	case <-time.After(stateTimeout):
		// The child may still write out: it is read only after its Wait.
		t.Fatal("child did not return within the bound; its output is not read before its Wait")
	}
	var exit *exec.ExitError
	if !errors.As(werr, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("child = %v, want its failed test\n%s", werr, out)
	}
	for _, want := range []string{"--- FAIL: TestDRAServerHelperHold", "without positive Store closure", "--- PASS: TestDRAServerHelperInspect", "INSPECTED kept"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("child output lacks %q:\n%s", want, out)
		}
	}
}

// draHelperServer is the server of TestDRAServerHelperHold, for
// TestDRAServerHelperInspect, which runs after it in the same child.
var draHelperServer *draServer

// TestDRAServerHelperHold is the child of TestDRAServerUnexpectedHold: its
// server keeps its Store and returns an error it did not declare.
func TestDRAServerHelperHold(t *testing.T) {
	if os.Getenv("SKYGO_DRA_SERVER_HELPER") == "" {
		return // only a child of TestDRAServerUnexpectedHold runs it
	}
	n := &draNative{class: true, objects: map[string]map[string]any{}}
	draHelperServer = startDRACommand(t, n, func(_ context.Context, cfg server.Config) error {
		st, err := store.Open(cfg.DataDir)
		if err != nil {
			return err
		}
		retainDRA(st)
		return errors.New("modeled integration closure remains held")
	})
	if !draHelperServer.wait(t) {
		t.Fatal("server not joined")
	}
}

// TestDRAServerHelperInspect inspects, after its cleanup ran, the server of
// TestDRAServerHelperHold in the same child.
func TestDRAServerHelperInspect(t *testing.T) {
	if os.Getenv("SKYGO_DRA_SERVER_HELPER") == "" {
		return // only a child of TestDRAServerUnexpectedHold runs it
	}
	s := draHelperServer
	if s == nil {
		t.Fatal("no server test ran")
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Get(s.api.URL + "/api/v1/namespaces/skygo-dra")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !s.retained {
		t.Fatalf("API status %d, retained %v", resp.StatusCode, s.retained)
	}
	for _, dir := range s.dirs {
		if _, err := os.Lstat(dir); err != nil {
			t.Fatalf("held directory %s: %v", dir, err)
		}
	}
	if st, err := store.Open(s.data); err == nil {
		st.Close()
		t.Fatal("held Store reopened: its owner was not retained")
	}
	fmt.Println("INSPECTED kept")
}
