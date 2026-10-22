package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// TestWorkflowWorkload is the program the workflow scripts submit. It records
// each start in SKYGO_WORKFLOW_DIR/starts and coordinates through files there,
// so no step depends on elapsed time.
func TestWorkflowWorkload(t *testing.T) {
	mode := os.Getenv("SKYGO_WORKFLOW_WORKLOAD")
	if mode == "" {
		return
	}
	fail := func(err error) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(99)
	}
	if os.Args[len(os.Args)-1] == "setup" {
		if err := os.WriteFile("prepared", []byte("prepared\n"), 0600); err != nil {
			fail(err)
		}
		os.Exit(0)
	}
	dir, node := os.Getenv("SKYGO_WORKFLOW_DIR"), os.Getenv("SKYGO_WORKFLOW_NODE")
	mark := func(name string) {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0600); err != nil {
			fail(err)
		}
	}
	f, err := os.OpenFile(filepath.Join(dir, "starts"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		fail(err)
	}
	if _, err := fmt.Fprintf(f, "start %s\n", node); err != nil {
		fail(err)
	}
	if err := f.Close(); err != nil {
		fail(err)
	}
	switch mode {
	case "succeed":
		// Every node named in SKYGO_WORKFLOW_AFTER must already have finished.
		for _, d := range strings.FieldsFunc(os.Getenv("SKYGO_WORKFLOW_AFTER"), func(r rune) bool { return r == ',' }) {
			if _, err := os.Stat(filepath.Join(dir, "done-"+d)); err != nil {
				fail(fmt.Errorf("node %s started before %s finished: %v", node, d, err))
			}
		}
		fmt.Println("node", node)
		mark("done-" + node)
	case "hold":
		fmt.Println("holding", node)
		mark("ready-" + node)
		// A failed script may leave the daemon running or remove dir; the
		// workload then exits instead of waiting out its deadline.
		parent := os.Getppid()
		deadline := time.Now().Add(2 * time.Minute)
		for {
			if _, err := os.Stat(filepath.Join(dir, "release-"+node)); err == nil {
				break
			} else if !errors.Is(err, os.ErrNotExist) {
				fail(err)
			}
			if _, err := os.Stat(dir); err != nil {
				fail(err)
			}
			if os.Getppid() != parent {
				fail(errors.New("workload parent exited"))
			}
			if time.Now().After(deadline) {
				fail(errors.New("workload release timed out"))
			}
			time.Sleep(10 * time.Millisecond)
		}
		mark("done-" + node)
	case "produce":
		seed, err := os.ReadFile("seed.txt")
		if err != nil {
			fail(err)
		}
		prepared, err := os.ReadFile("prepared")
		if err != nil {
			fail(err)
		}
		if err := os.Mkdir("out", 0700); err != nil {
			fail(err)
		}
		if err := os.WriteFile(filepath.Join("out", "result.txt"), append(seed, prepared...), 0600); err != nil {
			fail(err)
		}
	case "consume":
		data, err := os.ReadFile(filepath.Join("out", "result.txt"))
		if err != nil {
			fail(err)
		}
		if _, err := os.Stdout.Write(data); err != nil {
			fail(err)
		}
	default:
		fail(fmt.Errorf("unknown workload %q", mode))
	}
	os.Exit(0)
}

// TestWorkflowScripts runs testdata/workflows against the actual CLI and
// daemon executables with a real Store, Control and local executor. The
// invalid script also requires the exact predecessor daemon in
// SKYGO_S05_BINARY and SKYGO_S05_BINARY_SHA256.
func TestWorkflowScripts(t *testing.T) {
	exe := retentionScriptBinary(t)
	for _, name := range []string{"basic", "replay_restart", "handoff", "invalid", "cancel"} {
		t.Run(name, func(t *testing.T) {
			old := ""
			if name == "invalid" {
				old = retentionPinnedBinary(t, os.Getenv("SKYGO_S05_BINARY"), os.Getenv("SKYGO_S05_BINARY_SHA256"))
			}
			runWorkflowScript(t, exe, old, "testdata/workflows/"+name+".txt")
		})
	}
}

func runWorkflowScript(t *testing.T, exe, old, file string) {
	t.Helper()
	var env []string
	if dir := coverDir(t); dir != "" {
		env = append(env, "GOCOVERDIR="+dir)
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	data, authFile := filepath.Join(dir, "state"), filepath.Join(dir, "auth.json")
	root := filepath.Join(data, "artifacts")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	// Artifacts are initialized while stopped, as in the artifact scripts;
	// each script then activates scoped authentication on the same state.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	initialize := exec.CommandContext(ctx, exe, "state", "artifacts", "init", "--data-dir", data, "--artifact-dir", root, "--ack-writers-stopped")
	initialize.Env = append(os.Environ(), env...)
	out, err := initialize.CombinedOutput()
	cancel()
	if err != nil {
		t.Fatalf("initialize workflow script artifacts: %v\n%s", err, out)
	}
	if string(out) != "enabled: true\npolicy: default-v1\nrestart_required: true\n" {
		t.Fatalf("unexpected artifact initialization readback: %q", out)
	}
	process := &serverProcess{exe: exe, address: freeAddress(t), data: data, env: env, args: []string{"--auth-file", authFile, "--artifact-dir", root}}
	t.Cleanup(workflowServerCleanup(t, process))
	calls := &workflowCalls{paths: make(map[string]int)}
	counted := httptest.NewServer(calls.wrap(httputil.NewSingleHostReverseProxy(&url.URL{Scheme: "http", Host: process.address})))
	t.Cleanup(counted.Close)
	countedURL, err := url.Parse(counted.URL)
	if err != nil {
		t.Fatal(err)
	}
	fault := &faultProxy{target: countedURL, drop: make(map[string]int)}
	faulted := httptest.NewServer(fault)
	t.Cleanup(faulted.Close)

	e := executableScriptEngine(exe, nil, env)
	e.Cmds["server"] = process.command()
	e.Cmds["drop-response"] = fault.command()
	e.Cmds["calls"] = calls.command()
	e.Cmds["private-workdir"] = script.Command(script.CmdUsage{Summary: "secure this script's credential parent"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 0 {
			return nil, script.ErrUsage
		}
		return nil, os.Chmod(s.Getwd(), 0700)
	})
	e.Cmds["json-field"] = retentionJSONField()
	e.Cmds["await-file"] = awaitFile()
	e.Cmds["graph-file"] = workflowGraphFile(t)
	e.Cmds["await-workflow"] = awaitWorkflow(exe, env)
	e.Cmds["event-count"] = workflowEventCount()
	e.Cmds["same-graph"] = sameWorkflowGraph()
	e.Cmds["same-input"] = sameWorkflowInput()
	vars := []string{"SKYGO_ENDPOINT=" + counted.URL, "PROXY=" + faulted.URL, "DATA=" + data, "AUTH=" + authFile}
	if old != "" {
		previous := &serverProcess{exe: old, address: freeAddress(t), data: t.TempDir()}
		if err := os.Chmod(previous.data, 0700); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(workflowServerCleanup(t, previous))
		oldCalls := &workflowCalls{paths: make(map[string]int)}
		oldServer := httptest.NewServer(oldCalls.wrap(httputil.NewSingleHostReverseProxy(&url.URL{Scheme: "http", Host: previous.address})))
		t.Cleanup(oldServer.Close)
		e.Cmds["old-server"] = previous.command()
		e.Cmds["old-calls"] = oldCalls.command()
		vars = append(vars, "OLD_ENDPOINT="+oldServer.URL)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	scripttest.Test(t, ctx, e, vars, file)
}

// workflowServerCleanup stops a daemon that its script left running. Every
// script stops its daemons, so one still running is reported, together with
// any stop error, instead of being discarded. The daemon's output is not
// reported: it may name credential files.
func workflowServerCleanup(t *testing.T, p *serverProcess) func() {
	return func() {
		p.mu.Lock()
		running := p.cmd != nil
		p.mu.Unlock()
		if !running {
			return
		}
		_, _, err := p.stop(os.Interrupt)
		t.Errorf("%s daemon was still running after its script; stop: %v", filepath.Base(p.exe), err)
	}
}

// workflowCalls counts requests forwarded to one daemon, by RPC path.
type workflowCalls struct {
	mu    sync.Mutex
	paths map[string]int
}

func (c *workflowCalls) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		c.paths[r.URL.Path]++
		c.mu.Unlock()
		next.ServeHTTP(w, r)
	})
}

func (c *workflowCalls) command() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "check an actual forwarded RPC count; job-submissions sums every ControlService Submit method", Args: "Service/Method|job-submissions|all count"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		want, err := strconv.Atoi(args[1])
		if err != nil {
			return nil, err
		}
		got := 0
		c.mu.Lock()
		for path, n := range c.paths {
			if args[0] == "all" || args[0] == "job-submissions" && strings.HasPrefix(path, "/skygo.v1.ControlService/Submit") || path == "/skygo.v1."+args[0] {
				got += n
			}
		}
		c.mu.Unlock()
		if got != want {
			return nil, fmt.Errorf("%s calls=%d want=%d", args[0], got, want)
		}
		return nil, nil
	})
}

// workflowGraphFile writes a graph, or a literal expected graph, from a
// template. It replaces only @WORKLOAD@, @DIR@ and, given a Ref file,
// @REF_ID@, @REF_SHA256@ and @REF_SIZE@; any other placeholder is an error.
func workflowGraphFile(t *testing.T) script.Cmd {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	placeholder := regexp.MustCompile(`@[A-Z0-9_]+@`)
	return script.Command(script.CmdUsage{Summary: "write a graph or expected graph that runs TestWorkflowWorkload", Args: "template out [ref.json]"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 && len(args) != 3 {
			return nil, script.ErrUsage
		}
		data, err := os.ReadFile(s.Path(args[0]))
		if err != nil {
			return nil, err
		}
		pairs := []string{"@WORKLOAD@", exe, "@DIR@", s.Getwd()}
		if len(args) == 3 {
			ref, err := readArtifactRef(s.Path(args[2]))
			if err != nil {
				return nil, err
			}
			pairs = append(pairs, "@REF_ID@", ref.ID, "@REF_SHA256@", ref.SHA256, "@REF_SIZE@", strconv.FormatInt(ref.Size, 10))
		}
		text := strings.NewReplacer(pairs...).Replace(string(data))
		if m := placeholder.FindString(text); m != "" {
			return nil, fmt.Errorf("unresolved placeholder %s", m)
		}
		return nil, os.WriteFile(s.Path(args[1]), []byte(text), 0600)
	})
}

// awaitWorkflow polls the actual CLI until a node's bound job reaches state,
// or its disposition is state. A bound job in another terminal state fails
// at once.
func awaitWorkflow(exe string, env []string) script.Cmd {
	return script.Command(script.CmdUsage{Summary: "wait for an actual node state or disposition", Args: "token-file id node state"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 4 {
			return nil, script.ErrUsage
		}
		want := strings.ToUpper(args[3])
		ctx, cancel := context.WithTimeout(s.Context(), 30*time.Second)
		defer cancel()
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			cmd := exec.CommandContext(ctx, exe, "--token-file", s.Path(args[0]), "--json", "workflow", "get", args[1])
			cmd.Dir = s.Getwd()
			cmd.Env = append(s.Environ(), env...)
			var diag bytes.Buffer
			cmd.Stderr = &diag
			out, err := cmd.Output()
			if err != nil {
				return nil, fmt.Errorf("workflow get: %v: %s", err, diag.String())
			}
			var w v1.Workflow
			if err := protojson.Unmarshal(out, &w); err != nil {
				return nil, err
			}
			var node *v1.WorkflowNodeStatus
			for _, n := range w.Nodes {
				if n.GetName() == args[2] {
					node = n
				}
			}
			if node == nil {
				return nil, fmt.Errorf("workflow has no node %q", args[2])
			}
			if node.Disposition.String() == "WORKFLOW_DISPOSITION_"+want || node.JobId != "" && node.ChildState.String() == "JOB_STATE_"+want {
				return func(*script.State) (string, string, error) { return string(out), "", nil }, nil
			}
			if node.JobId != "" && terminal(node.ChildState) {
				return nil, fmt.Errorf("node %s job is %s; want %s", args[2], state(node.ChildState), args[3])
			}
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("node %s: %w", args[2], ctx.Err())
			case <-tick.C:
			}
		}
	})
}

func workflowEventCount() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "count actual workload starts", Args: "count"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		want, err := strconv.Atoi(args[0])
		if err != nil {
			return nil, err
		}
		data, err := os.ReadFile(s.Path("starts"))
		if errors.Is(err, os.ErrNotExist) {
			data = nil
		} else if err != nil {
			return nil, err
		}
		if got := bytes.Count(data, []byte("start ")); got != want {
			return nil, fmt.Errorf("actual starts=%d want=%d:\n%s", got, want, data)
		}
		return nil, nil
	})
}

// sameWorkflowGraph compares a literal expected graph, written in the
// script as protojson, with the graph in an actual workflow response, field
// for field. The expectation does not pass through the CLI's graph parser or
// task conversion, so a field those lose or change cannot match.
func sameWorkflowGraph() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "compare a literal expected graph with an actual workflow response", Args: "expected-graph.json workflow.json"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		var want v1.WorkflowGraph
		var w v1.Workflow
		for i, m := range []proto.Message{&want, &w} {
			data, err := os.ReadFile(s.Path(args[i]))
			if err != nil {
				return nil, err
			}
			if err := protojson.Unmarshal(data, m); err != nil {
				return nil, fmt.Errorf("%s: %w", args[i], err)
			}
		}
		if len(want.Nodes) == 0 {
			return nil, errors.New("expected graph has no nodes")
		}
		if !proto.Equal(&want, w.Graph) {
			return nil, fmt.Errorf("returned graph differs:\nwant %v\ngot  %v", &want, w.Graph)
		}
		return nil, nil
	})
}

// sameWorkflowInput checks that a node's resolved input is exactly the
// producer job's available result and the consumer job's actual workdir.
func sameWorkflowInput() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "compare a node input with actual producer and consumer jobs", Args: "workflow.json node producer.json consumer.json"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 4 {
			return nil, script.ErrUsage
		}
		var w v1.Workflow
		var producer, consumer v1.Job
		for i, m := range map[int]proto.Message{0: &w, 2: &producer, 3: &consumer} {
			data, err := os.ReadFile(s.Path(args[i]))
			if err != nil {
				return nil, err
			}
			if err := protojson.Unmarshal(data, m); err != nil {
				return nil, err
			}
		}
		var input *v1.ArtifactRef
		for _, n := range w.Nodes {
			if n.GetName() == args[1] {
				input = n.GetInput()
			}
		}
		if input == nil {
			return nil, fmt.Errorf("node %q has no resolved input", args[1])
		}
		result := producer.GetResult()
		if result.GetState() != v1.ArtifactResultState_ARTIFACT_RESULT_STATE_AVAILABLE || result.GetReference() == nil {
			return nil, errors.New("producer has no available result")
		}
		if !proto.Equal(input, result.Reference) {
			return nil, fmt.Errorf("input %v differs from producer result %v", input, result.Reference)
		}
		if !proto.Equal(input, consumer.GetTask().GetWorkdir()) {
			return nil, fmt.Errorf("input %v differs from consumer workdir %v", input, consumer.GetTask().GetWorkdir())
		}
		return nil, nil
	})
}
