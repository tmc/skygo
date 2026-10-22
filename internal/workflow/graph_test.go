package workflow_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/workflow"
)

var (
	refID     = strings.Repeat("a", 32)
	refSHA256 = strings.Repeat("b", 64)
	refJSON   = `{"id":"` + refID + `","sha256":"` + refSHA256 + `","size":1024}`
)

// workdirTask returns a task with a resolved Workdir. The workflow package
// and its tests import only the standard library and backend, so the Ref is
// decoded rather than constructed.
func workdirTask(argv ...string) backend.Task {
	var t backend.Task
	if err := json.Unmarshal([]byte(`{"workdir":`+refJSON+`}`), &t); err != nil {
		panic(err)
	}
	t.Argv = argv
	return t
}

func task(argv ...string) backend.Task { return backend.Task{Argv: argv} }

func node(name string, t backend.Task, deps ...string) workflow.Node {
	return workflow.Node{Name: name, Task: t, DependsOn: deps}
}

func input(n workflow.Node, producer string) workflow.Node {
	n.Input = &workflow.Input{Node: producer, Slot: "result"}
	return n
}

func graph(nodes ...workflow.Node) workflow.Graph { return workflow.Graph{Nodes: nodes} }

func TestCanonicalVectors(t *testing.T) {
	producer := workdirTask("./build")
	producer.Env = map[string]string{"Z": "1", "A": "2"}
	producer.Retry = &backend.RetryPolicy{MaxAttempts: 3, ExitCodes: []int32{7, 2, 7}}
	producer.Setup = []string{"./prep"}
	producer.Outputs = []string{"out"}
	consumer := task("./use")
	consumer.Setup = []string{"./next"}
	consumer.Outputs = []string{"final"}
	consumer.DataMounts = []backend.DataMount{{Reference: "z", Destination: "/z", ReadOnly: true}, {Reference: "a", Destination: "/a"}}

	for _, test := range []struct {
		name string
		g    workflow.Graph
		want string
	}{
		{
			"single",
			graph(node("a", task("echo", "hi"))),
			`{"nodes":[{"name":"a","task":{"argv":["echo","hi"]}}]}`,
		},
		{
			"nil argv",
			graph(node("a", backend.Task{})),
			`{"nodes":[{"name":"a","task":{"argv":null}}]}`,
		},
		{
			"node order retained and dependencies sorted",
			graph(node("c", task("c")), node("a", task("a")), node("b", task("b"), "c", "a")),
			`{"nodes":[{"name":"c","task":{"argv":["c"]}},{"name":"a","task":{"argv":["a"]}},{"name":"b","task":{"argv":["b"]},"depends_on":["a","c"]}]}`,
		},
		{
			"empty dependency list omitted",
			graph(workflow.Node{Name: "a", Task: task("a"), DependsOn: []string{}}),
			`{"nodes":[{"name":"a","task":{"argv":["a"]}}]}`,
		},
		{
			"dependencies sort bytewise",
			graph(node("b", task("b")), node("B", task("B")), node("_", task("_")), node("x", task("x"), "b", "_", "B")),
			`{"nodes":[{"name":"b","task":{"argv":["b"]}},{"name":"B","task":{"argv":["B"]}},{"name":"_","task":{"argv":["_"]}},{"name":"x","task":{"argv":["x"]},"depends_on":["B","_","b"]}]}`,
		},
		{
			"task bytes unchanged",
			graph(node("a", task("<a>&b"))),
			`{"nodes":[{"name":"a","task":{"argv":["\u003ca\u003e\u0026b"]}}]}`,
		},
		{
			"input chain",
			graph(node("a", producer), input(node("b", consumer, "a"), "a")),
			`{"nodes":[{"name":"a","task":{"argv":["./build"],"env":{"A":"2","Z":"1"},"retry":{"max_attempts":3,"delay_seconds":0,"exit_codes":[7,2,7]},` +
				`"workdir":` + refJSON + `,"setup":["./prep"],"outputs":["out"]}},` +
				`{"name":"b","task":{"argv":["./use"],"setup":["./next"],"outputs":["final"],"data_mounts":[{"reference":"z","destination":"/z","read_only":true},{"reference":"a","destination":"/a","read_only":false}]},` +
				`"depends_on":["a"],"input":{"node":"a","slot":"result"}}]}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := workflow.Canonical(test.g)
			if err != nil {
				t.Fatalf("Canonical() error = %v", err)
			}
			if string(got) != test.want {
				t.Fatalf("Canonical() =\n%s\nwant\n%s", got, test.want)
			}
			if err := workflow.Validate(test.g); err != nil {
				t.Fatalf("Validate() = %v", err)
			}
		})
	}
}

// TestCanonicalTaskBytes checks that each node's task encodes exactly as
// backend.TaskBytes, without normalization.
func TestCanonicalTaskBytes(t *testing.T) {
	a := workdirTask("<run>")
	a.Retry = &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{3, 1}}
	a.Outputs = []string{"out"}
	a.TargetLabels = map[string]string{"b": "2", "a": "1"}
	b := task("next")
	b.Env = map[string]string{"K": "&"}
	g := graph(node("a", a), input(node("b", b, "a"), "a"))
	data, err := workflow.Canonical(g)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Nodes []struct {
			Task json.RawMessage `json:"task"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	for i, n := range g.Nodes {
		want, err := backend.TaskBytes(n.Task)
		if err != nil {
			t.Fatal(err)
		}
		if string(decoded.Nodes[i].Task) != string(want) {
			t.Errorf("node %d task = %s; want TaskBytes %s", i, decoded.Nodes[i].Task, want)
		}
	}
}

func TestCanonicalOrder(t *testing.T) {
	a := graph(node("a", task("a")), node("b", task("b")), node("c", task("c"), "a", "b"))
	b := graph(node("a", task("a")), node("b", task("b")), node("c", task("c"), "b", "a"))
	swapped := graph(node("b", task("b")), node("a", task("a")), node("c", task("c"), "a", "b"))
	ca, err := workflow.Canonical(a)
	if err != nil {
		t.Fatal(err)
	}
	cb, err := workflow.Canonical(b)
	if err != nil {
		t.Fatal(err)
	}
	cs, err := workflow.Canonical(swapped)
	if err != nil {
		t.Fatal(err)
	}
	if string(ca) != string(cb) {
		t.Errorf("dependency order changed encoding:\n%s\n%s", ca, cb)
	}
	if string(ca) == string(cs) {
		t.Errorf("node order not retained: %s", cs)
	}
}

func TestValidate(t *testing.T) {
	producer := workdirTask("produce")
	producer.Outputs = []string{"out"}
	noOutputs := workdirTask("produce")
	withRef := workdirTask("consume")
	withDir := task("consume")
	withDir.Directory = "/work"
	badSetup := task("consume")
	badSetup.Setup = []string{""}
	overlap := task("consume")
	overlap.Outputs = []string{"a", "a/b"}
	setupOnly := task("consume")
	setupOnly.Setup = []string{"prepare"}
	outputsOnly := task("consume")
	outputsOnly.Outputs = []string{"out"}
	middle := task("middle")
	middle.Outputs = []string{"next"}
	badRef := workdirTask("x")
	badRef.Workdir.ID = "bad"
	badRetry := task("x")
	badRetry.Retry = &backend.RetryPolicy{MaxAttempts: 11, ExitCodes: []int32{1}}
	retry := task("x")
	retry.Retry = &backend.RetryPolicy{MaxAttempts: 2}

	for _, test := range []struct {
		name string
		g    workflow.Graph
		want string // empty means valid
	}{
		{"zero graph", workflow.Graph{}, "graph has no nodes"},
		{"empty nodes", workflow.Graph{Nodes: []workflow.Node{}}, "graph has no nodes"},
		{"name grammar", graph(node("A_z-9", task("x"))), ""},
		{"name 64 bytes", graph(node(strings.Repeat("n", 64), task("x"))), ""},
		{"name 65 bytes", graph(node(strings.Repeat("n", 65), task("x"))), "node 0: invalid name"},
		{"empty name", graph(node("", task("x"))), "node 0: invalid name"},
		{"name space", graph(node("a", task("x")), node("a b", task("x"))), "node 1: invalid name"},
		{"name dot", graph(node("a.b", task("x"))), "node 0: invalid name"},
		{"name slash", graph(node("a/b", task("x"))), "node 0: invalid name"},
		{"name non-ASCII", graph(node("é", task("x"))), "node 0: invalid name"},
		{"duplicate name", graph(node("a", task("x")), node("a", task("y"))), `node "a": duplicate name`},
		{"names case-sensitive", graph(node("a", task("x")), node("A", task("y"), "a")), ""},
		{"duplicate dependency", graph(node("a", task("x")), node("b", task("y"), "a", "a")), `node "b": duplicate dependency "a"`},
		{"missing dependency", graph(node("a", task("x"), "z")), `node "a": missing dependency "z"`},
		{"invalid dependency name", graph(node("a", task("x"), "a b")), `node "a": invalid dependency name`},
		{"self dependency", graph(node("a", task("x"), "a")), `node "a": depends on itself`},
		{"two-node cycle", graph(node("a", task("x"), "b"), node("b", task("y"), "a")), `node "a": dependency cycle`},
		{"cycle downstream root", graph(
			node("x", task("x"), "a"),
			node("a", task("a"), "c"),
			node("b", task("b"), "a"),
			node("c", task("c"), "b"),
		), `node "a": dependency cycle`},
		{"cycle after acyclic prefix", graph(
			node("a", task("a")),
			node("b", task("b"), "a"),
			node("c", task("c"), "d", "b"),
			node("d", task("d"), "c"),
		), `node "c": dependency cycle`},
		{"diamond", graph(
			node("a", task("a")),
			node("b", task("b"), "a"),
			node("c", task("c"), "a"),
			node("d", task("d"), "c", "b"),
		), ""},
		{"input", graph(node("p", producer), input(node("c", task("consume"), "p"), "p")), ""},
		{"input with other dependencies", graph(node("p", producer), node("q", task("q")), input(node("c", task("consume"), "q", "p"), "p")), ""},
		{"input chain", graph(node("p", producer), input(node("m", middle, "p"), "p"), input(node("c", setupOnly, "m"), "m")), ""},
		{"input outputs", graph(node("p", producer), input(node("c", outputsOnly, "p"), "p")), ""},
		{"input slot empty", graph(node("p", producer), workflow.Node{Name: "c", Task: task("c"), DependsOn: []string{"p"}, Input: &workflow.Input{Node: "p"}}), `node "c": input slot must be "result"`},
		{"input slot named output", graph(node("p", producer), workflow.Node{Name: "c", Task: task("c"), DependsOn: []string{"p"}, Input: &workflow.Input{Node: "p", Slot: "out"}}), `node "c": input slot must be "result"`},
		{"input without dependency", graph(node("p", producer), input(node("c", task("c")), "p")), `node "c": input node must be a dependency`},
		{"input on other dependency", graph(node("p", producer), node("q", task("q")), input(node("c", task("c"), "q"), "p")), `node "c": input node must be a dependency`},
		{"input missing node", graph(node("p", producer), input(node("c", task("c"), "p"), "z")), `node "c": input node must be a dependency`},
		{"input self", graph(input(node("c", task("c")), "c")), `node "c": input node must be a dependency`},
		{"input with workdir", graph(node("p", producer), input(node("c", withRef, "p"), "p")), `node "c": input conflicts with workdir`},
		{"input producer without outputs", graph(node("p", noOutputs), input(node("c", task("c"), "p"), "p")), `node "c": input node "p" declares no outputs`},
		{"input directory", graph(node("p", producer), input(node("c", withDir, "p"), "p")), `node "c": workspace template requires an empty directory`},
		{"input setup program", graph(node("p", producer), input(node("c", badSetup, "p"), "p")), `node "c": setup requires a program`},
		{"input overlapping outputs", graph(node("p", producer), input(node("c", overlap, "p"), "p")), `node "c": overlapping output roots`},
		{"setup without input", graph(node("p", producer), node("c", setupOnly, "p")), `node "c": setup and outputs require workdir`},
		{"outputs without input", graph(node("c", outputsOnly)), `node "c": setup and outputs require workdir`},
		{"producer without workdir", graph(node("p", outputsOnly), input(node("c", task("c"), "p"), "p")), `node "p": setup and outputs require workdir`},
		{"invalid workdir", graph(node("a", badRef)), `node "a": workdir: invalid artifact id`},
		{"directory without input", graph(node("a", withDir)), ""},
		{"invalid retry", graph(node("a", badRetry)), `node "a": retry max_attempts must be between 2 and 10`},
		{"retry without codes", graph(node("a", retry)), `node "a": retry exit_codes must contain 1..32 entries`},
	} {
		t.Run(test.name, func(t *testing.T) {
			verr := workflow.Validate(test.g)
			data, cerr := workflow.Canonical(test.g)
			if test.want == "" {
				if verr != nil || cerr != nil {
					t.Fatalf("Validate() = %v, Canonical() error = %v; want valid", verr, cerr)
				}
				return
			}
			if verr == nil || verr.Error() != test.want {
				t.Fatalf("Validate() = %v; want %q", verr, test.want)
			}
			if cerr == nil || cerr.Error() != verr.Error() {
				t.Fatalf("Canonical() error = %v; want %q", cerr, verr)
			}
			if data != nil {
				t.Fatalf("Canonical() = %s on error", data)
			}
		})
	}
}

// fanGraph returns n nodes where node i depends on earlier nodes, filling
// edges dependency edges in total.
func fanGraph(n, edges int) workflow.Graph {
	var g workflow.Graph
	for i := 0; i < n; i++ {
		var deps []string
		for j := 0; j < i && edges > 0; j++ {
			deps = append(deps, fmt.Sprint("n", j))
			edges--
		}
		g.Nodes = append(g.Nodes, node(fmt.Sprint("n", i), task("x"), deps...))
	}
	if edges > 0 {
		panic("too few nodes for edges")
	}
	return g
}

func TestLimits(t *testing.T) {
	for _, test := range []struct {
		name         string
		nodes, edges int
		want         string
	}{
		{"1 node", 1, 0, ""},
		{"64 nodes", 64, 0, ""},
		{"65 nodes", 65, 0, "graph exceeds 64 nodes"},
		{"256 edges", 64, 256, ""},
		{"257 edges", 64, 257, "graph exceeds 256 dependency edges"},
		{"257 edges few nodes", 24, 257, "graph exceeds 256 dependency edges"},
		{"max edges", 64, 64 * 63 / 2, "graph exceeds 256 dependency edges"},
	} {
		t.Run(test.name, func(t *testing.T) {
			g := fanGraph(test.nodes, test.edges)
			data, err := workflow.Canonical(g)
			if test.want != "" {
				if err == nil || err.Error() != test.want {
					t.Fatalf("Canonical() error = %v; want %q", err, test.want)
				}
				return
			}
			if err != nil {
				t.Fatalf("Canonical() error = %v", err)
			}
			var decoded workflow.Graph
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatal(err)
			}
			edges := 0
			for _, n := range decoded.Nodes {
				edges += len(n.DependsOn)
			}
			if len(decoded.Nodes) != test.nodes || edges != test.edges {
				t.Fatalf("decoded %d nodes, %d edges; want %d, %d", len(decoded.Nodes), edges, test.nodes, test.edges)
			}
		})
	}

	// An Input names one of its node's dependencies; it is not a second edge.
	t.Run("256 edges and input", func(t *testing.T) {
		g := fanGraph(64, 256)
		g.Nodes[0].Task = workdirTask("x")
		g.Nodes[0].Task.Outputs = []string{"out"}
		g.Nodes[1] = input(g.Nodes[1], "n0") // n1 depends on n0
		if err := workflow.Validate(g); err != nil {
			t.Fatalf("Validate() = %v", err)
		}
	})
}

// TestCycleOrder checks that cycle search follows sorted, not declared,
// dependency order when the two reach different cycles.
func TestCycleOrder(t *testing.T) {
	cyclic := func(deps ...string) workflow.Graph {
		return graph(
			node("x", task("x"), deps...),
			node("q", task("q"), "q2"),
			node("q2", task("q2"), "q"),
			node("p", task("p"), "p2"),
			node("p2", task("p2"), "p"),
		)
	}
	want := `node "p": dependency cycle`
	for _, deps := range [][]string{{"q", "p"}, {"p", "q"}} {
		if err := workflow.Validate(cyclic(deps...)); err == nil || err.Error() != want {
			t.Errorf("Validate() with x depending on %q = %v; want %q", deps, err, want)
		}
	}
}

// TestCanonicalSize checks the bound against the actual encoding. The
// expected bytes are built by hand rather than with json.Marshal.
func TestCanonicalSize(t *testing.T) {
	const (
		limit  = 256 << 10
		prefix = `{"nodes":[{"name":"a","task":{"argv":["`
		suffix = `"]}}]}`
	)
	pad := limit - len(prefix) - len(suffix)
	exact := graph(node("a", task(strings.Repeat("x", pad))))
	got, err := workflow.Canonical(exact)
	if err != nil {
		t.Fatalf("Canonical() at limit: %v", err)
	}
	if want := prefix + strings.Repeat("x", pad) + suffix; string(got) != want || len(got) != limit {
		t.Fatalf("Canonical() at limit returned %d bytes; want %d", len(got), limit)
	}
	if err := workflow.Validate(exact); err != nil {
		t.Fatalf("Validate() at limit: %v", err)
	}

	for _, test := range []struct {
		name string
		arg  string
	}{
		{"one byte over", strings.Repeat("x", pad+1)},
		// "<" encodes as \u003c: 6 bytes each, far fewer raw bytes.
		{"escaping over", strings.Repeat("<", pad/6+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			g := graph(node("a", task(test.arg)))
			want := "canonical graph exceeds 262144 bytes"
			if _, err := workflow.Canonical(g); err == nil || err.Error() != want {
				t.Fatalf("Canonical() error = %v; want %q", err, want)
			}
			if err := workflow.Validate(g); err == nil || err.Error() != want {
				t.Fatalf("Validate() = %v; want %q", err, want)
			}
		})
	}
}

// cloneGraph returns an independent copy of g.
func cloneGraph(g workflow.Graph) workflow.Graph {
	out := workflow.Graph{Nodes: slices.Clone(g.Nodes)}
	for i, n := range out.Nodes {
		out.Nodes[i].Task = backend.CloneTask(n.Task)
		out.Nodes[i].DependsOn = slices.Clone(n.DependsOn)
		if n.Input != nil {
			in := *n.Input
			out.Nodes[i].Input = &in
		}
	}
	return out
}

func TestNoCallerMutation(t *testing.T) {
	shared := []string{"b", "a"} // aliased by two nodes
	env := map[string]string{"K": "v"}
	retry := &backend.RetryPolicy{MaxAttempts: 3, ExitCodes: []int32{9, 2, 9}}
	p := workdirTask("produce")
	p.Env = env
	p.Retry = retry
	p.Outputs = []string{"out"}
	c := task("consume")
	c.Env = env
	c.Retry = retry
	in := &workflow.Input{Node: "a", Slot: "result"}
	g := graph(
		node("a", p),
		node("b", task("b")),
		workflow.Node{Name: "c", Task: c, DependsOn: shared, Input: in},
		workflow.Node{Name: "d", Task: task("d"), DependsOn: shared[:2:2]},
	)
	want := cloneGraph(g)

	if err := workflow.Validate(g); err != nil {
		t.Fatal(err)
	}
	data, err := workflow.Canonical(g)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(g, want) {
		t.Fatalf("graph changed:\n%+v\nwant\n%+v", g, want)
	}
	if g.Nodes[2].Input != in || g.Nodes[0].Task.Retry != retry || g.Nodes[2].Task.Retry != retry {
		t.Fatal("graph pointers changed")
	}
	if !slices.Equal(shared, []string{"b", "a"}) {
		t.Fatalf("shared dependencies = %q; want unsorted [b a]", shared)
	}
	if retry.DelaySeconds != 0 || !slices.Equal(retry.ExitCodes, []int32{9, 2, 9}) {
		t.Fatalf("retry normalized in place: %+v", *retry)
	}

	// The result is detached: later caller changes do not reach it, and
	// changes to it do not reach later results.
	saved := string(data)
	shared[0] = "a"
	env["K"] = "changed"
	in.Slot = "other"
	retry.ExitCodes[0] = 1
	if string(data) != saved {
		t.Fatal("canonical bytes alias the graph")
	}
	shared[0], env["K"], in.Slot, retry.ExitCodes[0] = "b", "v", "result", 9
	data[0] = '['
	again, err := workflow.Canonical(g)
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != saved {
		t.Fatalf("Canonical() = %s; want %s", again, saved)
	}
}

// TestConcurrentCanonical shares maps and slices across goroutines. Under
// -race it detects any write to caller state; otherwise it checks results.
func TestConcurrentCanonical(t *testing.T) {
	p := workdirTask("produce")
	p.Env = map[string]string{"K": "v"}
	p.Retry = &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{3, 1}}
	p.Outputs = []string{"out"}
	shared := []string{"q", "p"}
	g := graph(node("p", p), node("q", task("q")), input(node("c", task("c"), shared...), "p"))
	want, err := workflow.Canonical(g)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := workflow.Canonical(g)
			if err == nil && string(got) != string(want) {
				err = fmt.Errorf("Canonical() = %s; want %s", got, want)
			}
			if err == nil {
				err = workflow.Validate(g)
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
}
