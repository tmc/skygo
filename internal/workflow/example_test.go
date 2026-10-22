package workflow_test

import (
	"encoding/json"
	"fmt"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/workflow"
)

func Example() {
	g := workflow.Graph{Nodes: []workflow.Node{
		{Name: "build", Task: backend.Task{Argv: []string{"make"}}},
		{Name: "test", Task: backend.Task{Argv: []string{"make", "test"}}, DependsOn: []string{"build"}},
	}}
	data, err := workflow.Canonical(g)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(data))
	// Output:
	// {"nodes":[{"name":"build","task":{"argv":["make"]}},{"name":"test","task":{"argv":["make","test"]},"depends_on":["build"]}]}
}

func ExampleValidate() {
	g := workflow.Graph{Nodes: []workflow.Node{
		{Name: "a", Task: backend.Task{Argv: []string{"a"}}, DependsOn: []string{"b"}},
		{Name: "b", Task: backend.Task{Argv: []string{"b"}}, DependsOn: []string{"a"}},
	}}
	fmt.Println(workflow.Validate(g))
	// Output: node "a": dependency cycle
}

func ExampleCanonical() {
	// The consumer reads the producer's result as its workdir. Only the
	// producer carries a resolved Ref; the consumer's input is a typed slot.
	var g workflow.Graph
	err := json.Unmarshal([]byte(`{"nodes": [
		{"name": "fetch", "task": {"argv": ["./fetch"], "outputs": ["data"],
			"workdir": {"id": "0123456789abcdef0123456789abcdef",
				"sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "size": 1024}}},
		{"name": "lint", "task": {"argv": ["./lint"]}},
		{"name": "train", "task": {"argv": ["./train"], "setup": ["./prepare"], "outputs": ["model"]},
			"depends_on": ["lint", "fetch"], "input": {"node": "fetch", "slot": "result"}}
	]}`), &g)
	if err != nil {
		fmt.Println(err)
		return
	}
	data, err := workflow.Canonical(g)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(data))
	// Output:
	// {"nodes":[{"name":"fetch","task":{"argv":["./fetch"],"workdir":{"id":"0123456789abcdef0123456789abcdef","sha256":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","size":1024},"outputs":["data"]}},{"name":"lint","task":{"argv":["./lint"]}},{"name":"train","task":{"argv":["./train"],"setup":["./prepare"],"outputs":["model"]},"depends_on":["fetch","lint"],"input":{"node":"fetch","slot":"result"}}]}
}
