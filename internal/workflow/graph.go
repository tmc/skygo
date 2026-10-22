package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/tmc/skygo/internal/backend"
)

const (
	maxNodes          = 64
	maxEdges          = 256
	maxCanonicalBytes = 256 << 10

	// resultSlot is the only slot a node exposes: its aggregate output archive.
	resultSlot = "result"
)

// Graph is 1..64 named task nodes with at most 256 dependency edges and at
// most 256 KiB of canonical JSON. The zero Graph is invalid.
type Graph struct {
	Nodes []Node `json:"nodes" yaml:"nodes"`
}

// Node is one task in a Graph. Name is 1..64 ASCII letters, digits, '_' or '-'
// and is unique within the graph. DependsOn lists distinct other node names.
type Node struct {
	Name      string       `json:"name" yaml:"name"`
	Task      backend.Task `json:"task" yaml:"task"`
	DependsOn []string     `json:"depends_on,omitempty" yaml:"depends_on,omitempty"`
	Input     *Input       `json:"input,omitempty" yaml:"input,omitempty"`
}

// Input selects a producer's result as the node's workdir. Node must also be
// listed in DependsOn and must declare Task.Outputs; Slot must be "result".
// A node with an Input must not set Task.Workdir.
type Input struct {
	Node string `json:"node" yaml:"node"`
	Slot string `json:"slot" yaml:"slot"`
}

// Validate reports whether g is a valid graph.
func Validate(g Graph) error {
	_, err := canonical(g)
	return err
}

// Canonical returns the canonical JSON encoding of g, or an error if g is
// invalid. Nodes keep their declared order and tasks encode exactly as
// backend.TaskBytes; each DependsOn list is sorted.
func Canonical(g Graph) ([]byte, error) {
	return canonical(g)
}

// canonical checks g and encodes a copy with sorted dependency lists.
func canonical(g Graph) ([]byte, error) {
	if len(g.Nodes) == 0 {
		return nil, errors.New("graph has no nodes")
	}
	if len(g.Nodes) > maxNodes {
		return nil, fmt.Errorf("graph exceeds %d nodes", maxNodes)
	}
	edges := 0
	for _, n := range g.Nodes {
		edges += len(n.DependsOn)
		if edges > maxEdges {
			return nil, fmt.Errorf("graph exceeds %d dependency edges", maxEdges)
		}
	}
	index := make(map[string]int, len(g.Nodes))
	for i, n := range g.Nodes {
		if !validName(n.Name) {
			return nil, fmt.Errorf("node %d: invalid name", i)
		}
		if _, ok := index[n.Name]; ok {
			return nil, fmt.Errorf("node %q: duplicate name", n.Name)
		}
		index[n.Name] = i
	}

	out := Graph{Nodes: make([]Node, len(g.Nodes))}
	deps := make([][]int, len(g.Nodes))
	for i, n := range g.Nodes {
		c, err := checkNode(g.Nodes, index, n)
		if err != nil {
			return nil, fmt.Errorf("node %q: %w", n.Name, err)
		}
		out.Nodes[i] = c
		for _, d := range c.DependsOn {
			deps[i] = append(deps[i], index[d])
		}
	}
	if i, ok := cycle(deps); ok {
		return nil, fmt.Errorf("node %q: dependency cycle", g.Nodes[i].Name)
	}

	data, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("encode graph: %w", err)
	}
	if len(data) > maxCanonicalBytes {
		return nil, fmt.Errorf("canonical graph exceeds %d bytes", maxCanonicalBytes)
	}
	return data, nil
}

// checkNode checks n and returns a copy with a sorted DependsOn copy.
// The returned Task shares n.Task's maps and slices and must not be modified.
func checkNode(nodes []Node, index map[string]int, n Node) (Node, error) {
	deps := slices.Clone(n.DependsOn)
	slices.Sort(deps)
	for i, d := range deps {
		if i > 0 && d == deps[i-1] {
			return Node{}, fmt.Errorf("duplicate dependency %q", d)
		}
		if !validName(d) {
			return Node{}, errors.New("invalid dependency name")
		}
		if _, ok := index[d]; !ok {
			return Node{}, fmt.Errorf("missing dependency %q", d)
		}
		if d == n.Name {
			return Node{}, errors.New("depends on itself")
		}
	}
	if len(deps) == 0 {
		deps = nil
	}

	var in *Input
	if n.Input != nil {
		if n.Input.Slot != resultSlot {
			return Node{}, fmt.Errorf("input slot must be %q", resultSlot)
		}
		if _, ok := slices.BinarySearch(deps, n.Input.Node); !ok {
			return Node{}, errors.New("input node must be a dependency")
		}
		if n.Task.Workdir != nil {
			return Node{}, errors.New("input conflicts with workdir")
		}
		if len(nodes[index[n.Input.Node]].Task.Outputs) == 0 {
			return Node{}, fmt.Errorf("input node %q declares no outputs", n.Input.Node)
		}
		if err := backend.ValidateWorkspaceTemplate(n.Task); err != nil {
			return Node{}, err
		}
		v := *n.Input
		in = &v
	} else if err := backend.ValidateWorkspace(n.Task); err != nil {
		return Node{}, err
	}
	if _, err := backend.NormalizeRetry(n.Task.Retry); err != nil {
		return Node{}, err
	}
	return Node{Name: n.Name, Task: n.Task, DependsOn: deps, Input: in}, nil
}

// cycle reports a node on a dependency cycle. The depth-first search visits
// nodes in declared order and dependencies in sorted order, so the result is
// deterministic, and it visits each node and edge at most once.
func cycle(deps [][]int) (int, bool) {
	const (
		unvisited = iota
		visiting
		done
	)
	state := make([]int, len(deps))
	var visit func(int) (int, bool)
	visit = func(i int) (int, bool) {
		state[i] = visiting
		for _, j := range deps[i] {
			switch state[j] {
			case visiting:
				return j, true
			case unvisited:
				if k, ok := visit(j); ok {
					return k, true
				}
			}
		}
		state[i] = done
		return 0, false
	}
	for i := range deps {
		if state[i] == unvisited {
			if k, ok := visit(i); ok {
				return k, true
			}
		}
	}
	return 0, false
}

func validName(s string) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !('a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}
