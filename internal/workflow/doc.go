// Package workflow defines a bounded, acyclic graph of backend tasks.
//
// A Graph lists named nodes in declared order. DependsOn names the nodes that
// must succeed first. An Input names one of those producers and its fixed
// "result" slot: the producer's aggregate output archive becomes the
// consumer's unresolved workdir.
//
// Validate checks graph shape, dependencies and snapshot templates. Canonical
// returns the canonical JSON encoding of a valid graph: node order and task
// bytes are preserved and each dependency list is sorted. Both share one
// checker and never modify the caller's graph.
//
// Validate is not task admission. It does not apply control-plane task rules,
// data policy, backend capabilities, capacity or authority, and a valid graph
// is neither durable nor scheduled.
package workflow
