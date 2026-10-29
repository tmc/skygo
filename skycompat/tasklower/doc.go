// Package tasklower lowers a parsed SkyPilot task to a skygo task.
//
// [Lower] maps one [taskyaml.Document] to a [skyv1.Task] and reports one
// [Note] for every recognized source field: lowered, kept as an upstream
// default, or dropped by explicit request. A field it cannot represent is
// refused, never ignored. [Marshal] encodes the result as a skygo task file,
// which `skygo plan -f` and `skygo run -f` read.
//
// [CheckWorkspace] validates a declared workdir task before an artifact
// reference exists. It checks the same fields without returning an executable
// task. The caller obtains a real reference, then calls Lower; neither function
// reads a local directory or uploads its contents.
//
// Run and setup text become the argv [Options.Shell, "-c", TEXT]. Lowering
// never splits, interprets or executes the text, and it never reads the
// process environment or the filesystem. Planning, capability checks and
// submission remain with skygo.
//
// A lowered task runs on one node. Its environment is the source envs plus
// SKYPILOT_NUM_NODES (default "1", a source value is kept), SKYPILOT_NODE_RANK
// "0" and SKYPILOT_NUM_GPUS_PER_NODE, the requested accelerator count, or 0
// when no accelerator is requested or its name contains tpu, inferentia or
// trainium in any case, as upstream classifies accelerators. The count
// reports the request; it does not prove allocation or isolation. Setup sees
// the same environment as run, whereas upstream setup overwrites the node
// count without computing rank or GPU count. Setting SKYPILOT_NUM_NODES
// changes only the variable, never the number of nodes. Other upstream
// predefined variables, such as node IPs and cluster information, are not
// synthesized; their user-supplied values are kept.
package tasklower
