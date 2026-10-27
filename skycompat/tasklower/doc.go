// Package tasklower lowers a parsed SkyPilot task to a skygo task.
//
// [Lower] maps one [taskyaml.Document] to a [skyv1.Task] and reports one
// [Note] for every recognized source field: lowered, kept as an upstream
// default, or dropped by explicit request. A field it cannot represent is
// refused, never ignored. [Marshal] encodes the result as a skygo task file,
// which `skygo plan -f` and `skygo run -f` read.
//
// Run and setup text become the argv [Options.Shell, "-c", TEXT]. Lowering
// never splits, interprets or executes the text, and it never reads the
// process environment or the filesystem. Planning, capability checks and
// submission remain with skygo.
package tasklower
