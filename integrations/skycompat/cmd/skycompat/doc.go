// Skycompat converts selected SkyPilot inputs to skygo inputs.
//
// Usage:
//
//	skycompat import --shell PATH [flags] TASK.yaml
//
// Import reads one SkyPilot task YAML document, or standard input for "-",
// and writes one skygo task file to standard output for `skygo plan -f` and
// `skygo run -f`. For every recognized source field it writes one line
// PATH<TAB>STATUS<TAB>DETAIL to standard error, where STATUS is lowered,
// default or dropped. A field it cannot represent is refused: standard
// output stays empty, standard error carries one diagnostic, and the exit
// status is 1. Usage errors exit with status 2.
//
// Run and setup text run as the argv [PATH, "-c", TEXT] with the explicit
// --shell. A task with workdir needs --workdir-ref naming the reference file
// `skygo artifacts upload` writes; the workdir path is never read. Import
// reads only its explicit inputs, never the environment, and makes no
// network connections. See docs/import.md for the mapping and its
// differences from upstream execution.
package main
