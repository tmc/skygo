// Package taskyaml parses a bounded subset of SkyPilot task YAML at upstream
// commit 922a9f046c4a2714edf2b76fec3d1862dcd56ae3 (API 67).
//
// Parse reads one task; ParseDAG reads a name header and one task. Neither
// performs environment lookup, interpolation, filesystem access or execution.
// Returned scripts, references and provider names require separate semantic
// validation, capability checks and explicit shell permission before use.
// Parsing alone does not establish compatibility with upstream validation.
package taskyaml
