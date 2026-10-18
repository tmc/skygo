// Package local runs programs in private process groups on Linux and macOS.
// Cancellation kills the group; programs that create new sessions can escape it.
// A restarted backend reports old executions as unknown and never respawns them.
// Retained logs survive restart. Abrupt server death may leave running processes;
// recovery never signals an old PID because it may have been reused.
package local
