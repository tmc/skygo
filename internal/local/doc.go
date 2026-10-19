// Package local runs programs in private process groups on Linux and macOS.
// Cancellation kills the group; programs that create new sessions can escape it.
// A restarted backend reports old executions as unknown and never respawns them.
// Default execution retains its leader until output grace and group cleanup
// finish, then reaps once. Close bounds its wait for completion; blocked
// filesystem output can outlive that wait and is reported as cleanup uncertainty.
// Retained logs survive restart. Abrupt server death may leave running processes;
// recovery never signals an old PID because it may have been reused.
//
// Finite retries require Linux pidfds (Linux 5.3 or newer) and use a separate path: retain the unreaped leader, stop
// its process group, reap once, then require group absence and combined-output
// EOF before reporting stopped evidence. Cancellation, timeouts and uncertain
// cleanup never produce that evidence. Descendants that change process groups
// are outside this cooperative boundary. Darwin retry admission is unsupported
// until its nonreaping wait and lifecycle have been independently qualified.
package local
