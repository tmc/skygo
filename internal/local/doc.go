// Package local runs programs in private process groups on Linux and macOS.
// Cancellation kills the group; programs that create new sessions can escape it.
// Without durable execution, a restarted backend reports old executions as
// unknown and never respawns them.
// Default execution retains its leader until output grace and group cleanup
// finish, then reaps once. Close bounds its wait for completion; blocked
// filesystem output can outlive that wait and is reported as cleanup uncertainty.
// Retained logs survive restart. Abrupt server death may leave running processes;
// recovery never signals an old PID because it may have been reused.
//
// With EnableDurable, plain jobs (no workspace, no retry policy) instead run
// under a detached supervisor: the same binary re-executed with
// SupervisorCommand. The supervisor is the only owner of the workload. It
// launches it at most once, reaps it, writes its log and an authoritative
// terminal status, and survives the server's exit or SIGKILL. A restarted
// backend reattaches by verifying the supervisor's PID, kernel start time and
// boot identity; a PID alone never counts. The server never signals a durable
// workload; cancellation is a durable request the supervisor carries out.
// Success requires the original leader's Wait, complete output and observed
// absence of its cooperative group. Unknown group closure keeps its reservation.
// A durable start that no supervisor claimed is fenced so it can never launch
// later, and is reported as never started. A supervisor that vanished without
// a terminal status leaves its job unknown, and it is never re-run.
//
// Finite retries require Linux pidfds (Linux 5.3 or newer) and use a separate path: retain the unreaped leader, stop
// its process group, reap once, then require group absence and combined-output
// EOF before reporting stopped evidence. Cancellation, timeouts and uncertain
// cleanup never produce that evidence. Descendants that change process groups
// are outside this cooperative boundary. Darwin retry admission is unsupported
// until its nonreaping wait and lifecycle have been independently qualified.
//
// CPU, memory and accelerator requests are admission accounting, not limits.
// Check compares them with the host (runtime.NumCPU, installed memory, and
// nvidia-smi for GPUs); Start reserves them atomically across this Backend's
// executions and selects GPUs by UUID. Reservations are released only after
// positive closure of the execution's processes. Selected GPUs are exported
// in CUDA_VISIBLE_DEVICES and NVIDIA_VISIBLE_DEVICES, overriding inherited or
// task values; that steers cooperating CUDA processes and is not cgroup or
// device-file isolation. Other programs on the host are not accounted.
package local
