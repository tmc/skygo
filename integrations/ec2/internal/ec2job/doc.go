// Package ec2job runs finite skygo workspace jobs on one already-owned,
// running EC2 VM through a public SSH agent session, and keeps each job's
// progress in a private journal directory.
//
// It never creates, starts, stops, restarts, replaces or terminates a VM:
// [VerifyVM] only reads the lifecycle state ec2ctl keeps and describes the
// owned instance afresh. It is a provider command's job client, not a
// skygo execution target, and claims no remote-data, retry or environment
// capability.
//
// # Flow
//
// A private [Config] binds the VM's instance, image (stated arm64 by the
// operator), the address kind and exact address an SSH session may use,
// the SSH keys and dedicated known_hosts file, the agent's source and
// target, and an operator-initialized installation. [Config.Task] reads
// canonical task JSON, or SkyPilot task YAML with the existing strict
// lowerer and an explicit infrastructure binding, and refuses what the VM
// cannot run before any effect. [Snapshot] freezes the workdir.
//
// [VerifyVM] checks the lifecycle record and a fresh owned description,
// including the selected address. [Create] then stores the frozen
// snapshot and a journal binding the configuration digest, the VM, the
// task and a new request ID, before any SSH connection. The caller opens
// an sshagent session to the verified address, and [Job.Attach] checks
// what it verified. [Job.Start] reads the server's capabilities and
// retention policy, records the request domain once, uploads the
// snapshot in 64 KiB chunks, submits it with SubmitWorkspaceJob once,
// observes the job until it ends and downloads and verifies its declared
// outputs.
//
// # Ambiguity
//
// Each effect is preceded by a durable save of what it may do, and each
// identity it returns (upload ID and reference, acknowledged offset, job
// ID, observed state, output) is saved before the next effect. A failed
// save stops all calls until a save succeeds.
//
//   - A submit whose reply is lost is replayed only by an explicit
//     [Job.Resume], with exactly the stored task, request ID, method and
//     request domain, while the job is wanted run and the domain is the
//     server's current lineage within its retry window. An expired or
//     changed domain is refused ([ErrDomainExpired]); it is never
//     refreshed and the request never gets a new ID.
//   - A known job is only observed ([Job.Refresh], [Job.Logs]) or
//     canceled, never submitted.
//   - A Begin whose reply is lost holds the job ([ErrHeld]): no second
//     Begin and no submit. A known upload is reconciled with
//     GetArtifactUpload.
//   - A submit reply with a valid job ID but an invalid state, or an
//     accepted task or target that is not exactly the one submitted, has
//     its ID stored and the job held: it is observed and canceled by that
//     ID, never run as this task and never submitted again.
//   - [Job.RequestCancel] latches the wish before any request. A job never
//     submitted is then over; a job whose submit is in doubt and whose ID
//     is unknown is held, because there is no read-only lookup by request
//     ID and a submit could create it. Every later cancel, of a reopened
//     journal too, reports such a hold again. [Job.Cancel]'s nil reply is
//     delivery only: the job is canceled only once it is seen CANCELED.
//   - An ended job's result is recorded only as the server states it:
//     available (downloaded), absent, expired, or held (not recorded). A
//     missing result, or one of an unknown state, records nothing.
//   - A wait that ends with the caller's context leaves the job running
//     and its outcome unknown ([ErrNotTerminal]); nothing stops it.
//
// [Job.CheckResume] reports, from the journal alone, a refusal Resume
// would make in any case (a hold, a requested cancel, a Begin in doubt,
// an expired submit in doubt), so that a caller can refuse before any
// VM, SSH or job request.
//
// Errors never quote server messages ([RemoteError]); the journal and
// [Status] hold no credential and no server text.
//
// # Cleanup
//
// A canceled job that was never submitted, and whose upload ID is known
// and its upload not finalized, may have its upload deleted: [Job.Cancel]
// stores that it may send CancelArtifactUpload for that ID, then sends it.
// The server marks the upload for deletion and its maintenance deletes it
// later, so a nil reply is recorded as delivered, not as deleted. A failed
// request, including one whose context ended, leaves the cleanup unknown;
// a later Cancel sends the same request for the same ID. Nothing deletes
// an upload a submit may own, a finalized reference, or an upload whose
// Begin is in doubt, and no expiry is taken as cleanup. A job's end and
// its upload's cleanup are separate facts in [Status].
//
// # Downloads
//
// A result download allocates one of the server's few download
// descriptors, which the server may keep even when the Begin fails, and
// which only a Close of its known ID releases. The journal therefore
// records the Begin before it is sent, bound to the result's reference
// and attempt; the returned ID before anything else; and the Close before
// it is sent. A Begin or Close whose outcome is unknown holds the job:
// no second Begin is sent and the Close is not sent again. The verified
// output is frozen as "output.tar" before the Close, and recorded as the
// job's output only after the server's release is recorded.
//
// # Logs
//
// [Job.Logs] copies the log from an explicit offset; [Job.NextLogs] from
// the job's log checkpoint, which is keyed to the job, its request domain
// and the log's attempt. A copy moves the checkpoint only when the caller
// commits it with [Job.CommitLogs] after delivering it, and only if the
// copy, every write and the stream's Close succeeded. Delivering and
// committing are separate operations, so a crash between them repeats
// bytes on the next copy: delivery is at least once, never exactly once.
// The checkpoint keeps no stream cursor.
//
// # Custody
//
// Every original file and stream is closed once, and a failed Close is
// never retried. A failed Close of a file in the job directory or of the
// configuration is reported as [ErrUnsettled], with the original file
// kept by its error, which the caller must keep. After the Close of a
// temporary journal write, or of a file the Job read, fails, that file is
// neither removed nor reused, the Job refuses every later call, and the
// directory stays locked until the process exits. A failed write, sync
// or rename whose file closed is a pending state ([ErrUnpersisted]),
// stored again before the next effect.
//
// # Job directory
//
// The directory and its files must belong to the effective user and grant
// no group or other access; symbolic links and multiply linked files are
// refused. It holds "lock", "job.json", the frozen "workdir.tar" and, once
// collected, "output.tar". Writers take a nonblocking exclusive flock on
// "lock" and hold it until [Job.Close]; another gets [ErrBusy]. Nothing
// deletes or steals the lock. Saves write a temporary file, sync it,
// rename it and sync the directory; the frozen files are never replaced.
// "job.json" is version 3 of format "skygo-ec2-job-state". Version 2
// added the upload cleanup and log checkpoint to version 1; version 3
// adds the result download, so that a version 2 reader refuses a journal
// that may hold a download's custody. A version 1 or 2 journal is read as
// written and stored as version 3 at its next save, with the same task
// bytes, request ID, request domain and identities. Another format or
// version, an unknown field, a non-canonical JSON encoding or more than
// 256 KiB is refused, never rewritten. [Open] refuses such a journal
// before it creates the lock file or removes any temporary file, and
// removes this format's temporary files only after it has read the
// journal again under the lock. That first read is not a lock: it
// assumes every writer of the directory holds the lock. The task is kept
// as the bytes first stored and is never re-encoded for comparison; a
// stored task with fields this version does not know is refused. These
// checks need Linux or macOS.
package ec2job
