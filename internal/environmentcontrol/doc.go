// Package environmentcontrol accepts persistent environments, drives their
// lifecycle through a provider Runtime and admits their exec and forward
// sessions. It imports no provider; an integration implements Runtime.
//
// Every decision is a Store commit before its effect. Create accepts the
// spec, binding, delegation and a fresh nonce before any runtime call, and
// replays an identical request without consulting the runtime. Reconcile
// commits each runtime observation, including newly created resource
// identities, before anything depends on it; a runtime declares each
// mutating request in its native state and sends it only after that
// declaration is committed (ErrWritePending). A result whose commit is not
// positively known, after a lost response or a failed readback, is held and
// committed first, before any later runtime call or session admission for
// the environment; it is never obtained again by another call. That record
// is process-local: after a restart the runtime's durable declarations
// account for what may have been sent. Passes and session admissions are
// serialized, so no admission falls between a runtime call's return and the
// commit of its result; an admission stops waiting when its caller leaves,
// and admits a session only under the actor's authority as it is once the
// wait ends. That authority is checked once more after the session's
// commit, immediately before the runtime is called. Delete records Absent intent, which is never undone, before the
// runtime removes owned resources; borrowed resources are never removed.
//
// Sessions are recorded in the environment's checkpoint by revision
// compare-and-swap, the same one that latches deletion and idle expiry, so a
// session is admitted only to a Present, unexpired, Ready environment, and
// expiry never fires while a session is recorded. Closing a session cancels
// only its own frame. An exec that did not report an exit is recorded as
// unknown, after a restart as well, and keeps blocking expiry until the
// runtime observes the environment deleted: nothing here infers that a
// remote command stopped.
//
// A spec with artifacts is accepted only for a Runtime that is a
// Materializer and a Controller with a bound ArtifactOpener; otherwise
// Create refuses it with ErrUnsupported before any effect. Once a Reconcile
// pass commits the instance identity, the incarnation, that Ensure observed,
// a frame of this process admits one Store read of each distinct artifact
// for that committed incarnation, opens it, has the runtime stage it at every
// mount of the artifact, and releases the read only after the runtime's call
// is done and the body is closed. The frame keeps the incarnation it started
// for and never reads the row again to choose another. Ready is committed
// only once this frame staged every artifact and released its read; an
// artifact that was not staged holds the incarnation (View.Held) until the
// runtime observes a replacement. A new incarnation is committed, and
// Deleted, only once no frame of this process holds a call, a read, a file
// that may be open or a Begin of unknown result. A lost Begin result is
// replayed under the opener's producer; under an unknown producer it can only
// be held. StopMaterializing ends the frames' effects but not their
// releases; Close then waits for the frames, and reports any that still hold
// something.
package environmentcontrol
