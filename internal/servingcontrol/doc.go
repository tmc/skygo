// Package servingcontrol accepts services with one to eight fixed replicas on one
// configured target, drives them through a serving.Runtime and admits
// requests forwarded to them.
//
// Every runtime call follows a committed record. Acceptance commits the
// service and its generation 1 Apply operation before any call. Each Native
// a runtime returns is committed, with the operation it belongs to, before
// the runtime is called again for that service, even when the call failed;
// a result whose commit is not known stays held in this process and blocks
// every later runtime call for the service until it commits. An operation is
// cleared, and a deleted service's reservation released, only by a commit
// that carries an observation taken after its Native was committed. Neither
// a restart nor an uncertain result is permission to repeat a native write:
// the runtime decides what its committed Native permits.
//
// A Controller serves one target. Create and Delete require the actor's
// Mutate permission; Get and List require Read and show the actor's own
// services, or every service to an admin. Forward requires Mutate and the
// service's own principal: an admin forwarding to another principal's
// service gets store.ErrNotFound. It admits a request only for a service
// whose current generation is observed ready with no operation in flight.
//
// Every public method is admitted before it checks authority or touches the
// Store. Close stops admission, cancels runtime calls and forwards, joins
// every call it admitted, each after its last Store write, and commits any
// held result before it returns nil. A forward ends only when its exchange's
// original frame has settled; a deleted service is cleaned up only after its
// forwards end.
package servingcontrol
