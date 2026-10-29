// Package gce creates, stops, restarts and deletes one Compute Engine
// virtual machine and its one owned boot disk, keeping their lifecycle in
// a private, locked state directory.
//
// It manages lifecycle only. It runs no skygo job, artifact, log, cancel
// or SSH session and is not a skygo execution target.
//
// A Machine is the sole owner of its state directory lock and of at most
// one original HTTP request at a time. It is not safe for concurrent use.
// Every mutation is prepared with its own random request id and committed
// to the state before its single dispatch; nothing is ever resent. A
// stored operation must be exactly the request its kind implies for the
// stored config and reserved names, or the state is refused. A mutation
// whose reply is lost is recovered only by finding its original operation
// in a complete listing of the configured zone's operations; if that
// evidence is missing, truncated or expired, the machine holds rather
// than infer anything.
//
// Custody: if the first Close of a response body or state file fails, or
// a state commit is not confirmed, the Machine keeps the owner and its
// lock and admits nothing more. A failed Close is never retried; an
// unconfirmed commit is retried, in full and only by Close, and never
// rolled back to an earlier goal. Errors that combine such failures have
// a fixed message; errors.Is and errors.As reach each original cause.
// Every Machine method, Close included, is synchronous: no context bounds
// its file system calls, and a caller that stops waiting still owns the
// Machine until the method returns.
//
// Admission: the only supported profile is ARM64. Before any mutation
// that charges, one images.get of the configured image and one
// machineTypes.get of the configured machine type must report their exact
// names, ids, links and zone, the image READY, and both architecture ARM64;
// anything else, including an unspecified architecture, is refused with
// nothing sent. The machine records a private versioned admission witness
// of these replies before its first mutation, and only an admitted state
// starts, stops or reports a goal other than Absent reached. A state
// without the witness can still tear down the resources it exactly owns.
// Configuring an exact image id is the operator's enrollment of a trusted
// Linux image: Compute Engine attests no operating system, and this
// package never boots, connects to or inspects the VM.
//
// The disk is created by an explicit disks.insert from the pinned image
// and must report that image's numeric id before any VM is inserted. The
// VM attaches only that verified disk, with autoDelete false. Deletion
// verifies the VM gone before deleting the unused owned disk, then
// verifies the disk gone; an owned disk found holding another image is
// still deleted. A numeric id is pinned only with a durable witness of the
// settled successful create that reported it, kept apart from the bounded
// operation history. A resource is counted gone only if its id is so
// pinned and a fresh lookup finds nothing with no request unresolved;
// Status labels it ABSENT after this package's own delete and
// REMOVED-EXTERNALLY otherwise. A lost, failed or unpinned create holds.
//
// Trust assumption: Compute Engine start, stop and delete name resources
// and accept no resource-id precondition. If another writer deletes the
// owned VM and creates a new one with the same name between this
// package's check and its mutation, the mutation acts on the new VM; the
// operation's target id reveals this only afterward. Likewise a lookup by
// name cannot prove which incarnation is gone. This package does not
// prevent either. It requires an operator-controlled project and zone, an
// external lease on the reserved names and no other writer that reuses
// them.
//
// Drive and Refresh take an explicit OAuth 2 access token and never
// refresh or discover credentials. Refresh sends only GET requests.
// The authorized key must be one ssh-ed25519 public key.
package gce
