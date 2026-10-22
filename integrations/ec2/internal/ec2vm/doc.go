// Package ec2vm drives the lifecycle of one owned EC2 instance, one call
// at a time, from a durable [Record].
//
// [NewRecord] makes the record of a new instance: its identity, profile
// hash, the exact RunInstances request bytes and their ClientToken, the
// desired state and the observed phase. This package has no storage or
// encoding of its own. [FromFields] checks a record read back from
// storage, refusing one whose request, token, identity and profile hash do
// not agree.
//
// [Next] reports the one call Step makes for a record, and Step makes it:
// at most one EC2 mutation per Step.
//
// # Persistence boundary
//
// The caller durably stores every field of the record before the first
// [Step] and every record Step returns, error or not, before the next
// Step. Before Step first sends RunInstances it hands the record, marked
// CreateSent, to the Commit function of [Options], and sends only after
// Commit returns nil; a nil return is the caller's acknowledgement that
// the record is durably stored. Step refuses to create without Commit and
// sends nothing if Commit fails. A stored record marked CreateSent without
// an instance ID is an ambiguous create.
//
// # Lifecycle
//
// An ambiguous create is never resolved with a new ClientToken. Within
// Options.ReissueWindow after the first send, Step reissues the identical
// request. The window is zero by default, because EC2 documents no period
// for which it retains a ClientToken; Step then searches by ClientToken
// and, if nothing is observed, holds the record for the operator. An empty
// search or an InvalidInstanceID.NotFound reply never means the instance
// is gone: only an observed terminated state makes a record Terminated,
// and Terminated says nothing about the instance's volume and network
// interface.
//
// Desired state Absent is latched. A record that wants Absent while its
// create is ambiguous is still recovered by ClientToken and terminated.
//
// When a call returns a result together with an error, Step keeps the
// result: instances a create reply reported are held as unverified
// evidence, and a reported state change is applied, except that a
// reported terminated state goes no further than Terminating. Only an
// observation without error makes a record Terminated.
//
// An owned instance whose CPU credit option differs from the profile's is
// held: it is never reported Running, but it can still be stopped and
// terminated. An identity conflict (an IdempotentParameterMismatch, an
// instance that is not owned, credentials of another account) holds the
// record with no further calls.
//
// Step does not wait or retry; bounded polling is the caller's.
package ec2vm
