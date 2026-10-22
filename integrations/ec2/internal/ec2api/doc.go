// Package ec2api is a small EC2 and STS Query client for one owned
// skygo virtual machine.
//
// A [Profile] fixes the account, region, subnet, security groups, image,
// instance type and root volume of one target. An [Identity] names one
// environment generation. [NewCreateRequest] turns both into an immutable
// RunInstances request whose ClientToken is derived from the identity and
// whose TagSpecifications tag the instance, its volume and its network
// interface at creation. [ParseCreateRequest] accepts only the exact bytes
// NewCreateRequest produces, so a durable owner can persist the request and
// reissue it byte for byte.
//
// A [Client] signs requests with the AWS SDK SigV4 signer and makes exactly
// one HTTP attempt per call. It never retries, never generates a ClientToken,
// and never follows redirects or uses an ambient proxy. Before every create,
// stop, start and terminate it checks that the credentials belong to the
// profile's account; before every mutation other than create, and every
// credit read and console read, it describes the exact instance ID and
// refuses unless [VerifyOwner] accepts it. Start also requires the
// profile's CPU credit option, if set; Stop and Terminate of an owned
// instance do not, so cleanup stays possible.
//
// A ClientToken is at most 64 ASCII characters. EC2 documents no period for
// which it retains one, and this package assumes none: after an ambiguous
// create, reissue the identical request or observe the exact owned
// identity, and never use a new token.
//
// A failed call returns an [*Error]. When its Sent field is true the request
// may have reached EC2, so a failed mutation may have taken effect.
// An InvalidInstanceID.NotFound reply or an empty search is never evidence
// that an instance is gone.
//
// [ParseConsole] extracts SSH host key and agent fingerprints from console
// output. Parsing succeeds on text; it does not make that text trustworthy.
package ec2api
