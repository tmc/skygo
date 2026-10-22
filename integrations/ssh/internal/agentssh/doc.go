// Package agentssh reaches one operator-installed skygo agent through an
// authenticated SSH direct-tcpip channel.
//
// A client from newClient sends each HTTP request over its own SSH
// connection to a fixed loopback address on the SSH server and verifies the
// agent with TLS against a configured CA and server name. The SSH host key
// must be enrolled in a dedicated known_hosts file; unknown, mismatched and
// revoked keys are refused. The client never pools, retries, replays or
// follows redirects. checkAgent confirms with read-only requests that the
// agent reports the request domain the operator enrolled.
//
// The request domain identifies the agent's persistent retention store, not
// a process or an execution. The package starts no remote command, installs
// nothing and does not deploy a TLS-capable agent: the agent, or a TLS
// terminator in front of it, must already listen on the loopback port.
//
// Closing a response body, or a request failing, closes the request's TCP
// connection and joins the calls the package started for it. Disconnecting
// does not stop work the agent has already accepted.
package agentssh
