// Package sshagent opens a skygo agent on a Linux arm64 host reached with
// SSH, as the skygo-ssh command does, and returns a session whose control
// client is authenticated to it.
//
// Open builds the agent from a local skygo checkout, installs it in the
// remote ~/skygo-test directory with a fixed bootstrap script, starts it
// unless a running agent is recorded there, and waits until it answers an
// authorized GetServerInfo for the expected target. The SSH host key is
// checked against a dedicated known_hosts file and the agent against its
// own pinned TLS certificate. Each RPC uses its own SSH connection and is
// never retried.
//
//	s, err := sshagent.Open(ctx, cfg)
//	if err != nil {
//		return err // an *OpenError holds connections whose state is unknown
//	}
//	info, err := s.Control().GetServerInfo(ctx, connect.NewRequest(&v1.GetServerInfoRequest{}))
//	err = errors.Join(err, s.Close())
//
// A Session owns each Control request until it returned and its response
// body closed cleanly; a stream, with its connection, until its first
// CloseResponse returned and every frame it entered before then returned.
// A stream's CloseResponse speaks for that stream: while one of its frames
// is unfinished it returns a pending *CallError, and from the moment the
// first one starts the session admits no new request until the stream
// drains. A request whose connection did not settle returns a *CallError
// and faults the session, which then refuses every new request. Session.Close speaks for the whole session. It
// refuses new requests for good, but does not wait for those in flight:
// while one is unfinished, or after a fault, it returns a *CallError and
// leaves the SSH agent connection open. The remote agent and its jobs
// outlive the session: nothing in this package stops or signals them.
package sshagent
