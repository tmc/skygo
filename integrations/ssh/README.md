# skygo SSH integration

This module holds the private transport that reaches an operator-installed
skygo agent through SSH. It has no command and no public API yet.

Package `internal/agentssh` sends each HTTP request over its own SSH
connection and `direct-tcpip` channel to a fixed loopback port on the SSH
server, and verifies the agent with TLS against a configured CA and server
name. It requires:

- an unencrypted private key readable only by its owner;
- a known_hosts file dedicated to the server, listing literal host names
  only (hashed, wildcard, negated and `@cert-authority` entries are refused);
- an agent, or a TLS terminator in front of it, already listening on
  `127.0.0.1` or `[::1]` on the SSH server;
- the request domain the operator enrolled for that agent.

The transport never retries, replays, pools connections or follows
redirects, and refuses compression and protocol upgrades. Requests and
responses are limited to 1 MiB and 10 seconds.

It does not install or start the agent, run remote commands, configure
credentials, or stop agent work when a connection drops. Deployment, a
command entrypoint and qualification against real hosts are later work.
