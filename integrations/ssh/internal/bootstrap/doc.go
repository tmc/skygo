// Package bootstrap installs and starts the skygo-ssh agent on a remote
// host by running one fixed shell script over SSH.
//
// Everything the bootstrap writes is private (umask 077) and lives in
//
//	~/skygo-test/COMMIT/DIGEST/
//	    skygo-ssh                 agent binary whose SHA-256 is DIGEST
//	    cert.pem key.pem token    agent TLS identity and bearer token
//	    data/                     agent state
//	    agent.pid agent.log       process record and output
//
// Files are sent on standard input, never on a command line, and are
// stored only if absent: an existing file must already match, and a
// symlink or mismatch is refused, never replaced. The agent is started
// once per directory and recorded in agent.pid. A recorded process that is
// running is reused; a stopped one is restarted only on request; and a
// record that cannot be verified, such as one left by a start that was
// interrupted, is reported rather than replayed.
package bootstrap
