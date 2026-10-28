// Skygo-ssh runs skygo commands against a Linux arm64 host reached over
// SSH, building and starting the skygo agent there from the local source.
//
// Usage:
//
//	skygo-ssh --host ALIAS --known-hosts FILE [flags] SKYGO-ARGS...
//
// ALIAS is an OpenSSH host alias, resolved with `ssh -G`. SKYGO-ARGS are
// the arguments of the skygo command, such as `run -f task.yaml --wait`
// or `jobs logs ID`. Before running them, skygo-ssh
//
//   - builds integrations/ssh/cmd/skygo-ssh from the skygo checkout for
//     linux/arm64, identified by its commit and the SHA-256 of the binary;
//   - copies the binary and a private TLS certificate, key and token to
//     ~/skygo-test/COMMIT/SHA256/ on the host, if not already there;
//   - starts `skygo-ssh agent` there unless it is running, listening on
//     127.0.0.1 only; and
//   - waits until the agent answers as itself through SSH.
//
// The agent is the canonical skygo server with its built-in local target
// named after the host, so a task without a target runs on the host.
// Jobs persist in the agent directory and survive the SSH session.
//
// The flags are:
//
//	--host ALIAS
//		OpenSSH host alias. Proxies, agent forwarding, port forwarding and
//		remote or local commands in its configuration are refused.
//	--known-hosts FILE
//		known_hosts file dedicated to the host, with literal host names
//		only. Hashed, wildcard and @cert-authority entries are refused.
//	--identity-fingerprint SHA256:...
//		The SSH agent key to authenticate with. By default, the one key
//		whose IdentityFile.pub exists. No other agent key is offered.
//	--key-file FILE
//		An unencrypted private key, mode 0600, instead of the SSH agent.
//	--source DIR
//		The skygo checkout to build, uncommitted changes included. By
//		default, the checkout holding the current directory, which must
//		have no uncommitted changes.
//	--target-name NAME
//		The remote target name. By default, the host alias.
//	--agent-port PORT
//		The agent's loopback port on the host. The default is 47080.
//	--restart-stopped
//		Start a new agent if the recorded agent process has exited.
//
// See README.md for enrollment, the remote layout and current limits.
package main
