// Package agentbuild builds the skygo-ssh agent for a remote host from a
// local skygo checkout.
//
// The build is the checkout's own integrations/ssh/cmd/skygo-ssh, compiled
// with its own go.mod for linux/arm64 with cgo, workspaces and toolchain
// switching disabled. The result is identified by the checkout's HEAD
// commit and the SHA-256 of the binary, so a changed source tree never
// reuses an earlier build under the same commit.
package agentbuild
