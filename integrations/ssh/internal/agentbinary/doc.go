// Package agentbinary prepares a prebuilt skygo-ssh agent binary for a
// Linux arm64 host, the alternative to building one from a checkout with
// agentbuild. It runs no go, git or other command.
//
// The operator names the binary, the commit it was built from and its
// SHA-256 in a Spec. The commit is the operator's assertion that the
// binary was built from that clean commit; nothing here can check it.
// The digest is checked against the bytes Prepare copied, and those bytes
// must be a statically linked little-endian ELF64 executable for AArch64
// holding the Go build information of the skygo-ssh command, built for
// GOOS=linux, GOARCH=arm64 and CGO_ENABLED=0. These checks bind the bytes
// to the digest and the expected format; they are not an attestation of
// the source.
//
// Prepare copies the binary into a private cache directory and publishes
// the copy read-only, without replacing an existing file:
//
//	CACHE/prebuilt/COMMIT/SHA256/skygo-ssh-linux-arm64
//
// It returns an Artifact naming the copy. Artifact.Build checks the copy
// again and returns it as an agentbuild.Build for the bootstrap.
package agentbinary
