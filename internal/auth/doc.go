// Package auth authenticates scoped principals and manages private credential
// files. It uses generated random bearer tokens, not human passwords. File
// administration is local; no credential management API is exposed over HTTP.
//
// An Engine mints Actor values. Their zero values grant no authority. Call Check
// before each operation to refresh credential validity and role; callers enforce
// resource ownership separately. Legacy engines explicitly grant a single
// operator global access and do not provide team isolation.
//
// A file Engine may also accept OpenID Connect ID tokens from one configured
// issuer, verified with RS256 or ES256 keys read from a local JWKS file that
// must not be group or other writable. A private OIDC file maps each (issuer,
// subject) to an existing principal, whose role comes only from the credential
// file. OIDC Actors have no token ID and cannot delegate. Removing a mapping,
// key or principal revokes within the refresh envelope; otherwise authority
// ends when the token expires. An invalid OIDC or key file makes OIDC
// unavailable without affecting scoped tokens. Run reads the OIDC files on a
// separate goroutine, one read at a time, so a stalled read also makes only
// OIDC unavailable; Run returns only after a pending read has returned.
//
// The OIDC file has a generation. Compared with the last accepted OIDC file, a
// lower generation, or different bytes at the same generation, makes OIDC
// unavailable until the accepted bytes are restored or a higher generation is
// published. The JWKS file has no generation, so restoring an older valid key
// set is not detected; it is accepted like any rotation.
//
// File commands serialize mutations with a sibling .lock directory and a
// one-second wait. After a crashed command, a machine operator may remove that
// exact lock only after all credential writers are stopped. Published config
// and credential files are private; they must not be shared through logs or
// workload artifacts. A local edit is not acknowledgement of a running server's
// refresh. Server revocation is bounded by the separately tested refresh and
// transport envelope.
package auth
