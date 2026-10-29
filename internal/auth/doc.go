// Package auth authenticates scoped principals and manages private credential
// files. It uses generated random bearer tokens, not human passwords. File
// administration is local; no credential management API is exposed over HTTP.
//
// An Engine mints Actor values. Their zero values grant no authority. Call Check
// before each operation to refresh credential validity and role; callers enforce
// resource ownership separately. Legacy engines explicitly grant a single
// operator global access and do not provide team isolation.
//
// A file Engine constructed with NewFileOIDC may also accept OpenID Connect ID
// tokens from one configured issuer, verified with RS256 or ES256 keys read
// from a local JWKS file that must not be group or other writable, or fetched
// over HTTPS from the one jwks_uri the operator allowed when constructing the
// Engine. A private OIDC file maps each (issuer, subject) to an existing
// principal, whose role comes only from the credential file. OIDC Actors have
// no token ID and cannot delegate. Removing a mapping or principal, or a key
// from a JWKS file, revokes within the two-second refresh envelope. A fetched
// key set is refetched every five seconds. A fetch counts only if it
// completes, from its CA file read through the response, its Close and
// parsing the key set, within ten seconds of its start; its key set is then
// usable for at most 30 seconds after it completed. A failed, late or
// cancelled fetch never extends that. A key removed at the issuer therefore
// stops authenticating within 40 seconds, provided the allowed URL serves the
// issuer's current key set to every request sent after the removal; a stale
// cache or issuer behind that URL is not bounded. Otherwise authority ends
// when the token expires. An invalid OIDC or key file, or an issuer outage
// longer than 30 seconds, makes OIDC unavailable without affecting scoped
// tokens. Run reads the OIDC files and
// fetches keys on separate goroutines, one read and one fetch at a time, so a
// stalled read or fetch also makes only OIDC unavailable; Run returns only
// after a pending read or fetch has returned.
//
// Every file read, and every fetched response, is closed exactly once. If
// that Close fails, the Engine retains the error with the original file or
// response body, becomes unavailable for scoped tokens and OIDC alike, and
// reads nothing more; see Engine.Err.
//
// The OIDC file has a generation. Compared with the last accepted OIDC file, a
// lower generation, or different bytes at the same generation, makes OIDC
// unavailable until the accepted bytes are restored or a higher generation is
// published. A key set has no generation, so restoring an older valid key set
// is not detected; it is accepted like any rotation.
//
// File commands serialize mutations with a sibling .lock directory and a
// one-second wait. After a crashed command, a machine operator may remove that
// exact lock only after all credential writers are stopped. Published config
// and credential files are private; they must not be shared through logs or
// workload artifacts. A local edit is not acknowledgement of a running server's
// refresh. Server revocation is bounded by the separately tested refresh and
// transport envelope.
package auth
