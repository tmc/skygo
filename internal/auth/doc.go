// Package auth authenticates scoped principals and manages private credential
// files. It uses generated random bearer tokens, not human passwords. File
// administration is local; no credential management API is exposed over HTTP.
//
// An Engine mints Actor values. Their zero values grant no authority. Call Check
// before each operation to refresh credential validity and role; callers enforce
// resource ownership separately. Legacy engines explicitly grant a single
// operator global access and do not provide team isolation.
//
// File commands serialize mutations with a sibling .lock directory and a
// one-second wait. After a crashed command, a machine operator may remove that
// exact lock only after all credential writers are stopped. Published config
// and credential files are private; they must not be shared through logs or
// workload artifacts. A local edit is not acknowledgement of a running server's
// refresh. Server revocation is bounded by the separately tested refresh and
// transport envelope.
package auth
