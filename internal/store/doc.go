// Package store persists accepted jobs and submission identities in one
// exclusively locked database.
//
// Open requires a private data directory on a filesystem supporting file locks
// and mmap. Identical request retries return the original job. Records are kept
// until the operator removes the directory; a full store refuses new submissions
// without forgetting previous request bindings.
package store
