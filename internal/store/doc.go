// Package store persists accepted jobs and submission identities in one
// exclusively locked database.
//
// Open requires a private data directory on a filesystem supporting file locks
// and mmap. With retention disabled, identical request retries return the original
// job and records remain until the operator removes the directory. A full store
// refuses new submissions without forgetting previous request bindings.
//
// [EnableRetention] explicitly enables bounded request epochs on a stopped
// installation. Retained requests use a [RequestDomain]; replay can report expiry
// instead of creating another job. Pruning separates job metadata, output closure
// and proved execution cleanup. An unknown execution is never stopped proof,
// and acknowledged metadata removal can leave a manual cleanup obligation.
//
// Backup and Restore operate on stopped installations. Snapshots preserve the
// installation owner, accepted tasks, request bindings, and active job intent;
// local logs and execution files are separate. Restore never starts work. Keep
// the original stopped before resuming the restored copy, since both identify
// the same installation. Unknown execution state does not prove work stopped.
//
// Scoped authentication is an explicit offline activation. ActivateScoped
// preserves old rows and binds them to the reserved legacy principal. Scoped
// admission methods require opaque auth actors; trusted reconciliation retains
// unscoped reads and updates. Principal request namespaces and dense listing
// indexes keep another principal's submissions out of results and cursors.
// The separately managed auth file is not included in a database snapshot.
//
// Open migrates supported legacy state atomically and refuses newer schemas
// before writes. Rollback requires the matching binary and its old snapshot;
// unfenced older binaries must never open a newer database.
// Format upgrades do not activate scoped authentication or reset admission
// counters. Target bindings are immutable server metadata outside task equality.
// Unbound historical remote work requires separate operator recovery; it is
// never assigned a route from a current target alias.
//
// BeginAttempt and ObserveAttempt are trusted coordinator transactions. Retry
// policies reserve bounded history at admission; attempts never consume another
// submission sequence or change the accepted task. Generic updates cannot alter
// attempt history, deadlines or execution evidence. Backups include this state;
// log retention remains separate.
package store
