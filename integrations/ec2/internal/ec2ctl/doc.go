// Package ec2ctl keeps the lifecycle of one EC2 virtual machine in a
// private state directory and drives it with [ec2vm.Step] and an
// [ec2api.Client].
//
// A [Config] fixes the VM's profile, identity and user data. [Create]
// stores the record of a new VM, wanting it present with nothing sent,
// before any EC2 call; [Open] reopens it. [Machine.SetGoal] durably stores
// what the operator wants before anything acts on it, and [Machine.Drive]
// makes the calls. [ReadStatus] reads the stored state without the lock,
// and [Observe] describes the owned instance afresh without storing
// anything.
//
// # Input files
//
// [LoadConfig] and [LoadCredentials] read only private regular files: owned
// by the effective user, with owner read and write permission and no group
// or other permission (mode 0600), and one link. A symbolic link,
// directory, FIFO, device or socket is refused before anything is read,
// and opening a FIFO does not wait for a writer. The state directory's
// files are read the same way. These checks need Linux or macOS; elsewhere
// every read is refused.
//
// # State directory
//
// The directory and its files must belong to the effective user and grant
// no group or other access. A symbolic link in place of the directory or a
// file in it is refused, as is a regular file with more than one link.
// Components above the directory are not checked.
//
// [Create] makes the directory if it does not exist. Every writer, [Create]
// or [Open], first syncs the directory's parent, so that the directory's
// entry is durable before anything is stored in it or any EC2 call is
// made; if the parent cannot be opened, synced or closed, it fails before
// taking the lock. A directory left by a failed sync holds no state and
// is refused, never deleted. [Create], [Open] and [ReadStatus] check the
// [Config] before touching the file system.
//
// The directory holds "lock", "vm.json" and, briefly, "vm.json.tmp".
// Writers take a nonblocking exclusive flock on "lock" and hold it until
// Close, across every EC2 call; another writer gets [ErrBusy]. The lock is
// released by the kernel when its holder exits; nothing deletes it or
// steals it by age or process ID. A save writes "vm.json.tmp", syncs it,
// renames it over "vm.json" and syncs the directory. A leftover temporary
// file is an uncommitted save and is removed when the lock is taken.
//
// A [Machine] is not safe for concurrent use; Close it only after Drive has
// returned, even if Drive's context was canceled. A closed Machine no longer
// owns the directory, and its SetGoal, Drive and Close return
// [os.ErrClosed] before storing anything or calling EC2.
//
// "vm.json" is version 1 of format "skygo-ec2-vm-state": indented JSON
// holding the profile, a restart flag and every field of the
// [ec2vm.Record], including its identity, exact RunInstances request and
// ClientToken. Another format or version, an unknown field, trailing data,
// a non-canonical encoding or more than 256 KiB is refused, never
// rewritten. Every load checks the record with [ec2vm.FromFields] and
// refuses ([ErrMismatch]) a state whose profile hash, identity or create
// request differs from the configuration's. No credential is stored.
//
// # Crash safety
//
// Step's Commit stores the record marked CreateSent before RunInstances is
// sent, and every record Step returns, error or not, is stored before the
// next Step. If a store fails, the Machine keeps the record pending and
// makes no EC2 call until it is stored ([ErrUnpersisted]). A crash instead
// leaves the last stored record: an ambiguous create is then resolved only
// by searching for the stored ClientToken, never by a new token or a
// reissue, so at most one create intent exists per identity. A search
// that finds nothing holds the record; only a record that wants absent
// searches again. The repeated
// stop, start or terminate of an owned instance that a crash can cause is
// idempotent in EC2; a create is not and is never repeated.
//
// Desired absent is latched. A restart is stored as wanting stopped with a
// restart flag; once a Step observes the VM stopped, wanting present is
// stored, clearing the flag, before the start is sent. A restart
// interrupted by a crash resumes rather than starting over.
//
// A failed or canceled call, a deadline or a held record is not evidence
// that a mutation did or did not take effect, nor that the instance's
// volume or network interface is gone.
package ec2ctl
