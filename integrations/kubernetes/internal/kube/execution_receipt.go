package kube

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"k8s.io/apimachinery/pkg/util/validation"
)

// An execution receipt is the private durable custody record of one attempt's
// native parent object: a Job for ordinary execution or a directly managed Pod
// for retry execution. It is stored as one canonical JSON file per attempt in a
// scope directory derived from the installation owner, namespace and target
// binding. Records are bounded in size, count and dependent inventory.
//
// The parent phase only advances:
//
//	intent   identity and nonce recorded; no create request sent
//	sent     the single create grant was consumed before the request
//	rejected the API server definitely refused that request
//	pinned   the created UID is recorded
//	refused  a complete create response arrived but was not 201 Created
//	         with exactly the receipted object; a redirection included
//	retired  cleanup removed the pinned object; the record is a tombstone
//
// A record in intent or sent never grants a second request, in this or any
// later process. A retired record keeps its identity, UID and nonce, occupies
// a counted slot, and authorizes nothing: no create, read, cancel or cleanup.
// Tombstones are never reclaimed; a full store refuses new attempts.
//
// A refused record is terminal as well. Unlike rejected, it does not prove
// that nothing was persisted: the object the response reports may exist. The
// record keeps the original identity, nonce and any UID pinned earlier by a
// concurrent recovery read, adds a witness of the response, and authorizes
// nothing, so that a later read can never adopt an object whose actual
// create response was refused. It is left for the operator. Operations that
// loaded the record before the refusal recheck it after each native read and
// before each further native request or release of log bytes; see
// receiptCurrent.
const (
	receiptFormat     = "skygo-kubernetes-execution-receipt-v1"
	maxReceiptBytes   = 16 << 10
	maxReceiptObjects = 32
	maxReceipts       = maxRetryReceipts
)

const (
	phaseIntent   = "intent"
	phaseSent     = "sent"
	phaseRejected = "rejected"
	phasePinned   = "pinned"
	phaseRefused  = "refused"
	phaseRetired  = "retired"
)

// Cancellation and cleanup steps. Intent is recorded before the request and
// delivered after its successful response.
const (
	stepIntent    = "intent"
	stepDelivered = "delivered"
)

// Dependent object phases. Dependent records are typed custody rows for the
// resource-claim lifecycle; no backend method creates them yet, and a receipt
// carrying any is refused by every backend method.
const (
	objectIntent   = "intent"
	objectSent     = "sent"
	objectObserved = "observed"
	objectGone     = "gone"
)

// Scope directory entries other than receipts and their temporary files.
// The journal lock serializes short record transitions; the operation lock
// is held, shared, for the whole of each native operation and exclusively by
// cleanup.
const (
	journalLockName   = "lock"
	operationLockName = "operation"
)

type executionReceipt struct {
	Format     string          `json:"format"`
	Generation uint64          `json:"generation"`
	Owner      string          `json:"owner"`
	Namespace  string          `json:"namespace"`
	Binding    string          `json:"binding"`
	Attempt    string          `json:"attempt"`
	TaskSHA256 string          `json:"task_sha256"`
	Kind       string          `json:"kind"`
	Name       string          `json:"name"`
	Nonce      string          `json:"nonce"`
	SpecSHA256 string          `json:"spec_sha256"`
	Phase      string          `json:"phase"`
	UID        string          `json:"uid,omitempty"`
	Cancel     string          `json:"cancel,omitempty"`
	Cleanup    string          `json:"cleanup,omitempty"`
	Objects    []receiptObject `json:"objects,omitempty"`
	Witness    *receiptWitness `json:"witness,omitempty"`
}

// receiptWitness records what a refused, completely received create response
// reported, for the operator: its actual HTTP status, the SHA-256 and size of
// its exact body, whether the body decoded strictly as the parent kind and,
// only if it did, its UID, nonce and spec annotations and the canonical spec
// digest of the object it describes. A value that is absent or not well
// formed is omitted, never synthesized, so an omitted field does not say
// whether the response lacked it or carried a malformed one. The body itself
// is not kept.
type receiptWitness struct {
	Status         int    `json:"status"`
	ResponseSHA256 string `json:"response_sha256"`
	ResponseBytes  int64  `json:"response_bytes"`
	Decode         string `json:"decode"`
	UID            string `json:"uid,omitempty"`
	Nonce          string `json:"nonce,omitempty"`
	SpecAnnotation string `json:"spec_annotation,omitempty"`
	SpecSHA256     string `json:"spec_sha256,omitempty"`
}

// Witness decode outcomes.
const (
	witnessDecoded     = "decoded"
	witnessUndecodable = "undecodable"
)

// maxWitnessBytes bounds a witnessed response size; a larger one is not a
// create response the client accepted.
const maxWitnessBytes = 1 << 40

// valid reports a witness of a successful or redirection status, the only
// kinds a create response is refused for (see startReceipted); errors and lost
// responses are not witnessed. An undecodable body reports no values.
func (w *receiptWitness) valid() bool {
	digest := func(s string) bool { return s == "" || isLowerHex(s, 64) }
	if w.Status < 200 || w.Status > 399 || !isLowerHex(w.ResponseSHA256, 64) ||
		w.ResponseBytes < 0 || w.ResponseBytes > maxWitnessBytes {
		return false
	}
	switch w.Decode {
	case witnessDecoded:
	case witnessUndecodable:
		if w.UID != "" || w.Nonce != "" || w.SpecAnnotation != "" || w.SpecSHA256 != "" {
			return false
		}
	default:
		return false
	}
	return (w.UID == "" || validInstance(w.UID)) && digest(w.Nonce) && digest(w.SpecAnnotation) && digest(w.SpecSHA256)
}

// receiptObject records one dependent of the pinned parent. OwnerUID is the
// parent UID for a ResourceClaimTemplate or a Job's Pod. For a generated
// ResourceClaim it is the exact Pod UID: a recorded Pod row of a Job, or the
// parent UID of a directly managed Pod.
type receiptObject struct {
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	OwnerUID   string `json:"owner_uid"`
	SpecSHA256 string `json:"spec_sha256"`
	Phase      string `json:"phase"`
	UID        string `json:"uid,omitempty"`
}

var (
	errReceiptMissing  = fmt.Errorf("%w: kubernetes execution receipt unavailable", backend.ErrIdentityMismatch)
	errReceiptForeign  = fmt.Errorf("%w: kubernetes execution receipt belongs to another target or attempt", backend.ErrIdentityMismatch)
	errReceiptRetired  = fmt.Errorf("%w: kubernetes execution receipt retired", backend.ErrIdentityMismatch)
	errReceiptRefused  = fmt.Errorf("%w: kubernetes create response was refused", backend.ErrIdentityMismatch)
	errReceiptCorrupt  = errors.New("corrupt kubernetes execution receipt")
	errReceiptCapacity = errors.New("kubernetes execution receipt capacity reached")
	errReceiptExists   = errors.New("kubernetes execution receipt already exists")
	errOperationBusy   = errors.New("kubernetes execution operation pending")
)

// receiptUncertain reports a write whose durability is unknown. The caller
// must not act on the new state.
type receiptUncertain struct{ err error }

func (e receiptUncertain) Error() string {
	return "kubernetes execution receipt durability uncertain: " + e.err.Error()
}
func (e receiptUncertain) Unwrap() error { return e.err }

// fileID identifies a directory by device and inode.
type fileID struct{ dev, ino uint64 }

// receiptStore serializes transitions with an in-process mutex and an advisory
// journal lock shared with other processes. Neither is held across network
// I/O. Native operations separately hold the operation lock; see operate.
//
// Every acquisition reopens the scope directory through the root without
// following symlinks and requires both to be the private directories found
// when the store was opened; all file operations are then relative to that
// open directory. Trusted same-user administrators replacing ancestors of the
// root remain outside this check.
type receiptStore struct {
	root, scope               string // state directory and scope entry name
	dir                       string // root/scope, for diagnostics
	rootID, scopeID           fileID
	owner, namespace, binding string
	limit                     int

	mu   sync.Mutex
	seen map[string]uint64 // highest generation read or written, by attempt
	held *os.File          // scope directory while mu and the journal lock are held
	// blocked holds attempts whose create response this store refused,
	// whether or not the refused record was durably written. It authorizes
	// nothing in this store; other stores learn only from the record.
	blocked map[string]bool

	opMu   sync.Mutex
	ops    int      // in-process operation permits: n > 0 shared, -1 exclusive
	opFile *os.File // locked operation file while ops != 0

	// fault, when set by tests, fails the named persistence step.
	fault func(step string, r *executionReceipt) error
}

// checkReceiptRoot validates the configured state directory: an existing
// absolute, clean path without symlinks, owned by the effective user with
// mode 0700.
func checkReceiptRoot(dir string) error {
	if dir == "" || !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
		return errors.New("kubernetes execution state directory must be absolute")
	}
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return fmt.Errorf("kubernetes execution state directory: %w", err)
	}
	if resolved != dir {
		return errors.New("kubernetes execution state directory must not contain symlinks")
	}
	return checkPrivateDir(dir)
}

func receiptScope(owner, namespace, binding string) string {
	data, _ := json.Marshal(struct {
		Format    string `json:"format"`
		Owner     string `json:"owner"`
		Namespace string `json:"namespace"`
		Binding   string `json:"binding"`
	}{receiptFormat, owner, namespace, binding})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func openReceiptStore(root, owner, namespace, binding string) (*receiptStore, error) {
	return openReceiptStoreFault(root, owner, namespace, binding, nil)
}

// openReceiptStoreFault is openReceiptStore with the store's test fault hook
// set from construction on. Its "rootsync" step precedes the sync of root.
//
// Every successful construction, including one that finds the scope
// directory already created by an earlier or concurrent constructor whose
// own sync failed or has not happened yet, syncs root itself before it
// accepts the scope. A failed sync refuses the store, so that no Backend can
// consume a create grant from a journal whose directory entry is not durable.
func openReceiptStoreFault(root, owner, namespace, binding string, fault func(string, *executionReceipt) error) (*receiptStore, error) {
	if err := checkReceiptRoot(root); err != nil {
		return nil, err
	}
	if owner == "" || namespace == "" || binding == "" {
		return nil, errors.New("kubernetes execution receipts require a bound target")
	}
	rootID, err := dirID(root)
	if err != nil {
		return nil, err
	}
	scope := receiptScope(owner, namespace, binding)
	s := &receiptStore{root: root, scope: scope, dir: filepath.Join(root, scope), rootID: rootID,
		owner: owner, namespace: namespace, binding: binding,
		limit: maxReceipts, seen: make(map[string]uint64), blocked: make(map[string]bool), fault: fault}
	s.scopeID, err = makeScope(root, rootID, scope, func() error { return s.step("rootsync", nil) })
	if err != nil {
		return nil, fmt.Errorf("create kubernetes execution state: %w", err)
	}
	unlock, err := s.lock()
	if err != nil {
		return nil, err
	}
	defer unlock()
	if _, err := s.count(); err != nil {
		return nil, err
	}
	return s, nil
}

// openScope reopens and revalidates the pinned scope directory.
func (s *receiptStore) openScope() (*os.File, error) {
	resolved, err := filepath.EvalSymlinks(s.root)
	if err != nil {
		return nil, fmt.Errorf("kubernetes execution state directory: %w", err)
	}
	if resolved != s.root {
		return nil, errors.New("kubernetes execution state directory must not contain symlinks")
	}
	return openScope(s.root, s.rootID, s.scope, s.scopeID)
}

// openPrivate opens the private regular file name in dir.
func openPrivate(dir *os.File, name string, flag int) (*os.File, os.FileInfo, error) {
	f, err := openAt(dir, name, flag)
	if err != nil {
		return nil, nil, err
	}
	info, err := f.Stat()
	if err == nil {
		err = checkPrivate(info, false)
	}
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return f, info, nil
}

// count validates the scope directory entries and returns the number of
// receipts, retired ones included. Anything but receipts, their temporary
// files and the two lock files refuses the store.
func (s *receiptStore) count() (int, error) {
	entries, err := readDirAt(s.held)
	if err != nil {
		return 0, fmt.Errorf("read kubernetes execution state: %w", err)
	}
	if len(entries) > 2*s.limit+2 {
		return 0, errReceiptCapacity
	}
	n := 0
	for _, e := range entries {
		name := e.Name()
		id, ext, _ := strings.Cut(name, ".")
		switch {
		case name == journalLockName || name == operationLockName:
		case ext == "json" && isLowerHex(id, 32):
			n++
		case ext == "tmp" && isLowerHex(id, 32):
		default:
			return 0, fmt.Errorf("unexpected kubernetes execution state entry %q", name)
		}
		if !e.Type().IsRegular() {
			return 0, fmt.Errorf("unsafe kubernetes execution state entry %q", name)
		}
	}
	return n, nil
}

func isLowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for _, c := range []byte(s) {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// receiptEntry returns the scope entry name of attempt id's file with
// extension ext.
func receiptEntry(id, ext string) (string, error) {
	if _, err := jobName(id); err != nil {
		return "", err
	}
	return id + "." + ext, nil
}

// lock acquires the journal lock for a short record transition.
func (s *receiptStore) lock() (func(), error) {
	s.mu.Lock()
	dir, err := s.openScope()
	var f *os.File
	if err == nil {
		f, _, err = openPrivate(dir, journalLockName, os.O_RDWR|os.O_CREATE)
		if err == nil {
			if _, err = flock(f, true, true); err != nil {
				f.Close()
			}
		}
		if err != nil {
			dir.Close()
		}
	}
	if err != nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("lock kubernetes execution state: %w", err)
	}
	s.held = dir
	return func() {
		s.held = nil
		f.Close() // releases the journal lock
		dir.Close()
		s.mu.Unlock()
	}, nil
}

// operate acquires a shared native operation permit for the scope, waiting
// while cleanup holds it exclusively, until ctx is done. Permits are shared
// with nested calls, other Backends of the same scope and other processes.
// The caller holds the permit until its last network response, body and
// receipt write are finished, then calls the returned release.
func (s *receiptStore) operate(ctx context.Context) (func(), error) {
	delay := time.Millisecond
	for {
		ok, err := s.tryOperate(false)
		if err != nil {
			return nil, err
		}
		if ok {
			return s.release, nil
		}
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, fmt.Errorf("wait for kubernetes execution operation: %w", ctx.Err())
		case <-t.C:
		}
		delay = min(2*delay, 100*time.Millisecond)
	}
}

// exclusive acquires the operation permit for cleanup without waiting. Any
// operation in progress, in any Backend or process of the scope, makes it
// fail with errOperationBusy.
func (s *receiptStore) exclusive() (func(), error) {
	ok, err := s.tryOperate(true)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errOperationBusy
	}
	return s.release, nil
}

func (s *receiptStore) tryOperate(exclusive bool) (bool, error) {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if s.ops < 0 || s.ops > 0 && exclusive {
		return false, nil
	}
	if s.ops > 0 {
		s.ops++
		return true, nil
	}
	dir, err := s.openScope()
	if err != nil {
		return false, fmt.Errorf("lock kubernetes execution operation: %w", err)
	}
	defer dir.Close()
	f, _, err := openPrivate(dir, operationLockName, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return false, fmt.Errorf("lock kubernetes execution operation: %w", err)
	}
	ok, err := flock(f, exclusive, false)
	if err != nil || !ok {
		f.Close()
		if err != nil {
			return false, fmt.Errorf("lock kubernetes execution operation: %w", err)
		}
		return false, nil
	}
	s.opFile, s.ops = f, 1
	if exclusive {
		s.ops = -1
	}
	return true, nil
}

func (s *receiptStore) release() {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if s.ops > 1 {
		s.ops--
		return
	}
	// Closing the only descriptor of the open file releases its lock.
	s.opFile.Close()
	s.opFile, s.ops = nil, 0
}

// retain adds a reference to the shared operation permit that the caller
// holds and that is never released, so that the operation lock stays held,
// shared, until the process exits: cleanup of the scope then fails in every
// Backend and process. It takes no lock but opMu and does not upgrade the
// permit. The caller keeps s reachable for the process lifetime (see
// logCustodies), so that no finalizer closes the lock file.
func (s *receiptStore) retain() {
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if s.ops > 0 {
		s.ops++
	}
}

func (s *receiptStore) step(name string, r *executionReceipt) error {
	if s.fault == nil {
		return nil
	}
	return s.fault(name, r)
}

// check validates a decoded or proposed record for attempt id in this scope.
func (s *receiptStore) check(r *executionReceipt, id string) error {
	if r.Format != receiptFormat {
		return errReceiptCorrupt
	}
	if r.Owner != s.owner || r.Namespace != s.namespace || r.Binding != s.binding || r.Attempt != id {
		return errReceiptForeign
	}
	name, err := jobName(id)
	if err != nil || r.Generation == 0 || r.Name != name || r.Kind != "Job" && r.Kind != "Pod" ||
		!isLowerHex(r.TaskSHA256, 64) || !isLowerHex(r.Nonce, 64) || !isLowerHex(r.SpecSHA256, 64) {
		return errReceiptCorrupt
	}
	if r.Witness != nil && r.Phase != phaseRefused {
		return errReceiptCorrupt
	}
	switch r.Phase {
	case phaseIntent, phaseSent, phaseRejected:
		if r.UID != "" || r.Cancel != "" || r.Cleanup != "" || r.Objects != nil {
			return errReceiptCorrupt
		}
	case phasePinned:
		if !validInstance(r.UID) {
			return errReceiptCorrupt
		}
	case phaseRefused:
		// A UID, and steps recorded for it, remain only from an earlier pin.
		if r.Witness == nil || !r.Witness.valid() || r.Objects != nil ||
			r.UID != "" && !validInstance(r.UID) || r.UID == "" && (r.Cancel != "" || r.Cleanup != "") {
			return errReceiptCorrupt
		}
	case phaseRetired:
		if !validInstance(r.UID) || r.Cancel != "" || r.Objects != nil {
			return errReceiptCorrupt
		}
	default:
		return errReceiptCorrupt
	}
	if r.Cancel != "" && r.Cancel != stepIntent && r.Cancel != stepDelivered ||
		r.Cleanup != "" && (r.Cleanup != stepIntent || r.Kind != "Pod") {
		return errReceiptCorrupt
	}
	return checkObjects(r)
}

// checkObjects bounds the dependent inventory and checks its ownership graph:
// templates and a Job's Pods are owned by the parent UID. A generated claim is
// owned by one exact Pod: a recorded Pod row of a Job, never the Job itself,
// or the parent of a directly managed Pod, which has no Pod rows.
func checkObjects(r *executionReceipt) error {
	if r.Objects != nil && len(r.Objects) == 0 || len(r.Objects) > maxReceiptObjects {
		return errReceiptCorrupt
	}
	pods := make(map[string]bool)
	if r.Kind == "Pod" {
		pods[r.UID] = true
	} else {
		for _, o := range r.Objects {
			if o.Kind == "Pod" && o.UID != "" {
				pods[o.UID] = true
			}
		}
	}
	keys := make(map[string]bool)
	uids := map[string]bool{r.UID: true}
	for _, o := range r.Objects {
		key := o.Kind + "/" + o.Name
		if keys[key] || len(validation.IsDNS1123Subdomain(o.Name)) != 0 || !isLowerHex(o.SpecSHA256, 64) {
			return errReceiptCorrupt
		}
		keys[key] = true
		switch o.Phase {
		case objectIntent, objectSent:
			if o.UID != "" {
				return errReceiptCorrupt
			}
		case objectObserved, objectGone:
			if !validInstance(o.UID) || uids[o.UID] {
				return errReceiptCorrupt
			}
			uids[o.UID] = true
		default:
			return errReceiptCorrupt
		}
		switch o.Kind {
		case "ResourceClaimTemplate":
			if o.OwnerUID != r.UID {
				return errReceiptCorrupt
			}
		case "Pod":
			if r.Kind != "Job" || o.OwnerUID != r.UID {
				return errReceiptCorrupt
			}
		case "ResourceClaim":
			if !validInstance(o.OwnerUID) || !pods[o.OwnerUID] {
				return errReceiptCorrupt
			}
		default:
			return errReceiptCorrupt
		}
	}
	return nil
}

// readLocked returns the attempt's record, or nil if it has none.
func (s *receiptStore) readLocked(id string) (*executionReceipt, error) {
	data, err := s.readEntryLocked(id)
	if err != nil || data == nil {
		return nil, err
	}
	r := new(executionReceipt)
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(r); err != nil {
		return nil, errReceiptCorrupt
	}
	// Only the canonical encoding is accepted. This refuses trailing data,
	// duplicate keys, reordering and partial writes.
	canonical, err := json.Marshal(r)
	if err != nil || !bytes.Equal(canonical, data) {
		return nil, errReceiptCorrupt
	}
	if err := s.check(r, id); err != nil {
		return nil, err
	}
	if r.Generation < s.seen[id] {
		return nil, fmt.Errorf("%w: kubernetes execution receipt generation rolled back", backend.ErrIdentityMismatch)
	}
	s.seen[id] = r.Generation
	return r, nil
}

// readEntryLocked returns the bytes of the attempt's record file, at most
// maxReceiptBytes, or nil if it has none. It does not decode them. The DRA
// journal (dra_receipt.go) shares it with readLocked.
func (s *receiptStore) readEntryLocked(id string) ([]byte, error) {
	name, err := receiptEntry(id, "json")
	if err != nil {
		return nil, err
	}
	f, info, err := openPrivate(s.held, name, os.O_RDONLY)
	if errors.Is(err, fs.ErrNotExist) {
		if s.seen[id] != 0 {
			return nil, fmt.Errorf("%w: kubernetes execution receipt disappeared", backend.ErrIdentityMismatch)
		}
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open kubernetes execution receipt: %w", err)
	}
	if info.Size() <= 0 || info.Size() > maxReceiptBytes {
		f.Close()
		return nil, errReceiptCorrupt
	}
	data, err := io.ReadAll(io.LimitReader(f, maxReceiptBytes+1))
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, fmt.Errorf("read kubernetes execution receipt: %w", err)
	}
	if len(data) > maxReceiptBytes {
		return nil, errReceiptCorrupt
	}
	return data, nil
}

// writeLocked durably replaces the attempt's record with r: write a private
// temporary file, sync it, rename it into place and sync the directory. A
// failure after the rename returns receiptUncertain.
func (s *receiptStore) writeLocked(r *executionReceipt) error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return s.writeEntryLocked(r.Attempt, r.Generation, data, r)
}

// writeEntryLocked is writeLocked for data, the encoded record of attempt id
// at generation. r, which may be nil, is passed to the test fault hook. The
// DRA journal shares it with writeLocked.
func (s *receiptStore) writeEntryLocked(id string, generation uint64, data []byte, r *executionReceipt) error {
	if len(data) > maxReceiptBytes {
		return errReceiptCapacity
	}
	final, err := receiptEntry(id, "json")
	if err != nil {
		return err
	}
	tmp, _ := receiptEntry(id, "tmp")
	// A temporary file is only ever left by an interrupted write under this
	// lock; its rename never happened.
	if err := removeAt(s.held, tmp); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove kubernetes execution receipt temporary file: %w", err)
	}
	f, err := openAt(s.held, tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL)
	if err != nil {
		return fmt.Errorf("create kubernetes execution receipt: %w", err)
	}
	_, err = f.Write(data)
	if err == nil {
		err = s.step("sync", r)
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = s.step("rename", r)
	}
	if err != nil {
		if rerr := removeAt(s.held, tmp); rerr != nil {
			err = errors.Join(err, rerr)
		}
		return fmt.Errorf("write kubernetes execution receipt: %w", err)
	}
	if err := renameAt(s.held, tmp, final); err != nil {
		return receiptUncertain{err}
	}
	s.seen[id] = generation
	err = s.step("dirsync", r)
	if err == nil {
		err = syncAt(s.held)
	}
	if err != nil {
		return receiptUncertain{err}
	}
	return nil
}

// create durably records a new attempt in phase intent. An existing record of
// any phase, retired included, or a full store refuses it.
func (s *receiptStore) create(r executionReceipt) error {
	unlock, err := s.lock()
	if err != nil {
		return err
	}
	defer unlock()
	old, err := s.readLocked(r.Attempt)
	if err != nil {
		return err
	}
	if old != nil {
		return errReceiptExists
	}
	n, err := s.count()
	if err != nil {
		return err
	}
	if n >= s.limit {
		return errReceiptCapacity
	}
	r.Format, r.Generation = receiptFormat, 1
	r.Owner, r.Namespace, r.Binding = s.owner, s.namespace, s.binding
	if r.Phase != phaseIntent {
		return errReceiptCorrupt
	}
	if err := s.check(&r, r.Attempt); err != nil {
		return err
	}
	return s.writeLocked(&r)
}

// load returns the attempt's current record.
func (s *receiptStore) load(id string) (executionReceipt, error) {
	unlock, err := s.lock()
	if err != nil {
		return executionReceipt{}, err
	}
	defer unlock()
	r, err := s.readLocked(id)
	if err != nil {
		return executionReceipt{}, err
	}
	if r == nil {
		return executionReceipt{}, errReceiptMissing
	}
	if s.blocked[id] {
		return *r, errReceiptRefused
	}
	return *r, nil
}

// update applies f to the attempt's current record and durably replaces it if
// f changed it. f runs under the store lock and must not perform network I/O.
// A retired or refused record is never updated; only retire retires one and
// only refuse refuses one. The returned record is the one now stored, or the
// current one on error.
func (s *receiptStore) update(id string, f func(*executionReceipt) error) (executionReceipt, error) {
	unlock, err := s.lock()
	if err != nil {
		return executionReceipt{}, err
	}
	defer unlock()
	cur, err := s.readLocked(id)
	if err != nil {
		return executionReceipt{}, err
	}
	if cur == nil {
		return executionReceipt{}, errReceiptMissing
	}
	if cur.Phase == phaseRetired {
		return *cur, errReceiptRetired
	}
	if cur.Phase == phaseRefused || s.blocked[id] {
		return *cur, errReceiptRefused
	}
	next := *cur
	next.Objects = slices.Clone(cur.Objects)
	if err := f(&next); err != nil {
		return *cur, err
	}
	if next.Phase == phaseRetired || next.Phase == phaseRefused {
		return *cur, errReceiptCorrupt
	}
	return s.replaceLocked(cur, next)
}

// refuse records that the create response for the attempt's sent or pinned
// record, whose nonce is nonce, was received and refused, with its witness
// w. The attempt is blocked in this store before anything is read or
// written, so that a failed or uncertain write still authorizes nothing
// here; such a failure is returned and the record keeps its last durable
// phase. A record pinned meanwhile by a recovery read is refused as well and
// keeps its UID: the refusal of an actual response is not skipped because
// another read already accepted the object.
func (s *receiptStore) refuse(id, nonce string, w receiptWitness) error {
	s.mu.Lock()
	if s.blocked == nil {
		s.blocked = make(map[string]bool)
	}
	s.blocked[id] = true
	s.mu.Unlock()
	unlock, err := s.lock()
	if err != nil {
		return err
	}
	defer unlock()
	cur, err := s.readLocked(id)
	if err != nil {
		return err
	}
	if cur == nil {
		return errReceiptMissing
	}
	if cur.Nonce != nonce || cur.Phase != phaseSent && cur.Phase != phasePinned {
		return fmt.Errorf("%w: kubernetes execution receipt differs", backend.ErrIdentityMismatch)
	}
	next := *cur
	next.Objects = slices.Clone(cur.Objects)
	next.Phase, next.Witness = phaseRefused, &w
	_, err = s.replaceLocked(cur, next)
	return err
}

func (s *receiptStore) replaceLocked(cur *executionReceipt, next executionReceipt) (executionReceipt, error) {
	a, _ := json.Marshal(cur)
	b, _ := json.Marshal(&next)
	if bytes.Equal(a, b) {
		return *cur, nil
	}
	next.Generation = cur.Generation + 1
	if err := s.check(&next, cur.Attempt); err != nil {
		return *cur, err
	}
	if err := s.writeLocked(&next); err != nil {
		return *cur, err
	}
	return next, nil
}

// retire durably replaces the attempt's pinned record, after f accepts it,
// with its retired tombstone. The tombstone keeps every recorded field and
// its slot.
func (s *receiptStore) retire(id string, f func(*executionReceipt) error) error {
	unlock, err := s.lock()
	if err != nil {
		return err
	}
	defer unlock()
	cur, err := s.readLocked(id)
	if err != nil {
		return err
	}
	if cur == nil {
		return errReceiptMissing
	}
	if cur.Phase == phaseRetired {
		return errReceiptRetired
	}
	if cur.Phase == phaseRefused || s.blocked[id] {
		return errReceiptRefused
	}
	if cur.Phase != phasePinned {
		return fmt.Errorf("%w: kubernetes execution receipt is not pinned", backend.ErrIdentityMismatch)
	}
	if err := f(cur); err != nil {
		return err
	}
	next := *cur
	next.Objects = slices.Clone(cur.Objects)
	next.Phase = phaseRetired
	_, err = s.replaceLocked(cur, next)
	return err
}
