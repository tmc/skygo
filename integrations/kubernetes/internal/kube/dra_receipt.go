package kube

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"k8s.io/apimachinery/pkg/util/validation"
)

// The DRA state root is a separately configured, private directory that holds
// only DRA records. It is never the execution state directory, inside it or
// around it. Its layout is:
//
//	skygo-dra-state.json      marker: format and owner, written once
//	<scope>/                  one per (owner, namespace, binding) DRA target
//	  lock, operation         the journal and operation locks of receipts
//	  <attempt>.json          one record per attempt
//
// Scope names hash draRecordFormat, so a legacy execution receipt scope can
// never be taken for a DRA scope. Before any lock, scope, marker, record or
// native write, preflightDRAState reads the whole root and refuses anything
// else: an unknown or legacy entry, a scope of no configured DRA target (as
// after a binding change), a marker of another format or owner, a missing
// marker beside other entries, a lock that is not an empty private file, or
// any record that does not decode and check exactly. A temporary file of the
// marker or a record (skygo-dra-state.json.tmp, <attempt>.tmp) is left only
// by an interrupted write, and its content cannot be attributed: it is
// refused, for the operator to inspect, and never removed, replaced or
// adopted. A fresh root is empty. Nothing is migrated, adopted or removed.
//
// The marker does not exclude older executables: they never read it. That
// exclusion is the operator's deployment fence (docs/dra.md).
const (
	draStateFormat  = "skygo-kubernetes-dra-state-v1"
	draRecordFormat = "skygo-kubernetes-dra-record-v1"
	draMarkerName   = "skygo-dra-state.json"
	maxDRAMarker    = 1 << 10
)

// Record states. An open record is under custody; a retired record is the
// tombstone left by completed API cleanup and authorizes nothing.
const (
	draOpen    = "open"
	draRetired = "retired"
)

// Object send phases beyond those of receipts: withdrawn means the grant was
// never consumed and never will be.
const draWithdrawn = "withdrawn"

// Execution outcomes witnessed from the exact pinned Pod.
const (
	draExecSucceeded = "succeeded"
	draExecFailed    = "failed"
)

type draMarker struct {
	Format string `json:"format"`
	Owner  string `json:"owner"`
}

// draRecord is the durable custody record of one DRA attempt: both object
// identities, declared before either create, the namespace and class pinned
// at declaration, separately consumed send grants, and the execution witness,
// sticky HOLD reason and API cleanup steps, each recorded independently. Any
// nonempty HOLD, an earlier version's reason included, fences the attempt.
type draRecord struct {
	Format       string    `json:"format"`
	Generation   uint64    `json:"generation"`
	Owner        string    `json:"owner"`
	Namespace    string    `json:"namespace"`
	Binding      string    `json:"binding"`
	Attempt      string    `json:"attempt"`
	TaskSHA256   string    `json:"task_sha256"`
	NamespaceUID string    `json:"namespace_uid"`
	ClassName    string    `json:"class_name"`
	ClassUID     string    `json:"class_uid"`
	ClassSHA256  string    `json:"class_sha256"`
	Count        int64     `json:"count"`
	State        string    `json:"state"`
	Claim        draObject `json:"claim"`
	Pod          draObject `json:"pod"`
	Cancel       string    `json:"cancel,omitempty"`
	Execution    string    `json:"execution,omitempty"`
	ExitCode     *int32    `json:"exit_code,omitempty"`
	WitnessedAt  string    `json:"witnessed_at,omitempty"`
	Hold         string    `json:"hold,omitempty"`
}

// draObject is the custody of one declared object. Send is intent, sent,
// rejected, pinned, refused or withdrawn, as for receipts. Delete records an
// API cleanup deletion (intent before, delivered after its response) and
// Gone its exact absence observed after that deletion. Seen is the UID of a
// complete 201 of exactly the declared object whose original Close was held
// or failed: truth without authority, which only a later pin of that same
// UID may follow.
type draObject struct {
	Name       string          `json:"name"`
	Nonce      string          `json:"nonce"`
	SpecSHA256 string          `json:"spec_sha256"`
	Send       string          `json:"send"`
	UID        string          `json:"uid,omitempty"`
	Seen       string          `json:"seen,omitempty"`
	Witness    *receiptWitness `json:"witness,omitempty"`
	Delete     string          `json:"delete,omitempty"`
	Gone       bool            `json:"gone,omitempty"`
}

// kind returns "ResourceClaim" or "Pod" by name shape: only a Pod is named
// exactly as its attempt's Job would be.
func (o *draObject) kind() string {
	if strings.Count(o.Name, "-") == 2 {
		return "ResourceClaim"
	}
	return "Pod"
}

// draScopeKey identifies one DRA target's scope.
type draScopeKey struct{ owner, namespace, binding string }

func draScope(k draScopeKey) string {
	data, _ := json.Marshal(struct {
		Format    string `json:"format"`
		Owner     string `json:"owner"`
		Namespace string `json:"namespace"`
		Binding   string `json:"binding"`
	}{draRecordFormat, k.owner, k.namespace, k.binding})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

var errDRARecordCorrupt = errors.New("corrupt kubernetes dra record")

// check validates r as the record of attempt id in scope k.
func (r *draRecord) check(k draScopeKey, id string) error {
	if r.Format != draRecordFormat {
		return errDRARecordCorrupt
	}
	if r.Owner != k.owner || r.Namespace != k.namespace || r.Binding != k.binding || r.Attempt != id {
		return errReceiptForeign
	}
	pod, err := jobName(id)
	claim, cerr := draClaimName(id, r.Claim.Nonce)
	if err != nil || cerr != nil || r.Generation == 0 || !isLowerHex(r.TaskSHA256, 64) ||
		!validInstance(r.NamespaceUID) || len(validation.IsDNS1123Subdomain(r.ClassName)) != 0 ||
		!validInstance(r.ClassUID) || !isLowerHex(r.ClassSHA256, 64) || r.Count < 1 || r.Count > 1<<31-1 ||
		r.Claim.Name != claim || r.Pod.Name != pod || r.Claim.Nonce == r.Pod.Nonce ||
		r.Claim.check() != nil || r.Pod.check() != nil {
		return errDRARecordCorrupt
	}
	// No Pod send before the claim is pinned.
	if r.Pod.Send != phaseIntent && r.Pod.Send != draWithdrawn && r.Claim.Send != phasePinned {
		return errDRARecordCorrupt
	}
	pinned := r.Pod.Send == phasePinned
	switch r.Cancel {
	case "":
	case stepIntent, stepDelivered:
		if !pinned {
			return errDRARecordCorrupt
		}
	default:
		return errDRARecordCorrupt
	}
	switch r.Execution {
	case "":
		if r.ExitCode != nil || r.WitnessedAt != "" {
			return errDRARecordCorrupt
		}
	case draExecSucceeded, draExecFailed:
		if _, err := time.Parse(time.RFC3339Nano, r.WitnessedAt); err != nil || !pinned ||
			r.Execution == draExecSucceeded && (r.ExitCode == nil || *r.ExitCode != 0) {
			return errDRARecordCorrupt
		}
	default:
		return errDRARecordCorrupt
	}
	if len(r.Hold) > 128 || !printable(r.Hold) {
		return errDRARecordCorrupt
	}
	// API cleanup follows only a witnessed success without cancellation, the
	// Pod's first, then the claim's. A HOLD found later stops it but does
	// not undo a recorded step.
	if r.Pod.Delete != "" && (r.Execution != draExecSucceeded || r.Cancel != "") ||
		r.Claim.Delete != "" && !r.Pod.Gone {
		return errDRARecordCorrupt
	}
	switch r.State {
	case draOpen:
	case draRetired:
		if !r.Pod.Gone || !r.Claim.Gone {
			return errDRARecordCorrupt
		}
	default:
		return errDRARecordCorrupt
	}
	return nil
}

func (o *draObject) check() error {
	if !isLowerHex(o.Nonce, 64) || !isLowerHex(o.SpecSHA256, 64) || o.Witness != nil && o.Send != phaseRefused {
		return errDRARecordCorrupt
	}
	switch o.Send {
	case phaseIntent, phaseSent, phaseRejected, draWithdrawn:
		if o.UID != "" {
			return errDRARecordCorrupt
		}
	case phasePinned:
		if !validInstance(o.UID) {
			return errDRARecordCorrupt
		}
	case phaseRefused:
		if o.Witness == nil || !draWitnessValid(o.Witness) || o.UID != "" && !validInstance(o.UID) {
			return errDRARecordCorrupt
		}
	default:
		return errDRARecordCorrupt
	}
	if o.Seen != "" && (!validInstance(o.Seen) || o.Send != phaseSent && o.Send != phasePinned && o.Send != phaseRefused ||
		o.Send == phasePinned && o.UID != o.Seen) {
		return errDRARecordCorrupt
	}
	switch o.Delete {
	case "":
		if o.Gone {
			return errDRARecordCorrupt
		}
	case stepIntent, stepDelivered:
		if o.Send != phasePinned {
			return errDRARecordCorrupt
		}
	default:
		return errDRARecordCorrupt
	}
	return nil
}

// draWitnessValid is receiptWitness.valid for the DRA mode, which also
// witnesses every complete error status it refuses (startDRA); receipts'
// own witnesses are unchanged.
func draWitnessValid(w *receiptWitness) bool {
	c := *w
	if c.Status >= 400 && c.Status <= 599 {
		c.Status = 200
	}
	return c.valid()
}

func printable(s string) bool {
	for _, c := range []byte(s) {
		if c < 0x20 || c > 0x7e {
			return false
		}
	}
	return true
}

// decodeDRARecord decodes data, the canonical encoding of the record of
// attempt id in scope k.
func decodeDRARecord(data []byte, k draScopeKey, id string) (*draRecord, error) {
	r := new(draRecord)
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(r); err != nil {
		return nil, errDRARecordCorrupt
	}
	canonical, err := json.Marshal(r)
	if err != nil || !bytes.Equal(canonical, data) {
		return nil, errDRARecordCorrupt
	}
	if err := r.check(k, id); err != nil {
		return nil, err
	}
	return r, nil
}

// preflightDRAState checks the DRA state root read-only, as described above,
// for owner and the scopes of keys. It reports whether the marker exists.
// executionState is the execution state root, which must be a different
// directory, neither containing nor contained in root.
func preflightDRAState(root, executionState, owner string, keys []draScopeKey) (bool, error) {
	if err := checkReceiptRoot(root); err != nil {
		return false, fmt.Errorf("kubernetes dra state: %w", err)
	}
	if err := checkReceiptRoot(executionState); err != nil {
		return false, err
	}
	if inside(root, executionState) || inside(executionState, root) {
		return false, errors.New("kubernetes dra state must be separate from the execution state")
	}
	a, err := os.Lstat(root)
	if err != nil {
		return false, err
	}
	b, err := os.Lstat(executionState)
	if err != nil {
		return false, err
	}
	if os.SameFile(a, b) {
		return false, errors.New("kubernetes dra state must be separate from the execution state")
	}
	dir, err := os.Open(root)
	if err != nil {
		return false, fmt.Errorf("kubernetes dra state: %w", err)
	}
	defer dir.Close()
	info, err := dir.Stat()
	if err != nil || !os.SameFile(a, info) {
		return false, errors.New("kubernetes dra state directory was replaced")
	}
	entries, err := readDirAt(dir)
	if err != nil {
		return false, fmt.Errorf("read kubernetes dra state: %w", err)
	}
	scopes := make(map[string]draScopeKey, len(keys))
	for _, k := range keys {
		scopes[draScope(k)] = k
	}
	marker := false
	var found []string
	for _, e := range entries {
		name := e.Name()
		switch {
		case name == draMarkerName:
			if err := readDRAMarker(dir, owner); err != nil {
				return false, err
			}
			marker = true
		case name == draMarkerName+".tmp":
			return false, fmt.Errorf("kubernetes dra state has an interrupted write %q", name)
		default:
			k, ok := scopes[name]
			if !ok || !e.IsDir() {
				return false, fmt.Errorf("unexpected kubernetes dra state entry %q", name)
			}
			if err := preflightDRAScope(dir, name, k); err != nil {
				return false, err
			}
			found = append(found, name)
		}
	}
	if !marker && len(found) != 0 {
		return false, errors.New("kubernetes dra state has scopes without its marker")
	}
	return marker, nil
}

func inside(dir, path string) bool {
	rel, err := filepath.Rel(dir, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// readDRAMarker validates the marker in root for owner.
func readDRAMarker(root *os.File, owner string) error {
	f, info, err := openPrivate(root, draMarkerName, os.O_RDONLY)
	if err != nil {
		return fmt.Errorf("kubernetes dra state marker: %w", err)
	}
	defer f.Close()
	if info.Size() <= 0 || info.Size() > maxDRAMarker {
		return errors.New("corrupt kubernetes dra state marker")
	}
	data, err := io.ReadAll(io.LimitReader(f, maxDRAMarker+1))
	if err != nil {
		return fmt.Errorf("read kubernetes dra state marker: %w", err)
	}
	var m draMarker
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return errors.New("corrupt kubernetes dra state marker")
	}
	canonical, err := json.Marshal(m)
	if err != nil || !bytes.Equal(canonical, data) {
		return errors.New("corrupt kubernetes dra state marker")
	}
	if m.Format != draStateFormat {
		return fmt.Errorf("unsupported kubernetes dra state format %q", m.Format)
	}
	if m.Owner != owner {
		return fmt.Errorf("%w: kubernetes dra state belongs to another installation", backend.ErrIdentityMismatch)
	}
	return nil
}

// preflightDRAScope validates every entry and record of scope name of root.
func preflightDRAScope(root *os.File, name string, k draScopeKey) error {
	scope, _, err := openPrivateDir(root, name)
	if err != nil {
		return err
	}
	defer scope.Close()
	entries, err := readDirAt(scope)
	if err != nil {
		return fmt.Errorf("read kubernetes dra state: %w", err)
	}
	if len(entries) > 2*maxReceipts+2 {
		return errReceiptCapacity
	}
	for _, e := range entries {
		entry := e.Name()
		id, ext, _ := strings.Cut(entry, ".")
		if !e.Type().IsRegular() {
			return fmt.Errorf("unsafe kubernetes dra state entry %q", entry)
		}
		switch {
		case entry == journalLockName || entry == operationLockName:
			if err := preflightDRALock(scope, entry); err != nil {
				return err
			}
		case ext == "tmp" && isLowerHex(id, 32):
			return fmt.Errorf("kubernetes dra state has an interrupted write %q", entry)
		case ext == "json" && isLowerHex(id, 32):
			if err := preflightDRARecord(scope, entry, k, id); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected kubernetes dra state entry %q", entry)
		}
	}
	return nil
}

// preflightDRALock checks that lock file entry of scope is an empty, singly
// linked private file of the server user, as the store creates it.
func preflightDRALock(scope *os.File, entry string) error {
	f, info, err := openPrivate(scope, entry, os.O_RDONLY)
	if err != nil {
		return fmt.Errorf("kubernetes dra state lock %q: %w", entry, err)
	}
	defer f.Close()
	if info.Size() != 0 {
		return fmt.Errorf("unexpected kubernetes dra state lock content %q", entry)
	}
	return nil
}

func preflightDRARecord(scope *os.File, entry string, k draScopeKey, id string) error {
	f, info, err := openPrivate(scope, entry, os.O_RDONLY)
	if err != nil {
		return fmt.Errorf("open kubernetes dra record: %w", err)
	}
	defer f.Close()
	if info.Size() <= 0 || info.Size() > maxReceiptBytes {
		return errDRARecordCorrupt
	}
	data, err := io.ReadAll(io.LimitReader(f, maxReceiptBytes+1))
	if err != nil {
		return fmt.Errorf("read kubernetes dra record: %w", err)
	}
	_, err = decodeDRARecord(data, k, id)
	return err
}

// openPrivateDir opens the private directory name of dir without following
// a symlink.
func openPrivateDir(dir *os.File, name string) (*os.File, os.FileInfo, error) {
	f, err := openAt(dir, name, os.O_RDONLY)
	if err != nil {
		return nil, nil, err
	}
	info, err := f.Stat()
	if err == nil {
		err = checkPrivate(info, true)
	}
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return f, info, nil
}

// openDRAState preflights root and only then writes: the marker, if absent,
// and each scope of keys, then opens a store per scope. On error it may have
// created the marker or empty scopes, which a restart with the same
// configuration accepts.
func openDRAState(root, executionState, owner string, keys []draScopeKey) (map[draScopeKey]*draStore, error) {
	marker, err := preflightDRAState(root, executionState, owner, keys)
	if err != nil {
		return nil, err
	}
	rootID, err := dirID(root)
	if err != nil {
		return nil, err
	}
	if !marker {
		if err := writeDRAMarker(root, rootID, owner); err != nil {
			return nil, err
		}
	}
	stores := make(map[draScopeKey]*draStore, len(keys))
	for _, k := range keys {
		s := &receiptStore{root: root, scope: draScope(k), owner: k.owner, namespace: k.namespace, binding: k.binding,
			rootID: rootID, limit: maxReceipts, seen: make(map[string]uint64), blocked: make(map[string]bool)}
		s.dir = filepath.Join(root, s.scope)
		if s.scopeID, err = makeScope(root, rootID, s.scope, func() error { return nil }); err != nil {
			return nil, fmt.Errorf("create kubernetes dra state: %w", err)
		}
		stores[k] = &draStore{s: s, key: k}
	}
	return stores, nil
}

// writeDRAMarker durably writes the marker of a fresh root.
func writeDRAMarker(root string, rootID fileID, owner string) error {
	data, _ := json.Marshal(draMarker{Format: draStateFormat, Owner: owner})
	dir, err := os.Open(root)
	if err != nil {
		return err
	}
	defer dir.Close()
	if id, err := dirID(root); err != nil || id != rootID {
		return errors.New("kubernetes dra state directory was replaced")
	}
	// Preflight refused any existing temporary file; O_EXCL refuses one
	// created since, which is never removed or replaced.
	tmp := draMarkerName + ".tmp"
	f, err := openAt(dir, tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL)
	if err != nil {
		return fmt.Errorf("write kubernetes dra state marker: %w", err)
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = renameAt(dir, tmp, draMarkerName)
	}
	if err == nil {
		err = syncAt(dir)
	}
	if err != nil {
		return fmt.Errorf("write kubernetes dra state marker: %w", err)
	}
	return nil
}

// draStore is the DRA journal of one scope. It shares the receipt store's
// locks, operation permits, private file handling and durable write
// sequence; only the record format differs. No method performs network I/O.
type draStore struct {
	s   *receiptStore
	key draScopeKey
	// fault, when set by tests, fails the named step of a record write or
	// the send step; r is the record being written.
	fault func(step string, r *draRecord) error
}

func (d *draStore) step(name string, r *draRecord) error {
	if d.fault == nil {
		return nil
	}
	return d.fault(name, r)
}

func (d *draStore) readLocked(id string) (*draRecord, error) {
	data, err := d.s.readEntryLocked(id)
	if err != nil || data == nil {
		return nil, err
	}
	r, err := decodeDRARecord(data, d.key, id)
	if err != nil {
		return nil, err
	}
	if r.Generation < d.s.seen[id] {
		return nil, fmt.Errorf("%w: kubernetes dra record generation rolled back", backend.ErrIdentityMismatch)
	}
	d.s.seen[id] = r.Generation
	return r, nil
}

func (d *draStore) writeLocked(r *draRecord) error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if err := d.step("write", r); err != nil {
		return fmt.Errorf("write kubernetes dra record: %w", err)
	}
	// The shared writer replaces a temporary file left by an interrupted
	// write; in a DRA scope one is unattributable evidence, so it is refused.
	tmp, err := receiptEntry(r.Attempt, "tmp")
	if err != nil {
		return err
	}
	if f, err := openAt(d.s.held, tmp, os.O_RDONLY); err == nil {
		f.Close()
		return fmt.Errorf("kubernetes dra state has an interrupted write %q", tmp)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("write kubernetes dra record: %w", err)
	}
	if err := d.s.writeEntryLocked(r.Attempt, r.Generation, data, nil); err != nil {
		return err
	}
	if err := d.step("dirsync", r); err != nil {
		return receiptUncertain{err}
	}
	return nil
}

// create durably records a new attempt. Any existing record, a tombstone
// included, or a full scope refuses it.
func (d *draStore) create(r draRecord) error {
	unlock, err := d.s.lock()
	if err != nil {
		return err
	}
	defer unlock()
	old, err := d.readLocked(r.Attempt)
	if err != nil {
		return err
	}
	if old != nil {
		return errReceiptExists
	}
	n, err := d.s.count()
	if err != nil {
		return err
	}
	if n >= d.s.limit {
		return errReceiptCapacity
	}
	r.Format, r.Generation, r.State = draRecordFormat, 1, draOpen
	r.Owner, r.Namespace, r.Binding = d.key.owner, d.key.namespace, d.key.binding
	if r.Claim.Send != phaseIntent || r.Pod.Send != phaseIntent {
		return errDRARecordCorrupt
	}
	if err := r.check(d.key, r.Attempt); err != nil {
		return err
	}
	return d.writeLocked(&r)
}

// load returns the attempt's current record. A record whose create response
// this store refused is returned with errReceiptRefused.
func (d *draStore) load(id string) (draRecord, error) {
	unlock, err := d.s.lock()
	if err != nil {
		return draRecord{}, err
	}
	defer unlock()
	r, err := d.readLocked(id)
	if err != nil {
		return draRecord{}, err
	}
	if r == nil {
		return draRecord{}, errReceiptMissing
	}
	if d.s.blocked[id] {
		return *r, errReceiptRefused
	}
	return *r, nil
}

// update applies f to the attempt's current record under the journal lock
// and durably replaces it if f changed it. A retired record is never
// updated. f must not perform network I/O. The returned record is the one
// now stored, or the current one on error.
func (d *draStore) update(id string, f func(*draRecord) error) (draRecord, error) {
	unlock, err := d.s.lock()
	if err != nil {
		return draRecord{}, err
	}
	defer unlock()
	cur, err := d.readLocked(id)
	if err != nil {
		return draRecord{}, err
	}
	if cur == nil {
		return draRecord{}, errReceiptMissing
	}
	if cur.State == draRetired {
		return *cur, errReceiptRetired
	}
	if d.s.blocked[id] {
		return *cur, errReceiptRefused
	}
	next := *cur
	if err := f(&next); err != nil {
		return *cur, err
	}
	return d.replaceLocked(cur, next)
}

// hold durably records reason as the attempt's HOLD, unless it has one: the
// first reason stays, never overwritten, and every HOLD is the same fence
// (draRuntime.authority), so an existing one already records what reason
// would. Unlike update it also holds an attempt this store refused, since a
// HOLD authorizes nothing. A retired record, a tombstone that authorizes
// nothing either, is left unchanged.
func (d *draStore) hold(id, reason string) error {
	unlock, err := d.s.lock()
	if err != nil {
		return err
	}
	defer unlock()
	cur, err := d.readLocked(id)
	if err != nil {
		return err
	}
	if cur == nil {
		return errReceiptMissing
	}
	if cur.State == draRetired || cur.Hold != "" {
		return nil
	}
	next := *cur
	next.Hold = reason
	_, err = d.replaceLocked(cur, next)
	return err
}

func (d *draStore) replaceLocked(cur *draRecord, next draRecord) (draRecord, error) {
	a, _ := json.Marshal(cur)
	b, _ := json.Marshal(&next)
	if bytes.Equal(a, b) {
		return *cur, nil
	}
	next.Generation = cur.Generation + 1
	if err := next.check(d.key, cur.Attempt); err != nil {
		return *cur, err
	}
	if err := d.writeLocked(&next); err != nil {
		return *cur, err
	}
	return next, nil
}

// refuse records that the create response for the attempt's claim, or its
// Pod if pod, sent with nonce, was received and refused, with witness w. The
// attempt is blocked in this store first, so that a failed or uncertain write
// still authorizes nothing more here. A claim refusal also withdraws the
// unsent Pod grant. An object pinned meanwhile by a recovery read is refused
// as well and keeps its UID.
func (d *draStore) refuse(id string, pod bool, nonce string, w receiptWitness) error {
	d.s.mu.Lock()
	d.s.blocked[id] = true
	d.s.mu.Unlock()
	unlock, err := d.s.lock()
	if err != nil {
		return err
	}
	defer unlock()
	cur, err := d.readLocked(id)
	if err != nil {
		return err
	}
	if cur == nil {
		return errReceiptMissing
	}
	next := *cur
	o := &next.Claim
	if pod {
		o = &next.Pod
	}
	if o.Nonce != nonce || o.Send != phaseSent && o.Send != phasePinned {
		return fmt.Errorf("%w: kubernetes dra record differs", backend.ErrIdentityMismatch)
	}
	o.Send, o.Witness = phaseRefused, &w
	if !pod && next.Pod.Send == phaseIntent {
		next.Pod.Send = draWithdrawn
	}
	_, err = d.replaceLocked(cur, next)
	return err
}

// list returns the attempts that have a record in the scope.
func (d *draStore) list() ([]string, error) {
	unlock, err := d.s.lock()
	if err != nil {
		return nil, err
	}
	defer unlock()
	if _, err := d.s.count(); err != nil {
		return nil, err
	}
	entries, err := readDirAt(d.s.held)
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, e := range entries {
		if id, ext, _ := strings.Cut(e.Name(), "."); ext == "json" && isLowerHex(id, 32) {
			ids = append(ids, id)
		}
	}
	return ids, nil
}
