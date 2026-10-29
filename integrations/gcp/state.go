package gce

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	stateVersion  = 2
	stateName     = "state.json"
	stateTemp     = "state.json.tmp"
	lockName      = "lock"
	maxStateBytes = 1 << 20
	maxHistory    = 64
)

// Lifecycle phases of a record. A phase only advances.
const (
	phaseNew          = "new"           // nothing verified or sent
	phaseImagePinned  = "image-pinned"  // project and exact image verified
	phaseDiskVerified = "disk-verified" // own disk created and verified, no VM sent
	phaseCreated      = "created"       // own VM created and verified
	phaseImageChanged = "image-changed" // own disk holds another image; no VM may boot
)

// Operation kinds; each is one named Compute mutation.
const (
	kindDiskInsert     = "disk-insert"
	kindInstanceInsert = "instance-insert"
	kindStop           = "instance-stop"
	kindStart          = "instance-start"
	kindInstanceDelete = "instance-delete"
	kindDiskDelete     = "disk-delete"
)

// Operation states. "sent" means the original request may have reached
// Compute Engine; nothing ever moves an operation back to "prepared".
// The first four are states of the current operation, the last four of a
// retired one.
const (
	opPrepared  = "prepared"  // exact request stored, never dispatched
	opSent      = "sent"      // dispatched; reply not yet recorded
	opLost      = "lost"      // reply lost or unusable; operation unknown
	opRunning   = "running"   // correlated operation recorded, not DONE
	opDone      = "done"      // correlated DONE without error
	opFailed    = "failed"    // correlated DONE with error
	opRefused   = "refused"   // definite HTTP refusal of the original
	opWithdrawn = "withdrawn" // prepared, never sent, superseded by Absent
)

// Observed resource states recorded after a GET found nothing.
const (
	stateAbsent  = "ABSENT"             // gone after this machine's own delete of its id
	stateRemoved = "REMOVED-EXTERNALLY" // pinned id gone without an owned delete
)

// A record is the private durable state of one machine. It holds no
// credentials.
type record struct {
	Version         int         `json:"version"`
	ConfigHash      string      `json:"config_hash"`
	Config          Config      `json:"config"`
	Install         string      `json:"install"`
	InstanceName    string      `json:"instance_name"`
	DiskName        string      `json:"disk_name"`
	Phase           string      `json:"phase"`
	Desired         Goal        `json:"desired"`
	Intent          uint64      `json:"intent"`
	CompletedIntent uint64      `json:"completed_intent"`
	Restart         string      `json:"restart"` // "", "stop", "start" or "done"
	ImageID         string      `json:"image_id"`
	DiskID          string      `json:"disk_id"`
	InstanceID      string      `json:"instance_id"`
	DiskSent        bool        `json:"disk_sent"`
	InstanceSent    bool        `json:"instance_sent"`
	Op              *operation  `json:"op"`
	History         []operation `json:"history"`

	// Admission is set only by the admission check before any charged
	// mutation; a record without it may only tear down what it owns.
	// DiskCreate and InstanceCreate witness the settled successful
	// creates that pinned DiskID and InstanceID; they outlive History.
	Admission      *admission     `json:"admission"`
	DiskCreate     *createWitness `json:"disk_create"`
	InstanceCreate *createWitness `json:"instance_create"`

	InstanceState string    `json:"instance_state"`
	DiskState     string    `json:"disk_state"`
	ObservedAt    time.Time `json:"observed_at"`
	Reached       bool      `json:"reached"`
	Held          string    `json:"held"`
	Diagnostic    string    `json:"diagnostic"`
}

// An operation is one Compute mutation: the exact request prepared before
// its single dispatch and the evidence of what it did.
type operation struct {
	Kind       string    `json:"kind"`
	Intent     uint64    `json:"intent"`
	RequestID  string    `json:"request_id"`
	Method     string    `json:"method"`
	Path       string    `json:"path"`
	Query      string    `json:"query"`
	Body       []byte    `json:"body"`
	Target     string    `json:"target"`
	State      string    `json:"state"`
	PreparedAt time.Time `json:"prepared_at"`
	SentAt     time.Time `json:"sent_at"`
	Name       string    `json:"name"`
	ID         string    `json:"id"`
	TargetID   string    `json:"target_id"`
	HTTPStatus int       `json:"http_status"`
	Errors     string    `json:"errors"`
}

// An admission is the private witness that the configured image and
// machine type were each read once, through the machine's own client,
// with their exact identities and architecture ARM64, before any charged
// mutation. It binds the config hash and every observed identity. The
// exact image id is the operator's enrollment of a trusted Linux image;
// Compute Engine attests no operating system, and nothing here boots,
// connects to or inspects the VM.
type admission struct {
	Version    int    `json:"version"`
	ConfigHash string `json:"config_hash"`
	ImageID    string `json:"image_id"`
	ImageName  string `json:"image_name"`
	ImageLink  string `json:"image_link"` // canonical projects/... path of selfLink
	ImageArch  string `json:"image_architecture"`
	TypeID     string `json:"machine_type_id"` // pinned from the reply
	TypeName   string `json:"machine_type_name"`
	TypeZone   string `json:"machine_type_zone"` // bare zone name
	TypeLink   string `json:"machine_type_link"` // canonical projects/... path of selfLink
	TypeArch   string `json:"machine_type_architecture"`
}

const admissionVersion = 1

// check reports whether a is an admission of exactly r's config.
func (a *admission) check(r *record) error {
	cfg := &r.Config
	switch {
	case a.Version != admissionVersion:
		return fmt.Errorf("unsupported admission version %d", a.Version)
	case a.ConfigHash != r.ConfigHash:
		return errors.New("admission is of another config")
	case a.ImageID != cfg.Image.ID || a.ImageName != cfg.Image.Name || a.ImageLink != r.imagePath():
		return errors.New("admission image differs from the configured image")
	case checkID("machine type id", a.TypeID) != nil:
		return errors.New("admission machine type id is malformed")
	case a.TypeName != r.typeName() || a.TypeZone != cfg.Zone || a.TypeLink != r.machineTypePath():
		return errors.New("admission machine type differs from the configured machine type")
	case a.ImageArch != archARM64 || a.TypeArch != archARM64:
		return errors.New("admission architecture is not ARM64")
	}
	return nil
}

// A createWitness is the durable evidence of one settled successful
// create: its original operation, correlated and DONE without error, and
// the numeric id that operation reported. It is the only accepted source
// of a pinned resource id and is kept for the record's life, independent
// of the bounded history. It guards against a corrupted or carelessly
// edited state, not against an operator who forges one.
type createWitness struct {
	Kind        string `json:"kind"`
	RequestID   string `json:"request_id"`
	Operation   string `json:"operation"`
	OperationID string `json:"operation_id"`
	Target      string `json:"target"`
	TargetID    string `json:"target_id"`
	ImageID     string `json:"image_id"`
	Request     string `json:"request"` // hex SHA-256 of the exact request body
}

// witness returns the witness of op, a create just observed DONE.
func (r *record) witness(op *operation) *createWitness {
	return &createWitness{
		Kind:        op.Kind,
		RequestID:   op.RequestID,
		Operation:   op.Name,
		OperationID: op.ID,
		Target:      op.Target,
		TargetID:    op.TargetID,
		ImageID:     r.ImageID,
		Request:     bodyHash(op.Body),
	}
}

func bodyHash(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// checkCreate reports whether the pinned id of a create of kind agrees
// with its witness w: a nonempty id needs a witness, the witness must bind
// exactly the request kind implies for r and the pinned image, and no
// current operation or history entry may contradict it.
func (r *record) checkCreate(kind, id string, sent bool, w *createWitness) error {
	if w == nil {
		if id != "" {
			return errors.New("stored resource id has no successful create witness")
		}
		return nil
	}
	_, _, body, target, _ := r.request(kind)
	switch {
	case !sent || id == "" || w.TargetID != id:
		return errors.New("create witness does not match the pinned id")
	case w.Kind != kind || w.Target != target || w.Request != bodyHash(body) || r.ImageID == "" || w.ImageID != r.ImageID:
		return errors.New("create witness differs from the request its config and reserved names imply")
	case !uuidRE.MatchString(w.RequestID) || !opNameRE.MatchString(w.Operation) || checkID("operation id", w.OperationID) != nil:
		return errors.New("create witness ids are malformed")
	case r.Op != nil && r.Op.Kind == kind:
		return errors.New("create witness while that create is unresolved")
	}
	for _, o := range r.History {
		if o.Kind != kind {
			continue
		}
		if o.State == opDone && (o.RequestID != w.RequestID || o.Name != w.Operation || o.ID != w.OperationID || o.TargetID != w.TargetID) ||
			o.State != opDone && o.RequestID == w.RequestID {
			return errors.New("history contradicts the create witness")
		}
	}
	return nil
}

var (
	opNameRE   = regexp.MustCompile(`^[a-z][-a-z0-9]{0,127}$`)
	uuidRE     = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	vmNameRE   = regexp.MustCompile(`^sg-vm-[0-9a-f]{20}$`)
	dkNameRE   = regexp.MustCompile(`^sg-disk-[0-9a-f]{20}$`)
	resStateRE = regexp.MustCompile(`^[A-Z][-A-Z_]{0,31}$`)
)

// newUUID returns a random version 4 UUID.
func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate uuid: %w", err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:], nil
}

// newName returns prefix followed by 20 random hex digits, an RFC 1035
// name that this machine reserves forever.
func newName(prefix string) (string, error) {
	var b [10]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate name: %w", err)
	}
	return prefix + hex.EncodeToString(b[:]), nil
}

// newRecord returns the initial record for cfg with fresh reserved names.
func newRecord(cfg Config) (*record, error) {
	install, err := newUUID()
	if err != nil {
		return nil, err
	}
	vm, err := newName("sg-vm-")
	if err != nil {
		return nil, err
	}
	disk, err := newName("sg-disk-")
	if err != nil {
		return nil, err
	}
	return &record{
		Version:      stateVersion,
		ConfigHash:   cfg.hash(),
		Config:       cfg,
		Install:      install,
		InstanceName: vm,
		DiskName:     disk,
		Phase:        phaseNew,
		Desired:      Running,
		Intent:       1,
	}, nil
}

func (r *record) zonePath() string {
	return "projects/" + r.Config.Project + "/zones/" + r.Config.Zone
}
func (r *record) diskPath() string     { return r.zonePath() + "/disks/" + r.DiskName }
func (r *record) instancePath() string { return r.zonePath() + "/instances/" + r.InstanceName }

// imagePath and machineTypePath are the configured image and machine type
// under their projects; typeName is the machine type's name.
func (r *record) imagePath() string {
	return "projects/" + r.Config.Image.Project + "/global/images/" + r.Config.Image.Name
}
func (r *record) machineTypePath() string {
	return "projects/" + r.Config.Project + "/" + r.Config.MachineType
}
func (r *record) typeName() string {
	return strings.TrimPrefix(r.Config.MachineType, "zones/"+r.Config.Zone+"/machineTypes/")
}

// metadata returns the only instance metadata: the single approved key,
// blocked project-wide keys and the ownership witness.
func (r *record) metadata() []metadataItem {
	return []metadataItem{
		{Key: "ssh-keys", Value: r.Config.SSHUser + ":" + r.Config.AuthorizedKey},
		{Key: "block-project-ssh-keys", Value: "true"},
		{Key: ownerLabel, Value: r.Install},
	}
}

// request returns the one exact request of a mutation of kind on r's
// reserved resources. ok is false for an unknown kind. Every stored
// operation must carry exactly this request.
func (r *record) request(kind string) (method, path string, body []byte, target string, ok bool) {
	cfg := &r.Config
	switch kind {
	case kindDiskInsert:
		return "POST", r.zonePath() + "/disks", mustJSON(diskInsertBody{
			Name:        r.DiskName,
			SizeGb:      strconv.FormatInt(cfg.DiskGiB, 10),
			Type:        "projects/" + cfg.Project + "/" + cfg.DiskType,
			SourceImage: "projects/" + cfg.Image.Project + "/global/images/" + cfg.Image.Name,
			Labels:      map[string]string{ownerLabel: r.Install},
			Description: "skygo-gce owned boot disk",
		}), r.diskPath(), true
	case kindInstanceInsert:
		return "POST", r.zonePath() + "/instances", mustJSON(instanceInsertBody{
			Name:        r.InstanceName,
			MachineType: "projects/" + cfg.Project + "/" + cfg.MachineType,
			Disks: []attachedDiskWire{{
				Boot:       true,
				AutoDelete: false,
				Source:     r.diskPath(),
				Mode:       "READ_WRITE",
				Type:       "PERSISTENT",
			}},
			NetworkInterfaces: []networkWire{{Subnetwork: cfg.Subnet}},
			Metadata:          metadataWire{Items: r.metadata()},
			ServiceAccounts:   []json.RawMessage{},
			Labels:            map[string]string{ownerLabel: r.Install},
			Description:       "skygo-gce owned instance",
		}), r.instancePath(), true
	case kindStop:
		return "POST", r.instancePath() + "/stop", nil, r.instancePath(), true
	case kindStart:
		return "POST", r.instancePath() + "/start", nil, r.instancePath(), true
	case kindInstanceDelete:
		return "DELETE", r.instancePath(), nil, r.instancePath(), true
	case kindDiskDelete:
		return "DELETE", r.diskPath(), nil, r.diskPath(), true
	}
	return "", "", nil, "", false
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err) // fixed structs of strings
	}
	return b
}

// check reports whether r is a consistent record of this version: its
// config, reserved names, phase, ids and every stored operation agree.
func (r *record) check() error {
	if r.Version != 1 && r.Version != stateVersion {
		return fmt.Errorf("unsupported state version %d", r.Version)
	}
	if r.Version == 1 {
		if r.Intent != 0 || r.CompletedIntent != 0 {
			return errors.New("version 1 state has intent fields")
		}
	} else if r.Intent == 0 || r.CompletedIntent > r.Intent || r.Reached && r.CompletedIntent != r.Intent {
		return errors.New("stored intent or completion is invalid")
	}
	if err := r.Config.validate(); err != nil {
		return fmt.Errorf("stored config: %w", err)
	}
	if r.ConfigHash != r.Config.hash() {
		return errors.New("stored config hash does not match stored config")
	}
	if !uuidRE.MatchString(r.Install) || !vmNameRE.MatchString(r.InstanceName) || !dkNameRE.MatchString(r.DiskName) {
		return errors.New("stored names are not reserved names")
	}
	switch r.Phase {
	case phaseNew, phaseImagePinned, phaseDiskVerified, phaseCreated, phaseImageChanged:
	default:
		return fmt.Errorf("unknown phase %q", r.Phase)
	}
	if !r.Desired.valid() {
		return fmt.Errorf("unknown goal %q", r.Desired)
	}
	switch r.Restart {
	case "", "stop", "start", "done":
	default:
		return fmt.Errorf("unknown restart step %q", r.Restart)
	}
	for _, id := range []string{r.ImageID, r.DiskID, r.InstanceID} {
		if id != "" && checkID("id", id) != nil {
			return fmt.Errorf("stored id %q is not a decimal id", id)
		}
	}
	created := r.Phase == phaseDiskVerified || r.Phase == phaseCreated || r.Phase == phaseImageChanged
	switch {
	case (r.Desired == Restarted) != (r.Restart != ""):
		return errors.New("stored restart step does not match the goal")
	case r.ImageID != "" && r.ImageID != r.Config.Image.ID:
		return errors.New("stored image id differs from the configured image id")
	case (r.Phase == phaseNew) != (r.ImageID == ""):
		return errors.New("stored image id does not match the phase")
	case r.DiskID != "" && !r.DiskSent, r.InstanceID != "" && !r.InstanceSent:
		return errors.New("stored resource id has no sent create")
	case r.DiskSent && r.Phase == phaseNew:
		return errors.New("stored disk create precedes the image check")
	case r.InstanceSent && r.Phase != phaseDiskVerified && r.Phase != phaseCreated:
		return errors.New("stored instance create precedes the disk check")
	case created && r.DiskID == "", r.Phase == phaseCreated && r.InstanceID == "":
		return errors.New("stored phase has no resource id")
	case r.Reached && (r.Op != nil || r.Held != ""):
		return errors.New("stored status is reached with work outstanding")
	case !validHeld(r.Held):
		return fmt.Errorf("unknown held reason %q", r.Held)
	case len(r.Diagnostic) > 256:
		return errors.New("stored diagnostic is too long")
	case r.InstanceState != "" && !resStateRE.MatchString(r.InstanceState),
		r.DiskState != "" && !resStateRE.MatchString(r.DiskState):
		return errors.New("stored resource state is malformed")
	case len(r.History) > maxHistory:
		return errors.New("stored history is too long")
	}
	seen := map[string]bool{}
	for i := range r.History {
		o := &r.History[i]
		if err := r.checkOp(o, true); err != nil {
			return fmt.Errorf("stored history: %w", err)
		}
		if seen[o.RequestID] {
			return errors.New("stored request id is reused")
		}
		seen[o.RequestID] = true
	}
	if r.Op != nil {
		if err := r.checkOp(r.Op, false); err != nil {
			return fmt.Errorf("stored operation: %w", err)
		}
		if seen[r.Op.RequestID] {
			return errors.New("stored request id is reused")
		}
	}
	if err := r.checkCreate(kindDiskInsert, r.DiskID, r.DiskSent, r.DiskCreate); err != nil {
		return fmt.Errorf("stored disk: %w", err)
	}
	if err := r.checkCreate(kindInstanceInsert, r.InstanceID, r.InstanceSent, r.InstanceCreate); err != nil {
		return fmt.Errorf("stored instance: %w", err)
	}
	switch {
	case r.Admission == nil:
		// Never admitted, or a record from before admission: it may
		// observe and tear down what it owns, never charge anything new.
	case r.Phase == phaseNew:
		return errors.New("stored admission precedes the image check")
	default:
		if err := r.Admission.check(r); err != nil {
			return fmt.Errorf("stored admission: %w", err)
		}
	}
	return nil
}

// checkOp reports whether o is a consistent operation of r: its request
// is exactly the one its kind implies for r's config and reserved names,
// its evidence fits its state, and its state fits the record. retired
// reports whether o is in the history rather than the current operation.
func (r *record) checkOp(o *operation, retired bool) error {
	if r.Version == 1 && o.Intent != 0 || r.Version == stateVersion && (o.Intent == 0 || o.Intent > r.Intent) {
		return errors.New("operation intent does not fit the record")
	}
	method, path, body, target, ok := r.request(o.Kind)
	if !ok {
		return fmt.Errorf("unknown operation kind %q", o.Kind)
	}
	if o.Method != method || o.Path != path || o.Target != target || !bytes.Equal(o.Body, body) {
		return fmt.Errorf("%s request differs from the request its config and reserved names imply", o.Kind)
	}
	if !uuidRE.MatchString(o.RequestID) || o.Query != "requestId="+o.RequestID {
		return errors.New("request id is malformed")
	}
	named := o.Name != "" || o.ID != ""
	if named && (!opNameRE.MatchString(o.Name) || checkID("operation id", o.ID) != nil) ||
		o.TargetID != "" && checkID("target id", o.TargetID) != nil {
		return errors.New("operation ids are malformed")
	}
	sent := !o.SentAt.IsZero()
	var bad, final bool
	switch o.State {
	case opPrepared:
		bad = sent || named || o.HTTPStatus != 0
	case opSent:
		bad = !sent || named || o.HTTPStatus != 0
	case opLost:
		bad = !sent || named
	case opRunning:
		bad = !sent || !named
	case opDone:
		bad, final = !sent || !named || o.TargetID == "", true
	case opFailed:
		bad, final = !sent || !named || o.Errors == "", true
	case opRefused:
		bad, final = !sent || named || o.HTTPStatus < 400 || o.HTTPStatus > 499, true
	case opWithdrawn:
		bad, final = sent || named || o.HTTPStatus != 0 || o.Kind == kindInstanceDelete || o.Kind == kindDiskDelete, true
	default:
		return fmt.Errorf("unknown operation state %q", o.State)
	}
	switch {
	case bad, o.PreparedAt.IsZero(),
		o.State != opDone && o.TargetID != "",
		o.State != opFailed && o.Errors != "",
		len(o.Errors) > 512:
		return fmt.Errorf("%s evidence does not fit state %q", o.Kind, o.State)
	case final != retired:
		return fmt.Errorf("%s state %q is not valid here", o.Kind, o.State)
	}
	if o.State == opWithdrawn {
		return nil // never sent: binds nothing
	}
	var fits bool
	switch o.Kind {
	case kindDiskInsert:
		fits = r.DiskSent && (retired || r.Phase == phaseImagePinned)
	case kindInstanceInsert:
		fits = r.InstanceSent && (retired || r.Phase == phaseDiskVerified)
	case kindStop, kindStart:
		fits = r.InstanceID != "" && (retired || r.Phase == phaseCreated)
	case kindInstanceDelete:
		fits = r.InstanceID != "" && r.Desired == Absent
	case kindDiskDelete:
		fits = r.DiskID != "" && r.Desired == Absent
	}
	if !fits {
		return fmt.Errorf("%s does not fit the record's phase, goal or ids", o.Kind)
	}
	if o.State == opDone {
		want := r.InstanceID
		if o.Kind == kindDiskInsert || o.Kind == kindDiskDelete {
			want = r.DiskID
		}
		if o.TargetID != want {
			return fmt.Errorf("done %s target id differs from the owned id", o.Kind)
		}
	}
	return nil
}

// A closeError is the failed first Close of a file. It keeps the file:
// nothing closes it again, and whoever holds the error holds its owner.
type closeError struct {
	name string
	f    *os.File
	err  error
}

func (e *closeError) Error() string { return "close " + e.name + ": " + e.err.Error() }
func (e *closeError) Unwrap() error { return e.err }

// A lockedError is a constructor failure that left a file owner
// unsettled. It keeps that cause and the state lock, which stays held
// by this process so that no later Open in it admits work.
type lockedError struct {
	err  error
	lock *os.File
}

func (e *lockedError) Error() string {
	return e.err.Error() + "; the state directory stays locked by this process"
}
func (e *lockedError) Unwrap() error { return e.err }

// File system seams. Tests replace them to inject a failed first Close or
// directory sync; production uses the real calls.
var (
	fileClose = (*os.File).Close
	fsyncDir  = syncDir
)

// closeFile runs the first and only Close of f, named name, and returns a
// *closeError that keeps f if it fails.
func closeFile(name string, f *os.File) error {
	if err := fileClose(f); err != nil {
		return &closeError{name: name, f: f, err: err}
	}
	return nil
}

// A store is a locked state directory.
type store struct {
	dir  string
	lock *os.File // holds the directory lock until closed
}

// abandon ends a constructor that failed with err while holding s's lock.
// If err leaves a file owner unsettled, the returned error keeps it and
// the lock, which stays held. Otherwise the lock's first Close runs; if
// that fails, the returned error keeps the lock file too.
func (s *store) abandon(err error) error {
	var ce *closeError
	if errors.As(err, &ce) {
		return &lockedError{err: err, lock: s.lock}
	}
	if cerr := closeFile(lockName, s.lock); cerr != nil {
		return errors.Join(err, cerr)
	}
	return err
}

// checkDir reports whether dir is an existing private directory that is
// not a symbolic link.
func checkDir(dir string) error {
	fi, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	return checkPrivate(dir, fi, true)
}

// lockNew locks the newly opened lock file f, closing it on failure.
func lockNew(f *os.File, err error) (*os.File, error) {
	if err == nil {
		err = lockFile(f)
	}
	if err != nil {
		if cerr := closeFile(lockName, f); cerr != nil {
			return nil, errors.Join(err, cerr)
		}
		return nil, err
	}
	return f, nil
}

// createStore creates dir, which must not exist, and returns it locked
// after the new directory's entry is synced in its parent.
func createStore(dir string) (*store, error) {
	if err := os.Mkdir(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create state directory: %w", err)
	}
	if err := checkDir(dir); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, lockName), os.O_RDWR|os.O_CREATE|os.O_EXCL|openNoFollow, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create lock: %w", err)
	}
	if f, err = lockNew(f, nil); err != nil {
		return nil, err
	}
	s := &store{dir: dir, lock: f}
	if err := syncPath("parent directory", filepath.Dir(dir)); err != nil {
		return nil, s.abandon(err)
	}
	return s, nil
}

// openStore locks the existing private state directory dir.
func openStore(dir string) (*store, error) {
	if err := checkDir(dir); err != nil {
		return nil, fmt.Errorf("state directory: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, lockName), os.O_RDWR|openNoFollow, 0)
	if err != nil {
		return nil, fmt.Errorf("open lock: %w", err)
	}
	fi, err := f.Stat()
	if err == nil {
		err = checkPrivate(lockName, fi, false)
	}
	if f, err = lockNew(f, err); err != nil {
		return nil, err
	}
	return &store{dir: dir, lock: f}, nil
}

// syncPath flushes the entries of the directory dir, named name.
func syncPath(name, dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("sync %s: %w", name, err)
	}
	err = fsyncDir(d)
	if cerr := closeFile(name, d); cerr != nil {
		return errors.Join(err, cerr)
	}
	if err != nil {
		return fmt.Errorf("sync %s: %w", name, err)
	}
	return nil
}

// readState strictly reads the last committed record in dir. It takes no
// lock and creates nothing. A failed Close of the state file is returned
// as a *closeError keeping the file.
func readState(dir string) (*record, error) {
	if err := checkDir(dir); err != nil {
		return nil, fmt.Errorf("state directory: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, stateName), os.O_RDONLY|openNoFollow, 0)
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}
	fi, err := f.Stat()
	if err == nil {
		err = checkPrivate(stateName, fi, false)
	}
	var data []byte
	if err == nil {
		data, err = readBounded(f, maxStateBytes)
	}
	if cerr := closeFile(stateName, f); cerr != nil {
		return nil, errors.Join(err, cerr)
	}
	if err != nil {
		return nil, fmt.Errorf("read state: %w", err)
	}
	r := new(record)
	if err := decodeStrict(data, r, true); err != nil {
		return nil, fmt.Errorf("parse state: %w", err)
	}
	if err := intentFields(data, r.Version); err != nil {
		return nil, fmt.Errorf("state: %w", err)
	}
	if err := r.check(); err != nil {
		return nil, fmt.Errorf("state: %w", err)
	}
	if r.Version == 1 {
		// Version 1 did not distinguish completed intents. Its Reached
		// bit is the only evidence that the current goal was observed.
		// History alone never supplies that evidence.
		r.Version, r.Intent = stateVersion, 1
		if r.Reached {
			r.CompletedIntent = 1
		}
		for i := range r.History {
			r.History[i].Intent = 1
		}
		if r.Op != nil {
			r.Op.Intent = 1
		}
		if err := r.check(); err != nil {
			return nil, fmt.Errorf("upgraded state: %w", err)
		}
	}
	return r, nil
}

// intentFields requires the intent fields in version 2 and forbids them,
// even with zero values, in version 1. The whole document has already
// passed strict decoding, including duplicate-key and type checks.
func intentFields(data []byte, version int) error {
	if version != 1 && version != stateVersion {
		return fmt.Errorf("unsupported state version %d", version)
	}
	var r map[string]json.RawMessage
	if err := json.Unmarshal(data, &r); err != nil {
		return err
	}
	// Every writer emits op explicitly, including null. Requiring its
	// canonical container prevents an alias from hiding intent fields.
	if _, present := r["op"]; !present {
		return errors.New("state has no canonical op field")
	}
	want := version == stateVersion
	present := func(fields map[string]json.RawMessage, key string) bool {
		if want {
			_, ok := fields[key]
			return ok
		}
		// encoding/json also recognizes case-folded field names. A
		// version 1 record cannot carry these aliases either.
		for name := range fields {
			if strings.EqualFold(name, key) {
				return true
			}
		}
		return false
	}
	for _, key := range []string{"intent", "completed_intent"} {
		if present(r, key) != want {
			return fmt.Errorf("state version %d has inconsistent %s field", version, key)
		}
	}
	var ops []map[string]json.RawMessage
	if err := json.Unmarshal(r["history"], &ops); err != nil {
		return err
	}
	if op := r["op"]; len(op) > 0 && string(op) != "null" {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(op, &fields); err != nil {
			return err
		}
		ops = append(ops, fields)
	}
	for _, op := range ops {
		if present(op, "intent") != want {
			return fmt.Errorf("state version %d has inconsistent operation intent field", version)
		}
	}
	return nil
}

// save atomically replaces the record in s with r: it writes and syncs a
// private temporary file, renames it over the state file and syncs the
// directory. After any error the committed record may be the old one or
// r; a failed first Close is returned as a *closeError keeping its file.
func (s *store) save(r *record) error {
	if r.Version != stateVersion {
		return errors.New("refusing to save an old state version")
	}
	if err := r.check(); err != nil {
		return fmt.Errorf("refusing to save state: %w", err)
	}
	data, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	tmp := filepath.Join(s.dir, stateTemp)
	if err := os.Remove(tmp); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove stale state: %w", err)
	}
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL|openNoFollow, 0o600)
	if err != nil {
		return fmt.Errorf("write state: %w", err)
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if cerr := closeFile(stateTemp, f); cerr != nil {
		return errors.Join(err, cerr)
	}
	if err != nil {
		return fmt.Errorf("write state: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, stateName)); err != nil {
		return fmt.Errorf("commit state: %w", err)
	}
	return syncPath("state directory", s.dir)
}

// ReadStatus returns the last committed status in stateDir. It takes no
// lock, creates nothing and calls no service: the result is stored
// evidence, possibly stale, and never authority for a mutation.
func ReadStatus(stateDir string) (Status, error) {
	r, err := readState(stateDir)
	if err != nil {
		return Status{}, err
	}
	return r.status(), nil
}

// status returns the detached Status of r.
func (r *record) status() Status {
	st := Status{
		Project:          r.Config.Project,
		ComputeProjectID: r.Config.ComputeProjectID,
		Zone:             r.Config.Zone,
		InstanceName:     r.InstanceName,
		InstanceID:       r.InstanceID,
		InstanceState:    r.InstanceState,
		DiskName:         r.DiskName,
		DiskID:           r.DiskID,
		DiskState:        r.DiskState,
		ImageID:          r.ImageID,
		Desired:          r.Desired,
		ObservedAt:       r.ObservedAt,
		Reached:          r.Reached,
		Held:             r.Held,
	}
	if r.Op != nil {
		st.PendingRequestID = r.Op.RequestID
		st.PendingOperation = r.Op.Name
		st.Reached = false
	}
	return st
}
