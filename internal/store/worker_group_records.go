package store

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/workeradmission"
	bolt "go.etcd.io/bbolt"
)

// The worker_groups bucket, installed empty by the schema10 migration, holds
// flat typed rows for dedicated worker groups:
//
//	g/<job>          group record
//	m/<job>/<rank>   member record; rank is one byte
//
// The bucket sequence counts accepted groups. Prune removes the rows of a
// Done group with its job, so the sequence is a high-water mark, not a count.
var workerGroupsBucket = []byte("worker_groups")

const (
	// maxLiveWorkerGroups bounds groups that are not Done.
	maxLiveWorkerGroups = 64
	workerGroupLimit    = 2048
	workerMemberLimit   = 4096
	workerErrorLimit    = 256
	workerRevisionLimit = 128
	workerNodeLimit     = 253
)

// workerDemandLimit bounds one group's whole demand so that the sum over
// maxLiveWorkerGroups groups cannot overflow admission arithmetic.
var workerDemandLimit = workeradmission.Budget{Ranks: backend.MaxWorkers, CPUMillis: math.MaxUint64 / 64, MemoryBytes: math.MaxUint64 / 64, Accelerators: math.MaxUint64 / 64}

// workerGroupReserve is the MetadataBytes charge of a group of n ranks: the
// group row bound plus n member row bounds. Prune refunds it with the job.
func workerGroupReserve(n uint8) uint64 {
	return workerGroupLimit + workerMemberLimit*uint64(n)
}

func workerGroupKey(id string) []byte { return []byte("g/" + id) }
func workerMemberKey(id string, rank uint8) []byte {
	return append([]byte("m/"+id+"/"), rank)
}

// workerGen is this process's intent generation. bbolt's file lock admits one
// Store per database, so an intent of another generation has no live call.
var workerGen = sync.OnceValue(func() [16]byte {
	var b [16]byte
	for b == ([16]byte{}) {
		rand.Read(b[:])
	}
	return b
})

type workerGroupRecord struct {
	Job        string                    `json:"job"`
	Principal  string                    `json:"principal"`
	Domain     RequestDomain             `json:"domain"`
	RequestID  string                    `json:"request_id"`
	Authority  auth.Delegation           `json:"authority"`
	TaskDigest string                    `json:"task_digest"`
	Spec       backend.WorkerSpec        `json:"spec"`
	SpecDigest string                    `json:"spec_digest"`
	Demand     workeradmission.Demand    `json:"demand"`
	Phase      WorkerGroupPhase          `json:"phase"`
	Reserved   bool                      `json:"reserved"`
	Revoked    bool                      `json:"revoked"`
	Revision   uint64                    `json:"revision"`
	CreatedAt  time.Time                 `json:"created_at"`
	Barrier    *workerBarrierGroupRecord `json:"barrier,omitempty"`
}

type workerEffectRecord struct {
	State     WorkerIntent `json:"state"`
	Seq       uint64       `json:"seq"`
	Gen       string       `json:"gen"`
	At        time.Time    `json:"at"`
	Error     string       `json:"error"`
	Unsettled bool         `json:"unsettled,omitempty"`
}

type workerPodRecord struct {
	Name string `json:"name"`
	UID  string `json:"uid"`
}

type workerObservedRecord struct {
	State     backend.State `json:"state"`
	Gated     bool          `json:"gated"`
	Container string        `json:"container"`
	Started   time.Time     `json:"started"`
	ExitCode  *int32        `json:"exit_code"`
	Finished  time.Time     `json:"finished"`
	Node      string        `json:"node"`
	At        time.Time     `json:"at"`
}

type workerMemberRecord struct {
	Job        string                     `json:"job"`
	Rank       uint8                      `json:"rank"`
	Counter    uint64                     `json:"counter"`
	Create     workerEffectRecord         `json:"create"`
	Release    workerEffectRecord         `json:"release"`
	Cancel     workerEffectRecord         `json:"cancel"`
	Remove     workerEffectRecord         `json:"remove"`
	Pod        workerPodRecord            `json:"pod"`
	ReleaseAck bool                       `json:"release_ack"`
	Revision   string                     `json:"revision"`
	CancelAck  bool                       `json:"cancel_ack"`
	Removed    bool                       `json:"removed"`
	Observed   *workerObservedRecord      `json:"observed"`
	Barrier    *workerBarrierMemberRecord `json:"barrier,omitempty"`
}

func (m *workerMemberRecord) effect(e WorkerEffect) *workerEffectRecord {
	switch e {
	case WorkerCreate:
		return &m.Create
	case WorkerRelease:
		return &m.Release
	case WorkerCancel:
		return &m.Cancel
	case WorkerRemove:
		return &m.Remove
	case WorkerStart:
		if m.Barrier != nil {
			return &m.Barrier.Start
		}
	case WorkerRecover:
		if m.Barrier != nil {
			return &m.Barrier.Recover
		}
	}
	return nil
}

func (m *workerMemberRecord) effects() []*workerEffectRecord {
	out := []*workerEffectRecord{&m.Create, &m.Release, &m.Cancel, &m.Remove}
	if m.Barrier != nil {
		out = append(out, &m.Barrier.Start, &m.Barrier.Recover)
	}
	return out
}

func (m *workerMemberRecord) pod() backend.WorkerResource {
	return backend.WorkerResource{Name: m.Pod.Name, UID: m.Pod.UID}
}

// settled reports that no call is in flight and the member is either never
// created or retired by UID with stop or never-released proof. A member whose
// Create is Unknown is never settled, whatever removal evidence it has.
func (m *workerMemberRecord) settled() bool {
	if m.Barrier != nil {
		return workerBarrierMemberSettled(*m)
	}
	for _, e := range m.effects() {
		if e.State == WorkerIntended {
			return false
		}
	}
	switch m.Create.State {
	case WorkerNone:
		return true
	case WorkerUnknown:
		return false
	}
	exited := m.Observed != nil && m.Observed.ExitCode != nil
	return m.Pod.UID != "" && m.Removed && (exited || m.Release.State == WorkerNone)
}

// workerText deterministically sanitizes non-identity text: bytes outside
// [A-Za-z0-9._:/-] become '_' and the result is cut to limit bytes.
func workerText(s string, limit int) string {
	b := []byte(s)
	if len(b) > limit {
		b = b[:limit]
	}
	for i, c := range b {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.IndexByte("._:/-", c) >= 0) {
			b[i] = '_'
		}
	}
	return string(b)
}

// workerTime reports whether t is zero, or UTC within years 1..9999.
func workerTime(t time.Time) bool {
	return t.Location() == time.UTC && (t.IsZero() || t.Year() >= 1 && t.Year() <= 9999)
}

func workerGenString(g [16]byte) string { return hex.EncodeToString(g[:]) }

func workerSpecDigest(s backend.WorkerSpec) string {
	raw, _ := json.Marshal(s)
	d := sha256.Sum256(raw)
	return hex.EncodeToString(d[:])
}

func workerSpecValid(s backend.WorkerSpec) bool {
	n, err := backend.NormalizeWorkers(&s)
	return err == nil && n != nil && *n == s && s.Mode == backend.WorkerIndependent
}

// workerDemandValid reports whether d is an acceptable demand for spec.
func workerDemandValid(spec backend.WorkerSpec, d workeradmission.Demand) bool {
	return d.Ranks == uint32(spec.Count) && d.CPUMillis > 0 && d.MemoryBytes > 0 && workeradmission.Fits(workerDemandLimit, d) == nil
}

// workerTaskValid reports whether t can run as a dedicated group.
func workerTaskValid(t backend.Task) bool {
	return t.Retry == nil && t.Workdir == nil && len(t.Setup) == 0 && len(t.Outputs) == 0 && len(t.DataMounts) == 0
}

func encodeWorkerGroupRecord(r workerGroupRecord) ([]byte, error) {
	if !validAttemptID(r.Job) || !validPrincipal([]byte(r.Principal)) || !validRetainedRequestID(r.RequestID) || !validRequestDomain(r.Domain) || !validWorkflowAuthority(r.Authority, r.Principal) || !artifactHex(r.TaskDigest, 64) {
		return nil, errors.New("invalid worker group identity")
	}
	if !(r.Barrier == nil && workerSpecValid(r.Spec) || r.Barrier != nil && workerBarrierSpecValid(r.Spec)) || r.SpecDigest != workerSpecDigest(r.Spec) || !workerDemandValid(r.Spec, r.Demand) {
		return nil, errors.New("invalid worker group spec")
	}
	if r.Phase > WorkerGroupDone || r.Reserved != (r.Phase == WorkerGroupAdmitted || r.Phase == WorkerGroupActive || r.Phase == WorkerGroupHeld) || r.Revision == 0 || r.CreatedAt.IsZero() || !workerTime(r.CreatedAt) {
		return nil, errors.New("invalid worker group record")
	}
	if r.Barrier != nil {
		if err := validWorkerBarrierGroupRecord(*r.Barrier); err != nil {
			return nil, err
		}
	}
	data, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if len(data) > workerRecordGroupLimit(r) {
		return nil, errors.New("worker group record exceeds limit")
	}
	return data, nil
}

func decodeWorkerGroupRecord(raw []byte) (r workerGroupRecord, err error) {
	if err = workerStrictDecode(raw, &r); err != nil {
		return r, errors.New("invalid worker group record")
	}
	data, err := encodeWorkerGroupRecord(r)
	if err != nil {
		return r, err
	}
	if !bytes.Equal(data, raw) {
		return r, errors.New("invalid worker group record encoding")
	}
	return r, nil
}

func encodeWorkerMemberRecord(m workerMemberRecord) ([]byte, error) {
	if err := validWorkerMember(m); err != nil {
		return nil, err
	}
	data, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	limit := workerMemberLimit
	if m.Barrier != nil {
		limit = workerBarrierMemberLimit
	}
	if len(data) > limit {
		return nil, errors.New("worker member record exceeds limit")
	}
	return data, nil
}

func decodeWorkerMemberRecord(raw []byte) (m workerMemberRecord, err error) {
	if err = workerStrictDecode(raw, &m); err != nil {
		return m, errors.New("invalid worker member record")
	}
	data, err := encodeWorkerMemberRecord(m)
	if err != nil {
		return m, err
	}
	if !bytes.Equal(data, raw) {
		return m, errors.New("invalid worker member record encoding")
	}
	return m, nil
}

func workerStrictDecode(raw []byte, v any) error {
	if raw == nil {
		return ErrNotFound
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(v) != nil || d.Decode(new(any)) != io.EOF {
		return errors.New("invalid worker record")
	}
	return nil
}

// validWorkerMember checks every member invariant that needs no other row.
func validWorkerMember(m workerMemberRecord) error {
	if !validAttemptID(m.Job) || m.Rank >= backend.MaxWorkers {
		return errors.New("invalid worker member identity")
	}
	seqs := map[uint64]bool{}
	none := true
	for _, e := range m.effects() {
		set := e.State != WorkerNone
		if e.State > WorkerUnknown || set != (e.Seq != 0) || set != (e.Gen != "") || set != !e.At.IsZero() || !workerTime(e.At) {
			return errors.New("invalid worker effect state")
		}
		if set {
			none = false
			g, err := hex.DecodeString(e.Gen)
			if err != nil || !artifactHex(e.Gen, 32) || bytes.Equal(g, make([]byte, 16)) {
				return errors.New("invalid worker effect generation")
			}
			if e.Seq > m.Counter || seqs[e.Seq] {
				return errors.New("invalid worker effect sequence")
			}
			seqs[e.Seq] = true
		}
		if e.Unsettled && (m.Barrier == nil || !m.Barrier.LocalUnsettled || e.State != WorkerRecorded && e.State != WorkerUnknown || e.Error == "") {
			return errors.New("invalid worker unsettled effect")
		}
		if e.Error != workerText(e.Error, workerErrorLimit) || e.Error != "" && e.State != WorkerUnknown && !(e.State == WorkerRecorded && (e == &m.Create || m.Barrier != nil)) {
			return errors.New("invalid worker effect error")
		}
	}
	if none != (m.Counter == 0) {
		return errors.New("invalid worker member counter")
	}
	if m.CancelAck && m.Cancel.State != WorkerRecorded || (m.ReleaseAck || m.Revision != "") && m.Release.State != WorkerRecorded || m.Revision != workerText(m.Revision, workerRevisionLimit) {
		return errors.New("invalid worker acknowledgement")
	}
	pod := m.Pod != (workerPodRecord{})
	if pod && m.Create.State != WorkerRecorded && m.Create.State != WorkerUnknown || m.Create.State == WorkerRecorded && m.Pod.UID == "" && m.Barrier == nil {
		return errors.New("invalid worker pod")
	}
	if m.Release.State != WorkerNone && m.Create.State != WorkerRecorded || m.Cancel.State != WorkerNone && m.Pod.UID == "" || m.Remove.State != WorkerNone && m.Pod.UID == "" && m.Barrier == nil {
		return errors.New("invalid worker effect order")
	}
	if m.Removed && (m.Pod.UID == "" || m.Remove.State != WorkerRecorded && !(m.Barrier != nil && (m.Remove.State == WorkerUnknown || m.Barrier.Recover.State == WorkerRecorded))) {
		return errors.New("invalid worker removal")
	}
	if o := m.Observed; o != nil {
		if m.Pod.UID == "" {
			return errors.New("worker observation without pod")
		}
		if err := validWorkerObserved(*o); err != nil {
			return err
		}
	}
	if m.Barrier != nil {
		return validWorkerBarrierMember(m)
	}
	return nil
}

// validWorkerObserved is the exact image of RecordWorkerObservation's write
// rule.
func validWorkerObserved(o workerObservedRecord) error {
	if !workerTime(o.Started) || !workerTime(o.Finished) || !workerTime(o.At) || o.At.IsZero() || o.Node != workerText(o.Node, workerNodeLimit) || o.Container != "" && !validInstance(o.Container) {
		return errors.New("invalid worker observation")
	}
	exit := o.ExitCode != nil
	ran := o.Container != "" && !o.Started.IsZero()
	switch o.State {
	case backend.Succeeded, backend.Failed:
		if !exit || (*o.ExitCode == 0) != (o.State == backend.Succeeded) || *o.ExitCode < 0 || *o.ExitCode > 255 || !ran || o.Finished.IsZero() || o.Finished.Before(o.Started) {
			return errors.New("invalid worker exit observation")
		}
	case backend.Running:
		if exit || !ran || !o.Finished.IsZero() {
			return errors.New("invalid worker running observation")
		}
	case backend.Queued, backend.Starting:
		if exit || o.Container != "" || !o.Started.IsZero() || !o.Finished.IsZero() {
			return errors.New("invalid worker waiting observation")
		}
	case backend.Unknown:
		if exit || !o.Finished.IsZero() || (o.Container != "") != !o.Started.IsZero() {
			return errors.New("invalid worker unknown observation")
		}
	default:
		return errors.New("invalid worker observation state")
	}
	return nil
}

// workerGroupJob reports whether id is a dedicated worker group's job.
func workerGroupJob(tx *bolt.Tx, id string) bool {
	b := tx.Bucket(workerGroupsBucket)
	return b != nil && b.Get(workerGroupKey(id)) != nil
}

// workerGroupLive reports whether id is a worker group that is not Done.
func workerGroupLive(tx *bolt.Tx, id string) (bool, error) {
	b := tx.Bucket(workerGroupsBucket)
	if b == nil || b.Get(workerGroupKey(id)) == nil {
		return false, nil
	}
	g, err := decodeWorkerGroupRecord(b.Get(workerGroupKey(id)))
	return g.Phase != WorkerGroupDone, err
}

// workerGroupUsage rederives the bucket's MetadataBytes charge: one reserve
// per group row. Member rows are covered by their group's reserve.
func workerGroupUsage(tx *bolt.Tx) (n uint64, err error) {
	b := tx.Bucket(workerGroupsBucket)
	if b == nil || schemaNumber(tx) < 10 {
		return 0, nil
	}
	c := b.Cursor()
	for k, v := c.Seek([]byte("g/")); k != nil && bytes.HasPrefix(k, []byte("g/")); k, v = c.Next() {
		g, e := decodeWorkerGroupRecord(v)
		if e != nil {
			return 0, e
		}
		if n, e = addChecked(n, workerRecordReserve(g)); e != nil {
			return 0, e
		}
	}
	return n, nil
}

// dropWorkerGroup plans the removal of a Done group's rows with its pruned
// job and refunds the group reserve. A job that is not a group is unchanged.
func (m *retentionMutation) dropWorkerGroup(id string) error {
	b := m.tx.Bucket(workerGroupsBucket)
	if b == nil || b.Get(workerGroupKey(id)) == nil {
		return nil
	}
	g, err := decodeWorkerGroupRecord(b.Get(workerGroupKey(id)))
	if err != nil {
		return err
	}
	if g.Phase != WorkerGroupDone {
		return errors.New("worker group is not done")
	}
	for r := range g.Spec.Count {
		if err := m.coreDelete(workerGroupsBucket, workerMemberKey(id, r)); err != nil {
			return err
		}
	}
	if err := m.coreDelete(workerGroupsBucket, workerGroupKey(id)); err != nil {
		return err
	}
	return m.adjust(MetadataBytesQuota, 0, workerRecordReserve(g))
}

// migrateWorkerGroupFence validates the genuine schema9 installation before
// installing the empty worker group bucket and marker10. No existing row is
// rewritten. The caller owns the transaction.
func migrateWorkerGroupFence(tx *bolt.Tx) error {
	if schemaNumber(tx) != 9 || tx.Bucket(workerGroupsBucket) != nil {
		return ErrArtifactConflict
	}
	if err := validateSnapshot(tx); err != nil {
		return err
	}
	if _, err := tx.CreateBucket(workerGroupsBucket); err != nil {
		return err
	}
	return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(10))
}

// validateWorkerGroups checks every group and member row against its job,
// request, principal and retained metadata, and the phase table. Schema10
// requires the bucket; an earlier schema must not contain it.
func validateWorkerGroups(tx *bolt.Tx) error {
	b := tx.Bucket(workerGroupsBucket)
	if schemaNumber(tx) < 10 {
		if b != nil {
			return errors.New("worker group metadata requires schema 10")
		}
		return nil
	}
	if b == nil {
		return errors.New("missing worker group metadata")
	}
	groups := map[string]workerGroupRecord{}
	members := map[string]map[uint8]workerMemberRecord{}
	if err := b.ForEach(func(k, v []byte) error {
		if v == nil {
			return errors.New("unexpected worker group nested bucket")
		}
		switch key := string(k); {
		case strings.HasPrefix(key, "g/"):
			g, err := decodeWorkerGroupRecord(v)
			if err != nil {
				return err
			}
			if g.Job != key[2:] {
				return errors.New("worker group key mismatch")
			}
			groups[g.Job] = g
		case strings.HasPrefix(key, "m/"):
			m, err := decodeWorkerMemberRecord(v)
			if err != nil {
				return err
			}
			if !bytes.Equal(k, workerMemberKey(m.Job, m.Rank)) {
				return errors.New("worker member key mismatch")
			}
			if members[m.Job] == nil {
				members[m.Job] = map[uint8]workerMemberRecord{}
			}
			members[m.Job][m.Rank] = m
		default:
			return errors.New("unknown worker group row")
		}
		return nil
	}); err != nil {
		return err
	}
	if uint64(len(groups)) > b.Sequence() {
		return errors.New("worker group count exceeds sequence")
	}
	for id := range members {
		if _, ok := groups[id]; !ok {
			return errors.New("orphan worker member")
		}
	}
	live := 0
	for id, g := range groups {
		if g.Phase != WorkerGroupDone {
			live++
		}
		ms := make([]workerMemberRecord, g.Spec.Count)
		for r, m := range members[id] {
			if r >= g.Spec.Count {
				return errors.New("worker member rank out of range")
			}
			ms[r] = m
		}
		if len(members[id]) != int(g.Spec.Count) {
			return errors.New("missing worker member")
		}
		if err := validateWorkerGroup(tx, g, ms); err != nil {
			return err
		}
	}
	if live > maxLiveWorkerGroups {
		return errors.New("too many live worker groups")
	}
	return nil
}

func validateWorkerGroup(tx *bolt.Tx, g workerGroupRecord, ms []workerMemberRecord) error {
	j, err := get(tx, g.Job)
	if err != nil {
		return errors.New("worker group has no job")
	}
	digest, err := taskDigest(j.Task)
	if err != nil || hex.EncodeToString(digest[:]) != g.TaskDigest || !workerTaskValid(j.Task) || len(j.Attempts) != 0 {
		return errors.New("invalid worker group task")
	}
	if j.Target == "" || len(j.Target) > 128 || !validBinding(j.TargetBinding) || j.Status.Instance != "" || j.Status.Termination != backend.UnknownTermination || j.Status.ExitCode != nil {
		return errors.New("invalid worker group job")
	}
	if isScoped(tx) && string(tx.Bucket(jobPrincipalsBucket).Get([]byte(g.Job))) != g.Principal {
		return errors.New("worker group principal mismatch")
	}
	var meta retainedJob
	if err := loadJSON(retentionTable(tx, retainedJobsBucket), []byte(g.Job), &meta); err != nil {
		return errors.New("worker group has no retained metadata")
	}
	if meta.Principal != g.Principal || meta.Domain != g.Domain || meta.RequestID != g.RequestID || meta.WorkflowID != "" || meta.Legacy || meta.Started || meta.Storage != backend.RemoteOutput || meta.OutputReserve != 0 {
		return errors.New("worker group retained metadata mismatch")
	}
	var r requestRecord
	switch err := loadJSON(retentionTable(tx, retainedRequestsBucket), requestKey(g.Principal, g.Domain, g.RequestID), &r); {
	case err == nil:
		if r.JobID != g.Job || r.TaskDigest != digest || r.WorkflowID != "" {
			return errors.New("worker group request mismatch")
		}
	case !errors.Is(err, ErrNotFound):
		return err
	}
	if tx.Bucket(attemptJobsBucket).Get([]byte(g.Job)) != nil || remoteRoute(tx, g.Job) || retentionTable(tx, cleanupHeadersBucket).Get([]byte(g.Job)) != nil {
		return errors.New("worker group has an ordinary relation")
	}
	if g.Barrier != nil {
		return validateWorkerBarrierGroup(tx, g, ms)
	}
	for _, m := range ms {
		if m.Barrier != nil {
			return errors.New("mixed worker barrier records")
		}
	}
	ref := workerGroupIdentity(j, g)
	created, unknown, released := 0, 0, true
	for r, m := range ms {
		if m.Job != g.Job || int(m.Rank) != r {
			return errors.New("invalid worker member")
		}
		if m.Pod != (workerPodRecord{}) {
			if err := backend.ValidateWorkerReference(backend.WorkerReference{Group: ref, Rank: m.Rank, Pod: m.pod()}, false); err != nil {
				return errors.New("invalid worker pod identity")
			}
		}
		switch m.Create.State {
		case WorkerUnknown:
			unknown++
			created++
		case WorkerIntended:
			created++
		case WorkerRecorded:
			created++
		}
		released = released && m.Create.State == WorkerRecorded
	}
	for _, m := range ms {
		if m.Release.State != WorkerNone && !released {
			return errors.New("worker release before every create")
		}
	}
	state := j.Status.State
	switch g.Phase {
	case WorkerGroupQueued, WorkerGroupAdmitted:
		for _, m := range ms {
			if m.Counter != 0 {
				return errors.New("worker group intent before activation")
			}
		}
		if state != backend.Queued {
			return errors.New("invalid queued worker group status")
		}
	case WorkerGroupActive:
		if created == 0 || unknown != 0 || state == backend.Queued {
			return errors.New("invalid active worker group")
		}
	case WorkerGroupHeld:
		if unknown == 0 || state == backend.Queued {
			return errors.New("invalid held worker group")
		}
	case WorkerGroupDone:
		if unknown != 0 {
			return errors.New("done worker group has an unknown create")
		}
		for _, m := range ms {
			if !m.settled() {
				return errors.New("done worker group has an unsettled member")
			}
		}
		if !state.Terminal() || state == backend.Unknown {
			return errors.New("done worker group is not terminal")
		}
	}
	if state == backend.Unknown || state == backend.Retrying {
		return errors.New("invalid worker group status")
	}
	return nil
}

// workerGroupIdentity is the immutable identity of every member call.
func workerGroupIdentity(j backend.Job, g workerGroupRecord) backend.WorkerGroup {
	out := backend.WorkerGroup{Execution: backend.Reference{ID: j.ID, Task: backend.CloneTask(j.Task), Binding: j.TargetBinding}, AttemptID: j.ID, Spec: g.Spec}
	d, _ := hex.DecodeString(g.TaskDigest)
	copy(out.TaskDigest[:], d)
	return out
}
