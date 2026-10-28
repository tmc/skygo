package store

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

var retentionBucket = []byte("retention")
var retentionStateKey = []byte("state")
var retentionUsageKey = []byte("usage")
var retainedJobsBucket = []byte("job_metadata")
var retainedRequestsBucket = []byte("request_records")
var retainedOutputsBucket = []byte("output_reservations")
var retainedClosuresBucket = []byte("output_closures")
var cleanupHeadersBucket = []byte("cleanup_headers")
var cleanupHeaderIndexBucket = []byte("cleanup_header_index")
var cleanupOperationsBucket = []byte("cleanup_operations")
var cleanupOperationIndexBucket = []byte("cleanup_operation_index")
var retainedScopesBucket = []byte("scope_counters")

const jobOverheadReserve = 16384
const cleanupHeaderLimit = 300 << 10
const manualHeaderLimit = 4096
const cleanupOperationLimit = 2048
const maxChangeBytes = 1 << 20

type retentionState struct {
	Enabled            bool            `json:"enabled"`
	Policy             RetentionPolicy `json:"policy"`
	Lineage            string          `json:"lineage,omitempty"`
	Current            uint64          `json:"current"`
	RetiredThrough     uint64          `json:"retired_through"`
	Epochs             []requestEpoch  `json:"epochs,omitempty"`
	LegacyReplayOpen   bool            `json:"legacy_replay_open"`
	LegacyCutoff       time.Time       `json:"legacy_cutoff,omitempty"`
	Restored           bool            `json:"restored"`
	RequestAfter       string          `json:"request_after,omitempty"`
	LegacyJobsAfter    uint64          `json:"legacy_jobs_after,omitempty"`
	LegacyJobsMax      uint64          `json:"legacy_jobs_max,omitempty"`
	TaskCounterOffset  uint64          `json:"task_counter_offset"`
	TaskCounterDeficit uint64          `json:"task_counter_deficit"`
	LineageSerial      uint64          `json:"lineage_serial"`
}
type requestEpoch struct {
	Number       uint64    `json:"number"`
	AcceptUntil  time.Time `json:"accept_until"`
	RetryUntil   time.Time `json:"retry_until"`
	AcceptClosed bool      `json:"accept_closed"`
}
type retainedJob struct {
	Principal            string                `json:"principal"`
	Domain               RequestDomain         `json:"domain"`
	RequestID            string                `json:"request_id"`
	LegacyRequestID      []byte                `json:"legacy_request_id,omitempty"`
	LegacyRequestRetired bool                  `json:"legacy_retired,omitempty"`
	Storage              backend.OutputStorage `json:"storage"`
	OutputReserve        uint64                `json:"output_reserve"`
	Overhead             uint64                `json:"overhead"`
	Legacy               bool                  `json:"legacy"`
	Started              bool                  `json:"started"`
	OutputStart          uint32                `json:"output_start"`
	Ordinal              uint64                `json:"ordinal"`
	WorkflowID           string                `json:"workflow_id,omitempty"`
	Node                 uint8                 `json:"node,omitempty"`
}
type requestRecord struct {
	Principal       string        `json:"principal"`
	Domain          RequestDomain `json:"domain"`
	RequestID       string        `json:"request_id"`
	LegacyRequestID []byte        `json:"legacy_request_id,omitempty"`
	JobID           string        `json:"job_id"`
	TaskDigest      [32]byte      `json:"task_digest"`
	Expired         bool          `json:"expired"`
	LegacyBytes     uint64        `json:"legacy_bytes"`
	WorkflowID      string        `json:"workflow_id,omitempty"`
	GraphDigest     string        `json:"graph_digest,omitempty"`
}
type outputReservation struct {
	JobID      string `json:"job_id"`
	AttemptID  string `json:"attempt_id"`
	Binding    string `json:"binding"`
	Generation uint64 `json:"generation"`
	Bytes      uint64 `json:"bytes"`
	Expired    bool   `json:"expired"`
}
type outputClosure struct {
	Reference     backend.OutputReference `json:"reference"`
	SubjectDigest [32]byte                `json:"subject_digest"`
}

// A header owns a single Task copy for all native operation children. Manual
// headers and output-only headers contain no Task.
type cleanupHeader struct {
	ID              string        `json:"id"`
	Sequence        uint64        `json:"sequence"`
	JobID           string        `json:"job_id"`
	Principal       string        `json:"principal"`
	Target          string        `json:"target"`
	Binding         string        `json:"binding"`
	Task            *backend.Task `json:"task,omitempty"`
	TaskDigest      [32]byte      `json:"task_digest"`
	ExecutionIDs    []string      `json:"execution_ids"`
	NeedsManual     bool          `json:"needs_manual"`
	Remaining       uint32        `json:"remaining"`
	CreatedAt       time.Time     `json:"created_at"`
	OutputRemaining uint32        `json:"output_remaining"`
	NativeRemaining uint32        `json:"native_remaining"`
}
type cleanupOperation struct {
	ID              string           `json:"id"`
	HeaderID        string           `json:"header_id"`
	Sequence        uint64           `json:"sequence"`
	Operation       CleanupOperation `json:"operation"`
	AttemptID       string           `json:"attempt_id"`
	Stopped         bool             `json:"stopped"`
	Proof           *backend.Status  `json:"proof,omitempty"`
	LastError       string           `json:"last_error,omitempty"`
	UpdatedAt       time.Time        `json:"updated_at"`
	ClosureID       string           `json:"closure_id,omitempty"`
	Generation      uint64           `json:"generation,omitempty"`
	PayloadReleased bool             `json:"payload_released,omitempty"`
}

func retentionRoot(tx *bolt.Tx) *bolt.Bucket { return tx.Bucket(retentionBucket) }
func retentionTable(tx *bolt.Tx, name []byte) *bolt.Bucket {
	r := retentionRoot(tx)
	if r == nil {
		return nil
	}
	return r.Bucket(name)
}
func loadJSON(b *bolt.Bucket, key []byte, out any) error {
	if b == nil {
		return errors.New("missing retention table")
	}
	data := b.Get(key)
	if data == nil {
		return ErrNotFound
	}
	if h, ok := out.(*cleanupHeader); ok {
		return decodeCleanupHeader(data, h)
	}
	return json.Unmarshal(data, out)
}
func loadRetention(tx *bolt.Tx) (retentionState, error) {
	var s retentionState
	err := loadJSON(retentionRoot(tx), retentionStateKey, &s)
	return s, err
}
func putJSON(b *bolt.Bucket, key []byte, v any) error {
	data, e := json.Marshal(v)
	if e != nil {
		return e
	}
	return b.Put(key, data)
}
func taskDigest(t backend.Task) ([32]byte, error) {
	data, e := backend.TaskBytes(t)
	return sha256.Sum256(data), e
}

// requestKey hashes raw length-prefixed identity bytes; arbitrary legacy bytes
// never pass through JSON string normalization and keys stay below bbolt's bound.
func requestKey(principal string, d RequestDomain, id string) []byte {
	h := sha256.New()
	var n [8]byte
	for _, v := range []string{principal, d.Lineage} {
		binary.BigEndian.PutUint64(n[:], uint64(len(v)))
		h.Write(n[:])
		h.Write([]byte(v))
	}
	binary.BigEndian.PutUint64(n[:], d.Epoch)
	h.Write(n[:])
	binary.BigEndian.PutUint64(n[:], uint64(len(id)))
	h.Write(n[:])
	h.Write([]byte(id))
	return []byte(hex.EncodeToString(h.Sum(nil)))
}
func (m retainedJob) requestID() string {
	if m.LegacyRequestID != nil {
		return string(m.LegacyRequestID)
	}
	return m.RequestID
}
func (r requestRecord) requestID() string {
	if r.LegacyRequestID != nil {
		return string(r.LegacyRequestID)
	}
	return r.RequestID
}
func validRequestIdentity(d RequestDomain, id string, legacy []byte) bool {
	if d.Epoch == 0 {
		return (d.Lineage == "" || validAttemptID(d.Lineage)) && id == "" && len(legacy) > 0 && len(legacy) <= bolt.MaxKeySize
	}
	return validAttemptID(d.Lineage) && legacy == nil && validRetainedRequestID(id)
}

// legacyClosed consumes the existing irreversible epoch-zero retirement rule.
// It adds no closure, restore, execution or cleanup authority.
func legacyClosed(st retentionState, d RequestDomain) bool {
	return st.Enabled && d.Epoch == 0 && (d.Lineage != st.Lineage || !st.LegacyReplayOpen)
}

func validJobRequestIdentity(st retentionState, m retainedJob) bool {
	if m.LegacyRequestRetired {
		return m.Legacy && m.RequestID == "" && m.LegacyRequestID == nil &&
			(m.Domain.Lineage == "" || validAttemptID(m.Domain.Lineage)) && legacyClosed(st, m.Domain)
	}
	return m.Legacy == (m.Domain.Epoch == 0) && validRequestIdentity(m.Domain, m.RequestID, m.LegacyRequestID)
}

func validLegacyJobsCursor(st retentionState, high uint64) bool {
	return st.LegacyJobsMax == 0 && st.LegacyJobsAfter == 0 ||
		st.LegacyJobsMax != 0 && st.LegacyJobsMax <= high && st.LegacyJobsAfter <= st.LegacyJobsMax
}

func rowDigest(data []byte) [32]byte { return sha256.Sum256(data) }

func validRequestCursor(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := range len(s) {
		if !(s[i] >= '0' && s[i] <= '9' || s[i] >= 'a' && s[i] <= 'f') {
			return false
		}
	}
	return true
}
