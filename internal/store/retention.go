package store

import (
	"errors"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

// RequestDomain identifies a bounded request namespace. It is not task input.
type RequestDomain struct {
	Lineage string
	Epoch   uint64
}

// Page is one authenticated sparse listing page. After is a scope index key.
type Page struct {
	Jobs  []backend.Job
	After uint64
	More  bool
}
type QuotaDomain uint8

const (
	JobsQuota QuotaDomain = iota + 1
	TaskBytesQuota
	RetryBytesQuota
	MetadataBytesQuota
	RequestRecordsQuota
	RequestBytesQuota
	CleanupRecordsQuota
	CleanupBytesQuota
	OutputBytesQuota
	OutputDomainsQuota
	OutputFilesQuota
	PrincipalScopesQuota
)

type Usage struct {
	Domain      QuotaDomain
	Used, Limit uint64
}
type CapacityError struct {
	Domain      QuotaDomain
	Used, Limit uint64
}

func (e *CapacityError) Error() string { return "retention capacity reached" }

type ExpiryReason uint8

const (
	ResultExpired ExpiryReason = iota + 1
	RequestEpochExpired
	RequestDomainRequired
	RequestLineageChanged
	LogsExpired
)

type ExpiryError struct {
	Reason ExpiryReason
	JobID  string
}

func (e *ExpiryError) Error() string { return "retention result or request expired" }

var ErrRetentionDisabled = errors.New("retention is disabled")

// RetentionPolicy bounds logical accounting. Physical watermarks are guards,
// not preallocation or hard filesystem size guarantees.
type RetentionPolicy struct {
	LocalOutputEnabled bool          `json:"local_output_enabled"`
	Window             time.Duration `json:"window"`
	Jobs               uint64        `json:"jobs"`
	TaskBytes          uint64        `json:"task_bytes"`
	RetryBytes         uint64        `json:"retry_bytes"`
	MetadataBytes      uint64        `json:"metadata_bytes"`
	RequestRecords     uint64        `json:"request_records"`
	RequestBytes       uint64        `json:"request_bytes"`
	CleanupRecords     uint64        `json:"cleanup_records"`
	CleanupBytes       uint64        `json:"cleanup_bytes"`
	OutputBytes        uint64        `json:"output_bytes"`
	OutputDomains      uint64        `json:"output_domains"`
	OutputFiles        uint64        `json:"output_files"`
	PrincipalScopes    uint64        `json:"principal_scopes"`
	// OutputUnit and Volume identify the validated private local log root.
	OutputUnit uint64 `json:"output_unit"`
	Volume     uint64 `json:"volume"`
}

func DefaultRetentionPolicy() RetentionPolicy {
	return RetentionPolicy{LocalOutputEnabled: true, Window: 30 * 24 * time.Hour, Jobs: 10000, TaskBytes: 16 << 20, RetryBytes: 16 << 20, MetadataBytes: 32 << 20, RequestRecords: 20000, RequestBytes: 16 << 20, CleanupRecords: 1024, CleanupBytes: 4 << 20, OutputBytes: 256 << 20, OutputDomains: 256, OutputFiles: 512, PrincipalScopes: 1024}
}

type RetentionInfo struct {
	Policy                                RetentionPolicy
	PendingCleanupRecords                 uint64
	Enabled                               bool
	Domain                                RequestDomain
	AcceptUntil, RetryUntil, LegacyCutoff time.Time
	LegacyReplayOpen                      bool
	AcceptClosed                          bool
	Usage                                 []Usage
}
type PruneMode uint8

const (
	Whole PruneMode = iota
	MetadataOnly
	OutputOnly
)

type PruneSelection struct {
	Before                        time.Time
	Mode                          PruneMode
	AcknowledgeUnverifiedTerminal bool
	ObligationID                  string
	After, Max                    uint64
	Limit                         int
}
type PruneSkip struct {
	Reason string
	Count  uint32
}
type ChargeTransfer struct {
	Domain          QuotaDomain
	Added, Released uint64
}
type PrunePage struct {
	Inspected, Selected, Marked, Skipped uint32
	Skips                                []PruneSkip
	Transfers                            []ChargeTransfer
	After                                uint64
	More                                 bool
}
type CleanupOperation uint8

const (
	OutputCleanup CleanupOperation = iota + 1
	NativeCleanup
)

// CleanupRecord is validated dispatch data, not its persisted encoding.
type CleanupRecord struct {
	ID, HeaderID, Target, Binding string
	Sequence                      uint64
	Operation                     CleanupOperation
	Output                        backend.OutputReference
	Execution                     backend.Reference
	Digest                        [32]byte
	Stopped                       bool
	Proof                         backend.Status
	TaskDigest                    [32]byte
}
type Obligation struct {
	Reason                          string
	OutputPending, ExecutionPending bool
	ID, JobID, Target, Binding      string
	ExecutionIDs                    []string
	NeedsManual                     bool
	CreatedAt                       time.Time
	OutputBytes                     uint64
}
type ObligationPage struct {
	Items      []Obligation
	After, Max uint64
	More       bool
}

// PruneSubject supplies immutable identity for read-only backend closure checks.
type PruneSubject struct {
	Job        backend.Job
	Obligation *Obligation
	Digest     [32]byte
	Output     []backend.OutputReference
}
