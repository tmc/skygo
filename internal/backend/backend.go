package backend

import (
	"context"
	"errors"
	"time"

	"github.com/tmc/skygo/internal/artifact"
)

// Task describes one program. Zero resource quantities mean unspecified.
type Task struct {
	Argv             []string          `json:"argv" yaml:"argv"`
	Env              map[string]string `json:"env,omitempty" yaml:"env,omitempty"`
	Directory        string            `json:"directory,omitempty" yaml:"directory,omitempty"`
	Image            string            `json:"image,omitempty" yaml:"image,omitempty"`
	CPUMillis        int64             `json:"cpu_millis,omitempty" yaml:"cpu_millis,omitempty"`
	MemoryBytes      int64             `json:"memory_bytes,omitempty" yaml:"memory_bytes,omitempty"`
	Accelerator      string            `json:"accelerator,omitempty" yaml:"accelerator,omitempty"`
	AcceleratorCount int32             `json:"accelerator_count,omitempty" yaml:"accelerator_count,omitempty"`
	Target           string            `json:"target,omitempty" yaml:"target,omitempty"`
	TimeoutSeconds   int64             `json:"timeout_seconds,omitempty" yaml:"timeout_seconds,omitempty"`
	CPULimitMillis   int64             `json:"cpu_limit_millis,omitempty" yaml:"cpu_limit_millis,omitempty"`
	MemoryLimitBytes int64             `json:"memory_limit_bytes,omitempty" yaml:"memory_limit_bytes,omitempty"`
	TargetLabels     map[string]string `json:"target_labels,omitempty" yaml:"target_labels,omitempty"`
	NodeSelector     map[string]string `json:"node_selector,omitempty" yaml:"node_selector,omitempty"`
	Retry            *RetryPolicy      `json:"retry,omitempty" yaml:"retry,omitempty"`
	Workdir          *artifact.Ref     `json:"workdir,omitempty" yaml:"workdir,omitempty"`
	Setup            []string          `json:"setup,omitempty" yaml:"setup,omitempty"`
	Outputs          []string          `json:"outputs,omitempty" yaml:"outputs,omitempty"`
}

// State records an observed execution state. Unknown does not imply termination.
type State string

const (
	Queued    State = "queued"
	Starting  State = "starting"
	Running   State = "running"
	Retrying  State = "retrying"
	Succeeded State = "succeeded"
	Failed    State = "failed"
	Canceled  State = "canceled"
	Unknown   State = "unknown"
)

// Terminal reports whether the control plane will stop reconciling this state.
func (s State) Terminal() bool { return s == Succeeded || s == Failed || s == Canceled || s == Unknown }

// Termination records positive stopped evidence within a backend's documented
// ownership unit. It does not establish signal provenance or whole-tree cleanup.
type Termination uint8

const (
	UnknownTermination Termination = iota
	StoppedWithExitCode
)

// Status describes the last observed state; ExitCode is nil unless observed.
// Instance is an opaque execution identity: empty, or 1..128 printable ASCII bytes.
// StoppedWithExitCode requires both an exit code and backend stopped evidence.
type Status struct {
	State       State       `json:"state"`
	Reason      string      `json:"reason,omitempty"`
	ExitCode    *int32      `json:"exit_code,omitempty"`
	Termination Termination `json:"termination,omitempty"`
	Instance    string      `json:"instance,omitempty"`
}

// Attempt records one execution of an accepted retry task. ID is immutable.
// StartingAt records committed launch intent, not an exact process start time.
// FinishedAt records a committed terminal decision, including cancellation before
// execution. A retry job retains at most its policy's MaxAttempts entries.
type Attempt struct {
	ID         string     `json:"id"`
	Status     Status     `json:"status"`
	CreatedAt  time.Time  `json:"created_at"`
	StartingAt *time.Time `json:"starting_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

// Job is a durable accepted task. Task, Target and TargetBinding never change.
type Job struct {
	ID              string     `json:"id"`
	Task            Task       `json:"task"`
	Target          string     `json:"target"`
	Status          Status     `json:"status"`
	CancelRequested bool       `json:"cancel_requested"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
	Sequence        uint64     `json:"sequence"`
	TargetBinding   string     `json:"target_binding,omitempty"`
	Attempts        []Attempt  `json:"attempts,omitempty"`
	NextAttemptAt   *time.Time `json:"next_attempt_at,omitempty"`
}

// LogChunk is a bounded segment of the retained combined output prefix.
type LogChunk struct {
	Data       []byte
	NextOffset int64
	Complete   bool
	Truncated  bool
}

// MaxLogBytes caps retained output per job; MaxChunkBytes bounds one log read.
const MaxLogBytes = 1 << 20
const MaxChunkBytes = 64 << 10

// Reference binds execution identity to the immutable accepted task.
// Task lets recovering backends verify execution against durable state.
// Instance is empty or an opaque identity of 1..128 printable ASCII bytes.
type Reference struct {
	ID       string
	Task     Task
	Binding  string
	Instance string
}

// ErrIdentityMismatch means an execution does not match its durable reference.
// Retrying must not adopt or modify the conflicting execution.
var ErrIdentityMismatch = errors.New("execution identity mismatch")

// Backend executes jobs under stable coordinator-generated IDs. Start is called
// at most once by a coordinator, after durable Starting intent. Its context only
// bounds startup, not workload lifetime. Observe must report Unknown for missing
// execution and return an error for temporary uncertainty. Cancel nil confirms
// delivery of the request, not termination. Methods are safe for concurrent use.
// Logs addresses the retained byte prefix; it rejects offsets past that prefix.
type Backend interface {
	Check(Task) error
	Start(context.Context, string, Task) error
	Observe(context.Context, Reference) (Status, error)
	Cancel(context.Context, Reference) error
	Logs(context.Context, Reference, int64, int) (LogChunk, error)
}

// Target binds a configured name and kind to an executor.
// Retention and output facts come from the actual constructor; their zero values
// do not imply remote storage or cleanup support. OutputReservation is the fixed
// local charge in bytes, or zero for explicitly remote output.
type Target struct {
	Advice                 *TargetAdvice
	Name                   string
	Kind                   string
	Backend                Backend
	Binding                string
	Labels                 map[string]string
	AcceleratorNames       []string
	SupportsResourceLimits bool
	SupportsNodeSelector   bool
	SupportsRetry          bool
	Retention              *Retention
	OutputStorage          OutputStorage
	OutputReservation      uint64
}
