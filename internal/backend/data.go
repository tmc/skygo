package backend

import (
	"context"
	"errors"
	"math"
	"strings"
	"time"
	"unicode/utf8"
)

// DataBackend is the optional direct data-execution contract. It supplements
// Backend; a configured route must assert both interfaces before accepting work.
// Original calls and their partial results remain owned through actual return.
// RemoveData retires recorded resources, never proves execution stopped.
type DataBackend interface {
	StartData(context.Context, DataStart) (DataCreated, error)
	ObserveData(context.Context, Reference) (DataObservation, error)
	RemoveData(context.Context, DataResources) (DataRemoval, error)
}

// DataMount selects an operator-defined alias, not a native volume authority.
// ReadOnly is always present in canonical JSON, including when false.
type DataMount struct {
	Reference   string `json:"reference" yaml:"reference"`
	Destination string `json:"destination" yaml:"destination"`
	ReadOnly    bool   `json:"read_only" yaml:"read_only"`
}

// DataStart carries the one accepted task and immutable effective budgets.
// Tokens are scoped in-memory secrets: never format, log or persist this value.
// A failed call still returns all actual original creation receipts it obtained.
type DataStart struct {
	JobID                   string
	Execution               Reference
	Budgets                 DataBudgets
	InputToken, OutputToken []byte
}

// DataResource is an original Create-return identity. Zero is unknown or an
// absent optional scope; a later name lookup cannot fill a missing UID.
type DataResource struct{ Name, UID string }

// DataResources retains exact resources and the original accepted execution.
// An unknown Pod with known Secrets is a partial-create cleanup subject only.
type DataResources struct {
	Execution                      Reference
	Pod, InputSecret, OutputSecret DataResource
}

// DataCreated preserves original receipts even alongside an error. A nonempty
// Instance must equal the original Pod.UID, never an observationally adopted UID.
type DataCreated struct {
	Instance                       string
	Pod, InputSecret, OutputSecret DataResource
}

// ContainerStart is a trusted CURRENT Running receipt. Missing times or IDs
// cannot be replaced by server time, Pod phase or an attempted launch timestamp.
type ContainerStart struct {
	ContainerID string
	StartedAt   time.Time
}

// ContainerExit is a trusted current native termination receipt, not helper RPC
// data. Integration must qualify runtime identity, state and original ownership.
type ContainerExit struct {
	ContainerID           string
	ExitCode              int32
	StartedAt, FinishedAt time.Time
}

// DataTaskStop includes every declared init and the actual main task exit.
// Collector termination is separate and does not imply resource removal.
type DataTaskStop struct {
	Stage, Setup *ContainerExit
	Task         ContainerExit
}

// DataInitPhase selects the actually failed init. Zero is invalid.
type DataInitPhase uint8

const (
	StageFailure DataInitPhase = iota + 1
	SetupFailure
)

// DataNotStarted is an explicit qualified current Waiting receipt. A missing
// status is unknown and cannot produce this value.
type DataNotStarted struct{ Name, WaitingReason string }

// DataNoLaunch is distinct from main termination. Trusted integration requires
// two exact original-Pod observations and separately qualified native semantics.
type DataNoLaunch struct {
	FailedInit                                  DataInitPhase
	Stage, Setup                                *ContainerExit
	SetupBlocked, TaskBlocked, CollectorBlocked *DataNotStarted
}

// DataPhase identifies a finite observation phase. Zero is invalid; Held carries
// no implicit termination, no-launch, resource absence or closure authority.
type DataPhase uint8

const (
	DataStarting DataPhase = iota + 1
	DataStage
	DataTask
	DataCollect
	DataSettled
	DataHeld
)

// DataObservation is trusted in-process output, never worker RPC authority.
// TaskStart is Running-only; TaskStop and NoLaunch are disjoint alternatives.
type DataObservation struct {
	Status    Status
	TaskStart *ContainerStart
	TaskStop  *DataTaskStop
	NoLaunch  *DataNoLaunch
	Collector *ContainerExit
	Phase     DataPhase
}

// DataRemoval echoes each recorded original identity independently. Each true
// flag requires actual UID-qualified retirement after its separate ownership
// prerequisites. Unknown and optional absent scopes never fabricate a true flag.
type DataRemoval struct {
	Pod, InputSecret, OutputSecret          DataResource
	PodRemoved, InputRemoved, OutputRemoved bool
}

// DataBudgets is the immutable effective attempt tuple, in positive seconds.
type DataBudgets struct{ StageSeconds, TaskSeconds, CollectionSeconds, ShutdownSeconds int64 }

// DataPolicy is an immutable configured target value, not user deadline authority.
// Its zero value is invalid. The initial supported policy is fixed.
type DataPolicy struct {
	StageSeconds, CollectionSeconds                     int64
	DefaultTaskSeconds, MaxTaskSeconds, ShutdownSeconds int64
}

// DefaultDataPolicy returns the fixed initial supported target policy.
func DefaultDataPolicy() DataPolicy { return DataPolicy{600, 600, 86400, 604800, 120} }

// Validate refuses unsupported or unbounded data policy values.
func (p DataPolicy) Validate() error {
	if p != DefaultDataPolicy() {
		return errors.New("unsupported data policy")
	}
	return nil
}

// Budgets derives the exact effective tuple. Zero timeout selects the fixed
// default; positive timeout must not exceed the configured maximum.
func (p DataPolicy) Budgets(timeout int64) (DataBudgets, error) {
	if err := p.Validate(); err != nil {
		return DataBudgets{}, err
	}
	if timeout < 0 || timeout > p.MaxTaskSeconds {
		return DataBudgets{}, errors.New("data timeout outside policy")
	}
	if timeout == 0 {
		timeout = p.DefaultTaskSeconds
	}
	b := DataBudgets{p.StageSeconds, timeout, p.CollectionSeconds, p.ShutdownSeconds}
	if _, err := b.TotalSeconds(); err != nil {
		return DataBudgets{}, err
	}
	return b, nil
}

// TotalSeconds checks positive finite arithmetic and duration conversion. It
// does not assert that this tuple equals the accepted task's configured policy.
func (b DataBudgets) TotalSeconds() (int64, error) {
	var total int64
	for _, seconds := range []int64{b.StageSeconds, b.TaskSeconds, b.CollectionSeconds, b.ShutdownSeconds} {
		if seconds <= 0 || seconds > math.MaxInt64-total {
			return 0, errors.New("invalid data budget sum")
		}
		total += seconds
	}
	if total > math.MaxInt64/int64(time.Second) {
		return 0, errors.New("data budget duration overflows")
	}
	return total, nil
}

// DataDeadline adds positive finite seconds without duration or supported
// timestamp overflow. Callers supply trusted original time, never a fallback.
func DataDeadline(start time.Time, seconds int64) (time.Time, error) {
	if !dataTime(start) || seconds <= 0 || seconds > math.MaxInt64/int64(time.Second) {
		return time.Time{}, errors.New("invalid data deadline")
	}
	start = start.UTC()
	end := start.Add(time.Duration(seconds) * time.Second)
	if !dataTime(end) || !end.After(start) {
		return time.Time{}, errors.New("data deadline overflows")
	}
	return end, nil
}

// CloneDataPolicy copies an optional configured value; nil remains nil.
func CloneDataPolicy(p *DataPolicy) *DataPolicy {
	if p == nil {
		return nil
	}
	copy := *p
	return &copy
}

// ValidateDataTask checks the portable finite data profile and mount paths.
// It does not validate the main program, alias authorization or native profile.
// protected must be the complete trusted target-derived workspace/helper/secret
// path set, never user authority. This helper cannot prove that set is complete.
// An explicit mount-only Directory is additionally protected. No path is rewritten.
func ValidateDataTask(t Task, protected []string) error {
	if err := ValidateWorkspace(t); err != nil {
		return err
	}
	if t.Workdir == nil && len(t.DataMounts) == 0 {
		return errors.New("data task requires workdir or mounts")
	}
	return validateDataMounts(t, protected)
}

// ValidateDataTemplate checks the portable data mounts of a task whose input
// is not yet resolved, after the ValidateWorkspaceTemplate checks. Its mounts
// must satisfy the ValidateDataTask rules with no protected paths: the target
// is not chosen, so placement, alias authorization and protected paths are
// checked only once the input is resolved. It never supplies a Ref.
func ValidateDataTemplate(t Task) error {
	if err := ValidateWorkspaceTemplate(t); err != nil {
		return err
	}
	return validateDataMounts(t, nil)
}

// validateDataMounts checks DataMounts against protected and the explicit
// mount-only Directory.
func validateDataMounts(t Task, protected []string) error {
	if len(t.DataMounts) > 16 {
		return errors.New("data mounts exceeds 16 entries")
	}
	for _, name := range protected {
		if !dataAbsolutePath(name) {
			return errors.New("invalid protected data path")
		}
	}
	if t.Workdir == nil && t.Directory != "" && !dataAbsolutePath(t.Directory) {
		return errors.New("invalid data directory")
	}
	for i, mount := range t.DataMounts {
		if !dataAlias(mount.Reference) {
			return errors.New("invalid data reference alias")
		}
		if !dataAbsolutePath(mount.Destination) || !strings.HasPrefix(mount.Destination, "/mnt/skygo/") {
			return errors.New("invalid data mount destination")
		}
		for _, name := range protected {
			if dataPathOverlap(mount.Destination, name) {
				return errors.New("data mount overlaps protected path")
			}
		}
		if t.Workdir == nil && t.Directory != "" && dataPathOverlap(mount.Destination, t.Directory) {
			return errors.New("data mount overlaps directory")
		}
		for _, earlier := range t.DataMounts[:i] {
			if dataPathOverlap(mount.Destination, earlier.Destination) {
				return errors.New("overlapping data mount destinations")
			}
		}
	}
	return nil
}

func dataAlias(s string) bool {
	if len(s) == 0 || len(s) > 63 || s[0] == '-' || s[len(s)-1] == '-' {
		return false
	}
	for _, c := range s {
		if c < 'a' || c > 'z' {
			if c < '0' || c > '9' {
				if c != '-' {
					return false
				}
			}
		}
	}
	return true
}
func dataAbsolutePath(s string) bool {
	if s == "/" {
		return true
	}
	if len(s) < 2 || len(s) > 240 || !utf8.ValidString(s) || s[0] != '/' || strings.ContainsAny(s, "\\:\x00*?[]") {
		return false
	}
	parts := strings.Split(s[1:], "/")
	if len(parts) > 16 {
		return false
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}
func dataPathOverlap(a, b string) bool {
	return a == b || a == "/" || b == "/" || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}
func dataID(s string) bool { return dataText(s, 128) }
func dataText(s string, limit int) bool {
	if len(s) == 0 || len(s) > limit {
		return false
	}
	for _, c := range s {
		if c < 32 || c > 126 {
			return false
		}
	}
	return true
}
func dataTime(t time.Time) bool { return !t.IsZero() && t.UTC().Year() >= 1 && t.UTC().Year() <= 9999 }
func dataExit(e ContainerExit) bool {
	return dataID(e.ContainerID) && e.ExitCode >= 0 && e.ExitCode <= 255 && dataTime(e.StartedAt) && dataTime(e.FinishedAt) && !e.FinishedAt.Before(e.StartedAt)
}
func dataResource(r DataResource) bool {
	return r == (DataResource{}) || dataAlias(r.Name) && dataID(r.UID)
}

// ValidateDataCreated validates original receipt shape. allowPartial is only for
// preserving a failed original call's partial receipts; it grants no launch or
// worker authority. A required missing resource remains unresolved.
func ValidateDataCreated(t Task, c DataCreated, allowPartial bool) error {
	if err := ValidateDataTask(t, nil); err != nil {
		return err
	}
	if !dataResource(c.Pod) || !dataResource(c.InputSecret) || !dataResource(c.OutputSecret) || c.Instance != c.Pod.UID {
		return errors.New("invalid data creation identity")
	}
	if t.Workdir == nil && c.InputSecret != (DataResource{}) || len(t.Outputs) == 0 && c.OutputSecret != (DataResource{}) {
		return errors.New("unexpected data secret receipt")
	}
	if c.InputSecret != (DataResource{}) && c.InputSecret.UID == c.OutputSecret.UID {
		return errors.New("data scopes share a resource")
	}
	if !allowPartial && (c.Pod == (DataResource{}) || t.Workdir != nil && c.InputSecret == (DataResource{}) || len(t.Outputs) != 0 && c.OutputSecret == (DataResource{})) {
		return errors.New("missing data creation receipt")
	}
	return nil
}

// ValidateDataResources validates exact recorded shapes, including the narrow
// known-Secret-only partial-create case. It proves no scope revocation or join.
func ValidateDataResources(r DataResources) error {
	if r.Pod == (DataResource{}) && r.InputSecret == (DataResource{}) && r.OutputSecret == (DataResource{}) {
		return errors.New("data cleanup lacks known resource")
	}
	if err := ValidateDataCreated(r.Execution.Task, DataCreated{Instance: r.Execution.Instance, Pod: r.Pod, InputSecret: r.InputSecret, OutputSecret: r.OutputSecret}, true); err != nil {
		return err
	}
	if !dataID(r.Execution.ID) || !dataText(r.Execution.Binding, 256) {
		return errors.New("invalid data execution reference")
	}
	return nil
}

// ValidateDataRemoval checks exact original echoes and independent flags. It
// does not prove UID-qualified deletion, execution stop or original owner joins.
func ValidateDataRemoval(original DataResources, removed DataRemoval) error {
	if err := ValidateDataResources(original); err != nil {
		return err
	}
	if removed.Pod != original.Pod || removed.InputSecret != original.InputSecret || removed.OutputSecret != original.OutputSecret {
		return errors.New("data removal identity mismatch")
	}
	if removed.PodRemoved && original.Pod == (DataResource{}) || removed.InputRemoved && original.InputSecret == (DataResource{}) || removed.OutputRemoved && original.OutputSecret == (DataResource{}) {
		return errors.New("data removal acknowledges unknown resource")
	}
	return nil
}

func dataBlocked(p *DataNotStarted, name string, required bool) bool {
	if !required {
		return p == nil
	}
	return p != nil && p.Name == name && p.WaitingReason == "PodInitializing"
}
func dataNoLaunch(t Task, n *DataNoLaunch) bool {
	if n == nil || t.Workdir == nil || n.Stage == nil || !dataExit(*n.Stage) || !dataBlocked(n.TaskBlocked, "task", true) || !dataBlocked(n.CollectorBlocked, "collector", len(t.Outputs) != 0) {
		return false
	}
	switch n.FailedInit {
	case StageFailure:
		return n.Stage.ExitCode != 0 && n.Setup == nil && dataBlocked(n.SetupBlocked, "setup", len(t.Setup) != 0)
	case SetupFailure:
		return len(t.Setup) != 0 && n.Stage.ExitCode == 0 && n.Setup != nil && dataExit(*n.Setup) && n.Setup.ContainerID != n.Stage.ContainerID && n.Setup.ExitCode != 0 && !n.Setup.StartedAt.Before(n.Stage.FinishedAt) && n.SetupBlocked == nil
	}
	return false
}
func dataTaskStop(t Task, s *DataTaskStop) bool {
	if s == nil || !dataExit(s.Task) || (s.Stage != nil) != (t.Workdir != nil) || (s.Setup != nil) != (len(t.Setup) != 0) {
		return false
	}
	before := time.Time{}
	if s.Stage != nil {
		if !dataExit(*s.Stage) || s.Stage.ExitCode != 0 || s.Stage.ContainerID == s.Task.ContainerID {
			return false
		}
		before = s.Stage.FinishedAt
	}
	if s.Setup != nil {
		if !dataExit(*s.Setup) || s.Setup.ExitCode != 0 || s.Setup.ContainerID == s.Task.ContainerID || s.Stage != nil && s.Setup.ContainerID == s.Stage.ContainerID || s.Setup.StartedAt.Before(before) {
			return false
		}
		before = s.Setup.FinishedAt
	}
	return !s.Task.StartedAt.Before(before)
}

// ValidateDataObservation checks only value/profile/chronology unions. Trusted
// integration must separately verify original Pod/spec/containers, actual state,
// restarts/last states and two-observation/native stop/no-launch prerequisites.
func ValidateDataObservation(ref Reference, o DataObservation) error {
	if err := ValidateDataTask(ref.Task, nil); err != nil {
		return err
	}
	if o.Phase < DataStarting || o.Phase > DataHeld {
		return errors.New("invalid data phase")
	}
	if !dataID(ref.Instance) {
		return errors.New("missing original data pod identity")
	}
	terminal := o.Status.State == Succeeded || o.Status.State == Failed || o.Status.State == Canceled
	if o.Status.Instance != ref.Instance && (o.Status.Instance != "" || terminal || o.TaskStart != nil || o.TaskStop != nil || o.NoLaunch != nil || o.Collector != nil) {
		return errors.New("data observation identity mismatch")
	}
	switch o.Status.State {
	case Starting, Running, Succeeded, Failed, Canceled, Unknown:
	default:
		return errors.New("invalid data status")
	}
	if o.Status.Termination != UnknownTermination && o.Status.Termination != StoppedWithExitCode {
		return errors.New("invalid data termination")
	}
	if o.Status.ExitCode != nil && (*o.Status.ExitCode < 0 || *o.Status.ExitCode > 255) {
		return errors.New("invalid data exit code")
	}
	unions := 0
	if o.TaskStart != nil {
		unions++
	}
	if o.TaskStop != nil {
		unions++
	}
	if o.NoLaunch != nil {
		unions++
	}
	if unions > 1 || terminal && o.TaskStop == nil && o.NoLaunch == nil {
		return errors.New("invalid data observation union")
	}
	if o.Status.State == Running && o.TaskStart == nil {
		return errors.New("running data lacks current task start")
	}
	if o.TaskStart != nil && (o.Status.State != Running || !dataID(o.TaskStart.ContainerID) || !dataTime(o.TaskStart.StartedAt) || o.Status.ExitCode != nil || o.Status.Termination != UnknownTermination) {
		return errors.New("invalid running data receipt")
	}
	if o.TaskStop != nil {
		if !dataTaskStop(ref.Task, o.TaskStop) || o.Status.ExitCode != nil && *o.Status.ExitCode != o.TaskStop.Task.ExitCode {
			return errors.New("invalid data task stop")
		}
	} else if o.Status.Termination == StoppedWithExitCode || o.Status.ExitCode != nil {
		return errors.New("data status lacks task stop")
	}
	if o.Status.Termination == StoppedWithExitCode && o.Status.ExitCode == nil {
		return errors.New("data termination lacks exit code")
	}
	if o.Status.State == Succeeded && (o.TaskStop == nil || o.TaskStop.Task.ExitCode != 0) {
		return errors.New("successful data lacks successful main exit")
	}
	if o.NoLaunch != nil && (!dataNoLaunch(ref.Task, o.NoLaunch) || o.Status.State != Failed || o.Status.ExitCode != nil || o.Status.Termination != UnknownTermination || o.Collector != nil) {
		return errors.New("invalid data no-launch receipt")
	}
	if o.Collector != nil && (len(ref.Task.Outputs) == 0 || !dataExit(*o.Collector)) {
		return errors.New("invalid data collector receipt")
	}
	if o.Status.Termination == StoppedWithExitCode && !terminal {
		return errors.New("stopped data lacks terminal status")
	}
	switch o.Phase {
	case DataStarting:
		if o.Status.State != Starting || unions != 0 || o.Collector != nil {
			return errors.New("invalid starting data phase")
		}
	case DataStage:
		if ref.Task.Workdir == nil || o.Status.State != Starting || unions != 0 || o.Collector != nil {
			return errors.New("invalid staging data phase")
		}
	case DataTask:
		if o.TaskStart == nil {
			return errors.New("task phase lacks running receipt")
		}
	case DataCollect:
		if o.TaskStop == nil || !terminal || len(ref.Task.Outputs) == 0 {
			return errors.New("collection phase lacks task stop or outputs")
		}
	}
	if o.Collector != nil {
		id := o.Collector.ContainerID
		if o.TaskStop != nil {
			var initFinished time.Time
			if o.TaskStop.Stage != nil {
				initFinished = o.TaskStop.Stage.FinishedAt
			}
			if o.TaskStop.Setup != nil {
				initFinished = o.TaskStop.Setup.FinishedAt
			}
			if o.Collector.StartedAt.Before(initFinished) {
				return errors.New("collector starts before init completion")
			}
		}
		if o.TaskStart != nil && id == o.TaskStart.ContainerID || o.TaskStop != nil && (id == o.TaskStop.Task.ContainerID || o.TaskStop.Stage != nil && id == o.TaskStop.Stage.ContainerID || o.TaskStop.Setup != nil && id == o.TaskStop.Setup.ContainerID) {
			return errors.New("data containers share an identity")
		}
	}
	if o.Phase == DataSettled && !terminal {
		return errors.New("settled data lacks terminal status")
	}
	if o.Status.Termination == StoppedWithExitCode && ((o.Collector != nil) != (len(ref.Task.Outputs) != 0) || o.Collector != nil && o.Collector.FinishedAt.Before(o.TaskStop.Task.FinishedAt)) {
		return errors.New("stopped data lacks collector termination")
	}
	if o.Phase == DataSettled {
		if o.TaskStop == nil && o.NoLaunch == nil {
			return errors.New("settled data lacks termination union")
		}
		if o.TaskStop != nil && ((o.Collector != nil) != (len(ref.Task.Outputs) != 0) || o.Collector != nil && o.Collector.FinishedAt.Before(o.TaskStop.Task.FinishedAt)) {
			return errors.New("settled data lacks collector termination")
		}
	}
	return nil
}

// CloneDataObservation copies every optional receipt and exit-code pointer.
func CloneDataObservation(o DataObservation) DataObservation {
	if o.Status.ExitCode != nil {
		copy := *o.Status.ExitCode
		o.Status.ExitCode = &copy
	}
	if o.TaskStart != nil {
		copy := *o.TaskStart
		o.TaskStart = &copy
	}
	if o.Collector != nil {
		copy := *o.Collector
		o.Collector = &copy
	}
	if o.TaskStop != nil {
		copy := *o.TaskStop
		if copy.Stage != nil {
			stage := *copy.Stage
			copy.Stage = &stage
		}
		if copy.Setup != nil {
			setup := *copy.Setup
			copy.Setup = &setup
		}
		o.TaskStop = &copy
	}
	if o.NoLaunch != nil {
		copy := *o.NoLaunch
		if copy.Stage != nil {
			stage := *copy.Stage
			copy.Stage = &stage
		}
		if copy.Setup != nil {
			setup := *copy.Setup
			copy.Setup = &setup
		}
		if copy.SetupBlocked != nil {
			blocked := *copy.SetupBlocked
			copy.SetupBlocked = &blocked
		}
		if copy.TaskBlocked != nil {
			blocked := *copy.TaskBlocked
			copy.TaskBlocked = &blocked
		}
		if copy.CollectorBlocked != nil {
			blocked := *copy.CollectorBlocked
			copy.CollectorBlocked = &blocked
		}
		o.NoLaunch = &copy
	}
	return o
}
