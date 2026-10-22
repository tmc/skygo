package control

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

type maintenanceRequest struct {
	readerRecovery      *artifactReaderRecoveryRequest
	downloadClose       *artifactDownloadClose
	closeOwner          *artifactIO
	artifactDelete      *artifactDeleteRequest
	workspaceRetirement *workspaceRetirementRequest
	physicalOwner       *Control
	physicalClose       bool
	sequence            uint64
	record              *store.CleanupRecord
	target              backend.Target
	directory           string
	reusable            uint64
	reservedBytes       uint64
	reservedFiles       uint64
	usage               func(context.Context) (uint64, uint64, error)
}
type maintenanceResult struct {
	readerRecovery *artifactReaderRecoveryResult
	downloadClose  *artifactDownloadCloseResult
	deletion       *artifactDeleteResult
	retirement     *workspaceRetirementResult
	physical       *physicalQuantumResult
	request        maintenanceRequest
	sample         *RetentionAdmin
	err            error
}
type maintenanceState struct {
	remoteInputAfter     string
	remoteInputDue       time.Time
	preparationFamily    uint8
	expiryUploadAfter    string
	expiryDownloadAfter  string
	expiryFamily         uint8
	recoveryAfter        string
	expiryHeld           *artifactDownloadClose
	input                chan maintenanceRequest
	results              chan maintenanceResult
	pending              *maintenanceRequest
	after                uint64
	due                  time.Time
	nextClass            maintenanceClass
	workspaceAfter       string
	workspaceFinishLast  bool
	workspaceFinishAfter int
	serial               uint64
	done                 chan struct{}
	received             *receivedMaintenance
	physicalDue          time.Time
	physicalResult       *receivedMaintenance
	physicalSerial       uint64
}

func newMaintenance(ctx context.Context) *maintenanceState {
	m := &maintenanceState{input: make(chan maintenanceRequest, 1), results: make(chan maintenanceResult, 1), done: make(chan struct{})}
	go func() {
		defer close(m.done)
		runMaintenance(ctx, m.input, m.results)
	}()
	return m
}
func runMaintenance(ctx context.Context, input <-chan maintenanceRequest, results chan<- maintenanceResult) {
	for request := range input {
		result := maintenanceResult{request: request}
		if ctx.Err() != nil {
			result.err = ctx.Err()
			if request.workspaceRetirement != nil && request.workspaceRetirement.owner != nil {
				request.workspaceRetirement.owner.finish(nil)
			}
			if request.readerRecovery != nil {
				result.readerRecovery = &artifactReaderRecoveryResult{}
				request.readerRecovery.owner.finish(nil)
			}
			if request.downloadClose != nil {
				result.downloadClose = &artifactDownloadCloseResult{}
			}
			if request.closeOwner != nil {
				request.closeOwner.finish(nil)
			}
			if request.artifactDelete != nil && request.artifactDelete.owner != nil {
				request.artifactDelete.owner.finish(nil)
			}
			results <- result
			continue
		}
		call, cancel := context.WithTimeout(ctx, 10*time.Second)
		if request.readerRecovery != nil {
			result.readerRecovery, result.err = runArtifactReaderRecovery(call, request)
		} else if request.downloadClose != nil || request.closeOwner != nil {
			result.downloadClose, result.err = runArtifactDownloadClose(call, request)
		} else if request.artifactDelete != nil {
			result.deletion, result.err = runArtifactDelete(call, request)
		} else if request.workspaceRetirement != nil {
			result.retirement, result.err = runWorkspaceRetirement(call, request)
		} else if request.physicalOwner != nil {
			if !physicalRequestShape(request) {
				result.err = ErrInvalid
			} else {
				physical := request.physicalOwner.physicalQuantum(call)
				result.physical = &physical
			}
		} else if request.record == nil {
			result.sample = sampleStorage(call, request.directory, request.reusable, request.usage)
		} else if err := validateCleanup(*request.record, request.target); err != nil {
			result.err = err
		} else {
			switch request.record.Operation {
			case store.OutputCleanup:
				result.err = request.target.Retention.RemoveOutput(call, request.record.Output)
			case store.NativeCleanup:
				result.err = request.target.Retention.RemoveExecution(call, request.record.Execution)
			}
		}
		cancel()
		results <- result
	}
}
func validateCleanup(r store.CleanupRecord, t backend.Target) error {
	if r.Sequence == 0 || !jobName.MatchString(r.ID) || !jobName.MatchString(r.HeaderID) || r.Target != t.Name || r.Binding == "" || r.Binding != t.Binding || t.Retention == nil {
		return errors.New("cleanup route or identity unavailable")
	}
	switch r.Operation {
	case store.OutputCleanup:
		ref := r.Output
		if t.OutputStorage != backend.LocalOutput || t.Retention.CheckOutputClosed == nil || t.Retention.RemoveOutput == nil || r.Stopped || r.Execution.ID != "" || r.Proof.State != "" || r.Proof.Termination != 0 || r.Proof.ExitCode != nil || r.Proof.Instance != "" || r.TaskDigest != ([32]byte{}) || !jobName.MatchString(ref.Owner) || !jobName.MatchString(ref.JobID) || !jobName.MatchString(ref.AttemptID) || ref.Binding != r.Binding || ref.Generation != 1 || !validClosureID(ref.ClosureID) {
			return errors.New("output cleanup lacks exact closed domain")
		}
	case store.NativeCleanup:
		ref, p := r.Execution, r.Proof
		if t.Retention.RemoveExecution == nil || !r.Stopped || ref.Task.Retry == nil || !jobName.MatchString(ref.ID) || ref.Binding != r.Binding || !validClosureID(ref.Instance) || p.Instance != ref.Instance || p.Termination != backend.StoppedWithExitCode || p.ExitCode == nil || *p.ExitCode < 0 || *p.ExitCode > 255 || !p.State.Terminal() || p.State == backend.Unknown || r.Output != (backend.OutputReference{}) {
			return errors.New("native cleanup lacks positive stopped identity")
		}
		normal, err := backend.NormalizeRetry(ref.Task.Retry)
		if err != nil || normal == nil || normal.MaxAttempts != ref.Task.Retry.MaxAttempts || normal.DelaySeconds != ref.Task.Retry.DelaySeconds || !slices.Equal(normal.ExitCodes, ref.Task.Retry.ExitCodes) {
			return errors.New("native cleanup retry policy is not accepted normalized data")
		}
		if err = Validate(ref.Task); err != nil {
			return errors.New("native cleanup task is invalid")
		}
		task, err := backend.TaskBytes(ref.Task)
		if err != nil || sha256.Sum256(task) != r.TaskDigest {
			return errors.New("native cleanup task digest differs")
		}
	default:
		return errors.New("invalid cleanup operation")
	}
	return nil
}
func (c *Control) maintain(ctx context.Context, m *maintenanceState) error {
	if m.pending != nil {
		return nil
	}
	now := time.Now()
	if c.physicalCloseWanted() && c.physicalRegistered() && !physicalMaintenanceHeld(m) {
		if previous := m.physicalResult; previous != nil && previous.request != nil && previous.request.physicalClose && previous.result.physical != nil && previous.result.physical.Registered && errors.Is(previous.result.physical.Err, ErrArtifactBusy) && now.Before(m.physicalDue) {
			return nil
		}
		return c.dispatchPhysical(m)
	}
	if now.Before(m.due) {
		return nil
	}
	m.due = now.Add(time.Second)
	if c.retention.enabled {
		c.retention.mu.Lock()
		if c.retention.sample == nil || now.Before(c.retention.sample.ObservedAt) || now.Sub(c.retention.sample.ObservedAt) >= time.Second {
			c.retention.wantSample.Store(true)
		}
		c.retention.mu.Unlock()
		if err := c.store.MaintainRetention(now.UTC()); err != nil {
			return err
		}
	}
	c.retention.mu.Lock()
	want := c.retention.wantSample.Load()
	c.retention.mu.Unlock()
	for checked := maintenanceClass(0); checked < maintenanceClassCount; checked++ {
		class := (m.nextClass + checked) % maintenanceClassCount
		admitted := false
		var err error
		switch class {
		case maintenancePhysicalClass:
			if c.wantPhysicalMaintenance(m, now) {
				err = c.dispatchPhysical(m)
				admitted = m.pending != nil
			}
		case maintenanceLegacySampleClass:
			if want {
				err = c.dispatchSample(m)
				admitted = m.pending != nil
			}
		case maintenanceRetainedCleanupClass:
			admitted, err = c.dispatchRetainedCleanup(m, now)
		case maintenanceWorkspaceClass:
			admitted, err = c.dispatchWorkspaceRetirement(m, now)
		case maintenanceArtifactExpiryClass:
			admitted, err = c.dispatchArtifactExpiry(m, now)
		}
		if err != nil {
			return err
		}
		if admitted {
			m.nextClass = (class + 1) % maintenanceClassCount
			return nil
		}
	}
	return nil
}

// Five finite ordinary classes share the same worker and common-four budget.
// A metadata expiry mark consumes a quantum without fabricating a worker call.
// Terminal original scan Close remains above the ordinary classes.
type maintenanceClass uint8

const (
	maintenancePhysicalClass maintenanceClass = iota
	maintenanceLegacySampleClass
	maintenanceRetainedCleanupClass
	maintenanceWorkspaceClass
	maintenanceArtifactExpiryClass
	maintenanceClassCount
)

func (c *Control) dispatchRetainedCleanup(m *maintenanceState, now time.Time) (bool, error) {
	record, found, err := c.store.NextCleanup(m.after)
	if err != nil || !found {
		return false, err
	}
	m.after = record.Sequence
	target, ok := c.target(record.Target)
	if !ok || validateCleanup(record, target) != nil {
		return true, c.store.FailCleanup(record.ID, record.Digest, "cleanup_unavailable", now.UTC())
	}
	c.retention.mu.Lock()
	blocked, err := c.blockedLocked()
	if err != nil {
		c.retention.mu.Unlock()
		return false, err
	}
	id := record.Execution.ID
	if record.Operation == store.OutputCleanup {
		id = record.Output.AttemptID
	}
	pending := false
	for _, x := range blocked {
		if x == id {
			pending = true
			break
		}
	}
	c.retention.mu.Unlock()
	if pending {
		return false, nil
	}
	if m.serial == ^uint64(0) {
		return false, ErrUnavailable
	}
	request := maintenanceRequest{sequence: m.serial + 1, record: &record, target: cloneTarget(target)}
	m.serial = request.sequence
	m.pending = &request
	m.input <- request
	return true, nil
}

func (c *Control) dispatchSample(m *maintenanceState) error {
	reusable, err := c.store.ReusableBytes()
	if err != nil {
		return err
	}
	var reservedBytes, reservedFiles uint64
	if c.retention.enabled {
		reservedBytes, reservedFiles, err = c.store.OutputReservations()
		if err != nil {
			return err
		}
	}
	c.retention.mu.Lock()
	c.retention.wantSample.Store(false)
	usage := c.retention.local.OutputUsage
	c.retention.mu.Unlock()
	request := maintenanceRequest{directory: c.store.Directory(), reusable: reusable, usage: usage, reservedBytes: reservedBytes, reservedFiles: reservedFiles}
	if m.serial == ^uint64(0) {
		return ErrUnavailable
	}
	m.serial++
	request.sequence = m.serial
	m.pending = &request
	m.input <- request
	return nil
}
func (c *Control) integrateMaintenance(m *maintenanceState, r maintenanceResult) error {
	if r.request.readerRecovery != nil || r.readerRecovery != nil || m.pending != nil && m.pending.readerRecovery != nil {
		return c.integrateArtifactReaderRecovery(m, r)
	}
	if r.request.downloadClose != nil || r.request.closeOwner != nil || r.downloadClose != nil || m.pending != nil && (m.pending.downloadClose != nil || m.pending.closeOwner != nil) {
		return c.integrateArtifactDownloadClose(m, r)
	}
	if r.request.artifactDelete != nil || r.deletion != nil || m.pending != nil && m.pending.artifactDelete != nil {
		return c.integrateArtifactDelete(m, r)
	}
	if r.request.workspaceRetirement != nil || r.retirement != nil || m.pending != nil && m.pending.workspaceRetirement != nil {
		return c.integrateWorkspaceRetirement(m, r)
	}
	if r.request.physicalOwner != nil || r.physical != nil || m.pending != nil && m.pending.physicalOwner != nil {
		return c.integratePhysical(m, r)
	}
	if m.pending == nil {
		return nil
	}
	pending := m.pending
	if pending.sequence != r.request.sequence {
		return nil
	}
	if pending.record == nil {
		if r.request.record != nil || r.request.directory != pending.directory || r.request.reusable != pending.reusable {
			return nil
		}
		if r.sample == nil {
			return fmt.Errorf("%w: missing storage observation", ErrUnavailable)
		}
		c.retention.mu.Lock()
		c.retention.sample = cloneRetentionAdmin(r.sample)
		c.retention.reservedBytes = pending.reservedBytes
		c.retention.reservedFiles = pending.reservedFiles
		c.retention.mu.Unlock()
		m.pending = nil
		return nil
	}
	if r.request.record == nil || r.request.record.ID != pending.record.ID || r.request.record.Operation != pending.record.Operation || r.request.record.Digest != pending.record.Digest || r.request.target.Name != pending.target.Name || r.request.target.Binding != pending.target.Binding {
		return nil
	}
	m.pending = nil
	if r.err != nil {
		return c.store.FailCleanup(pending.record.ID, pending.record.Digest, "cleanup_failed", time.Now().UTC())
	}
	err := c.store.FinishCleanup(pending.record.ID, pending.record.Operation, pending.record.Digest, time.Now().UTC())
	if err == nil {
		c.retention.wantSample.Store(true)
	}
	return err
}
