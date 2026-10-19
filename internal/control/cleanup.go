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
	sequence      uint64
	record        *store.CleanupRecord
	target        backend.Target
	directory     string
	reusable      uint64
	reservedBytes uint64
	reservedFiles uint64
	usage         func(context.Context) (uint64, uint64, error)
}
type maintenanceResult struct {
	request maintenanceRequest
	sample  *RetentionAdmin
	err     error
}
type maintenanceState struct {
	input       chan maintenanceRequest
	results     chan maintenanceResult
	pending     *maintenanceRequest
	after       uint64
	due         time.Time
	sampledLast bool
	serial      uint64
}

func newMaintenance(ctx context.Context) *maintenanceState {
	m := &maintenanceState{input: make(chan maintenanceRequest, 1), results: make(chan maintenanceResult, 1)}
	go runMaintenance(ctx, m.input, m.results)
	return m
}
func runMaintenance(ctx context.Context, input <-chan maintenanceRequest, results chan<- maintenanceResult) {
	for {
		select {
		case <-ctx.Done():
			return
		case request := <-input:
			if ctx.Err() != nil {
				return
			}
			call, cancel := context.WithTimeout(ctx, 10*time.Second)
			result := maintenanceResult{request: request}
			if request.record == nil {
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
			select {
			case <-ctx.Done():
				return
			case results <- result:
			}
		}
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
	if want && !m.sampledLast {
		return c.dispatchSample(m)
	}
	record, found, err := c.store.NextCleanup(m.after)
	if err != nil {
		return err
	}
	if found {
		m.after = record.Sequence
		target, ok := c.target(record.Target)
		if !ok || validateCleanup(record, target) != nil {
			return c.store.FailCleanup(record.ID, record.Digest, "cleanup_unavailable", now.UTC())
		}
		c.retention.mu.Lock()
		blocked, err := c.blockedLocked()
		if err != nil {
			c.retention.mu.Unlock()
			return err
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
		if !pending {
			request := maintenanceRequest{record: &record, target: cloneTarget(target)}
			if m.serial == ^uint64(0) {
				return ErrUnavailable
			}
			m.serial++
			request.sequence = m.serial
			m.pending = &request
			m.sampledLast = false
			m.input <- request
			return nil
		}
	}
	if want {
		return c.dispatchSample(m)
	}
	m.sampledLast = false
	return nil
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
	m.sampledLast = true
	m.input <- request
	return nil
}
func (c *Control) integrateMaintenance(m *maintenanceState, r maintenanceResult) error {
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
