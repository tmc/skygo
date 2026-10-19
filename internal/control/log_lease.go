package control

import (
	"context"

	"fmt"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

// LogLease owns one fixed output domain through backend and HTTP completion.
// Close is idempotent and must follow the enclosing HTTP response's completion.
type LogLease struct {
	control                             *Control
	jobID, attemptID, binding, instance string
	cursor                              logCursor
	active                              bool
	closed                              bool
}

// AttemptID returns the immutable selected execution ID.
func (l *LogLease) AttemptID() string { return l.attemptID }

// Offset returns the validated opening offset, including a supplied cursor.
func (l *LogLease) Offset() int64 { return l.cursor.Offset }

// Cursor returns a canonical resume position without granting read authority.
func (l *LogLease) Cursor(offset int64) (string, error) {
	if offset < 0 || offset > backend.MaxLogBytes {
		return "", fmt.Errorf("%w: invalid log offset", ErrInvalid)
	}
	cur := l.cursor
	cur.Offset = offset
	return encodeLogCursor(cur)
}

// OpenLogs resolves an attempt once and registers one of at most32 leases.
// With a cursor, offset must be zero and an explicit ordinal must agree.
func (c *Control) OpenLogs(actor auth.Actor, id string, ordinal uint32, cursor string, offset int64) (*LogLease, error) {
	return c.openLogs(actor, id, "", ordinal, cursor, offset)
}
func (c *Control) openLogs(actor auth.Actor, id, selected string, ordinal uint32, token string, offset int64) (*LogLease, error) {
	actor, err := c.engine.Check(actor, auth.Read)
	if err != nil {
		return nil, err
	}
	if !jobName.MatchString(id) || offset < 0 || offset > backend.MaxLogBytes {
		return nil, fmt.Errorf("%w: invalid log selector or offset", ErrInvalid)
	}
	c.retention.mu.Lock()
	defer c.retention.mu.Unlock()
	j, err := c.Get(actor, id)
	if err != nil {
		// Preserve an authorized expiry witness before returning generic absence.
		if e := c.store.CheckLogDomain(actor, id, selected); e != nil {
			return nil, e
		}
		return nil, err
	}
	info, err := c.store.RetentionInfo(actor)
	if err != nil {
		return nil, err
	}
	cur := c.scopeLogCursor(actor, info.Domain.Lineage)
	cur.Job = id
	cur.Generation = 1
	if token != "" {
		if offset != 0 {
			return nil, fmt.Errorf("%w: cursor conflicts with offset", ErrInvalid)
		}
		parsed, e := decodeLogCursor(token)
		if e != nil {
			return nil, e
		}
		expected := cur
		expected.Lineage = parsed.Lineage
		expected.Attempt = parsed.Attempt
		expected.Offset = parsed.Offset
		if parsed != expected || !jobName.MatchString(parsed.Attempt) || parsed.Offset < 0 || parsed.Offset > backend.MaxLogBytes {
			return nil, fmt.Errorf("%w: log cursor scope differs", ErrInvalid)
		}
		if parsed.Lineage != cur.Lineage {
			return nil, &store.ExpiryError{Reason: store.RequestLineageChanged}
		}
		cur = parsed
		selected = parsed.Attempt
	}
	if selected == "" || ordinal != 0 {
		var wanted string
		if len(j.Attempts) == 0 {
			if ordinal > 1 {
				return nil, fmt.Errorf("%w: invalid attempt ordinal", ErrInvalid)
			}
			wanted = j.ID
		} else if ordinal == 0 {
			wanted = j.CurrentAttemptID()
		} else {
			if ordinal > uint32(len(j.Attempts)) {
				return nil, fmt.Errorf("%w: invalid attempt ordinal", ErrInvalid)
			}
			wanted = j.Attempts[ordinal-1].ID
		}
		if selected != "" && selected != wanted {
			return nil, fmt.Errorf("%w: cursor conflicts with attempt", ErrInvalid)
		}
		selected = wanted
	}
	status, _, err := logStatus(j, selected)
	if err != nil {
		return nil, err
	}
	if err = c.store.CheckLogDomain(actor, id, selected); err != nil {
		return nil, err
	}
	if len(c.retention.leases) >= 32 {
		return nil, fmt.Errorf("%w: log lease limit reached", ErrUnavailable)
	}
	cur.Attempt = selected
	if token == "" {
		cur.Offset = offset
	}
	l := &LogLease{control: c, jobID: id, attemptID: selected, binding: j.TargetBinding, instance: status.Instance, cursor: cur}
	c.retention.leases[l] = struct{}{}
	return l, nil
}

// Check rechecks fresh authorization and the fixed output domain before Send.
func (l *LogLease) Check(actor auth.Actor) error {
	c := l.control
	actor, err := c.engine.Check(actor, auth.Read)
	if err != nil {
		return err
	}
	c.retention.mu.Lock()
	defer c.retention.mu.Unlock()
	if l.closed {
		return ErrUnavailable
	}
	if _, ok := c.retention.leases[l]; !ok {
		return ErrUnavailable
	}
	if err = c.store.CheckLogDomain(actor, l.jobID, l.attemptID); err != nil {
		return err
	}
	j, err := c.Get(actor, l.jobID)
	if err != nil {
		return err
	}
	status, _, err := logStatus(j, l.attemptID)
	if err != nil {
		return err
	}
	if j.TargetBinding != l.binding || l.instance != "" && status.Instance != l.instance {
		return ErrLogsUnavailable
	}
	return nil
}

// Read holds backend ownership until the actual backend call returns, including
// when cancellation is ignored. Concurrent reads on one lease are refused.
func (l *LogLease) Read(ctx context.Context, actor auth.Actor, offset int64) (backend.LogChunk, error) {
	if err := l.Check(actor); err != nil {
		return backend.LogChunk{}, err
	}
	c := l.control
	c.retention.mu.Lock()
	if l.closed || l.active {
		c.retention.mu.Unlock()
		return backend.LogChunk{}, ErrUnavailable
	}
	l.active = true
	c.retention.mu.Unlock()
	defer func() {
		c.retention.mu.Lock()
		l.active = false
		if l.closed {
			delete(c.retention.leases, l)
		}
		c.retention.mu.Unlock()
	}()
	return c.readLogs(ctx, actor, l.jobID, l.attemptID, offset)
}

// Close relinquishes HTTP ownership; an active backend read retains the lease.
func (l *LogLease) Close() {
	c := l.control
	c.retention.mu.Lock()
	defer c.retention.mu.Unlock()
	l.closed = true
	if !l.active {
		delete(c.retention.leases, l)
	}
}

// Logs reads one fixed attempt under a temporary lease. Streaming callers use
// OpenLogs and retain their lease across reads and enclosing HTTP completion.
func (c *Control) Logs(ctx context.Context, actor auth.Actor, id, attemptID string, offset int64) (backend.LogChunk, error) {
	l, err := c.openLogs(actor, id, attemptID, 0, "", offset)
	if err != nil {
		return backend.LogChunk{}, err
	}
	defer l.Close()
	return l.Read(ctx, actor, offset)
}
