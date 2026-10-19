package local

import (
	"errors"
	"strings"
)

// attachRetention holds b.mu. The trusted pre-start binding owns this row;
// ordinary New never adds retention state or changes command payload lifetime.
func (b *Backend) attachRetention(e *execution, id string) {
	if b.retention != nil {
		e.retained = b.retention.outputs[id]
	}
}

// finishPayload holds b.mu and is called only after the lifecycle's last command
// access. A late sole reaper retries it after actual Wait completion. The output
// goroutine has published completion after its last reader/file access. No caller
// can replace these original owners with status or timeout-based assumptions.
func (b *Backend) finishPayload(e *execution) {
	r := e.retained
	if r == nil || r.released {
		return
	}
	if e.owned != nil {
		select {
		case <-e.owned.outputDone:
		default:
			return
		}
	} else if e.retry != nil {
		select {
		case <-e.retry.outputDone:
		default:
			return
		}
	}
	e.output.mu.Lock()
	closed := e.output.closed && e.output.markerCloseErr == nil
	e.output.mu.Unlock()
	if !closed {
		return
	}
	b.publishOutputClosure(r)
	if e.owned != nil {
		select {
		case <-e.owned.reaped:
		default:
			return
		}
		if !e.owned.signalClosed || !e.owned.handleClosed {
			return
		}
	} else if e.retry != nil {
		select {
		case <-e.retry.reaped:
		default:
			return
		}
		if !e.retry.handleClosed || !e.retry.reaping && e.cmd.Process != nil {
			return
		}
	}
	// Completion closes signal admission before this point. A timer callback that
	// was already queued takes b.mu only after done is closed; stop checks done
	// before dereferencing cmd. A default callback also sees signalClosed. Thus
	// Stop need not join a callback while holding the lock that callback needs.
	e.cmd = nil
	e.output.mu.Lock()
	e.output.file = nil
	e.output.err = nil
	e.output.mu.Unlock()
	if e.owned != nil {
		e.owned.reader = nil
		e.owned.handleErr = nil
		e.owned.waitErr = nil
		e.owned.outputResult = defaultOutputResult{}
		if e.owned.cleanupErr != nil {
			e.owned.cleanupErr = errors.New(strings.Clone(retryReason(e.owned.cleanupErr.Error())))
		}
	}
	if e.retry != nil {
		e.retry.reader = nil
		e.retry.startErr = nil
	}
	e.status.Reason = strings.Clone(retryReason(e.status.Reason))
	r.closed, r.released = true, true
}

// closeUnstartedOutput holds b.mu. No execution goroutine or Cmd owner was
// installed on these early startup paths; the retained row contains only a hash.
func (b *Backend) closeUnstartedOutput(out *output) {
	if err := out.file.Close(); err == nil && out.retained != nil {
		out.closed = true
		b.publishOutputClosure(out.retained)
		out.retained.released = true
	}
}

// publishOutputClosure holds b.mu and runs only after actual writer/reader closure.
// A MAC authenticates this prior identity on a later lost-reply absence retry;
// it does not authenticate payload release or execution termination.
func (b *Backend) publishOutputClosure(r *retainedOutput) {
	if r.closure == "" {
		r.closure = b.outputClosure(r.reference)
	}
	r.closed = true
}
