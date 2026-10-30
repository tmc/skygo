package control

import "errors"

// PendingResults reports original worker results awaiting their exact Store
// acknowledgment, local custody held after acknowledgment, and admitted worker
// log lifetimes. A nil or zero Workers has no pending results.
//
// The caller must first fence new admission and positively join its Step loop
// and RPC work. PendingResults only inspects memory: it does not stop admission,
// join calls, read the Store, retry a record, close an owner or release a lease.
// A nonnil error retains the original private records and their saved causes.
// It wraps backend.ErrWorkerUnsettled only when an original cause does.
//
// Nil describes this local snapshot, not native absence, group completion,
// global idleness or permission to refund a reservation. In particular, a
// durably recorded unknown native effect may have no pending local result.
func (w *Workers) PendingResults() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.logMu.Lock()
	defer w.logMu.Unlock()

	p := &workerPendingResults{workers: w}
	var causes []error
	for _, b := range w.barriers {
		if b == nil {
			continue
		}
		for _, result := range []*barrierResult{b.pending, b.held} {
			if result != nil {
				p.barriers = append(p.barriers, result)
				causes = append(causes, result.cause, result.recordErr)
			}
		}
	}
	for _, owners := range w.logFailures {
		for _, result := range owners {
			if result != nil {
				p.logs = append(p.logs, result)
				causes = append(causes, result.cause, result.recordErr)
			}
		}
	}
	for _, count := range w.logs {
		if count > 0 {
			p.activeLogs += count
		}
	}
	if len(p.barriers) == 0 && len(p.logs) == 0 && p.activeLogs == 0 {
		return nil
	}
	p.cause = errors.Join(causes...)
	return p
}

// workerPendingResults keeps the actual records alive. Its error chain is a
// snapshot: Unwrap never reads record fields that a later Store-only retry
// might change. Active log counts retain their Workers, not a proof of native
// reader or enclosing response closure; those original owners must still join.
type workerPendingResults struct {
	workers    *Workers
	barriers   []*barrierResult
	logs       []*workerLogResult
	activeLogs int
	cause      error
}

func (p *workerPendingResults) Error() string { return "worker results remain pending" }
func (p *workerPendingResults) Unwrap() error { return p.cause }
