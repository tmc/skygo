package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/workflow"
	bolt "go.etcd.io/bbolt"
)

// ErrWorkflowStale reports a candidate whose durable facts changed before
// its admission transaction: cancellation, a binding, a dependency or input
// change, different authority or a different resolved Task.
var ErrWorkflowStale = errors.New("workflow candidate is stale")

// Workflow is one accepted static workflow. A bound node's execution state is
// its child Job's; Store keeps no second execution record.
type Workflow struct {
	ID          string
	Principal   string
	Domain      RequestDomain
	RequestID   string
	GraphDigest [32]byte
	Graph       workflow.Graph
	CreatedAt   time.Time
	Canceled    bool
	Nodes       []WorkflowNode
}

// WorkflowDisposition is a node's logical fact. A Pending node may be bound to
// a child Job; Skipped and Canceled nodes never are.
type WorkflowDisposition uint8

const (
	WorkflowPending WorkflowDisposition = iota + 1
	WorkflowSkipped
	WorkflowCanceled
)

// WorkflowNode reports one node in declared order. State is the bound child's
// current state, or its terminal state after the child Job was retired.
type WorkflowNode struct {
	Name        string
	Disposition WorkflowDisposition
	JobID       string
	State       backend.State
	Input       *ArtifactRef
}

// WorkflowPage is one authenticated listing page. After is a scope ordinal.
type WorkflowPage struct {
	Workflows []Workflow
	After     uint64
	More      bool
}

// WorkflowCandidate is one admissible node, for trusted reconciliation only.
// Task is fully resolved. Revision is the workflow's eligibility revision,
// which admission binds into the child's node; cancellation, a dependency
// skip or an input publication advances it, a sibling admission does not.
// Authority is the persisted file delegation that the caller must check
// fresh before planning and before admission.
type WorkflowCandidate struct {
	Workflow  string
	Node      int
	Revision  uint64
	Principal string
	Authority auth.Delegation
	Task      backend.Task
}

// WorkflowAdmission is the caller's plan for one candidate. A nonnil Policy
// selects the remote data route. With retention enabled, Storage and Reserve
// follow SubmitRetained; disabled retention ignores them, as Submit does.
type WorkflowAdmission struct {
	Candidate       WorkflowCandidate
	Target, Binding string
	Storage         backend.OutputStorage
	Reserve         uint64
	Policy          *backend.DataPolicy
}

// SubmitWorkflow atomically accepts g under requestID in the actor's
// namespace, or returns the workflow already bound to an identical graph
// before any capacity check. A nil domain selects disabled retention, as Submit
// does. The same request identity bound to a job, or to a different graph, is
// ErrConflict. authority is the actor's file delegation, checked fresh by the
// caller before a new acceptance; Store keeps it unchanged and never derives
// authority from it.
func (s *Store) SubmitWorkflow(actor auth.Actor, domain *RequestDomain, requestID string, g workflow.Graph, authority auth.Delegation) (out Workflow, err error) {
	if err = s.authorize(actor, auth.Mutate); err != nil {
		return
	}
	if !validRetainedRequestID(requestID) {
		return out, errors.New("invalid request ID")
	}
	if domain != nil && (!validAttemptID(domain.Lineage) || domain.Epoch == 0) {
		return out, errors.New("invalid request domain")
	}
	graph, err := workflow.Canonical(g)
	if err != nil {
		return
	}
	if len(graph) > workflowGraphLimit {
		return out, errors.New("workflow graph exceeds limit")
	}
	id, err := newID()
	if err != nil {
		return
	}
	var result error
	err = s.db.Update(func(tx *bolt.Tx) error {
		var e error
		out, result, e = acceptWorkflow(tx, actor.Principal(), domain, requestID, graph, authority, id, s.now().UTC())
		return e
	})
	if err == nil {
		err = result
	}
	return
}

// LookupWorkflow returns the workflow that SubmitWorkflow would replay for
// requestID and g in the actor's namespace, with the same domain rules. A miss
// reports found false and creates nothing; it carries the typed expiry that an
// acceptance would return. The same request identity bound to a job, or to a
// different graph, is ErrConflict; an expired witness is ResultExpired.
func (s *Store) LookupWorkflow(actor auth.Actor, domain *RequestDomain, requestID string, g workflow.Graph) (out Workflow, found bool, err error) {
	if err = s.authorize(actor, auth.Read); err != nil {
		return
	}
	if !validRetainedRequestID(requestID) {
		return out, false, errors.New("invalid request ID")
	}
	if domain != nil && (!validAttemptID(domain.Lineage) || domain.Epoch == 0) {
		return out, false, errors.New("invalid request domain")
	}
	graph, err := workflow.Canonical(g)
	if err != nil {
		return
	}
	if len(graph) > workflowGraphLimit {
		return out, false, errors.New("workflow graph exceeds limit")
	}
	var result error
	look := func(tx *bolt.Tx) error {
		var e error
		out, found, result, e = lookupWorkflow(tx, actor.Principal(), domain, requestID, graph, s.now().UTC())
		return e
	}
	// Only the retention gate writes; without it the lookup is read-only.
	gate := domain != nil
	if !gate {
		err = s.db.View(func(tx *bolt.Tx) error {
			st, e := loadRetention(tx)
			if e != nil || st.Enabled {
				gate = st.Enabled
				return e
			}
			return look(tx)
		})
	}
	if err == nil && gate {
		err = s.db.Update(look)
	}
	if err == nil {
		err = result
	}
	return
}

// GetWorkflowFor reads an authorized workflow. Foreign and missing
// identifiers both return ErrNotFound; a retired workflow is not found.
func (s *Store) GetWorkflowFor(actor auth.Actor, id string) (out Workflow, err error) {
	if err = s.authorize(actor, auth.Read); err != nil {
		return
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		if !validAttemptID(id) {
			return ErrNotFound
		}
		st, e := loadWorkflowState(tx, id)
		if errors.Is(e, ErrNotFound) || e == nil && !workflowVisible(tx, actor, st) {
			return ErrNotFound
		}
		if e != nil {
			return e
		}
		out, e = workflowView(tx, id)
		return e
	})
	return
}

// ListWorkflowsFor lists the authenticated scope in acceptance order. A
// nonadmin pages by its own dense ordinal, never the global count.
func (s *Store) ListWorkflowsFor(actor auth.Actor, after uint64, limit int) (out WorkflowPage, err error) {
	if err = s.authorize(actor, auth.Read); err != nil {
		return
	}
	if limit < 1 || limit > 200 {
		return out, errors.New("list limit outside 1..200")
	}
	if after == math.MaxUint64 {
		return
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(workflowsBucket)
		if b == nil {
			return nil
		}
		prefix := []byte("i/")
		if isScoped(tx) && actor.Role() != auth.Admin {
			prefix = workflowScopePrefix(actor.Principal())
		}
		c := b.Cursor()
		for k, v := c.Seek(append(bytes.Clone(prefix), sequenceKey(after+1)...)); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if len(k) != len(prefix)+8 {
				return errors.New("invalid workflow index")
			}
			if len(out.Workflows) == limit {
				out.More = true
				break
			}
			w, e := workflowView(tx, string(v))
			if e != nil {
				return e
			}
			out.Workflows = append(out.Workflows, w)
			out.After = binary.BigEndian.Uint64(k[len(prefix):])
		}
		return nil
	})
	return
}

// CancelWorkflowFor atomically closes further admission, cancels every
// unbound node and releases only their logical input reservations. Bound
// children receive cancellation intent one per WorkflowCandidates turn; no
// stop is implied by either step. Repeating it returns the workflow unchanged.
func (s *Store) CancelWorkflowFor(actor auth.Actor, id string) (out Workflow, err error) {
	if err = s.authorize(actor, auth.Mutate); err != nil {
		return
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		if !validAttemptID(id) {
			return ErrNotFound
		}
		st, e := loadWorkflowState(tx, id)
		if errors.Is(e, ErrNotFound) || e == nil && !workflowVisible(tx, actor, st) {
			return ErrNotFound
		}
		if e != nil {
			return e
		}
		if !st.Canceled {
			if e = cancelWorkflow(tx, id, st); e != nil {
				return e
			}
		}
		out, e = workflowView(tx, id)
		return e
	})
	return
}

// WorkflowCandidates is the single bounded fair selector for trusted
// reconciliation. It visits at most four workflows after its cursor and
// returns at most four admissible nodes, one ready node from each visited
// workflow per round, so every visited workflow with a ready node is offered.
// Each workflow's ready nodes are taken from a start that moves on by one
// node per pass of the cursor, so nodes Control keeps holding cannot hide the
// rest. When none of the visited workflows needs settling the call is
// read-only and commits nothing. Otherwise one write transaction settles at
// most ONE workflow: it skips nodes after a definite dependency failure,
// delivers one bound child's cancellation intent, or retires a finished
// workflow's payload while keeping its typed request witness.
func (s *Store) WorkflowCandidates() (out []WorkflowCandidate, err error) {
	s.wf.mu.Lock()
	defer s.wf.mu.Unlock()
	pos := s.wf.next
	next, settle := pos, false
	// An ordinary poll is read-only: it neither commits nor syncs.
	err = s.db.View(func(tx *bolt.Tx) error {
		var e error
		out, next, settle, e = workflowScan(tx, pos)
		return e
	})
	if err == nil && settle {
		err = s.db.Update(func(tx *bolt.Tx) error {
			var e error
			out, next, e = workflowAdvance(tx, pos)
			return e
		})
	}
	if err != nil {
		return nil, err
	}
	s.wf.next = next
	return out, nil
}

// workflowCursor is a Store's position in the global workflow index. It is
// only a fairness hint and is kept only in memory: no transaction writes or
// charges it, and a reopened Store starts again at the beginning.
type workflowCursor struct {
	mu   sync.Mutex
	next workflowPosition
}

// AdmitWorkflowChild atomically accepts one candidate as an ordinary child
// job with no public request, binds it to its node and, for an input edge,
// moves the edge's protection to the child's input pin. Repeating an admitted
// candidate returns its child before any other check. The caller must first
// check fresh delegation, target capability and the full resolved Task.
func (s *Store) AdmitWorkflowChild(a WorkflowAdmission) (j backend.Job, err error) {
	task := backend.CloneTask(a.Candidate.Task)
	kind := submissionOrdinary
	switch {
	case a.Policy != nil:
		kind = submissionRemote
	case hasWorkspaceFields(task):
		kind = submissionWorkspace
	}
	if err = validateSubmitWorkspace(task, kind); err != nil {
		return
	}
	id, err := newID()
	if err != nil {
		return
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		var e error
		j, e = admitWorkflowChild(tx, a, task, kind, id, s.now().UTC())
		return e
	})
	return
}

func workflowVisible(tx *bolt.Tx, actor auth.Actor, st workflowState) bool {
	return !isScoped(tx) || actor.Role() == auth.Admin || st.Owner == actor.Principal()
}
