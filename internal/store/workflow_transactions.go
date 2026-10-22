package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/workflow"
	bolt "go.etcd.io/bbolt"
)

// acceptWorkflow is graph acceptance: one transaction writes the immutable
// graph, the bounded state, both indexes, the typed parent request and every
// input edge reservation, or nothing. Exact replay precedes capacity checks.
func acceptWorkflow(tx *bolt.Tx, principal string, domain *RequestDomain, requestID string, graph []byte, authority auth.Delegation, id string, now time.Time) (out Workflow, result, err error) {
	b := tx.Bucket(workflowsBucket)
	if b == nil {
		return out, nil, errors.New("workflow metadata requires schema 8")
	}
	st, err := loadRetention(tx)
	if err != nil {
		return
	}
	var d RequestDomain
	switch {
	case domain != nil:
		d = *domain
	case st.Enabled:
		// Only replay of a parent accepted before activation remains possible.
		d = RequestDomain{st.Lineage, 0}
	}
	var high uint64
	if v := b.Get(workflowHighKey(principal)); v != nil {
		if len(v) != 8 {
			return out, nil, errors.New("invalid workflow scope high-water")
		}
		high = binary.BigEndian.Uint64(v)
	}
	key := requestKey(principal, d, requestID)
	l := new(artifactCoreLedger)
	wf := artifactCorePath{root: string(workflowsBucket)}
	l.bucket(tx, wf)
	for _, k := range [][]byte{workflowGraphKey(id), workflowStateKey(id), workflowIndexKey(b.Sequence() + 1), workflowScopeKey(principal, high+1), workflowHighKey(principal)} {
		l.row(tx, wf, k)
	}
	retention := artifactCorePath{root: string(retentionBucket)}
	l.row(tx, retention, retentionStateKey)
	l.row(tx, retention, retentionUsageKey)
	l.row(tx, artifactCorePath{string(retentionBucket), string(retainedScopesBucket)}, []byte(principal))
	l.row(tx, artifactCorePath{string(retentionBucket), string(retainedRequestsBucket)}, key)
	if domain != nil || st.Enabled {
		var r error
		if st, r, err = gateDomain(tx, d, now); err != nil || r != nil {
			return out, r, err
		}
	}
	digest := sha256.Sum256(graph)
	existing, found, r, err := workflowLookup(tx, principal, d, requestID, digest)
	if err != nil {
		return
	}
	if found && r == nil {
		out, err = workflowView(tx, existing)
		return
	}
	if found {
		return out, r, nil
	}
	if st.Enabled {
		if d.Epoch == 0 {
			return out, &ExpiryError{Reason: RequestDomainRequired}, nil
		}
		for _, ep := range st.Epochs {
			if ep.Number == d.Epoch && ep.AcceptClosed {
				return out, &ExpiryError{Reason: RequestEpochExpired}, nil
			}
		}
	}
	if !validWorkflowAuthority(authority, principal) {
		return out, nil, auth.ErrUnauthenticated
	}
	if b.Sequence() == math.MaxUint64 || high == math.MaxUint64 {
		return out, nil, errors.New("workflow sequence exhausted")
	}
	if b.Get(workflowGraphKey(id)) != nil || b.Get(workflowStateKey(id)) != nil {
		return out, nil, errors.New("random workflow identity collision")
	}
	var g workflow.Graph
	if err = json.Unmarshal(graph, &g); err != nil {
		return
	}
	names := workflowNames(g)
	err = workflowCoreChange(tx, l, workflowNeedsArtifacts(g), func(art *artifactState) error {
		ordinal, e := b.NextSequence()
		if e != nil {
			return e
		}
		ws := workflowState{Owner: principal, Domain: d, RequestID: requestID, Graph: hex.EncodeToString(digest[:]), Authority: authority, Created: now, Ordinal: ordinal, Scope: high + 1, Revision: 1, Nodes: make([]workflowNode, len(g.Nodes))}
		for i := range ws.Nodes {
			ws.Nodes[i].Disposition = WorkflowPending
		}
		state, e := encodeWorkflowState(id, ws)
		if e != nil {
			return e
		}
		type row struct{ k, v []byte }
		rows := []row{
			{workflowGraphKey(id), graph},
			{workflowStateKey(id), state},
			{workflowIndexKey(ordinal), []byte(id)},
			{workflowScopeKey(principal, ws.Scope), []byte(id)},
			{workflowHighKey(principal), sequenceKey(ws.Scope)},
		}
		// The graph key and state reservation, plus actual index rows. The
		// high-water row is charged once, when the principal first submits.
		metadata := uint64(len(rows[0].k)) + workflowStateCharge(len(ws.Nodes))
		for _, r := range rows[2:4] {
			metadata += uint64(len(r.k) + len(r.v))
		}
		if high == 0 {
			metadata += uint64(len(rows[4].k) + len(rows[4].v))
		}
		for _, r := range rows {
			if e := b.Put(r.k, r.v); e != nil {
				return e
			}
		}
		if e := adjust(tx, TaskBytesQuota, uint64(len(graph)), 0); e != nil {
			return e
		}
		if e := adjust(tx, MetadataBytesQuota, metadata, 0); e != nil {
			return e
		}
		if e := principalScope(tx, principal); e != nil {
			return e
		}
		if e := putRetentionRecord(tx, retainedRequestsBucket, key, requestRecord{Principal: principal, Domain: d, RequestID: requestID, WorkflowID: id, GraphDigest: ws.Graph}); e != nil {
			return e
		}
		for i, n := range g.Nodes {
			if n.Input == nil {
				continue
			}
			k := workflowInputKey(id, i)
			if _, ok := art.WorkflowInputs[k]; ok {
				return ErrArtifactConflict
			}
			art.WorkflowInputs[k] = workflowInput{Owner: principal, Workflow: id, Producer: uint8(names[n.Input.Node]), Consumer: uint8(i)}
		}
		return nil
	})
	if err != nil {
		return
	}
	out, err = workflowView(tx, id)
	return
}

// lookupWorkflow replays the typed parent request as acceptWorkflow does but
// never creates anything. A miss reports found false, with the typed expiry an
// acceptance would return.
func lookupWorkflow(tx *bolt.Tx, principal string, domain *RequestDomain, requestID string, graph []byte, now time.Time) (out Workflow, found bool, result, err error) {
	if tx.Bucket(workflowsBucket) == nil {
		return out, false, nil, errors.New("workflow metadata requires schema 8")
	}
	st, err := loadRetention(tx)
	if err != nil {
		return
	}
	var d RequestDomain
	switch {
	case domain != nil:
		d = *domain
	case st.Enabled:
		d = RequestDomain{st.Lineage, 0}
	}
	if domain != nil || st.Enabled {
		if st, result, err = gateDomain(tx, d, now); err != nil || result != nil {
			return
		}
	}
	id, found, result, err := workflowLookup(tx, principal, d, requestID, sha256.Sum256(graph))
	if err != nil || result != nil {
		return
	}
	if found {
		out, err = workflowView(tx, id)
		return
	}
	if st.Enabled {
		if d.Epoch == 0 {
			return out, false, &ExpiryError{Reason: RequestDomainRequired}, nil
		}
		for _, ep := range st.Epochs {
			if ep.Number == d.Epoch && ep.AcceptClosed {
				return out, false, &ExpiryError{Reason: RequestEpochExpired}, nil
			}
		}
	}
	return
}

// workflowLookup finds the typed parent request. The same identity bound to a
// job, or to a different graph, conflicts; an expired witness is typed.
func workflowLookup(tx *bolt.Tx, principal string, d RequestDomain, request string, digest [32]byte) (id string, found bool, result, err error) {
	var r requestRecord
	err = loadJSON(retentionTable(tx, retainedRequestsBucket), requestKey(principal, d, request), &r)
	if errors.Is(err, ErrNotFound) {
		return "", false, nil, nil
	}
	if err != nil {
		return
	}
	if r.Principal != principal || r.Domain != d || r.requestID() != request {
		return "", true, nil, errors.New("request identity hash collision")
	}
	if r.WorkflowID == "" || r.GraphDigest != hex.EncodeToString(digest[:]) {
		return "", true, ErrConflict, nil
	}
	if r.Expired {
		return r.WorkflowID, true, &ExpiryError{Reason: ResultExpired}, nil
	}
	return r.WorkflowID, true, nil, nil
}

func workflowView(tx *bolt.Tx, id string) (out Workflow, err error) {
	st, err := loadWorkflowState(tx, id)
	if err != nil {
		return
	}
	g, err := loadWorkflowGraph(tx, id)
	if err != nil {
		return
	}
	if len(g.Nodes) != len(st.Nodes) {
		return out, errors.New("workflow graph mismatch")
	}
	out = Workflow{ID: id, Principal: st.Owner, Domain: st.Domain, RequestID: st.RequestID, Graph: g, CreatedAt: st.Created, Canceled: st.Canceled}
	if _, err = hex.Decode(out.GraphDigest[:], []byte(st.Graph)); err != nil {
		return
	}
	for i, n := range st.Nodes {
		node := WorkflowNode{Name: g.Nodes[i].Name, Disposition: n.Disposition, JobID: n.Job}
		if n.Input != nil {
			ref := *n.Input
			node.Input = &ref
		}
		if n.Job != "" {
			if node.State, err = workflowJobState(tx, n); err != nil {
				return
			}
		}
		out.Nodes = append(out.Nodes, node)
	}
	return
}

func workflowJobState(tx *bolt.Tx, n workflowNode) (backend.State, error) {
	if n.Final != "" {
		return n.Final, nil
	}
	j, err := get(tx, n.Job)
	return j.Status.State, err
}

func cancelWorkflow(tx *bolt.Tx, id string, st workflowState) error {
	l := new(artifactCoreLedger)
	l.row(tx, artifactCorePath{root: string(workflowsBucket)}, workflowStateKey(id))
	l.row(tx, artifactCorePath{root: string(retentionBucket)}, retentionUsageKey)
	return workflowCoreChange(tx, l, false, func(art *artifactState) error {
		st.Canceled = true
		st.Revision++
		for i := range st.Nodes {
			n := &st.Nodes[i]
			if n.Job == "" && n.Disposition == WorkflowPending {
				n.Disposition = WorkflowCanceled
				if art != nil {
					delete(art.WorkflowInputs, workflowInputKey(id, i))
				}
			}
		}
		return putWorkflowState(tx, id, st)
	})
}

type workflowVisit struct {
	ordinal uint64
	id      string
	wrapped bool // visited after the index wrapped to its start
}

// workflowPosition is the selector's place in the global workflow index: the
// last visited ordinal and the number of times visits wrapped past the
// index's end. While the index is unchanged, visits wrap exactly once between
// two visits of one workflow.
type workflowPosition struct {
	ordinal uint64
	passes  uint64
}

// workflowVisits returns at most four live workflows after cursor, wrapping
// once to the start of the global index.
func workflowVisits(b *bolt.Bucket, cursor uint64) ([]workflowVisit, error) {
	var out []workflowVisit
	prefix := []byte("i/")
	c := b.Cursor()
	k, v := c.Seek(workflowIndexKey(cursor + 1))
	for wrapped := false; len(out) < 4; k, v = c.Next() {
		if k == nil || !bytes.HasPrefix(k, prefix) {
			if wrapped || cursor == 0 {
				break
			}
			wrapped = true
			k, v = c.Seek(prefix)
			if k == nil || !bytes.HasPrefix(k, prefix) {
				break
			}
		}
		if len(k) != 10 {
			return nil, errors.New("invalid workflow index")
		}
		n := binary.BigEndian.Uint64(k[2:])
		if wrapped && n > cursor {
			break
		}
		out = append(out, workflowVisit{n, string(v), wrapped})
	}
	return out, nil
}

// workflowPlan is one workflow's derived logical step. Unknown, retrying and
// running dependencies hold; a definite Failed or Canceled one skips.
type workflowPlan struct {
	ready  []int
	inputs map[int]workflowInput
	skip   []int
	cancel string
	done   bool
}

func workflowEvaluate(tx *bolt.Tx, art *artifactState, id string, st workflowState, g workflow.Graph) (p workflowPlan, err error) {
	if len(g.Nodes) != len(st.Nodes) {
		return p, errors.New("workflow graph mismatch")
	}
	names := workflowNames(g)
	const (
		waiting = iota
		succeeded
		blocked
	)
	outcome := make([]int, len(st.Nodes))
	unbound := func(i int) bool { return st.Nodes[i].Job == "" && st.Nodes[i].Disposition == WorkflowPending }
	for i, n := range st.Nodes {
		switch {
		case n.Disposition != WorkflowPending:
			outcome[i] = blocked
		case n.Job != "":
			s, e := workflowJobState(tx, n)
			if e != nil {
				return p, e
			}
			switch s {
			case backend.Succeeded:
				outcome[i] = succeeded
			case backend.Failed, backend.Canceled:
				outcome[i] = blocked
			}
		}
	}
	for changed := true; changed; {
		changed = false
		for i, n := range g.Nodes {
			if !unbound(i) || outcome[i] == blocked {
				continue
			}
			for _, d := range n.DependsOn {
				if outcome[names[d]] == blocked {
					outcome[i] = blocked
					p.skip = append(p.skip, i)
					changed = true
					break
				}
			}
		}
	}
	p.done = len(p.skip) == 0
	for i, n := range st.Nodes {
		if unbound(i) || n.Job != "" && !n.Retired {
			p.done = false
		}
	}
	if st.Canceled {
		for _, n := range st.Nodes {
			if n.Job == "" || n.Final != "" {
				continue
			}
			j, e := get(tx, n.Job)
			if e != nil {
				return p, e
			}
			if !j.Status.State.Terminal() && !j.CancelRequested {
				p.cancel = j.ID
				break
			}
		}
		return p, nil
	}
	p.inputs = map[int]workflowInput{}
	for i, n := range g.Nodes {
		if !unbound(i) || outcome[i] == blocked {
			continue
		}
		ready := true
		for _, d := range n.DependsOn {
			ready = ready && outcome[names[d]] == succeeded
		}
		if !ready {
			continue
		}
		if n.Input != nil {
			if art == nil {
				return p, ErrArtifactsDisabled
			}
			e, ok := art.WorkflowInputs[workflowInputKey(id, i)]
			if !ok {
				return p, artifactCorrupt()
			}
			if e.Ref == nil {
				continue
			}
			p.inputs[i] = e
		}
		p.ready = append(p.ready, i)
	}
	return p, nil
}

// workflowOffer is one visited workflow's evaluated step.
type workflowOffer struct {
	visit workflowVisit
	st    workflowState
	g     workflow.Graph
	p     workflowPlan
}

func workflowOfferLoad(tx *bolt.Tx, art *artifactState, v workflowVisit) (o workflowOffer, err error) {
	o.visit = v
	if o.st, err = loadWorkflowState(tx, v.id); err != nil {
		return
	}
	if o.g, err = loadWorkflowGraph(tx, v.id); err != nil {
		return
	}
	o.p, err = workflowEvaluate(tx, art, v.id, o.st, o.g)
	return
}

// workflowSelect fills at most four candidates from offers, taking one ready
// node from each offer per round. There are at most four offers, so each
// offer with a ready node contributes one. An offer's ready nodes are taken
// from its pass number modulo their count, so a workflow's start moves on by
// one node per visit. It returns the position of the last offer.
func workflowSelect(offers []workflowOffer, pos workflowPosition) ([]WorkflowCandidate, workflowPosition, error) {
	var out []WorkflowCandidate
	for round, took := 0, true; took && len(out) < 4; round++ {
		took = false
		for _, o := range offers {
			n := len(o.p.ready)
			if len(out) == 4 || round >= n {
				continue
			}
			pass := pos.passes
			if o.visit.wrapped {
				pass++
			}
			i := o.p.ready[(pass+uint64(round))%uint64(n)]
			var ref *ArtifactRef
			if e, ok := o.p.inputs[i]; ok {
				ref = e.Ref
			}
			task, err := workflowChildTask(o.g.Nodes[i].Task, ref)
			if err != nil {
				return nil, pos, err
			}
			out = append(out, WorkflowCandidate{Workflow: o.visit.id, Node: i, Revision: o.st.Revision, Principal: o.st.Owner, Authority: o.st.Authority, Task: task})
			took = true
		}
	}
	if len(offers) != 0 {
		last := offers[len(offers)-1].visit
		pos.ordinal = last.ordinal
		if last.wrapped {
			pos.passes++
		}
	}
	return out, pos, nil
}

// workflowScan is workflowAdvance's read-only evaluation of the visits after
// pos. It reports settle when one of them needs a skip, cancellation or
// retirement, which only workflowAdvance may write; otherwise it returns the
// same candidates and the next position.
func workflowScan(tx *bolt.Tx, pos workflowPosition) (out []WorkflowCandidate, next workflowPosition, settle bool, err error) {
	next = pos
	b := tx.Bucket(workflowsBucket)
	if b == nil {
		return
	}
	ret, err := loadRetention(tx)
	if err != nil {
		return
	}
	visits, err := workflowVisits(b, pos.ordinal)
	if err != nil || len(visits) == 0 {
		return
	}
	var art *artifactState
	if tx.Bucket(artifactsBucket) != nil {
		if art, err = artifactLoad(tx); err != nil {
			return
		}
	}
	var offers []workflowOffer
	for _, v := range visits {
		o, e := workflowOfferLoad(tx, art, v)
		if e != nil {
			return nil, pos, false, e
		}
		if len(o.p.skip) != 0 || o.p.cancel != "" || ret.Enabled && o.p.done {
			return nil, pos, true, nil
		}
		offers = append(offers, o)
	}
	out, next, err = workflowSelect(offers, pos)
	return
}

// workflowAdvance is WorkflowCandidates' settling transaction over the visits
// after pos. Every row it may write is registered before that write; a
// second settling workflow waits a turn. It returns the next position and
// writes no cursor.
func workflowAdvance(tx *bolt.Tx, pos workflowPosition) ([]WorkflowCandidate, workflowPosition, error) {
	b := tx.Bucket(workflowsBucket)
	if b == nil {
		return nil, pos, nil
	}
	ret, err := loadRetention(tx)
	if err != nil {
		return nil, pos, err
	}
	visits, err := workflowVisits(b, pos.ordinal)
	if err != nil || len(visits) == 0 {
		return nil, pos, err
	}
	l := new(artifactCoreLedger)
	l.row(tx, artifactCorePath{root: string(retentionBucket)}, retentionUsageKey)
	var out []WorkflowCandidate
	next := pos
	err = workflowCoreChange(tx, l, false, func(art *artifactState) error {
		var offers []workflowOffer
		settled := false
		for _, v := range visits {
			o, e := workflowOfferLoad(tx, art, v)
			if e != nil {
				return e
			}
			p := o.p
			retire := ret.Enabled && p.done
			if len(p.skip) != 0 || p.cancel != "" || retire {
				if settled {
					break
				}
				settled = true
				switch {
				case len(p.skip) != 0:
					e = workflowSkip(tx, l, art, v.id, o.st, p.skip)
				case p.cancel != "":
					e = workflowDeliverCancel(tx, l, p.cancel)
				default:
					e = workflowRetire(tx, l, v.id, o.st)
				}
				if e != nil {
					return e
				}
			}
			offers = append(offers, o)
		}
		var e error
		out, next, e = workflowSelect(offers, pos)
		return e
	})
	if err != nil {
		return nil, pos, err
	}
	return out, next, nil
}

func workflowSkip(tx *bolt.Tx, l *artifactCoreLedger, art *artifactState, id string, st workflowState, skip []int) error {
	l.row(tx, artifactCorePath{root: string(workflowsBucket)}, workflowStateKey(id))
	for _, i := range skip {
		st.Nodes[i].Disposition = WorkflowSkipped
		if art != nil {
			delete(art.WorkflowInputs, workflowInputKey(id, i))
		}
	}
	st.Revision++
	return putWorkflowState(tx, id, st)
}

// workflowDeliverCancel records ordinary cancellation intent on ONE bound
// child, exactly as Cancel does. It is not a stop or retirement receipt.
func workflowDeliverCancel(tx *bolt.Tx, l *artifactCoreLedger, job string) error {
	l.row(tx, artifactCorePath{root: string(jobsBucket)}, []byte(job))
	l.row(tx, artifactCorePath{string(retentionBucket), string(retainedJobsBucket)}, []byte(job))
	_, err := update(tx, job, func(j *backend.Job) error {
		if !j.Status.State.Terminal() {
			j.CancelRequested = true
		}
		return nil
	})
	return err
}

// workflowRetire removes a finished workflow's graph, state and indexes and
// releases their charges. The typed parent request stays, charged, as its
// expired replay witness; the principal high-water stays too.
func workflowRetire(tx *bolt.Tx, l *artifactCoreLedger, id string, st workflowState) error {
	b := tx.Bucket(workflowsBucket)
	keys := [][]byte{workflowGraphKey(id), workflowStateKey(id), workflowIndexKey(st.Ordinal), workflowScopeKey(st.Owner, st.Scope)}
	request := requestKey(st.Owner, st.Domain, st.RequestID)
	for _, k := range keys {
		l.row(tx, artifactCorePath{root: string(workflowsBucket)}, k)
	}
	l.row(tx, artifactCorePath{string(retentionBucket), string(retainedRequestsBucket)}, request)
	graph := uint64(len(b.Get(keys[0])))
	if graph == 0 {
		return errors.New("missing workflow graph")
	}
	metadata := uint64(len(keys[0])) + workflowStateCharge(len(st.Nodes)) + uint64(len(keys[2])+len(id)+len(keys[3])+len(id))
	for _, k := range keys {
		if err := b.Delete(k); err != nil {
			return err
		}
	}
	if err := adjust(tx, TaskBytesQuota, 0, graph); err != nil {
		return err
	}
	if err := adjust(tx, MetadataBytesQuota, 0, metadata); err != nil {
		return err
	}
	var r requestRecord
	if err := loadJSON(retentionTable(tx, retainedRequestsBucket), request, &r); err != nil {
		return err
	}
	if r.WorkflowID != id || r.Expired {
		return errors.New("workflow request mismatch")
	}
	r.Expired = true
	return putRetentionRecord(tx, retainedRequestsBucket, request, r)
}

// workflowChildTask resolves a node's Task: the template unchanged except
// normalized retry and, for an input edge, the producer's exact result Ref.
func workflowChildTask(t backend.Task, ref *ArtifactRef) (backend.Task, error) {
	t = backend.CloneTask(t)
	var err error
	if t.Retry, err = backend.NormalizeRetry(t.Retry); err != nil {
		return t, err
	}
	if ref != nil {
		if t.Workdir != nil {
			return t, ErrArtifactConflict
		}
		t.Workdir = &artifact.Ref{ID: ref.ID, SHA256: ref.SHA256, Size: int64(ref.Size)}
	}
	return t, nil
}

// admitWorkflowChild is child admission. The same transaction rechecks the
// node, cancellation, authority, dependencies and input, creates the ordinary
// job rows without a request, binds node and reverse row, pins the input and
// only then consumes the edge. Any failure rolls back every write.
func admitWorkflowChild(tx *bolt.Tx, a WorkflowAdmission, task backend.Task, kind submissionKind, id string, now time.Time) (j backend.Job, err error) {
	c := a.Candidate
	st, err := loadWorkflowState(tx, c.Workflow)
	if err != nil {
		return
	}
	if c.Node < 0 || c.Node >= len(st.Nodes) {
		return j, ErrWorkflowStale
	}
	digest, err := taskDigest(task)
	if err != nil {
		return
	}
	if n := st.Nodes[c.Node]; n.Job != "" {
		if n.Task != hex.EncodeToString(digest[:]) {
			return j, ErrWorkflowStale
		}
		if n.Final != "" {
			return j, &ExpiryError{Reason: ResultExpired, JobID: n.Job}
		}
		return get(tx, n.Job)
	}
	if st.Revision != c.Revision || st.Canceled || st.Nodes[c.Node].Disposition != WorkflowPending || st.Owner != c.Principal || st.Authority != c.Authority {
		return j, ErrWorkflowStale
	}
	ret, err := loadRetention(tx)
	if err != nil {
		return
	}
	storage, reserve := backend.UnspecifiedOutput, uint64(0)
	if ret.Enabled {
		if a.Storage == backend.LocalOutput && !ret.Policy.LocalOutputEnabled {
			return j, errors.New("local output retention is disabled")
		}
		if a.Storage != backend.LocalOutput && a.Storage != backend.RemoteOutput || a.Storage == backend.RemoteOutput && a.Reserve != 0 || a.Storage == backend.LocalOutput && a.Reserve != uint64(backend.MaxLogBytes)+4*ret.Policy.OutputUnit || a.Policy != nil && a.Storage != backend.RemoteOutput {
			return j, errors.New("invalid output placement charge")
		}
		storage, reserve = a.Storage, a.Reserve
	}
	if a.Target == "" || len(a.Target) > 128 || !validBinding(a.Binding) {
		return j, errors.New("invalid retained target")
	}
	data, err := backend.TaskBytes(task)
	if err != nil {
		return
	}
	if len(data) > 256<<10 {
		return j, errors.New("task exceeds limit")
	}
	if !ret.Enabled {
		// Disabled retention keeps Submit's whole-store limits.
		used := binary.BigEndian.Uint64(tx.Bucket(metadataBucket).Get(taskBytesKey))
		if used >= maxTaskBytes || uint64(len(data)) > maxTaskBytes-used || tx.Bucket(sequenceBucket).Sequence() >= maxJobs {
			return j, ErrFull
		}
	}
	g, err := loadWorkflowGraph(tx, c.Workflow)
	if err != nil {
		return
	}
	if workflowRow(tx, workflowChildKey(id)) != nil {
		return j, errors.New("random job identity collision")
	}
	l := artifactJobLedger(tx, id, st.Owner)
	l.row(tx, artifactCorePath{root: string(workflowsBucket)}, workflowStateKey(c.Workflow))
	l.row(tx, artifactCorePath{root: string(workflowsBucket)}, workflowChildKey(id))
	err = workflowCoreChange(tx, l, kind != submissionOrdinary, func(art *artifactState) error {
		p, e := workflowEvaluate(tx, art, c.Workflow, st, g)
		if e != nil {
			return e
		}
		if !slices.Contains(p.ready, c.Node) {
			return ErrWorkflowStale
		}
		edge, input := p.inputs[c.Node]
		want, e := workflowChildTask(g.Nodes[c.Node].Task, edge.Ref)
		if e != nil {
			return e
		}
		planned, e := backend.TaskBytes(want)
		if e != nil {
			return e
		}
		if !bytes.Equal(planned, data) {
			return ErrWorkflowStale
		}
		var remote *remoteSubmission
		if kind == submissionRemote {
			remote = &remoteSubmission{st.Authority, *a.Policy, now}
			if e := remote.validate(st.Owner, task); e != nil {
				return e
			}
		}
		var ref ArtifactRef
		if task.Workdir != nil {
			if ref, e = artifactTaskRef(task); e != nil {
				return e
			}
			if e = artifactGranted(art, st.Owner, ref, now); e != nil {
				return e
			}
		}
		if j, e = acceptJobRow(tx, id, st.Owner, task, a.Target, a.Binding, now); e != nil {
			return e
		}
		if e = trackJobMetadata(tx, j, retainedJob{Principal: st.Owner, Storage: storage, OutputReserve: reserve, WorkflowID: c.Workflow, Node: uint8(c.Node)}); e != nil {
			return e
		}
		if task.Workdir != nil {
			if e = artifactAcceptInput(tx, art, st.Owner, id, ref, now); e != nil {
				return e
			}
		}
		node := workflowNode{Disposition: WorkflowPending, Job: id, Task: hex.EncodeToString(digest[:]), Revision: st.Revision}
		if input {
			delete(art.WorkflowInputs, workflowInputKey(c.Workflow, c.Node))
			in := *edge.Ref
			node.Producer, node.Attempt, node.Input = edge.JobID, edge.AttemptID, &in
		}
		if remote != nil {
			if e = remoteAccept(art, id, st.Owner, remote); e != nil {
				return e
			}
		}
		st.Nodes[c.Node] = node
		if e = putWorkflowState(tx, c.Workflow, st); e != nil {
			return e
		}
		child, e := encodeWorkflowChild(id, workflowChild{c.Workflow, uint8(c.Node)})
		if e != nil {
			return e
		}
		return tx.Bucket(workflowsBucket).Put(workflowChildKey(id), child)
	})
	if err != nil {
		return backend.Job{}, err
	}
	return j, nil
}

// workflowHeld reports whether a child Job must stay: its node can capture
// only a definite terminal state, and an unbound dependent, by control or
// input edge, may still need its outcome.
func workflowHeld(tx *bolt.Tx, j backend.Job) (bool, error) {
	var m retainedJob
	err := loadJSON(retentionTable(tx, retainedJobsBucket), []byte(j.ID), &m)
	if errors.Is(err, ErrNotFound) || err == nil && m.WorkflowID == "" {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !terminalState(j.Status.State) {
		return true, nil
	}
	st, err := loadWorkflowState(tx, m.WorkflowID)
	if err != nil {
		return false, err
	}
	g, err := loadWorkflowGraph(tx, m.WorkflowID)
	if err != nil {
		return false, err
	}
	if len(g.Nodes) != len(st.Nodes) || int(m.Node) >= len(g.Nodes) {
		return false, errors.New("workflow graph mismatch")
	}
	name := g.Nodes[m.Node].Name
	for i, n := range g.Nodes {
		if st.Nodes[i].Job == "" && st.Nodes[i].Disposition == WorkflowPending && slices.Contains(n.DependsOn, name) {
			return true, nil
		}
	}
	return false, nil
}

func (m *retentionMutation) workflowState(id string) (st workflowState, err error) {
	err = workflowDecode(m.coreGet(workflowsBucket, workflowStateKey(id)), &st)
	return
}

func (m *retentionMutation) putWorkflowState(id string, st workflowState) error {
	data, err := encodeWorkflowState(id, st)
	if err != nil {
		return err
	}
	m.corePut(workflowsBucket, workflowStateKey(id), data)
	return nil
}

// captureWorkflowChild copies the selected child's terminal state into its
// node before markPrune deletes the Job. The child row stays until the
// header's last positive acknowledgement, or the zero-operation path.
func (m *retentionMutation) captureWorkflowChild(j backend.Job, meta retainedJob) error {
	st, err := m.workflowState(meta.WorkflowID)
	if err != nil {
		return err
	}
	if int(meta.Node) >= len(st.Nodes) || st.Nodes[meta.Node].Job != j.ID || st.Nodes[meta.Node].Final != "" || m.coreGet(workflowsBucket, workflowChildKey(j.ID)) == nil {
		return errors.New("invalid workflow child binding")
	}
	if !terminalState(j.Status.State) {
		return errors.New("workflow child is not terminal")
	}
	st.Nodes[meta.Node].Final = j.Status.State
	return m.putWorkflowState(meta.WorkflowID, st)
}

// retireWorkflowChild runs inside removeHeader. It retires a child only when
// the prospective plan has no Job and the header completed without manual
// work; an output-only header of a live Job retires nothing.
func (m *retentionMutation) retireWorkflowChild(h cleanupHeader) error {
	key := workflowChildKey(h.JobID)
	raw := m.coreGet(workflowsBucket, key)
	if raw == nil || m.coreGet(jobsBucket, []byte(h.JobID)) != nil {
		return nil
	}
	if h.Remaining != 0 || h.NeedsManual {
		return errors.New("workflow child cleanup is incomplete")
	}
	var c workflowChild
	if err := workflowDecode(raw, &c); err != nil {
		return err
	}
	st, err := m.workflowState(c.Workflow)
	if err != nil {
		return err
	}
	if int(c.Node) >= len(st.Nodes) || st.Nodes[c.Node].Job != h.JobID || !terminalState(st.Nodes[c.Node].Final) || st.Nodes[c.Node].Retired || st.Owner != h.Principal {
		return errors.New("invalid workflow child retirement")
	}
	st.Nodes[c.Node].Retired = true
	if err := m.putWorkflowState(c.Workflow, st); err != nil {
		return err
	}
	return m.coreDelete(workflowsBucket, key)
}

// workflowCaps is the checked-cap predicate applied unconditionally to every
// workflow transaction: total retained usage, not a workflow subtotal, may
// not grow past an existing policy cap, even while retention is disabled.
func workflowCaps(tx *bolt.Tx, before retentionUsage) error {
	st, err := loadRetention(tx)
	if err != nil {
		return err
	}
	after, err := usage(tx)
	if err != nil {
		return err
	}
	for q := JobsQuota; q <= PrincipalScopesQuota; q++ {
		if err := capacityError(st.Policy, q, before[q], after[q]); err != nil {
			return err
		}
	}
	return nil
}
