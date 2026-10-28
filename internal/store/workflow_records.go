package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/workflow"
	bolt "go.etcd.io/bbolt"
)

// The workflows bucket, installed empty by the schema8 migration, holds flat
// typed rows. Ordinals are fixed eight-byte big-endian values.
//
//	g/<id>                   canonical immutable graph
//	s/<id>                   bounded mutable workflow state
//	i/<ordinal>              id; the bucket sequence is the global high-water
//	h/<principal>            dense per-principal high-water, kept after retirement
//	p/<principal>/<ordinal>  id
//	c/<job>                  reciprocal child binding
//	advance                  fair candidate cursor
var workflowsBucket = []byte("workflows")
var workflowAdvanceKey = []byte("advance")

// A workflow is charged at acceptance for its whole future state: an envelope
// plus, per node, a state cell and its child row. Actual rows consume that
// reservation; they are never charged a second time.
const (
	workflowGraphLimit    = 256 << 10
	workflowStateEnvelope = 4096
	workflowNodeBytes     = 896
	workflowChildBytes    = 128
	workflowRequestLimit  = 2048
)

type workflowState struct {
	Owner     string          `json:"owner"`
	Domain    RequestDomain   `json:"domain"`
	RequestID string          `json:"request_id"`
	Graph     string          `json:"graph"`
	Authority auth.Delegation `json:"authority"`
	Created   time.Time       `json:"created"`
	Ordinal   uint64          `json:"ordinal"`
	Scope     uint64          `json:"scope"`
	Revision  uint64          `json:"revision"` // advanced only by cancel, skip and input publication
	Canceled  bool            `json:"canceled,omitempty"`
	Nodes     []workflowNode  `json:"nodes"`
}

// workflowNode holds only logical facts. A bound node's execution state is
// its child Job's; Final copies that terminal state when the Job is deleted,
// and Retired records the child's last positive cleanup acknowledgement.
type workflowNode struct {
	Disposition WorkflowDisposition `json:"disposition"`
	Job         string              `json:"job,omitempty"`
	Task        string              `json:"task,omitempty"`
	Producer    string              `json:"producer,omitempty"`
	Attempt     string              `json:"attempt,omitempty"`
	Input       *ArtifactRef        `json:"input,omitempty"`
	Revision    uint64              `json:"revision,omitempty"`
	Final       backend.State       `json:"final,omitempty"`
	Retired     bool                `json:"retired,omitempty"`
}

type workflowChild struct {
	Workflow string `json:"workflow"`
	Node     uint8  `json:"node"`
}

func workflowGraphKey(id string) []byte  { return []byte("g/" + id) }
func workflowStateKey(id string) []byte  { return []byte("s/" + id) }
func workflowChildKey(job string) []byte { return []byte("c/" + job) }
func workflowIndexKey(n uint64) []byte   { return append([]byte("i/"), sequenceKey(n)...) }
func workflowHighKey(principal string) []byte {
	return []byte("h/" + principal)
}
func workflowScopePrefix(principal string) []byte { return []byte("p/" + principal + "/") }
func workflowScopeKey(principal string, n uint64) []byte {
	return append(workflowScopePrefix(principal), sequenceKey(n)...)
}

func workflowStateCharge(nodes int) uint64 {
	return workflowStateEnvelope + uint64(nodes)*(workflowNodeBytes+workflowChildBytes)
}

func workflowDecode(raw []byte, out any) error {
	if raw == nil {
		return ErrNotFound
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return errors.New("invalid workflow record")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("invalid workflow record")
	}
	return nil
}

func encodeWorkflowState(id string, st workflowState) ([]byte, error) {
	if len(st.Nodes) == 0 || len(st.Nodes) > 64 {
		return nil, errors.New("invalid workflow node count")
	}
	data, err := json.Marshal(st)
	if err != nil {
		return nil, err
	}
	if uint64(len(workflowStateKey(id))+len(data)) > workflowStateEnvelope+uint64(len(st.Nodes))*workflowNodeBytes {
		return nil, errors.New("workflow state exceeds limit")
	}
	return data, nil
}

func encodeWorkflowChild(job string, c workflowChild) ([]byte, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	if len(workflowChildKey(job))+len(data) > workflowChildBytes {
		return nil, errors.New("workflow child binding exceeds limit")
	}
	return data, nil
}

func workflowRow(tx *bolt.Tx, key []byte) []byte {
	if b := tx.Bucket(workflowsBucket); b != nil {
		return b.Get(key)
	}
	return nil
}

func loadWorkflowState(tx *bolt.Tx, id string) (st workflowState, err error) {
	err = workflowDecode(workflowRow(tx, workflowStateKey(id)), &st)
	return
}

// loadWorkflowGraph decodes the stored graph of workflow id. The stored bytes
// must be exactly canonical, as validateWorkflow requires: json.Unmarshal
// drops members that Graph lacks, such as a node task's workers, and only the
// comparison refuses a graph that holds one.
func loadWorkflowGraph(tx *bolt.Tx, id string) (g workflow.Graph, err error) {
	raw := workflowRow(tx, workflowGraphKey(id))
	if raw == nil {
		return g, ErrNotFound
	}
	if err = json.Unmarshal(raw, &g); err != nil {
		return
	}
	canonical, err := workflow.Canonical(g)
	if err != nil || !bytes.Equal(canonical, raw) {
		return workflow.Graph{}, errors.New("workflow graph mismatch")
	}
	return g, nil
}

func putWorkflowState(tx *bolt.Tx, id string, st workflowState) error {
	data, err := encodeWorkflowState(id, st)
	if err != nil {
		return err
	}
	return tx.Bucket(workflowsBucket).Put(workflowStateKey(id), data)
}

// workflowNames maps node names to declared ordinals.
func workflowNames(g workflow.Graph) map[string]int {
	names := make(map[string]int, len(g.Nodes))
	for i, n := range g.Nodes {
		names[n.Name] = i
	}
	return names
}

// workflowNeedsArtifacts reports whether any node can create an artifact
// obligation: an input edge, a snapshot field or a remote data mount.
func workflowNeedsArtifacts(g workflow.Graph) bool {
	for _, n := range g.Nodes {
		if n.Input != nil || hasWorkspaceFields(n.Task) || len(n.Task.DataMounts) != 0 {
			return true
		}
	}
	return false
}

// validWorkflowRequest checks the typed workflow alternative of the request
// union. It has no job, task digest or raw legacy binding.
func validWorkflowRequest(r requestRecord) bool {
	if r.JobID != "" || r.TaskDigest != ([32]byte{}) || r.LegacyRequestID != nil || r.LegacyBytes != 0 || !artifactHex(r.WorkflowID, 32) || !artifactHex(r.GraphDigest, 64) || !validRetainedRequestID(r.RequestID) || !validPrincipal([]byte(r.Principal)) {
		return false
	}
	if r.Domain.Epoch == 0 {
		return r.Domain.Lineage == "" || validAttemptID(r.Domain.Lineage)
	}
	return validAttemptID(r.Domain.Lineage)
}

func validWorkflowAuthority(d auth.Delegation, owner string) bool {
	return d.Principal == owner && artifactHex(d.TokenID, 32) && d.Digest != ([32]byte{})
}

// workflowUsage rederives the workflow bucket's charges: graph bytes count
// once toward TaskBytes, every other row toward MetadataBytes, and child rows
// are covered by their workflow's state reservation.
func workflowUsage(tx *bolt.Tx) (task, metadata uint64, err error) {
	b := tx.Bucket(workflowsBucket)
	if b == nil {
		return 0, 0, nil
	}
	err = b.ForEach(func(k, v []byte) error {
		if v == nil {
			return errors.New("unexpected workflow nested bucket")
		}
		var e error
		switch {
		case bytes.HasPrefix(k, []byte("g/")):
			if task, e = addChecked(task, uint64(len(v))); e != nil {
				return e
			}
			metadata, e = addChecked(metadata, uint64(len(k)))
		case bytes.HasPrefix(k, []byte("s/")):
			var st workflowState
			if e = workflowDecode(v, &st); e != nil {
				return e
			}
			metadata, e = addChecked(metadata, workflowStateCharge(len(st.Nodes)))
		case bytes.HasPrefix(k, []byte("c/")):
		default:
			metadata, e = addChecked(metadata, uint64(len(k)+len(v)))
		}
		return e
	})
	return
}

// validateWorkflows checks every workflow row against its graph, request,
// indexes, child jobs and input edges. Schema8 requires the bucket; an
// earlier schema must not contain it.
func validateWorkflows(tx *bolt.Tx, art *artifactState) error {
	b := tx.Bucket(workflowsBucket)
	if schemaNumber(tx) < 8 {
		if b != nil {
			return errors.New("workflow metadata requires schema 8")
		}
		return nil
	}
	if b == nil {
		return errors.New("missing workflow metadata")
	}
	states := map[string]workflowState{}
	children := map[string]workflowChild{}
	index := map[uint64]string{}
	scopes := map[string]map[uint64]string{}
	highs := map[string]uint64{}
	graphs := map[string]bool{}
	if err := b.ForEach(func(k, v []byte) error {
		if v == nil {
			return errors.New("unexpected workflow nested bucket")
		}
		key := string(k)
		switch {
		case bytes.Equal(k, workflowAdvanceKey):
			if len(v) != 8 || binary.BigEndian.Uint64(v) > b.Sequence() {
				return errors.New("invalid workflow cursor")
			}
		case bytes.HasPrefix(k, []byte("g/")):
			if !validAttemptID(key[2:]) {
				return errors.New("invalid workflow graph key")
			}
			graphs[key[2:]] = true
		case bytes.HasPrefix(k, []byte("s/")):
			var st workflowState
			if !validAttemptID(key[2:]) || workflowDecode(v, &st) != nil {
				return errors.New("invalid workflow state")
			}
			if data, err := encodeWorkflowState(key[2:], st); err != nil || !bytes.Equal(data, v) {
				return errors.New("invalid workflow state encoding")
			}
			states[key[2:]] = st
		case bytes.HasPrefix(k, []byte("c/")):
			var c workflowChild
			if !validAttemptID(key[2:]) || workflowDecode(v, &c) != nil {
				return errors.New("invalid workflow child binding")
			}
			if data, err := encodeWorkflowChild(key[2:], c); err != nil || !bytes.Equal(data, v) {
				return errors.New("invalid workflow child encoding")
			}
			children[key[2:]] = c
		case bytes.HasPrefix(k, []byte("i/")):
			if len(k) != 10 || !validAttemptID(string(v)) {
				return errors.New("invalid workflow index")
			}
			n := binary.BigEndian.Uint64(k[2:])
			if n == 0 || n > b.Sequence() {
				return errors.New("invalid workflow index ordinal")
			}
			index[n] = string(v)
		case bytes.HasPrefix(k, []byte("h/")):
			if !validPrincipal(k[2:]) || len(v) != 8 || binary.BigEndian.Uint64(v) == 0 {
				return errors.New("invalid workflow scope high-water")
			}
			highs[key[2:]] = binary.BigEndian.Uint64(v)
		case bytes.HasPrefix(k, []byte("p/")):
			if len(k) < 11 || k[len(k)-9] != '/' || !validPrincipal(k[2:len(k)-9]) || !validAttemptID(string(v)) {
				return errors.New("invalid workflow scope index")
			}
			p := key[2 : len(k)-9]
			if scopes[p] == nil {
				scopes[p] = map[uint64]string{}
			}
			scopes[p][binary.BigEndian.Uint64(k[len(k)-8:])] = string(v)
		default:
			return errors.New("unknown workflow row")
		}
		return nil
	}); err != nil {
		return err
	}
	if len(graphs) != len(states) {
		return errors.New("workflow graph without state")
	}
	edges := 0
	bound := map[string]bool{}
	for id, st := range states {
		if err := validateWorkflow(tx, art, id, st, children, &edges); err != nil {
			return err
		}
		if index[st.Ordinal] != id || scopes[st.Owner][st.Scope] != id || st.Scope == 0 || st.Scope > highs[st.Owner] {
			return errors.New("workflow index mismatch")
		}
		for _, n := range st.Nodes {
			if n.Job != "" && !n.Retired {
				bound[n.Job] = true
			}
		}
	}
	for job := range children {
		if !bound[job] {
			return errors.New("orphan workflow child binding")
		}
	}
	if len(index) != len(states) {
		return errors.New("orphan workflow index")
	}
	n := 0
	for _, m := range scopes {
		n += len(m)
	}
	if n != len(states) {
		return errors.New("orphan workflow scope index")
	}
	for p := range scopes {
		if highs[p] == 0 {
			return errors.New("workflow scope lacks high-water")
		}
	}
	if art != nil && edges != len(art.WorkflowInputs) {
		return errors.New("orphan workflow input")
	}
	return nil
}

func validateWorkflow(tx *bolt.Tx, art *artifactState, id string, st workflowState, children map[string]workflowChild, edges *int) error {
	raw := workflowRow(tx, workflowGraphKey(id))
	var g workflow.Graph
	if raw == nil || len(raw) > workflowGraphLimit || json.Unmarshal(raw, &g) != nil {
		return errors.New("invalid workflow graph")
	}
	canonical, err := workflow.Canonical(g)
	digest := sha256.Sum256(raw)
	if err != nil || !bytes.Equal(canonical, raw) || st.Graph != hex.EncodeToString(digest[:]) || len(g.Nodes) != len(st.Nodes) {
		return errors.New("workflow graph mismatch")
	}
	// The exact canonical comparison also refuses every node member that
	// Graph lacks, such as a task's workers, whatever the node's disposition:
	// a pending node has no job for the stored job fence to refuse.
	if !validPrincipal([]byte(st.Owner)) || !validWorkflowAuthority(st.Authority, st.Owner) || st.Created.IsZero() || st.Revision == 0 || st.Ordinal == 0 {
		return errors.New("invalid workflow identity")
	}
	if art == nil && workflowNeedsArtifacts(g) {
		return errors.New("workflow requires artifact metadata")
	}
	var r requestRecord
	if err := loadJSON(retentionTable(tx, retainedRequestsBucket), requestKey(st.Owner, st.Domain, st.RequestID), &r); err != nil {
		return errors.New("missing workflow request")
	}
	if r.WorkflowID != id || r.Expired || r.GraphDigest != st.Graph || r.RequestID != st.RequestID {
		return errors.New("workflow request mismatch")
	}
	names := workflowNames(g)
	for i, n := range st.Nodes {
		in := g.Nodes[i].Input
		if n.Disposition < WorkflowPending || n.Disposition > WorkflowCanceled {
			return errors.New("invalid workflow disposition")
		}
		if n.Job == "" {
			if n.Task != "" || n.Producer != "" || n.Attempt != "" || n.Input != nil || n.Revision != 0 || n.Final != "" || n.Retired || st.Canceled && n.Disposition == WorkflowPending {
				return errors.New("invalid unbound workflow node")
			}
			if in != nil && n.Disposition == WorkflowPending {
				if art == nil {
					return errors.New("workflow input requires artifact metadata")
				}
				e, ok := art.WorkflowInputs[workflowInputKey(id, i)]
				if !ok || e.Owner != st.Owner || e.Workflow != id || int(e.Producer) != names[in.Node] || int(e.Consumer) != i {
					return errors.New("missing workflow input")
				}
				if e.Ref != nil && st.Nodes[e.Producer].Job != e.JobID {
					return errors.New("workflow input producer mismatch")
				}
				*edges++
			}
			continue
		}
		if n.Disposition != WorkflowPending || !validAttemptID(n.Job) || !artifactHex(n.Task, 64) || n.Revision == 0 || n.Revision > st.Revision {
			return errors.New("invalid bound workflow node")
		}
		if (in == nil) != (n.Input == nil) || in != nil && (n.Producer != st.Nodes[names[in.Node]].Job || !validAttemptID(n.Attempt) || !artifactRefValid(*n.Input)) || in == nil && (n.Producer != "" || n.Attempt != "") {
			return errors.New("invalid workflow input binding")
		}
		c, linked := children[n.Job]
		job, err := get(tx, n.Job)
		live := err == nil
		if err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
		switch {
		case n.Retired:
			if linked || live || !terminalState(n.Final) {
				return errors.New("invalid retired workflow child")
			}
		case live:
			if !linked || c.Workflow != id || int(c.Node) != i || n.Final != "" {
				return errors.New("invalid workflow child relation")
			}
			if d, err := taskDigest(job.Task); err != nil || hex.EncodeToString(d[:]) != n.Task {
				return errors.New("workflow child task mismatch")
			}
		default:
			// After Mark, the matching cleanup header carries the child until
			// its last positive acknowledgement retires the relation.
			var h cleanupHeader
			if !linked || c.Workflow != id || int(c.Node) != i || !terminalState(n.Final) || loadJSON(retentionTable(tx, cleanupHeadersBucket), []byte(n.Job), &h) != nil || h.JobID != n.Job || h.Principal != st.Owner {
				return errors.New("workflow child lost its job without retirement")
			}
		}
	}
	return nil
}

// validateWorkflowChild checks a child job's workflow-owned metadata. It has
// no public request identity; its node and reciprocal row bind it instead.
func validateWorkflowChild(tx *bolt.Tx, j backend.Job, m retainedJob) error {
	if m.Legacy || m.Domain != (RequestDomain{}) || m.RequestID != "" || m.LegacyRequestID != nil || m.LegacyRequestRetired || !validBinding(j.TargetBinding) {
		return errors.New("invalid workflow child identity")
	}
	var c workflowChild
	if err := workflowDecode(workflowRow(tx, workflowChildKey(j.ID)), &c); err != nil || c.Workflow != m.WorkflowID || c.Node != m.Node {
		return errors.New("invalid workflow child binding")
	}
	st, err := loadWorkflowState(tx, m.WorkflowID)
	if err != nil || int(m.Node) >= len(st.Nodes) || st.Nodes[m.Node].Job != j.ID || st.Owner != m.Principal {
		return errors.New("workflow child node mismatch")
	}
	return nil
}

func terminalState(s backend.State) bool {
	return s == backend.Succeeded || s == backend.Failed || s == backend.Canceled
}
