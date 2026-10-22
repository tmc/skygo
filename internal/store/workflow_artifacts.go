package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// workflowInput is one input edge in the artifact table workflow_inputs.
// Graph acceptance reserves it Pending (no Ref). The producer's publication
// transaction fills it Ready with the exact result. Child admission replaces
// it with the child's ordinary input pin; skip and cancel delete it.
type workflowInput struct {
	Owner     string       `json:"owner"`
	Workflow  string       `json:"workflow"`
	Producer  uint8        `json:"producer"`
	Consumer  uint8        `json:"consumer"`
	JobID     string       `json:"job_id,omitempty"`
	AttemptID string       `json:"attempt_id,omitempty"`
	Ref       *ArtifactRef `json:"ref,omitempty"`
}

// workflowInputKey derives an edge's artifact key from its workflow and
// consumer ordinal. The hash is domain-separated from every other key.
func workflowInputKey(id string, consumer int) string {
	sum := sha256.Sum256(append([]byte("skygo-workflow-input\x00"+id), byte(consumer)))
	return hex.EncodeToString(sum[:16])
}

// workflowInputValid checks one edge row without core state.
func workflowInputValid(s *artifactState, id string, in workflowInput) error {
	if !validAttemptID(in.Workflow) || in.Producer >= 64 || in.Consumer >= 64 || in.Producer == in.Consumer || id != workflowInputKey(in.Workflow, int(in.Consumer)) {
		return artifactCorrupt()
	}
	if (in.Ref == nil) != (in.JobID == "") || (in.Ref == nil) != (in.AttemptID == "") {
		return artifactCorrupt()
	}
	if in.Ref == nil {
		return nil
	}
	if !validAttemptID(in.JobID) || !validAttemptID(in.AttemptID) || !artifactRefValid(*in.Ref) {
		return artifactCorrupt()
	}
	if o, ok := s.Objects[in.Ref.ID]; !ok || o.State != "complete" || o.Ref != *in.Ref || o.Owner != in.Owner {
		return artifactCorrupt()
	}
	return nil
}

// workflowValidateInputs joins every edge to its workflow state and, when
// Ready, to the producer's result. A nonnil plan supplies prospective rows.
func workflowValidateInputs(tx *bolt.Tx, st *artifactState, plan *retentionMutation) error {
	row := func(key []byte) []byte {
		if plan != nil {
			return plan.coreGet(workflowsBucket, key)
		}
		return workflowRow(tx, key)
	}
	for _, in := range st.WorkflowInputs {
		var ws workflowState
		if err := workflowDecode(row(workflowStateKey(in.Workflow)), &ws); err != nil {
			return artifactCorrupt()
		}
		if ws.Owner != in.Owner || ws.Canceled || int(in.Consumer) >= len(ws.Nodes) || int(in.Producer) >= len(ws.Nodes) {
			return artifactCorrupt()
		}
		if n := ws.Nodes[in.Consumer]; n.Job != "" || n.Disposition != WorkflowPending {
			return artifactCorrupt()
		}
		if in.Ref == nil {
			continue
		}
		r, ok := st.Results[in.AttemptID]
		if ws.Nodes[in.Producer].Job != in.JobID || !ok || r.Ref != *in.Ref || r.JobID != in.JobID || r.Owner != in.Owner {
			return artifactCorrupt()
		}
	}
	return nil
}

// workflowPublish runs in the producer's publication transaction, after
// observeAttemptCore. It fills every pending edge that consumes this
// attempt's result and advances the workflow's revision; the attempt
// ledger already observes that state row. A job that is not a workflow
// child publishes nothing.
func workflowPublish(tx *bolt.Tx, st *artifactState, j backend.Job, attempt string) error {
	if j.Status.State != backend.Succeeded || j.CurrentAttemptID() != attempt {
		return nil
	}
	r, ok := st.Results[attempt]
	if !ok {
		return nil
	}
	var c workflowChild
	if err := workflowDecode(workflowRow(tx, workflowChildKey(j.ID)), &c); errors.Is(err, ErrNotFound) {
		return nil
	} else if err != nil {
		return err
	}
	ws, err := loadWorkflowState(tx, c.Workflow)
	if err != nil {
		return err
	}
	g, err := loadWorkflowGraph(tx, c.Workflow)
	if err != nil {
		return err
	}
	if len(g.Nodes) != len(ws.Nodes) || int(c.Node) >= len(ws.Nodes) || ws.Nodes[c.Node].Job != j.ID {
		return artifactCorrupt()
	}
	name := g.Nodes[c.Node].Name
	filled := false
	for i, n := range g.Nodes {
		if n.Input == nil || n.Input.Node != name || ws.Nodes[i].Job != "" || ws.Nodes[i].Disposition != WorkflowPending {
			continue
		}
		k := workflowInputKey(c.Workflow, i)
		in, ok := st.WorkflowInputs[k]
		if !ok || in.Ref != nil || in.Producer != c.Node || in.Owner != r.Owner || r.JobID != j.ID {
			return artifactCorrupt()
		}
		ref := r.Ref
		in.JobID, in.AttemptID, in.Ref = j.ID, attempt, &ref
		st.WorkflowInputs[k] = in
		filled = true
	}
	if !filled {
		return nil
	}
	ws.Revision++
	return putWorkflowState(tx, c.Workflow, ws)
}

// workflowCoreChange runs one workflow transaction's change under its core
// ledger. With the artifact bucket it is artifactCoreChange. Without it, the
// change must not touch artifact state, and only the core ledger applies.
// Either way the checked caps apply to the whole retained usage.
func workflowCoreChange(tx *bolt.Tx, l *artifactCoreLedger, artifacts bool, change func(*artifactState) error) error {
	before, err := usage(tx)
	if err != nil {
		return err
	}
	if tx.Bucket(artifactsBucket) != nil {
		if _, err := artifactCoreChange(tx, l, maxChangeBytes, 100, change); err != nil {
			return err
		}
	} else {
		if artifacts {
			return ErrArtifactsDisabled
		}
		if err := change(nil); err != nil {
			return err
		}
		core, err := l.measure(tx)
		if err != nil {
			return err
		}
		if core.bytes > maxChangeBytes || core.created > 100 {
			return errArtifactMutationBudget
		}
	}
	return workflowCaps(tx, before)
}
