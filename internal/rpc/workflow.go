package rpc

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"unicode/utf8"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
	"github.com/tmc/skygo/internal/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The WorkflowService methods check fresh authority before parsing, then call
// the producing Control, which owns replay, admission and cursor scope.
// Access audit reuses the fixed submit, get, list and cancel categories with
// an empty resource: an audit resource names only an authorized job, never a
// workflow.

// SubmitWorkflow accepts a static workflow. Control replays an identical
// request before capturing new authority; the RPC never falls back to job
// submission.
func (h *handler) SubmitWorkflow(ctx context.Context, r *connect.Request[v1.SubmitWorkflowRequest]) (*connect.Response[v1.SubmitWorkflowResponse], error) {
	actor, err := h.engine.Check(requestActor(ctx), auth.Mutate)
	if err != nil {
		return nil, rpcError(err)
	}
	if r == nil || r.Msg == nil {
		return nil, rpcError(control.ErrInvalid)
	}
	g, err := workflowGraph(r.Msg.Graph)
	if err != nil {
		return nil, rpcError(err)
	}
	var domain *store.RequestDomain
	if r.Msg.Domain != nil {
		d, err := requestDomain(r.Msg.Domain)
		if err != nil {
			return nil, rpcError(fmt.Errorf("%w: malformed request domain", control.ErrInvalid))
		}
		domain = &d
	}
	w, err := h.control.SubmitWorkflow(actor, domain, r.Msg.RequestId, g)
	h.record(ctx, "submit", "", err)
	if err != nil {
		return nil, rpcError(err)
	}
	out, err := wireWorkflow(w)
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&v1.SubmitWorkflowResponse{Workflow: out}), nil
}

// GetWorkflow returns one visible workflow. Foreign and missing workflows are
// both NotFound.
func (h *handler) GetWorkflow(ctx context.Context, r *connect.Request[v1.GetWorkflowRequest]) (*connect.Response[v1.GetWorkflowResponse], error) {
	actor, err := h.engine.Check(requestActor(ctx), auth.Read)
	if err != nil {
		return nil, rpcError(err)
	}
	w, err := h.control.GetWorkflow(actor, r.Msg.Id)
	h.record(ctx, "get", "", err)
	if err != nil {
		return nil, rpcError(err)
	}
	out, err := wireWorkflow(w)
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&v1.GetWorkflowResponse{Workflow: out}), nil
}

// ListWorkflows returns summaries only; Get returns a graph and node status.
func (h *handler) ListWorkflows(ctx context.Context, r *connect.Request[v1.ListWorkflowsRequest]) (*connect.Response[v1.ListWorkflowsResponse], error) {
	actor, err := h.engine.Check(requestActor(ctx), auth.Read)
	if err != nil {
		return nil, rpcError(err)
	}
	ws, next, err := h.control.ListWorkflows(actor, int(r.Msg.PageSize), r.Msg.PageToken)
	h.record(ctx, "list", "", err)
	if err != nil {
		return nil, rpcError(err)
	}
	out := &v1.ListWorkflowsResponse{NextPageToken: next}
	for _, w := range ws {
		summary, err := wireWorkflowSummary(w)
		if err != nil {
			return nil, rpcError(err)
		}
		out.Workflows = append(out.Workflows, summary)
	}
	return connect.NewResponse(out), nil
}

// CancelWorkflow records cancellation intent and returns without waiting for
// bound children to stop.
func (h *handler) CancelWorkflow(ctx context.Context, r *connect.Request[v1.CancelWorkflowRequest]) (*connect.Response[v1.CancelWorkflowResponse], error) {
	actor, err := h.engine.Check(requestActor(ctx), auth.Mutate)
	if err != nil {
		return nil, rpcError(err)
	}
	w, err := h.control.CancelWorkflow(actor, r.Msg.Id)
	h.record(ctx, "cancel", "", err)
	if err != nil {
		return nil, rpcError(err)
	}
	out, err := wireWorkflow(w)
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&v1.CancelWorkflowResponse{Workflow: out}), nil
}

// workflowGraph converts every node without dropping fields. Graph shape and
// Task rules are Control's; this rejects only what cannot be converted.
func workflowGraph(g *v1.WorkflowGraph) (workflow.Graph, error) {
	if g == nil || len(g.Nodes) == 0 {
		return workflow.Graph{}, fmt.Errorf("%w: workflow graph required", control.ErrInvalid)
	}
	out := workflow.Graph{Nodes: make([]workflow.Node, len(g.Nodes))}
	for i, n := range g.Nodes {
		if n == nil || !utf8.ValidString(n.Name) || !validDataTaskUTF8(n.Task) {
			return workflow.Graph{}, fmt.Errorf("%w: invalid workflow node %d", control.ErrInvalid, i)
		}
		out.Nodes[i] = workflow.Node{Name: n.Name, Task: task(n.Task), DependsOn: slices.Clone(n.DependsOn)}
		if n.Input != nil {
			out.Nodes[i].Input = &workflow.Input{Node: n.Input.Node, Slot: n.Input.Slot}
		}
	}
	return out, nil
}

func wireGraph(g workflow.Graph) *v1.WorkflowGraph {
	out := &v1.WorkflowGraph{Nodes: make([]*v1.WorkflowNode, len(g.Nodes))}
	for i, n := range g.Nodes {
		out.Nodes[i] = &v1.WorkflowNode{Name: n.Name, Task: wireTask(n.Task), DependsOn: slices.Clone(n.DependsOn)}
		if n.Input != nil {
			out.Nodes[i].Input = &v1.WorkflowInput{Node: n.Input.Node, Slot: n.Input.Slot}
		}
	}
	return out
}

// wireWorkflow projects a stored workflow. A record outside the Store contract
// is an internal error, never a guessed state.
func wireWorkflow(w store.Workflow) (*v1.Workflow, error) {
	created, err := workflowCreated(w)
	if err != nil {
		return nil, err
	}
	if len(w.Nodes) != len(w.Graph.Nodes) {
		return nil, errors.New("workflow node count differs from graph")
	}
	out := &v1.Workflow{Id: w.ID, Graph: wireGraph(w.Graph), GraphSha256: hex.EncodeToString(w.GraphDigest[:]), CreatedAt: created, CancelRequested: w.Canceled, Nodes: make([]*v1.WorkflowNodeStatus, len(w.Nodes))}
	for i, n := range w.Nodes {
		if n.Name != w.Graph.Nodes[i].Name {
			return nil, errors.New("workflow node order differs from graph")
		}
		if out.Nodes[i], err = wireNodeStatus(n); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func wireWorkflowSummary(w store.Workflow) (*v1.WorkflowSummary, error) {
	created, err := workflowCreated(w)
	if err != nil {
		return nil, err
	}
	return &v1.WorkflowSummary{Id: w.ID, GraphSha256: hex.EncodeToString(w.GraphDigest[:]), CreatedAt: created, CancelRequested: w.Canceled}, nil
}

func workflowCreated(w store.Workflow) (*timestamppb.Timestamp, error) {
	created := timestamppb.New(w.CreatedAt)
	if !hexID(w.ID) || w.CreatedAt.IsZero() || created.CheckValid() != nil {
		return nil, errors.New("invalid workflow record")
	}
	return created, nil
}

// wireNodeStatus reports a child state only for a bound node; an unbound
// node's state is unspecified, not queued.
func wireNodeStatus(n store.WorkflowNode) (*v1.WorkflowNodeStatus, error) {
	out := &v1.WorkflowNodeStatus{Name: n.Name}
	switch n.Disposition {
	case store.WorkflowPending:
		out.Disposition = v1.WorkflowDisposition_WORKFLOW_DISPOSITION_PENDING
	case store.WorkflowSkipped:
		out.Disposition = v1.WorkflowDisposition_WORKFLOW_DISPOSITION_SKIPPED
	case store.WorkflowCanceled:
		out.Disposition = v1.WorkflowDisposition_WORKFLOW_DISPOSITION_CANCELED
	default:
		return nil, errors.New("invalid workflow disposition")
	}
	if n.JobID != "" {
		state, ok := states[n.State]
		if !ok || !hexID(n.JobID) || n.Disposition != store.WorkflowPending {
			return nil, errors.New("invalid workflow child")
		}
		out.JobId, out.ChildState = n.JobID, state
	}
	if n.Input != nil {
		ref := artifact.Ref{ID: n.Input.ID, SHA256: n.Input.SHA256, Size: int64(n.Input.Size)}
		if n.Input.Size > 32<<20 || ref.Validate() != nil {
			return nil, errors.New("invalid workflow input")
		}
		out.Input = wireTaskWorkdir(&ref)
	}
	return out, nil
}
