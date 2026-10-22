package control

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
	"github.com/tmc/skygo/internal/workflow"
)

const (
	maxWorkflowTurn   = 4  // check dispatches, and child admissions, per turn
	maxWorkflowChecks = 32 // retained candidate continuations
	maxWorkflowHolds  = 64 // holds that pause intake
	workflowHoldTime  = 5 * time.Second
)

// SubmitWorkflow durably accepts a static workflow, or returns the workflow
// already bound to requestID with an identical graph before any transient
// refusal. Acceptance validates every node's Task, but plans no node: a node is
// placed only when its dependencies finish and its input is published, so
// acceptance does not promise that a node can run.
func (c *Control) SubmitWorkflow(actor auth.Actor, domain *store.RequestDomain, requestID string, g workflow.Graph) (store.Workflow, error) {
	if err := c.beginExternal(); err != nil {
		return store.Workflow{}, err
	}
	defer c.endExternal()
	actor, err := c.engine.Check(actor, auth.Mutate)
	if err != nil {
		return store.Workflow{}, err
	}
	if !requestName.MatchString(requestID) {
		return store.Workflow{}, fmt.Errorf("%w: request_id must contain 1..128 letters, digits, underscores or hyphens", ErrInvalid)
	}
	if err := validateGraph(g); err != nil {
		return store.Workflow{}, err
	}
	w, found, err := c.store.LookupWorkflow(actor, domain, requestID, g)
	if err != nil || found {
		return w, err
	}
	if domain == nil && c.retention.enabled {
		return store.Workflow{}, &store.ExpiryError{Reason: store.RequestDomainRequired}
	}
	artifacts := graphNeedsArtifacts(g)
	if c.failed.Load() || artifacts && c.artifactAdmissionStopped() {
		return store.Workflow{}, ErrUnavailable
	}
	if err := c.storageGuard(false, false); err != nil {
		return store.Workflow{}, err
	}
	authority, err := c.engine.Delegate(actor)
	if err != nil {
		return store.Workflow{}, err
	}
	if c.failed.Load() || artifacts && c.artifactAdmissionStopped() {
		return store.Workflow{}, ErrUnavailable
	}
	return c.store.SubmitWorkflow(actor, domain, requestID, g, authority)
}

// GetWorkflow returns one workflow visible to actor.
func (c *Control) GetWorkflow(actor auth.Actor, id string) (store.Workflow, error) {
	actor, err := c.engine.Check(actor, auth.Read)
	if err != nil {
		return store.Workflow{}, err
	}
	if !jobName.MatchString(id) {
		return store.Workflow{}, fmt.Errorf("%w: invalid workflow id", ErrInvalid)
	}
	return c.store.GetWorkflowFor(actor, id)
}

// ListWorkflows lists visible workflows in acceptance order. The page token is
// bound to this installation, retention lineage and caller scope; the listing
// is live, so a later page includes workflows accepted after the first.
func (c *Control) ListWorkflows(actor auth.Actor, size int, token string) ([]store.Workflow, string, error) {
	actor, err := c.engine.Check(actor, auth.Read)
	if err != nil {
		return nil, "", err
	}
	if size < 0 || size > 200 {
		return nil, "", fmt.Errorf("%w: page size outside 0..200", ErrInvalid)
	}
	if size == 0 {
		size = 50
	}
	info, err := c.store.RetentionInfo(actor)
	if err != nil {
		return nil, "", err
	}
	cur := c.scopeCursor(actor, "workflow-list", info.Domain.Lineage)
	if token != "" {
		parsed, err := decodeWorkflowCursor(token)
		if err != nil {
			return nil, "", err
		}
		expected := cur
		expected.After = parsed.After
		if parsed != expected {
			return nil, "", fmt.Errorf("%w: page token scope differs", ErrInvalid)
		}
		cur = parsed
	}
	page, err := c.store.ListWorkflowsFor(actor, cur.After, size)
	if err != nil {
		return nil, "", err
	}
	next := ""
	if page.More {
		cur.After = page.After
		if next, err = encodeRetentionCursor(cur); err != nil {
			return nil, "", err
		}
	}
	return page.Workflows, next, nil
}

// CancelWorkflow persists cancellation intent: no further node is admitted and
// bound children receive ordinary cancellation. It does not wait for them.
func (c *Control) CancelWorkflow(actor auth.Actor, id string) (store.Workflow, error) {
	actor, err := c.engine.Check(actor, auth.Mutate)
	if err != nil {
		return store.Workflow{}, err
	}
	if !jobName.MatchString(id) {
		return store.Workflow{}, fmt.Errorf("%w: invalid workflow id", ErrInvalid)
	}
	return c.store.CancelWorkflowFor(actor, id)
}

func decodeWorkflowCursor(token string) (retentionCursor, error) {
	var cur retentionCursor
	err := decodeCursor(token, &cur, 512)
	if err == nil && (cur.Version != 1 || cur.Kind != "workflow-list" || cur.Max != 0 || cur.Selector != "" || cur.After == 0) {
		err = fmt.Errorf("%w: invalid cursor version or kind", ErrInvalid)
	}
	return cur, err
}

// validateGraph checks g's shape and each node's Task on a normalized copy;
// g itself is never changed. A node with an input has no Workdir until its
// producer publishes, so it gets the template rules, including the portable
// data mount rules.
func validateGraph(g workflow.Graph) error {
	if err := workflow.Validate(g); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	for _, n := range g.Nodes {
		t, err := normalizeTask(cloneTask(n.Task))
		if err != nil {
			return fmt.Errorf("node %q: %w", n.Name, err)
		}
		if n.Input != nil {
			if err = validateTemplate(t); err == nil {
				err = validateTemplateShape(t)
			}
		} else if err = Validate(t); err == nil {
			err = validateChildShape(t)
		}
		if err != nil {
			return fmt.Errorf("node %q: %w", n.Name, err)
		}
	}
	return nil
}

// validateChildShape applies the submission shape rules of the ordinary,
// workspace or data submission that a resolved node corresponds to.
func validateChildShape(t backend.Task) error {
	if len(t.DataMounts) != 0 {
		if err := backend.ValidateDataTask(t, nil); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalid, err)
		}
		return nil
	}
	if t.Workdir == nil && (len(t.Setup) != 0 || len(t.Outputs) != 0) {
		return fmt.Errorf("%w: workspace input required", ErrInvalid)
	}
	return nil
}

// validateTemplateShape applies the portable data mount rules to a node whose
// input is not yet resolved. Placement and protected paths are checked once it
// is resolved.
func validateTemplateShape(t backend.Task) error {
	if len(t.DataMounts) == 0 {
		return nil
	}
	if err := backend.ValidateDataTemplate(t); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	return nil
}

func graphNeedsArtifacts(g workflow.Graph) bool {
	for _, n := range g.Nodes {
		t := n.Task
		if n.Input != nil || t.Workdir != nil || len(t.Setup) != 0 || len(t.Outputs) != 0 || len(t.DataMounts) != 0 {
			return true
		}
	}
	return false
}

type workflowKey struct {
	workflow string
	node     int
}

type workflowHold struct {
	workflowKey
	revision uint64
}

// workflowRoute is one configured placement. A nonnil policy is the data route.
type workflowRoute struct {
	target backend.Target
	policy *backend.DataPolicy
}

// workflowCheck is one candidate's private continuation. Its first route's
// actual Backend.Check runs as the original pending request of that target's
// worker; Backend.Check has no context, so a held call holds only that worker.
type workflowCheck struct {
	candidate store.WorkflowCandidate
	task      backend.Task // normalized copy of candidate.Task
	routes    []workflowRoute
	checking  bool
	checked   bool
}

func (p *workflowCheck) key() workflowKey {
	return workflowKey{p.candidate.Workflow, p.candidate.Node}
}

func (p *workflowCheck) hold() workflowHold {
	return workflowHold{p.key(), p.candidate.Revision}
}

// advanceWorkflows admits checked candidates, dispatches route checks and
// takes new candidates from Store's bounded selector. Run calls it only
// outside shutdown preparation. Restart reselects from Store.
func (c *Control) advanceWorkflows(ctx context.Context, s *loopState) error {
	now := time.Now()
	for h, until := range s.workflowHeld {
		if !now.Before(until) {
			delete(s.workflowHeld, h)
		}
	}
	admitted := 0
	kept := s.workflows[:0]
	for _, p := range s.workflows {
		if p.checked && admitted < maxWorkflowTurn && ctx.Err() == nil {
			admitted++
			if err := c.admitWorkflow(p); err != nil {
				s.holdWorkflow(p, now)
			}
			continue
		}
		kept = append(kept, p)
	}
	clear(s.workflows[len(kept):])
	s.workflows = kept
	// Only a continuation whose check is in flight or passed is retained.
	// One that cannot dispatch, because its route's worker is busy or held,
	// is dropped: Store reselects its node and a later poll plans it again.
	dispatched := 0
	kept = s.workflows[:0]
	for _, p := range s.workflows {
		if !p.checking && !p.checked {
			if dispatched == maxWorkflowTurn || !s.dispatchWorkflowCheck(p) {
				continue
			}
			dispatched++
		}
		kept = append(kept, p)
	}
	clear(s.workflows[len(kept):])
	s.workflows = kept
	if now.Before(s.workflowDue) {
		return nil
	}
	s.workflowDue = now.Add(100 * time.Millisecond)
	// The selector also delivers cancellation and retires finished
	// workflows, so it runs even when no new candidate can be retained.
	candidates, err := c.pollWorkflows(s)
	if err != nil {
		// A failed poll pauses workflow intake only; ordinary reconciliation
		// continues, and the poll is retried after the backoff.
		s.workflowDue = now.Add(workflowHoldTime)
		return nil
	}
	for _, candidate := range candidates {
		// A full hold table pauses intake until holds expire, so a refused
		// node never loses its backoff.
		if dispatched == maxWorkflowTurn || len(s.workflows) == maxWorkflowChecks || len(s.workflowHeld) >= maxWorkflowHolds || ctx.Err() != nil {
			break
		}
		p := &workflowCheck{candidate: candidate}
		if s.workflowRetained(p) {
			continue
		}
		if err := c.planWorkflow(p); err != nil {
			s.holdWorkflow(p, now)
			continue
		}
		if s.dispatchWorkflowCheck(p) {
			s.workflows = append(s.workflows, p)
			dispatched++
		}
	}
	return nil
}

func (c *Control) pollWorkflows(s *loopState) ([]store.WorkflowCandidate, error) {
	if s.workflowPoll != nil {
		return s.workflowPoll()
	}
	return c.store.WorkflowCandidates()
}

func (s *loopState) workflowRetained(p *workflowCheck) bool {
	if _, held := s.workflowHeld[p.hold()]; held {
		return true
	}
	for _, q := range s.workflows {
		if q.key() == p.key() {
			return true
		}
	}
	return false
}

// holdWorkflow always records the hold. Intake stops while the table has
// maxWorkflowHolds entries, so only retained continuations, at most
// maxWorkflowChecks, can add holds beyond that.
func (s *loopState) holdWorkflow(p *workflowCheck, now time.Time) {
	if s.workflowHeld == nil {
		s.workflowHeld = make(map[workflowHold]time.Time)
	}
	s.workflowHeld[p.hold()] = now.Add(workflowHoldTime)
}

func (s *loopState) dropWorkflow(p *workflowCheck) {
	if i := slices.Index(s.workflows, p); i >= 0 {
		s.workflows = slices.Delete(s.workflows, i, i+1)
	}
}

// dispatchWorkflowCheck sends p's first route check as the one original
// pending request of that route's idle worker.
func (s *loopState) dispatchWorkflowCheck(p *workflowCheck) bool {
	route := p.routes[0].target
	for i := range s.workers {
		w := &s.workers[i]
		if w.target.Name != route.Name || w.target.Binding != route.Binding {
			continue
		}
		if w.pending != nil || w.received != nil || w.dataCompletion != nil || w.completion != nil && !w.completion.acked {
			return false
		}
		op := operation{kind: workflowCheckOperation, workflow: p}
		w.pending = &op
		p.checking = true
		w.input <- op // One pending operation per worker; its one-slot queue is empty.
		return true
	}
	return false
}

// integrateWorkflowCheck settles the original check request of w. A passed
// check waits for a later turn's admission; a failed one moves to the next
// route. During shutdown preparation the result is dropped, never admitted.
func (c *Control) integrateWorkflowCheck(s *loopState, w *targetWorker, r operationResult) {
	p := w.pending.workflow
	w.pending = nil
	p.checking = false
	switch {
	case s.preparing:
		s.dropWorkflow(p)
	case r.err == nil:
		p.checked = true
	case len(p.routes) > 1:
		p.routes = p.routes[1:]
	default:
		s.dropWorkflow(p)
		s.holdWorkflow(p, time.Now())
	}
}

// planWorkflow selects p's routes, in configured order, from configuration
// facts alone. It applies the same Validate, shape, route and guard rules as
// the ordinary, workspace or data submission the resolved child corresponds
// to; the actual Backend.Check runs later on the route's worker.
func (c *Control) planWorkflow(p *workflowCheck) error {
	if err := c.engine.CheckDelegation(p.candidate.Authority); err != nil {
		return err
	}
	t, err := normalizeTask(cloneTask(p.candidate.Task))
	if err != nil {
		return err
	}
	if err := Validate(t); err != nil {
		return err
	}
	data := c.workflowData(t)
	if !data {
		if err := validateChildShape(t); err != nil {
			return err
		}
	} else if err := backend.ValidateDataTask(t, nil); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if err := c.workflowGuards(t, data); err != nil {
		return err
	}
	var routes []workflowRoute
	if data {
		for _, route := range c.dataTargets {
			if len(t.DataMounts) != 0 && !route.target.SupportsDataMounts || t.Retry != nil && !route.target.SupportsRemoteDataRetry {
				continue
			}
			if targetFacts(t, route.target, c.retention.enabled) == nil {
				policy := route.policy
				routes = append(routes, workflowRoute{cloneTarget(route.target), &policy})
			}
		}
	} else {
		for _, target := range c.targets {
			if workspaceTargetFacts(t, target, c.retention.enabled, c.workspace) == nil {
				routes = append(routes, workflowRoute{target: cloneTarget(target)})
			}
		}
	}
	if len(routes) == 0 {
		return ErrUnsupported
	}
	p.task, p.routes = t, routes
	return nil
}

// workflowData reports whether t takes the data route: data mounts always do,
// and an explicit remote data target disambiguates a Workdir, as in plan.
func (c *Control) workflowData(t backend.Task) bool {
	if len(t.DataMounts) != 0 {
		return true
	}
	if t.Target == "" || t.Workdir == nil {
		return false
	}
	target, ok := c.target(t.Target)
	return ok && target.SupportsRemoteData
}

// workflowGuards applies the acceptance guards of the corresponding
// submission. Planning grants neither artifact access nor capacity.
func (c *Control) workflowGuards(t backend.Task, data bool) error {
	if c.failed.Load() {
		return ErrUnavailable
	}
	switch {
	case data:
		if err := c.checkDataArtifacts(); err != nil {
			return err
		}
		if c.artifactAdmissionStopped() {
			return ErrUnavailable
		}
	case t.Workdir != nil:
		if !c.ArtifactsEnabled() {
			return ErrUnsupported
		}
		if c.artifactAdmissionStopped() {
			return ErrUnavailable
		}
	}
	return c.storageGuard(false, false)
}

// admitWorkflow accepts p's child on its checked route. It holds an admitted
// external call, so a shutdown that closed admission first admits nothing and
// one that closes it later waits for this Store transaction. Delegation is
// checked after the possibly blocking guards, immediately before Store.
func (c *Control) admitWorkflow(p *workflowCheck) error {
	if err := c.beginExternal(); err != nil {
		return err
	}
	defer c.endExternal()
	route := p.routes[0]
	if err := c.engine.CheckDelegation(p.candidate.Authority); err != nil {
		return err
	}
	if err := c.workflowGuards(p.task, route.policy != nil); err != nil {
		return err
	}
	if err := c.engine.CheckDelegation(p.candidate.Authority); err != nil {
		return err
	}
	if c.failed.Load() || (route.policy != nil || p.task.Workdir != nil) && c.artifactAdmissionStopped() {
		return ErrUnavailable
	}
	_, err := c.store.AdmitWorkflowChild(store.WorkflowAdmission{
		Candidate: p.candidate,
		Target:    route.target.Name,
		Binding:   route.target.Binding,
		Storage:   route.target.OutputStorage,
		Reserve:   route.target.OutputReservation,
		Policy:    route.policy,
	})
	if err == nil {
		c.retention.wantSample.Store(c.retention.enabled)
	}
	return err
}
