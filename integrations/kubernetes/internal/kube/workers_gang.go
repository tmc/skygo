package kube

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Refusals of gangReleasePlan. A refusal of one member refuses the group.
var (
	errGangShape       = errors.New("worker gang does not match its plans")
	errGangMissing     = errors.New("worker gang member is missing")
	errGangGates       = errors.New("worker gang member scheduling gates differ")
	errGangTerminating = errors.New("worker gang member is terminating")
)

// gangReleasePlan returns the gate release updates of an admitted worker gang:
// for each rank, in order, a deep copy of got[rank] with workerSchedulingGate
// removed. Each copy keeps its UID and resourceVersion so that the update
// applies only to the observed original Pod.
//
// plans, got and uids must have the same length N, from 2 to maxWorkers; plan
// i must be rank i of one N-rank group; and each got[i] must match plans[i]
// and uids[i] under workerPodMatches, carry exactly the one skygo gate and not
// be terminating. Every plan must use the same trusted registry snapshot and
// each observed Pod must have a resourceVersion. Otherwise gangReleasePlan
// returns a nil plan and an error:
// errGangShape, errGangMissing, errGangGates, errGangTerminating or
// backend.ErrIdentityMismatch. It never removes another gate and never
// returns a partial plan. It modifies none of its arguments.
func (b *Backend) gangReleasePlan(plans []workerPodPlan, got []*corev1.Pod, uids []types.UID) ([]*corev1.Pod, error) {
	n := len(plans)
	if n < minWorkers || n > maxWorkers || len(got) != n || len(uids) != n {
		return nil, errGangShape
	}
	q := plans[0]
	for i, p := range plans {
		if p.Rank != uint32(i) || p.Count != uint32(n) || p.JobID != q.JobID ||
			p.AttemptID != q.AttemptID || p.Mode != q.Mode ||
			p.Registry.ServiceAccountUID != q.Registry.ServiceAccountUID ||
			!slices.Equal(p.Registry.PullSecrets, q.Registry.PullSecrets) {
			return nil, errGangShape
		}
	}
	out := make([]*corev1.Pod, 0, n)
	for i, g := range got {
		if g == nil {
			return nil, fmt.Errorf("worker rank %d: %w", i, errGangMissing)
		}
		if g.ResourceVersion == "" {
			return nil, fmt.Errorf("worker rank %d: %w: resource version is missing", i, errGangShape)
		}
		uid := uids[i]
		if g.UID != uid {
			return nil, fmt.Errorf("worker rank %d: %w: original uid differs", i, backend.ErrIdentityMismatch)
		}
		gates := g.Spec.SchedulingGates
		if len(gates) != 1 || gates[0].Name != workerSchedulingGate {
			return nil, fmt.Errorf("worker rank %d: %w", i, errGangGates)
		}
		if g.DeletionTimestamp != nil {
			return nil, fmt.Errorf("worker rank %d: %w", i, errGangTerminating)
		}
		if err := b.workerPodMatches(g, plans[i], uid); err != nil {
			return nil, fmt.Errorf("worker rank %d: %w", i, err)
		}
		if i > 0 && g.Annotations[workerGroupAnnotation] != got[0].Annotations[workerGroupAnnotation] {
			return nil, errGangShape
		}
		// The one gate is the skygo gate, checked above.
		c := g.DeepCopy()
		c.Spec.SchedulingGates = nil
		out = append(out, c)
	}
	return out, nil
}

// gangState is the assembly state of a released worker gang.
type gangState uint8

const (
	// gangAssembling: some ranks are not yet bound to a node.
	gangAssembling gangState = iota + 1
	// gangAssembled: every rank is bound to a node.
	gangAssembled
	// gangAssemblyFailed: some ranks are unbound after the assembly limit.
	gangAssemblyFailed
	// gangHeld: some original Pod is missing or foreign; neither assembly
	// nor failure can be decided from got.
	gangHeld
)

// gangAssembly classifies the gang whose rank i has original UID uids[i] and
// observed Pod got[i], released at released and observed at now.
//
// A rank is bound when got[i] has UID uids[i] and a nonempty spec.nodeName.
// PodScheduled conditions and the phase are not consulted. If any got[i] is
// nil, has an invalid original UID or has another UID, gangAssembly returns
// gangHeld and those ranks. If got and uids differ in length or have fewer
// than minWorkers or more than maxWorkers entries, it
// returns gangHeld and no ranks. Otherwise it returns gangAssembled when every
// rank is bound, gangAssemblyFailed and the unbound ranks when now is more
// than limit after released, and gangAssembling and the unbound ranks
// otherwise. Returned ranks are in increasing order.
func gangAssembly(got []*corev1.Pod, uids []types.UID, released, now time.Time, limit time.Duration) (gangState, []uint32) {
	if len(got) != len(uids) || len(uids) < minWorkers || len(uids) > maxWorkers {
		return gangHeld, nil
	}
	var held, unbound []uint32
	for i, g := range got {
		switch {
		case g == nil || !validInstance(string(uids[i])) || g.UID != uids[i]:
			held = append(held, uint32(i))
		case g.Spec.NodeName == "":
			unbound = append(unbound, uint32(i))
		}
	}
	switch {
	case held != nil:
		return gangHeld, held
	case unbound == nil:
		return gangAssembled, nil
	case now.Sub(released) > limit:
		return gangAssemblyFailed, unbound
	}
	return gangAssembling, unbound
}
