package serving

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
)

// Revision is one immutable numbered spec. Spec is normalized and SHA256 is
// its Digest; equal digests mean equal specs.
type Revision struct {
	Number uint64 `json:"number"`
	Spec   Spec   `json:"spec"`
	SHA256 string `json:"sha256"`
}

// Kind records why a rollout was requested. Zero is invalid.
type Kind uint8

const (
	Create Kind = iota + 1
	Update
	Rollback
)

// Outcome is a rollout's committed result. Zero is invalid. Pending is
// committed intent; Rolling means the runtime observed the generation.
// Only the current rollout may be Pending or Rolling. Superseded replaces an
// unfinished outcome when a newer rollout is committed.
type Outcome uint8

const (
	Pending Outcome = iota + 1
	Rolling
	Complete
	Failed
	Superseded
)

// Rollout is one numbered request to run a revision. Generation increases by
// one for every create, update and rollback of a service.
type Rollout struct {
	Generation uint64  `json:"generation"`
	Revision   uint64  `json:"revision"`
	Kind       Kind    `json:"kind"`
	Outcome    Outcome `json:"outcome"`
}

// History is one service's retained revisions and rollouts, both in ascending
// order. The last rollout is current. Methods never modify their receiver.
type History struct {
	Revisions []Revision `json:"revisions"`
	Rollouts  []Rollout  `json:"rollouts"`
}

const (
	maxRevisions = 8
	maxRollouts  = 32
)

// NewHistory returns the history of a new service: revision 1 of s and a
// pending create rollout with generation 1.
func NewHistory(s Spec) (History, error) {
	n, err := s.Normalize()
	if err != nil {
		return History{}, err
	}
	return History{
		Revisions: []Revision{{Number: 1, Spec: n, SHA256: n.digest()}},
		Rollouts:  []Rollout{{Generation: 1, Revision: 1, Kind: Create, Outcome: Pending}},
	}, nil
}

// Validate checks numbering, normalized specs and digests, rollout ordering
// and outcomes, and that the current and last complete rollouts' revisions
// are retained.
func (h History) Validate() error {
	if len(h.Revisions) == 0 || len(h.Rollouts) == 0 {
		return errors.New("empty service history")
	}
	for i, r := range h.Revisions {
		if r.Number == 0 || i > 0 && r.Number <= h.Revisions[i-1].Number {
			return errors.New("revision numbers not ascending")
		}
		n, err := r.Spec.Normalize()
		if err != nil {
			return fmt.Errorf("revision %d: %w", r.Number, err)
		}
		if !bytes.Equal(n.canonical(), r.Spec.canonical()) {
			return fmt.Errorf("revision %d: spec not normalized", r.Number)
		}
		if r.SHA256 != n.digest() {
			return fmt.Errorf("revision %d: digest mismatch", r.Number)
		}
	}
	newest := h.Revisions[len(h.Revisions)-1].Number
	last := len(h.Rollouts) - 1
	for i, r := range h.Rollouts {
		if r.Generation == 0 || i > 0 && r.Generation <= h.Rollouts[i-1].Generation {
			return errors.New("rollout generations not ascending")
		}
		if r.Revision == 0 || r.Revision > newest {
			return fmt.Errorf("rollout %d: unknown revision", r.Generation)
		}
		if r.Kind < Create || r.Kind > Rollback || (r.Kind == Create) != (r.Generation == 1) {
			return fmt.Errorf("rollout %d: invalid kind", r.Generation)
		}
		switch r.Outcome {
		case Pending, Rolling:
			if i != last {
				return fmt.Errorf("rollout %d: unfinished but not current", r.Generation)
			}
		case Superseded:
			if i == last {
				return fmt.Errorf("rollout %d: current rollout superseded", r.Generation)
			}
		case Complete, Failed:
		default:
			return fmt.Errorf("rollout %d: invalid outcome", r.Generation)
		}
	}
	if _, ok := h.Revision(h.Current().Revision); !ok {
		return errors.New("current revision not retained")
	}
	if c, ok := h.LastComplete(); ok {
		if _, ok := h.Revision(c.Revision); !ok {
			return errors.New("last complete revision not retained")
		}
	}
	return nil
}

// Current returns the last rollout, or the zero Rollout for an empty history.
func (h History) Current() Rollout {
	if len(h.Rollouts) == 0 {
		return Rollout{}
	}
	return h.Rollouts[len(h.Rollouts)-1]
}

// LastComplete returns the newest Complete rollout.
func (h History) LastComplete() (Rollout, bool) {
	for i := len(h.Rollouts) - 1; i >= 0; i-- {
		if h.Rollouts[i].Outcome == Complete {
			return h.Rollouts[i], true
		}
	}
	return Rollout{}, false
}

// Revision returns the retained revision numbered n.
func (h History) Revision(n uint64) (Revision, bool) {
	i, ok := slices.BinarySearchFunc(h.Revisions, n, func(r Revision, n uint64) int {
		switch {
		case r.Number < n:
			return -1
		case r.Number > n:
			return 1
		}
		return 0
	})
	if !ok {
		return Revision{}, false
	}
	return h.Revisions[i].clone(), true
}

// Update adds a revision of s with the next number and a pending rollout of it
// with the next generation, superseding an unfinished current rollout. It
// refuses a spec equal to the current rollout's revision. The caller must
// re-run admission for the new revision before committing the result.
func (h History) Update(s Spec) (History, error) {
	if err := h.Validate(); err != nil {
		return History{}, err
	}
	n, err := s.Normalize()
	if err != nil {
		return History{}, err
	}
	cur := h.Current()
	target, _ := h.Revision(cur.Revision)
	d := n.digest()
	if d == target.SHA256 {
		return History{}, errors.New("spec equals current revision")
	}
	newest := h.Revisions[len(h.Revisions)-1].Number
	if newest == math.MaxUint64 || cur.Generation == math.MaxUint64 {
		return History{}, errors.New("service history exhausted")
	}
	out := h.clone()
	out.supersede()
	out.Revisions = append(out.Revisions, Revision{Number: newest + 1, Spec: n, SHA256: d})
	out.Rollouts = append(out.Rollouts, Rollout{Generation: cur.Generation + 1, Revision: newest + 1, Kind: Update, Outcome: Pending})
	return out, nil
}

// Rollback adds a pending rollout of retained revision n with the next
// generation, superseding an unfinished current rollout. It creates no
// revision and refuses the current rollout's revision. The caller must
// re-run admission for revision n before committing the result.
func (h History) Rollback(n uint64) (History, error) {
	if err := h.Validate(); err != nil {
		return History{}, err
	}
	if _, ok := h.Revision(n); !ok {
		return History{}, fmt.Errorf("revision %d not retained", n)
	}
	cur := h.Current()
	if n == cur.Revision {
		return History{}, fmt.Errorf("revision %d is the current revision", n)
	}
	if cur.Generation == math.MaxUint64 {
		return History{}, errors.New("service history exhausted")
	}
	out := h.clone()
	out.supersede()
	out.Rollouts = append(out.Rollouts, Rollout{Generation: cur.Generation + 1, Revision: n, Kind: Rollback, Outcome: Pending})
	return out, nil
}

// Settle advances an unfinished current rollout from runtime observation o.
// The rollout is Complete only when o reports its generation ready, Failed
// only when o reports a rollout failure for its generation, and otherwise
// Rolling once o has observed it. Settle leaves the history unchanged for a
// stale, absent or conflicting observation, and refuses one that is ahead of
// the current rollout.
func (h History) Settle(o Observation) (History, error) {
	if err := h.Validate(); err != nil {
		return History{}, err
	}
	if err := o.Validate(); err != nil {
		return History{}, err
	}
	cur := h.Current()
	if o.ObservedGeneration > cur.Generation {
		return History{}, errors.New("observation ahead of current rollout")
	}
	out := h.clone()
	if o.Absent || o.Reason == IdentityConflict || o.ObservedGeneration < cur.Generation || cur.Outcome != Pending && cur.Outcome != Rolling {
		return out, nil
	}
	outcome := Rolling
	switch {
	case o.ReadyGeneration == cur.Generation:
		outcome = Complete
	case o.Reason.failure():
		outcome = Failed
	}
	out.Rollouts[len(out.Rollouts)-1].Outcome = outcome
	return out, nil
}

// Prune returns h without its oldest revisions beyond 8 and oldest rollouts
// beyond 32, and the removed revisions. It never removes the current rollout,
// the last complete rollout, their revisions or the newest revision. The
// caller releases a removed revision's resources only after committing the
// result.
func (h History) Prune() (History, []Revision, error) {
	if err := h.Validate(); err != nil {
		return History{}, nil, err
	}
	cur := h.Current()
	complete, hasComplete := h.LastComplete()
	newest := h.Revisions[len(h.Revisions)-1].Number
	var out History
	var removed []Revision
	excess := len(h.Revisions) - maxRevisions
	for _, r := range h.Revisions {
		if excess > 0 && r.Number != cur.Revision && r.Number != newest && !(hasComplete && r.Number == complete.Revision) {
			removed = append(removed, r.clone())
			excess--
			continue
		}
		out.Revisions = append(out.Revisions, r.clone())
	}
	excess = len(h.Rollouts) - maxRollouts
	for _, r := range h.Rollouts {
		if excess > 0 && r.Generation != cur.Generation && !(hasComplete && r.Generation == complete.Generation) {
			excess--
			continue
		}
		out.Rollouts = append(out.Rollouts, r)
	}
	return out, removed, nil
}

func (h History) clone() History {
	out := History{Revisions: make([]Revision, len(h.Revisions)), Rollouts: slices.Clone(h.Rollouts)}
	for i, r := range h.Revisions {
		out.Revisions[i] = r.clone()
	}
	return out
}

// supersede marks an unfinished current rollout Superseded.
func (h History) supersede() {
	if r := &h.Rollouts[len(h.Rollouts)-1]; r.Outcome == Pending || r.Outcome == Rolling {
		r.Outcome = Superseded
	}
}

func (r Revision) clone() Revision {
	r.Spec = r.Spec.clone()
	return r
}

func (k Kind) String() string {
	return enumString(int(k), "create", "update", "rollback")
}

func (o Outcome) String() string {
	return enumString(int(o), "pending", "rolling", "complete", "failed", "superseded")
}

// enumString returns names[v-1], or "invalid" outside 1..len(names).
func enumString(v int, names ...string) string {
	if v < 1 || v > len(names) {
		return "invalid"
	}
	return names[v-1]
}
