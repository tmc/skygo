package environmentcontrol

import (
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/environment"
)

// Support is whether a Controller's runtime accepts a spec under its current
// configuration.
type Support int

const (
	// Supported means only that the runtime's Plan accepted the spec. It is
	// not proof that a command or a served port will work.
	Supported Support = iota + 1
	// Unsupported carries no reason.
	Unsupported
)

// Fact is what planning learned about a precondition that only acceptance
// enforces.
type Fact int

const (
	// FactNotApplicable means the precondition does not apply.
	FactNotApplicable Fact = iota
	// FactUnknown means it was not checked; nothing is implied.
	FactUnknown
	// FactVerified means a current authorized read found it met. It is
	// not reserved.
	FactVerified
	// FactRefused means a current authorized read found it unmet.
	FactRefused
)

// A Plan is what Create would check of a spec before acceptance. It holds no
// binding, native state, checkpoint or profile.
type Plan struct {
	// Spec is the spec normalized as Create would accept it.
	Spec environment.Spec
	// SpecDigest is the SHA-256 of Spec's canonical encoding, which Create
	// would record.
	SpecDigest [32]byte
	Support    Support
	// Artifacts is whether every artifact mount names a complete artifact
	// the principal may pin now: FactNotApplicable exactly when Spec has
	// no artifacts, and otherwise FactUnknown, because Plan reads nothing
	// of the Store.
	Artifacts Fact
}

// Plan checks spec as Create would before acceptance, with only actor's read
// authority. It records nothing and has no effect: no environment, request,
// name or alias reservation, artifact pin, delegation or runtime request. It
// checks neither the name, the storage alias nor any quota, so a later Create
// may still be refused.
//
// An invalid spec is ErrInvalid. A spec with artifacts that this Controller
// does not materialize, or that the runtime's Plan refuses with
// ErrUnsupported, is Unsupported, with no reason. Any other runtime error is
// returned; its text may name configuration and is not for the caller.
func (c *Controller) Plan(actor auth.Actor, spec environment.Spec) (Plan, error) {
	actor, err := c.auth.Check(actor, auth.Read)
	if err != nil {
		return Plan{}, err
	}
	spec, err = environment.Normalize(spec)
	if err != nil {
		return Plan{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	raw, err := environment.Canonical(spec)
	if err != nil {
		return Plan{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	p := Plan{Spec: spec, SpecDigest: sha256.Sum256(raw), Support: Unsupported}
	if len(spec.Artifacts) != 0 {
		p.Artifacts = FactUnknown
		if !c.ArtifactsEnabled() {
			return p, nil
		}
	}
	// The binding and native state are dropped: they are server-only.
	_, native, err := c.runtime.Plan(actor.Principal(), spec.Clone(), p.SpecDigest)
	switch {
	case errors.Is(err, ErrUnsupported):
		return p, nil
	case err != nil:
		return Plan{}, err
	case len(native) > MaxNativeBytes:
		return Plan{}, errors.New("runtime native state exceeds limit")
	}
	p.Support = Supported
	return p, nil
}
