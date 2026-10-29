package serving

import (
	"context"
	"errors"
	"net/http"
	"strings"
)

// ErrUnsupported means a target cannot run a spec feature. Check returns it
// before any side effect.
var ErrUnsupported = errors.New("unsupported service feature")

// ErrIdentityConflict means native objects do not match a service's recorded
// identity. Retrying must not adopt, modify or re-create them.
var ErrIdentityConflict = errors.New("service identity conflict")

// Desired is a service's committed lifecycle intent. Zero is invalid.
type Desired uint8

const (
	Present Desired = iota + 1
	Absent
)

// Native is a target's opaque record of a service's native identity, at most
// 4 KiB. Only the integration that returned it interprets it. Empty means
// nothing has been recorded.
type Native string

// Ref addresses one service's native objects. Service, Owner and Nonce are
// 32 lowercase hexadecimal characters: the service ID, the installation ID
// and the creation nonce committed before any native request. Binding is the
// accepted target binding.
type Ref struct {
	Service string `json:"service"`
	Owner   string `json:"owner"`
	Nonce   string `json:"nonce"`
	Binding string `json:"binding"`
	Native  Native `json:"native,omitempty"`
}

// Validate checks r's shape. It proves no ownership of native objects.
func (r Ref) Validate() error {
	if !lowerHex(r.Service, 32) || !lowerHex(r.Owner, 32) || !lowerHex(r.Nonce, 32) {
		return errors.New("invalid service reference id")
	}
	if len(r.Binding) == 0 || len(r.Binding) > 256 {
		return errors.New("invalid service target binding")
	}
	for i := 0; i < len(r.Binding); i++ {
		if c := r.Binding[i]; c < ' ' || c > '~' {
			return errors.New("invalid service target binding")
		}
	}
	if len(r.Native) > 4<<10 {
		return errors.New("native identity exceeds 4 KiB")
	}
	return nil
}

// Reason classifies an observation. Zero is none. IdentityConflict marks
// foreign objects; every other reason is a rollout failure of the observed
// generation's replicas.
type Reason uint8

const (
	NoReason         Reason = iota
	ProgressDeadline        // progress deadline exceeded
	ImagePull               // image pull error
	ImagePullBackOff        // image pull back-off
	CrashLoop               // container crash loop back-off
	ContainerConfig         // container configuration error
	InvalidImage            // invalid image name
	IdentityConflict        // native objects are not the recorded ones
)

func (r Reason) failure() bool { return r >= ProgressDeadline && r <= InvalidImage }

// Observation is one runtime read of a service's native state, counting only
// objects of the recorded identity. Generations are rollout generations.
// ObservedGeneration is the newest one the native controller has acted on.
// ReadyGeneration is the newest one whose replicas are all updated, ready
// and available with no older replicas left. Absent reports that every owned
// object was observed gone; a failed read is an error, never Absent.
type Observation struct {
	ObservedGeneration uint64 `json:"observed_generation"`
	ReadyGeneration    uint64 `json:"ready_generation"`
	DesiredReplicas    int32  `json:"desired_replicas"`
	UpdatedReplicas    int32  `json:"updated_replicas"`
	ReadyReplicas      int32  `json:"ready_replicas"`
	AvailableReplicas  int32  `json:"available_replicas"`
	ReadyEndpoints     int32  `json:"ready_endpoints"`
	Reason             Reason `json:"reason,omitempty"`
	Absent             bool   `json:"absent,omitempty"`
}

// Validate checks o's internal consistency.
func (o Observation) Validate() error {
	if o.Absent {
		if o != (Observation{Absent: true}) {
			return errors.New("absent observation reports state")
		}
		return nil
	}
	if o.ReadyGeneration > o.ObservedGeneration {
		return errors.New("ready generation ahead of observed generation")
	}
	if o.DesiredReplicas < 0 || o.UpdatedReplicas < 0 || o.ReadyReplicas < 0 || o.AvailableReplicas < 0 || o.ReadyEndpoints < 0 {
		return errors.New("negative replica count")
	}
	if o.AvailableReplicas > o.ReadyReplicas {
		return errors.New("available replicas exceed ready replicas")
	}
	if o.Reason > IdentityConflict {
		return errors.New("invalid observation reason")
	}
	return nil
}

// Phase is a service's reported state. Zero is invalid. Held means native
// state is unknown or not the recorded one; it keeps charges and pins and is
// never Deleted or Ready.
type Phase uint8

const (
	Creating Phase = iota + 1
	Progressing
	Ready
	Degraded
	RolloutFailed
	Unavailable
	Deleting
	Deleted
	Held
)

// PhaseOf derives a service's phase from its intent d, valid history h and
// last observation o, which is nil before the first one. Only an observation
// shows availability: Ready requires the current generation ready and at
// least one ready endpoint. An invalid or conflicting observation is Held
// under either intent. Deleted requires a valid observation of absence; no
// other phase permits releasing a service's charges or pins.
func PhaseOf(d Desired, h History, o *Observation) Phase {
	if o != nil && (o.Validate() != nil || o.Reason == IdentityConflict) {
		return Held
	}
	switch d {
	case Absent:
		if o != nil && o.Absent {
			return Deleted
		}
		return Deleting
	case Present:
	default:
		return Held
	}
	_, complete := h.LastComplete()
	if o == nil {
		if complete {
			return Held
		}
		return Creating
	}
	cur := h.Current()
	if o.Absent || o.ObservedGeneration > cur.Generation {
		return Held
	}
	switch {
	case cur.Outcome == Failed:
		return RolloutFailed
	case o.ReadyGeneration == cur.Generation:
		if o.ReadyEndpoints == 0 {
			return Unavailable
		}
		if o.ReadyReplicas < o.DesiredReplicas {
			return Degraded
		}
		return Ready
	case o.ObservedGeneration == cur.Generation && o.Reason.failure():
		return RolloutFailed
	case o.ReadyEndpoints > 0:
		return Progressing
	case !complete:
		return Creating
	}
	return Unavailable
}

func (p Phase) String() string {
	return enumString(int(p), "creating", "progressing", "ready", "degraded", "rollout-failed", "unavailable", "deleting", "deleted", "held")
}

func (r Reason) String() string {
	if r == NoReason {
		return "none"
	}
	return enumString(int(r), "progress-deadline", "image-pull", "image-pull-back-off", "crash-loop", "container-config", "invalid-image", "identity-conflict")
}

// Runtime is a target's service implementation; a target without one does
// not support services. The core reconciler calls it only after committing
// durable intent. Methods are safe for concurrent use.
//
// Check validates a normalized spec against target capability without I/O.
// It returns an error wrapping ErrUnsupported for a feature the target lacks.
//
// Apply makes native state run rev as rollout generation. It returns the
// native identity it recorded, also alongside an error, and the caller
// persists a nonempty result before any other call. With a nonempty
// ref.Native, Apply never re-creates objects; foreign or missing objects are
// ErrIdentityConflict. A nil error is not availability.
//
// Observe reads native state. It returns an error for temporary uncertainty
// and reports foreign objects as IdentityConflict and missing ones as Absent.
//
// Delete removes only objects of ref's recorded identity and returns the
// resulting observation, Absent only once every owned object is gone.
//
// Forward serves one authorized request from a ready endpoint of ref's
// service. It never takes a destination from the caller, and sends the
// workload only the headers ForwardHeader keeps. It returns only once every
// original owner of the exchange — its request body, response body and
// connections — has returned, whether or not ctx has ended.
type Runtime interface {
	Check(Spec) error
	Apply(ctx context.Context, ref Ref, rev Revision, generation uint64) (Native, error)
	Observe(ctx context.Context, ref Ref) (Observation, error)
	Delete(ctx context.Context, ref Ref) (Observation, error)
	Forward(ctx context.Context, ref Ref, w http.ResponseWriter, r *http.Request) error
}

// ForwardHeader returns a copy of the header of a request forwarded to a
// service's workload without the control plane's credentials and proofs:
// Authorization, Proxy-Authorization and Cookie; the dashboard's session
// proof and login marker and every other header of the X-Skygo- and Skygo-
// control namespaces; and Kubernetes impersonation, Impersonate-User,
// Impersonate-Uid, Impersonate-Group and every Impersonate-Extra- field.
// Every other header is kept as it is.
func ForwardHeader(h http.Header) http.Header {
	out := h.Clone()
	if out == nil {
		out = http.Header{}
	}
	for name := range out {
		if controlHeader(http.CanonicalHeaderKey(name)) {
			delete(out, name)
		}
	}
	return out
}

func controlHeader(name string) bool {
	switch name {
	case "Authorization", "Proxy-Authorization", "Cookie",
		"Impersonate-User", "Impersonate-Uid", "Impersonate-Group":
		return true
	}
	for _, prefix := range []string{"Impersonate-Extra-", "X-Skygo-", "Skygo-"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}
