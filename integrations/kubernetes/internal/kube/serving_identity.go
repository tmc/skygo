package kube

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/tmc/skygo/internal/serving"
	"k8s.io/apimachinery/pkg/types"
)

// Labels and annotations of the objects a service runtime creates. The
// labels select; they are never evidence of ownership. Ownership is the UID
// that the committed Native records, or, for a create whose response was
// lost, the declaration annotation that only that declared create carries.
const (
	serviceLabel       = "skygo.dev/service"
	serviceNonceLabel  = "skygo.dev/service-nonce"
	serviceOwnerLabel  = "skygo.dev/owner"
	serviceDeclaration = "skygo.dev/declaration"
	serviceGeneration  = "skygo.dev/generation"
	serviceContainer   = "service"
	serviceNativeV1    = 1
)

// serviceNative is version 1 of a service's Native: the identity of its
// owned Deployment and Service, and the one create of each that may have
// been sent. A create is declared — its Declaration set, Sent false — in a
// Native the controller commits before it is sent. Sent records that this
// runtime handed it to the transport; UID records its positive outcome. A
// declaration recorded as sent without a UID, or one made before a restart,
// is unresolved: only an object carrying its declaration resolves it.
type serviceNative struct {
	Version    int           `json:"v"`
	Port       int32         `json:"port"`
	Generation uint64        `json:"generation"`
	Deployment serviceObject `json:"deployment"`
	Service    serviceObject `json:"service"`
}

// serviceObject is one owned object. Spec is the witness of the object's
// exact rendering, fixed with its declaration: an object is accepted as this
// one only if its identity projection has this witness (see
// serving_accept.go), and Pod is, for the Deployment, the witness of its Pod
// template. Refused records a create whose complete answer was not a 201
// Created of exactly the rendering: any other status, a redirect included,
// or a 201 of another object. It is terminal: no later read resolves it, no
// UID the answer reported is recorded, and the runtime deletes nothing on
// its account. It is not proof that nothing was created, so a refused
// create is never observed gone. Refusal is the bounded evidence of that
// answer.
type serviceObject struct {
	Declaration string          `json:"declaration,omitempty"`
	Spec        string          `json:"spec,omitempty"`
	Pod         string          `json:"pod,omitempty"`
	Sent        bool            `json:"sent,omitempty"`
	Refused     bool            `json:"refused,omitempty"`
	Refusal     *serviceRefusal `json:"refusal,omitempty"`
	UID         types.UID       `json:"uid,omitempty"`
}

// serviceRefusal is the bounded witness of the complete answer that refused
// a create: its status, the size and SHA-256 of its exact body bytes, and
// whether the body decoded as the expected kind. It never holds the body.
type serviceRefusal struct {
	Status  int    `json:"status"`
	Size    int64  `json:"size"`
	SHA256  string `json:"sha256"`
	Decoded bool   `json:"decoded,omitempty"`
}

// declared reports whether o's create was declared.
func (o serviceObject) declared() bool { return o.Declaration != "" }

// unresolved reports whether o's create may have been sent with no known
// outcome.
func (o serviceObject) unresolved() bool { return o.Declaration != "" && o.UID == "" && !o.Refused }

func (n serviceNative) encode() serving.Native {
	raw, _ := json.Marshal(n)
	return serving.Native(raw)
}

// decodeServiceNative decodes a committed Native. Empty is the zero Native
// of a service with nothing declared.
func decodeServiceNative(raw serving.Native) (serviceNative, error) {
	if raw == "" {
		return serviceNative{Version: serviceNativeV1}, nil
	}
	var n serviceNative
	d := json.NewDecoder(bytes.NewReader([]byte(raw)))
	d.DisallowUnknownFields()
	if err := d.Decode(&n); err != nil || n.Version != serviceNativeV1 || n.encode() != raw {
		return serviceNative{}, errors.New("invalid kubernetes service native state")
	}
	for i, o := range []serviceObject{n.Deployment, n.Service} {
		// A declaration fixes its witnesses: the Deployment's spec and Pod
		// template, the Service's spec.
		witnessed := lowerHexString(o.Spec, 64) && lowerHexString(o.Pod, 64) == (i == 0) && (i == 0 || o.Pod == "")
		if !o.declared() {
			witnessed = o.Spec == "" && o.Pod == ""
		}
		if !witnessed ||
			o.UID != "" && !o.declared() || o.Sent && !o.declared() || o.Refused && (!o.Sent || o.UID != "") ||
			o.Refused != (o.Refusal != nil) || o.Refusal != nil && !o.Refusal.valid() ||
			len(o.Declaration) > 64 || len(o.UID) > 128 {
			return serviceNative{}, errors.New("invalid kubernetes service native state")
		}
	}
	return n, nil
}

func (r *serviceRefusal) valid() bool {
	return r.Status >= 100 && r.Status <= 599 && r.Size >= 0 && r.Size <= maxServiceResponse && lowerHexString(r.SHA256, 64)
}

func lowerHexString(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// serviceObjectName returns the name of both owned objects of service id: a
// fixed prefix and the ID's first 20 hex digits, a DNS label of 30 bytes.
// The name never identifies ownership; it only places the create.
func serviceObjectName(id string) string { return "skygo-svc-" + id[:20] }

// serviceSelector returns the labels that select service ref's Pods.
func serviceSelector(ref serving.Ref) map[string]string {
	return map[string]string{serviceLabel: ref.Service, serviceNonceLabel: ref.Nonce}
}
