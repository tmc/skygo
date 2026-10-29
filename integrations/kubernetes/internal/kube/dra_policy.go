package kube

import (
	"errors"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/tmc/skygo/internal/backend"
	"k8s.io/apimachinery/pkg/util/validation"
)

// DRAPolicy maps a portable accelerator name to dynamic resource allocation
// through an operator-provisioned resource.k8s.io/v1 DeviceClass. A task
// that names the accelerator gets one standalone ResourceClaim with a single
// request for exactly its accelerator count of devices of DeviceClass that
// satisfy every selector, and exactly one directly managed Pod whose task
// container uses that request. Tasks supply only the name and count; they
// cannot supply classes, selectors or Kubernetes objects.
//
// DeviceClass is a DNS subdomain. Selectors are at most 8 CEL expressions of
// 1 to 1024 bytes each, kept in order. Only the DRA integration
// (NewDRAIntegration) enforces and advertises a DRA accelerator; other
// constructors refuse its tasks.
type DRAPolicy struct {
	DeviceClass string   `yaml:"device_class"`
	Selectors   []string `yaml:"selectors"`
}

// The fixed shape of a DRA attempt's claim and its reference (dra_identity.go).
const (
	maxDRASelectors     = 8
	maxDRASelectorBytes = 1024
	draRequestName      = "accelerator" // the claim's only request
	draPodClaimName     = "accelerator" // the Pod's only resource claim entry
	draAllocationMode   = "ExactCount"
	draReference        = "resourceClaimName"
)

var errDRAUnavailable = errors.New("kubernetes dra accelerator requires the dra integration")

func validateDRAPolicy(p DRAPolicy) error {
	if len(validation.IsDNS1123Subdomain(p.DeviceClass)) != 0 {
		return errors.New("invalid dra device class")
	}
	if len(p.Selectors) > maxDRASelectors {
		return errors.New("dra selectors exceed 8 entries")
	}
	for i, s := range p.Selectors {
		if s == "" || len(s) > maxDRASelectorBytes || !utf8.ValidString(s) || strings.ContainsRune(s, 0) || strings.TrimSpace(s) == "" {
			return errors.New("invalid dra selector")
		}
		if slices.Contains(p.Selectors[:i], s) {
			return errors.New("duplicate dra selector")
		}
	}
	return nil
}

func cloneDRAPolicy(p *DRAPolicy) *DRAPolicy {
	if p == nil {
		return nil
	}
	c := *p
	c.Selectors = slices.Clone(p.Selectors)
	return &c
}

// hasDRA reports whether p maps any accelerator to DRA.
func (p ResourcePolicy) hasDRA() bool {
	for _, a := range p.Accelerators {
		if a.DRA != nil {
			return true
		}
	}
	return false
}

// draPolicy returns the DRA policy of t's accelerator, or nil if t names no
// DRA accelerator. It is the single routing predicate of the DRA mode: Start,
// Observe, Cancel and Logs of such a task never take the Job or retry path.
func (b *Backend) draPolicy(t backend.Task) *DRAPolicy {
	if t.Accelerator == "" {
		return nil
	}
	return b.policy.Accelerators[t.Accelerator].DRA
}

// checkDRA is the pure admission of a DRA task, after checkPolicy. It refuses
// every mode this first slice does not implement before any effect: a target
// without the DRA integration, retry, data, workdir, setup or outputs.
func (b *Backend) checkDRA(t backend.Task) error {
	if b.dra == nil {
		return errDRAUnavailable
	}
	if t.Retry != nil {
		return errors.New("kubernetes dra does not support retry")
	}
	if dataTask(t) {
		return errors.New("kubernetes dra does not support data, workdir, setup or outputs")
	}
	if t.AcceleratorCount <= 0 {
		return errors.New("kubernetes dra requires a positive accelerator count")
	}
	return nil
}
