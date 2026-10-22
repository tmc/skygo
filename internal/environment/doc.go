// Package environment defines the portable values of one persistent
// environment: its accepted Spec, its Identity, its desired and observed
// State, and the server-only provider Checkpoint that a concrete adapter
// stores with it.
//
// Canonical checks a Spec and returns its versioned canonical JSON, and Parse
// accepts only those bytes. Normalize returns an independent copy in which an
// empty selector is nil. None of them applies image, profile, target or
// authority rules; the selected concrete adapter checks those before
// acceptance.
//
// Desired Absent is latched. Phase Ready reports a fresh matching provider
// observation of the environment's own resources. It is not evidence that an
// installed agent can execute commands or that a service can accept traffic.
// Phase Unknown keeps every reservation; it never implies that remote
// resources stopped or were removed.
//
// A Checkpoint is opaque provider state with an immutable kind and version.
// This package bounds its size and copies it; it never interprets, runs or
// converts its bytes. A Checkpoint must never hold credentials or tokens.
package environment
