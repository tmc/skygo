// Package environmentwire converts persistent environment specs between
// [environment.Spec] and the skygo.v1 wire messages, and checks the
// environment messages a server returns.
//
// A message carrying unknown fields or undeclared enum values at any depth is
// refused, so a field added later is never silently dropped. Conversions copy:
// a result shares no memory with its input. [environment.Normalize] remains
// the only validator of a spec.
package environmentwire
