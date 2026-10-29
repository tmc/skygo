// Package servingwire converts between ServingService messages and the
// portable serving types.
//
// SpecFromProto accepts only what the first serving endpoint runs: a
// digest-pinned image, argv, plain env, CPU and memory requests, one HTTP
// port with its readiness path, and exactly one fixed replica. Mounts,
// workdir, setup, CPU scaling and any other replica count are refused with
// ErrUnsupported before conversion, never dropped. A message with unknown
// fields at any depth is refused.
//
// The converters in the other direction project portable values into
// responses; CheckService validates a response on the client.
package servingwire
