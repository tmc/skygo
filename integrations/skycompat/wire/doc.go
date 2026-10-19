// Package wire encodes the selected SkyPilot API67 JSON profile.
//
// DecodeValidate accepts a bounded validation body and discards explicitly
// recognized, nonauthoritative transport context. It does not validate the DAG's
// YAML, authorize a caller, or dispatch work. The response encoders require
// server-selected operation metadata and safe error messages. They emit only
// JSON and a fixed, write-only primitive pickle subset required by the pinned
// SDK; the package cannot decode pickle.
//
// The profile is pinned to SkyPilot commit
// 922a9f046c4a2714edf2b76fec3d1862dcd56ae3. None bookkeeping fields support
// SDK result/error decoding, not request introspection or reexecution.
// This package does not implement HTTP routing or advertise full API67 support.
package wire
