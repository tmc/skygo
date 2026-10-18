// Package rpc serves the generated Connect control API over net/http.
//
// NewHandler applies request and stream bounds and optional bearer authentication.
// The caller owns the listener, HTTP timeouts, shutdown, and transport encryption.
package rpc
