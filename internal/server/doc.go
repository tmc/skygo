// Package server assembles the singleton control plane, durable store, optional
// execution targets, and HTTP lifecycle.
//
// Run binds a loopback listener by default through the CLI. A non-loopback
// listener requires a bearer token and a trusted network or TLS proxy. Canceling
// its context drains connections and stops owned local process groups; native
// Kubernetes Jobs continue independently.
package server
