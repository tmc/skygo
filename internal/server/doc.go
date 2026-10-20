// Package server assembles the singleton control plane, durable store, optional
// execution targets, and HTTP lifecycle.
//
// Run binds a loopback listener by default through the CLI. A non-loopback
// listener requires a bearer token and a trusted network or TLS proxy. Canceling
// its context requests original-owner preparation and draining. Kubernetes jobs
// continue independently. An unsettled lifetime returns RunError and retains
// its resources; cancellation and HTTP return do not prove resource closure.
//
// Config.ArtifactDir opens an existing private root installed by the explicit
// stopped InitializeArtifacts operation. It creates no marker or directory.
// Borrowed Config.Artifacts never supplies workspace-root authority.
package server
