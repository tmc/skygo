package server

import (
	"context"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/environmentcontrol"
)

// An Integration is a set of targets, and the work behind them, whose
// lifetime the server owns. Config.Install constructs it. The server runs it
// after Control starts, and closes it after Control and its Run have
// returned, before authentication stops and the Store closes.
//
// Install must return a nil interface, not a nil pointer, for no Integration.
type Integration interface {
	// Targets returns the integration's targets. The server calls it once,
	// before Control starts, and keeps the result.
	Targets() []backend.Target

	// Run runs the integration until ctx ends. A return before then shuts
	// the server down. Run must return promptly once ctx ends: if it has
	// not returned within the ten seconds the server allows Control and
	// the integration to return, the server returns a RunError that
	// retains the integration, never calls Close, and closes nothing the
	// integration depends on.
	Run(ctx context.Context) error

	// Close stops the integration and returns once all of its work has
	// returned. The server calls it once, without Run if startup failed
	// before the server started. If it returns an error, including
	// ctx's, the server closes nothing the integration depends on and
	// returns a RunError.
	//
	// ctx has a ten-second deadline, and honoring it is up to Close: the
	// server waits for Close synchronously, however long it takes, and
	// cannot make it return. A Close that never returns keeps the server's
	// Run, the integration and everything it depends on held for the life
	// of the process; nothing reports such a Close complete. A nil result
	// after the deadline is still a positive close.
	Close(ctx context.Context) error
}

// A ControlBinder is an Integration that uses the server's Control. The
// server calls BindControl once, after it constructs Control and before
// Control, the integration or the listener runs. An error fails startup; the
// integration is still closed before anything it depends on.
type ControlBinder interface {
	Integration
	BindControl(*control.Control) error
}

// An EnvironmentProvider is an Integration that owns an environment
// Controller, which must share the server's Store and authentication Engine.
// After BindControl, if any, succeeds and before the listener runs, the server
// calls EnvironmentController once and serves EnvironmentService and the
// authenticated forward route over the result; a nil result fails startup.
// The server neither runs nor closes the Controller: the integration's Run and
// Close own it, and Close also joins every runtime session and request the
// Controller does not. Only an EnvironmentProvider's targets may report
// backend.Target.SupportsPersistentEnvironments.
type EnvironmentProvider interface {
	Integration
	EnvironmentController() *environmentcontrol.Controller
}

// An EffectStopper is an Integration whose work can hold Control's artifact
// I/O. At shutdown the server calls StopEffects once, after Control's
// shutdown preparation and before it drains Control's artifact I/O, so that
// the work releases that I/O and the drain can complete. StopEffects must
// return promptly without waiting for the work: the integration's Run keeps
// running until Control's Run has returned, and Close still joins the work.
type EffectStopper interface {
	Integration
	StopEffects()
}
