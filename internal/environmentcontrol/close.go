package environmentcontrol

import (
	"context"
	"errors"
	"fmt"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
)

// StopMaterializing stops starting materializations and cancels the effects
// of those in progress: no frame starts and no Materialize is called after
// it returns, and a running Materialize is canceled. It returns at once. An
// opener call made with the ended effects refuses them before it opens
// anything, but StopMaterializing does not wait for an opener call already
// past that check: it may still open and verify its file after
// StopMaterializing returns. That call keeps its read's lease, and the
// opener its permit, until it returns and its body is closed; its body is
// never staged. The
// reconciliation of what was already begun continues: a Begin whose result
// is unknown is retried with its captured tuple, a lease so recovered is only
// released, and a body is closed only after its Materialize's Done.
// Mark and Finish of every read continue to be retried until they succeed.
// It is safe to call more than once.
func (c *Controller) StopMaterializing() {
	for _, f := range c.stopFrames() {
		f.stop()
	}
}

// stopFrames sets c.stopping and returns c's frames.
func (c *Controller) stopFrames() []*frame {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopping = true
	frames := make([]*frame, 0, len(c.materializing))
	for _, f := range c.materializing {
		frames = append(frames, f)
	}
	return frames
}

// Close calls StopMaterializing and waits until every materialization has
// returned or ctx ends. It returns nil only if none holds anything: no
// running call, no lease, no open or uncertain file and no Begin of unknown
// result. A read held by an unknown producer's lost Begin (S4) holds nothing
// of this process. Reconciliation continues after Close returns an error.
// Close does not stop or join exec or forward sessions; Integration.Close
// does.
func (c *Controller) Close(ctx context.Context) error {
	frames := c.stopFrames()
	for _, f := range frames {
		f.stop()
	}
	for _, f := range frames {
		select {
		case <-f.done:
		case <-ctx.Done():
		}
	}
	n := 0
	for _, f := range frames {
		if !f.settled() {
			n++
		}
	}
	if n != 0 {
		return errors.Join(fmt.Errorf("%d environment artifact materializations unsettled", n), ctx.Err())
	}
	return nil
}

// Integration is a Controller as a server integration.
type Integration struct {
	*Controller
	// StopRuntime, if not nil, stops the runtime's own frames: it cancels
	// them and refuses new ones, and returns at once. It must never delete
	// or stop an environment's resources. Close calls it before it waits
	// for any session, so that no wait depends on a runtime call that only
	// stopping the runtime ends. The runtime's owner joins the runtime's
	// frames after Close.
	StopRuntime func()
}

// Close stops the Controller's sessions: it refuses every new Exec and
// Forward at entry and cancels every tracked session frame. It then calls
// StopRuntime, calls Controller.Close for artifact materializations and
// waits until every session owner, including a settlement that outlived its
// Exec or Forward, has made its last Store operation. It returns an error if
// any materialization or session owner is unsettled when ctx ends; the
// caller must then keep the Store and authentication open. Controller.Close
// itself still joins materializations only.
func (in Integration) Close(ctx context.Context) error {
	in.stopSessions()
	if in.StopRuntime != nil {
		in.StopRuntime()
	}
	return errors.Join(in.Controller.Close(ctx), in.joinSessions(ctx))
}

// Targets returns nil.
func (Integration) Targets() []backend.Target { return nil }

// BindControl binds c as the opener of artifact reads. It implements
// server.ControlBinder.
func (in Integration) BindControl(c *control.Control) error {
	if c == nil {
		return errors.New("nil control")
	}
	return in.BindArtifacts(c)
}

// StopEffects calls StopMaterializing. It implements server.EffectStopper.
func (in Integration) StopEffects() { in.StopMaterializing() }

// EnvironmentController returns the Controller. It implements
// server.EnvironmentProvider.
func (in Integration) EnvironmentController() *Controller { return in.Controller }
