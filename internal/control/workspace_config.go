package control

import (
	"fmt"
	"os"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

// LocalWorkspaceConfig binds the one concrete local workspace sequencer.
// Backend must be the exact configured target pointer. Roots and Files remain
// caller-owned until every original owner and later root retirement has joined.
type LocalWorkspaceConfig struct {
	Target, Binding string
	Backend         *local.Backend
	Roots           *os.Root
}

type workspaceTarget struct {
	name, binding string
	backend       *local.Backend
}

func (w *workspaceTarget) matches(target backend.Target) bool {
	return w != nil && w.name == target.Name && w.binding == target.Binding && target.Backend == w.backend && w.backend.SupportsWorkspace()
}

func (c *Control) configureWorkspace(config *LocalWorkspaceConfig) error {
	if config == nil {
		return nil
	}
	if c.artifacts == nil || config.Backend == nil || config.Roots == nil {
		return fmt.Errorf("workspace requires installed artifacts, local backend and root")
	}
	binding, err := backend.LocalBinding(c.store.Owner())
	if err != nil {
		return err
	}
	if config.Binding != binding {
		return fmt.Errorf("workspace binding differs from installation")
	}
	target, ok := c.target(config.Target)
	if !ok || target.Binding != binding || target.Backend != config.Backend {
		return fmt.Errorf("workspace target differs from concrete local backend")
	}
	if err := config.Backend.ConfigureWorkspace(local.WorkspaceConfig{
		ServerLifetime: workspaceLocalProducer(c.producer),
		Files:          c.artifacts.files, Roots: config.Roots, InstallationID: c.store.Owner(), RootID: c.artifacts.root.ID,
		CheckGrowth: c.cachedLinkGrowthCheck, PhysicalChanged: c.physicalChanged,
		TryCollectionIO: func() (func(), bool) {
			owner, err := c.tryArtifactIO()
			if err != nil {
				return nil, false
			}
			return func() { owner.finish(nil) }, true
		},
	}); err != nil {
		return fmt.Errorf("configure local workspace: %w", err)
	}
	c.workspace = &workspaceTarget{name: target.Name, binding: binding, backend: config.Backend}
	return nil
}

// ArtifactsEnabled reports an installed marker-verified artifact service.
// It is not readiness, free capacity, a grant, or target workspace support.
func (c *Control) ArtifactsEnabled() bool { return c != nil && c.artifacts != nil }

// WorkspaceTargetEnabled reports the actual configured local route's current
// platform support. It grants no execution and does no filesystem I/O.
func (c *Control) WorkspaceTargetEnabled(name, binding string) bool {
	if c == nil {
		return false
	}
	target, ok := c.target(name)
	return ok && target.Binding == binding && c.workspace.matches(target)
}

func (c *Control) artifactAdmissionStopped() bool {
	if c.artifacts == nil {
		return true
	}
	c.artifacts.mu.Lock()
	defer c.artifacts.mu.Unlock()
	return c.artifacts.closing
}

// SubmitWorkspace accepts or replays a workspace task without a request domain.
// Enabled request retention requires an explicit domain on a new acceptance.
func (c *Control) SubmitWorkspace(actor auth.Actor, requestID string, task backend.Task) (backend.Job, error) {
	return c.submit(actor, nil, requestID, task, true)
}

// SubmitWorkspaceInDomain accepts or replays the exact supplied request domain.
// Durable replay precedes current placement and artifact availability checks.
func (c *Control) SubmitWorkspaceInDomain(actor auth.Actor, domain store.RequestDomain, requestID string, task backend.Task) (backend.Job, error) {
	return c.submit(actor, &domain, requestID, task, true)
}
