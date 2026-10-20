package control

import (
	"fmt"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

// Config selects the concrete optional services. Zero installs none.
// Artifacts must refer to the actual marker-verified root matching Store.
type Config struct {
	// LocalBackend is the one server-owned built-in Local pointer. Additional
	// targets remain borrowed; preparation never closes or cancels remote work.
	LocalBackend *local.Backend
	// ServerLifetime is the single original known or positively settled unavailable
	// self capture. Zero does not settle a ServerLifetimeCloseError; the server must
	// retain that original error/resource frame independently. Never capture per job.
	ServerLifetime local.ServerLifetime
	LocalOutput    LocalOutputConfig
	// RecoverStoppedArtifactReaders requires a qualified same-domain kernel and
	// no surviving descriptor copy or use from ALL historical producers. It is
	// restricted to isolated artifact-only/no-child/no-transfer deployments.
	RecoverStoppedArtifactReaders bool
	Artifacts                     *artifact.Files
	LocalWorkspace                *LocalWorkspaceConfig
}

func newArtifactService(s *store.Store, files *artifact.Files) (*artifactService, error) {
	installation, err := s.ArtifactInstallation()
	if err != nil {
		return nil, err
	}
	if !installation.Enabled {
		return nil, fmt.Errorf("artifact metadata not installed")
	}
	facts, err := files.RootFacts()
	if err != nil {
		return nil, fmt.Errorf("artifact root facts: %w", err)
	}
	if !jobName.MatchString(facts.ID) || facts.ID != installation.ID || facts.Unit != installation.Unit || facts.Volume != installation.Volume || facts.File.Device == 0 || facts.File.Inode == 0 || facts.Volume != facts.File.Device || facts.Unit < 512 || facts.Unit > 65536 || facts.Unit&(facts.Unit-1) != 0 {
		return nil, fmt.Errorf("artifact root differs from installed identity")
	}
	return &artifactService{physical: artifactPhysical{wantSample: true}, files: files, root: facts, calls: make(map[*ArtifactCall]struct{}), uploads: make(map[string]*ArtifactCall), downloads: make(map[*ArtifactDownload]struct{}), workspaceIO: make(map[*artifactIO]struct{}), changed: make(chan struct{}), terminal: make(chan struct{})}, nil
}
