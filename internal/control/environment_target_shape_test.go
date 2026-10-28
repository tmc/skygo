package control

import (
	"testing"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

// These MODELED vectors test the maintenance request union only. They do
// not dispatch work, touch Files or Store, or observe any native lifetime.
func TestEnvironmentTargetFactsRefuseMaintenanceShapes(t *testing.T) {
	recovery, closure := artifactRecoveryVector()
	closing := artifactCallControl()
	deleting, work := artifactDeleteVector()
	tests := []struct {
		name    string
		request maintenanceRequest
		valid   func(maintenanceRequest) bool
	}{
		{"physical", maintenanceRequest{physicalOwner: &Control{}}, physicalRequestShape},
		{"workspace retirement", maintenanceRequest{sequence: 1,
			workspaceRetirement: &workspaceRetirementRequest{
				original: &workspacePending{}, control: &Control{}, backend: &local.Backend{},
				owner: &artifactIO{}, step: workspaceRetirementRoot,
			}}, workspaceRetirementShape},
		{"reader recovery", maintenanceRequest{sequence: 1,
			readerRecovery: &artifactReaderRecoveryRequest{
				control: recovery, current: workspaceLocalProducer(recovery.producer),
				closure: closure, owner: &artifactIO{service: recovery.artifacts},
			}}, artifactReaderRecoveryShape},
		{"download close", maintenanceRequest{sequence: 1,
			downloadClose: &artifactDownloadClose{
				download: &ArtifactDownload{control: closing},
				closure:  store.ArtifactDownloadClosure{LeaseID: "lease", Revision: 1}, neverOpened: true,
			}, closeOwner: &artifactIO{service: closing.artifacts}}, artifactDownloadCloseShape},
		{"artifact delete", maintenanceRequest{sequence: 1,
			artifactDelete: &artifactDeleteRequest{control: deleting, work: work,
				owner: &artifactIO{service: deleting.artifacts}}}, artifactDeleteShape},
	}
	facts := []struct {
		name string
		set  func(*backend.Target)
	}{
		{"environments", func(t *backend.Target) { t.SupportsPersistentEnvironments = true }},
		{"environment artifacts", func(t *backend.Target) { t.SupportsEnvironmentArtifacts = true }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The zero-target twin must pass before either new fact is set;
			// another failed precondition cannot mask the field being tested.
			if !tt.valid(tt.request) {
				t.Fatal("valid zero-target request was refused")
			}
			for _, fact := range facts {
				t.Run(fact.name, func(t *testing.T) {
					r := tt.request
					fact.set(&r.target)
					if tt.valid(r) {
						t.Fatal("nonzero target fact accepted in maintenance request")
					}
				})
			}
		})
	}
}
