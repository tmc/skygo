//go:build linux || darwin

package control

import (
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
)

// The reservation comes from actual Store acceptance; measured numbers below
// are explicit policy vectors, NOT a physical scan or a native writer receipt.
// TestControlWorkspacePublishesCompletedMain separately exercises real setup,
// main-created files and publication with the default maintenance/growth path.
func TestWorkspaceWorkingAllowanceGrowthAndExcess(t *testing.T) {
	c, _ := artifactResultFixture(t, false)
	input := workspaceUploadedInput(t, c)
	target := c.targets[0]
	j, err := c.store.SubmitWorkspace(testActor(), nil, "working-allowance", backend.Task{Argv: []string{"never-started"}, Workdir: &input, Outputs: []string{"out"}}, target.Name, target.Binding, backend.RemoteOutput, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, start, err := c.store.BeginAttempt(j.ID, j.CurrentAttemptID(), 0, time.Now()); err != nil || !start {
		t.Fatal(start, err)
	}
	r, err := c.store.ArtifactReservations()
	if err != nil {
		t.Fatal(err)
	}
	if r.Files != 2566 {
		t.Fatalf("input/result/workspace reservation files=%d", r.Files)
	}
	now := time.Now()
	for _, tt := range []struct {
		name         string
		bytes, files uint64
		want         string
	}{
		{"observed completed main", 32768, 8, ""},
		{"exact prepaid limit", r.AllocatedBytes, r.Files, ""},
		{"one excess inode", 32768, r.Files + 1, "artifact_accounting_hold"},
		{"one excess byte", r.AllocatedBytes + 1, 8, "artifact_accounting_hold"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := &artifactPhysical{
				points: physicalPoints{databaseBytes: physicalUint(65536), databaseFree: physicalUint(4 << 30), artifactFree: physicalUint(4 << 30), databaseAt: now, databaseFreeAt: now, artifactFreeAt: now},
				cached: &physicalObservation{sample: artifact.PhysicalSample{Status: artifact.PhysicalIntervalComplete, StartedAt: now.Add(-time.Millisecond), EndedAt: now, AllocatedBytes: physicalUint(tt.bytes), Inodes: physicalUint(tt.files)}, cohort: physicalCohort{allocated: r.AllocatedBytes, files: r.Files}},
			}
			if got := physicalGrowthCondition(p, now); got != tt.want {
				t.Fatalf("condition=%q want=%q", got, tt.want)
			}
		})
	}
}
