package store

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestArtifactWorkspaceAllocation(t *testing.T) {
	ref := ArtifactRef{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Size: artifactMaxSize}
	for _, tt := range []struct {
		name                                               string
		unit, expanded, regular, directories, bytes, files uint64
	}{
		{"empty", 4096, 0, 0, 0, (32 << 20) + 2561*4096, 2561},
		{"one input", 4096, 1, 1, 0, (32 << 20) + 2563*4096, 2562},
		{"maximum", 65536, 32 << 20, 2048, 512, (64 << 20) + 5121*65536, 5121},
		{"small unit", 512, 513, 2, 1, (32 << 20) + 2566*512, 2564},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m := ArtifactManifest{Reference: ref, ExpandedBytes: tt.expanded, RegularFiles: tt.regular, Directories: tt.directories}
			allocated, files, err := artifactWorkspaceAllocation(m, tt.unit)
			if err != nil || allocated != tt.bytes || files != tt.files {
				t.Fatalf("allocation=(%d,%d,%v), want (%d,%d)", allocated, files, err, tt.bytes, tt.files)
			}
		})
	}
	valid := ArtifactManifest{Reference: ref, ExpandedBytes: 1, RegularFiles: 1}
	for _, tt := range []struct {
		name string
		m    ArtifactManifest
		unit uint64
	}{
		{"zero unit", valid, 0},
		{"unaligned unit", valid, 513},
		{"overflow unit", valid, ^uint64(0)},
		{"excess files", ArtifactManifest{Reference: ref, RegularFiles: 2049}, 4096},
		{"excess directories", ArtifactManifest{Reference: ref, Directories: 513}, 4096},
		{"excess payload", ArtifactManifest{Reference: ref, RegularFiles: 1, ExpandedBytes: (32 << 20) + 1}, 4096},
	} {
		t.Run(tt.name, func(t *testing.T) {
			allocated, files, err := artifactWorkspaceAllocation(tt.m, tt.unit)
			if err == nil || allocated != 0 || files != 0 {
				t.Fatalf("invalid allocation=(%d,%d,%v)", allocated, files, err)
			}
		})
	}
}

// Actual Store transactions with modeled closure/removal receipts. No physical
// deletion, native writer settlement or kernel quota is claimed here.
func TestArtifactWorkspaceWorkingAllowanceUntilCleanup(t *testing.T) {
	for _, outputs := range []bool{false, true} {
		t.Run(map[bool]string{false: "no outputs", true: "published output"}[outputs], func(t *testing.T) {
			s, j, _ := artifactRegressionWorkspace(t, false, outputs)
			before := reservationSnapshot(t, s)
			if before.Files != uint64(2565)+map[bool]uint64{false: 0, true: 1}[outputs] {
				t.Fatalf("working tree not prepaid before launch: %+v", before)
			}
			artifactRegressionComplete(t, s, j, false)
			marked := reservationSnapshot(t, s)
			if marked.Files != before.Files {
				t.Fatal("completion credited working tree before cleanup", before, marked)
			}
			work := maintenanceWork(t, s, j.ID, j.CurrentAttemptID())
			bad := work
			bad.Digest[0] ^= 1
			unchanged := artifactDatabaseBytes(t, s)
			if err := s.FinishWorkspaceCleanup(bad, WorkspaceCleanupReceipt{Work: bad}); err == nil {
				t.Fatal("changed cleanup work credited reservation")
			}
			if !bytes.Equal(unchanged, artifactDatabaseBytes(t, s)) {
				t.Fatal("refused cleanup changed database")
			}
			if err := s.FinishWorkspaceCleanup(work, WorkspaceCleanupReceipt{Work: work}); err != nil {
				t.Fatal(err)
			}
			after := reservationSnapshot(t, s)
			// Extracted1byte/1file+root and additional32MiB/2560entries leave
			// together, only after this exact root work's completion. A published
			// candidate remains an independently charged immutable object.
			if marked.Files-after.Files != 2562 || marked.AllocatedBytes-after.AllocatedBytes != (32<<20)+2563*4096 {
				t.Fatal("wrong exact workspace cleanup credit", marked, after)
			}
			if after.Files != uint64(3)+map[bool]uint64{false: 0, true: 1}[outputs] {
				t.Fatal("cleanup released input/result archive charge", after)
			}
			if err := s.db.View(validateSnapshot); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestArtifactWorkspaceWorkingAllowancePreservesWorkspaceQuota(t *testing.T) {
	var before, after artifactUsage
	before[aWorkspaces], after[aWorkspaces] = 64, 65
	// Physical reservations cannot enlarge the existing64-workspace quota.
	after[aFiles], after[aAllocatedBytes] = 65*5121, 65*((64<<20)+5121*65536)
	var capacity *ArtifactCapacityError
	if err := artifactAdmission(before, after, false); !errors.As(err, &capacity) || capacity.Domain != "workspaces" || capacity.Limit != 64 {
		t.Fatalf("workspace quota changed: %v", err)
	}
}
