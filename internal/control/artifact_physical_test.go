package control

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

// Numeric vectors test admission policy only, not a filesystem measurement or
// positive producer/cleanup receipt. Actual Files/Store tests are separate.
func TestPhysicalGrowthIndependentPoints(t *testing.T) {
	now := time.Now()
	fresh := func() *artifactPhysical {
		return &artifactPhysical{
			points: physicalPoints{databaseBytes: physicalUint(1024), databaseFree: physicalUint(4 << 30), artifactFree: physicalUint(4 << 30), databaseAt: now, databaseFreeAt: now, artifactFreeAt: now},
			cached: &physicalObservation{sample: artifact.PhysicalSample{Status: artifact.PhysicalIntervalComplete, StartedAt: now.Add(-time.Second), EndedAt: now, AllocatedBytes: physicalUint(8192), Inodes: physicalUint(2)}, cohort: physicalCohort{allocated: 8192, files: 2}},
		}
	}
	tests := []struct {
		name   string
		change func(*artifactPhysical)
		want   string
	}{
		{"fresh", func(*artifactPhysical) {}, ""},
		{"db stale", func(p *artifactPhysical) { p.points.databaseAt = now.Add(-6 * time.Second) }, "artifact_points_unavailable"},
		{"db free stale", func(p *artifactPhysical) { p.points.databaseFreeAt = now.Add(-6 * time.Second) }, "artifact_points_unavailable"},
		{"artifact free stale", func(p *artifactPhysical) { p.points.artifactFreeAt = now.Add(-6 * time.Second) }, "artifact_points_unavailable"},
		{"future", func(p *artifactPhysical) { p.points.databaseAt = now.Add(time.Nanosecond) }, "artifact_points_unavailable"},
		{"db threshold", func(p *artifactPhysical) { p.points.databaseBytes = physicalUint(256 << 20) }, "artifact_pressure"},
		{"db volume pressure", func(p *artifactPhysical) { p.points.databaseFree = physicalUint(2<<30 - 1) }, "artifact_pressure"},
		{"artifact volume pressure", func(p *artifactPhysical) { p.points.artifactFree = physicalUint(2<<30 - 1) }, "artifact_pressure"},
		{"partial", func(p *artifactPhysical) { p.cached.sample.Status = artifact.PhysicalPartial }, "artifact_inventory_unavailable"},
		{"long interval", func(p *artifactPhysical) { p.cached.sample.StartedAt = now.Add(-2*time.Minute - time.Nanosecond) }, "artifact_inventory_unavailable"},
		{"bytes exceed", func(p *artifactPhysical) { p.cached.sample.AllocatedBytes = physicalUint(8193) }, "artifact_accounting_hold"},
		{"inode exceed", func(p *artifactPhysical) { p.cached.sample.Inodes = physicalUint(3) }, "artifact_accounting_hold"},
		{"close", func(p *artifactPhysical) { p.wantClose = true }, "artifact_closing"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := fresh()
			tt.change(p)
			if got := physicalGrowthCondition(p, now); got != tt.want {
				t.Fatalf("condition %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPhysicalLocalStructuralProjection(t *testing.T) {
	state := local.WorkspacePhysical{AttemptID: "attempt", Binding: "binding", Instance: "instance", RootID: strings.Repeat("a", 32), Parent: artifact.FileID{Device: 7, Inode: 1}, Root: artifact.FileID{Device: 7, Inode: 2}, RootAttempted: true}
	first, err := physicalLocalProjection([]local.WorkspacePhysical{state})
	if err != nil {
		t.Fatal(err)
	}
	a, err := physicalHash(first)
	if err != nil {
		t.Fatal(err)
	}
	state.WriterActive = true
	state.Cleanup = true
	next, err := physicalLocalProjection([]local.WorkspacePhysical{state})
	if err != nil {
		t.Fatal(err)
	}
	b, err := physicalHash(next)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatal("writer/cleanup context changed structural identity")
	}
	for _, which := range []string{"transition", "uncertain", "unknown-root", "unknown-candidate", "duplicate"} {
		t.Run(which, func(t *testing.T) {
			states := []local.WorkspacePhysical{state}
			switch which {
			case "transition":
				states[0].Transitioning = true
			case "uncertain":
				states[0].Uncertain = true
			case "unknown-root":
				states[0].Root = artifact.FileID{}
			case "unknown-candidate":
				states[0].CandidateAttempted = true
			case "duplicate":
				states = append(states, state)
			}
			if _, err := physicalLocalProjection(states); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("unknown state admitted: %v", err)
			}
		})
	}
}

func TestPhysicalEffectiveRootCohort(t *testing.T) {
	root := artifact.RootFacts{ID: strings.Repeat("a", 32), File: artifact.FileID{Device: 7, Inode: 1}, Unit: 4096, Volume: 7}
	r := store.ArtifactReservations{Installation: store.ArtifactInstallation{Enabled: true, ID: root.ID, Unit: 4096, Volume: 7}, AllocatedBytes: 1 << 20, Files: 100, Subjects: []store.ArtifactPhysicalSubject{{Kind: "workspace", ID: "attempt"}}}
	state := local.WorkspacePhysical{AttemptID: "attempt", Binding: "binding", Instance: "instance", RootID: root.ID, Parent: root.File, Root: artifact.FileID{Device: 7, Inode: 2}, RootAttempted: true}
	first, err := makePhysicalCohort(root, r, []local.WorkspacePhysical{state})
	if err != nil {
		t.Fatal(err)
	}
	marked := r
	marked.Subjects = append([]store.ArtifactPhysicalSubject(nil), r.Subjects...)
	marked.Subjects[0].Parent = store.ArtifactFile{Device: 7, Inode: 1}
	marked.Subjects[0].Root = store.ArtifactFile{Device: 7, Inode: 2}
	marked.Subjects[0].RootState = store.WorkspaceRootOwned
	second, err := makePhysicalCohort(root, marked, []local.WorkspacePhysical{state})
	if err != nil {
		t.Fatal(err)
	}
	if first.digest != second.digest {
		t.Fatal("same actual marked/live identity changed effective fence")
	}
	if _, err := makePhysicalCohort(root, r, nil); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("missing live origin supplied absence: %v", err)
	}
	state.Root.Inode++
	if _, err := makePhysicalCohort(root, marked, []local.WorkspacePhysical{state}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("different original root admitted: %v", err)
	}
}

func TestPhysicalPreInvalidationPreservesOriginalFrame(t *testing.T) {
	c := artifactCallControl()
	p := &c.artifacts.physical
	original := &physicalFrame{serial: 1}
	p.frame = original
	p.cached = &physicalObservation{}
	c.physicalChanged()
	if p.frame != original || !original.invalidated || p.cached != nil || !p.wantSample {
		t.Fatal("pre-invalidation discarded original frame or retained cache")
	}
	c.requestPhysicalClose()
	c.physicalChanged()
	if !p.wantClose || p.frame != original || c.cachedLinkGrowthCheck() == nil {
		t.Fatal("new invalidation erased terminal close request")
	}
}

func TestPhysicalCacheConcurrentInvalidation(t *testing.T) {
	c := artifactCallControl()
	var group sync.WaitGroup
	for range 4 {
		group.Add(1)
		go func() {
			defer group.Done()
			for range 100 {
				c.physicalChanged()
				_ = c.cachedLinkGrowthCheck()
				_ = c.physicalRegistered()
			}
		}()
	}
	group.Wait()
	c.requestPhysicalClose()
	if !errors.Is(c.cachedLinkGrowthCheck(), artifact.ErrFilesBusy) {
		t.Fatal("closing cache admitted growth")
	}
}

func TestPhysicalRootNamesUseActualIdentityAndType(t *testing.T) {
	marker := artifact.PhysicalEntry{Name: ".skygo-artifact-root", File: artifact.FileID{Device: 7, Inode: 2}, Type: artifact.PhysicalEntryRegular}
	object := artifact.PhysicalEntry{Name: "object.tar", File: artifact.FileID{Device: 7, Inode: 3}, Type: artifact.PhysicalEntryRegular}
	sample := artifact.PhysicalSample{Status: artifact.PhysicalIntervalComplete, Root: artifact.RootFacts{Volume: 7}, RootEntries: []artifact.PhysicalEntry{marker, object}}
	cohort := physicalCohort{subjects: []store.ArtifactPhysicalSubject{{Kind: "object", ID: "object", File: store.ArtifactFile{Device: 7, Inode: 3}}}}
	if err := physicalNamesMatch(sample, cohort); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		edit func(*artifact.PhysicalSample)
	}{
		{"unexplained", func(s *artifact.PhysicalSample) {
			s.RootEntries = append(s.RootEntries, artifact.PhysicalEntry{Name: "workspace-unknown", File: artifact.FileID{Device: 7, Inode: 4}, Type: artifact.PhysicalEntryDirectory})
		}},
		{"wrong inode", func(s *artifact.PhysicalSample) { s.RootEntries[1].File.Inode++ }},
		{"symlink", func(s *artifact.PhysicalSample) { s.RootEntries[1].Type = artifact.PhysicalEntryOther }},
		{"missing", func(s *artifact.PhysicalSample) { s.RootEntries = s.RootEntries[:1] }},
		{"duplicate", func(s *artifact.PhysicalSample) { s.RootEntries = append(s.RootEntries, s.RootEntries[1]) }},
		{"partial", func(s *artifact.PhysicalSample) { s.Status = artifact.PhysicalPartial }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			copy := sample
			copy.RootEntries = append([]artifact.PhysicalEntry(nil), sample.RootEntries...)
			tt.edit(&copy)
			if !errors.Is(physicalNamesMatch(copy, cohort), ErrUnavailable) {
				t.Fatal("unexplained namespace admitted")
			}
		})
	}
}

// These are observation-projection values, not fabricated positive Writer,
// publication or kernel-close receipts. Actual Store validates the object/GC
// relation before ArtifactReservations can supply these copied subjects.
func TestPhysicalMarkedPublishedResultUsesBoundObject(t *testing.T) {
	root := artifact.RootFacts{ID: strings.Repeat("a", 32), File: artifact.FileID{Device: 7, Inode: 1}, Unit: 4096, Volume: 7}
	resultID := strings.Repeat("b", 32)
	r := store.ArtifactReservations{Installation: store.ArtifactInstallation{Enabled: true, ID: root.ID, Unit: 4096, Volume: 7}, AllocatedBytes: 1 << 20, Files: 100, Subjects: []store.ArtifactPhysicalSubject{
		{Kind: "object", ID: resultID, File: store.ArtifactFile{Device: 7, Inode: 3}},
		{Kind: "workspace", ID: "attempt", Parent: store.ArtifactFile{Device: 7, Inode: 1}, Root: store.ArtifactFile{Device: 7, Inode: 2}, RootState: store.WorkspaceRootOwned},
	}}
	live := local.WorkspacePhysical{AttemptID: "attempt", Binding: "binding", Instance: "instance", RootID: root.ID, CandidateID: resultID, Parent: root.File, Root: artifact.FileID{Device: 7, Inode: 2}, Candidate: artifact.FileID{Device: 7, Inode: 3}, RootAttempted: true, CandidateAttempted: true}
	cohort, err := makePhysicalCohort(root, r, []local.WorkspacePhysical{live})
	if err != nil {
		t.Fatal(err)
	}
	// PublishedIndependent's GC staging prefix must stay ZERO. The independent
	// object already supplies the .tar identity; no staging receipt is invented.
	for _, s := range cohort.subjects {
		if s.Kind == "workspace" && (s.CandidateID != "" || s.CandidateFile != (store.ArtifactFile{})) {
			t.Fatal("published object fabricated GC staging prefix")
		}
	}
	sample := artifact.PhysicalSample{Status: artifact.PhysicalIntervalComplete, Root: root, RootEntries: []artifact.PhysicalEntry{
		{Name: ".skygo-artifact-root", File: artifact.FileID{Device: 7, Inode: 4}, Type: artifact.PhysicalEntryRegular},
		{Name: "workspace-attempt", File: live.Root, Type: artifact.PhysicalEntryDirectory},
		{Name: resultID + ".tar", File: live.Candidate, Type: artifact.PhysicalEntryRegular},
	}}
	if err := physicalNamesMatch(sample, cohort); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		change func(*store.ArtifactReservations, *local.WorkspacePhysical)
	}{
		{"wrong accepted result id", func(_ *store.ArtifactReservations, l *local.WorkspacePhysical) {
			l.CandidateID = strings.Repeat("c", 32)
		}},
		{"wrong original inode", func(_ *store.ArtifactReservations, l *local.WorkspacePhysical) { l.Candidate.Inode++ }},
		{"unknown object inode", func(r *store.ArtifactReservations, _ *local.WorkspacePhysical) {
			r.Subjects[0].File = store.ArtifactFile{}
		}},
		{"missing object", func(r *store.ArtifactReservations, _ *local.WorkspacePhysical) { r.Subjects = r.Subjects[1:] }},
		{"wrong object id", func(r *store.ArtifactReservations, _ *local.WorkspacePhysical) {
			r.Subjects[0].ID = strings.Repeat("d", 32)
		}},
		{"wrong object inode", func(r *store.ArtifactReservations, _ *local.WorkspacePhysical) { r.Subjects[0].File.Inode++ }},
		{"foreign staging id", func(r *store.ArtifactReservations, _ *local.WorkspacePhysical) {
			r.Subjects[1].CandidateID = strings.Repeat("e", 32)
			r.Subjects[1].CandidateFile = store.ArtifactFile{Device: 7, Inode: 3}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			copy := r
			copy.Subjects = append([]store.ArtifactPhysicalSubject(nil), r.Subjects...)
			state := live
			tt.change(&copy, &state)
			if _, err := makePhysicalCohort(root, copy, []local.WorkspacePhysical{state}); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("unbound result reconciled: %v", err)
			}
		})
	}
}

func TestPhysicalMarkedStagingAndUncreatedCandidates(t *testing.T) {
	root := artifact.RootFacts{ID: strings.Repeat("a", 32), File: artifact.FileID{Device: 7, Inode: 1}, Unit: 4096, Volume: 7}
	id := strings.Repeat("b", 32)
	tests := []struct {
		name      string
		candidate artifact.FileID
		attempted bool
		stagingID string
		noOutput  bool
	}{
		{"closed unpublished", artifact.FileID{Device: 7, Inode: 3}, true, id, false},
		{"never attempted", artifact.FileID{}, false, "", false},
		{"no output", artifact.FileID{}, false, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultID := id
			if tt.noOutput {
				resultID = ""
			}
			state := local.WorkspacePhysical{AttemptID: "attempt", Binding: "binding", Instance: "instance", RootID: root.ID, CandidateID: resultID, Parent: root.File, Root: artifact.FileID{Device: 7, Inode: 2}, Candidate: tt.candidate, RootAttempted: true, CandidateAttempted: tt.attempted}
			r := store.ArtifactReservations{Installation: store.ArtifactInstallation{Enabled: true, ID: root.ID, Unit: 4096, Volume: 7}, AllocatedBytes: 1 << 20, Files: 100, Subjects: []store.ArtifactPhysicalSubject{{Kind: "workspace", ID: "attempt", Parent: store.ArtifactFile{Device: 7, Inode: 1}, Root: store.ArtifactFile{Device: 7, Inode: 2}, RootState: store.WorkspaceRootOwned, CandidateID: tt.stagingID, CandidateFile: physicalStoreFile(tt.candidate)}}}
			if !tt.noOutput {
				r.Subjects = append(r.Subjects, store.ArtifactPhysicalSubject{Kind: "object", ID: id})
			}
			if _, err := makePhysicalCohort(root, r, []local.WorkspacePhysical{state}); err != nil {
				t.Fatal(err)
			}
			if tt.attempted {
				r.Subjects[0].CandidateFile.Inode++
				if _, err := makePhysicalCohort(root, r, []local.WorkspacePhysical{state}); !errors.Is(err, ErrUnavailable) {
					t.Fatalf("different staging prefix reconciled: %v", err)
				}
			}
		})
	}
}
