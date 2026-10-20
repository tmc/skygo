package control

import (
	"fmt"
	"sort"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

type physicalCohort struct {
	digest           [32]byte
	allocated, files uint64
	storeDigest      [32]byte
	subjects         []store.ArtifactPhysicalSubject
	local            []physicalLocal
}

// physicalLocal excludes writer activity and cleanup context, neither of which
// changes an interval's identity/allowance cohort. Transitioning/Uncertain are
// independently refused before projection rather than silently excluded.
type physicalLocal struct {
	AttemptID, Binding, Instance, RootID, CandidateID string
	Parent, Root, Candidate                           artifact.FileID
	RootAttempted, CandidateAttempted                 bool
}

func physicalLocalProjection(states []local.WorkspacePhysical) ([]physicalLocal, error) {
	if len(states) > 64 {
		return nil, fmt.Errorf("%w: workspace physical owner bound", ErrUnavailable)
	}
	out := make([]physicalLocal, 0, len(states))
	seen := make(map[string]bool, len(states))
	for _, s := range states {
		if s.AttemptID == "" || seen[s.AttemptID] || s.Transitioning || s.Uncertain || (s.RootAttempted && !physicalFileValid(s.Root)) || (s.CandidateAttempted && !physicalFileValid(s.Candidate)) {
			return nil, fmt.Errorf("%w: workspace physical identity unavailable", ErrUnavailable)
		}
		seen[s.AttemptID] = true
		out = append(out, physicalLocal{s.AttemptID, s.Binding, s.Instance, s.RootID, s.CandidateID, s.Parent, s.Root, s.Candidate, s.RootAttempted, s.CandidateAttempted})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AttemptID < out[j].AttemptID })
	return out, nil
}

func physicalFileValid(id artifact.FileID) bool { return id.Device != 0 && id.Inode != 0 }
func physicalStoreFile(id artifact.FileID) store.ArtifactFile {
	return store.ArtifactFile{Device: id.Device, Inode: id.Inode}
}

// physicalCohort uses one bounded Store View and copied Local mirrors. The caller
// must have registered its invalidatable frame BEFORE calling this function.
// This does no FS calls, ownership adoption or task/native reconciliation.
func (c *Control) physicalCohort() (physicalCohort, error) {
	// The frame was registered before this copied mirror. Flags never cover
	// ordinary same-prepaid-inode IO; errors retain their actual original owner.
	s := c.artifacts
	s.mu.Lock()
	changing := false
	for call := range s.calls {
		if call.uploadTransition || call.fileError != nil {
			changing = true
			break
		}
	}
	s.mu.Unlock()
	if changing {
		return physicalCohort{}, fmt.Errorf("%w: upload physical identity unavailable", ErrUnavailable)
	}
	r, err := c.store.ArtifactReservations()
	if err != nil {
		return physicalCohort{}, err
	}
	var states []local.WorkspacePhysical
	if c.workspace != nil {
		states = c.workspace.backend.PhysicalState()
	}
	return makePhysicalCohort(c.artifacts.root, r, states)
}

func makePhysicalCohort(root artifact.RootFacts, r store.ArtifactReservations, states []local.WorkspacePhysical) (physicalCohort, error) {
	fail := func() (physicalCohort, error) {
		return physicalCohort{}, fmt.Errorf("%w: artifact physical cohort differs", ErrUnavailable)
	}
	if !r.Installation.Enabled || root.ID != r.Installation.ID || root.Volume != r.Installation.Volume || root.Unit != r.Installation.Unit || root.File.Device != root.Volume || !physicalFileValid(root.File) {
		return fail()
	}
	ls, err := physicalLocalProjection(states)
	if err != nil {
		return physicalCohort{}, err
	}
	byID := make(map[string]physicalLocal, len(ls))
	for _, s := range ls {
		if s.Parent != root.File || s.RootID != root.ID {
			return fail()
		}
		byID[s.AttemptID] = s
	}
	subjects := append([]store.ArtifactPhysicalSubject(nil), r.Subjects...)
	objects := make(map[string]store.ArtifactFile, len(subjects))
	for _, subject := range subjects {
		if subject.Kind == "object" {
			objects[subject.ID] = subject.File
		}
	}
	seen := make(map[string]bool, len(subjects))
	for i := range subjects {
		s := &subjects[i]
		if s.Kind != "workspace" {
			if s.Kind != "upload" && s.Kind != "object" {
				return fail()
			}
			// A prepaid upload without Part has not proved an object absent. Its
			// exact identity remains unknown until actual CreatePart publication.
			if s.Kind == "upload" && (s.File.Device == 0 || s.File.Inode == 0) {
				return fail()
			}
			if s.File.Device != 0 && s.File.Device != root.Volume {
				return fail()
			}
			continue
		}
		live, ok := byID[s.ID]
		seen[s.ID] = true
		if s.RootState == 0 {
			if !ok {
				return fail()
			}
			s.Parent = physicalStoreFile(live.Parent)
			if live.RootAttempted {
				s.RootState = store.WorkspaceRootOwned
				s.Root = physicalStoreFile(live.Root)
			} else {
				s.RootState = store.WorkspaceRootNeverCreated
			}
			s.CandidateID = live.CandidateID
			s.CandidateFile = physicalStoreFile(live.Candidate)
		} else {
			if s.Parent != physicalStoreFile(root.File) {
				return fail()
			}
			if s.RootState != store.WorkspaceRootOwned && s.RootState != store.WorkspaceRootNeverCreated {
				return fail()
			}
			if ok {
				if s.Root != physicalStoreFile(live.Root) {
					return fail()
				}
				if s.CandidateID != "" || s.CandidateFile != (store.ArtifactFile{}) {
					// A committed staging prefix remains distinct from a published object.
					if s.CandidateID != live.CandidateID || s.CandidateFile != physicalStoreFile(live.Candidate) {
						return fail()
					}
				} else if physicalFileValid(live.Candidate) {
					// PublishedIndependent requires ZERO GC staging fields. The same actual
					// Local candidate may now be the independently bound immutable object.
					// Keep the zero GC prefix unchanged; only that exact accepted ID+inode
					// relation reconciles the live mirror with the validated object subject.
					file, exists := objects[live.CandidateID]
					if live.CandidateID == "" || !exists || file != physicalStoreFile(live.Candidate) {
						return fail()
					}
				}
			}
		}
	}
	// An owner absent from Store cannot silently disappear from the fence.
	for _, s := range ls {
		if !seen[s.AttemptID] {
			return fail()
		}
	}
	// Store's canonical digest intentionally excludes allowed Ack/revision noise.
	// Effective subjects normalize only an observed live origin; never persist it.
	out := physicalCohort{allocated: r.AllocatedBytes, files: r.Files, storeDigest: r.Digest, subjects: subjects, local: ls}
	projection := struct {
		Root             artifact.RootFacts
		Allocated, Files uint64
		Subjects         []store.ArtifactPhysicalSubject
		Local            []physicalLocal
	}{root, r.AllocatedBytes, r.Files, subjects, ls}
	out.digest, err = physicalHash(projection)
	return out, err
}
