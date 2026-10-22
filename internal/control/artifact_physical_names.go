package control

import (
	"fmt"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/store"
)

type physicalExpectedName struct {
	file  artifact.FileID
	kind  artifact.PhysicalEntryType
	group string
}

// physicalNamesMatch is an interval-consistency check over Files' copied root
// observations, NOT permission to open, adopt, remove or grant any entry. No
// filesystem lookup fills a missing Local/Store identity. Missing required
// observed identities refuse rather than proving positive cleanup.
func physicalNamesMatch(sample artifact.PhysicalSample, cohort physicalCohort) error {
	refuse := func() error { return fmt.Errorf("%w: unexplained artifact root entry", ErrUnavailable) }
	if sample.Status != artifact.PhysicalIntervalComplete || len(sample.RootEntries) > 4096 {
		return refuse()
	}
	names := make(map[string]physicalExpectedName, len(cohort.subjects)*2+1)
	groups := make(map[string]bool, len(cohort.subjects))
	add := func(name string, file artifact.FileID, kind artifact.PhysicalEntryType, group string) bool {
		if old, ok := names[name]; ok {
			return old.file == file && old.kind == kind && old.group == group
		}
		names[name] = physicalExpectedName{file: file, kind: kind, group: group}
		groups[group] = false
		return true
	}
	// Only Files' already verified exact bounded marker is exempt from a Store
	// object name; its descriptor/identity check is part of the actual scan.
	markerSeen := false
	// Store sorts object before upload; establish known upload identities before
	// evaluating its paired publishing object, independent of subject ordering.
	for _, s := range cohort.subjects {
		if s.NotAttempted && (s.Kind != "object" || s.File != (store.ArtifactFile{})) {
			return refuse()
		}
		if s.Kind == "upload" {
			file := artifact.FileID{Device: s.File.Device, Inode: s.File.Inode}
			if !physicalFileValid(file) || !add(s.ID+".part", file, artifact.PhysicalEntryRegular, "file:"+s.ID) || !add(s.ID+".tar", file, artifact.PhysicalEntryRegular, "file:"+s.ID) {
				return refuse()
			}
		}
	}
	for _, s := range cohort.subjects {
		file := artifact.FileID{Device: s.File.Device, Inode: s.File.Inode}
		switch s.Kind {
		case "upload":
			if !physicalFileValid(file) || !add(s.ID+".part", file, artifact.PhysicalEntryRegular, "file:"+s.ID) || !add(s.ID+".tar", file, artifact.PhysicalEntryRegular, "file:"+s.ID) {
				return refuse()
			}
		case "object":
			if s.NotAttempted {
				if s.File != (store.ArtifactFile{}) {
					return refuse()
				}
				if _, upload := names[s.ID+".part"]; upload {
					return refuse()
				}
				// The exact Store reservation says CREATE has not been entered.
				// Add NO filename/group: any actual .part/.tar entry for this ID
				// remains unexplained below, never an adopted unknown inode.
				continue
			}
			if physicalFileValid(file) {
				if !add(s.ID+".tar", file, artifact.PhysicalEntryRegular, "file:"+s.ID) {
					return refuse()
				}
			} else {
				// A publishing upload's same immutable ID/File can supply its observed
				// inode. An unmarked result requires Local's allocated CandidateID seam;
				// absence of an arbitrary object File is not absence of a file on disk.
				if _, ok := names[s.ID+".part"]; !ok {
					known := false
					for _, w := range cohort.subjects {
						if w.Kind == "workspace" && w.CandidateID == s.ID && w.CandidateFile.Device != 0 && w.CandidateFile.Inode != 0 {
							known = true
						}
					}
					// Actual original NotAttempted mirror supplies the prepaid future-result
					// relation. It admits NO basename/zero inode; any observed entry for that
					// ID below remains unexplained until original creation publishes identity.
					for _, w := range cohort.local {
						if w.CandidateID == s.ID && !w.CandidateAttempted && !physicalFileValid(w.Candidate) {
							known = true
						}
					}
					if !known {
						return refuse()
					}
				}
			}
		case "workspace":
			if s.RootState == store.WorkspaceRootOwned {
				root := artifact.FileID{Device: s.Root.Device, Inode: s.Root.Inode}
				if !physicalFileValid(root) || !add("workspace-"+s.ID, root, artifact.PhysicalEntryDirectory, "workspace:"+s.ID) {
					return refuse()
				}
			} else if s.RootState != store.WorkspaceRootNeverCreated {
				return refuse()
			}
			if s.CandidateFile.Device != 0 || s.CandidateFile.Inode != 0 {
				candidate := artifact.FileID{Device: s.CandidateFile.Device, Inode: s.CandidateFile.Inode}
				if s.CandidateID == "" || !physicalFileValid(candidate) || !add(s.CandidateID+".part", candidate, artifact.PhysicalEntryRegular, "file:"+s.CandidateID) || !add(s.CandidateID+".tar", candidate, artifact.PhysicalEntryRegular, "file:"+s.CandidateID) {
					return refuse()
				}
			}
		default:
			return refuse()
		}
	}
	seen := make(map[string]bool, len(sample.RootEntries))
	for _, entry := range sample.RootEntries {
		if seen[entry.Name] {
			return refuse()
		}
		seen[entry.Name] = true
		if entry.Name == ".skygo-artifact-root" {
			if markerSeen || entry.Type != artifact.PhysicalEntryRegular || !physicalFileValid(entry.File) || entry.File.Device != sample.Root.Volume {
				return refuse()
			}
			markerSeen = true
			continue
		}
		expected, ok := names[entry.Name]
		if !ok || expected.file != entry.File || expected.kind != entry.Type {
			return refuse()
		}
		groups[expected.group] = true
	}
	if !markerSeen {
		return refuse()
	}
	for _, seen := range groups {
		if !seen {
			return refuse()
		}
	}
	return nil
}
