package control

import (
	"fmt"
	"strings"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

// workspacePrepared is the SAME original target worker's returned observation
// and physical preorigin. Neither readiness nor a DTO alone supplies ownership.
type workspacePrepared struct {
	observation local.WorkspaceObservation
	origin      local.WorkspacePhysicalOrigin
}

func workspaceStoreProducer(p local.ServerLifetime) (store.OriginalServerLifetime, error) {
	if p != (local.ServerLifetime{}) && (!jobName.MatchString(p.Host) || p.Host == strings.Repeat("0", 32) || !jobName.MatchString(p.Boot) || p.Boot == strings.Repeat("0", 32) || p.NamespaceInode == 0 || p.PID == 0 || p.PID > 1<<31-1) {
		return store.OriginalServerLifetime{}, fmt.Errorf("%w: invalid original server lifetime", ErrInvalid)
	}
	return store.OriginalServerLifetime{Host: strings.Clone(p.Host), Boot: strings.Clone(p.Boot), NamespaceDevice: p.NamespaceDevice, NamespaceInode: p.NamespaceInode, PID: p.PID, StartTicks: p.StartTicks}, nil
}

func workspaceLocalProducer(p store.OriginalServerLifetime) local.ServerLifetime {
	return local.ServerLifetime{Host: p.Host, Boot: p.Boot, NamespaceDevice: p.NamespaceDevice, NamespaceInode: p.NamespaceInode, PID: p.PID, StartTicks: p.StartTicks}
}

func retainWorkspaceObservation(o local.WorkspaceObservation) local.WorkspaceObservation {
	if o.Status.ExitCode != nil {
		code := *o.Status.ExitCode
		o.Status.ExitCode = &code
	}
	if o.Writer != nil {
		writer := *o.Writer
		writer.Execution.Task = backend.Task{}
		o.Writer = &writer
	}
	if o.Candidate != nil {
		candidate := *o.Candidate
		o.Candidate = &candidate
	}
	return o
}

func retainWorkspaceOrigin(o local.WorkspacePhysicalOrigin) local.WorkspacePhysicalOrigin {
	if o.Prefix != nil {
		prefix := *o.Prefix
		o.Prefix = &prefix
	}
	return o
}

func workspaceStoreOrigin(o local.WorkspacePhysicalOrigin) (store.WorkspacePhysicalOrigin, error) {
	producer, err := workspaceStoreProducer(o.Producer)
	if err != nil {
		return store.WorkspacePhysicalOrigin{}, err
	}
	out := store.WorkspacePhysicalOrigin{Parent: physicalStoreFile(o.Parent), Root: physicalStoreFile(o.Root), Producer: producer}
	switch o.RootState {
	case local.WorkspaceRootNeverCreated:
		out.RootState = store.WorkspaceRootNeverCreated
	case local.WorkspaceRootOwned:
		out.RootState = store.WorkspaceRootOwned
	default:
		return store.WorkspacePhysicalOrigin{}, ErrUnavailable
	}
	switch o.CandidateState {
	case local.WorkspacePhysicalNoOutputs:
		out.CandidateState = store.WorkspacePhysicalNoOutputs
	case local.WorkspacePhysicalNeverAttempted:
		out.CandidateState = store.WorkspacePhysicalNeverAttempted
	case local.WorkspacePhysicalClosedPrefix:
		out.CandidateState = store.WorkspacePhysicalClosedPrefix
	case local.WorkspacePhysicalSealed:
		out.CandidateState = store.WorkspacePhysicalSealed
	default:
		return store.WorkspacePhysicalOrigin{}, ErrUnavailable
	}
	if o.CandidateState == local.WorkspacePhysicalClosedPrefix {
		if o.Prefix == nil {
			return store.WorkspacePhysicalOrigin{}, ErrUnavailable
		}
		p := o.Prefix
		out.Prefix = &store.WorkspaceCandidatePrefix{ID: p.ID, File: physicalStoreFile(p.File), Size: p.Size, SHA256: p.SHA256}
	} else if o.Prefix != nil {
		return store.WorkspacePhysicalOrigin{}, ErrUnavailable
	}
	return out, nil
}

// workspaceLocalIntent maps only the ACTUAL atomic Store result. It allocates
// no final Subject, hash, operation ID, identity or cleanup permission. The Local
// original owner independently checks this exact acknowledged value against its
// previously prepared facts; this DTO itself authenticates no transaction.
func workspaceLocalIntent(work store.WorkspaceCleanupWork) (local.WorkspaceRootIntent, error) {
	o := work.Origin
	s := o.Subject
	input, err := artifactPortableRef(s.InputRef)
	if err != nil {
		return local.WorkspaceRootIntent{}, err
	}
	var result artifact.Ref
	if s.ResultRef != (store.ArtifactRef{}) {
		result, err = artifactPortableRef(s.ResultRef)
		if err != nil {
			return local.WorkspaceRootIntent{}, err
		}
	}
	producer := workspaceLocalProducer(o.Producer)
	if _, err := workspaceStoreProducer(producer); err != nil {
		return local.WorkspaceRootIntent{}, err
	}
	origin := local.WorkspaceCleanupOrigin{
		Subject: local.WorkspaceCleanupIdentity{Owner: s.Owner, JobID: s.JobID, AttemptID: s.AttemptID, Binding: s.Binding, Instance: s.Instance, RootID: s.RootID, TaskDigest: s.TaskDigest, CompletionDigest: s.CompletionDigest, WriterReceipt: s.WriterReceipt, InputRef: input, ResultID: s.ResultID, ResultState: s.ResultState, ResultRef: result, ResultFile: artifact.FileID{Device: s.ResultFile.Device, Inode: s.ResultFile.Inode}},
		Parent:  artifact.FileID{Device: o.Parent.Device, Inode: o.Parent.Inode}, Root: artifact.FileID{Device: o.Root.Device, Inode: o.Root.Inode}, CandidateID: o.CandidateID, CandidateFile: artifact.FileID{Device: o.CandidateFile.Device, Inode: o.CandidateFile.Inode}, CandidateSize: o.CandidateSize, CandidateSHA256: o.CandidateSHA256, Producer: producer,
	}
	switch o.RootState {
	case store.WorkspaceRootNeverCreated:
		origin.RootState = local.WorkspaceRootNeverCreated
	case store.WorkspaceRootOwned:
		origin.RootState = local.WorkspaceRootOwned
	default:
		return local.WorkspaceRootIntent{}, ErrUnavailable
	}
	switch o.Disposition {
	case store.WorkspaceNoOutputs:
		origin.Disposition = local.WorkspaceNoOutputs
	case store.WorkspaceCandidateNeverCreated:
		origin.Disposition = local.WorkspaceCandidateNeverCreated
	case store.WorkspaceCandidateClosedUnpublished:
		origin.Disposition = local.WorkspaceCandidateClosedUnpublished
	case store.WorkspaceResultPublishedIndependent:
		origin.Disposition = local.WorkspaceResultPublishedIndependent
	default:
		return local.WorkspaceRootIntent{}, ErrUnavailable
	}
	return local.WorkspaceRootIntent{ID: work.ID, Revision: work.Revision, Digest: work.Digest, Origin: origin}, nil
}
