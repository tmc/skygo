package store

import (
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
)

// This source example is compile-only: a runnable upload example must use the
// separately owned concrete Files implementation. Original I/O/HTTP operation
// ownership and error-path settlement remain the coordinator’s responsibility.
// This is Store call ordering, not a replacement coordinator. Never manufacture file
// or archive receipts to make an Output comment pass. All examples are UNRUN.
func ExampleStore_BeginArtifactUpload() {
	// Files functions return only after required I/O owners settle. Fresh
	// reacquires actual Engine authority after every blocking I/O boundary.
	upload := func(s *Store, fresh func() (auth.Actor, error), sha string, size uint64, create func(ArtifactRef) (ArtifactFile, error), writeSynced func(ArtifactUpload, ArtifactFile) error, seal func(ArtifactUpload, ArtifactFile) (ArtifactManifest, error)) (ArtifactRef, error) {
		actor, err := fresh()
		if err != nil {
			return ArtifactRef{}, err
		}
		u, err := s.BeginArtifactUpload(actor, sha, size)
		if err != nil {
			return ArtifactRef{}, err
		}
		file, err := create(u.Reference)
		if err != nil {
			return ArtifactRef{}, err
		}
		actor, err = fresh()
		if err != nil {
			return ArtifactRef{}, err
		}
		u, err = s.ArtifactPartCreated(actor, ArtifactCursor{u.Reference.ID, u.Revision}, file)
		if err != nil {
			return ArtifactRef{}, err
		}
		// A real caller repeats this block per <=64KiB chunk. This example
		// requires size<=64KiB, with one exact-offset synced write.
		if err := writeSynced(u, file); err != nil {
			return ArtifactRef{}, err
		}
		actor, err = fresh()
		if err != nil {
			return ArtifactRef{}, err
		}
		u, err = s.ArtifactChunkSynced(actor, ArtifactCursor{u.Reference.ID, u.Revision}, file, 0, size, sha)
		if err != nil {
			return ArtifactRef{}, err
		}
		u, err = s.BeginArtifactFinalize(actor, ArtifactCursor{u.Reference.ID, u.Revision})
		if err != nil {
			return ArtifactRef{}, err
		}
		manifest, err := seal(u, file)
		if err != nil {
			return ArtifactRef{}, err
		}
		actor, err = fresh()
		if err != nil {
			return ArtifactRef{}, err
		}
		return s.CompleteArtifactUpload(actor, ArtifactCursor{u.Reference.ID, u.Revision}, u.Reference, file, manifest)
	}
	_ = upload
}

func ExampleStore_GetArtifactUploadFile() {
	resume := func(s *Store, actor auth.Actor, id string) (ArtifactUpload, ArtifactFile, error) {
		u, err := s.GetArtifactUpload(actor, id)
		if err != nil {
			return ArtifactUpload{}, ArtifactFile{}, err
		}
		// The cursor snapshot is one View. A zero creating File stays held;
		// no caller may adopt a file merely because its path already exists.
		return s.GetArtifactUploadFile(actor, ArtifactCursor{id, u.Revision})
	}
	cancel := func(s *Store, actor auth.Actor, c ArtifactCursor) (string, error) {
		// This marks intent only. The concrete maintenance owner must perform
		// exact owned unlink and parent sync before charging any release.
		return s.CancelArtifactUpload(actor, c)
	}
	_ = resume
	_ = cancel
}

func ExampleStore_BeginArtifactDownload() {
	download := func(s *Store, actor auth.Actor, ref ArtifactRef, producer OriginalServerLifetime, readAndJoin func(ArtifactLease) error, closeFD func() error) error {
		// producer is captured from this original server outside Store. An
		// unavailable zero value permits live transfer, but not stopped recovery.
		lease, err := s.BeginArtifactDownload(actor, ref, producer)
		if err != nil {
			return err
		}
		// Retain the original file/root/lease identity across later revocation.
		closure := ArtifactDownloadClosure{Owner: actor.Principal(), LeaseID: lease.ID, Revision: lease.Revision, Ref: lease.Reference, File: lease.File, RootID: lease.RootID, Producer: lease.Producer}
		_, readErr := s.GetArtifactDownload(actor, lease.ID)
		if readErr == nil {
			readErr = readAndJoin(lease)
		}
		// The real owner always joins actual I/O and HTTP owners, including on
		// errors. This sample callback must uphold that ownership requirement.
		closing, err := s.MarkArtifactDownloadClosing(closure)
		if err != nil {
			return err
		}
		if err := closeFD(); err != nil {
			return err
		}
		if err := s.FinishArtifactDownload(closing, time.Now().UTC()); err != nil {
			return err
		}
		return readErr
	}
	_ = download
}

func ExampleStore_RemoveArtifact() {
	remove := func(s *Store, actor auth.Actor, ref ArtifactRef) (string, error) {
		// A returned operation is durable intent, not a deletion receipt.
		// Concrete consumers/downloads and deletion-not-before still apply.
		return s.RemoveArtifact(actor, ref)
	}
	_ = remove
}

func ExampleInstallArtifacts() {
	install := func(directory string, verifiedFacts ArtifactInstallation) (ArtifactInstallation, error) {
		// Stop the server first. verifiedFacts must come from the concrete
		// Files initializer/RootFacts outside this Store call, never an RPC.
		return InstallArtifacts(directory, verifiedFacts)
	}
	_ = install
}

func ExampleStore_AbandonArtifactCreate() {
	abandon := func(s *Store, original ArtifactCreateAbandonment) (string, error) {
		// Original Create returned nil, and all actual I/O/FD owners settled.
		// A later revoked Actor cannot publish writable bytes. Preserve only
		// the original operation's exact identity in charged deleting intent.
		return s.AbandonArtifactCreate(original)
	}
	_ = abandon
}

func ExampleStore_CompleteWorkspaceAttempt() {
	complete := func(s *Store, jobID string, execution backend.Reference, status backend.Status, joined WorkspaceCompletion) (WorkspaceCommit, error) {
		// joined is supplied by the concrete original local writer owner and
		// sealed Files candidate. It is never decoded from a request. Missing
		// writer closure keeps the job nonterminal and retains every charge.
		return s.CompleteWorkspaceAttempt(jobID, execution, status, joined, time.Now().UTC())
	}
	_ = complete
}

func ExampleStore_SubmitWorkspace() {
	accept := func(s *Store, actor auth.Actor, domain *RequestDomain, request string, task backend.Task) (ArtifactAttemptResult, error) {
		// task.Workdir is an owned canonical Ref. Setup/output fields remain
		// immutable; the pin is created atomically with this acceptance.
		j, err := s.SubmitWorkspace(actor, domain, request, task, "remote", "installed-binding", backend.RemoteOutput, 0)
		if err != nil {
			return ArtifactAttemptResult{}, err
		}
		return s.ArtifactResultFor(actor, j.ID, j.CurrentAttemptID())
	}
	_ = accept
}
