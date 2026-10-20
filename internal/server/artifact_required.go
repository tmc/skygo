package server

import (
	"errors"
	"fmt"
	"io"

	"github.com/tmc/skygo/internal/artifact"
)

// preflightArtifacts checks the SAME owned or borrowed Files before any Local,
// Control, listener or dispatch exists. Borrowed transfer storage gains no
// workspace authority. The caller still supplies the original-writer handoff;
// checking bytes now does not exclude later same-user filesystem mutations.
func (f *serverRun) preflightArtifacts() error {
	installed, err := f.store.ArtifactInstallation()
	if err != nil {
		return err
	}
	facts, err := f.files.RootFacts()
	if err != nil {
		return err
	}
	if !installed.Enabled || facts.ID != installed.ID || facts.Unit != installed.Unit || facts.Volume != installed.Volume {
		return artifact.ErrFileConflict
	}
	objects, err := f.store.ArtifactRequiredObjects()
	if err != nil {
		return fmt.Errorf("read required artifacts: %w", err)
	}
	for _, object := range objects {
		// The validated Store View bounds each Ref to 32 MiB and the complete
		// deduplicated inventory to 128 objects/512 MiB before this conversion.
		ref := artifact.Ref{ID: object.Reference.ID, SHA256: object.Reference.SHA256, Size: int64(object.Reference.Size)}
		reader, err := f.files.OpenObject(ref, artifact.FileID{Device: object.File.Device, Inode: object.File.Inode})
		if err != nil {
			return fmt.Errorf("open required artifact: %w", err)
		}
		manifest, verifyErr := artifact.Verify(ref, io.NewSectionReader(reader, 0, ref.Size))
		if verifyErr == nil && (object.Manifest.Reference != object.Reference || manifest.Reference != ref || manifest.ExpandedBytes != object.Manifest.ExpandedBytes || manifest.RegularFiles != object.Manifest.RegularFiles || manifest.Directories != object.Manifest.Directories) {
			verifyErr = artifact.ErrFileConflict
		}
		// Exactly one actual Close after Verify returns, including failures.
		// On uncertainty Files retains this Reader in readers and its exact
		// FileCloseError in uncertain. The held server frame keeps that SAME
		// Files/Store/root/cause; no registry, raw retry or later close runs.
		if closeErr := reader.Close(); closeErr != nil {
			return f.hold("required artifact reader closure remains held", errors.Join(verifyErr, closeErr))
		}
		if verifyErr != nil {
			return fmt.Errorf("verify required artifact: %w", verifyErr)
		}
	}
	return nil
}
