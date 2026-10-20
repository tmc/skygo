package artifact

import "io"

// Verify checks ref's shape, encoded size, SHA-256 and complete archive policy,
// returning the manifest derived from the same bounded archive parser as Validate.
// It borrows r and never closes it; r may block. Success grants no filesystem identity,
// ownership, publication, adoption or copying authority.
func Verify(ref Ref, r io.Reader) (Manifest, error) {
	if err := ref.Validate(); err != nil {
		return Manifest{}, err
	}
	var summary archiveSummary
	digest, err := readArchive(r, ref.Size, nil, &summary)
	if err != nil {
		return Manifest{}, err
	}
	if digest != ref.SHA256 || summary.expanded < 0 || summary.files < 0 || summary.directories < 0 {
		return Manifest{}, ErrFileConflict
	}
	return Manifest{Reference: ref, ExpandedBytes: uint64(summary.expanded), RegularFiles: uint64(summary.files), Directories: uint64(summary.directories)}, nil
}
