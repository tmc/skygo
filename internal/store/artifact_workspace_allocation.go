package store

// The additional working tree has the existing archive policy's finite entry
// and payload allowance, separate from extracted input and the result archive.
// This is a reservation, not an OS quota or permission to ignore unexpected
// physical entries. Excess observed usage still refuses growth.
const artifactWorkspaceEntries uint64 = 2048 + 512

func artifactWorkspaceAllocation(m ArtifactManifest, unit uint64) (allocated, files uint64, err error) {
	if !artifactManifestValid(m, m.Reference) || unit < 512 || unit > 65536 || unit&(unit-1) != 0 {
		return 0, 0, artifactCorrupt()
	}
	// Validated bounds make the rounding and multiplication finite: payload
	// <=32MiB, files<=5121 and unit<=65536. The workspace root is counted once.
	allocated = (m.ExpandedBytes + unit - 1) / unit * unit
	files = m.RegularFiles + m.Directories + 1
	if err = artifactAdd(&files, artifactWorkspaceEntries); err != nil {
		return 0, 0, err
	}
	if err = artifactAdd(&allocated, artifactMaxSize); err != nil {
		return 0, 0, err
	}
	if err = artifactAdd(&allocated, files*unit); err != nil {
		return 0, 0, err
	}
	return allocated, files, nil
}
