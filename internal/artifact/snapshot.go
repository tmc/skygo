package artifact

import (
	"errors"
	"io"
	"os"
)

// Snapshot writes a whole input directory as bounded USTAR through the original
// opened root. Empty input is a valid empty archive. All files/directories count
// toward the same limits as Collect; input has no 16 declared-output-root limit.
//
// The caller independently establishes writer quiescence and owns a private,
// unpublished writer. On error it discards every byte, even a complete-looking
// archive. Observed metadata changes refuse; transient hostile ABA changes are
// not ruled out. Root is not a sandbox or a proof of content provenance.
func Snapshot(root *os.Root, w io.Writer) error { return snapshot(root, w, nil) }

func snapshot(root *os.Root, w io.Writer, open rootOpener) (err error) {
	if root == nil || w == nil {
		return errors.New("nil artifact snapshot root or writer")
	}
	authority, err := rootMetadata(root)
	if err != nil {
		return err
	}
	before, err := root.Lstat(".")
	if err != nil {
		return err
	}
	directory, err := openCheckedWith(root, ".", before, authority, true, open)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, closeArchiveFile(directory)) }()
	var names []string
	for {
		// Keep both each read and total selected-name storage bounded. Read at
		// most one entry beyond the accepted manifest limit before refusing.
		entries, readErr := directory.ReadDir(min(64, maxEntries+1-len(names)))
		if len(entries) == 0 && readErr == nil {
			return io.ErrNoProgress
		}
		for _, entry := range entries {
			name, err := canonicalName(entry.Name(), false)
			if err != nil {
				return err
			}
			names = append(names, name)
		}
		if len(names) > maxEntries {
			return errors.New("artifact entry limit exceeded")
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				return readErr
			}
			break
		}
	}
	checkRoot := func() error {
		opened, err := directory.Stat()
		if err != nil {
			return err
		}
		current, err := root.Lstat(".")
		if err != nil {
			return err
		}
		if !sameMetadata(before, opened) || !sameMetadata(before, current) {
			return errors.New("artifact snapshot root changed")
		}
		return nil
	}
	if err = checkRoot(); err == nil {
		err = collectRoots(root, names, w, open)
	}
	err = errors.Join(err, checkRoot())
	return err
}
