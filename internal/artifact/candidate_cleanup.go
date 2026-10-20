package artifact

import (
	"errors"
	"os"
)

// RemoveCandidateNames removes the exact generated part and object prefixes of
// a closed, unpublished candidate. Empty and malformed archive prefixes are
// valid cleanup subjects; c must contain the actual original inode, counted
// size and lowercase prefix digest, not a fabricated publication Ref.
//
// The trusted caller must already hold the committed workspace cleanup intent,
// positive original writer/collection closure and the common artifact I/O permit.
// These facts cannot be proved by CandidateFile or by this method. Live Readers,
// Candidates and Files-owned uncertain closes prevent removal. The caller must
// separately retain and settle archive-owned closure errors, which Files cannot
// observe. A non-nil error,
// including partial unlink or root Sync failure, leaves the intent and every
// reservation charged at the caller. A retry checks only the remaining exact
// names; both missing requires that same prior intent and original-root Sync.
//
// All observed names/descriptors must agree before deletion. The caller keeps
// owned writers quiescent: pathname checks and Remove are not a universal atomic
// inode-conditional unlink against arbitrary concurrent same-user replacement.
func (f *Files) RemoveCandidateNames(c CandidateFile) error {
	if !candidateID(c.ID) || !lowerHex(c.SHA256, 64) || !c.File.valid() || c.Size < 0 || c.Size > MaxSize {
		return ErrFileConflict
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.ready(); err != nil {
		return err
	}
	if f.candidateOwned(c.ID) {
		return ErrFilesBusy
	}
	for reader := range f.readers {
		if reader.reference.ID == c.ID || reader.identity == c.File {
			return ErrFilesBusy
		}
	}
	if _, err := f.rootFacts(); err != nil {
		return err
	}
	names := [2]string{c.ID + ".part", c.ID + ".tar"}
	present, before, err := f.candidatePrefixes(c)
	if err != nil {
		return err
	}
	if present == 0 {
		return f.syncRoot()
	}
	flag, err := nonblockingRead()
	if err != nil {
		return err
	}
	links := uint64(present&1) + uint64(present>>1)
	// At most two original FDs are opened, checked and positively closed. No
	// name is removed if either open/Stat/path/Close check fails.
	for i, name := range names {
		if present&(1<<i) == 0 {
			continue
		}
		file, err := f.root.OpenFile(name, flag, 0)
		if err != nil {
			return err
		}
		after, err := file.Stat()
		if err == nil && !sameMetadata(before[i], after) {
			err = ErrFileConflict
		}
		if err == nil {
			err = f.checkOwnedFile(after, c.File, links, MaxSize)
		}
		if err == nil && after.Size() != c.Size {
			err = ErrFileConflict
		}
		for j, other := range names {
			if err == nil && present&(1<<j) != 0 {
				err = f.checkPath(other, after)
			}
		}
		if err = errors.Join(err, f.closeFile(file)); err != nil {
			return err
		}
	}
	remaining := present
	for i, name := range names {
		if remaining&(1<<i) == 0 {
			continue
		}
		observed, infos, err := f.candidatePrefixes(c)
		if err != nil {
			return err
		}
		if observed != remaining {
			return ErrFileConflict
		}
		if remaining == present {
			for j := range names {
				if present&(1<<j) != 0 && !sameMetadata(before[j], infos[j]) {
					return ErrFileConflict
				}
			}
		}
		if err := f.remove(name); err != nil {
			return err
		}
		remaining &^= 1 << i
	}
	return f.syncRoot()
}

// candidatePrefixes inspects exactly the two generated names in the original
// root. A single surviving name must have one link; a pair must be the same
// expected inode with exactly two links. Any third/foreign link refuses.
func (f *Files) candidatePrefixes(c CandidateFile) (uint8, [2]os.FileInfo, error) {
	var infos [2]os.FileInfo
	if err := f.checkRoot(); err != nil {
		return 0, infos, err
	}
	var present uint8
	for i, name := range [2]string{c.ID + ".part", c.ID + ".tar"} {
		info, err := f.root.Lstat(name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, infos, err
		}
		infos[i] = info
		present |= 1 << i
	}
	links := uint64(present&1) + uint64(present>>1)
	for _, info := range infos {
		if info == nil {
			continue
		}
		if err := f.checkOwnedFile(info, c.File, links, MaxSize); err != nil {
			return 0, infos, err
		}
		if info.Size() != c.Size {
			return 0, infos, ErrFileConflict
		}
	}
	if present == 3 && !os.SameFile(infos[0], infos[1]) {
		return 0, infos, ErrFileConflict
	}
	return present, infos, nil
}
