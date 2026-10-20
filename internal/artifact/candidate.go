package artifact

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"os"
	"sync"
)

const maxCandidates = 4

// CandidateFile identifies a staging file, not a valid archive or publication.
// SHA256 is the actual counted prefix digest, including the empty prefix.
// It proves no archive validity, publication, permission, closure or reclamation.
// Size may be zero. The trusted cleanup caller must already hold committed
// cleanup intent and have joined all original owners; this value grants neither.
type CandidateFile struct {
	ID     string
	File   FileID
	Size   int64
	SHA256 string
}

// Candidate writes one reserved result ID in the original Files root. Its zero
// value is unusable. Calls serialize actual I/O, including Close; no background
// operation is abandoned on timeout. Unproved original closure retains ownership.
type Candidate struct {
	mu                         sync.Mutex
	files                      *Files
	file                       *os.File
	description                CandidateFile
	digest                     hash.Hash
	err                        error
	closed, aborted, uncertain bool
	reference                  Ref
}

func (f *Files) candidateOwned(id string) bool {
	for _, owned := range f.candidates {
		if owned == id {
			return true
		}
	}
	return false
}
func candidateID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for _, c := range id {
		if c < '0' || c > '9' && c < 'a' || c > 'f' {
			return false
		}
	}
	return true
}

// CreateCandidate exclusively creates a private part for a trusted reserved ID.
// The caller supplies one of the installation's four I/O permits. A non-nil
// Candidate returned with an error still owns the observed file and must be
// retained; neither timeout nor EEXIST authorizes adoption or charge release.
func (f *Files) CreateCandidate(id string) (*Candidate, error) {
	if !candidateID(id) {
		return nil, ErrFileConflict
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.ready(); err != nil {
		return nil, err
	}
	if len(f.candidates) >= maxCandidates || f.candidateOwned(id) {
		return nil, ErrFilesBusy
	}
	if _, err := f.root.Lstat(id + ".tar"); err == nil {
		return nil, ErrFileConflict
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	file, err := f.root.OpenFile(id+".part", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	c := &Candidate{files: f, file: file, description: CandidateFile{ID: id}, digest: sha256.New()}
	c.description.SHA256 = hex.EncodeToString(c.digest.Sum(nil))
	f.candidates[c] = id
	info, err := file.Stat()
	if err == nil {
		var m metadata
		m, err = fileMetadata(info)
		if err == nil {
			c.description.File = fileID(m)
		}
	}
	if err == nil {
		err = f.checkOwnedFile(info, c.description.File, 1, MaxSize)
	}
	if err == nil {
		err = f.checkPath(id+".part", info)
	}
	if err == nil {
		err = f.syncFile(file)
	}
	if err == nil {
		err = f.syncRoot()
	}
	c.err = err
	return c, err
}

// Description observes the actual counted prefix and inode. It is not a receipt
// of archive validation, writer closure, permission or reclaimed storage.
func (c *Candidate) Description() CandidateFile {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.description
}

// Write appends at most MaxSize bytes in total. Errors are sticky. A short or
// failed write records only the actual accepted prefix and cannot be sealed.
func (c *Candidate) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, ErrFilesClosed
	}
	if c.err != nil {
		return 0, c.err
	}
	f := c.files
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.ready(); err != nil {
		c.err = err
		return 0, err
	}
	if int64(len(p)) > MaxSize-c.description.Size {
		c.err = ErrFileConflict
		return 0, c.err
	}
	info, err := c.file.Stat()
	if err == nil {
		err = f.checkOwnedFile(info, c.description.File, 1, MaxSize)
	}
	if err == nil && info.Size() != c.description.Size {
		err = ErrFileConflict
	}
	if err == nil {
		err = f.checkPath(c.description.ID+".part", info)
	}
	if err != nil {
		c.err = err
		return 0, err
	}
	var n int
	if f.hooks.writeAt != nil {
		n, err = f.hooks.writeAt(c.file, p, c.description.Size)
	} else {
		n, err = c.file.WriteAt(p, c.description.Size)
	}
	if n < 0 || n > len(p) {
		c.err = ErrFileConflict
		return 0, c.err
	}
	if n > 0 {
		c.digest.Write(p[:n])
		c.description.Size += int64(n)
		c.description.SHA256 = hex.EncodeToString(c.digest.Sum(nil))
	}
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	c.err = err
	return n, err
}

// Close aborts the candidate and joins any actual Write. It never publishes.
// A failed original Close remains uncertain. Ordinary write/sync failures after
// successful actual Close release only FD ownership, never storage charge.
func (c *Candidate) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		if !c.uncertain {
			c.aborted = true
			c.files.mu.Lock()
			delete(c.files.candidates, c)
			c.files.mu.Unlock()
		}
		return c.err
	}
	c.aborted = true
	f := c.files
	f.mu.Lock()
	defer f.mu.Unlock()
	err := f.closeFile(c.file)
	c.closed = true
	if err != nil {
		c.uncertain = true
	}
	c.err = errors.Join(c.err, err)
	if !c.uncertain {
		delete(f.candidates, c)
	}
	return c.err
}

// Seal may be called ONLY after the caller's actual Collect returned nil. A
// complete-looking archive after Collect failed is still aborted, even if Write
// saw no error. Seal joins the original FD, then uses the same full archive parser
// and exclusive publication path as SealPart. Only a nil error permits use of
// the receipt. A positively closed publication failure may retry this owner or
// abort with exact cleanup facts. An unproved Close retains the original owner.
// checkLinkGrowth has SealPart's cached-only contract and runs after original
// writer Close, under Candidate.mu then Files.mu; nil refuses only a new link.
func (c *Candidate) Seal(checkLinkGrowth func() error) (SealReceipt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.aborted || c.uncertain || c.err != nil {
		return SealReceipt{}, errors.Join(ErrFileConflict, c.err)
	}
	f := c.files
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.ready(); err != nil {
		return SealReceipt{}, err
	}
	if !c.closed {
		info, err := c.file.Stat()
		if err == nil {
			err = f.checkOwnedFile(info, c.description.File, 1, MaxSize)
		}
		if err == nil && info.Size() != c.description.Size {
			err = ErrFileConflict
		}
		if err == nil {
			err = f.checkPath(c.description.ID+".part", info)
		}
		if err == nil {
			err = f.syncFile(c.file)
		}
		closeErr := f.closeFile(c.file)
		c.closed = true
		if closeErr != nil {
			c.uncertain = true
		}
		c.err = errors.Join(err, closeErr)
		if c.err != nil {
			return SealReceipt{}, c.err
		}
		c.reference = Ref{ID: c.description.ID, SHA256: hex.EncodeToString(c.digest.Sum(nil)), Size: c.description.Size}
		if err := c.reference.Validate(); err != nil {
			c.err = err
			return SealReceipt{}, err
		}
	}
	receipt, err := f.sealPart(c.reference, c.description.File, checkLinkGrowth)
	if err == nil {
		delete(f.candidates, c)
	} else if errors.Is(err, ErrFileCloseUncertain) {
		c.uncertain = true
		c.err = err
	}
	return receipt, err
}

// RemoveCandidate removes only an exact, closed, single-link staging prefix.
// The caller must hold committed cleanup intent, all joined original owners and
// this installation's I/O permit. Missing is confirmed in the original root and
// synced; no object link, foreign inode or uncertain active owner is adopted.
func (f *Files) RemoveCandidate(c CandidateFile) error {
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
	// A missing part alone is not absence: publication can have removed the
	// part before its final root sync failed. Any object prefix requires the
	// explicit two-name cleanup plan, even if its inode is unrelated.
	if _, err := f.root.Lstat(c.ID + ".tar"); err == nil {
		return ErrFileConflict
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	name := c.ID + ".part"
	info, err := f.root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return f.syncRoot()
	}
	if err != nil {
		return err
	}
	if err = f.checkOwnedFile(info, c.File, 1, MaxSize); err != nil {
		return err
	}
	if info.Size() != c.Size {
		return ErrFileConflict
	}
	flag, err := nonblockingRead()
	if err != nil {
		return err
	}
	file, err := f.root.OpenFile(name, flag, 0)
	if err != nil {
		return err
	}
	after, err := file.Stat()
	if err == nil && !sameMetadata(info, after) {
		err = ErrFileConflict
	}
	if err == nil {
		err = f.checkOwnedFile(after, c.File, 1, MaxSize)
	}
	if err == nil {
		err = f.checkPath(name, after)
	}
	if err = errors.Join(err, f.closeFile(file)); err != nil {
		return err
	}
	if err = f.remove(name); err != nil {
		return err
	}
	return f.syncRoot()
}
