package artifact

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"sync"
)

const chunkSize = 64 << 10
const maxReaders = 32

// ErrFilesClosed means file-operation admission has closed.
var ErrFilesClosed = errors.New("artifact files closed")

// ErrFilesBusy means actual readers or candidate writers retain ownership.
var ErrFilesBusy = errors.New("artifact files still owned")

// ErrFileConflict means the exact inode, range or operation does not match.
var ErrFileConflict = errors.New("artifact file identity or operation conflict")

// FileID identifies an opened inode. It is neither permission nor proof of bytes.
// The trusted adapter converts Device/Inode explicitly to Store's corresponding DTO.
type FileID struct{ Device, Inode uint64 }

// Manifest comes only from the same parser that validated Reference's full bytes.
// Its values do not authorize a grant, publish metadata or prove process quiescence.
type Manifest struct {
	Reference                                Ref
	ExpandedBytes, RegularFiles, Directories uint64
}

// ChunkReceipt describes a fully compared or synced chunk. A non-nil error never
// authorizes advancing a durable acknowledgement, even when File is nonzero.
type ChunkReceipt struct {
	File         FileID
	Offset, Size int64
	SHA256       string
}

// SealReceipt describes a validated object after link, part removal and root sync.
// Only a nil error permits handing this receipt to the metadata completion caller.
type SealReceipt struct {
	File     FileID
	Manifest Manifest
}

// Files operates in one borrowed, private, caller-owned Root. The caller keeps
// Root open until Files.Close succeeds and serializes this installation against
// another Files writer. Files owns its extra directory FD and returned Readers,
// never the caller Root. Its zero value is unusable; use Open.
//
// Generated flat names have no caller paths. Store intents, ownership, four global
// I/O permits and fresh authorization before metadata publication remain external.
// No method interprets a context deadline as actual I/O completion.
type Files struct {
	mu              sync.Mutex
	closeMu         sync.Mutex
	uncertain       map[*os.File]*FileCloseError
	physical        *PhysicalScan
	physicalIO      physicalIO
	root            *os.Root
	directory       *os.File
	identity        FileID
	authority       metadata
	readers         map[*Reader]struct{}
	candidates      map[*Candidate]string
	closing, closed bool
	closeErr        error
	hooks           filesIO
}

type filesIO struct {
	sync     func(*os.File) error
	close    func(*os.File) error
	writeAt  func(*os.File, []byte, int64) (int, error)
	readAt   func(*os.File, []byte, int64) (int, error)
	link     func(int, string, string) error
	remove   func(*os.Root, string) error
	truncate func(*os.File, int64) error
}

func fileID(m metadata) FileID { return FileID{Device: m.device, Inode: m.number} }
func (id FileID) valid() bool  { return id.Device != 0 && id.Inode != 0 }

// Open verifies the expected original root identity, ownership and private mode.
// It requires and verifies an existing installation marker. It does not create,
// inventory or adopt any existing part or object. The expected
// identity must come from the installation's trusted root handoff, not a client.
func Open(root *os.Root, expected FileID) (*Files, error) {
	f, err := openFiles(root, expected)
	if err != nil {
		return nil, err
	}
	if _, err = f.RootFacts(); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return f, nil
}

func openFiles(root *os.Root, expected FileID) (*Files, error) {
	if root == nil || !expected.valid() {
		return nil, ErrFileConflict
	}
	m, err := rootMetadata(root)
	if err != nil {
		return nil, err
	}
	if fileID(m) != expected {
		return nil, ErrFileConflict
	}
	flag, err := nonblockingRead()
	if err != nil {
		return nil, err
	}
	dir, err := root.OpenFile(".", flag, 0)
	if err != nil {
		return nil, err
	}
	f := &Files{root: root, directory: dir, identity: expected, authority: m, readers: make(map[*Reader]struct{}), candidates: make(map[*Candidate]string), uncertain: make(map[*os.File]*FileCloseError)}
	if err = f.checkRoot(); err != nil {
		return nil, errors.Join(err, f.closeFile(dir))
	}
	return f, nil
}

func (f *Files) checkRoot() error {
	path, err := f.root.Lstat(".")
	if err != nil {
		return err
	}
	opened, err := f.directory.Stat()
	if err != nil {
		return err
	}
	m, err := fileMetadata(path)
	if err != nil {
		return err
	}
	if !path.IsDir() || path.Mode().Perm()&0077 != 0 || path.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 || m.uid != uint32(os.Geteuid()) || fileID(m) != f.identity || !os.SameFile(path, opened) {
		return ErrFileConflict
	}
	return nil
}
func (f *Files) ready() error {
	if f.closing || f.closed {
		return ErrFilesClosed
	}
	if f.hasUncertainClose() {
		return ErrFilesBusy
	}
	return f.checkRoot()
}
func (f *Files) syncFile(file *os.File) error {
	if f.hooks.sync != nil {
		return f.hooks.sync(file)
	}
	return file.Sync()
}
func (f *Files) truncate(file *os.File, size int64) error {
	if f.hooks.truncate != nil {
		return f.hooks.truncate(file, size)
	}
	return file.Truncate(size)
}

// trim releases speculative allocation past size on the same writable file.
func (f *Files) trim(file *os.File, size int64) error {
	return trimAllocation(file, size, f.truncate)
}
func (f *Files) remove(name string) error {
	if f.hooks.remove != nil {
		return f.hooks.remove(f.root, name)
	}
	return f.root.Remove(name)
}
func (f *Files) syncRoot() error {
	if err := f.checkRoot(); err != nil {
		return err
	}
	return f.syncFile(f.directory)
}
func partName(ref Ref) string   { return ref.ID + ".part" }
func objectName(ref Ref) string { return ref.ID + ".tar" }

func (f *Files) checkFile(info os.FileInfo, ref Ref, id FileID, links uint64) error {
	return f.checkOwnedFile(info, id, links, ref.Size)
}
func (f *Files) checkOwnedFile(info os.FileInfo, id FileID, links uint64, limit int64) error {
	m, err := fileMetadata(info)
	if err != nil {
		return err
	}
	if !id.valid() || fileID(m) != id || m.uid != uint32(os.Geteuid()) || m.device != f.authority.device || m.links != links || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 || info.Size() < 0 || info.Size() > limit {
		return ErrFileConflict
	}
	return nil
}
func (f *Files) checkPath(name string, info os.FileInfo) error {
	if err := f.checkRoot(); err != nil {
		return err
	}
	path, err := f.root.Lstat(name)
	if err != nil {
		return err
	}
	if !sameMetadata(path, info) {
		return ErrFileConflict
	}
	return nil
}
func (f *Files) openFile(ref Ref, id FileID, name string, write bool, links uint64) (*os.File, os.FileInfo, error) {
	return f.openOwned(id, name, write, links, ref.Size)
}

// openScratchPart opens the exact single-link part of ref writable. It is the
// only open that admits the one scratch byte past ref.Size that an interrupted
// trimAllocation can leave. Its caller truncates the part to a durable prefix
// before any ordinary check, read, seal or removal; the byte is never payload.
func (f *Files) openScratchPart(ref Ref, id FileID) (*os.File, os.FileInfo, error) {
	return f.openOwned(id, partName(ref), true, 1, ref.Size+1)
}
func (f *Files) openOwned(id FileID, name string, write bool, links uint64, limit int64) (*os.File, os.FileInfo, error) {
	before, err := f.root.Lstat(name)
	if err != nil {
		return nil, nil, err
	}
	if err = f.checkOwnedFile(before, id, links, limit); err != nil {
		return nil, nil, err
	}
	flag, err := nonblockingRead()
	if err != nil {
		return nil, nil, err
	}
	if write {
		flag |= os.O_RDWR
	}
	file, err := f.root.OpenFile(name, flag, 0)
	if err != nil {
		return nil, nil, err
	}
	info, err := file.Stat()
	if err == nil {
		err = f.checkOwnedFile(info, id, links, limit)
	}
	if err == nil && !sameMetadata(before, info) {
		err = ErrFileConflict
	}
	if err == nil {
		err = f.checkPath(name, info)
	}
	if err != nil {
		return nil, nil, errors.Join(err, f.closeFile(file))
	}
	return file, info, nil
}
func (f *Files) finishFile(file *os.File, ref Ref, id FileID, name string, links uint64) error {
	info, err := file.Stat()
	if err == nil {
		err = f.checkFile(info, ref, id, links)
	}
	if err == nil {
		err = f.checkPath(name, info)
	}
	return errors.Join(err, f.closeFile(file))
}

// CreatePart creates one private empty part exclusively, then syncs it and root.
// On an error AFTER creation, FileID remains the actual observed inode and must
// be retained by the operation owner for explicit recovery. EEXIST is never adoption.
func (f *Files) CreatePart(ref Ref) (id FileID, err error) {
	if err = ref.Validate(); err != nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err = f.ready(); err != nil {
		return
	}
	if f.candidateOwned(ref.ID) {
		err = ErrFilesBusy
		return
	}
	if _, e := f.root.Lstat(objectName(ref)); e == nil {
		err = ErrFileConflict
		return
	} else if !errors.Is(e, os.ErrNotExist) {
		err = e
		return
	}
	file, e := f.root.OpenFile(partName(ref), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		err = e
		return
	}
	info, e := file.Stat()
	if e == nil {
		var m metadata
		m, e = fileMetadata(info)
		id = fileID(m)
	}
	if e == nil {
		e = f.checkFile(info, ref, id, 1)
	}
	if e == nil {
		e = f.checkPath(partName(ref), info)
	}
	if e == nil {
		e = f.syncFile(file)
	}
	e = errors.Join(e, f.closeFile(file))
	if e == nil {
		e = f.syncRoot()
	}
	err = e
	return
}

// RecoverPart truncates only the exact owned part's unacknowledged suffix,
// including one scratch byte past ref.Size left by an interrupted allocation
// trim, trims allocation past ack on the same descriptor and syncs it. It never
// appends. A short durable prefix or already-published link is a conflict.
func (f *Files) RecoverPart(ref Ref, id FileID, ack int64) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	if ack < 0 || ack > ref.Size {
		return ErrFileConflict
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.ready(); err != nil {
		return err
	}
	if f.candidateOwned(ref.ID) {
		return ErrFilesBusy
	}
	file, info, err := f.openScratchPart(ref, id)
	if err != nil {
		return err
	}
	if info.Size() < ack {
		return errors.Join(ErrFileConflict, f.closeFile(file))
	}
	if info.Size() > ack {
		err = f.truncate(file, ack)
	}
	if err == nil {
		err = f.trim(file, ack)
	}
	if err == nil {
		err = f.syncFile(file)
	}
	return errors.Join(err, f.finishFile(file, ref, id, partName(ref), 1))
}

// WritePart appends exactly at ack or compares an earlier wholly acknowledged
// range. It rejects holes/partial overlap and requires recovery of any extra suffix.
// A new append trims allocation past its end on the same descriptor before Sync.
// The caller may advance its CAS only after this actual operation returns nil.
func (f *Files) WritePart(ref Ref, id FileID, ack, offset int64, p []byte) (receipt ChunkReceipt, err error) {
	if err = ref.Validate(); err != nil {
		return
	}
	if len(p) < 1 || len(p) > chunkSize || ack < 0 || ack > ref.Size || offset < 0 || offset > ref.Size || int64(len(p)) > ref.Size-offset || offset > ack || offset < ack && int64(len(p)) > ack-offset {
		err = ErrFileConflict
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err = f.ready(); err != nil {
		return
	}
	if f.candidateOwned(ref.ID) {
		err = ErrFilesBusy
		return
	}
	file, info, e := f.openFile(ref, id, partName(ref), true, 1)
	if e != nil {
		err = e
		return
	}
	receipt.File = id
	if info.Size() != ack {
		err = errors.Join(ErrFileConflict, f.closeFile(file))
		return
	}
	if offset < ack {
		old := make([]byte, len(p))
		var n int
		n, e = file.ReadAt(old, offset)
		if e == nil && (n != len(p) || !bytes.Equal(old, p)) {
			e = ErrFileConflict
		}
	} else {
		var n int
		if f.hooks.writeAt != nil {
			n, e = f.hooks.writeAt(file, p, offset)
		} else {
			n, e = file.WriteAt(p, offset)
		}
		if e == nil && n != len(p) {
			e = io.ErrShortWrite
		}
		if e == nil {
			e = f.trim(file, offset+int64(len(p)))
		}
		if e == nil {
			e = f.syncFile(file)
		}
	}
	e = errors.Join(e, f.finishFile(file, ref, id, partName(ref), 1))
	if e != nil {
		err = e
		return
	}
	h := sha256.Sum256(p)
	receipt = ChunkReceipt{File: id, Offset: offset, Size: int64(len(p)), SHA256: hex.EncodeToString(h[:])}
	return
}

func (f *Files) validateFile(file *os.File, info os.FileInfo, ref Ref, id FileID, name string, links uint64) (Manifest, error) {
	if info.Size() != ref.Size {
		return Manifest{}, ErrFileConflict
	}
	var summary archiveSummary
	digest, err := readArchive(io.NewSectionReader(file, 0, ref.Size), ref.Size, nil, &summary)
	if err != nil {
		return Manifest{}, err
	}
	if digest != ref.SHA256 || summary.expanded < 0 || summary.files < 0 || summary.directories < 0 {
		return Manifest{}, ErrFileConflict
	}
	after, err := file.Stat()
	if err != nil {
		return Manifest{}, err
	}
	if !sameMetadata(info, after) {
		return Manifest{}, ErrFileConflict
	}
	if err = f.checkFile(after, ref, id, links); err != nil {
		return Manifest{}, err
	}
	if err = f.checkPath(name, after); err != nil {
		return Manifest{}, err
	}
	return Manifest{Reference: ref, ExpandedBytes: uint64(summary.expanded), RegularFiles: uint64(summary.files), Directories: uint64(summary.directories)}, nil
}

// SealPart fully validates the exact part, links exclusively to the object name,
// syncs that name, removes the part and syncs again. Exact same-inode crash prefixes
// can resume; an unrelated existing object never gets overwritten or adopted.
// A single-link part is first restored to ref.Size and trimmed on its own
// descriptor; a part already linked to the object is never modified.
// checkLinkGrowth runs only immediately before a new link, after validation and
// file Sync. Nil refuses a new link. Existing-prefix recovery never calls it.
// It may read only the caller's dedicated bounded capacity cache: no Files,
// filesystem, Store, auth, service mutex or blocking wait while Files.mu is held.
func (f *Files) SealPart(ref Ref, id FileID, checkLinkGrowth func() error) (receipt SealReceipt, err error) {
	if err = ref.Validate(); err != nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err = f.ready(); err != nil {
		return
	}
	if f.candidateOwned(ref.ID) {
		return SealReceipt{}, ErrFilesBusy
	}
	return f.sealPart(ref, id, checkLinkGrowth)
}
func (f *Files) sealPart(ref Ref, id FileID, checkLinkGrowth func() error) (receipt SealReceipt, err error) {
	name := partName(ref)
	part, e := f.root.Lstat(name)
	if errors.Is(e, os.ErrNotExist) {
		return f.sealedObject(ref, id)
	}
	if e != nil {
		err = e
		return
	}
	links := uint64(1)
	obj, e := f.root.Lstat(objectName(ref))
	if e == nil {
		links = 2
		if !os.SameFile(part, obj) {
			err = ErrFileConflict
			return
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		err = e
		return
	}
	var file *os.File
	var info os.FileInfo
	if links == 1 {
		file, info, e = f.preparePart(ref, id)
	} else {
		file, info, e = f.openFile(ref, id, name, false, links)
	}
	if e != nil {
		err = e
		return
	}
	manifest, e := f.validateFile(file, info, ref, id, name, links)
	if e == nil {
		e = f.syncFile(file)
	}
	if e == nil && links == 1 {
		if checkLinkGrowth == nil {
			e = ErrFilesBusy
		} else {
			e = checkLinkGrowth()
		}
	}
	if e == nil && links == 1 {
		if f.hooks.link != nil {
			e = f.hooks.link(int(f.directory.Fd()), name, objectName(ref))
		} else {
			e = linkFile(int(f.directory.Fd()), name, objectName(ref))
		}
	}
	if e == nil {
		after, statErr := file.Stat()
		e = statErr
		if e == nil {
			e = f.checkFile(after, ref, id, 2)
		}
		if e == nil {
			e = f.checkPath(name, after)
		}
		if e == nil {
			e = f.checkPath(objectName(ref), after)
		}
	}
	if e == nil {
		e = f.syncRoot()
	}
	if e == nil {
		e = f.remove(name)
	}
	if e == nil {
		e = f.syncRoot()
	}
	if e == nil {
		e = f.finishFile(file, ref, id, objectName(ref), 1)
	} else {
		e = errors.Join(e, f.closeFile(file))
	}
	if e != nil {
		err = e
		return
	}
	return SealReceipt{File: id, Manifest: manifest}, nil
}

// preparePart opens the exact single-link part writable, removes a scratch
// byte past ref.Size and trims allocation before the ordinary strict checks.
func (f *Files) preparePart(ref Ref, id FileID) (*os.File, os.FileInfo, error) {
	file, info, err := f.openScratchPart(ref, id)
	if err != nil {
		return nil, nil, err
	}
	if info.Size() < ref.Size {
		err = ErrFileConflict
	}
	if err == nil && info.Size() > ref.Size {
		err = f.truncate(file, ref.Size)
	}
	if err == nil {
		err = f.trim(file, ref.Size)
	}
	if err == nil {
		info, err = file.Stat()
	}
	if err == nil {
		err = f.checkFile(info, ref, id, 1)
	}
	if err == nil {
		err = f.checkPath(partName(ref), info)
	}
	if err != nil {
		return nil, nil, errors.Join(err, f.closeFile(file))
	}
	return file, info, nil
}
func (f *Files) sealedObject(ref Ref, id FileID) (SealReceipt, error) {
	file, info, err := f.openFile(ref, id, objectName(ref), false, 1)
	if err != nil {
		return SealReceipt{}, err
	}
	manifest, err := f.validateFile(file, info, ref, id, objectName(ref), 1)
	if err == nil {
		err = f.syncFile(file)
	}
	if err == nil {
		err = f.syncRoot()
	}
	err = errors.Join(err, f.finishFile(file, ref, id, objectName(ref), 1))
	if err != nil {
		return SealReceipt{}, err
	}
	return SealReceipt{File: id, Manifest: manifest}, nil
}

// RemovePart and RemoveObject require a prior durable deletion intent and joined
// operation owners. Missing is confirmed only in this original root, followed by
// root sync. They never erase another inode or credit metadata reservations.
// RemovePart first repairs an interrupted trim's scratch byte on the exact
// single-link part, then removes it through the ordinary strict checks.
func (f *Files) RemovePart(ref Ref, id FileID) error   { return f.removeOwned(ref, id, false) }
func (f *Files) RemoveObject(ref Ref, id FileID) error { return f.removeOwned(ref, id, true) }
func (f *Files) removeOwned(ref Ref, id FileID, object bool) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	if !id.valid() {
		return ErrFileConflict
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.ready(); err != nil {
		return err
	}
	if f.candidateOwned(ref.ID) {
		return ErrFilesBusy
	}
	for r := range f.readers {
		if r.reference.ID == ref.ID {
			return ErrFilesBusy
		}
	}
	name, mate := partName(ref), objectName(ref)
	if object {
		name, mate = mate, name
	}
	info, err := f.root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return f.syncRoot()
	}
	if err != nil {
		return err
	}
	m, err := fileMetadata(info)
	if err != nil {
		return err
	}
	links := m.links
	if links != 1 && links != 2 {
		return ErrFileConflict
	}
	if links == 2 {
		other, e := f.root.Lstat(mate)
		if e != nil || !os.SameFile(info, other) {
			return ErrFileConflict
		}
	}
	if !object && links == 1 && info.Size() > ref.Size {
		// An interrupted trim's scratch byte gets the same private repair as
		// RecoverPart before the ordinary strict removal below.
		file, _, err := f.openScratchPart(ref, id)
		if err != nil {
			return err
		}
		err = f.truncate(file, ref.Size)
		if err == nil {
			err = f.syncFile(file)
		}
		if err = errors.Join(err, f.finishFile(file, ref, id, name, 1)); err != nil {
			return err
		}
	}
	file, _, err := f.openFile(ref, id, name, false, links)
	if err != nil {
		return err
	}
	if err = f.finishFile(file, ref, id, name, links); err != nil {
		return err
	}
	if err = f.remove(name); err != nil {
		return err
	}
	return f.syncRoot()
}

// Close closes admission and its own directory FD after all Readers and candidate
// writers and registered physical scan close without uncertainty. A busy
// or failed close retains external protections; it never closes the borrowed Root.
func (f *Files) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closing = true
	if f.closed {
		return f.closeErr
	}
	if len(f.readers) != 0 || len(f.candidates) != 0 || f.physical != nil || f.hasUncertainClose() {
		return ErrFilesBusy
	}
	f.closeErr = f.closeFile(f.directory)
	f.closed = true
	return f.closeErr
}
