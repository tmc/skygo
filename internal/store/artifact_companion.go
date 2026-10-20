package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	bolt "go.etcd.io/bbolt"
)

const companionManifestLimit = 2 << 20

type companionObject struct {
	Reference ArtifactRef
	File      ArtifactFile
	Manifest  ArtifactManifest
}
type companionManifest struct {
	Version        uint64
	Owner          string
	Root           artifactRoot
	Facts          artifact.RootFacts
	DatabaseSize   uint64
	DatabaseSHA256 string
	Objects        []companionObject
}

// companionFile records ONE original acquisition and ONE Close attempt. A
// failed Close is sticky; the pointer is custody, never retry authority.
type companionFile struct {
	file      *os.File
	attempted bool
	err       error
	closeFile func(*os.File) error // a private test seam; nil uses the original Close
}

func (f *companionFile) close() error {
	if f.file == nil || f.attempted {
		return f.err
	}
	f.attempted = true
	if f.closeFile != nil {
		f.err = f.closeFile(f.file)
	} else {
		f.err = f.file.Close()
	}
	if f.err == nil {
		f.file = nil
	}
	return f.err
}
func (f *companionFile) take(file *os.File) error {
	if f.file != nil || f.err != nil {
		return errors.New("companion original file is unsettled")
	}
	*f = companionFile{file: file}
	return nil
}

type companionDB struct {
	db        *bolt.DB
	file      *os.File // alias transferred to bbolt; NEVER raw-close this pointer
	attempted bool
	err       error
}

func (d *companionDB) close() error {
	if d.db == nil || d.attempted {
		return d.err
	}
	d.attempted = true
	d.err = d.db.Close()
	if d.err == nil {
		d.db = nil
		d.file = nil
	}
	return d.err
}

// The fixed operation frame retains uncertain original owners. It is not a
// persistent owner registry, retry handle, or proof that an OS FD still exists.
type companionOperation struct {
	ctx                                                   context.Context
	parentPath, finalName, stagePath                      string
	stageName                                             string
	stageID                                               ArtifactFile
	published                                             bool
	parentInfo                                            os.FileInfo
	parent, data, sourceObjects, sourceLogs               companionFile
	stage, objects, logs, input, output                   companionFile
	sourceDB, stageDB                                     companionDB
	root, newRoot                                         *os.Root
	parentRoot, stageRoot                                 *os.Root
	files, newFiles                                       *artifact.Files
	reader                                                *artifact.Reader
	readerAttempted, filesAttempted, rootAttempted        bool
	newFilesAttempted, newRootAttempted                   bool
	parentRootAttempted, stageRootAttempted               bool
	readerErr, filesErr, rootErr, newFilesErr, newRootErr error
	parentRootErr, stageRootErr                           error
	filesAcquisitionErr, newFilesAcquisitionErr           error
}
type companionError struct {
	operation *companionOperation
	cause     error
}

func (e *companionError) Error() string {
	state := "unpublished"
	path := e.operation.stagePath
	if e.operation.published {
		state = "published but unsettled"
		path = filepath.Join(e.operation.parentPath, e.operation.finalName)
	}
	return fmt.Sprintf("artifact companion %s (original path %s): %v", state, path, e.cause)
}
func (e *companionError) Unwrap() error { return e.cause }
func (o *companionOperation) closeReader() error {
	if o.reader != nil && !o.readerAttempted {
		o.readerAttempted = true
		o.readerErr = o.reader.Close()
		if o.readerErr == nil {
			o.reader = nil
			o.readerAttempted = false
		}
	}
	return o.readerErr
}

// A failed constructor can return no Files while its typed error owns an
// uncertain original Files descriptor. Keep that error AND its borrowed Root.
// The pointer supplies no authority to retry Close or infer an OS FD is live.
func (o *companionOperation) artifactConstructorError(err error, destination bool) error {
	if errors.Is(err, artifact.ErrFileCloseUncertain) {
		if destination {
			o.newFilesAcquisitionErr = err
		} else {
			o.filesAcquisitionErr = err
		}
	}
	return err
}

func (o *companionOperation) settle() error {
	err := errors.Join(o.input.close(), o.output.close(), o.closeReader())
	if o.readerErr == nil && o.files != nil && !o.filesAttempted {
		o.filesAttempted = true
		o.filesErr = o.files.Close()
		if o.filesErr == nil {
			o.files = nil
		}
	}
	if o.filesErr == nil && o.filesAcquisitionErr == nil && o.readerErr == nil && o.root != nil && !o.rootAttempted {
		o.rootAttempted = true
		o.rootErr = o.root.Close()
		if o.rootErr == nil {
			o.root = nil
		}
	}
	if o.newFiles != nil && !o.newFilesAttempted {
		o.newFilesAttempted = true
		o.newFilesErr = o.newFiles.Close()
		if o.newFilesErr == nil {
			o.newFiles = nil
		}
	}
	if o.newFilesErr == nil && o.newFilesAcquisitionErr == nil && o.newRoot != nil && !o.newRootAttempted {
		o.newRootAttempted = true
		o.newRootErr = o.newRoot.Close()
		if o.newRootErr == nil {
			o.newRoot = nil
		}
	}
	// OpenRoot returns independent root owners; these are not the Roots
	// borrowed by Files. Independent DB/directory closure remains permitted.
	if o.stageRoot != nil && !o.stageRootAttempted {
		o.stageRootAttempted = true
		o.stageRootErr = o.stageRoot.Close()
		if o.stageRootErr == nil {
			o.stageRoot = nil
		}
	}
	if o.parentRoot != nil && !o.parentRootAttempted {
		o.parentRootAttempted = true
		o.parentRootErr = o.parentRoot.Close()
		if o.parentRootErr == nil {
			o.parentRoot = nil
		}
	}
	return errors.Join(err, o.filesErr, o.rootErr, o.newFilesErr, o.newRootErr, o.filesAcquisitionErr, o.newFilesAcquisitionErr, o.stageRootErr, o.parentRootErr, o.sourceDB.close(), o.stageDB.close(), o.sourceObjects.close(), o.sourceLogs.close(), o.data.close(), o.objects.close(), o.logs.close(), o.stage.close())
}
func (o *companionOperation) finish(cause error) error {
	settle := o.settle()
	parent := o.parent.close()
	err := errors.Join(cause, settle, parent)
	if err == nil {
		return nil
	}
	// Failed stages are retained for an operator. No pathname-based recursive
	// deletion can adopt a substituted stage after its original handle closes.
	return &companionError{o, err}
}
func (o *companionOperation) checkContext() error { return o.ctx.Err() }

func companionCheck(file *os.File, directory bool, minimum int64) (os.FileInfo, ArtifactFile, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, ArtifactFile{}, err
	}
	uid, links, id, err := companionStat(info)
	if err != nil {
		return nil, ArtifactFile{}, err
	}
	mode := os.FileMode(0600)
	if directory {
		mode = 0700
	}
	if uid != uint32(os.Geteuid()) || info.Mode().Perm() != mode || info.Mode()&(os.ModeSymlink|os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 || directory != info.IsDir() || !directory && (!info.Mode().IsRegular() || links != 1 || info.Size() < minimum) {
		return nil, ArtifactFile{}, errors.New("companion requires private owned directories and single-link regular files")
	}
	return info, id, nil
}
func companionPathDir(path string, slot *companionFile) error {
	before, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !before.IsDir() || before.Mode()&os.ModeSymlink != 0 {
		return errors.New("companion directory is not a real directory")
	}
	f, err := companionOpen(nil, path, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	if err := slot.take(f); err != nil {
		return errors.Join(err, f.Close())
	}
	after, _, err := companionCheck(f, true, 0)
	if err == nil && !os.SameFile(before, after) {
		err = errors.New("companion directory identity changed")
	}
	return err
}
func companionChild(parent *os.File, name string, flags int, dir bool, slot *companionFile) error {
	f, err := companionOpen(parent, name, flags, 0600)
	if err != nil {
		return err
	}
	if err := slot.take(f); err != nil {
		return errors.Join(err, f.Close())
	}
	_, _, err = companionCheck(f, dir, 0)
	return err
}
func companionNames(file *os.File, limit int) ([]string, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	entries, err := file.ReadDir(limit + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if len(entries) > limit {
		return nil, errors.New("companion directory inventory exceeds limit")
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out, nil
}
func companionEmpty(dir *os.File) error {
	names, err := companionNames(dir, 1)
	if err != nil {
		return err
	}
	if len(names) != 0 {
		return errors.New("local output is not included in companion backup")
	}
	return nil
}
func companionOpenDB(ctx context.Context, dir *os.File, path string, slot *companionDB, expected *companionManifest) error {
	var preclose error
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second, NoFreelistSync: true, OpenFile: func(_ string, flags int, mode os.FileMode) (*os.File, error) {
		if slot.file != nil {
			return nil, errors.New("duplicate companion database acquisition")
		}
		f, e := companionOpen(dir, "jobs.db", flags&^os.O_CREATE, mode)
		if e != nil {
			return nil, e
		}
		_, _, e = companionCheck(f, false, 2048)
		// Verify the same original before bbolt can mmap or parse its pages.
		// The stopped, immutable bundle premise also covers the later flock.
		if e == nil && expected != nil {
			e = companionDatabaseDigest(ctx, f, expected.DatabaseSize, expected.DatabaseSHA256)
		}
		if e != nil {
			preclose = f.Close()
			if preclose != nil {
				slot.file = f
				slot.err = preclose
			}
			return nil, errors.Join(e, preclose)
		}
		slot.file = f // bbolt owns it from this point, including failed Open cleanup
		return f, nil
	}})
	if err != nil {
		if slot.file != nil {
			slot.err = errors.Join(slot.err, errors.New("database constructor cleanup is uncertain"), err)
		}
		return errors.Join(err, preclose)
	}
	slot.db = db
	return nil
}

func companionDatabaseDigest(ctx context.Context, f *os.File, size uint64, digest string) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() < 0 || uint64(info.Size()) != size {
		return errors.New("companion database size differs")
	}
	hash := sha256.New()
	n, err := io.CopyBuffer(hash, companionReader{ctx, io.NewSectionReader(f, 0, info.Size())}, make([]byte, artifactChunkSize))
	if err != nil {
		return err
	}
	if n != info.Size() || hex.EncodeToString(hash.Sum(nil)) != digest {
		return errors.New("companion database digest differs")
	}
	return nil
}

// Preserve both original meta pages and the full physical file, including
// bbolt's allocation tail. tx.Size is only the committed logical high-water
// mark. Exclusive locking plus stopped writers and no Update make this a raw
// stopped-file copy, not a replacement for ordinary transactional snapshots.
func companionDatabaseSize(source *companionDB) (size uint64, err error) {
	err = source.db.View(func(tx *bolt.Tx) error {
		if err := validateSnapshot(tx); err != nil {
			return err
		}
		info, err := source.file.Stat()
		if err != nil {
			return err
		}
		if info.Size() < tx.Size() {
			return errors.New("companion database is shorter than its committed size")
		}
		size = uint64(info.Size())
		return nil
	})
	return
}

func companionSnapshot(ctx context.Context, source *companionDB, size uint64, w io.Writer) error {
	return source.db.View(func(tx *bolt.Tx) error {
		if err := validateSnapshot(tx); err != nil {
			return err
		}
		before, err := source.file.Stat()
		if err != nil {
			return err
		}
		if before.Size() < tx.Size() || uint64(before.Size()) != size {
			return errors.New("companion database size changed")
		}
		n, err := io.CopyBuffer(w, companionReader{ctx, io.NewSectionReader(source.file, 0, before.Size())}, make([]byte, artifactChunkSize))
		if err != nil {
			return err
		}
		after, err := source.file.Stat()
		if err != nil {
			return err
		}
		if n != before.Size() || after.Size() != before.Size() {
			return errors.New("companion database size changed")
		}
		return nil
	})
}
func newCompanion(ctx context.Context, destination string) (*companionOperation, error) {
	o := &companionOperation{ctx: ctx}
	if ctx == nil || destination == "" {
		return o, errors.New("companion requires context and new destination")
	}
	path, err := filepath.Abs(destination)
	if err != nil {
		return o, err
	}
	o.parentPath, o.finalName = filepath.Dir(path), filepath.Base(path)
	if o.finalName == "." || o.finalName == string(filepath.Separator) {
		return o, errors.New("invalid companion destination")
	}
	// An owned archive parent need not be0700, but group/other writes are
	// refused. The caller separately keeps its namespace stable throughout.
	info, err := os.Lstat(o.parentPath)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return o, errors.Join(err, errors.New("invalid companion publication parent"))
	}
	f, err := companionOpen(nil, o.parentPath, os.O_RDONLY, 0)
	if err != nil {
		return o, err
	}
	o.parent.take(f)
	o.parentInfo, err = f.Stat()
	if err != nil || !os.SameFile(info, o.parentInfo) {
		return o, errors.Join(err, errors.New("companion parent changed"))
	}
	if err := o.parentBinding(); err != nil {
		return o, err
	}
	o.parentRoot, err = os.OpenRoot(o.parentPath)
	if err != nil {
		return o, err
	}
	if err := o.parentBinding(); err != nil {
		return o, err
	}
	if _, err := o.parentRoot.Lstat(o.finalName); !errors.Is(err, os.ErrNotExist) {
		return o, errors.Join(err, errors.New("companion destination already exists"))
	}
	return o, nil
}

// Namespace stability is a separate operator premise from stopping workload
// writers. Private ownership/modes fence other users, not hostile same-user
// replacement. Every observed identity loss refuses, and nothing is adopted.
func (o *companionOperation) parentBinding() error {
	info, err := o.parent.file.Stat()
	if err != nil {
		return err
	}
	uid, _, _, err := companionStat(info)
	if err != nil {
		return err
	}
	if !info.IsDir() || uid != uint32(os.Geteuid()) || info.Mode().Perm()&0022 != 0 || info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 || !os.SameFile(info, o.parentInfo) {
		return errors.New("companion publication parent must be owned and stable without group or other writes")
	}
	path, err := os.Lstat(o.parentPath)
	if err != nil {
		return err
	}
	if !os.SameFile(path, info) || path.Mode()&os.ModeSymlink != 0 {
		return errors.New("companion publication parent identity changed")
	}
	if o.parentRoot != nil {
		root, err := o.parentRoot.Lstat(".")
		if err != nil {
			return err
		}
		if !os.SameFile(root, info) {
			return errors.New("companion parent root identity changed")
		}
	}
	return nil
}

func (o *companionOperation) stageBinding() error {
	if err := o.parentBinding(); err != nil {
		return err
	}
	id, err := companionDirectoryID(o.parent.file, o.stageName)
	if err != nil {
		return err
	}
	if id != o.stageID {
		return errors.New("companion stage identity changed")
	}
	if o.stage.file != nil {
		_, actual, err := companionCheck(o.stage.file, true, 0)
		if err != nil {
			return err
		}
		if actual != id {
			return errors.New("companion stage descriptor changed")
		}
	}
	if o.stageRoot != nil {
		info, err := o.stageRoot.Lstat(".")
		if err != nil {
			return err
		}
		_, _, actual, err := companionStat(info)
		if err != nil {
			return err
		}
		if actual != id {
			return errors.New("companion stage root changed")
		}
	}
	return nil
}

func (o *companionOperation) makeStage(objects string) error {
	if err := o.checkContext(); err != nil {
		return err
	}
	if err := o.parentBinding(); err != nil {
		return err
	}
	id, err := newID()
	if err != nil {
		return err
	}
	o.stageName = ".skygo-companion-" + id
	if err := companionMkdir(o.parent.file, o.stageName, 0700); err != nil {
		return err
	}
	o.stagePath = filepath.Join(o.parentPath, o.stageName)
	o.stageID, err = companionDirectoryID(o.parent.file, o.stageName)
	if err != nil {
		return err
	}
	if err := companionChild(o.parent.file, o.stageName, os.O_RDONLY, true, &o.stage); err != nil {
		return err
	}
	o.stageRoot, err = o.parentRoot.OpenRoot(o.stageName)
	if err != nil {
		return err
	}
	if err := o.stageBinding(); err != nil {
		return err
	}
	if err := companionMkdir(o.stage.file, objects, 0700); err != nil {
		return err
	}
	return companionChild(o.stage.file, objects, os.O_RDONLY, true, &o.objects)
}

// Stage creation must not change any source namespace. Resolve ancestor
// symlinks before comparing paths; actual parent identity is checked separately.
// The stable namespace premise applies throughout these separate observations.
func (o *companionOperation) separateSources(sources ...string) error {
	parent, err := filepath.EvalSymlinks(o.parentPath)
	if err != nil {
		return err
	}
	for _, source := range sources {
		absolute, err := filepath.Abs(source)
		if err != nil {
			return err
		}
		canonical, err := filepath.EvalSymlinks(absolute)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(canonical, parent)
		if err != nil {
			return err
		}
		if relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return errors.New("companion destination parent overlaps a source directory")
		}
	}
	return o.parentBinding()
}

func (o *companionOperation) publish() error {
	if err := o.checkContext(); err != nil {
		return err
	}
	if err := o.stageBinding(); err != nil {
		return err
	}
	if err := o.settle(); err != nil {
		return err
	}
	if err := o.checkContext(); err != nil {
		return err
	}
	if err := o.stageBinding(); err != nil {
		return err
	}
	if err := companionRename(o.parent.file, o.stageName, o.finalName); err != nil {
		return err
	}
	o.published = true
	return o.parent.file.Sync()
}

type companionReader struct {
	ctx context.Context
	r   io.Reader
}

func (r companionReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if len(p) > artifactChunkSize {
		p = p[:artifactChunkSize]
	}
	return r.r.Read(p)
}
func companionRef(r ArtifactRef) artifact.Ref {
	return artifact.Ref{ID: r.ID, SHA256: r.SHA256, Size: int64(r.Size)}
}
func companionFileID(f ArtifactFile) artifact.FileID {
	return artifact.FileID{Device: f.Device, Inode: f.Inode}
}
func companionManifestDTO(m artifact.Manifest) ArtifactManifest {
	return ArtifactManifest{Reference: ArtifactRef{ID: m.Reference.ID, SHA256: m.Reference.SHA256, Size: uint64(m.Reference.Size)}, ExpandedBytes: m.ExpandedBytes, RegularFiles: m.RegularFiles, Directories: m.Directories}
}
func companionInventory(st *artifactState) []companionObject {
	out := make([]companionObject, 0, len(st.Objects))
	for _, o := range st.Objects {
		out = append(out, companionObject{o.Ref, o.File, o.Manifest})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Reference.ID < out[j].Reference.ID })
	return out
}
func companionObjectNames(objects []companionObject, marker bool) []string {
	out := make([]string, 0, len(objects)+1)
	if marker {
		out = append(out, ".skygo-artifact-root")
	}
	for _, o := range objects {
		out = append(out, o.Reference.ID+".tar")
	}
	sort.Strings(out)
	return out
}
func companionSameNames(dir *os.File, want []string) error {
	got, err := companionNames(dir, len(want))
	if err != nil {
		return err
	}
	if len(got) != len(want) {
		return errors.New("companion inventory membership differs")
	}
	for i := range got {
		if got[i] != want[i] {
			return errors.New("companion inventory contains unknown or missing entries")
		}
	}
	return nil
}
func (o *companionOperation) space(size uint64) error {
	need, err := addChecked(size, 2<<30)
	if err != nil {
		return err
	}
	free, err := filesystemFree(o.parentPath)
	if err != nil {
		return err
	}
	if free < need {
		return errors.New("insufficient companion staging space")
	}
	return nil
}

// BackupCompanion copies a stopped installation's database and complete artifact
// bytes to a new directory. The caller must stop and join all original writers;
// the exclusive database lock alone does not prove that handoff.
// The publication parent must be owned, not group/other writable, and remain
// stable under the caller's trusted namespace authority throughout the call.
func BackupCompanion(ctx context.Context, dataDir, artifactDir, outputDir string) (err error) {
	o, e := newCompanion(ctx, outputDir)
	defer func() { err = o.finish(err) }()
	if e != nil {
		return e
	}
	if dataDir == "" || artifactDir == "" {
		return errors.New("companion backup requires state and artifact directories")
	}
	if e = companionPathDir(dataDir, &o.data); e != nil {
		return e
	}
	if e = companionOpenDB(o.ctx, o.data.file, filepath.Join(dataDir, "jobs.db"), &o.sourceDB, nil); e != nil {
		return e
	}
	var st *artifactState
	var owner string
	if e = o.sourceDB.db.View(func(tx *bolt.Tx) error {
		var e error
		st, e = companionEligible(tx)
		if e == nil {
			owner = string(tx.Bucket(metadataBucket).Get([]byte("owner")))
		}
		return e
	}); e != nil {
		return e
	}
	if e = companionChild(o.data.file, "logs", os.O_RDONLY, true, &o.sourceLogs); e != nil && !errors.Is(e, os.ErrNotExist) {
		return e
	}
	if o.sourceLogs.file != nil {
		if e = companionEmpty(o.sourceLogs.file); e != nil {
			return e
		}
	}
	if e = companionPathDir(artifactDir, &o.sourceObjects); e != nil {
		return e
	}
	_, id, e := companionCheck(o.sourceObjects.file, true, 0)
	if e != nil {
		return e
	}
	o.root, e = os.OpenRoot(artifactDir)
	if e != nil {
		return e
	}
	o.files, e = artifact.Open(o.root, companionFileID(id))
	if e != nil {
		return o.artifactConstructorError(e, false)
	}
	facts, e := o.files.RootFacts()
	if e != nil {
		return e
	}
	if facts.ID != st.Root.ID || facts.Unit != st.Root.Unit || facts.Volume != st.Root.Volume {
		return errors.New("artifact root does not match stopped database")
	}
	objects := companionInventory(st)
	if e = companionSameNames(o.sourceObjects.file, companionObjectNames(objects, true)); e != nil {
		return e
	}
	var total uint64
	for _, obj := range objects {
		if e = artifactAdd(&total, obj.Reference.Size); e != nil {
			return e
		}
	}
	dbSize, e := companionDatabaseSize(&o.sourceDB)
	if e != nil {
		return e
	}
	if e = artifactAdd(&total, dbSize); e != nil {
		return e
	}
	if e = o.space(total); e != nil {
		return e
	}
	if e = o.separateSources(dataDir, artifactDir); e != nil {
		return e
	}
	if e = o.makeStage("objects"); e != nil {
		return e
	}
	if e = companionChild(o.stage.file, "jobs.db", os.O_WRONLY|os.O_CREATE|os.O_EXCL, false, &o.output); e != nil {
		return e
	}
	hash := sha256.New()
	if e = companionSnapshot(o.ctx, &o.sourceDB, dbSize, io.MultiWriter(o.output.file, hash)); e != nil {
		return e
	}
	info, e := o.output.file.Stat()
	if e != nil {
		return e
	}
	if info.Size() < 0 || uint64(info.Size()) != dbSize {
		return errors.New("companion database size changed")
	}
	if e = o.output.file.Sync(); e != nil {
		return e
	}
	if e = o.output.close(); e != nil {
		return e
	}
	manifest := companionManifest{Version: 1, Owner: owner, Root: st.Root, Facts: facts, DatabaseSize: dbSize, DatabaseSHA256: hex.EncodeToString(hash.Sum(nil)), Objects: objects}
	if e = companionChild(o.stage.file, "jobs.db", os.O_RDONLY, false, &o.input); e != nil {
		return e
	}
	copyHash := sha256.New()
	if _, e = io.CopyBuffer(copyHash, companionReader{o.ctx, o.input.file}, make([]byte, artifactChunkSize)); e != nil {
		return e
	}
	if hex.EncodeToString(copyHash.Sum(nil)) != manifest.DatabaseSHA256 {
		return errors.New("copied database digest differs")
	}
	if e = o.input.close(); e != nil {
		return e
	}
	for _, obj := range objects {
		if e = o.checkContext(); e != nil {
			return e
		}
		o.reader, e = o.files.OpenObject(companionRef(obj.Reference), companionFileID(obj.File))
		if e != nil {
			return e
		}
		if e = companionChild(o.objects.file, obj.Reference.ID+".tar", os.O_WRONLY|os.O_CREATE|os.O_EXCL, false, &o.output); e != nil {
			return e
		}
		r := companionReader{o.ctx, io.NewSectionReader(o.reader, 0, int64(obj.Reference.Size))}
		m, e := artifact.Verify(companionRef(obj.Reference), io.TeeReader(r, o.output.file))
		if e != nil {
			return e
		}
		if companionManifestDTO(m) != obj.Manifest {
			return errors.New("artifact manifest changed during backup")
		}
		if e = o.output.file.Sync(); e != nil {
			return e
		}
		if e = o.output.close(); e != nil {
			return e
		}
		if e = o.closeReader(); e != nil {
			return e
		}
		if e = o.verifyCopied(o.objects.file, obj); e != nil {
			return e
		}
	}
	// Check the same source inventory again before releasing its original lock.
	if e = companionSameNames(o.sourceObjects.file, companionObjectNames(objects, true)); e != nil {
		return e
	}
	afterFacts, e := o.files.RootFacts()
	if e != nil {
		return e
	}
	if afterFacts != facts {
		return errors.New("original artifact installation changed during backup")
	}
	if e = o.writeManifest(manifest); e != nil {
		return e
	}
	if e = o.objects.file.Sync(); e != nil {
		return e
	}
	if e = o.stage.file.Sync(); e != nil {
		return e
	}
	return o.publish()
}
func (o *companionOperation) verifyCopied(dir *os.File, obj companionObject) error {
	if err := companionChild(dir, obj.Reference.ID+".tar", os.O_RDONLY, false, &o.input); err != nil {
		return err
	}
	info, _, err := companionCheck(o.input.file, false, 0)
	if err != nil {
		return err
	}
	if uint64(info.Size()) != obj.Reference.Size {
		return errors.New("copied artifact size differs")
	}
	m, err := artifact.Verify(companionRef(obj.Reference), companionReader{o.ctx, o.input.file})
	if err == nil && companionManifestDTO(m) != obj.Manifest {
		err = errors.New("copied artifact manifest differs")
	}
	return errors.Join(err, o.input.close())
}
func (o *companionOperation) writeManifest(m companionManifest) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if len(data) > companionManifestLimit {
		return errors.New("companion manifest exceeds limit")
	}
	if err := companionChild(o.stage.file, "manifest.json", os.O_WRONLY|os.O_CREATE|os.O_EXCL, false, &o.output); err != nil {
		return err
	}
	n, err := o.output.file.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = o.output.file.Sync()
	}
	return errors.Join(err, o.output.close())
}
func (o *companionOperation) readManifest() (m companionManifest, err error) {
	if err = companionChild(o.data.file, "manifest.json", os.O_RDONLY, false, &o.input); err != nil {
		return
	}
	info, _, e := companionCheck(o.input.file, false, 1)
	if e != nil {
		return m, e
	}
	if info.Size() > companionManifestLimit {
		return m, errors.New("companion manifest exceeds limit")
	}
	data, e := io.ReadAll(io.LimitReader(companionReader{o.ctx, o.input.file}, companionManifestLimit+1))
	if e == nil && int64(len(data)) != info.Size() {
		e = errors.New("companion manifest size changed")
	}
	err = errors.Join(e, o.input.close())
	if err != nil {
		return
	}
	if err = artifactDecode(data, &m); err != nil {
		return
	}
	canonical, e := json.Marshal(m)
	if e != nil {
		return m, e
	}
	if !bytes.Equal(data, append(canonical, '\n')) || m.Version != 1 || !artifactHex(m.Owner, 32) || !artifactHex(m.DatabaseSHA256, 64) || m.DatabaseSize < 2048 || m.Facts.ID != m.Root.ID || m.Facts.Unit != m.Root.Unit || m.Facts.Volume != m.Root.Volume || m.Facts.File.Device != m.Facts.Volume || m.Facts.MetadataFiles != 2 || m.Facts.MetadataAllowance != 2*m.Facts.Unit || !artifactFileValid(ArtifactFile{m.Facts.File.Device, m.Facts.File.Inode}) {
		return m, errors.New("invalid canonical companion manifest")
	}
	return
}

// RestoreCompanion validates and relocates a complete stopped companion into a
// new data directory. Keep all original installations stopped before starting
// the restored copy: their persistent owner and artifact identities are shared.
// The publication parent must be owned, not group/other writable, and remain
// stable under the caller's trusted namespace authority throughout the call.
func RestoreCompanion(ctx context.Context, inputDir, dataDir string) (err error) {
	o, e := newCompanion(ctx, dataDir)
	defer func() { err = o.finish(err) }()
	if e != nil {
		return e
	}
	if inputDir == "" {
		return errors.New("companion restore requires input directory")
	}
	if e = companionPathDir(inputDir, &o.data); e != nil {
		return e
	}
	if e = companionSameNames(o.data.file, []string{"jobs.db", "manifest.json", "objects"}); e != nil {
		return e
	}
	manifest, e := o.readManifest()
	if e != nil {
		return e
	}
	if e = companionOpenDB(o.ctx, o.data.file, filepath.Join(inputDir, "jobs.db"), &o.sourceDB, &manifest); e != nil {
		return e
	}
	info, _, e := companionCheck(o.sourceDB.file, false, 2048)
	if e != nil {
		return e
	}
	if uint64(info.Size()) != manifest.DatabaseSize {
		return errors.New("companion database size differs")
	}
	hash := sha256.New()
	if _, e = io.CopyBuffer(hash, companionReader{o.ctx, io.NewSectionReader(o.sourceDB.file, 0, info.Size())}, make([]byte, artifactChunkSize)); e != nil {
		return e
	}
	if hex.EncodeToString(hash.Sum(nil)) != manifest.DatabaseSHA256 {
		return errors.New("companion database digest differs")
	}
	var st *artifactState
	if e = o.sourceDB.db.View(func(tx *bolt.Tx) error {
		var e error
		st, e = companionEligible(tx)
		if e != nil {
			return e
		}
		if string(tx.Bucket(metadataBucket).Get([]byte("owner"))) != manifest.Owner || st.Root != manifest.Root {
			return errors.New("companion database identity differs")
		}
		return nil
	}); e != nil {
		return e
	}
	objects := companionInventory(st)
	if len(objects) != len(manifest.Objects) {
		return errors.New("companion object inventory differs")
	}
	var total uint64
	for i, obj := range objects {
		if obj != manifest.Objects[i] {
			return errors.New("companion original object tuple differs")
		}
		if e = artifactAdd(&total, obj.Reference.Size); e != nil {
			return e
		}
	}
	if e = artifactAdd(&total, manifest.DatabaseSize); e != nil {
		return e
	}
	if e = o.space(total); e != nil {
		return e
	}
	if e = companionChild(o.data.file, "objects", os.O_RDONLY, true, &o.sourceObjects); e != nil {
		return e
	}
	if e = companionSameNames(o.sourceObjects.file, companionObjectNames(objects, false)); e != nil {
		return e
	}
	// Keep the actual locked bundle descriptor, and reject changed bytes even
	// when every individual object remains independently well formed.
	lastInfo, e := o.sourceDB.file.Stat()
	if e != nil {
		return e
	}
	if lastInfo.Size() != info.Size() {
		return errors.New("companion database changed during restore")
	}
	lastHash := sha256.New()
	if _, e = io.CopyBuffer(lastHash, companionReader{o.ctx, io.NewSectionReader(o.sourceDB.file, 0, lastInfo.Size())}, make([]byte, artifactChunkSize)); e != nil {
		return e
	}
	if hex.EncodeToString(lastHash.Sum(nil)) != manifest.DatabaseSHA256 {
		return errors.New("companion database changed during restore")
	}
	// Validate every byte before constructing a destination or rebinding identity.
	for _, obj := range objects {
		if e = o.verifyCopied(o.sourceObjects.file, obj); e != nil {
			return e
		}
	}
	if e = o.separateSources(inputDir); e != nil {
		return e
	}
	if e = o.makeStage("artifacts"); e != nil {
		return e
	}
	if e = companionChild(o.stage.file, "jobs.db", os.O_WRONLY|os.O_CREATE|os.O_EXCL, false, &o.output); e != nil {
		return e
	}
	if e = companionSnapshot(o.ctx, &o.sourceDB, manifest.DatabaseSize, o.output.file); e != nil {
		return e
	}
	if e = o.output.file.Sync(); e != nil {
		return e
	}
	if e = o.output.close(); e != nil {
		return e
	}
	if e = companionOpenDB(o.ctx, o.stage.file, filepath.Join(o.stagePath, "jobs.db"), &o.stageDB, nil); e != nil {
		return e
	}
	_, rootID, e := companionCheck(o.objects.file, true, 0)
	if e != nil {
		return e
	}
	o.newRoot, e = o.stageRoot.OpenRoot("artifacts")
	if e != nil {
		return e
	}
	facts, e := artifact.Initialize(o.newRoot, companionFileID(rootID), st.Root.ID)
	if e != nil {
		return o.artifactConstructorError(e, true)
	}
	o.newFiles, e = artifact.Open(o.newRoot, companionFileID(rootID))
	if e != nil {
		return o.artifactConstructorError(e, true)
	}
	files := make(map[string]ArtifactFile, len(objects))
	for _, obj := range objects {
		if e = o.checkContext(); e != nil {
			return e
		}
		if e = companionChild(o.sourceObjects.file, obj.Reference.ID+".tar", os.O_RDONLY, false, &o.input); e != nil {
			return e
		}
		ref := companionRef(obj.Reference)
		id, e := o.newFiles.CreatePart(ref)
		if e != nil {
			return e
		}
		var offset int64
		buf := make([]byte, artifactChunkSize)
		for offset < ref.Size {
			if e = o.checkContext(); e != nil {
				return e
			}
			n := int64(len(buf))
			if ref.Size-offset < n {
				n = ref.Size - offset
			}
			if _, e = io.ReadFull(o.input.file, buf[:int(n)]); e != nil {
				return e
			}
			if _, e = o.newFiles.WritePart(ref, id, offset, offset, buf[:int(n)]); e != nil {
				return e
			}
			offset += n
		}
		if e = o.input.close(); e != nil {
			return e
		}
		receipt, e := o.newFiles.SealPart(ref, id, func() error {
			// Offline reservation was checked before stage creation. This finite
			// resample is not a live Control physical-cache receipt.
			return o.space(0)
		})
		if e != nil {
			return e
		}
		if companionManifestDTO(receipt.Manifest) != obj.Manifest {
			return errors.New("restored artifact manifest differs")
		}
		files[obj.Reference.ID] = ArtifactFile{Device: receipt.File.Device, Inode: receipt.File.Inode}
	}
	if e = o.stageDB.db.Update(func(tx *bolt.Tx) error { return companionRebind(tx, st, facts.Unit, facts.Volume, files) }); e != nil {
		return e
	}
	if e = companionMkdir(o.stage.file, "logs", 0700); e != nil {
		return e
	}
	if e = companionChild(o.stage.file, "logs", os.O_RDONLY, true, &o.logs); e != nil {
		return e
	}
	if e = companionEmpty(o.logs.file); e != nil {
		return e
	}
	_, logsID, e := companionCheck(o.logs.file, true, 0)
	if e != nil {
		return e
	}
	if e = o.stageBinding(); e != nil {
		return e
	}
	unit, volume, _, e := outputVolume(filepath.Join(o.stagePath, "logs"), map[string]uint64{})
	if e != nil {
		return e
	}
	if e = o.stageBinding(); e != nil {
		return e
	}
	after, _, e := companionCheck(o.logs.file, true, 0)
	if e != nil {
		return e
	}
	pathInfo, e := os.Lstat(filepath.Join(o.stagePath, "logs"))
	if e != nil {
		return e
	}
	_, _, pathID, e := companionStat(pathInfo)
	if e != nil || pathID != logsID || !os.SameFile(after, pathInfo) {
		return errors.Join(e, errors.New("restored logs identity changed"))
	}
	var policy retentionState
	if e = o.stageDB.db.View(func(tx *bolt.Tx) error { var e error; policy, e = loadRetention(tx); return e }); e != nil {
		return e
	}
	if policy.Enabled && policy.Policy.LocalOutputEnabled {
		var plan outputRevalidation
		if e = o.stageDB.db.View(func(tx *bolt.Tx) error {
			var e error
			plan, e = planOutputRevalidation(tx, unit, volume, map[string]uint64{})
			return e
		}); e != nil {
			return e
		}
		need, e := revalidationSpace(plan.changedBytes)
		if e != nil {
			return e
		}
		free, e := filesystemFree(o.stagePath)
		if e != nil {
			return e
		}
		if free < need {
			return errors.New("insufficient companion output revalidation space")
		}
		if e = o.stageDB.db.Update(func(tx *bolt.Tx) error { return applyOutputRevalidation(tx, plan) }); e != nil {
			return e
		}
	}
	// The output plan saves retentionState: apply it BEFORE issuing a lineage.
	lineage, e := newID()
	if e != nil {
		return e
	}
	now := time.Now().UTC()
	if e = o.stageDB.db.Update(func(tx *bolt.Tx) error { return freshenRestoredTx(tx, lineage, now) }); e != nil {
		return e
	}
	if e = o.stageDB.db.View(validateSnapshot); e != nil {
		return e
	}
	for _, obj := range objects {
		o.reader, e = o.newFiles.OpenObject(companionRef(obj.Reference), companionFileID(files[obj.Reference.ID]))
		if e != nil {
			return e
		}
		m, e := artifact.Verify(companionRef(obj.Reference), companionReader{o.ctx, io.NewSectionReader(o.reader, 0, int64(obj.Reference.Size))})
		if e != nil {
			return e
		}
		if companionManifestDTO(m) != obj.Manifest {
			return errors.New("restored required archive differs")
		}
		if e = o.closeReader(); e != nil {
			return e
		}
	}
	if e = companionSameNames(o.objects.file, companionObjectNames(objects, true)); e != nil {
		return e
	}
	if e = companionSameNames(o.sourceObjects.file, companionObjectNames(objects, false)); e != nil {
		return e
	}
	if e = o.stageDB.db.Sync(); e != nil {
		return e
	}
	if e = o.objects.file.Sync(); e != nil {
		return e
	}
	if e = o.logs.file.Sync(); e != nil {
		return e
	}
	if e = o.stage.file.Sync(); e != nil {
		return e
	}
	return o.publish()
}
