package artifact

import (
	"errors"
	"io"
	"os"
	"sync"
)

// Reader holds one verified immutable object's exact descriptor. ReadAt is bounded
// to 64 KiB; authorization and the durable read lease are external. Close waits for
// its actual ReadAt owner before closing. A close error keeps Files busy/held.
type Reader struct {
	mu        sync.Mutex
	owner     *Files
	file      *os.File
	info      os.FileInfo
	reference Ref
	identity  FileID
	closed    bool
	closeErr  error
}

// OpenObject verifies full bytes once before returning a held Reader. It requires
// an exact trusted inode and Ref; neither an ID nor a digest grants access.
func (f *Files) OpenObject(ref Ref, id FileID) (*Reader, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.ready(); err != nil {
		return nil, err
	}
	if len(f.readers) >= maxReaders {
		return nil, ErrFilesBusy
	}
	file, info, err := f.openFile(ref, id, objectName(ref), false, 1)
	if err != nil {
		return nil, err
	}
	if _, err = f.validateFile(file, info, ref, id, objectName(ref), 1); err != nil {
		return nil, errors.Join(err, f.closeFile(file))
	}
	r := &Reader{owner: f, file: file, info: info, reference: ref, identity: id}
	f.readers[r] = struct{}{}
	return r, nil
}

// ReadAt reads only within the immutable Ref and checks observed descriptor
// metadata before and after. The caller discards p on error and reauthorizes every
// response. This is not a proof against transient hostile same-user content writes.
func (r *Reader) ReadAt(p []byte, offset int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, os.ErrClosed
	}
	if len(p) > chunkSize || offset < 0 || offset > r.reference.Size {
		return 0, ErrFileConflict
	}
	if len(p) == 0 {
		return 0, nil
	}
	if offset == r.reference.Size {
		return 0, io.EOF
	}
	if int64(len(p)) > r.reference.Size-offset {
		return 0, ErrFileConflict
	}
	r.owner.mu.Lock()
	err := r.owner.ready()
	r.owner.mu.Unlock()
	if err != nil {
		return 0, err
	}
	before, err := r.file.Stat()
	if err != nil {
		return 0, err
	}
	if !sameMetadata(before, r.info) {
		return 0, ErrFileConflict
	}
	var n int
	if r.owner.hooks.readAt != nil {
		n, err = r.owner.hooks.readAt(r.file, p, offset)
	} else {
		n, err = r.file.ReadAt(p, offset)
	}
	after, statErr := r.file.Stat()
	if statErr != nil {
		err = errors.Join(err, statErr)
	} else if !sameMetadata(after, r.info) {
		err = errors.Join(err, ErrFileConflict)
	}
	return n, err
}

// Close joins the actual read before closing its descriptor. Only successful
// close releases Files' in-process owner; metadata lease credit is still external.
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return r.closeErr
	}
	r.closeErr = r.owner.closeFile(r.file)
	r.closed = true
	if r.closeErr == nil {
		r.owner.mu.Lock()
		delete(r.owner.readers, r)
		r.owner.mu.Unlock()
	}
	return r.closeErr
}
