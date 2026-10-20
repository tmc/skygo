package artifact

import (
	"errors"
	"os"
)

// ErrFileCloseUncertain means this operation attempted to close an original file
// but did not prove closure. The caller must retain that same operation's owner,
// I/O permit and durable protection. It must not replace it, retry a raw numeric
// descriptor, infer completion from timeout or release storage reservations.
// Ordinary validation/write/sync failures whose actual Close succeeded do not
// carry this sentinel. Use errors.Is; errors may include other failures as well.
var ErrFileCloseUncertain = errors.New("artifact original file closure is uncertain")

// FileCloseError retains the exact original file and Files owner privately. It
// cannot be constructed as a proof or used to retry Close after FD reuse. A
// stopped, independently authorized reconciliation is outside this API. Keep the
// error until that handoff; discarding it does not make the operation joined.
type FileCloseError struct {
	owner *Files
	file  *os.File
	err   error
}

func (e *FileCloseError) Error() string        { return "close artifact file: " + e.err.Error() }
func (e *FileCloseError) Unwrap() error        { return e.err }
func (e *FileCloseError) Is(target error) bool { return target == ErrFileCloseUncertain }

func (f *Files) hasUncertainClose() bool {
	f.closeMu.Lock()
	defer f.closeMu.Unlock()
	return len(f.uncertain) != 0
}

// This separate mutex is never held while acquiring f.mu. Reader.Close owns its
// own read join before entering here; ordinary file operations already hold mu.
// After the first uncertain Close, new admission is fenced. Already admitted
// Readers/candidates are bounded and may still join their original descriptors.
func (f *Files) closeFile(file *os.File) error {
	f.closeMu.Lock()
	if previous := f.uncertain[file]; previous != nil {
		f.closeMu.Unlock()
		return previous
	}
	f.closeMu.Unlock()
	var err error
	if f.hooks.close != nil {
		err = f.hooks.close(file)
	} else {
		err = file.Close()
	}
	if err == nil {
		return nil
	}
	failure := &FileCloseError{owner: f, file: file, err: err}
	f.closeMu.Lock()
	if f.uncertain == nil {
		f.uncertain = make(map[*os.File]*FileCloseError)
	}
	f.uncertain[file] = failure
	f.closeMu.Unlock()
	return failure
}

// archiveCloseResult retains an archive helper's own original descriptor in the
// returned error, without inventing Files registry ownership. The caller keeps
// its original root/operation permit and this error through stopped handoff.
func archiveCloseResult(file *os.File, err error) error {
	if err == nil {
		return nil
	}
	return &FileCloseError{file: file, err: err}
}
func closeArchiveFile(file *os.File) error { return archiveCloseResult(file, file.Close()) }
