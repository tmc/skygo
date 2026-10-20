package artifact

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

// Extract validates and extracts an archive into an owned private empty root.
// It creates directories 0700 and files 0600, or 0700 if any executable bit is set.
// Files and directories are synchronized before success. On error the caller
// must discard the unpublished destination before launching any process.
//
// Size is the exact encoded length. A materialization caller must independently
// compare the expected immutable digest, for example with io.TeeReader and a
// post-Extract digest check before execution; prior validation alone is not proof.
func Extract(dst *os.Root, r io.Reader, size int64) error {
	return extract(dst, r, size, extractionIO{})
}

// These optional hooks let tests report a failure from the actual owned file's
// Sync or Close without substituting a fake filesystem. A failed original Close
// retains its exact descriptor in the typed error; no completion is inferred.
type extractionIO struct {
	sync  func(*os.File) error
	close func(*os.File) error
}

func (ops extractionIO) syncFile(f *os.File) error {
	if ops.sync != nil {
		return ops.sync(f)
	}
	return f.Sync()
}
func (ops extractionIO) closeFile(f *os.File) error {
	if ops.close != nil {
		return archiveCloseResult(f, ops.close(f))
	}
	return closeArchiveFile(f)
}

func extract(dst *os.Root, r io.Reader, size int64, ops extractionIO) error {
	if dst == nil {
		return errors.New("nil artifact destination")
	}
	authority, err := rootMetadata(dst)
	if err != nil {
		return err
	}
	rootInfo, err := dst.Lstat(".")
	if err != nil {
		return err
	}
	if rootInfo.Mode().Perm()&0077 != 0 {
		return errors.New("artifact destination must be private")
	}
	flag, err := nonblockingRead()
	if err != nil {
		return err
	}
	dir, err := dst.OpenFile(".", flag, 0)
	if err != nil {
		return err
	}
	entries, readErr := dir.ReadDir(1)
	closeErr := ops.closeFile(dir)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return errors.Join(readErr, closeErr)
	}
	if closeErr != nil {
		return closeErr
	}
	if len(entries) != 0 {
		return errors.New("artifact destination is not empty")
	}
	dirs := map[string]os.FileInfo{".": rootInfo}
	buffer := make([]byte, 32<<10)
	var ensureDir func(string) error
	ensureDir = func(name string) error {
		if previous, ok := dirs[name]; ok {
			info, err := checkComponents(dst, name, authority)
			if err != nil {
				return err
			}
			if !info.IsDir() || !os.SameFile(info, previous) {
				return errors.New("artifact directory identity changed")
			}
			return nil
		}
		if at := strings.LastIndex(name, "/"); at >= 0 {
			if err := ensureDir(name[:at]); err != nil {
				return err
			}
		}
		if err := dst.Mkdir(name, 0700); err != nil {
			return fmt.Errorf("create artifact directory: %w", err)
		}
		info, err := checkComponents(dst, name, authority)
		if err != nil {
			return err
		}
		if err := checkMetadata(info, authority, true); err != nil {
			return err
		}
		dirs[name] = info
		return nil
	}
	_, err = readArchive(r, size, func(header *tar.Header, name string, data io.Reader) error {
		if header.Typeflag == tar.TypeDir {
			return ensureDir(name)
		}
		if at := strings.LastIndex(name, "/"); at >= 0 {
			if err := ensureDir(name[:at]); err != nil {
				return err
			}
		}
		mode := os.FileMode(0600)
		if header.Mode&0111 != 0 {
			mode = 0700
		}
		file, err := dst.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
		if err != nil {
			return fmt.Errorf("create artifact file: %w", err)
		}
		before, err := file.Stat()
		if err == nil {
			err = checkMetadata(before, authority, false)
		}
		if err != nil {
			return errors.Join(err, ops.closeFile(file))
		}
		n, err := io.CopyBuffer(file, io.LimitReader(data, header.Size), buffer)
		if err == nil && n != header.Size {
			err = io.ErrUnexpectedEOF
		}
		after, statErr := file.Stat()
		err = errors.Join(err, statErr)
		if statErr == nil && (!os.SameFile(before, after) || after.Size() != header.Size) {
			err = errors.Join(err, errors.New("artifact file changed while extracting"))
		}
		if err == nil {
			err = ops.syncFile(file)
		}
		err = errors.Join(err, ops.closeFile(file))
		if err != nil {
			return fmt.Errorf("extract artifact file: %w", err)
		}
		current, err := checkComponents(dst, name, authority)
		if err != nil {
			return err
		}
		if !sameMetadata(after, current) {
			return errors.New("artifact file replaced after extraction")
		}
		return nil
	}, nil)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(dirs))
	for name := range dirs {
		names = append(names, name)
	}
	// Children must be durable before the directory entries that lead to them.
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	for _, name := range names {
		if name != "." {
			if err := syncDirectory(dst, name, dirs[name], authority, ops); err != nil {
				return err
			}
		}
	}
	return syncDirectory(dst, ".", rootInfo, authority, ops)
}

func syncDirectory(root *os.Root, name string, expected os.FileInfo, authority metadata, ops extractionIO) error {
	info, err := checkComponents(root, name, authority)
	if err != nil {
		return err
	}
	if err := checkMetadata(info, authority, true); err != nil {
		return err
	}
	if !os.SameFile(info, expected) {
		return errors.New("artifact sync directory changed")
	}
	flag, err := nonblockingRead()
	if err != nil {
		return err
	}
	f, err := root.OpenFile(name, flag, 0)
	if err != nil {
		return err
	}
	actual, err := f.Stat()
	if err == nil && (!actual.IsDir() || !os.SameFile(actual, expected)) {
		err = errors.New("artifact sync inode changed")
	}
	if err == nil {
		err = ops.syncFile(f)
	}
	return errors.Join(err, ops.closeFile(f))
}
