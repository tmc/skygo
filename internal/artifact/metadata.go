package artifact

import (
	"errors"
	"os"
	"strings"
	"time"
)

type inode struct{ device, number uint64 }
type metadata struct {
	inode
	uid                   uint32
	links                 uint64
	changeSec, changeNsec int64
}

func sameMetadata(a, b os.FileInfo) bool {
	x, err := fileMetadata(a)
	if err != nil {
		return false
	}
	y, err := fileMetadata(b)
	if err != nil {
		return false
	}
	return x == y && a.Size() == b.Size() && a.Mode() == b.Mode() && a.ModTime().Equal(b.ModTime())
}

func rootMetadata(root *os.Root) (metadata, error) {
	info, err := root.Lstat(".")
	if err != nil {
		return metadata{}, err
	}
	m, err := fileMetadata(info)
	if err != nil {
		return metadata{}, err
	}
	if !info.IsDir() || m.uid != uint32(os.Geteuid()) {
		return metadata{}, errors.New("artifact root must be an owned directory")
	}
	return m, nil
}

func checkMetadata(info os.FileInfo, root metadata, directory bool) error {
	m, err := fileMetadata(info)
	if err != nil {
		return err
	}
	if m.device != root.device || m.uid != uint32(os.Geteuid()) || info.Mode()&(os.ModeSymlink|os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
		return errors.New("unsupported artifact file ownership or mode")
	}
	if directory {
		if !info.IsDir() {
			return errors.New("artifact parent is not a directory")
		}
	} else if !info.Mode().IsRegular() || m.links != 1 || info.Size() < 0 || info.Size() > MaxSize {
		return errors.New("artifact requires a bounded single-link regular file")
	}
	return nil
}

func checkComponents(root *os.Root, name string, authority metadata) (os.FileInfo, error) {
	parts := strings.Split(name, "/")
	var info os.FileInfo
	for i := range parts {
		current := strings.Join(parts[:i+1], "/")
		var err error
		info, err = root.Lstat(current)
		if err != nil {
			return nil, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("artifact path contains a symlink")
		}
		if i < len(parts)-1 {
			if err := checkMetadata(info, authority, true); err != nil {
				return nil, err
			}
		}
	}
	return info, nil
}

// openChecked uses the COMPLETE root-relative path through the original Root.
// No intermediate Root/host pathname becomes an authority after rename.
type rootOpener func(*os.Root, string, int) (*os.File, error)

func openChecked(root *os.Root, name string, expected os.FileInfo, authority metadata, directory bool) (*os.File, error) {
	return openCheckedWith(root, name, expected, authority, directory, nil)
}

func openCheckedWith(root *os.Root, name string, expected os.FileInfo, authority metadata, directory bool, open rootOpener) (*os.File, error) {
	before, err := checkComponents(root, name, authority)
	if err != nil {
		return nil, err
	}
	if !sameMetadata(before, expected) {
		return nil, errors.New("artifact path changed before open")
	}
	flag, err := nonblockingRead()
	if err != nil {
		return nil, err
	}
	var f *os.File
	if open != nil {
		f, err = open(root, name, flag)
	} else {
		f, err = root.OpenFile(name, flag, 0)
	}
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err == nil {
		err = checkMetadata(info, authority, directory)
	}
	if err == nil && !sameMetadata(info, expected) {
		err = errors.New("artifact inode changed during open")
	}
	after, pathErr := checkComponents(root, name, authority)
	if err == nil {
		err = pathErr
	}
	if err == nil && !sameMetadata(info, after) {
		err = errors.New("artifact path changed during open")
	}
	if err != nil {
		return nil, errors.Join(err, closeArchiveFile(f))
	}
	return f, nil
}

func archiveMode(info os.FileInfo) int64 {
	if info.IsDir() || info.Mode().Perm()&0111 != 0 {
		return 0700
	}
	return 0600
}

var archiveTime = time.Unix(0, 0).UTC()
