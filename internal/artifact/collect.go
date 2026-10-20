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

// Collect writes deterministic USTAR containing 1..16 disjoint declared roots.
// Names are canonical relative paths representable by USTAR, without glob
// expansion. Unrepresentable names fail; no PAX extension is substituted. It checks regular
// file type, ownership, link count, device, size and observed metadata changes.
//
// The caller must independently establish writer quiescence and provide a private
// unpublished w. On error it must discard all bytes, not expose a partial result.
// Root confinement does not prevent a workload intentionally copying host bytes
// into its own output or prove absence of transient content changes.
func Collect(root *os.Root, names []string, w io.Writer) error { return collect(root, names, w, nil) }

type collectedEntry struct {
	name string
	info os.FileInfo
}
type collector struct {
	root               *os.Root
	authority          metadata
	entries            map[string]collectedEntry
	inodes             map[inode]bool
	files, directories int
	payload, encoded   int64
	buffer             []byte
	// Tests order the real checked complete-path open; no production substitution.
	open rootOpener
}

func collect(root *os.Root, names []string, w io.Writer, open rootOpener) error {
	if root == nil || w == nil {
		return errors.New("nil artifact collection root or writer")
	}
	if len(names) < 1 || len(names) > 16 {
		return errors.New("artifact collection requires 1..16 roots")
	}
	return collectRoots(root, names, w, open)
}

// collectRoots is shared by declared output collection and whole input snapshots.
// Callers validate their own root selection; archive/type/count/byte policy and all
// actual opens remain here. In particular only Snapshot may select an empty set.
func collectRoots(root *os.Root, names []string, w io.Writer, open rootOpener) error {
	if len(names) > maxEntries {
		return errors.New("artifact entry limit exceeded")
	}
	selected := append([]string(nil), names...)
	for _, name := range selected {
		if _, err := canonicalName(name, false); err != nil {
			return err
		}
	}
	sort.Strings(selected)
	for i := range selected {
		for j := 0; j < i; j++ {
			if selected[i] == selected[j] || strings.HasPrefix(selected[i], selected[j]+"/") {
				return errors.New("overlapping artifact output roots")
			}
		}
	}
	authority, err := rootMetadata(root)
	if err != nil {
		return err
	}
	c := &collector{root: root, authority: authority, entries: make(map[string]collectedEntry), inodes: make(map[inode]bool), encoded: 2 * blockSize, buffer: make([]byte, 32<<10), open: open}
	for _, name := range selected {
		parts := strings.Split(name, "/")
		for i := 1; i < len(parts); i++ {
			if _, err := c.add(strings.Join(parts[:i], "/"), true); err != nil {
				return err
			}
		}
		if err := c.walk(name); err != nil {
			return err
		}
	}
	manifest := make([]collectedEntry, 0, len(c.entries))
	for _, entry := range c.entries {
		manifest = append(manifest, entry)
	}
	sort.Slice(manifest, func(i, j int) bool { return manifest[i].name < manifest[j].name })
	output := &archiveWriter{writer: w}
	tw := tar.NewWriter(output)
	for _, entry := range manifest {
		directory := entry.info.IsDir()
		f, err := openCheckedWith(root, entry.name, entry.info, authority, directory, c.open)
		if err != nil {
			return err
		}
		header := &tar.Header{Name: entry.name, Mode: archiveMode(entry.info), ModTime: archiveTime, Format: tar.FormatUSTAR, Typeflag: tar.TypeReg, Size: entry.info.Size()}
		if directory {
			header.Typeflag = tar.TypeDir
			header.Name += "/"
			header.Size = 0
		}
		err = tw.WriteHeader(header)
		if err == nil && !directory {
			n, copyErr := io.CopyBuffer(tw, io.LimitReader(f, entry.info.Size()), c.buffer)
			err = copyErr
			if err == nil && n != entry.info.Size() {
				err = io.ErrUnexpectedEOF
			}
			if err == nil {
				var extra [1]byte
				n, readErr := f.Read(extra[:])
				if n != 0 || !errors.Is(readErr, io.EOF) {
					err = errors.New("artifact file size changed during collection")
				}
			}
		}
		after, statErr := f.Stat()
		err = errors.Join(err, statErr)
		if statErr == nil && !sameMetadata(after, entry.info) {
			err = errors.Join(err, errors.New("artifact metadata changed during collection"))
		}
		err = errors.Join(err, closeArchiveFile(f))
		if err != nil {
			return fmt.Errorf("collect artifact entry: %w", err)
		}
		// Reopen through the SAME original Root after copying, not a retained parent
		// descriptor or host pathname which may now name a different tree.
		check, err := openChecked(root, entry.name, entry.info, authority, directory)
		if err != nil {
			return err
		}
		if err := closeArchiveFile(check); err != nil {
			return err
		}
	}
	for _, entry := range manifest {
		if entry.info.IsDir() {
			check, err := openChecked(root, entry.name, entry.info, authority, true)
			if err != nil {
				return err
			}
			if err := closeArchiveFile(check); err != nil {
				return err
			}
		}
	}
	return tw.Close()
}

func (c *collector) add(name string, requireDirectory bool) (collectedEntry, error) {
	if _, err := canonicalName(name, false); err != nil {
		return collectedEntry{}, err
	}
	info, err := checkComponents(c.root, name, c.authority)
	if err != nil {
		return collectedEntry{}, err
	}
	if err := checkMetadata(info, c.authority, info.IsDir()); err != nil {
		return collectedEntry{}, err
	}
	if requireDirectory && !info.IsDir() {
		return collectedEntry{}, errors.New("artifact output parent is not a directory")
	}
	if old, ok := c.entries[name]; ok {
		if !sameMetadata(old.info, info) {
			return collectedEntry{}, errors.New("artifact output identity changed")
		}
		return old, nil
	}
	id, err := fileMetadata(info)
	if err != nil {
		return collectedEntry{}, err
	}
	if id.inode == c.authority.inode || c.inodes[id.inode] {
		return collectedEntry{}, errors.New("artifact output contains inode aliases")
	}
	if info.IsDir() {
		c.directories++
	} else {
		c.files++
		if info.Size() > MaxSize-c.payload {
			return collectedEntry{}, errors.New("artifact payload limit exceeded")
		}
		c.payload += info.Size()
	}
	if c.files > maxFiles || c.directories > maxDirectories || len(c.entries) >= maxEntries {
		return collectedEntry{}, errors.New("artifact entry limit exceeded")
	}
	amount := int64(blockSize)
	if !info.IsDir() {
		amount += (info.Size() + blockSize - 1) / blockSize * blockSize
	}
	if amount > MaxSize-c.encoded {
		return collectedEntry{}, errors.New("artifact encoded limit exceeded")
	}
	c.encoded += amount
	entry := collectedEntry{name, info}
	c.entries[name] = entry
	c.inodes[id.inode] = true
	return entry, nil
}

func (c *collector) walk(name string) error {
	entry, err := c.add(name, false)
	if err != nil {
		return err
	}
	if !entry.info.IsDir() {
		return nil
	}
	dir, err := openChecked(c.root, name, entry.info, c.authority, true)
	if err != nil {
		return err
	}
	for {
		children, readErr := dir.ReadDir(64)
		for _, child := range children {
			if err := c.walk(name + "/" + child.Name()); err != nil {
				return errors.Join(err, closeArchiveFile(dir))
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				return errors.Join(readErr, closeArchiveFile(dir))
			}
			break
		}
	}
	after, err := dir.Stat()
	if err == nil && !sameMetadata(after, entry.info) {
		err = errors.New("artifact directory changed during enumeration")
	}
	err = errors.Join(err, closeArchiveFile(dir))
	if err != nil {
		return err
	}
	check, err := openChecked(c.root, name, entry.info, c.authority, true)
	if err != nil {
		return err
	}
	return closeArchiveFile(check)
}

type archiveWriter struct {
	writer io.Writer
	n      int64
}

func (w *archiveWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > MaxSize-w.n {
		return 0, errors.New("artifact encoded limit exceeded")
	}
	n, err := w.writer.Write(p)
	if n < 0 || n > len(p) {
		return 0, errors.New("invalid artifact writer count")
	}
	w.n += int64(n)
	if n != len(p) && err == nil {
		err = io.ErrShortWrite
	}
	return n, err
}
