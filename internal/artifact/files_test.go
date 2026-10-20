package artifact

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func filesFixture(t *testing.T) (*Files, *os.Root, string) {
	t.Helper()
	root, name := testRoot(t)
	info, err := root.Lstat(".")
	if err != nil {
		t.Fatal(err)
	}
	m, err := fileMetadata(info)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Initialize(root, fileID(m), strings.Repeat("c", 32)); err != nil {
		t.Fatal(err)
	}
	files, err := Open(root, fileID(m))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := files.Close(); err != nil {
			t.Error(err)
		}
	})
	return files, root, name
}
func filesRef(id string, data []byte) Ref {
	h := sha256.Sum256(data)
	return Ref{ID: id, SHA256: hex.EncodeToString(h[:]), Size: int64(len(data))}
}
func completePart(t *testing.T, f *Files, ref Ref, data []byte) FileID {
	t.Helper()
	id, err := f.CreatePart(ref)
	if err != nil {
		t.Fatal(err)
	}
	for offset := 0; offset < len(data); {
		end := min(offset+chunkSize, len(data))
		if _, err := f.WritePart(ref, id, int64(offset), int64(offset), data[offset:end]); err != nil {
			t.Fatal(err)
		}
		offset = end
	}
	return id
}
func TestFilesUploadDownload(t *testing.T) {
	f, root, _ := filesFixture(t)
	data := testArchive(t, testEntry{name: "dir/file", body: "trusted bytes"}, testEntry{name: "dir/", directory: true})
	ref := filesRef(strings.Repeat("1", 32), data)
	id := completePart(t, f, ref, data)
	receipt, err := f.SealPart(ref, id, testPublicationGrowth)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.File != id || receipt.Manifest.Reference != ref || receipt.Manifest.ExpandedBytes != 13 || receipt.Manifest.RegularFiles != 1 || receipt.Manifest.Directories != 1 {
		t.Fatalf("manifest=%+v", receipt)
	}
	if _, err := root.Lstat(partName(ref)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("part not durably removed: %v", err)
	}
	reader, err := f.OpenObject(ref, id)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(data))
	if n, err := reader.ReadAt(got, 0); err != nil || n != len(data) || !bytes.Equal(got, data) {
		t.Fatalf("read=%d %v", n, err)
	}
	if err := f.RemoveObject(ref, id); !errors.Is(err, ErrFilesBusy) {
		t.Fatalf("read lease bypass=%v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.RemoveObject(ref, id); err != nil {
		t.Fatal(err)
	}
	if err := f.RemoveObject(ref, id); err != nil {
		t.Fatalf("durable absence retry: %v", err)
	}
}
func TestFilesChunkRulesAndRecovery(t *testing.T) {
	f, root, _ := filesFixture(t)
	data := testArchive(t, testEntry{name: "file", body: "payload"})
	ref := filesRef(strings.Repeat("2", 32), data)
	id, err := f.CreatePart(ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WritePart(ref, id, 0, 0, data[:64]); err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int{0, 16} {
		receipt, err := f.WritePart(ref, id, 64, int64(offset), data[offset:offset+16])
		if err != nil || receipt.File != id || receipt.Offset != int64(offset) {
			t.Fatalf("retry=%+v %v", receipt, err)
		}
	}
	for _, test := range []struct {
		ack, offset int64
		p           []byte
	}{{64, 65, data[:1]}, {64, 63, data[:2]}, {64, 0, []byte("different")}, {64, 64, make([]byte, chunkSize+1)}} {
		if _, err := f.WritePart(ref, id, test.ack, test.offset, test.p); err == nil {
			t.Fatalf("invalid chunk accepted: %+v", test)
		}
	}
	file, err := root.OpenFile(partName(ref), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.WriteAt([]byte("unacknowledged suffix"), 64)
	if err := errors.Join(writeErr, file.Close()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WritePart(ref, id, 64, 64, data[64:80]); err == nil {
		t.Fatal("extra suffix silently treated as acknowledged")
	}
	if err := f.RecoverPart(ref, id, 64); err != nil {
		t.Fatal(err)
	}
	if info, err := root.Stat(partName(ref)); err != nil || info.Size() != 64 {
		t.Fatalf("recovery size=%v %v", info, err)
	}
	if err := f.RecoverPart(ref, id, 65); err == nil {
		t.Fatal("missing durable prefix accepted")
	}
	if _, err := f.WritePart(ref, id, 64, 64, data[64:]); err != nil {
		t.Fatal(err)
	}
	if _, err := f.SealPart(ref, id, testPublicationGrowth); err != nil {
		t.Fatal(err)
	}
}
func TestFilesPublicationCrashPrefixes(t *testing.T) {
	for _, stage := range []string{"linked", "unlinked"} {
		t.Run(stage, func(t *testing.T) {
			f, root, _ := filesFixture(t)
			data := testArchive(t, testEntry{name: "file", body: "payload"})
			ref := filesRef(strings.Repeat("3", 32), data)
			id := completePart(t, f, ref, data)
			injected := errors.New("injected directory sync failure")
			calls := 0
			f.hooks.sync = func(file *os.File) error {
				err := file.Sync()
				if file == f.directory {
					calls++
					if stage == "linked" && calls == 1 || stage == "unlinked" && calls == 2 {
						return errors.Join(err, injected)
					}
				}
				return err
			}
			if receipt, err := f.SealPart(ref, id, testPublicationGrowth); !errors.Is(err, injected) || receipt.Manifest.Reference.ID != "" {
				t.Fatalf("false completion=%+v %v", receipt, err)
			}
			f.hooks = filesIO{}
			if stage == "linked" {
				part, err := root.Stat(partName(ref))
				if err != nil {
					t.Fatal(err)
				}
				obj, err := root.Stat(objectName(ref))
				if err != nil || !os.SameFile(part, obj) {
					t.Fatal("not actual two-link prefix")
				}
			} else {
				if _, err := root.Lstat(partName(ref)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("not actual unlinked prefix")
				}
			}
			receipt, err := f.SealPart(ref, id, testPublicationGrowth)
			if err != nil || receipt.File != id {
				t.Fatalf("prefix reconciliation=%+v %v", receipt, err)
			}
		})
	}
}
func TestFilesNoAdoptionOrOverwrite(t *testing.T) {
	f, root, name := filesFixture(t)
	data := testArchive(t, testEntry{name: "file", body: "payload"})
	ref := filesRef(strings.Repeat("4", 32), data)
	id := completePart(t, f, ref, data)
	if _, err := f.CreatePart(ref); err == nil {
		t.Fatal("existing part adopted")
	}
	wrong := id
	wrong.Inode++
	if err := f.RecoverPart(ref, wrong, 0); err == nil {
		t.Fatal("wrong inode admitted")
	}
	writeFile(t, root, objectName(ref), "foreign sentinel", 0600)
	if _, err := f.SealPart(ref, id, testPublicationGrowth); err == nil {
		t.Fatal("foreign object overwritten")
	}
	if string(readFile(t, root, objectName(ref))) != "foreign sentinel" {
		t.Fatal("foreign bytes changed")
	}
	if err := f.RemoveObject(ref, id); err == nil {
		t.Fatal("foreign object deleted")
	}
	outside := filepath.Join(t.TempDir(), "sentinel")
	if err := os.WriteFile(outside, []byte("outside sentinel"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := root.Remove(objectName(ref)); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(name, objectName(ref))); err != nil {
		t.Fatal(err)
	}
	if _, err := f.OpenObject(ref, id); err == nil {
		t.Fatal("symlink object opened")
	}
	if err := f.RemoveObject(ref, id); err == nil {
		t.Fatal("unowned symlink deletion accepted")
	}
	got, err := os.ReadFile(outside)
	if err != nil || string(got) != "outside sentinel" {
		t.Fatal("outside sentinel changed")
	}
}
func TestFilesRootAndDigest(t *testing.T) {
	f, root, name := filesFixture(t)
	data := testArchive(t, testEntry{name: "file", body: "payload"})
	ref := filesRef(strings.Repeat("5", 32), data)
	id := completePart(t, f, ref, data)
	wrong := ref
	wrong.SHA256 = strings.Repeat("0", 64)
	if _, err := f.SealPart(wrong, id, testPublicationGrowth); err == nil {
		t.Fatal("wrong digest published")
	}
	moved := name + "-moved"
	if err := os.Rename(name, moved); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(moved) })
	if err := os.Mkdir(name, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := f.SealPart(ref, id, testPublicationGrowth); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(name, objectName(ref))); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("replacement root pathname used")
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := root.Lstat(objectName(ref)); err != nil {
		t.Fatalf("Files closed caller Root: %v", err)
	}
	if _, err := Open(root, FileID{Device: f.identity.Device, Inode: f.identity.Inode + 1}); err == nil {
		t.Fatal("wrong root identity accepted")
	}
}
func TestFilesActualIOFailures(t *testing.T) {
	for _, stage := range []string{"create-sync", "chunk-write", "chunk-sync", "seal-close", "delete-sync"} {
		t.Run(stage, func(t *testing.T) {
			f, root, _ := filesFixture(t)
			data := testArchive(t, testEntry{name: "file", body: "payload"})
			ref := filesRef(strings.Repeat("6", 32), data)
			injected := errors.New("injected actual owned I/O error")
			if stage == "create-sync" {
				f.hooks.sync = func(file *os.File) error { return errors.Join(file.Sync(), injected) }
				id, err := f.CreatePart(ref)
				f.hooks = filesIO{}
				if !errors.Is(err, injected) || !id.valid() {
					t.Fatalf("lost created ownership=%+v %v", id, err)
				}
				if err := f.RemovePart(ref, id); err != nil {
					t.Fatal(err)
				}
				return
			}
			id, err := f.CreatePart(ref)
			if err != nil {
				t.Fatal(err)
			}
			if stage == "chunk-write" {
				f.hooks.writeAt = func(file *os.File, p []byte, offset int64) (int, error) {
					n, err := file.WriteAt(p[:1], offset)
					return n, errors.Join(err, injected)
				}
			}
			if stage == "chunk-sync" {
				f.hooks.sync = func(file *os.File) error { return errors.Join(file.Sync(), injected) }
			}
			if stage == "chunk-write" || stage == "chunk-sync" {
				receipt, err := f.WritePart(ref, id, 0, 0, data)
				f.hooks = filesIO{}
				if !errors.Is(err, injected) || receipt.SHA256 != "" {
					t.Fatalf("false ack=%+v %v", receipt, err)
				}
				if err := f.RecoverPart(ref, id, 0); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := f.WritePart(ref, id, 0, 0, data); err != nil {
				t.Fatal(err)
			}
			if stage == "seal-close" {
				f.hooks.close = func(file *os.File) error { return errors.Join(file.Close(), injected) }
			}
			_, err = f.SealPart(ref, id, testPublicationGrowth)
			f.hooks = filesIO{}
			if stage == "seal-close" {
				if !errors.Is(err, injected) || !errors.Is(err, ErrFileCloseUncertain) {
					t.Fatal("close uncertainty lost")
				}
				if _, err = f.SealPart(ref, id, testPublicationGrowth); !errors.Is(err, ErrFilesBusy) {
					t.Fatal("replacement after uncertain close")
				}
				discardTestClosedOwners(t, f)
			} else if err != nil {
				t.Fatal(err)
			}
			if stage == "delete-sync" {
				f.hooks.sync = func(file *os.File) error { return errors.Join(file.Sync(), injected) }
				if err := f.RemoveObject(ref, id); !errors.Is(err, injected) {
					t.Fatal("delete sync failure credited")
				}
				f.hooks = filesIO{}
				if _, err := root.Lstat(objectName(ref)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("not actual unlink prefix")
				}
				if err := f.RemoveObject(ref, id); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
func TestFilesReaderBoundsAndClose(t *testing.T) {
	f, _, _ := filesFixture(t)
	data := testArchive(t, testEntry{name: "file", body: "payload"})
	ref := filesRef(strings.Repeat("7", 32), data)
	id := completePart(t, f, ref, data)
	if _, err := f.SealPart(ref, id, testPublicationGrowth); err != nil {
		t.Fatal(err)
	}
	r, err := f.OpenObject(ref, id)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		p      []byte
		offset int64
	}{{make([]byte, chunkSize+1), 0}, {make([]byte, 1), -1}, {make([]byte, 2), ref.Size - 1}} {
		if _, err := r.ReadAt(test.p, test.offset); err == nil {
			t.Fatal("unbounded read admitted")
		}
	}
	if _, err := r.ReadAt(make([]byte, 1), ref.Size); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if err := f.Close(); !errors.Is(err, ErrFilesBusy) {
		t.Fatalf("close before reader completion=%v", err)
	}
	if _, err := r.ReadAt(make([]byte, 1), 0); !errors.Is(err, ErrFilesClosed) {
		t.Fatal("shutdown admitted a new read")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestFilesReaderCloseJoinsActualRead(t *testing.T) {
	f, _, _ := filesFixture(t)
	data := testArchive(t, testEntry{name: "file", body: "payload"})
	ref := filesRef(strings.Repeat("8", 32), data)
	id := completePart(t, f, ref, data)
	if _, err := f.SealPart(ref, id, testPublicationGrowth); err != nil {
		t.Fatal(err)
	}
	r, err := f.OpenObject(ref, id)
	if err != nil {
		t.Fatal(err)
	}
	ready, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(resume) }) }
	f.hooks.readAt = func(file *os.File, p []byte, offset int64) (int, error) {
		close(ready)
		<-resume
		return file.ReadAt(p, offset)
	}
	readDone, closeDone := make(chan error, 1), make(chan error, 1)
	readFinished, closeFinished := make(chan struct{}), make(chan struct{})
	closeStarted := false
	defer func() {
		release()
		select {
		case <-readFinished:
		case <-time.After(3 * time.Second):
			t.Error("owned read did not join")
		}
		if closeStarted {
			select {
			case <-closeFinished:
			case <-time.After(3 * time.Second):
				t.Error("owned close did not join")
			}
		} else {
			if err := r.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	go func() { defer close(readFinished); _, err := r.ReadAt(make([]byte, 16), 0); readDone <- err }()
	select {
	case <-ready:
	case err := <-readDone:
		t.Fatalf("actual read barrier not reached: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("read barrier timeout")
	}
	closeStarted = true
	go func() { defer close(closeFinished); closeDone <- r.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("closed while actual read owner active: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	release()
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
}

func TestFilesReaderCountBound(t *testing.T) {
	f, _, _ := filesFixture(t)
	data := testArchive(t, testEntry{name: "file"})
	ref := filesRef(strings.Repeat("9", 32), data)
	id := completePart(t, f, ref, data)
	if _, err := f.SealPart(ref, id, testPublicationGrowth); err != nil {
		t.Fatal(err)
	}
	var readers []*Reader
	defer func() {
		for _, r := range readers {
			if err := r.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	for i := 0; i < maxReaders; i++ {
		r, err := f.OpenObject(ref, id)
		if err != nil {
			t.Fatal(err)
		}
		readers = append(readers, r)
	}
	if _, err := f.OpenObject(ref, id); !errors.Is(err, ErrFilesBusy) {
		t.Fatalf("unbounded descriptors: %v", err)
	}
}

// Explicit unit-fixture admission. Production callers must instead provide the
// actual fresh cached Control check; this file is never linked into production.
func testPublicationGrowth() error { return nil }
