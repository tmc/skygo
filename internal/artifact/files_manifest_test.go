package artifact

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestFilesManifestSameParser(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(map[bool]string{false: "implicit-directories", true: "duplicate-policy"}[invalid], func(t *testing.T) {
			f, root, _ := filesFixture(t)
			entries := []testEntry{{name: "a/b/c", body: "counted"}}
			if invalid {
				entries = append(entries, testEntry{name: "a/b/c", body: "again"})
			}
			data := testArchive(t, entries...)
			ref := filesRef(strings.Repeat("a", 32), data)
			id := completePart(t, f, ref, data)
			receipt, err := f.SealPart(ref, id, testPublicationGrowth)
			if invalid {
				if err == nil || receipt.Manifest.Reference.ID != "" {
					t.Fatalf("invalid archive produced manifest: %+v %v", receipt, err)
				}
				if _, err := root.Lstat(objectName(ref)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("invalid archive published")
				}
			} else if err != nil || receipt.Manifest.RegularFiles != 1 || receipt.Manifest.Directories != 2 || receipt.Manifest.ExpandedBytes != 7 {
				t.Fatalf("wrong parser counts: %+v %v", receipt, err)
			}
		})
	}
	previous := archiveSummary{expanded: 123, files: 4, directories: 5}
	summary := previous
	if _, err := readArchive(bytes.NewReader(make([]byte, 1024)), 1025, nil, &summary); err == nil || summary != previous {
		t.Fatalf("failed parser elevated stale summary: %+v %v", summary, err)
	}
}

func TestFilesCloseFailureKeepsProtection(t *testing.T) {
	root, _ := testRoot(t)
	info, err := root.Lstat(".")
	if err != nil {
		t.Fatal(err)
	}
	m, err := fileMetadata(info)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Initialize(root, fileID(m), strings.Repeat("d", 32)); err != nil {
		t.Fatal(err)
	}
	f, err := Open(root, fileID(m))
	if err != nil {
		t.Fatal(err)
	}
	data := testArchive(t, testEntry{name: "file"})
	ref := filesRef(strings.Repeat("b", 32), data)
	id := completePart(t, f, ref, data)
	if _, err := f.SealPart(ref, id, testPublicationGrowth); err != nil {
		t.Fatal(err)
	}
	reader, err := f.OpenObject(ref, id)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected close uncertainty")
	f.hooks.close = func(file *os.File) error { return errors.Join(file.Close(), injected) }
	if err := reader.Close(); !errors.Is(err, injected) {
		t.Fatal("reader close failure not reported")
	}
	if _, err := reader.file.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("fixture did not close actual descriptor: %v", err)
	}
	if len(f.readers) != 1 {
		t.Fatal("uncertain close silently released owner")
	}
	if err := f.Close(); !errors.Is(err, ErrFilesBusy) {
		t.Fatalf("uncertainty credited: %v", err)
	}
	// Test-owned final disposal is not a production reconciliation receipt: the
	// injected callback already closed the exact reader FD, and this fixture is
	// discarded. Close only the remaining original directory descriptor directly.
	f.hooks = filesIO{}
	if err := f.directory.Close(); err != nil {
		t.Fatal(err)
	}
}
