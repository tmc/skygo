package artifact

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSnapshotEmpty(t *testing.T) {
	root, _ := testRoot(t)
	var archive bytes.Buffer
	if err := Snapshot(root, &archive); err != nil {
		t.Fatal(err)
	}
	if archive.Len() != 1024 || !zeroBytes(archive.Bytes()) {
		t.Fatalf("empty archive has %d noncanonical bytes", archive.Len())
	}
	if digest, err := Validate(bytes.NewReader(archive.Bytes()), int64(archive.Len())); err != nil || len(digest) != 64 {
		t.Fatalf("empty validation=%q %v", digest, err)
	}
	dst, _ := testRoot(t)
	if err := Extract(dst, bytes.NewReader(archive.Bytes()), int64(archive.Len())); err != nil {
		t.Fatal(err)
	}
}
func TestSnapshotAllTopLevelRoots(t *testing.T) {
	root, _ := testRoot(t)
	var names []string
	for i := 0; i < 32; i++ {
		name := fmt.Sprintf("file%02d", i)
		writeFile(t, root, name, name, 0600)
		names = append(names, name)
	}
	if err := root.Mkdir("empty", 0700); err != nil {
		t.Fatal(err)
	}
	var archive, again bytes.Buffer
	if err := Snapshot(root, &archive); err != nil {
		t.Fatal(err)
	}
	if err := Snapshot(root, &again); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(archive.Bytes(), again.Bytes()) {
		t.Fatal("snapshot not deterministic")
	}
	tr := tar.NewReader(bytes.NewReader(archive.Bytes()))
	files, dirs := 0, 0
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if h.Typeflag == tar.TypeDir {
			dirs++
		} else {
			files++
		}
	}
	if files != 32 || dirs != 1 {
		t.Fatalf("snapshot files=%d dirs=%d", files, dirs)
	}
	if err := Collect(root, names, io.Discard); err == nil {
		t.Fatal("input API weakened output16-root constraint")
	}
	if err := Collect(root, []string{"."}, io.Discard); err == nil {
		t.Fatal("input API added output dot-root syntax")
	}
}
func TestSnapshotInventoryLimits(t *testing.T) {
	for _, kind := range []string{"files", "directories", "top-level"} {
		t.Run(kind, func(t *testing.T) {
			root, _ := testRoot(t)
			files, dirs := 0, 0
			switch kind {
			case "files":
				files = maxFiles + 1
			case "directories":
				dirs = maxDirectories + 1
			case "top-level":
				files = maxFiles
				dirs = maxDirectories + 1
			}
			for i := 0; i < files; i++ {
				writeFile(t, root, fmt.Sprintf("f%04d", i), "", 0600)
			}
			for i := 0; i < dirs; i++ {
				if err := root.Mkdir(fmt.Sprintf("d%04d", i), 0700); err != nil {
					t.Fatal(err)
				}
			}
			var out bytes.Buffer
			if err := Snapshot(root, &out); err == nil {
				t.Fatal("oversized inventory accepted")
			}
			if out.Len() != 0 {
				t.Fatal("oversized inventory reached archive writer")
			}
		})
	}
}
func TestSnapshotRootMembershipChange(t *testing.T) {
	root, _ := testRoot(t)
	writeFile(t, root, "original", "safe", 0600)
	writer := &callbackWriter{callback: func() { writeFile(t, root, "unselected", "added after enumeration", 0600) }}
	if err := Snapshot(root, writer); err == nil {
		t.Fatal("observed root membership change accepted")
	}
	// The shared writer may have finished a valid tar before the final root
	// check. Non-nil Snapshot error still requires discarding every byte.
	if writer.Len() == 0 {
		t.Fatal("fixture never reached actual archive write")
	}
}
func TestSnapshotReplacement(t *testing.T) {
	for _, parent := range []bool{false, true} {
		t.Run(fmt.Sprint(parent), func(t *testing.T) {
			root, name := testRoot(t)
			outside := t.TempDir()
			sentinel := "snapshot outside sentinel"
			if err := root.Mkdir("dir", 0700); err != nil {
				t.Fatal(err)
			}
			writeFile(t, root, "dir/file", "safe original", 0600)
			if err := os.WriteFile(filepath.Join(outside, "file"), []byte(sentinel), 0600); err != nil {
				t.Fatal(err)
			}
			ready, resume, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			var once sync.Once
			release := func() { once.Do(func() { close(resume) }) }
			defer func() {
				release()
				select {
				case <-finished:
				case <-time.After(3 * time.Second):
					t.Error("owned snapshot did not join")
				}
			}()
			opener := func(r *os.Root, path string, flags int) (*os.File, error) {
				if path == "dir/file" {
					close(ready)
					<-resume
				}
				return r.OpenFile(path, flags, 0)
			}
			var out bytes.Buffer
			go func() { defer close(finished); done <- snapshot(root, &out, opener) }()
			select {
			case <-ready:
			case err := <-done:
				t.Fatalf("did not reach real complete-path open: %v", err)
			case <-time.After(3 * time.Second):
				t.Fatal("open barrier timeout")
			}
			if parent {
				if err := os.Rename(filepath.Join(name, "dir"), filepath.Join(outside, "moved")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, filepath.Join(name, "dir")); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Rename(filepath.Join(name, "dir/file"), filepath.Join(name, "dir/original")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(outside, "file"), filepath.Join(name, "dir/file")); err != nil {
					t.Fatal(err)
				}
			}
			release()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("observed replacement accepted")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("snapshot completion timeout")
			}
			if bytes.Contains(out.Bytes(), []byte(sentinel)) {
				t.Fatal("outside bytes entered snapshot")
			}
		})
	}
}
func TestSnapshotRootRename(t *testing.T) {
	root, name := testRoot(t)
	writeFile(t, root, "file", "original root bytes", 0600)
	moved := name + "-moved"
	if err := os.Rename(name, moved); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(moved) })
	if err := os.Mkdir(name, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(name, "file"), []byte("replacement pathname sentinel"), 0600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := Snapshot(root, &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out.Bytes(), []byte("original root bytes")) || bytes.Contains(out.Bytes(), []byte("replacement pathname sentinel")) {
		t.Fatal("snapshot lost original opened root authority")
	}
}
func TestSnapshotValidationAndWriterFault(t *testing.T) {
	root, _ := testRoot(t)
	writeFile(t, root, "file", "safe", 0600)
	if err := Snapshot(nil, io.Discard); err == nil {
		t.Fatal("nil root accepted")
	}
	if err := Snapshot(root, nil); err == nil {
		t.Fatal("nil writer accepted")
	}
	if err := Snapshot(root, failingWriter{}); err == nil {
		t.Fatal("writer failure accepted")
	}
	long := strings.Repeat("x", maxName+1)
	writeFile(t, root, long, "too long", 0600)
	var out bytes.Buffer
	if err := Snapshot(root, &out); err == nil {
		t.Fatal("invalid immediate name accepted")
	}
	if out.Len() != 0 {
		t.Fatal("invalid root name reached archive writer")
	}
}
