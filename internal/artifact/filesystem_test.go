package artifact

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func testRoot(t *testing.T) (*os.Root, string) {
	t.Helper()
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("filesystem qualification requires Linux or Darwin")
	}
	name := t.TempDir()
	if err := os.Chmod(name, 0700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	})
	return root, name
}
func writeFile(t *testing.T, root *os.Root, name, body string, mode os.FileMode) {
	t.Helper()
	f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := io.WriteString(f, body)
	if err := errors.Join(writeErr, f.Close()); err != nil {
		t.Fatal(err)
	}
}
func readFile(t *testing.T, root *os.Root, name string) []byte {
	t.Helper()
	f, err := root.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(f)
	if err := errors.Join(readErr, f.Close()); err != nil {
		t.Fatal(err)
	}
	return data
}

func TestFilesystemRoundTrip(t *testing.T) {
	src, _ := testRoot(t)
	for _, name := range []string{"out", "out/empty", "out/sub"} {
		if err := src.Mkdir(name, 0700); err != nil {
			t.Fatal(err)
		}
	}
	writeFile(t, src, "out/sub/data", "hello", 0600)
	writeFile(t, src, "out/run", "#!/bin/sh\n", 0700)
	var first, second bytes.Buffer
	if err := Collect(src, []string{"out"}, &first); err != nil {
		t.Fatal(err)
	}
	if err := Collect(src, []string{"out"}, &second); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Bytes(), second.Bytes()) {
		t.Fatal("collection is not deterministic")
	}
	digest, err := Validate(bytes.NewReader(first.Bytes()), int64(first.Len()))
	if err != nil {
		t.Fatal(err)
	}
	dst, _ := testRoot(t)
	h := sha256.New()
	if err := Extract(dst, io.TeeReader(bytes.NewReader(first.Bytes()), h), int64(first.Len())); err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(h.Sum(nil)) != digest {
		t.Fatal("materialized encoded digest differs")
	}
	if got := string(readFile(t, dst, "out/sub/data")); got != "hello" {
		t.Fatalf("data=%q", got)
	}
	for _, test := range []struct {
		name string
		mode os.FileMode
	}{{"out", 0700}, {"out/empty", 0700}, {"out/sub/data", 0600}, {"out/run", 0700}} {
		info, err := dst.Stat(test.name)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != test.mode {
			t.Fatalf("%s mode=%o", test.name, info.Mode().Perm())
		}
	}
}

func TestExtractRefusesUnsafeArchive(t *testing.T) {
	root, rootName := testRoot(t)
	outside := filepath.Join(filepath.Dir(rootName), "outside")
	t.Cleanup(func() { os.Remove(outside) })
	if err := os.WriteFile(outside, []byte("unchanged sentinel"), 0600); err != nil {
		t.Fatal(err)
	}
	data := changeArchive(t, func(h []byte) { rawString(h[:100], "../outside") })
	if err := Extract(root, bytes.NewReader(data), int64(len(data))); err == nil {
		t.Fatal("escaping path accepted")
	}
	got, err := os.ReadFile(outside)
	if err != nil || string(got) != "unchanged sentinel" {
		t.Fatalf("outside changed: %q %v", got, err)
	}
	// An invalid trailer is detected even after a valid entry has been written.
	// The partial owned destination is unpublished and must be discarded.
	data = testArchive(t, testEntry{name: "safe", body: "private"})
	data = append(data, 1)
	if err := Extract(root, bytes.NewReader(data), int64(len(data))); err == nil {
		t.Fatal("invalid trailing domain accepted")
	}
	if got := string(readFile(t, root, "safe")); got != "private" {
		t.Fatalf("partial fixture=%q", got)
	}
}

func TestExtractDestination(t *testing.T) {
	data := testArchive(t, testEntry{name: "file", body: "replacement"})
	t.Run("not-empty", func(t *testing.T) {
		root, _ := testRoot(t)
		writeFile(t, root, "file", "sentinel", 0600)
		if err := Extract(root, bytes.NewReader(data), int64(len(data))); err == nil {
			t.Fatal("nonempty destination accepted")
		}
		if string(readFile(t, root, "file")) != "sentinel" {
			t.Fatal("existing file overwritten")
		}
	})
	t.Run("not-private", func(t *testing.T) {
		root, name := testRoot(t)
		if err := os.Chmod(name, 0755); err != nil {
			t.Fatal(err)
		}
		if err := Extract(root, bytes.NewReader(data), int64(len(data))); err == nil {
			t.Fatal("public destination accepted")
		}
	})
	t.Run("implicit-parent", func(t *testing.T) {
		root, _ := testRoot(t)
		data := testArchive(t, testEntry{name: "a/b", body: "ok"}, testEntry{name: "a/", directory: true})
		if err := Extract(root, bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("case-alias", func(t *testing.T) {
		root, _ := testRoot(t)
		writeFile(t, root, "probe", "x", 0600)
		_, err := root.Lstat("PROBE")
		folds := err == nil
		if err := root.Remove("probe"); err != nil {
			t.Fatal(err)
		}
		data := testArchive(t, testEntry{name: "a", body: "one"}, testEntry{name: "A", body: "two"})
		err = Extract(root, bytes.NewReader(data), int64(len(data)))
		if folds && err == nil {
			t.Fatal("case alias overwrote existing inode")
		}
		if !folds && err != nil {
			t.Fatal(err)
		}
		if string(readFile(t, root, "a")) != "one" {
			t.Fatal("first inode overwritten")
		}
	})
}

func TestRootRename(t *testing.T) {
	for _, extract := range []bool{false, true} {
		t.Run(fmt.Sprint(extract), func(t *testing.T) {
			root, name := testRoot(t)
			if !extract {
				writeFile(t, root, "data", "original safe bytes", 0600)
			}
			moved := name + "-moved"
			if err := os.Rename(name, moved); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { os.RemoveAll(moved) })
			if err := os.Mkdir(name, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(name, "data"), []byte("replacement outside sentinel"), 0600); err != nil {
				t.Fatal(err)
			}
			if extract {
				data := testArchive(t, testEntry{name: "data", body: "original safe bytes"})
				if err := Extract(root, bytes.NewReader(data), int64(len(data))); err != nil {
					t.Fatal(err)
				}
			} else {
				var buf bytes.Buffer
				if err := Collect(root, []string{"data"}, &buf); err != nil {
					t.Fatal(err)
				}
				if bytes.Contains(buf.Bytes(), []byte("replacement outside sentinel")) {
					t.Fatal("used replaced root pathname")
				}
			}
			if string(readFile(t, root, "data")) != "original safe bytes" {
				t.Fatal("lost original root anchor")
			}
			got, err := os.ReadFile(filepath.Join(name, "data"))
			if err != nil || string(got) != "replacement outside sentinel" {
				t.Fatalf("replacement tree changed: %q %v", got, err)
			}
		})
	}
}

func TestCollectDeclaredRoots(t *testing.T) {
	root, _ := testRoot(t)
	if err := root.Mkdir("a", 0700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, root, "a/file", "safe", 0600)
	writeFile(t, root, "a-b", "safe", 0600)
	for _, names := range [][]string{nil, {"."}, {"a/"}, {"../a"}, {"missing"}, {"a", "a"}, {"a", "a/file"}, {"a", "a-b", "a/file"}, strings.Fields(strings.Repeat("a ", 17))} {
		var out bytes.Buffer
		if err := Collect(root, names, &out); err == nil {
			t.Fatalf("invalid roots accepted: %q", names)
		}
	}
	var out bytes.Buffer
	if err := Collect(root, []string{"a/file", "a-b"}, &out); err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(bytes.NewReader(out.Bytes()))
	var names []string
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, h.Name)
	}
	if strings.Join(names, ",") != "a/,a-b,a/file" {
		t.Fatalf("manifest=%q", names)
	}
}

type failingWriter struct{ short bool }

func (w failingWriter) Write(p []byte) (int, error) {
	if w.short {
		return len(p) - 1, nil
	}
	return 0, errors.New("injected output failure")
}
func TestCollectWriteFailure(t *testing.T) {
	root, _ := testRoot(t)
	writeFile(t, root, "data", "safe", 0600)
	for _, short := range []bool{false, true} {
		if err := Collect(root, []string{"data"}, failingWriter{short}); err == nil {
			t.Fatal("writer failure accepted")
		}
	}
}

// Each barrier is inside the real complete-path opener, after enumeration and
// component checks. Replacement is performed by the test goroutine, not a mock
// rejection. Every opened descriptor is still checked before a content read.
func TestCollectReplacement(t *testing.T) {
	for _, intermediate := range []bool{false, true} {
		for _, afterOpen := range []bool{false, true} {
			t.Run(fmt.Sprintf("parent=%v/after-open=%v", intermediate, afterOpen), func(t *testing.T) {
				root, name := testRoot(t)
				outside := t.TempDir()
				sentinel := "NEVER COLLECT OUTSIDE SENTINEL"
				if err := root.Mkdir("dir", 0700); err != nil {
					t.Fatal(err)
				}
				writeFile(t, root, "dir/data", "safe owned bytes", 0600)
				if err := os.WriteFile(filepath.Join(outside, "data"), []byte(sentinel), 0600); err != nil {
					t.Fatal(err)
				}
				ready, resume := make(chan struct{}), make(chan struct{})
				var release sync.Once
				unblock := func() { release.Do(func() { close(resume) }) }
				defer unblock()
				var once sync.Once
				opener := func(r *os.Root, path string, flags int) (*os.File, error) {
					var f *os.File
					var err error
					if afterOpen {
						f, err = r.OpenFile(path, flags, 0)
					}
					if path == "dir/data" {
						once.Do(func() { close(ready); <-resume })
					}
					if !afterOpen {
						f, err = r.OpenFile(path, flags, 0)
					}
					return f, err
				}
				var out bytes.Buffer
				done := make(chan error, 1)
				finished := make(chan struct{})
				go func() { defer close(finished); done <- collect(root, []string{"dir/data"}, &out, opener) }()
				defer func() {
					unblock()
					select {
					case <-finished:
					case <-time.After(3 * time.Second):
						t.Error("owned collection goroutine did not finish")
					}
				}()
				select {
				case <-ready:
				case err := <-done:
					t.Fatalf("did not reach actual open barrier: %v", err)
				case <-time.After(3 * time.Second):
					t.Fatal("open barrier timed out")
				}
				if intermediate {
					if err := os.Rename(filepath.Join(name, "dir"), filepath.Join(outside, "moved")); err != nil {
						unblock()
						<-done
						t.Fatal(err)
					}
					if err := os.Symlink(outside, filepath.Join(name, "dir")); err != nil {
						unblock()
						<-done
						t.Fatal(err)
					}
				} else {
					if err := os.Rename(filepath.Join(name, "dir/data"), filepath.Join(name, "dir/original")); err != nil {
						unblock()
						<-done
						t.Fatal(err)
					}
					if err := os.Symlink(filepath.Join(outside, "data"), filepath.Join(name, "dir/data")); err != nil {
						unblock()
						<-done
						t.Fatal(err)
					}
				}
				unblock()
				select {
				case err := <-done:
					if err == nil {
						t.Fatal("observed replacement accepted")
					}
				case <-time.After(3 * time.Second):
					t.Fatal("collection did not join")
				}
				if bytes.Contains(out.Bytes(), []byte(sentinel)) {
					t.Fatal("outside bytes escaped through collection")
				}
			})
		}
	}
}

type callbackWriter struct {
	bytes.Buffer
	callback func()
}

func (w *callbackWriter) Write(p []byte) (int, error) {
	if w.callback != nil {
		f := w.callback
		w.callback = nil
		f()
	}
	return w.Buffer.Write(p)
}
func TestCollectHeldFileReplacement(t *testing.T) {
	root, name := testRoot(t)
	outside := filepath.Join(t.TempDir(), "data")
	sentinel := "outside held-file sentinel"
	writeFile(t, root, "data", "safe original bytes", 0600)
	if err := os.WriteFile(outside, []byte(sentinel), 0600); err != nil {
		t.Fatal(err)
	}
	out := &callbackWriter{callback: func() {
		if err := os.Rename(filepath.Join(name, "data"), filepath.Join(name, "original")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(name, "data")); err != nil {
			t.Fatal(err)
		}
	}}
	if err := Collect(root, []string{"data"}, out); err == nil {
		t.Fatal("replacement after checked open accepted")
	}
	if bytes.Contains(out.Bytes(), []byte(sentinel)) {
		t.Fatal("read replaced pathname instead of held descriptor")
	}
	if !bytes.Contains(out.Bytes(), []byte("safe original bytes")) {
		t.Fatal("test did not exercise actual held descriptor read")
	}
}

func TestCollectMutation(t *testing.T) {
	for _, changeSize := range []bool{false, true} {
		t.Run(fmt.Sprint(changeSize), func(t *testing.T) {
			root, name := testRoot(t)
			writeFile(t, root, "data", "original bytes", 0600)
			out := &callbackWriter{callback: func() {
				f, err := os.OpenFile(filepath.Join(name, "data"), os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				var writeErr error
				if changeSize {
					writeErr = f.Truncate(0)
				} else {
					_, writeErr = f.WriteAt([]byte("changed!"), 0)
					if writeErr == nil {
						writeErr = os.Chtimes(filepath.Join(name, "data"), time.Unix(1, 0), time.Unix(1, 0))
					}
				}
				if err := errors.Join(writeErr, f.Close()); err != nil {
					t.Fatal(err)
				}
			}}
			if err := Collect(root, []string{"data"}, out); err == nil {
				t.Fatal("file mutation after checked open accepted")
			}
		})
	}
}

func TestCollectOpenFailure(t *testing.T) {
	root, _ := testRoot(t)
	writeFile(t, root, "data", "safe", 0600)
	injected := errors.New("injected complete-path open failure")
	calls := 0
	opener := func(*os.Root, string, int) (*os.File, error) { calls++; return nil, injected }
	var out bytes.Buffer
	if err := collect(root, []string{"data"}, &out, opener); !errors.Is(err, injected) || calls != 1 || out.Len() != 0 {
		t.Fatalf("open fault err=%v calls=%d bytes=%d", err, calls, out.Len())
	}
}

func TestCollectCopiedContentBoundary(t *testing.T) {
	root, _ := testRoot(t)
	outside := []byte("intentionally copied same-user content")
	writeFile(t, root, "data", string(outside), 0600)
	var out bytes.Buffer
	if err := Collect(root, []string{"data"}, &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out.Bytes(), outside) {
		t.Fatal("test did not copy owned regular-file bytes")
	}
	// This is permitted content, not a path-resolution escape or secrecy promise.
}

func TestCollectRepeatedSwap(t *testing.T) {
	root, name := testRoot(t)
	if err := root.Mkdir("dir", 0700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, root, "dir/data", "safe bytes", 0600)
	outside := filepath.Join(t.TempDir(), "data")
	sentinel := "repeated-swap outside sentinel"
	if err := os.WriteFile(outside, []byte(sentinel), 0600); err != nil {
		t.Fatal(err)
	}
	path, saved := filepath.Join(name, "dir/data"), filepath.Join(name, "dir/saved")
	request, restore, stop := make(chan struct{}), make(chan struct{}), make(chan struct{})
	swapped, restored := make(chan error, 1), make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for {
			select {
			case <-stop:
				return
			case <-request:
			}
			err := os.Rename(path, saved)
			if err == nil {
				err = os.Symlink(outside, path)
			}
			swapped <- err
			select {
			case <-restore:
			case <-stop:
				os.Remove(path)
				os.Rename(saved, path)
				return
			}
			err = errors.Join(os.Remove(path), os.Rename(saved, path))
			restored <- err
		}
	}()
	defer func() {
		close(stop)
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("owned swap goroutine did not join")
		}
	}()
	attempts := 0
	opener := func(r *os.Root, path string, flags int) (*os.File, error) {
		if path == "dir/data" {
			request <- struct{}{}
			if err := <-swapped; err != nil {
				return nil, err
			}
			attempts++
		}
		return r.OpenFile(path, flags, 0)
	}
	for i := 0; i < 16; i++ {
		var out bytes.Buffer
		before := attempts
		err := collect(root, []string{"dir/data"}, &out, opener)
		if attempts == before {
			t.Fatalf("iteration %d did not reach actual replacement open: %v", i, err)
		}
		restore <- struct{}{}
		if restoreErr := <-restored; restoreErr != nil {
			t.Fatal(restoreErr)
		}
		if err == nil {
			t.Fatalf("iteration %d accepted observed symlink replacement", i)
		}
		if bytes.Contains(out.Bytes(), []byte(sentinel)) {
			t.Fatalf("iteration %d copied outside bytes", i)
		}
	}
	if attempts != 16 {
		t.Fatalf("actual complete-path attempts=%d", attempts)
	}
}
