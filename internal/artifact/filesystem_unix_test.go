//go:build linux || darwin

package artifact

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestCollectSpecialFiles(t *testing.T) {
	for _, kind := range []string{"symlink", "hardlink", "fifo", "socket", "oversized", "setuid", "setgid", "sticky"} {
		t.Run(kind, func(t *testing.T) {
			root, name := testRoot(t)
			path := filepath.Join(name, "data")
			outside := filepath.Join(t.TempDir(), "sentinel")
			if err := os.WriteFile(outside, []byte("outside sentinel"), 0600); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "symlink":
				if err := os.Symlink(outside, path); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				writeFile(t, root, "data", "owned", 0600)
				if err := os.Link(path, filepath.Join(name, "alias")); err != nil {
					t.Fatal(err)
				}
			case "fifo":
				if err := syscall.Mkfifo(path, 0600); err != nil {
					t.Fatal(err)
				}
			case "socket":
				// Chdir is confined to this nonparallel test and restored by
				// testing after the listener's later-registered cleanup. Binding
				// a relative name avoids Unix socket address limits in long TMPDIRs;
				// collection still traverses the original absolute opened Root.
				t.Chdir(name)
				listener, err := net.Listen("unix", "data")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := listener.Close(); err != nil {
						t.Error(err)
					}
					if _, err := root.Lstat("data"); !errors.Is(err, os.ErrNotExist) {
						t.Errorf("socket not unlinked before working-directory restore: %v", err)
					}
				})
				info, err := root.Lstat("data")
				if err != nil || info.Mode()&os.ModeSocket == 0 {
					t.Fatalf("fixture is not an actual socket: %v %v", info, err)
				}
			case "oversized":
				f, err := root.OpenFile("data", os.O_CREATE|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				if err := errors.Join(f.Truncate(MaxSize+1), f.Close()); err != nil {
					t.Fatal(err)
				}
			default:
				writeFile(t, root, "data", "owned", 0600)
				mode := os.FileMode(0600)
				switch kind {
				case "setuid":
					mode |= os.ModeSetuid
				case "setgid":
					mode |= os.ModeSetgid
				case "sticky":
					mode |= os.ModeSticky
				}
				if err := os.Chmod(path, mode); err != nil {
					t.Fatal(err)
				}
			}
			var buf bytes.Buffer
			if err := Collect(root, []string{"data"}, &buf); err == nil {
				t.Fatalf("%s accepted", kind)
			}
			if bytes.Contains(buf.Bytes(), []byte("outside sentinel")) {
				t.Fatal("outside data copied")
			}
		})
	}
}

func TestCollectNonblockingReplacement(t *testing.T) {
	root, name := testRoot(t)
	writeFile(t, root, "data", "safe", 0600)
	path := filepath.Join(name, "data")
	// Force replacement AFTER regular-file Lstat and BEFORE the actual open.
	// An ordinary blocking open would wait for a writer. The production flags
	// must open nonblocking and f.Stat must reject the FIFO before any read.
	opener := func(r *os.Root, name string, flags int) (*os.File, error) {
		if name == "data" {
			if err := os.Remove(path); err != nil {
				return nil, err
			}
			if err := syscall.Mkfifo(path, 0600); err != nil {
				return nil, err
			}
		}
		return r.OpenFile(name, flags, 0)
	}
	var buf bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- collect(root, []string{"data"}, &buf, opener) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("replacement FIFO accepted")
		}
	case <-time.After(3 * time.Second):
		// A regression must not leave the test's own FIFO-open goroutine behind.
		fd, err := syscall.Open(path, syscall.O_RDWR|syscall.O_NONBLOCK, 0)
		if err != nil {
			t.Fatalf("blocked open; recovery: %v", err)
		}
		defer syscall.Close(fd)
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("owned FIFO test goroutine did not join")
		}
		t.Fatal("FIFO replacement blocked the actual open")
	}
	if buf.Len() != 0 {
		t.Fatal("special-file replacement reached content writer")
	}
}

func TestCollectInventoryBounds(t *testing.T) {
	t.Run("files", func(t *testing.T) {
		root, _ := testRoot(t)
		if err := root.Mkdir("out", 0700); err != nil {
			t.Fatal(err)
		}
		for i := 0; i <= maxFiles; i++ {
			writeFile(t, root, fmt.Sprintf("out/f%04d", i), "", 0600)
		}
		var out bytes.Buffer
		if err := Collect(root, []string{"out"}, &out); err == nil {
			t.Fatal("too many files accepted")
		}
		if out.Len() != 0 {
			t.Fatal("oversized inventory was partially published")
		}
	})
	t.Run("depth", func(t *testing.T) {
		root, _ := testRoot(t)
		path := ""
		for i := 0; i < maxDepth+1; i++ {
			if path != "" {
				path += "/"
			}
			path += "d"
			if err := root.Mkdir(path, 0700); err != nil {
				t.Fatal(err)
			}
		}
		var out bytes.Buffer
		if err := Collect(root, []string{"d"}, &out); err == nil {
			t.Fatal("deep inventory accepted")
		}
	})
	t.Run("encoded", func(t *testing.T) {
		root, _ := testRoot(t)
		f, err := root.OpenFile("data", os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if err := errors.Join(f.Truncate(MaxSize), f.Close()); err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		if err := Collect(root, []string{"data"}, &out); err == nil {
			t.Fatal("payload without header/terminator space accepted")
		}
		if out.Len() != 0 {
			t.Fatal("oversized encoded inventory reached writer")
		}
	})
	t.Run("ustar-representation", func(t *testing.T) {
		root, _ := testRoot(t)
		name := strings.Repeat("x", 101)
		writeFile(t, root, name, "safe", 0600)
		var out bytes.Buffer
		if err := Collect(root, []string{name}, &out); err == nil {
			t.Fatal("USTAR-unrepresentable basename accepted")
		}
	})
}

func TestCollectDirectoryLimit(t *testing.T) {
	root, _ := testRoot(t)
	if err := root.Mkdir("out", 0700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxDirectories; i++ {
		if err := root.Mkdir(fmt.Sprintf("out/d%04d", i), 0700); err != nil {
			t.Fatal(err)
		}
	}
	var out bytes.Buffer
	if err := Collect(root, []string{"out"}, &out); err == nil {
		t.Fatal("too many directories accepted")
	}
	if out.Len() != 0 {
		t.Fatal("oversized directory inventory reached writer")
	}
}
