package agentbinary

import (
	"bytes"
	"context"
	"crypto/sha256"
	"debug/elf"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testCommit = "0123456789012345678901234567890123456789"

func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

// fixture consumes real compiler output. It never builds a substitute.
func fixture(t *testing.T) Spec {
	t.Helper()
	s := Spec{Path: os.Getenv("SKYGO_SSH_AGENT_BINARY"), Commit: os.Getenv("SKYGO_SSH_AGENT_COMMIT"), Digest: os.Getenv("SKYGO_SSH_AGENT_SHA256")}
	if s == (Spec{}) {
		if os.Getenv("SKYGO_REQUIRE_FIXTURES") == "1" {
			t.Fatal("required prebuilt agent fixture pins missing")
		}
		t.Skip("prebuilt agent fixture not supplied")
	}
	if err := s.Check(); err != nil {
		t.Fatal(err)
	}
	if err := verifyPath(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSpecCheck(t *testing.T) {
	good := Spec{Path: "/agent", Commit: testCommit, Digest: strings.Repeat("a", 64)}
	for _, tt := range []struct {
		name string
		edit func(*Spec)
	}{
		{"relative", func(s *Spec) { s.Path = "agent" }},
		{"unclean", func(s *Spec) { s.Path = "/a/../agent" }},
		{"commit", func(s *Spec) { s.Commit = "HEAD" }},
		{"digest", func(s *Spec) { s.Digest = "SHA256:x" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := good
			tt.edit(&s)
			if s.Check() == nil {
				t.Fatal("accepted invalid spec")
			}
		})
	}
	if err := good.Check(); err != nil {
		t.Fatal(err)
	}
}

func TestPrepareRefusals(t *testing.T) {
	for _, tt := range []struct {
		name string
		mode os.FileMode
		data []byte
		want string
	}{
		{"empty", 0555, nil, "size"}, {"writable", 0755, []byte("x"), "read-only"},
		{"nonexecutable", 0444, []byte("x"), "executable"},
		{"notELF", 0555, []byte("not an executable"), "ELF"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "agent")
			if err := os.WriteFile(path, tt.data, tt.mode); err != nil {
				t.Fatal(err)
			}
			_, err := Prepare(context.Background(), filepath.Join(dir, "cache"), Spec{Path: path, Commit: testCommit, Digest: digest(tt.data)})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Prepare=%v, want %q", err, tt.want)
			}
			files, _ := filepath.Glob(filepath.Join(dir, "cache", "prebuilt", testCommit, "*", ".agent-*"))
			if len(files) != 0 {
				t.Fatalf("temporary snapshots remain: %v", files)
			}
		})
	}
	t.Run("symlink", func(t *testing.T) {
		d := t.TempDir()
		p := filepath.Join(d, "agent")
		link := filepath.Join(d, "link")
		if err := os.WriteFile(p, []byte("x"), 0555); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(p, link); err != nil {
			t.Fatal(err)
		}
		if _, err := Prepare(context.Background(), filepath.Join(d, "cache"), Spec{Path: link, Commit: testCommit, Digest: digest([]byte("x"))}); err == nil {
			t.Fatal("accepted symlink")
		}
	})
	t.Run("oversize", func(t *testing.T) {
		d := t.TempDir()
		p := filepath.Join(d, "agent")
		f, err := os.Create(p)
		if err != nil {
			t.Fatal(err)
		}
		err = f.Truncate(maxSize + 1)
		cerr := f.Close()
		if err != nil || cerr != nil {
			t.Fatal(errors.Join(err, cerr))
		}
		if err := os.Chmod(p, 0555); err != nil {
			t.Fatal(err)
		}
		if _, err := Prepare(context.Background(), filepath.Join(d, "cache"), Spec{Path: p, Commit: testCommit, Digest: strings.Repeat("0", 64)}); err == nil {
			t.Fatal("accepted oversize")
		}
	})
	t.Run("cancel", func(t *testing.T) {
		d := t.TempDir()
		cache := filepath.Join(d, "cache")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := Prepare(ctx, cache, Spec{Path: filepath.Join(d, "absent"), Commit: testCommit, Digest: strings.Repeat("0", 64)})
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if _, err := os.Stat(cache); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("cancellation created cache")
		}
	})
}

func TestPrepareSnapshot(t *testing.T) {
	s := fixture(t)
	dir := t.TempDir()
	input := filepath.Join(dir, "input")
	b, err := os.ReadFile(s.Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(input, b, 0555); err != nil {
		t.Fatal(err)
	}
	s.Path = input
	a, err := Prepare(context.Background(), filepath.Join(dir, "cache"), s)
	if err != nil {
		t.Fatal(err)
	}
	build, err := a.Build()
	if err != nil {
		t.Fatal(err)
	}
	if build.Commit != s.Commit || build.Digest != s.Digest || build.Dirty || build.Path == s.Path {
		t.Fatalf("Build=%+v", build)
	}
	// The selected copy survives replacement of the original pathname.
	if err := os.Remove(input); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(input, []byte("replaced"), 0555); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Build(); err != nil {
		t.Fatal(err)
	}
	s.Path = fixture(t).Path
	again, err := Prepare(context.Background(), filepath.Join(dir, "cache"), s)
	if err != nil {
		t.Fatal(err)
	}
	next, err := again.Build()
	if err != nil || next != build {
		t.Fatalf("reuse=%+v,%v", next, err)
	}
	// A mismatching cached entry is refused, never overwritten.
	if err := os.Chmod(build.Path, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(build.Path, []byte("corrupt"), 0555); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(build.Path, 0555); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Build(); err == nil {
		t.Fatal("accepted corrupt snapshot")
	}
	if _, err := Prepare(context.Background(), filepath.Join(dir, "cache"), s); err == nil {
		t.Fatal("replaced corrupt snapshot")
	}
	got, err := os.ReadFile(build.Path)
	if err != nil || !bytes.Equal(got, []byte("corrupt")) {
		t.Fatal("cache changed", err)
	}
}

func TestPrepareCorruptExecutable(t *testing.T) {
	s := fixture(t)
	original, err := os.ReadFile(s.Path)
	if err != nil {
		t.Fatal(err)
	}
	// These are MODELED corruptions of actual compiler output, not other native builds.
	for _, tt := range []struct {
		name   string
		change func([]byte)
		want   string
	}{
		{"digest", func(b []byte) { b[len(b)-1] ^= 1 }, "digest"},
		{"class", func(b []byte) { b[4] = 1 }, "ELF"},
		{"architecture", func(b []byte) { b[18] = 62; b[19] = 0 }, "arm64"},
		{"load range", func(b []byte) {
			changeLoad(t, b, func(header []byte) { binary.LittleEndian.PutUint64(header[8:16], uint64(len(b))+4096) })
		}, "outside binary"},
		{"load virtual overflow", func(b []byte) {
			changeLoad(t, b, func(header []byte) { binary.LittleEndian.PutUint64(header[40:48], ^uint64(0)) })
		}, "virtual range"},
		{"load memory", func(b []byte) {
			changeLoad(t, b, func(header []byte) { binary.LittleEndian.PutUint64(header[40:48], 0) })
		}, "memory size"},
		{"entry", func(b []byte) { binary.LittleEndian.PutUint64(b[24:32], 0) }, "entry"},
		{"PIE", func(b []byte) { b[16] = 3; b[17] = 0 }, "arm64"},
		{"cgo", func(b []byte) {
			old := []byte("CGO_ENABLED=0")
			e, err := elf.NewFile(bytes.NewReader(b))
			if err != nil {
				t.Fatal(err)
			}
			section := e.Section(".go.buildinfo")
			if section == nil {
				t.Fatal("missing Go build info section")
			}
			start, end := int(section.Offset), int(section.Offset+section.Size)
			i := bytes.Index(b[start:end], old)
			if i < 0 {
				t.Fatal("missing real CGO metadata")
			}
			b[start+i+len(old)-1] = '1'
		}, "build setting"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := bytes.Clone(original)
			tt.change(b)
			p := filepath.Join(t.TempDir(), "agent")
			if err := os.WriteFile(p, b, 0555); err != nil {
				t.Fatal(err)
			}
			spec := s
			spec.Path = p
			if tt.name != "digest" {
				spec.Digest = digest(b)
			}
			_, err := Prepare(context.Background(), filepath.Join(t.TempDir(), "cache"), spec)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Prepare=%v,want %q", err, tt.want)
			}
		})
	}
}

func TestPrepareUnsafeCache(t *testing.T) {
	s := fixture(t)
	for _, kind := range []string{"public", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			d := t.TempDir()
			cache := filepath.Join(d, "cache")
			if kind == "public" {
				if err := os.Mkdir(cache, 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(cache, 0755); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Mkdir(filepath.Join(d, "other"), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(d, "other"), cache); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := Prepare(context.Background(), cache, s); err == nil {
				t.Fatal("accepted unsafe cache")
			}
		})
	}
}

// changeLoad changes a real ELF64 program header for a MODELED corruption.
func changeLoad(t *testing.T, b []byte, change func([]byte)) {
	t.Helper()
	start := int(binary.LittleEndian.Uint64(b[32:40]))
	size := int(binary.LittleEndian.Uint16(b[54:56]))
	count := int(binary.LittleEndian.Uint16(b[56:58]))
	for i := 0; i < count; i++ {
		header := b[start+i*size : start+(i+1)*size]
		if elf.ProgType(binary.LittleEndian.Uint32(header[:4])) == elf.PT_LOAD {
			change(header)
			return
		}
	}
	t.Fatal("actual compiled fixture has no load segment")
}

func TestFileCloseErrorRetainsOwner(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "file-")
	if err != nil {
		t.Fatal(err)
	}
	// MODELED preclosed input makes the production first-Close helper fail
	// through actual os.File.Close. This is not a native filesystem fault.
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	held := closeFile(f, f.Name())
	if held == nil {
		t.Fatal("missing actual Close error")
	}
	wrapped := errors.Join(errors.New("operation refused"), held)
	var got *fileCloseError
	if !errors.As(wrapped, &got) || got != held || got.files[0] != f || !errors.Is(wrapped, os.ErrClosed) {
		t.Fatal("failed Close owner or cause lost")
	}
	if _, err := os.Stat(got.temporary); err != nil {
		t.Fatal("snapshot removed", err)
	}
}
