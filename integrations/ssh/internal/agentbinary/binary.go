package agentbinary

import (
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"debug/elf"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/tmc/skygo/integrations/ssh/internal/agentbuild"
)

const maxSize = 256 << 20
const binaryName = "skygo-ssh-linux-arm64"

// A Spec names a local agent binary and the operator's assertion of its
// clean source commit. Digest is the SHA-256 of the actual binary bytes.
type Spec struct{ Path, Commit, Digest string }

// Check checks the shape of s without reading files or running commands.
func (s Spec) Check() error {
	if !filepath.IsAbs(s.Path) || filepath.Clean(s.Path) != s.Path {
		return errors.New("agentbinary: binary must be a clean absolute path")
	}
	if !agentbuild.ValidCommit(s.Commit) {
		return errors.New("agentbinary: commit must be a full lowercase hex commit")
	}
	if !agentbuild.ValidDigest(s.Digest) {
		return errors.New("agentbinary: digest must be a lowercase SHA-256")
	}
	return nil
}

// An Artifact is a verified private snapshot. Only Prepare makes a usable
// Artifact. Its zero value names nothing. The cache must remain trusted:
// read-only permissions do not protect it from another process of this user.
type Artifact struct{ spec Spec }

// IsZero reports whether a names no snapshot.
func (a Artifact) IsZero() bool { return a == Artifact{} }

// Build rechecks the snapshot and returns the build the bootstrap can use.
// Commit is the operator's assertion, not an attestation of source provenance.
func (a Artifact) Build() (agentbuild.Build, error) {
	if a.IsZero() {
		return agentbuild.Build{}, errors.New("agentbinary: zero artifact")
	}
	if err := verifyPath(a.spec); err != nil {
		return agentbuild.Build{}, err
	}
	return agentbuild.Build{Path: a.spec.Path, Commit: a.spec.Commit, Digest: a.spec.Digest}, nil
}

// Prepare copies s into cache/prebuilt/COMMIT/DIGEST, verifies that copy,
// and publishes it read-only without replacing any existing file. Cache
// must be a clean absolute path in a trusted private directory. Prepare
// runs no commands. It accepts static Linux arm64 Go executables only.
func Prepare(ctx context.Context, cache string, s Spec) (Artifact, error) {
	if err := s.Check(); err != nil {
		return Artifact{}, err
	}
	if !filepath.IsAbs(cache) || filepath.Clean(cache) != cache {
		return Artifact{}, errors.New("agentbinary: cache must be a clean absolute path")
	}
	if err := ctx.Err(); err != nil {
		return Artifact{}, err
	}
	dir := cache
	if err := os.MkdirAll(dir, 0700); err != nil {
		return Artifact{}, fmt.Errorf("agentbinary: cache: %w", err)
	}
	if err := privateDir(dir); err != nil {
		return Artifact{}, err
	}
	for _, part := range []string{"prebuilt", s.Commit, s.Digest} {
		dir = filepath.Join(dir, part)
		if err := os.Mkdir(dir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return Artifact{}, fmt.Errorf("agentbinary: cache: %w", err)
		}
		if err := privateDir(dir); err != nil {
			return Artifact{}, err
		}
	}
	tmp, err := copyVerified(ctx, dir, s)
	if err != nil {
		return Artifact{}, err
	}
	defer os.Remove(tmp)
	if err := ctx.Err(); err != nil {
		return Artifact{}, err
	}
	final := Spec{Path: filepath.Join(dir, binaryName), Commit: s.Commit, Digest: s.Digest}
	// Link is atomic and does not replace an existing directory entry.
	if err := os.Link(tmp, final.Path); err != nil && !errors.Is(err, os.ErrExist) {
		return Artifact{}, fmt.Errorf("agentbinary: publish: %w", err)
	}
	if err := verifyPath(final); err != nil {
		return Artifact{}, err
	}
	return Artifact{spec: final}, nil
}

func privateDir(path string) error {
	st, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("agentbinary: cache: %w", err)
	}
	if !st.IsDir() || st.Mode().Perm()&0077 != 0 {
		return errors.New("agentbinary: cache directory must be private and not a symlink")
	}
	return nil
}

// A fileCloseError keeps the actual files whose first Close failed. No
// retry or cleanup removes their snapshot; callers retain the error until
// they have resolved that ownership.
type fileCloseError struct {
	files     []*os.File
	causes    []error
	temporary string
}

func (e *fileCloseError) Error() string {
	return "agentbinary: file close failed; file and snapshot retained"
}
func (e *fileCloseError) Unwrap() []error { return e.causes }

// closeFile is the one first Close of f. On failure, the returned error
// retains the exact file, its original cause and its snapshot name.
func closeFile(f *os.File, temporary string) *fileCloseError {
	if err := f.Close(); err != nil {
		return &fileCloseError{files: []*os.File{f}, causes: []error{err}, temporary: temporary}
	}
	return nil
}

func openBinary(path string) (*os.File, os.FileInfo, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, nil, fmt.Errorf("agentbinary: binary: %w", err)
	}
	if err := checkFile(before); err != nil {
		return nil, nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("agentbinary: open binary: %w", err)
	}
	after, err := f.Stat()
	if err == nil {
		err = checkFile(after)
	}
	if err == nil && !os.SameFile(before, after) {
		err = errors.New("binary changed while opening")
	}
	if err != nil {
		cause := fmt.Errorf("agentbinary: binary: %w", err)
		if cerr := closeFile(f, path); cerr != nil {
			return nil, nil, errors.Join(cause, cerr)
		}
		return nil, nil, cause
	}
	return f, after, nil
}

func checkFile(st os.FileInfo) error {
	if !st.Mode().IsRegular() || st.Mode().Perm()&0222 != 0 || st.Mode().Perm()&0111 == 0 {
		return errors.New("agentbinary: binary must be a regular read-only executable, not a symlink")
	}
	if st.Size() <= 0 || st.Size() > maxSize {
		return errors.New("agentbinary: binary size must be in 1..256 MiB")
	}
	return nil
}

func copyVerified(ctx context.Context, dir string, s Spec) (name string, err error) {
	// Cleanup runs after both first Close calls. A failed Close retains
	// the original file and snapshot instead of deleting under that owner.
	var held *fileCloseError
	keepClose := func(f *os.File) {
		cause := closeFile(f, name)
		if cause == nil {
			return
		}
		if held == nil {
			held = new(fileCloseError)
		}
		held.files = append(held.files, cause.files...)
		held.causes = append(held.causes, cause.causes...)
	}
	defer func() {
		if held != nil {
			held.temporary = name
			err = errors.Join(err, held)
			name = ""
			return
		}
		if err != nil && name != "" {
			err = errors.Join(err, os.Remove(name))
			name = ""
		}
	}()
	src, before, err := openBinary(s.Path)
	if err != nil {
		return "", err
	}
	defer func() { keepClose(src) }()
	out, err := os.CreateTemp(dir, ".agent-*")
	if err != nil {
		return "", fmt.Errorf("agentbinary: snapshot: %w", err)
	}
	name = out.Name()
	defer func() { keepClose(out) }()
	buf := make([]byte, 32<<10)
	var size int64
	for {
		if err = ctx.Err(); err != nil {
			return name, err
		}
		n, rerr := src.Read(buf)
		if n > 0 {
			size += int64(n)
			if size > maxSize {
				return name, errors.New("agentbinary: binary exceeds 256 MiB")
			}
			if _, err = out.Write(buf[:n]); err != nil {
				return name, fmt.Errorf("agentbinary: snapshot: %w", err)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return name, fmt.Errorf("agentbinary: read binary: %w", rerr)
		}
		if n == 0 {
			return name, io.ErrNoProgress
		}
	}
	after, err := src.Stat()
	if err != nil {
		return name, err
	}
	current, err := os.Lstat(s.Path)
	if err != nil {
		return name, err
	}
	if size != before.Size() || after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) || !os.SameFile(before, current) || current.Mode() != before.Mode() {
		return name, errors.New("agentbinary: binary changed while copying")
	}
	if err = validate(out, size, s); err != nil {
		return name, err
	}
	if err = ctx.Err(); err != nil {
		return name, err
	}
	if err = out.Chmod(0555); err != nil {
		return name, err
	}
	if err = out.Sync(); err != nil {
		return name, err
	}
	return name, nil
}

func verifyPath(s Spec) (err error) {
	f, st, err := openBinary(s.Path)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := closeFile(f, s.Path); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()
	return validate(f, st.Size(), s)
}

func validate(f *os.File, size int64, s Spec) error {
	h := sha256.New()
	if n, err := io.Copy(h, io.NewSectionReader(f, 0, size)); err != nil || n != size {
		return fmt.Errorf("agentbinary: hash snapshot: %w", errors.Join(err, io.ErrUnexpectedEOF))
	}
	if hex.EncodeToString(h.Sum(nil)) != s.Digest {
		return errors.New("agentbinary: binary digest mismatch")
	}
	e, err := elf.NewFile(f)
	if err != nil {
		return fmt.Errorf("agentbinary: ELF: %w", err)
	}
	if e.Class != elf.ELFCLASS64 || e.Data != elf.ELFDATA2LSB || e.Type != elf.ET_EXEC || e.Machine != elf.EM_AARCH64 {
		return errors.New("agentbinary: binary must be a static Linux arm64 ELF executable")
	}
	entryMapped := false
	for _, p := range e.Progs {
		if p.Off > uint64(size) || p.Filesz > uint64(size)-p.Off {
			return errors.New("agentbinary: ELF program outside binary")
		}
		if p.Type == elf.PT_LOAD {
			if p.Memsz > ^uint64(0)-p.Vaddr {
				return errors.New("agentbinary: ELF virtual range overflows")
			}
			if p.Memsz < p.Filesz {
				return errors.New("agentbinary: ELF load size exceeds memory size")
			}
			if p.Align > 1 && (p.Align&(p.Align-1) != 0 || p.Vaddr%p.Align != p.Off%p.Align) {
				return errors.New("agentbinary: invalid ELF load alignment")
			}
			if p.Flags&elf.PF_X != 0 && e.Entry >= p.Vaddr && e.Entry-p.Vaddr < p.Filesz {
				entryMapped = true
			}
		}
		if p.Type == elf.PT_INTERP {
			return errors.New("agentbinary: binary has an interpreter")
		}
		if p.Type == elf.PT_DYNAMIC {
			if p.Off > uint64(size) || p.Filesz > uint64(size)-p.Off || p.Filesz%16 != 0 {
				return errors.New("agentbinary: invalid dynamic program")
			}
			r := p.Open()
			var entry [16]byte
			for left := p.Filesz; left > 0; left -= 16 {
				if _, err := io.ReadFull(r, entry[:]); err != nil {
					return fmt.Errorf("agentbinary: dynamic program: %w", err)
				}
				if elf.DynTag(binary.LittleEndian.Uint64(entry[:8])) == elf.DT_NEEDED {
					return errors.New("agentbinary: binary has dynamic dependencies")
				}
			}
		}
	}
	if !entryMapped {
		return errors.New("agentbinary: ELF entry is not in an executable load segment")
	}
	needed, err := e.DynValue(elf.DT_NEEDED)
	if err != nil {
		return fmt.Errorf("agentbinary: ELF dependencies: %w", err)
	}
	if len(needed) != 0 {
		return errors.New("agentbinary: binary has dynamic dependencies")
	}
	info, err := buildinfo.Read(f)
	if err != nil {
		return fmt.Errorf("agentbinary: Go build info: %w", err)
	}
	if info.Path != "github.com/tmc/skygo/integrations/ssh/cmd/skygo-ssh" || info.Main.Path != "github.com/tmc/skygo/integrations/ssh" {
		return errors.New("agentbinary: binary is not the skygo-ssh command")
	}
	want := map[string]string{"GOOS": "linux", "GOARCH": "arm64", "CGO_ENABLED": "0"}
	seen := make(map[string]bool)
	for _, setting := range info.Settings {
		expected, required := want[setting.Key]
		if !required && setting.Key != "vcs.revision" && setting.Key != "vcs.modified" {
			continue
		}
		if seen[setting.Key] {
			return errors.New("agentbinary: duplicate Go build setting")
		}
		seen[setting.Key] = true
		if required && setting.Value != expected {
			return errors.New("agentbinary: incompatible Go build setting")
		}
		if setting.Key == "vcs.revision" && setting.Value != s.Commit {
			return errors.New("agentbinary: embedded commit contradicts supplied commit")
		}
		if setting.Key == "vcs.modified" && setting.Value != "false" {
			return errors.New("agentbinary: embedded checkout is not clean")
		}
	}
	for key := range want {
		if !seen[key] {
			return errors.New("agentbinary: missing Go build setting")
		}
	}
	return nil
}
