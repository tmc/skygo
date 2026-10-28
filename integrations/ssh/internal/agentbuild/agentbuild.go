package agentbuild

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
)

const (
	rootModule = "github.com/tmc/skygo"
	sshModule  = "github.com/tmc/skygo/integrations/ssh"
	sshDir     = "integrations/ssh"
	mainPkg    = "./cmd/skygo-ssh"
	binaryName = "skygo-ssh-linux-arm64"
)

// buildEnv are the variables every build sets, replacing inherited values.
var buildEnv = []string{"GOOS=linux", "GOARCH=arm64", "CGO_ENABLED=0", "GOWORK=off", "GOTOOLCHAIN=local"}

// FindRoot returns the nearest directory at or above dir whose go.mod
// declares the skygo root module and which holds the SSH module.
func FindRoot(dir string) (string, error) {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	for d := dir; ; d = filepath.Dir(d) {
		if module(filepath.Join(d, "go.mod")) == rootModule {
			if module(filepath.Join(d, sshDir, "go.mod")) != sshModule {
				return "", fmt.Errorf("agentbuild: %s has no %s module", d, sshModule)
			}
			return d, nil
		}
		if filepath.Dir(d) == d {
			return "", fmt.Errorf("agentbuild: no %s checkout at or above %s", rootModule, dir)
		}
	}
}

// module returns the module path that the go.mod file name declares, or "".
func module(name string) string {
	f, err := os.Open(name)
	if err != nil {
		return ""
	}
	defer f.Close()
	s := bufio.NewScanner(io.LimitReader(f, 64<<10))
	for s.Scan() {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(s.Text()), "module "); ok {
			return strings.Trim(strings.TrimSpace(rest), `"`)
		}
	}
	return ""
}

// A Builder builds agents. Its zero value runs go and git from PATH.
type Builder struct {
	Go, Git string                // commands; "" means "go" and "git"
	Run     func(*exec.Cmd) error // runs each command; nil means (*exec.Cmd).Run
}

// A Build is one built agent binary.
type Build struct {
	Commit string // full hex HEAD commit of the checkout
	Dirty  bool   // the checkout had uncommitted changes
	Digest string // hex SHA-256 of the binary
	Path   string // the binary, under the cache directory
}

// Build builds the agent from the checkout at root into the private
// directory cache. A checkout with uncommitted changes is refused unless
// allowDirty is set.
func (b *Builder) Build(ctx context.Context, root, cache string, allowDirty bool) (Build, error) {
	commit, err := b.commit(ctx, root)
	if err != nil {
		return Build{}, err
	}
	dirty, err := b.dirty(ctx, root)
	if err != nil {
		return Build{}, err
	}
	if dirty && !allowDirty {
		return Build{}, fmt.Errorf("agentbuild: %s has uncommitted changes; commit them or pass --source to build the tree as is", root)
	}
	dir := filepath.Join(cache, commit)
	if err := privateDir(dir); err != nil {
		return Build{}, err
	}
	tmp, err := os.CreateTemp(dir, "build-*")
	if err != nil {
		return Build{}, fmt.Errorf("agentbuild: %w", err)
	}
	out := tmp.Name()
	tmp.Close()
	defer os.Remove(out)
	cmd := exec.CommandContext(ctx, or(b.Go, "go"), "build", "-trimpath", "-buildvcs=false", "-o", out, mainPkg)
	cmd.Dir = filepath.Join(root, sshDir)
	cmd.Env = Env(os.Environ())
	if _, err := b.output(cmd); err != nil {
		return Build{}, fmt.Errorf("agentbuild: go build: %w", err)
	}
	digest, err := fileDigest(out)
	if err != nil {
		return Build{}, err
	}
	final := filepath.Join(dir, digest, binaryName)
	if err := privateDir(filepath.Dir(final)); err != nil {
		return Build{}, err
	}
	if got, err := fileDigest(final); err != nil || got != digest {
		if err := os.Rename(out, final); err != nil {
			return Build{}, fmt.Errorf("agentbuild: %w", err)
		}
	}
	return Build{Commit: commit, Dirty: dirty, Digest: digest, Path: final}, nil
}

// Env returns env with the build variables set and every inherited value
// of them removed.
func Env(env []string) []string {
	var out []string
	for _, kv := range env {
		key, _, _ := strings.Cut(kv, "=")
		set := func(s string) bool { return strings.HasPrefix(s, key+"=") }
		if !slices.ContainsFunc(buildEnv, set) {
			out = append(out, kv)
		}
	}
	return append(out, buildEnv...)
}

func (b *Builder) commit(ctx context.Context, root string) (string, error) {
	cmd := exec.CommandContext(ctx, or(b.Git, "git"), "-C", root, "rev-parse", "--verify", "HEAD^{commit}")
	out, err := b.output(cmd)
	if err != nil {
		return "", fmt.Errorf("agentbuild: git rev-parse: %w", err)
	}
	commit := strings.TrimSuffix(string(out), "\n")
	if !ValidCommit(commit) {
		return "", fmt.Errorf("agentbuild: git rev-parse printed %q, not a full hex commit", commit)
	}
	return commit, nil
}

func (b *Builder) dirty(ctx context.Context, root string) (bool, error) {
	cmd := exec.CommandContext(ctx, or(b.Git, "git"), "-C", root, "status", "--porcelain=v1", "-z", "--untracked-files=normal")
	out, err := b.output(cmd)
	if err != nil {
		return false, fmt.Errorf("agentbuild: git status: %w", err)
	}
	return len(out) != 0, nil
}

// output runs cmd and returns its standard output. A failure includes the
// end of its standard error.
func (b *Builder) output(cmd *exec.Cmd) ([]byte, error) {
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	run := b.Run
	if run == nil {
		run = (*exec.Cmd).Run
	}
	if err := run(cmd); err != nil {
		msg := stderr.Bytes()
		if len(msg) > 4<<10 {
			msg = msg[len(msg)-4<<10:]
		}
		if s := strings.TrimSpace(string(msg)); s != "" {
			return nil, fmt.Errorf("%w\n%s", err, s)
		}
		return nil, err
	}
	return stdout.Bytes(), nil
}

// ValidCommit reports whether s is a full SHA-1 or SHA-256 hex commit.
func ValidCommit(s string) bool { return (len(s) == 40 || len(s) == 64) && lowerHex(s) }

// ValidDigest reports whether s is a hex SHA-256 digest.
func ValidDigest(s string) bool { return len(s) == 64 && lowerHex(s) }

func lowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		if !('0' <= s[i] && s[i] <= '9' || 'a' <= s[i] && s[i] <= 'f') {
			return false
		}
	}
	return true
}

func fileDigest(name string) (string, error) {
	f, err := os.Open(name)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// privateDir creates dir and its missing parents with mode 0700 and
// refuses an existing dir that is a symlink or accessible to others.
func privateDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("agentbuild: %w", err)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("agentbuild: %w", err)
	}
	if !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("agentbuild: %s must be a private directory", dir)
	}
	return nil
}

func or(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
