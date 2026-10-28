package agentbuild

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const testCommit = "0123456789abcdef0123456789abcdef01234567"

// A fakeTools answers git and go commands without running them. Go build
// writes source as the binary, standing in for a deterministic build.
type fakeTools struct {
	commit string
	status string
	source string
	cmds   []*exec.Cmd
}

func (f *fakeTools) run(cmd *exec.Cmd) error {
	f.cmds = append(f.cmds, cmd)
	switch {
	case slices.Contains(cmd.Args, "rev-parse"):
		fmt.Fprint(cmd.Stdout, f.commit)
	case slices.Contains(cmd.Args, "status"):
		fmt.Fprint(cmd.Stdout, f.status)
	case slices.Contains(cmd.Args, "build"):
		if f.source == "" {
			fmt.Fprint(cmd.Stderr, "main.go:1: syntax error")
			return errors.New("exit status 1")
		}
		out := cmd.Args[slices.Index(cmd.Args, "-o")+1]
		return os.WriteFile(out, []byte(f.source), 0o700)
	default:
		return fmt.Errorf("unexpected command %q", cmd.Args)
	}
	return nil
}

func (f *fakeTools) builds() []*exec.Cmd {
	var out []*exec.Cmd
	for _, c := range f.cmds {
		if slices.Contains(c.Args, "build") {
			out = append(out, c)
		}
	}
	return out
}

func TestBuildCommand(t *testing.T) {
	t.Setenv("GOOS", "darwin")
	t.Setenv("GOARCH", "amd64")
	t.Setenv("CGO_ENABLED", "1")
	t.Setenv("GOWORK", "/elsewhere/go.work")
	t.Setenv("GOTOOLCHAIN", "auto")
	t.Setenv("SKYGO_KEPT", "yes")
	tools := &fakeTools{commit: testCommit + "\n", source: "agent v1"}
	b := &Builder{Go: "/usr/local/go/bin/go", Git: "/usr/bin/git", Run: tools.run}
	root, cache := "/src/skygo", filepath.Join(t.TempDir(), "cache")
	got, err := b.Build(context.Background(), root, cache, false)
	if err != nil {
		t.Fatal(err)
	}
	if got.Commit != testCommit || got.Dirty || !ValidDigest(got.Digest) {
		t.Fatalf("Build = %+v", got)
	}
	if want := filepath.Join(cache, testCommit, got.Digest, "skygo-ssh-linux-arm64"); got.Path != want {
		t.Fatalf("Path = %q, want %q", got.Path, want)
	}
	if data, err := os.ReadFile(got.Path); err != nil || string(data) != "agent v1" {
		t.Fatalf("binary = %q, %v", data, err)
	}

	git := tools.cmds[0]
	if want := []string{"/usr/bin/git", "-C", root, "rev-parse", "--verify", "HEAD^{commit}"}; !slices.Equal(git.Args, want) {
		t.Errorf("git argv = %q, want %q", git.Args, want)
	}
	build := tools.builds()[0]
	out := build.Args[5]
	if want := []string{"/usr/local/go/bin/go", "build", "-trimpath", "-buildvcs=false", "-o", out, "./cmd/skygo-ssh"}; !slices.Equal(build.Args, want) {
		t.Errorf("go argv = %q, want %q", build.Args, want)
	}
	if !strings.HasPrefix(out, filepath.Join(cache, testCommit)+string(filepath.Separator)) {
		t.Errorf("build output %q is outside the cache", out)
	}
	if build.Dir != filepath.Join(root, "integrations/ssh") {
		t.Errorf("build dir = %q", build.Dir)
	}
	env := map[string][]string{}
	for _, kv := range build.Env {
		k, v, _ := strings.Cut(kv, "=")
		env[k] = append(env[k], v)
	}
	for k, v := range map[string]string{"GOOS": "linux", "GOARCH": "arm64", "CGO_ENABLED": "0", "GOWORK": "off", "GOTOOLCHAIN": "local", "SKYGO_KEPT": "yes"} {
		if !slices.Equal(env[k], []string{v}) {
			t.Errorf("env %s = %q, want exactly %q", k, env[k], v)
		}
	}
	if _, err := os.Stat(out); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("temporary build output remains: %v", err)
	}
}

func TestBuildIdentity(t *testing.T) {
	tools := &fakeTools{commit: testCommit, source: "agent v1"}
	b := &Builder{Run: tools.run}
	cache := t.TempDir()
	first, err := b.Build(context.Background(), "/src", cache, false)
	if err != nil {
		t.Fatal(err)
	}
	again, err := b.Build(context.Background(), "/src", cache, false)
	if err != nil {
		t.Fatal(err)
	}
	if again != first {
		t.Fatalf("unchanged source built %+v, then %+v", first, again)
	}

	// A changed tree under the same commit is never the earlier build.
	tools.status = " M internal/server/server.go\x00"
	tools.source = "agent v1 with local edits"
	if _, err := b.Build(context.Background(), "/src", cache, false); err == nil || !strings.Contains(err.Error(), "uncommitted changes") {
		t.Fatalf("dirty Build = %v, want refusal", err)
	}
	if len(tools.builds()) != 2 {
		t.Fatal("go build ran for a refused dirty tree")
	}
	dirty, err := b.Build(context.Background(), "/src", cache, true)
	if err != nil {
		t.Fatal(err)
	}
	if !dirty.Dirty || dirty.Commit != first.Commit || dirty.Digest == first.Digest || dirty.Path == first.Path {
		t.Fatalf("dirty build %+v shares identity with %+v", dirty, first)
	}
	if data, _ := os.ReadFile(first.Path); string(data) != "agent v1" {
		t.Fatalf("earlier build changed to %q", data)
	}
}

func TestBuildRefused(t *testing.T) {
	tests := []struct {
		name  string
		tools *fakeTools
		err   string
	}{
		{"symbolic head", &fakeTools{commit: "HEAD\n", source: "x"}, "not a full hex commit"},
		{"short commit", &fakeTools{commit: testCommit[:12], source: "x"}, "not a full hex commit"},
		{"upper case commit", &fakeTools{commit: strings.ToUpper(testCommit), source: "x"}, "not a full hex commit"},
		{"shell text", &fakeTools{commit: "$(id)" + testCommit[5:], source: "x"}, "not a full hex commit"},
		{"compile error", &fakeTools{commit: testCommit}, "syntax error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Builder{Run: tt.tools.run}
			_, err := b.Build(context.Background(), "/src", t.TempDir(), false)
			if err == nil || !strings.Contains(err.Error(), tt.err) {
				t.Fatalf("Build = %v, want %q", err, tt.err)
			}
			if tt.name != "compile error" && len(tt.tools.builds()) != 0 {
				t.Fatal("go build ran for an invalid commit")
			}
		})
	}
}

func TestFindRoot(t *testing.T) {
	root := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		name = filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(name, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("repo/go.mod", "module github.com/tmc/skygo\n\ngo 1.24.1\n")
	write("repo/integrations/ssh/go.mod", "module github.com/tmc/skygo/integrations/ssh\n")
	write("repo/integrations/ssh/cmd/skygo-ssh/main.go", "package main\n")
	write("other/go.mod", "module example.com/other\n")
	write("broken/go.mod", "module github.com/tmc/skygo\n")

	for _, start := range []string{"repo", "repo/integrations/ssh", "repo/integrations/ssh/cmd/skygo-ssh"} {
		got, err := FindRoot(filepath.Join(root, start))
		if err != nil || got != filepath.Join(root, "repo") {
			t.Errorf("FindRoot(%s) = %q, %v", start, got, err)
		}
	}
	if _, err := FindRoot(filepath.Join(root, "other")); err == nil {
		t.Error("FindRoot found a root outside any skygo checkout")
	}
	if _, err := FindRoot(filepath.Join(root, "broken")); err == nil || !strings.Contains(err.Error(), "integrations/ssh") {
		t.Errorf("FindRoot(broken) = %v, want missing SSH module", err)
	}
}
