package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestWorkspacePathExpansion(t *testing.T) {
	dir := workspaceTestDirectory(t)
	t.Setenv("HOME", dir)
	for _, tc := range []struct{ in, want string }{{"~", dir}, {"~/source", filepath.Join(dir, "source")}, {dir, dir}} {
		got, err := resolvedWorkspacePath(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("%q: %q %v", tc.in, got, err)
		}
	}
	for _, in := range []string{"", "~another", "~another/source"} {
		if _, err := resolvedWorkspacePath(in); err == nil {
			t.Errorf("accepted %q", in)
		}
	}
	// Dollar text is literal; there is no shell or environment expansion.
	got, err := resolvedWorkspacePath("$HOME")
	if err != nil || filepath.Base(got) != "$HOME" {
		t.Fatal(got, err)
	}
}

func TestWorkspaceSnapshotPaths(t *testing.T) {
	for _, kind := range []string{"root-link", "ancestor-link", "descendant-link", "inside-journal"} {
		t.Run(kind, func(t *testing.T) {
			dir := workspaceTestDirectory(t)
			source := filepath.Join(dir, "source")
			if err := os.Mkdir(source, 0700); err != nil {
				t.Fatal(err)
			}
			path := source
			journal := filepath.Join(dir, "journal")
			switch kind {
			case "root-link":
				path = filepath.Join(dir, "link")
				if err := os.Symlink(source, path); err != nil {
					t.Fatal(err)
				}
			case "ancestor-link":
				if err := os.Mkdir(filepath.Join(source, "child"), 0700); err != nil {
					t.Fatal(err)
				}
				link := filepath.Join(dir, "link")
				if err := os.Symlink(source, link); err != nil {
					t.Fatal(err)
				}
				path = filepath.Join(link, "child")
			case "descendant-link":
				if err := os.Symlink(dir, filepath.Join(source, "link")); err != nil {
					t.Fatal(err)
				}
			case "inside-journal":
				journal = filepath.Join(source, "journal")
			}
			archive, digest, err := snapshotWorkspace(path, journal)
			if err == nil || archive != nil || digest != "" {
				t.Fatalf("published refused path: %d %q %v", len(archive), digest, err)
			}
		})
	}
}

func TestWorkspaceSnapshotSameArchivePolicy(t *testing.T) {
	dir := workspaceTestDirectory(t)
	source := filepath.Join(dir, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "input"), []byte("unchanged"), 0600); err != nil {
		t.Fatal(err)
	}
	got, digest, err := snapshotWorkspace(source, filepath.Join(dir, "journal"))
	if err != nil {
		t.Fatal(err)
	}
	want, wd, err := snapshotDirectory(source)
	if err != nil || !bytes.Equal(got, want) || digest != wd {
		t.Fatalf("snapshot policy diverged: %s %s %v", digest, wd, err)
	}
}

func TestWorkspacePathReplacement(t *testing.T) {
	dir := workspaceTestDirectory(t)
	path := filepath.Join(dir, "source")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	parts, err := workspacePath(path, false, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, path+"-old"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := recheckWorkspacePath(parts); err == nil {
		t.Fatal("accepted replaced root")
	}
}

func TestWorkspaceJournalAncestors(t *testing.T) {
	dir := workspaceTestDirectory(t)
	parent := filepath.Join(dir, "shared")
	if err := os.Mkdir(parent, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0777); err != nil {
		t.Fatal(err)
	}
	failed, err := openWorkspaceRun(filepath.Join(parent, "journal"), workspaceTestIntent(dir))
	if failed != nil {
		_ = failed.Close()
	}
	if err == nil {
		t.Fatal("accepted writable ancestor")
	}
	if err := os.Chmod(parent, 0755); err != nil {
		t.Fatal(err)
	}
	r, err := openWorkspaceRun(filepath.Join(parent, "journal"), workspaceTestIntent(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	// An ordinary 0755 current-user ancestor is allowed; the managed intent is0700.
	info, err := os.Stat(r.dir)
	if err != nil || info.Mode().Perm() != 0700 {
		t.Fatal(info, err)
	}
}

func TestWorkspaceHomePathPreservesSymlinkObservation(t *testing.T) {
	home := workspaceTestDirectory(t)
	t.Setenv("HOME", home)
	for _, name := range []string{"target", "input"} {
		if err := os.Mkdir(filepath.Join(home, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(filepath.Join(home, "target"), filepath.Join(home, "link")); err != nil {
		t.Fatal(err)
	}
	if _, err := resolvedWorkspacePath("~/link/../input"); err == nil {
		t.Fatal("home expansion erased a symlink before inspection")
	}
	// The same spelling without a link still resolves to the existing input.
	got, err := resolvedWorkspacePath("~/target/../input")
	if err != nil || got != filepath.Join(home, "input") {
		t.Fatal(got, err)
	}
}

func TestWorkspaceJournalRawAncestorSpelling(t *testing.T) {
	for _, kind := range []string{"explicit", "default"} {
		t.Run(kind, func(t *testing.T) {
			dir := workspaceTestDirectory(t)
			if err := os.Mkdir(filepath.Join(dir, "real"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(dir, "real"), filepath.Join(dir, "link")); err != nil {
				t.Fatal(err)
			}
			for _, component := range []string{"link", "real"} {
				raw := dir + string(filepath.Separator) + component + string(filepath.Separator) + ".."
				journal := raw + string(filepath.Separator) + "intent"
				if kind == "default" {
					t.Setenv("HOME", raw)
					t.Setenv("XDG_CACHE_HOME", raw+"/cache")
					journal = ""
				}
				if component == "real" && kind == "default" {
					cache, err := os.UserCacheDir()
					if err != nil {
						t.Fatal(err)
					}
					if err := os.MkdirAll(cache, 0700); err != nil {
						t.Fatal(err)
					}
				}
				r, err := openWorkspaceRun(journal, workspaceTestIntent(dir))
				if component == "link" {
					if r != nil {
						_ = r.Close()
					}
					if err == nil {
						t.Fatal("clean erased journal ancestor symlink")
					}
				} else {
					if err != nil {
						t.Fatal("refused ordinary directory parent traversal", err)
					}
					if err := r.Close(); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}
