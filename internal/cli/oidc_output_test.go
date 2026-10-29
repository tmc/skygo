//go:build linux || darwin

package cli

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

const outputToken = "OUTPUT-TOKEN-SECRET"

// privateTempDir returns a private test directory with symlinks resolved;
// on Darwin the temporary directory is reached through the /var symlink.
func privateTempDir(t *testing.T) string {
	t.Helper()
	raw, err := os.MkdirTemp("", "skygo-oidc-output-")
	if err != nil {
		t.Fatal(err)
	}
	dir, err := filepath.EvalSymlinks(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func mkdir(t *testing.T, path string, perm os.FileMode) {
	t.Helper()
	if err := os.Mkdir(path, perm); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, perm); err != nil {
		t.Fatal(err)
	}
}

func writeFile(t *testing.T, path, data string, perm os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, []byte(data), perm); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, perm); err != nil {
		t.Fatal(err)
	}
}

// tree lists every file under root with its contents, so a test can check
// both where a token went and that nothing else was left.
func tree(t *testing.T, root string) map[string]string {
	t.Helper()
	out := make(map[string]string)
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			out[rel] = "symlink"
		case info.Mode().IsRegular():
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			out[rel] = string(data)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// requireNoToken fails if any file under root holds the token.
func requireNoToken(t *testing.T, root string) {
	t.Helper()
	for name, data := range tree(t, root) {
		if strings.Contains(data, outputToken) {
			t.Fatalf("%s holds the token", name)
		}
	}
}

func publishToken(t *testing.T, path string, replace bool, hook func(temp string)) (err error) {
	t.Helper()
	o, err := openTokenOutput(path, replace)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, o.Close()) }()
	o.beforePublish = hook
	return o.publish(outputToken)
}

func TestTokenOutputPublish(t *testing.T) {
	dir := privateTempDir(t)
	path := filepath.Join(dir, "id.token")
	if err := publishToken(t, path, false, nil); err != nil {
		t.Fatalf("publish: %v", err)
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("output = %v, %v; want a 0600 regular file", info, err)
	}
	if got := tree(t, dir); len(got) != 1 || got["id.token"] != outputToken+"\n" {
		t.Fatalf("directory = %q, want only id.token with the token and a newline", got)
	}
	if token, err := readTokenFile(path); err != nil || token != outputToken {
		t.Fatalf("readTokenFile = %v", err)
	}
	if err := publishToken(t, path, false, nil); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("second publish = %v, want already exists", err)
	}
}

// TestTokenOutputUmask publishes under a umask that would make a new file
// read-only. The output must still be exactly 0600.
func TestTokenOutputUmask(t *testing.T) {
	dir := privateTempDir(t)
	path := filepath.Join(dir, "id.token")
	defer unix.Umask(unix.Umask(0o277))
	if err := publishToken(t, path, false, nil); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if info, err := os.Lstat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("output = %v, %v; want mode 0600", info, err)
	}
}

// TestTokenOutputRefusesDirectories checks directories that other users
// could change. Nothing may be written under the test directory.
func TestTokenOutputRefusesDirectories(t *testing.T) {
	for _, tt := range []struct {
		name  string
		setup func(t *testing.T, dir string) string // returns the output path
		root  bool                                  // needs root to set up
	}{
		{"group writable parent", func(t *testing.T, dir string) string {
			mkdir(t, filepath.Join(dir, "out"), 0o770)
			return filepath.Join(dir, "out", "id.token")
		}, false},
		{"other writable parent", func(t *testing.T, dir string) string {
			mkdir(t, filepath.Join(dir, "out"), 0o703)
			return filepath.Join(dir, "out", "id.token")
		}, false},
		{"readable parent", func(t *testing.T, dir string) string {
			mkdir(t, filepath.Join(dir, "out"), 0o755)
			return filepath.Join(dir, "out", "id.token")
		}, false},
		{"writable ancestor", func(t *testing.T, dir string) string {
			mkdir(t, filepath.Join(dir, "a"), 0o777)
			mkdir(t, filepath.Join(dir, "a", "out"), 0o700)
			return filepath.Join(dir, "a", "out", "id.token")
		}, false},
		{"immediate symlink", func(t *testing.T, dir string) string {
			mkdir(t, filepath.Join(dir, "real"), 0o700)
			if err := os.Symlink("real", filepath.Join(dir, "out")); err != nil {
				t.Fatal(err)
			}
			return filepath.Join(dir, "out", "id.token")
		}, false},
		{"ancestor symlink", func(t *testing.T, dir string) string {
			mkdir(t, filepath.Join(dir, "real"), 0o700)
			mkdir(t, filepath.Join(dir, "real", "out"), 0o700)
			if err := os.Symlink("real", filepath.Join(dir, "a")); err != nil {
				t.Fatal(err)
			}
			return filepath.Join(dir, "a", "out", "id.token")
		}, false},
		{"other owner parent", func(t *testing.T, dir string) string {
			mkdir(t, filepath.Join(dir, "out"), 0o700)
			if err := os.Chown(filepath.Join(dir, "out"), 65534, 65534); err != nil {
				t.Fatal(err)
			}
			return filepath.Join(dir, "out", "id.token")
		}, true},
		{"other owner ancestor", func(t *testing.T, dir string) string {
			mkdir(t, filepath.Join(dir, "a"), 0o755)
			mkdir(t, filepath.Join(dir, "a", "out"), 0o700)
			if err := os.Chown(filepath.Join(dir, "a"), 65534, 65534); err != nil {
				t.Fatal(err)
			}
			return filepath.Join(dir, "a", "out", "id.token")
		}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.root && os.Geteuid() != 0 {
				t.Skip("needs root to create a directory owned by another user")
			}
			dir := privateTempDir(t)
			path := tt.setup(t, dir)
			before := tree(t, dir)
			if err := publishToken(t, path, false, nil); err == nil {
				t.Fatal("published through an unsafe directory")
			}
			if after := tree(t, dir); !mapsEqual(before, after) {
				t.Fatalf("refused publication changed files: %q", after)
			}
		})
	}
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

// TestTokenOutputDirectoryReplaced replaces the output directory, or an
// ancestor, while the original is held. Neither the held original nor the
// replacement may receive the token.
func TestTokenOutputDirectoryReplaced(t *testing.T) {
	for _, tt := range []struct {
		name     string
		parts    []string // directories to create, in order
		replaced string   // the directory moved aside and recreated
	}{
		{"parent", []string{"out"}, "out"},
		{"ancestor", []string{"a", "a/out"}, "a"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := privateTempDir(t)
			for _, p := range tt.parts {
				mkdir(t, filepath.Join(dir, p), 0o700)
			}
			path := filepath.Join(dir, tt.parts[len(tt.parts)-1], "id.token")
			err := publishToken(t, path, false, func(string) {
				if err := os.Rename(filepath.Join(dir, tt.replaced), filepath.Join(dir, "held")); err != nil {
					t.Fatal(err)
				}
				for _, p := range tt.parts {
					mkdir(t, filepath.Join(dir, p), 0o700)
				}
			})
			if err == nil || !strings.Contains(err.Error(), "changed during sign-in") {
				t.Fatalf("publish = %v, want directory changed", err)
			}
			if got := tree(t, dir); len(got) != 0 {
				t.Fatalf("files left after refusal: %q", got)
			}
		})
	}
}

// TestTokenOutputTempSubstituted replaces the temporary file's entry before
// publication. The substitute is not published or removed, and the token is
// nowhere.
func TestTokenOutputTempSubstituted(t *testing.T) {
	dir := privateTempDir(t)
	path := filepath.Join(dir, "id.token")
	var temp string
	err := publishToken(t, path, false, func(name string) {
		temp = name
		writeFile(t, filepath.Join(dir, "decoy"), "DECOY", 0o600)
		if err := os.Rename(filepath.Join(dir, "decoy"), filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	})
	if err == nil || !strings.Contains(err.Error(), "temporary file was replaced") {
		t.Fatalf("publish = %v, want temporary file replaced", err)
	}
	if got := tree(t, dir); len(got) != 1 || got[temp] != "DECOY" {
		t.Fatalf("directory = %q, want only the untouched substitute", got)
	}
	requireNoToken(t, dir)
}

// TestTokenOutputConcurrentCreate creates the output while sign-in runs.
// Publication must not clobber it.
func TestTokenOutputConcurrentCreate(t *testing.T) {
	dir := privateTempDir(t)
	path := filepath.Join(dir, "id.token")
	err := publishToken(t, path, false, func(string) { writeFile(t, path, "OTHER", 0o600) })
	if err == nil || !strings.Contains(err.Error(), "appeared during sign-in") {
		t.Fatalf("publish = %v, want appeared during sign-in", err)
	}
	if got := tree(t, dir); len(got) != 1 || got["id.token"] != "OTHER" {
		t.Fatalf("directory = %q, want the concurrent file unchanged", got)
	}
}

func TestTokenOutputReplace(t *testing.T) {
	dir := privateTempDir(t)
	path := filepath.Join(dir, "id.token")
	writeFile(t, path, "OLD\n", 0o600)
	err := publishToken(t, path, true, func(string) {
		// The old file is intact until the atomic rename.
		if data, err := os.ReadFile(path); err != nil || string(data) != "OLD\n" {
			t.Errorf("old output before publication = %q, %v", data, err)
		}
	})
	if err != nil {
		t.Fatalf("publish with replace: %v", err)
	}
	if got := tree(t, dir); len(got) != 1 || got["id.token"] != outputToken+"\n" {
		t.Fatalf("directory = %q, want only the new token", got)
	}
	if info, err := os.Lstat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("replaced output = %v, %v", info, err)
	}
}

func TestTokenOutputReplaceRefused(t *testing.T) {
	for _, tt := range []struct {
		name   string
		setup  func(t *testing.T, path string)
		change func(t *testing.T, path string) // after open, before publication
	}{
		{"readable", func(t *testing.T, path string) { writeFile(t, path, "OLD", 0o644) }, nil},
		{"directory", func(t *testing.T, path string) { mkdir(t, path, 0o700) }, nil},
		{"symlink", func(t *testing.T, path string) {
			writeFile(t, path+".target", "OLD", 0o600)
			if err := os.Symlink(filepath.Base(path)+".target", path); err != nil {
				t.Fatal(err)
			}
		}, nil},
		{"made readable during sign-in", func(t *testing.T, path string) { writeFile(t, path, "OLD", 0o600) }, func(t *testing.T, path string) {
			if err := os.Chmod(path, 0o644); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := privateTempDir(t)
			path := filepath.Join(dir, "id.token")
			tt.setup(t, path)
			before := tree(t, dir)
			var hook func(string)
			if tt.change != nil {
				hook = func(string) { tt.change(t, path) }
			}
			err := publishToken(t, path, true, hook)
			if err == nil || !strings.Contains(err.Error(), "private regular file") {
				t.Fatalf("publish = %v, want private regular file refusal", err)
			}
			if after := tree(t, dir); !mapsEqual(before, after) {
				t.Fatalf("refused replacement changed files: %q, want %q", after, before)
			}
			requireNoToken(t, dir)
		})
	}
	dir := privateTempDir(t)
	if err := publishToken(t, dir+"/", true, nil); err == nil {
		t.Fatal("published to a directory path")
	}
	if got := tree(t, dir); len(got) != 0 {
		t.Fatalf("files left: %q", got)
	}
}
