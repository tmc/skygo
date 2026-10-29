package agentidentity

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testName = "agent.skygo.invalid"

func TestReadToken(t *testing.T) {
	tests := []struct {
		name    string
		content string
		mode    os.FileMode
		want    string
		err     string // "" if accepted
	}{
		{"line", "abc\n", 0o600, "abc", ""},
		{"no newline", "abc", 0o400, "abc", ""},
		{"empty", "\n", 0o600, "", "invalid token"},
		{"space", "a b\n", 0o600, "", "invalid token"},
		{"two lines", "a\nb\n", 0o600, "", "invalid token"},
		{"too long", strings.Repeat("a", 4097), 0o600, "", "invalid token"},
		{"larger than limit", strings.Repeat("a", 4098), 0o600, "", "larger than 4097 bytes"},
		{"readable", "abc\n", 0o644, "", "group or others"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(name, []byte(tt.content), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(name, tt.mode); err != nil {
				t.Fatal(err)
			}
			got, err := ReadToken(name)
			if tt.err != "" {
				if err == nil || !strings.Contains(err.Error(), tt.err) {
					t.Fatalf("ReadToken = %q, %v; want %q", got, err, tt.err)
				}
				if strings.Contains(err.Error(), tt.content) && tt.content != "\n" {
					t.Fatalf("ReadToken error %q holds the file's content", err)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("ReadToken = %q, %v; want %q", got, err, tt.want)
			}
		})
	}
}

func TestReadPrivateRefuses(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadPrivate(link, 10); err == nil || !strings.Contains(err.Error(), "not a regular file") {
		t.Errorf("ReadPrivate of a symlink = %v", err)
	}
	if _, err := ReadPrivate(dir, 10); err == nil {
		t.Error("ReadPrivate of a directory succeeded")
	}
	if _, err := ReadPrivate(filepath.Join(dir, "missing"), 10); !os.IsNotExist(err) {
		t.Errorf("ReadPrivate of a missing file = %v", err)
	}
}

func TestLoad(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "agents", "dgx", "c", "d")
	first, err := Load(dir, testName)
	if err != nil {
		t.Fatal(err)
	}
	if first.Cert != filepath.Join(dir, "cert.pem") || first.Key != filepath.Join(dir, "key.pem") || first.Token != filepath.Join(dir, "token") {
		t.Fatalf("Load = %+v", first)
	}
	if len(first.Bearer) != 64 {
		t.Errorf("token length %d, want 64 hex digits", len(first.Bearer))
	}
	for _, name := range []string{dir, first.Cert, first.Key, first.Token} {
		info, err := os.Lstat(name)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm()&0o077 != 0 {
			t.Errorf("%s mode %v, want private", name, info.Mode())
		}
	}
	again, err := Load(dir, testName)
	if err != nil || again != first {
		t.Fatalf("second Load = %+v, %v; want the same identity", again, err)
	}
	if _, err := Check(dir, "other.invalid"); err == nil || !strings.Contains(err.Error(), "not for other.invalid") {
		t.Errorf("Check for another name = %v", err)
	}
	if err := os.Chmod(first.Key, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(dir, testName); err == nil || !strings.Contains(err.Error(), "group or others") {
		t.Errorf("Load with a readable key = %v", err)
	}
}
