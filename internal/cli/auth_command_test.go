package cli

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestTokenFile(t *testing.T) {
	secret := "skygo-test-secret"
	for _, tc := range []struct {
		name, data string
		mode       os.FileMode
		want       bool
	}{
		{"token", secret, 0600, true},
		{"newline", secret + "\n", 0600, true},
		{"empty", "", 0600, false},
		{"multiline", secret + "\nother", 0600, false},
		{"two newlines", secret + "\n\n", 0600, false},
		{"whitespace", secret + " ", 0600, false},
		{"public", secret, 0644, false},
		{"oversize", strings.Repeat("x", 4098), 0600, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), secret)
			if err := os.WriteFile(path, []byte(tc.data), tc.mode); err != nil {
				t.Fatal(err)
			}
			got, err := readTokenFile(path)
			if tc.want {
				if err != nil || got != secret {
					t.Fatalf("valid token was not read: %v", err)
				}
			} else if err == nil || got != "" || strings.Contains(err.Error(), secret) {
				t.Fatalf("unsafe token-file result: error=%v", err)
			}
		})
	}
	dir := t.TempDir()
	path, link := filepath.Join(dir, "token"), filepath.Join(dir, "link")
	if err := os.WriteFile(path, []byte(secret), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readTokenFile(link); err == nil {
		t.Fatal("symlink accepted")
	}
}

func TestCredentialHelpAndConflicts(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	const secret = "help-must-not-print-this-token"
	for _, tc := range []struct {
		name string
		args []string
		want string
		ok   bool
	}{
		{"help", []string{"--help"}, "--token-file", true},
		{"literal help", []string{"--token", secret, "--help"}, "--token-file", true},
		{"literal conflict", []string{"--token", secret, "info"}, "choose only one credential source", false},
		{"file conflict", []string{"--token-file", "absent", "info"}, "choose only one credential source", false},
		{"server conflict", []string{"server", "--auth-file", "absent"}, "conflicts with legacy token", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"-test.run=^TestCLIHelper$", "--"}, tc.args...)
			cmd := exec.Command(exe, args...)
			cmd.Env = append(os.Environ(), "SKYGO_TEST_HELPER=1", "SKYGO_TOKEN="+secret)
			out, err := cmd.CombinedOutput()
			if (err == nil) != tc.ok || !strings.Contains(string(out), tc.want) || strings.Contains(string(out), secret) {
				t.Fatalf("credential handling status=%v, diagnostic matched=%v, leaked=%v", err, strings.Contains(string(out), tc.want), strings.Contains(string(out), secret))
			}
		})
	}
}
