package sshagent

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testFingerprint is a canonical pin: the SHA256 fingerprint form of the
// SHA-256 of no bytes. No key need hold it.
const testFingerprint = "SHA256:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU"

func validConfig(dir string) Config {
	return Config{
		Address:        "dgx.example:22",
		User:           "skygo",
		KnownHostsFile: filepath.Join(dir, "known_hosts"),
		AgentSocket:    filepath.Join(dir, "agent.sock"),
		Fingerprint:    testFingerprint,
		Source:         filepath.Join(dir, "src"),
		IdentityDir:    filepath.Join(dir, "agents"),
		CacheDir:       filepath.Join(dir, "cache"),
		Target:         "dgx",
	}
}

func TestConfigCheck(t *testing.T) {
	scoped := func(c *Config) {
		c.Installation = Installation{DataDir: "/srv/skygo/data", AuthFile: "/srv/skygo/auth.json", ArtifactDir: "/srv/skygo/artifacts"}
		c.ScopedTokenFile = "/home/u/dgx.token"
	}
	tests := []struct {
		name string
		edit func(c *Config)
		err  string // "" if accepted
	}{
		{"agent socket", func(*Config) {}, ""},
		{"key file", func(c *Config) { c.AgentSocket, c.Fingerprint, c.KeyFile = "", "", "/home/u/id" }, ""},
		{"ipv4", func(c *Config) { c.Address = "192.0.2.1:2222" }, ""},
		{"ipv6", func(c *Config) { c.Address = "[2001:db8::1]:22" }, ""},
		{"default port", func(c *Config) { c.Port = 0 }, ""},
		{"port", func(c *Config) { c.Port = 50000 }, ""},
		{"scoped", scoped, ""},
		{"dirty", func(c *Config) { c.AllowDirty = true }, ""},

		{"no port", func(c *Config) { c.Address = "dgx.example" }, "address"},
		{"alias", func(c *Config) { c.Address = "DGX:22" }, "canonical host:port"},
		{"zero port", func(c *Config) { c.Address = "dgx:0" }, "canonical host:port"},
		{"padded port", func(c *Config) { c.Address = "dgx:022" }, "canonical host:port"},
		{"injected line", func(c *Config) { c.Address = "dgx\nport 23:22" }, "canonical host:port"},
		{"zone", func(c *Config) { c.Address = "[fe80::1%en0]:22" }, "canonical host:port"},
		{"no user", func(c *Config) { c.User = "" }, "user"},
		{"user with space", func(c *Config) { c.User = "a b" }, "user"},
		{"relative known hosts", func(c *Config) { c.KnownHostsFile = "known_hosts" }, "known hosts"},
		{"no key", func(c *Config) { c.AgentSocket, c.Fingerprint = "", "" }, "exactly one"},
		{"two keys", func(c *Config) { c.KeyFile = "/home/u/id" }, "exactly one"},
		{"relative key file", func(c *Config) { c.AgentSocket, c.Fingerprint, c.KeyFile = "", "", "id" }, "key file"},
		{"key file pinned", func(c *Config) { c.AgentSocket, c.KeyFile = "", "/home/u/id" }, "not used with a key file"},
		{"unclean socket", func(c *Config) { c.AgentSocket = "/tmp/../agent.sock" }, "agent socket"},
		{"no fingerprint", func(c *Config) { c.Fingerprint = "" }, "SHA256"},
		{"md5 fingerprint", func(c *Config) { c.Fingerprint = "MD5:aa" }, "SHA256"},
		{"short fingerprint", func(c *Config) { c.Fingerprint = "SHA256:abc" }, "SHA256"},
		{"padded fingerprint", func(c *Config) { c.Fingerprint = testFingerprint + "=" }, "SHA256"},
		{"noncanonical fingerprint", func(c *Config) { c.Fingerprint = testFingerprint[:len(testFingerprint)-1] + "V" }, "SHA256"},
		{"lowercase prefix", func(c *Config) { c.Fingerprint = "sha256" + testFingerprint[len("SHA256"):] }, "SHA256"},
		{"uppercase ipv6", func(c *Config) { c.Address = "[2001:DB8::1]:22" }, "canonical host:port"},
		{"relative source", func(c *Config) { c.Source = "." }, "source"},
		{"no identity directory", func(c *Config) { c.IdentityDir = "" }, "identity directory"},
		{"no cache directory", func(c *Config) { c.CacheDir = "" }, "cache directory"},
		{"no target", func(c *Config) { c.Target = "" }, "target name"},
		{"bad target", func(c *Config) { c.Target = "a;b" }, "target name"},
		{"low port", func(c *Config) { c.Port = 22 }, "agent port"},
		{"high port", func(c *Config) { c.Port = 65536 }, "agent port"},
		{"token without installation", func(c *Config) { c.ScopedTokenFile = "/home/u/dgx.token" }, "an installation needs"},
		{"installation without token", func(c *Config) { scoped(c); c.ScopedTokenFile = "" }, "an installation needs"},
		{"partial installation", func(c *Config) { scoped(c); c.Installation.AuthFile = "" }, "an installation needs"},
		{"shell installation path", func(c *Config) { scoped(c); c.Installation.ArtifactDir = "/r;id" }, "clean absolute paths"},
		{"relative token", func(c *Config) { scoped(c); c.ScopedTokenFile = "dgx.token" }, "scoped token file"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := validConfig("/home/u")
			tt.edit(&c)
			err := c.check()
			if tt.err == "" {
				if err != nil {
					t.Fatalf("check = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.err) {
				t.Fatalf("check = %v, want %q", err, tt.err)
			}
		})
	}
}

func TestConfigOptions(t *testing.T) {
	c := validConfig("/home/u")
	if o := c.options(); o.Port != 47080 || o.SSHAddress != c.Address || o.SignerKey != c.Fingerprint || o.AllowDirty || !o.Installation.IsZero() || o.TokenFile != "" {
		t.Errorf("options = %+v", o)
	}
	c.Port, c.AllowDirty = 50000, true
	c.Installation = Installation{DataDir: "/d/x", AuthFile: "/d/a", ArtifactDir: "/d/r"}
	c.ScopedTokenFile = "/home/u/t"
	o := c.options()
	if o.Port != 50000 || !o.AllowDirty || o.Installation.DataDir != "/d/x" || o.Installation.AuthFile != "/d/a" || o.Installation.ArtifactDir != "/d/r" || o.TokenFile != "/home/u/t" || o.Bearer != "" {
		t.Errorf("options = %+v", o)
	}
}

// TestOpenRefusesBeforeEffects checks that a refused Config is reported
// before Open reads the scoped token, logs, or creates any directory.
func TestOpenRefusesBeforeEffects(t *testing.T) {
	dir := t.TempDir()
	var log bytes.Buffer
	c := validConfig(dir)
	c.Log = &log
	c.Installation = Installation{DataDir: "/srv/skygo/data", AuthFile: "/srv/skygo/auth.json", ArtifactDir: "/srv/skygo/artifacts"}
	c.ScopedTokenFile = filepath.Join(dir, "missing.token")
	c.Target = "Not-A-Target"
	s, err := Open(context.Background(), c)
	if s != nil || err == nil || !strings.Contains(err.Error(), "target name") {
		t.Fatalf("Open = %v, %v; want a target refusal", s, err)
	}
	checkNoEffects(t, dir, &log)
}

// TestOpenRefusesTransportInvalid checks that Open refuses an address or
// pin the SSH transport would refuse before it reads the scoped token,
// dials the SSH agent, builds, logs or creates anything. The SSH agent
// socket does not exist: had Open dialed it, the error would say so.
func TestOpenRefusesTransportInvalid(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*Config)
		err  string
	}{
		{"short fingerprint", func(c *Config) { c.Fingerprint = "SHA256:abc" }, "SHA256 key fingerprint"},
		{"padded fingerprint", func(c *Config) { c.Fingerprint = testFingerprint + "=" }, "SHA256 key fingerprint"},
		{"uppercase ipv6", func(c *Config) { c.Address = "[2001:DB8::1]:22" }, "canonical host:port"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			var log bytes.Buffer
			c := validConfig(dir)
			c.Log = &log
			c.Installation = Installation{DataDir: "/srv/skygo/data", AuthFile: "/srv/skygo/auth.json", ArtifactDir: "/srv/skygo/artifacts"}
			c.ScopedTokenFile = filepath.Join(dir, "missing.token")
			tt.edit(&c)
			s, err := Open(context.Background(), c)
			if s != nil || err == nil || !strings.Contains(err.Error(), tt.err) || strings.Contains(err.Error(), "ssh agent") || strings.Contains(err.Error(), "scoped token") {
				t.Fatalf("Open = %v, %v; want %q before any effect", s, err, tt.err)
			}
			checkNoEffects(t, dir, &log)
		})
	}
}

// TestOpenReadsScopedTokenFirst checks that an unusable scoped token is
// refused before Open contacts the SSH agent, builds or logs anything.
func TestOpenReadsScopedTokenFirst(t *testing.T) {
	dir := t.TempDir()
	var log bytes.Buffer
	c := validConfig(dir)
	c.Log = &log
	c.Installation = Installation{DataDir: "/srv/skygo/data", AuthFile: "/srv/skygo/auth.json", ArtifactDir: "/srv/skygo/artifacts"}
	c.ScopedTokenFile = filepath.Join(dir, "token")
	if err := os.WriteFile(c.ScopedTokenFile, []byte("abc\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), c)
	if s != nil || err == nil || !strings.Contains(err.Error(), "sshagent: scoped token") || !strings.Contains(err.Error(), "group or others") {
		t.Fatalf("Open = %v, %v; want a scoped token refusal", s, err)
	}
	checkNoEffects(t, dir, &log)
}

// TestOpenSignerRefusedSettled checks that Open fails with an ordinary
// error, not an *OpenError, when the SSH agent cannot be reached: it
// acquired nothing and built nothing.
func TestOpenSignerRefusedSettled(t *testing.T) {
	dir := t.TempDir()
	var log bytes.Buffer
	c := validConfig(dir)
	c.Log = &log
	s, err := Open(context.Background(), c)
	var oe *OpenError
	if s != nil || err == nil || !strings.Contains(err.Error(), "ssh agent") || errors.As(err, &oe) {
		t.Fatalf("Open = %v, %v; want an ordinary ssh agent error", s, err)
	}
	checkNoEffects(t, dir, &log)
}

func checkNoEffects(t *testing.T, dir string, log *bytes.Buffer) {
	t.Helper()
	if log.Len() != 0 {
		t.Errorf("Open logged %q", log)
	}
	for _, name := range []string{"agents", "cache", "src"} {
		if _, err := os.Lstat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Errorf("%s: %v; want it absent", name, err)
		}
	}
}
