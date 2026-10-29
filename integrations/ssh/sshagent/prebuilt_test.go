package sshagent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrebuiltConfig(t *testing.T) {
	b := Binary{Path: "/agent", Commit: strings.Repeat("a", 40), Digest: strings.Repeat("b", 64)}
	for mask := 1; mask < 7; mask++ {
		c := validConfig(t.TempDir())
		c.Source = ""
		if mask&1 != 0 {
			c.Prebuilt.Path = b.Path
		}
		if mask&2 != 0 {
			c.Prebuilt.Commit = b.Commit
		}
		if mask&4 != 0 {
			c.Prebuilt.Digest = b.Digest
		}
		if c.check() == nil {
			t.Fatalf("accepted partial prebuilt mask %d", mask)
		}
	}
	c := validConfig(t.TempDir())
	c.Source = ""
	c.Prebuilt = b
	if err := c.check(); err != nil {
		t.Fatal(err)
	}
	c.Source = "/source"
	if c.check() == nil {
		t.Fatal("accepted source conflict")
	}
	c.Source = ""
	c.AllowDirty = true
	if c.check() == nil {
		t.Fatal("accepted dirty conflict")
	}
	c.AllowDirty = false
	c.Prebuilt.Path = "relative"
	if c.check() == nil {
		t.Fatal("accepted relative path")
	}
}

func TestPrebuiltTokenFirst(t *testing.T) {
	d := t.TempDir()
	c := validConfig(d)
	c.Source = ""
	c.Prebuilt = Binary{Path: filepath.Join(d, "missing-agent"), Commit: strings.Repeat("a", 40), Digest: strings.Repeat("b", 64)}
	c.Installation = Installation{DataDir: "/d", AuthFile: "/a", ArtifactDir: "/r"}
	c.ScopedTokenFile = filepath.Join(d, "missing-token")
	s, err := Open(context.Background(), c)
	if s != nil || err == nil || !strings.Contains(err.Error(), "scoped token") {
		t.Fatalf("Open=%v,%v", s, err)
	}
	if _, err := os.Stat(c.CacheDir); !os.IsNotExist(err) {
		t.Fatal("binary cache acquired before token")
	}
}

func TestPrebuiltBeforeSigner(t *testing.T) {
	d := t.TempDir()
	c := validConfig(d)
	c.Source = ""
	c.Prebuilt = Binary{Path: filepath.Join(d, "missing-agent"), Commit: strings.Repeat("a", 40), Digest: strings.Repeat("b", 64)}
	s, err := Open(context.Background(), c)
	if s != nil || err == nil || !strings.Contains(err.Error(), "agentbinary:") {
		t.Fatalf("Open=%v,%v", s, err)
	}
	if _, err := os.Stat(c.IdentityDir); !os.IsNotExist(err) {
		t.Fatal("identity acquired for invalid prebuilt")
	}
}
