package agentssh

import (
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

func TestConfigValidate(t *testing.T) {
	valid := config{
		sshAddress:     "host.example:2222",
		user:           "skygo",
		keyFile:        "/k/id",
		knownHostsFile: "/k/known_hosts",
		agentAddress:   "127.0.0.1:8443",
		agentName:      "agent.example",
		agentCAFile:    "/k/ca.pem",
	}
	tests := []struct {
		name string
		edit func(*config)
		ok   bool
	}{
		{"valid", func(*config) {}, true},
		{"ipv6", func(c *config) { c.sshAddress = "[::1]:22"; c.agentAddress = "[::1]:8443" }, true},
		{"no port", func(c *config) { c.sshAddress = "host.example" }, false},
		{"upper case host", func(c *config) { c.sshAddress = "Host.example:22" }, false},
		{"padded port", func(c *config) { c.sshAddress = "host.example:022" }, false},
		{"port zero", func(c *config) { c.sshAddress = "host.example:0" }, false},
		{"zone", func(c *config) { c.sshAddress = "[fe80::1%lo0]:22" }, false},
		{"no user", func(c *config) { c.user = "" }, false},
		{"user space", func(c *config) { c.user = "a b" }, false},
		{"remote agent", func(c *config) { c.agentAddress = "10.0.0.1:8443" }, false},
		{"agent dns", func(c *config) { c.agentAddress = "localhost:8443" }, false},
		{"agent port", func(c *config) { c.agentAddress = "127.0.0.1:08443" }, false},
		{"name ip", func(c *config) { c.agentName = "127.0.0.1" }, false},
		{"name upper", func(c *config) { c.agentName = "Agent.example" }, false},
		{"name empty label", func(c *config) { c.agentName = "agent..example" }, false},
		{"name hyphen", func(c *config) { c.agentName = "-agent.example" }, false},
		{"relative key", func(c *config) { c.keyFile = "id" }, false},
		{"unclean ca", func(c *config) { c.agentCAFile = "/k/../ca.pem" }, false},
		{"no known hosts", func(c *config) { c.knownHostsFile = "" }, false},
	}
	for _, tt := range tests {
		c := valid
		tt.edit(&c)
		if err := c.validate(); (err == nil) != tt.ok {
			t.Errorf("%s: validate() = %v, want ok %v", tt.name, err, tt.ok)
		}
	}
}

func TestReadFile(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string, mode os.FileMode) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		return path
	}
	owner := write("owner", "abcd", 0o400)
	link := filepath.Join(dir, "link")
	if err := os.Symlink(owner, link); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		path    string
		private bool
		ok      bool
	}{
		{"owner only", owner, true, true},
		{"group readable", write("group", "abcd", 0o640), true, false},
		{"public not private", write("public", "abcd", 0o644), false, true},
		{"symlink", link, false, false},
		{"directory", dir, false, false},
		{"too large", write("large", "abcde", 0o600), false, false},
		{"missing", filepath.Join(dir, "missing"), false, false},
	}
	for _, tt := range tests {
		data, err := readFile(tt.path, 4, tt.private)
		if (err == nil) != tt.ok {
			t.Errorf("%s: readFile = %q, %v; want ok %v", tt.name, data, err, tt.ok)
		}
		if err == nil && string(data) != "abcd" {
			t.Errorf("%s: read %q", tt.name, data)
		}
	}
}

func TestLoadSignerRefuses(t *testing.T) {
	dir := t.TempDir()
	s := newSigner(t)
	priv, _ := signerKeys.Load(s)
	block, err := ssh.MarshalPrivateKeyWithPassphrase(priv, "", []byte("passphrase"))
	if err != nil {
		t.Fatal(err)
	}
	encrypted := filepath.Join(dir, "encrypted")
	if err := os.WriteFile(encrypted, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadSigner(encrypted); err == nil || !strings.Contains(err.Error(), "encrypted") {
		t.Errorf("encrypted key: %v, want refusal", err)
	}
	const secret = "c2VjcmV0LWtleS1ieXRlcw"
	garbage := filepath.Join(dir, "garbage")
	if err := os.WriteFile(garbage, []byte("-----BEGIN OPENSSH PRIVATE KEY-----\n"+secret+"\n-----END OPENSSH PRIVATE KEY-----\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadSigner(garbage); err == nil || strings.Contains(err.Error(), secret) {
		t.Errorf("invalid key: %v, want refusal without contents", err)
	}
}

func TestKnownHosts(t *testing.T) {
	const addr = "host.example:2222"
	a, b := newSigner(t).PublicKey(), newSigner(t).PublicKey()
	line := func(host string, k ssh.PublicKey) string { return knownhosts.Line([]string{host}, k) }
	keyText := func(k ssh.PublicKey) string { return strings.TrimSpace(string(ssh.MarshalAuthorizedKey(k))) }
	type want struct {
		key    ssh.PublicKey
		reason string // "" if accepted
	}
	tests := []struct {
		name    string
		data    string
		parseOK bool
		check   []want
	}{
		{"enrolled", line(addr, a), true, []want{{a, ""}, {b, "mismatched"}}},
		{"comment and blank", "# c\n\n" + line(addr, a) + " comment\n", true, []want{{a, ""}}},
		{"rotation", line(addr, a) + "\n" + line(addr, b), true, []want{{a, ""}, {b, ""}}},
		{"multiple hosts", "[other]:2222,[host.example]:2222 " + keyText(a), true, []want{{a, ""}}},
		{"default port is distinct", line("host.example", a), true, []want{{a, "unknown"}}},
		{"other host", line("other.example:2222", a), true, []want{{a, "unknown"}}},
		{"revoked", line(addr, a) + "\n@revoked * " + keyText(a), true, []want{{a, "revoked"}, {b, "mismatched"}}},
		{"hashed", "|1|c2FsdA==|aGFzaA== " + keyText(a), false, nil},
		{"wildcard", "*.example " + keyText(a), false, nil},
		{"negated", "!other," + line(addr, a), false, nil},
		{"cert authority", "@cert-authority " + line(addr, a), false, nil},
		{"unknown marker", "@trusted " + line(addr, a), false, nil},
		{"no key", "host.example", false, nil},
		{"type mismatch", "[host.example]:2222 ssh-rsa " + strings.Fields(keyText(a))[1], false, nil},
		{"carriage return", line(addr, a) + "\r" + line("x", b), false, nil},
	}
	for _, tt := range tests {
		k, err := parseKnownHosts([]byte(tt.data), addr)
		if (err == nil) != tt.parseOK {
			t.Errorf("%s: parse error %v, want ok %v", tt.name, err, tt.parseOK)
			continue
		}
		for _, w := range tt.check {
			err := k.check(addr, w.key)
			var hk *hostKeyError
			switch {
			case w.reason == "" && err != nil:
				t.Errorf("%s: check = %v, want accepted", tt.name, err)
			case w.reason != "" && (!errors.As(err, &hk) || hk.reason != w.reason):
				t.Errorf("%s: check = %v, want %s", tt.name, err, w.reason)
			}
		}
	}
	k, err := parseKnownHosts([]byte(line(addr, a)), addr)
	if err != nil {
		t.Fatal(err)
	}
	if len(k.algorithms) != 1 || k.algorithms[0] != ssh.KeyAlgoED25519 {
		t.Errorf("algorithms %q, want only the enrolled key's", k.algorithms)
	}
}
