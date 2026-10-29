//go:build unix

package gce

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// NOT_RUN by author.

func configJSON(t *testing.T, edit func(map[string]any)) string {
	t.Helper()
	b, _ := json.Marshal(testConfig())
	var m map[string]any
	json.Unmarshal(b, &m)
	if edit != nil {
		edit(m)
	}
	b, _ = json.Marshal(m)
	return string(b)
}

func TestParseConfig(t *testing.T) {
	valid := configJSON(t, nil)
	tests := []struct {
		name string
		in   string
		ok   bool
	}{
		{"valid", valid, true},
		{"one escaped key", strings.Replace(valid, `"zone"`, `"zon`+jsonEscape('e')+`"`, 1), true},
		{"unknown field", configJSON(t, func(m map[string]any) { m["metadata"] = "x" }), false},
		{"duplicate", strings.Replace(valid, `{`, `{"zone":"us-central1-a",`, 1), false},
		{"escaped duplicate", strings.Replace(valid, `{`, `{"zon`+jsonEscape('e')+`":"us-central1-a",`, 1), false},
		{"case duplicate", strings.Replace(valid, `{`, `{"Zone":"us-central1-a",`, 1), false},
		{"nested duplicate", strings.Replace(valid, `"image":{`, `"image":{"id":"4242",`, 1), false},
		{"trailing", valid + "{}", false},
		{"oversize", strings.Repeat(" ", maxConfigBytes+1), false},
		{"image family", configJSON(t, func(m map[string]any) { m["image"].(map[string]any)["name"] = "family/debian-12" }), false},
		{"image id", configJSON(t, func(m map[string]any) { m["image"].(map[string]any)["id"] = "007" }), false},
		{"zone mismatch", configJSON(t, func(m map[string]any) { m["machine_type"] = "zones/us-east1-b/machineTypes/t2a-standard-1" }), false},
		{"subnet region", configJSON(t, func(m map[string]any) { m["subnet"] = "projects/p-net1/regions/us-east1/subnetworks/default" }), false},
		{"project id", configJSON(t, func(m map[string]any) { m["compute_project_id"] = "0" }), false},
		{"disk small", configJSON(t, func(m map[string]any) { m["disk_gib"] = 9 }), false},
		{"disk large", configJSON(t, func(m map[string]any) { m["disk_gib"] = 65537 }), false},
		{"user", configJSON(t, func(m map[string]any) { m["ssh_user"] = "Root:x" }), false},
		{"two keys", configJSON(t, func(m map[string]any) { m["authorized_key"] = testKey + "\n" + testKey }), false},
		{"private key", configJSON(t, func(m map[string]any) { m["authorized_key"] = "-----BEGIN OPENSSH PRIVATE KEY-----" }), false},
		{"credentials field", configJSON(t, func(m map[string]any) { m["credentials"] = "adc" }), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := ParseConfig(strings.NewReader(tt.in))
			if (err == nil) != tt.ok {
				t.Fatalf("ParseConfig = %+v, %v; want ok=%v", cfg, err, tt.ok)
			}
			if tt.ok && cfg != testConfig() {
				t.Errorf("ParseConfig = %+v", cfg)
			}
		})
	}
}

// sshKey returns an authorized key line of typ whose blob is the SSH wire
// strings parts followed by extra raw bytes.
func sshKey(typ string, extra []byte, parts ...[]byte) string {
	var b bytes.Buffer
	for _, p := range parts {
		binary.Write(&b, binary.BigEndian, uint32(len(p)))
		b.Write(p)
	}
	b.Write(extra)
	return typ + " " + base64.StdEncoding.EncodeToString(b.Bytes())
}

// TestAuthorizedKeyProfile checks the only supported key profile: a
// complete ssh-ed25519 public key wire, exactly 32 key bytes and nothing
// after. Every refused key creates no state.
func TestAuthorizedKeyProfile(t *testing.T) {
	ed := []byte("ssh-ed25519")
	key := bytes.Repeat([]byte{7}, 32)
	tests := []struct {
		name string
		key  string
		ok   bool
	}{
		{"test key", testKey, true},
		{"built", sshKey("ssh-ed25519", nil, ed, key), true},
		{"built with comment", sshKey("ssh-ed25519", nil, ed, key) + " me@host", true},
		{"algorithm only", "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5", false},
		{"short key", sshKey("ssh-ed25519", nil, ed, key[:31]), false},
		{"long key", sshKey("ssh-ed25519", nil, ed, append(key, 7)), false},
		{"trailing wire", sshKey("ssh-ed25519", []byte{0}, ed, key), false},
		{"trailing string", sshKey("ssh-ed25519", nil, ed, key, []byte("x")), false},
		{"length overruns", sshKey("ssh-ed25519", append([]byte{0, 0, 0, 40}, key...), ed), false},
		{"inner algorithm", sshKey("ssh-ed25519", nil, []byte("ssh-ed25518"), key), false},
		{"rsa", strings.Replace(testKey, "ssh-ed25519", "ssh-rsa", 1), false},
		{"ecdsa", sshKey("ecdsa-sha2-nistp256", nil, []byte("ecdsa-sha2-nistp256"), []byte("nistp256"), bytes.Repeat([]byte{4}, 65)), false},
		{"bad base64", "ssh-ed25519 AAAA*AAA", false},
		{"double space", strings.Replace(testKey, " ", "  ", 1), false},
		{"quote in comment", sshKey("ssh-ed25519", nil, ed, key) + ` a"b`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkKey(tt.key)
			if (err == nil) != tt.ok {
				t.Fatalf("checkKey = %v, want ok=%v", err, tt.ok)
			}
			if tt.ok {
				return
			}
			cfg := testConfig()
			cfg.AuthorizedKey = tt.key
			dir := filepath.Join(t.TempDir(), "state")
			if m, err := Create(dir, cfg); err == nil {
				m.Close()
				t.Fatal("Create accepted the key")
			}
			if _, err := os.Stat(dir); err == nil {
				t.Error("refused key created a state directory")
			}
		})
	}
}

func TestConfigHashCoversEveryField(t *testing.T) {
	base := testConfig()
	h := base.hash()
	c := base
	c.DiskGiB++
	if c.hash() == h {
		t.Error("hash ignores disk size")
	}
	c = base
	c.Image.ID = "4243"
	if c.hash() == h {
		t.Error("hash ignores image id")
	}
}
