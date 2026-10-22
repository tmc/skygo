package ec2api

import (
	"reflect"
	"strings"
	"testing"
)

const (
	fpECDSA   = "Q2l2aWxpemF0aW9uIGlzIGEgc2VyaWVzIG9mIGRpc2N"
	fpED25519 = "b3ZlcmVkIGFuZCB0aGVuIGZvcmdvdHRlbiBmaWxlc3l"
	fpForged  = "Rm9yZ2VkRm9yZ2VkRm9yZ2VkRm9yZ2VkRm9yZ2VkRm9"
	agentSum  = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
	lineage   = "0123456789abcdef0123456789abcdef"
)

const testConsole = "[    0.000000] Linux version 6.1.0\r\n" +
	"Cloud-init v. 24.1 running 'modules:final'\n" +
	"-----BEGIN SSH HOST KEY FINGERPRINTS-----\n" +
	"256 SHA256:" + fpECDSA + " root@ip-10-0-0-5 (ECDSA)\n" +
	"256 SHA256:" + fpED25519 + " no comment (ED25519)\n" +
	"-----END SSH HOST KEY FINGERPRINTS-----\n" +
	"-----BEGIN SKYGO AGENT IDENTITY-----\n" +
	"tls-sha256: " + agentSum + "\n" +
	"request-domain: " + lineage + ":7\n" +
	"-----END SKYGO AGENT IDENTITY-----\n" +
	"workload output follows\n"

var testConsoleWant = &Console{
	HostKeys: []HostKey{
		{Type: "ECDSA", Bits: 256, SHA256: fpECDSA},
		{Type: "ED25519", Bits: 256, SHA256: fpED25519},
	},
	AgentSHA256:   agentSum,
	RequestDomain: lineage + ":7",
}

func TestParseConsole(t *testing.T) {
	c, err := ParseConsole([]byte(testConsole))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c, testConsoleWant) {
		t.Fatalf("ParseConsole = %+v, want %+v", c, testConsoleWant)
	}
}

func TestParseConsolePrefix(t *testing.T) {
	var b strings.Builder
	for _, l := range strings.Split(testConsole, "\n") {
		if l != "" {
			b.WriteString("ec2: " + l + "\n")
		}
	}
	c, err := ParseConsole([]byte(b.String()))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c, testConsoleWant) {
		t.Fatalf("ParseConsole = %+v", c)
	}
}

// TestParseConsoleFirstBlockOnly injects workload-written blocks and
// stray fingerprint lines after the image's blocks.
func TestParseConsoleFirstBlockOnly(t *testing.T) {
	forged := testConsole +
		"256 SHA256:" + fpForged + " root@evil (RSA)\n" +
		"-----BEGIN SSH HOST KEY FINGERPRINTS-----\n" +
		"256 SHA256:" + fpForged + " root@evil (ECDSA)\n" +
		"-----END SSH HOST KEY FINGERPRINTS-----\n" +
		"-----BEGIN SKYGO AGENT IDENTITY-----\n" +
		"tls-sha256: " + strings.Repeat("0", 64) + "\n" +
		"request-domain: " + lineage + ":8\n" +
		"-----END SKYGO AGENT IDENTITY-----\n"
	c, err := ParseConsole([]byte(forged))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c, testConsoleWant) {
		t.Fatalf("later blocks changed the result: %+v", c)
	}
}

func TestParseConsoleRejects(t *testing.T) {
	replace := func(old, new string) string {
		if !strings.Contains(testConsole, old) {
			t.Fatalf("console lacks %q", old)
		}
		return strings.Replace(testConsole, old, new, 1)
	}
	goodLater := "-----BEGIN SSH HOST KEY FINGERPRINTS-----\n256 SHA256:" + fpForged + " x (ECDSA)\n-----END SSH HOST KEY FINGERPRINTS-----\n"
	tests := []struct {
		name string
		text string
	}{
		{"empty", ""},
		{"no host block", replace("-----BEGIN SSH HOST KEY FINGERPRINTS-----\n", "")},
		{"no agent block", replace("-----BEGIN SKYGO AGENT IDENTITY-----\n", "")},
		{"incomplete host block", replace("-----END SSH HOST KEY FINGERPRINTS-----\n", "")},
		{"interleaved line", replace(" (ECDSA)\n", " (ECDSA)\nkernel: hello\n")},
		{"malformed first block, good later block", replace(" (ECDSA)\n", " (DSA)\n") + goodLater},
		{"empty host block", "-----BEGIN SSH HOST KEY FINGERPRINTS-----\n-----END SSH HOST KEY FINGERPRINTS-----\n" + testConsole},
		{"repeated key type", replace(" no comment (ED25519)", " no comment (ECDSA)")},
		{"short fingerprint", replace(fpECDSA, fpECDSA[1:])},
		{"agent order", replace("tls-sha256: "+agentSum+"\nrequest-domain: "+lineage+":7\n", "request-domain: "+lineage+":7\ntls-sha256: "+agentSum+"\n")},
		{"agent uppercase", replace(agentSum, strings.ToUpper(agentSum))},
		{"zero epoch", replace(lineage+":7", lineage+":0")},
		{"epoch overflow", replace(lineage+":7", lineage+":99999999999999999999")},
		{"extra agent line", replace("-----END SKYGO AGENT IDENTITY-----", "epoch: 7\n-----END SKYGO AGENT IDENTITY-----")},
		{"mixed prefix", replace(" (ECDSA)\n256", " (ECDSA)\nec2: 256")},
		{"too large", testConsole + strings.Repeat("x", maxConsoleBytes)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if c, err := ParseConsole([]byte(tt.text)); err == nil {
				t.Fatalf("accepted: %+v", c)
			}
		})
	}
}

func TestParseConsolePrefixedBlockNeedsPrefix(t *testing.T) {
	text := "ec2: -----BEGIN SSH HOST KEY FINGERPRINTS-----\n" +
		"256 SHA256:" + fpECDSA + " root@h (ECDSA)\n" +
		"ec2: -----END SSH HOST KEY FINGERPRINTS-----\n"
	if _, err := ParseConsole([]byte(text + testConsole)); err == nil {
		t.Fatal("unprefixed line in prefixed block accepted")
	}
}
