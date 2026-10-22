package ec2api

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Console block markers. cloud-init prints the SSH block; the skygo agent
// image prints the agent block at first boot.
const (
	hostKeysBegin = "-----BEGIN SSH HOST KEY FINGERPRINTS-----"
	hostKeysEnd   = "-----END SSH HOST KEY FINGERPRINTS-----"
	agentBegin    = "-----BEGIN SKYGO AGENT IDENTITY-----"
	agentEnd      = "-----END SKYGO AGENT IDENTITY-----"

	// consolePrefix is an optional prefix some images write before
	// every console line of a block.
	consolePrefix = "ec2: "

	maxHostKeys = 8
)

var (
	hostKeyRE       = regexp.MustCompile(`^([0-9]{3,5}) SHA256:([A-Za-z0-9+/]{43}) [\x21-\x7e][\x20-\x7e]{0,254} \((RSA|ECDSA|ED25519)\)$`)
	agentSHA256RE   = regexp.MustCompile(`^tls-sha256: ([0-9a-f]{64})$`)
	requestDomainRE = regexp.MustCompile(`^request-domain: ([0-9a-f]{32}):([1-9][0-9]{0,19})$`)
)

// A HostKey is one SSH host key fingerprint.
type HostKey struct {
	Type   string // RSA, ECDSA or ED25519
	Bits   int
	SHA256 string // unpadded base64 SHA-256, without the "SHA256:" prefix
}

// A Console holds the identity blocks parsed from console output.
type Console struct {
	HostKeys      []HostKey // in output order
	AgentSHA256   string    // lowercase hexadecimal SHA-256 of the agent TLS certificate
	RequestDomain string    // agent request domain as LINEAGE:EPOCH
}

// ParseConsole parses the first SSH host key fingerprint block and the
// first skygo agent identity block in output:
//
//	-----BEGIN SSH HOST KEY FINGERPRINTS-----
//	256 SHA256:<43 base64 characters> root@host (ED25519)
//	-----END SSH HOST KEY FINGERPRINTS-----
//	-----BEGIN SKYGO AGENT IDENTITY-----
//	tls-sha256: <64 lowercase hexadecimal digits>
//	request-domain: <32 lowercase hexadecimal digits>:<positive epoch>
//	-----END SKYGO AGENT IDENTITY-----
//
// Every line of a block may carry the prefix "ec2: " if its BEGIN line
// does. Only the first block of each kind counts: text outside it is
// ignored, and a first block that is incomplete or holds any other line is
// an error, never a reason to read a later block.
//
// ParseConsole checks form only. Whether the first block of a boot was
// written by the image and not by a workload is a property of the image and
// of how the output was obtained, which ParseConsole cannot establish.
func ParseConsole(output []byte) (*Console, error) {
	if len(output) > maxConsoleBytes {
		return nil, fmt.Errorf("console output exceeds %d bytes", maxConsoleBytes)
	}
	lines := strings.Split(string(output), "\n")
	for i, l := range lines {
		lines[i] = strings.TrimSuffix(l, "\r")
	}
	hostLines, err := firstBlock(lines, hostKeysBegin, hostKeysEnd)
	if err != nil {
		return nil, err
	}
	agentLines, err := firstBlock(lines, agentBegin, agentEnd)
	if err != nil {
		return nil, err
	}

	c := new(Console)
	if len(hostLines) == 0 || len(hostLines) > maxHostKeys {
		return nil, fmt.Errorf("SSH host key block has %d keys, want 1 to %d", len(hostLines), maxHostKeys)
	}
	seen := make(map[string]bool)
	for _, l := range hostLines {
		m := hostKeyRE.FindStringSubmatch(l)
		if m == nil {
			return nil, errors.New("SSH host key block has a malformed line")
		}
		if seen[m[3]] {
			return nil, fmt.Errorf("SSH host key block repeats %s", m[3])
		}
		seen[m[3]] = true
		bits, _ := strconv.Atoi(m[1])
		c.HostKeys = append(c.HostKeys, HostKey{Type: m[3], Bits: bits, SHA256: m[2]})
	}

	if len(agentLines) != 2 {
		return nil, errors.New("agent identity block must have exactly two lines")
	}
	m := agentSHA256RE.FindStringSubmatch(agentLines[0])
	if m == nil {
		return nil, errors.New("agent identity block has a malformed tls-sha256 line")
	}
	c.AgentSHA256 = m[1]
	m = requestDomainRE.FindStringSubmatch(agentLines[1])
	if m == nil {
		return nil, errors.New("agent identity block has a malformed request-domain line")
	}
	if _, err := strconv.ParseUint(m[2], 10, 64); err != nil {
		return nil, errors.New("agent identity block epoch is out of range")
	}
	c.RequestDomain = m[1] + ":" + m[2]
	return c, nil
}

// firstBlock returns the lines between the first begin marker and the
// next end marker, with the BEGIN line's prefix removed from each.
func firstBlock(lines []string, begin, end string) ([]string, error) {
	start, prefix := -1, ""
	for i, l := range lines {
		if l == begin || l == consolePrefix+begin {
			start, prefix = i, strings.TrimSuffix(l, begin)
			break
		}
	}
	if start < 0 {
		return nil, fmt.Errorf("console output has no %s line", begin)
	}
	var block []string
	for _, l := range lines[start+1:] {
		if prefix != "" {
			var ok bool
			if l, ok = strings.CutPrefix(l, prefix); !ok {
				return nil, fmt.Errorf("block %s has a line without its prefix", begin)
			}
		}
		if l == end {
			return block, nil
		}
		if len(block) == maxHostKeys {
			return nil, fmt.Errorf("block %s has more than %d lines", begin, maxHostKeys)
		}
		block = append(block, l)
	}
	return nil, fmt.Errorf("block %s is incomplete", begin)
}
