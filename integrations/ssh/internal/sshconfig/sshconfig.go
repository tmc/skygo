package sshconfig

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// maxOutput bounds the `ssh -G` output Resolve reads.
const maxOutput = 256 << 10

// A Host is the resolved configuration of one alias.
type Host struct {
	Alias         string
	Address       string // canonical host:port
	User          string
	IdentityFiles []string // as configured; "~/" is expanded
	IdentityAgent string   // "" for SSH_AUTH_SOCK, "none", or an absolute path
}

// A Runner runs name with args and returns its standard output.
type Runner func(ctx context.Context, name string, args ...string) ([]byte, error)

// Resolve evaluates alias with `ssh -G`. A nil run executes the local ssh.
func Resolve(ctx context.Context, alias, home string, run Runner) (Host, error) {
	if !validAlias(alias) {
		return Host{}, fmt.Errorf("sshconfig: invalid host alias %q", alias)
	}
	if run == nil {
		run = runSSH
	}
	out, err := run(ctx, "ssh", "-G", "--", alias)
	if err != nil {
		return Host{}, fmt.Errorf("sshconfig: ssh -G %s: %w", alias, err)
	}
	return Parse(alias, home, out)
}

func runSSH(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &limitedWriter{buf: &stdout, n: maxOutput + 1}
	cmd.Stderr = &limitedWriter{buf: &stderr, n: 4 << 10}
	if err := cmd.Run(); err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return nil, fmt.Errorf("%w: %s", err, msg)
		}
		return nil, err
	}
	if stdout.Len() > maxOutput {
		return nil, errors.New("sshconfig: ssh -G output too large")
	}
	return stdout.Bytes(), nil
}

// A limitedWriter keeps the first n bytes written and discards the rest.
type limitedWriter struct {
	buf *bytes.Buffer
	n   int
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if room := w.n - w.buf.Len(); room > 0 {
		w.buf.Write(p[:min(len(p), room)])
	}
	return len(p), nil
}

// Parse interprets the `ssh -G` output for alias. Home expands "~/" in
// identity file names.
func Parse(alias, home string, out []byte) (Host, error) {
	if len(out) > maxOutput {
		return Host{}, errors.New("sshconfig: ssh -G output too large")
	}
	h := Host{Alias: alias}
	var hostname, port string
	for _, line := range strings.Split(string(out), "\n") {
		key, value, _ := strings.Cut(strings.TrimSpace(line), " ")
		key, value = strings.ToLower(key), strings.TrimSpace(value)
		if err := refuse(key, value); err != nil {
			return Host{}, fmt.Errorf("sshconfig: %s: %w", alias, err)
		}
		switch key {
		case "hostname":
			hostname = value
		case "port":
			port = value
		case "user":
			h.User = value
		case "identityfile":
			h.IdentityFiles = append(h.IdentityFiles, expandHome(value, home))
		case "identityagent":
			h.IdentityAgent = value
		}
	}
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil || n == 0 || strconv.FormatUint(n, 10) != port {
		return Host{}, fmt.Errorf("sshconfig: %s: invalid port %q", alias, port)
	}
	if !validHost(hostname) {
		return Host{}, fmt.Errorf("sshconfig: %s: hostname %q must be a lowercase name or an IP address", alias, hostname)
	}
	h.Address = net.JoinHostPort(hostname, port)
	if h.User == "" || strings.ContainsFunc(h.User, func(r rune) bool { return r <= ' ' || r > '~' }) {
		return Host{}, fmt.Errorf("sshconfig: %s: invalid user", alias)
	}
	switch a := h.IdentityAgent; {
	case a == "SSH_AUTH_SOCK":
		h.IdentityAgent = ""
	case a == "" || a == "none":
	case filepath.IsAbs(a) && filepath.Clean(a) == a:
	default:
		return Host{}, fmt.Errorf("sshconfig: %s: identityagent must be SSH_AUTH_SOCK, none or a clean absolute path", alias)
	}
	return h, nil
}

// refuse reports settings a direct connection would silently bypass.
func refuse(key, value string) error {
	switch key {
	case "proxycommand", "proxyjump", "remotecommand", "localcommand":
		if value != "" && value != "none" {
			return fmt.Errorf("%s is not supported", key)
		}
	case "localforward", "remoteforward", "dynamicforward":
		return fmt.Errorf("%s is not supported", key)
	case "forwardagent", "permitlocalcommand", "forwardx11":
		if value != "no" {
			return fmt.Errorf("%s %s is not supported", key, value)
		}
	}
	return nil
}

func expandHome(name, home string) string {
	if rest, ok := strings.CutPrefix(name, "~/"); ok && home != "" {
		return filepath.Join(home, rest)
	}
	return name
}

func validAlias(s string) bool {
	if s == "" || len(s) > 255 || s[0] == '-' {
		return false
	}
	return strings.Trim(s, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-") == ""
}

func validHost(s string) bool {
	if net.ParseIP(s) != nil {
		return !strings.Contains(s, "%")
	}
	if s == "" || len(s) > 253 {
		return false
	}
	for _, label := range strings.Split(s, ".") {
		if label == "" || label[0] == '-' || label[len(label)-1] == '-' ||
			strings.Trim(label, "abcdefghijklmnopqrstuvwxyz0123456789-") != "" {
			return false
		}
	}
	return true
}
