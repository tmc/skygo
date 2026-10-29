package sshagent

import (
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"

	"github.com/tmc/skygo/integrations/ssh/internal/agentbinary"
	"github.com/tmc/skygo/integrations/ssh/internal/agentsession"
	"github.com/tmc/skygo/integrations/ssh/internal/agentssh"
	"github.com/tmc/skygo/integrations/ssh/internal/bootstrap"
	"github.com/tmc/skygo/integrations/ssh/internal/sshconfig"
)

// A Binary names a read-only Linux arm64 skygo-ssh executable. Commit is
// the operator's assertion of its clean source commit; Digest is its actual
// SHA-256. The executable format and digest do not attest source provenance.
type Binary struct{ Path, Commit, Digest string }

// A Config names one SSH host and how to select, install and reach the
// skygo agent on it. Every file and directory is a clean absolute path.
// Open checks the whole Config before it reads, builds or connects to
// anything.
type Config struct {
	// Address is the SSH server as a canonical host:port whose host is a
	// lowercase name or an IP address, and User the SSH user. Both are
	// used as given: Open reads no OpenSSH configuration.
	Address string
	User    string

	// KnownHostsFile is a known_hosts file dedicated to Address, listing
	// it by literal name.
	KnownHostsFile string

	// Exactly one client key is offered: the unencrypted private key in
	// KeyFile, or the key whose SHA256 fingerprint is Fingerprint in the
	// SSH agent listening on AgentSocket. Fingerprint is in the form
	// ssh-keygen -l prints: "SHA256:" and 43 unpadded base64 digits.
	KeyFile     string
	AgentSocket string
	Fingerprint string

	// Source is the skygo checkout the agent is built from. Without
	// AllowDirty it must have no uncommitted changes; with it, they are
	// built too.
	Source     string
	AllowDirty bool

	// Prebuilt selects a local binary instead of building Source. All three
	// fields must be set, Source empty and AllowDirty false. Its zero value
	// keeps source-build mode.
	Prebuilt Binary

	// IdentityDir holds the local TLS identity and token of each agent
	// build, in a private directory COMMIT/SHA256 below it. CacheDir holds
	// the builds.
	IdentityDir string
	CacheDir    string

	// Target names the agent's built-in local target: a lowercase DNS
	// name of at most 63 bytes. Port is the agent's remote loopback port,
	// 1024..65535, or 0 for 47080.
	Target string
	Port   int

	// RestartStopped starts a new agent if the recorded one has exited.
	RestartStopped bool

	// Installation, if not zero, is an operator-initialized installation
	// on the host for the agent to serve, and ScopedTokenFile a local
	// private file holding a token its auth file issued. Both are set or
	// neither. Without them the agent keeps its own store and legacy token.
	Installation    Installation
	ScopedTokenFile string

	// Log receives progress and diagnostics, never file contents or
	// credentials. Nil discards them.
	Log io.Writer
}

// An Installation names an operator-initialized skygo installation on the
// remote host: an existing state directory with scoped authentication
// active, its auth file, and its initialized artifact directory. Each is
// a clean absolute path of at most 200 letters, digits, '.', '_', '-' and
// '/'. Open never creates or changes any of them.
type Installation struct {
	DataDir     string
	AuthFile    string
	ArtifactDir string
}

// IsZero reports whether i names no installation.
func (i Installation) IsZero() bool { return i == Installation{} }

// check reports the first problem with c. It has no effects.
func (c Config) check() error {
	host, port, err := net.SplitHostPort(c.Address)
	if err != nil {
		return fmt.Errorf("sshagent: address: %w", err)
	}
	// sshconfig.Parse holds the checks skygo-ssh applies to a resolved
	// host, and agentssh.ValidAddress those of the SSH transport. A host
	// or port either refuses or would change is refused.
	h, err := sshconfig.Parse("sshagent", "", []byte("hostname "+host+"\nport "+port+"\nuser u\n"))
	if err != nil || h.Address != c.Address || !agentssh.ValidAddress(c.Address) {
		return fmt.Errorf("sshagent: address %q must be a canonical host:port with a lowercase name or an IP address", c.Address)
	}
	if c.User == "" || len(c.User) > 256 || strings.ContainsFunc(c.User, func(r rune) bool { return r <= ' ' || r > '~' }) {
		return errors.New("sshagent: user must be printable ASCII without spaces")
	}
	if !cleanAbs(c.KnownHostsFile) {
		return errors.New("sshagent: known hosts file must be a clean absolute path")
	}
	switch {
	case (c.KeyFile == "") == (c.AgentSocket == ""):
		return errors.New("sshagent: exactly one of key file and agent socket must be set")
	case c.KeyFile != "" && !cleanAbs(c.KeyFile):
		return errors.New("sshagent: key file must be a clean absolute path")
	case c.KeyFile != "" && c.Fingerprint != "":
		return errors.New("sshagent: fingerprint pins an agent key; it is not used with a key file")
	case c.AgentSocket != "" && !cleanAbs(c.AgentSocket):
		return errors.New("sshagent: agent socket must be a clean absolute path")
	case c.AgentSocket != "" && !agentssh.ValidFingerprint(c.Fingerprint):
		return errors.New("sshagent: an agent socket needs a SHA256 key fingerprint")
	}
	if c.Prebuilt == (Binary{}) {
		if !cleanAbs(c.Source) {
			return errors.New("sshagent: source must be a clean absolute path")
		}
	} else {
		if c.Source != "" || c.AllowDirty {
			return errors.New("sshagent: prebuilt binary and source build options are exclusive")
		}
		if err := (agentbinary.Spec(c.Prebuilt)).Check(); err != nil {
			return err
		}
	}
	for _, d := range []struct{ name, path string }{{"identity directory", c.IdentityDir}, {"cache directory", c.CacheDir}} {
		if !cleanAbs(d.path) {
			return fmt.Errorf("sshagent: %s must be a clean absolute path", d.name)
		}
	}
	if !agentsession.ValidTarget(c.Target) {
		return fmt.Errorf("sshagent: target name %q must be a lowercase DNS name", c.Target)
	}
	if c.Port != 0 && (c.Port < 1024 || c.Port > 65535) {
		return errors.New("sshagent: agent port must be in 1024..65535")
	}
	in := c.Installation
	switch {
	case in.IsZero() && c.ScopedTokenFile == "":
	case in.DataDir == "" || in.AuthFile == "" || in.ArtifactDir == "" || c.ScopedTokenFile == "":
		return errors.New("sshagent: an installation needs its data directory, auth file, artifact directory and a scoped token file")
	case !bootstrap.ValidPath(in.DataDir) || !bootstrap.ValidPath(in.AuthFile) || !bootstrap.ValidPath(in.ArtifactDir):
		return errors.New("sshagent: remote installation paths must be clean absolute paths of at most 200 letters, digits, '.', '_', '-' and '/'")
	case !cleanAbs(c.ScopedTokenFile):
		return errors.New("sshagent: scoped token file must be a clean absolute path")
	}
	return nil
}

func cleanAbs(name string) bool { return filepath.IsAbs(name) && filepath.Clean(name) == name }

// options returns the session options c names, less the scoped token.
func (c Config) options() agentsession.Options {
	port := c.Port
	if port == 0 {
		port = agentsession.DefaultPort
	}
	return agentsession.Options{
		Name:           c.Address,
		SSHAddress:     c.Address,
		User:           c.User,
		KnownHostsFile: c.KnownHostsFile,
		KeyFile:        c.KeyFile,
		AgentSocket:    c.AgentSocket,
		SignerKey:      c.Fingerprint,
		Source:         c.Source,
		AllowDirty:     c.AllowDirty,
		CacheDir:       c.CacheDir,
		IdentityDir:    c.IdentityDir,
		Target:         c.Target,
		Port:           port,
		RestartStopped: c.RestartStopped,
		Installation:   bootstrap.Installation(c.Installation),
		TokenFile:      c.ScopedTokenFile,
		Log:            c.Log,
	}
}
