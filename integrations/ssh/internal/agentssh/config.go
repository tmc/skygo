package agentssh

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// Size limits for configuration files.
const (
	maxKeyFile        = 64 << 10
	maxKnownHostsFile = 1 << 20
	maxCAFile         = 1 << 20
)

// A config names one SSH server, the agent installed behind it, and the
// files that authenticate both. Every field is required.
type config struct {
	sshAddress     string // SSH server as canonical host:port
	user           string // SSH user
	keyFile        string // unencrypted private key, mode 0400 or 0600
	knownHostsFile string // known_hosts file dedicated to sshAddress
	agentAddress   string // agent as seen from the SSH server: 127.0.0.1:port or [::1]:port
	agentName      string // DNS name in the agent's TLS certificate
	agentCAFile    string // PEM certificates that sign the agent's certificate
}

// endpoint returns the base URL of the agent.
func (c config) endpoint() string { return "https://" + c.agentName }

func (c config) validate() error {
	if _, err := hostPort(c.sshAddress); err != nil {
		return fmt.Errorf("ssh address: %w", err)
	}
	if !validUser(c.user) {
		return errors.New("ssh user must be printable ASCII without spaces")
	}
	host, err := hostPort(c.agentAddress)
	if err != nil {
		return fmt.Errorf("agent address: %w", err)
	}
	if host != "127.0.0.1" && host != "::1" {
		return errors.New("agent address must be 127.0.0.1 or [::1]")
	}
	if !validDNSName(c.agentName) {
		return errors.New("agent name must be a lowercase DNS name")
	}
	for _, name := range []string{c.keyFile, c.knownHostsFile, c.agentCAFile} {
		if !filepath.IsAbs(name) || filepath.Clean(name) != name {
			return fmt.Errorf("file name %q is not a clean absolute path", name)
		}
	}
	return nil
}

// hostPort checks that address is a canonical host:port whose host is a
// lowercase name or an IP address, and returns the host.
func hostPort(address string) (string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", err
	}
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil || n == 0 || strconv.FormatUint(n, 10) != port {
		return "", fmt.Errorf("invalid port %q", port)
	}
	if host == "" || strings.Trim(host, "abcdefghijklmnopqrstuvwxyz0123456789.-:") != "" ||
		strings.Contains(host, ":") && net.ParseIP(host) == nil ||
		net.JoinHostPort(host, port) != address {
		return "", fmt.Errorf("non-canonical address %q", address)
	}
	return host, nil
}

func validUser(user string) bool {
	if user == "" || len(user) > 256 {
		return false
	}
	for i := 0; i < len(user); i++ {
		if user[i] <= ' ' || user[i] > '~' {
			return false
		}
	}
	return true
}

func validDNSName(name string) bool {
	if name == "" || len(name) > 253 || net.ParseIP(name) != nil {
		return false
	}
	for _, label := range strings.Split(name, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' ||
			strings.Trim(label, "abcdefghijklmnopqrstuvwxyz0123456789-") != "" {
			return false
		}
	}
	return true
}

// A closeError reports an original owner whose Close failed. The owner
// stays reachable; it is never closed again or reopened.
type closeError struct {
	owner io.Closer // *os.File or request body
	err   error     // from Close
	cause error     // earlier failure, if any
}

func (e *closeError) Error() string {
	if f, ok := e.owner.(*os.File); ok {
		return "agentssh: close " + f.Name() + ": " + e.err.Error()
	}
	return "agentssh: close request body: " + e.err.Error()
}

func (e *closeError) Unwrap() []error { return nonNil(e.err, e.cause) }

func nonNil(errs ...error) []error {
	var out []error
	for _, err := range errs {
		if err != nil {
			out = append(out, err)
		}
	}
	return out
}

// readFile returns the contents of the regular file name, refusing files
// larger than limit. If private is set, the file must not be accessible to
// group or others. The file is opened once and must be the file that was
// inspected under name; the parent directories are trusted not to change.
// On failure, any bytes read are zeroed.
func readFile(name string, limit int64, private bool) ([]byte, error) {
	info, err := os.Lstat(name)
	if err != nil {
		return nil, err
	}
	if err := checkFile(name, info, private); err != nil {
		return nil, err
	}
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	data, rerr := readOpen(f, info, limit, private)
	if err := f.Close(); err != nil {
		clear(data)
		return nil, &closeError{owner: f, err: err, cause: rerr}
	}
	if rerr != nil {
		clear(data)
		return nil, rerr
	}
	return data, nil
}

func readOpen(f *os.File, before os.FileInfo, limit int64, private bool) ([]byte, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(before, info) {
		return nil, fmt.Errorf("%s: changed while opening", f.Name())
	}
	if err := checkFile(f.Name(), info, private); err != nil {
		return nil, err
	}
	if info.Size() > limit {
		return nil, fmt.Errorf("%s: larger than %d bytes", f.Name(), limit)
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err == nil && int64(len(data)) > limit {
		err = fmt.Errorf("%s: larger than %d bytes", f.Name(), limit)
	}
	return data, err
}

func checkFile(name string, info os.FileInfo, private bool) error {
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s: not a regular file", name)
	}
	if private && info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("%s: private key is accessible to group or others", name)
	}
	return nil
}

// loadSigner reads an unencrypted private key. Errors never include the
// file's contents.
func loadSigner(name string) (ssh.Signer, error) {
	data, err := readFile(name, maxKeyFile, true)
	if err != nil {
		return nil, err
	}
	defer clear(data)
	signer, err := ssh.ParsePrivateKey(data)
	if err != nil {
		var missing *ssh.PassphraseMissingError
		if errors.As(err, &missing) {
			return nil, fmt.Errorf("%s: encrypted private keys are not supported", name)
		}
		return nil, fmt.Errorf("%s: invalid private key", name)
	}
	return signer, nil
}

// hostKeys holds the keys a strict known_hosts file enrolls for one SSH
// server.
//
// Only literal host names are supported. Hashed names, wildcards, negations
// and @cert-authority lines are refused rather than partially emulated, and
// so is any line that does not parse. A key marked @revoked is refused for
// every host.
type hostKeys struct {
	host       string   // server address in knownhosts.Normalize form
	keys       [][]byte // enrolled host keys in wire form
	revoked    [][]byte
	algorithms []string // host key algorithms the enrolled keys verify
}

func parseKnownHosts(data []byte, address string) (*hostKeys, error) {
	k := &hostKeys{host: knownhosts.Normalize(address)}
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || line[0] == '#' {
			continue
		}
		if err := k.add(line); err != nil {
			return nil, fmt.Errorf("known_hosts line %d: %w", i+1, err)
		}
	}
	return k, nil
}

func (k *hostKeys) add(line string) error {
	marker, hosts, key, _, rest, err := ssh.ParseKnownHosts([]byte(line))
	if err != nil || len(rest) != 0 || strings.ContainsRune(line, '\r') {
		return errors.New("invalid entry")
	}
	fields := strings.Fields(line)
	if marker != "" {
		fields = fields[1:]
	}
	if _, ok := key.(*ssh.Certificate); ok || fields[1] != key.Type() {
		return errors.New("unsupported key")
	}
	switch marker {
	case "":
	case "revoked":
		k.revoked = append(k.revoked, key.Marshal())
		return nil
	default:
		return fmt.Errorf("@%s entries are not supported", marker)
	}
	match := false
	for _, h := range hosts {
		if h == "" || h[0] == '|' || strings.ContainsAny(h, "*?!") {
			return fmt.Errorf("host pattern %q is not supported", h)
		}
		if knownhosts.Normalize(h) == k.host {
			match = true
		}
	}
	if !match {
		return nil
	}
	k.keys = append(k.keys, key.Marshal())
	algorithms := []string{key.Type()}
	if key.Type() == ssh.KeyAlgoRSA {
		algorithms = []string{ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSASHA256}
	}
	for _, a := range algorithms {
		if !slices.Contains(k.algorithms, a) {
			k.algorithms = append(k.algorithms, a)
		}
	}
	return nil
}

// check reports whether key is an enrolled, unrevoked host key for hostname.
func (k *hostKeys) check(hostname string, key ssh.PublicKey) error {
	wire := key.Marshal()
	fingerprint := ssh.FingerprintSHA256(key)
	for _, r := range k.revoked {
		if bytes.Equal(r, wire) {
			return &hostKeyError{fingerprint, "revoked"}
		}
	}
	if knownhosts.Normalize(hostname) != k.host || len(k.keys) == 0 {
		return &hostKeyError{fingerprint, "unknown"}
	}
	for _, e := range k.keys {
		if bytes.Equal(e, wire) {
			return nil
		}
	}
	return &hostKeyError{fingerprint, "mismatched"}
}

// A hostKeyError reports a refused SSH host key.
type hostKeyError struct {
	fingerprint string // SHA256 fingerprint of the presented key
	reason      string // "unknown", "mismatched" or "revoked"
}

func (e *hostKeyError) Error() string {
	return "agentssh: " + e.reason + " ssh host key " + e.fingerprint
}

// newClient returns an HTTP client whose requests reach the agent c names.
// It refuses redirects; its transport never retries.
func newClient(c config) (*http.Client, error) {
	t, err := newTransport(c)
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: t, CheckRedirect: refuseRedirect}, nil
}

func refuseRedirect(*http.Request, []*http.Request) error {
	return errors.New("agentssh: redirect refused")
}

func newTransport(c config) (*transport, error) {
	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("agentssh: %w", err)
	}
	signer, err := loadSigner(c.keyFile)
	if err != nil {
		return nil, fmt.Errorf("agentssh: %w", err)
	}
	data, err := readFile(c.knownHostsFile, maxKnownHostsFile, false)
	if err != nil {
		return nil, fmt.Errorf("agentssh: %w", err)
	}
	hosts, err := parseKnownHosts(data, c.sshAddress)
	if err != nil {
		return nil, fmt.Errorf("agentssh: %s: %w", c.knownHostsFile, err)
	}
	data, err = readFile(c.agentCAFile, maxCAFile, false)
	if err != nil {
		return nil, fmt.Errorf("agentssh: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("agentssh: %s: no PEM certificates", c.agentCAFile)
	}
	return &transport{
		config: c,
		signer: signer,
		hosts:  hosts,
		tls: &tls.Config{
			RootCAs:    roots,
			ServerName: c.agentName,
			MinVersion: tls.VersionTLS12,
			NextProtos: []string{"http/1.1"},
		},
		dial:        (&net.Dialer{}).DialContext,
		budget:      requestBudget,
		closeBudget: requestBudget,
	}, nil
}
