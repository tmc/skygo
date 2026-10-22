package agentssh

import (
	"context"
	"crypto/rand"
	"errors"
	"net"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

func keyID(s ssh.Signer) string { return ssh.FingerprintSHA256(s.PublicKey()) }

// A listOnly agent supports only Signers. Any other method panics through
// the nil embedded Agent, so a test fails if agentSigner signs, adds,
// removes or locks anything.
type listOnly struct {
	agent.Agent
	signers []ssh.Signer
	err     error
	calls   int
}

func (a *listOnly) Signers() ([]ssh.Signer, error) {
	a.calls++
	return a.signers, a.err
}

// newCert returns a user certificate for s's key, signed by a new CA.
func newCert(t *testing.T, s ssh.Signer) *ssh.Certificate {
	t.Helper()
	cert := &ssh.Certificate{
		Key:             s.PublicKey(),
		CertType:        ssh.UserCert,
		ValidPrincipals: []string{testUser},
		ValidBefore:     ssh.CertTimeInfinity,
	}
	if err := cert.SignCert(rand.Reader, newSigner(t)); err != nil {
		t.Fatal(err)
	}
	return cert
}

func certSigner(t *testing.T, s ssh.Signer) ssh.Signer {
	t.Helper()
	cs, err := ssh.NewCertSigner(newCert(t, s), s)
	if err != nil {
		t.Fatal(err)
	}
	return cs
}

// serveAgent serves a keyring holding the private keys of keys over an
// in-memory connection and returns an agent client for it. The returned
// stop closes the client's connection and joins the server.
func serveAgent(t *testing.T, keys ...ssh.Signer) (agent.ExtendedAgent, func()) {
	t.Helper()
	var added []agent.AddedKey
	for _, s := range keys {
		priv, _ := signerKeys.Load(s)
		added = append(added, agent.AddedKey{PrivateKey: priv})
	}
	return serveKeys(t, added...)
}

// serveKeys is serveAgent for explicit keys, such as certificates.
func serveKeys(t *testing.T, keys ...agent.AddedKey) (agent.ExtendedAgent, func()) {
	t.Helper()
	keyring := agent.NewKeyring()
	for _, k := range keys {
		if err := keyring.Add(k); err != nil {
			t.Fatal(err)
		}
	}
	client, server := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		agent.ServeAgent(keyring, server)
		server.Close()
	}()
	stopped := false
	stop := func() {
		if !stopped {
			stopped = true
			client.Close()
			<-done
		}
	}
	t.Cleanup(stop)
	return agent.NewClient(client), stop
}

func TestCheckPinned(t *testing.T) {
	a, b := newSigner(t), newSigner(t)
	cert := certSigner(t, a)
	tests := []struct {
		name   string
		signer ssh.Signer
		want   string
		err    string // "" if accepted
	}{
		{"match", a, keyID(a), ""},
		{"other key", a, keyID(b), "is not the pinned"},
		{"no signer", nil, keyID(a), "no signer"},
		{"empty pin", a, "", "must be"},
		{"md5 pin", a, ssh.FingerprintLegacyMD5(a.PublicKey()), "must be"},
		{"padded pin", a, keyID(a) + "=", "must be"},
		{"short pin", a, keyID(a)[:20], "must be"},
		{"certificate", cert, keyID(cert), "certificate"},
		{"certificate by key", cert, keyID(a), "certificate"},
	}
	for _, tt := range tests {
		err := checkPinned(tt.signer, tt.want)
		if tt.err == "" && err != nil || tt.err != "" && (err == nil || !strings.Contains(err.Error(), tt.err)) {
			t.Errorf("%s: checkPinned = %v, want %q", tt.name, err, tt.err)
		}
	}
}

func TestAgentSigner(t *testing.T) {
	a, b, c := newSigner(t), newSigner(t), newSigner(t)
	errList := errors.New("agent list failed")
	tests := []struct {
		name  string
		agent *listOnly
		want  string
		calls int
		err   string // "" if a is returned
	}{
		{"one of several", &listOnly{signers: []ssh.Signer{b, a, c}}, keyID(a), 1, ""},
		{"absent", &listOnly{signers: []ssh.Signer{b, c}}, keyID(a), 1, "holds no key"},
		{"empty agent", &listOnly{}, keyID(a), 1, "holds no key"},
		{"ambiguous", &listOnly{signers: []ssh.Signer{a, b, a}}, keyID(a), 1, "holds 2 keys"},
		{"agent error", &listOnly{err: errList}, keyID(a), 1, "agent list failed"},
		{"malformed pin", &listOnly{signers: []ssh.Signer{a}}, "SHA256:x", 0, "must be"},
	}
	for _, tt := range tests {
		got, err := agentSigner(tt.agent, tt.want)
		switch {
		case tt.err == "" && (err != nil || got != a):
			t.Errorf("%s: agentSigner = %v, %v; want the pinned signer", tt.name, got, err)
		case tt.err != "" && (err == nil || got != nil || !strings.Contains(err.Error(), tt.err)):
			t.Errorf("%s: agentSigner = %v, %v; want error %q", tt.name, got, err, tt.err)
		}
		if tt.agent.calls != tt.calls {
			t.Errorf("%s: listed agent %d times, want %d", tt.name, tt.agent.calls, tt.calls)
		}
	}
	if _, err := agentSigner(&listOnly{err: errList}, keyID(a)); !errors.Is(err, errList) {
		t.Errorf("agent error not wrapped: %v", err)
	}
}

// TestAgentSignerProtocol selects a key over the real agent protocol,
// signs through the agent, and checks that a closed agent fails rather
// than falling back.
func TestAgentSignerProtocol(t *testing.T) {
	a, b := newSigner(t), newSigner(t)
	client, stop := serveAgent(t, b, a)
	s, err := agentSigner(client, keyID(a))
	if err != nil {
		t.Fatal(err)
	}
	if keyID(s) != keyID(a) {
		t.Fatalf("selected %s, want %s", keyID(s), keyID(a))
	}
	data := []byte("session data")
	sig, err := s.Sign(rand.Reader, data)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.PublicKey().Verify(data, sig); err != nil {
		t.Errorf("agent signature does not verify with the pinned key: %v", err)
	}
	keys, err := client.List()
	if err != nil || len(keys) != 2 {
		t.Errorf("agent holds %d keys (%v) after selection, want the original 2", len(keys), err)
	}
	stop()
	if _, err := s.Sign(rand.Reader, data); err == nil {
		t.Error("signed through a closed agent")
	}
}

// TestAgentCertificateRefused pins a certificate held by a real agent by
// the certificate's own fingerprint. Agent-listed keys are not
// *ssh.Certificate values, so only a wire-form check refuses them.
func TestAgentCertificateRefused(t *testing.T) {
	a := newSigner(t)
	priv, _ := signerKeys.Load(a)
	cert := newCert(t, a)
	pin := ssh.FingerprintSHA256(cert)
	ag, _ := serveKeys(t, agent.AddedKey{PrivateKey: priv, Certificate: cert})
	if s, err := agentSigner(ag, pin); err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Errorf("agentSigner = %v, %v; want certificate refusal", s, err)
	}
	listed, err := ag.Signers()
	if err != nil || len(listed) != 1 {
		t.Fatalf("agent lists %d keys (%v), want the certificate", len(listed), err)
	}
	if err := checkPinned(listed[0], pin); err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Errorf("checkPinned(agent certificate) = %v, want certificate refusal", err)
	}
	c := config{
		sshAddress:     "host.example:2222",
		user:           "skygo",
		signer:         listed[0],
		signerKey:      pin,
		knownHostsFile: "/k/known_hosts",
		agentAddress:   "127.0.0.1:8443",
		agentName:      "agent.example",
		agentCAFile:    "/k/ca.pem",
	}
	if err := c.validate(); err == nil {
		t.Error("validate accepted an agent-held certificate signer")
	}
}

func TestConfigSignerMode(t *testing.T) {
	a, b := newSigner(t), newSigner(t)
	valid := config{
		sshAddress:     "host.example:2222",
		user:           "skygo",
		signer:         a,
		signerKey:      keyID(a),
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
		{"signer", func(*config) {}, true},
		{"signer and key file", func(c *config) { c.keyFile = "/k/id" }, false},
		{"neither", func(c *config) { c.signer, c.signerKey = nil, "" }, false},
		{"pin without signer", func(c *config) { c.signer, c.keyFile = nil, "/k/id" }, false},
		{"unpinned signer", func(c *config) { c.signerKey = "" }, false},
		{"other pin", func(c *config) { c.signerKey = keyID(b) }, false},
		{"key file", func(c *config) { c.signer, c.signerKey, c.keyFile = nil, "", "/k/id" }, true},
	}
	for _, tt := range tests {
		c := valid
		tt.edit(&c)
		if err := c.validate(); (err == nil) != tt.ok {
			t.Errorf("%s: validate() = %v, want ok %v", tt.name, err, tt.ok)
		}
	}
}

// TestSignerTransport authenticates the real transport with a key held
// only by an SSH agent; the config has no keyFile.
func TestSignerTransport(t *testing.T) {
	t.Run("pinned agent key", func(t *testing.T) {
		f := newFixture(t)
		ag, _ := serveAgent(t, newSigner(t), f.userKey)
		s, err := agentSigner(ag, keyID(f.userKey))
		if err != nil {
			t.Fatal(err)
		}
		client, _ := f.client(func(c *config) { c.keyFile, c.signer, c.signerKey = "", s, keyID(f.userKey) })
		if err := checkAgent(context.Background(), client, f.cfg, f.token, identity()); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("unenrolled agent key", func(t *testing.T) {
		f := newFixture(t)
		other := newSigner(t)
		ag, _ := serveAgent(t, other, f.userKey)
		s, err := agentSigner(ag, keyID(other))
		if err != nil {
			t.Fatal(err)
		}
		client, _ := f.client(func(c *config) { c.keyFile, c.signer, c.signerKey = "", s, keyID(other) })
		err = checkAgent(context.Background(), client, f.cfg, f.token, identity())
		if err == nil || !strings.Contains(err.Error(), "unable to authenticate") {
			t.Fatalf("err = %v, want authentication failure without trying the agent's other key", err)
		}
		if _, opens, _, got := f.snapshot(); len(opens) != 0 || len(got) != 0 {
			t.Errorf("opens %q, agent requests %d; want none", opens, len(got))
		}
	})
	t.Run("closed agent", func(t *testing.T) {
		f := newFixture(t)
		ag, stop := serveAgent(t, f.userKey)
		s, err := agentSigner(ag, keyID(f.userKey))
		if err != nil {
			t.Fatal(err)
		}
		client, _ := f.client(func(c *config) { c.keyFile, c.signer, c.signerKey = "", s, keyID(f.userKey) })
		stop()
		if err := checkAgent(context.Background(), client, f.cfg, f.token, identity()); err == nil {
			t.Fatal("authenticated through a closed agent")
		}
		if _, opens, _, got := f.snapshot(); len(opens) != 0 || len(got) != 0 {
			t.Errorf("opens %q, agent requests %d; want none", opens, len(got))
		}
	})
}
