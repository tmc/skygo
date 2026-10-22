package agentssh

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

// checkPinned reports whether signer's public key is a plain key, not a
// certificate, whose SHA256 fingerprint is want, in ssh.FingerprintSHA256
// form. Certificates are recognized by their wire form, because a key
// listed by an SSH agent is never an *ssh.Certificate value even when it
// holds a certificate. The pinned client identity is separate from the
// server host key and the agent's TLS identity.
func checkPinned(signer ssh.Signer, want string) error {
	if !validFingerprint(want) {
		return errors.New("signer fingerprint must be SHA256:<unpadded base64>")
	}
	if signer == nil {
		return errors.New("no signer")
	}
	key := signer.PublicKey()
	if key == nil {
		return errors.New("signer has no public key")
	}
	parsed, err := ssh.ParsePublicKey(key.Marshal())
	if err != nil {
		return errors.New("signer public key does not parse")
	}
	if _, ok := parsed.(*ssh.Certificate); ok {
		return errors.New("certificate signers are not supported")
	}
	if got := ssh.FingerprintSHA256(key); got != want {
		return fmt.Errorf("signer key %s is not the pinned %s", got, want)
	}
	return nil
}

func validFingerprint(s string) bool {
	hash, ok := strings.CutPrefix(s, "SHA256:")
	if !ok {
		return false
	}
	sum, err := base64.RawStdEncoding.DecodeString(hash)
	return err == nil && len(sum) == 32 && base64.RawStdEncoding.EncodeToString(sum) == hash
}

// agentSigner returns the one key held by a whose SHA256 fingerprint is
// want, refusing an agent that holds no such key or more than one, and a
// match that checkPinned refuses, such as a certificate. It
// only lists the agent's keys: it signs, adds, removes and forwards
// nothing, and only the selected key is ever offered to a server.
//
// The returned signer signs through a, so the caller keeps owning a's
// connection, typically to SSH_AUTH_SOCK. The caller bounds each agent
// operation, for example with a deadline on that connection, keeps the
// connection open while any client using the signer may still connect,
// and then closes it. A client whose signer's agent is closed fails to
// authenticate; it does not fall back to another key.
func agentSigner(a agent.Agent, want string) (ssh.Signer, error) {
	if !validFingerprint(want) {
		return nil, errors.New("agentssh: signer fingerprint must be SHA256:<unpadded base64>")
	}
	signers, err := a.Signers()
	if err != nil {
		return nil, fmt.Errorf("agentssh: list ssh agent keys: %w", err)
	}
	var match []ssh.Signer
	for _, s := range signers {
		if k := s.PublicKey(); k != nil && ssh.FingerprintSHA256(k) == want {
			match = append(match, s)
		}
	}
	switch len(match) {
	case 0:
		return nil, fmt.Errorf("agentssh: ssh agent holds no key %s", want)
	case 1:
		if err := checkPinned(match[0], want); err != nil {
			return nil, fmt.Errorf("agentssh: %w", err)
		}
		return match[0], nil
	default:
		return nil, fmt.Errorf("agentssh: ssh agent holds %d keys %s", len(match), want)
	}
}
