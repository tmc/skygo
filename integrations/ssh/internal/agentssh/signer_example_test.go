package agentssh

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

// newKey returns a new private key and its SHA256 fingerprint.
func newKey() (ed25519.PrivateKey, string) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	pub, err := ssh.NewPublicKey(priv.Public())
	if err != nil {
		panic(err)
	}
	return priv, ssh.FingerprintSHA256(pub)
}

// This example selects one pinned key from an SSH agent. A real caller
// would use agent.NewClient on its own connection to SSH_AUTH_SOCK, and
// close that connection only after every client using the signer is done.
func Example_agentSigner() {
	keyring := agent.NewKeyring()
	other, _ := newKey()
	enrolled, pinned := newKey()
	_, absent := newKey()
	for _, k := range []ed25519.PrivateKey{other, enrolled} {
		if err := keyring.Add(agent.AddedKey{PrivateKey: k}); err != nil {
			panic(err)
		}
	}

	signer, err := agentSigner(keyring, pinned)
	if err != nil {
		panic(err)
	}
	c := config{signer: signer, signerKey: pinned}
	fmt.Println(ssh.FingerprintSHA256(c.signer.PublicKey()) == pinned)

	_, err = agentSigner(keyring, absent)
	fmt.Println(err != nil)
	// Output:
	// true
	// true
}
