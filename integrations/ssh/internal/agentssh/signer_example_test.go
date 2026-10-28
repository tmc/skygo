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

// This example selects one pinned key from an SSH agent and names it in a
// Target. A real caller would use agent.NewClient on its own connection to
// SSH_AUTH_SOCK, and close that connection only after every client using
// the signer is done. The Target's files are not read and nothing is
// dialed. NewClient reads the enrollment files; Dial also connects.
func ExampleAgentSigner() {
	keyring := agent.NewKeyring()
	other, _ := newKey()
	enrolled, pinned := newKey()
	_, absent := newKey()
	for _, k := range []ed25519.PrivateKey{other, enrolled} {
		if err := keyring.Add(agent.AddedKey{PrivateKey: k}); err != nil {
			panic(err)
		}
	}

	signer, err := AgentSigner(keyring, pinned)
	if err != nil {
		panic(err)
	}
	fmt.Println(ssh.FingerprintSHA256(signer.PublicKey()) == pinned)

	_, err = AgentSigner(keyring, absent)
	fmt.Println(err != nil)

	t := Target{
		SSHAddress:     "gpu1.example.com:22",
		User:           "ubuntu",
		Signer:         signer,
		SignerKey:      pinned,
		KnownHostsFile: "/home/alice/.config/skygo/ssh/gpu1/known_hosts",
		AgentAddress:   "127.0.0.1:8443",
		AgentName:      "agent.skygo.internal",
		AgentCAFile:    "/home/alice/.config/skygo/ssh/gpu1/ca.pem",
	}
	fmt.Println(t.Endpoint())
	// Output:
	// true
	// true
	// https://agent.skygo.internal
}
