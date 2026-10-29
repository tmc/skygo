package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"slices"

	"golang.org/x/crypto/ssh"
)

// publicFingerprint returns the SHA256 fingerprint of the one public key
// found next to the identity files, as NAME.pub. It reads no private key.
func publicFingerprint(identityFiles []string) (string, error) {
	var found []string
	for _, name := range identityFiles {
		data, err := readLimited(name+".pub", 64<<10)
		if err != nil {
			continue
		}
		key, _, _, _, err := ssh.ParseAuthorizedKey(data)
		if err != nil {
			continue
		}
		if fp := ssh.FingerprintSHA256(key); !slices.Contains(found, fp) {
			found = append(found, fp)
		}
	}
	switch len(found) {
	case 0:
		return "", errors.New("no public key next to the configured identity files; pass --identity-fingerprint")
	case 1:
		return found[0], nil
	default:
		return "", fmt.Errorf("identity files hold %d public keys %v; pass --identity-fingerprint", len(found), found)
	}
}

func readLimited(name string, limit int64) ([]byte, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, limit))
}
