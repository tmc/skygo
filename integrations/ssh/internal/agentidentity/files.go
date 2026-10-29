// Package agentidentity reads and creates the private files that identify
// a skygo-ssh agent: its TLS certificate and key and its bearer token.
// Both the agent and its clients read them with ReadPrivate.
package agentidentity

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// An Identity holds the local copies of one agent's TLS identity and
// bearer token. The certificate is self-signed and is its own root, so a
// client that trusts it reaches only the agent holding its key.
type Identity struct {
	Cert, Key, Token string // file names
	Bearer           string // the token
}

func files(dir string) Identity {
	return Identity{
		Cert:  filepath.Join(dir, "cert.pem"),
		Key:   filepath.Join(dir, "key.pem"),
		Token: filepath.Join(dir, "token"),
	}
}

// Load returns the identity in dir, creating it for the TLS server name if
// dir does not exist. Every file is private. Concurrent callers agree on
// one identity: a new one is written to a temporary directory and renamed
// into place only if dir is still absent.
func Load(dir, name string) (Identity, error) {
	if _, err := os.Lstat(dir); errors.Is(err, fs.ErrNotExist) {
		if err := create(dir, name); err != nil {
			return Identity{}, err
		}
	} else if err != nil {
		return Identity{}, err
	}
	return Check(dir, name)
}

func create(dir, name string) error {
	parent := filepath.Dir(dir)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return err
	}
	tmp, err := os.MkdirTemp(parent, ".new-")
	if err != nil {
		return err
	}
	files, err := newIdentity(name)
	if err == nil {
		err = writeFiles(tmp, files)
	}
	if err == nil {
		err = os.Rename(tmp, dir)
	}
	if err != nil {
		for file := range files {
			os.Remove(filepath.Join(tmp, file))
		}
		os.Remove(tmp)
		if _, statErr := os.Lstat(dir); statErr == nil {
			// Another caller created dir first.
			return nil
		}
		return fmt.Errorf("create agent identity: %w", err)
	}
	return nil
}

func writeFiles(dir string, files map[string][]byte) error {
	for name, data := range files {
		f, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return err
		}
		_, err = f.Write(data)
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// newIdentity returns a new ECDSA P-256 key, a self-signed certificate for
// name valid for ten years, and a 256-bit bearer token.
func newIdentity(name string) (map[string][]byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, err
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: name},
		DNSNames:              []string{name},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.AddDate(10, 0, 0),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		return nil, err
	}
	return map[string][]byte{
		"cert.pem": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		"key.pem":  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
		"token":    []byte(hex.EncodeToString(token) + "\n"),
	}, nil
}

// Check loads the identity in dir and checks that its files are private
// and that its key and certificate pair for name.
func Check(dir, name string) (Identity, error) {
	id := files(dir)
	info, err := os.Lstat(dir)
	if err != nil {
		return Identity{}, err
	}
	if !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return Identity{}, fmt.Errorf("%s must be a private directory", dir)
	}
	certPEM, err := ReadPrivate(id.Cert, 64<<10)
	if err != nil {
		return Identity{}, err
	}
	keyPEM, err := ReadPrivate(id.Key, 64<<10)
	if err != nil {
		return Identity{}, err
	}
	defer clear(keyPEM)
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return Identity{}, fmt.Errorf("%s: invalid agent identity", dir)
	}
	leaf := pair.Leaf
	if leaf == nil {
		leaf, err = x509.ParseCertificate(pair.Certificate[0])
		if err != nil {
			return Identity{}, fmt.Errorf("%s: invalid certificate", dir)
		}
	}
	if !slices.Contains(leaf.DNSNames, name) {
		return Identity{}, fmt.Errorf("%s: certificate is not for %s", dir, name)
	}
	if id.Bearer, err = ReadToken(id.Token); err != nil {
		return Identity{}, err
	}
	return id, nil
}

// ReadToken returns the bearer token in the private file name: one line of
// at most 4096 printable ASCII bytes without spaces.
func ReadToken(name string) (string, error) {
	data, err := ReadPrivate(name, 4097)
	if err != nil {
		return "", err
	}
	token := strings.TrimSuffix(string(data), "\n")
	if token == "" || len(token) > 4096 || strings.ContainsFunc(token, func(r rune) bool { return r < 0x21 || r > 0x7e }) {
		return "", fmt.Errorf("%s: invalid token", name)
	}
	return token, nil
}

// ReadPrivate returns the contents of the regular file name, which must
// not be accessible to group or others and must hold at most limit bytes.
func ReadPrivate(name string, limit int64) ([]byte, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	before, err := os.Lstat(name)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || !os.SameFile(before, info) {
		return nil, fmt.Errorf("%s: not a regular file", name)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("%s: accessible to group or others", name)
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("%s: larger than %d bytes", name, limit)
	}
	return data, nil
}
