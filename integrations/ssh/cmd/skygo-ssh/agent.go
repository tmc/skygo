package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/tmc/skygo/internal/server"
)

// runAgent serves the canonical skygo server from an agent directory that
// the bootstrap filled. It adds no execution backend or protocol: serve is
// server.Run, given the directory's TLS identity and bearer token, the
// built-in local target renamed to the remote host's name, and state in
// the directory's data subdirectory.
func runAgent(ctx context.Context, args []string, stderr io.Writer, serve func(context.Context, server.Config) error) error {
	fs := flag.NewFlagSet("skygo-ssh agent", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("dir", "", "agent directory holding cert.pem, key.pem and token")
	listen := fs.String("listen", "", "loopback address, 127.0.0.1:PORT or [::1]:PORT")
	target := fs.String("target", "", "name of the built-in local target")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("agent: unexpected argument %q", fs.Arg(0))
	}
	cfg, err := agentConfig(*dir, *listen, *target)
	if err != nil {
		return fmt.Errorf("agent: %w", err)
	}
	return serve(ctx, cfg)
}

func agentConfig(dir, listen, target string) (server.Config, error) {
	if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
		return server.Config{}, errors.New("directory must be a clean absolute path")
	}
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		return server.Config{}, fmt.Errorf("listen address: %w", err)
	}
	if host != "127.0.0.1" && host != "::1" {
		return server.Config{}, errors.New("listen address must be 127.0.0.1 or [::1]")
	}
	if target == "" {
		return server.Config{}, errors.New("no target name")
	}
	certPEM, err := readPrivate(filepath.Join(dir, "cert.pem"), 64<<10)
	if err != nil {
		return server.Config{}, err
	}
	keyPEM, err := readPrivate(filepath.Join(dir, "key.pem"), 64<<10)
	if err != nil {
		return server.Config{}, err
	}
	defer clear(keyPEM)
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return server.Config{}, errors.New("invalid TLS identity")
	}
	token, err := readToken(filepath.Join(dir, "token"))
	if err != nil {
		return server.Config{}, err
	}
	exe, err := os.Executable()
	if err != nil {
		return server.Config{}, fmt.Errorf("locate supervisor executable: %w", err)
	}
	return server.Config{
		DurableLocal:    true,
		LocalSupervisor: exe,
		DataDir:         filepath.Join(dir, "data"),
		Listen:          listen,
		Token:           token,
		LocalTargetName: target,
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
			NextProtos:   []string{"http/1.1"},
		},
	}, nil
}

// readToken returns the bearer token in the private file name: one line of
// at most 4096 printable ASCII bytes without spaces.
func readToken(name string) (string, error) {
	data, err := readPrivate(name, 4097)
	if err != nil {
		return "", err
	}
	token := strings.TrimSuffix(string(data), "\n")
	if token == "" || len(token) > 4096 || strings.ContainsFunc(token, func(r rune) bool { return r < 0x21 || r > 0x7e }) {
		return "", fmt.Errorf("%s: invalid token", name)
	}
	return token, nil
}

// readPrivate returns the contents of the regular file name, which must
// not be accessible to group or others and must hold at most limit bytes.
func readPrivate(name string, limit int64) ([]byte, error) {
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
