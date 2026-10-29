package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"

	"github.com/tmc/skygo/integrations/ssh/internal/agentidentity"
	"github.com/tmc/skygo/integrations/ssh/internal/bootstrap"
	"github.com/tmc/skygo/internal/server"
)

// runAgent serves the canonical skygo server from an agent directory that
// the bootstrap filled. It adds no execution backend or protocol: serve is
// server.Run, given the directory's TLS identity and the built-in local
// target renamed to the remote host's name. By default it serves state in
// the directory's data subdirectory with the directory's bearer token.
// Given an operator-initialized installation instead, it serves that
// installation's state, scoped auth file and artifact directory, and the
// directory must hold no token.
func runAgent(ctx context.Context, args []string, stderr io.Writer, serve func(context.Context, server.Config) error) error {
	fs := flag.NewFlagSet("skygo-ssh agent", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("dir", "", "agent directory holding cert.pem, key.pem and, without an installation, token")
	listen := fs.String("listen", "", "loopback address, 127.0.0.1:PORT or [::1]:PORT")
	target := fs.String("target", "", "name of the built-in local target")
	var in bootstrap.Installation
	fs.StringVar(&in.DataDir, "data-dir", "", "existing state directory of an initialized installation")
	fs.StringVar(&in.AuthFile, "auth-file", "", "the installation's scoped auth file")
	fs.StringVar(&in.ArtifactDir, "artifact-dir", "", "the installation's initialized artifact directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("agent: unexpected argument %q", fs.Arg(0))
	}
	cfg, err := agentConfig(*dir, *listen, *target, in)
	if err != nil {
		return fmt.Errorf("agent: %w", err)
	}
	return serve(ctx, cfg)
}

func agentConfig(dir, listen, target string, in bootstrap.Installation) (server.Config, error) {
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
	certPEM, err := agentidentity.ReadPrivate(filepath.Join(dir, "cert.pem"), 64<<10)
	if err != nil {
		return server.Config{}, err
	}
	keyPEM, err := agentidentity.ReadPrivate(filepath.Join(dir, "key.pem"), 64<<10)
	if err != nil {
		return server.Config{}, err
	}
	defer clear(keyPEM)
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return server.Config{}, errors.New("invalid TLS identity")
	}
	var token string
	if in.IsZero() {
		token, err = agentidentity.ReadToken(filepath.Join(dir, "token"))
	} else {
		err = checkInstallation(dir, in)
	}
	if err != nil {
		return server.Config{}, err
	}
	exe, err := os.Executable()
	if err != nil {
		return server.Config{}, fmt.Errorf("locate supervisor executable: %w", err)
	}
	cfg := server.Config{
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
	}
	if !in.IsZero() {
		cfg.DataDir, cfg.AuthFile, cfg.ArtifactDir = in.DataDir, in.AuthFile, in.ArtifactDir
		cfg.ExistingInstallation = true
	}
	return cfg, nil
}

// checkInstallation refuses an incomplete installation, or one that is not
// already initialized, before the server opens anything. It requires the
// three paths to be clean and absolute, the state and artifact directories
// and the state's jobs.db and auth file to exist privately without
// symlinks, and the agent directory to hold no legacy token. The server's
// Config.ExistingInstallation route, which inspects the store's current
// scoped state, the auth file's owner and the artifact root's marker before
// taking the writer and never creates or migrates state, remains
// authoritative; this only refuses earlier.
func checkInstallation(dir string, in bootstrap.Installation) error {
	for _, p := range []string{in.DataDir, in.AuthFile, in.ArtifactDir} {
		if !bootstrap.ValidPath(p) {
			return errors.New("an installation requires clean absolute --data-dir, --auth-file and --artifact-dir")
		}
	}
	if _, err := os.Lstat(filepath.Join(dir, "token")); !errors.Is(err, fs.ErrNotExist) {
		return errors.New("agent directory holds a legacy token; an installation agent uses only the auth file")
	}
	for _, p := range []struct {
		name string
		dir  bool
	}{{in.DataDir, true}, {filepath.Join(in.DataDir, "jobs.db"), false}, {in.AuthFile, false}, {in.ArtifactDir, true}} {
		info, err := os.Lstat(p.name)
		if err != nil {
			return fmt.Errorf("installation not initialized: %w", err)
		}
		if p.dir && !info.IsDir() {
			return fmt.Errorf("installation not initialized: %s is not a directory", p.name)
		}
		if !p.dir && !info.Mode().IsRegular() {
			return fmt.Errorf("installation not initialized: %s is not a regular file", p.name)
		}
		if info.Mode().Perm()&0o077 != 0 {
			return fmt.Errorf("%s: accessible to group or others", p.name)
		}
	}
	return nil
}
