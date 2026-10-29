package agentssh

import (
	"context"
	"errors"
	"strings"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"golang.org/x/crypto/ssh/knownhosts"
)

func (f *fixture) target() Target {
	c := f.cfg
	return Target{
		SSHAddress:     c.sshAddress,
		User:           c.user,
		KeyFile:        c.keyFile,
		KnownHostsFile: c.knownHostsFile,
		AgentAddress:   c.agentAddress,
		AgentName:      c.agentName,
		AgentCAFile:    c.agentCAFile,
	}
}

func TestTargetClient(t *testing.T) {
	f := newFixture(t)
	tg := f.target()
	if tg.Endpoint() != "https://"+testAgentName {
		t.Fatalf("Endpoint = %q", tg.Endpoint())
	}
	client, err := NewClient(tg)
	if err != nil {
		t.Fatal(err)
	}
	cc := skyv1connect.NewControlServiceClient(client, tg.Endpoint(), connect.WithAcceptCompression("gzip", nil, nil))
	req := connect.NewRequest(&v1.GetServerInfoRequest{})
	req.Header().Set("Authorization", "Bearer "+f.token)
	if _, err := cc.GetServerInfo(context.Background(), req); err != nil {
		t.Fatal(err)
	}
}

func TestDial(t *testing.T) {
	f := newFixture(t)
	client, err := Dial(context.Background(), f.target())
	if err != nil {
		t.Fatal(err)
	}
	f.waitHandshake()
	if f.handshakeCount() != 1 {
		t.Errorf("handshakes = %d, want 1", f.handshakeCount())
	}
	// The fixture refuses sessions; Dial itself opened nothing.
	if _, opens, requests, _ := f.snapshot(); len(opens) != 0 || len(requests) != 0 {
		t.Errorf("opens %q, requests %q; want none", opens, requests)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDialRefused(t *testing.T) {
	tests := []struct {
		name string
		edit func(f *fixture, tg *Target)
		err  string
	}{
		{"mismatched host key", func(f *fixture, tg *Target) {
			tg.KnownHostsFile = f.write("known_hosts.other", knownhosts.Line([]string{f.cfg.sshAddress}, newSigner(t).PublicKey())+"\n", 0o644)
		}, "mismatched ssh host key"},
		{"hashed known_hosts", func(f *fixture, tg *Target) {
			tg.KnownHostsFile = f.write("known_hosts.hashed", knownhosts.HashHostname(f.cfg.sshAddress)+" "+strings.SplitN(knownhosts.Line(nil, f.hostKey.PublicKey()), " ", 2)[1]+"\n", 0o644)
		}, "not supported"},
		{"other client key", func(f *fixture, tg *Target) {
			tg.KeyFile = f.writeKey("other", newSigner(t))
		}, "unable to authenticate"},
		{"unpinned signer", func(f *fixture, tg *Target) {
			tg.KeyFile, tg.Signer, tg.SignerKey = "", f.userKey, keyID(newSigner(t))
		}, "is not the pinned"},
		{"key file and signer", func(f *fixture, tg *Target) {
			tg.Signer, tg.SignerKey = f.userKey, keyID(f.userKey)
		}, "exactly one"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			tg := f.target()
			tt.edit(f, &tg)
			client, err := Dial(context.Background(), tg)
			if err == nil {
				client.Close()
				t.Fatalf("Dial succeeded, want %q", tt.err)
			}
			if !strings.Contains(err.Error(), tt.err) {
				t.Fatalf("Dial = %v, want %q", err, tt.err)
			}
			if strings.HasPrefix(tt.name, "mismatched") {
				var hk *hostKeyError
				if !errors.As(err, &hk) {
					t.Errorf("Dial = %v, want a *hostKeyError", err)
				}
			}
			if _, opens, _, _ := f.snapshot(); len(opens) != 0 {
				t.Errorf("opens %q, want none", opens)
			}
		})
	}
}

func TestDialCanceled(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client, err := Dial(ctx, f.target())
	if err == nil {
		client.Close()
		t.Fatal("Dial succeeded with a canceled context")
	}
	if f.handshakeCount() != 0 {
		t.Errorf("handshakes = %d, want 0", f.handshakeCount())
	}
}
