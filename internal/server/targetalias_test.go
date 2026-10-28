package server_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/server"
)

// authorized returns a request carrying the bearer token "secret".
func authorized[T any](m *T) *connect.Request[T] {
	r := connect.NewRequest(m)
	r.Header().Set("Authorization", "Bearer secret")
	return r
}

func TestLocalTargetName(t *testing.T) {
	r := startConfig(t, server.Config{DataDir: t.TempDir(), Token: "secret", LocalTargetName: "dgx"})
	api := skyv1connect.NewControlServiceClient(&http.Client{Timeout: 5 * time.Second}, r.url)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	targets, err := api.ListTargets(ctx, authorized(&v1.ListTargetsRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if len(targets.Msg.Targets) != 1 || targets.Msg.Targets[0].Name != "dgx" || targets.Msg.Targets[0].Kind != "local" {
		t.Fatalf("targets = %v; want one local target named dgx", targets.Msg.Targets)
	}
	info, err := api.GetServerInfo(ctx, authorized(&v1.GetServerInfoRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if len(info.Msg.Targets) != 1 || info.Msg.Targets[0].Name != "dgx" {
		t.Fatalf("server info targets = %v", info.Msg.Targets)
	}

	// An untargeted task lands on the renamed target; the old name is gone.
	plan, err := api.PlanTask(ctx, authorized(&v1.PlanTaskRequest{Task: &v1.Task{Argv: []string{"/bin/echo", "alias"}}}))
	if err != nil {
		t.Fatal(err)
	}
	if plan.Msg.Target != "dgx" {
		t.Fatalf("planned target = %q; want dgx", plan.Msg.Target)
	}
	if _, err := api.PlanTask(ctx, authorized(&v1.PlanTaskRequest{Task: &v1.Task{Argv: []string{"/bin/echo"}, Target: "local"}})); err == nil {
		t.Fatal("task naming the default local target was planned")
	}
	accepted, err := api.SubmitJob(ctx, authorized(&v1.SubmitJobRequest{RequestId: "alias", Task: &v1.Task{Argv: []string{"/bin/echo", "alias"}, Target: "dgx"}}))
	if err != nil {
		t.Fatal(err)
	}
	job := accepted.Msg.Job
	for job.State != v1.JobState_JOB_STATE_SUCCEEDED {
		if job.State == v1.JobState_JOB_STATE_FAILED || job.State == v1.JobState_JOB_STATE_CANCELED || job.State == v1.JobState_JOB_STATE_UNKNOWN {
			t.Fatalf("job ended %v: %s", job.State, job.Reason)
		}
		time.Sleep(20 * time.Millisecond)
		got, err := api.GetJob(ctx, authorized(&v1.GetJobRequest{Id: job.Id}))
		if err != nil {
			t.Fatal(err)
		}
		job = got.Msg.Job
	}
	if job.Target != "dgx" {
		t.Fatalf("job target = %q; want dgx", job.Target)
	}
	stop(t, r)
}

func TestLocalTargetNameRefused(t *testing.T) {
	for _, tt := range []struct {
		name string
		cfg  server.Config
		want string
	}{
		{"upper case", server.Config{LocalTargetName: "DGX"}, "lowercase DNS name"},
		{"leading hyphen", server.Config{LocalTargetName: "-dgx"}, "lowercase DNS name"},
		{"empty label", server.Config{LocalTargetName: "dgx..lan"}, "lowercase DNS name"},
		{"space", server.Config{LocalTargetName: "dgx box"}, "lowercase DNS name"},
		{"slash", server.Config{LocalTargetName: "dgx/0"}, "lowercase DNS name"},
		{"too long", server.Config{LocalTargetName: strings.Repeat("a", 64)}, "lowercase DNS name"},
		{"local disabled", server.Config{LocalTargetName: "dgx", DisableLocal: true, Targets: noTargets}, "disabled local target"},
		{"tls without certificate", server.Config{TLSConfig: &tls.Config{}}, "no certificate"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tt.cfg.DataDir = filepath.Join(t.TempDir(), "state")
			tt.cfg.Listen = "127.0.0.1:0"
			err := server.Run(context.Background(), tt.cfg)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Run error = %v; want %q", err, tt.want)
			}
			if _, err := os.Stat(tt.cfg.DataDir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("refused configuration created state: %v", err)
			}
		})
	}
}

func TestTLSConfig(t *testing.T) {
	cert, roots := selfSigned(t, "agent.test")
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"}}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	dir := t.TempDir()
	done := make(chan struct{})
	var runErr error
	go func() {
		runErr = server.Run(ctx, server.Config{DataDir: dir, Listen: address, Token: "secret", LocalTargetName: "dgx", TLSConfig: cfg})
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
			if runErr != nil {
				t.Error(runErr)
			}
		case <-time.After(15 * time.Second):
			t.Error("TLS server did not shut down")
			<-done // Do not retire its directory while the original Run is live.
		}
	})
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, ServerName: "agent.test"}}}
	deadline := time.Now().Add(8 * time.Second)
	for {
		resp, err := client.Get("https://" + address + "/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				if resp.TLS == nil || resp.TLS.NegotiatedProtocol == "h2" {
					t.Fatalf("TLS state = %+v; want HTTP/1.1 over TLS", resp.TLS)
				}
				break
			}
		}
		select {
		case <-done:
			t.Fatalf("server exited during startup: %v", runErr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("TLS server did not become ready: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	api := skyv1connect.NewControlServiceClient(client, "https://"+address)
	targets, err := api.ListTargets(context.Background(), authorized(&v1.ListTargetsRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if len(targets.Msg.Targets) != 1 || targets.Msg.Targets[0].Name != "dgx" {
		t.Fatalf("targets = %v", targets.Msg.Targets)
	}
	plain := &http.Client{Timeout: time.Second}
	if resp, err := plain.Get("http://" + address + "/ready"); err == nil {
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Fatal("plain HTTP request was served")
		}
	}
	cancel()
	select {
	case <-done:
		if runErr != nil {
			t.Fatal(runErr)
		}
	case <-time.After(12 * time.Second):
		t.Fatal("TLS server shutdown exceeded bound")
	}
	if c, err := net.DialTimeout("tcp", address, time.Second); err == nil {
		c.Close()
		t.Fatal("listener remained available after shutdown")
	}
}

func noTargets(string) ([]backend.Target, error) { return nil, nil }

// selfSigned returns a certificate for name that is its own root.
func selfSigned(t *testing.T, name string) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		DNSNames:              []string{name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(parsed)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: parsed}, roots
}
