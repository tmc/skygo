package sshagent

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
)

type commandForwardPeer struct {
	skyv1connect.UnimplementedControlServiceHandler
}

func (commandForwardPeer) GetServerInfo(context.Context, *connect.Request[v1.GetServerInfoRequest]) (*connect.Response[v1.GetServerInfoResponse], error) {
	return connect.NewResponse(&v1.GetServerInfoResponse{Capabilities: []v1.Capability{v1.Capability_CAPABILITY_ENVIRONMENTS_V1}}), nil
}

type commandForwardAnnouncement struct {
	once  sync.Once
	ready chan string
}

func (w *commandForwardAnnouncement) Write(b []byte) (int, error) {
	if s := strings.Fields(string(b)); len(s) > 1 && s[0] == "forwarding" {
		w.once.Do(func() { w.ready <- s[1] })
	}
	return len(b), nil
}

type commandCloseFault struct {
	io.ReadCloser
	err   error
	count *atomic.Int32
}

func (b *commandCloseFault) Close() error {
	b.count.Add(1)
	return errors.Join(b.ReadCloser.Close(), b.err)
}

func TestCommandCanonicalForward(t *testing.T) {
	// CLI, Connect, loopback listener/relay, request contexts and original
	// HTTP bodies are actual. SSH enrollment and the environment capability
	// announcement/application are MODELS; no environment VM is provisioned.
	for _, badClose := range []bool{false, true} {
		t.Run(map[bool]string{false: "clean", true: "failed-body-close"}[badClose], func(t *testing.T) {
			const id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			mux := http.NewServeMux()
			p, h := skyv1connect.NewControlServiceHandler(commandForwardPeer{})
			mux.Handle(p, h)
			var served atomic.Int32
			mux.HandleFunc("/environments/"+id+"/forward/", func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer captured-token" {
					t.Error("forward escaped captured session bearer")
				}
				data, err := io.ReadAll(r.Body)
				if err != nil || string(data) != "request-payload" {
					t.Error("forward request payload changed")
				}
				served.Add(1)
				io.WriteString(w, "forwarded")
			})
			srv := httptest.NewServer(mux)
			defer srv.Close()
			token := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(token, []byte("caller-token\n"), 0600); err != nil {
				t.Fatal(err)
			}
			hc := *srv.Client()
			base := hc.Transport
			var closed atomic.Int32
			raw := &secretError{"modeled forwarded body Close", secret}
			hc.Transport = commandRoundTrip(func(req *http.Request) (*http.Response, error) {
				r, err := base.RoundTrip(req)
				if badClose && r != nil && strings.HasPrefix(req.URL.Path, "/environments/") {
					r.Body = &commandCloseFault{ReadCloser: r.Body, err: raw, count: &closed}
				}
				return r, err
			})
			signer := &modelSigner{}
			announce := &commandForwardAnnouncement{ready: make(chan string, 1)}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			done := make(chan struct{})
			var result error
			t.Cleanup(func() { cancel(); <-done })
			go func() {
				result = runCommand(ctx, commandAgent{client: &hc, owner: signer, endpoint: srv.URL, tokenFile: token, bearer: "captured-token", host: "alias", target: "local"}, Command{Args: []string{"env", "forward", id}, Stdout: io.Discard, Stderr: announce})
				close(done)
			}()
			var addr string
			select {
			case addr = <-announce.ready:
			case <-done:
				t.Fatalf("forward ended: %v", result)
			case <-ctx.Done():
				t.Fatal("listener not announced")
			}
			req, err := http.NewRequestWithContext(ctx, "POST", addr+"/probe", strings.NewReader("request-payload"))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer local-caller")
			client := &http.Client{Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}}
			defer client.CloseIdleConnections()
			response, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			data, readErr := io.ReadAll(response.Body)
			closeErr := response.Body.Close()
			if readErr != nil || closeErr != nil || string(data) != "forwarded" || served.Load() != 1 {
				t.Fatal("forward body failed")
			}
			cancel()
			<-done
			if badClose {
				if !errors.Is(result, raw) || signer.count() != 0 || closed.Load() != 1 || strings.Contains(result.Error(), secret) {
					t.Fatal("forward failed first Close was discarded or retried")
				}
			} else if result != nil || signer.count() != 1 {
				t.Fatalf("forward cleanup: %v", result)
			}
		})
	}
}
