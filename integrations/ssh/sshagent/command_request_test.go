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

	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
)

type commandRequestFault struct {
	io.ReadCloser
	gate    <-chan struct{}
	entered chan struct{}
	once    sync.Once
	closes  atomic.Int32
	err     error
}

func (b *commandRequestFault) Close() error {
	b.closes.Add(1)
	b.once.Do(func() { close(b.entered) })
	<-b.gate
	return errors.Join(b.ReadCloser.Close(), b.err)
}

func TestCommandCanonicalRefusalFirstClose(t *testing.T) {
	// Only SSH enrollment, capability/app announcement and the original
	// Close failure are MODELED. The actual command runner, CLI listener,
	// ReverseProxy, requests, firstClose guards and loopback bodies run.
	mux := http.NewServeMux()
	p, h := skyv1connect.NewControlServiceHandler(commandForwardPeer{})
	mux.Handle(p, h)
	var app atomic.Int32
	mux.HandleFunc("/environments/", func(w http.ResponseWriter, r *http.Request) {
		app.Add(1)
		io.Copy(io.Discard, r.Body)
		io.WriteString(w, "unexpected dispatch")
	})
	mux.HandleFunc("/fill", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "held response") })
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("caller-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	gate, both := make(chan struct{}), make(chan struct{})
	var releaseGate, releaseBoth, fillOnce sync.Once
	var clients sync.WaitGroup
	done := make(chan struct{})
	var result error
	t.Cleanup(func() {
		cancel()
		releaseBoth.Do(func() { close(both) })
		releaseGate.Do(func() { close(gate) })
		<-done
		clients.Wait()
	})
	var count atomic.Int32
	raw := []error{&secretError{"first original request Close", secret}, &secretError{"second original request Close", secret}}
	originals := make(chan *commandRequestFault, 2)
	calls := make(chan *call, 2)
	contexts := make(chan (<-chan struct{}), 2)
	base := srv.Client().Transport
	agent := commandAgent{client: srv.Client(), owner: &modelSigner{}, endpoint: srv.URL, tokenFile: token, bearer: "captured-token", host: "alias", target: "local"}
	agent.bodyFault = func(c *call, original io.ReadCloser) io.ReadCloser {
		c.l.mu.Lock()
		frame, ok := c.resp.(*commandFrame)
		c.l.mu.Unlock()
		if !ok {
			t.Error("actual command frame not retained")
			return original
		}
		rootCtx := frame.root.Context()
		fillOnce.Do(func() {
			tr := &commandTransport{call: c, token: "captured-token", recorder: recorder{l: c.l, next: base}}
			for i := 0; i < 63; i++ {
				req, err := http.NewRequestWithContext(rootCtx, "GET", srv.URL+"/fill", nil)
				if err != nil {
					t.Error(err)
					return
				}
				if _, err := tr.RoundTrip(req); err != nil {
					t.Error("failed to prepare actual admitted response frontier")
					return
				}
			}
		})
		index := int(count.Add(1)) - 1
		if index >= 2 {
			t.Error("unexpected extra accepted request")
			return original
		}
		b := &commandRequestFault{ReadCloser: original, gate: gate, entered: make(chan struct{}), err: raw[index]}
		originals <- b
		calls <- c
		contexts <- rootCtx.Done()
		if index == 1 {
			releaseBoth.Do(func() { close(both) })
		}
		select {
		case <-both:
		case <-ctx.Done():
		}
		return b
	}
	announce := &commandForwardAnnouncement{ready: make(chan string, 1)}
	go func() {
		result = runCommand(ctx, agent, Command{Args: []string{"env", "forward", strings.Repeat("a", 32)}, Stdout: io.Discard, Stderr: announce})
		close(done)
	}()
	var addr string
	select {
	case addr = <-announce.ready:
	case <-done:
		t.Fatalf("forward ended: %v", result)
	case <-ctx.Done():
		t.Fatal("no listener")
	}
	client := &http.Client{Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}}
	t.Cleanup(client.CloseIdleConnections)
	responses := make(chan error, 2)
	for i := 0; i < 2; i++ {
		req, err := http.NewRequestWithContext(ctx, "POST", addr+"/probe", strings.NewReader("nonempty-request"))
		if err != nil {
			t.Fatal(err)
		}
		clients.Add(1)
		go func() {
			defer clients.Done()
			resp, err := client.Do(req)
			if resp != nil {
				_, rerr := io.Copy(io.Discard, resp.Body)
				cerr := resp.Body.Close()
				err = errors.Join(err, rerr, cerr)
			}
			responses <- err
		}()
	}
	captured := make([]*commandRequestFault, 0, 2)
	owners := make([]*call, 0, 2)
	for i := 0; i < 2; i++ {
		select {
		case b := <-originals:
			captured = append(captured, b)
		case <-ctx.Done():
			t.Fatal("request did not enter actual hook")
		}
		owners = append(owners, <-calls)
	}
	if owners[0] != owners[1] {
		t.Fatal("forward requests did not share actual command owner")
	}
	for i := 0; i < 2; i++ {
		stopped := <-contexts
		select {
		case <-stopped:
		case <-ctx.Done():
			t.Fatal("actual command context not canceled before blocked Close")
		}
		select {
		case <-captured[i].entered:
		case <-ctx.Done():
			t.Fatal("first Close not entered")
		}
	}
	c := owners[0]
	c.l.mu.Lock()
	pending, retained, high := c.refusalPending, len(c.refusals), c.refusalHighWater
	guardOwners := append([]*refusedRequest(nil), c.refusals...)
	c.l.mu.Unlock()
	if pending != 2 || retained != 2 || high != 2 || app.Load() != 0 {
		t.Fatal("pending frontier/owner/dispatch mismatch")
	}
	for _, r := range guardOwners {
		if r.request == nil || r.request.Body != r || r.original == nil || r.closed {
			t.Fatal("not the SAME proxy-captured guard/original")
		}
	}
	select {
	case <-done:
		t.Fatal("cancellation fabricated a Close join")
	default:
	}
	releaseGate.Do(func() { close(gate) })
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("actual command did not settle after original releases")
	}
	clients.Wait()
	for i := 0; i < 2; i++ {
		if err := <-responses; err != nil {
			t.Error(err)
		}
	}
	if !errors.Is(result, raw[0]) || !errors.Is(result, raw[1]) || strings.Contains(result.Error(), secret) {
		t.Fatal("raw original cause lost or leaked")
	}
	if agent.owner.(*modelSigner).count() != 0 || app.Load() != 0 {
		t.Fatal("unknown signer closed or refused request dispatched")
	}
	for _, b := range captured {
		if b.closes.Load() != 1 {
			t.Fatalf("original first Close invoked %d times by transport/proxy", b.closes.Load())
		}
	}
	c.l.mu.Lock()
	n := len(c.refusals)
	c.l.mu.Unlock()
	if n != 2 {
		t.Fatal("failed second owner pruned")
	}
}

func TestCommandRequestGuardRetires(t *testing.T) {
	l, c, tr := commandCall(t, nil)
	for i := 0; i < 10000; i++ {
		original := newModelBody(nil)
		r := tr.WrapRequestBody(original).(*refusedRequest)
		if _, err := io.ReadAll(r); err != nil {
			t.Fatal(err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		if len(c.refusals) != 0 || !r.retired || r.original != nil {
			t.Fatal("clean request guard retained")
		}
		before := original.readCount()
		if _, err := r.Read(make([]byte, 1)); err == nil || original.readCount() != before {
			t.Fatal("postClose underlying Read")
		}
		r.Close()
		if original.closeCount() != 1 {
			t.Fatal("first Close retried")
		}
	}
	if _, err := c.settle(nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := l.close(); err != nil {
		t.Fatal(err)
	}
}

func TestCommandRequestGuardLatePoison(t *testing.T) {
	raw := &secretError{"modeled late request read", secret}
	original := newModelBody(nil)
	original.readErr = errors.Join(io.EOF, raw)
	original.readGate = make(chan struct{})
	var release sync.Once
	done := make(chan struct{})
	started := false
	var result error
	t.Cleanup(func() {
		release.Do(func() { close(original.readGate) })
		if started {
			<-done
		}
	})
	l, c, tr := commandCall(t, nil)
	signer := &modelSigner{}
	l.signer = signer
	stopped := make(chan struct{})
	c.cancel = func() { close(stopped) }
	r := tr.WrapRequestBody(original).(*refusedRequest)
	started = true
	go func() { _, result = r.Read(make([]byte, 1)); close(done) }()
	<-original.readIn
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if len(c.refusals) != 1 || r.retired {
		t.Fatal("active actual request Read pruned after positive Close")
	}
	if _, err := c.settle(nil, nil); err == nil {
		t.Fatal("pending original invented settled")
	}
	release.Do(func() { close(original.readGate) })
	<-done
	if !errors.Is(result, raw) || strings.Contains(result.Error(), secret) {
		t.Fatal("late raw cause discarded/leaked")
	}
	select {
	case <-stopped:
	default:
		t.Fatal("late request poison did not stop command")
	}
	if err := l.close(); !errors.Is(err, raw) || signer.count() != 0 || len(c.refusals) != 1 {
		t.Fatal("request owner/cause lost")
	}
	r.Close()
	if original.closeCount() != 1 {
		t.Fatal("failed command retried first Close")
	}
}
