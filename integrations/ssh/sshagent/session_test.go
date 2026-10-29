package sshagent

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/integrations/ssh/internal/agentsession"
)

// A captureTransport records each request it is given and fails it, so
// that nothing is sent anywhere. It is a MODEL of the agent transport.
type captureTransport struct {
	mu  sync.Mutex
	got []*http.Request
}

var errModeled = errors.New("modeled transport refuses every request")

func (c *captureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	c.mu.Lock()
	c.got = append(c.got, req)
	c.mu.Unlock()
	if req.Body != nil {
		req.Body.Close()
	}
	return nil, errModeled
}

// TestControlFixedAuth checks that the session's control client sends the
// session's token in place of the caller's, offers no compression through
// the session transport, and sends each request once.
func TestControlFixedAuth(t *testing.T) {
	next := &captureTransport{}
	hc := &http.Client{Transport: agentsession.Transport{Next: next}}
	cc := newControl(newLedger(nil), hc, "https://"+agentsession.AgentName, "session-token")
	req := connect.NewRequest(&v1.GetServerInfoRequest{})
	req.Header().Set("Authorization", "Bearer caller-token")
	if _, err := cc.GetServerInfo(context.Background(), req); !errors.Is(err, errModeled) {
		t.Fatalf("GetServerInfo = %v, want the modeled failure", err)
	}
	if len(next.got) != 1 {
		t.Fatalf("sent %d requests, want 1: a failure is not retried", len(next.got))
	}
	out := next.got[0]
	if got := out.Header.Values("Authorization"); len(got) != 1 || got[0] != "Bearer session-token" {
		t.Errorf("Authorization = %q, want the session's token only", got)
	}
	if out.Header.Get("Accept-Encoding") != "" || out.Header.Get("Connect-Accept-Encoding") != "" {
		t.Errorf("sent encodings %q", out.Header)
	}
	if out.URL.String() != "https://"+agentsession.AgentName+"/skygo.v1.ControlService/GetServerInfo" {
		t.Errorf("URL = %s", out.URL)
	}
}

// TestOpenError checks that an *OpenError names only the cleanup that did
// not settle, never the text of its cause or Close errors, which here
// carry a secret, and that it unwraps each of them unchanged.
func TestOpenError(t *testing.T) {
	const secret = "s3cret-bearer-0f9e"
	cause := errors.New("skygo-ssh: agent stopped before it was ready: remote said " + secret)
	signerClose := errors.New("modeled ssh agent close failure " + secret)
	sshClose := &modelCloseError{secret}
	var err error = &OpenError{err: &agentsession.UnsettledError{Err: cause, SignerClose: signerClose, SSHClose: sshClose}}
	want := "sshagent: open failed and its cleanup did not settle: ssh agent connection, bootstrap ssh connection"
	if err.Error() != want {
		t.Errorf("Error = %q, want %q", err, want)
	}
	for _, format := range []string{"%v", "%s", "%+v", "%q"} {
		if s := fmt.Sprintf(format, err); strings.Contains(s, secret) {
			t.Errorf("Sprintf(%q) = %q, which holds the secret", format, s)
		}
	}
	for _, e := range []error{cause, signerClose, sshClose} {
		if !errors.Is(err, e) {
			t.Errorf("errors.Is(err, %q) = false", e)
		}
	}
	var u *agentsession.UnsettledError
	if !errors.As(err, &u) || u.SignerClose != signerClose || u.SSHClose != sshClose {
		t.Errorf("errors.As = %v", u)
	}
	var mc *modelCloseError
	if !errors.As(err, &mc) || mc != sshClose {
		t.Errorf("errors.As(*modelCloseError) = %v, want the original", mc)
	}

	// The cause alone, settled by a nested ssh agent Close, is named too.
	nested := &OpenError{err: &agentsession.UnsettledError{Err: fmt.Errorf("skygo-ssh: %w", &agentsession.UnsettledError{Err: cause, SignerClose: signerClose})}}
	if got := nested.Error(); got != "sshagent: open failed and its cleanup did not settle: ssh agent connection" {
		t.Errorf("nested Error = %q", got)
	}
}

// A modelCloseError is a MODELED typed Close failure whose text quotes a
// secret.
type modelCloseError struct{ secret string }

func (e *modelCloseError) Error() string { return "close: remote said " + e.secret }

// TestSessionClose checks that a session using a key file, which holds no
// SSH agent connection, closes cleanly every time, and that a nil or zero
// Session owns nothing. The sticky Close of an SSH agent connection itself
// is tested in agentsession.
func TestSessionClose(t *testing.T) {
	s := newTestSession(nil, &modelTransport{})
	for range 3 {
		if err := s.Close(); err != nil {
			t.Fatalf("Close with a key file = %v", err)
		}
	}
	var zero Session
	if err := zero.Close(); err != nil || zero.Control() != nil || zero.Binding() != (Binding{}) {
		t.Errorf("zero Session: Close = %v, Control = %v, Binding = %+v", err, zero.Control(), zero.Binding())
	}
	var nilSession *Session
	if err := nilSession.Close(); err != nil || nilSession.Control() != nil || nilSession.Binding() != (Binding{}) {
		t.Errorf("nil Session: Close = %v, Control = %v, Binding = %+v", err, nilSession.Control(), nilSession.Binding())
	}
}
