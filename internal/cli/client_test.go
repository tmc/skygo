package cli

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
)

// A recordingTransport records the path of every request it carries.
type recordingTransport struct {
	mu    sync.Mutex
	paths []string
}

func TestOptionsHTTPClientDefault(t *testing.T) {
	srv := serve(t, &testService{})
	o := &options{endpoint: srv.URL, token: "test-token"}
	r, err := o.client().ListTargets(context.Background(), request(o, &v1.ListTargetsRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Msg.Targets) != 1 || r.Msg.Targets[0].Name != "local" {
		t.Fatalf("targets = %v", r.Msg.Targets)
	}
}

// This example sends a command's RPC through an injected client to a
// loopback test server. ClientOptions stays empty: a plain HTTP client
// verifies no infrastructure binding, so it must not claim one.
func ExampleNewWithClient() {
	p, h := skyv1connect.NewControlServiceHandler(&testService{})
	mux := http.NewServeMux()
	mux.Handle(p, h)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	rt := &recordingTransport{}
	c := NewWithClient(nil, &http.Client{Transport: rt}, ClientOptions{})
	var out, errout bytes.Buffer
	c.SetOut(&out)
	c.SetErr(&errout)
	c.SetArgs([]string{"targets", "list", "--endpoint", srv.URL, "--token", "test-token"})
	err := c.ExecuteContext(context.Background())
	fmt.Print(out.String())
	fmt.Println(err, rt.count())
	// Output:
	// NAME   KIND
	// local  local
	// <nil> 1
}

func (t *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	t.paths = append(t.paths, req.URL.Path)
	t.mu.Unlock()
	return http.DefaultTransport.RoundTrip(req)
}

func (t *recordingTransport) count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.paths)
}

func TestNewWithClient(t *testing.T) {
	p, h := skyv1connect.NewControlServiceHandler(&testService{})
	mux := http.NewServeMux()
	mux.Handle(p, h)
	var mu sync.Mutex
	served := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		served++
		mu.Unlock()
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	defaultTransport := http.DefaultClient.Transport

	rt := &recordingTransport{}
	for _, args := range [][]string{
		{"targets", "list"},
		{"workflow", "list"},
		{"artifacts", "status", strings.Repeat("a", 32)},
		{"jobs", "get", "j1"},
	} {
		c := NewWithClient(nil, &http.Client{Transport: rt}, ClientOptions{})
		var out, errout bytes.Buffer
		c.SetOut(&out)
		c.SetErr(&errout)
		c.SetArgs(append(args, "--endpoint", srv.URL, "--token", "test-token"))
		err := c.ExecuteContext(context.Background())
		if args[0] == "targets" && (err != nil || !strings.Contains(out.String(), "local")) {
			t.Fatalf("targets list = %q, %v", out.String(), err)
		}
		mu.Lock()
		n := served
		mu.Unlock()
		if rt.count() == 0 || rt.count() != n {
			t.Fatalf("%v: injected client carried %d of %d requests (%v)", args, rt.count(), n, err)
		}
	}
	if http.DefaultClient.Transport != defaultTransport {
		t.Fatal("NewWithClient changed http.DefaultClient")
	}

	// A nil client selects http.DefaultClient, as New does.
	c := NewWithClient(nil, nil, ClientOptions{})
	var out bytes.Buffer
	c.SetOut(&out)
	c.SetArgs([]string{"targets", "list", "--endpoint", srv.URL, "--token", "test-token"})
	if err := c.ExecuteContext(context.Background()); err != nil || !strings.Contains(out.String(), "local") {
		t.Fatalf("nil client targets list = %q, %v", out.String(), err)
	}
}
