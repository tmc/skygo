package cli

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
)

// AUTHORED_NOT_RUN. TestServeForwardCommand runs the serve forward command
// end to end against a fixture endpoint that advertises services and
// serves a fixture forward route; the route is MODELED, not the server's.
func TestServeForwardCommand(t *testing.T) {
	id := strings.Repeat("a", 32)
	var mu sync.Mutex
	var seen []forwardSeen
	mux := http.NewServeMux()
	mux.Handle(skyv1connect.NewControlServiceHandler(&serveControlInfo{endpoint: []v1.Capability{v1.Capability_CAPABILITY_SERVICES_V1}}))
	mux.Handle(skyv1connect.NewServingServiceHandler(&servingFixture{}))
	mux.HandleFunc("/services/"+id+"/forward/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, forwardSeen{r.URL.Path, r.URL.RawPath, r.URL.RawQuery, r.Header.Get("Authorization"), r.Header.Get("Proxy-Authorization")})
		mu.Unlock()
		io.Copy(io.Discard, r.Body)
		fmt.Fprint(w, "served")
	})
	upstream := serveMux(t, mux)

	// The command prints its address on stderr, which execute buffers, so
	// the test chooses a free loopback port first.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	probe.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := execute(ctx, "--endpoint", upstream, "--token", "fwd-token", "serve", "forward", id, "--listen", addr)
		done <- err
	}()
	var resp *http.Response
	deadline := time.Now().Add(10 * time.Second)
	for {
		req, _ := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/predict?x=1", strings.NewReader("input"))
		req.Header.Set("Authorization", "Bearer local")
		req.Header.Set("Proxy-Authorization", "Basic eDp5")
		if resp, err = http.DefaultClient.Do(req); err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "served" {
		t.Fatalf("response %d %q", resp.StatusCode, body)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("forward = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("forward did not stop")
	}
	mu.Lock()
	defer mu.Unlock()
	want := forwardSeen{path: "/services/" + id + "/forward/v1/predict", query: "x=1", auth: "Bearer fwd-token"}
	if len(seen) != 1 || seen[0] != want {
		t.Fatalf("relayed %+v, want %+v", seen, want)
	}
}

func serveMux(t *testing.T, h http.Handler) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: h}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return "http://" + ln.Addr().String()
}
