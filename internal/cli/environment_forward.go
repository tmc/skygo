package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/tmc/skygo/internal/environment"
)

// forwardDrain bounds how long a stopping forward waits for exchanges in
// progress before it closes their connections.
const forwardDrain = 5 * time.Second

func (o *options) environmentForwardCommand() *cobra.Command {
	listen := "127.0.0.1:0"
	c := &cobra.Command{Use: "forward ID", Short: "Relay a loopback HTTP listener to an environment's declared port", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		id := args[0]
		if !environment.ValidID(id) {
			return errors.New("environment ID requires 32 lowercase hexadecimal digits")
		}
		if err := checkLoopback(listen); err != nil {
			return err
		}
		target, err := forwardURL(o.endpoint, id)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), o.timeout)
		err = o.requireEnvironments(ctx, environment.Spec{})
		cancel()
		if err != nil {
			return err
		}
		ln, err := net.Listen("tcp", listen)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(cmd.ErrOrStderr(), "forwarding http://%s to environment %s\n", ln.Addr(), id); err != nil {
			ln.Close()
			return err
		}
		// The relay uses the selected transport, as every other RPC does;
		// the default client's transport is nil, http.DefaultTransport.
		return relayForward(cmd.Context(), ln, target, o.token, cmd.ErrOrStderr(), o.httpClient().Transport, forwardDrain)
	}}
	c.Flags().StringVar(&listen, "listen", listen, "Loopback IP address and port to listen on")
	return c
}

// checkLoopback requires addr to be a loopback IP address and port. A host
// name is refused, so no lookup can widen the listener.
func checkLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("listen address: %w", err)
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		return errors.New("listen address must be a loopback IP address")
	}
	return nil
}

// forwardURL returns the forward route of environment id at endpoint:
// /environments/<id>/forward below the endpoint's own path.
func forwardURL(endpoint, id string) (*url.URL, error) {
	return routeURL(endpoint, "environments", id)
}

// routeURL returns the forward route /<kind>/<id>/forward below endpoint's
// own path.
func routeURL(endpoint, kind, id string) (*url.URL, error) {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("endpoint must be an http or https URL without user, query or fragment")
	}
	u.Path = strings.TrimSuffix(u.Path, "/") + "/" + kind + "/" + id + "/forward"
	u.RawPath = ""
	return u, nil
}

// serveForward relays each request accepted on ln to target until ctx is
// done. It then stops accepting, waits up to forwardDrain for exchanges in
// progress, closes the connections of any that remain, and returns only
// after every admitted exchange's handler has returned. If serving fails
// first, it closes the connections, waits the same way, and returns that
// error. Each exchange is relayed with its local request's context, so a
// local client that leaves cancels its relayed request. CONNECT and
// protocol upgrades are refused, and redirects are returned to the local
// client, never followed.
func serveForward(ctx context.Context, ln net.Listener, target *url.URL, token string, errlog io.Writer) error {
	return relayForward(ctx, ln, target, token, errlog, nil, forwardDrain)
}

// relayForward is serveForward with the upstream transport (nil for
// http.DefaultTransport) and the drain bound as parameters, so tests can
// hold an exchange past the closing of its connection.
func relayForward(ctx context.Context, ln net.Listener, target *url.URL, token string, errlog io.Writer, transport http.RoundTripper, drain time.Duration) error {
	proxy := &httputil.ReverseProxy{
		Rewrite:   func(r *httputil.ProxyRequest) { rewriteForward(r, target, token) },
		Transport: transport,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			fmt.Fprintf(errlog, "forward %s: %v\n", r.URL.Path, err)
			w.WriteHeader(http.StatusBadGateway)
		},
	}
	// Each exchange is admitted under mu and counted in active until its
	// handler returns. Neither http.Server.Close nor Serve's return waits
	// for handlers, so join stops admission and then waits: every Add
	// happens before stopping is set, so Wait cannot race one.
	var (
		mu       sync.Mutex
		stopping bool
		active   sync.WaitGroup
	)
	join := func() {
		mu.Lock()
		stopping = true
		mu.Unlock()
		active.Wait()
	}
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			if stopping {
				mu.Unlock()
				http.Error(w, "forward is stopping", http.StatusServiceUnavailable)
				return
			}
			active.Add(1)
			mu.Unlock()
			defer active.Done()
			switch {
			case r.Method == http.MethodConnect || r.Header.Get("Upgrade") != "":
				http.Error(w, "CONNECT and upgrades are not forwarded", http.StatusNotImplemented)
			case !strings.HasPrefix(r.URL.Path, "/"):
				http.Error(w, "request path must be absolute", http.StatusBadRequest)
			default:
				// Wrap before ReverseProxy clones r and captures Body.Close. A
				// transport's guard must cover both its own and the proxy's Close.
				if wrap, ok := transport.(interface {
					WrapRequestBody(io.ReadCloser) io.ReadCloser
				}); ok && r.Body != nil && r.Body != http.NoBody {
					r.Body = wrap.WrapRequestBody(r.Body)
				}
				proxy.ServeHTTP(w, r)
			}
		}),
		ReadHeaderTimeout: 10 * time.Second,
		ErrorLog:          log.New(errlog, "forward: ", 0),
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()
	select {
	case err := <-done:
		// Serving failed on its own; its connections may still carry
		// exchanges.
		srv.Close()
		join()
		return err
	case <-ctx.Done():
	}
	shut, cancel := context.WithTimeout(context.Background(), drain)
	defer cancel()
	if srv.Shutdown(shut) != nil {
		srv.Close()
	}
	err := <-done
	join()
	if !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// rewriteForward sends r to target, keeping the application path, its
// escaping and the query. The local client's credentials are replaced by
// the CLI's own bearer; the server removes that before the application.
func rewriteForward(r *httputil.ProxyRequest, target *url.URL, token string) {
	r.Out.URL.Scheme, r.Out.URL.Host = target.Scheme, target.Host
	r.Out.URL.Path = target.Path + r.In.URL.Path
	r.Out.URL.RawPath = target.EscapedPath() + r.In.URL.EscapedPath()
	r.Out.Host = ""
	r.Out.Header.Del("Authorization")
	r.Out.Header.Del("Proxy-Authorization")
	if token != "" {
		r.Out.Header.Set("Authorization", "Bearer "+token)
	}
}
