package cli

import (
	"errors"
	"net/http"
	"net/url"

	"github.com/spf13/cobra"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/dashboard"
)

// dashboardCommand serves the local read-only dashboard. The resolved bearer
// credential stays in this process's HTTP transport; the dashboard package,
// its pages and the browser never see it.
func (o *options) dashboardCommand() *cobra.Command {
	var listen string
	c := &cobra.Command{Use: "dashboard", Short: "Serve a local read-only web view of this endpoint", Long: `Serve a local read-only web view of this endpoint.

The dashboard listens only on 127.0.0.1 or [::1]. It prints its URL and a
one-time login code to standard error; the code works once, for ten minutes.
Pages show what this credential may read, using the same server checks as the
CLI. Environment values, result downloads and job changes are not available.
Stopping the dashboard never cancels a job.`, Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		hc, err := dashboardHTTPClient(o.endpoint, o.token)
		if err != nil {
			return err
		}
		ln, err := dashboard.Listen(listen)
		if err != nil {
			return err
		}
		client := skyv1connect.NewControlServiceClient(hc, o.endpoint)
		return dashboard.Serve(cmd.Context(), ln, dashboard.Config{Client: client, Timeout: o.timeout}, cmd.ErrOrStderr())
	}}
	c.Flags().StringVar(&listen, "listen", "127.0.0.1:0", "Loopback address and port (127.0.0.1 or [::1]; port 0 picks one)")
	return c
}

// dashboardHTTPClient returns the client for the dashboard's RPCs. It never
// follows redirects, and it sends the credential only to the endpoint's own
// scheme and host, so no redirect or other URL can receive it.
func dashboardHTTPClient(endpoint, token string) (*http.Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errors.New("dashboard requires an http or https endpoint URL")
	}
	return &http.Client{
		Transport:     bearer{token: token, scheme: u.Scheme, host: u.Host, base: http.DefaultTransport},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}, nil
}

// bearer adds the credential to requests for the endpoint's scheme and host.
type bearer struct {
	token, scheme, host string
	base                http.RoundTripper
}

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	if b.token == "" || r.URL.Scheme != b.scheme || r.URL.Host != b.host {
		return b.base.RoundTrip(r)
	}
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return b.base.RoundTrip(r)
}
