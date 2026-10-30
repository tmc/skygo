//go:build darwin || linux

package ec2job

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
)

// deadlineProgressClient preserves the selected real HTTP reply, but joins its
// network owners before the upload retry wait. Live loopback I/O would otherwise
// prevent synctest's MODELED clock from advancing to the context deadline.
type deadlineProgressClient struct {
	server         *httptest.Server
	procedure      string
	ctx            context.Context
	requests       int
	closed         bool
	beforeDeadline error
}

func (c *deadlineProgressClient) Do(r *http.Request) (*http.Response, error) {
	c.requests++
	response, err := c.server.Client().Do(r)
	if err != nil || r.URL.Path != c.procedure {
		return response, err
	}
	body, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	c.beforeDeadline = c.ctx.Err()
	c.server.Client().CloseIdleConnections()
	c.server.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return nil, err
	}
	c.closed = true
	response.Body = io.NopCloser(bytes.NewReader(body))
	return response, nil
}
