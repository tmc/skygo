package facade

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"connectrpc.com/connect"
	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/integrations/skycompat/wire"
	"github.com/tmc/skygo/skycompat/taskyaml"
)

const (
	version          = "0.0.0+skycompat.api67"
	profile          = "sky-api67/task-v1; routes=health,validate"
	operationTimeout = 5 * time.Second
	canonicalTimeout = 2 * time.Second
)

// Options configures the canonical endpoint. Its zero value is invalid.
// Endpoint is an operator-selected HTTP or HTTPS base URL; requests cannot
// change it. It must not contain credentials, a query or a fragment.
type Options struct{ Endpoint string }

type handler struct {
	client   skyv1connect.ControlServiceClient
	admitted chan struct{}
}

// NewHandler returns a health and validation handler using an owned standard
// HTTP transport. It performs no network request until a supported HTTP request
// arrives. Each such request forwards its original Authorization header to a
// fresh canonical GetServerInfo call; no authority or identity is cached.
func NewHandler(opts Options) (http.Handler, error) {
	u, err := url.Parse(opts.Endpoint)
	if err != nil || u.Opaque != "" || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return nil, errors.New("facade: invalid canonical endpoint")
	}
	transport := &http.Transport{
		DialContext:       (&net.Dialer{Timeout: canonicalTimeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2: true,
		MaxConnsPerHost:   32, MaxIdleConns: 32, MaxIdleConnsPerHost: 32,
		IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: canonicalTimeout,
		ResponseHeaderTimeout: canonicalTimeout, ExpectContinueTimeout: time.Second,
	}
	client := &http.Client{Transport: transport, Timeout: canonicalTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &handler{client: skyv1connect.NewControlServiceClient(client, strings.TrimRight(u.String(), "/"), connect.WithReadMaxBytes(512<<10)), admitted: make(chan struct{}, 32)}, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	method := ""
	switch r.URL.Path {
	case "/api/health":
		method = http.MethodGet
	case "/validate":
		method = http.MethodPost
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(wire.APIVersionHeader, "67")
	w.Header().Set(wire.VersionHeader, version)
	w.Header().Set("X-Skygo-Compat-Profile", profile)
	controller := http.NewResponseController(w)
	deadline := time.Now().Add(operationTimeout)
	if earlier, ok := r.Context().Deadline(); ok && earlier.Before(deadline) {
		deadline = earlier
	}
	admitted := false
	originalBody := r.Body
	defer func() {
		_ = controller.Flush()
		// Complete body cleanup while the original read deadline still applies.
		if originalBody != nil {
			_ = originalBody.Close()
		}
		// An expired deadline must still bound net/http's remaining body drain.
		if time.Now().Before(deadline) {
			_ = controller.SetReadDeadline(time.Time{})
			_ = controller.SetWriteDeadline(time.Time{})
		}
		if admitted {
			<-h.admitted
		}
	}()
	if err := controller.SetReadDeadline(deadline); err != nil {
		failure(w, http.StatusServiceUnavailable, "transport unavailable")
		return
	}
	if err := controller.SetWriteDeadline(deadline); err != nil {
		failure(w, http.StatusServiceUnavailable, "transport unavailable")
		return
	}
	if method == "" {
		failure(w, http.StatusNotFound, "not found")
		return
	}
	if r.Method != method {
		w.Header().Set("Allow", method)
		failure(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	select {
	case h.admitted <- struct{}{}:
		admitted = true
	default:
		failure(w, http.StatusServiceUnavailable, "admission unavailable")
		return
	}
	if r.URL.RawQuery != "" || r.URL.ForceQuery {
		failure(w, http.StatusBadRequest, "query unsupported")
		return
	}
	versions := r.Header.Values(wire.APIVersionHeader)
	if (len(versions) != 1 || versions[0] != "67") && !(method == http.MethodGet && len(versions) == 0) {
		respond(w, http.StatusBadRequest, struct {
			Error   string `json:"error"`
			Message string `json:"message"`
		}{"version_mismatch", "unsupported skycompat API profile"})
		return
	}
	auth := r.Header.Values("Authorization")
	if len(auth) > 1 || (len(auth) == 1 && len(auth[0]) > 4096) {
		failure(w, http.StatusUnauthorized, "unauthenticated")
		return
	}
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	defer cancel()
	checkCtx, checkCancel := context.WithTimeout(ctx, canonicalTimeout)
	req := connect.NewRequest(&skyv1.GetServerInfoRequest{})
	if len(auth) == 1 {
		req.Header().Set("Authorization", auth[0])
	}
	_, err := h.client.GetServerInfo(checkCtx, req)
	checkCancel()
	if err != nil {
		switch connect.CodeOf(err) {
		case connect.CodeUnauthenticated:
			if method == http.MethodGet {
				health(w, "needs_auth")
			} else {
				failure(w, http.StatusUnauthorized, "unauthenticated")
			}
		case connect.CodePermissionDenied:
			failure(w, http.StatusForbidden, "permission denied")
		default:
			failure(w, http.StatusServiceUnavailable, "canonical unavailable")
		}
		return
	}
	if method == http.MethodGet {
		if r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
			failure(w, http.StatusBadRequest, "body unsupported")
			return
		}
		health(w, "healthy")
		return
	}
	media, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" || len(params) > 1 || (len(params) == 1 && !strings.EqualFold(params["charset"], "utf-8")) {
		failure(w, http.StatusUnsupportedMediaType, "content type unsupported")
		return
	}
	body, err := wire.DecodeValidate(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		validationError(w)
		return
	}
	doc, err := taskyaml.ParseDAG(strings.NewReader(body.DAG), taskyaml.Options{})
	if err != nil || !supported(doc) {
		validationError(w)
		return
	}
	if ctx.Err() != nil {
		failure(w, http.StatusServiceUnavailable, "request unavailable")
		return
	}
	respond(w, http.StatusOK, nil)
}

func supported(d *taskyaml.Document) bool {
	if d == nil {
		return false
	}
	t := d.Task
	if t.NumNodes != 1 || (t.Setup != nil && *t.Setup != "") || (t.Workdir != nil && *t.Workdir != "") || len(t.FileMounts) != 0 || len(t.Volumes) != 0 {
		return false
	}
	r := t.Resources
	if r == nil {
		return true
	}
	if r.CPUs != nil || r.Memory != nil || len(r.Accelerators) != 0 ||
		r.Cloud != nil || r.Region != nil || r.Zone != nil ||
		r.ImageID != nil || r.InstanceType != nil || (r.UseSpot != nil && *r.UseSpot) ||
		r.Infra != nil || r.Autostop != nil || r.JobRecovery != nil {
		return false
	}
	return r.DiskSize == nil || (r.DiskSize.Value == "256" && r.DiskSize.Unit == "" && !r.DiskSize.AtLeast)
}
func validationError(w http.ResponseWriter) {
	var body bytes.Buffer
	if err := wire.EncodeValidationError(&body, wire.Exception{Type: wire.ValueError, Message: "task is invalid or unsupported by the selected skycompat profile"}); err != nil {
		failure(w, http.StatusInternalServerError, "encoding unavailable")
		return
	}
	w.WriteHeader(http.StatusBadRequest)
	_, _ = w.Write(body.Bytes())
}
func failure(w http.ResponseWriter, status int, message string) {
	respond(w, status, struct {
		Detail string `json:"detail"`
	}{message})
}
func respond(w http.ResponseWriter, status int, value any) {
	body, err := json.Marshal(value)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

type healthResponse struct {
	Status         string    `json:"status"`
	APIVersion     string    `json:"api_version"`
	Version        string    `json:"version"`
	VersionOnDisk  string    `json:"version_on_disk"`
	Commit         string    `json:"commit"`
	User           *struct{} `json:"user"`
	BasicAuth      bool      `json:"basic_auth_enabled"`
	IngressAuth    bool      `json:"ingress_basic_auth_enabled"`
	ServiceAccount bool      `json:"service_account_token_enabled"`
	ExternalAuth   bool      `json:"external_proxy_auth_enabled"`
	Telemetry      bool      `json:"telemetry_enabled"`
	RestrictConfig bool      `json:"restrict_config_to_admins"`
}

func health(w http.ResponseWriter, status string) {
	respond(w, http.StatusOK, healthResponse{Status: status, APIVersion: "67", Version: version, VersionOnDisk: version, RestrictConfig: true})
}
