package rpc

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
)

func remoteProfileRequest() *http.Request {
	r := httptest.NewRequest(http.MethodPost, skyv1connect.RemoteDataServiceReadInputProcedure, nil)
	r.Header.Set("Content-Type", "application/proto")
	r.Header.Set("Connect-Protocol-Version", "1")
	return r
}

func TestRemoteRequestProfile(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*http.Request)
		want   bool
	}{
		{"protobuf", func(r *http.Request) {}, true},
		{"json", func(r *http.Request) { r.Header.Set("Content-Type", "application/json") }, true},
		{"response-gzip-advertisement", func(r *http.Request) { r.Header.Set("Accept-Encoding", "gzip") }, true},
		{"request-identity", func(r *http.Request) { r.Header.Set("Content-Encoding", "identity") }, true},
		{"known-length", func(r *http.Request) { r.ContentLength = 3; r.Header.Set("Content-Length", "3") }, true},
		{"zero-length", func(r *http.Request) { r.Header.Set("Content-Length", "0") }, true},
		{"lowercase-header-names", func(r *http.Request) {
			r.Header = http.Header{"content-type": {"application/proto"}, "connect-protocol-version": {"1"}}
		}, true},
		{"get", func(r *http.Request) { r.Method = http.MethodGet }, false},
		{"options", func(r *http.Request) { r.Method = http.MethodOptions }, false},
		{"put", func(r *http.Request) { r.Method = http.MethodPut }, false},
		{"method-case", func(r *http.Request) { r.Method = "post" }, false},
		{"ordinary-service", func(r *http.Request) { r.URL.Path = skyv1connect.ControlServiceGetJobProcedure }, false},
		{"unknown-method", func(r *http.Request) { r.URL.Path = "/skygo.v1.RemoteDataService/Unknown" }, false},
		{"trailing-slash", func(r *http.Request) { r.URL.Path += "/" }, false},
		{"path-prefix", func(r *http.Request) { r.URL.Path = "/prefix" + r.URL.Path }, false},
		{"path-case", func(r *http.Request) { r.URL.Path = "/skygo.v1.RemoteDataService/readInput" }, false},
		{"escaped-path", func(r *http.Request) { r.URL.RawPath = "/skygo.v1.RemoteDataService/%52eadInput" }, false},
		{"query", func(r *http.Request) { r.URL.RawQuery = "connect=v1" }, false},
		{"empty-query", func(r *http.Request) { r.URL.ForceQuery = true }, false},
		{"fragment", func(r *http.Request) { r.URL.Fragment = "suffix" }, false},
		{"opaque-url", func(r *http.Request) { r.URL.Opaque = r.URL.Path }, false},
		{"different-request-uri", func(r *http.Request) { r.RequestURI += "?" }, false},
		{"missing-media-type", func(r *http.Request) { r.Header.Del("Content-Type") }, false},
		{"media-type-case", func(r *http.Request) { r.Header.Set("Content-Type", "Application/Proto") }, false},
		{"json-charset", func(r *http.Request) { r.Header.Set("Content-Type", "application/json; charset=utf-8") }, false},
		{"media-type-list", func(r *http.Request) { r.Header.Set("Content-Type", "application/proto, application/json") }, false},
		{"duplicate-media-type", func(r *http.Request) { r.Header.Add("Content-Type", "application/proto") }, false},
		{"media-type-alias-key", func(r *http.Request) { r.Header["content-type"] = []string{"application/json"} }, false},
		{"nil-media-type-values", func(r *http.Request) { r.Header["Content-Type"] = nil }, false},
		{"grpc", func(r *http.Request) { r.Header.Set("Content-Type", "application/grpc+proto") }, false},
		{"grpc-web", func(r *http.Request) { r.Header.Set("Content-Type", "application/grpc-web+proto") }, false},
		{"grpc-web-text", func(r *http.Request) { r.Header.Set("Content-Type", "application/grpc-web-text") }, false},
		{"stream-protobuf", func(r *http.Request) { r.Header.Set("Content-Type", "application/connect+proto") }, false},
		{"stream-json", func(r *http.Request) { r.Header.Set("Content-Type", "application/connect+json") }, false},
		{"missing-version", func(r *http.Request) { r.Header.Del("Connect-Protocol-Version") }, false},
		{"wrong-version", func(r *http.Request) { r.Header.Set("Connect-Protocol-Version", "2") }, false},
		{"version-whitespace", func(r *http.Request) { r.Header.Set("Connect-Protocol-Version", " 1") }, false},
		{"version-list", func(r *http.Request) { r.Header.Set("Connect-Protocol-Version", "1,1") }, false},
		{"duplicate-version", func(r *http.Request) { r.Header.Add("Connect-Protocol-Version", "1") }, false},
		{"version-alias-key", func(r *http.Request) { r.Header["connect-protocol-version"] = []string{"1"} }, false},
		{"empty-version-values", func(r *http.Request) { r.Header["Connect-Protocol-Version"] = []string{} }, false},
		{"request-gzip", func(r *http.Request) { r.Header.Set("Content-Encoding", "gzip") }, false},
		{"request-deflate", func(r *http.Request) { r.Header.Set("Content-Encoding", "deflate") }, false},
		{"empty-encoding", func(r *http.Request) { r.Header.Set("Content-Encoding", "") }, false},
		{"duplicate-encoding", func(r *http.Request) { r.Header["Content-Encoding"] = []string{"identity", "identity"} }, false},
		{"encoding-alias-key", func(r *http.Request) {
			r.Header.Set("Content-Encoding", "identity")
			r.Header["content-encoding"] = []string{"gzip"}
		}, false},
		{"unknown-body-length", func(r *http.Request) { r.ContentLength = -1 }, false},
		{"chunked-body", func(r *http.Request) { r.TransferEncoding = []string{"chunked"} }, false},
		{"transfer-header", func(r *http.Request) { r.Header.Set("Transfer-Encoding", "chunked") }, false},
		{"wrong-length", func(r *http.Request) { r.Header.Set("Content-Length", "1") }, false},
		{"noncanonical-length", func(r *http.Request) { r.Header.Set("Content-Length", "00") }, false},
		{"duplicate-length", func(r *http.Request) { r.Header["Content-Length"] = []string{"0", "0"} }, false},
		{"length-alias-key", func(r *http.Request) {
			r.Header.Set("Content-Length", "0")
			r.Header["content-length"] = []string{"0"}
		}, false},
		{"trailer-map", func(r *http.Request) { r.Trailer = http.Header{"Checksum": nil} }, false},
		{"trailer-header", func(r *http.Request) { r.Header.Set("Trailer", "Checksum") }, false},
		{"te-header", func(r *http.Request) { r.Header.Set("Te", "trailers") }, false},
		{"stream-compression", func(r *http.Request) { r.Header.Set("Connect-Content-Encoding", "identity") }, false},
		{"stream-accept-compression", func(r *http.Request) { r.Header.Set("Connect-Accept-Encoding", "gzip") }, false},
		{"grpc-web-marker", func(r *http.Request) { r.Header.Set("X-Grpc-Web", "1") }, false},
		{"grpc-timeout", func(r *http.Request) { r.Header.Set("Grpc-Timeout", "1S") }, false},
		{"grpc-encoding", func(r *http.Request) { r.Header["gRpC-EnCoDiNg"] = []string{"identity"} }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := remoteProfileRequest()
			body := new(remoteProfileBody)
			r.Body = body
			test.change(r)
			if got := remoteRequestProfile(r); got != test.want {
				t.Fatalf("profile=%v, want %v", got, test.want)
			}
			if body.reads != 0 || body.closes != 0 {
				t.Fatalf("profile touched original body: reads=%d closes=%d", body.reads, body.closes)
			}
		})
	}
	if remoteRequestProfile(nil) || remoteRequestProfile(&http.Request{}) || remoteRequestProfile(&http.Request{Method: http.MethodPost, URL: new(url.URL)}) {
		t.Fatal("missing request/url/path accepted")
	}
}

type remoteProfileBody struct{ reads, closes int }

func (b *remoteProfileBody) Read([]byte) (int, error) { b.reads++; return 0, io.EOF }
func (b *remoteProfileBody) Close() error             { b.closes++; return nil }

// This fixture establishes actual generated-client request framing only. It
// has no Control, auth, artifact Files, transfer ownership or completion facts.
type remoteProfileFixture struct {
	skyv1connect.UnimplementedRemoteDataServiceHandler
}

func (*remoteProfileFixture) ReadInput(context.Context, *connect.Request[v1.ReadInputRequest]) (*connect.Response[v1.ReadInputResponse], error) {
	return connect.NewResponse(&v1.ReadInputResponse{Data: []byte("x")}), nil
}

func (*remoteProfileFixture) PollRelease(context.Context, *connect.Request[v1.PollReleaseRequest]) (*connect.Response[v1.PollReleaseResponse], error) {
	return connect.NewResponse(&v1.PollReleaseResponse{}), nil
}

func (*remoteProfileFixture) BeginOutput(context.Context, *connect.Request[v1.BeginOutputRequest]) (*connect.Response[v1.BeginOutputResponse], error) {
	return connect.NewResponse(&v1.BeginOutputResponse{}), nil
}

func (*remoteProfileFixture) WriteOutput(context.Context, *connect.Request[v1.WriteOutputRequest]) (*connect.Response[v1.WriteOutputResponse], error) {
	return connect.NewResponse(&v1.WriteOutputResponse{}), nil
}

func (*remoteProfileFixture) FinishOutput(context.Context, *connect.Request[v1.FinishOutputRequest]) (*connect.Response[v1.FinishOutputResponse], error) {
	return connect.NewResponse(&v1.FinishOutputResponse{}), nil
}

func TestRemoteRequestProfileGeneratedClient(t *testing.T) {
	for _, codec := range []struct {
		name        string
		options     []connect.ClientOption
		contentType string
	}{
		{"protobuf", nil, "application/proto"},
		{"json", []connect.ClientOption{connect.WithProtoJSON()}, "application/json"},
	} {
		t.Run(codec.name, func(t *testing.T) {
			_, service := skyv1connect.NewRemoteDataServiceHandler(&remoteProfileFixture{})
			requests := make(chan *http.Request, 5)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Capture copied metadata, never retain or fabricate a body owner.
				observed := r.Clone(context.Background())
				observed.Body = nil
				requests <- observed
				if !remoteRequestProfile(r) {
					http.Error(w, "request profile refused", http.StatusUnsupportedMediaType)
					return
				}
				service.ServeHTTP(w, r)
			}))
			defer server.Close()
			client := skyv1connect.NewRemoteDataServiceClient(server.Client(), server.URL, codec.options...)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for _, call := range []struct {
				path string
				run  func() error
			}{
				{skyv1connect.RemoteDataServiceReadInputProcedure, func() error {
					_, err := client.ReadInput(ctx, connect.NewRequest(&v1.ReadInputRequest{Limit: 1}))
					return err
				}},
				{skyv1connect.RemoteDataServicePollReleaseProcedure, func() error {
					_, err := client.PollRelease(ctx, connect.NewRequest(&v1.PollReleaseRequest{}))
					return err
				}},
				{skyv1connect.RemoteDataServiceBeginOutputProcedure, func() error {
					_, err := client.BeginOutput(ctx, connect.NewRequest(&v1.BeginOutputRequest{Size: 1}))
					return err
				}},
				{skyv1connect.RemoteDataServiceWriteOutputProcedure, func() error {
					_, err := client.WriteOutput(ctx, connect.NewRequest(&v1.WriteOutputRequest{Data: []byte("x")}))
					return err
				}},
				{skyv1connect.RemoteDataServiceFinishOutputProcedure, func() error {
					_, err := client.FinishOutput(ctx, connect.NewRequest(&v1.FinishOutputRequest{}))
					return err
				}},
			} {
				if err := call.run(); err != nil {
					t.Fatalf("generated fixture call %s: %v", call.path, err)
				}
				select {
				case r := <-requests:
					if r.URL.Path != call.path || r.Header.Get("Content-Type") != codec.contentType || r.Header.Get("Connect-Protocol-Version") != "1" || r.Header.Get("Accept-Encoding") != "gzip" || r.Header.Get("Content-Encoding") != "" {
						t.Fatalf("unexpected generated framing: method=%s path=%s headers=%v", r.Method, r.URL.Path, r.Header)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
		})
	}
}

func TestRemoteRequestProfileGeneratedRefusal(t *testing.T) {
	for _, test := range []struct {
		name    string
		options []connect.ClientOption
	}{
		{"grpc", []connect.ClientOption{connect.WithGRPC()}},
		{"grpc-web", []connect.ClientOption{connect.WithGRPCWeb()}},
		{"compressed-request", []connect.ClientOption{connect.WithSendGzip(), connect.WithCompressMinBytes(0)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			observed := make(chan bool, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				observed <- remoteRequestProfile(r)
				// No generated service or application handler is dispatched on
				// this request-only rejection fixture.
				http.Error(w, "request profile refused", http.StatusUnsupportedMediaType)
			}))
			defer server.Close()
			client := skyv1connect.NewRemoteDataServiceClient(server.Client(), server.URL, test.options...)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := client.WriteOutput(ctx, connect.NewRequest(&v1.WriteOutputRequest{Data: bytes.Repeat([]byte("x"), 4096)})); err == nil {
				t.Fatal("rejected fixture request unexpectedly succeeded")
			}
			select {
			case accepted := <-observed:
				if accepted {
					t.Fatal("unsupported generated framing accepted")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}
