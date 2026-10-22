package rpc

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
)

// This generated-handler fixture qualifies only default codec/threshold behavior.
// It is not a Control substitute and cannot establish INPUT4 or a reader join.
type remoteCodecFixture struct {
	skyv1connect.UnimplementedRemoteDataServiceHandler
	message *v1.ReadInputResponse
}

func (f *remoteCodecFixture) ReadInput(context.Context, *connect.Request[v1.ReadInputRequest]) (*connect.Response[v1.ReadInputResponse], error) {
	return connect.NewResponse(f.message), nil
}

type remoteCodecClient struct {
	*http.Client
	encoding string
}

func (c *remoteCodecClient) Do(r *http.Request) (*http.Response, error) {
	response, err := c.Client.Do(r)
	if err == nil {
		c.encoding = response.Header.Get("Content-Encoding")
	}
	return response, err
}

func TestRemoteDefaultGeneratedCodecsUseIdentityAtMaximumRange(t *testing.T) {
	data := make([]byte, 64<<10)
	for i := range data {
		data[i] = byte(i)
	}
	message, err := wireRemoteInput(remoteTestReference(int64(len(data))), data, 0, len(data))
	if err != nil {
		t.Fatal(err)
	}
	_, service := skyv1connect.NewRemoteDataServiceHandler(&remoteCodecFixture{message: message},
		connect.WithCompressMinBytes(1<<20), connect.WithReadMaxBytes(96<<10), connect.WithSendMaxBytes(96<<10))
	server := httptest.NewServer(service)
	defer server.Close()
	for _, json := range []bool{false, true} {
		httpClient := &remoteCodecClient{Client: server.Client()}
		var options []connect.ClientOption
		if json {
			options = append(options, connect.WithProtoJSON())
		}
		client := skyv1connect.NewRemoteDataServiceClient(httpClient, server.URL, options...)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		response, err := client.ReadInput(ctx, connect.NewRequest(&v1.ReadInputRequest{Limit: 64 << 10}))
		cancel()
		if err != nil {
			t.Fatalf("json=%t: %v", json, err)
		}
		if httpClient.encoding != "" || !bytes.Equal(response.Msg.Data, data) || !response.Msg.Complete || response.Msg.NextOffset != int64(len(data)) {
			t.Fatalf("default gzip accept changed bounded identity response: json=%t encoding=%q", json, httpClient.encoding)
		}
	}
}
