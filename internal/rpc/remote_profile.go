package rpc

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
)

// remoteRequestProfile recognizes the worker lane's uncompressed Connect unary
// request framing. It neither reads the body nor authenticates the caller, and
// says nothing about response publication or transfer completion.
func remoteRequestProfile(r *http.Request) bool {
	if r == nil || r.URL == nil || r.Method != http.MethodPost {
		return false
	}
	switch r.URL.Path {
	case skyv1connect.RemoteDataServiceReadInputProcedure,
		skyv1connect.RemoteDataServicePollReleaseProcedure,
		skyv1connect.RemoteDataServiceBeginOutputProcedure,
		skyv1connect.RemoteDataServiceWriteOutputProcedure,
		skyv1connect.RemoteDataServiceFinishOutputProcedure:
	default:
		return false
	}
	if r.URL.RawQuery != "" || r.URL.ForceQuery || r.URL.Fragment != "" || r.URL.Opaque != "" || r.URL.EscapedPath() != r.URL.Path {
		return false
	}
	if r.RequestURI != "" && r.RequestURI != r.URL.Path {
		return false
	}
	contentType, ok := remoteProfileHeader(r.Header, "Content-Type")
	if !ok || contentType != "application/proto" && contentType != "application/json" {
		return false
	}
	version, ok := remoteProfileHeader(r.Header, "Connect-Protocol-Version")
	if !ok || version != "1" {
		return false
	}
	encoding, ok := remoteProfileHeader(r.Header, "Content-Encoding")
	if !ok || encoding != "" && encoding != "identity" {
		return false
	}
	if r.ContentLength < 0 || len(r.TransferEncoding) != 0 || len(r.Trailer) != 0 {
		return false
	}
	length, ok := remoteProfileHeader(r.Header, "Content-Length")
	if !ok || length != "" && length != strconv.FormatInt(r.ContentLength, 10) {
		return false
	}
	for key := range r.Header {
		switch strings.ToLower(key) {
		case "transfer-encoding", "trailer", "te", "connect-content-encoding", "connect-accept-encoding", "x-grpc-web":
			return false
		}
		if strings.HasPrefix(strings.ToLower(key), "grpc-") {
			return false
		}
	}
	return true
}

// remoteProfileHeader accepts one nonempty value or an absent field. Unlike
// Header.Get it rejects duplicate values and differently cased duplicate keys.
func remoteProfileHeader(header http.Header, name string) (string, bool) {
	var value string
	for key, values := range header {
		if !strings.EqualFold(key, name) {
			continue
		}
		if value != "" || len(values) != 1 || values[0] == "" {
			return "", false
		}
		value = values[0]
	}
	return value, true
}
