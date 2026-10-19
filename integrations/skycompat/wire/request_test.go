package wire

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func testRequest() Request {
	end := 1700000001.0
	return Request{ID: "11111111-2222-4333-8444-555555555555", Name: "sky.jobs.launch", UserID: "conformance-user", Status: Succeeded, CreatedAt: 1700000000, Schedule: Long, FinishedAt: &end, Result: json.RawMessage(`{"job_id":1,"handle":null}`)}
}
func TestEncodeRequest(t *testing.T) {
	var b bytes.Buffer
	if err := EncodeRequest(&b, testRequest()); err != nil {
		t.Fatal(err)
	}
	var got, want map[string]any
	if err := json.Unmarshal(b.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join("..", "testdata", "wire", "request-success.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &want); err != nil {
		t.Fatal(err)
	}
	want["user_name"] = nil
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %s, want %s", b.Bytes(), data)
	}
	for _, state := range []RequestStatus{Pending, Waiting, Running, Succeeded, Cancelled} {
		r := testRequest()
		r.Status = state
		r.Schedule = Short
		r.Result = nil
		b.Reset()
		if err := EncodeRequest(&b, r); err != nil {
			t.Errorf("state %s: %v", state, err)
		}
	}
}
func TestEncodeRequestInvalid(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*Request)
	}{
		{"id", func(r *Request) { r.ID = "" }}, {"name", func(r *Request) { r.Name = strings.Repeat("x", 257) }},
		{"user", func(r *Request) { r.UserID = "\xff" }}, {"status", func(r *Request) { r.Status = "secret-canary" }},
		{"failed", func(r *Request) { r.Status = Failed }}, {"schedule", func(r *Request) { r.Schedule = "secret-canary" }},
		{"negative timestamp", func(r *Request) { r.CreatedAt = -1 }}, {"nan timestamp", func(r *Request) { r.CreatedAt = math.NaN() }},
		{"infinite finish", func(r *Request) { n := math.Inf(1); r.FinishedAt = &n }},
		{"text", func(r *Request) { s := strings.Repeat("x", 4097); r.StatusMessage = &s }},
		{"text utf8", func(r *Request) { s := "\xff"; r.ClusterName = &s }},
		{"duplicate result", func(r *Request) { r.Result = json.RawMessage(`{"secret-canary":1,"secret-canary":2}`) }},
		{"trailing result", func(r *Request) { r.Result = json.RawMessage(`1 2`) }},
		{"large result", func(r *Request) { r.Result = json.RawMessage(strings.Repeat(" ", maxResult+1)) }},
		{"large scalar", func(r *Request) { r.Result = json.RawMessage(`"` + strings.Repeat("x", (64<<10)+1) + `"`) }},
		{"number overflow", func(r *Request) { r.Result = json.RawMessage(`1e999`) }},
		{"nan", func(r *Request) { r.Result = json.RawMessage(`NaN`) }},
		{"deep result", func(r *Request) { r.Result = json.RawMessage(strings.Repeat("[", 34) + "0" + strings.Repeat("]", 34)) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := testRequest()
			tt.mutate(&r)
			var b bytes.Buffer
			err := EncodeRequest(&b, r)
			if err == nil || b.Len() != 0 || strings.Contains(err.Error(), "secret-canary") {
				t.Fatalf("bytes=%d,error=%v", b.Len(), err)
			}
		})
	}
	if err := EncodeRequest(nil, testRequest()); err == nil {
		t.Fatal("nil writer accepted")
	}
	if err := EncodeRequest(shortWriter{}, testRequest()); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write: %v", err)
	}
	if err := EncodeRequest(failWriter{}, testRequest()); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("write failure: %v", err)
	}
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestEncodeErrors(t *testing.T) {
	e := Exception{Type: ValueError, Message: "unsupported resources.cloud"}
	var b bytes.Buffer
	if err := EncodeValidationError(&b, e); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join("..", "testdata", "wire", "validation-error.json"))
	if err != nil {
		t.Fatal(err)
	}
	var got, want any
	if err := json.Unmarshal(b.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("validation: %s", b.Bytes())
	}
	for _, kind := range []ExceptionType{ValueError, RuntimeError} {
		r := testRequest()
		r.Status = Failed
		r.Result = nil
		b.Reset()
		e.Type = kind
		if err := EncodeRequestError(&b, r, e); err != nil {
			t.Fatal(err)
		}
		var envelope struct {
			Detail requestPayload `json:"detail"`
		}
		if err := json.Unmarshal(b.Bytes(), &envelope); err != nil {
			t.Fatal(err)
		}
		if envelope.Detail.Status != Failed || envelope.Detail.Result != "null" || envelope.Detail.Entrypoint != "gAROLg==" || envelope.Detail.Body != "gAROLg==" {
			t.Fatalf("detail=%+v", envelope.Detail)
		}
		var encoded struct{ Object, Type, Message string }
		if err := json.Unmarshal([]byte(envelope.Detail.Error), &encoded); err != nil {
			t.Fatal(err)
		}
		primitive, err := base64.StdEncoding.DecodeString(encoded.Object)
		if err != nil || len(primitive) == 0 || encoded.Type != string(kind) || encoded.Message != e.Message {
			t.Fatalf("error=%+v, %v", encoded, err)
		}
	}
	for _, e := range []Exception{{Type: "eval", Message: "secret-canary"}, {Type: ValueError, Message: strings.Repeat("x", 4097)}, {Type: ValueError, Message: "\xff"}} {
		b.Reset()
		if err := EncodeValidationError(&b, e); err == nil || b.Len() != 0 {
			t.Fatalf("validation err=%v", err)
		}
		r := testRequest()
		r.Status = Failed
		r.Result = nil
		if err := EncodeRequestError(&b, r, e); err == nil || b.Len() != 0 {
			t.Fatalf("async err=%v", err)
		}
	}
	for _, r := range []Request{testRequest(), func() Request { r := testRequest(); r.Status = Failed; return r }()} {
		b.Reset()
		if err := EncodeRequestError(&b, r, Exception{Type: ValueError}); err == nil || b.Len() != 0 {
			t.Fatalf("invalid async request: %v", err)
		}
	}
}

func TestResultTokenBoundary(t *testing.T) {
	r := testRequest()
	r.Result = json.RawMessage("[" + strings.Repeat("0,", maxTokens-3) + "0]")
	var b bytes.Buffer
	if err := EncodeRequest(&b, r); err != nil {
		t.Fatalf("exact token limit: %v", err)
	}
	r.Result = json.RawMessage("[" + strings.Repeat("0,", maxTokens-2) + "0]")
	b.Reset()
	if err := EncodeRequest(&b, r); err == nil || b.Len() != 0 {
		t.Fatalf("excess token accepted: %v", err)
	}
}
