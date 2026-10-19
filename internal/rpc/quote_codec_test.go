package rpc

import (
	"bytes"
	"strings"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// These qualify real serializer/budget helpers using existing generated messages.
// New QuoteTask generated-message/HTTP tests await the parent-owned wire handoff.
func TestQuoteEncodingLimit(t *testing.T) {
	for _, size := range []int{maxQuoteResponseBytes - 1, maxQuoteResponseBytes, maxQuoteResponseBytes + 1} {
		m := &v1.Target{Name: strings.Repeat("a", size-len(`{"name":""}`))}
		b, err := marshalQuoteJSON(m, false)
		if err != nil || len(b) != size {
			t.Fatalf("actual compact JSON=%d,%v; want%d", len(b), err, size)
		}
		if proto.Size(m) > maxQuoteResponseBytes {
			t.Fatal("fixture does not isolate JSON limit")
		}
		err = checkQuoteResponseSize(m)
		if size <= maxQuoteResponseBytes && err != nil || size > maxQuoteResponseBytes && connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Fatalf("size%d: %v", size, err)
		}
	}
}

func TestQuoteEncodingProfiles(t *testing.T) {
	m := &v1.StreamJobLogsResponse{NextOffset: 1, AttemptId: "attempt", Data: []byte("data"), Complete: true}
	camel, err := marshalQuoteJSON(m, false)
	if err != nil {
		t.Fatal(err)
	}
	snake, err := marshalQuoteJSON(m, true)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(camel, []byte(`"nextOffset":`)) || !bytes.Contains(snake, []byte(`"next_offset":`)) || bytes.Equal(camel, snake) {
		t.Fatalf("profiles camel=%s snake=%s", camel, snake)
	}
	for _, name := range []string{"json", "json; charset=utf-8"} {
		codec := quoteJSONCodec{name: name}
		actual, err := codec.Marshal(m)
		if err != nil || !bytes.Equal(actual, camel) || codec.Name() != name {
			t.Fatalf("codec%s=%s,%v", name, actual, err)
		}
		for i := 0; i < 10; i++ {
			repeat, err := codec.Marshal(m)
			if err != nil || !bytes.Equal(repeat, actual) {
				t.Fatal("emitted JSON differs from measured compact bytes")
			}
		}
		var got v1.StreamJobLogsResponse
		if err := codec.Unmarshal(append(actual[:len(actual)-1:len(actual)-1], []byte(`,"futureField":true}`)...), &got); err != nil || !proto.Equal(&got, m) {
			t.Fatalf("request unknown semantics: %v %v", &got, err)
		}
		if err := codec.Unmarshal(nil, &got); err == nil {
			t.Fatal("empty JSON accepted")
		}
	}
}

func TestQuoteEscapedResponseLimit(t *testing.T) {
	m := &v1.Target{Name: strings.Repeat("\x01", 12000)}
	if len(m.Name) >= maxQuoteResponseBytes || proto.Size(m) >= maxQuoteResponseBytes {
		t.Fatal("fixture does not isolate escaping")
	}
	b, err := marshalQuoteJSON(m, false)
	if err != nil || len(b) <= maxQuoteResponseBytes {
		t.Fatalf("escape expansion not reached: %d,%v", len(b), err)
	}
	if err := checkQuoteResponseSize(m); connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "quote response exceeds 64 KiB") {
		t.Fatalf("oversize=%v", err)
	}
	invalid := &v1.Job{CreatedAt: &timestamppb.Timestamp{Seconds: 253402300800}}
	if err := checkQuoteResponseSize(invalid); connect.CodeOf(err) != connect.CodeInternal {
		t.Fatalf("invalid timestamp=%v", err)
	}
	if err := checkQuoteResponseSize(&v1.Target{Name: string([]byte{0xff})}); connect.CodeOf(err) != connect.CodeInternal {
		t.Fatalf("invalid UTF8=%v", err)
	}
}
