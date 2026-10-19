package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func quoteBoundaryFixture(t *testing.T, size int, protoNames bool) *v1.QuoteTaskResponse {
	t.Helper()
	data, err := os.ReadFile("../testdata/quote-boundary-seed.json")
	if err != nil {
		t.Fatal(err)
	}
	out := &v1.QuoteTaskResponse{}
	if err := protojson.Unmarshal(data, out); err != nil {
		t.Fatal(err)
	}
	seed := out.Candidates[0]
	out.Candidates = nil
	for i := 0; i < 32; i++ {
		c := proto.Clone(seed).(*v1.QuoteCandidate)
		c.Target = strings.Repeat("x", 254) + fmt.Sprintf("%02d", i)
		out.Candidates = append(out.Candidates, c)
	}
	measure := func() int {
		b, err := (protojson.MarshalOptions{UseProtoNames: protoNames}).Marshal(out)
		if err != nil {
			t.Fatal(err)
		}
		var compact bytes.Buffer
		if err := json.Compact(&compact, b); err != nil {
			t.Fatal(err)
		}
		return compact.Len()
	}
	delta := size - measure()
	if delta < 0 || delta/5 > 32*254 {
		t.Fatalf("boundary outside valid fixture budget: %d", delta)
	}
	controls := delta / 5
	for i, c := range out.Candidates {
		n := min(controls, 254)
		c.Target = strings.Repeat("\x01", n) + strings.Repeat("x", 254-n) + fmt.Sprintf("%02d", i)
		controls -= n
	}
	out.Candidates[31].Quote.Source += strings.Repeat("x", delta%5)
	if got := measure(); got != size {
		t.Fatalf("fixture bytes=%d want%d", got, size)
	}
	return out
}

func TestQuoteCLIActualFullResponseBoundaries(t *testing.T) {
	for _, protoNames := range []bool{false, true} {
		for _, size := range []int{65535, 65536, 65537} {
			out := quoteBoundaryFixture(t, size, protoNames)
			profiles := make([][]byte, 0, 2)
			for _, names := range []bool{false, true} {
				raw, err := (protojson.MarshalOptions{UseProtoNames: names}).Marshal(out)
				if err != nil {
					t.Fatal(err)
				}
				var compact bytes.Buffer
				if err := json.Compact(&compact, raw); err != nil {
					t.Fatal(err)
				}
				profiles = append(profiles, append([]byte(nil), compact.Bytes()...))
			}
			allowed := proto.Size(out) <= 65536 && len(profiles[0]) <= 65536 && len(profiles[1]) <= 65536
			var emitted bytes.Buffer
			err := writeQuoteJSON(&emitted, out)
			if allowed {
				want := append(append([]byte(nil), profiles[1]...), '\n')
				if err != nil || !bytes.Equal(emitted.Bytes(), want) {
					t.Fatalf("profile=%t boundary=%d bytes=%d err=%v", protoNames, size, emitted.Len(), err)
				}
				var decoded v1.QuoteTaskResponse
				if err := protojson.Unmarshal(emitted.Bytes(), &decoded); err != nil || !proto.Equal(out, &decoded) {
					t.Fatalf("boundary emission changed message: %v", err)
				}
			} else if err == nil || emitted.Len() != 0 {
				t.Fatalf("global oversize partial output: bytes=%d err=%v", emitted.Len(), err)
			}
			t.Logf("actual full CLI: protobuf=%d compactCamel=%d compactSnake=%d allowed=%t", proto.Size(out), len(profiles[0]), len(profiles[1]), allowed)
		}
	}
}
