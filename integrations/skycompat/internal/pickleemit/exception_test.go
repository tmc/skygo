package pickleemit

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"
)

func TestPrimitiveExceptionBytes(t *testing.T) {
	got, err := Exception("ValueError", "bad")
	if err != nil {
		t.Fatal(err)
	}
	// Protocol4: dictionary of strings, one tuple, empty attributes and None.
	want, err := hex.DecodeString("80047d288c04747970658c0a56616c75654572726f728c076d6573736167658c036261648c04617267738c03626164858c0a617474726962757465737d8c0a737461636b74726163654e752e")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %x, want %x", got, want)
	}
}
func TestPrimitiveExceptionLimits(t *testing.T) {
	for _, n := range []int{0, 255, 256, 4096} {
		data, err := Exception("RuntimeError", strings.Repeat("x", n))
		if err != nil || len(data) > 2*n+128 {
			t.Fatalf("length %d: bytes=%d,error=%v", n, len(data), err)
		}
	}
	for _, tt := range []struct{ kind, message string }{{"eval", "x"}, {"ValueError", "\xff"}, {"ValueError", strings.Repeat("x", 4097)}} {
		if data, err := Exception(tt.kind, tt.message); err == nil || data != nil {
			t.Fatalf("invalid exception bytes=%x,error=%v", data, err)
		}
	}
}
