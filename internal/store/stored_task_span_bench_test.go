package store

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
)

// spanBenchRecord returns a valid record of about n bytes whose bulk is an
// unrelated status value, followed by a task with a null workers member.
// It is representative only; it does not replace the archive fixture.
func spanBenchRecord(n int) []byte {
	var b strings.Builder
	b.WriteString(`{"id":"modeled","status":{"log":"`)
	for b.Len() < n {
		b.WriteString(`line \"quoted\" {brace} [bracket] \\ path\n`)
	}
	b.WriteString(`","codes":[0`)
	for i := 1; i < 1000; i++ {
		b.WriteString("," + strconv.Itoa(i))
	}
	b.WriteString(`]},"task":{"argv":["work"],"workers":null}}`)
	return []byte(b.String())
}

func BenchmarkStoredTask(b *testing.B) {
	data := spanBenchRecord(8 << 20)
	if !json.Valid(data) {
		b.Fatal("invalid benchmark record")
	}
	for _, bm := range []struct {
		name string
		f    func([]byte, error) error
	}{
		{"span", storedTask},
		{"reference", spanRefStoredTask},
	} {
		b.Run(bm.name, func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := bm.f(data, errStoredJobWorkers); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
