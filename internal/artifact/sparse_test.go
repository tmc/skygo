package artifact

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"runtime"
	"strconv"
	"testing"
)

func sparseNumber(field []byte, n int) { copy(field, fmt.Sprintf("%0*o\x00", len(field)-1, n)) }

// Layouts are pinned to Go1.24 archive/tar/format.go: headerGNU sparse386,
// extended flag482, realSize483; extension entries24*21, flag504. Zero-length
// data entries are valid and force the decoder to append every map entry.
func oldGNUSparse(t *testing.T, extensions int) []byte {
	t.Helper()
	base := testArchive(t, testEntry{name: "sparse-gnu"})
	header := append([]byte(nil), base[:blockSize]...)
	header[156] = tar.TypeGNUSparse
	copy(header[257:265], "ustar  \x00")
	clear(header[345:])
	for i := 0; i < 4; i++ {
		sparseNumber(header[386+i*24:386+i*24+12], 0)
		sparseNumber(header[398+i*24:398+i*24+12], 0)
	}
	if extensions > 0 {
		header[482] = 1
	}
	sparseNumber(header[483:495], 0)
	checksum(header)
	out := make([]byte, (extensions+3)*blockSize)
	copy(out, header)
	for i := 0; i < extensions; i++ {
		block := out[(i+1)*blockSize : (i+2)*blockSize]
		for j := 0; j < 21; j++ {
			sparseNumber(block[j*24:j*24+12], 0)
			sparseNumber(block[j*24+12:j*24+24], 0)
		}
		if i+1 < extensions {
			block[504] = 1
		}
	}
	return out
}

func paxRecord(key, value string) string {
	body := key + "=" + value + "\n"
	n := len(body) + 2
	for {
		record := strconv.Itoa(n) + " " + body
		if len(record) == n {
			return record
		}
		n = len(record)
	}
}

// This is real PAX GNU sparse1.0: a small metadata body followed by an ordinary
// file body containing the potentially large newline-delimited sparse map.
func paxSparse(t *testing.T, entries int) []byte {
	t.Helper()
	records := paxRecord("GNU.sparse.major", "1") + paxRecord("GNU.sparse.minor", "0") + paxRecord("GNU.sparse.realsize", "0") + paxRecord("GNU.sparse.name", "sparse-pax")
	metadata := testArchive(t, testEntry{name: "PaxHeaders/sparse", body: records})
	metadata[156] = tar.TypeXHeader
	checksum(metadata[:blockSize])
	metadata = metadata[:len(metadata)-2*blockSize]
	body := strconv.Itoa(entries) + "\n" + string(bytes.Repeat([]byte("0\n0\n"), entries))
	padded := (len(body) + blockSize - 1) / blockSize * blockSize
	data := testArchive(t, testEntry{name: "sparse-pax", body: body + string(make([]byte, padded-len(body)))})
	return append(metadata, data...)
}

func TestSparseFixturesUseRealDecoder(t *testing.T) {
	for _, data := range [][]byte{oldGNUSparse(t, 2), paxSparse(t, 16)} {
		read := &measuredReader{r: bytes.NewReader(data)}
		tr := tar.NewReader(read)
		header, err := tr.Next()
		if err != nil {
			t.Fatal(err)
		}
		if header.Size != 0 || header.Format != tar.FormatGNU && header.Format != tar.FormatPAX {
			t.Fatalf("not a valid sparse map: %+v", header)
		}
		if read.n <= blockSize {
			t.Fatal("fixture did not reach real extension/map decoder")
		}
		if _, err := tr.Next(); err != io.EOF {
			t.Fatalf("sparse fixture trailer: %v", err)
		}
	}
}

func TestArchiveSparsePredecodeBound(t *testing.T) {
	for _, test := range []struct {
		name string
		data []byte
	}{
		{"old-gnu-chain", oldGNUSparse(t, int(MaxSize/blockSize)-3)},
		{"pax-1.0-map", paxSparse(t, (int(MaxSize)-4096)/4)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if int64(len(test.data)) > MaxSize {
				t.Fatalf("fixture exceeds encoded policy: %d", len(test.data))
			}
			read := &measuredReader{r: bytes.NewReader(test.data)}
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			digest, err := Validate(read, int64(len(test.data)))
			runtime.ReadMemStats(&after)
			if err == nil || digest != "" || read.n != blockSize {
				t.Fatalf("forbidden sparse decoder entered: digest=%q err=%v read=%d", digest, err, read.n)
			}
			if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 256<<10 {
				t.Fatalf("predecoder allocation=%d", allocated)
			}
		})
	}
}

func TestArchiveHeaderGateOffsets(t *testing.T) {
	for _, size := range []int{0, 1, 511, 512, 513} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			prefix := testArchive(t, testEntry{name: "prefix", body: string(bytes.Repeat([]byte{'p'}, size))})
			prefix = prefix[:len(prefix)-2*blockSize]
			for _, forbidden := range [][]byte{oldGNUSparse(t, 2), paxSparse(t, 16)} {
				data := append(append([]byte(nil), prefix...), forbidden...)
				read := &measuredReader{r: bytes.NewReader(data)}
				if _, err := Validate(read, int64(len(data))); err == nil || read.n != int64(len(prefix)+blockSize) {
					t.Fatalf("later header gate err=%v read=%d want=%d", err, read.n, len(prefix)+blockSize)
				}
			}
		})
	}
	// A forbidden-looking block INSIDE an ordinary file is content, not a header.
	raw := oldGNUSparse(t, 2)[:blockSize]
	data := testArchive(t, testEntry{name: "ordinary", body: string(raw)}, testEntry{name: "next", body: "ok"})
	if _, err := Validate(bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("payload mistaken for metadata header: %v", err)
	}
}
