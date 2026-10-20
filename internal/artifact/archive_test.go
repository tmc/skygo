package artifact

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"
)

type testEntry struct {
	name, body string
	directory  bool
	mode       int64
}

func testArchive(t *testing.T, entries ...testEntry) []byte {
	t.Helper()
	var out bytes.Buffer
	tw := tar.NewWriter(&out)
	for _, entry := range entries {
		typ := byte(tar.TypeReg)
		if entry.directory {
			typ = tar.TypeDir
		}
		mode := entry.mode
		if mode == 0 {
			mode = 0600
		}
		h := &tar.Header{Name: entry.name, Typeflag: typ, Size: int64(len(entry.body)), Mode: mode, Format: tar.FormatUSTAR, ModTime: archiveTime}
		if entry.directory {
			h.Size = 0
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if !entry.directory {
			if _, err := io.WriteString(tw, entry.body); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}
func checksum(header []byte) {
	for i := 148; i < 156; i++ {
		header[i] = ' '
	}
	var sum int
	for _, b := range header[:blockSize] {
		sum += int(b)
	}
	copy(header[148:156], fmt.Sprintf("%06o\x00 ", sum))
}
func changeArchive(t *testing.T, change func([]byte)) []byte {
	t.Helper()
	data := testArchive(t, testEntry{name: "safe"})
	change(data[:blockSize])
	checksum(data[:blockSize])
	return data
}
func rawString(field []byte, s string) { clear(field); copy(field, s) }

type measuredReader struct {
	r     io.Reader
	n     int64
	calls int
}

func (r *measuredReader) Read(p []byte) (int, error) {
	r.calls++
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}

func TestArchiveDomain(t *testing.T) {
	base := testArchive(t, testEntry{name: "a/b", body: "hello"}, testEntry{name: "a/", directory: true})
	for _, padding := range []int{0, 19, 512, 4096} {
		t.Run(fmt.Sprint(padding), func(t *testing.T) {
			data := append(append([]byte(nil), base...), make([]byte, padding)...)
			read := &measuredReader{r: bytes.NewReader(data)}
			digest, err := Validate(read, int64(len(data)))
			want := sha256.Sum256(data)
			if err != nil || digest != hex.EncodeToString(want[:]) || read.n != int64(len(data)) {
				t.Fatalf("digest=%q err=%v bytes=%d", digest, err, read.n)
			}
		})
	}
	for _, test := range []struct {
		name string
		data []byte
		size int64
	}{
		{"no-terminator", base[:len(base)-1024], int64(len(base) - 1024)},
		{"one-zero-block", base[:len(base)-512], int64(len(base) - 512)},
		{"short-declaration", base, int64(len(base) - 1)},
		{"long-declaration", base, int64(len(base) + 1)},
		{"unacknowledged-zero", append(append([]byte(nil), base...), 0), int64(len(base))},
		{"concatenated", append(append([]byte(nil), base...), testArchive(t, testEntry{name: "hidden", body: "outside"})...), int64(len(base) + len(testArchive(t, testEntry{name: "hidden", body: "outside"})))},
		{"nonzero-trailer", append(append([]byte(nil), base...), 1), int64(len(base) + 1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			read := &measuredReader{r: bytes.NewReader(test.data)}
			digest, err := Validate(read, test.size)
			if err == nil || digest != "" || read.n > test.size+1 {
				t.Fatalf("invalid domain digest=%q err=%v bytes=%d", digest, err, read.n)
			}
		})
	}
	data := testArchive(t, testEntry{name: "file", body: "x"})
	data[blockSize+1] = 1
	if _, err := Validate(bytes.NewReader(data), int64(len(data))); err == nil {
		t.Fatal("nonzero file padding accepted")
	}
}

func TestArchiveEntries(t *testing.T) {
	for _, name := range []string{"../outside", "/outside", "C:/outside", "a\\outside", "a//b", "a/./b", "a/../b", "file/", strings.Repeat("d/", 16) + "file"} {
		t.Run(name, func(t *testing.T) {
			data := changeArchive(t, func(header []byte) { rawString(header[:100], name) })
			if _, err := Validate(bytes.NewReader(data), int64(len(data))); err == nil {
				t.Fatal("unsafe path accepted")
			}
		})
	}
	for _, test := range []struct {
		name   string
		change func([]byte)
	}{
		{"invalid-utf8", func(h []byte) { h[0] = 0xff }},
		{"embedded-nul", func(h []byte) { rawString(h[:100], "safe\x00../outside") }},
		{"long-name", func(h []byte) {
			rawString(h[345:500], strings.Repeat("p", 155))
			rawString(h[:100], strings.Repeat("f", 86))
		}},
		{"symlink", func(h []byte) { h[156] = tar.TypeSymlink; rawString(h[157:257], "../outside") }},
		{"hardlink", func(h []byte) { h[156] = tar.TypeLink; rawString(h[157:257], "../outside") }},
		{"device", func(h []byte) { h[156] = tar.TypeChar }},
		{"block-device", func(h []byte) { h[156] = tar.TypeBlock }},
		{"fifo", func(h []byte) { h[156] = tar.TypeFifo }},
		{"socket", func(h []byte) { h[156] = 's' }},
		{"sparse", func(h []byte) { h[156] = tar.TypeGNUSparse }},
		{"pax", func(h []byte) { h[156] = tar.TypeXHeader }},
		{"pax-global", func(h []byte) { h[156] = tar.TypeXGlobalHeader }},
		{"gnu-long", func(h []byte) { h[156] = tar.TypeGNULongName }},
		{"setuid", func(h []byte) { copy(h[100:108], "0004600\x00") }},
		{"setgid", func(h []byte) { copy(h[100:108], "0002600\x00") }},
		{"sticky", func(h []byte) { copy(h[100:108], "0001600\x00") }},
		{"directory-data", func(h []byte) { h[156] = tar.TypeDir; copy(h[124:136], "00000000001\x00") }},
		{"oversized", func(h []byte) { copy(h[124:136], fmt.Sprintf("%011o\x00", MaxSize+1)) }},
		{"bad-version", func(h []byte) { copy(h[263:265], "99") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := changeArchive(t, test.change)
			read := &measuredReader{r: bytes.NewReader(data)}
			digest, err := Validate(read, int64(len(data)))
			if err == nil || digest != "" || read.n > int64(len(data))+1 {
				t.Fatalf("invalid entry digest=%q error=%v count=%d", digest, err, read.n)
			}
		})
	}
	for _, entries := range [][]testEntry{
		{{name: "same"}, {name: "same"}}, {{name: "dir/", directory: true}, {name: "dir", directory: true}},
		{{name: "file"}, {name: "file/child"}}, {{name: "dir/child"}, {name: "dir"}},
	} {
		data := testArchive(t, entries...)
		if _, err := Validate(bytes.NewReader(data), int64(len(data))); err == nil {
			t.Fatalf("duplicate/prefix conflict accepted: %+v", entries)
		}
	}
	data := testArchive(t, testEntry{name: "safe"})
	data[0] ^= 1
	if _, err := Validate(bytes.NewReader(data), int64(len(data))); err == nil {
		t.Fatal("bad checksum accepted")
	}
}

func TestArchiveCounts(t *testing.T) {
	for _, directory := range []bool{false, true} {
		limit := maxFiles
		if directory {
			limit = maxDirectories
		}
		entries := make([]testEntry, limit+1)
		for i := range entries {
			entries[i] = testEntry{name: fmt.Sprintf("entry%04d", i), directory: directory}
		}
		data := testArchive(t, entries[:limit]...)
		if _, err := Validate(bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatalf("maximum inventory rejected: %v", err)
		}
		data = testArchive(t, entries...)
		if _, err := Validate(bytes.NewReader(data), int64(len(data))); err == nil {
			t.Fatal("overflow inventory accepted")
		}
	}
}

type zeroSource struct{ remaining int64 }

func (r *zeroSource) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := int64(len(p))
	if n > r.remaining {
		n = r.remaining
	}
	clear(p[:n])
	r.remaining -= n
	return int(n), nil
}

type noProgress struct{ calls int }

func (r *noProgress) Read([]byte) (int, error) { r.calls++; return 0, nil }

func TestArchiveReadBound(t *testing.T) {
	read := &measuredReader{r: &zeroSource{remaining: MaxSize + 2}}
	if _, err := Validate(read, MaxSize); err == nil || read.n != MaxSize+1 {
		t.Fatalf("encoded bound err=%v n=%d", err, read.n)
	}
	stalled := &noProgress{}
	if _, err := Validate(stalled, 1024); err == nil || stalled.calls > 100 {
		t.Fatalf("no-progress err=%v calls=%d", err, stalled.calls)
	}
	// A forbidden metadata header must stop before the standard decoder can
	// consume any of its large body.
	data := testArchive(t, testEntry{name: "metadata", body: strings.Repeat("x", int(MaxSize)-1536)})
	data[156] = tar.TypeXHeader
	checksum(data[:blockSize])
	read = &measuredReader{r: bytes.NewReader(data)}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := Validate(read, int64(len(data)))
	runtime.ReadMemStats(&after)
	if err == nil || read.n != blockSize {
		t.Fatalf("metadata bound err=%v n=%d", err, read.n)
	}
	if after.TotalAlloc-before.TotalAlloc > 256<<10 {
		t.Fatalf("malformed metadata allocated %d bytes", after.TotalAlloc-before.TotalAlloc)
	}
}

func TestArchiveCompressedInputs(t *testing.T) {
	var gz, zipData bytes.Buffer
	zw := gzip.NewWriter(&gz)
	if _, err := io.Copy(zw, &zeroSource{remaining: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	zipWriter := zip.NewWriter(&zipData)
	entry, err := zipWriter.Create("expanded")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(entry, &zeroSource{remaining: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	if err := zipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	for _, data := range [][]byte{gz.Bytes(), zipData.Bytes(), {0x28, 0xb5, 0x2f, 0xfd}} {
		encoded := append([]byte(nil), data...)
		if len(encoded) < 1024 {
			encoded = append(encoded, make([]byte, 1024-len(encoded))...)
		}
		read := &measuredReader{r: bytes.NewReader(encoded)}
		if _, err := Validate(read, int64(len(encoded))); err == nil {
			t.Fatal("compressed input accepted")
		}
		if read.n > 512 {
			t.Fatalf("compressed input read beyond first header: %d", read.n)
		}
	}
}

func TestArchiveSizeAndReadFailure(t *testing.T) {
	for _, size := range []int64{-1, 0, 1023, MaxSize + 1} {
		r := &measuredReader{r: &zeroSource{remaining: MaxSize + 2}}
		if _, err := Validate(r, size); err == nil || r.n != 0 {
			t.Fatalf("invalid size=%d err=%v read=%d", size, err, r.n)
		}
	}
	if _, err := Validate(nil, 1024); err == nil {
		t.Fatal("nil reader accepted")
	}
	data := testArchive(t, testEntry{name: "maximal", body: strings.Repeat("x", int(MaxSize)-1536)})
	if int64(len(data)) != MaxSize {
		t.Fatalf("maximal fixture=%d", len(data))
	}
	if _, err := Validate(bytes.NewReader(data), MaxSize); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected archive read failure")
	reader := io.MultiReader(bytes.NewReader(data[:513]), errorReader{injected})
	if _, err := Validate(reader, MaxSize); !errors.Is(err, injected) {
		t.Fatalf("read failure lost: %v", err)
	}
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }
