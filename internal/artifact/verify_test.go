package artifact

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"
)

// verifySource is a borrowed reader whose Close must never be called. Faults
// can accompany bytes or arrive on the extra read that establishes exact EOF.
type verifySource struct {
	data                   []byte
	offset, fragment, fail int
	failure                error
	withData, closed       bool
	calls, largest         int
}

func (r *verifySource) Read(p []byte) (int, error) {
	r.calls++
	r.largest = max(r.largest, len(p))
	if r.failure != nil && r.offset == r.fail {
		return 0, r.failure
	}
	if r.offset == len(r.data) {
		return 0, io.EOF
	}
	if r.fragment > 0 && len(p) > r.fragment {
		p = p[:r.fragment]
	}
	if r.failure != nil && len(p) > r.fail-r.offset {
		p = p[:r.fail-r.offset]
	}
	n := copy(p, r.data[r.offset:])
	r.offset += n
	if r.failure != nil && r.withData && r.offset == r.fail {
		return n, r.failure
	}
	return n, nil
}

func (r *verifySource) Close() error {
	r.closed = true
	return errors.New("borrowed reader closed")
}

func verifyReference(data []byte) Ref {
	digest := sha256.Sum256(data)
	return Ref{ID: strings.Repeat("a", 32), SHA256: hex.EncodeToString(digest[:]), Size: int64(len(data))}
}

func TestVerifyManifest(t *testing.T) {
	for _, test := range []struct {
		name        string
		entries     []testEntry
		bytes       uint64
		files, dirs uint64
	}{
		{"file-and-directories", []testEntry{{name: "a/b/file", body: strings.Repeat("hello", 7000)}, {name: "a/", directory: true}}, 35000, 1, 2},
		{"empty-archive", nil, 0, 0, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := testArchive(t, test.entries...)
			ref := verifyReference(data)
			r := &verifySource{data: data, fragment: 7}
			got, err := Verify(ref, r)
			want := Manifest{Reference: ref, ExpandedBytes: test.bytes, RegularFiles: test.files, Directories: test.dirs}
			if err != nil || got != want || r.offset != len(data) || r.closed || r.largest > 32<<10 {
				t.Fatalf("Verify = %+v, %v; want %+v; reader=%+v", got, err, want, r)
			}
		})
	}
}

func TestVerifyInvalidReferenceDoesNotRead(t *testing.T) {
	valid := verifyReference(testArchive(t))
	for _, ref := range []Ref{{}, {ID: "bad", SHA256: valid.SHA256, Size: valid.Size}, {ID: valid.ID, SHA256: strings.Repeat("A", 64), Size: valid.Size}, {ID: valid.ID, SHA256: valid.SHA256, Size: 0}, {ID: valid.ID, SHA256: valid.SHA256, Size: MaxSize + 1}} {
		r := &verifySource{data: []byte("must not read")}
		if got, err := Verify(ref, r); err == nil || got != (Manifest{}) || r.calls != 0 || r.closed {
			t.Fatalf("Verify(%+v) = %+v, %v; reader=%+v", ref, got, err, r)
		}
	}
}

func TestVerifyRejects(t *testing.T) {
	data := testArchive(t, testEntry{name: "safe", body: "hello"})
	ref := verifyReference(data)
	wrongDigest, shortSize, longSize := ref, ref, ref
	wrongDigest.SHA256 = strings.Repeat("0", 64)
	shortSize.Size--
	longSize.Size++
	unsafe := testArchive(t, testEntry{name: "../outside", body: "secret"})
	trailer := append(bytes.Clone(data), 1)
	extra := append(bytes.Clone(data), testArchive(t, testEntry{name: "hidden"})...)
	for _, test := range []struct {
		name  string
		ref   Ref
		data  []byte
		cause error
	}{
		{"wrong-digest", wrongDigest, data, ErrFileConflict},
		{"short-size", shortSize, data, nil},
		{"long-size", longSize, data, nil},
		{"short-reader", ref, data[:len(data)-1], nil},
		{"long-reader", ref, append(bytes.Clone(data), 0, 0), nil},
		{"malicious-path", verifyReference(unsafe), unsafe, nil},
		{"nonzero-trailer", verifyReference(trailer), trailer, nil},
		{"extra-archive-exact-length", verifyReference(extra), extra, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &verifySource{data: test.data}
			got, err := Verify(test.ref, r)
			if err == nil || got != (Manifest{}) || test.cause != nil && !errors.Is(err, test.cause) || int64(r.offset) > test.ref.Size+1 || r.closed {
				t.Fatalf("Verify = %+v, %v; cause=%v; reader=%+v", got, err, test.cause, r)
			}
		})
	}
	if got, err := Verify(ref, nil); err == nil || got != (Manifest{}) {
		t.Fatalf("nil reader = %+v, %v", got, err)
	}
}

func TestVerifyPreservesReaderFailure(t *testing.T) {
	data := testArchive(t, testEntry{name: "safe", body: "payload"})
	failure := errors.New("source failed")
	for _, test := range []struct {
		name     string
		at       int
		withData bool
	}{
		{"before-data", 0, false},
		{"mid-payload", blockSize + 2, false},
		{"mid-payload-with-data", blockSize + 2, true},
		{"last-bytes-with-error", len(data), true},
		{"after-exact-data", len(data), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := &verifySource{data: data, fail: test.at, failure: failure, withData: test.withData}
			got, err := Verify(verifyReference(data), r)
			if !errors.Is(err, failure) || got != (Manifest{}) || r.closed || r.offset != test.at {
				t.Fatalf("Verify = %+v, %v; reader=%+v", got, err, r)
			}
		})
	}
}
