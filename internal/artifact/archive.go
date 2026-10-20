package artifact

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"strings"
	"unicode/utf8"
)

// MaxSize is the maximum encoded archive size and regular-file byte total.
const MaxSize int64 = 32 << 20

const (
	blockSize      = 512
	maxFiles       = 2048
	maxDirectories = 512
	maxEntries     = maxFiles + maxDirectories
	maxDepth       = 16
	maxName        = 240
)

// Validate reads at most size+1 bytes and returns the SHA-256 of an accepted
// complete archive. It permits only bounded regular files and directories, two
// terminating zero blocks and zero trailing padding. It never decompresses data.
// A failed validation returns no digest. The caller owns a possibly blocking r.
func Validate(r io.Reader, size int64) (string, error) { return readArchive(r, size, nil, nil) }

type archiveEntry struct{ directory, explicit bool }
type archiveNames struct {
	entries            map[string]archiveEntry
	files, directories int
	bytes              int64
}

func canonicalName(name string, directory bool) (string, error) {
	if name == "" || strings.HasPrefix(name, "/") || strings.ContainsAny(name, "\\:\x00") || !utf8.ValidString(name) {
		return "", errors.New("invalid artifact path")
	}
	if directory && strings.HasSuffix(name, "/") {
		name = strings.TrimSuffix(name, "/")
	}
	if len(name) == 0 || len(name) > maxName {
		return "", errors.New("artifact path exceeds limit")
	}
	parts := strings.Split(name, "/")
	if len(parts) > maxDepth {
		return "", errors.New("artifact path exceeds depth limit")
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." || len(part) > 255 {
			return "", errors.New("invalid artifact path component")
		}
	}
	return name, nil
}

func (s *archiveNames) add(name string, directory bool, size int64) error {
	parts := strings.Split(name, "/")
	for i := 1; i < len(parts); i++ {
		parent := strings.Join(parts[:i], "/")
		entry, ok := s.entries[parent]
		if ok && !entry.directory {
			return errors.New("artifact file conflicts with parent directory")
		}
		if !ok {
			s.entries[parent] = archiveEntry{directory: true}
			s.directories++
		}
	}
	if old, ok := s.entries[name]; ok {
		if !directory || !old.directory || old.explicit {
			return errors.New("duplicate artifact destination")
		}
	} else if directory {
		s.directories++
	} else {
		s.files++
	}
	if s.files > maxFiles || s.directories > maxDirectories {
		return errors.New("artifact entry limit exceeded")
	}
	if size < 0 || size > MaxSize-s.bytes {
		return errors.New("artifact expanded byte limit exceeded")
	}
	s.bytes += size
	s.entries[name] = archiveEntry{directory: directory, explicit: true}
	return nil
}

type archiveReader struct {
	reader             io.Reader
	limit, n           int64
	hash               hash.Hash
	tail               [2 * blockSize]byte
	zeroStart, zeroEnd int64
	empty              int
	err                error
}

func (r *archiveReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.n >= r.limit {
		return 0, io.EOF
	}
	if int64(len(p)) > r.limit-r.n {
		p = p[:r.limit-r.n]
	}
	n, err := r.reader.Read(p)
	if n < 0 || n > len(p) {
		return 0, errors.New("invalid archive reader count")
	}
	if n == 0 && err == nil {
		r.empty++
		if r.empty >= 100 {
			return 0, io.ErrNoProgress
		}
	} else {
		r.empty = 0
	}
	if n > 0 {
		r.hash.Write(p[:n])
		if n >= len(r.tail) {
			copy(r.tail[:], p[n-len(r.tail):n])
		} else {
			copy(r.tail[:], r.tail[n:])
			copy(r.tail[len(r.tail)-n:], p[:n])
		}
		begin, end := max(r.n, r.zeroStart), min(r.n+int64(n), r.zeroEnd)
		for i := begin; i < end; i++ {
			if p[i-r.n] != 0 {
				err = errors.New("nonzero artifact padding")
				break
			}
		}
		r.n += int64(n)
	}
	if err != nil {
		r.err = err
	}
	return n, err
}

// headerReader checks fixed fields before tar.Next can process GNU/PAX metadata.
// The replay contains only the preceding body padding and one header. Its offset
// comes from the previous standard-decoded Header.Size, never a numeric parser here.
// Replayed bytes have already passed through the bounded hashing input exactly once.
type headerReader struct {
	input   *archiveReader
	replay  [2 * blockSize]byte
	pending []byte
}

func (r *headerReader) prepare(end int64) error {
	padding := end - r.input.n
	if len(r.pending) != 0 || padding < 0 || padding >= blockSize {
		return errors.New("invalid artifact header boundary")
	}
	n := int(padding) + blockSize
	if _, err := io.ReadFull(r.input, r.replay[:n]); err != nil {
		return fmt.Errorf("read artifact framing: %w", err)
	}
	// ReadFull can suppress an error accompanying a complete read. Do not lose
	// a prior padding/read failure merely because the header-sized read completed.
	if r.input.err != nil && !errors.Is(r.input.err, io.EOF) {
		return r.input.err
	}
	raw := r.replay[n-blockSize : n]
	if !zeroBytes(raw) && (string(raw[257:263]) != "ustar\x00" || string(raw[263:265]) != "00" || raw[156] != tar.TypeReg && raw[156] != tar.TypeRegA && raw[156] != tar.TypeDir) {
		return errors.New("artifact requires unextended USTAR")
	}
	r.pending = r.replay[:n]
	return nil
}

func (r *headerReader) Read(p []byte) (int, error) {
	if len(r.pending) != 0 {
		n := copy(p, r.pending)
		r.pending = r.pending[n:]
		return n, nil
	}
	return r.input.Read(p)
}

type zeros struct{}

func (zeros) Write(p []byte) (int, error) {
	for _, b := range p {
		if b != 0 {
			return 0, errors.New("nonzero trailing artifact data")
		}
	}
	return len(p), nil
}

func cStringPadding(p []byte) bool {
	if i := bytes.IndexByte(p, 0); i >= 0 {
		for _, b := range p[i:] {
			if b != 0 {
				return false
			}
		}
	}
	return true
}

type archiveSummary struct {
	expanded           int64
	files, directories int
}

func readArchive(r io.Reader, size int64, visit func(*tar.Header, string, io.Reader) error, summary *archiveSummary) (string, error) {
	if r == nil || size < 2*blockSize || size > MaxSize {
		return "", errors.New("invalid artifact archive size")
	}
	input := &archiveReader{reader: r, limit: size + 1, hash: sha256.New()}
	gate := &headerReader{input: input}
	tr := tar.NewReader(gate)
	state := archiveNames{entries: make(map[string]archiveEntry)}
	var end int64
	buffer := make([]byte, 32<<10)
	for {
		if err := gate.prepare(end); err != nil {
			return "", err
		}
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			if input.n != end+2*blockSize {
				return "", errors.New("artifact lacks two terminating zero blocks")
			}
			if _, err := io.CopyBuffer(zeros{}, input, buffer); err != nil {
				return "", fmt.Errorf("read artifact trailer: %w", err)
			}
			if input.n != size {
				return "", errors.New("artifact encoded length mismatch")
			}
			if summary != nil {
				*summary = archiveSummary{expanded: state.bytes, files: state.files, directories: state.directories}
			}
			return hex.EncodeToString(input.hash.Sum(nil)), nil
		}
		if err != nil {
			return "", fmt.Errorf("read artifact header: %w", err)
		}
		// The fixed header gate refused extension formats before decoder entry.
		// The standard decoder still owns checksum and numeric parsing; counted
		// framing verifies it consumed exactly this one accepted header.
		if input.n != end+blockSize || header.Format != tar.FormatUSTAR {
			return "", errors.New("artifact requires unextended USTAR")
		}
		raw := input.tail[blockSize:]
		if string(raw[257:263]) != "ustar\x00" || string(raw[263:265]) != "00" || !cStringPadding(raw[:100]) || !cStringPadding(raw[345:500]) || !zeroBytes(raw[157:257]) {
			return "", errors.New("invalid USTAR string fields")
		}
		directory := raw[156] == tar.TypeDir
		if (!directory && raw[156] != tar.TypeReg && raw[156] != tar.TypeRegA) || directory && header.Size != 0 || header.Linkname != "" || len(header.PAXRecords) != 0 || len(header.Xattrs) != 0 || header.Mode & ^int64(0777) != 0 || header.Devmajor != 0 || header.Devminor != 0 {
			return "", errors.New("unsupported artifact entry")
		}
		name, err := canonicalName(header.Name, directory)
		if err != nil {
			return "", err
		}
		if err := state.add(name, directory, header.Size); err != nil {
			return "", err
		}
		payloadEnd := input.n + header.Size
		end = input.n + (header.Size+blockSize-1)/blockSize*blockSize
		if end > size-2*blockSize {
			return "", errors.New("artifact entry exceeds encoded length")
		}
		input.zeroStart, input.zeroEnd = payloadEnd, end
		if visit != nil {
			err = visit(header, name, tr)
		} else {
			_, err = io.CopyBuffer(io.Discard, io.LimitReader(tr, header.Size), buffer)
		}
		if err != nil {
			return "", err
		}
		if input.n != payloadEnd {
			return "", errors.New("artifact entry was not completely consumed")
		}
	}
}

func zeroBytes(p []byte) bool {
	for _, b := range p {
		if b != 0 {
			return false
		}
	}
	return true
}
