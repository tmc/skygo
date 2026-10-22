//go:build darwin || linux

package artifact

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

// trimLog records actual owned I/O in order. Every hook performs the real
// operation first unless the test injects a failure for that exact call.
type trimLog struct {
	ops  []string
	fail func(op string, size int64) error
}

func (l *trimLog) install(f *Files) {
	l.ops = nil
	f.hooks = filesIO{
		writeAt: func(file *os.File, p []byte, offset int64) (int, error) {
			l.ops = append(l.ops, "write")
			return file.WriteAt(p, offset)
		},
		truncate: func(file *os.File, size int64) error {
			op := fmt.Sprintf("truncate %d", size)
			l.ops = append(l.ops, op)
			if l.fail != nil {
				if err := l.fail(op, size); err != nil {
					return err
				}
			}
			return file.Truncate(size)
		},
		sync: func(file *os.File) error {
			if file != f.directory {
				l.ops = append(l.ops, "sync")
				if l.fail != nil {
					if err := l.fail("sync", 0); err != nil {
						return errors.Join(file.Sync(), err)
					}
				}
			}
			return file.Sync()
		},
		close: func(file *os.File) error {
			l.ops = append(l.ops, "close")
			err := file.Close()
			if l.fail != nil {
				err = errors.Join(err, l.fail("close", 0))
			}
			return err
		},
	}
}

// trimOps is the actual grow/shrink pair expected at size, or nothing where
// trimAllocation is the documented no-op.
func trimOps(size int64) []string {
	if runtime.GOOS != "darwin" {
		return nil
	}
	return []string{fmt.Sprintf("truncate %d", size+1), fmt.Sprintf("truncate %d", size)}
}

func requireTrim(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "darwin" {
		t.Skip("allocation trim is a no-op outside Darwin")
	}
}

type trimFact struct {
	id     FileID
	size   int64
	blocks int64
	digest string
}

func trimObserve(t *testing.T, root *os.Root, name string) trimFact {
	t.Helper()
	info, err := root.Lstat(name)
	if err != nil {
		t.Fatal(err)
	}
	m, err := fileMetadata(info)
	if err != nil {
		t.Fatal(err)
	}
	s, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("allocation unavailable")
	}
	h := sha256.Sum256(readFile(t, root, name))
	return trimFact{id: fileID(m), size: info.Size(), blocks: int64(s.Blocks), digest: hex.EncodeToString(h[:])}
}

func trimExtend(t *testing.T, root *os.Root, name string, size int64) {
	t.Helper()
	file, err := root.OpenFile(name, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(file.Truncate(size), file.Close()); err != nil {
		t.Fatal(err)
	}
}

func TestTrimWritePartOrder(t *testing.T) {
	f, _, _ := filesFixture(t)
	data := testArchive(t, testEntry{name: "file", body: strings.Repeat("p", 3000)})
	ref := filesRef(strings.Repeat("7", 32), data)
	id, err := f.CreatePart(ref)
	if err != nil {
		t.Fatal(err)
	}
	var log trimLog
	log.install(f)
	if _, err := f.WritePart(ref, id, 0, 0, data[:1000]); err != nil {
		t.Fatal(err)
	}
	want := append(append([]string{"write"}, trimOps(1000)...), "sync", "close")
	if !slices.Equal(log.ops, want) {
		t.Fatalf("append ops=%q, want %q", log.ops, want)
	}
	log.install(f)
	if _, err := f.WritePart(ref, id, 1000, 100, data[100:300]); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(log.ops, []string{"close"}) {
		t.Fatalf("acknowledged replay ops=%q, want compare only", log.ops)
	}
	log.install(f)
	if err := f.RecoverPart(ref, id, 1000); err != nil {
		t.Fatal(err)
	}
	want = append(trimOps(1000), "sync", "close")
	if !slices.Equal(log.ops, want) {
		t.Fatalf("recover ops=%q, want %q", log.ops, want)
	}
	log.install(f)
	if _, err := f.WritePart(ref, id, 1000, 1000, data[1000:]); err != nil {
		t.Fatal(err)
	}
	log.install(f)
	if _, err := f.SealPart(ref, id, testPublicationGrowth); err != nil {
		t.Fatal(err)
	}
	want = append(trimOps(ref.Size), "sync", "close")
	if !slices.Equal(log.ops, want) {
		t.Fatalf("seal ops=%q, want %q", log.ops, want)
	}
}

func TestTrimCandidateOrder(t *testing.T) {
	f, root, _ := filesFixture(t)
	data := testArchive(t, testEntry{name: "file", body: strings.Repeat("c", 3000)})
	c, err := f.CreateCandidate(strings.Repeat("d", 32))
	if err != nil {
		t.Fatal(err)
	}
	var log trimLog
	log.install(f)
	for _, part := range [][]byte{data[:37], data[37:]} {
		before := c.Description().Size
		log.ops = nil
		if n, err := c.Write(part); err != nil || n != len(part) {
			t.Fatalf("write=%d %v", n, err)
		}
		want := append([]string{"write"}, trimOps(before+int64(len(part)))...)
		if !slices.Equal(log.ops, want) {
			t.Fatalf("candidate write ops=%q, want %q", log.ops, want)
		}
	}
	desc := c.Description()
	log.ops = nil
	receipt, err := c.Seal(testPublicationGrowth)
	if err != nil {
		t.Fatal(err)
	}
	// The original writer trims, syncs and closes; sealPart's own single-link
	// preparation then repeats the trim on its validation descriptor.
	want := append(trimOps(desc.Size), "sync", "close")
	want = append(want, trimOps(desc.Size)...)
	want = append(want, "sync", "close")
	if !slices.Equal(log.ops, want) {
		t.Fatalf("candidate seal ops=%q, want %q", log.ops, want)
	}
	if receipt.File != desc.File || receipt.Manifest.Reference != filesRef(desc.ID, data) {
		t.Fatalf("receipt=%+v", receipt)
	}
	if got := trimObserve(t, root, desc.ID+".tar"); got.id != desc.File || got.size != int64(len(data)) || got.digest != desc.SHA256 {
		t.Fatalf("sealed object=%+v, want %+v", got, desc)
	}
}

func TestTrimLegFailures(t *testing.T) {
	requireTrim(t)
	for _, leg := range []string{"grow", "shrink"} {
		t.Run(leg, func(t *testing.T) {
			f, root, _ := filesFixture(t)
			data := testArchive(t, testEntry{name: "file", body: strings.Repeat("l", 3000)})
			ref := filesRef(strings.Repeat("8", 32), data)
			id, err := f.CreatePart(ref)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.WritePart(ref, id, 0, 0, data[:1000]); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected truncate failure")
			var log trimLog
			log.fail = func(op string, size int64) error {
				if leg == "shrink" && size == ref.Size {
					return injected
				}
				return nil
			}
			log.install(f)
			if leg == "grow" {
				// The grow reports failure after it changed the length.
				f.hooks.truncate = func(file *os.File, size int64) error {
					log.ops = append(log.ops, fmt.Sprintf("truncate %d", size))
					err := file.Truncate(size)
					if size == ref.Size+1 {
						err = errors.Join(err, injected)
					}
					return err
				}
			}
			receipt, err := f.WritePart(ref, id, 1000, 1000, data[1000:])
			f.hooks = filesIO{}
			if !errors.Is(err, injected) || receipt.SHA256 != "" {
				t.Fatalf("false final ack=%+v %v", receipt, err)
			}
			if slices.Contains(log.ops, "sync") {
				t.Fatalf("synced after failed trim: %q", log.ops)
			}
			name := partName(ref)
			// A failed grow restores the length; a failed shrink leaves exactly
			// one scratch byte, here past ref.Size.
			want := ref.Size
			if leg == "shrink" {
				want++
			}
			got := trimObserve(t, root, name)
			if got.id != id || got.size != want {
				t.Fatalf("after failed %s: %+v, want size %d", leg, got, want)
			}
			if leg == "shrink" {
				// Ordinary operations stay strict about the scratch byte.
				if _, err := f.WritePart(ref, id, 1000, 0, data[:16]); !errors.Is(err, ErrFileConflict) {
					t.Fatalf("strict replay admitted scratch byte: %v", err)
				}
			}
			if err := f.RecoverPart(ref, id, 1000); err != nil {
				t.Fatal(err)
			}
			if got := trimObserve(t, root, name); got.id != id || got.size != 1000 {
				t.Fatalf("recovered=%+v", got)
			}
			if _, err := f.WritePart(ref, id, 1000, 1000, data[1000:]); err != nil {
				t.Fatal(err)
			}
			sealed, err := f.SealPart(ref, id, testPublicationGrowth)
			if err != nil || sealed.File != id || sealed.Manifest.Reference != ref {
				t.Fatalf("seal=%+v %v", sealed, err)
			}
			if got := trimObserve(t, root, objectName(ref)); got.id != id || got.digest != ref.SHA256 {
				t.Fatalf("object=%+v", got)
			}
		})
	}
}

// trimFullArchive is a valid archive of exactly MaxSize encoded bytes.
func trimFullArchive(t *testing.T) []byte {
	t.Helper()
	var b bytes.Buffer
	w := tar.NewWriter(&b)
	body := MaxSize - 3*blockSize
	if err := w.WriteHeader(&tar.Header{Name: "full", Mode: 0600, Size: body, Format: tar.FormatUSTAR, ModTime: archiveTime}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(bytes.Repeat([]byte("f"), int(body))); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if int64(b.Len()) != MaxSize {
		t.Fatalf("archive=%d, want %d", b.Len(), MaxSize)
	}
	return b.Bytes()
}

// trimReopen positively closes Files and opens a new one on the same Root, as
// a restart does. No original owner remains.
func trimReopen(t *testing.T, f *Files, root *os.Root) *Files {
	t.Helper()
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := Open(root, f.identity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := next.Close(); err != nil {
			t.Error(err)
		}
	})
	return next
}

func TestTrimCapEndpointRestart(t *testing.T) {
	requireTrim(t)
	root, _ := testRoot(t)
	info, err := root.Lstat(".")
	if err != nil {
		t.Fatal(err)
	}
	m, err := fileMetadata(info)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Initialize(root, fileID(m), strings.Repeat("c", 32)); err != nil {
		t.Fatal(err)
	}
	f, err := Open(root, fileID(m))
	if err != nil {
		t.Fatal(err)
	}
	data := trimFullArchive(t)
	ref := filesRef(strings.Repeat("9", 32), data)
	if err := ref.Validate(); err != nil {
		t.Fatal(err)
	}
	id, err := f.CreatePart(ref)
	if err != nil {
		t.Fatal(err)
	}
	last := ref.Size - chunkSize
	for offset := int64(0); offset < last; offset += chunkSize {
		if _, err := f.WritePart(ref, id, offset, offset, data[offset:offset+chunkSize]); err != nil {
			t.Fatal(err)
		}
	}
	// Final append: the grow to MaxSize+1 succeeds, the shrink fails.
	injected := errors.New("injected shrink failure")
	var log trimLog
	log.fail = func(op string, size int64) error {
		if size == MaxSize {
			return injected
		}
		return nil
	}
	log.install(f)
	if _, err := f.WritePart(ref, id, last, last, data[last:]); !errors.Is(err, injected) {
		t.Fatalf("final append=%v", err)
	}
	f.hooks = filesIO{}
	name := partName(ref)
	if got := trimObserve(t, root, name); got.id != id || got.size != MaxSize+1 {
		t.Fatalf("scratch=%+v", got)
	}
	f = trimReopen(t, f, root)
	if _, err := f.WritePart(ref, id, last, 0, data[:chunkSize]); !errors.Is(err, ErrFileConflict) {
		t.Fatalf("strict replay admitted MaxSize+1: %v", err)
	}
	if err := f.RecoverPart(ref, id, last); err != nil {
		t.Fatal(err)
	}
	if got := trimObserve(t, root, name); got.id != id || got.size != last {
		t.Fatalf("recovered=%+v", got)
	}
	if _, err := f.WritePart(ref, id, last, last, data[last:]); err != nil {
		t.Fatal(err)
	}
	if got := trimObserve(t, root, name); got.id != id || got.size != MaxSize || got.digest != ref.SHA256 {
		t.Fatalf("full part=%+v", got)
	}
	// A crash between grow and shrink at full ack, then restart.
	trimExtend(t, root, name, MaxSize+1)
	f = trimReopen(t, f, root)
	if err := f.RecoverPart(ref, id, MaxSize); err != nil {
		t.Fatal(err)
	}
	if got := trimObserve(t, root, name); got.id != id || got.size != MaxSize || got.digest != ref.SHA256 {
		t.Fatalf("full-ack repair=%+v", got)
	}
	// The same boundary inside sealPart's own preparation.
	trimExtend(t, root, name, MaxSize+1)
	f = trimReopen(t, f, root)
	sealed, err := f.SealPart(ref, id, testPublicationGrowth)
	if err != nil || sealed.File != id || sealed.Manifest.Reference != ref {
		t.Fatalf("seal=%+v %v", sealed, err)
	}
	if got := trimObserve(t, root, objectName(ref)); got.id != id || got.size != MaxSize || got.digest != ref.SHA256 {
		t.Fatalf("object=%+v", got)
	}
}

func TestTrimScratchEnvelopeIsOneByte(t *testing.T) {
	f, root, _ := filesFixture(t)
	data := testArchive(t, testEntry{name: "file", body: "payload"})
	ref := filesRef(strings.Repeat("a", 32), data)
	id := completePart(t, f, ref, data)
	name := partName(ref)
	trimExtend(t, root, name, ref.Size+2)
	before := trimObserve(t, root, name)
	if err := f.RecoverPart(ref, id, ref.Size); !errors.Is(err, ErrFileConflict) {
		t.Fatalf("recover admitted two bytes: %v", err)
	}
	if _, err := f.SealPart(ref, id, testPublicationGrowth); !errors.Is(err, ErrFileConflict) {
		t.Fatalf("seal admitted two bytes: %v", err)
	}
	if err := f.RemovePart(ref, id); !errors.Is(err, ErrFileConflict) {
		t.Fatalf("remove admitted two bytes: %v", err)
	}
	if after := trimObserve(t, root, name); after.id != before.id || after.size != before.size || after.digest != before.digest {
		t.Fatalf("refused part changed: %+v -> %+v", before, after)
	}
	wrong := id
	wrong.Inode++
	trimExtend(t, root, name, ref.Size+1)
	if err := f.RecoverPart(ref, wrong, ref.Size); !errors.Is(err, ErrFileConflict) {
		t.Fatalf("wrong inode repaired: %v", err)
	}
	if got := trimObserve(t, root, name); got.size != ref.Size+1 {
		t.Fatalf("wrong-inode refusal changed part: %+v", got)
	}
	// Expiry removal repairs the exact single-link part, then removes it
	// strictly.
	var log trimLog
	log.install(f)
	err := f.RemovePart(ref, id)
	f.hooks = filesIO{}
	if err != nil {
		t.Fatal(err)
	}
	want := []string{fmt.Sprintf("truncate %d", ref.Size), "sync", "close", "close"}
	if !slices.Equal(log.ops, want) {
		t.Fatalf("remove ops=%q, want %q", log.ops, want)
	}
	if _, err := root.Lstat(name); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("part remains: %v", err)
	}
}

func TestTrimNeverTouchesLinkedObject(t *testing.T) {
	for _, stage := range []string{"linked", "object-only"} {
		t.Run(stage, func(t *testing.T) {
			f, root, _ := filesFixture(t)
			data := testArchive(t, testEntry{name: "file", body: "payload"})
			ref := filesRef(strings.Repeat("b", 32), data)
			id := completePart(t, f, ref, data)
			injected := errors.New("injected directory sync failure")
			calls := 0
			f.hooks.sync = func(file *os.File) error {
				err := file.Sync()
				if file == f.directory {
					calls++
					if stage == "linked" && calls == 1 || stage == "object-only" && calls == 2 {
						return errors.Join(err, injected)
					}
				}
				return err
			}
			if _, err := f.SealPart(ref, id, testPublicationGrowth); !errors.Is(err, injected) {
				t.Fatalf("seal=%v", err)
			}
			f.hooks = filesIO{}
			before := trimObserve(t, root, objectName(ref))
			var log trimLog
			log.install(f)
			receipt, err := f.SealPart(ref, id, testPublicationGrowth)
			f.hooks = filesIO{}
			if err != nil || receipt.File != id {
				t.Fatalf("prefix seal=%+v %v", receipt, err)
			}
			for _, op := range log.ops {
				if strings.HasPrefix(op, "truncate") {
					t.Fatalf("linked object truncated: %q", log.ops)
				}
			}
			if after := trimObserve(t, root, objectName(ref)); after.id != before.id || after.size != before.size || after.digest != before.digest {
				t.Fatalf("object changed: %+v -> %+v", before, after)
			}
			// A linked object with a trailing byte is never repaired or adopted.
			trimExtend(t, root, objectName(ref), ref.Size+1)
			if _, err := f.SealPart(ref, id, testPublicationGrowth); !errors.Is(err, ErrFileConflict) {
				t.Fatalf("object scratch admitted: %v", err)
			}
			if got := trimObserve(t, root, objectName(ref)); got.size != ref.Size+1 {
				t.Fatalf("object mutated: %+v", got)
			}
		})
	}
}

func TestTrimStickyFailures(t *testing.T) {
	injected := errors.New("injected owned I/O failure")
	t.Run("candidate-trim", func(t *testing.T) {
		requireTrim(t)
		f, root, _ := filesFixture(t)
		c, err := f.CreateCandidate(strings.Repeat("e", 32))
		if err != nil {
			t.Fatal(err)
		}
		var log trimLog
		log.fail = func(op string, size int64) error {
			if size == 64 {
				return injected
			}
			return nil
		}
		log.install(f)
		n, err := c.Write(bytes.Repeat([]byte("x"), 64))
		// The transient failure stops before Close.
		f.hooks = filesIO{}
		if n != 64 || !errors.Is(err, injected) {
			t.Fatalf("write=%d %v", n, err)
		}
		desc := c.Description()
		h := sha256.Sum256(bytes.Repeat([]byte("x"), 64))
		if desc.Size != 64 || desc.SHA256 != hex.EncodeToString(h[:]) {
			t.Fatalf("accepted prefix=%+v", desc)
		}
		if n, err := c.Write([]byte("y")); n != 0 || !errors.Is(err, injected) {
			t.Fatalf("sticky write=%d %v", n, err)
		}
		if _, err := c.Seal(testPublicationGrowth); !errors.Is(err, injected) {
			t.Fatalf("sticky seal=%v", err)
		}
		if got := trimObserve(t, root, desc.ID+".part"); got.id != desc.File || got.size != 65 {
			t.Fatalf("scratch=%+v", got)
		}
		if err := c.Close(); !errors.Is(err, injected) {
			t.Fatalf("abort=%v", err)
		}
		// Close restored the counted prefix on the original writer, so the
		// ordinary exact cleanup of the closed prefix succeeds.
		if got := trimObserve(t, root, desc.ID+".part"); got.id != desc.File || got.size != 64 || got.digest != desc.SHA256 {
			t.Fatalf("restored=%+v, want %+v", got, desc)
		}
		if err := f.RemoveCandidateNames(desc); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("candidate-seal-sync", func(t *testing.T) {
		f, root, _ := filesFixture(t)
		data := testArchive(t, testEntry{name: "file", body: "payload"})
		c, err := f.CreateCandidate(strings.Repeat("f", 32))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Write(data); err != nil {
			t.Fatal(err)
		}
		var log trimLog
		log.fail = func(op string, size int64) error {
			if op == "sync" {
				return injected
			}
			return nil
		}
		log.install(f)
		_, err = c.Seal(testPublicationGrowth)
		f.hooks = filesIO{}
		if !errors.Is(err, injected) {
			t.Fatalf("seal=%v", err)
		}
		want := append(trimOps(int64(len(data))), "sync", "close")
		if !slices.Equal(log.ops, want) {
			t.Fatalf("ops=%q, want %q", log.ops, want)
		}
		if _, err := c.Seal(testPublicationGrowth); !errors.Is(err, injected) {
			t.Fatalf("sticky seal=%v", err)
		}
		desc := c.Description()
		if _, err := root.Lstat(desc.ID + ".tar"); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("published after failure: %v", err)
		}
		if got := trimObserve(t, root, desc.ID+".part"); got.id != desc.File || got.size != desc.Size {
			t.Fatalf("part=%+v", got)
		}
		if err := c.Close(); !errors.Is(err, injected) {
			t.Fatalf("abort=%v", err)
		}
	})
	for _, stage := range []string{"sync", "close"} {
		t.Run("write-"+stage, func(t *testing.T) {
			f, root, _ := filesFixture(t)
			data := testArchive(t, testEntry{name: "file", body: "payload"})
			ref := filesRef(strings.Repeat("c", 32), data)
			id, err := f.CreatePart(ref)
			if err != nil {
				t.Fatal(err)
			}
			var log trimLog
			log.fail = func(op string, size int64) error {
				if op == stage {
					return injected
				}
				return nil
			}
			log.install(f)
			receipt, err := f.WritePart(ref, id, 0, 0, data)
			f.hooks = filesIO{}
			if !errors.Is(err, injected) || receipt.SHA256 != "" {
				t.Fatalf("false ack=%+v %v", receipt, err)
			}
			want := append(append([]string{"write"}, trimOps(ref.Size)...), "sync", "close")
			if !slices.Equal(log.ops, want) {
				t.Fatalf("ops=%q, want %q", log.ops, want)
			}
			if stage == "close" {
				if !errors.Is(err, ErrFileCloseUncertain) {
					t.Fatal("close uncertainty lost")
				}
				if err := f.RecoverPart(ref, id, 0); !errors.Is(err, ErrFilesBusy) {
					t.Fatalf("admitted after uncertain close: %v", err)
				}
				discardTestClosedOwners(t, f)
			}
			if err := f.RecoverPart(ref, id, 0); err != nil {
				t.Fatal(err)
			}
			if got := trimObserve(t, root, partName(ref)); got.id != id || got.size != 0 {
				t.Fatalf("recovered=%+v", got)
			}
		})
	}
}

// trimAllowance is Store's prepaid per-file physical allowance.
func trimAllowance(size int64, unit uint64) int64 {
	u := int64(unit)
	return ((size+u-1)/u + 2) * u
}

// trimControl appends like the pre-trim product: one open/pwrite/Sync/Close
// per append, or one descriptor for a candidate writer. It shows whether this
// volume reproduces retained allocation at all.
type trimControl struct {
	file *os.File
	name string
}

func (c *trimControl) append(t *testing.T, p []byte, offset int64, perAppend bool) {
	t.Helper()
	file := c.file
	if perAppend {
		var err error
		file, err = os.OpenFile(c.name, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err := file.WriteAt(p, offset)
	if err == nil {
		err = file.Sync()
	}
	if perAppend {
		err = errors.Join(err, file.Close())
	}
	if err != nil {
		t.Fatal(err)
	}
}

func trimBlocks(t *testing.T, name string) int64 {
	t.Helper()
	info, err := os.Lstat(name)
	if err != nil {
		t.Fatal(err)
	}
	return int64(info.Sys().(*syscall.Stat_t).Blocks) * 512
}

// TestTrimDelayedAllocation runs the measured delayed append patterns through
// the actual WritePart and Candidate paths beside an untrimmed control. The
// bound is asserted only on Darwin, and native coverage is reported only when
// the control actually retained excess on this volume.
func TestTrimDelayedAllocation(t *testing.T) {
	requireTrim(t)
	if testing.Short() {
		t.Skip("delayed appends take seconds")
	}
	for _, test := range []struct {
		name      string
		candidate bool
		sizes     []int
	}{
		{"input-81920", false, []int{65536, 16384}},
		{"input-three-appends", false, []int{65536, 65536, 16384}},
		{"output-71168", true, []int{37, 65536, 5595}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, root, dir := filesFixture(t)
			facts, err := f.RootFacts()
			if err != nil {
				t.Fatal(err)
			}
			var total int
			for _, n := range test.sizes {
				total += n
			}
			payload := bytes.Repeat([]byte("a"), total)
			id := strings.Repeat("d", 32)
			name := id + ".part"
			var ref Ref
			var candidate *Candidate
			var file FileID
			if test.candidate {
				candidate, err = f.CreateCandidate(id)
				if err == nil {
					file = candidate.Description().File
				}
			} else {
				ref = filesRef(id, payload)
				file, err = f.CreatePart(ref)
			}
			if err != nil {
				t.Fatal(err)
			}
			control := &trimControl{name: filepath.Join(t.TempDir(), "control")}
			control.file, err = os.OpenFile(control.name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := control.file.Close(); err != nil {
					t.Error(err)
				}
			}()
			excess := false
			offset := 0
			for i, n := range test.sizes {
				if i > 0 {
					time.Sleep(1200 * time.Millisecond)
				}
				p := payload[offset : offset+n]
				if test.candidate {
					if w, err := candidate.Write(p); err != nil || w != n {
						t.Fatalf("candidate write=%d %v", w, err)
					}
				} else if _, err := f.WritePart(ref, file, int64(offset), int64(offset), p); err != nil {
					t.Fatal(err)
				}
				control.append(t, p, int64(offset), !test.candidate)
				offset += n
				allowance := trimAllowance(int64(offset), facts.Unit)
				got := trimObserve(t, root, name)
				untrimmed := trimBlocks(t, control.name)
				t.Logf("append %d: size=%d product=%d control=%d allowance=%d", i+1, offset, got.blocks*512, untrimmed, allowance)
				if got.id != file || got.size != int64(offset) {
					t.Fatalf("identity/size=%+v", got)
				}
				if got.blocks*512 > allowance {
					t.Errorf("append %d: product allocation %d exceeds allowance %d", i+1, got.blocks*512, allowance)
				}
				excess = excess || untrimmed > allowance
			}
			if test.candidate {
				h := sha256.Sum256(payload)
				if desc := candidate.Description(); desc.File != file || desc.Size != int64(total) || desc.SHA256 != hex.EncodeToString(h[:]) {
					t.Fatalf("candidate=%+v", desc)
				}
				if err := candidate.Close(); err != nil {
					t.Fatal(err)
				}
			}
			h := sha256.Sum256(payload)
			if got := trimObserve(t, root, name); got.id != file || got.digest != hex.EncodeToString(h[:]) {
				t.Fatalf("final part=%+v", got)
			}
			if excess {
				t.Logf("NATIVE OBSERVED: untrimmed control retained excess in %s; product stayed within allowance", dir)
			} else {
				t.Logf("NATIVE NOT-OBSERVED: untrimmed control retained no excess on this volume; portable correctness only")
			}
		})
	}
}

// TestTrimRecoverOldPart repairs a part whose delayed appends were written
// without trimming, as by an earlier binary, on the exact same inode.
func TestTrimRecoverOldPart(t *testing.T) {
	requireTrim(t)
	if testing.Short() {
		t.Skip("delayed appends take seconds")
	}
	f, root, _ := filesFixture(t)
	facts, err := f.RootFacts()
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("o"), 81920)
	ref := filesRef(strings.Repeat("e", 32), payload)
	id, err := f.CreatePart(ref)
	if err != nil {
		t.Fatal(err)
	}
	name := partName(ref)
	old := &trimControl{name: filepath.Join(root.Name(), name)}
	old.append(t, payload[:65536], 0, true)
	time.Sleep(1200 * time.Millisecond)
	old.append(t, payload[65536:], 65536, true)
	before := trimObserve(t, root, name)
	allowance := trimAllowance(ref.Size, facts.Unit)
	if err := f.RecoverPart(ref, id, ref.Size); err != nil {
		t.Fatal(err)
	}
	after := trimObserve(t, root, name)
	t.Logf("old part allocation %d -> %d, allowance %d", before.blocks*512, after.blocks*512, allowance)
	if after.id != id || after.id != before.id || after.size != ref.Size || after.digest != ref.SHA256 {
		t.Fatalf("repair changed identity or bytes: %+v -> %+v", before, after)
	}
	if after.blocks*512 > allowance {
		t.Errorf("repaired allocation %d exceeds allowance %d", after.blocks*512, allowance)
	}
	if before.blocks*512 > allowance {
		t.Logf("NATIVE OBSERVED: old part retained excess before repair")
	} else {
		t.Logf("NATIVE NOT-OBSERVED: old part retained no excess on this volume; portable correctness only")
	}
}

// trimCandidateFailure writes size counted bytes, failing exactly one shrink
// at size: in Write, or in Seal's own backstop trim.
func trimCandidateFailure(t *testing.T, f *Files, id string, size int64, stage string, injected error) (*Candidate, CandidateFile) {
	t.Helper()
	c, err := f.CreateCandidate(id)
	if err != nil {
		t.Fatal(err)
	}
	failed := false
	var log trimLog
	log.fail = func(op string, n int64) error {
		if !failed && n == size && strings.HasPrefix(op, "truncate") {
			failed = true
			return injected
		}
		return nil
	}
	if stage == "write" {
		log.install(f)
	}
	payload := bytes.Repeat([]byte("r"), int(size))
	n, err := c.Write(payload)
	if stage == "write" {
		if n != len(payload) || !errors.Is(err, injected) {
			t.Fatalf("write=%d %v", n, err)
		}
	} else {
		if n != len(payload) || err != nil {
			t.Fatalf("write=%d %v", n, err)
		}
		log.install(f)
		if _, err := c.Seal(testPublicationGrowth); !errors.Is(err, injected) {
			t.Fatalf("seal=%v", err)
		}
	}
	f.hooks = filesIO{}
	if !failed {
		t.Fatal("shrink failure not injected")
	}
	h := sha256.Sum256(payload)
	desc := c.Description()
	if desc.Size != size || desc.SHA256 != hex.EncodeToString(h[:]) {
		t.Fatalf("counted prefix=%+v", desc)
	}
	return c, desc
}

// TestTrimCandidateRestore covers a failed candidate shrink in Write and in
// Seal's backstop, at a small size and at MaxSize: the original writer is
// restored to its counted prefix before it is relinquished, the trim error
// stays sticky, and the ordinary exact cleanup of the closed prefix succeeds.
func TestTrimCandidateRestore(t *testing.T) {
	requireTrim(t)
	injected := errors.New("injected shrink failure")
	for _, stage := range []string{"write", "seal"} {
		for _, size := range []int64{64, MaxSize} {
			t.Run(fmt.Sprintf("%s-%d", stage, size), func(t *testing.T) {
				f, root, _ := filesFixture(t)
				c, desc := trimCandidateFailure(t, f, strings.Repeat("a", 32), size, stage, injected)
				name := desc.ID + ".part"
				if stage == "write" {
					if got := trimObserve(t, root, name); got.size != size+1 {
						t.Fatalf("scratch=%+v", got)
					}
					if _, err := c.Seal(testPublicationGrowth); !errors.Is(err, injected) {
						t.Fatalf("sticky seal=%v", err)
					}
				}
				if err := c.Close(); !errors.Is(err, injected) {
					t.Fatalf("close=%v", err)
				}
				if got := trimObserve(t, root, name); got.id != desc.File || got.size != size || got.digest != desc.SHA256 {
					t.Fatalf("restored=%+v, want %+v", got, desc)
				}
				if _, err := root.Lstat(desc.ID + ".tar"); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("published: %v", err)
				}
				if err := f.RemoveCandidateNames(desc); err != nil {
					t.Fatal(err)
				}
				if _, err := root.Lstat(name); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("part remains: %v", err)
				}
			})
		}
	}
}

// TestTrimCandidateRestoreRefusals keeps ownership and the actual cause when
// restoration cannot proceed, and never truncates a linked or foreign file.
func TestTrimCandidateRestoreRefusals(t *testing.T) {
	requireTrim(t)
	injected := errors.New("injected shrink failure")
	t.Run("restore-failure", func(t *testing.T) {
		f, root, _ := filesFixture(t)
		c, desc := trimCandidateFailure(t, f, strings.Repeat("b", 32), 64, "write", injected)
		restoreErr := errors.New("injected restoration failure")
		f.hooks.truncate = func(*os.File, int64) error { return restoreErr }
		err := c.Close()
		f.hooks = filesIO{}
		if !errors.Is(err, injected) || !errors.Is(err, restoreErr) {
			t.Fatalf("close=%v", err)
		}
		if got := trimObserve(t, root, desc.ID+".part"); got.size != 65 {
			t.Fatalf("part=%+v", got)
		}
		if err := f.RemoveCandidateNames(desc); !errors.Is(err, ErrFilesBusy) {
			t.Fatalf("writer relinquished: %v", err)
		}
		// The retained original writer restores once the failure stops.
		if err := c.Close(); !errors.Is(err, injected) || errors.Is(err, restoreErr) {
			t.Fatalf("retry close=%v", err)
		}
		if err := f.RemoveCandidateNames(desc); err != nil {
			t.Fatal(err)
		}
	})
	for _, kind := range []string{"published-link", "foreign-name"} {
		t.Run(kind, func(t *testing.T) {
			f, root, dir := filesFixture(t)
			c, desc := trimCandidateFailure(t, f, strings.Repeat("c", 32), 64, "write", injected)
			part := filepath.Join(dir, desc.ID+".part")
			other := filepath.Join(dir, desc.ID+".tar")
			var err error
			if kind == "published-link" {
				err = os.Link(part, other)
			} else {
				err = errors.Join(os.Rename(part, other), os.WriteFile(part, []byte("foreign"), 0600))
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Close(); !errors.Is(err, ErrFileConflict) {
				t.Fatalf("close=%v", err)
			}
			if got := trimObserve(t, root, desc.ID+".tar"); got.id != desc.File || got.size != 65 {
				t.Fatalf("original truncated through another name: %+v", got)
			}
			if kind == "foreign-name" && string(readFile(t, root, desc.ID+".part")) != "foreign" {
				t.Fatal("foreign file changed")
			}
			if err := f.RemoveCandidateNames(desc); !errors.Is(err, ErrFilesBusy) {
				t.Fatalf("writer relinquished: %v", err)
			}
			// Undo the fixture's own interference; the owner then restores.
			if kind == "published-link" {
				err = os.Remove(other)
			} else {
				err = errors.Join(os.Remove(part), os.Rename(other, part))
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Close(); !errors.Is(err, injected) {
				t.Fatalf("retry close=%v", err)
			}
			if err := f.RemoveCandidateNames(desc); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("close-uncertain", func(t *testing.T) {
		f, root, _ := filesFixture(t)
		c, desc := trimCandidateFailure(t, f, strings.Repeat("d", 32), 64, "write", injected)
		uncertain := errors.New("injected close uncertainty")
		f.hooks.close = func(file *os.File) error { return errors.Join(file.Close(), uncertain) }
		err := c.Close()
		f.hooks = filesIO{}
		if !errors.Is(err, injected) || !errors.Is(err, ErrFileCloseUncertain) {
			t.Fatalf("close=%v", err)
		}
		if got := trimObserve(t, root, desc.ID+".part"); got.size != 64 || got.digest != desc.SHA256 {
			t.Fatalf("restored=%+v", got)
		}
		if err := f.RemoveCandidateNames(desc); !errors.Is(err, ErrFilesBusy) {
			t.Fatalf("uncertain owner relinquished: %v", err)
		}
		if err := c.Close(); !errors.Is(err, ErrFileCloseUncertain) {
			t.Fatalf("sticky uncertainty=%v", err)
		}
		// Fixture bookkeeping only, after the assertions.
		discardTestClosedOwners(t, f)
		f.mu.Lock()
		delete(f.candidates, c)
		f.mu.Unlock()
	})
}
