//go:build darwin || linux

package rpc_test

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
)

// allocationArchive is a valid archive of exactly size encoded bytes.
func allocationArchive(t *testing.T, size int) ([]byte, string) {
	t.Helper()
	var b bytes.Buffer
	w := tar.NewWriter(&b)
	body := size - 3*512
	if err := w.WriteHeader(&tar.Header{Name: "input.txt", Mode: 0600, Size: int64(body), Format: tar.FormatUSTAR}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(bytes.Repeat([]byte("i"), body)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if b.Len() != size {
		t.Fatalf("archive=%d, want %d", b.Len(), size)
	}
	h := sha256.Sum256(b.Bytes())
	return b.Bytes(), hex.EncodeToString(h[:])
}

// allocationRefusal records the body of a 503 that the enclosing HTTP
// wrapper wrote before any Control method ran, so a refusal for stale
// authentication freshness is told apart from an artifact failure.
type allocationRefusal struct {
	http.ResponseWriter
	refused *allocationRefusals
	code    int
}

type allocationRefusals struct {
	mu   sync.Mutex
	last string
	auth int
}

func (w *allocationRefusal) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *allocationRefusal) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}
func (w *allocationRefusal) Write(p []byte) (int, error) {
	if w.code == http.StatusServiceUnavailable {
		w.refused.mu.Lock()
		w.refused.last = strings.TrimSpace(string(p))
		w.refused.mu.Unlock()
	}
	return w.ResponseWriter.Write(p)
}

// authenticationUnavailable reports whether err is the wrapper's 503 for
// stale file authentication. That refusal precedes Control admission.
func (r *allocationRefusals) authenticationUnavailable(err error) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if connect.CodeOf(err) != connect.CodeUnavailable || r.last != "authentication unavailable" {
		return false
	}
	r.last = ""
	r.auth++
	return true
}

// allocationRetry repeats call while the only refusal is stale
// authentication, at most five times. Every attempt is a new HTTP request
// after the original handler returned.
func allocationRetry[T any](t *testing.T, r *allocationRefusals, ctx context.Context, call func() (T, error)) (T, error) {
	t.Helper()
	for attempt := 0; ; attempt++ {
		out, err := call()
		if attempt == 5 || !r.authenticationUnavailable(err) {
			return out, err
		}
		t.Logf("authentication unavailable before Control admission; retrying (%d)", attempt+1)
		if err := artifactRetryWait(ctx); err != nil {
			return out, err
		}
	}
}

type allocationFact struct {
	inode, links uint64
	size         int64
	allocated    int64
}

func allocationObserve(t *testing.T, name string) allocationFact {
	t.Helper()
	info, err := os.Lstat(name)
	if err != nil {
		t.Fatal(err)
	}
	s, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("allocation unavailable")
	}
	return allocationFact{inode: uint64(s.Ino), links: uint64(s.Nlink), size: info.Size(), allocated: int64(s.Blocks) * 512}
}

// TestArtifactHTTPDelayedAppendAllocation sends the measured delayed append
// patterns through real loopback HTTP and the original Run sampler. After each
// returned response it records the part's actual allocation beside Store's
// prepaid allowance. The bound is asserted only on Darwin; whether the volume
// would have retained excess without the trim is shown by an untrimmed
// control file written with the same delays.
func TestArtifactHTTPDelayedAppendAllocation(t *testing.T) {
	if testing.Short() {
		t.Skip("delayed appends take seconds")
	}
	for _, test := range []struct {
		name   string
		chunks []int
		// suffix first writes the next chunk directly into the part with a
		// delay and without an acknowledgement, as a crash after an untrimmed
		// append would leave it; the HTTP retransmit must repair it.
		suffix bool
	}{
		{"input-81920", []int{65536, 16384}, false},
		{"input-three-appends", []int{65536, 65536, 16384}, false},
		{"input-71168", []int{37, 65536, 5595}, false},
		{"unacknowledged-delayed-suffix", []int{65536, 16384}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			refused := new(allocationRefusals)
			f := artifactRPC(t, func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					next.ServeHTTP(&allocationRefusal{ResponseWriter: w, refused: refused}, r)
				})
			})
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			facts, err := f.files.RootFacts()
			if err != nil {
				t.Fatal(err)
			}
			unit := int64(facts.Unit)
			allowance := func(n int64) int64 { return ((n+unit-1)/unit + 2) * unit }
			total := 0
			for _, n := range test.chunks {
				total += n
			}
			data, digest := allocationArchive(t, total)
			started, err := allocationRetry(t, refused, ctx, func() (*connect.Response[v1.BeginArtifactUploadResponse], error) {
				return f.beginUpload(t, ctx, digest, int64(total))
			})
			if err != nil {
				t.Fatal(err)
			}
			id := started.Msg.Upload.Id
			part := filepath.Join(f.rootName, id+".part")
			control := filepath.Join(f.directory, "control")
			if err := os.WriteFile(control, nil, 0600); err != nil {
				t.Fatal(err)
			}
			first := allocationObserve(t, part)
			excess := false
			offset := 0
			for i, n := range test.chunks {
				if i > 0 {
					time.Sleep(1200 * time.Millisecond)
				}
				p := data[offset : offset+n]
				if test.suffix && i > 0 {
					file, err := os.OpenFile(part, os.O_WRONLY, 0)
					if err != nil {
						t.Fatal(err)
					}
					_, err = file.WriteAt(p, int64(offset))
					if err := errors.Join(err, file.Sync(), file.Close()); err != nil {
						t.Fatal(err)
					}
					time.Sleep(1200 * time.Millisecond)
				}
				out, err := allocationRetry(t, refused, ctx, func() (*connect.Response[v1.WriteArtifactChunkResponse], error) {
					return f.writeUpload(t, ctx, id, int64(offset), p)
				})
				if err != nil || out.Msg.Upload.Acknowledged != int64(offset+n) {
					t.Fatalf("write %d: %v %v", i+1, out, err)
				}
				file, err := os.OpenFile(control, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, err = file.WriteAt(p, int64(offset))
				if err := errors.Join(err, file.Sync(), file.Close()); err != nil {
					t.Fatal(err)
				}
				offset += n
				got := allocationObserve(t, part)
				untrimmed := allocationObserve(t, control).allocated
				t.Logf("append %d: ack=%d product=%d control=%d allowance=%d", i+1, offset, got.allocated, untrimmed, allowance(int64(offset)))
				if got.inode != first.inode || got.links != 1 || got.size != int64(offset) {
					t.Fatalf("part identity=%+v, first %+v", got, first)
				}
				if runtime.GOOS == "darwin" && got.allocated > allowance(int64(offset)) {
					t.Errorf("append %d: product allocation %d exceeds allowance %d", i+1, got.allocated, allowance(int64(offset)))
				}
				excess = excess || untrimmed > allowance(int64(offset))
			}
			// A wholly acknowledged earlier range compares and does not advance.
			if out, err := allocationRetry(t, refused, ctx, func() (*connect.Response[v1.WriteArtifactChunkResponse], error) {
				return f.writeUpload(t, ctx, id, 8, data[8:24])
			}); err != nil || out.Msg.Upload.Acknowledged != int64(total) {
				t.Fatalf("replay: %v %v", out, err)
			}
			finalized, err := allocationRetry(t, refused, ctx, func() (*connect.Response[v1.FinalizeArtifactUploadResponse], error) {
				return f.finalizeUpload(t, ctx, id)
			})
			if err != nil {
				t.Fatal(err)
			}
			ref := finalized.Msg.Reference
			if ref.Id != id || ref.Sha256 != digest || ref.Size != int64(total) {
				t.Fatalf("reference=%v", ref)
			}
			object := allocationObserve(t, filepath.Join(f.rootName, id+".tar"))
			if object.inode != first.inode || object.links != 1 || object.size != int64(total) {
				t.Fatalf("object=%+v, part %+v", object, first)
			}
			if _, err := os.Lstat(part); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("part remains: %v", err)
			}
			again, err := allocationRetry(t, refused, ctx, func() (*connect.Response[v1.FinalizeArtifactUploadResponse], error) {
				return f.finalizeUpload(t, ctx, id)
			})
			if err != nil || again.Msg.Reference.Sha256 != digest || again.Msg.Reference.Size != int64(total) {
				t.Fatalf("finalize replay: %v %v", again, err)
			}
			opened, err := allocationRetry(t, refused, ctx, func() (*connect.Response[v1.BeginArtifactDownloadResponse], error) {
				out, err := f.client.BeginArtifactDownload(ctx, authorized(f.tokens["alice"], &v1.BeginArtifactDownloadRequest{Reference: ref}))
				f.awaitReturned(t, skyv1connect.ControlServiceBeginArtifactDownloadProcedure, 1)
				return out, err
			})
			if err != nil {
				t.Fatal(err)
			}
			var got []byte
			for {
				chunk, err := allocationRetry(t, refused, ctx, func() (*connect.Response[v1.ReadArtifactChunkResponse], error) {
					out, err := f.client.ReadArtifactChunk(ctx, authorized(f.tokens["alice"], &v1.ReadArtifactChunkRequest{DownloadId: opened.Msg.Id, Offset: int64(len(got)), Limit: 64 << 10}))
					f.awaitReturned(t, skyv1connect.ControlServiceReadArtifactChunkProcedure, 1)
					return out, err
				})
				if err != nil {
					t.Fatal(err)
				}
				got = append(got, chunk.Msg.Data...)
				if chunk.Msg.Complete {
					break
				}
			}
			if _, err := allocationRetry(t, refused, ctx, func() (*connect.Response[v1.CloseArtifactDownloadResponse], error) {
				return f.client.CloseArtifactDownload(ctx, authorized(f.tokens["alice"], &v1.CloseArtifactDownloadRequest{DownloadId: opened.Msg.Id}))
			}); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, data) {
				t.Fatal("downloaded bytes differ")
			}
			t.Logf("authentication-unavailable refusals retried: %d", refused.auth)
			switch {
			case runtime.GOOS != "darwin":
				t.Logf("NATIVE NOT-OBSERVED: no allocation trim on %s", runtime.GOOS)
			case excess:
				t.Logf("NATIVE OBSERVED: untrimmed control retained excess; product stayed within allowance")
			default:
				t.Logf("NATIVE NOT-OBSERVED: untrimmed control retained no excess on this volume; portable correctness only")
			}
		})
	}
}
