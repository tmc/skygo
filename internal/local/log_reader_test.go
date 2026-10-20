package local

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/backend"
)

// These are UNRUN source oracles. Executions use the existing strong fixture,
// persistent task directory and actual original done publication. No synthetic
// writer receipt or seeded execution completion supplies native closure.
func logReaderExecution(t *testing.T, f *executionFixture, id, mode string) backend.OutputReference {
	t.Helper()
	b := f.backend
	var output backend.OutputReference
	if b.retention != nil {
		output = retainedReference(t, b, id)
		if err := b.BindOutput(output); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Start(context.Background(), id, task(mode)); err != nil {
		f.held = true
		t.Fatal(err)
	}
	b.mu.Lock()
	e := b.jobs[id]
	b.mu.Unlock()
	if e == nil {
		t.Fatal("actual execution missing")
	}
	awaitDefault(t, e.done, "original log writer execution")
	if b.retention != nil {
		output = closureReference(t, b, output)
		if !output.PayloadReleased {
			t.Fatal("actual writer payload not released")
		}
	}
	return output
}

func emptyLogReaderChunk(chunk backend.LogChunk) bool {
	return chunk.Data == nil && chunk.NextOffset == 0 && !chunk.Complete && !chunk.Truncated
}

func TestLogReaderActualDefaultAndRetainedRanges(t *testing.T) {
	for _, retained := range []bool{false, true} {
		name := "default"
		if retained {
			name = "retained"
		}
		for _, mode := range []string{"success", "output"} {
			t.Run(name+"/"+mode, func(t *testing.T) {
				f := newExecutionFixture(t, retained)
				b := f.backend
				id := strings.Repeat("1", 32)
				logReaderExecution(t, f, id, mode)
				payload := []byte("hello world")
				truncated := false
				if mode == "output" {
					payload = bytes.Repeat([]byte("x"), backend.MaxLogBytes)
					truncated = true
				}
				ref := backend.Reference{ID: id}
				first, err := b.Logs(context.Background(), ref, 0, 4)
				if err != nil || !bytes.Equal(first.Data, payload[:4]) || first.NextOffset != 4 || first.Complete || first.Truncated != truncated {
					t.Fatalf("actual first range=%+v error=%v", first, err)
				}
				again, err := b.Logs(context.Background(), ref, 0, 4)
				if err != nil || !bytes.Equal(again.Data, first.Data) || again.NextOffset != first.NextOffset || again.Complete != first.Complete || again.Truncated != first.Truncated {
					t.Fatalf("actual repeated range=%+v error=%v", again, err)
				}
				offset := int64(len(payload) - 3)
				tail, err := b.Logs(context.Background(), ref, offset, 8)
				if err != nil || !bytes.Equal(tail.Data, payload[len(payload)-3:]) || tail.NextOffset != int64(len(payload)) || !tail.Complete || tail.Truncated != truncated {
					t.Fatalf("actual final range=%+v error=%v", tail, err)
				}
				end, err := b.Logs(context.Background(), ref, int64(len(payload)), 1)
				if err != nil || len(end.Data) != 0 || end.NextOffset != int64(len(payload)) || !end.Complete || end.Truncated != truncated {
					t.Fatalf("actual EOF=%+v error=%v", end, err)
				}
				for _, rangeCase := range []struct {
					offset int64
					limit  int
				}{{-1, 1}, {0, 0}, {0, backend.MaxChunkBytes + 1}, {int64(len(payload)) + 1, 1}} {
					chunk, err := b.Logs(context.Background(), ref, rangeCase.offset, rangeCase.limit)
					if !errors.Is(err, fs.ErrInvalid) || !emptyLogReaderChunk(chunk) {
						t.Fatalf("invalid range returned bytes: %+v %v", chunk, err)
					}
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if chunk, err := b.Logs(ctx, ref, 0, 1); !errors.Is(err, context.Canceled) || !emptyLogReaderChunk(chunk) {
					t.Fatalf("canceled read=%+v %v", chunk, err)
				}
				b.mu.Lock()
				sticky := b.logReaderErr
				b.mu.Unlock()
				if sticky != nil {
					t.Fatalf("positive actual reader Close became sticky: %v", sticky)
				}
				if err := b.CheckShutdown(); err != nil {
					t.Fatalf("positive original writer/readers not settled: %v", err)
				}
			})
		}
	}
}

func TestLogReaderMarkerIdentityAndReadErrors(t *testing.T) {
	f := newExecutionFixture(t, true)
	b := f.backend
	id := strings.Repeat("2", 32)
	logReaderExecution(t, f, id, "output")
	ref := backend.Reference{ID: id}
	marker := filepath.Join(b.directory, id+".truncated")
	// Prepare a distinct actual inode before replacing the closed writer's marker;
	// this is a filesystem identity oracle, not a failed-Close or kernel vector.
	foreign := filepath.Join(b.directory, "foreign-marker")
	if err := os.WriteFile(foreign, []byte("truncated\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(foreign, marker); err != nil {
		t.Fatal(err)
	}
	chunk, err := b.Logs(context.Background(), ref, 0, 1)
	if !errors.Is(err, backend.ErrIdentityMismatch) || !emptyLogReaderChunk(chunk) {
		t.Fatalf("foreign marker accepted: %+v %v", chunk, err)
	}
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	chunk, err = b.Logs(context.Background(), ref, 0, 1)
	if !errors.Is(err, backend.ErrIdentityMismatch) || !emptyLogReaderChunk(chunk) {
		t.Fatalf("missing original marker accepted: %+v %v", chunk, err)
	}
	if err := os.Remove(filepath.Join(b.directory, id+".log")); err != nil {
		t.Fatal(err)
	}
	chunk, err = b.Logs(context.Background(), ref, 0, 1)
	if !errors.Is(err, fs.ErrNotExist) || !emptyLogReaderChunk(chunk) {
		t.Fatalf("missing actual log=%+v %v", chunk, err)
	}
	b.mu.Lock()
	sticky := b.logReaderErr
	b.mu.Unlock()
	if sticky != nil {
		t.Fatalf("returned identity/open error fabricated reader Close hold: %v", sticky)
	}
}

func TestLogReaderCloseRefusesBeforeOpeningPath(t *testing.T) {
	for _, retained := range []bool{false, true} {
		name := "default"
		if retained {
			name = "retained"
		}
		t.Run(name, func(t *testing.T) {
			f := newExecutionFixture(t, retained)
			b := f.backend
			id := strings.Repeat("3", 32)
			logReaderExecution(t, f, id, "success")
			if err := f.close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(b.directory, id+".log")
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			// An open-first implementation would report the missing path instead. This
			// verifies refusal precedence, not a dynamic syscall/open counter.
			chunk, err := b.Logs(context.Background(), backend.Reference{ID: id}, 0, 1)
			if err == nil || err.Error() != "local backend is closed" || errors.Is(err, fs.ErrNotExist) || !emptyLogReaderChunk(chunk) {
				t.Fatalf("closed read reached missing path: %+v %v", chunk, err)
			}
		})
	}
}

// A preclosed *os.File produces a REAL Go Close error, but is only a finisher
// policy vector: it does not model failed kernel Close, a live numeric FD or a
// native execution receipt. The same strong fixture retains original file/error.
func logReaderPreclosed(t *testing.T, f *executionFixture) *os.File {
	t.Helper()
	file, err := os.OpenFile(filepath.Join(f.directory, "reader-policy-vector"), os.O_CREATE|os.O_RDWR|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		// Unexpected setup uncertainty still retains its actual Go file/error; no
		// retry or namespace cleanup may discard it during test failure unwind.
		f.held = true
		f.backend.mu.Lock()
		f.backend.logReaderErr = &logReaderCloseError{file: file, err: err}
		f.backend.mu.Unlock()
		t.Fatal(err)
	}
	return file
}

func TestLogReaderFinisherPreservesExactFirstOwnerAndCause(t *testing.T) {
	for _, retained := range []bool{false, true} {
		name := "default"
		if retained {
			name = "retained"
		}
		t.Run(name, func(t *testing.T) {
			f := newExecutionFixture(t, retained)
			b := f.backend
			file := logReaderPreclosed(t, f)
			f.held = true
			readErr := errors.New("returned reader data error")
			result := error(readErr)
			chunk := backend.LogChunk{Data: []byte("must not escape"), NextOffset: 15, Complete: true, Truncated: true}
			b.mu.Lock()
			b.finishLogReader(file, &chunk, &result)
			first := b.logReaderErr
			b.mu.Unlock()
			if first == nil || first.file != file || !errors.Is(first, os.ErrClosed) || !errors.Is(result, readErr) || !errors.Is(result, first.err) || !emptyLogReaderChunk(chunk) {
				t.Fatalf("first owner/cause/chunk not preserved: %+v %v", chunk, result)
			}
			var owner *logReaderCloseError
			var cause *os.PathError
			if !errors.As(result, &owner) || owner != first || !errors.As(first.err, &cause) || cause.Op != "close" || cause.Path != file.Name() {
				t.Fatalf("typed exact owner/Go cause missing: %v", result)
			}
			originalCause := first.err
			for i := 0; i < 2; i++ {
				other := errors.New("later returned reader diagnostic")
				chunk = backend.LogChunk{Data: []byte("later data"), NextOffset: 10, Complete: true}
				result = other
				// Defensive repetition is SAME original Go file only. Production admission
				// excludes any second opened owner after the first sticky Close failure.
				b.mu.Lock()
				b.finishLogReader(file, &chunk, &result)
				same := b.logReaderErr
				b.mu.Unlock()
				if same != first || same.file != file || same.err != originalCause || !errors.Is(result, other) || !errors.Is(result, originalCause) || !emptyLogReaderChunk(chunk) {
					t.Fatalf("sticky owner/cause overwritten: %v", result)
				}
			}
			id := strings.Repeat("4", 32)
			chunk, result = b.Logs(context.Background(), backend.Reference{ID: id}, 0, 1)
			if !errors.As(result, &owner) || owner != first || !errors.Is(result, originalCause) || !emptyLogReaderChunk(chunk) {
				t.Fatalf("sticky read reached absent file/domain: %+v %v", chunk, result)
			}
			if err := b.CheckShutdown(); !errors.Is(err, errShutdownHeld) || !errors.Is(err, originalCause) {
				t.Fatalf("shutdown lost held original reader: %v", err)
			}
			closeErr := f.close()
			if !errors.As(closeErr, &owner) || owner != first || !errors.Is(closeErr, originalCause) || f.close() != closeErr {
				t.Fatalf("Close lost cached original reader error: %v", closeErr)
			}
			b.mu.Lock()
			same := b.logReaderErr
			b.mu.Unlock()
			if same != first || same.file != file || same.err != originalCause {
				t.Fatal("Close dropped/replaced original reader")
			}
			executionFixtureHolds.Lock()
			matches := 0
			for _, frame := range executionFixtureHolds.frames {
				if frame == f && frame.backend == b && frame.closeErr == closeErr {
					matches++
				}
			}
			executionFixtureHolds.Unlock()
			if matches != 1 || !f.retained {
				t.Fatal("same Backend/Go reader/error/path frame was not strongly retained exactly once")
			}
		})
	}
}

func TestLogReaderWriterReceiptCannotRemoveHeldReader(t *testing.T) {
	f := newExecutionFixture(t, true)
	b := f.backend
	presentID, absentID := strings.Repeat("5", 32), strings.Repeat("6", 32)
	present := logReaderExecution(t, f, presentID, "success")
	absent := logReaderExecution(t, f, absentID, "success")
	if err := b.Retention().RemoveOutput(context.Background(), absent); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	absentDomain := b.retention.outputs[absentID]
	b.mu.Unlock()
	if absentDomain != nil {
		t.Fatal("positive prior removal did not reach actual absent-domain branch")
	}
	before, err := os.ReadFile(filepath.Join(b.directory, presentID+".log"))
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CheckShutdown(); err != nil {
		t.Fatalf("actual original writers not positively joined: %v", err)
	}
	file := logReaderPreclosed(t, f)
	f.held = true
	var result error
	chunk := backend.LogChunk{Data: []byte("reader data")}
	b.mu.Lock()
	b.finishLogReader(file, &chunk, &result)
	original := b.logReaderErr
	b.mu.Unlock()
	if original == nil || original.file != file || !errors.Is(result, os.ErrClosed) {
		t.Fatal("preclosed Go reader vector did not retain original error", result)
	}
	// Actual writer receipts remain valid, including their released payload. They
	// cannot settle the separate reader owner or authorize any cleanup branch.
	receipt, err := b.Retention().CheckOutputClosed(context.Background(), present)
	if err != nil || receipt.ClosureID != present.ClosureID || !receipt.PayloadReleased {
		t.Fatalf("actual writer receipt changed: %+v %v", receipt, err)
	}
	for _, ref := range []backend.OutputReference{present, absent} {
		err := b.Retention().RemoveOutput(context.Background(), ref)
		var owner *logReaderCloseError
		if !errors.As(err, &owner) || owner != original || !errors.Is(err, original.err) {
			t.Fatalf("writer receipt bypassed held reader: %v", err)
		}
	}
	after, err := os.ReadFile(filepath.Join(b.directory, presentID+".log"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("held reader permitted actual present log removal/change", err)
	}
	if _, err := os.Lstat(filepath.Join(b.directory, absentID+".log")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("absent output changed", err)
	}
	if err := b.CheckShutdown(); !errors.Is(err, original.err) || !errors.Is(err, errShutdownHeld) {
		t.Fatalf("writer receipt fabricated reader shutdown: %v", err)
	}
	if err := f.close(); !errors.Is(err, original.err) {
		t.Fatalf("strong fixture lost original reader cause: %v", err)
	}
}
