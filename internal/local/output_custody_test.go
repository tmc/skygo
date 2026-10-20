package local

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/backend"
)

// No subprocess or native fault injector is used here. Each test file is
// removed only after its original actual Close returned nil. The negative
// vectors deliberately Close an already positively closed Go File; they test
// error/owner custody, not a kernel Close failure or a still-live descriptor.
type custodyFile struct {
	file           *os.File
	joined         bool
	closeAttempted bool
}

type custodyFiles struct {
	directory string
	files     []*custodyFile
	outputs   []*output
}

func newCustodyFiles(t *testing.T) *custodyFiles {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(home, "tmp", "skypilot", "20261019-local-output-custody", "tests")
	if err := os.MkdirAll(base, 0700); err != nil {
		t.Fatal(err)
	}
	directory, err := os.MkdirTemp(base, "output-")
	if err != nil {
		t.Fatal(err)
	}
	f := &custodyFiles{directory: directory}
	t.Cleanup(func() {
		joined := true
		for _, owned := range f.files {
			if owned.joined {
				continue
			}
			if owned.closeAttempted {
				t.Errorf("original test Close remains uncertain; preserving %s", directory)
				joined = false
				continue
			}
			owned.closeAttempted = true
			if err := owned.file.Close(); err != nil {
				t.Errorf("original test file Close uncertain; preserving %s: %v", directory, err)
				joined = false
				continue
			}
			owned.joined = true
		}
		for _, out := range f.outputs {
			if out.markerFile != nil || out.markerCloseErr != nil {
				t.Errorf("original production marker Close remains uncertain; preserving %s", directory)
				joined = false
			}
		}
		if joined {
			if err := os.RemoveAll(directory); err != nil {
				t.Error(err)
			}
		}
	})
	return f
}

func (f *custodyFiles) create(t *testing.T, name string) *custodyFile {
	t.Helper()
	file, err := os.OpenFile(filepath.Join(f.directory, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	owned := &custodyFile{file: file}
	f.files = append(f.files, owned)
	return owned
}

func (f *custodyFile) close(t *testing.T) {
	t.Helper()
	if f.joined {
		t.Fatal("test attempted a second positive Close")
	}
	if f.closeAttempted {
		t.Fatal("test attempted to retry an uncertain Close")
	}
	f.closeAttempted = true
	if err := f.file.Close(); err != nil {
		t.Fatal(err)
	}
	f.joined = true
}

func TestUnstartedOutputPositiveClose(t *testing.T) {
	for _, retained := range []bool{false, true} {
		name := "ordinary"
		if retained {
			name = "retained"
		}
		t.Run(name, func(t *testing.T) {
			if !supported() {
				t.Skip("local execution requires Linux or macOS")
			}
			files := newCustodyFiles(t)
			var b *Backend
			var err error
			if retained {
				b, err = NewRetained(files.directory, retentionConfig(t, files.directory))
			} else {
				b, err = New(files.directory)
			}
			if err != nil {
				t.Fatal(err)
			}
			id := strings.Repeat("a", 32)
			if retained {
				if err := b.BindOutput(retainedReference(t, b, id)); err != nil {
					t.Fatal(err)
				}
				b.mu.Lock()
				err = b.admitOutput(context.Background(), id, backend.Task{Argv: []string{"unstarted"}})
				b.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			}
			file := files.create(t, id+".log")
			out := &output{file: file.file}
			b.mu.Lock()
			if retained {
				out.retained = b.retention.outputs[id]
				out.retained.log, err = file.file.Stat()
			}
			if err == nil {
				file.closeAttempted = true
				err = b.closeUnstartedOutput(out)
			}
			b.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			file.joined = true // the original production Close just returned nil
			if !out.closed || b.unstartedOutput != nil || b.unstartedOutputErr != nil || len(b.jobs) != 0 {
				t.Fatal("positive output Close did not settle only its owner")
			}
			if retained && (!out.retained.closed || !out.retained.released || out.retained.closure == "") {
				t.Fatal("positive retained output Close not recorded")
			}
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestUnstartedOutputCloseErrorSticky(t *testing.T) {
	if !supported() {
		t.Skip("local execution requires Linux or macOS")
	}
	files := newCustodyFiles(t)
	b, err := New(files.directory)
	if err != nil {
		t.Fatal(err)
	}
	file := files.create(t, "original.log")
	file.close(t)
	out := &output{file: file.file, retained: &retainedOutput{}}
	b.mu.Lock()
	held := b.closeUnstartedOutput(out)
	again := b.closeUnstartedOutput(out)
	b.mu.Unlock()
	if !errors.Is(held, os.ErrClosed) || again != held || b.unstartedOutput != out || b.unstartedOutputErr != held || out.file != file.file {
		t.Fatal("original Go File/Close error not retained exactly", held, again)
	}
	if out.closed || out.retained.closed || out.retained.released || out.retained.closure != "" || len(b.jobs) != 0 {
		t.Fatal("uncertain Close manufactured positive closure or execution")
	}
	id := strings.Repeat("b", 32)
	if err := b.Start(context.Background(), id, backend.Task{Argv: []string{"must-not-execute"}}); !errors.Is(err, held) {
		t.Fatal("new ordinary Start lost held Close error", err)
	}
	if _, err := os.Lstat(filepath.Join(files.directory, id+".log")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("held output admitted another file", err)
	}
	if len(b.jobs) != 0 {
		t.Fatal("held output installed an execution")
	}
	for i := 0; i < 2; i++ {
		if err := b.Close(); !errors.Is(err, held) {
			t.Fatal("Close lost original held error", err)
		}
		if b.unstartedOutput != out || b.unstartedOutputErr != held {
			t.Fatal("Close retried or discarded original owner")
		}
	}
}

func TestOutputMarkerCreationSemantics(t *testing.T) {
	for _, tt := range []struct {
		name               string
		retained, existing bool
	}{
		{"ordinary-create", false, false},
		{"ordinary-truncate", false, true},
		{"retained-create", true, false},
		{"retained-exclusive", true, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			files := newCustodyFiles(t)
			log := files.create(t, "job.log")
			marker := filepath.Join(files.directory, "job.truncated")
			old := []byte("existing-marker-must-not-be-appended")
			if tt.existing {
				f := files.create(t, "job.truncated")
				if _, err := f.file.Write(old); err != nil {
					t.Fatal(err)
				}
				f.close(t)
			}
			out := &output{file: log.file, marker: marker}
			files.outputs = append(files.outputs, out)
			if tt.retained {
				out.retained = &retainedOutput{}
			}
			payload := make([]byte, backend.MaxLogBytes+1)
			if n, err := out.Write(payload); err != nil || n != len(payload) {
				t.Fatal(n, err)
			}
			if out.size != backend.MaxLogBytes || !out.truncated || out.markerFile != nil || out.markerCloseErr != nil {
				t.Fatal("marker owner did not positively close", out.err)
			}
			got, err := os.ReadFile(marker)
			if err != nil {
				t.Fatal(err)
			}
			if tt.retained && tt.existing {
				if !errors.Is(out.err, fs.ErrExist) || string(got) != string(old) || out.retained.marker != nil {
					t.Fatal("exclusive retained marker replaced existing bytes", out.err)
				}
			} else {
				if out.err != nil || string(got) != "truncated\n" {
					t.Fatal("marker content differs", out.err, string(got))
				}
				if tt.retained {
					info, err := os.Lstat(marker)
					if err != nil || out.retained.marker == nil || !os.SameFile(out.retained.marker, info) {
						t.Fatal("original marker identity not retained", err)
					}
				}
			}
			info, err := log.file.Stat()
			if err != nil || info.Size() != backend.MaxLogBytes {
				t.Fatal("retained output prefix differs", err)
			}
			log.close(t)
		})
	}
}

func TestOutputMarkerCloseErrorCustody(t *testing.T) {
	files := newCustodyFiles(t)
	marker := files.create(t, "marker")
	marker.close(t)
	out := &output{markerFile: marker.file, closed: true, retained: &retainedOutput{}}
	out.mu.Lock()
	held := out.closeMarker()
	again := out.closeMarker()
	out.mu.Unlock()
	if !errors.Is(held, os.ErrClosed) || again != held || out.markerCloseErr != held || out.markerFile != marker.file {
		t.Fatal("original marker owner/error discarded or retried", held)
	}
	// A modeled completion with the REAL Go Close error must not publish a
	// retention closure. This is not an injected kernel/Write Close failure.
	done := make(chan struct{})
	close(done)
	e := &execution{output: out, retained: out.retained, done: done}
	b := &Backend{jobs: map[string]*execution{"modeled": e}}
	b.mu.Lock()
	b.finishPayload(e)
	b.mu.Unlock()
	if out.retained.closed || out.retained.released || out.retained.closure != "" || out.markerFile != marker.file {
		t.Fatal("held marker manufactured payload closure")
	}
	if err := b.Close(); !errors.Is(err, held) {
		t.Fatal("Close lost retained marker error", err)
	}
	if out.markerFile != marker.file || out.markerCloseErr != held {
		t.Fatal("Close discarded or retried original marker")
	}
}
