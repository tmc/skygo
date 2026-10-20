package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// This concrete case retains actual returned errors (and their original
// operation frames) plus at most one original observation file. It never retries
// Close, reopens beneath uncertainty, or removes an unsettled namespace.
type companionReadbackCase struct {
	path                                            string
	needsPrebackup                                  bool
	initialized, prebackup, completed, checked      bool
	setupErr, operationErr, observationErr, failure error
	observation                                     *os.File
}

var companionReadbackHolds struct {
	sync.Mutex
	cases []*companionReadbackCase
}

func newCompanionReadbackCase(t *testing.T, needsPrebackup bool) *companionReadbackCase {
	t.Helper()
	f := &companionReadbackCase{needsPrebackup: needsPrebackup}
	t.Cleanup(func() {
		settled := f.initialized && (!f.needsPrebackup || f.prebackup) && f.completed && f.checked && f.observation == nil && f.observationErr == nil && f.failure == nil
		if settled {
			if err := os.RemoveAll(f.path); err == nil {
				return
			} else {
				f.failure = err
				t.Errorf("remove positively settled case: %v", err)
			}
		}
		companionReadbackHolds.Lock()
		companionReadbackHolds.cases = append(companionReadbackHolds.cases, f)
		companionReadbackHolds.Unlock()
		t.Logf("HOLD same companion readback case %p path=%q setup=%v operation=%v observation=%v failure=%v; namespace retained, no Close retry", f, f.path, f.setupErr, f.operationErr, f.observationErr, f.failure)
	})
	home, err := os.UserHomeDir()
	if err == nil {
		parent := filepath.Join(home, "tmp", "skypilot", "20261020-companion-cli-readback-tests")
		err = os.MkdirAll(parent, 0700)
		if err == nil {
			f.path, err = os.MkdirTemp(parent, "case-")
		}
	}
	if err != nil {
		f.setupErr = err
		t.Fatal(err)
	}
	return f
}

func (f *companionReadbackCase) read(path string) (os.FileInfo, []byte, error) {
	if f.observation != nil || f.observationErr != nil {
		return nil, nil, errors.New("original observation remains held")
	}
	original, err := os.Open(path)
	if err != nil {
		f.observationErr = err
		return nil, nil, err
	}
	f.observation = original
	info, statErr := original.Stat()
	data, readErr := io.ReadAll(io.LimitReader(original, (16<<20)+1))
	if len(data) > 16<<20 {
		readErr = errors.Join(readErr, errors.New("empty-fixture observation exceeds 16 MiB"))
	}
	closeErr := original.Close() // sole attempt; retain ORIGINAL on any error
	if closeErr == nil {
		f.observation = nil
	}
	f.observationErr = errors.Join(statErr, readErr, closeErr)
	if f.observationErr != nil {
		return nil, nil, f.observationErr
	}
	return info, data, nil
}

type companionReadbackWriter struct {
	cause          error
	writes         int
	observe        func() error
	observationErr error
}

func (w *companionReadbackWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes == 1 {
		w.observationErr = w.observe()
	}
	return 0, w.cause
}

// Real stopped empty installations exercise the full command and Store API.
// Only the final stdout writer fails; no Store operation or filesystem owner is
// mocked. This is not a spontaneous kernel failure/uncertain-close oracle.
func TestArtifactCompanionCompletedReadbackFailure(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("artifact filesystem metadata unsupported")
	}
	for _, action := range []string{"backup", "restore"} {
		for _, jsonOutput := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/json=%v", action, jsonOutput), func(t *testing.T) {
				f := newCompanionReadbackCase(t, action == "restore")
				base := f.path
				fail := func(format string, args ...any) {
					t.Helper()
					f.failure = fmt.Errorf(format, args...)
					t.Fatal(f.failure)
				}
				data, root, bundle := filepath.Join(base, "state"), filepath.Join(base, "artifacts"), filepath.Join(base, "bundle")
				if err := os.Mkdir(root, 0700); err != nil {
					f.setupErr = err
					fail("create original root: %w", err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
				defer cancel()
				if _, _, err := execute(ctx, "state", "artifacts", "init", "--data-dir", data, "--artifact-dir", root, "--ack-writers-stopped"); err != nil {
					f.setupErr = err
					fail("initialize original root: %w", err)
				}
				f.initialized = true
				final := bundle
				args := []string{"state", "artifacts", "backup", "--data-dir", data, "--artifact-dir", root, "--output", bundle, "--ack-writers-stopped"}
				files := []string{"jobs.db", "manifest.json"}
				if action == "restore" {
					if _, _, err := execute(ctx, args...); err != nil {
						f.setupErr = err
						fail("original prebackup: %w", err)
					}
					f.prebackup = true
					final = filepath.Join(base, "restored")
					args = []string{"state", "artifacts", "restore", "--input", bundle, "--data-dir", final, "--ack-writers-stopped"}
					files = []string{"jobs.db", "artifacts/.skygo-artifact-root"}
				}
				if jsonOutput {
					args = append([]string{"--json"}, args...)
				}
				// Observe actual published originals at the first readback write.
				// Their identity/bytes must survive that write error without replacement.
				var directory os.FileInfo
				infos := make(map[string]os.FileInfo)
				contents := make(map[string][]byte)
				want := errors.New("readback writer failed")
				w := &companionReadbackWriter{cause: want, observe: func() error {
					f.completed = true // this Write occurs only AFTER Store returned nil
					var err error
					directory, err = os.Stat(final)
					if err != nil {
						return err
					}
					if !directory.IsDir() {
						return errors.New("completed operation did not publish a directory")
					}
					for _, name := range files {
						infos[name], contents[name], err = f.read(filepath.Join(final, name))
						if err != nil {
							return err
						}
					}
					return nil
				}}
				c := New(nil)
				c.SetArgs(args)
				c.SetOut(w)
				var diagnostics bytes.Buffer
				c.SetErr(&diagnostics)
				err := c.ExecuteContext(ctx)
				f.operationErr = err // retain the exact returned error and its frame
				if w.observationErr != nil {
					f.observationErr = w.observationErr
				}
				if !errors.Is(err, want) || !strings.Contains(err.Error(), "companion "+action+" completed; write readback") {
					fail("completion/cause lost: %v", err)
				}
				if w.writes != 1 || w.observationErr != nil {
					fail("writes=%d published observation=%v", w.writes, w.observationErr)
				}
				current, statErr := os.Stat(final)
				if statErr != nil || !os.SameFile(directory, current) {
					fail("completed output removed/replaced: %v", statErr)
				}
				for _, name := range files {
					info, got, err := f.read(filepath.Join(final, name))
					if err != nil || !os.SameFile(infos[name], info) {
						fail("completed member removed/replaced %s: %v", name, err)
					}
					if !bytes.Equal(got, contents[name]) {
						fail("completed member changed after readback failure %s", name)
					}
				}
				f.checked = true // all original operation and observer frontiers positive
			})
		}
	}
}
