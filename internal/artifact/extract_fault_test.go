package artifact

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"
)

func TestExtractOwnedIOFailure(t *testing.T) {
	for _, directory := range []bool{false, true} {
		for _, closing := range []bool{false, true} {
			t.Run(fmt.Sprintf("directory=%v/close=%v", directory, closing), func(t *testing.T) {
				root, _ := testRoot(t)
				data := testArchive(t, testEntry{name: "dir/file", body: "owned bytes"})
				injected := errors.New("injected owned extraction I/O failure")
				calls := 0
				var failed *os.File
				fail := func(f *os.File) bool {
					info, err := f.Stat()
					if err != nil {
						t.Fatal(err)
					}
					return info.IsDir() == directory && (!directory || info.Name() == "dir")
				}
				ops := extractionIO{}
				if closing {
					ops.close = func(f *os.File) error {
						selected := fail(f)
						err := f.Close()
						if selected {
							calls++
							failed = f
							return errors.Join(err, injected)
						}
						return err
					}
				} else {
					ops.sync = func(f *os.File) error {
						selected := fail(f)
						err := f.Sync()
						if selected {
							calls++
							failed = f
							return errors.Join(err, injected)
						}
						return err
					}
				}
				err := extract(root, bytes.NewReader(data), int64(len(data)), ops)
				if !errors.Is(err, injected) {
					t.Fatalf("actual owned failure lost: %v", err)
				}
				if errors.Is(err, ErrFileCloseUncertain) != closing {
					t.Fatalf("close classification=%v closing=%v", err, closing)
				}
				if closing {
					var original *FileCloseError
					if !errors.As(err, &original) || original.file != failed || original.owner != nil {
						t.Fatal("lost archive-owned original close record")
					}
				}
				if calls != 1 || failed == nil {
					t.Fatalf("fault calls=%d file=%v", calls, failed)
				}
				if _, err := failed.Stat(); !errors.Is(err, os.ErrClosed) {
					t.Fatalf("failed owned descriptor remains open: %v", err)
				}
				if string(readFile(t, root, "dir/file")) != "owned bytes" {
					t.Fatal("fault fixture did not create actual partial unpublished file")
				}
			})
		}
	}
}
