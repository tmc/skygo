package artifact

import (
	"errors"
	"os"
	"testing"
)

func TestCollectionActualCloseClassification(t *testing.T) {
	for _, snapshotting := range []bool{false, true} {
		for _, uncertain := range []bool{false, true} {
			t.Run(map[bool]string{false: "collect", true: "snapshot"}[snapshotting]+map[bool]string{false: "/positive-close-writer-error", true: "/actual-close-error"}[uncertain], func(t *testing.T) {
				root, _ := testRoot(t)
				writeFile(t, root, "file", "owned bytes", 0600)
				var opened []*os.File
				opener := func(r *os.Root, name string, flags int) (*os.File, error) {
					file, err := r.OpenFile(name, flags, 0)
					if err != nil {
						return nil, err
					}
					opened = append(opened, file)
					if uncertain {
						// Order an actual failed cleanup Close on this ORIGINAL File:
						// the injected owner closed it before returning it. No raw FD,
						// unrelated process, fake filesystem or synthetic proof is used.
						if err := file.Close(); err != nil {
							t.Fatal(err)
						}
					}
					return file, nil
				}
				var err error
				if snapshotting {
					err = snapshot(root, failingWriter{}, opener)
				} else {
					err = collect(root, []string{"file"}, failingWriter{}, opener)
				}
				if err == nil || errors.Is(err, ErrFileCloseUncertain) != uncertain {
					t.Fatalf("classification=%v uncertain=%v", err, uncertain)
				}
				if len(opened) == 0 {
					t.Fatal("did not reach original owned complete-path open")
				}
				for _, file := range opened {
					if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
						t.Fatal("original descriptor not closed")
					}
				}
				if uncertain {
					var original *FileCloseError
					if !errors.As(err, &original) || original.file != opened[len(opened)-1] || original.owner != nil || !errors.Is(err, os.ErrClosed) {
						t.Fatal("lost actual archive cleanup Close failure")
					}
				}
			})
		}
	}
}
