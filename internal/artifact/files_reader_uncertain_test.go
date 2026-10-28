package artifact

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// corruptObject flips the first byte of ref's sealed object in place, keeping
// its inode, size, links and a private mode, so that only OpenObject's full
// byte verification can refuse it.
func corruptObject(t *testing.T, name string, root *os.Root, ref Ref) {
	t.Helper()
	if err := os.Chmod(filepath.Join(name, objectName(ref)), 0600); err != nil {
		t.Fatal(err)
	}
	file, err := root.OpenFile(objectName(ref), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 1)
	if _, err := file.ReadAt(b, 0); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if _, err := file.WriteAt(b, 0); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestOpenObjectValidationClose checks OpenObject's result when the full
// verification of an opened object fails. With an actual Close that
// succeeds, the error is ordinary and nothing is held. With an injected
// uncertain Close the Reader is still nil, yet the error retains the exact
// file the Close hook was given and the Files owner, and Files stays held: a
// nil Reader is not proof that nothing is open.
//
// The uncertain Close is MODELED: the hook closes the actual file and then
// returns uncertainty, so no kernel descriptor is physically left open. It
// proves Files' retention of the original, not a recovery.
func TestOpenObjectValidationClose(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		name := "proven close"
		if uncertain {
			name = "uncertain close"
		}
		t.Run(name, func(t *testing.T) {
			f, root, dir := filesFixture(t)
			data := testArchive(t, testEntry{name: "file", body: "payload"})
			ref := filesRef(strings.Repeat("2", 32), data)
			id := completePart(t, f, ref, data)
			if _, err := f.SealPart(ref, id, testPublicationGrowth); err != nil {
				t.Fatal(err)
			}
			corruptObject(t, dir, root, ref)
			injected := errors.New("actual close reported uncertainty")
			calls := 0
			var closed *os.File // the exact file the Close hook was given
			if uncertain {
				f.hooks.close = func(file *os.File) error {
					calls++
					closed = file
					return errors.Join(file.Close(), injected)
				}
			}
			reader, err := f.OpenObject(ref, id)
			if reader != nil || err == nil {
				t.Fatalf("open of corrupt bytes = %v, %v; want nil and an error", reader, err)
			}
			if !uncertain {
				if errors.Is(err, ErrFileCloseUncertain) || f.hasUncertainClose() || len(f.readers) != 0 {
					t.Fatalf("proven close = %v, want an ordinary error and nothing held", err)
				}
				return
			}
			if !errors.Is(err, ErrFileCloseUncertain) || !errors.Is(err, injected) || calls != 1 {
				t.Fatalf("uncertain close = %v after %d closes, want the typed original", err, calls)
			}
			var original *FileCloseError
			if !errors.As(err, &original) || original.owner != f || original.file == nil {
				t.Fatal("nil Reader lost the actual original owner")
			}
			if original.file != closed {
				t.Fatal("retained file is not the file the Close hook was given")
			}
			if !f.hasUncertainClose() {
				t.Fatal("Files released the uncertain original")
			}
			f.hooks = filesIO{}
			if _, err := f.OpenObject(ref, id); !errors.Is(err, ErrFilesBusy) || errors.Is(err, ErrFileCloseUncertain) {
				t.Fatalf("replacement open = %v, want ErrFilesBusy before any open", err)
			}
			if err := f.Close(); !errors.Is(err, ErrFilesBusy) {
				t.Fatalf("Files close = %v, want held by the uncertain original", err)
			}
			discardTestClosedOwners(t, f)
		})
	}
}
